
/************************************
 *                                  *
 *         Movies Status  2          *
 *                                  *
 ************************************/



MESSAGE 'Populate field MOVIES_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES  already exists - Drop AND recreate' type status to client
    drop table  TEMP_MOVIES
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_movies IS NULL THEN 0 ELSE ncel.prem_movies END AS current_movies_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_MOVIES
FROM  cust_subs_hist AS csh
         inner join  cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.currency_code = 'GBP' -- Exclude Republic of Ireland
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_MOVIES ON  TEMP_MOVIES(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES_PREMIUMS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_PREMIUMS already exists - Drop AND recreate' type status to client
    drop table  TEMP_MOVIES_PREMIUMS
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_PREMIUMS' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_movies_premiums) AS HIGHEST
       ,MIN(current_movies_premiums) AS LOWEST
INTO  TEMP_MOVIES_PREMIUMS
FROM  TEMP_MOVIES
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES1 ON  TEMP_MOVIES_PREMIUMS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES_DG_DATE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_DG_DATE already exists - Drop AND recreate' type status to client
    drop table  TEMP_MOVIES_DG_DATE
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_DG_DATE' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  TEMP_MOVIES_DG_DATE
FROM  TEMP_MOVIES
WHERE current_movies_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES2 ON  TEMP_MOVIES_DG_DATE(account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART  
SET	MOVIES_STATUS_2 = CASE WHEN HIGHEST = 0 AND LOWEST = 0                  															THEN 'Never had Movies'
                         WHEN current_movies_premiums > 0 AND end_date >= TODAY()                       								THEN 'Has Movies'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30  				THEN 'No Movies downgraded in last 0-1 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90 	THEN 'No Movies downgraded in last 2-3 month'
						 WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 	THEN 'No Movies downgraded in last 4-12 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              	THEN 'No Movies downgraded 13 months+'
                         ELSE MOVIES_STATUS_2
                    END
FROM ADSMART AS AD
INNER JOIN  TEMP_MOVIES_PREMIUMS AS TMP ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES_DG_DATE AS TMDD ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES AS TM ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

DROP TABLE  TEMP_MOVIES
DROP TABLE  TEMP_MOVIES_PREMIUMS
DROP TABLE  TEMP_MOVIES_DG_DATE
GO

MESSAGE 'Populate field MOVIES_STATUS - END' type status to client
GO

/************************************
 *                                  *
 *         Sports Status 2           *
 *                                  *
 ************************************/

MESSAGE 'Populate field SPORTS_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS already exists - Drop AND recreate' type status to client
    drop table  TEMP_SPORTS
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_SPORTS IS NULL THEN 0 ELSE ncel.prem_SPORTS END AS current_SPORTS_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_SPORTS
FROM  cust_subs_hist AS csh
         inner join  cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.currency_code = 'GBP' -- Exclude Republic of Ireland
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_SPORTS ON  TEMP_SPORTS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS_PREMIUMS'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_PREMIUMS already exists - Drop AND recreate' type status to client
    drop table  TEMP_SPORTS_PREMIUMS
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_PREMIUMS' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_SPORTS_premiums) AS HIGHEST
       ,MIN(current_SPORTS_premiums) AS LOWEST
INTO  TEMP_SPORTS_PREMIUMS
FROM  TEMP_SPORTS
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS1 ON  TEMP_SPORTS_PREMIUMS(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS_DG_DATE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_DG_DATE already exists - Drop AND recreate' type status to client
    drop table  TEMP_SPORTS_DG_DATE
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_DG_DATE' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  TEMP_SPORTS_DG_DATE
FROM  TEMP_SPORTS
WHERE current_SPORTS_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS2 ON  TEMP_SPORTS_DG_DATE(account_number)
GO

-- Update ADSMART Table
UPDATE ADSMART
SET SPORTS_STATUS_2 = CASE WHEN HIGHEST = 0 AND LOWEST = 0                                                                             THEN 'Never had Sports'
                         WHEN current_SPORTS_premiums > 0 AND end_date >= TODAY()                                                      THEN 'Has Sports'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30              THEN 'No Sports downgraded in last 0-1 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90  THEN 'No Sports downgraded in last  2-3 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Sports downgraded in last 4-12 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Sports downgraded 13 months+'
                         ELSE SPORTS_STATUS_2
                    END
					


FROM ADSMART AS AD
INNER JOIN  TEMP_SPORTS_PREMIUMS AS TMP
ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  TEMP_SPORTS_DG_DATE AS TMDD
ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_SPORTS AS TM
ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

DROP TABLE  TEMP_SPORTS
DROP TABLE  TEMP_SPORTS_PREMIUMS
DROP TABLE  TEMP_SPORTS_DG_DATE

MESSAGE 'Populate field SPORTS_STATUS - END' type status to client
GO