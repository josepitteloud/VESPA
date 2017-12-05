/* ***********************************
 *                                  *
 *         Sky Sports status	    *
 *                              	*
 ************************************/

MESSAGE 'Populate field SKY_SPORTS_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= '${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(tname)='TEMP_SPORTS_2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_2 already exists - Drop AND recreate' type status to client
    drop table  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_2' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_SPORTS IS NULL THEN 0 ELSE ncel.prem_SPORTS END AS current_SPORTS_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2
FROM  ${CBAF_DB_LIVE_SCHEMA}.cust_subs_hist AS csh
inner join  ${CBAF_DB_LIVE_SCHEMA}.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_SPORTS_3 ON  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS_PREMIUMS_2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_PREMIUMS_2 already exists - Drop AND recreate' type status to client
    drop table  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_PREMIUMS_2
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_PREMIUMS_2' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_SPORTS_premiums) AS HIGHEST
       ,MIN(current_SPORTS_premiums) AS LOWEST
INTO  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_PREMIUMS_2
FROM  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS13 ON  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_PREMIUMS_2(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SPORTS_DG_DATE_2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SPORTS_DG_DATE_2 already exists - Drop AND recreate' type status to client
    drop table  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_DG_DATE_2
  END
MESSAGE 'CREATE TABLE TEMP_SPORTS_DG_DATE_2' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_DG_DATE_2
FROM  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2
WHERE current_SPORTS_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_SPORTS12 ON  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_DG_DATE_2(account_number)
GO

-- 30 days Pipeline accounts
select account_number, 1 dummy
INTO down_30_sport
FROM ${CBAF_DB_DATA_SCHEMA}.CUST_DOWNGRADE_PIPELINE
where subscription_sub_type = 'DTV Primary Viewing'
and current_entitlement_prem_sports >0
and future_entitlement_prem_sports = 0
COMMIT
GO
-- Create Index
CREATE INDEX indx_SPORTS1 ON  ${CBAF_DB_DATA_SCHEMA}.down_30_sport(account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET SKY_SPORTS_STATUS = CASE WHEN HIGHEST = 0 AND LOWEST = 0                                                                           	 THEN 'Never had Sports'
                         WHEN current_SPORTS_premiums > 0 AND end_date >= TODAY()                                                      	 THEN 'Has Sports'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30              	 THEN 'No Sports downgraded in last 0-1 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90  	 THEN 'No Sports downgraded in last 2-3 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 	 THEN 'No Sports downgraded in last 4-12 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 366 AND 730  THEN 'No Sports downgraded in last 13-24 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 731 AND 1826 THEN 'No Sports downgraded in last 25-60 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 1825               THEN 'No Sports downgraded 61 months+'
                         ELSE SKY_SPORTS_STATUS
                    END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD 
INNER JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_PREMIUMS_2 	AS TMP 	ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_DG_DATE_2 	AS TMDD ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2 			AS TM 	ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET SKY_SPORTS_STATUS = 'Has Sports-1 month cancellation period'
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD 
INNER JOIN  ${CBAF_DB_DATA_SCHEMA}.down_30_sport AS b 	ON AD.account_number = b.account_number
GO

DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2
DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_PREMIUMS_2
DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_DG_DATE_2
DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.down_30_sport
MESSAGE 'Populate field SKY_SPORTS_STATUS - END' type status to client
GO

/* ***********************************
 *                                  *
 *        SKY_MOVIES_STATUS	        *
 *                                  *
 ************************************/



MESSAGE 'Populate field SKY_MOVIES_STATUS - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(tname)='TEMP_MOVIES_2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_2  already exists - Drop AND recreate' type status to client
    drop table  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_2' type status to client
GO

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_movies IS NULL THEN 0 ELSE ncel.prem_movies END AS current_movies_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_2
FROM  ${CBAF_DB_LIVE_SCHEMA}.cust_subs_hist AS csh
         inner join  ${CBAF_DB_LIVE_SCHEMA}.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
AND subscription_sub_type = 'DTV Primary Viewing'
AND status_code IN ('AC','PC','AB')   -- Active records
AND csh.account_number IS NOT NULL
GO

-- Create Index
CREATE INDEX indx_MOVIES_1 ON  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_2(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES_PREMIUMS_2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_PREMIUMS_2 already exists - Drop AND recreate' type status to client
    drop table  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_PREMIUMS_2
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_PREMIUMS_2' type status to client
GO

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_movies_premiums) AS HIGHEST
       ,MIN(current_movies_premiums) AS LOWEST
INTO  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_PREMIUMS_2
FROM  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_2
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES12 ON  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_PREMIUMS_2(account_number)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_MOVIES_DG_DATE_2'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_MOVIES_DG_DATE_2 already exists - Drop AND recreate' type status to client
    drop table  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_DG_DATE_2
  END
MESSAGE 'CREATE TABLE TEMP_MOVIES_DG_DATE_2' type status to client
GO

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_DG_DATE_2
FROM  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_2
WHERE current_movies_premiums > 0
GROUP BY Account_number
GO

-- Create Index
CREATE INDEX indx_MOVIES22 ON  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_DG_DATE_2(account_number)
GO
-- 30 days Pipeline accounts
select account_number, 1 dummy
INTO down_30_movies
FROM ${CBAF_DB_DATA_SCHEMA}.CUST_DOWNGRADE_PIPELINE
where subscription_sub_type = 'DTV Primary Viewing'
and current_entitlement_prem_movies >0
and future_entitlement_prem_movies = 0

COMMIT
GO
-- Create Index
CREATE INDEX indx_MOVIES1 ON  ${CBAF_DB_DATA_SCHEMA}.down_30_movies(account_number)
GO
-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART  
SET	SKY_MOVIES_STATUS = CASE WHEN HIGHEST = 0 AND LOWEST = 0                  															THEN 'Never had Movies'
                         WHEN current_movies_premiums > 0 AND end_date >= TODAY()                       								THEN 'Has Movies'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30  				THEN 'No Movies downgraded in last 0-1 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90 	THEN 'No Movies downgraded in last 2-3 month'
						 WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 	THEN 'No Movies downgraded in last 4-12 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              	THEN 'No Movies downgraded 13 months+'
                         ELSE SKY_MOVIES_STATUS
                    END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_PREMIUMS_2 AS TMP ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_DG_DATE_2 AS TMDD ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_2 AS TM ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET SKY_MOVIES_STATUS = 'Has Movies-1 month cancellation period'
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD 
INNER JOIN  ${CBAF_DB_DATA_SCHEMA}.down_30_movies AS b 	ON AD.account_number = b.account_number
GO

DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_2
DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_PREMIUMS_2
DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.TEMP_MOVIES_DG_DATE_2
DROP TABLE  ${CBAF_DB_DATA_SCHEMA}.down_30_movies
GO

MESSAGE 'Populate field SKY_MOVIES_STATUS - END' type status to client
GO

/* ***********************************
 *                                  *
 *           Primary Box Type       *
 *                                  *
 ************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET    PRIMARY_BOX_TYPE = CSHP_T.PrimaryBoxType
FROM   ${CBAF_DB_DATA_SCHEMA}.ADSMART AS base
   INNER JOIN (SELECT  stb.account_number
  ,SUBSTR(MIN(CASE
	
		WHEN x_description  in ('Sky Q Silver','Sky Q Mini') 									THEN '1 SkyQ Silver'
		WHEN x_description = 'Sky Q'												 			THEN '2 SkyQ'
		WHEN (stb.x_model_number LIKE '%W%' OR UPPER(stb.x_description) LIKE '%WI-FI%') AND  UPPER(stb.x_model_number) NOT LIKE '%UNKNOWN%'		THEN '3 890 or 895 Wifi Enabled'
		WHEN stb.x_model_number IN ('DRX 890','DRX 895') AND stb.x_pvr_type IN ('PVR5','PVR6')  THEN '4 890 or 895 Not Wifi Enabled'
		WHEN stb.x_manufacturer IN ('Samsung','Pace')  AND x_box_type = 'Sky+HD'				THEN '5 Samsung or Pace Not Wifi Enabled'
          ELSE '9 Unknown' END
                 ),3 ,100) AS PrimaryBoxType
   FROM  ${CBAF_DB_LIVE_SCHEMA}.cust_set_top_box AS stb
   WHERE          stb.x_active_box_flag_new = ‘Y’ 
   AND account_number IS NOT NULL
GROUP BY  stb.account_number
    ) AS CSHP_T
   ON CSHP_T.account_number = base.account_number
GO

/************************************
 *                                  *
 *         YOUNGEST_ADULT_HOUSEHOLD *
 *                                  *
 ************************************/

MESSAGE 'Populate field age_group - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_AGE_GROUP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_AGE_GROUP already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
  END

MESSAGE 'CREATE TABLE TEMP_AGE_GROUP' type status to client
GO

SELECT  CON.cb_key_household
       ,CON.cb_key_individual
       ,CON.p_actual_age
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
FROM
(select cb_key_individual, max(CB_ROW_ID) AS MAX_ROW_ID
from ${CBAF_DB_LIVE_SCHEMA}.experian_consumerview
GROUP BY cb_key_individual) AS DUPE
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.experian_consumerview AS CON
ON DUPE.cb_key_individual = CON.cb_key_individual and DUPE.MAX_ROW_ID = CON.cb_row_id
GO

-- Create Index
CREATE HG INDEX ix_cbkeyhh ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP (cb_key_household)
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_AGE_GROUP_MAX'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_AGE_GROUP_MAX already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX
  END

MESSAGE 'CREATE TABLE TEMP_AGE_GROUP_MAX' type status to client
GO

SELECT  cb_key_household
       ,MAX(CASE WHEN p_actual_age >= 16 AND p_actual_age < 25 THEN 1 ELSE 0 END) AS HH_Has_Age_16to24
       ,MAX(CASE WHEN p_actual_age >= 25 AND p_actual_age < 35 THEN 1 ELSE 0 END) AS HH_Has_Age_25to34
       ,MAX(CASE WHEN p_actual_age >= 35 AND p_actual_age < 45 THEN 1 ELSE 0 END) AS HH_Has_Age_35to44
       ,MAX(CASE WHEN p_actual_age >= 45 AND p_actual_age < 55 THEN 1 ELSE 0 END) AS HH_Has_Age_45to54
       ,MAX(CASE WHEN p_actual_age >= 55					   THEN 1 ELSE 0 END) AS HH_Has_Age_Over_55
      
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX
FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
GROUP BY cb_key_household
GO

-- Create Index
CREATE HG INDEX ix_cbkeyhh2 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX (cb_key_household)
GO


-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET YOUNGEST_ADULT_HOUSEHOLD = CASE 	
						WHEN HH_Has_Age_16to24 = 1          THEN 'Youngest adult is 16-24'
						WHEN HH_Has_Age_25to34 = 1 			THEN 'Youngest adult is 25-34'
						WHEN HH_Has_Age_35to44 = 1 			THEN 'Youngest Adult is 35-44'
						WHEN HH_Has_Age_45to54 = 1 			THEN 'Youngest Adult is 45-54'
						WHEN HH_Has_Age_Over_55 = 1 		THEN 'Youngest Adult is 55+'
                     ELSE 'Unknown'
                END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX AS TAG
ON AD.cb_key_household = TAG.cb_key_household
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET YOUNGEST_ADULT_HOUSEHOLD = CASE 	
						WHEN CL_CURRENT_AGE BETWEEN 16 AND 24 	THEN 'Youngest adult is 16-24'
						WHEN CL_CURRENT_AGE	BETWEEN	24 AND 34 	THEN 'Youngest adult is 25-34'
						WHEN CL_CURRENT_AGE BETWEEN	34 AND 44 	THEN 'Youngest Adult is 35-44'
						WHEN CL_CURRENT_AGE BETWEEN	45 AND 54 	THEN 'Youngest Adult is 45-54'
						WHEN CL_CURRENT_AGE >= 55 			 	THEN 'Youngest Adult is 55+'
                     ELSE YOUNGEST_ADULT_HOUSEHOLD
                END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW AS sav
ON AD.account_number = sav.account_number
WHERE account_number NOT IN (SELECT account_number FROM TEMP_AGE_GROUP_MAX) 
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP
DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_AGE_GROUP_MAX
GO


MESSAGE 'Populate field age_group - END' type status to client
GO


/************************************
 *                                  *
 *        SIMPLE_SEGMENTATION       *
 *                                  *
 ************************************/

MESSAGE 'POPULATE SIMPLE_SEGMENTATION FIELDS - STARTS' type status to client
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART 
SET SIMPLE_SEGMENTATION = b.CASE 	
									WHEN LOWER(b.segment) LIKE '%support%'		THEN 	'Support'
									WHEN LOWER(b.segment) LIKE '%secure%'		THEN	'Secure'
									WHEN LOWER(b.segment) LIKE '%stimulate%'	THEN	'Stimulate'
									WHEN LOWER(b.segment) LIKE '%stabilise'		THEN	'Stabilise'
									WHEN LOWER(b.segment) LIKE '%start'			THEN	'Start'
													ELSE 'Unknown' END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
JOIN $${CBAF_DB_LIVE_SCHEMA}.SIMPLE_SEGMENTS AS b 
		ON a.account_number = b.account_number 
GO
MESSAGE 'POPULATE SIMPLE_SEGMENTATION FIELDS - END' type status to client
GO

---------------------------
--		BUNDLE, hd_status & tenure_split
--------------------------

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a



		Sky_HD_Status =   CASE 	WHEN sav.prod_count_of_active_hd_subs  > 0  AND PROD_LATEST_HD_STATUS_CODE <> 'PC' 	THEN 'Has HD'
								WHEN sav.PROD_LATEST_HD_STATUS_CODE = 'PC' 											THEN 'Has HD-1 month cancellation period'
								WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) <=30                 	THEN 'No HD downgraded in last 0-1 months'
								WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 31 AND 90		THEN 'No HD downgraded in last 2-3 months'
								WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 91 AND 365   	THEN 'No HD downgraded in last 4-12 months'
								WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) > 365                	THEN 'No HD downgraded 13 months+'
								WHEN sav.prod_active_hd = 0 AND sav.acct_latest_hd_cancellation_dt IS NULL          THEN 'Never had HD'
								ELSE 'Never had HD'
						  END
		 ,tenure_split = CASE WHEN CAST(sav.acct_tenure_total_months AS INTEGER) <= 12               THEN '0 - 12 months'
							 WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 13 AND 15  THEN '13-15 months'
							 WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 16 AND 24  THEN '16-24 months'
							 WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 25 AND 36  THEN '2 - 3 yrs'
							 WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 37 AND 60  THEN '3 - 5 yrs'
							 WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 61 AND 120 THEN '5 - 10 yrs'
							 WHEN CAST(sav.acct_tenure_total_months AS INTEGER) >= 121             THEN '10 yrs+'
						ELSE 'Unknown'
						END  
						
		 , cb_address_postcode_area = case 
			when sav.cb_address_postcode_district in ('BT1','BT2','BT3','BT4','BT5','BT6','BT7','BT8','BT9','BT10','BT11','BT12','BT13','BT14','BT15'
													 ,'BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24','BT25','BT26','BT27','BT28','BT29'
													 ,'BT36','BT37','BT38','BT39','BT40','BT41') 		THEN 'BT1'
			when sav.cb_address_postcode_district in ('BT42','BT43','BT44','BT45','BT46','BT47','BT48','BT49','BT50','BT51','BT52','BT53','BT54','BT55'
													 ,'BT56','BT57','BT58','BT59','BT78','BT81','BT82','BT83','BT84','BT85','BT86','BT87','BT88','BT90','BT91') THEN 'BT2'
			when sav.cb_address_postcode_district in ('BT30','BT31','BT32','BT33','BT34','BT35','BT60','BT61','BT62','BT63','BT64','BT65','BT66','BT67','BT68'
													 ,'BT69','BT70','BT71','BT71','BT73','BT74','BT75','BT76','BT77','BT79','BT80','BT92','BT93','BT94') 	THEN 'BT3'
			when sav.cb_address_postcode_district is null then 'Unknown'
			else sav.cb_address_postcode_area 
			end 
		, BUNDLE_TYPE = case 	when prod_latest_entitlement_genre in ('Original','Variety','Family') then  prod_latest_entitlement_genre
								when prod_latest_entitlement_genre in ('Sky Q Bundle') then  'SkyQ'
								else 'Others'
								end 		

FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW sav
where a.account_number = sav.account_number 
    AND sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
    
    AND sav.cb_key_household IS NOT NULL
    AND sav.account_number IS NOT NULL
	AND UPPER(sav.PTY_COUNTRY_CODE) in ('GBR','IRL')
GO
		
/************************************
 *                                  *
 *        Engagement Matrix		    *
 *                                  *
 ************************************/
		
DECLARE @m VARCHAR (6)
SELECT @m = MAX(observation_month)
FROM vespa_shared.M004_ENGAGEMENT_SCORE_H
COMMIT 
GO
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET ENGAGEMENT_MATRIX_SCORE = CASE 	WHEN UPPER (engagement_segment) LIKE '%HIGH%' THEN 'High'
									WHEN UPPER (engagement_segment) LIKE '%MED%'  THEN 'Medium'
									WHEN UPPER (engagement_segment) LIKE '%LOW%'  THEN 'Low'
									ELSE 'Unknown' END 
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a 
JOIN vespa_shared.M004_ENGAGEMENT_SCORE_H AS b ON a.account_number = b.account_number AND observation_month = @m

COMMIT 					  
GO

/* ***********************************
 *                                  *
 *         ON OFFER                 *
 *                                  *
 ************************************/
 
 
 /* Logic:
	1.- Pull all the offers from cust_product_offers ranking them by active/expired, end of offer date, offer type (BB, TV,etc) 			====>>>> 	temp_Adsmart_end_of_offer_raw	
	2.- Count Active and expired offer by account  						====>>>> temp_Adsmart_end_of_offer_aggregated
	3.- Delete Expired offer in _raw table for accounts that have active offers
	4.- Identify the main(s) offer for each account (main_offer flag) The main offer is the one that's end_date is closer to Today. If there are more than 1 offer ending the same date then both are flagged as main_offers
	5.- Delete non-main_offer 
	6.- Flag multi_offer accounts - Those with more than one main offer type
	7.- Dedupe the raw TABLE
	8.- Update adsmart main table
 */
MESSAGE 'POPULATE FIELD FOR ON_OFFER' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_raw'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_raw' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated
    END
GO

SELECT b.account_number
    , CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END AS sport_flag
    , CASE WHEN (lower (offer_dim_description) LIKE '%movie%' OR lower (offer_dim_description) LIKE '%cinema%') THEN 1 ELSE 0 END AS movie_flag
	, CASE  WHEN    x_subscription_type IN ('SKY TALK','BROADBAND') THEN 'BBT'
            WHEN    x_subscription_type LIKE 'ENHANCED' AND
                    x_subscription_sub_type LIKE 'Broadband DSL Line' THEN 'BBT'
            WHEN    x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')
                AND sport_flag = 1
                AND movie_flag = 0 THEN 'Sports'
            WHEN    x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')
                AND sport_flag = 0 
                AND movie_flag = 1 THEN 'Movies'
			WHEN    x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')
                AND sport_flag = 1
                AND movie_flag = 1 THEN 'Top Tier'
            WHEN    x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')
                AND sport_flag = 0
                AND movie_flag = 0 THEN 'TV Pack '
            ELSE 'Unknown'  END 																						AS Offer_type
    , offer_status
	, Active_offer = CASE WHEN offer_status IN  ('Active',' Pending Terminated', 'Blocked')  THEN 1 ELSE 0 END 
    , CASE 	WHEN Active_offer = 1 													THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
			WHEN offer_status = 'Terminated' 										THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
			END AS offer_end_date
    , ABS(DATEDIFF(dd, offer_end_date, getDATE())) 																		AS days_from_today
    , rank() OVER(PARTITION BY b.account_number 			ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_1
    , rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_2
    , CAST (0 AS bit)                                                      												AS main_offer
INTO    ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
FROM    ${CBAF_DB_LIVE_SCHEMA}.cust_product_offers 	AS CPO
JOIN    ${CBAF_DB_DATA_SCHEMA}.ADSMART 				AS b     ON CPO.account_number = b.account_number
WHERE    offer_id                NOT IN (SELECT offer_id FROM citeam.sk2010_offers_to_exclude)
		AND first_activation_dt > '1900-01-01'
		AND offer_end_dt >= DATEADD(year, -5, GETDATE())
		AND x_subscription_sub_type <> 'DTV Season Ticket'
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge','Sky Go Extra No Additional Charge with Sky Multiscreen')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE UPPER('%Price Protection Offer%')
        AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1              -- To keep the latest offer by each offer type
GO
CREATE HG INDEX id1 ON ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw(account_number)
GO
-----------     Identifying Accounts with more than one active offer
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_Adsmart_end_of_offer_aggregated ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated
    END
		MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_aggregated' TYPE STATUS TO CLIENT
GO

SELECT
    account_number
	, Active_offer
    , COUNT(*) offers
    , MIN(ABS(DATEDIFF(dd, getDATE(), offer_end_date)))  AS min_end_date    
INTO ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated
FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
GROUP BY account_number,Active_offer

COMMIT
GO
CREATE HG INDEX id2 ON ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated(account_number)
GO
-- Deleting expired offers when the account has an active offer
DELETE FROM temp_Adsmart_end_of_offer_raw
WHERE account_number IN (SELECT account_number FROM temp_Adsmart_end_of_offer_aggregated WHERE Active_offer = 1) 
	AND Active_offer = 0 

-- Flagging the main(s) offer (Closest ending offer)
UPDATE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN    b.min_end_date = a.days_from_today THEN 1 ELSE 0 END
FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw           AS a
JOIN ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated    AS b ON a.account_number = b.account_number AND a.Active_offer = b.Active_offer

-----------     Deleting other offers - not the main(s) (which end date is not the min date)
DELETE FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw        AS a
WHERE   main_offer = 0
GO
-----------     Updating multi offers (When the account has 2 or more main offers ending the same day)
UPDATE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw AS a
JOIN (SELECT account_number, count(*) hits FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw GROUP BY account_number HAVING hits > 1) AS b ON a.account_number = b.account_number
-----------     DEleting duplicates
DELETE FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw WHERE rankk_1 > 1              -- To keep the latest offer by each offer type
GO
-----------     Updating Adsmart table
UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart
SET ON_OFFER = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
					ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL                    				THEN 'No info on dates'
														WHEN Active_offer = 1 AND days_from_today  > 90           		THEN 'Live, ends in over 90 days'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Live, ends in 31-90 Days'
														WHEN Active_offer = 1 AND days_from_today  <= 30          		THEN 'Live, ends in next 30 days'
														WHEN Active_offer = 0 AND days_from_today  > 90           		THEN 'Expired, ended over 90 days ago'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Expired, ended 31-90 Days ago'
														WHEN Active_offer = 0 AND days_from_today  <= 30          		THEN 'Expired, ended in last 30 days'
														ELSE 'No Offer Ever' END
													  END
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart as a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw as b
ON a.account_number = b.account_number

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated
					  
					  
			