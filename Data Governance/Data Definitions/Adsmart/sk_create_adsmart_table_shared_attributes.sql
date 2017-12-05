/* ***************************************************************************************
 *                                                                                       *
 *                          UPDATE ADSMART TABLE - SHARED_ATTRIBUTES                     *
 *                                                                                       *
 ***************************************************************************************/
MESSAGE 'Populate Table ${CBAF_DB_DATA_SCHEMA}.ADSMART for SHARED_ATTRIBUTES - Start' type status to client
go


MESSAGE 'Populate Table ${CBAF_DB_DATA_SCHEMA}.ADSMART for SHARED_ATTRIBUTES FROM CSAV' type status to client
go

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
   SET   sky_id = sav.sky_id
        ,tenure_split = CASE WHEN CAST(sav.acct_tenure_total_months AS INTEGER) <= 12             THEN 'Less than 1 year'
			     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 13 AND 15  THEN '13-15 months'
			     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 16 AND 24  THEN '16-24 months'
			     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 25 AND 36  THEN '2-3 yrs'
			     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 37 AND 60  THEN '3-5 yrs'
			     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 61 AND 120 THEN '5-10 yrs'
			     WHEN CAST(sav.acct_tenure_total_months AS INTEGER) >= 121             THEN '10+ yrs'
			 ELSE 'Unknown'
			 END  
        ,Sky_HD_Status =   CASE WHEN sav.prod_count_of_active_hd_subs  > 0  AND PROD_LATEST_HD_STATUS_CODE <> 'PC'
                                THEN 'Has HD'
				WHEN sav.PROD_LATEST_HD_STATUS_CODE = 'PC' 											      THEN 'Has HD-1 month cancellation period'
                        	WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) <=30
                         	THEN 'No HD downgraded in last 0-1 months'
				WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 31 AND 90
                 		THEN 'No HD downgraded in last 2-3 months'
		        	WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 91 AND 365
                        	THEN 'No HD downgraded in last 4-12 months'
				WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) > 365
                               	THEN 'No HD downgraded 13 months+'
				WHEN sav.prod_active_hd = 0 AND sav.acct_latest_hd_cancellation_dt IS NULL
                                THEN 'Never had HD'
				ELSE 'Never had HD'
			  END
       ,  hd_status =   CASE WHEN sav.prod_count_of_active_hd_subs  > 0 THEN 'Has HD'
			    WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) <=90 THEN 'No HD, downgraded in last 3 mth'
			    WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 91 AND 365   THEN 'No HD, downgraded in last 4 - 12 months'
			    WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) > 365 THEN 'No HD, hasn''t downgraded in last 12mths, had HD previously'
			    WHEN sav.prod_active_hd = 0 AND sav.acct_latest_hd_cancellation_dt IS NULL THEN 'Never had HD'
			ELSE 'Never had HD'
			END
	   ,  mr_status = CASE WHEN sav.prod_active_multiroom = 1 THEN 'Has MR'
			               WHEN sav.prod_active_multiroom = 0 AND sav.prod_latest_multiroom_cancellation_dt IS NOT NULL THEN 'No MR and never had previously'
				           WHEN sav.prod_active_multiroom = 0 AND sav.prod_latest_multiroom_cancellation_dt IS NULL     THEN 'Never had MR'
				           ELSE 'Never had MR'
		             END 					  
     , sky_go_extra = CASE WHEN sav.prod_latest_sky_go_extra_status_code IN ('AC','AB','PC') THEN 'Has Sky Go Extra'
                           WHEN sav.prod_first_sky_go_extra_activation_dt IS NULL THEN 'Never had Sky Go Extra'
	                       ELSE 'Never Had Sky Go Extra'
                      END
        ,  box_type = coalesce(sav.box_type,'Unknown')                					  
 ,  current_package = coalesce(sav.current_package,'Unknown')
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW sav
where a.account_number = sav.account_number 
    AND sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
--    AND sav.cb_key_household > 0
    AND sav.cb_key_household IS NOT NULL
    AND sav.account_number IS NOT NULL
	AND UPPER(sav.PTY_COUNTRY_CODE) in ('GBR','IRL')
GO
	
/* ***********************************
 *                                  *
 *         ON DEMAND LAST 6 MONTHS  *
 *                                  *
 ************************************/
--------------------------------------------------------------------
-- Populate on_demand_last_6_months FROM CUST_EST_ACCOUNT_LVL_AGGREGATIONS

MESSAGE 'Update field ON_DEMAND_IN_LAST_6_MONTHS in ADSMART Table' type status to client
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
        SET on_demand_in_last_6_months = case
                WHEN  cala.on_demand_latest_conn_dt >= dateadd(MONTH,-6,now()) 
			and cala.on_demand_latest_conn_dt > dateadd(DAY,14,sav.prod_dtv_activation_dt) THEN 'Yes'
                ELSE 'No'
                END
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_EST_AGGREGATIONS cala,
${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW sav
WHERE a.account_number = cala.account_number and
cala.account_number = sav.account_number
GO
MESSAGE 'Populate field ON_DEMAND_IN_LAST_6_MONTHS - COMPLETE' type status to client
GO
---------------------------------------------------------------------------

/* ***********************************
 *                                  *
 *         SKY_GO_REG               *
 *                                  *
 ************************************/
MESSAGE 'Populate field SKY_GO_REG - START' type status to client
go     
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_SKYGO_USAGE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SKYGO_USAGE already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE
  END
MESSAGE 'Create Table TEMP_SKYGO_USAGE' type status to client
SELECT sky.account_number,
       1 AS sky_go_reg
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE
FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_PLAYER_USAGE_DETAIL AS sky
INNER JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART as base
    ON sky.account_number = base.account_number
WHERE sky.cb_data_date >= dateadd(month, -12, now())
  AND sky.cb_data_date < now()
GROUP BY sky.account_number
go
-- Create Index
CREATE  HG INDEX idx04 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE(account_number)
go

MESSAGE 'Update field SKY_GO_REG to ADSMART Table' type status to client
go
-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
    SET Sky_Go_Reg = case when sky_go.sky_go_reg = 1 then 'Yes' else 'No' end
    FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE AS sky_go
    WHERE a.account_number = sky_go.account_number                                                                                    
go
MESSAGE 'Drop Table TEMP_SKYGO_USAGE' type status to client
go
drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE 
go
MESSAGE 'Populate field SKY_GO_REG - COMPLETE' type status to client
go


/* ***********************************
 *                                  *
 *         LEGACY SPORT            *
 *                                  *
 ************************************/
 
MESSAGE 'POPULATE FIELD FOR LEGACY_SPORT' TYPE STATUS TO CLIENT
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART A
SET LEGACY_SPORT = 'Yes'
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_SUBS_HIST CBH
WHERE A.ACCOUNT_NUMBER = CBH.ACCOUNT_NUMBER
AND UPPER(CBH.SUBSCRIPTION_SUB_TYPE) = 'ESPN'
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
INTO ${CBAF_DB_DATA_SCHEMA}.down_30_movies
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_DOWNGRADE_PIPELINE
where subscription_sub_type = 'DTV Primary Viewing'
and current_entitlement_prem_movies >0
and future_entitlement_prem_movies = 0
GO

-- Create Index
CREATE INDEX indx_MOVIES1 ON  ${CBAF_DB_DATA_SCHEMA}.down_30_movies(account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART  
SET	SKY_MOVIES_STATUS = CASE WHEN HIGHEST = 0 AND LOWEST = 0 THEN 'Never had Movies'
                         WHEN current_movies_premiums > 0 AND end_date >= TODAY() THEN 'Has Movies'
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

/* ***********************************
 *                                  *
 *          Catch Up Viewing        *
 *                                  *
 ************************************/

MESSAGE 'Populate field catch_up - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_CATCH_UP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CATCH_UP already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_CATCH_UP
  END

MESSAGE 'CREATE TABLE TEMP_CATCH_UP' type status to client
GO


SELECT   account_number
         ,MAX(last_modified_dt) AS last_modified_dt
INTO     ${CBAF_DB_LIVE_SCHEMA}.TEMP_CATCH_UP
FROM     ${CBAF_DB_LIVE_SCHEMA}.CUST_ANYTIME_PLUS_DOWNLOADS AS CAPD
WHERE    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND      x_actual_downloaded_size_mb > 1   -- to exclude any spurious header/trailer download records
AND      cs_referer LIKE '%Catch Up%'
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX TCU_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_CATCH_UP (account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET catch_up = CASE WHEN datediff(dd,last_modified_dt,TODAY()) <= 90  THEN 'Downloaded within 0 - 3 months'
                    WHEN datediff(dd,last_modified_dt,TODAY()) <= 180 THEN 'Downloaded within 3 - 6 months'
                    WHEN datediff(dd,last_modified_dt,TODAY()) <= 365 THEN 'Downloaded within  6 - 12 months'
		ELSE 'Unknown'
               END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_CATCH_UP AS TCU
ON AD.account_number = TCU.account_number
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_CATCH_UP

MESSAGE 'Populate field catch_up - END' type status to client
GO

/* ***********************************
 *                                  *
 *        Sky Sports Status         *
 *                                  *
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
INTO ${CBAF_DB_DATA_SCHEMA}.down_30_sport
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_DOWNGRADE_PIPELINE
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
SET SKY_SPORTS_STATUS = CASE WHEN HIGHEST = 0 AND LOWEST = 0 THEN 'Never had Sports'
                             WHEN current_SPORTS_premiums > 0 AND end_date >= TODAY() THEN 'Has Sports'
                             WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30                                      THEN 'No Sports downgraded in last 0-1 month'
                            WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90  	                  THEN 'No Sports downgraded in last 2-3 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 	 THEN 'No Sports downgraded in last 4-12 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 366 AND 730  THEN 'No Sports downgraded in last 13-24 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 731 AND 1826 THEN 'No Sports downgraded in last 25-60 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 1825               THEN 'No Sports downgraded 61 months+'
                         ELSE SKY_SPORTS_STATUS
                    END
      FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD 
INNER JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_PREMIUMS_2 	AS TMP 	ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_DG_DATE_2 	AS TMDD ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  ${CBAF_DB_DATA_SCHEMA}.TEMP_SPORTS_2 		AS TM 	ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
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
GO

MESSAGE 'Populate field SKY_SPORTS_STATUS - END' type status to client
GO


/* ***********************************
 *                                  *
 *           Box Set Viewing        *
 *                                  *
 ************************************/
MESSAGE 'Populate field box_set - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_BOX_SET'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_BOX_SET already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_BOX_SET
  END

MESSAGE 'CREATE TABLE TEMP_BOX_SET' type status to client
GO

SELECT   account_number
         ,MAX(last_modified_dt) AS last_modified_dt
INTO     ${CBAF_DB_LIVE_SCHEMA}.TEMP_BOX_SET
FROM     ${CBAF_DB_LIVE_SCHEMA}.CUST_ANYTIME_PLUS_DOWNLOADS AS CAPD
WHERE    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND      x_actual_downloaded_size_mb > 1   -- to exclude any spurious header/trailer download records
AND      cs_referer LIKE '%Box Sets%'
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX TBS_ACT ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_BOX_SET (account_number)
GO

-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET BOX_SET = CASE WHEN datediff(dd,last_modified_dt,TODAY()) <= 90  THEN 'Downloaded within 0 - 3 months'
                   WHEN datediff(dd,last_modified_dt,TODAY()) <= 180 THEN 'Downloaded within 3 - 6 months'
                   WHEN datediff(dd,last_modified_dt,TODAY()) <= 365 THEN 'Downloaded within  6 - 12 months'
		ELSE 'Unknown'
               END
FROM  ${CBAF_DB_DATA_SCHEMA}.ADSMART AS AD
INNER JOIN ${CBAF_DB_LIVE_SCHEMA}.TEMP_BOX_SET AS TBS
ON AD.account_number = TBS.account_number
GO

DROP TABLE ${CBAF_DB_LIVE_SCHEMA}.TEMP_BOX_SET

MESSAGE 'Populate field box_set - END' type status to client
GO


/* ***********************************
 *                                  *
 *           Primary Box Type              *
 *                                  *
 ************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET    PRIMARY_BOX_TYPE = CSHP_T.PrimaryBoxType
FROM   ${CBAF_DB_DATA_SCHEMA}.ADSMART AS base
   INNER JOIN (SELECT  stb.account_number
  ,SUBSTR(MIN(CASE WHEN x_description  in ('Sky Q Silver','Sky Q Mini') THEN '1 SkyQ Silver'
		   WHEN x_description = 'Sky Q'	THEN '2 SkyQ'
		   WHEN (stb.x_model_number LIKE '%W%' OR UPPER(stb.x_description) LIKE '%WI-FI%') AND  UPPER(stb.x_model_number) NOT LIKE '%UNKNOWN%'		THEN '3 890 or 895 Wifi Enabled'
		   WHEN stb.x_model_number IN ('DRX 890','DRX 895') AND stb.x_pvr_type IN ('PVR5','PVR6') THEN '4 890 or 895 Not Wifi Enabled'
		   WHEN stb.x_manufacturer IN ('Samsung','Pace') AND x_box_type = 'Sky+HD' THEN '5 Samsung or Pace Not Wifi Enabled'
                   ELSE '9 Unknown' END
                 ),3 ,100) AS PrimaryBoxType
   FROM  ${CBAF_DB_LIVE_SCHEMA}.cust_set_top_box AS stb
   WHERE stb.x_active_box_flag_new = 'Y' 
     AND account_number IS NOT NULL
GROUP BY  stb.account_number
    ) AS CSHP_T
      ON CSHP_T.account_number = base.account_number
GO

/* ************************
 *                       *
 *      A/B TESTING      *
 *                       *
 *************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
	SET AB_TESTING  = ROUND(CAST(RIGHT(CAST (ACCOUNT_NUMBER AS VARCHAR) ,2) AS INT)/5,0)+1 
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE 

/* ****************************************
 *                                       *
 *        SKY GO USAGE			 *
 *                                       *
 **************************************** --REPLACE BY A PRODUCTIONIZED TABLE -REWRITE THE DEFINITION ACCORDING TO THE DEFINITION */

MESSAGE 'POPULATE SKY GO USAGE - STARTS' type status to client
GO

SELECT   	  ACCOUNT_NUMBER 
		, SKYGO_USAGE_SEGMENT = CASE WHEN SKYGO_LATEST_USAGE_DATE >= DATEADD(MM,-3,GETDATE()) THEN 'Active'  -- ACTIVE USER: HAS USED SKYGO IN THE PAST 3 MONTHS
                                	WHEN SKYGO_LATEST_USAGE_DATE < DATEADD(MM,-3,GETDATE()) THEN 'Lapsed'        -- LAPSED > 1 YR: HAS USED SKYGO BETWEEN THE PAST YEAR AND 3 MONTHS AGO
                                	WHEN SKYGO_LATEST_USAGE_DATE IS NULL THEN 'Registered but never used'
                                        ELSE 'Non registered' END
    , RANK () OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY SKYGO_LATEST_USAGE_DATE DESC, SKYGO_FIRST_STREAM_DATE DESC, CB_ROW_ID DESC) TMP_RANK
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE
FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_OTT_USAGE_SUMMARY_ACCOUNT
GO

DELETE FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE
WHERE TMP_RANK > 1
GO

CREATE HG INDEX SKYGO1 ON ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE(ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET BASE.VIEWING_OF_SKY_GO = COALESCE(TMP_SKYGO_USG.SKYGO_USAGE_SEGMENT, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE
JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE AS TMP_SKYGO_USG ON BASE.ACCOUNT_NUMBER = TMP_SKYGO_USG.ACCOUNT_NUMBER  
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE
GO

MESSAGE 'POPULATE SKY GO USAGE - COMPLETED' type status to client
GO

/* ***********************************
 *                                  *
 *         ON OFFER                 *
 *                                  *
 ************************************/
 
 
 /* Logic:
	1.- Pull all the offers FROM cust_product_offers ranking them by active/expired, end of offer date, offer type (BB, TV,etc) 			====>>>> 	temp_Adsmart_end_of_offer_raw	
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
              AND UPPER(TNAME)='TEMP_ADSMART_END_OF_OFFER_RAW'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_raw' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND UPPER(TNAME)='TEMP_ADSMART_END_OF_OFFER_AGGREGATED'
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
WHERE    offer_id                NOT IN (SELECT offer_id FROM ${CBAF_DB_LIVE_SCHEMA}.sk2010_offers_to_exclude)
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
DELETE FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
WHERE account_number IN (SELECT account_number FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated WHERE Active_offer = 1) 
	AND Active_offer = 0 

-- Flagging the main(s) offer (Closest ending offer)
UPDATE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN    b.min_end_date = a.days_from_today THEN 1 ELSE 0 END
FROM ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw           AS a
JOIN ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated    AS b ON a.account_number = b.account_number 
AND a.Active_offer = b.Active_offer

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
ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL THEN 'No info on dates'
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
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_raw
DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_Adsmart_end_of_offer_aggregated
GO

---------------------------------------------------------------------------------------------------------------

-- Quarterly Release 1 : Update RENTAL_USAGE_OVER_LAST_12_MONTHS / SKY_STORE_RENTALS_USAGE_RECENCY     - Start

---------------------------------------------------------------------------------------------------------------

MESSAGE 'Update field RENTAL_USAGE_OVER_LAST_12_MONTHS in ADSMART Table' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND LOWER(TNAME)='temp_rental_usage_over_last_12_months'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_rental_usage_over_last_12_months ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_rental_usage_over_last_12_months
    END
MESSAGE 'CREATE TABLE temp_rental_usage_over_last_12_months' TYPE STATUS TO CLIENT
GO

SELECT b.account_number
	, MAX(ppv_ordered_dt) AS max_dt
	, SUM (CASE WHEN DATEDIFF(dd, ppv_ordered_dt, GETDATE()) <= 365 THEN 1 ELSE 0 END) rentals
INTO ${CBAF_DB_DATA_SCHEMA}.temp_rental_usage_over_last_12_months
FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_PRODUCT_CHARGES_PPV AS a 
JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART b 
ON a.account_number = b.account_number
WHERE ( ca_product_id LIKE 'PVOD%' OR ca_product_id LIKE 'NAM%' OR ca_product_id LIKE 'VCM%')
AND ppv_cancelled_dt = '9999-09-09'
GROUP BY b.account_number
GO

CREATE HG INDEX id1 ON ${CBAF_DB_DATA_SCHEMA}.temp_rental_usage_over_last_12_months (account_number)
CREATE LF INDEX id3 ON ${CBAF_DB_DATA_SCHEMA}.temp_rental_usage_over_last_12_months (rentals)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET RENTAL_USAGE_OVER_LAST_12_MONTHS = CASE WHEN  cps.rentals BETWEEN 1 AND 4 	THEN 'Rented 1-4'
											WHEN  cps.rentals BETWEEN 5 AND 7 	THEN 'Rented 5-7'
											WHEN  cps.rentals BETWEEN 8 AND 10	THEN 'Rented 8-10'
											WHEN  cps.rentals BETWEEN 11 AND 18	THEN 'Rented 11-18'
											WHEN  cps.rentals > 18			THEN 'Rented 18+'
                                                                                        WHEN  cps.rentals = 0			THEN 'Never Rented'
											ELSE 'Unknown'
										END
	, SKY_STORE_RENTALS_USAGE_RECENCY = CASE 	WHEN DATEDIFF(dd, max_dt, GETDATE()) <= 90 THEN 'Rented 0-3 mths back'
											WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 91	AND 180 THEN 'Rented 4-6 mths back'
											WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 181 AND 365 THEN 'Rented 7-12 mths back'
											WHEN DATEDIFF(dd, max_dt, GETDATE())  > 365 THEN 'Rented 12+ mths back'
 											WHEN max_dt IS NULL THEN 'Never Rented'
											ELSE 'Unknown'
										END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS a LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_rental_usage_over_last_12_months	AS cps 
ON a.account_number = cps.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_rental_usage_over_last_12_months

-- CCN1857 : Add RENTAL_USAGE_OVER_LAST_12_MONTHS - End

/* ***************************************************************************************
 *                                                                                      *
 *     BUY & KEEP Recency  										            			*
 *                                                                                      *
 ***************************************************************************************/

 MESSAGE 'Update field BUY_AND_KEEP_USAGE_RECENCY in ADSMART Table' type status to client
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_buy_and_keep_usage_recency'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_buy_and_keep_usage_recency ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_buy_and_keep_usage_recency
    END

MESSAGE 'CREATE TABLE temp_buy_and_keep_usage_recency' TYPE STATUS TO CLIENT
GO 
 
SELECT b.account_number
	, MAX(est_latest_purchase_dt) AS max_dt
INTO ${CBAF_DB_DATA_SCHEMA}.temp_buy_and_keep_usage_recency
FROM ${CBAF_DB_LIVE_SCHEMA}.FACT_EST_CUSTOMER_SNAPSHOT	AS a 
JOIN ${CBAF_DB_DATA_SCHEMA}.adsmart	AS b ON a.account_number = b.account_number
WHERE est_latest_purchase_dt IS NOT NULL 
	OR est_first_purchase_dt IS NOT NULL 	
GROUP BY b.account_number
GO

-- Create Index 
CREATE HG INDEX id1 ON ${CBAF_DB_DATA_SCHEMA}.temp_buy_and_keep_usage_recency (account_number)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart a
SET BUY_AND_KEEP_USAGE_RECENCY = CASE 	WHEN DATEDIFF(dd, max_dt, GETDATE()) <= 90 				THEN 'Bought 0-3 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 91 	AND 180 THEN 'Bought 4-6 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 181 	AND 365 THEN 'Bought 7-12 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE())  > 365 				THEN 'Bought 12+ mths back'
										WHEN max_dt IS NULL 													THEN 'Never Bought'
										ELSE 'Unknown' 
								 END
FROM ${CBAF_DB_DATA_SCHEMA}.adsmart a
LEFT JOIN ${CBAF_DB_DATA_SCHEMA}.temp_buy_and_keep_usage_recency AS cps 
ON a.account_number = cps.account_number
GO	

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.temp_buy_and_keep_usage_recency
GO

/* ***********************************************
*            ROI On/Off Net Fibre                   *
************************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET POC_COUNT  = COALESCE(b.attribute_value, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART  AS a
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.SKY_ADVANCE_AGGREGATED_ADSMART_TRAITS_POC AS b ON a.account_number = b.account_number
GO

/* ******************************************
*           Residency                       *
********************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET residency =  CASE  WHEN PTY_COUNTRY_CODE LIKE 'GBR' THEN 'UK'
                       WHEN PTY_COUNTRY_CODE LIKE 'IRL' THEN 'ROI'
                       ELSE  'Unknown'
                 END
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART a
LEFT JOIN ${CBAF_DB_LIVE_SCHEMA}.CUST_SINGLE_ACCOUNT_VIEW b
ON a.account_number = b.account_number
WHERE b.account_number <> '99999999999999'
    AND b.account_number not like '%.%'
    AND b.cust_active_dtv = 1
    AND b.cust_primary_service_instance_id is not null
    AND b.cb_key_household > 0
    AND b.cb_key_household IS NOT NULL
    AND b.account_number IS NOT NULL
    AND b.PTY_COUNTRY_CODE in ('GBR','IRL')
GO

/************************************
 *                                  *
 *         PANEL ID                 *
 *                                  *
 ************************************/   
MESSAGE 'Populate field VIEWING_PANEL_ID - START' type status to client
go  
Update ${CBAF_DB_DATA_SCHEMA}.ADSMART a
    SET  a.viewing_panel_id = ves.panel_id_vespa
    FROM ${VA_SCHEMA}.Vespa_Single_Box_View ves
    where a.account_number = ves.account_number
--    and ves.panel_id_vespa = 12
go
MESSAGE 'Populate field VIEWING_PANEL_ID - COMPLETE' type status to client
go

/*************************************************
* Default all the UK attributes that are not 
* relevant for IRL customers to Unknown so that 
* IRL customers dont pick up UK defaults 
************************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART SET
	engagement_matrix_score		= 'Unknown',
	onnet_bb_area			= 'Unknown',
	fibre_available			= 'Unknown',
	broadband_ip			= 'Unknown',
	household_campaign_demand	= 'Unknown',
	family_lifestage		= 'Unknown'
WHERE UPPER(residency) = 'ROI'
GO

/*************************************************
* Default all the ROI attributes that are not 
* relevant for UK customers to Unknown so that 
* UK customers dont pick up IRL defaults 
************************************************/

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART SET
	ROI_FIBRE_AVAILABLE		= 'Unknown',
	ROI_CABLE_AVAILABLE		= 'Unknown',
	ROI_MOSAIC			= 'Unknown',
	ROI_SIMPLE_SEGMENTS		= 'Unknown'
WHERE UPPER(residency) = 'UK'
GO

/* ***************************************************************************************
 *                                                                                      *
 *                          CREATE ADSMART VIEW                                         *
 *                                                                                      *
 ***************************************************************************************/ 
MESSAGE 'Create ADSMART View for the new ADSMART Table' type status to client
go 
  create view ${CBAF_DB_LIVE_SCHEMA}.ADSMART as
  select * FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART
  go
MESSAGE 'View ${CBAF_DB_LIVE_SCHEMA}.ADSMART created successfully' type status to client
MESSAGE 'Creating Restricted Views on the ADSMART view' type status to client
go 
call  dba.create_restricted_views_all('ADSMART','${CBAF_DB_LIVE_SCHEMA}',1)
GO
MESSAGE 'Build ADSMART Table & View Process - Completed Successfully' type status to client

go
