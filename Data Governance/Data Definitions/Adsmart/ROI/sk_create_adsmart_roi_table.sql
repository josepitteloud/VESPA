/*  Title       : ADSMART_ROI Table Build Process 
    Created by  : Jose PItteloud
    Date        : Jan 2016
    Description : This is a sql to build the ROI ADSMART_ROI Table FROM the CUST_SINGLE_ACCOUNT view AND other tables.
                

    Modified by : 
    Changes     : 


*/
MESSAGE 'Process to build the ROI_ADSMART Table & View' type status to client
go
MESSAGE 'Drop table ROI_ADSMART AND view if it already exists' type status to client
IF EXISTS(SELECT tname FROM syscatalog 
            WHERE creator='' 
              AND UPPER(tname)='ADSMART_ROI' 
              AND UPPER(tabletype)='TABLE')
    BEGIN
        DROP TABLE ADSMART_ROI
        IF EXISTS(SELECT tname FROM syscatalog 
            WHERE creator= user_name()  
              AND upper(tname)='ADSMART_ROI' 
              AND upper(tabletype)='VIEW')
          BEGIN
             DROP VIEW  ADSMART_ROI
          END
        ELSE
          BEGIN
            MESSAGE 'WARN: Table ADSMART_ROI exists. View  ADSMART_ROI does not exists' type status to client
          END
    END
go

MESSAGE 'Create Table ADSMART_ROI' type status to client
CREATE TABLE ADSMART_ROI
(
	record_type             	integer         NULL DEFAULT NULL,
	account_number          	VARCHAR(20)     NULL DEFAULT NULL,
	version_number          	integer         NULL DEFAULT NULL,
	cb_key_household        	bigint          NULL DEFAULT NULL,
	-HAS_SKY_GO              	VARCHAR(3)      NULL DEFAULT 'Unknown',
	-HAS_SKY_ID                  VARCHAR(3)      NULL DEFAULT 'Unknown',
	-RENTAL_USAGE_OVER_LAST_12_MONTHS 	VARCHAR (15) NULL DEFAULT 'Unknown',
	-TENURE_SPLIT                  		VARCHAR(20)  NULL DEFAULT 'Unknown',
	-ON_DEMAND_IN_LAST_6_MONTHS			VARCHAR(3)   NULL DEFAULT 'Unknown',
	-BROADBAND_STATUS 					VARCHAR(100) NULL DEFAULT 'Never Had BB',
	-SKY_GO_EXTRA						VARCHAR(100) NULL DEFAULT 'Never had Sky Go Extra',
	-PRIMARY_BOX_TYPE					VARCHAR(100) NULL DEFAULT 'Unknown',
	-HD_STATUS							VARCHAR(100) NULL DEFAULT 'Never had HD',
	-MULTI_ROOM_STATUS					VARCHAR(100) NULL DEFAULT 'Never had MR',
	-MOVIES_STATUS						VARCHAR(100) NULL DEFAULT 'Never had Movies',
	-SPORTS_STATUS						VARCHAR(100) NULL DEFAULT 'Never had Sports',
	-VIEWING_OF_CATCH_UP                	VARCHAR(100) NULL DEFAULT 'Unknown',
	-VIEWING_OF_BOX_SETS                 VARCHAR(100) NULL DEFAULT 'Unknown',
	-VIEWING_OF_SKY_GO					VARCHAR(100) NULL DEFAULT 'Unknown',
	-AB_TESTING							integer		 NULL DEFAULT  NULL,
	-ON_OFFER 							VARCHAR(50)  NULL DEFAULT 'Unknown',
	-LEGACY_SPORT 						VARCHAR(3)   NULL DEFAULT 'Unknown',
	-SKY_STORE_RENTAL_USAGE_RECENCY 		VARCHAR(31)  NULL DEFAULT 'Unknown',
	-BUY_AND_KEEP_USAGE_RECENCY 			VARCHAR(26)  NULL DEFAULT 'Never Bought'
	-FIBRE_AVAILABLE           			VARCHAR(10)  NULL DEFAULT 'Unknown',
	-ON_OFF_NET_FIBRE             		VARCHAR(100) NULL DEFAULT 'Unknown',
	-CABLE_AVAILABLE			 			VARCHAR(10)  NULL DEFAULT 'Unknown'
	ROI_MOSAIC_HE						VARCHAR(100) NULL DEFAULT 'Unknown',
	ROI_COUNTY							VARCHAR(100) NULL DEFAULT 'Unknown',
	-Residency							VARCHAR(10)  NULL DEFAULT NULL,
	-ROI_SIMPLE_SEGMENTS					VARCHAR(100) NULL DEFAULT 'Unknown',
	-ROI_BROADBAND_IP 					VARCHAR(12)  NULL DEFAULT 'No IP Data',

)
go

MESSAGE 'Create Index for Table ADSMART_ROI - Start' type status to client
CREATE INDEX ACCOUNT_NUMBER_HG ON ADSMART_ROI(account_number)
go
CREATE INDEX CB_KEY_HOUSEHOLD_HG ON ADSMART_ROI(cb_key_household)
GO
MESSAGE 'Create Index for Table ADSMART_ROI - Complete' type status to client
go

/****************************************************************************************
 *                                                                                      *
 *                          POPULATE ADSMART_ROI TABLE                                      *
 *                                                                                      *
 ***************************************************************************************/
MESSAGE 'Populate Table ADSMART_ROI FROM the CUST_SINGLE_ACCOUNT_VIEW - Start' type status to client
go
INSERT INTO ADSMART_ROI
 ( 
	   record_type   
	 , account_number      
	 , version_number 
	 , cb_key_household              
	 , HAS_SKY_ID              
	 , TENURE_SPLIT
	 , BROADBAND_STATUS
	 , SKY_GO_EXTRA
	 , HD_STATUS   
	 , MULTI_ROOM_STATUS 
	 , Residency
	 , ROI_COUNTY
 )    
 SELECT 
		  4 as record_type             
		, sav.account_number          
		, 1 as version_number 
		, sav.cb_key_household        
		, CASE 	WHEN sav.sky_id IS NOT NULL THEN 'Yes' ELSE 'No' END 	AS HAS_SKY_ID
		, CASE 	WHEN CAST(sav.acct_tenure_total_months AS INTEGER) < 12               THEN 'Less than 1 year'
				WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 12 AND 23  THEN '1-2 yrs'
				WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 24 AND 35  THEN '2-3 yrs'
				WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 36 AND 59  THEN '3-5 yrs'
				WHEN CAST(sav.acct_tenure_total_months AS INTEGER) BETWEEN 60 AND 119 THEN '5-10 yrs'
				WHEN CAST(sav.acct_tenure_total_months AS INTEGER) >= 120             THEN '10+ yrs'
				ELSE 'Unknown'
				END tenure_split
		, CASE 	WHEN sav.prod_active_broadband_package_desc IS NULL AND broadband_latest_agreement_end_dt IS NULL THEN 'Never Had BB'
				WHEN sav.prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) <=365  THEN 'No BB, downgraded in last 0 - 12 months'
				WHEN sav.prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) BETWEEN 366 AND 730            THEN 'No BB, downgraded in last 12- 24 mths'
				WHEN sav.prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) >730                           THEN 'No BB, downgraded 24 months+'
				WHEN sav.prod_active_broadband_package_desc = 'Broadband Connect'                                                                             THEN 'Has BB  Product Connect'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited Pro' OR sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited' THEN 'Has BB Product Unlimited'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Lite'                                                                            THEN 'Has BB  Product Lite'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited Fibre'                                                                 THEN 'Has BB  Product Fibre'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Fibre Unlimited Pro'                                                                       THEN 'Has BB Product Fibre Pro'
				WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Everyday'                                                                        THEN 'Has BB Product Everyday'
				ELSE 'Never had BB'
				END broadband_status
		, CASE 	WHEN prod_latest_sky_go_extra_status_code IN ('AC','AB','PC') THEN 'Has Sky Go Extra'
				WHEN prod_first_sky_go_extra_activation_dt IS NULL THEN 'Never had Sky Go Extra'
				ELSE 'Never Had Sky Go Extra'
				END sky_go_extra
		, CASE 
				WHEN sav.prod_count_of_active_hd_subs  > 0                                        THEN 'Has HD'
				WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) <=90                 THEN 'No HD, downgraded in last 3 mth'
				WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) BETWEEN 91 AND 365   THEN 'No HD, downgraded in last 4 - 12 months'
				WHEN DATEDIFF(dd,sav.acct_latest_hd_cancellation_dt,TODAY()) > 365                THEN 'No HD, hasn''t downgraded in last 12mths, had HD previously'
				WHEN sav.prod_active_hd = 0 AND sav.acct_latest_hd_cancellation_dt IS NULL            THEN 'Never had HD'
				ELSE 'Never had HD'
				END hd_status
		, CASE 
				WHEN sav.prod_active_multiroom = 1 THEN 'Has MR'
				WHEN sav.prod_active_multiroom = 0 AND sav.prod_latest_multiroom_cancellation_dt IS NOT NULL THEN 'No MR AND never had previously'
				WHEN sav.prod_active_multiroom = 0 AND sav.prod_latest_multiroom_cancellation_dt IS NULL     THEN 'Never had MR'
				ELSE 'Never had MR'
				END mr_status
		, Residency = CASE 	WHEN PTY_COUNTRY_CODE LIKE 'GBP' THEN 'UK' 
							WHEN PTY_COUNTRY_CODE LIKE 'IRL' THEN 'ROI'
							ELSE 'Unknown' END
		, CASE
				-- take cleansed geographic county where address has been fully matched to Geodirectory
				WHEN cb_address_status = '1' and roi_address_match_source is not null and cb_address_county is not null THEN cb_address_county
				-- otherwise use standardised form of county from the Chordiant raw county field for all 26 counties
				WHEN UPPER(pty_county_raw) like '%DUBLIN%' THEN 'DUBLIN'
				-- make sure WESTMEATH is above MEATH in the hierarchy otherwise WESTMEATH will get set to MEATH!
				WHEN UPPER(pty_county_raw) like '%WESTMEATH%' THEN 'WESTMEATH'
				WHEN UPPER(pty_county_raw) like '%CARLOW%' THEN 'CARLOW'
				WHEN UPPER(pty_county_raw) like '%CAVAN%' THEN 'CAVAN'
				WHEN UPPER(pty_county_raw) like '%CLARE%' THEN 'CLARE'
				WHEN UPPER(pty_county_raw) like '%CORK%' THEN 'CORK'
				WHEN UPPER(pty_county_raw) like '%DONEGAL%' THEN 'DONEGAL'
				WHEN UPPER(pty_county_raw) like '%GALWAY%' THEN 'GALWAY'
				WHEN UPPER(pty_county_raw) like '%KERRY%' THEN 'KERRY'
				WHEN UPPER(pty_county_raw) like '%KILDARE%' THEN 'KILDARE'
				WHEN UPPER(pty_county_raw) like '%KILKENNY%' THEN 'KILKENNY'
				WHEN UPPER(pty_county_raw) like '%LAOIS%' THEN 'LAOIS'
				WHEN UPPER(pty_county_raw) like '%LEITRIM%' THEN 'LEITRIM'
				WHEN UPPER(pty_county_raw) like '%LIMERICK%' THEN 'LIMERICK'
				WHEN UPPER(pty_county_raw) like '%LONGFORD%' THEN 'LONGFORD'
				WHEN UPPER(pty_county_raw) like '%LOUTH%' THEN 'LOUTH'
				WHEN UPPER(pty_county_raw) like '%MAYO%' THEN 'MAYO'
				WHEN UPPER(pty_county_raw) like '%MEATH%' THEN 'MEATH'
				WHEN UPPER(pty_county_raw) like '%MONAGHAN%' THEN 'MONAGHAN'
				WHEN UPPER(pty_county_raw) like '%OFFALY%' THEN 'OFFALY'
				WHEN UPPER(pty_county_raw) like '%ROSCOMMON%' THEN 'ROSCOMMON'
				WHEN UPPER(pty_county_raw) like '%SLIGO%' THEN 'SLIGO'
				WHEN UPPER(pty_county_raw) like '%TIPPERARY%' THEN 'TIPPERARY'
				WHEN UPPER(pty_county_raw) like '%WATERFORD%' THEN 'WATERFORD'
				WHEN UPPER(pty_county_raw) like '%WEXFORD%' THEN 'WEXFORD'
				WHEN UPPER(pty_county_raw) like '%WICKLOW%' THEN 'WICKLOW'
				-- otherwise look for Dublin postal districts as raw county often null for these
				WHEN pty_county_raw is null and UPPER(pty_town_raw) like '%DUBLIN%' THEN 'DUBLIN'
				else 'UNKNOWN'
				end as county_clean
					
FROM  CUST_SINGLE_ACCOUNT_VIEW sav
WHERE sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
    AND sav.cust_active_dtv = 1
    AND sav.cust_primary_service_instance_id is not null
    AND sav.account_number IS NOT NULL               
    AND sav.fin_currency_code = 'EUR'

  go                                                                                                                                                                        
MESSAGE 'Populate Table ADSMART_ROI FROM the CUST_SINGLE_ACCOUNT_VIEW - Complete' type status to client
go    

/****************************************************************************************
 *                                                                                      *
 *                          UPDATE ADSMART_ROI TABLE                                    *
 *                                                                                      *
 ***************************************************************************************/
                                                                                                                     
/************************************
 *                                  *
 *         HAS_SKY_GO               *
 *                                  *
 ************************************/
MESSAGE 'Populate field HAS_SKY_GO - START' type status to client
go     
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_SKYGO_USAGE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SKYGO_USAGE already exists - Drop AND recreate' type status to client
    drop table  TEMP_SKYGO_USAGE
  END
MESSAGE 'Create Table TEMP_SKYGO_USAGE' type status to client
SELECT sky.account_number,
       1 AS sky_go_reg
INTO  TEMP_SKYGO_USAGE
FROM  SKY_PLAYER_USAGE_DETAIL AS sky
INNER JOIN ADSMART_ROI as base
    ON sky.account_number = base.account_number
WHERE sky.cb_data_date >= dateadd(month, -12, now())
  AND sky.cb_data_date < now()
GROUP BY sky.account_number
go
-- Create Index
CREATE  HG INDEX idx04 ON  TEMP_SKYGO_USAGE(account_number)
go

MESSAGE 'Update field SKY_GO_REG to ADSMART_ROI Table' type status to client
go
-- Update ADSMART_ROI Table
UPDATE ADSMART_ROI a
    SET HAS_SKY_GO = case when sky_go.sky_go_reg = 1 THEN 'Yes' else 'No' end
    FROM  TEMP_SKYGO_USAGE AS sky_go
    WHERE a.account_number = sky_go.account_number                                                                                    
go
MESSAGE 'Drop Table TEMP_SKYGO_USAGE' type status to client
go
drop table  TEMP_SKYGO_USAGE 
go
MESSAGE 'Populate field HAS_SKY_GO - COMPLETE' type status to client
go
/************************************
 *                                  *
 *         ON DEMAND LAST 6 MONTHS  *
 *                                  *
 ************************************/
--------------------------------------------------------------------
-- Populate on_demand_last_6_months FROM CUST_EST_ACCOUNT_LVL_AGGREGATIONS

MESSAGE 'Update field ON_DEMAND_IN_LAST_6_MONTHS in ADSMART_ROI Table' type status to client
GO

UPDATE ADSMART_ROI a
        SET ON_DEMAND_IN_LAST_6_MONTHS = case WHEN  cala.on_demand_latest_conn_dt >= dateadd(MONTH,-6,now()) 
								AND cala.on_demand_latest_conn_dt > dateadd(DAY,14,sav.prod_dtv_activation_dt) THEN 'Yes'
								ELSE 'No'
								END
FROM  CUST_EST_AGGREGATIONS cala,
 CUST_SINGLE_ACCOUNT_VIEW sav
WHERE a.account_number = cala.account_number AND
cala.account_number = sav.account_number
GO
MESSAGE 'Populate field ON_DEMAND_IN_LAST_6_MONTHS - COMPLETE' type status to client
GO
---------------------------------------------------------------------------

/************************************
 *                                  *
 *         Movies Status            *
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
AND csh.currency_code = 'EUR' 
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

-- Update ADSMART_ROI Table
UPDATE ADSMART_ROI
SET MOVIES_STATUS = CASE WHEN HIGHEST = 0 AND LOWEST = 0                  THEN 'Never had Movies'
                         WHEN current_movies_premiums > 0 AND end_date >= TODAY()                       THEN 'Has Movies'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30  THEN 			'No Movies, downgraded in last 0 - 1 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90 THEN  'No Movies, downgraded in last 2 - 3 month'
						 WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Movies, downgraded in last 4 - 12 month'
                         WHEN current_movies_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Movies, downgraded 13 months +'
                         ELSE Movies_Status
                    END
FROM ADSMART_ROI AS AD
INNER JOIN  TEMP_MOVIES_PREMIUMS AS TMP
ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES_DG_DATE AS TMDD
ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES AS TM
ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
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
 *         Sports Status            *
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
AND csh.currency_code = 'EUR'  
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

-- Update ADSMART_ROI Table
UPDATE ADSMART_ROI
SET SPORTS_STATUS = CASE WHEN HIGHEST = 0 AND LOWEST = 0                                                                               THEN 'Never had Sports'
                         WHEN current_SPORTS_premiums > 0 AND end_date >= TODAY()                                                      THEN 'Has Sports'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) <= 30              THEN 'No Sports, downgraded in last 0 - 1 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 31 AND 90  THEN 'No Sports, downgraded in last 2 - 3 month'
						 WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) BETWEEN 91 AND 365 THEN 'No Sports, downgraded in last 4 - 12 month'
                         WHEN current_SPORTS_premiums = 0 AND HIGHEST > 0 AND DATEDIFF(dd,premium_end_date,TODAY()) > 365              THEN 'No Sports, downgraded 13 months +'
                         ELSE SPORTS_Status
                    END
FROM ADSMART_ROI AS AD
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


/************************************
 *                                  *
 *         VIEWING_OF_CATCH_UP      *
 *                                  *
 ************************************/

MESSAGE 'Populate field catch_up - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_CATCH_UP'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_CATCH_UP already exists - Drop AND recreate' type status to client
    drop table  TEMP_CATCH_UP
  END

MESSAGE 'CREATE TABLE TEMP_CATCH_UP' type status to client
GO


SELECT   account_number
         ,MAX(last_modified_dt) AS last_modified_dt
INTO      TEMP_CATCH_UP
FROM      CUST_ANYTIME_PLUS_DOWNLOADS AS CAPD
WHERE    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND      x_actual_downloaded_size_mb > 1   -- to exclude any spurious header/trailer download records
AND      cs_referer LIKE '%Catch Up%'
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX TCU_ACT ON  TEMP_CATCH_UP (account_number)
GO

-- Update ADSMART_ROI Table
UPDATE ADSMART_ROI
SET catch_up = CASE WHEN datediff(dd,last_modified_dt,TODAY()) <= 90  THEN 'Downloaded within 0 - 3 months'
                    WHEN datediff(dd,last_modified_dt,TODAY()) <= 180 THEN 'Downloaded within 3 - 6 months'
                    WHEN datediff(dd,last_modified_dt,TODAY()) <= 365 THEN 'Downloaded within  6 - 12 months'
		ELSE 'Unknown'
               END
FROM  ADSMART_ROI AS AD
INNER JOIN  TEMP_CATCH_UP AS TCU
ON AD.account_number = TCU.account_number
GO

DROP TABLE  TEMP_CATCH_UP

MESSAGE 'Populate field catch_up - END' type status to client
GO

/************************************
 *                                  *
 *           Box Set Viewing        *
 *                                  *
 ************************************/
MESSAGE 'Populate field box_set - START' type status to client
GO

IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator= user_name() 
              AND UPPER(tname)='TEMP_BOX_SET'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_BOX_SET already exists - Drop AND recreate' type status to client
    drop table  TEMP_BOX_SET
  END

MESSAGE 'CREATE TABLE TEMP_BOX_SET' type status to client
GO

SELECT   account_number
         ,MAX(last_modified_dt) AS last_modified_dt
INTO      TEMP_BOX_SET
FROM      CUST_ANYTIME_PLUS_DOWNLOADS AS CAPD
WHERE    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND      x_actual_downloaded_size_mb > 1   -- to exclude any spurious header/trailer download records
AND      cs_referer LIKE '%Box Sets%'
GROUP BY account_number
GO

-- Create Index
CREATE HG INDEX TBS_ACT ON  TEMP_BOX_SET (account_number)
GO

-- Update ADSMART_ROI Table
UPDATE ADSMART_ROI
SET VIEWING_OF_BOX_SETS = CASE WHEN datediff(dd,last_modified_dt,TODAY()) <= 90  THEN 'Downloaded within 0 - 3 months'
                   WHEN datediff(dd,last_modified_dt,TODAY()) <= 180 THEN 'Downloaded within 3 - 6 months'
                   WHEN datediff(dd,last_modified_dt,TODAY()) <= 365 THEN 'Downloaded within  6 - 12 months'
		ELSE 'Unknown'
               END
FROM  ADSMART_ROI AS AD
INNER JOIN  TEMP_BOX_SET AS TBS
ON AD.account_number = TBS.account_number
GO

DROP TABLE  TEMP_BOX_SET

MESSAGE 'Populate field box_set - END' type status to client
GO


/************************************
 *                                  *
 *           Primary Box Type              *
 *                                  *
 ************************************/

UPDATE ADSMART_ROI
SET    PRIMARY_BOX_TYPE = CSHP_T.PrimaryBoxType
FROM   ADSMART_ROI AS base
   INNER JOIN (SELECT  stb.account_number
  ,SUBSTR(MIN(CASE
          WHEN (stb.x_model_number LIKE '%W%' OR UPPER(stb.x_description) LIKE '%WI-FI%') THEN '1 890 or 895 Wifi Enabled'
          WHEN stb.x_model_number IN ('DRX 890','DRX 895') AND stb.x_pvr_type IN ('PVR5','PVR6')  THEN '2 890 or 895 Not Wifi Enabled'
          WHEN stb.x_manufacturer IN ('Samsung','Pace') THEN '3 Samsung or Pace Not Wifi Enabled'
          ELSE '9 Unknown' END
                 ),3 ,100) AS PrimaryBoxType
   FROM   cust_set_top_box AS stb
   WHERE          stb.active_box_flag = 'Y'
   AND account_number IS NOT NULL
   AND x_model_number <> 'Unknown'
   GROUP BY  stb.account_number
    ) AS CSHP_T
   ON CSHP_T.account_number = base.account_number
GO

/************************************
 *                                  *
 *        ROI_SIMPLE_SEGMENTS       *
 *                                  *
 ************************************/

MESSAGE 'POPULATE ROI_SIMPLE_SEGMENTS FIELDS - STARTS' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='TEMP_SIMPLE_SEGMENTATION'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SIMPLE_SEGMENTATION ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE TEMP_SIMPLE_SEGMENTATION
    END

MESSAGE 'CREATE TABLE TEMP_SIMPLE_SEGMENTATION' TYPE STATUS TO CLIENT
GO

SELECT a.account_number
		, SEGMENTATION =  CASE 	
									WHEN LOWER(b.segment) LIKE '%support%'		THEN 	'Support'
									WHEN LOWER(b.segment) LIKE '%secure%'		THEN	'Secure'
									WHEN LOWER(b.segment) LIKE '%stimulate%'	THEN	'Stimulate'
									WHEN LOWER(b.segment) LIKE '%stabilise'		THEN	'Stabilise'
													ELSE 'Unknown' END
		, row_number()  OVER (PARTITION BY a.account_number ORDER BY observation_date DESC) AS rank_1
INTO TEMP_SIMPLE_SEGMENTATION
FROM ADSMART_ROI as a 						
JOIN  SIMPLE_SEGMENTS_HISTORY as b ON a.account_number = b.account_number

CREATE HG INDEX ISIMSEG ON TEMP_SIMPLE_SEGMENTATION(ACCOUNT_NUMBER)
GO

UPDATE ADSMART_ROI 
SET ROI_SIMPLE_SEGMENTS = b.SEGMENTATION
FROM ADSMART_ROI AS a 
JOIN TEMP_SIMPLE_SEGMENTATION AS b 
ON a.account_number = b.account_number AND b.rank_1 = 1
GO

DROP TABLE TEMP_SIMPLE_SEGMENTATION
GO

MESSAGE 'POPULATE ROI_SIMPLE_SEGMENTS FIELDS - END' type status to client
GO


/*		QA

SELECT account_number
	, Segment
	, rank() OVER (PARTITION BY account_number ORDER BY observation_date DESC) rankk 
INTO #roi_segment	
FROM  SIMPLE_SEGMENTS_ROI

SELECT 
	CASE  WHEN b.Segment LIKE '%Secure%'  THEN 'Secure'
								WHEN b.Segment LIKE '%Simulate%'  THEN 'Simulate'
								WHEN b.Segment LIKE '%Support%'  THEN 'Support'
								WHEN b.Segment LIKE '%Stabilise%'  THEN 'Stabilise'
								ELSE 'Unknown' END AS ROI_SIMPLE_SEGMENTS
		,  count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_segment AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY ROI_SIMPLE_SEGMENTS


ROI_SIMPLE_SEGMENTS	hits
Secure				151098
Stabilise			88069
Support				82023
Unknown				147976

*/

/*****************************************
 *                                       *
 *       VIEWING_OF_SKY_GO		 		*
 *                                       *
 **************************************** --REPLACE BY A PRODUCTIONIZED TABLE -REWRITE THE DEFINITION ACCORDING TO THE DEFINITION */

MESSAGE 'POPULATE VIEWING_OF_SKY_GO - STARTS' type status to client
GO

SELECT   	  ACCOUNT_NUMBER 
		, SKYGO_USAGE_SEGMENT = CASE WHEN SKYGO_LATEST_USAGE_DATE >= DATEADD(MM,-3,GETDATE()) THEN 'Active'  -- ACTIVE USER: HAS USED SKYGO IN THE PAST 3 MONTHS
                                	WHEN SKYGO_LATEST_USAGE_DATE < DATEADD(MM,-3,GETDATE()) THEN 'Lapsed'        -- LAPSED > 1 YR: HAS USED SKYGO BETWEEN THE PAST YEAR AND 3 MONTHS AGO
                                	WHEN SKYGO_LATEST_USAGE_DATE IS NULL THEN 'Registered but never used'
                                        ELSE 'Non registered' END
    , RANK () OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY SKYGO_LATEST_USAGE_DATE DESC, SKYGO_FIRST_STREAM_DATE DESC, CB_ROW_ID DESC) TMP_RANK
INTO TEMP_SKYGO_USAGE
FROM  SKY_OTT_USAGE_SUMMARY_ACCOUNT
GO

DELETE FROM TEMP_SKYGO_USAGE
WHERE TMP_RANK > 1
GO

CREATE HG INDEX SKYGO1 ON TEMP_SKYGO_USAGE(ACCOUNT_NUMBER)
GO

UPDATE ADSMART_ROI
SET BASE.VIEWING_OF_SKY_GO = COALESCE(TMP_SKYGO_USG.SKYGO_USAGE_SEGMENT, 'Unknown')
FROM ADSMART_ROI AS BASE
JOIN TEMP_SKYGO_USAGE AS TMP_SKYGO_USG ON BASE.ACCOUNT_NUMBER = TMP_SKYGO_USG.ACCOUNT_NUMBER  
GO

DROP TABLE TEMP_SKYGO_USAGE
GO

MESSAGE 'POPULATE VIEWING_OF_SKY_GO - COMPLETED' type status to client
GO

/*************************
 *                       *
 *      A/B TESTING      *
 *                       *
 *************************/

UPDATE ADSMART_ROI
	SET AB_TESTING  = ROUND(CAST(RIGHT(CAST (ACCOUNT_NUMBER AS VARCHAR) ,2) AS INT)/5,0)+1 
FROM ADSMART_ROI AS BASE 						

/************************************************
 *                       						*
 *      RENTAL_USAGE_OVER_LAST_12_MONTHS      	*
 *                       						*
 ************************************************/

MESSAGE 'Update field RENTAL_USAGE_OVER_LAST_12_MONTHS in ADSMART_ROI Table' type status to client
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND LOWER(TNAME)='temp_rental_usage_over_last_12_months'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_rental_usage_over_last_12_months ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_rental_usage_over_last_12_months
    END
MESSAGE 'CREATE TABLE temp_rental_usage_over_last_12_months' TYPE STATUS TO CLIENT
GO

SELECT b.account_number
	, MAX(ppv_ordered_dt) AS max_dt
	, SUM (CASE WHEN DATEDIFF(dd, ppv_ordered_dt, GETDATE()) <= 365 THEN 1 ELSE 0 END) rentals
INTO temp_rental_usage_over_last_12_months
FROM  CUST_PRODUCT_CHARGES_PPV AS a 
JOIN ADSMART_ROI b 
ON a.account_number = b.account_number
WHERE ( ca_product_id LIKE 'PVOD%' OR ca_product_id LIKE 'NAM%' OR ca_product_id LIKE 'VCM%')
AND ppv_cancelled_dt = '9999-09-09'
GROUP BY b.account_number
GO

CREATE HG INDEX id1 ON temp_rental_usage_over_last_12_months (account_number)
CREATE LF INDEX id3 ON temp_rental_usage_over_last_12_months (rentals)
GO

UPDATE ADSMART_ROI
SET RENTAL_USAGE_OVER_LAST_12_MONTHS = CASE WHEN  cps.rentals BETWEEN 1 AND 4 	THEN 'Rented 1-4'
											WHEN  cps.rentals BETWEEN 5 AND 7 	THEN 'Rented 5-7'
											WHEN  cps.rentals BETWEEN 8 AND 10	THEN 'Rented 8-10'
											WHEN  cps.rentals BETWEEN 11 AND 18	THEN 'Rented 11-18'
											WHEN  cps.rentals >  	18			THEN 'Rented 18+'
											ELSE 'Unknown'
										END
	, SKY_STORE_RENTALS_USAGE_RECENCY = CASE 	WHEN DATEDIFF(dd, max_dt, GETDATE()) <= 90 				THEN 'Rented 0-3 mths back'
												WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 91 	AND 180 THEN 'Rented 4-6 mths back'
												WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 181 	AND 365 THEN 'Rented 7-12 mths back'
												WHEN DATEDIFF(dd, max_dt, GETDATE())  > 365 				THEN 'Rented 12+ mths back'
												ELSE 'Unknown'
										END
FROM ADSMART_ROI AS a LEFT JOIN temp_rental_usage_over_last_12_months	AS cps 
ON a.account_number = cps.account_number
GO

DROP TABLE temp_rental_usage_over_last_12_months

/************************************
 *                                  *
 *         ON OFFER                 *
 *                                  *
 ************************************/
MESSAGE 'POPULATE FIELD FOR ON_OFFER' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_raw'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_Adsmart_end_of_offer_raw
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_raw' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_Adsmart_end_of_offer_aggregated
    END
GO 

SELECT b.account_number
	, CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END AS sport_flag
	, CASE WHEN lower (offer_dim_description) LIKE '%movie%' THEN 1 ELSE 0 END AS movie_flag
	, CASE 	WHEN 	x_subscription_type IN ('SKY TALK','BROADBAND')	THEN 'BBT'
			WHEN 	x_subscription_type LIKE 'ENHANCED' AND
					x_subscription_sub_type LIKE 'Broadband DSL Line' THEN 'BBT'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')
				AND sport_flag = 1
				AND movie_flag = 0 THEN 'Sports'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 0
				AND movie_flag = 1 THEN 'Movies'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 1
				AND movie_flag = 1 THEN 'Top Tier'				
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED')	
				AND sport_flag = 0
				AND movie_flag = 0 THEN 'TV Pack      '	
			ELSE 'Unknown' 	END AS Offer_type
	, CASE WHEN offer_end_dt >= GETDATE() THEN 1 ELSE 0 END	AS live_offer
	, DATE(offer_end_dt) AS offer_end_date
	, ABS(DATEDIFF(dd, offer_end_date, getDATE())) AS days_from_today
	, rank() OVER(PARTITION BY b.account_number ORDER BY live_offer DESC, days_from_today,cb_row_id) 			AS rankk_1
	, rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY live_offer DESC, days_from_today,cb_row_id) 			AS rankk_2
	, CAST (0 AS bit) 														AS main_offer 
INTO 	temp_Adsmart_end_of_offer_raw
FROM     cust_product_offers AS CPO  
JOIN 	ADSMART_ROI AS b 	ON CPO.account_number = b.account_number
WHERE    offer_id                NOT IN (SELECT offer_id FROM citeam.sk2010_offers_to_exclude)
        --AND offer_end_dt          > getdate() 
        AND offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
		AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1 				-- To keep the latest offer by each offer type 
GO
CREATE HG INDEX id1 ON temp_Adsmart_end_of_offer_raw(account_number)
GO
-----------		Identifying Accounts with more than one active offer
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_Adsmart_end_of_offer_aggregated ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_Adsmart_end_of_offer_aggregated
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_aggregated' TYPE STATUS TO CLIENT
GO

SELECT 
	account_number
	, COUNT(*) offers
	, MAX(live_offer)			AS live_offer_c
	, MIN(CASE 	WHEN offer_end_date >  GETDATE() THEN DATEDIFF(dd, getDATE(), offer_end_date) 	ELSE NULL END) 	AS live_date	
	, MIN(CASE	WHEN offer_end_date <= GETDATE() THEN DATEDIFF(dd,  offer_end_date, getDATE()) 	ELSE NULL END)	AS past_date
INTO temp_Adsmart_end_of_offer_aggregated
FROM temp_Adsmart_end_of_offer_raw
GROUP BY account_number
HAVING offers > 1
GO

CREATE HG INDEX id2 ON temp_Adsmart_end_of_offer_aggregated(account_number)
GO

UPDATE temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN  	b.live_offer_c = a.live_offer 
							AND (CASE WHEN live_offer_c =1 	THEN b.live_date 
															ELSE b.past_date END) = a.days_from_today THEN 1 ELSE 0 END
FROM temp_Adsmart_end_of_offer_raw 			AS a 
JOIN temp_Adsmart_end_of_offer_aggregated 	AS b ON a.account_number = b.account_number

-----------		Deleting offers which end date is not the min date 
DELETE FROM temp_Adsmart_end_of_offer_raw		AS a
WHERE  	main_offer = 0 
GO
-----------		Updating multi offers
UPDATE temp_Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM temp_Adsmart_end_of_offer_raw AS a 
JOIN (SELECT account_number, count(*) hits FROM temp_Adsmart_end_of_offer_raw GROUP BY account_number HAVING hits > 1) AS b ON a.account_number = b.account_number 
-----------		DEleting duplicates
DELETE FROM temp_Adsmart_end_of_offer_raw WHERE rankk_1 > 1 				-- To keep the latest offer by each offer type 
GO

-----------		Updating ADSMART_ROI table

UPDATE ADSMART_ROI
SET ON_OFFER = CASE WHEN b.account_number IS NULL THEN 'Unknown'
ELSE TRIM(offer_type) ||' '||
  CASE 	WHEN days_from_today IS NULL 					THEN 'No info on dates'
	WHEN live_offer = 1 AND days_from_today  > 90			THEN 'Live, ends in over 90 days'
	WHEN live_offer = 1 AND days_from_today  BETWEEN 31 AND 90	THEN 'Live, ends in 31-90 Days'
	WHEN live_offer = 1 AND days_from_today  <= 30 			THEN 'Live, ends in next 30 days'
	WHEN live_offer = 0 AND days_from_today  > 90 			THEN 'Expired, ended over 90 days ago'
	WHEN live_offer = 0 AND days_from_today  BETWEEN 31 AND 90	THEN 'Expired, ended 31-90 Days ago'
	WHEN live_offer = 0 AND days_from_today  <= 30 			THEN 'Expired, ended in last 30 days'
	ELSE 'Unknown' END 
  END 
FROM ADSMART_ROI as a 
LEFT JOIN temp_Adsmart_end_of_offer_raw as b 
ON a.account_number = b.account_number

DROP TABLE temp_Adsmart_end_of_offer_raw
DROP TABLE temp_Adsmart_end_of_offer_aggregated


/************************************
 *                                  *
 *         LEGACY SPORT            *
 *                                  *
 ************************************/
 
MESSAGE 'POPULATE FIELD FOR LEGACY_SPORT' TYPE STATUS TO CLIENT
GO

UPDATE ADSMART_ROI A
SET LEGACY_SPORT = 'Yes'
FROM  CUST_SUBS_HIST CBH
WHERE A.ACCOUNT_NUMBER = CBH.ACCOUNT_NUMBER
AND UPPER(CBH.SUBSCRIPTION_SUB_TYPE) = 'ESPN'
GO


/****************************************************************************************
 *                                                                                      *
 *     BUY & KEEP Recency  										            			*
 *                                                                                      *
 ***************************************************************************************/

 MESSAGE 'Update field BUY_AND_KEEP_USAGE_RECENCY in ADSMART_ROI Table' type status to client
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR=''
              AND lower(TNAME)='temp_buy_and_keep_usage_recency'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_buy_and_keep_usage_recency ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_buy_and_keep_usage_recency
    END

MESSAGE 'CREATE TABLE temp_buy_and_keep_usage_recency' TYPE STATUS TO CLIENT
GO 
 
SELECT b.account_number
	, MAX(est_latest_purchase_dt) AS max_dt
INTO temp_buy_and_keep_usage_recency
FROM  FACT_EST_CUSTOMER_SNAPSHOT	AS a 
JOIN ADSMART_ROI	AS b ON a.account_number = b.account_number
WHERE est_latest_purchase_dt IS NOT NULL 
	OR est_first_purchase_dt IS NOT NULL 	
GROUP BY b.account_number
GO

-- Create Index 
CREATE HG INDEX id1 ON temp_buy_and_keep_usage_recency (account_number)
GO

UPDATE ADSMART_ROI a
SET BUY_AND_KEEP_USAGE_RECENCY = CASE 	WHEN DATEDIFF(dd, max_dt, GETDATE()) <= 90 				THEN 'Bought 0-3 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 91 	AND 180 THEN 'Bought 4-6 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE()) BETWEEN 181 	AND 365 THEN 'Bought 7-12 mths back'
										WHEN DATEDIFF(dd, max_dt, GETDATE())  > 365 				THEN 'Bought 12+ mths back'
										WHEN max_dt IS NULL 													THEN 'Never Bought'
										ELSE 'Unknown' 
								 END
FROM ADSMART_ROI a
LEFT JOIN temp_buy_and_keep_usage_recency AS cps 
ON a.account_number = cps.account_number
GO	

DROP TABLE temp_buy_and_keep_usage_recency
GO


/************************************************
*				 Fibre Available				*
************************************************/
-- Sources: http://sp-sharepoint.bskyb.com/sites/CIKM436/documentcentre/Documents/Analytics%20Data%20Dictionaries/Data%20Dictionaries%203rd%20Party%20Data/Rep%20of%20Ireland/ROI%20Broadband%20Coverage%20Data%20Dictionary%20v1.2.xlsx 

SELECT DISTINCT b.account_number
INTO #roi_fibre_accounts
FROM ROI_BB_FIBRE_PREQUAL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE a.cb_address_status = '1'
	AND rfo_date is null 

COMMIT 

CREATE HG INDEX id1 ON #roi_fibre_accounts(account_number)

UPDATE ADSMART_ROI
SET FIBRE_AVAILABLE	= COALESCE(CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END , 'Unknown')
FROM ADSMART_ROI AS a 
LEFT JOIN #roi_fibre_accounts AS b ON a.account_number = b.account_number

COMMIT 

/*		QA
SELECT DISTINCT b.account_number
INTO #roi_fibre_accounts
FROM ROI_BB_FIBRE_PREQUAL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE a.cb_address_status = '1'
        AND rfo_date is null

SELECT CASE WHEN b.account_number is null THEN 'NO' ELSE 'Yes' END fibre_available ,  count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_fibre_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY fibre_available



fibre_available hits
NO      209244
Yes     259922
*/

/************************************************
*				Cable Available					*
************************************************/
SELECT DISTINCT account_number, cb_key_household
INTO #roi_cable_accounts
FROM SKY_ROI_ADDRESS_MODEL AS a 
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE b_invalid = 'N'
	AND x_sabs is not null
	AND x_sabs in ( SELECT small_area_code
					FROM SKY_ROI_POINTTOPIC_SAB_BB
					WHERE cable_available = 'y')
COMMIT 
CREATE HG INDEX id1 ON #roi_cable_accounts(account_number)
COMMIT 
UPDATE ADSMART_ROI 
SET CABLE_AVAILABLE	= COALESCE(CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END , 'Unknown')
FROM ADSMART_ROI AS a 
LEFT JOIN #roi_cable_accounts AS b ON a.account_number = b.account_number

COMMIT 
DROP TABLE #roi_cable_accounts
COMMIT 

/*		QA
SELECT DISTINCT account_number
INTO #roi_cable_accounts
FROM SKY_ROI_ADDRESS_MODEL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE b_invalid = 'N'
        AND x_sabs is not null
        AND x_sabs in ( SELECT small_area_code
                                        FROM SKY_ROI_POINTTOPIC_SAB_BB
                                        WHERE cable_available = 'y')
COMMIT
CREATE HG INDEX id1 ON #roi_cable_accounts(account_number)
COMMIT
SELECT CASE WHEN b.account_number is null THEN 'NO' ELSE 'Yes' END cable_available ,  count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_cable_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY cable_available

cable_available	hits
NO	366644
Yes	102522
*/ 

/************************************************
*			 On/Off Net Fibre					*
************************************************/
SELECT DISTINCT   
	MAX(CASE WHEN a.llu_exchange = 'Y' or a.bmb_exchange = 'Y'  THEN 1 ELSE 0 END) AS On_net
	, b.address_reference
INTO #exch
FROM ROI_BB_EXCHANGE_LOOKUP     AS a
JOIN ROI_BB_ADDRESS_TO_EXCHANGE AS b ON a.exchange_id_3 = b.exchange_id
GROUP BY b.address_reference
COMMIT 
CREATE HG INDEX ed ON  #exch(address_reference)
CREATE LF INDEX aed ON  #exch(On_net)
COMMIT 

SELECT DISTINCT d.account_number
                , onnet  = max( CASE  	WHEN On_net = 1 AND e.account_number IS NOT 	NULL THEN 'On net, has fibre'
									WHEN On_net = 1 AND e.account_number IS 	   	NULL THEN 'On net, no fibre'
									WHEN On_net = 0 AND e.account_number IS NOT 	NULL THEN 'Off net, has fibre'
									WHEN On_net = 0  AND e.account_number IS     	NULL THEN 'Off net, no fibre'
									ELSE 'Unknown' END) 
					
INTO #roi_onnet_accounts
FROM #exch AS a  
JOIN SKY_ROI_ADDRESS_MODEL      AS c ON a.address_reference = c.ROI_ADDRESS_REFERENCE AND CB_ADDRESS_STATUS = '1'
JOIN CUST_SINGLE_ACCOUNT_VIEW   AS d ON c.cb_key_household = d.cb_key_household
LEFT JOIN #roi_fibre_accounts	AS e ON e.account_number = d.account_number 
GROUP BY d.account_number
            
COMMIT 
CREATE HG INDEX id1 ON #roi_onnet_accounts(account_number)

UPDATE ADSMART_ROI
SET ON_OFF_NET_FIBRE	= COALESCE(b.onnet, 'Unknown')
FROM ADSMART_ROI AS a 
LEFT JOIN #roi_onnet_accounts AS b ON a.account_number = b.account_number

COMMIT 
DROP TABLE #roi_fibre_accounts
DROP TABLE #roi_onnet_accounts

/* 		QA

SELECT DISTINCT b.account_number
INTO #roi_fibre_accounts
FROM ROI_BB_FIBRE_PREQUAL AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS b ON a.cb_key_household = b.cb_key_household
WHERE a.cb_address_status = '1'
        AND rfo_date is null
commit
CREATE HG INDEX id1 ON #roi_fibre_accounts(account_number)
commit
SELECT DISTINCT   
	MAX(CASE WHEN a.llu_exchange = 'Y' or a.bmb_exchange = 'Y'  THEN 1 ELSE 0 END) AS On_net
	, b.address_reference
INTO #exch
FROM ROI_BB_EXCHANGE_LOOKUP     AS a
JOIN ROI_BB_ADDRESS_TO_EXCHANGE AS b ON a.exchange_id_3 = b.exchange_id
GROUP BY b.address_reference
COMMIT 
CREATE HG INDEX ed ON  #exch(address_reference)
CREATE LF INDEX aed ON  #exch(On_net)
COMMIT 

SELECT DISTINCT d.account_number
                , onnet  = max( CASE  	WHEN On_net = 1 AND e.account_number IS NOT 	NULL THEN 'On net, has fibre'
									WHEN On_net = 1 AND e.account_number IS 	   	NULL THEN 'On net, no fibre'
									WHEN On_net = 0 AND e.account_number IS NOT 	NULL THEN 'Off net, has fibre'
									WHEN On_net = 0  AND e.account_number IS     	NULL THEN 'Off net, no fibre'
									ELSE 'Unknown' END) 
					
INTO #roi_onnet_accounts
FROM #exch AS a  
JOIN SKY_ROI_ADDRESS_MODEL      AS c ON a.address_reference = c.ROI_ADDRESS_REFERENCE AND CB_ADDRESS_STATUS = '1'
JOIN CUST_SINGLE_ACCOUNT_VIEW   AS d ON c.cb_key_household = d.cb_key_household
LEFT JOIN #roi_fibre_accounts	AS e ON e.account_number = d.account_number 
GROUP BY d.account_number

COMMIT 
CREATE HG INDEX id1 ON #roi_onnet_accounts(account_number)
COMMIT

SELECT COALESCE(Onnet ,'Unknown') Onnet , count(*) hits
from adsmartables_ROI_Nov_2015 As a
LEFT JOIN #roi_onnet_accounts AS b ON a.account_number = b.account_number
WHERE sky_base_universe LIKE 'Adsmartable with consent%'
GROUP BY Onnet




*/

/************************************************
** 					Broadband IP				*	
*************************************************/

MESSAGE 'POPULATE FIELD FOR BROADBAND_IP' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG WHERE CREATOR='' AND lower(TNAME)='temp_broadband_ip_1' AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_broadband_ip_1 ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_broadband_ip_1
    END

MESSAGE 'CREATE TABLE temp_broadband_ip_1' TYPE STATUS TO CLIENT
GO
      
SELECT a.account_number
        , CASE WHEN network_code = 'bskyb' 	THEN 'Sky IP'
               WHEN network_code = 'virgin' THEN 'Virgin IP'
               WHEN network_code = 'bt' 	THEN 'BT IP'
               WHEN network_code = 'none' 	THEN 'No IP Data'
               WHEN network_code = 'vodafo' THEN 'Vodafone IP'
			   WHEN network_code = 'upc'	THEN 'UPC IP'
               ELSE 'Other IP' 
		  END AS network_code_1
        , MAX (last_modified_dt) latest_date
INTO temp_broadband_ip_1
FROM  CUST_ANYTIME_PLUS_DOWNLOADS a
JOIN ADSMART_ROI b ON a.account_number = b.account_number --################ ADSMART_ROI TABLE must be validated ##############
WHERE network_code IS NOT NULL
GROUP BY a.account_number, network_code_1
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG WHERE CREATOR='' AND lower(TNAME)='temp_broadband_ip_2' AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_broadband_ip_2 ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE temp_broadband_ip_2
    END

MESSAGE 'CREATE TABLE temp_broadband_ip_2' TYPE STATUS TO CLIENT
GO

SELECT account_number
     , network_code_1
     , latest_date
     , rank() OVER (PARTITION BY account_number ORDER BY latest_date ) AS rankk
INTO temp_broadband_ip_2 
FROM temp_broadband_ip_1
GO

UPDATE ADSMART_ROI
SET ROI_BROADBAND_IP = coalesce(network_code_1, 'No IP Data')              -- Name changed to match Excel definition  Update 2015-02-06
FROM ADSMART_ROI a
LEFT JOIN (SELECT * FROM temp_broadband_ip_2  WHERE rankk = 1) b ON a.account_number = b.account_number
GO

DROP TABLE temp_broadband_ip_1
DROP TABLE temp_broadband_ip_2
GO
/*		QA

SELECT a.account_number
        , CASE WHEN network_code = 'bskyb' 	THEN 'Sky IP'
               WHEN network_code = 'virgin' THEN 'Virgin IP'
               WHEN network_code = 'bt' 	THEN 'BT IP'
               WHEN network_code = 'none' 	THEN 'No IP Data'
               WHEN network_code = 'vodafo' THEN 'Vodafone IP'
			   WHEN network_code = 'upc'	THEN 'UPC IP'
               ELSE network_code 
		  END AS network_code_1
        , MAX (last_modified_dt) latest_date
INTO temp_broadband_ip_1
FROM  CUST_ANYTIME_PLUS_DOWNLOADS a
JOIN adsmartables_ROI_Nov_2015 b ON a.account_number = b.account_number --################ ADSMART TABLE must be validated ##############
WHERE network_code IS NOT NULL
GROUP BY a.account_number, network_code_1

SELECT account_number
     , network_code_1
     , latest_date
     , rank() OVER (PARTITION BY account_number ORDER BY latest_date ) AS rankk
INTO temp_broadband_ip_2 
FROM temp_broadband_ip_1
GO

SELECT network_code_1 AS ROI_BROADBAND_IP
	, count(*) hits
FROM 	temp_broadband_ip_2
WHERE rankk = 1
GROUP BY network_code_1


ISP			Hit
No IP Data	138619
Vodafone IP	80560
BT IP		67625
UPC IP		30979
h3g			15552
telefo		5210
Sky IP		128
turkte		78
Virgin IP	46
talkta		42
france		21
ti			10
comcas		6
dtag		5
qwest		4
plusne		2
hinet		1
roadru		1
softla		1



*/


UPDATE ADSMART_ROI 
SET A.ROI_MOSAIC_HE  = CASE WHEN B.MOS_Group_ID IS NULL THEN 'UNKNOWN' ELSE B.MOS_Group_ID END
FROM (	SELECT A.ACCOUNT_NUMBER
			,B.cb_key_household
			,MIN(B.MOS_Group_ID) AS MOS_Group_ID
		FROM ADSMART_ROI AS A
		INNER JOIN (SELECT ROI_ADDRESS_MODEL.cb_key_household
					CASE MOSAIC.mosaic_group_code  
								WHEN 'A' THEN 'Established Elites'
								WHEN 'B' THEN 'Upwardly Mobile Enclaves'
								WHEN 'C' THEN 'City Centre mix'
								WHEN 'D' THEN 'Struggling Society'
								WHEN 'E' THEN 'Poorer Greys'
								WHEN 'F' THEN 'Industrious Urban Fringe'
								WHEN 'G' THEN 'Careers & Kids'
								WHEN 'H' THEN 'Young & Mortgaged'
								WHEN 'I' THEN 'Better Off Greys'
								WHEN 'J' THEN 'Commuter Farming Mix'
								WHEN 'K' THEN 'Regional Identity'
								WHEN 'L' THEN 'Farming Families'
								WHEN 'M' THEN 'Arcadian Inheritance'
								ELSE 'Unknown' END MOS_Group_ID
					FROM SKY_ROI_ADDRESS_MODEL AS ROI_ADDRESS_MODEL
					LEFT JOIN SKY_ROI_MOSAIC_2013 AS MOSAIC ON MOSAIC.building_id = ROI_ADDRESS_MODEL.building_id
					GROUP BY ROI_ADDRESS_MODEL.cb_key_household, MOS_Group_ID						
					) B ON A.cb_key_household = B.cb_key_household
	GROUP BY A.ACCOUNT_NUMBER
		,B.cb_key_household
	) B
WHERE A.account_number = B.account_number













/****************************************************************************************
 *                                                                                      *
 *                          CREATE ADSMART_ROI VIEW                                         *
 *                                                                                      *
 ***************************************************************************************/ 
MESSAGE 'Create ADSMART_ROI View for the new ADSMART_ROI Table' type status to client
go 
  create view  ADSMART_ROI as
  select * FROM ADSMART_ROI
  go
MESSAGE 'View  ADSMART_ROI created successfully' type status to client
MESSAGE 'Creating Restricted Views on the ADSMART_ROI view' type status to client
go 
call  dba.create_restricted_views_all('ADSMART_ROI', user_name() ,1)
GO
MESSAGE 'Build ADSMART_ROI Table & View Process - Completed Successfully' type status to client

go
