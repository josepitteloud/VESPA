/*  Title       : Adsmart Attributes/ QR1 2016-2017
    Created by  : Jose Pitteloud	
    Date        : 26 July 2016
    Description : This is a sql to build the ADSMART attributes included in the 1st quarterly release of 2016-2017 
				: The attributes included are:
					1.- SKY_CINEMA_VIEWING
					2.- SKY_PL_FOOTBALL_VIEWING
					3.- SKY_SPORTS_VIEWING
					4.- SKY_COMEDY_VIEWING
					5.- SKY_DRAMA_VIEWING
					6.- NON_SKY_DRAMA_VIEWING
					7.- SKY_BROADBAND_STATUS (Variation of the existing attribute)
					8.- MOVIES_ON_DEMAND	 (Variation of the existing attribute)
                
    Modified by : Jose Pitteloud 
    Changes     :

*/


/*		====================	QA		============== 
CREATE TABLE ADSMART_TEST
(   account_number VARCHAR (12)
	, SKY_CINEMA_VIEWING	VARCHAR(20)
	, SKY_PL_FOOTBALL_VIEWING	VARCHAR(20)
	, SKY_SPORTS_VIEWING	VARCHAR(20)
	, SKY_COMEDY_VIEWING	VARCHAR(20)
	, SKY_DRAMA_VIEWING	VARCHAR(20)
	, NON_SKY_DRAMA_VIEWING	VARCHAR(20)
	, SKY_BROADBAND_STATUS VARCHAR(20)
	, MOVIES_ON_DEMAND	VARCHAR(20)
)



CREATE TABLE PROMOSMART_TRAITS_DUMMY
(   account_number VARCHAR (12)
  , TRAIT_NAME      VARCHAR (300)
  , TRAIT_TYPE      VARCHAR(50)
  , TRAIT_MEASURE   INT
  , VIEWING_TRAIT_BAND VARCHAR(20))
  
  COMMIT 
  CREATE HG INDEX id1 ON PROMOSMART_TRAITS_DUMMY(account_number)
  CREATE HG INDEX id3 ON PROMOSMART_TRAITS_DUMMY(VIEWING_TRAIT_BAND)

INSERT INTO PROMOSMART_TRAITS_dummy 
SELECT top 1000 account_number
    , 'Cinema_viewing_AQ1_over_Last30days'
    , 'Programme'
    , CAST(RIGHT(account_number,2) AS INT) + DATEPART(second, getdate()) AS ta
    , CASE  WHEN ta <10 THEN 'NoViewing'
            WHEN ta BETWEEN 10 AND 19 THEN 'Percent11-20'
            WHEN ta BETWEEN 20 AND 29 THEN 'Percent21-30'
            WHEN ta BETWEEN 30 AND 39 THEN 'Percent31-40'
            WHEN ta BETWEEN 40 AND 49 THEN 'Percent41-50'
            WHEN ta BETWEEN 50 AND 59 THEN 'Percent51-60'
            WHEN ta BETWEEN 60 AND 69 THEN 'Percent61-70'
            WHEN ta BETWEEN 70 AND 79 THEN 'Percent71-80'
            WHEN ta BETWEEN 80 AND 89 THEN 'Percent81-90'
            WHEN ta >= 90             THEN 'Percent91-100'
            ELSE 'W' END 
FROM ADSMART 


  
*/

  
MESSAGE 'Process to build the ADSMART ATTRIBUTES in QR1 2016-2007' type status to client



go
--------------------------------------------
--------- 1- SKY_CINEMA_VIEWING
--------------------------------------------
UPDATE ADSMART
SET SKY_CINEMA_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'Cinema_viewing_AQ1_over_Last30days'
--------------------------------------------
--------- 2- SKY_PL_FOOTBALL_VIEWING
--------------------------------------------
UPDATE ADSMART
SET SKY_PL_FOOTBALL_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'PLFootball_viewing_AQ1_over_Last90days'
--------------------------------------------
--------- 3- SKY_SPORTS_VIEWING
--------------------------------------------
UPDATE ADSMART
SET SKY_SPORTS_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'Sports_viewing_AQ1_over_Last90days'
--------------------------------------------
--------- 4- SKY_COMEDY_VIEWING
--------------------------------------------
UPDATE ADSMART
SET SKY_COMEDY_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'Comedy_viewing_AQ1_Last90days'
--------------------------------------------
--------- 5- SKY_DRAMA_VIEWING
--------------------------------------------
UPDATE ADSMART
SET SKY_DRAMA_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'Drama_viewing_AQ1_Last90days'
--------------------------------------------
--------- 6- NON_SKY_DRAMA_VIEWING
--------------------------------------------
UPDATE ADSMART
SET NON_SKY_DRAMA_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'NonSky_Drama_viewing_AQ1_Last90days'

--------------------------------------------
--------- 7- SKY_BROADBAND_STATUS
--------------------------------------------

UPDATE ADSMART 
SET  SKY_BROADBAND_STATUS = CASE WHEN sav.prod_active_broadband_package_desc IS NULL AND PROD_EARLIEST_BROADBAND_ACTIVATION_DT  IS NULL THEN 'Never had BB '
							----------- Lapsed customers 
							WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) <=365 				THEN 'No BB downgraded in last 0-12 months'
                            WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) BETWEEN 366 AND 730 	THEN 'No BB downgraded in last 12-24 months'
                            WHEN PROD_LATEST_BROADBAND_STATUS_CODE in ('PO','SC','CN')  AND PROD_LATEST_BROADBAND_ACTIVATION_DT IS NOT NULL AND DATEDIFF(dd,PROD_LATEST_BROADBAND_STATUS_START_DT,TODAY()) >730 				THEN 'No BB downgraded 24+ months'
                            -----------  Existing Packages
							WHEN sav.prod_active_broadband_package_desc = 'Broadband Connect'                                                                             THEN 'Has BB Product Connect'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited Pro' OR sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited' THEN 'Has BB Product Unlimited'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Lite'                                                                            THEN 'Has BB Product Lite'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Unlimited Fibre'                                                                 THEN 'Has BB Product Fibre Unlimited'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Fibre Unlimited Pro'                                                                       THEN 'Has BB Product Fibre Pro'
                            WHEN sav.prod_active_broadband_package_desc = 'Sky Broadband Everyday'                                                                        THEN 'Has BB Product Everyday'
							---------------------------------------- added in QR1 2016-17
							WHEN sav.prod_active_broadband_package_desc LIKE '%Sky Broadband 12GB%'              		                                                      	THEN 'Has BB product 12GB'
							WHEN sav.prod_active_broadband_package_desc LIKE '%Sky Fibre Lite%'          		                                                              	THEN 'Has BB product Fibre Lite'
							WHEN sav.prod_active_broadband_package_desc LIKE '%Sky Fibre Max%'			                                                                      	THEN 'Has BB Product Fibre Max'
							WHEN sav.prod_active_broadband_package_desc LIKE '%Sky Fibre%'			                                                                      		THEN 'Has BB Product Fibre'
							---------------------------------------- Other BB Packages
                            ELSE 'Other BB package'
			END 
FROM ADSMART AS a
JOIN CUST_SINGLE_ACCOUNT_VIEW AS sav  ON a.account_number = sav.account_number

--------------------------------------------
--------- 8- Movies On Demand
--------------------------------------------
MESSAGE 'Update field movies_on_demand in ADSMART Table' type status to client
GO
 
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='${CBAF_DB_DATA_SCHEMA}'
              AND lower(TNAME)='temp_adsmart_q2_on_demand_raw'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_ADSMART_Q2_ON_DEMAND_RAW ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW
    END

MESSAGE 'CREATE TABLE TEMP_ADSMART_Q2_ON_DEMAND_RAW' TYPE STATUS TO CLIENT
GO 

SELECT cala.account_number
        ,MAX(last_modified_dt) last_dt
    INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW
    FROM ${CBAF_DB_LIVE_SCHEMA}.CUST_ANYTIME_PLUS_DOWNLOADS cala
         INNER JOIN ${CBAF_DB_DATA_SCHEMA}.adsmart AS sav ON cala.account_number = sav.account_number
                                       AND last_modified_dt <= now()
   WHERE UPPER(genre_desc) LIKE UPPER('%MOVIE%')
     AND provider_brand IN ('Sky Disney','Sky Disney HD','Sky Movies','Sky Movies HD','Sky Cinema','Sky Cinema HD' ,'Disney Movies' , 'Disney HD')	-- Added: Cinema/HD & Disney Movies/HD
	 AND last_modified_dt >= DATEADD(YEAR, -2, NOW())		--- Limiting the search to 2 years back due to the massive size of the source table 
GROUP BY cala.account_number
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart SET MOVIES_ON_DEMAND = 'Never'
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.adsmart as bas
     SET MOVIES_ON_DEMAND = CASE WHEN DATEDIFF (day, last_dt, getDATE())  <= 91                   THEN 'Downloaded movies 0-3 months'
                                 WHEN DATEDIFF (day, last_dt, getDATE())  BETWEEN 92 AND 182      THEN 'Downloaded movies 4-6 months'
                                 WHEN DATEDIFF (day, last_dt, getDATE())  >= 183                  THEN 'Downloaded movies 7+ months'
                                 ELSE 'Never'
                             END
    FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW AS sub
    WHERE bas.account_number = sub.account_number
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_ADSMART_Q2_ON_DEMAND_RAW
GO

