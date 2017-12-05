------------------------------------------------------
------------------------------------------------------
---- 			BB CHURN Forecast
------------------------------------------------------
------------------------------------------------------

/*
		Segmentation variables:
			- Affluence 
			- Age
			- Tenure
			- Package
			- Offer
			- Bill amount
		TABLES FROM OLIVE: 
			- sharmaa.View_attachments_201609 
			- CALLS_DETAILS: hold call to BBCoE - 	Filter by final_sct_grouping = 'Retention - BBCoE'
			
		
			
			
*/

	
--- Creates an aggregated view of BBCoE calls for 2016 at acct level
		
SELECT 
      account_number
    , subs_year
    , subs_week_of_year
    , final_sct_grouping
    , SUM(no_of_calls)  AS s_calls 
    , count(*)          AS hits
INTO BB_CHURN_calls_details_raw_3yr
FROM CALLS_DETAILS AS a 
JOIN SKY_calendar AS b ON a.call_date = b.calendar_date 
WHERE call_date >= '2016-01-01'
	AND     final_sct_grouping = 'Retention - BBCoE'
	AND account_number IS NOT NULL 
GROUP BY account_number
    , subs_year
    , subs_week_of_year
    , final_sct_grouping
 
 
--- Appending account details to call history    
	SELECT DISTINCT 	 
	  a.account_number
    , product_holding
	, bb_type
	, h_AGE_coarse_description      	AS age
	, COALESCE(h_fss_v3_group, 'U')    	AS fss
	, CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 	AS bb_tenure
	, COALESCE(LEFT (affluence,2), 'U') AS affluence
	, COALESCE(LEFT (life_stage,2),'U') AS life_stage, subs_year, subs_week_of_year , s_calls, hits 
INTO BB_CHURN_append
FROM BB_CHURN_calls_details_raw AS a 
LEFT JOIN sharmaa.View_attachments_201601  AS b ON a.account_number = b.account_number


COMMIT 
CREATE HG INDEX id1 ON BB_CHURN_append(account_number) 
CREATE LF INDEX id2 ON BB_CHURN_append(subs_year,subs_week_of_year) 
CREATE LF INDEX id2 ON BB_CHURN_append(subs_year) 


--- Filling missing account data info (loops through the attachement tables since 2015)

UPDATE BB_CHURN_append 
SET 
	  a.product_holding 	= b.product_holding 
	, a.bb_type 			= b.bb_type
	, a.age					= b.h_AGE_coarse_description
	, a.fss					= COALESCE(h_fss_v3_group, 'U')
	, a.bb_tenure			= CASE 	WHEN DATEDIFF (MONTH, b.BB_latest_act_date, GETDATE()) 	<12 THEN 'Less 1 year'
								WHEN DATEDIFF (MONTH, b.BB_latest_act_date, GETDATE()) 	BETWEEN 12 AND 23 THEN '1 year'	
								WHEN DATEDIFF (MONTH, b.BB_latest_act_date, GETDATE())  BETWEEN 24 AND 35 THEN '2 years'
								WHEN DATEDIFF (MONTH, b.BB_latest_act_date, GETDATE()) 	BETWEEN 36 AND 47 THEN '3 years'
								WHEN DATEDIFF (MONTH, b.BB_latest_act_date, GETDATE()) 	BETWEEN 48 AND 59 THEN '4 years'
								WHEN DATEDIFF (MONTH, b.BB_latest_act_date, GETDATE())  >= 60 		THEN '5+ years'
								ELSE 'weird'		END 
	, a.affluence			= COALESCE(LEFT (b.affluence,2), 'U')
	, a.life_stage			= COALESCE(LEFT (b.life_stage,2),'U')
	FROM BB_CHURN_append AS a 
	JOIN sharmaa.View_attachments_201501  AS b ON a.account_number = b.account_number

---- Creates a view of the base - snapshot @September 2016	at account level

SELECT a.account_number
	, a.product_holding
	, a.bb_type
	, a.h_AGE_coarse_description      	AS age
	, COALESCE(a.h_fss_v3_group, 'U')    	AS fss
	, CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 	AS bb_tenure
	, COALESCE(LEFT (a.affluence,2), 'U') AS affluence
	, COALESCE(LEFT (a.life_stage,2),'U') AS life_stage
     , TA_call  = CASE WHEN b.account_number is not null then 1 ELSE 0 END
INTO BB_CHURN_full_201609
FROM sharmaa.View_attachments_201609 AS a 
LEFT JOIN BB_CHURN_append AS b ON a.account_number = b.account_number AND b.subs_year = 2016 AND subs_week_of_year BETWEEN 1 AND 13 --- Q1 2016
WHERE broadband = 1 

--- Adding extra columns 
ALTER TABLE   BB_CHURN_full_201609 ADD offer VARCHAR(20)
ALTER TABLE BB_CHURN_full_201609 ADD rnd AS float ;

--- Adding random number
UPDATE BB_CHURN_full_201609 
SET rnd = RAND(CAST(RIGHT (account_number ,7) AS FLOAT )+  DATEPART(us, getdate()));

--- Adding offer info (ADSMART code)
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='pitteloudj'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_raw'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE  temp_Adsmart_end_of_offer_raw
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_raw' TYPE STATUS TO CLIENT
GO

IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='pitteloudj'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE  temp_Adsmart_end_of_offer_aggregated
    END
GO

SELECT b.account_number
    , CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END AS sport_flag
    , CASE WHEN (lower (offer_dim_description) LIKE '%movie%' OR lower (offer_dim_description) LIKE '%cinema%') THEN 1 ELSE 0 END AS movie_flag
	, CASE  WHEN    x_subscription_type IN ('SKY TALK','BROADBAND') THEN 'BBT'
            WHEN    x_subscription_type LIKE 'ENHANCED' AND
                    x_subscription_sub_type LIKE 'Broadband DSL Line' THEN 'BBT'
            ELSE 'OTHER'  END 																						AS Offer_type
    , offer_status
	, Active_offer = CASE WHEN offer_status IN  ('Active',' Pending Terminated', 'Blocked')  THEN 1 ELSE 0 END 
    , CASE 	WHEN Active_offer = 1 													THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
			WHEN offer_status = 'Terminated' 										THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
			END AS offer_end_date
    , ABS(DATEDIFF(dd, offer_end_date, '2016-10-01')) 																		AS days_from_today
    , rank() OVER(PARTITION BY b.account_number 			ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_1
    , rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_2
    , CAST (0 AS bit)                                                      												AS main_offer
INTO     temp_Adsmart_end_of_offer_raw
FROM     cust_product_offers 	AS CPO
JOIN     BB_CHURN_full_201609 				AS b     ON CPO.account_number = b.account_number
WHERE    offer_id                NOT IN (SELECT offer_id FROM citeam.sk2010_offers_to_exclude)
		AND first_activation_dt > '1900-01-01'
		AND offer_end_dt >= DATEADD(year, -1, '2016-10-01')
		AND x_subscription_sub_type <> 'DTV Season Ticket'
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge','Sky Go Extra No Additional Charge with Sky Multiscreen')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE UPPER('%Price Protection Offer%')
        AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1              -- To keep the latest offer by each offer type
GO
CREATE HG INDEX id1 ON  temp_Adsmart_end_of_offer_raw(account_number)
GO
-----------     Identifying Accounts with more than one active offer
IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='pitteloudj'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE temp_Adsmart_end_of_offer_aggregated ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE  temp_Adsmart_end_of_offer_aggregated
    END
		MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_aggregated' TYPE STATUS TO CLIENT
GO

SELECT
    account_number
	, Active_offer
    , COUNT(*) offers
    , MIN(ABS(DATEDIFF(dd,'2016-10-01' , offer_end_date)))  AS min_end_date    
INTO  temp_Adsmart_end_of_offer_aggregated
FROM  temp_Adsmart_end_of_offer_raw
GROUP BY account_number,Active_offer

COMMIT
GO
CREATE HG INDEX id2 ON  temp_Adsmart_end_of_offer_aggregated(account_number)
GO
-- Deleting expired offers when the account has an active offer
DELETE FROM temp_Adsmart_end_of_offer_raw
WHERE account_number IN (SELECT account_number FROM temp_Adsmart_end_of_offer_aggregated WHERE Active_offer = 1) 
	AND Active_offer = 0 

-- Flagging the main(s) offer (Closest ending offer)
UPDATE  temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN    b.min_end_date = a.days_from_today THEN 1 ELSE 0 END
FROM  temp_Adsmart_end_of_offer_raw           AS a
JOIN  temp_Adsmart_end_of_offer_aggregated    AS b ON a.account_number = b.account_number AND a.Active_offer = b.Active_offer

-----------     Deleting other offers - not the main(s) (which end date is not the min date)
DELETE FROM  temp_Adsmart_end_of_offer_raw        AS a
WHERE   main_offer = 0
GO
-----------     Updating multi offers (When the account has 2 or more main offers ending the same day)
UPDATE  temp_Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM  temp_Adsmart_end_of_offer_raw AS a
JOIN (SELECT account_number, count(*) hits FROM  temp_Adsmart_end_of_offer_raw GROUP BY account_number HAVING hits > 1) AS b ON a.account_number = b.account_number
-----------     DEleting duplicates
DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_1 > 1              -- To keep the latest offer by each offer type
GO
-----------     Updating Adsmart table
UPDATE  BB_CHURN_full_201609
SET ON_OFFER = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
					ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL                    				THEN 'No info'
														WHEN Active_offer = 1 AND days_from_today  > 90           		THEN 'Live +90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Live 31-90'
														WHEN Active_offer = 1 AND days_from_today  <= 30          		THEN 'Live -30'
														WHEN Active_offer = 0 AND days_from_today  > 90           		THEN 'Exp +90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Exp 31-90'
														WHEN Active_offer = 0 AND days_from_today  <= 30          		THEN 'Exp -30'
														ELSE 'No Offer Ever' END
													  END
FROM  BB_CHURN_full_201609 as a
LEFT JOIN  temp_Adsmart_end_of_offer_raw as b
ON a.account_number = b.account_number

DROP TABLE  temp_Adsmart_end_of_offer_raw
DROP TABLE  temp_Adsmart_end_of_offer_aggregated
					  
					  
  SELECT count(*) FROM BB_CHURN_full_201609 WHERE TA_call = 1 
-- 194525
  
--- Generating output CSV file
--- Extracting non-callers sample 
	SELECT top 195000 * 
	INTO #t1 
	FROM BB_CHURN_full_201609 WHERE TA_call = 0 
	ORDER BY rnd 
	
--- To be run in Sybase	
	SELECT * FROM BB_CHURN_full_201609 WHERE TA_call = 1 
	UNION ALL SELECT * FROM #t1;
	OUTPUT TO 'C:\Users\pitteloj\Documents\BB Forecast\sample 3.csv' Format ASCII Delimited by ',' quote'';
---- END

GRANT SELECT ON BB_CHURN_full_201609, TO spencerc2
GRANT SELECT ON BB_CHURN_calls_details_raw TO spencerc2
GRANT SELECT ON BB_CHURN_append TO spencerc2
			  





UPDATE BB_CHURN_full_201609

SET segment= (CASE WHEN (T0.offer = 'BBT Exp +90') THEN 1 
					WHEN (((T0.offer = 'BBT Exp -30') OR (T0.offer = 'Multi Live 31-90')) OR (T0.offer = 'OTHER Live -30')) THEN 2 
					WHEN ((T0.offer = 'BBT Exp 31-90') OR (T0.offer = 'OTHER Exp -30')) THEN 2 
					WHEN ((((T0.offer = 'BBT Live -30') OR (T0.offer = 'BBT Live 31-90')) OR (T0.offer = 'Multi Exp +90')) OR (T0.offer = 'Multi Exp 31-90')) THEN 4
					WHEN (((T0.offer = 'Multi Exp -30') OR (T0.offer = 'Multi Live -30')) OR (T0.offer = 'OTHER Live 31-90')) THEN 5
					WHEN (T0.offer = 'Multi Live +90') THEN (CASE WHEN ((((((((T0.bb_type = 'Sky Broadband 12GB') OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																		OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) 
																		OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 
																	ELSE (CASE WHEN ((((((T0.fss = 'A') OR (T0.fss = 'B')) OR (T0.fss = 'F')) OR (T0.fss = 'I')) OR (T0.fss = 'J')) OR (T0.fss = 'L')) THEN 7 
																				ELSE 8 END) END) 
					WHEN (((T0.offer = 'No Offer Ever') OR (T0.offer = 'OTHER Exp +90')) OR (T0.offer = 'OTHER Exp 31-90')) THEN (CASE WHEN (T0.product_holding = 'E. SABB') THEN 9 
																																			ELSE (CASE WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 10 
																																						WHEN ((T0.age = 'Age 66+') OR (T0.age = 'Unclassified')) THEN 11
																																						ELSE 12 END) END) 
					WHEN (T0.offer = 'OTHER Live +90') THEN (CASE WHEN (((((T0.bb_type = 'Sky Broadband Unlimited Fibre') OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																			OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 13 
																	ELSE (CASE WHEN ((T0.bb_tenure LIKE '%1 year%') OR (T0.bb_tenure  LIKE '%Less 1 year%')) THEN 14 
																				ELSE (CASE WHEN ((((T0.age = 'Age 18-25') OR (T0.age = 'Age 46-55')) OR (T0.age = 'Age 56-65')) OR (T0.age = 'Unclassified')) THEN 15 
																							ELSE 16 END) END) END) 
					ELSE (CASE 	WHEN (T0.bb_tenure  LIKE '%1 year%') THEN 17
								WHEN ((T0.bb_tenure  LIKE '%2 years%') OR (T0.bb_tenure  LIKE '%3 years%')) THEN (CASE WHEN ((T0.product_holding = 'B. DTV + Triple play') OR (T0.product_holding = 'C. DTV + BB Only')) THEN 18 
																																		ELSE 19 END) 
								WHEN (T0.bb_tenure  LIKE '%Less 1 year%') THEN 20 
								ELSE (CASE WHEN ((((T0.bb_type = 'Sky Broadband Unlimited Fibre') OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) THEN 21 
					ELSE 22 END) END) END) 
FROM BB_CHURN_full_201609 T0



CREATE TABLE BB_CHURN_attachement_2yr_final
    ( Year      INT
    , Q         tinyint
    , Segment   tinyint
    , BASE      bigint
    , Calls     BIGINT) 
    COMMIT 
    CREATE LF index i1 ON BB_CHURN_attachement_2yr_final(year)
    CREATE LF index i2 ON BB_CHURN_attachement_2yr_final(Q)
    CREATE LF index i3 ON BB_CHURN_attachement_2yr_final(segment)
    COMMIT 			  
	
	
	
	
	
	DECLARE @y INT
	DECLARE @Q INT
	DECLARE @w1 INT
	DECLARE @wf INT
	SET @y = 2015
	SET @q = 1
	SET @w1 = 1
	SET @wf = 13
	
SELECT a.account_number
	, a.product_holding
	, a.bb_type
	, a.h_AGE_coarse_description      	AS age
	, COALESCE(a.h_fss_v3_group, 'U')    	AS fss
	, CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 	AS bb_tenure
	, COALESCE(LEFT (a.affluence,2), 'U') AS affluence
	, COALESCE(LEFT (a.life_stage,2),'U') AS life_stage
     , TA_call  = CASE WHEN b.account_number is not null then 1 ELSE 0 END
	 , CAST (null AS VARCHAR(30) )AS offer 
	 , CAST (null AS tinyint) AS segment
INTO base 
FROM sharmaa.View_attachments_201501 AS a 
LEFT JOIN BB_CHURN_append AS b ON a.account_number = b.account_number AND b.subs_year = @y AND subs_week_of_year BETWEEN @w1 AND @wf --- Q1 2015
WHERE broadband = 1 

COMMIT 
CREATE HG INDEX id1 ON base(account_number) 	
CREATE LF INDEX id2 ON base(bb_tenure) 
CREATE LF INDEX id3 ON base(offer) 
CREATE LF INDEX id4 ON base(segment) 
CREATE LF INDEX id5 ON base(product_holding) 
CREATE LF INDEX id6 ON base(bb_type) 
COMMIT


CALL   pitteloudj.on_offer '2015-01-01', '2016-01-01'
   


UPDATE base 
SET segment=  CASE 
					WHEN 	offer =  'BBT Exp +90' THEN 1
					WHEN	offer in ( 'BBT Exp -30' ,'Multi Live 31-90' ,'OTHER Live -30' )  THEN 2
					WHEN	offer in ( 'BBT Exp 31-90', 'OTHER Exp -30' )  THEN 3
					WHEN	offer in ( 'BBT Live +90' ) THEN 
								CASE	WHEN	TRIM(bb_tenure) LIKE  '%1 year%'  THEN 4
										WHEN	TRIM(bb_tenure) LIKE '%2 years%' OR TRIM(bb_tenure)  LIKE '%3 years%' THEN
										CASE	WHEN	product_holding in ( 'B. DTV + Triple play' ,'C. DTV + BB Only' ) THEN 5
												WHEN	product_holding in ( 'E. SABB' ) THEN 6 ELSE 100 END
										WHEN 	TRIM(bb_tenure) LIKE '%4 years%' OR TRIM(bb_tenure) LIKE '%5+ years%' THEN 
										CASE	WHEN bb_type in ( 'Broadband Connect', 'Sky Broadband 12GB' ,'Sky Broadband Everyday' ,'Sky Broadband Lite','Sky Broadband Unlimited' ,'Sky Broadband Unlimited Pro', 'Sky Fibre Unlimited Pro' ) THEN 7
												WHEN bb_type in ( 'Sky Broadband Unlimited Fibre', 'Sky Fibre', 'Sky Fibre Lite', 'Sky Fibre Max' ) THEN 8 ELSE 101 END
										WHEN 	TRIM(bb_tenure)	LIKE '%Less 1 year%'   THEN 9 ELSE 102 END
					WHEN 	offer in ( 'BBT Live -30', 'BBT Live 31-90' ,'Multi Exp +90', 'Multi Exp 31-90' )  THEN 10 
					WHEN 	offer in ( 'Multi Exp -30', 'Multi Live -30', 'OTHER Live 31-90' )  THEN 11 
					WHEN 	offer in ( 'Multi Live +90' ) THEN 
								CASE	WHEN bb_type in ( 'Broadband Connect' ,'Sky Broadband Lite' ,'Sky Broadband Unlimited', 'Sky Broadband Unlimited (ROI - Legacy)' ,'Sky Broadband Unlimited (ROI)', 'Sky Connect Lite (ROI - Legacy)', 'Sky Connect Unlimited (ROI - Legacy)', 'Sky Fibre Unlimited (ROI - Legacy)', 'Sky Fibre Unlimited (ROI)' ) THEN
										CASE 	WHEN 		fss in ( 'C', 'D', 'E', 'G' ,'H', 'K', 'M' ,'N' ,'U' ) THEN 12
												WHEN 		fss in ( 'A' ,'B' ,'F' ,'I', 'J', 'L' ) THEN 13 ELSE 103 END 
										WHEN bb_type in ( 'Sky Broadband 12GB', 'Sky Broadband Everyday', 'Sky Broadband Unlimited Fibre', 'Sky Broadband Unlimited Pro', 'Sky Fibre', 'Sky Fibre Lite', 'Sky Fibre Max', 'Sky Fibre Unlimited Pro' ) THEN 14 ELSE 104 END
					WHEN 	offer in ( 'No Offer Ever', 'OTHER Exp +90', 'OTHER Exp 31-90' )  THEN 
								CASE	WHEN product_holding in ( 'B. DTV + Triple play', 'C. DTV + BB Only' ,'D. DTV + Other Comms' )  THEN 
										CASE	WHEN 		age in ( 'Age 18-25', 'Age 26-35', 'Age 36-45' )  THEN 15
												WHEN 		age in ( 'Age 46-55', 'Age 56-65' )  THEN 16
												WHEN 		age in ( 'Age 66+', 'Unclassified' )  THEN 17 ELSE 105 END 
										WHEN product_holding in ( 'E. SABB' )  THEN 18 ELSE 106 END
					WHEN 	offer in ( 'OTHER Live +90' )  THEN 
										CASE 	WHEN bb_type in ( 'Broadband Connect', 'Sky Broadband 12GB', 'Sky Broadband Everyday', 'Sky Broadband Lite', 'Sky Broadband Lite (ROI - Legacy)', 'Sky Broadband Unlimited', 'Sky Broadband Unlimited (ROI - Legacy)', 'Sky Broadband Unlimited (ROI)', 'Sky Connect Lite (ROI - Legacy)', 'Sky Connect Unlimited (ROI - Legacy)', 'Sky Connect Unlimited (ROI)', 'Sky Fibre', 'Sky Fibre (ROI - Legacy)', 'Sky Fibre Unlimited (ROI - Legacy)', 'Sky Fibre Unlimited (ROI)' ) THEN 
											CASE	WHEN 		TRIM(bb_tenure) LIKE  '%1 year%' OR TRIM(bb_tenure) LIKE '%Less 1 year%'   THEN 19 
													WHEN 		TRIM(bb_tenure) LIKE  '%2 years%' OR TRIM(bb_tenure) LIKE '%3 years%' OR TRIM(bb_tenure) LIKE '%4 years%' OR TRIM(bb_tenure) LIKE '%5+ years%' THEN 
													CASE	WHEN 			age in ( 'Age 18-25', 'Age 46-55', 'Age 56-65', 'Unclassified' ) THEN 20
															WHEN 			age in ( 'Age 26-35' ,'Age 36-45','Age 66+' ) THEN 21 ELSE 108 END  ELSE 107 END
										WHEN bb_type in ( 'Sky Broadband Unlimited Fibre', 'Sky Broadband Unlimited Pro', 'Sky Fibre Lite', 'Sky Fibre Max', 'Sky Fibre Unlimited Pro' ) THEN 22 ELSE 109 END  ELSE 110 END
FROM base 
   
   	
	
INSERT INTO BB_CHURN_attachement_2yr 
SELECT @y, @q, segment, count(*) hits, 0 		
FROM #base
group by segment



UPDATE BB_CHURN_append
SET v_year = CASE   WHEN subs_year = 2014 THEN 2015
                    WHEN subs_year = 2016 THEN 2016
                    WHEN subs_year = 2015 AND subs_week_of_year <= 27 THEN 2015
                    WHEN subs_year = 2015 AND subs_week_of_year > 27 THEN 2016 ELSE 0 END
    , q = CASE WHEN subs_week_of_year BETWEEN 28 AND 40 THEN 1
                WHEN subs_week_of_year BETWEEN 41 AND 52 THEN 2
                WHEN subs_week_of_year BETWEEN 1 AND 12 THEN 3
                WHEN subs_week_of_year BETWEEN 13 AND 27 THEN 4
				ELSE 0 END



Segment_Chris = (CASE WHEN (T0.offer = 'BBT Exp +90') THEN (CASE WHEN (T0.product_holding = 'E. SABB') 
														THEN (CASE WHEN ((((((T0.life_stage = '1.') OR (T0.life_stage = '14')) 
																			OR (T0.life_stage = '2.')) OR (T0.life_stage = '4.')) 
																			OR (T0.life_stage = '5.')) OR (T0.life_stage = 'U')) 	THEN 0 
																	ELSE 0 END) 
														ELSE (CASE WHEN (((((T0.fss = 'A') OR (T0.fss = 'C')) OR (T0.fss = 'D')) 
																			OR (T0.fss = 'E')) OR (T0.fss = 'F')) 					THEN 0 
																	WHEN ((((T0.fss = 'B') OR (T0.fss = 'G')) OR (T0.fss = 'H')) 
																			OR (T0.fss = 'K')) 										THEN 0 
																	ELSE 0 END) END)
			WHEN ((T0.offer = 'BBT Exp -30') OR (T0.offer = 'OTHER Live -30')) THEN (CASE WHEN (T0.product_holding = 'E. SABB') 	THEN 18 
																	ELSE (CASE WHEN ((((((((T0.fss = '') OR (T0.fss = 'G')) 
																					OR (T0.fss = 'H')) OR (T0.fss = 'J')) 
																					OR (T0.fss = 'K')) OR (T0.fss = 'L')) 
																					OR (T0.fss = 'M')) OR (T0.fss = 'U')) 			THEN 0 
																				ELSE 0 END) END)
			WHEN ((T0.offer = 'BBT Exp 31-90') OR (T0.offer = 'OTHER Exp -30')) THEN (CASE WHEN ((T0.tenure = '1 year') OR (T0.tenure = 'Less 1 year')) THEN 0 
											ELSE (CASE WHEN ((((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) 
															OR (T0.age = 'Unclassified')) THEN 0 
														ELSE 0 END) END) 
			WHEN (T0.offer = 'BBT Live +90') THEN (CASE WHEN (T0.tenure = '1 year') THEN (CASE WHEN (((T0.product_holding = 'B. DTV + Triple play') 
																							OR (T0.product_holding = 'C. DTV + BB Only')) 
																							OR (T0.product_holding = 'D. DTV + Other Comms')) THEN 7
																						ELSE 1 END) 
														WHEN (T0.tenure = '2 years') THEN 4 
														WHEN (T0.tenure = '3 years') THEN 5 
														WHEN (T0.tenure = '4 years') THEN 8 
														WHEN (T0.tenure = '5+ years') THEN (CASE WHEN (((((((((T0.bb_type = 'Broadband Connect') 
																									OR (T0.bb_type = 'Sky Broadband Everyday')) 
																									OR (T0.bb_type = 'Sky Broadband Lite')) 
																									OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																									OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) 
																									OR (T0.bb_type = 'Sky Fibre')) 
																									OR (T0.bb_type = 'Sky Fibre Lite')) 
																									OR (T0.bb_type = 'Sky Fibre Max')) 
																									OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 
																								ELSE 10 END) 
													ELSE (CASE WHEN ((((((((((((((((T0.bb_type = 'Broadband Connect') 
																OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Lite')) 
																OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) 
																OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) 
																OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) 
																OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) 
																OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
																OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) 	THEN 25 
															ELSE 0 END) END)
				WHEN ((T0.offer = 'BBT Live -30') OR (T0.offer = 'Multi Exp 31-90')) THEN (CASE WHEN ((((T0.affluence = '2.') OR (T0.affluence = '5.')) 
																									OR (T0.affluence = '6.')) OR (T0.affluence = '8.')) THEN 0 
																								ELSE 0 END) 
				WHEN (T0.offer = 'BBT Live 31-90') THEN (CASE WHEN (T0.tenure = 'Less 1 year') THEN 0 
															ELSE 0 END) 
				WHEN (T0.offer = 'Multi Exp +90') THEN 0 
				WHEN ((T0.offer = 'Multi Exp -30') OR (T0.offer = 'Multi Live -30')) THEN 20 
				WHEN (T0.offer = 'Multi Live +90') THEN (CASE WHEN (((((((T0.bb_type = 'Sky Broadband Everyday') OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre'))
																	OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max'))
																	OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 2 
																ELSE (CASE 	WHEN (T0.tenure = '1 year') THEN 3 
																			WHEN (T0.tenure = '5+ years') THEN 13 
																			WHEN ((T0.tenure = 'Less 1 year') OR (T0.tenure = 'weird')) THEN 24 
																		ELSE 12 END) END) 
				WHEN (T0.offer = 'Multi Live 31-90') THEN 26
				WHEN (T0.offer = 'OTHER Exp +90') THEN (CASE WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) 
																OR (T0.age = 'Age 36-45')) THEN 0 
															ELSE (CASE WHEN ((((((((((T0.fss = 'B') OR (T0.fss = 'C')) 
																				OR (T0.fss = 'D')) OR (T0.fss = 'E')) 
																				OR (T0.fss = 'F')) OR (T0.fss = 'G')) 
																				OR (T0.fss = 'I')) OR (T0.fss = 'J')) 
																				OR (T0.fss = 'K')) OR (T0.fss = 'U')) THEN 0 
																		ELSE 0 END) END) 
				WHEN (T0.offer = 'OTHER Exp 31-90') THEN 0 
				WHEN (T0.offer = 'OTHER Live +90') THEN (CASE WHEN (((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) 
																	OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Connect Lite (ROI)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre')) 
																	OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre (ROI)')) 
																	OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) 
																	OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) 
																	OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) 
																			THEN (CASE 	WHEN ((T0.tenure = '4 years') OR (T0.tenure = '5+ years')) THEN 9 
																						WHEN ((T0.tenure = 'Less 1 year') OR (T0.tenure = 'weird')) THEN 21 ELSE 11 END) 
																ELSE (CASE WHEN (T0.tenure = '1 year') 		THEN 14 
																			WHEN (T0.tenure = '2 years') 	THEN 15
																			WHEN ((T0.tenure = '3 years') OR (T0.tenure = '4 years')) 	THEN 16
																			WHEN ((T0.tenure = 'Less 1 year') OR (T0.tenure = 'weird')) THEN 0
																		ELSE 17 END) END) 
				WHEN (T0.offer = 'OTHER Live 31-90') THEN (CASE WHEN ((T0.tenure = '2 years') OR (T0.tenure = '3 years')) 	THEN 23
																WHEN ((T0.tenure = 'Less 1 year') OR (T0.tenure = 'weird')) THEN 0
															ELSE (CASE WHEN (((((((T0.fss = '') OR (T0.fss = 'A')) OR (T0.fss = 'F')) OR (T0.fss = 'G')) 
																			OR (T0.fss = 'H')) OR (T0.fss = 'K')) OR (T0.fss = 'U')) THEN 22
																		ELSE 19 END) END) 
				ELSE (CASE 	WHEN ((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) THEN 0 
							WHEN (T0.product_holding = 'E. SABB') THEN (CASE WHEN ((((T0.life_stage = '1.') OR (T0.life_stage = '2.')) OR (T0.life_stage = '4.')) 
																				OR (T0.life_stage = '8.')) THEN 0 
																			WHEN (((((T0.life_stage = '3.') OR (T0.life_stage = '5.')) OR (T0.life_stage = '6.')) 
																					OR (T0.life_stage = '7.')) OR (T0.life_stage = 'U')) THEN 0 
																			ELSE 0 END) 
							ELSE (CASE WHEN ((((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) 
												OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Lite')) 
												OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) 
												OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) 
												OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) 
												OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Lite (ROI)')) 
												OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) 
												OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre (ROI)')) 
												OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
												OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 0 
										ELSE 0 END) END) END) 					AS C0
/* --- Not used in this verion. This creates churn history
SELECT * 
INTO BB_churn_Q1_raw 
FROM sharmaa.bb_dashboard_churn
WHERE Subs_Year = 2016 
	AND Subs_Quarter_Of_Year = 1 
COMMIT 
CREATE HG INDEX id1 ON BB_churn_Q1_raw (account_number)
CREATE LF INDEX id1 ON BB_churn_Q1_raw (status_code)
COMMIT


SELECT 
  	 Event
	, status_code
	, churn_status
	, churn_route
	, CASE WHEN current_product_description IN ('Sky Fibre','Sky Fibre (ROI - Legacy)','Sky Fibre Lite'
			,'Sky Fibre Max','Sky Fibre Unlimited (ROI - Legacy)','Sky Fibre Unlimited (ROI)','Sky Fibre Unlimited Pro') THEN 'Sky Fibre'
			WHEN current_product_description LIKE 'Sky Connect%' OR
			current_product_description IN ('Broadband Connect', 'Sky Broadband 12GB','Sky Broadband Everyday') THEN 'Sky Connect'
			WHEN current_product_description LIKE 'Sky Broadband Lite%' THEN 'Sky Broadband Lite'
			WHEN current_product_description LIKE 'Sky Broadband Unlimited%' THEN 'Sky Broadband Unlimited'
			ELSE current_product_description END product_desc
	
	, COALESCE(affluence, 'U') AS affluence
	, Age
	, COALESCE(LifeStage,'U') AS LifeStage
	, COALESCE(FSS, 'U') AS FSS
	, CASE 	WHEN DATEDIFF (MONTH, BB_Activation_Date, GETDATE()) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_Activation_Date, GETDATE()) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_Activation_Date, GETDATE())  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_Activation_Date, GETDATE()) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_Activation_Date, GETDATE()) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_Activation_Date, GETDATE())  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 	bb_tenure
	, COUNT (DISTINCT account_number) hits
INTO BB_churn_Q1_agg_CN
FROM BB_churn_Q1_raw
WHERE Status_code	= 'CN' AND age IS NOT NULL 
GROUP BY 

  	 Event
	, status_code
	, churn_status
	, churn_route

	, product_desc
	, affluence
	, Age
	, LifeStage
	, FSS
	, bb_tenure

COMMIT 
CREATE HG INDEX id1 ON BB_churn_Q1_agg_CN(hits)
COMMIT 




---- Creates a agregated view of the base - snapshot @September 2016
SELECT 
	, product_holding
	, bb_type
	, h_AGE_coarse_description      	AS age
	, COALESCE(h_fss_v3_group, 'U')    	AS fss
	, CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE()) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, GETDATE())  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 	AS bb_tenure
	, COALESCE(LEFT (affluence,2), 'U') AS affluence
	, COALESCE(LEFT (life_stage,2),'U') AS life_stage
	, COUNT(DISTINCT account_number) hits
INTO BB_CHURN_agg_201609
FROM sharmaa.View_attachments_201609
WHERE broadband = 1 
GROUP BY 
	  product_holding
	, bb_type
	, age
	, fss
	, bb_tenure
	, affluence
	, life_stage
	
	
	*/			