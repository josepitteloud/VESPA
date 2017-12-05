SELECT 
      account_number
    , DATEPART(wk,call_date ) wk_normal
    , MONTH(call_date ) mnth_normal
    , year(call_date ) tr_normal
    , subs_year
    , subs_week_of_year
    , SUM(no_of_calls)  AS s_calls 
    , count(*)          AS hits
INTO BB_CHURN_calls_details_raw_3yr
FROM CALLS_DETAILS
WHERE call_date >= '2013-09-01'
	AND     final_sct_grouping = 'Retention - BBCoE'
	AND account_number IS NOT NULL 
GROUP BY account_number
    , subs_year
    , subs_week_of_year
    , final_sct_grouping
    , wk_normal
    , mnth_normal
    , tr_normal

--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
DECLARE @y INT 
DECLARE @m INT 
SET @y = 2015
SET @M = 1 

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
	, @y AS yr
	, @m AS mnth
INTO BB_CHURN_append_3yr
FROM BB_CHURN_calls_details_raw_3yr AS a 
JOIN sharmaa.View_attachments_201501  AS b ON a.account_number = b.account_number WHERE tr_normal = @y AND mnth_normal = @m 
 
 
 
 ALTER TABLE BB_CHURN_calls_details_raw_3yr Add (Offer VARCHAR(30))

 --------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
CREATE VARIABLE  @y INT 
CREATE VARIABLE @w INT 
SET @y = 2015
SET @w = 1 






CREATE PROCEDURE xoffer_2 (@y INT, @w INT)
AS BEGIN 

DECLARE @d DATE 
SET @d = (SELECT MIN(calendar_date) FROM SKY_CALENDAR WHERE year(calendar_date) = @y AND DATEPART(wk,calendar_date) = @w)
 
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


IF EXISTS( SELECT TNAME FROM SYSCATALOG
            WHERE CREATOR='pitteloudj'
              AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated'
              AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE  temp_Adsmart_end_of_offer_aggregated
    END
SELECT b.account_number
    , CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END AS sport_flag
    , CASE WHEN (lower (offer_dim_description) LIKE '%movie%' OR lower (offer_dim_description) LIKE '%cinema%') THEN 1 ELSE 0 END AS movie_flag
	, CASE  WHEN    x_subscription_type IN ('SKY TALK','BROADBAND') THEN 'BBT'
            WHEN    x_subscription_type LIKE 'ENHANCED' AND
                    x_subscription_sub_type LIKE 'Broadband DSL Line' THEN 'BBT'
            ELSE 'OTHER'  END 																						AS Offer_type
    , offer_status
	, Active_offer = CASE WHEN offer_status  IN  ('Active',' Pending Terminated', 'Blocked') AND offer_end_dt > @d THEN 1 ELSE 0 END 
    , CASE 	WHEN Active_offer = 1  							THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
			WHEN offer_status = 'Terminated' 				THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
			END AS offer_end_date
    , ABS(DATEDIFF(dd, offer_end_date, @d)) 																		AS days_from_today
    , rank() OVER(PARTITION BY b.account_number 			ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_1
    , rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_2
    , CAST (0 AS bit)                                                      												AS main_offer
INTO     temp_Adsmart_end_of_offer_raw
FROM     cust_product_offers 	AS CPO
JOIN     BB_CHURN_calls_details_raw_3yr 				AS b     ON CPO.account_number = b.account_number AND wk_normal = @w AND tr_normal = @y 
WHERE    offer_id                NOT IN (SELECT offer_id FROM citeam.sk2010_offers_to_exclude)
		AND first_activation_dt > '1900-01-01'
		AND offer_end_dt >= DATEADD(year, -1, @d)
		AND created_dt <= @d
		AND x_subscription_sub_type <> 'DTV Season Ticket'
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge','Sky Go Extra No Additional Charge with Sky Multiscreen')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE UPPER('%Price Protection Offer%')
        AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1              -- To keep the latest offer by each offer type

CREATE HG INDEX id1 ON  temp_Adsmart_end_of_offer_raw(account_number)

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


SELECT
    account_number
	, Active_offer
    , COUNT(*) offers
    , MIN(ABS(DATEDIFF(dd,@d , offer_end_date)))  AS min_end_date    
INTO  temp_Adsmart_end_of_offer_aggregated
FROM  temp_Adsmart_end_of_offer_raw
GROUP BY account_number,Active_offer

COMMIT

CREATE HG INDEX id2 ON  temp_Adsmart_end_of_offer_aggregated(account_number)

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

-----------     Updating multi offers (When the account has 2 or more main offers ending the same day)
UPDATE  temp_Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM  temp_Adsmart_end_of_offer_raw AS a
JOIN (SELECT account_number, count(*) hits FROM  temp_Adsmart_end_of_offer_raw GROUP BY account_number HAVING hits > 1) AS b ON a.account_number = b.account_number
-----------     DEleting duplicates
DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_1 > 1              -- To keep the latest offer by each offer type

-----------     Updating Adsmart table
UPDATE  BB_CHURN_calls_details_raw_3yr
SET OFFER = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
					ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL                    				THEN 'No info'
														WHEN Active_offer = 1 AND days_from_today  > 90           		THEN 'Live +90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Live 31-90'
														WHEN Active_offer = 1 AND days_from_today  <= 30          		THEN 'Live -30'
														WHEN Active_offer = 0 AND days_from_today  > 90           		THEN 'Exp +90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Exp 31-90'
														WHEN Active_offer = 0 AND days_from_today  <= 30          		THEN 'Exp -30'
														ELSE 'No Offer Ever' END
													  END
FROM  BB_CHURN_calls_details_raw_3yr as a
LEFT JOIN  temp_Adsmart_end_of_offer_raw as b ON a.account_number = b.account_number
WHERE a.wk_normal = @w AND tr_normal = @y 

DROP TABLE  temp_Adsmart_end_of_offer_raw
DROP TABLE  temp_Adsmart_end_of_offer_aggregated
					  
END 



UPDATE BB_CHURN_calls_details_raw_3yr
SET seg_JP = (CASE WHEN (a.offer = 'BBT Exp +90') THEN 1 
					WHEN (((a.offer = 'BBT Exp -30') OR (a.offer = 'Multi Live 31-90')) OR (a.offer = 'OTHER Live -30')) THEN 2 
					WHEN ((a.offer = 'BBT Exp 31-90') OR (a.offer = 'OTHER Exp -30')) THEN 2 
					WHEN ((((a.offer = 'BBT Live -30') OR (a.offer = 'BBT Live 31-90')) OR (a.offer = 'Multi Exp +90')) OR (a.offer = 'Multi Exp 31-90')) THEN 4
					WHEN (((a.offer = 'Multi Exp -30') OR (a.offer = 'Multi Live -30')) OR (a.offer = 'OTHER Live 31-90')) THEN 5
					WHEN (a.offer = 'Multi Live +90') THEN (CASE WHEN ((((((((T0.bb_type = 'Sky Broadband 12GB') OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																		OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) 
																		OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 
																	ELSE (CASE WHEN ((((((T0.fss = 'A') OR (T0.fss = 'B')) OR (T0.fss = 'F')) OR (T0.fss = 'I')) OR (T0.fss = 'J')) OR (T0.fss = 'L')) THEN 7 
																				ELSE 8 END) END) 
					WHEN (((a.offer = 'No Offer Ever') OR (a.offer = 'OTHER Exp +90')) OR (a.offer = 'OTHER Exp 31-90')) THEN (CASE WHEN (T0.product_holding = 'E. SABB') THEN 9 
																																			ELSE (CASE WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 10 
																																						WHEN ((T0.age = 'Age 66+') OR (T0.age = 'Unclassified')) THEN 11
																																						ELSE 12 END) END) 
					WHEN (a.offer = 'OTHER Live +90') THEN (CASE WHEN (((((T0.bb_type = 'Sky Broadband Unlimited Fibre') OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																			OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 13 
																	ELSE (CASE WHEN ((TRIM(T0.bb_tenure) LIKE '%1 year%') OR (TRIM(T0.bb_tenure)  LIKE '%Less 1 year%')) THEN 14 
																				ELSE (CASE WHEN ((((T0.age = 'Age 18-25') OR (T0.age = 'Age 46-55')) OR (T0.age = 'Age 56-65')) OR (T0.age = 'Unclassified')) THEN 15 
																							ELSE 16 END) END) END) 
					ELSE (CASE 	WHEN (TRIM(T0.bb_tenure)  LIKE '%1 year%') THEN 17
								WHEN ((TRIM(T0.bb_tenure)  LIKE '%2 years%') OR (TRIM(T0.bb_tenure)  LIKE '%3 years%')) THEN (CASE WHEN ((T0.product_holding = 'B. DTV + Triple play') OR (T0.product_holding = 'C. DTV + BB Only')) THEN 18 
																																		ELSE 19 END) 
								WHEN (TRIM(T0.bb_tenure)  LIKE '%Less 1 year%') THEN 20 
								ELSE (CASE WHEN ((((T0.bb_type = 'Sky Broadband Unlimited Fibre') OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) THEN 21 
					ELSE 22 END) END) END) 
FROM BB_CHURN_calls_details_raw_3yr AS a 
JOIN BB_CHURN_append_3yr AS T0 ON a.account_number = T0.account_number AND mnth_normal = mnth AND yr  = tr_normal 















UPDATE BB_CHURN_calls_details_raw_3yr
SET seg_CS1 =  (CASE WHEN (a.offer = 'BBT Exp +90') THEN (CASE WHEN (T0.product_holding = 'E. SABB') 
														THEN (CASE WHEN ((((((T0.life_stage = '1.') OR (T0.life_stage = '14')) 
																			OR (T0.life_stage = '2.')) OR (T0.life_stage = '4.')) 
																			OR (T0.life_stage = '5.')) OR (T0.life_stage = 'U')) 	THEN 0 
																	ELSE 0 END) 
														ELSE (CASE WHEN (((((T0.fss = 'A') OR (T0.fss = 'C')) OR (T0.fss = 'D')) 
																			OR (T0.fss = 'E')) OR (T0.fss = 'F')) 					THEN 0 
																	WHEN ((((T0.fss = 'B') OR (T0.fss = 'G')) OR (T0.fss = 'H')) 
																			OR (T0.fss = 'K')) 										THEN 0 
																	ELSE 0 END) END)
			WHEN ((a.offer = 'BBT Exp -30') OR (a.offer = 'OTHER Live -30')) THEN (CASE WHEN (T0.product_holding = 'E. SABB') 	THEN 18 
																	ELSE (CASE WHEN ((((((((T0.fss = '') OR (T0.fss = 'G')) 
																					OR (T0.fss = 'H')) OR (T0.fss = 'J')) 
																					OR (T0.fss = 'K')) OR (T0.fss = 'L')) 
																					OR (T0.fss = 'M')) OR (T0.fss = 'U')) 			THEN 0 
																				ELSE 0 END) END)
			WHEN ((a.offer = 'BBT Exp 31-90') OR (a.offer = 'OTHER Exp -30')) THEN (CASE WHEN ((TRIM(T0.bb_tenure) = '1 year') OR (TRIM(T0.bb_tenure) = 'Less 1 year')) THEN 0 
											ELSE (CASE WHEN ((((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) 
															OR (T0.age = 'Unclassified')) THEN 0 
														ELSE 0 END) END) 
			WHEN (a.offer = 'BBT Live +90') THEN (CASE WHEN (TRIM(T0.bb_tenure) = '1 year') THEN (CASE WHEN (((T0.product_holding = 'B. DTV + Triple play') 
																							OR (T0.product_holding = 'C. DTV + BB Only')) 
																							OR (T0.product_holding = 'D. DTV + Other Comms')) THEN 7
																						ELSE 1 END) 
														WHEN (TRIM(T0.bb_tenure) = '2 years') THEN 4 
														WHEN (TRIM(T0.bb_tenure) = '3 years') THEN 5 
														WHEN (TRIM(T0.bb_tenure) = '4 years') THEN 8 
														WHEN (TRIM(T0.bb_tenure) = '5+ years') THEN (CASE WHEN (((((((((T0.bb_type = 'Broadband Connect') 
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
				WHEN ((a.offer = 'BBT Live -30') OR (a.offer = 'Multi Exp 31-90')) THEN (CASE WHEN ((((T0.affluence = '2.') OR (T0.affluence = '5.')) 
																									OR (T0.affluence = '6.')) OR (T0.affluence = '8.')) THEN 0 
																								ELSE 0 END) 
				WHEN (a.offer = 'BBT Live 31-90') THEN (CASE WHEN (TRIM(T0.bb_tenure) = 'Less 1 year') THEN 0 
															ELSE 0 END) 
				WHEN (a.offer = 'Multi Exp +90') THEN 0 
				WHEN ((a.offer = 'Multi Exp -30') OR (a.offer = 'Multi Live -30')) THEN 20 
				WHEN (a.offer = 'Multi Live +90') THEN (CASE WHEN (((((((T0.bb_type = 'Sky Broadband Everyday') OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre'))
																	OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max'))
																	OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 2 
																ELSE (CASE 	WHEN (TRIM(T0.bb_tenure) = '1 year') THEN 3 
																			WHEN (TRIM(T0.bb_tenure) = '5+ years') THEN 13 
																			WHEN ((TRIM(T0.bb_tenure) = 'Less 1 year') OR (TRIM(T0.bb_tenure) = 'weird')) THEN 24 
																		ELSE 12 END) END) 
				WHEN (a.offer = 'Multi Live 31-90') THEN 26
				WHEN (a.offer = 'OTHER Exp +90') THEN (CASE WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) 
																OR (T0.age = 'Age 36-45')) THEN 0 
															ELSE (CASE WHEN ((((((((((T0.fss = 'B') OR (T0.fss = 'C')) 
																				OR (T0.fss = 'D')) OR (T0.fss = 'E')) 
																				OR (T0.fss = 'F')) OR (T0.fss = 'G')) 
																				OR (T0.fss = 'I')) OR (T0.fss = 'J')) 
																				OR (T0.fss = 'K')) OR (T0.fss = 'U')) THEN 0 
																		ELSE 0 END) END) 
				WHEN (a.offer = 'OTHER Exp 31-90') THEN 0 
				WHEN (a.offer = 'OTHER Live +90') THEN (CASE WHEN (((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) 
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
																			THEN (CASE 	WHEN ((TRIM(T0.bb_tenure) = '4 years') OR (TRIM(T0.bb_tenure) = '5+ years')) THEN 9 
																						WHEN ((TRIM(T0.bb_tenure) = 'Less 1 year') OR (TRIM(T0.bb_tenure) = 'weird')) THEN 21 ELSE 11 END) 
																ELSE (CASE WHEN (TRIM(T0.bb_tenure) = '1 year') 		THEN 14 
																			WHEN (TRIM(T0.bb_tenure) = '2 years') 	THEN 15
																			WHEN ((TRIM(T0.bb_tenure) = '3 years') OR (TRIM(T0.bb_tenure) = '4 years')) 	THEN 16
																			WHEN ((TRIM(T0.bb_tenure) = 'Less 1 year') OR (TRIM(T0.bb_tenure) = 'weird')) THEN 0
																		ELSE 17 END) END) 
				WHEN (a.offer = 'OTHER Live 31-90') THEN (CASE WHEN ((TRIM(T0.bb_tenure) = '2 years') OR (TRIM(T0.bb_tenure) = '3 years')) 	THEN 23
																WHEN ((TRIM(T0.bb_tenure) = 'Less 1 year') OR (TRIM(T0.bb_tenure) = 'weird')) THEN 0
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
										ELSE 0 END) END) END) 					
FROM BB_CHURN_calls_details_raw_3yr AS a 
JOIN BB_CHURN_append_3yr AS T0 ON a.account_number = T0.account_number AND mnth_normal = mnth AND yr  = tr_normal 
										

UPDATE BB_CHURN_calls_details_raw_3yr
SET seg_CS2 =  (CASE 	WHEN ((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) THEN (CASE WHEN (((((((((((((((((a.offer = 'BBT Exp +90') OR (a.offer = 'BBT Exp -30')) OR (a.offer = 'BBT Exp 31-90')) 
																																OR (a.offer = 'BBT Live +90')) OR (a.offer = 'BBT Live 31-90')) OR (a.offer = 'Multi Exp +90')) 
																																OR (a.offer = 'Multi Exp -30')) OR (a.offer = 'Multi Exp 31-90')) OR (a.offer = 'Multi Live +90')) 
																																OR (a.offer = 'Multi Live -30')) OR (a.offer = 'Multi Live 31-90')) OR (a.offer = 'OTHER Exp +90')) 
																																OR (a.offer = 'OTHER Exp -30')) OR (a.offer = 'OTHER Exp 31-90')) OR (a.offer = 'OTHER Live +90')) 
																																OR (a.offer = 'OTHER Live -30')) OR (a.offer = 'OTHER Live 31-90')) THEN 0 ELSE 0 END) 
		WHEN (T0.product_holding = 'E. SABB') THEN (CASE 	WHEN (((((a.offer = 'BBT Exp +90') OR (a.offer = 'Multi Exp -30')) OR (a.offer = 'Multi Live +90')) OR (a.offer = 'Multi Live -30')) OR (a.offer = 'Multi Live 31-90')) THEN 10 
															WHEN (((((a.offer = 'BBT Exp -30') OR (a.offer = 'Multi Exp +90')) OR (a.offer = 'OTHER Exp -30')) OR (a.offer = 'OTHER Exp 31-90')) OR (a.offer = 'OTHER Live 31-90')) THEN 11 
															WHEN ((((a.offer = 'BBT Exp 31-90') OR (a.offer = 'BBT Live -30')) OR (a.offer = 'OTHER Exp +90')) OR (a.offer = 'OTHER Live -30')) THEN 0 
															WHEN (a.offer = 'BBT Live 31-90') THEN 0 
															WHEN (a.offer = 'No Offer Ever') THEN (CASE WHEN ((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) THEN 0 
																										WHEN (T0.age = 'Age 36-45') THEN 0 
																										WHEN (T0.age = 'Age 46-55') THEN 0 ELSE 0 END) 
																										ELSE (CASE 	WHEN (TRIM(T0.bb_tenure) = '1 year') THEN 1 
																													WHEN ((TRIM(T0.bb_tenure) = '2 years') OR (TRIM(T0.bb_tenure) = '3 years')) THEN 2 
																													WHEN ((TRIM(T0.bb_tenure) = '4 years') OR (TRIM(T0.bb_tenure) = '5+ years')) THEN 3 ELSE 0 END) END)
															ELSE (CASE WHEN ((a.offer = 'BBT Exp +90') OR (a.offer = 'OTHER Exp +90')) THEN (CASE WHEN (((((((T0.life_stage = '1.') OR (T0.life_stage = '14')) OR (T0.life_stage = '3.')) 
																																							OR (T0.life_stage = '4.')) OR (T0.life_stage = '5.')) OR (T0.life_stage = '6.')) OR (T0.life_stage = '8.')) THEN 0 
																																					WHEN (((T0.life_stage = '11') OR (T0.life_stage = '2.')) OR (T0.life_stage = '7.')) THEN 0 
																																					WHEN ((T0.life_stage = '12') OR (T0.life_stage = 'U')) THEN 0 ELSE 0 END) 
															WHEN ((a.offer = 'BBT Exp -30') OR (a.offer = 'Multi Exp 31-90')) THEN (CASE WHEN (((((((T0.life_stage = '2.') OR (T0.life_stage = '3.')) OR (T0.life_stage = '4.')) OR (T0.life_stage = '6.')) OR (T0.life_stage = '7.')) 
																																					OR (T0.life_stage = '8.')) OR (T0.life_stage = '9.')) THEN 0 ELSE 0 END) 
															WHEN (a.offer = 'BBT Exp 31-90') THEN (CASE WHEN (((((T0.life_stage = '10') OR (T0.life_stage = '12')) OR (T0.life_stage = '13')) OR (T0.life_stage = '9.')) OR (T0.life_stage = 'U')) THEN 0 ELSE 0 END) 
															WHEN (a.offer = 'BBT Live +90') THEN (CASE WHEN (TRIM(T0.bb_tenure) = '1 year') THEN 5 
																										WHEN (((TRIM(T0.bb_tenure) = '2 years') OR (TRIM(T0.bb_tenure) = '3 years')) OR (TRIM(T0.bb_tenure) = '4 years')) THEN 8 
																										WHEN (TRIM(T0.bb_tenure) = '5+ years') THEN 7 ELSE 0 END) 
															WHEN ((a.offer = 'BBT Live -30') OR (a.offer = 'Multi Exp -30')) THEN 0 
															WHEN (a.offer = 'BBT Live 31-90') THEN (CASE WHEN (TRIM(T0.bb_tenure) = 'Less 1 year') THEN 0 ELSE 0 END) 
															WHEN (a.offer = 'Multi Exp +90') THEN 0 
															WHEN (a.offer = 'Multi Live +90') THEN (CASE WHEN (((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) 
																											OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																											OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) 
																											OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 4 ELSE 9 END) 
															WHEN ((a.offer = 'Multi Live -30') OR (a.offer = 'OTHER Live 31-90')) THEN (CASE WHEN (((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) 
																																				OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) 
																																				OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																																				OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) 
																																				OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre (ROI)')) 
																																				OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
																																				OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 13 ELSE 0 END) 
															WHEN ((a.offer = 'Multi Live 31-90') OR (a.offer = 'OTHER Live -30')) THEN (CASE WHEN (((T0.age = 'Age 46-55') OR (T0.age = 'Age 56-65')) OR (T0.age = 'Age 66+')) THEN 0 ELSE 0 END) 
															WHEN (a.offer = 'OTHER Exp -30') THEN 0 
															WHEN (a.offer = 'OTHER Exp 31-90') THEN 0 
															WHEN (a.offer = 'OTHER Live +90') THEN (CASE WHEN ((((((T0.bb_type = 'Sky Broadband Everyday') OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																												OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 ELSE 12 END) 
															ELSE (CASE WHEN ((((((((((T0.bb_type = 'Sky Broadband 12GB') OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																				OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI)')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																				OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 0 ELSE 0 END) END) END) 
FROM BB_CHURN_calls_details_raw_3yr AS a 
JOIN BB_CHURN_append_3yr AS T0 ON a.account_number = T0.account_number AND mnth_normal = mnth AND yr  = tr_normal 
										
										
										
										
										
										
										
										
										
										
										
CREATE OR REPLACE VIEW attach_view AS 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201411 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
    UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201412 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
    UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201501 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
    UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201502 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
    UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201503 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201504 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201505 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201506 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201507 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201508 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201509 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201510 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201511             WHERE broadband = 1 OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201512 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201601 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
        UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
, BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201602 WHERE broadband = 1 
UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201603 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201604 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201605 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201606 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201607 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201608 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201609 WHERE broadband = 1  OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201610					 WHERE broadband = 1 					 OR product_holding LIKE '%E. SABB%'
            UNION ALL 
SELECT account_number , observation_dt , product_holding	, bb_type	, h_AGE_coarse_description   , h_fss_v3_group
    , BB_latest_act_date, affluence, life_stage, monthyear FROM sharmaa.View_attachments_201611					 WHERE broadband = 1 					 OR product_holding LIKE '%E. SABB%'	