-- MIDAS loading data 

 
LOAD TABLE BB_CHURN_Midas_data_3 (
    call_date, account_number, SG2,  call_offered, call_answered
 '\n' )
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/query_20161114_164004.csv' QUOTES OFF ESCAPES OFF NOTIFY 1000 DELIMITED BY '|'-- START ROW ID 1
;
commit;


UPDATE BB_CHURN_Midas_data_3
SET call_wk  = shs_week_of_year
    , call_yr = shs_year
    , call_q = shs_quarter_of_year
FROM     BB_CHURN_Midas_data_3 as a 
JOIN sky_calendar as b on a.call_date =b.calendar_date 



UPDATE BB_CHURN_Midas_data_3
SET monthyear = CAST(year(call_date) || RIGHT('00'||MONTH(call_date ),2) AS INT) 

UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear 



UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear-1
WHERE a.age is null 



UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear+1
WHERE a.age is null 


UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear+2
WHERE a.age is null 

UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear-2
WHERE a.age is null 



UPDATE BB_CHURN_Midas_data_3  SET flag = 1 WHERE AGE is not  null 

UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view2 AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear 
WHERE a.age is null 

UPDATE BB_CHURN_Midas_data_3
SET a.product_holding  = b.product_holding
	,a.bb_type = b.bb_type 
	,a.fss = COALESCE(b.h_fss_v3_group,'U')
	,a.affluence = COALESCE(LEFT (b.affluence,2), 'U')
	,a.age = b.h_AGE_coarse_description
	,a.bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, call_date)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
	,a.life_stage = COALESCE(LEFT (b.life_stage,2),'U') 
FROM  BB_CHURN_Midas_data_3 AS a 
JOIN attach_view2 AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear -1
WHERE a.age is null 


--==========================================================--
--==========================================================--
--==========================================================--



IF EXISTS( SELECT TNAME FROM SYSCATALOG WHERE CREATOR='pitteloudj' AND UPPER(TNAME)='temp_Adsmart_end_of_offer_raw' AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE  temp_Adsmart_end_of_offer_raw
    END
MESSAGE 'CREATE TABLE temp_Adsmart_end_of_offer_raw' TYPE STATUS TO CLIENT

IF EXISTS( SELECT TNAME FROM SYSCATALOG WHERE CREATOR='pitteloudj' AND UPPER(TNAME)='temp_Adsmart_end_of_offer_aggregated' AND UPPER(TABLETYPE)='TABLE')
    BEGIN
       MESSAGE 'WARN: TEMP TABLE TEMP_SKY_STORE_RENTAL ALREADY EXISTS - DROP AND RECREATE' TYPE STATUS TO CLIENT
       DROP TABLE  temp_Adsmart_end_of_offer_aggregated
    END

SELECT call_date AS dt, b.account_number
		, CASE WHEN lower (offer_dim_description) LIKE '%sport%' THEN 1 ELSE 0 END AS sport_flag
		, CASE WHEN (lower (offer_dim_description) LIKE '%movie%' OR lower (offer_dim_description) LIKE '%cinema%') THEN 1 ELSE 0 END AS movie_flag
		, CASE  WHEN    x_subscription_type IN ('SKY TALK','BROADBAND') THEN 'BBT'
				WHEN    x_subscription_type LIKE 'ENHANCED' AND
						x_subscription_sub_type LIKE 'Broadband DSL Line' THEN 'BBT'
				ELSE 'OTHER'  END 																						AS Offer_type
		, offer_status
		, Active_offer = CASE WHEN offer_status  IN  ('Active',' Pending Terminated', 'Blocked') AND offer_end_dt > dt THEN 1 ELSE 0 END 
		, CASE 	WHEN Active_offer = 1  							THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
				WHEN offer_status = 'Terminated' 				THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
				END AS offer_end_date
		, ABS(DATEDIFF(dd, offer_end_date, dt)) 																		AS days_from_today
		, rank() OVER(PARTITION BY b.account_number,dt 			ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_1
		, rank() OVER(PARTITION BY b.account_number,dt, Offer_type ORDER BY Active_offer DESC, days_from_today,cb_row_id)      AS rankk_2
		, CAST (0 AS bit)                                                      												AS main_offer
	INTO     temp_Adsmart_end_of_offer_raw
	FROM     cust_product_offers 	AS CPO
JOIN     BB_CHURN_Midas_data_3 				AS b ON CPO.account_number =      b.account_number
WHERE    offer_id                NOT IN (SELECT offer_id FROM citeam.sk2010_offers_to_exclude)
		AND first_activation_dt > '1900-01-01'
		AND offer_end_dt >=  DATEADD(month, -12, dt) 
		AND created_dt <= dt
		AND x_subscription_sub_type <> 'DTV Season Ticket'
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge','Sky Go Extra No Additional Charge with Sky Multiscreen')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE UPPER('%Price Protection Offer%')
        AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1              -- To keep the latest offer by each offer type

CREATE HG INDEX id1 ON  temp_Adsmart_end_of_offer_raw(account_number)

------------------------------------------------------------------------------

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
    , COUNT(*) offers, dt 
    , MIN(ABS(DATEDIFF(dd,dt , offer_end_date)))  AS min_end_date    
INTO  temp_Adsmart_end_of_offer_aggregated
FROM  temp_Adsmart_end_of_offer_raw
GROUP BY account_number,Active_offer, dt 

COMMIT
GO
CREATE HG INDEX id2 ON  temp_Adsmart_end_of_offer_aggregated(account_number)
GO
-- Deleting expired offers when the account has an active offer
DELETE FROM temp_Adsmart_end_of_offer_raw
FROM temp_Adsmart_end_of_offer_raw AS a 
JOIN (SELECT DISTINCT account_number , dt FROM temp_Adsmart_end_of_offer_aggregated WHERE Active_offer = 1 ) AS b ON a.account_number = b.account_number AND a.dt = b.dt 
	WHERE Active_offer = 0

-- Flagging the main(s) offer (Closest ending offer)
UPDATE  temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN    b.min_end_date = a.days_from_today THEN 1 ELSE 0 END
FROM  temp_Adsmart_end_of_offer_raw           AS a
JOIN  temp_Adsmart_end_of_offer_aggregated    AS b ON a.account_number = b.account_number AND a.Active_offer = b.Active_offer AND a.dt = b.dt 

-----------     Deleting other offers - not the main(s) (which end date is not the min date)
DELETE FROM  temp_Adsmart_end_of_offer_raw        AS a
WHERE   main_offer = 0
GO
-----------     Updating multi offers (When the account has 2 or more main offers ending the same day)
UPDATE  temp_Adsmart_end_of_offer_raw
SET Offer_type = 'Multi offer'
FROM  temp_Adsmart_end_of_offer_raw AS a
JOIN (SELECT account_number,dt, count(*) hits FROM  temp_Adsmart_end_of_offer_raw GROUP BY account_number, dt HAVING hits > 1) AS b ON a.account_number = b.account_number AND a.dt = b.dt 
-----------     DEleting duplicates
DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_1 > 1              -- To keep the latest offer by each offer type
GO
-----------     Updating Adsmart table
--     Updating Adsmart table
UPDATE  BB_CHURN_Midas_data_3
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
FROM BB_CHURN_Midas_data_3 as a
LEFT JOIN  temp_Adsmart_end_of_offer_raw as b
ON a.account_number = b.account_number and a.call_date = b.dt 


DROP TABLE  temp_Adsmart_end_of_offer_raw
DROP TABLE  temp_Adsmart_end_of_offer_aggregated
					  
			GRANT SELECT ON BB_CHURN_Midas_data_3  TO spencerc2, vespa_group_low_security
			
			
			commit
			

			
UPDATE BB_CHURN_Midas_data_3
SET segment = (CASE WHEN (((((T0.Offer = 'BBT Exp +90') OR (T0.Offer = 'BBT Live -30')) OR (T0.Offer = 'BBT Live 31-90')) OR (T0.Offer = 'Multi Exp +90')) OR (T0.Offer = 'OTHER Live 31-90')) 
									THEN CASE 	WHEN (((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) OR (T0.product_holding = 'E. SABB')) 
													THEN (CASE 	WHEN (((T0.age = 'Age 46-55') OR (T0.age = 'Age 56-65')) OR (T0.age = 'Age 66+')) 			THEN 12	--0.071969341173376403 
																ELSE  7 /*0.090568659162695797 */END) 
												ELSE (CASE 	WHEN ((((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) OR (T0.bb_tenure = 'Less 1 year')) 		THEN  23	--0.0407354305473559 
															WHEN (T0.bb_tenure = '5+ years') 			THEN	0 -- 0.029488344673429801 
															ELSE 0 /*0.0200276053711642 END)*/ END) END
				WHEN (T0.Offer = 'BBT Exp -30') THEN (CASE 	WHEN ((T0.product_holding = 'D. DTV + Other Comms') OR (T0.product_holding = 'E. SABB')) THEN 1 	--0.46219288445177897 
															ELSE (CASE 	WHEN (T0.bb_type = 'Sky Broadband Unlimited Fibre') THEN 2		-- 0.15791238890234599 
																		ELSE (CASE 	WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = 'Less 1 year')) THEN 5 	--0.121322560814964 
																					WHEN (((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) 		THEN	14	-- 0.067305186343497594 
																					ELSE 17 	/*0.057837933291090597 */ END) END) END) 
				WHEN (T0.Offer = 'BBT Exp 31-90') THEN (CASE WHEN (((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) OR (T0.product_holding = 'E. SABB')) 
																THEN (CASE 	WHEN (((((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '2 years')) OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) OR (T0.bb_tenure = '5+ years')) 		THEN 6 	--0.095081252573199895 
																			ELSE 0 /*0.0366479127164216 */ END) 
															ELSE (CASE 	WHEN (((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) 		THEN 0 		--0.033736153071500602 
																			WHEN (T0.bb_tenure = '5+ years') 		THEN 0 		--0.027494042725157101 
																			ELSE 0 /*0.018916031018905401 */ END) END) 
				WHEN (T0.Offer = 'BBT Live +90') THEN (CASE WHEN (T0.bb_tenure = '1 year') THEN  0	--0.0251699380306298 
															WHEN (T0.bb_tenure = '2 years') THEN 0	--0.022279173771429201 
															WHEN (T0.bb_tenure = '3 years') THEN 0	--0.024865823774373401 
															WHEN (T0.bb_tenure = '4 years') THEN 0	--0.023571669098664299 
															WHEN (T0.bb_tenure = '5+ years') THEN (CASE WHEN ((((((((T0.FSS = '') OR (T0.FSS = 'A')) OR (T0.FSS = 'D')) OR (T0.FSS = 'F')) OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'L')) OR (T0.FSS = 'U')) THEN 0 --0.020821778004570399 
																										ELSE 0 /* 0.0227527199746488 */END) 
															ELSE (CASE WHEN (T0.product_holding = 'E. SABB') THEN (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 56-65')) OR (T0.age = 'Age 66+')) 		THEN	0  --0.015411170016027599 
																															WHEN ((T0.age = 'Age 46-55') OR (T0.age = 'Unclassified')) 		THEN 0 		--0.012902989090553299 
																															ELSE 0 /*0.011767332396907901 */ END) 
																							ELSE (CASE 	WHEN ((((T0.FSS = '') OR (T0.FSS = 'A')) OR (T0.FSS = 'H')) OR (T0.FSS = 'U')) 		THEN 0 		--0.0084012909901697505 
																										WHEN (((((T0.FSS = 'B') OR (T0.FSS = 'D')) OR (T0.FSS = 'E')) OR (T0.FSS = 'F')) OR (T0.FSS = 'G')) 		THEN 0	--0.0089692958059850898 
																										ELSE 0  /*0.010710271256177 */ END) END) END) 
				WHEN (T0.Offer = 'Multi Exp -30') THEN (CASE	WHEN ((((((T0.FSS = 'C') OR (T0.FSS = 'D')) OR (T0.FSS = 'F')) OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'I')) THEN 4		--0.12218879816645201 
																ELSE 3	/*0.13822765150383801 */ END) 
				WHEN (T0.Offer = 'Multi Exp 31-90') THEN 22		--0.040746737095359198 
				WHEN (T0.Offer = 'Multi Live +90') THEN (CASE 	WHEN ((((T0.FSS = '') OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'H')) THEN 0 		--0.022611353170751899 
																WHEN (((T0.FSS = 'A') OR (T0.FSS = 'E')) OR (T0.FSS = 'I')) THEN 0		--0.024297113646281399 
																ELSE 0	/*0.028232410106399399 */ END) 
				WHEN (T0.Offer = 'OTHER Exp +90') THEN (CASE 	WHEN ((((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '2 years')) OR (T0.bb_tenure = '4 years')) OR (T0.bb_tenure = 'Less 1 year')) THEN 21	--0.041045047667564297 
																ELSE 0 /*0.029204183440377798 */ END) 
				WHEN (T0.Offer = 'OTHER Exp -30') THEN (CASE 	WHEN (((((T0.FSS = '') OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'E')) OR (T0.FSS = 'F')) THEN 9 --0.081942630484206497 
																WHEN (((T0.FSS = 'G') OR (T0.FSS = 'H')) OR (T0.FSS = 'I')) THEN 11 --0.076030169242089995 
																ELSE 8 		--0.086027772515991696
																END) 
				WHEN (T0.Offer = 'OTHER Exp 31-90') THEN (CASE 	WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = 'Less 1 year')) THEN 0 		--0.023675916673072499 
																ELSE (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 0 		--0.030293689048313002 
																			ELSE 0 	/* 0.025213527737037399 */  END) END) 
				WHEN (T0.Offer = 'OTHER Live +90') THEN (CASE 	WHEN ((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Lite')) 
																		OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) 
																					THEN (CASE 	WHEN ((((((((T0.FSS = 'A') OR (T0.FSS = 'I')) OR (T0.FSS = 'J')) OR (T0.FSS = 'K')) OR (T0.FSS = 'L')) OR (T0.FSS = 'M')) OR (T0.FSS = 'N')) OR (T0.FSS = 'U')) THEN 13 	--0.0678449588497195 
																								ELSE 16  /*0.0581719402384425 */ END) 
																ELSE (CASE 	WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '4 years')) THEN (CASE 	WHEN (((((((T0.FSS = '') OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'D')) 
																																							OR (T0.FSS = 'E')) OR (T0.FSS = 'N')) OR (T0.FSS = 'U')) THEN 19 	--0.043658449446230797 
																																						ELSE 0  /* 0.0371177474158077 */ END) 	
																			WHEN (T0.bb_tenure = '2 years') THEN 24 	--0.039010349684610202 
																			WHEN (T0.bb_tenure = '3 years') THEN 0		--0.038385269121812997 
																			WHEN (T0.bb_tenure = 'Less 1 year') THEN (CASE 	WHEN (((((T0.FSS = '') OR (T0.FSS = 'E')) OR (T0.FSS = 'H')) OR (T0.FSS = 'I')) OR (T0.FSS = 'U')) THEN 0 --0.026595120315251099 
																															ELSE 0 /*0.030118734218019701 */ END) 
																			ELSE (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 20 	--0.0419366322534711 
																						ELSE 0 	/*0.034859421625617101*/  END) END) END) 
				ELSE (CASE 	WHEN ((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) THEN  0		--0.013732760940004 
							WHEN (T0.product_holding = 'E. SABB') THEN (CASE 	WHEN ((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) THEN 10 -- 0.078304239401496195 
																				WHEN (T0.age = 'Age 46-55') THEN 18 	--0.048419009220890798 
																				WHEN ((T0.age = 'Age 56-65') OR (T0.age = 'Age 66+')) THEN 25 		--0.038948570242358101 
																				ELSE 15 	/* 0.059921425111673199 */ END) 
							ELSE (CASE 	WHEN ((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
												OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 0 	--0.032349027311323802 
										ELSE (CASE 	WHEN (((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '2 years')) OR (T0.bb_tenure = 'Less 1 year')) THEN (CASE	WHEN ((((((T0.FSS = '') OR (T0.FSS = 'A')) OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'I')) OR (T0.FSS = 'N')) THEN 0 --0.0231400806920046 
																																									ELSE 0 /*0.017947272512749302 */ END) 
													WHEN (T0.bb_tenure = '3 years') THEN (CASE 	WHEN (((((((T0.FSS = 'F') OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'J')) OR (T0.FSS = 'L')) OR (T0.FSS = 'M')) OR (T0.FSS = 'N')) THEN 0	--0.0111224605606628 
																								ELSE 0 /*0.0138119597812665 */ END) 
													WHEN (T0.bb_tenure = '4 years') THEN (CASE 	WHEN (((((((T0.FSS = 'F') OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'J')) OR (T0.FSS = 'L')) OR (T0.FSS = 'M')) OR (T0.FSS = 'N')) THEN 0	--0.0106805526520838 
																								ELSE 0 /*0.014627152786599601 */ END) 
													ELSE (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 0		--0.016050568259763798 
																WHEN (T0.age = 'Age 56-65') THEN 0		--0.012175184993976899 
																WHEN (T0.age = 'Age 66+') THEN 0		--0.010625694187338001 
																ELSE 0	/*0.0135210262972947 */ END) END) END) END) END) 

FROM BB_CHURN_Midas_data_3 AS T0 	


UPDATE BB_CHURN_Midas_consolidated 
SET segment  = 26 
WHERE product_holding NOT IN( 'B. DTV + Triple play','E. SABB','C. DTV + BB Only','D. DTV + Other Comms')		