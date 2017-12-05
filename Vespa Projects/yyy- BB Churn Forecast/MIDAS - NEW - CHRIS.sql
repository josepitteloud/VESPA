        
SELECT         account_number, observation_dt, monthyear 
    , product_holding
	,bb_type 
	,COALESCE(h_fss_v3_group,'U') AS fss 
	,COALESCE(LEFT (affluence,2), 'U') AS affluence 
	,h_AGE_coarse_description AS age 
	,CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
			AS bb_tenure 
	,COALESCE(LEFT (life_stage,2),'U')  AS life_stage 
	, CAST(null AS VARCHAR (30)) AS offer 
	, CAST(NULL AS INT) AS call_offered
	, CAST (NULL AS tinyint) AS segment
	, CAST(null AS smallint) AS m_y
INTO BB_CHURN_ALL_BASES_2014
FROM attach_view_all
WHERE monthyear  <= 201412
	
	

        
SELECT         account_number, observation_dt, monthyear 
    , product_holding
	,bb_type 
	,COALESCE(h_fss_v3_group,'U') AS fss 
	,COALESCE(LEFT (affluence,2), 'U') AS affluence 
	,h_AGE_coarse_description AS age 
	,CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
			AS bb_tenure 
	,COALESCE(LEFT (life_stage,2),'U')  AS life_stage 
	, CAST(null AS VARCHAR (30)) AS offer 
	, CAST(NULL AS INT) AS call_offered
	, CAST (NULL AS tinyint) AS segment
	, CAST(null AS smallint) AS m_y
INTO BB_CHURN_ALL_BASES_2015
FROM attach_view_all
WHERE monthyear BETWEEN  201501 AND 201512	




SELECT         account_number, observation_dt, monthyear 
    , product_holding
	,bb_type 
	,COALESCE(h_fss_v3_group,'U') AS fss 
	,COALESCE(LEFT (affluence,2), 'U') AS affluence 
	,h_AGE_coarse_description AS age 
	,CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) <12 THEN 'Less 1 year'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 12 AND 23 THEN '1 year'	
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  BETWEEN 24 AND 35 THEN '2 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 36 AND 47 THEN '3 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 48 AND 59 THEN '4 years'
			WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  >= 60 		THEN '5+ years'
			ELSE 'weird'		END 
			AS bb_tenure 
	,COALESCE(LEFT (life_stage,2),'U')  AS life_stage 
	, CAST(null AS VARCHAR (30)) AS offer 
	, CAST(NULL AS INT) AS call_offered
	, CAST (NULL AS tinyint) AS segment
	, CAST(null AS smallint) AS m_y
INTO BB_CHURN_ALL_BASES_2016
FROM attach_view_all
WHERE monthyear BETWEEN  201601 AND 201612	

ALTER TABLE BB_CHURN_ALL_BASES_2016 ADD broadband bit default 0 
ALTER TABLE BB_CHURN_ALL_BASES_2015 ADD broadband bit default 0 
ALTER TABLE BB_CHURN_ALL_BASES_2014 ADD broadband bit default 0 



UPDATE BB_CHURN_ALL_BASES_2014
SEt broadband = b.broadband
FROM BB_CHURN_ALL_BASES_2014 AS a 
JOIN attach_view_all AS b ON a.account_number = b.account_number AND a.monthyear= b.monthyear 

UPDATE BB_CHURN_ALL_BASES_2015
SEt broadband = b.broadband
FROM BB_CHURN_ALL_BASES_2015 AS a 
JOIN attach_view_all AS b ON a.account_number = b.account_number AND a.monthyear= b.monthyear 

UPDATE BB_CHURN_ALL_BASES_2016
SEt broadband = b.broadband
FROM BB_CHURN_ALL_BASES_2016 AS a 
JOIN attach_view_all AS b ON a.account_number = b.account_number AND a.monthyear= b.monthyear 





CREATE HG INDEX id1 ON BB_CHURN_ALL_BASES_2014(account_number)
CREATE DATE INDEX id2 ON BB_CHURN_ALL_BASES_2014(observation_dt)
CREATE LF INDEX id3 ON BB_CHURN_ALL_BASES_2014(age)
CREATE LF INDEX id4 ON BB_CHURN_ALL_BASES_2014(offer)
CREATE LF INDEX id5 ON BB_CHURN_ALL_BASES_2014(product_holding)
CREATE LF INDEX id6 ON BB_CHURN_ALL_BASES_2014(monthyear)
CREATE LF INDEX id7 ON BB_CHURN_ALL_BASES_2014(segment)
CREATE LF INDEX id8 ON BB_CHURN_ALL_BASES_2014(m_y)

UPDATE BB_CHURN_ALL_BASES_2014 SET my =  monthyear + 1 
UPDATE BB_CHURN_ALL_BASES_2015 SET my =  monthyear + 1 
UPDATE BB_CHURN_ALL_BASES_2016 SET my =  monthyear + 1 

UPDATE BB_CHURN_ALL_BASES_2014 SET my = 201501 WHERE my = 201413
UPDATE BB_CHURN_ALL_BASES_2015 SET my = 201601 WHERE my = 201513
UPDATE BB_CHURN_ALL_BASES_2016 SET my = 201701 WHERE my = 201613


SELECT 
      account_number
    , monthyear
    , SUM(call_offered) calls 
INTO t1 
FROM BB_CHURN_Midas_NEW
GROUP BY 
    account_number
    , monthyear
	
COMMIT 

CREATE HG INDEX id1 ON t1(account_number)
CREATE LF INDEX id2 ON t1(monthyear)
COMMIT

UPDATE BB_CHURN_ALL_BASES_2015  
SET call_offered = calls
FROM  BB_CHURN_ALL_BASES_2015   AS a 
JOIN t1 AS b ON a.account_number = b.account_number AND a.my = b.monthyear
COMMIT 
UPDATE BB_CHURN_ALL_BASES_2016  
SET call_offered = calls
FROM  BB_CHURN_ALL_BASES_2016   AS a 
JOIN t1 AS b ON a.account_number = b.account_number AND a.my = b.monthyear

COMMIT 
UPDATE BB_CHURN_ALL_BASES_2014  
SET call_offered = calls
FROM  BB_CHURN_ALL_BASES_2014   AS a 
JOIN t1 AS b ON a.account_number = b.account_number AND a.my = b.monthyear
COMMIT 

	DELETE FROM t1 
	FROM  t1 AS b
	JOIN BB_CHURN_ALL_BASES_2014 AS a ON a.account_number = b.account_number AND a.my = b.monthyear

	DELETE FROM t1 
	FROM  t1 AS b
	JOIN BB_CHURN_ALL_BASES_2015 AS a ON a.account_number = b.account_number AND a.my = b.monthyear

	DELETE FROM t1 
	FROM  t1 AS b
	JOIN BB_CHURN_ALL_BASES_2016 AS a ON a.account_number = b.account_number AND a.my = b.monthyear


INSERT INTO BB_CHURN_ALL_BASES_2014 (account_number , my, call_offered)
SELECT account_number , monthyear, calls
FROM t1 
WHERE monthyear <= 201501


INSERT INTO BB_CHURN_ALL_BASES_2015 (account_number , my, call_offered)
SELECT account_number , monthyear, calls
FROM t1 
WHERE monthyear BETWEEN  201502 AND 201601


INSERT INTO BB_CHURN_ALL_BASES_2016 (account_number , my, call_offered)
SELECT account_number , monthyear, calls
FROM t1 
WHERE monthyear BETWEEN  201602 AND 201701
	
	
	
	
	
	

DROP TABLE  temp_Adsmart_end_of_offer_aggregated
DROP TABLE  temp_Adsmart_end_of_offer_raw

SELECT      observation_dt  AS dt
        , b.account_number
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
JOIN     BB_CHURN_ALL_BASES_2016 				AS b ON CPO.account_number =      b.account_number
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
        AND monthyear BETWEEN 201605 AND 201608
GO
DELETE FROM  temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1              -- To keep the latest offer by each offer type

CREATE HG INDEX id1 ON  temp_Adsmart_end_of_offer_raw(account_number)
CREATE DATE INDEX id2 ON  temp_Adsmart_end_of_offer_raw(offer_end_date)
CREATE HG INDEX id3 ON  temp_Adsmart_end_of_offer_raw(days_from_today)
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
CREATE HG INDEX id3 ON  temp_Adsmart_end_of_offer_aggregated(min_end_date)
GO

DELETE FROM temp_Adsmart_end_of_offer_raw
FROM temp_Adsmart_end_of_offer_raw AS a 
JOIN (SELECT DISTINCT account_number , dt FROM temp_Adsmart_end_of_offer_aggregated WHERE Active_offer = 1 ) AS b ON a.account_number = b.account_number AND a.dt = b.dt 
	WHERE Active_offer = 0
GO
-- Flagging the main(s) offer (Closest ending offer)
UPDATE  temp_Adsmart_end_of_offer_raw
SET main_offer = CASE WHEN    b.min_end_date = a.days_from_today THEN 1 ELSE 0 END
FROM  temp_Adsmart_end_of_offer_raw           AS a
JOIN  temp_Adsmart_end_of_offer_aggregated    AS b ON a.account_number = b.account_number AND a.Active_offer = b.Active_offer AND a.dt = b.dt 
GO
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
	
	
	
	
UPDATE  BB_CHURN_ALL_BASES_2016
SET OFFER = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
					ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL                    				THEN 'No info'
														WHEN Active_offer = 1 AND days_from_today  > 90           		THEN 'Live +90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Live 31-90'
														WHEN Active_offer = 1 AND days_from_today  <= 30          		THEN 'Live -30'
														
														WHEN Active_offer = 0 AND days_from_today  > 90           		THEN 'Exp +90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Exp 31-90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 15 AND 30  	THEN 'Exp 15-30'
														WHEN Active_offer = 0 AND days_from_today  <= 30          		THEN 'Exp -30'
														ELSE 'No Offer Ever' END
													  END
FROM BB_CHURN_ALL_BASES_2016 as a
LEFT JOIN  temp_Adsmart_end_of_offer_raw as b
ON a.account_number = b.account_number and a.observation_dt = b.dt 
WHERE monthyear BETWEEN 201605 AND 201608


SELECT offer, monthyear, count(*) from BB_CHURN_ALL_BASES_2016 group by offer, monthyear
	
	
	
	
	
	
	
	
	
	