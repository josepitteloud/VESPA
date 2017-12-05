
  
  SELECT account_number
,call_date
,call_wk
,call_yr
,call_Q
,call_offered
,call_answered
,segment
,Offer
,product_holding
,bb_type
,age
,fss
,bb_tenure
,affluence
,life_stage
,call_wk_2
,call_yr_2
,call_Q_2 INTO BB_CHURN_Midas_NEW from BB_CHURN_Midas_data_2 
  UNION 
  SELECT account_number
,call_date
,call_wk
,call_yr
,call_Q
,call_offered
,call_answered
,segment
,Offer
,product_holding
,bb_type
,age
,fss
,bb_tenure
,affluence
,life_stage
,call_wk_2
,call_yr_2
,call_Q_2 from BB_CHURN_Midas_data_3
  
  
  
  
  
  UPDATE BB_CHURN_Midas_NEW  SET 
  
  segment =NULL
,Offer=NULL
,product_holding=NULL
,bb_type=NULL
,age=NULL
,fss=NULL
,bb_tenure=NULL
,affluence=NULL
,life_stage=NULL


ALTER TABLE  BB_CHURN_Midas_NEW add monthyear int 

UPDATE BB_CHURN_Midas_NEW
SET monthyear = CAST(year(call_date) || RIGHT('00'||MONTH(call_date ),2) AS INT) 


CREATE HG INDEX id1 ON BB_CHURN_Midas_NEW(account_number)
CREATE DTTM INDEX id2 ON BB_CHURN_Midas_NEW(call_date)
CREATE LF INDEX id3 ON BB_CHURN_Midas_NEW(age)
CREATE LF INDEX id4 ON BB_CHURN_Midas_NEW(offer)
CREATE LF INDEX id5 ON BB_CHURN_Midas_NEW(product_holding)
CREATE LF INDEX id6 ON BB_CHURN_Midas_NEW(call_wk_2)
CREATE LF INDEX id7 ON BB_CHURN_Midas_NEW(call_q_2)
CREATE LF INDEX id8 ON BB_CHURN_Midas_NEW(call_yr_2)






UPDATE BB_CHURN_Midas_NEW
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
FROM  BB_CHURN_Midas_NEW AS a 
JOIN attach_view_all AS b ON A.account_number = b.account_number AND a.monthyear = b.monthyear 




SELECT call_date AS dt, b.account_number
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
JOIN     BB_CHURN_Midas_NEW 				AS b ON CPO.account_number =      b.account_number
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
CREATE DATE INDEX id2 ON  temp_Adsmart_end_of_offer_raw(offer_end_date)
CREATE HG INDEX id3 ON  temp_Adsmart_end_of_offer_raw(days_from_today)

DROP TABLE  temp_Adsmart_end_of_offer_aggregated

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

UPDATE  BB_CHURN_Midas_NEW
SET OFFER = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
					ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL                    				THEN 'No info'
														WHEN Active_offer = 1 AND days_from_today  > 90           		THEN 'Live +90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Live 31-90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 15 AND 30  	THEN 'Live 15-30'
														WHEN Active_offer = 1 AND days_from_today  < 15          		THEN 'Live <14'
														
														WHEN Active_offer = 0 AND days_from_today  > 90           		THEN 'Exp +90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Exp 31-90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 15 AND 30  	THEN 'Exp 15-30'
														WHEN Active_offer = 0 AND days_from_today  < 15          		THEN 'Exp <14'
														ELSE 'No Offer Ever' END
													  END
FROM BB_CHURN_Midas_NEW as a
LEFT JOIN  temp_Adsmart_end_of_offer_raw as b
ON a.account_number = b.account_number and a.call_date = b.dt 

GO
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
----------------------- THE BASE 
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT CAST(NULL AS BIGINT ) base,CAST(NULL AS INT) call_offered, CAST (NULL AS INT ) call_yr, offer  INTO t_offer
FROM BB_CHURN_Midas_NEW 

SELECT DISTINCT product_holding, 1  INTO t_PH FROM attach_view_all
SELECT DISTINCT CASE WHEN UPPER(bb_type)  LIKE '%FIBRE%' THEN 'Fibre' 
                    WHEN UPPER(bb_type)  LIKE '%UNLIMITED%' THEN 'Unlimited' 
                    WHEN UPPER(bb_type)  LIKE '%LITE%' THEN 'Lite' 
                    else 'Other' END AS bb_type , 1 FROM attach_view_all

SELECT DISTINCT h_AGE_coarse_description AS age  INTO t_age FROM attach_view_all
SELECT DISTINCT COALESCE(h_fss_v3_group,'U') AS fss , 1 INTO t_fss FROM attach_view_all
SELECT DISTINCT bb_tenure , 1 INTO t_bt from  BB_CHURN_Midas_NEW 


SELECT * INTO BB_CHURN_NEW_BASE from t_offer
CROSS JOIN t_PH
CROSS JOIN t_age
CROSS JOIN t_fss
CROSS JOIN t_bt
CROSS JOIN t_bb





DECLARE @my INT
SET @my = 201411



DROP TABLE  temp_Adsmart_end_of_offer_aggregated
DROP TABLE  temp_Adsmart_end_of_offer_raw
DROP TABLE  BASE_TEMP

SELECT account_number, observation_dt, CAST(null AS VARCHAR(30) ) OFFER 
    ,product_holding
	,CASE   WHEN UPPER(bb_type)  LIKE '%FIBRE%' THEN 'Fibre' 
            WHEN UPPER(bb_type)  LIKE '%UNLIMITED%' THEN 'Unlimited' 
            WHEN UPPER(bb_type)  LIKE '%LITE%' THEN 'Lite' 
            else 'Other' END AS bb_type
	,fss = COALESCE(h_fss_v3_group,'U')
	,affluence = COALESCE(LEFT (affluence,2), 'U')
	,age = h_AGE_coarse_description
	,bb_tenure = CASE 	WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) <12 THEN 'Less 1 year'
                        WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 12 AND 23 THEN '1 year'	
                        WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  BETWEEN 24 AND 35 THEN '2 years'
                        WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 36 AND 47 THEN '3 years'
                        WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt) BETWEEN 48 AND 59 THEN '4 years'
                        WHEN DATEDIFF (MONTH, BB_latest_act_date, observation_dt)  >= 60 		THEN '5+ years'
                    ELSE 'weird'		END 
	,life_stage = COALESCE(LEFT (life_stage,2),'U') 
	, monthyear
into BASE_TEMP
from attach_view_all 
where                   MONTHYEAR = @my
COMMIT 
CREATE HG INDEX ID1 ON BASE_TEMP(account_number)
CREATE DATE  INDEX ID2 ON BASE_TEMP(observation_dt)
COMMIT 

SELECT observation_dt  AS dt, b.account_number
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
JOIN     BASE_TEMP 				AS b ON CPO.account_number =      b.account_number
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

UPDATE  BASE_TEMP
SET OFFER = CASE WHEN b.account_number IS NULL THEN 'No Offer Ever'
					ELSE TRIM(offer_type) ||' '|| CASE  WHEN days_from_today IS NULL                    				THEN 'No info'
														WHEN Active_offer = 1 AND days_from_today  > 90           		THEN 'Live +90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Live 31-90'
														WHEN Active_offer = 1 AND days_from_today  BETWEEN 15 AND 30  	THEN 'Live 15-30'
														WHEN Active_offer = 1 AND days_from_today  < 15          		THEN 'Live <14'
														
														WHEN Active_offer = 0 AND days_from_today  > 90           		THEN 'Exp +90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 31 AND 90  	THEN 'Exp 31-90'
														WHEN Active_offer = 0 AND days_from_today  BETWEEN 15 AND 30  	THEN 'Exp 15-30'
														WHEN Active_offer = 0 AND days_from_today  < 15          		THEN 'Exp <14'
														ELSE 'No Offer Ever' END
													  END
FROM BASE_TEMP as a
LEFT JOIN  temp_Adsmart_end_of_offer_raw as b
ON a.account_number = b.account_number and a.observation_dt = b.dt 

GO



INSERT INTO BB_CHURN_NEW_BASE_1
SELECT observation_dt
        ,OFFER
        ,product_holding
        ,bb_type
        ,fss
        ,age
        ,bb_tenure
        ,monthyear
        , COUNT(DISTINCT account_number) acct
FROM BASE_TEMP
GROUP BY observation_dt
        ,OFFER
        ,product_holding
        ,bb_type
        ,fss
        ,age
        ,bb_tenure
        ,monthyear
        
        
		
		
		
		
		