-------------------------------------------------------------------------------
------- 						End_of_offer						-----------
-------------------------------------------------------------------------------
MESSAGE 'SKY_ENDOFFER_TV Start' type status to client



SELECT DISTINCT
	o.account_number
		,subscription_id
	,offer_id
	,offer_dim_description
	,offer_amount
	,CAST (0 AS FLOAT ) AS price
	,CAST (0 AS FLOAT ) AS offer_perc
	, Active_offer = CASE WHEN offer_status IN  ('Active',' Pending Terminated', 'Blocked')  THEN 1 ELSE 0 END 
	,CASE 	WHEN Active_offer = 1 													THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
			WHEN offer_status = 'Terminated' 										THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
			END AS offer_end_date
	, rank() OVER(PARTITION BY o.account_number 			ORDER BY offer_end_date DESC, cb_row_id DESC)      AS rankk_1
	
INTO #temp_offers	
FROM cust_product_offers AS o
-- JOIN cust_subscriptions AS s ON o.subscription_id = s.subscription_id
WHERE 
	    offer_end_date >= DATEADD(day, -90, GETDATE()) 
	AND o.first_activation_dt > '1900-01-01'
	AND x_subscription_sub_type = 'DTV Primary Viewing'
	AND lower(offer_dim_description) NOT LIKE '%price_protection%'
	AND lower(offer_dim_description) NOT LIKE '%vat%'
	AND lower(offer_dim_description) NOT LIKE '%staff%'
	AND lower(offer_dim_description) NOT LIKE '%control%'
	AND lower(offer_dim_description) NOT LIKE '%monitoring%'
	AND lower(offer_dim_description) NOT LIKE '%no charge%'
	AND offer_amount < 0
	--AND s.price > 0

	
COMMIT 
--- Keeping the most recent offer
DELETE FROM #temp_offers	 WHERE rankk_1 > 1 
--- Keeping only offers that are ending in the next 60 days 
DELETE FROM #temp_offers	 WHERE offer_end_date >=  DATEADD(day, 60, GETDATE()) 
COMMIT
CREATE UNIQUE HG INDEX id1 ON #temp_offers	(account_number)
CREATE HG INDEX id2 ON #temp_offers	(offer_perc)
CREATE HG INDEX id3 ON #temp_offers	(subscription_id)

UPDATE #temp_offers
SET a.price = b.price
   , a.offer_perc = - (a.offer_amount / b.price) * 100 
FROM #temp_offers AS a 
JOIN cust_subscriptions AS b ON a.subscription_id = b.subscription_id
WHERE b.price > 0 

UPDATE ADSMART 
SET SKY_ENDOFFER_TV = CASE 	WHEN offer_perc BETWEEN 0 AND 14 	THEN 'Percent 0-15'
							WHEN offer_perc BETWEEN 15 AND 30 	THEN 'Percent 15-30'
							WHEN offer_perc BETWEEN 30 AND 50 	THEN 'Percent 30-50'
							WHEN offer_perc > 50				THEN 'Percent 50+'
							ELSE 'No'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_offers AS b on a.account_number = b.account_number
DROP TABLE #temp_offers

MESSAGE 'SKY_ENDOFFER_TV END' type status to client
---------------------	BB	 -------------------------------------							
MESSAGE 'SKY_ENDOFFER_BB Start' type status to client
SELECT DISTINCT 
	o.account_number
	, Active_offer = CASE WHEN offer_status IN  ('Active',' Pending Terminated', 'Blocked')  THEN 1 ELSE 0 END 
	,CASE 	WHEN Active_offer = 1 													THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
			WHEN offer_status = 'Terminated' 										THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
			END AS offer_end_date
		, rank() OVER(PARTITION BY o.account_number 			ORDER BY offer_end_date DESC, cb_row_id DESC)      AS rankk_1
INTO #temp_offers	
FROM cust_product_offers AS o
WHERE 
	
	 offer_end_date >=  DATEADD(day, -90, GETDATE())
	AND o.first_activation_dt > '1900-01-01'
	AND x_subscription_sub_type = 'Broadband DSL Line'
	AND lower(offer_dim_description) NOT LIKE '%price_protection%'
	AND lower(offer_dim_description) NOT LIKE '%vat%'
	AND lower(offer_dim_description) NOT LIKE '%staff%'
	AND lower(offer_dim_description) NOT LIKE '%control%'
	AND lower(offer_dim_description) NOT LIKE '%monitoring%'
	AND lower(offer_dim_description) NOT LIKE '%no charge%'
	AND offer_amount < 0


	
COMMIT 
--- Keeping the most recent offer
DELETE FROM #temp_offers	 WHERE rankk_1 > 1 
--- Keeping only offers that are ending in the next 60 days 
DELETE FROM #temp_offers	 WHERE offer_end_date >=  DATEADD(day, 60, GETDATE()) 

COMMIT
COMMIT 
CREATE HG INDEX id1 ON #temp_offers	(account_number)

UPDATE ADSMART 
SET SKY_ENDOFFER_BB =  CASE 	WHEN b.account_number IS NOT NULL THEN 'Yes'
							ELSE 'No'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_offers AS b on a.account_number = b.account_number
COMMIT
DROP TABLE #temp_offers

MESSAGE 'SKY_ENDOFFER_BB END' type status to client
GO 
---------------------	Comms	 -------------------------------------								
MESSAGE 'SKY_ENDOFFER_TALK_LR Start' type status to client
SELECT DISTINCT
	o.account_number
	, Active_offer = CASE WHEN offer_status IN  ('Active',' Pending Terminated', 'Blocked')  THEN 1 ELSE 0 END 
	,CASE 	WHEN Active_offer = 1 													THEN DATE(offer_end_dt) 			-- ACTIVE OFFERS																	
			WHEN offer_status = 'Terminated' 										THEN DATE(STATUS_CHANGE_DATE) 		-- Ended or terminated OFFERS
			END AS offer_end_date
	, rank() OVER(PARTITION BY o.account_number 			ORDER BY offer_end_date DESC)      AS rankk_1
	
INTO #temp_offers_comms
FROM cust_product_offers AS o
WHERE 
	 offer_end_date >=  DATEADD(day, -90, GETDATE())  
	AND o.first_activation_dt > '1900-01-01'
	AND x_subscription_sub_type LIKE 'SKY TALK%'
	AND lower(offer_dim_description) NOT LIKE '%price_protection%'
	AND lower(offer_dim_description) NOT LIKE '%vat%'
	AND lower(offer_dim_description) NOT LIKE '%staff%'
	AND lower(offer_dim_description) NOT LIKE '%control%'
	AND lower(offer_dim_description) NOT LIKE '%monitoring%'
	AND lower(offer_dim_description) NOT LIKE '%no charge%'
	AND offer_amount < 0


COMMIT 
--- Keeping the most recent offer
DELETE FROM #temp_offers	 WHERE rankk_1 > 1 
--- Keeping only offers that are ending in the next 60 days 
DELETE FROM #temp_offers	 WHERE offer_end_date >=  DATEADD(day, 60, GETDATE()) 

COMMIT
CREATE HG INDEX id1 ON #temp_offers_comms	(account_number)

UPDATE ADSMART 
SET SKY_ENDOFFER_TALK_LR = CASE 	WHEN b.account_number IS NOT NULL THEN 'Yes'
							ELSE 'No'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_offers_comms AS b on a.account_number = b.account_number
COMMIT
DROP TABLE #temp_offers_comms	
	
MESSAGE 'End_of_offer_Comm END' type status to client
GO