/*  Title       : Adsmart Attributes/ QR2 2016-2017
    Created by  : Jose Pitteloud	
    Date        : 8 September 2016
    Description : This is a sql to build the ADSMART attributes included in the 2nd quarterly release of 2016-2017 
				: The attributes included are:

					1.- HH Impression Demand
					2.- HH Revenue Demand
					3.- End of offer - TV
					4.- End of offer - BB
					5.- End of offer - Line rental & Sky talk
					6.- Sky Service App Usage
					7.- Cyclical Offer
					8.- Mobile
					9.- Viewing attributes
					
    Modified by : Jose Pitteloud 
    Changes     :

*/


/*		====================	QA		============== 
CREATE TABLE ADSMART_TEST2
(   account_number 				VARCHAR (12)
	, cb_key_household BIGINT
	--, HH_Kids					VARCHAR(20)
	, IMPRESSION_DEMAND			VARCHAR(20)
	, REVENUE_DEMAND			VARCHAR(20)
	, NON_SKY_SPORT_VIEWING		VARCHAR(20)
	, SKY_ENDOFFER_TV			VARCHAR(20)
	, SKY_ENDOFFER_BB			VARCHAR(20)
	, SKY_ENDOFFER_TALK_LR		VARCHAR(20)
	, SKYQ						VARCHAR(40)
	, SKY_CYCLICAL				VARCHAR(20)
	, MOBILE_USAGE_SEGMENT		VARCHAR(40)
)

*/

  
MESSAGE 'Process to build the ADSMART ATTRIBUTES in QR2 2016-2017' type status to client

/* OUT OF SCOPE
-------------------------------------------------------------------------------
------- 						HH_kids								-----------
-------------------------------------------------------------------------------
MESSAGE 'HH_Kids' type status to client

DECLARE @mx_dt DATE 
SELECT @mx_dt = MAX(month) from  mckanej.child_in_hh_flags

SELECT 
	account_number 
	, CASE WHEN Child_15_17yr_flag 	= 1 THEN 5
				Child_10_14yr_flag 	= 1 THEN 4 
				Child_6_9yr_flag	= 1 THEN 3
				Child_3_5yr_flag	= 1 THEN 2
				Child_0_2yr_flag	= 1 THEN 1 
				ELSE 0 
				END oldest
INTO #temp_kids
FROM mckanej.child_in_hh_flags
WHERE month = @mx_dt

COMMIT 
CREATE HG INDEX id1 ON #temp_kids (account_number) 
CREATE LF INDEX id2 ON #temp_kids (oldest) 
COMMIT 
GO 
UPDATE ADSMART 
SET HH_Kids = CASE 	WHEN oldest  = 5 THEN '15 to 17 years old'
					WHEN oldest  = 4 THEN '10 to 14 years old'
					WHEN oldest  = 3 THEN '6 to 9 years old'
					WHEN oldest  = 2 THEN '3 to 5 years old'
					WHEN oldest  = 1 THEN '0 to 2 years old'
					ELSE 'No kids in the HH' 
FROM ADSMART AS a 
LEFT JOIN #temp_kids AS b ON a.account_number = b.account_number 
COMMIT 
DROP TABLE #temp_kids
COMMIT 
GO
*/
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
SET SKY_ENDOFFER_TV = CASE 	WHEN offer_perc BETWEEN 0 AND 15 	THEN 'Percent 0-15'
							WHEN offer_perc BETWEEN 16 AND 30 	THEN 'Percent 16-30'
							WHEN offer_perc BETWEEN 31 AND 50 	THEN 'Percent 31-50'
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
GO
---------------------	cyclical	 -------------------------------------								
MESSAGE 'SKY_CYCLICAL Start' type status to client

DECLARE @Q tinyint 
DECLARE @y int
SET @Q = (SELECT QUARTER( DATEADD (quarter, 1, getdate())))
SET @y = (SELECT YEAR(getdate()))

SELECT account_number
	, YEAR(created_dt) 		AS subs_year
	, QUARTER(created_dt) 	AS subs_quarter
	, count(*) hits
INTO #temp_cyclical_1
FROM CUST_CHANGE_ATTEMPT AS a 
WHERE change_attempt_type = 'CANCELLATION ATTEMPT'
	AND subscription_sub_type = 'DTV Primary Viewing'
	AND YEAR(created_dt) IN (@y-1, @y-2)
	AND subs_quarter = @Q
GROUP BY 	
      account_number
	, subs_year
	, subs_quarter
		
COMMIT
CREATE HG INDEX id1 ON #temp_cyclical_1(account_number)
CREATE LF INDEX id2 ON #temp_cyclical_1(subs_year)
CREATE LF INDEX id3 ON #temp_cyclical_1(subs_quarter)
COMMIT 



SELECT DISTINCT a.account_number, 1 dummy 
INTO #temp_cyclical_2 
FROM #temp_cyclical_1 as a 
JOIN #temp_cyclical_1 AS b   ON a.account_number = b.account_number 
							AND a.subs_year = (b.subs_year -1) 
							AND a.subs_quarter = b.subs_quarter
COMMIT 
CREATE UNIQUE HG INDEX Id1 ON #temp_cyclical_2(account_number)
COMMIT 							

UPDATE ADSMART 
SET SKY_CYCLICAL = CASE WHEN b.account_number  IS NOT NULL THEN 'Yes'
							ELSE 'No'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_cyclical_2 AS b on a.account_number = b.account_number
COMMIT							
DROP TABLE #temp_cyclical_1	
DROP TABLE #temp_cyclical_2
	
MESSAGE 'Cyclical_offer END' type status to client
GO

-------------------------------------------------------------------------------
------- 						MOBILE_USAGE_SEGMENT				-----------
-------------------------------------------------------------------------------

UPDATE ADSMART 
SET MOBILE_USAGE_SEGMENT = CASE 	WHEN MOBILE_SEGMENT_PROD_HH_201608 = '1)SUPER USERS' 			THEN 'Super Users'        
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '2)HEAVY USERS' 			THEN 'Heavy Users'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '3)NEXT GEN USERS' 		THEN 'Next Gen Users'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '4)TAPPERS'     			THEN 'Tappers'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '5)DATA FIRST' 			THEN 'Data First'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '6)HEAVY COMMUNICATORS' 	THEN 'Heavy Communicators'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '7)TALKERS'     			THEN 'Talkers'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '8)TEXTERS'	 			THEN 'Texters'
									WHEN MOBILE_SEGMENT_PROD_HH_201608 = '9)DISENGAGED'	 			THEN 'Unconnected'
									ELSE 'Unknown' END 
FROM ADSMART AS a 
JOIN vespa_shared.MOBILE_SEGMENT_PROD AS b ON a.cb_key_household = b.cb_key_household 

go
-------------------------------------------------------------------------------
--------- Non-Sky Sport Viewing					-------------------------------
-------------------------------------------------------------------------------
UPDATE ADSMART
SET NON_SKY_SPORT_VIEWING =  VIEWING_TRAIT_BAND
FROM ADSMART AS a 
JOIN PROMOSMART_TRAITS  AS b ON a.account_number = b.account_number 
WHERE VIEWING_TRAIT_NAME = 'NonSky_Sport_viewing_AQ2_Last90days'

COMMIT 

GO

-------------------------------------------------------------------------------
------- 						SKYQ								-----------
-------------------------------------------------------------------------------
--- Selecting accounts with MS+
SELECT DISTINCT 
	a.account_number
	, prod_latest_q_ms_activation_dt act
    , prod_latest_q_ms_cancellation_dt cancel
	, DATEDIFF (DAY, act	, GETDATE()) AS act_dt
	, DATEDIFF (DAY, cancel	, GETDATE()) AS cancel_dt
	, CASE WHEN DATEDIFF (DAY, acct_first_account_activation_dt,act) <= 2 THEN 1 ELSE 0 END AS new_customer
	, CASE 	WHEN act IS NULL THEN 'Never had Sky Q'
			WHEN PROD_LATEST_Q_MS_STATUS_CODE = 'PC' AND PROD_PREVIOUS_Q_MS_STATUS_CODE IN('AC', 'AB') THEN 'Had Q customer in cancellation period'
			WHEN cancel_dt >= 365 				AND PROD_LATEST_Q_MS_STATUS_CODE <> 'AC' THEN 'Had Q downgraded in last 12+month'
			WHEN cancel_dt BETWEEN 91 AND 364  	AND PROD_LATEST_Q_MS_STATUS_CODE <> 'AC' THEN 'Had Q downgraded in last 4-12 month'
			WHEN cancel_dt BETWEEN 31 AND 91 	AND PROD_LATEST_Q_MS_STATUS_CODE <> 'AC' THEN 'Had Q downgraded in last 2-3 month' 
			WHEN cancel_dt <= 30				AND PROD_LATEST_Q_MS_STATUS_CODE <> 'AC' THEN 'Had Q downgraded in 0-1 month' 
			
			WHEN cancel IS NULL AND new_customer = 1 AND act_dt >= 365 				THEN 'New Q customer 12+ month'
			WHEN cancel IS NULL AND new_customer = 1 AND act_dt BETWEEN 91 AND 364 	THEN 'New Q customer 4-12 month'
			WHEN cancel IS NULL AND new_customer = 1 AND act_dt BETWEEN 31 AND 91 	THEN 'New Q customer 2-3 month'
			WHEN cancel IS NULL AND new_customer = 1 AND act_dt <= 30 				THEN 'New Q customer 0-1 month'

			WHEN cancel IS NULL AND new_customer = 0 AND act_dt >= 365 				THEN 'Upgrade Q customer 12+ month'
			WHEN cancel IS NULL AND new_customer = 0 AND act_dt BETWEEN 91 AND 364 	THEN 'Upgrade Q customer 4-12 month'
			WHEN cancel IS NULL AND new_customer = 0 AND act_dt BETWEEN 31 AND 91 	THEN 'Upgrade Q customer 2-3 month'
			WHEN cancel IS NULL AND new_customer = 0 AND act_dt <= 30 				THEN 'Upgrade Q customer 0-1 month'
			ELSE 'Unknown'
			END 
			AS Q_status 
INTO #temp_MS
FROM CUST_SINGLE_ACCOUNT_VIEW AS a
JOIN ADSMART AS b ON a.account_number = b.account_number
WHERE cust_active_dtv = 1
COMMIT
CREATE UNIQUE HG INDEX id2 ON #temp_MS(account_number)


--- Selecting accounts from Cust_subs_hist that have or had Sky Q bundle
SELECT account_number 
	, effective_from_dt
	, effective_to_dt
	, first_activation_dt
	, current_product_description
	, status_code
	, rank() OVER (PARTITION BY account_number ORDER BY effective_from_dt DESC, effective_to_dt DESC, cb_row_id DESC) AS RANKK
	, CASE WHEN   status_code IN ('AC', 'AB' ) AND effective_to_dt > GETDATE () THEN 1
		   WHEN   status_code IN ('PC' ) AND effective_to_dt > GETDATE () THEN 2 
		   ELSE 0 END 	AS active
	, CAST(0 AS BIT) AS new_q
INTO #temp_Q_bundle	
FROM cust_subs_hist 
WHERE current_product_description LIKE 'Sky Q Bundle%' 
COMMIT 
CREATE LF INDEX id1r ON #temp_Q_bundle(rankk) 

--- Identifyng accounts which 1st subscription was SkyQ bundle
SELECT DISTINCT account_number, 1 dummy 
INTO #temp_Q_new
FROM #temp_Q_bundle 
WHERE first_activation_dt = effective_from_dt
COMMIT
CREATE UNIQUE HG INDEX id1df ON #temp_Q_new(account_number)
COMMIT 

-- Deduping
DELETE FROM #temp_Q_bundle	WHERE RANKK > 1 
CREATE UNIQUE HG INDEX id1g ON #temp_Q_bundle(account_number) 

UPDATE #temp_Q_bundle
SET new_q = 1 
FROM #temp_Q_bundle AS a JOIN #temp_Q_new AS b ON a.account_number = b.account_number
COMMIT 

--- Updating Adsmart table. 1st goes the Sky Bundle condition
UPDATE ADSMART 
SET SKYQ = CASE 	WHEN active = 1 AND new_q = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) >= 365  				THEN 'Upgrade Q customer 12+ month'
					WHEN active = 1 AND new_q = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) BETWEEN 91 AND 364 	THEN 'Upgrade Q customer 4-12 month'
					WHEN active = 1 AND new_q = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) BETWEEN 31 AND 91 	THEN 'Upgrade Q customer 2-3 month'
					WHEN active = 1 AND new_q = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) <= 30 				THEN 'Upgrade Q customer 0-1 month' 
					
					WHEN active = 1 AND new_q = 1 AND DATEDIFF (day, effective_from_dt, GETDATE()) >= 365  				THEN 'New Q customer 12+ month'
					WHEN active = 1 AND new_q = 1 AND DATEDIFF (day, effective_from_dt, GETDATE()) BETWEEN 91 AND 364 	THEN 'New Q customer 4-12 month'
					WHEN active = 1 AND new_q = 1 AND DATEDIFF (day, effective_from_dt, GETDATE()) BETWEEN 31 AND 91 	THEN 'New Q customer 2-3 month'
					WHEN active = 1 AND new_q = 1 AND DATEDIFF (day, effective_from_dt, GETDATE()) <= 30 				THEN 'New Q customer 0-1 month' 
					
					WHEN active = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) >= 365  				THEN 'Had Q downgraded in last 12+month'
					WHEN active = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) BETWEEN 91 AND 364 	THEN 'Had Q downgraded in last 4-12 month'
					WHEN active = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) BETWEEN 31 AND 91 		THEN 'Had Q downgraded in last 2-3 month'
					WHEN active = 0 AND DATEDIFF (day, effective_from_dt, GETDATE()) <= 30 					THEN 'Had Q downgraded in 0-1 month' 
					ELSE SKYQ
					END 
FROM ADSMART AS a 
JOIN #temp_Q_bundle AS b ON a.account_number = b.account_number 

--- Updating Adsmart table. If the customers has an active SkyQ bundle subs it will keep it. If not then is updated with the MS+ status
UPDATE ADSMART 
SET SKYQ = CASE WHEN SKYQ LIKE 'New%' OR  SKYQ LIKE 'Upgrade%' THEN SKYQ ELSE Q_status END 
FROM ADSMART AS a
JOIN #temp_MS AS b ON a.account_number = b.account_number 
COMMIT 

GO 

DROP TABLE #temp_Q_bundle
DROP TABLE #temp_MS
DROP TABLE #temp_Q_new
					
					
-------------------------------------------------------------------------------
------- 						INcome Band							-----------
-------------------------------------------------------------------------------
----- To be added to the SAV update statement


, CASE 	WHEN income_bands  = '< £15,000' 			THEN 'Less than £15000'
		WHEN income_bands  = '£15,000 - £19,999' 	THEN '£15000-£19999'
		WHEN income_bands  = '£20,000 - £29,999' 	THEN '£20000-£29999'
		WHEN income_bands  = '£30,000 - £39,999' 	THEN '£30000-£39999'
		WHEN income_bands  = '£40,000 - £49,999' 	THEN '£40,000-£49999'
		WHEN income_bands  = '£50,000 - £59,999' 	THEN '£50000-£59999'
		WHEN income_bands  = '£60,000 - £69,999' 	THEN '£60,000-£69999'
		WHEN income_bands  = '£70,000 - £99,999' 	THEN '£70000-£99999'
		WHEN income_bands  = '£100,000 - £149,999' 	THEN '£100000-£149999'
		WHEN income_bands  = '£150,000 +' 			THEN '£150000+'
		ELSE  'Unknown'
		END AS income_band
		








		
		
		
		
		

