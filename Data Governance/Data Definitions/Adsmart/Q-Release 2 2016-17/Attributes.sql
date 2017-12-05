/*  Title       : Adsmart Attributes/ QR2 2016-2017
    Created by  : Jose Pitteloud	
    Date        : 8 September 2016
    Description : This is a sql to build the ADSMART attributes included in the 2nd quarterly release of 2016-2017 
				: The attributes included are:
					1.- HH With Kids
					2.- HH Impression Demand
					3.- HH Revenue Demand
					4.- Update Viewing Propensity
					5.- End of offer - TV
					6.- End of offer - BB
					7.- End of offer - Line rental & Sky talk
					8.- Sky Service App Usage
					9.- Cyclical Offer
					10.- Mobile
					11.- Talk
					12.- Viewing attributes
					
    Modified by : Jose Pitteloud 
    Changes     :

*/


/*		====================	QA		============== 
CREATE TABLE ADSMART_TEST
(   account_number 				VARCHAR (12)
	, HH_Kids					VARCHAR(20)
	, HH_Impression_Demand 		VARCHAR(20)
	, HH Revenue Demand			VARCHAR(20)
	, Viewing Propensity		VARCHAR(20)
	, End of offer - TV			VARCHAR(20)
	, End of offer - BB			VARCHAR(20)
	, End of offer - Comm		VARCHAR(20)
	, Sky Service App Usage		VARCHAR(20)
	, Cyclical Offer			VARCHAR(20)
	, Talk						VARCHAR(20)
)

*/

  
MESSAGE 'Process to build the ADSMART ATTRIBUTES in QR2 2016-2017' type status to client


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

-------------------------------------------------------------------------------
------- 						End_of_offer						-----------
-------------------------------------------------------------------------------
MESSAGE 'End_of_offer_TV Start' type status to client


SELECT 
	o.account_number
	,offer_id
	,offer_dim_description
	,offer_amount
	,s.price
	,(offer_amount / s.price) * 100 AS offer_perc
	, rank() OVER(PARTITION BY b.account_number 			ORDER BY offer_end_dt DESC)      AS rankk_1
INTO #temp_offers	
FROM cust_product_offers AS o
JOIN cust_subscriptions AS s ON o.subscription_id = s.subscription_id
WHERE 
	--offer_status IN ('Active',' Pending Terminated')
	AND offer_end_dt BETWEEN DATEADD(day, -90, GETDATE()) AND DATEADD(day, 60, GETDATE()) 
	AND o.first_activation_dt > '1900-01-01'
	AND x_subscription_sub_type = 'DTV Primary Viewing'
	AND lower(offer_dim_description) NOT LIKE '%price_protection%'
	AND lower(offer_dim_description) NOT LIKE '%vat%'
	AND lower(offer_dim_description) NOT LIKE '%staff%'
	AND lower(offer_dim_description) NOT LIKE '%control%'
	AND lower(offer_dim_description) NOT LIKE '%monitoring%'
	AND lower(offer_dim_description) NOT LIKE '%no charge%'
	AND offer_amount < 0
	AND s.price > 0

COMMIT 
DELETE FROM #temp_offers	 WHERE rankk_1 > 1 
COMMIT
CREATE HG INDEX id1 ON #temp_offers	(account_number)
CREATE HG INDEX id1 ON #temp_offers	(offer_perc)

UPDATE ADSMART 
SET End_of_offer_tv = CASE 	WHEN offer_perc BETWEEN 0 AND 14 	THEN 'Less 15%'
							WHEN offer_perc BETWEEN 15 AND 30 	THEN 'Between 15% and 30%'
							WHEN offer_perc BETWEEN 30 AND 50 	THEN 'Between 30% and 50%'
							WHEN offer_perc > 50				THEN 'More then 50%'
							ELSE 'No offer'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_offers AS b on a.account_number = b.account_number
DROP TABLE #temp_offers

MESSAGE 'End_of_offer_TV END' type status to client
---------------------	BB	 -------------------------------------							
MESSAGE 'End_of_offer_BB Start' type status to client
SELECT 
	o.account_number
	,offer_id
	,offer_dim_description
	,offer_amount
	,s.price
	,(offer_amount / s.price) * 100 AS offer_perc
	, rank() OVER(PARTITION BY b.account_number 			ORDER BY offer_end_dt DESC)      AS rankk_1
INTO #temp_offers	
FROM cust_product_offers AS o
JOIN cust_subscriptions AS s ON o.subscription_id = s.subscription_id
WHERE 
	--offer_status IN ('Active',' Pending Terminated')
	AND offer_end_dt BETWEEN DATEADD(day, -90, GETDATE()) AND DATEADD(day, 60, GETDATE()) 
	AND o.first_activation_dt > '1900-01-01'
	AND x_subscription_sub_type = 'Broadband DSL Line'
	AND lower(offer_dim_description) NOT LIKE '%price_protection%'
	AND lower(offer_dim_description) NOT LIKE '%vat%'
	AND lower(offer_dim_description) NOT LIKE '%staff%'
	AND lower(offer_dim_description) NOT LIKE '%control%'
	AND lower(offer_dim_description) NOT LIKE '%monitoring%'
	AND lower(offer_dim_description) NOT LIKE '%no charge%'
	AND offer_amount < 0
	AND s.price > 0

COMMIT 
DELETE FROM #temp_offers	 WHERE rankk_1 > 1 
COMMIT
CREATE HG INDEX id1 ON #temp_offers	(account_number)
CREATE HG INDEX id1 ON #temp_offers	(offer_perc)

UPDATE ADSMART 
SET End_of_offer_BB = CASE 	WHEN offer_perc BETWEEN 0 AND 14 	THEN 'Less 15%'
							WHEN offer_perc BETWEEN 15 AND 30 	THEN 'Between 15% and 30%'
							WHEN offer_perc BETWEEN 30 AND 50 	THEN 'Between 30% and 50%'
							WHEN offer_perc > 50				THEN 'More then 50%'
							ELSE 'No offer'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_offers AS b on a.account_number = b.account_number
COMMIT
DROP TABLE #temp_offers

MESSAGE 'End_of_offer_BB END' type status to client
---------------------	Comms	 -------------------------------------								
MESSAGE 'End_of_offer_Comm Start' type status to client
SELECT DISTINCT
	o.account_number
INTO #temp_offers_comms
FROM cust_product_offers AS o
JOIN cust_subscriptions AS s ON o.subscription_id = s.subscription_id
WHERE 
	AND offer_end_dt BETWEEN DATEADD(day, -90, GETDATE()) AND DATEADD(day, 60, GETDATE()) 
	AND o.first_activation_dt > '1900-01-01'
	AND x_subscription_sub_type = 'SKY TALK%'
	AND lower(offer_dim_description) NOT LIKE '%price_protection%'
	AND lower(offer_dim_description) NOT LIKE '%vat%'
	AND lower(offer_dim_description) NOT LIKE '%staff%'
	AND lower(offer_dim_description) NOT LIKE '%control%'
	AND lower(offer_dim_description) NOT LIKE '%monitoring%'
	AND lower(offer_dim_description) NOT LIKE '%no charge%'
	AND offer_amount < 0
	AND s.price > 0

COMMIT 

COMMIT
CREATE HG INDEX id1 ON #temp_offers_comms	(account_number)


UPDATE ADSMART 
SET End_of_offer_Comm = CASE 	WHEN b.account_number IS NOT NULL THEN 'Yes'
							ELSE 'No'
							END 
FROM ADSMART AS a 
LEFT JOIN #temp_offers_comms AS b on a.account_number = b.account_number
COMMIT
DROP TABLE #temp_offers_comms	
	
MESSAGE 'End_of_offer_Comm END' type status to client
---------------------	Comms	 -------------------------------------								
MESSAGE 'offer_cyclical Start' type status to client


SELECT account_number
	, subs_year
	, subs_quarter_of_year
	, count(*) hits
INTO #temp_cyclical_1
FROM CUST_CHANGE_ATTEMPT AS a 
JOIN SKY_CALENDAR AS b ON a.created_dt = b.calendar_date
WHERE change_attempt_type = 'CANCELLATION ATTEMPT'
	AND subscription_sub_type = 'DTV Primary Viewing'
	AND created_dt >= DATEADD (year, -3, GETDATE())
COMMIT
CREATE HG INDEX id1 ON #temp_cyclical_1(account_number)
CREATE LF INDEX id2 ON #temp_cyclical_1(subs_year)
CREATE LF INDEX id3 ON #temp_cyclical_1(subs_quarter_of_year)
COMMIT 

	
	
SELECT account_number 
FROM #temp_cyclical_1	