/************************************
*			Unstable customers		*
************************************/
--


/************************************
*			Debt Level				*
************************************/
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_AB_accounts') AND UPPER(tabletype)='TABLE')
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_debt') 		AND UPPER(tabletype)='TABLE')
			  
SELECT account_number, CAST  (0 AS INT ) AS debt 
INTO SMG_AB_accounts
FROM cust_single_account_view as a
WHERE acct_status_code = 'AB'
COMMIT

CREATE HG INDEX id1 ON SMG_AB accounts(account_number) 
COMMIT 

SELECT distinct missed.account_number
	, SUM(balance_due_amt) AS debt
INTO SMG_debt
FROM cust_bills AS missed
JOIN SMG_AB_accounts as VIQ on missed.account_number = VIQ.account_number
WHERE payment_due_dt <= DATEADD(day,-1,today())
	AND Status = 'Unbilled'

 COMMIT
 
 /************************************
*	Had Offer before downgrade		*
************************************/
-- Line Rental downgrade
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('TEMP_LINE_RENTAL') 	AND UPPER(tabletype)='TABLE')
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_dowgrade_dt') 	AND UPPER(tabletype)='TABLE')
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Offers') 			AND UPPER(tabletype)='TABLE')
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_offer_before_downgrade') 	AND UPPER(tabletype)='TABLE')

SELECT account_number
       ,MAX(effective_to_dt) AS LR_end_date
INTO  TEMP_LINE_RENTAL
FROM  cust_subs_hist
WHERE subscription_sub_type = 'SKY TALK LINE RENTAL'
AND status_code IN ('A','a','R','r','CRQ','crq')
GROUP BY account_number
HAVING DATEDIFF(dd, LR_end_date,TODAY()) <= 365            
GO
-- Create Index
CREATE HG INDEX LINE_RENTAL ON  TEMP_LINE_RENTAL (ACCOUNT_NUMBER)
GO

SELECT COALESCE(a.account_number, b.account_number) AS account_number
	, prod_latest_downgrade_date 			AS DTV_dt
	, broadband_latest_agreement_end_dt 	AS BB_dt
	, prod_latest_skytalk_cancellation_dt 	AS ST_dt
	, LR_end_date		
INTO SMG_dowgrade_dt
FROM CUST_SINGLE_ACCOUNT_VIEW AS a
FULL OUTER JOIN TEMP_LINE_RENTAL AS b on a.account_number = b.account_number
WHERE (prod_latest_downgrade_date >= DATEADD (DAY,-365, getdate()))  -- DTV downgrade
	OR (prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) <=365 -- BB downgrade
	OR (prod_latest_skytalk_cancellation_dt >= DATEADD (DAY,-365, getdate()))  -- Sky talk downgrade
	OR (LR_end_date >= DATEADD (DAY,-365, getdate()))  -- Sky talk downgrade
	
COMMIT
CREATE HG INDEX id1 ON  SMG_dowgrade_dt (ACCOUNT_NUMBER)	
COMMIT

SELECT b.account_number
	, CASE 	WHEN 	x_subscription_type IN ('BROADBAND')														THEN 'BB'
			WHEN 	x_subscription_type IN ('SKY TALK')															THEN 'Sky Talk'
			WHEN 	x_subscription_type LIKE 'ENHANCED' AND x_subscription_sub_type LIKE 'Broadband DSL Line' 	THEN 'BB'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED') 								THEN 'DTV'
			ELSE 'Unknown' 	END 																					AS Offer_type
	, CASE WHEN offer_end_dt >= GETDATE() THEN 1 ELSE 0 END															AS live_offer
	, DATE(offer_end_dt) 																							AS offer_end_date
	, rank() OVER(PARTITION BY b.account_number ORDER BY live_offer DESC, days_from_today,cb_row_id) 				AS rankk_1
	, rank() OVER(PARTITION BY b.account_number, Offer_type ORDER BY live_offer DESC, days_from_today,cb_row_id) 	AS rankk_2
INTO 	SMG_Offers
FROM    cust_product_offers AS CPO  
WHERE   offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
		AND x_subscription_type NOT IN ('MCAFEE')

DELETE FROM temp_Adsmart_end_of_offer_raw WHERE rankk_2 > 1 				-- To keep the latest offer by each offer type 
GO
CREATE HG INDEX id1 ON temp_Adsmart_end_of_offer_raw(account_number)
GO

SELECT 
	  DISTINCT a.account_number 
INTO SMG_offer_before_downgrade
FROM SMG_Offers	AS a
JOIN SMG_dowgrade_dt AS b ON a.account_number = b.account_number
WHERE rankk_2 = 1 
	AND 	(CASE WHEN Offer_type = 'BB' 		THEN offer_end_date END <= broadband_latest_agreement_end_dt
		OR 	 CASE WHEN Offer_type = 'DTV' 		THEN offer_end_date END <= DTV_dt
		OR 	 CASE WHEN Offer_type = 'Sky Talk' 	THEN offer_end_date END <= prod_latest_skytalk_cancellation_dt
		OR 	 CASE WHEN Offer_type = 'Sky Talk' 	THEN offer_end_date END <= LR_end_date)
		

 /***********************************
*		Time since cancellation		*
************************************/
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_time_cancel') 			AND UPPER(tabletype)='TABLE')

SELECT 
	  account_number
	, CASE 	WHEN DTV_dt >= BB_dt  AND DTV_dt >= ST_dt AND DTV_dt >= LR_end_date THEN DTV_dt
			WHEN DTV_dt <= BB_dt  AND BB_dt >=  ST_dt AND BB_dt  >= LR_end_date THEN BB_dt
			WHEN ST_dt  >= DTV_dt AND ST_dt >=  BB_dt AND ST_dt  >= LR_end_date THEN DTV_dt
			WHEN LR_end_date  >= DTV_dt AND LR_end_date >=  BB_dt AND ST_dt  <= LR_end_date THEN LR_end_date
			END AS LATEST_Cancel 
	, CASE 	WHEN LATEST_Cancel  <= 7	THEN 'Last 7 days'
			WHEN LATEST_Cancel  <= 30	THEN 'Last 30 days'
			WHEN LATEST_Cancel  <= 90	THEN 'Last 90 days'
			WHEN LATEST_Cancel  <= 180	THEN 'Last 180 days'
			WHEN LATEST_Cancel  <= 365	THEN 'Last 1 year'
			WHEN LATEST_Cancel  > 365	THEN 'More than 1 year'
			ELSE 'No cancellation'
			END AS time_since_cancellation
INTO SMG_time_cancel
FROM SMG_dowgrade_dt


 /***********************************
*		Same day Downgrades			*
************************************/



 /*******************************************************
*	Unstable customers with Extended Low Price Offers	*
*********************************************************/

 /************************************
*		Sky Q Tenure				*
************************************/

 /***********************************
*			Sky Q Package			*
************************************/

 /***********************************
*			Sky Q Box				*
************************************/








