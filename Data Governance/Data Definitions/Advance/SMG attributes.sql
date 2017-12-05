--------------------------------------------------------------------------------------
------- CREATING THE MAIN TABLE WITH SAM_REGISTRANT ACCOUNTS
--------------------------------------------------------------------------------------
IF EXISTS(SELECT tname FROM syscatalog WHERE creator = user_name() and tabletype = 'TABLE' and upper(tname) = UPPER('SMG_Advance_attributes')) DROP TABLE  SMG_Advance_attributes

SELECT 
	  account_number
	, samprofileid 					
	, cb_key_household					
	, sky_ID_type = CASE WHEN user_type = 'guest' THEN 'Guest'
						 WHEN user_type = 'primary' THEN 'Primary'
						 WHEN user_type = 'secondary' THEN 'Secondary'
						 WHEN user_type = 'userGuest' THEN 'User Guest'
						 WHEN user_type = 'userPrimary' THEN 'User Primary'
						 WHEN user_type = 'userSecondary' THEN 'User Secondary'
						 ELSE 'Unknown' END  			
        ,cast('Unknown' as varchar(10)) 	AS BB_3rd_party_cancel
        ,cast('Unknown' as varchar(30)) 	AS BB_account_status
        ,cast('Unknown' as varchar(10)) 	AS BB_only_customers
        ,cast('Unknown' as varchar(30)) 	as BB_tenure
        ,cast('No offer' as varchar(30)) 	as BB_offer_end 
        ,cast('Unknown' as varchar(20)) 	as debt_level 
        ,cast(NULL as varchar(100)) 		as offer_before_downgrade
		,cast('Unknown' as varchar(100)) 	as time_since_cancellation
        ,cast('Unknown' as varchar(100)) 	as sky_Q_tenure 
        ,cast('Unknown' as varchar(100)) 	as Sky_Q_Package
        ,cast('Unknown' as varchar(50)) 	as Sky_Q_box
        ,cast('Unknown' as varchar(40)) 	as registered_for_sky_go_extra 
        ,cast('No' as varchar(100)) 		as on_demand_usage
        ,cast('Unknown' as varchar(100)) 	as number_of_downgrades
        ,cast('Unknown' as varchar(10)) 	as triple_play_cancel
        ,cast('Unknown' as varchar(20)) 	as nps_score
        ,cast('No' as varchar(10)) 			as Upgraded_to_Sport 
        ,cast('No' as varchar(10)) 			as Upgraded_to_Movies
		,cast('No' as varchar(25)) 			as Upgraded_to_Family
		,cast('Unknown' as varchar(10)) 	as Triple_play_TV_cancel
  
        ,cast('Unknown' as varchar(25)) 	as Call_into_BB_CoE_date_range 
		,cast('Unknown' as varchar(25)) 	as Mailshot_offer_account
		,cast('Unknown' as varchar(25)) 	as Unstable_customers
		,cast(NULL as varchar(50)) 			as Same_day_Downgrades
		,cast('Unknown' as varchar(25)) 	as Engagement_Matrix_Score
		,cast('Unknown' as varchar(25)) 	as Previous_retention_offer
		
		
INTO SMG_Advance_attributes
FROM SAM_REGISTRANT 
WHERE x_user_type in ('Primary', 'Secondary','primary','secondary') 
	AND marked_as_deleted = 'N'
	
COMMIT 	
CREATE HG INDEX id1 ON SMG_Advance_attributes(account_number)
COMMIT 

GO

/************************************
*			3rd party cancel		*
************************************/
--Identify Broadband customers that have joined another provider through a 3rd party cancel
--CUST_CHANGE_ATTEMPT
MESSAGE 'Starting 3rd party cancel ' type status to client

UPDATE SMG_Advance_attributes
SET BB_3rd_party_cancel = 'Yes' 
FROM SMG_Advance_attributes AS a 
JOIN CUST_SUBS_HIST 		AS b ON a.account_number = b.account_number
WHERE effective_from_dt > '2015-01-01'
    AND status_reason	IN ('BB Third Party Cancellation','Competitor - Cost','Going To Other Provider','Virgin',
		'Staying with BT Vision','Going to Virgin','Alternative Product/Package/Offer','BT',
		'Staying with Other Provider','Going to BT Vision')
    AND subscription_sub_type ='Broadband DSL Line'
	AND status_code in ('PO','SC','PC', 'CN','PT','PA','PAX') 
	AND prev_status_code in ('AC','AB','PC')             --Previously ACTIVE
	AND status_code_changed = 'Y'
	AND effective_from_dt != effective_to_dt
 
GO
 
 /*************************************
BB Third Party Cancellation			566451
Competitor - Cost					2565
Virgin								1175
BT									279
Alternative Product/Package/Offer	243
Going To Other Provider				25
Going to BT Vision					5
Going to Virgin						5
Staying with Other Provider			2
**************************************/

/************************************
*	Broadband account status		*
************************************/
MESSAGE 'Broadband account status' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_triple_play') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_triple_play
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_bb_status') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_bb_status
		
--DEFINING current product holding
SELECT account_number
	,MAX(CASE WHEN csh.subscription_sub_type = 'DTV Primary Viewing'
				AND csh.status_code IN ('AC','AB','PC') THEN 1 ELSE 0 END) AS dtv_active
	,max(CASE WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
				AND (csh.status_code = 'A'OR (csh.status_code = 'FBP'AND csh.prev_status_code IN ('PC','A')	)
					OR (csh.status_code = 'RI'AND csh.prev_status_code IN ('FBP','A'))
					OR (csh.status_code = 'PC'AND csh.prev_status_code = 'A')) THEN 1 ELSE 0 END) AS skytalk_active
	,max(CASE WHEN csh.subscription_sub_type = 'SKY TALK LINE RENTAL'
				AND csh.Status_code IN ('A','CRQ','PAX') THEN 1 ELSE 0 END) AS wlr
	,max(CASE WHEN csh.subscription_sub_type = 'Broadband DSL Line'
				AND (csh.status_code IN ('AC','AB')
					OR (csh.status_code = 'PC' AND prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA'))
					OR (csh.status_code = 'CF' AND prev_status_code = 'PC' ) 
					OR (csh.status_code = 'AP' AND sale_type = 'SNS Bulk Migration') ) THEN 1 ELSE 0 END) AS bb_active
	,MAX(CASE WHEN csh.subscription_sub_type = 'DTV Primary Viewing'
				AND csh.status_code IN ('PC') THEN 1 ELSE 0 END) AS dtv_PC
INTO SMG_triple_play
FROM cust_subs_hist AS csh
WHERE effective_from_dt <= TODAY()
	AND effective_to_dt > TODAY()
	AND effective_from_dt != effective_to_dt
GROUP BY account_number

MESSAGE 'SMG_triple_play: '||@@rowcount type status to client	
COMMIT
CREATE HG INDEX id1 ON SMG_triple_play (account_number)
COMMIT

SELECT a.account_number 
	, bb_status = CASE 	WHEN status_code = 'PC' THEN 'Pending Cancel'
						WHEN status_code = 'AC' THEN 'Active'
						WHEN status_code = 'AB' THEN 'Active Block'
						ELSE 'Unknown' END
INTO SMG_bb_status
FROM SMG_triple_play AS a 
JOIN cust_subs_hist AS b ON a.account_number = b.account_number
WHERE bb_active = 1 
	AND b.subscription_sub_type = 'Broadband DSL Line'
	AND status_code IN ('PC' ,'AB', 'AC')
	AND effective_from_dt <= TODAY()
	AND effective_to_dt > TODAY()
	AND effective_from_dt != effective_to_dt
COMMIT 
CREATE HG INDEX ed ON SMG_bb_status(account_number)
COMMIT 

UPDATE SMG_Advance_attributes
SET 	BB_account_status = bb_status
FROM SMG_Advance_attributes AS a
JOIN SMG_bb_status AS b ON a.account_number = b.account_number

DROP TABLE SMG_bb_status
	
MESSAGE 'SMG_triple_play: '||@@rowcount type status to client		
COMMIT

GO
/*********	QA 	*********************
SELECT 
	bb_status
	, count(*) hits 
FROM SMG_bb_status
GROUP BY bb_status


*************************************/

/************************************
*		Triple play cancels	*
************************************/
MESSAGE 'Triple play cancels' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_triple_play_cancel') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_triple_play_cancel
		
		
SELECT 	account_number
		, prod_latest_skytalk_cancellation_dt				AS talk_dt
		, acct_latest_primary_viewing_cancellation_dt 		AS dtv_dt
		, broadband_latest_agreement_end_dt				AS bb_dt
INTO SMG_triple_play_cancel
FROM CUST_SINGLE_ACCOUNT_VIEW
WHERE 	DATEDIFF(DAY, talk_dt, dtv_dt) BETWEEN -31 AND 31
	AND DATEDIFF(DAY, talk_dt, bb_dt ) BETWEEN -31 AND 31
	AND cust_active_dtv = 0
	AND talk_dt IS NOT NULL 
	AND dtv_dt 	IS NOT NULL
	AND bb_dt	IS NOT NULL 
	AND prod_active_broadband_package_desc IS NULL
	
	
CREATE HG INDEX id1 ON  SMG_triple_play_cancel (ACCOUNT_NUMBER)	
COMMIT 
	
UPDATE SMG_Advance_attributes
SET 	triple_play_cancel = CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END
FROM SMG_Advance_attributes AS a
LEFT JOIN SMG_triple_play_cancel AS b ON a.account_number = b.account_number

DROP TABLE SMG_triple_play_cancel
COMMIT 	
MESSAGE 'SMG_triple_play_cancel: '||@@rowcount type status to client	

GO
/*********	QA 	*********************
SELECT 
	count(*) hits 
FROM SMG_triple_play_cancel

A*************************************/


/************************************
*		Broadband only customers	*
************************************/
MESSAGE 'Broadband only customers' type status to client

UPDATE 	SMG_Advance_attributes
SET 	BB_only_customers = CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END 
FROM SMG_Advance_attributes AS a 
LEFT JOIN SMG_triple_play AS b ON a.account_number = b.account_number AND bb_active = 1 AND dtv_active = 0 AND skytalk_active = 0 

COMMIT
MESSAGE 'SMG_bb_only: '||@@rowcount type status to client		

/************************************
*			Broadband Tenure		*
************************************/
MESSAGE 'Broadband Tenure' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Broadband_tenure') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_Broadband_tenure
		
		
SELECT 
	a.account_number 
	, Broadband_tenure = CASE 	WHEN DATEDIFF(day,prod_earliest_broadband_activation_dt, today()) <=  365  THEN 'Less than 1 Year'
			WHEN DATEDIFF(day,prod_earliest_broadband_activation_dt, today()) BETWEEN 366 AND 1095  	THEN '1 - 2 yrs'
			WHEN DATEDIFF(day,prod_earliest_broadband_activation_dt, today()) BETWEEN 1096 AND 1461  	THEN '2 - 3 yrs'
			WHEN DATEDIFF(day,prod_earliest_broadband_activation_dt, today()) BETWEEN 1462 AND 2191  	THEN '3 - 5 yrs'
			WHEN DATEDIFF(day,prod_earliest_broadband_activation_dt, today()) BETWEEN 2191 AND 3652  	THEN '5 - 10 yrs'
            WHEN datediff(day,acct_first_account_activation_dt, today()) > 3652 THEN  '10 yrs+'
			ELSE 'Unknown' END 
INTO SMG_Broadband_tenure
FROM CUST_SINGLE_ACCOUNT_VIEW as a 
JOIN SMG_triple_play as b ON a.account_number = b.account_number
WHERE bb_active = 1
	AND dtv_active = 0  
	AND skytalk_active = 0

CREATE HG INDEX id1 ON  SMG_Broadband_tenure (ACCOUNT_NUMBER)	
COMMIT 
		
UPDATE 	SMG_Advance_attributes
SET 	BB_tenure = COALESCE (Broadband_tenure, 'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_Broadband_tenure	AS b ON a.account_number = b.account_number

DROP TABLE SMG_Broadband_tenure
COMMIT	
MESSAGE 'SMG_Broadband_tenure: '||@@rowcount type status to client	
/*********   QA ********************
	
	SELECT Broadband_tenure, count(*) hits
	FROM SMG_Broadband_tenure
	GROUP BY Broadband_tenure
	
************************************/


/************************************
*		Broadband Offer end date	*
************************************/
MESSAGE 'Broadband Offer end date' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Offers') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_Offers
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_bb_offer_end_dt') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_bb_offer_end_dt		
		
SELECT account_number
	, CASE 	WHEN 	x_subscription_type IN ('BROADBAND')														THEN 'BB'
			WHEN 	x_subscription_type IN ('SKY TALK')															THEN 'Sky Talk'
			WHEN 	x_subscription_type LIKE 'ENHANCED' AND x_subscription_sub_type LIKE 'Broadband DSL Line' 	THEN 'BB'
			WHEN 	x_subscription_type IN ('DTV PACKAGE','A-LA-CARTE','ENHANCED') 								THEN 'DTV'
			ELSE 'Unknown' 	END 																					AS Offer_type
	, CASE WHEN offer_end_dt >= GETDATE() THEN 1 ELSE 0 END															AS live_offer
	, DATE(offer_end_dt) 																							AS offer_end_date
	, ABS(DATEDIFF(dd, offer_end_date, getDATE())) AS days_from_today
	, rank() OVER(PARTITION BY account_number ORDER BY live_offer DESC, days_from_today,cb_row_id) 				AS rankk_1
	, rank() OVER(PARTITION BY account_number, Offer_type ORDER BY live_offer DESC, days_from_today,cb_row_id) 	AS rankk_2
INTO 	SMG_Offers
FROM    cust_product_offers  
WHERE   offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
		AND x_subscription_type NOT IN ('MCAFEE')

MESSAGE 'SMG_Offers: '||@@rowcount type status to client	

COMMIT
DELETE FROM SMG_Offers WHERE rankk_2 > 1 				-- To keep the latest offer by each offer type 
CREATE HG INDEX id1 ON SMG_Offers(account_number)
COMMIT

SELECT 
	account_number
	, Broadband_offer_end = CASE 	WHEN DATEDIFF (DAY, GETDATE(),offer_end_date) <=14 THEN 'Offer ends in next 14 days'
									WHEN DATEDIFF (DAY, GETDATE(),offer_end_date) BETWEEN 15 AND 30 THEN 'Offer ends in next 15-30 days'
									WHEN DATEDIFF (DAY, GETDATE(),offer_end_date) BETWEEN 31 AND 90 THEN 'Offer ends in 31-90 days'
									WHEN DATEDIFF (DAY, GETDATE(),offer_end_date) > 90  THEN 'Offer ends after 90 days'
									ELSE 'No Offer' END 
			
INTO SMG_bb_offer_end_dt
FROM SMG_Offers
WHERE Offer_type = 'BB'
	AND live_offer = 1 

CREATE HG INDEX id1 ON  SMG_bb_offer_end_dt (ACCOUNT_NUMBER)	
COMMIT
	
UPDATE 	SMG_Advance_attributes
SET 	BB_offer_end = COALESCE (Broadband_offer_end, 'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_bb_offer_end_dt	AS b ON a.account_number = b.account_number

DROP TABLE SMG_bb_offer_end_dt
COMMIT	
MESSAGE 'SMG_bb_offer_end_dt: '||@@rowcount type status to client	
GO


/************************************
*			Debt Level				*
************************************/
MESSAGE 'Debt Level' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_AB_accounts') AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_AB_accounts
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_debt') 		AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_debt

		
SELECT account_number
	, CAST  (0 AS INT ) AS debt 
INTO SMG_AB_accounts
FROM cust_single_account_view as a
WHERE acct_status_code = 'AB'

MESSAGE 'SMG_AB_accounts: '||@@rowcount type status to client	

COMMIT
CREATE HG INDEX id1 ON SMG_AB_accounts(account_number) 
COMMIT 

SELECT distinct missed.account_number
	, SUM(balance_due_amt) AS debt
INTO SMG_debt
FROM cust_bills AS missed
JOIN SMG_AB_accounts as VIQ on missed.account_number = VIQ.account_number
WHERE payment_due_dt <= DATEADD(day,-1,today())
	AND Status = 'Unbilled'
GROUP BY missed.account_number

CREATE HG INDEX id1 ON  SMG_debt (debt)	
CREATE HG INDEX id2 ON  SMG_debt (ACCOUNT_NUMBER)	
COMMIT

UPDATE 	SMG_Advance_attributes
SET 	debt_level = CASE 	WHEN debt	= 0 	OR debt IS NULL 	THEN 'Has no debt' 	-- Has no debt
							WHEN debt	<= 50 				THEN 'Under 50 pounds'			-- Under 50 pounds
							WHEN debt BETWEEN 51  AND 100	THEN '51 - 100 pounds'			-- 51 - 100 pounds
							WHEN debt BETWEEN 101 AND 200	THEN '101 - 200 pounds'			-- 101 - 200 pounds
							WHEN debt	>= 201 				THEN 'More than 201 pounds'		-- More than 201 pounds
							ELSE 'Has no debt' END 
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_debt			AS b ON a.account_number = b.account_number








DROP TABLE SMG_debt

MESSAGE 'SMG_debt: '||@@rowcount type status to client	
 COMMIT
 GO
 /************************************
*	Had Offer before downgrade		*
************************************/
MESSAGE 'Had Offer before downgrade' type status to client

-- Line Rental downgrade
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('TEMP_LINE_RENTAL') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE TEMP_LINE_RENTAL
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_dowgrade_dt') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_dowgrade_dt

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_offer_before_downgrade') 	AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_offer_before_downgrade
		
		
SELECT account_number
       ,MAX(effective_to_dt) AS LR_end_date
INTO  TEMP_LINE_RENTAL
FROM  cust_subs_hist
WHERE subscription_sub_type = 'SKY TALK LINE RENTAL'
	AND status_code IN ('A','a','R','r','CRQ','crq')
	AND effective_to_dt <= TODAY()
GROUP BY account_number
HAVING DATEDIFF(dd, LR_end_date,TODAY()) <= 365            

MESSAGE 'TEMP_LINE_RENTAL: '||@@rowcount type status to client	
COMMIT
-- Create Index
CREATE HG INDEX LINE_RENTAL ON  TEMP_LINE_RENTAL (ACCOUNT_NUMBER)
COMMIT

SELECT COALESCE(a.account_number, b.account_number) AS account_number
	, CAST(COALESCE(DATE(prod_latest_downgrade_date), '1970-01-01') AS DATE)			AS DTV_dt
	, CAST(COALESCE(DATE(broadband_latest_agreement_end_dt), '1970-01-01') AS DATE)	AS BB_dt
	, CAST(COALESCE(DATE(prod_latest_skytalk_cancellation_dt), '1970-01-01') AS DATE)AS ST_dt
	, CAST(COALESCE(DATE(LR_end_date), '1970-01-01')	AS DATE)						AS LR_end_date
INTO SMG_dowgrade_dt
FROM CUST_SINGLE_ACCOUNT_VIEW AS a
FULL OUTER JOIN TEMP_LINE_RENTAL AS b on a.account_number = b.account_number
WHERE (prod_latest_downgrade_date >= DATEADD (DAY,-365, getdate()))  -- DTV downgrade
	OR (prod_active_broadband_package_desc IS NULL AND DATEDIFF(dd,broadband_latest_agreement_end_dt,TODAY()) <=365 )-- BB downgrade
	OR (prod_latest_skytalk_cancellation_dt >= DATEADD (DAY,-365, getdate()))  -- Sky talk downgrade
	OR (LR_end_date >= DATEADD (DAY,-365, getdate()) AND LR_end_date <= TODAY())  -- Sky talk downgrade

MESSAGE 'SMG_dowgrade_dt: '||@@rowcount type status to client		
COMMIT
CREATE HG INDEX id1 ON  SMG_dowgrade_dt (ACCOUNT_NUMBER)	
COMMIT

COMMIT 

SELECT 
	  DISTINCT a.account_number, 1 AS dummy
INTO SMG_offer_before_downgrade
FROM SMG_Offers	AS a
JOIN SMG_dowgrade_dt AS b ON a.account_number = b.account_number
WHERE rankk_2 = 1 
	AND 	(CASE WHEN Offer_type = 'BB' 		THEN offer_end_date END <= BB_dt
		OR 	 CASE WHEN Offer_type = 'DTV' 		THEN offer_end_date END <= DTV_dt
		OR 	 CASE WHEN Offer_type = 'Sky Talk' 	THEN offer_end_date END <= ST_dt
		OR 	 CASE WHEN Offer_type = 'Sky Talk' 	THEN offer_end_date END <= LR_end_date)

COMMIT 

CREATE HG INDEX id1 ON  SMG_offer_before_downgrade (ACCOUNT_NUMBER)	

UPDATE 	SMG_Advance_attributes
SET 	offer_before_downgrade = CASE WHEN dummy = 1 THEN 'Yes' ELSE 'Unknown' END
FROM 	SMG_Advance_attributes 			AS a 
LEFT JOIN SMG_offer_before_downgrade 	AS b ON a.account_number = b.account_number

DROP TABLE SMG_offer_before_downgrade	
COMMIT 	

MESSAGE 'SMG_offer_before_downgrade: '||@@rowcount type status to client				
GO
 /***********************************
*		Time since cancellation		*
************************************/
MESSAGE 'Time since cancellation' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_time_cancel') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_time_cancel
		
		
SELECT 
	  account_number
	, CAST(COALESCE(DATE(prod_latest_downgrade_date), '1970-01-01') AS DATE)			AS DTV_dt
	, CAST(COALESCE(DATE(broadband_latest_agreement_end_dt), '1970-01-01') AS DATE)		AS BB_dt
	, CAST(COALESCE(DATE(prod_latest_skytalk_cancellation_dt), '1970-01-01') AS DATE)	AS ST_dt 
	, CASE 	WHEN DTV_dt >= BB_dt  AND DTV_dt >= ST_dt THEN DTV_dt
			WHEN DTV_dt <= BB_dt  AND BB_dt >=  ST_dt THEN BB_dt
			WHEN ST_dt  >= DTV_dt AND ST_dt >=  BB_dt THEN ST_dt
			END AS LATEST_Cancel 
	, CASE 	WHEN DATEDIFF (DAY, LATEST_Cancel,TODAY())  <= 7	THEN 'Last 7 days'
			WHEN DATEDIFF (DAY, LATEST_Cancel,TODAY())  <= 30	THEN 'Last 30 days'
			WHEN DATEDIFF (DAY, LATEST_Cancel,TODAY())  <= 90	THEN 'Last 90 days'
			WHEN DATEDIFF (DAY, LATEST_Cancel,TODAY())  <= 180	THEN 'Last 180 days'
			WHEN DATEDIFF (DAY, LATEST_Cancel,TODAY())  <= 365	THEN 'Last 1 year'
			WHEN DATEDIFF (DAY, LATEST_Cancel,TODAY())  > 365	THEN 'More than 1 year'
			ELSE 'No cancellation'
			END AS time_since_cancellation
INTO SMG_time_cancel
FROM CUST_SINGLE_ACCOUNT_VIEW


MESSAGE 'SMG_time_cancel: '||@@rowcount type status to client	

CREATE HG INDEX id1 ON  SMG_time_cancel (ACCOUNT_NUMBER)	
COMMIT 

UPDATE 	SMG_Advance_attributes
SET 	a.time_since_cancellation = COALESCE(b.time_since_cancellation,'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_time_cancel		AS b ON a.account_number = b.account_number

DROP TABLE SMG_time_cancel
COMMIT 
GO
 
 /************************************
*		Sky Q Tenure				*
************************************/
MESSAGE 'Sky Q Tenure' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Sky_Q_tenure') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_Sky_Q_tenure
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('STB_PRE_REGISTRATION_raw') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE STB_PRE_REGISTRATION_raw
		
SELECT account_number
		, CASE WHEN registration_status = 'Registered' THEN 'YES'
				WHEN registration_status = 'Validated' THEN 'YES'
				WHEN registration_status = 'Deregistered' THEN 'NO' END as STB_PRE_REGISTRATION
INTO STB_PRE_REGISTRATION_raw
FROM ETHAN_REGISTRATIONS_UK_CUSTOMERS
WHERE STB_PRE_REGISTRATION = 'YES'
GROUP BY account_number
		, STB_PRE_REGISTRATION		

COMMIT 
CREATE HG INDEX id1 ON  STB_PRE_REGISTRATION_raw (ACCOUNT_NUMBER)	
COMMIT 

		
UPDATE 	SMG_Advance_attributes
SET 	a.sky_Q_tenure = 'Preactive'
FROM 	SMG_Advance_attributes 	AS a 
JOIN STB_PRE_REGISTRATION_raw		AS b ON a.account_number = b.account_number
		
SELECT account_number
        , max(effective_from_dt) mx_dt
        , CASE 	WHEN mx_dt >= DATEADD ( MONTH, -6 , today()) THEN '0-6 months from activation'
				WHEN mx_dt BETWEEN DATEADD ( MONTH, -12 , today()) AND DATEADD ( MONTH, -6 , today()) THEN '7-12 months from activation'
				WHEN mx_dt BETWEEN DATEADD ( MONTH, -18 , today()) AND DATEADD ( MONTH, -13 , today()) THEN '13-18 months from activation'
				WHEN mx_dt < DATEADD ( MONTH, -18 , today()) THEN '18+ months from activation'
				ELSE 'Unknown' END AS tenure
INTO SMG_Sky_Q_tenure
FROM CUST_SUBS_HIST
WHERE current_product_description LIKE 'Sky Q%'
	AND effective_to_dt > TODAY()
	AND status_code  in ('AC','PC','AB')
GROUP BY  ACCOUNT_number 


CREATE HG INDEX id1 ON  SMG_Sky_Q_tenure (ACCOUNT_NUMBER)	
COMMIT 

UPDATE 	SMG_Advance_attributes
SET 	a.sky_Q_tenure = tenure
FROM 	SMG_Advance_attributes 	AS a 
JOIN SMG_Sky_Q_tenure		AS b ON a.account_number = b.account_number

DROP TABLE STB_PRE_REGISTRATION_raw
DROP TABLE SMG_Sky_Q_tenure
COMMIT 

/*	Preactive
	18+ months from activation
*/
go
 /***********************************
*			Sky Q Package			*
************************************/
MESSAGE 'Sky Q Package' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Sky_Q_Package') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_Sky_Q_Package			
/*	HD+
	MS+
	Unknown
*/
SELECT account_number
	, CASE WHEN prod_latest_ms_plus_status_code in ('AC','PC','AB') THEN 'MS+'
		ELSE 'HD+'		END 		AS 	Sky_Q_Package			
INTO SMG_Sky_Q_Package			
from CUST_SINGLE_ACCOUNT_VIEW
where cust_active_dtv = 1
and prod_latest_entitlement_product_desc like 'Sky Q Bundle%'

CREATE HG INDEX id1 ON  SMG_Sky_Q_Package (ACCOUNT_NUMBER)	
COMMIT  

UPDATE 	SMG_Advance_attributes
SET 	a.Sky_Q_Package			 = COALESCE(b.Sky_Q_Package,'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_Sky_Q_Package 		AS b ON a.account_number = b.account_number

DROP TABLE SMG_Sky_Q_Package			
COMMIT 
go
 /***********************************
*			Sky Q Box				*
************************************/
MESSAGE 'Sky Q Package' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_Sky_q_box') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_Sky_q_box

SELECT 		
	account_number
	, CASE 	WHEN x_description = 'Sky Q' 		THEN 'Q Box'
			WHEN x_description = 'Sky Q Silver' THEN 'Q Silver'
			ELSE 'Unknown' END AS sky_q_box
INTO  SMG_Sky_q_box
FROM CUST_set_top_box
WHERE x_description IN ('Sky Q Silver', 'Sky Q')
	AND active_box_flag = 'Y'
	AND box_replaced_dt = '9999-09-09'

CREATE HG INDEX id1 ON  SMG_Sky_q_box (ACCOUNT_NUMBER)	
COMMIT 	

UPDATE 	SMG_Advance_attributes
SET 	a.Sky_Q_box	= COALESCE(b.sky_q_box, 'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_Sky_Q_box			AS b ON a.account_number = b.account_number

DROP TABLE SMG_Sky_Q_box

COMMIT 
/*
	Q Silver
	Q Box
	Unknown
*/
go

 /***********************************
*	Registered for Sky Go Extra		*
************************************/
MESSAGE 'Sky Go Extra' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_sky_go_extra') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_sky_go_extra

SELECT 
	account_number
	, CASE WHEN prod_latest_sky_go_extra_status_code IN ('AC','AB','PC') THEN 'Yes'
        WHEN prod_first_sky_go_extra_activation_dt IS NULL THEN 'No'
		ELSE 'No' END AS sky_go_extra
INTO SMG_sky_go_extra		
FROM  CUST_SINGLE_ACCOUNT_VIEW AS sav
WHERE sav.account_number <> '99999999999999'
    AND sav.account_number not like '%.%'
	
CREATE HG INDEX id1 ON  SMG_sky_go_extra (ACCOUNT_NUMBER)	
COMMIT 	
	
	
UPDATE 	SMG_Advance_attributes
SET 	a.registered_for_sky_go_extra	= COALESCE(b.sky_go_extra, 'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_sky_go_extra			AS b ON a.account_number = b.account_number

DROP TABLE SMG_sky_go_extra
COMMIT 
  go  
 /***********************************
*		On Demand Usage				*
************************************/
MESSAGE 'On Demand Usage' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_on_demand_usage') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_on_demand_usage

SELECT 
	a.account_number 
	, COUNT(*)  downloads
	, CASE 	WHEN  downloads <= 4 			THEN 'More than 1'
			WHEN  downloads BETWEEN 5 AND 9 THEN 'More than 5'
			WHEN  downloads >= 10 			THEN 'More than 10'
			ELSE 'Not used'
			END on_demand_usage
INTO SMG_on_demand_usage
FROM SMG_Advance_attributes AS a 
JOIN CUST_ANYTIME_PLUS_DOWNLOADS AS b ON a.account_number = b.account_number
WHERE download_type = 'M'
	AND last_modified_dt >= DATEADD(MONTH, -12, TODAY())
GROUP BY a.account_number	

CREATE HG INDEX id1 ON  SMG_on_demand_usage (ACCOUNT_NUMBER)	
COMMIT 	
	
UPDATE 	SMG_Advance_attributes
SET 	a.on_demand_usage	= COALESCE(b.on_demand_usage, 'No')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_on_demand_usage			AS b ON a.account_number = b.account_number

DROP TABLE SMG_on_demand_usage		
COMMIT 

go		
 /***********************************
*		Number of downgrades		*
************************************/

UPDATE 	SMG_Advance_attributes
SET 	a.number_of_downgrades	= CASE 	WHEN b.prod_count_of_rev_downgrades_last_24_mnth < 2 THEN 'Less than 2'
										WHEN b.prod_count_of_rev_downgrades_last_24_mnth BETWEEN 2 AND 3 THEN '2 to 3 times'
										WHEN b.prod_count_of_rev_downgrades_last_24_mnth > 3 THEN '3+'
										ELSE 'Unknown' END 
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN CUST_SINGLE_ACCOUNT_VIEW			AS b ON  a.account_number = b.account_number
COMMIT 

 /***********************************
*			NPS Score				*
************************************/
MESSAGE 'NPS Score' type status to client

IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('SMG_NPS_raw') 			AND UPPER(tabletype)='TABLE')
        DROP TABLE SMG_NPS_raw


SELECT account_number
        , latest_nps_score
        , rankk = rank() OVER (PARTITION BY account_number ORDER BY cb_data_date DESC, latest_nps_capture_date DESC)
		, CAST(NULL as VARCHAR (20) ) AS score
INTO SMG_NPS_raw
FROM CUST_NPS_SUMMARY
COMMIT
DELETE FROM SMG_NPS_raw WHERE rankk >1
COMMIT

CREATE HG INDEX id1 ON  SMG_NPS_raw (ACCOUNT_NUMBER)	
COMMIT 	
	




UPDATE 	SMG_Advance_attributes
SET 	nps_score	= COALESCE(CASE 	WHEN latest_nps_score <=  2 THEN 'Super-Detractors'
					WHEN latest_nps_score BETWEEN  3 AND 6 THEN 'Detractors'
					WHEN latest_nps_score BETWEEN  7 AND 8 THEN 'Passives'
					WHEN latest_nps_score BETWEEN  9 AND 10 THEN 'Promoters'
				ELSE 'Unknown' END , 'Unknown')
FROM 	SMG_Advance_attributes 	AS a 
LEFT JOIN SMG_NPS_raw			AS b ON  a.account_number = b.account_number 

DROP TABLE SMG_NPS_raw
GO 
 /***********************************
*		Upgraded to Sport			*
************************************/
--To be able to target customers who didn’t have any sports package and have upgraded to Sports and/or dual sports in the last 30 days

MESSAGE 'Populate field SPORTS_STATUS - START' type status to client

IF EXISTS( SELECT tname FROM syscatalog WHERE creator= user_name()  AND UPPER(tname)='TEMP_SPORTS' AND UPPER(tabletype)='TABLE')
	drop table  TEMP_SPORTS
IF EXISTS( SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)='TEMP_SPORTS_PREMIUMS' AND UPPER(tabletype)='TABLE')
	drop table  TEMP_SPORTS_PREMIUMS
IF EXISTS( SELECT tname FROM syscatalog WHERE creator= user_name()  AND UPPER(tname)='TEMP_SPORTS_DG_DATE' AND UPPER(tabletype)='TABLE') 
	drop table  TEMP_SPORTS_DG_DATE


SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_SPORTS IS NULL THEN 0 ELSE ncel.prem_SPORTS END AS current_SPORTS_premiums
         ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_SPORTS
FROM  cust_subs_hist 			AS csh
join  cust_entitlement_lookup 	AS ncel ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
		AND subscription_sub_type = 'DTV Primary Viewing'
		AND status_code IN ('AC','PC','AB')   -- Active records
		AND csh.currency_code = 'EUR' -- Exclude Republic of Ireland
		AND csh.account_number IS NOT NULL

COMMIT
CREATE INDEX indx_SPORTS ON  TEMP_SPORTS(account_number)

SELECT Account_number
       ,MAX(current_SPORTS_premiums) AS HIGHEST
       ,MIN(current_SPORTS_premiums) AS LOWEST
INTO  TEMP_SPORTS_PREMIUMS
FROM  TEMP_SPORTS
GROUP BY Account_number
COMMIT
CREATE INDEX indx_SPORTS1 ON  TEMP_SPORTS_PREMIUMS(account_number)
COMMIT
  
--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_end_date
INTO  TEMP_SPORTS_DG_DATE
FROM  TEMP_SPORTS
WHERE current_SPORTS_premiums = 0
GROUP BY Account_number
COMMIT
CREATE INDEX indx_SPORTS2 ON  TEMP_SPORTS_DG_DATE(account_number)


UPDATE SMG_Advance_attributes 
SET Upgraded_to_Sport = CASE WHEN current_SPORTS_premiums > 0 AND end_date BETWEEN DATEADD(DAY,-30,TODAY()) AND TODAY()  THEN 'Yes'
                         ELSE 'No'
                    END
FROM SMG_Advance_attributes AS AD 
JOIN  TEMP_SPORTS_PREMIUMS 		AS TMP 	ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER 
LEFT JOIN  TEMP_SPORTS_DG_DATE 	AS TMDD ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_SPORTS 			AS TM 	ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1


DROP TABLE  TEMP_SPORTS
DROP TABLE  TEMP_SPORTS_PREMIUMS
DROP TABLE  TEMP_SPORTS_DG_DATE


 /***********************************
*		Upgraded to Movies			*
************************************/
MESSAGE 'Populate field MOVIES_STATUS - START' type status to client

IF EXISTS( SELECT tname FROM syscatalog WHERE creator= user_name()  AND UPPER(tname)='TEMP_MOVIES' AND UPPER(tabletype)='TABLE')
    drop table  TEMP_MOVIES
IF EXISTS( SELECT tname FROM syscatalog WHERE creator= user_name()  AND UPPER(tname)='TEMP_MOVIES_PREMIUMS' AND UPPER(tabletype)='TABLE')
    drop table  TEMP_MOVIES_PREMIUMS
IF EXISTS( SELECT tname FROM syscatalog WHERE creator= user_name()  AND UPPER(tname)='TEMP_MOVIES_DG_DATE' AND UPPER(tabletype)='TABLE')
        drop table  TEMP_MOVIES_DG_DATE



SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,CASE WHEN ncel.prem_movies IS NULL THEN 0 ELSE ncel.prem_movies END AS current_movies_premiums
        ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_MOVIES
FROM  	cust_subs_hist 			AS csh
JOIN 	cust_entitlement_lookup AS ncel  ON csh.current_short_description = ncel.short_description
WHERE csh.effective_to_dt > csh.effective_from_dt
		AND subscription_sub_type = 'DTV Primary Viewing'
		AND status_code IN ('AC','PC','AB')
		AND csh.currency_code = 'GBP'
		AND csh.account_number IS NOT NULL
COMMIT
-- Create Index
CREATE INDEX indx_MOVIES ON  TEMP_MOVIES(account_number)

--WORKOUT IF PREMIUM EVER CHANGED
SELECT Account_number
       ,MAX(current_movies_premiums) AS HIGHEST
       ,MIN(current_movies_premiums) AS LOWEST
INTO  TEMP_MOVIES_PREMIUMS
FROM  TEMP_MOVIES
GROUP BY Account_number
COMMIT 
-- Create Index
CREATE INDEX indx_MOVIES1 ON  TEMP_MOVIES_PREMIUMS(account_number)
COMMIT

--WORK OUT DOWNGRADE DATE
SELECT Account_number
       ,MAX(end_date)AS premium_start_date
INTO  TEMP_MOVIES_DG_DATE
FROM  TEMP_MOVIES
WHERE current_movies_premiums = 0
GROUP BY Account_number
COMMIT
CREATE INDEX indx_MOVIES2 ON  TEMP_MOVIES_DG_DATE(account_number)


-- Update ADSMART Table
UPDATE SMG_Advance_attributes 
SET Upgraded_to_Movies  = CASE 	WHEN current_movies_premiums > 0 AND end_date BETWEEN DATEADD(DAY, -30, TODAY()) AND TODAY() THEN 'Yes'
							ELSE 'No' END
FROM SMG_Advance_attributes 						AS AD 
JOIN  TEMP_MOVIES_PREMIUMS 		AS TMP  ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES_DG_DATE 	AS TMDD ON AD.ACCOUNT_NUMBER = TMDD.ACCOUNT_NUMBER
LEFT JOIN  TEMP_MOVIES 			AS TM   ON AD.ACCOUNT_NUMBER = TM.ACCOUNT_NUMBER
WHERE sorting_rank = 1
COMMIT

DROP TABLE  TEMP_MOVIES
DROP TABLE  TEMP_MOVIES_PREMIUMS
DROP TABLE  TEMP_MOVIES_DG_DATE
GO

MESSAGE 'Populate field MOVIES_STATUS - END' type status to client


 /***********************************
*		Upgraded to Family			*
************************************/

SELECT  csh.Account_number
        ,csh.effective_from_dt AS start_date
        ,csh.effective_to_dt AS end_date
        ,rank() over (PARTITION BY csh.account_number ORDER BY end_date DESC, start_date DESC, csh.status_start_dt DESC, csh.cb_row_id DESC) AS sorting_rank
INTO  TEMP_FAMILY
FROM  	cust_subs_hist 			AS csh
WHERE csh.effective_to_dt > csh.effective_from_dt
		AND start_date >= DATEADD (DAY, -30, TODAY())
		AND LOWER(previous_description) NOT LIKE '%family%'
		AND LOWER(current_product_description) LIKE '%family%'
		AND subscription_sub_type = 'DTV Primary Viewing'
		AND status_code IN ('AC','PC','AB')
		AND csh.currency_code = 'GBP'
		AND csh.account_number IS NOT NULL
COMMIT
-- Create Index
CREATE INDEX indx_MOVIES ON  TEMP_FAMILY(account_number)
COMMIT

UPDATE SMG_Advance_attributes 
SET Upgraded_to_Family  = CASE 	WHEN TMP.ACCOUNT_NUMBER IS NOT NULL THEN 'Yes'
							ELSE 'No' END
FROM SMG_Advance_attributes 						AS AD 
LEFT JOIN  TEMP_FAMILY 		AS TMP  ON AD.ACCOUNT_NUMBER = TMP.ACCOUNT_NUMBER
WHERE sorting_rank = 1
COMMIT
DROP TABLE  TEMP_FAMILY
GO

/************************************
*		Triple play TV cancel		*
************************************/
MESSAGE 'Triple play TV cancel' type status to client

UPDATE SMG_Advance_attributes 
SET Triple_play_TV_cancel = CASE WHEN b.account_number IS NOT NULL THEN 'Yes' ELSE 'No' END
FROM SMG_Advance_attributes AS a
LEFT JOIN SMG_triple_play AS b on a.account_number = b.account_number
WHERE dtv_PC = 1  
	AND bb_active = 1 
	AND skytalk_active = 1 
	AND dtv_active = 1 

	COMMIT	
GO


/***********************************
*	Engagement_Matrix_Score			*
************************************/
DECLARE @m VARCHAR (6)
SELECT @m = MAX(observation_month)
FROM zubizaa.M004_ENGAGEMENT_SCORE_H
COMMIT 

UPDATE SMG_Advance_attributes
SET Engagement_Matrix_Score = engagement_segment
FROM SMG_Advance_attributes AS a 
JOIN zubizaa.M004_ENGAGEMENT_SCORE_H AS b ON a.account_number = b.account_number AND observation_month = @m

COMMIT 



 /*******************************************************
*	Unstable customers with Extended Low Price Offers	*
*********************************************************/

 /***********************************
*	Previous retention offer		*
************************************/

/************************************
*			Unstable customers		*
************************************/
--
/* ***********************************
*		Mailshot offer account		*
************************************/

SELECT account_number
	, 'Yes' AS Mailshot_offer_account
INTO Mailshot_offer_account_table
FROM Cust_single_account_view
WHERE cust_postal_mail_allowed = 'Y'
	AND account_number IS NOT NULL
-- 25460538 Row(s) affected

UPDATE SMG_Advance_attributes a
SET a.Mailshot_offer_account = coalesce(b.Mailshot_offer_account, 'No')
FROM SMG_Advance_attributes a
LEFT JOIN Mailshot_offer_account_table AS b ON a.account_number = b.account_number

COMMIT

DROP TABLE Mailshot_offer_account_table

/* 

Mailshot_offer_account	count()
No						4,508,160
Yes						17,161,792

*/


/* ***********************************
*	Call into BB CoE date range		*
************************************/

MESSAGE 'Call into BB CoE date range' type status to client
IF EXISTS(SELECT tname FROM syscatalog WHERE creator= user_name() AND UPPER(tname)=UPPER('Call_into_BB_CoE_date_range') AND UPPER(tabletype)='TABLE')
        DROP TABLE Call_into_BB_CoE_date_range

SELECT account_number
	, bb_active
	, CASE WHEN bb_active = 1
			AND today() - last_call_date < 15 THEN 'Last 14 days' WHEN bb_active = 1
			AND today() - last_call_date BETWEEN 15
				AND 90 THEN 'Last 15 - 90 days' WHEN bb_active = 1
			AND today() - last_call_date > 90 THEN 'More than 90 days' WHEN bb_active = 0 THEN 'Unknown' ELSE 'No calls made' END AS Call_into_BB_CoE_date_range
INTO Call_into_BB_CoE_date_range_table
FROM (
	SELECT c.account_number
		, max(call_date) AS last_call_date
		, bb_active
	FROM SMG_triple_play c
	LEFT JOIN cust_inbound_calls a ON c.account_number = a.account_number
	LEFT JOIN (
		SELECT *
		FROM SCT_lookup
		WHERE lower(sg3) = 'sg3_salval_bbcoe'
		) b ON a.service_call_type = b.service_call_type
	WHERE c.account_number IS NOT NULL
	GROUP BY c.account_number
		, bb_active
	) y
	
	COMMIT
GO 

UPDATE SMG_Advance_attributes
SET a.Call_into_BB_CoE_date_range = COALESCE(b.Call_into_BB_CoE_date_range, 'No calls made')
FROM SMG_Advance_attributes a
LEFT JOIN Call_into_BB_CoE_date_range_table AS b on a.account_number = b.account_number

	COMMIT	
GO
DROP TABLE SMG_triple_play
DROP TABLE IF EXISTS Call_into_BB_CoE_date_range_table


/*
Call_into_BB_CoE_date_range	bb_active	count()
Last 14 days				1			496,251
Last 15 - 90 days			1			1,536,349
More than 90 days			1			3,480,858
No calls made				1			393,072
Unknown						0			24,005,525
*/


/***********************************
*		Same day Downgrades			*
************************************/

-- Selecting the latest downgrade using SMG_dowgrade_dt table from above
SELECT account_number
	, CASE 	WHEN DTV_dt >= BB_dt  AND DTV_dt >= ST_dt THEN DTV_dt
			WHEN DTV_dt <= BB_dt  AND BB_dt >=  ST_dt THEN BB_dt
			WHEN ST_dt  >= DTV_dt AND ST_dt >=  BB_dt THEN ST_dt
			END AS LATEST_Cancel 
	, CASE 	WHEN DTV_dt >= BB_dt  AND DTV_dt >= ST_dt THEN 'TV'
			WHEN DTV_dt <= BB_dt  AND BB_dt >=  ST_dt THEN 'BB'
			WHEN ST_dt  >= DTV_dt AND ST_dt >=  BB_dt THEN 'ST'
			END AS LATEST_Cancel_desc
INTO SMG_last_dowgrade
FROM SMG_dowgrade_dt

COMMIT 
CREATE HG INDEX id1 ON SMG_last_dowgrade(account_number)
CREATE LF INDEX id2 ON SMG_last_dowgrade(LATEST_Cancel_desc)
COMMIT

-- Listing all the statuses that ended or started the day of the downgrade by subscription_sub_type
SELECT 
	a.account_number 
	, status_code
	, subscription_sub_type
	, RANK () OVER (PARTITION BY a.account_number, subscription_sub_type ORDER BY  effective_from_dt DESC , effective_from_datetime DESC)  as rankk
	, RANK () OVER (PARTITION BY a.account_number, subscription_sub_type ORDER BY  effective_from_dt , effective_from_datetime )  as rankk2
INTO same_day_acct
FROM SMG_last_dowgrade 	AS a 
JOIN CUST_SUBS_HIST 	AS b ON a.account_number = b.account_number 
WHERE DATEDIFF(DAY,effective_from_dt,LATEST_Cancel) BETWEEN -1 AND 1
	 OR DATEDIFF(DAY,effective_to_dt,LATEST_Cancel) BETWEEN -1 AND 1
COMMIT 
CREATE HG INDEX id1x ON same_day_acct(account_number)

--DELETING downgrades that are still active after the downgrade request with some conditions (pending or blocked)
-- or were conditioned already before the cancelation (Pending, Blocked)
SELECT DISTINCT account_number, subscription_sub_type
INTO #delete_acct
FROM same_day_acct
WHERE (rankk =1 OR rankk2 = 1) 			-- Initial or final status
	and status_code in ('AB','PC','BL','EB','EPC','PAX')		-- 
COMMIT
CREATE HG INDEX id1x ON #delete_acct(account_number)
DELETE FROM  same_day_acct 
FROM same_day_acct AS a
JOIN  #delete_acct AS b ON a.account_number = b.account_number AND a.subscription_sub_type = b.subscription_sub_type

DROP TABLE #delete_acct
--- Selecting TV downgrades
SELECT DISTINCT account_number, 'Yes with TV subscription' AS status
INTO same_day_tv
FROM same_day_acct 
WHERE subscription_sub_type = 'DTV Primary Viewing'
COMMIT
CREATE HG INDEX id2 ON same_day_tv(account_number)


--- Updating all the downgrades
UPDATE SMG_Advance_attributes
SET Same_day_Downgrades = CASE WHEN b.account_number IS NULL THEN 'No' ELSE 'Yes with no TV subscription' END
FROM SMG_Advance_attributes AS a
LEFT JOIN same_day_acct AS b ON a.account_number = b.account_number

--- Updating the TV downgrades
UPDATE SMG_Advance_attributes
SET Same_day_Downgrades = status
FROM SMG_Advance_attributes AS a
JOIN same_day_tv AS b ON a.account_number = b.account_number

DROP TABLE same_day_tv
DROP TABLE same_day_acct
DROP TABLE SMG_dowgrade_dt

COMMIT