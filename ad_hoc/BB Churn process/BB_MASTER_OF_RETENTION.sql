-- Select Cast(status_start_dt-prev_status_start_dt as integer) AB_Dur,count(*) SysCans
-- from cust_subs_hist
-- where status_start_dt >= '2016-07-01'
--         and status_code = 'SC' and prev_status_code = 'AB'
--         and status_code_changed = 'Y'
-- group by AB_Dur
-- 
-- Select top 1000 * from master_of_retention where event_dt >= '2016-07-01' and AB_Pending_Terminations > 0 and next_status_code = 'AC'

--------------------------------------------------------------------------------------
--------------------------- CVM Master of Retention ----------------------------------
--------------------------------------------------------------------------------------
/*
CREATE TABLE Master_Of_Retention (
	Subs_Year INT DEFAULT NULL
	, Subs_Week_Of_Year INT DEFAULT NULL
	, Subs_Week_And_Year INT DEFAULT NULL
	, Event_Dt DATE DEFAULT NULL
	, Account_Number VARCHAR(20) DEFAULT NULL
	, Country VARCHAR(3) DEFAULT NULL
	, PC_To_AB SMALLINT DEFAULT 0
	, AB_Pending_Terminations SMALLINT DEFAULT 0
	, Same_Day_Cancels SMALLINT DEFAULT 0
	, Same_Day_PC_Reactivations SMALLINT DEFAULT 0
	, PC_Pending_Cancellations SMALLINT DEFAULT 0
	, Gross_Terminations SMALLINT DEFAULT 0
	, Pipeline_Cancellations SMALLINT DEFAULT 0
	, BCRQ_Total_Reinstate SMALLINT DEFAULT 0
	, BCRQ_Winback SMALLINT DEFAULT 0
	, BCRQ_Reinstate_Over12m SMALLINT DEFAULT 0
	, PC_Effective_To_Dt DATE DEFAULT NULL
	, PC_Future_Sub_Effective_Dt DATE DEFAULT NULL
	, PC_Next_Status_Code VARCHAR(3) DEFAULT NULL
	, PC_Reactivation_Offer_ID INT DEFAULT NULL
	, PC_Reactivation_Offer_Description VARCHAR(465) DEFAULT NULL
	, PC_Reactivation_Offer_Amount DECIMAL(6, 2) DEFAULT NULL
	, AB_Effective_To_Dt DATE DEFAULT NULL
	, AB_Future_Sub_Effective_Dt DATE DEFAULT NULL
	, AB_Next_Status_Code VARCHAR(3) DEFAULT NULL
	, AB_Reactivation_Offer_ID INT DEFAULT NULL
	, AB_Reactivation_Offer_Description VARCHAR(465) DEFAULT NULL
	, AB_Reactivation_Offer_Amount DECIMAL(6, 2) DEFAULT NULL
	
	)

select top 1000 * from Master_Of_Retention
sp_columns 'OFFER_USAGE_ALL'
sp_columns 'Master_Of_Retention'

Delete from Master_Of_Retention
DROP variable Refresh_dt
*/
CREATE variable Refresh_dt DATE
SET Refresh_dt = COALESCE ((SELECT max(Event_Dt) - 6 * 7 FROM Master_Of_Retention), GETDATE())

SELECT Account_Number
	, CASE WHEN currency_code = 'EUR' THEN 'ROI' ELSE 'UK' END 		AS Country
	, count(PH_SUBS_SK) 		AS Number_Of_Subs  
	, SUBSCRIPTION_SUB_TYPE
	, CURRENCY_CODE
	, STATUS_CODE
	, PREV_STATUS_CODE
	, STATUS_CODE 				AS Movement_Type 
	, STATUS_START_DT 			AS Calendar_Date
	, CASE WHEN CAST(STATUS_START_DT AS DATE) - CAST(STATUS_END_DT AS DATE) = 0 THEN 'Y' ELSE 'N' END 				AS Same_Day_Movement
	, CASE WHEN CAST(PREV_STATUS_START_DT AS DATE) - CAST(STATUS_START_DT AS DATE) = 0 THEN 'Y' ELSE 'N' END 		AS Same_Day_Prev_Status_Move
	, CAST(STATUS_START_DT AS DATE) 																				AS STATUS_START_DT -- Dummy Date 1
	, CAST(PREV_STATUS_START_DT AS DATE) 																			AS PREV_STATUS_START_DT -- Dummy Date 2
INTO #MoR_Subs_Events
FROM Cust_Subs_Hist 			AS CSH
WHERE CSH.EFFECTIVE_FROM_DT >= Refresh_dt 
	AND OWNING_CUST_ACCOUNT_ID > '1' 
	AND STATUS_CODE_CHANGED = 'Y' 
	AND CSH.SUBSCRIPTION_SUB_TYPE = 'Broadband DSL Line'
	AND CAST(CSH.FIRST_ACTIVATION_DT as date) < '9999-01-01'
	AND CSH.EFFECTIVE_TO_DT >= '2014-01-01 00:00:00'
	
GROUP BY CSH.Account_Number
	, Country
	, SUBSCRIPTION_SUB_TYPE
	, CURRENCY_CODE
	, STATUS_CODE
	, PREV_STATUS_CODE
	, Calendar_Date
	, Same_Day_Movement
	, Same_Day_Prev_Status_Move
	, STATUS_START_DT
	, PREV_STATUS_START_DT 

COMMIT

CREATE hg INDEX idx_1 ON #MoR_Subs_Events (Account_number)
CREATE hg INDEX idx_2 ON #MoR_Subs_Events (Calendar_Date)

DELETE FROM Master_Of_Retention MoR
WHERE Event_Dt >= Refresh_dt

INSERT INTO Master_Of_Retention (Event_Dt, account_number, Country, PC_To_AB
	, AB_Pending_Terminations, Same_Day_Cancels, Same_Day_PC_Reactivations, PC_Pending_Cancellations
	, BCRQ_Total_Reinstate, BCRQ_Reinstate_Over12m, BCRQ_Winback
	, Gross_Terminations, Pipeline_Cancellations)
	
SELECT Calendar_Date
	, Account_number
	, Country
	, Sum(CASE WHEN STATUS_CODE = 'AB' AND PREV_STATUS_CODE = 'PC' THEN Number_Of_Subs ELSE 0 END) 																		AS v_PC_To_AB  							-- Includes intraday movements where customer in AB <24Hrs
	, Sum(CASE WHEN STATUS_CODE = 'AB' AND PREV_STATUS_CODE = 'AC' THEN Number_Of_Subs ELSE 0 END) + v_PC_To_AB 														AS V_pending_terminations 				-- Includes intraday movements where customer in AB <24Hrs
	, Sum(CASE WHEN Same_Day_Prev_Status_Move = 'Y' AND Movement_Type = 'CN' AND PREV_STATUS_CODE = 'PC' AND STATUS_CODE = 'CN' THEN Number_Of_Subs ELSE 0 END) 		AS Same_Day_Cancels						-- Includes intraday movements activating same day as cancel but excludes customers going direct from AC --> PO
	, Sum(CASE WHEN Same_Day_Prev_Status_Move = 'Y' AND PREV_STATUS_CODE = 'PC' AND STATUS_CODE = 'AC' THEN Number_Of_Subs ELSE 0 END) 									AS Same_Day_PC_Reactivations						
	, Sum(CASE WHEN STATUS_CODE = 'PC' THEN Number_Of_Subs ELSE 0 END) - Same_Day_PC_Reactivations 
			+ Sum(CASE WHEN Same_Day_Prev_Status_Move = 'Y' AND Movement_Type = 'AB' 
						AND PREV_STATUS_CODE = 'PC' AND STATUS_CODE = 'AB' THEN Number_Of_Subs ELSE 0 END) - Same_Day_Cancels 											AS V_pending_cancellations
	
	, Sum(CASE WHEN Movement_Type = 'AC' AND PREV_STATUS_CODE = 'BCRQ' THEN Number_Of_Subs ELSE 0 END) 																	AS BCRQ_Reinstate
	, Sum(CASE WHEN Movement_Type = 'AC' AND PREV_STATUS_CODE = 'BCRQ' AND DATEDIFF(month, PREV_STATUS_START_DT, STATUS_START_DT) > 11 THEN Number_Of_Subs ELSE 0 END) 	AS BCRQ_Reinstate_Over12m
	, BCRQ_Reinstate - BCRQ_Reinstate_Over12m 																															AS BCRQ_Winback
	, Sum(CASE WHEN Movement_Type = 'CN' THEN Number_Of_Subs ELSE 0 END) 																								AS V_gross_terminations
	, Sum(CASE WHEN Movement_Type = 'CN' THEN Number_Of_Subs ELSE 0 END) - Same_Day_Cancels 																			AS V_pipeline_cancellations -- Includes intraday POs
FROM #MoR_Subs_Events
GROUP BY Account_number
	, Calendar_Date
	, Country
HAVING v_PC_To_AB + V_pending_terminations + Same_Day_Cancels + Same_Day_PC_Reactivations + V_pending_cancellations 
		+ BCRQ_Reinstate +  V_gross_terminations + V_pipeline_cancellations > 0

		
		
UPDATE Master_Of_Retention MoR
SET Subs_Year = sc.subs_year
	, Subs_Week_Of_Year = sc.Subs_Week_Of_Year
	, Subs_Week_And_Year = sc.Subs_Week_And_Year
FROM Master_Of_Retention MoR
INNER JOIN sky_calendar sc ON sc.calendar_date = MoR.Event_dt
WHERE MoR.Subs_Week_And_Year IS NULL

COMMIT









--------------------------------------------------------------------------------------
------------------------- Add Future Subs Effective Dt -------------------------------
--------------------------------------------------------------------------------------
DROP TABLE

IF EXISTS #PC_Future_Effective_Dt
	SELECT MoR.account_number
		, MoR.event_dt
		, csh.status_end_dt status_end_dt
		, csh.future_sub_effective_dt
		, csh.effective_from_datetime
		, csh.effective_to_datetime
		, row_number() OVER (
			PARTITION BY MoR.account_number
			, MoR.event_dt ORDER BY csh.effective_from_datetime DESC
			) PC_Rnk
	INTO #PC_Future_Effective_Dt
	FROM Master_Of_Retention MoR
	INNER JOIN cust_subs_hist csh ON csh.account_number = MoR.account_number AND csh.status_start_dt = MoR.Event_dt 
								AND csh.status_end_dt >= Refresh_dt AND csh.subscription_sub_type = 'Broadband DSL Line' 
								AND csh.OWNING_CUST_ACCOUNT_ID > '1' AND csh.STATUS_CODE_CHANGED = 'Y' AND csh.status_code = 'PC'
	WHERE Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0 
			AND (MoR.PC_Effective_To_Dt IS NULL OR MoR.PC_Future_Sub_Effective_Dt IS NULL)

COMMIT

CREATE hg INDEX idx_1 ON #PC_Future_Effective_Dt (account_number)
CREATE DATE INDEX idx_2 ON #PC_Future_Effective_Dt (event_dt)
CREATE lf INDEX idx_3 ON #PC_Future_Effective_Dt (PC_Rnk)
DELETE
FROM #PC_Future_Effective_Dt
WHERE PC_Rnk > 1

COMMIT

UPDATE Master_Of_Retention MoR
SET MoR.PC_Future_Sub_Effective_Dt = PC.future_sub_effective_dt
FROM Master_Of_Retention MoR
INNER JOIN #PC_Future_Effective_Dt PC ON PC.account_number = MoR.account_number AND pc.event_dt = MoR.event_dt

UPDATE Master_Of_Retention MoR
SET MoR.AB_Future_Sub_Effective_Dt = Cast(event_dt + 50 AS DATE)
WHERE AB_Pending_Terminations > 0 AND (AB_Effective_To_Dt IS NULL OR AB_Future_Sub_Effective_Dt IS NULL)

--------------------------------------------------------------------------------------
--------------------------- Add PC Effective To Dt -----------------------------------
--------------------------------------------------------------------------------------
SELECT MoR.account_number
	, MoR.event_dt
	, CSH.status_start_dt PC_Effective_To_dt
	, csh.status_code Next_Status_Code
	, Row_number() OVER (
		PARTITION BY MoR.account_number
		, MoR.event_dt ORDER BY status_start_dt DESC
		) Status_change_rnk
INTO #PC_Status_Change
FROM Master_Of_Retention MoR
INNER JOIN cust_subs_hist CSH ON CSH.account_number = MoR.account_number AND CSH.prev_status_start_dt = MoR.event_dt AND csh.subscription_sub_type = 'Broadband DSL Line'
WHERE csh.status_start_dt >= MoR.event_dt AND csh.status_end_dt >= Refresh_dt AND prev_status_code = 'PC' AND status_code != 'PC' AND status_code_changed = 'Y' AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0)

UPDATE Master_Of_Retention
SET PC_Effective_To_Dt = CSH.PC_Effective_To_dt
	, PC_Next_Status_Code = CSH.Next_Status_Code
FROM Master_Of_Retention MoR
INNER JOIN #PC_Status_Change CSH ON CSH.account_number = MoR.account_number AND CSH.event_dt = MoR.event_dt
WHERE Status_change_rnk = 1

COMMIT

--------------------------------------------------------------------------------------
--------------------------- Add AB Effective To Dt -----------------------------------
--------------------------------------------------------------------------------------
SELECT MoR.account_number
	, MoR.event_dt
	, CSH.status_start_dt AB_Effective_To_dt
	, csh.status_code Next_Status_Code
	, Row_number() OVER (
		PARTITION BY MoR.account_number
		, MoR.event_dt ORDER BY status_start_dt DESC
		) Status_change_rnk
INTO #AB_Status_Change
FROM Master_Of_Retention MoR
INNER JOIN cust_subs_hist CSH ON CSH.account_number = MoR.account_number AND CSH.prev_status_start_dt = MoR.event_dt AND csh.subscription_sub_type = 'Broadband DSL Line'
WHERE csh.status_start_dt >= MoR.event_dt AND csh.status_end_dt >= Refresh_dt AND prev_status_code = 'AB' AND status_code != 'AB' AND status_code_changed = 'Y' AND AB_Pending_Terminations > 0

UPDATE Master_Of_Retention
SET AB_Effective_To_Dt = CSH.AB_Effective_To_dt
	, AB_Next_Status_Code = CSH.Next_Status_Code
FROM Master_Of_Retention MoR
INNER JOIN #AB_Status_Change CSH ON CSH.account_number = MoR.account_number AND CSH.event_dt = MoR.event_dt
WHERE Status_change_rnk = 1

COMMIT
GO
--------------------------------------------------------------------------------------
-------------------------- Add Reactivation Offer Details ----------------------------
--------------------------------------------------------------------------------------
UPDATE master_of_retention MoR
SET PC_Reactivation_Offer_ID = oua.offer_id
	, PC_Reactivation_Offer_Description = oua.offer_dim_description
	, PC_Reactivation_Offer_Amount = oua.Intended_offer_Amount
FROM master_of_retention MoR
INNER JOIN offer_usage_all oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt AND MoR.PC_Next_Status_Code = 'AC' 
								AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' AND oua.subs_type = 'Broadband DSL Line'
WHERE MoR.PC_effective_to_dt >= Refresh_dt AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0)
GO


SELECT offer_id
		,offer_dim_description
		, Intended_offer_Amount
		, account_number
		, offer_Start_Dt_Actual
		, Whole_offer_Start_Dt_Actual
		, subs_type
		INTO #oua
FROM master_of_retention MoR
INNER JOIN offer_usage_all oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt AND MoR.AB_Next_Status_Code = 'AC' AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' AND oua.subs_type = 'Broadband DSL Line'

COMMIT 
CREATE HG INDEX id1 ON #oua (account_number)
CREATE DATE INDEX id2 ON #oua (offer_Start_Dt_Actual)
CREATE DATE INDEX id3 ON #oua (Whole_offer_Start_Dt_Actual)
CREATE HG INDEX id4 ON #oua (offer_dim_description)

									
UPDATE master_of_retention MoR
SET AB_Reactivation_Offer_ID = oua.offer_id
	, AB_Reactivation_Offer_Description = oua.offer_dim_description
	, AB_Reactivation_Offer_Amount = oua.Intended_offer_Amount
FROM master_of_retention 	AS MoR
INNER JOIN #oua 			AS oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt AND MoR.AB_Next_Status_Code = 'AC' AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%'
WHERE MoR.AB_effective_to_dt >= Refresh_dt AND AB_Pending_Terminations > 0
GO





