/*----------------------------------------------------------------------------------------------------------*/
-------------------------------- ForeCAST Model Development Log ----------------------------------------------
/*----------------------------------------------------------------------------------------------------------*/
/*
V5  -- Initial 5Yr Plan and Q2 F'CAST
V6  -- Update default rentention offer length from 6months to 10 months
V7  -- A year after rolling off an offer if a customer hasn't taken a new offer they move back into the lower risk No Offer segment
    -- Last Time since last TA updated so customers foreCAST to call TA will move into higher risk segments that have recently called TA
V8  -- Predicted rates and trends for Sky Q  customers applied to account for there being no history of Q customer events
V9  -- Logic added for Pending Cancels
V10 -- HD segment added to simulation table
    -- All figures updated with new TA definition (i.e. all Cancellation Attempts with a TA Save or Non-Save outcome
    -- Historic base update to use lookup table to assign customer cuscan/syscan segments
V11 -- Correction to churn fix so churn removes customers with TA
V12 -- CusCan ForeCAST Segment updated to use Time to Offer End across DTV,BB and LR instead of just DTV
    -- CusCan/SysCan Weekly Agg tables removed and replaced with CITeam.DTV_FCAST_Weekly_Base to speed up query
    -- Status code used as foreCAST segment in CITeam.DTV_FCAST_Weekly_Base
    -- DTV_BB_LR_offer_end_dt added to CITeam.Cust_FCAST_Weekly_Base
    -- cuscan_weekly_agg updated to aggregate CITeam.DTV_FCAST_Weekly_Base
    -- Code for cuscan_align_mXX and Syscan_align_mXXtables removed
    -- CITeam.Cust_FCAST_Weekly_Base used for churn vol in to Pred_Rates in place of CITeam.Weekly_Agg_Actions_CusCan
V13 -- Code corrected for sampling last 6 Wks of Acquisition
V14 -- Pending Cancels logic added to foreCAST
V15 -- Large blocks of code replaced by procedures
    -- Regression trend updated to remove oscillations in trend caused by starting and eding with different quarters, issue with null rates also corrected
    -- Logic for Cust_FCAST_Weekly_Base offers applied corrected so it uses whole offer start instead of individual offer legs
    -- Logic added for PC entries, durations and rectivations
    -- Phasing for CusCan rates table shift back 1Wk to account for Wk 53 where appropriate -- SysCan Rates TBC
    -- Offer duration for new offers based on distribution from last 6 Wks rather than fixed 10m duration
V16 -- New Segment added for customers in 1st of Tenure
    -- ROI Segments added
    -- Rate multiplier added to a buffer into foreCAST and prevent underforeCASTing
    -- All rate procs updated to only use actuals from DTV_FCAST_Weekly_Base table
*/

/*----------------------------------------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------------------------------------*/
----------------------------------------------------------------------------------------------------------------
-- PART 0: Test if foreCAST should run -------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
/*			Not needeed now
SET Nxt_sim_Dt = (SELECT Min(Misc_dt_1) FROM SQL_Automation_Execution_Flags
		WHERE Automation_Script = 'FORECAST_Tableau_Tables' 
			AND Automation_Variable = 'Cuscan_FCAST_Table_Status'
		)

IF today() < Nxt_sim_Dt 
RETURN
*/



-----------------------------------------------------------------------------------------------
----PART I: Create Variables and Set ForeCAST Parameters --------------------------------------
-----------------------------------------------------------------------------------------------
	MESSAGE CAST(now() as timestamp)||' | Initialising Environment' TO CLIENT

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

DROP variable IF EXISTS Nxt_sim_Dt
DROP variable IF EXISTS ForeCAST_Start_Wk

DROP variable IF EXISTS sample_pct
DROP variable IF EXISTS true_sample_rate
DROP variable IF EXISTS counter
DROP variable IF EXISTS run_rate_weeks
DROP variable IF EXISTS Y3W52
DROP variable IF EXISTS Y3W40
DROP variable IF EXISTS Y3W01
DROP variable IF EXISTS Y2W01
DROP variable IF EXISTS Y1W01
DROP variable IF EXISTS ForeCAST_End_Wk 
DROP variable IF EXISTS n_weeks_to_foreCAST 
DROP TABLE IF EXISTS #Sky_Calendar
DROP TABLE IF EXISTS SABB_predicted_values
DROP TABLE IF EXISTS FCAST_Regr_Coeffs
DROP TABLE IF EXISTS IntraWk_PC_Pct
DROP TABLE IF EXISTS IntraWk_AB_Pct
DROP TABLE IF EXISTS IntraWk_BCRQ_Pct
DROP TABLE IF EXISTS DTV_PC_Duration_Dist
DROP TABLE IF EXISTS TA_DTV_PC_Vol
DROP TABLE IF EXISTS PC_PL_Status_Change_Dist
DROP TABLE IF EXISTS AB_PL_Status_Change_Dist
DROP TABLE IF EXISTS Offer_Applied_Dur_Dist
DROP TABLE IF EXISTS Activation_Vols
DROP TABLE IF EXISTS ForeCAST_Loop_Table
DROP TABLE IF EXISTS #Sky_Calendar
GO
		CREATE variable Nxt_sim_Dt DATE;
		CREATE variable ForeCAST_Start_Wk INT;--1st Wk of ForeCAST
		CREATE variable n_weeks_to_foreCAST INT;
		CREATE variable sample_pct DECIMAL(7, 4);
		CREATE variable ForeCAST_End_Wk INT;--Last Wk of ForeCAST
		CREATE variable Y3W52 INT;
		CREATE variable Y3W40 INT;
		CREATE variable Y3W01 INT;
		CREATE variable Y2W01 INT;
		CREATE variable Y1W01 INT;
		CREATE variable true_sample_rate FLOAT;
		CREATE variable counter INT;
		CREATE variable run_rate_weeks INT;

	MESSAGE CAST(now() as timestamp)||' | Variables Created' TO CLIENT;
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;
	SET ForeCAST_Start_Wk 	= 201601
	SET ForeCAST_End_Wk 	= 201602
	SET n_weeks_to_foreCAST = (SELECT count(DISTINCT subs_week_and_year) FROM #sky_calendar
								WHERE CAST(subs_week_and_year AS INT) BETWEEN ForeCAST_Start_Wk AND ForeCAST_End_Wk )
	SET sample_pct 			= 0.25
	SET run_rate_weeks 		= 13
	SET Y1W01 				= CASE 	WHEN ((CAST(ForeCAST_Start_Wk AS FLOAT) / 100) % 1) * 100 = 53 THEN (ForeCAST_Start_Wk / 100 - 2) * 100 + 1 
									ELSE ForeCAST_Start_Wk - 300 END
	SET Y2W01 				= Y1W01 + 100
	SET Y3W01 				= Y1W01 + 200
	SET Y3W52 				= CASE 	WHEN ((CAST(ForeCAST_Start_Wk AS FLOAT) / 10) % 1) * 10 = 1 THEN (ForeCAST_Start_Wk / 100 - 1) * 100 + 52 
									ELSE ForeCAST_Start_Wk - 1 END
	SET Y3W40 				= CASE 	WHEN ((CAST(Y3W52 AS FLOAT) / 100) % 1) * 100 <= 12 THEN (Y3W52 / 100 - 1) * 100 + (52 - 12) + ((CAST(Y3W52 AS FLOAT) / 100) % 1) * 100 
									WHEN Y3W52 = 53 THEN Y3W52 - 13 
									ELSE Y3W52 - 12 END

	MESSAGE CAST(now() as timestamp)||' | Variables set' TO CLIENT

	SELECT * INTO	#Sky_Calendar 			FROM Subs_Calendar(ForeCAST_Start_Wk / 100, ForeCAST_End_Wk / 100)

	MESSAGE CAST(now() as timestamp)||' | Sky_Calendar set' TO CLIENT

	SELECT * INTO 	SABB_predicted_values 	FROM ForeCAST_SABB_Rates(ForeCAST_Start_Wk)
									
		MESSAGE CAST(now() as timestamp)||' | Initialising Environment DONE' TO CLIENT;
-----------------------------------------------------------------------------------------------
----PART II: Calculate historic rates and trends for foreCAST
-----------------------------------------------------------------------------------------------

	MESSAGE CAST(now() as timestamp)||' | Calculate historic rates and trends for foreCAST' TO CLIENT;
	
		SELECT * INTO FCAST_Regr_Coeffs 		FROM SABB_Regression_Coefficient(ForeCAST_Start_Wk, 2);   	-- Trend coefficients
		SELECT * INTO IntraWk_PC_Pct 			FROM Intraweek_PCs_Dist(ForeCAST_Start_Wk);					--Intraweek PC probablilities
		SELECT * INTO IntraWk_AB_Pct 			FROM Intraweek_ABs_Dist(ForeCAST_Start_Wk);					--Intraweek PC probablilities
		SELECT * INTO IntraWk_BCRQ_Pct 			FROM Intraweek_BCRQ_Dist(ForeCAST_Start_Wk);				-- Intended PC Duration Distribution
		SELECT * INTO DTV_PC_Duration_Dist 		FROM PC_Duration_Dist(ForeCAST_Start_Wk);					-- PC Reactivation, PC ABs 
		SELECT * INTO PC_PL_Status_Change_Dist 	FROM PC_Status_Movement_Probabilities(ForeCAST_Start_Wk);	--Select * into TA_DTV_PC_Vol from ForeCAST_PC_Conversion_Rates(Y3W52) --- ############## CONFIRM -- ??? Dont think we need this step now
		SELECT * INTO AB_PL_Status_Change_Dist 	FROM AB_Status_Movement_Probabilities(ForeCAST_Start_Wk);	-- AB Reactivation
		SELECT * INTO Offer_Applied_Dur_Dist 	FROM SABB_Offer_Applied_Duration_Dist(ForeCAST_Start_Wk, 6);	-- Offer Duration Distribution
		CALL 										SABB_Build_ForeCAST_New_Cust_Sample(ForeCAST_Start_Wk);	-- Last 6 Wks activations to sample in simulation
		SELECT * INTO Activation_Vols 			FROM SABB_ForeCAST_Activation_Vols(Y2W01,Y3W52);			-- Weekly Activation Volumes
		CALL SABB_ForeCAST_Create_Opening_Base(ForeCAST_Start_Wk,sample_pct);

	MESSAGE CAST(now() as timestamp)||' | Calculate historic rates and trends for foreCAST DONE' TO CLIENT;

----------------------------------------------------------------------------------------------------------------
---PART IV - CREATE OPENING BASE FOR SIMULATION ----------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
	MESSAGE CAST(now() as timestamp)||' | Create opening base for simulation' TO CLIENT;

SELECT 
	  account_number
	, end_date
	, subs_week_and_year
	, subs_year
	, subs_week_of_year
	, weekid
	, CAST(TRIM(BB_Status_Code) AS VARCHAR(4)) AS BB_Status_Code 
	, trim(churn_type) as churn_type
	, CAST(NULL AS VARCHAR(4)) AS BB_Status_Code_EoW
	, BB_Segment
	, country
	, BB_package
	, BB_offer_rem_and_end_raw
	, BB_offer_rem_and_end
	, BB_tenure_raw
	, BB_tenure
	, my_sky_login_3m_raw
	, my_sky_login_3m
	, talk_type
	, home_owner_status
	, BB_all_calls_1m_raw
	, BB_all_calls_1m
	, Simple_Segments
	, CAST(node_SA AS VARCHAR(4) )  AS SABB_forecast_segment 
	, segment_SA
	, PL_Future_Sub_Effective_Dt
	, DTV_Activation_Type
	, Curr_Offer_start_Date_BB
	, Curr_offer_end_date_Intended_BB
	, Prev_offer_end_date_BB
	, Future_offer_Start_dt
	, Future_end_Start_dt
	, BB_first_act_dt
	, rand_sample
	, sample
	, SABB_flag
	, cast (0 as tinyint) as BB_SysCan
	, cast (0 as tinyint) as BB_CusCan
	, cast (0 as tinyint) as BB_HM
	, cast (0 as tinyint) as BB_3rd_Party
	, CAST(NULL AS FLOAT) AS rand_action_Pipeline 				--- used to simulate status flows in loop
	, CAST(NULL AS FLOAT) AS rand_BB_Offer_Applied 				--- used to simulate status flows in loop
	, CAST(NULL AS FLOAT) AS rand_Intrawk_BB_NotSysCan 			--- used to simulate status flows in loop
	, CAST(NULL AS FLOAT) AS rand_Intrawk_BB_SysCan 			--- used to simulate status flows in loop
	, CAST(NULL AS FLOAT) AS rand_BB_Pipeline_Status_Change 	--- used to simulate status flows in loop
	, CAST(NULL AS FLOAT) AS rand_New_Off_Dur 					-- used in prep for state of next loop
	, CAST(NULL AS FLOAT) AS rand_BB_NotSysCan_Duration 		-- used in prep for state of next loop
	, CAST(0 AS TINYINT)  AS CusCan
	, CAST(0 AS TINYINT)  AS SysCan
	, CAST(0 AS TINYINT)  AS HM
	, CAST(0 AS TINYINT)  AS _3rd_Party
	, cast(0 AS TINYINT)  AS calls_LW
	, cast(0 AS TINYINT)  AS my_sky_login_LW
INTO ForeCAST_Loop_Table
FROM /* CITeam.*/FORECAST_Base_Sample
WHERE sample = 'A';
COMMIT ;
	MESSAGE CAST(now() as timestamp)||' | Create opening base for simulation DONE '||@@rowcount TO CLIENT;

		CALL SABB_my_sky_login_prob; 			--- My SKy login prob
		CALL SABB_BB_Calls_prob;				--- BB calls prob
	
----------------------------------------------------------------------------------------------------------------
-- PART V - RUN SIMULATION LOOP --------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
	MESSAGE CAST(now() as timestamp)||' | Run simulation loop' TO CLIENT;
SET counter = 1;
SET True_Sample_Rate = (SELECT CAST(sum(CASE WHEN sample = 'A' THEN 1 ELSE 0 END) AS FLOAT) / count(*) FROM FORECAST_Base_Sample );			-- Select True_Sample_Rate
DELETE FROM FORECAST_Looped_Sim_Output_Platform;

-- Start Loop
WHILE Counter <= n_weeks_to_foreCAST loop
		MESSAGE counter ||' | Counter' TO CLIENT;
		MESSAGE n_weeks_to_foreCAST ||' | n_weeks_to_foreCAST' TO CLIENT;
		
		CALL SABB_ForeCAST_Create_ForeCAST_Loop_Table_2(ForeCAST_Start_Wk, ForeCAST_End_Wk, True_Sample_Rate);
		CALL Forecast_SABB_Loop_Table_2_Actions (Counter, 1.02) ;
		CALL SABB_ForeCAST_Insert_New_Custs_Into_Loop_Table_2(ForeCAST_Start_Wk, ForeCAST_End_Wk, True_Sample_Rate);

		/* ---- Temporary code to remove churned customers ----*/
		-- Insert table into Output table
		INSERT INTO FORECAST_Looped_Sim_Output_Platform
		SELECT * FROM ForeCAST_Loop_Table_2;

		-- Remove churned customers
		DELETE FROM ForeCAST_Loop_Table_2 	WHERE CusCan = 1;
		DELETE ForeCAST_Loop_Table_2 		WHERE SysCan = 1;
		DELETE ForeCAST_Loop_Table_2 		WHERE HM = 1;
		DELETE ForeCAST_Loop_Table_2 		WHERE _3rd_Party = 1;
		
		SET counter = counter + 1;
		-- Update ForeCAST_Loop_Table_2 fields for next week
		CALL SABB_ForeCAST_Loop_Table_2_Update_For_Nxt_Wk;
		-- Create new foreCAST loop table for start of next week's loop
		CALL SABB_ForeCAST_Create_New_ForeCAST_Loop_Table;
			
END ;

MESSAGE CAST(now() as timestamp)||' | Run simulation loop DONE' TO CLIENT;
GO
