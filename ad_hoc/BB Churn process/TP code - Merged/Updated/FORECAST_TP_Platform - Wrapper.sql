/*----------------------------------------------------------------------------------------------------------*/
-------------------------------- ForeCAST Model Development Log ----------------------------------------------
/*----------------------------------------------------------------------------------------------------------*/

-----------------------------------------------------------------------------------------------
----PART I: Create Variables and Set ForeCAST Parameters --------------------------------------
-----------------------------------------------------------------------------------------------
MESSAGE CAST(now() as timestamp)||' | Initialising Environment' TO CLIENT;

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

DROP variable IF EXISTS TP_Nxt_sim_Dt;
DROP variable IF EXISTS TP_ForeCAST_Start_Wk;
DROP variable IF EXISTS TP_sample_pct;
DROP variable IF EXISTS TP_true_sample_rate;
DROP variable IF EXISTS TP_counter;
DROP variable IF EXISTS TP_run_rate_weeks;
DROP variable IF EXISTS TP_Y3W52;
DROP variable IF EXISTS TP_Y3W40;
DROP variable IF EXISTS TP_Y3W01;
DROP variable IF EXISTS TP_Y2W01;
DROP variable IF EXISTS TP_Y1W01;
DROP variable IF EXISTS TP_ForeCAST_End_Wk;
DROP variable IF EXISTS TP_n_weeks_to_foreCAST;
DROP TABLE IF EXISTS #Sky_Calendar;
DROP TABLE IF EXISTS TP_predicted_values;
DROP TABLE IF EXISTS TP_FCAST_Regr_Coeffs;
DROP TABLE IF EXISTS TP_IntraWk_PC_Pct;
DROP TABLE IF EXISTS TP_IntraWk_AB_Pct;
DROP TABLE IF EXISTS TP_IntraWk_BCRQ_Pct;
DROP TABLE IF EXISTS TP_DTV_PC_Duration_Dist;
DROP TABLE IF EXISTS TP_TA_DTV_PC_Vol;
DROP TABLE IF EXISTS TP_PC_PL_Status_Change_Dist;
DROP TABLE IF EXISTS TP_AB_PL_Status_Change_Dist;
DROP TABLE IF EXISTS TP_Offer_Applied_Dur_Dist;
DROP TABLE IF EXISTS TP_Activation_Vols;
DROP TABLE IF EXISTS TP_ForeCAST_Loop_Table;
DROP TABLE IF EXISTS TP_intraweek_movements;
DROP TABLE IF EXISTS TP_weekly_movements;
DROP TABLE IF EXISTS my_sky;
--DROP TABLE IF EXISTS #Sky_Calendar;

	CREATE variable TP_Nxt_sim_Dt DATE;
	CREATE variable TP_ForeCAST_Start_Wk INT;--1st Wk of ForeCAST
	CREATE variable TP_n_weeks_to_foreCAST INT;
	CREATE variable TP_sample_pct DECIMAL(7, 4);
	CREATE variable TP_ForeCAST_End_Wk INT;--Last Wk of ForeCAST
	CREATE variable TP_Y3W52 INT;
	CREATE variable TP_Y3W40 INT;
	CREATE variable TP_Y3W01 INT;
	CREATE variable TP_Y2W01 INT;
	CREATE variable TP_Y1W01 INT;
	CREATE variable TP_true_sample_rate FLOAT;
	CREATE variable TP_counter INT;
	CREATE variable TP_run_rate_weeks INT;

/*---------------=============&&&&&&&&&&&&&&&===================----------------
-----------------				PARAMETERS 						----------------
-----------------=============&&&&&&&&&&&&&&&===================--------------*/

SET TP_ForeCAST_Start_Wk 	= 201601;
SET TP_ForeCAST_End_Wk 	= 201626;
SET TP_sample_pct 			= 0.25;
SET TP_run_rate_weeks 		= 13;

/*---------------=============&&&&&&&&&&&&&&&===================--------------*/

SELECT * INTO	#Sky_Calendar 			FROM Subs_Calendar(TP_ForeCAST_Start_Wk / 100, TP_ForeCAST_End_Wk / 100);
SET TP_n_weeks_to_foreCAST = (SELECT count(DISTINCT subs_week_and_year) FROM #sky_calendar
							WHERE CAST(subs_week_and_year AS INT) BETWEEN TP_ForeCAST_Start_Wk AND TP_ForeCAST_End_Wk );
SET TP_Y1W01 				= CASE 	WHEN ((CAST(TP_ForeCAST_Start_Wk AS FLOAT) / 100) % 1) * 100 = 53 THEN (TP_ForeCAST_Start_Wk / 100 - 2) * 100 + 1  ELSE TP_ForeCAST_Start_Wk - 300 END;
SET TP_Y2W01 				= TP_Y1W01 + 100;
SET TP_Y3W01 				= TP_Y1W01 + 200;
SET TP_Y3W52 				= CASE 	WHEN ((CAST(TP_ForeCAST_Start_Wk AS FLOAT) / 10) % 1) * 10 = 1 THEN (TP_ForeCAST_Start_Wk / 100 - 1) * 100 + 52 ELSE TP_ForeCAST_Start_Wk - 1 END;
SET TP_Y3W40 				= CASE 	WHEN ((CAST(TP_Y3W52 AS FLOAT) / 100) % 1) * 100 <= 12 THEN (TP_Y3W52 / 100 - 1) * 100 + (52 - 12) + ((CAST(TP_Y3W52 AS FLOAT) / 100) % 1) * 100 
								WHEN TP_Y3W52 = 53 THEN TP_Y3W52 - 13 
								ELSE TP_Y3W52 - 12 END;

MESSAGE CAST(now() as timestamp)||' | Initialising Environment DONE' TO CLIENT;



/*---------------=============&&&&&&&&&&&&&&&===================--------------*/
/*---------------=============&&&&&&&&&&&&&&&===================--------------*/
/*---------------=============&&&&&&&&&&&&&&&===================--------------*/

	SELECT base.account_number
		, base.end_Date
		, visit = COUNT(DISTINCT visit_date ) 
        , CASE WHEN visit > 2 THEN 3 ELSE visit END AS mysky
	INTO my_sky
	FROM vespa_shared.mysky_daily_usage AS v 
	INNER JOIN citeam.DTV_Fcast_Weekly_Base  AS base ON BASE.account_number = v.account_number AND Subs_Week_And_Year BETWEEN 201401 AND 201601
		AND dtV_active = 1 AND bb_active = 1 
	AND visit_date BETWEEN DATEADD(wk, 1, DATEADD(mm, - 3, end_date)) AND end_date
	GROUP BY base.account_number
		, base.end_date;

COMMIT;
CREATE HG INDEX id1 ON  my_sky(account_number);
CREATE DATE INDEX id2 ON  my_sky(end_date);
CREATE LF INDEX id3 ON  my_sky(mysky);

COMMIT;

/*---------------=============&&&&&&&&&&&&&&&===================--------------*/
/*---------------=============&&&&&&&&&&&&&&&===================--------------*/
/*---------------=============&&&&&&&&&&&&&&&===================--------------*/



SELECT * INTO 	TP_predicted_values 	FROM ForeCAST_TP_Rates(TP_ForeCAST_Start_Wk);
								
MESSAGE CAST(now() as timestamp)||' | Weekly Rate Calculation DONE' TO CLIENT;
-----------------------------------------------------------------------------------------------
----PART II: Calculate historic rates and trends for foreCAST
-----------------------------------------------------------------------------------------------

	MESSAGE CAST(now() as timestamp)||' | Calculate historic rates and trends for foreCAST' TO CLIENT;

	
	
-- Trend coefficients
	SELECT * INTO TP_FCAST_Regr_Coeffs 			FROM TP_Regression_Coefficient(TP_ForeCAST_Start_Wk, 2);
--Intraweek PL probablilities
	SELECT * INTO TP_IntraWk_AB_Pct 			FROM TP_Intraweek_ABs_Dist(TP_ForeCAST_Start_Wk);
	SELECT * INTO TP_IntraWk_PC_Pct 			FROM TP_Intraweek_PCs_Dist(TP_ForeCAST_Start_Wk);
	SELECT * INTO TP_IntraWk_BCRQ_Pct 			FROM TP_Intraweek_BCRQ_Dist(TP_ForeCAST_Start_Wk);
-- Intended PC Duration Distribution
	SELECT * INTO TP_DTV_PC_Duration_Dist 		FROM TP_PC_Duration_Dist(TP_ForeCAST_Start_Wk);
-- PC Reactivation, PC ABs 
	SELECT * INTO TP_PC_PL_Status_Change_Dist 	FROM TP_PC_Status_Movement_Probabilities(TP_ForeCAST_Start_Wk);
-- AB Reactivation
	SELECT * INTO TP_AB_PL_Status_Change_Dist 	FROM TP_AB_Status_Movement_Probabilities(TP_ForeCAST_Start_Wk);
-- Offer Duration Distribution
	SELECT * INTO TP_Offer_Applied_Dur_Dist 	FROM TP_Offer_Applied_Duration_Dist(TP_ForeCAST_Start_Wk, 6);
-- Last 6 Wks activations to sample in simulation
	CALL TP_Build_ForeCAST_New_Cust_Sample(TP_ForeCAST_Start_Wk);
-- Weekly Activation Volumes
	SELECT * INTO TP_Activation_Vols 			FROM TP_Forecast_Activation_Vols(TP_Y2W01,TP_Y3W52);
	CALL TP_ForeCAST_Create_Opening_Base  (TP_ForeCAST_Start_Wk,TP_sample_pct);

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
	, CAST(NULL AS VARCHAR(4)) AS BB_Status_Code_EoW
	, BB_Segment
	, country
	, BB_package
	
	, trim(churn_type) as churn_type
	, DTV_TA_calls_1m_raw
	, DTV_TA_calls_1m
	, RTM
	, Talk_tenure_raw
	
	, Talk_tenure
	, my_sky_login_3m_raw
	, my_sky_login_3m
	, BB_all_calls_1m_raw
	, BB_all_calls_1m
	
	, Simple_Segments
	, CAST(node_TP AS VARCHAR(4) )  AS TP_forecast_segment
	, segment_TP
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
	, TP_flag
	
	, cast (0 as BIT) as Sky_plus  	
	, cast (0 as BIT) as nowtv_flag
	
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
	, CAST(0 AS TINYINT) 		AS CusCan
	, CAST(0 AS TINYINT) 		AS SysCan
	, CAST(0 AS TINYINT) 		AS HM
	, CAST(0 AS TINYINT) 		AS _3rd_Party
    ,cast (0 as smallint) 		AS calls_LW
    ,cast (0 as smallint) 		AS my_sky_login_LW
	,cast (0 as smallint) 		AS DTV_LW
INTO TP_ForeCAST_Loop_Table
FROM /* CITeam.*/TP_FORECAST_Base_Sample
WHERE sample = 'A';

COMMIT ;
	
MESSAGE CAST(now() as timestamp)||' | Create opening base for simulation DONE '||@@rowcount TO CLIENT;

	CALL TP_my_sky_login_prob ;
	CALL TP_BB_Calls_prob ;	
	CALL TP_DTV_Calls_prob ;	
----------------------------------------------------------------------------------------------------------------
-- PART V - RUN SIMULATION LOOP --------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
	MESSAGE CAST(now() as timestamp)||' | Run simulation loop' TO CLIENT;
			
		SET TP_counter = 1;
		SET TP_true_sample_rate = (SELECT CAST(sum(CASE WHEN sample = 'A' THEN 1 ELSE 0 END) AS FLOAT) / count(*) FROM TP_FORECAST_Base_Sample );
		DELETE FROM TP_FORECAST_Looped_Sim_Output_Platform;

		-- Start Loop
		WHILE TP_counter <= TP_n_weeks_to_foreCAST loop
				MESSAGE TP_counter ||' | TP_counter' TO CLIENT;
				MESSAGE TP_n_weeks_to_foreCAST ||' | TP_n_weeks_to_foreCAST' TO CLIENT;
				MESSAGE TP_true_sample_rate ||' | True_Sample_Rat' TO CLIENT;
					-- Create ForeCAST Loop Table 2
						CALL TP_ForeCAST_Create_ForeCAST_Loop_Table_2(TP_ForeCAST_Start_Wk, TP_ForeCAST_End_Wk, TP_true_sample_rate);
						CALL ForeCAST_TP_Loop_Table_2_Actions(TP_counter, 1.02) ;
						CALL TP_ForeCAST_Insert_New_Custs_Into_Loop_Table_2(TP_ForeCAST_Start_Wk, TP_ForeCAST_End_Wk, TP_true_sample_rate);
				MESSAGE CAST(now() as timestamp)||' | Before insert' TO CLIENT;	
						INSERT INTO TP_FORECAST_Looped_Sim_Output_Platform
						SELECT * FROM TP_ForeCAST_Loop_Table_2;
				MESSAGE CAST(now() as timestamp)||' | After insert' TO CLIENT;	
						DELETE FROM TP_ForeCAST_Loop_Table_2 	WHERE CusCan = 1;
						DELETE TP_ForeCAST_Loop_Table_2 		WHERE SysCan = 1;
						DELETE TP_ForeCAST_Loop_Table_2 		WHERE HM = 1;
						DELETE TP_ForeCAST_Loop_Table_2 		WHERE _3rd_Party = 1;
				MESSAGE CAST(now() as timestamp)||' | Delete done' TO CLIENT;
					
						SET TP_counter = TP_counter + 1;
					-- Update TP_ForeCAST_Loop_Table_2 fields for next week
						CALL TP_ForeCAST_Loop_Table_2_Update_For_Nxt_Wk();
					-- Create new foreCAST loop table for start of next week's loop
						CALL TP_ForeCAST_Create_New_ForeCAST_Loop_Table;
							
		END loop;
		--Select * into FORECAST_Looped_Sim_Output_Platform_201601_V16 from TP_FORECAST_Looped_Sim_Output_Platform;
		MESSAGE CAST(now() as timestamp)||' | Run simulation loop DONE' TO CLIENT;

GO