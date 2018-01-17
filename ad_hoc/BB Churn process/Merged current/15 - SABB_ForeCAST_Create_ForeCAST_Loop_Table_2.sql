CREATE OR REPLACE PROCEDURE SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 (
	@ForeCAST_Start_Wk INT
	, @ForeCAST_End_Wk INT
	, @true_sample_rate REAL
	) AS

BEGIN
	message convert(TIMESTAMP, now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Initializaing' TO client

	SET TEMPORARY
	OPTION Query_Temp_Space_Limit = 0

	DECLARE @multiplier BIGINT
	DROP TABLE IF EXISTS #Loop_Sky_Calendar 
	DROP TABLE IF EXISTS Pred_Rates 
	DROP TABLE IF EXISTS ForeCAST_Loop_Table_2
	
	SET @multiplier = DATEPART(millisecond, now()) + 1 
	
	message convert(TIMESTAMP, now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Initializaing DONE' TO client
	message convert(TIMESTAMP, now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating Sub-Structures' TO client

	SELECT *
	INTO #Loop_Sky_Calendar
	FROM /*CITeam.*/Subs_Calendar(@ForeCAST_Start_Wk / 100, @ForeCAST_End_Wk / 100)

	UPDATE ForeCAST_Loop_Table AS a
	SET a.subs_week_and_year = sc.subs_week_and_year
		, a.subs_week_of_year = sc.subs_week_of_year
		, a.subs_year = sc.subs_year
	FROM ForeCAST_Loop_Table AS a
	INNER JOIN #Loop_Sky_Calendar AS sc ON sc.calendar_date = a.end_date + 7

	-- update the segments
	UPDATE ForeCAST_Loop_Table
	SET SABB_forecast_segment = CASE WHEN BB_status_code IN ('AB', 'PC', 'BCRQ') THEN BB_status_code ELSE SABB_forecast_segment END
		, segment_SA = 			CASE WHEN BB_status_code IN ('AB', 'PC', 'BCRQ') THEN BB_status_code ELSE segment_SA END

	UPDATE ForeCAST_Loop_Table
	SET rand_action_Pipeline = rand(number() * @multiplier + 1)
		, rand_BB_Offer_Applied = rand(number() * @multiplier + 2)
		, rand_Intrawk_BB_NotSysCan = rand(number() * @multiplier + 3)
		, rand_Intrawk_BB_SysCan = rand(number() * @multiplier + 4)
		, rand_BB_Pipeline_Status_Change = rand(number() * @multiplier + 5)
		, rand_New_Off_Dur = rand(number() * @multiplier + 6)
		, rand_BB_NotSysCan_Duration = rand(number() * @multiplier + 7)

	SELECT a.*
	INTO Pred_Rates
	FROM ForeCAST_Loop_Table AS a
	
		message convert(TIMESTAMP, now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating Structures DONE ' TO client
		-- 3.04 Calculate Proportions for random event allocation and bring in event rates --
		-- we have calculated above the distributions for TA_Calls and WC_Calls
		--     we need to treat somehow the overlapping customers - that go in PC and AB
		-- we calculate first the cuscan and then we exclude the cuscan in order to caluclate the syscan
		-- we set syscan rank as null
		message convert(TIMESTAMP, now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating ForeCAST_Loop_Table_2' TO client

	SELECT a.*
		, 'SABB_forecast_segment_count' = COUNT() OVER (PARTITION BY a.SABB_forecast_segment)
		, 'SABB_Group_rank' = convert(REAL, row_number() OVER (PARTITION BY a.SABB_forecast_segment ORDER BY rand_action_Pipeline ASC))
		, 'pct_SABB_COUNT' = SABB_Group_rank / SABB_forecast_segment_count
		, 'SABB_Churn' = convert(TINYINT, 0)
		, 'pred_bb_enter_SysCan_rate' = convert(REAL, 0)
		, 'pred_bb_enter_SysCan_YoY_Trend' = convert(REAL, 0)
		, 'cum_bb_enter_SysCan_rate' = convert(REAL, 0)
		, 'pred_bb_enter_CusCan_rate' = convert(REAL, 0)
		, 'pred_bb_enter_CusCan_YoY_Trend' = convert(REAL, 0)
		, 'cum_bb_enter_CusCan_rate' = convert(REAL, 0)
		, 'pred_bb_enter_HM_rate' = convert(REAL, 0)
		, 'pred_bb_enter_HM_YoY_Trend' = convert(REAL, 0)
		, 'cum_bb_enter_HM_rate' = convert(REAL, 0)
		, 'pred_bb_enter_3rd_party_rate' = convert(REAL, 0)
		, 'pred_bb_enter_3rd_party_YoY_Trend' = convert(REAL, 0)
		, 'cum_bb_enter_3rd_party_rate' = convert(REAL, 0)
		, 'pred_BB_Offer_Applied_rate' = convert(REAL, 0)
		, 'pred_BB_Offer_Applied_YoY_Trend' = convert(REAL, 0)
		, 'cum_BB_Offer_Applied_rate' = convert(REAL, 0)
		, 'DTV_AB' = convert(TINYINT, 0)
		, 'BB_Offer_Applied' = convert(TINYINT, 0)
	INTO ForeCAST_Loop_Table_2
	FROM Pred_Rates AS a 
	
	message convert(TIMESTAMP, now()) || ' | ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating ForeCAST_Loop_Table_2: ' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX id1 ON ForeCAST_Loop_Table_2 (account_number)
	CREATE lf INDEX id2 ON ForeCAST_Loop_Table_2 (churn_type)
	CREATE lf INDEX id3 ON ForeCAST_Loop_Table_2 (SABB_forecast_segment)
	CREATE lf INDEX id4 ON ForeCAST_Loop_Table_2 (BB_Status_Code)
	CREATE lf INDEX id5 ON ForeCAST_Loop_Table_2 (subs_week_and_year)
	CREATE lf INDEX id6 ON ForeCAST_Loop_Table_2 (weekid)
	COMMIT WORK 
	
	message convert(TIMESTAMP, now()) || ' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - THE END ! ' TO client
END

-- Grant execute rights to the members of CITeam
GRANT EXECUTE
	ON SABB_ForeCAST_Create_ForeCAST_Loop_Table_2
	TO CITeam
		, vespa_group_low_security
GO

