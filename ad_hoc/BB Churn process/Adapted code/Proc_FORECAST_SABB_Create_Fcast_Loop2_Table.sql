-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 
	( IN ForeCAST_Start_Wk INT
	, IN ForeCAST_End_Wk INT
	, IN true_sample_rate FLOAT
	) 
AS 
BEGIN
	MESSAGE CAST(now() as timestamp)||' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Initializaing' TO CLIENT
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
	DECLARE @multiplier BIGINT
	
	DROP TABLE IF EXISTS #Loop_Sky_Calendar
	DROP TABLE IF EXISTS Pred_Rates
	DROP TABLE IF EXISTS ForeCAST_Loop_Table_2
	
	SET @multiplier = DATEPART(millisecond, now()) + 1

	MESSAGE CAST(now() as timestamp)||' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Initializaing DONE' TO CLIENT
	-- update the dates first
	MESSAGE CAST(now() as timestamp)||' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating Sub-Structures' TO CLIENT
	
	SELECT * INTO #Loop_Sky_Calendar FROM CITeam.Subs_Calendar(ForeCAST_Start_Wk / 100, ForeCAST_End_Wk / 100)

	UPDATE ForeCAST_Loop_Table a
	SET subs_week_and_year = sc.subs_week_and_year
		, subs_week_of_year = sc.subs_week_of_year
	FROM ForeCAST_Loop_Table a
	INNER JOIN #Loop_Sky_Calendar sc ON sc.calendar_date = a.end_date + 7

	-- update the segments
	UPDATE ForeCAST_Loop_Table 
	SET   SABB_forecast_segment 		= CASE 	WHEN BB_status_code IN ( 'AB'  , 'PC', 'BCRQ' ) THEN BB_status_code 
										ELSE SABB_forecast_segment END
		, segment_SA 	= CASE WHEN BB_status_code IN ( 'AB'  , 'PC', 'BCRQ' ) THEN BB_status_code 
										ELSE segment_SA END												
										
			
	UPDATE ForeCAST_Loop_Table
	SET   rand_action_Pipeline 				= rand(number(*) * @multiplier + 1)
		, rand_BB_Offer_Applied				= rand(number(*) * @multiplier + 2)
		, rand_Intrawk_BB_NotSysCan 		= rand(number(*) * @multiplier + 3)
		, rand_Intrawk_BB_SysCan 			= rand(number(*) * @multiplier + 4)
		, rand_BB_Pipeline_Status_Change 	= rand(number(*) * @multiplier + 5)
		, rand_New_Off_Dur 					= rand(number(*) * @multiplier + 6)
		, rand_BB_NotSysCan_Duration 		= rand(number(*) * @multiplier + 7)
		
	
	-- 3.02 Add Random Number and Segment Size for random event allocations later --
	/* --============================= Missing fields from the master of retention/ bb pipeline/ cust_fcast table: PO_Pipeline_Cancellations; Same_Day_Cancels; SC_Gross_Terminations -===========*/
		SELECT a.*
		INTO Pred_Rates
		FROM ForeCAST_Loop_Table AS a
		-- this can removed once cuscan and syscan are foreCASTed - ----
		/*
		LEFT JOIN (SELECT Cuscan_foreCAST_segment 
				, sum(PO_Pipeline_Cancellations) + sum(Same_Day_Cancels) AS Cuscan
			FROM citeam.DTV_FCAST_Weekly_Base
			WHERE Subs_year = 2015 AND subs_week = (SELECT max(subs_week_of_year) FROM ForeCAST_Loop_Table) GROUP BY Cuscan_foreCAST_segment ) AS b ON a.Cuscan_foreCAST_segment = b.Cuscan_foreCAST_segment
		LEFT JOIN (SELECT Syscan_foreCAST_segment 
				, sum(SC_Gross_Terminations) AS Syscan 
			FROM citeam.DTV_FCAST_Weekly_Base WHERE Subs_year = 2015 AND subs_week = (SELECT max(subs_week_of_year) FROM ForeCAST_Loop_Table) GROUP BY Syscan_foreCAST_segment ) AS c ON a.syscan_foreCAST_segment = c.syscan_foreCAST_segment
			*/
			
	MESSAGE CAST(now() as timestamp)||' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating Structures DONE ' TO CLIENT

	-- 3.04 Calculate Proportions for random event allocation and bring in event rates --
	-- we have calculated above the distributions for TA_Calls and WC_Calls
	--     we need to treat somehow the overlapping customers - that go in PC and AB
	-- we calculate first the cuscan and then we exclude the cuscan in order to caluclate the syscan
	-- we set syscan rank as null
	
	MESSAGE CAST(now() as timestamp)||' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating ForeCAST_Loop_Table_2' TO CLIENT
	
		SELECT a.* 
			, COUNT(*) OVER (PARTITION BY a.SABB_forecast_segment) 													AS SABB_forecast_segment_count
			, CAST(row_number() OVER (PARTITION BY a.SABB_forecast_segment ORDER BY rand_action_Pipeline) AS FLOAT) 	AS SABB_Group_rank
			, SABB_Group_rank / SABB_forecast_segment_count 														AS pct_SABB_COUNT
			, CAST(0 AS TINYINT) 																		AS SABB_Churn
			-- cuscan
			, Cast(0 AS FLOAT) AS pred_bb_enter_SysCan_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_SysCan_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_bb_enter_SysCan_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_CusCan_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_CusCan_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_bb_enter_CusCan_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_HM_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_HM_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_bb_enter_HM_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_3rd_party_rate
			, Cast(0 AS FLOAT) AS pred_bb_enter_3rd_party_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_bb_enter_3rd_party_rate
			, Cast(0 AS FLOAT) AS pred_BB_Offer_Applied_rate
			, Cast(0 AS FLOAT) AS pred_BB_Offer_Applied_YoY_Trend
			, Cast(0 AS FLOAT) AS cum_BB_Offer_Applied_rate
			, cast(0 AS TINYINT) AS DTV_AB ----??? we need equivalent of this for our four froms of pipeline entry?
			, cast(0 AS TINYINT) AS BB_Offer_Applied
		
		INTO ForeCAST_Loop_Table_2
		FROM Pred_Rates AS a

MESSAGE CAST(now() as timestamp)||' | ForeCAST_Create_ForeCAST_Loop_Table_2 - Generating ForeCAST_Loop_Table_2: '||@@rowcount TO CLIENT
COMMIT 
CREATE HG INDEX id1 ON ForeCAST_Loop_Table_2(account_number)
CREATE LF INDEX id2 ON ForeCAST_Loop_Table_2(churn_type)
CREATE LF INDEX id3 ON ForeCAST_Loop_Table_2(SABB_forecast_segment)
CREATE LF INDEX id4 ON ForeCAST_Loop_Table_2(BB_Status_Code)
CREATE LF INDEX id5 ON ForeCAST_Loop_Table_2(subs_week_and_year)
CREATE LF INDEX id6 ON ForeCAST_Loop_Table_2(weekid)
COMMIT

MESSAGE CAST(now() as timestamp)||' | SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 - THE END ! ' TO CLIENT
END 

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON SABB_ForeCAST_Create_ForeCAST_Loop_Table_2 TO CITeam, vespa_group_low_security

-- Test it
-- Call CITeam.ForeCAST_Create_ForeCAST_Loop_Table_2(201601, 201652)
