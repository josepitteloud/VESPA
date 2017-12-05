CREATE OR REPLACE PROCEDURE Forecast_SABB_Rates (IN Forecast_Start_Wk INT) result (
	Subs_Week SMALLINT
	, SABB_forecast_segment VARCHAR(50)
	, pred_SysCan_rate REAL
	, pred_CusCan_rate REAL
	, pred_HM_rate REAL
	, pred_3rd_party_rate REAL
	, pred_BB_Offer_Applied_rate REAL
	, prev_SysCan_rate REAL
	, prev_CusCan_rate REAL
	, prev_HM_rate REAL
	, prev_3rd_party_rate REAL
	, prev_BB_Offer_Applied_rate REAL
	)

BEGIN
	DECLARE var_End_date DATE;
	DECLARE _1st_Wk1 INT;
	DECLARE _Lst_Wk INT;

	SELECT *
	INTO #sky_calendar
	FROM subs_calendar(Forecast_Start_Wk / 100 - 3, Forecast_Start_Wk / 100);

	SET var_End_date = (
			SELECT max(calendar_date - 7)
			FROM #sky_calendar
			WHERE subs_week_and_year = Forecast_Start_Wk
			);
	SET _Lst_Wk = (
			SELECT max(subs_week_and_year)
			FROM #sky_calendar
			WHERE calendar_date = var_End_date
			);
	SET _1st_Wk1 = CASE WHEN remainder(Forecast_Start_Wk, 100) < 52 THEN (Forecast_Start_Wk / 100 - 3) * 100 + remainder(Forecast_Start_Wk, 100) ELSE (Forecast_Start_Wk / 100 - 2) * 100 + 1 END;
	SET TEMPORARY
	OPTION Query_Temp_Space_Limit = 0;

	DROP TABLE

	IF EXISTS #SABB_weekly_agg;
		SELECT subs_year
			, subs_week_of_year AS subs_week
			, cast(subs_week_and_year AS INT) AS subs_week_and_year
			, node_sa AS SABB_forecast_segment
			, Count() AS n
			, cast(sum(BB_Offer_Applied) AS REAL) AS BB_Offer_Applied
			, cast(sum(bb_enter_SysCan) AS REAL) AS bb_enter_SysCan
			, cast(sum(bb_enter_CusCan) AS REAL) AS bb_enter_CusCan
			, cast(sum(bb_enter_HM) AS REAL) AS bb_enter_HM
			, cast(sum(bb_enter_3rd_party) AS REAL) AS bb_enter_3rd_party
			, dense_rank() OVER (
				ORDER BY subs_week_and_year DESC
				) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, (week_id / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) AS Max_Subs_Week
		INTO #SABB_weekly_agg
		FROM pitteloudj.DTV_Fcast_Weekly_Base_2 AS agg
		WHERE subs_week_and_year BETWEEN _1st_Wk1 AND _Lst_Wk
		GROUP BY subs_year
			, subs_week
			, subs_week_and_year
			, node_sa;

	UPDATE #SABB_weekly_agg
	SET subs_week = subs_week - 1
	WHERE Max_Subs_Week = 53;

	UPDATE #SABB_weekly_agg
	SET Subs_Week_And_Year = Subs_Year * 100 + subs_week;

	DELETE
	FROM #SABB_weekly_agg
	WHERE subs_week = 0;

	DROP TABLE

	IF EXISTS #SABB_forecast_summary_1;
		SELECT subs_week
			, sabb_forecast_segment
			, sum(CASE WHEN Week_Position = 'Prev' THEN n ELSE 0 END) AS prev_n
			, sum(CASE WHEN Week_Position = 'Prev' THEN agg.BB_Offer_Applied ELSE 0 END) AS prev_BB_Offer_Applied
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_SysCan ELSE 0 END) AS prev_bb_enter_SysCan
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_CusCan ELSE 0 END) AS prev_bb_enter_CusCan
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_HM ELSE 0 END) AS prev_bb_enter_HM
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_3rd_party ELSE 0 END) AS prev_bb_enter_3rd_party
			, sum(CASE WHEN Week_Position = 'Curr' THEN n ELSE 0 END) AS curr_n
			, sum(CASE WHEN Week_Position = 'Curr' THEN agg.BB_Offer_Applied ELSE 0 END) AS curr_BB_Offer_Applied
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_SysCan ELSE 0 END) AS curr_bb_enter_SysCan
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_CusCan ELSE 0 END) AS curr_bb_enter_CusCan
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_HM ELSE 0 END) AS curr_bb_enter_HM
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_3rd_party ELSE 0 END) AS curr_bb_enter_3rd_party
			, sum(0) AS LQ_n
			, sum(0) AS LQ_BB_Offer
		INTO #SABB_forecast_summary_1
		FROM #SABB_weekly_agg AS agg
		GROUP BY subs_week
			, sabb_forecast_segment;

	DROP TABLE

	IF EXISTS #cuscan_forecast_summary_LQ;
		SELECT SABB_forecast_segment
			, sum(n) AS LQ_n
			, sum(BB_offer_applied) AS LQ_BB_Offer
		INTO #cuscan_forecast_summary_LQ
		FROM #SABB_weekly_agg
		WHERE last_quarter = 'Y'
		GROUP BY SABB_forecast_segment;

	message cast(now() AS TIMESTAMP) || ' | 7' TO client;

	UPDATE #SABB_forecast_summary_1 AS a
	SET a.LQ_n = b.LQ_n
		, a.LQ_BB_Offer = b.LQ_BB_Offer
	FROM #SABB_forecast_summary_1 AS a
	LEFT JOIN #cuscan_forecast_summary_LQ AS b ON a.SABB_forecast_segment = b.SABB_forecast_segment;

	DROP TABLE

	IF EXISTS #SABB_forecast_summary_2;
		SELECT *
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_SysCan AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_SysCan_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_CusCan AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_CusCan_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_HM AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_HM_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_3rd_party AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_3rd_party_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_BB_Offer_Applied AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_BB_Offer_Applied_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_SysCan AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_SysCan_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_CusCan AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_CusCan_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_HM AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_HM_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_3rd_party AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_3rd_party_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_BB_Offer_Applied AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_BB_Offer_Applied_rate
			, CASE WHEN LQ_n >= 100 THEN cast(LQ_BB_Offer AS REAL) / cast(LQ_n AS REAL) ELSE 0 END AS LQ_DTV_Offer_rate
			, CASE WHEN (curr_n + prev_n) >= 100 THEN cast(curr_n AS REAL) / (cast(curr_n AS REAL) + cast(prev_n AS REAL)) ELSE 0 END AS curr_share
			, 1 - curr_share AS prev_share
		INTO #SABB_forecast_summary_2
		FROM #SABB_forecast_summary_1;

	DROP TABLE

	IF EXISTS #SABB_forecast_summary_3;
		SELECT *
			, curr_bb_enter_SysCan_rate AS pred_bb_enter_SysCan_rate
			, curr_bb_enter_CusCan_rate AS pred_bb_enter_CusCan_rate
			, curr_bb_enter_HM_rate AS pred_bb_enter_HM_rate
			, curr_bb_enter_3rd_party_rate AS pred_bb_enter_3rd_party_rate
			, curr_BB_Offer_Applied_rate AS pred_BB_Offer_Applied_rate
		INTO #SABB_forecast_summary_3
		FROM #SABB_forecast_summary_2;

	SELECT subs_week
		, SABB_forecast_segment
		, pred_bb_enter_SysCan_rate
		, pred_bb_enter_CusCan_rate
		, pred_bb_enter_HM_rate
		, pred_bb_enter_3rd_party_rate
		, pred_BB_Offer_Applied_rate
		, prev_bb_enter_SysCan_rate
		, prev_bb_enter_CusCan_rate
		, prev_bb_enter_HM_rate
		, prev_bb_enter_3rd_party_rate
		, prev_BB_Offer_Applied_rate
	FROM #SABB_forecast_summary_3 AS a
END
GO

CREATE OR REPLACE PROCEDURE SABB_Regression_Coefficient (IN LV INT, IN Regression_Yrs TINYINT) 
	result (
	LV INT
	, Metric VARCHAR(30)
	, Fcast_Segment VARCHAR(100)
	, Grad_Coeff REAL
	, Intercept_Coeff REAL
	)

BEGIN
	DECLARE Dynamic_SQL VARCHAR(1000);
	DECLARE Y3W52 INT;
	DECLARE Y1W01 INT;

	SELECT * INTO #Sky_Calendar FROM CITeam.Subs_Calendar(LV / 100 - Regression_Yrs - 1, LV / 100);

	DROP TABLE IF EXISTS #Regr_Wks;
	
	SELECT cast(sc.subs_week_and_year AS INT) AS Subs_week_and_year
			, Row_Number() OVER (ORDER BY Subs_week_and_year DESC ) AS Wk_Rnk
		INTO #Regr_Wks
		FROM #sky_calendar AS sc
		WHERE cast(sc.subs_Week_and_year AS INT) < LV AND Subs_Week_of_year <> 53
		GROUP BY Subs_week_and_year;

	DELETE FROM #Regr_Wks WHERE Wk_Rnk > Regression_Yrs * 52 + 13;

	SET Y1W01 = (SELECT min(Subs_week_and_year) FROM #Regr_Wks );
	SET Y3W52 = (SELECT max(Subs_week_and_year) FROM #Regr_Wks);

	DROP TABLE IF EXISTS #SABB_weekly_agg;
	
	SELECT subs_year
			, subs_week_of_year AS subs_week
			, cast(subs_week_and_year AS INT) AS subs_week_and_year
			, node_sa AS SABB_forecast_segment
			, Count(*) 								AS n
			, cast(sum(CASE WHEN bb_offer_rem_and_end = - 9999 THEN 0 ELSE 1 END) AS REAL) AS BB_Offer_Applied
			, cast(sum(bb_enter_SysCan) AS REAL) 	AS bb_enter_SysCan
			, cast(sum(bb_enter_CusCan) AS REAL) 	AS bb_enter_CusCan
			, cast(sum(bb_enter_HM) AS REAL) 		AS bb_enter_HM
			, cast(sum(bb_enter_3rd_party) AS REAL) AS bb_enter_3rd_party
			, dense_rank() OVER (ORDER BY subs_week_and_year DESC	) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, (week_id / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) AS Max_Subs_Week
		INTO #SABB_weekly_agg
		FROM pitteloudj.DTV_Fcast_Weekly_Base_2 AS agg ---??? update this source later
		WHERE subs_week_and_year BETWEEN Y1W01 AND Y3W52 AND subs_week <> 53
		GROUP BY subs_year
			, subs_week
			, subs_week_and_year
			, node_sa;-- segment

	-----------------------------------------------------------------------------------------------------------
	----------  Pipeline entry events -----------------------------------------------------------------
	-----------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #Regr_inputs;
		SELECT quarter_id
			, agg.SABB_forecast_segment
			, row_number() OVER (PARTITION BY agg.SABB_forecast_segment ORDER BY quarter_id DESC ) AS x
			, sum(cast(BB_enter_CusCan AS REAL)) / sum(n) AS BB_enter_CusCan
			, sum(cast(BB_enter_SysCan AS REAL)) / sum(n) AS BB_enter_SysCan
			, sum(cast(BB_enter_HM AS REAL)) / sum(n) AS BB_enter_HM
			, sum(cast(BB_enter_3rd_party AS REAL)) / sum(n) AS BB_enter_3rd_party
			, sum(cast(BB_Offer_Applied AS REAL)) / sum(n) AS BB_Offer_Applied
			, x * x AS xx
			, x * BB_enter_CusCan AS x_BB_enter_CusCan
			, x * BB_enter_SysCan AS x_BB_enter_SysCan
			, x * BB_enter_HM AS x_BB_enter_HM
			, x * BB_enter_3rd_party AS x_BB_enter_3rd_party
			, x * BB_Offer_Applied AS x_BB_Offer_Applied
			, Sum(n) AS cell_n
			, cast(NULL AS REAL) AS BB_enter_CusCan_regression
			, cast(NULL AS REAL) AS BB_enter_SysCan_regression
			, cast(NULL AS REAL) AS BB_enter_HM_regression
			, cast(NULL AS REAL) AS BB_enter_3rd_party_regression
			, cast(NULL AS REAL) AS BB_Offer_Applied_regression
		INTO #Regr_inputs
		FROM #SABB_weekly_agg AS agg
		GROUP BY quarter_id
			, agg.SABB_forecast_segment;

	DROP TABLE IF EXISTS #Regr_coeff;
	
	SELECT SABB_forecast_segment
			, sum(cell_n) AS n
			, sum(cast(cell_n AS BIGINT) * x) AS sum_x
			, sum(cast(cell_n AS BIGINT) * xx) AS sum_xx
			, sum(cell_n * BB_enter_CusCan) AS sum_BB_enter_CusCan
			, sum(cell_n * BB_enter_SysCan) AS sum_BB_enter_SysCan
			, sum(cell_n * BB_enter_HM) AS sum_BB_enter_HM
			, sum(cell_n * BB_enter_3rd_party) AS sum_BB_enter_3rd_party
			, sum(cell_n * BB_Offer_Applied) AS sum_BB_Offer_Applied
			, sum(cell_n * x_BB_enter_CusCan) AS sum_x_BB_enter_CusCan
			, sum(cell_n * x_BB_enter_SysCan) AS sum_x_BB_enter_SysCan
			, sum(cell_n * x_BB_enter_HM) AS sum_x_BB_enter_HM
			, sum(cell_n * x_BB_enter_3rd_party) AS sum_x_BB_enter_3rd_party
			, sum(cell_n * x_BB_Offer_Applied) AS sum_x_BB_Offer_Applied
			, cast(NULL AS REAL) AS b0_BB_enter_CusCan
			, cast(NULL AS REAL) AS b0_BB_enter_SysCan
			, cast(NULL AS REAL) AS b0_BB_enter_HM
			, cast(NULL AS REAL) AS b0_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b0_BB_Offer_Applied
			, cast(NULL AS REAL) AS b1_BB_enter_CusCan
			, cast(NULL AS REAL) AS b1_BB_enter_SysCan
			, cast(NULL AS REAL) AS b1_BB_enter_HM
			, cast(NULL AS REAL) AS b1_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b1_BB_Offer_Applied
		INTO #Regr_coeff
		FROM #Regr_inputs
		GROUP BY SABB_forecast_segment
		HAVING n > 1000;

	UPDATE #Regr_coeff
	SET b1_BB_enter_CusCan = (sum_x_BB_enter_CusCan - (sum_BB_enter_CusCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff
	SET b1_BB_enter_SysCan = (sum_x_BB_enter_SysCan - (sum_BB_enter_SysCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff
	SET b1_BB_enter_HM = (sum_x_BB_enter_HM - (sum_BB_enter_HM * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff
	SET b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party - (sum_BB_enter_3rd_party * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff
	SET b1_BB_Offer_Applied = (sum_x_BB_Offer_Applied - (sum_BB_Offer_Applied * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff
	SET b0_BB_enter_CusCan = sum_BB_enter_CusCan / n - b1_BB_enter_CusCan * sum_x / n;

	UPDATE #Regr_coeff
	SET b0_BB_enter_SysCan = sum_BB_enter_SysCan / n - b1_BB_enter_SysCan * sum_x / n;

	UPDATE #Regr_coeff
	SET b0_BB_enter_HM = sum_BB_enter_HM / n - b1_BB_enter_HM * sum_x / n;

	UPDATE #Regr_coeff
	SET b0_BB_enter_3rd_party = sum_BB_enter_3rd_party / n - b1_BB_enter_3rd_party * sum_x / n;

	UPDATE #Regr_coeff
	SET b0_BB_Offer_Applied = sum_BB_Offer_Applied / n - b1_BB_Offer_Applied * sum_x / n;

	---------------------------------------------------------------------------------------------------
	-- Set proc outputs -------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------------------
	SELECT LV
		, 'CusCan Entry' AS Metric
		, SABB_forecast_segment AS forecast_segment
		, b1_BB_enter_CusCan
		, b0_BB_enter_CusCan
	FROM #Regr_coeff
	
	UNION ALL
	
	SELECT LV
		, 'SysCan Entry' AS Metric
		, SABB_forecast_segment AS forecast_segment
		, b1_BB_enter_SysCan
		, b0_BB_enter_SysCan
	FROM #Regr_coeff
	
	UNION ALL
	
	SELECT LV
		, 'HM Entry' AS Metric
		, SABB_forecast_segment AS forecast_segment
		, b1_BB_enter_HM
		, b0_BB_enter_HM
	FROM #Regr_coeff
	
	UNION ALL
	
	SELECT LV
		, '3rd Party Entry' AS Metric
		, SABB_forecast_segment AS forecast_segment
		, b1_BB_enter_3rd_party
		, b0_BB_enter_3rd_party
	FROM #Regr_coeff
	
	UNION ALL
	
	SELECT LV
		, 'BB Offer Applied' AS Metric
		, SABB_forecast_segment AS forecast_segment
		, b1_BB_Offer_Applied
		, b0_BB_Offer_Applied
	FROM #Regr_coeff
END
GO

CREATE OR REPLACE PROCEDURE Intraweek_PCs_Dist 
		(IN ForeCAST_Start_Week INT) 
	result (
	Churn_type VARCHAR(10)
	, Status_code VARCHAR(4)
	, Next_Status_Code VARCHAR(4)
	, PC_ReAC_Offer_Applied TINYINT
	, PCs INT
	, IntaWk_PC_Lower_Pctl REAL
	, IntaWk_PC_Upper_Pctl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | Intraweek_PCs_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, mor.event_dt + 2) AS PC_Event_End_Dt
		, mor.PC_Effective_To_Dt
		, mor.PC_Effective_To_Dt - datepart(weekday, mor.PC_Effective_To_Dt + 2) AS PC_Effective_To_End_Dt
		, mor.account_number
		, MoR.PC_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS PC_ReAC_Offer_Applied
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #Acc_PC_Events_Same_Week
	FROM citeam.Broadband_Comms_Pipeline AS MoR
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt AND MoR.PC_Next_Status_Code = 'AC' 
									AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' AND oua.subs_type = 'Broadband DSL Line'
	WHERE mor.event_dt BETWEEN (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) -- Last 6 Wk PC conversions
			AND ( SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
			AND mor.status_code = 'PC';

	SELECT Coalesce(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'PC') AS Next_Status_Code
		, cast(CASE Next_Status_Code 	WHEN 'AC' THEN 1 
										WHEN 'CN' THEN 2 
										WHEN 'BCRQ' THEN 3 
										WHEN 'AB' THEN 4 
										WHEN 'SC' THEN 5 
										WHEN 'PO' THEN 5 
										ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.PC_ReAC_Offer_Applied ELSE 0 END AS INT) AS PC_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk ASC , PC_ReAC_Offer_Applied ASC ) AS Row_ID
		, churn_type
		, COUNT() AS PCs
	INTO #PC_Events_Same_Week
	FROM #Acc_PC_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, PC_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC ) AS Cum_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type) AS Total_PCs
		, cast(Cum_PCs AS REAL) / Total_PCs AS IntaWk_PC_Upper_Pctl
	INTO #PC_Events
	FROM #PC_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type;

	SELECT pc1.churn_type
		, 'PC' AS Status_code
		, pc1.Next_Status_Code
		, pc1.PC_ReAC_Offer_Applied
		, pc1.PCs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) AS IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #PC_Events AS pc1
	LEFT JOIN #PC_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.churn_type = pc2.churn_type;

	message cast(now() AS TIMESTAMP) || ' | Intraweek_PCs_Dist - COMPLETED' TO client
END
GO

CREATE OR REPLACE PROCEDURE Intraweek_PCs_Dist 
		(IN ForeCAST_Start_Week INT) 
	result (
	Churn_type VARCHAR(10)
	, Status_code VARCHAR(4)
	, Next_Status_Code VARCHAR(4)
	, PC_ReAC_Offer_Applied TINYINT
	, PCs INT
	, IntaWk_PC_Lower_Pctl REAL
	, IntaWk_PC_Upper_Pctl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | Intraweek_PCs_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, mor.event_dt + 2) AS PC_Event_End_Dt
		, mor.PC_Effective_To_Dt
		, mor.PC_Effective_To_Dt - datepart(weekday, mor.PC_Effective_To_Dt + 2) AS PC_Effective_To_End_Dt
		, mor.account_number
		, MoR.PC_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS PC_ReAC_Offer_Applied
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #Acc_PC_Events_Same_Week
	FROM citeam.Broadband_Comms_Pipeline AS MoR
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt AND MoR.PC_Next_Status_Code = 'AC' 
									AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' AND oua.subs_type = 'Broadband DSL Line'
	WHERE mor.event_dt BETWEEN (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) -- Last 6 Wk PC conversions
			AND ( SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
			AND mor.status_code = 'PC';

	SELECT Coalesce(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'PC') AS Next_Status_Code
		, cast(CASE Next_Status_Code 	WHEN 'AC' THEN 1 
										WHEN 'CN' THEN 2 
										WHEN 'BCRQ' THEN 3 
										WHEN 'AB' THEN 4 
										WHEN 'SC' THEN 5 
										WHEN 'PO' THEN 5 
										ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.PC_ReAC_Offer_Applied ELSE 0 END AS INT) AS PC_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk ASC , PC_ReAC_Offer_Applied ASC ) AS Row_ID
		, churn_type
		, COUNT() AS PCs
	INTO #PC_Events_Same_Week
	FROM #Acc_PC_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, PC_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC ) AS Cum_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type) AS Total_PCs
		, cast(Cum_PCs AS REAL) / Total_PCs AS IntaWk_PC_Upper_Pctl
	INTO #PC_Events
	FROM #PC_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type;

	SELECT pc1.churn_type
		, 'PC' AS Status_code
		, pc1.Next_Status_Code
		, pc1.PC_ReAC_Offer_Applied
		, pc1.PCs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) AS IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #PC_Events AS pc1
	LEFT JOIN #PC_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.churn_type = pc2.churn_type;

	message cast(now() AS TIMESTAMP) || ' | Intraweek_PCs_Dist - COMPLETED' TO client
END
GO

CREATE OR REPLACE PROCEDURE Intraweek_ABs_Dist (IN ForeCAST_Start_Week INT) result (
	Churn_type VARCHAR(10)
	, Status_Code VARCHAR(4)
	, Next_Status_Code VARCHAR(4)
	, AB_ReAC_Offer_Applied TINYINT
	, ABs INT
	, IntaWk_AB_Lower_Pctl REAL
	, IntaWk_AB_Upper_Pctl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | Intraweek_ABs_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, event_dt + 2) AS AB_Event_End_Dt
		, mor.AB_Effective_To_Dt
		, mor.AB_Effective_To_Dt - datepart(weekday, mor.AB_Effective_To_Dt + 2) AS AB_Effective_To_End_Dt
		, mor.account_number
		, MoR.AB_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS AB_ReAC_Offer_Applied
		, CASE WHEN Enter_SysCan > 0 THEN 'SysCan' WHEN Enter_CusCan > 0 THEN 'CusCan' WHEN Enter_HM > 0 THEN 'HM' WHEN Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END AS Churn_type
	INTO #Acc_AB_Events_Same_Week
	FROM citeam.Broadband_Comms_Pipeline AS MoR
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt AND MoR.AB_Next_Status_Code = 'AC' AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' AND oua.subs_type = 'Broadband DSL Line'
	WHERE mor.event_dt BETWEEN (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
						AND (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
			AND Mor.status_code = 'AB';

	SELECT Churn_type
		, Coalesce(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'AB') AS Next_Status_Code
		, cast(CASE WHEN Next_Status_Code = 'AC' AND AB_ReAC_Offer_Applied = 0 THEN 1 
					WHEN Next_Status_Code = 'AC' AND AB_ReAC_Offer_Applied = 1 THEN 2 
					WHEN Next_Status_Code = 'CN' THEN 3 WHEN Next_Status_Code = 'BCRQ' THEN 4 
					WHEN Next_Status_Code = 'PC' THEN 5 WHEN Next_Status_Code = 'PO' THEN 6 
					WHEN Next_Status_Code = 'SC' THEN 7 
					ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.AB_ReAC_Offer_Applied ELSE 0 END AS INT) AS AB_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY Churn_type ORDER BY Next_Status_Code_Rnk ASC) AS Row_ID
		, count(*) AS ABs
	INTO #AB_Events_Same_Week
	FROM #Acc_AB_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, AB_ReAC_Offer_Applied
		, Churn_type;

	DROP TABLE #Acc_AB_Events_Same_Week;

	SELECT Row_ID
		, Churn_type
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, sum(ABs) OVER (
			PARTITION BY Churn_type ORDER BY Row_ID ASC
			) AS Cum_ABs
		, sum(ABs) OVER (PARTITION BY Churn_type) AS Total_ABs
		, cast(Cum_ABs AS REAL) / Total_ABs AS IntaWk_PC_Upper_Pctl
	INTO #AB_Events
	FROM #AB_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, Churn_type;

	DROP TABLE #AB_Events_Same_Week;

	SELECT pc1.Churn_type
		, 'AB' AS Status_code
		, pc1.Next_Status_Code
		, pc1.AB_ReAC_Offer_Applied
		, pc1.ABs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) AS IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #AB_Events AS pc1
	LEFT JOIN #AB_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.Churn_type = pc2.Churn_type;

	message cast(now() AS TIMESTAMP) || ' | Intraweek_ABs_Dist - Completed' TO client

	
	
END
GO

CREATE OR REPLACE PROCEDURE Intraweek_BCRQ_Dist 
		(IN ForeCAST_Start_Week INT) 
	result (
			Churn_type VARCHAR(10)
		, Status_Code VARCHAR(4)
		, Next_Status_Code VARCHAR(4)
		, BCRQ_ReAC_Offer_Applied TINYINT
		, BCRQ INT
		, IntaWk_BCRQ_Lower_Pctl REAL
		, IntaWk_BCRQ_Upper_Pctl REAL
		)

BEGIN
	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, event_dt + 2) AS BCRQ_Event_End_Dt
		, mor.BCRQ_Effective_To_Dt
		, mor.BCRQ_Effective_To_Dt - datepart(weekday, mor.BCRQ_Effective_To_Dt + 2) AS BCRQ_Effective_To_End_Dt
		, mor.account_number
		, MoR.BCRQ_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS BCRQ_ReAC_Offer_Applied
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #Acc_BCRQ_Events_Same_Week
	FROM citeam.Broadband_Comms_Pipeline AS MoR
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number 
									AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt 
									AND MoR.PC_Next_Status_Code = 'AC' 
									AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%'
									AND oua.subs_type = 'Broadband DSL Line'
	WHERE mor.event_dt BETWEEN (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
							AND (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
		AND mor.status_code = 'BCRQ';

	SELECT Coalesce(CASE WHEN BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'BCRQ') AS Next_Status_Code
		, cast(CASE Next_Status_Code 	WHEN 'AC' THEN 1 
										WHEN 'CN' THEN 2 
										WHEN 'BCRQ' THEN 3 
										WHEN 'AB' THEN 4 
										WHEN 'SC' THEN 5 
										WHEN 'PO' THEN 5 
										ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt THEN MoR.BCRQ_ReAC_Offer_Applied ELSE 0 END AS INT) AS BCRQ_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk ASC , BCRQ_ReAC_Offer_Applied ASC ) AS Row_ID
		, churn_type
		, COUNT() AS BCRQs
	INTO #BCRQ_Events_Same_Week
	FROM #Acc_BCRQ_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, BCRQs
		, churn_type
		, SUM(BCRQs) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC ) AS Cum_BCRQs
		, SUM(BCRQs) OVER (PARTITION BY churn_type) AS Total_BCRQs
		, cast(Cum_BCRQs AS REAL) / Total_BCRQs AS IntaWk_BCRQ_Upper_Pctl
	INTO #BCRQ_Events
	FROM #BCRQ_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, BCRQs
		, churn_type;

	SELECT pc1.churn_type
		, 'BCRQ' AS Status_code
		, pc1.Next_Status_Code
		, pc1.BCRQ_ReAC_Offer_Applied
		, pc1.BCRQs
		, Coalesce(pc2.IntaWk_BCRQ_Upper_Pctl, 0) AS IntaWk_BCRQ_Lower_Pctl
		, pc1.IntaWk_BCRQ_Upper_Pctl
	FROM #BCRQ_Events AS pc1
	LEFT JOIN #BCRQ_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.churn_type = pc2.churn_type
END
GO

CREATE OR REPLACE PROCEDURE PC_Duration_Dist 
			(IN ForeCAST_Start_Week INT) 
	result (
		churn_type VARCHAR(10)
	, Days_To_churn INT
	, PCs INT
	, Total_PCs INT
	, PC_Days_Lower_Prcntl REAL
	, PC_Days_Upper_Prcntl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | PC_Duration_Dist - BEGIN ' TO client;

	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT CASE WHEN status_code IN ('PC') THEN 1 WHEN status_code IN ('BCRQ') AND churn_type IN ('CusCan' , '3rd Party' , 'HM' ) THEN 2 ELSE 0 END AS source
		, event_dt - datepart(weekday, event_dt + 2) AS PC_Event_End_Dt
		, CASE 	WHEN source = 1 THEN PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) 
				WHEN source = 2 THEN PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) 
				ELSE NULL END AS PC_Effective_To_End_Dt
		, CASE 	WHEN source = 1 THEN PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) 
				WHEN source = 2 THEN PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) 
				ELSE NULL END AS PC_Future_Sub_End_Dt
		, PC_Future_Sub_Effective_Dt - PC_Event_End_Dt AS Days_To_churn
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #PC_Events_Days_To_Intended_Churn
	FROM citeam.Broadband_Comms_Pipeline
	WHERE event_dt BETWEEN (SELECT max(calendar_date - 6 * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
					AND (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
			AND (status_code IN ('PC') OR (status_code IN ('BCRQ') AND churn_type IN ('CusCan', '3rd Party', 'HM'))) 
			AND Days_To_churn > 0;

	SELECT churn_type
		, Days_To_churn
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Days_To_churn ASC) AS Row_ID
		, count() AS PCs
		, SUM(PCs) OVER (PARTITION BY churn_type) AS Total_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Days_To_churn ASC) AS Cum_PCs
		, cast(PCs AS REAL) / Total_PCs AS Pct_PCs
		, cast(NULL AS REAL) AS PC_Days_Lower_Prcntl
		, cast(Cum_PCs AS REAL) / Total_PCs AS PC_Days_Upper_Prcntl
	INTO #PC_Days_Prcntl
	FROM #PC_Events_Days_To_Intended_Churn
	GROUP BY Days_To_churn
		, churn_type
	ORDER BY churn_type ASC
		, Days_To_churn ASC;

	UPDATE #PC_Days_Prcntl AS pc1
	SET pc1.PC_Days_Lower_Prcntl = Coalesce(pc2.PC_Days_Upper_Prcntl, 0)
	FROM #PC_Days_Prcntl AS pc1
	LEFT JOIN #PC_Days_Prcntl AS pc2 ON pc2.Row_ID = pc1.Row_ID - 1;

	SELECT churn_type
		, Days_To_churn
		, PCs
		, Total_PCs
		, PC_Days_Lower_Prcntl
		, PC_Days_Upper_Prcntl
	FROM #PC_Days_Prcntl;

	message cast(now() AS TIMESTAMP) || ' | PC_Duration_Dist - BEGIN ' TO client
END
GO


CREATE OR REPLACE PROCEDURE PC_Status_Movement_Probabilities 
			(IN ForeCAST_Start_Week INT) 
	result (
		churn_type VARCHAR(10)
	, Initial_status_code VARCHAR(10)
	, Wks_To_Intended_Churn VARCHAR(20)
	, Status_Code_EoW VARCHAR(4)
	, Status_Code_EoW_Rnk INT
	, ReAC_Offer_Applied TINYINT
	, Cnt INT
	, Cum_Total_Cohort INT
	, Total_Cohort INT
	, Percentile_Lower_Bound REAL
	, Percentile_Upper_Bound REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | PC_Status_Movement_Probabilities - Initialising Environment' TO client;

	SELECT * INTO #Sky_Calendar FROM CITeam.Subs_Calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT MoR.account_number
		, MoR.status_code
		, MoR.event_dt
		, MoR.PC_Future_Sub_Effective_Dt
		, cast(MoR.PC_Future_Sub_Effective_Dt - datepart(weekday, MoR.PC_Future_Sub_Effective_Dt + 2) + 7 AS DATE) AS PC_Future_Sub_Effective_Dt_End_Dt
		, MoR.PC_Effective_To_Dt
		, MoR.PC_Next_status_code AS Next_status_code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS PC_ReAC_Offer_Applied
		, CASE 	WHEN MoR.Enter_SysCan > 0 THEN 'SysCan' 
				WHEN MoR.Enter_CusCan > 0 THEN 'CusCan' 
				WHEN MoR.Enter_HM > 0 THEN 'HM' 
				WHEN MoR.Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #PC_Intended_Churn
	FROM CITEAM.Broadband_Comms_Pipeline AS mor
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number 
								AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt 
								AND MoR.PC_Next_Status_Code = 'AC' 
								AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
								AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
								AND oua.subs_type = 'Broadband DSL Line'
	WHERE MoR.PC_Future_Sub_Effective_Dt BETWEEN (SELECT min(calendar_date - 6 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
										AND (SELECT min(calendar_date - 1) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
		AND (MoR.status_code IN ('PC') OR (MoR.status_code IN ('BCRQ') AND churn_type IN ('CusCan', '3rd Party', 'HM'))) 
		AND MoR.PC_Future_Sub_Effective_Dt IS NOT NULL 
		AND Next_status_code IS NOT NULL 
		AND MoR.PC_Effective_To_Dt <= MoR.PC_Future_Sub_Effective_Dt;

	SELECT PCs.*
		, CASE 	WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 >= 5 THEN 'Churn in next 6+ wks' END AS Wks_To_Intended_Churn
		, sc.Calendar_date AS End_date
		, CASE 	WHEN sc.calendar_date + 7 BETWEEN event_dt AND PC_Effective_To_Dt THEN 'PC' 
				WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END AS Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt AND Status_Code_EoW = 'AC' THEN PCs.PC_ReAC_Offer_Applied 
				ELSE 0 END AS PC_ReAC_Offer_Applied_EoW
		, (CASE Status_Code_EoW WHEN 'AC' THEN 1 
								WHEN 'CN' THEN 2 
								WHEN 'BCRQ' THEN 3 
								WHEN 'PO' THEN 4 
								WHEN 'AB' THEN 5 
								WHEN 'SC' THEN 6 END) - PC_ReAC_Offer_Applied_EoW AS Status_Code_EoW_Rnk
	INTO #PC_PL_Status
	FROM #PC_Intended_Churn AS PCs
	INNER JOIN #sky_calendar AS sc ON sc.calendar_date BETWEEN PCs.event_dt AND PCs.PC_Effective_To_Dt - 1 
			AND sc.subs_last_day_of_week = 'Y';

	SELECT churn_type
		, status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, PC_ReAC_Offer_Applied_EoW
		, count() AS PCs
		, SUM(PCs) OVER (PARTITION BY Wks_To_Intended_Churn ORDER BY Status_Code_EoW_Rnk ASC) AS Cum_Total_Cohort_PCs
		, SUM(PCs) OVER (PARTITION BY Wks_To_Intended_Churn) AS Total_Cohort_PCs
		, cast(NULL AS REAL) AS PC_Percentile_Lower_Bound
		, cast(Cum_Total_Cohort_PCs AS REAL) / Total_Cohort_PCs AS PC_Percentile_Upper_Bound
	INTO #PC_Percentiles
	FROM #PC_PL_Status
	GROUP BY status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, PC_ReAC_Offer_Applied_EoW
		, churn_type
	ORDER BY Wks_To_Intended_Churn ASC
		, Status_Code_EoW_Rnk ASC
		, Status_Code_EoW ASC
		, PC_ReAC_Offer_Applied_EoW ASC
		, churn_type ASC;

	message cast(now() AS TIMESTAMP) || ' | PC_Status_Movement_Probabilities - PC_Percentiles Populated: ' || @@rowcount TO client;

	UPDATE #PC_Percentiles AS pcp
	SET PC_Percentile_Lower_Bound = cast(Coalesce(pcp2.PC_Percentile_Upper_Bound, 0) AS REAL)
	FROM #PC_Percentiles AS pcp
	LEFT JOIN #PC_Percentiles AS pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Status_Code_EoW_Rnk = pcp.Status_Code_EoW_Rnk - 1;

	message cast(now() AS TIMESTAMP) || ' | PC_Status_Movement_Probabilities - Initialising Completed' TO client;

	SELECT * FROM #PC_Percentiles
END
GO

CREATE OR REPLACE PROCEDURE AB_Status_Movement_Probabilities 
			(IN @ForeCAST_Start_Week INT) 
	result (
		Churn_type VARCHAR(10)
	, Status_Code VARCHAR(4)
	, Wks_To_Intended_Churn VARCHAR(20)
	, Status_Code_EoW VARCHAR(4)
	, Status_Code_EoW_Rnk INT
	, ReAC_Offer_Applied TINYINT
	, Cnt INT
	, Cum_Total_Cohort_ABs INT
	, Total_Cohort_ABs INT
	, Percentile_Lower_Bound REAL
	, Percentile_Upper_Bound REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - Initialization Begin' TO client;

	SELECT * INTO #Sky_Calendar FROM CITeam.Subs_Calendar(@ForeCAST_Start_Week / 100 - 1, @ForeCAST_Start_Week / 100);

	SELECT mor.account_number
		, mor.status_code
		, mor.event_dt
		, mor.AB_Future_Sub_Effective_Dt
		, cast(mor.AB_Future_Sub_Effective_Dt - datepart(weekday, mor.AB_Future_Sub_Effective_Dt + 2) + 7 AS DATE) AS AB_Future_Sub_Effective_Dt_End_Dt
		, mor.AB_Effective_To_Dt
		, mor.AB_Next_status_code AS Next_status_code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS AB_ReAC_Offer_Applied
		, CASE 	WHEN mor.Enter_SysCan > 0 THEN 'SysCan' 
				WHEN mor.Enter_CusCan > 0 THEN 'CusCan' 
				WHEN mor.Enter_HM > 0 THEN 'HM' 
				WHEN mor.Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #AB_Intended_Churn
	FROM citeam.Broadband_Comms_Pipeline AS mor
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number 
									AND oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt 
									AND MoR.AB_Next_Status_Code = 'AC' 
									AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
									AND oua.subs_type = 'Broadband DSL Line'
	WHERE AB_Future_Sub_Effective_Dt BETWEEN (SELECT min(calendar_date - 6 * 7) FROM #sky_calendar WHERE subs_week_and_year = @ForeCAST_Start_Week) 
									AND (SELECT min(calendar_date - 1) FROM #sky_calendar WHERE subs_week_and_year = @ForeCAST_Start_Week) 
									AND AB_Future_Sub_Effective_Dt IS NOT NULL 
									AND AB_Next_status_code IS NOT NULL 
									AND AB_Effective_To_Dt <= AB_Future_Sub_Effective_Dt 
									AND (status_code = 'AB' OR (status_code = 'BCRQ' AND Churn_type = 'SysCan'));

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts Begin' TO client;

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - Index created' TO client;

	SELECT a.account_number
		, a.event_dt
		, b.status_code AS next_cancel_status
		, b.effective_from_dt AS next_cancel_dt
		, RANK() OVER (PARTITION BY a.account_number ORDER BY b.effective_from_dt ASC, b.cb_row_id ASC) AS rankk
	INTO #AB_BCRQ
	FROM #AB_Intended_Churn AS a
	INNER JOIN cust_subs_hist AS b ON a.account_number = b.account_number AND a.AB_Effective_To_Dt <= b.effective_from_dt
	WHERE b.subscription_sub_type = 'Broadband DSL Line' AND b.status_code_changed = 'Y' AND b.status_code IN ('PO', 'SC', 'CN') 
		AND a.Next_status_code IN ('BCRQ') 
		AND b.effective_from_dt <> b.effective_to_dt 
		AND b.prev_status_code IN ('BCRQ');

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 1/2' TO client;

	DELETE FROM #AB_BCRQ WHERE rankk > 1;

	UPDATE #AB_Intended_Churn AS a
	SET AB_Future_Sub_Effective_Dt = next_cancel_dt
		, Next_status_code = next_cancel_status
	FROM #AB_Intended_Churn AS a
	INNER JOIN #AB_BCRQ AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt AND a.status_code = 'AB';

	DROP TABLE #AB_BCRQ;

	UPDATE #AB_Intended_Churn
	SET Next_status_code = 'CN'
	WHERE Next_status_code = 'BCRQ';

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 2/2' TO client;

	SELECT AB_s.*
		, CASE 	WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 5 THEN 'Churn in next 6 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 6 THEN 'Churn in next 7 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 7 THEN 'Churn in next 8 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 8 THEN 'Churn in next 9 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 >= 9 THEN 'Churn in next 10+ wks' END AS Wks_To_Intended_Churn
		, sc.Calendar_date AS End_date
		, CASE 	WHEN sc.calendar_date + 7 BETWEEN event_dt AND AB_Effective_To_Dt THEN 'AB' 
				WHEN sc.calendar_date + 7 BETWEEN AB_Effective_To_Dt AND AB_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END AS Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 = AB_Effective_To_Dt - datepart(weekday, AB_Effective_To_Dt + 2) + 7 AND Status_Code_EoW = 'AC' THEN AB_s.AB_ReAC_Offer_Applied ELSE 0 END AS AB_ReAC_Offer_Applied_EoW
		, (CASE WHEN Status_Code_EoW = 'AC' AND AB_ReAC_Offer_Applied = 0 THEN 1 
				WHEN Status_Code_EoW = 'AC' AND AB_ReAC_Offer_Applied = 1 THEN 2 
				WHEN Status_Code_EoW = 'CN' THEN 3 
				WHEN Status_Code_EoW = 'BCRQ' THEN 4 
				WHEN Status_Code_EoW = 'PC' THEN 5 
				WHEN Status_Code_EoW = 'PO' THEN 6 
				WHEN Status_Code_EoW = 'SC' THEN 7 ELSE 0 END) AS Status_Code_EoW_Rnk
	INTO #AB_PL_Status
	FROM #AB_Intended_Churn AS AB_s
	INNER JOIN #sky_calendar AS sc ON sc.calendar_date BETWEEN AB_s.event_dt AND AB_s.AB_Effective_To_Dt - 1 AND sc.subs_last_day_of_week = 'Y';

	SELECT Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, AB_ReAC_Offer_Applied_EoW
		, count() AS AB_s
		, Sum(AB_s) OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type ORDER BY Status_Code_EoW_Rnk ASC) AS Cum_Total_Cohort_ABs
		, Sum(AB_s) OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type) AS Total_Cohort_ABs
		, cast(NULL AS REAL) AS AB_Percentile_Lower_Bound
		, cast(Cum_Total_Cohort_ABs AS REAL) / Total_Cohort_ABs AS AB_Percentile_Upper_Bound
		, Row_Number() OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type ORDER BY Status_Code_EoW_Rnk ASC) AS Row_ID
		, Churn_type
		, status_code
	INTO #AB_Percentiles
	FROM #AB_PL_Status
	GROUP BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, AB_ReAC_Offer_Applied_EoW
		, Churn_type
		, status_code
	ORDER BY status_code ASC
		, Churn_type ASC
		, Wks_To_Intended_Churn ASC
		, Status_Code_EoW_Rnk ASC
		, Status_Code_EoW ASC
		, AB_ReAC_Offer_Applied_EoW ASC;

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - AB_Percentiles populated: ' || @@rowcount TO client;

	UPDATE #AB_Percentiles AS pcp
	SET AB_Percentile_Lower_Bound = cast(Coalesce(pcp2.AB_Percentile_Upper_Bound, 0) AS REAL)
	FROM #AB_Percentiles AS pcp
	LEFT JOIN #AB_Percentiles AS pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Row_ID = pcp.Row_ID - 1 AND pcp.Churn_type = pcp2.Churn_type;

	SELECT Churn_type
		, status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, AB_ReAC_Offer_Applied_EoW
		, AB_s
		, Cum_Total_Cohort_ABs
		, Total_Cohort_ABs
		, AB_Percentile_Lower_Bound
		, AB_Percentile_Upper_Bound
	FROM #AB_Percentiles;

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - Completed' TO client
END
GO

CREATE OR REPLACE PROCEDURE SABB_Offer_Applied_Duration_Dist 
		(IN Forecast_Start_Wk INT
		, IN Num_Wks INT) 
	result (
		Offer_segment VARCHAR(30)
	, Total_Offer_Duration_Mth INT
	, Weekly_Avg_New_Offers INT
	, Total_New_Offers INT
	, Cum_New_Offers INT
	, Dur_Pctl_Lower_Bound REAL
	, Dur_Pctl_Upper_Bound REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | SABB_Offer_Applied_Duration_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM subs_calendar(Forecast_Start_Wk / 100 - 1, Forecast_Start_Wk / 100);

	SELECT CASE overall_offer_segment 	WHEN '2.(BB)A1.Acquisition/Upgrade' THEN 'Activations' 
										WHEN '2.(BB)B1.TA' THEN 'TA' 
										WHEN '2.(BB)B2.CoE' THEN 'Other' 
										WHEN '2.(BB)B3.PAT' THEN 'Other' 
										WHEN '2.(BB)B4.Pipeline ReInstate' THEN 'Reactivations' 
										WHEN '2.(BB)B5.Other Retention' THEN 'Reactivations' 
										WHEN '2.(BB)C1.BB Package Movement' THEN 'Other' 
										WHEN '2.(BB)C2.Offer On Call' THEN 'Other' 
										WHEN '2.(BB)C4.Other' THEN 'Other' END AS overall_offer_segment
		, Total_Offer_Duration_Mth
		, COUNT() / Num_Wks AS Weekly_Avg_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment) AS Total_New_Offers
		, Sum(Weekly_Avg_New_Offers) OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth ASC) AS Cum_New_Offers
		, cast(Cum_New_Offers AS REAL) / Total_New_Offers AS Pctl_New_Offers
		, Row_Number() OVER (PARTITION BY overall_offer_segment ORDER BY Total_Offer_Duration_Mth ASC) AS Dur_Rnk
	INTO #Offer_Dur
	FROM citeam.offer_usage_all AS oua
	WHERE offer_start_dt_Actual BETWEEN (SELECT max(calendar_date - 7 - Num_Wks * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk ) 
									AND (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk ) 
			AND Total_Offer_Duration_Mth <= 36 
			AND offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual 
			AND Subs_Type = 'Broadband DSL Line' 
			AND lower(offer_dim_description) NOT LIKE '%price protection%' 
			AND oua.overall_offer_segment_grouped_1 <> 'Price Protection'
	GROUP BY overall_offer_segment
		, Total_Offer_Duration_Mth;

	message cast(now() AS TIMESTAMP) || ' | SABB_Offer_Applied_Duration_Dist - Offer_Dur table completed: ' || @@rowcount TO client;

	SELECT dur1.overall_offer_segment
		, dur1.Total_Offer_Duration_Mth
		, dur1.Weekly_Avg_New_Offers
		, dur1.Total_New_Offers
		, dur1.Cum_New_Offers
		, Coalesce(dur2.Pctl_New_Offers, 0) AS Dur_Pctl_Lower_Bound
		, dur1.Pctl_New_Offers AS Dur_Pctl_Upper_Bound
	FROM #Offer_Dur AS dur1
	LEFT JOIN #Offer_Dur AS dur2 ON dur2.overall_offer_segment = dur1.overall_offer_segment AND dur2.Dur_Rnk = dur1.Dur_Rnk - 1;

	message cast(now() AS TIMESTAMP) || ' | SABB_Offer_Applied_Duration_Dist - COMPLETED' TO client;
	DROP TABLE #Sky_Calendar ;
	DROP TABLE #Offer_Dur;
END
GO

CREATE OR REPLACE PROCEDURE SABB_Build_ForeCAST_New_Cust_Sample (IN LV INT)

BEGIN
	DECLARE Obs_Dt DATE;
	DECLARE @multiplier BIGINT;

	SET @multiplier = DATEPART(millisecond, now()) + 738;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample - Build_ForeCAST_New_Cust_Sample -  Begin ' TO client;

	TRUNCATE TABLE FORECAST_New_Cust_Sample;

	SET Obs_Dt = (SELECT max(calendar_date) FROM citeam.subs_calendar(LV / 100 - 1, LV / 100) WHERE Subs_Week_And_Year < LV );
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	INSERT INTO FORECAST_New_Cust_Sample
	SELECT end_date
		, subs_year AS year
		, subs_week_of_year AS week
		, subs_week_and_year AS year_week
		, account_number
		, BB_status_code
		, CASE WHEN BB_Active > 0 THEN 'BB' ELSE 'Non BB' END AS BB_Segment
		, CASE WHEN ROI > 0 THEN 'ROI' ELSE 'UK' END AS country
		, BB_package
		, CASE 	WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI' 
				WHEN BB_Enter_SysCan > 0 THEN 'SysCan' 
				WHEN BB_Enter_CusCan > 0 THEN 'CusCan' 
				WHEN BB_Enter_HM > 0 THEN 'HM' 
				WHEN BB_Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
		, cast(NULL AS VARCHAR(4)) AS BB_Status_Code_EoW
		, CASE 	WHEN trim(simple_segment) IN ('1 Secure') THEN '1 Secure' 
				WHEN trim(simple_segment) IN ('2 Start') THEN '2 Start' 
				WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
				WHEN trim(simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
				WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
				WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
				ELSE 'Other/Unknown' END AS Simple_Segments
		, BB_offer_rem_and_end_raw
		, cast(NULL AS INT) AS BB_offer_rem_and_end
		, BB_tenure_raw
		, cast(NULL AS INT) AS BB_tenure
		, my_sky_login_3m_raw
		, cast(NULL AS INT) AS my_sky_login_3m
		, talk_type
		, home_owner_status
		, BB_all_calls_1m_raw
		, cast(NULL AS INT) AS BB_all_calls_1m
		, cast(0 AS TINYINT) AS node_SA
		, cast(NULL AS VARCHAR(20)) AS segment_SA
		, cast(NULL AS DATE) AS PL_Future_Sub_Effective_Dt
		, cast(NULL AS VARCHAR(100)) AS DTV_Activation_Type
		, Curr_Offer_start_Date_BB
		, curr_offer_end_date_Intended_BB
		, Prev_offer_end_date_BB
		, cast(NULL AS DATE) AS Future_offer_Start_dt
		, cast(NULL AS DATE) AS Future_end_Start_dt
		, BB_latest_act_dt
		, BB_first_act_dt
		, rand(number() * @multiplier) AS rand_sample
		, cast(NULL AS VARCHAR(10)) AS sample
		, CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END AS SABB_flag
	FROM pitteloudj.cust_fcast_weekly_base_2
	WHERE end_date BETWEEN Obs_Dt - 5 * 7 AND Obs_Dt 
			AND bb_active = 1 
			AND dtv_active = 0 
			AND BB_latest_act_dt BETWEEN (end_date - 6) 
			AND end_date 
			AND BB_latest_act_dt IS NOT NULL;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  Insert Into FORECAST_New_Cust_Sample completed: ' || @@rowcount TO client;

	COMMIT WORK;

	SELECT a.account_number
		, a.end_date
		, B.subs_year
		, B.subs_week_of_year
		, CASE WHEN b.Enter_SysCan > 0 THEN 'SysCan' WHEN b.Enter_CusCan > 0 THEN 'CusCan' WHEN b.Enter_HM > 0 THEN 'HM' WHEN b.Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END AS Churn_type
		, RANK() OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC) AS week_rnk
	INTO #t1
	FROM FORECAST_New_Cust_Sample AS a
	INNER JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND a.year = b.subs_year AND a.week = b.subs_week_of_year
	WHERE a.Churn_type = 'MULTI';

	COMMIT WORK;

	DELETE FROM #t1 WHERE week_rnk > 1;
	CREATE hg INDEX IO1 ON #t1 (account_number);
	CREATE DTTM INDEX IO2 ON #t1 (end_date);
	COMMIT WORK;

	UPDATE FORECAST_New_Cust_Sample AS a
	SET a.Churn_type = b.Churn_type
	FROM FORECAST_New_Cust_Sample AS a
	INNER JOIN #t1 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  Churn_type fixed: ' || @@rowcount TO client;

	DROP TABLE #t1;

	COMMIT WORK;

	UPDATE FORECAST_New_Cust_Sample AS a
	SET a.BB_offer_rem_and_end = b.BB_offer_rem_and_end
		, a.BB_tenure = b.BB_tenure
		, a.my_sky_login_3m = b.my_sky_login_3m
		, a.BB_all_calls_1m = b.BB_all_calls_1m
		, a.node_SA = b.node_SA
		, a.segment_SA = b.segment_SA
	FROM FORECAST_New_Cust_Sample AS a
	INNER JOIN pitteloudj.DTV_FCAST_WEEKLY_BASE_2 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  DTV_fcast variables updated: ' || @@rowcount TO client;

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET sample.PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC';

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'AB';

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ';

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE PL_Future_Sub_Effective_Dt IS NULL;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  COMPLETED' TO client
END
GO


CREATE OR REPLACE PROCEDURE SABB_Forecast_Activation_Vols (IN Y2W01 INT, IN Y3W52 INT) 
	result (
		Subs_Week_Of_Year SMALLINT
	, Reinstates INT
	, Acquisitions INT
	, New_Customers INT
	)

BEGIN
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Initialization begin ' TO client;

	DROP TABLE IF EXISTS #Sky_Calendar;
		CREATE TABLE #Sky_Calendar (calendar_date DATE NULL, subs_week_of_year INT NULL,);

	CREATE lf INDEX idx_1 ON #Sky_Calendar (calendar_date);

	INSERT INTO #Sky_Calendar
	SELECT calendar_date
		, subs_week_of_year
	FROM CITeam.Subs_Calendar(Y2W01 / 100, Y3W52 / 100)
	WHERE subs_week_and_year BETWEEN Y2W01 AND Y3W52 AND subs_last_day_of_week = 'Y' AND subs_week_of_year <> 53;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Calendar setup' TO client;

	SELECT end_date
		, cast(NULL AS INT) AS Subs_Week_Of_Year
		, sum(CASE WHEN BB_latest_act_dt BETWEEN (end_date - 6) AND end_date AND (BB_first_act_dt < BB_latest_act_dt) THEN 1 ELSE 0 END) AS Reinstates
		, sum(CASE WHEN BB_latest_act_dt BETWEEN (end_date - 6) AND end_date AND (BB_first_act_dt = BB_latest_act_dt) THEN 1 ELSE 0 END) AS Acquisitions
		, Reinstates + Acquisitions AS New_Customers
	INTO #Activation_Vols
	FROM pitteloudj.cust_fcast_weekly_Base_2 AS base
	WHERE base.end_date = ANY (SELECT calendar_date FROM #Sky_Calendar ) 
		AND DTV_ACTIVE = 0 
		AND bb_active = 1
	GROUP BY end_date;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Activations table DONE:' || @@rowcount TO client;

	UPDATE #Activation_Vols AS av
	SET subs_week_of_year = sc.subs_week_of_year
	FROM #Activation_Vols AS av
	INNER JOIN #Sky_Calendar AS sc ON sc.calendar_date = av.end_date;

	SELECT Subs_Week_Of_Year
		, Avg(Coalesce(av.Reinstates, 0)) AS Reinstates
		, Avg(Coalesce(av.Acquisitions, 0)) AS Acquisitions
		, Avg(Coalesce(av.New_Customers, 0)) AS New_Customers
	FROM #Activation_Vols AS av
	GROUP BY Subs_Week_Of_Year;

	message cast(now() AS TIMESTAMP) || ' | Forecast_Activation_Vols - Proc completed :' TO client
END
GO


CREATE OR REPLACE PROCEDURE SABB_Forecast_Create_Opening_Base (
	@Forecast_Start_Wk INT
	, @sample_pct REAL
	)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Begining - Initialising Environment' TO client

	IF NOT EXISTS (SELECT tname FROM syscatalog WHERE creator = user_name() AND tabletype = 'TABLE' AND upper(tname) = UPPER('FORECAST_Base_Sample'))
	
	BEGIN
		CREATE TABLE FORECAST_Base_Sample (
			account_number VARCHAR(20) NULL DEFAULT NULL
			, end_date DATETIME NULL DEFAULT NULL
			, subs_year INT NULL DEFAULT NULL
			, subs_week_and_year INT NULL DEFAULT NULL
			, subs_week_of_year TINYINT NULL DEFAULT NULL
			, weekid BIGINT NULL DEFAULT NULL
			, BB_Status_Code VARCHAR(4) NULL DEFAULT NULL
			, BB_Segment VARCHAR(30) NULL DEFAULT NULL
			, country VARCHAR(3) NULL DEFAULT NULL
			, BB_package VARCHAR(50) NULL DEFAULT NULL
			, churn_type VARCHAR(10) NULL DEFAULT NULL
			, BB_offer_rem_and_end_raw INT NULL DEFAULT - 9999
			, BB_offer_rem_and_end INT NULL DEFAULT - 9999
			, BB_tenure_raw INT NULL DEFAULT 0
			, BB_tenure INT NULL DEFAULT 0
			, my_sky_login_3m_raw INT NULL DEFAULT 0
			, my_sky_login_3m INT NULL DEFAULT 0
			, talk_type VARCHAR(30) NULL DEFAULT 'NONE'
			, home_owner_status VARCHAR(20) NULL DEFAULT 'UNKNOWN'
			, BB_all_calls_1m_raw INT NULL DEFAULT 0
			, BB_all_calls_1m INT NULL DEFAULT 0
			, Simple_Segments VARCHAR(13) NULL DEFAULT 'UNKNOWN'
			, node_SA TINYINT NULL DEFAULT 0
			, segment_SA VARCHAR(20) NULL DEFAULT 'UNKNOWN'
			, PL_Future_Sub_Effective_Dt DATETIME NULL DEFAULT NULL
			, DTV_Activation_Type VARCHAR(100) NULL DEFAULT NULL
			, Curr_Offer_start_Date_BB DATETIME NULL DEFAULT NULL
			, Curr_offer_end_date_Intended_BB DATETIME NULL DEFAULT NULL
			, Prev_offer_end_date_BB DATETIME NULL DEFAULT NULL
			, Future_offer_Start_dt DATETIME NULL DEFAULT NULL
			, Future_end_Start_dt DATETIME NULL DEFAULT NULL
			, BB_latest_act_dt DATETIME NULL DEFAULT NULL
			, BB_first_act_dt DATETIME NULL DEFAULT NULL
			, rand_sample REAL NULL DEFAULT NULL
			, sample VARCHAR(10) NULL DEFAULT NULL
			, SABB_flag BIT NOT NULL DEFAULT 0
			) 
			
	message convert (TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - FORECAST_Base_Sample' TO client
	END

	DECLARE @base_date DATE
	DECLARE @true_sample_rate REAL
	DECLARE @multiplier BIGINT

	SET @multiplier = DATEPART(millisecond, now()) + 738
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0

	SELECT * INTO #Sky_Calendar FROM Subs_Calendar(@Forecast_Start_Wk / 100 - 1, @Forecast_Start_Wk / 100)

	SET @base_date = ( SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = @Forecast_Start_Wk )
	SET @multiplier = DATEPART(millisecond, now()) + 1 message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - @base_date: ' || @base_date TO client

	-- drop table if exists #base_sample
	DELETE FROM FORECAST_Base_Sample 
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Cleaning FORECAST_Base_Sample ' TO client

	INSERT INTO FORECAST_Base_Sample
	SELECT account_number
		, end_date
		, subs_year
		, 'subs_week_and_year' = convert(INT, subs_week_and_year)
		, subs_week_of_year
		, 'weekid' = (subs_year - 2010) * 52 + subs_week_of_year
		, BB_Status_Code
		, 'BB_Segment' = CASE WHEN BB_Active > 0 THEN 'BB' ELSE 'Non BB' END
		, 'country' = CASE WHEN ROI > 0 THEN 'ROI' ELSE 'UK' END
		, BB_package
		, 'Churn_type' = CASE WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI' --- UPDATED next
			WHEN BB_Enter_SysCan > 0 THEN 'SysCan' WHEN BB_Enter_CusCan > 0 THEN 'CusCan' WHEN BB_Enter_HM > 0 THEN 'HM' WHEN BB_Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END
		, BB_offer_rem_and_end_raw
		, 'BB_offer_rem_and_end' = convert(INT, NULL)
		, BB_tenure_raw
		, 'BB_tenure' = convert(INT, NULL)
		, my_sky_login_3m_raw
		, 'my_sky_login_3m' = convert(INT, NULL)
		, talk_type
		, home_owner_status
		, BB_all_calls_1m_raw
		, 'BB_all_calls_1m' = convert(INT, NULL)
		, 'Simple_Segments' = CASE 	WHEN trim(simple_segment) IN ('1 Secure') THEN '1 Secure' 
									WHEN trim(simple_segment) IN ('2 Start') THEN '2 Start' 
									WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
									WHEN trim(simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
									WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
									WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
									ELSE 'Other/Unknown' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
									END
		, 'node_SA' 		= convert(TINYINT, 0)
		, 'segment_SA' 		= convert(VARCHAR(20), NULL)
		, 'PL_Future_Sub_Effective_Dt' 	= convert(DATE, NULL)
		, 'DTV_Activation_Type' 		= convert(VARCHAR(100), NULL)
		, Curr_Offer_start_Date_BB
		, curr_offer_end_date_Intended_BB
		, Prev_offer_end_date_BB
		, 'Future_offer_Start_dt' = convert(DATE, NULL)
		, 'Future_end_Start_dt' = convert(DATE, NULL)
		, BB_latest_act_dt
		, BB_first_act_dt
		, 'rand_sample' = rand(number() * @multiplier)
		, 'sample' = convert(VARCHAR(10), NULL)
		, 'SABB_flag' = CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END
	FROM pitteloudj.cust_fcast_weekly_base_2
	WHERE end_date = @base_date AND bb_active = 1 AND dtv_active = 0 --??? do we need a sabb flag?
		AND BB_latest_act_dt IS NOT NULL --??? do we have this, or a bb_act_date?
		--???? changes to the where clause here?
		
		
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Insert Into FORECAST_Base_Sample completed: ' || @@rowcount TO client

	COMMIT WORK

	SELECT a.account_number
		, a.end_date
		, a.subs_year
		, a.subs_week_of_year
		, 'Churn_type' = CASE WHEN b.Enter_SysCan > 0 THEN 'SysCan' WHEN b.Enter_CusCan > 0 THEN 'CusCan' WHEN b.Enter_HM > 0 THEN 'HM' WHEN b.Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END
		, 'week_rnk' = RANK() OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC, rand_sample ASC)
	INTO #t1
	FROM FORECAST_Base_Sample AS a
	INNER JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND a.end_date >= Event_Dt
	WHERE (a.Churn_type = 'MULTI' OR (a.Churn_type IS NULL AND BB_Status_Code IN ('AB', 'PC', 'BCRQ')))

	COMMIT WORK

	DELETE	FROM #t1
	WHERE week_rnk > 1

	CREATE hg INDEX IO1 ON #t1 (account_number)

	CREATE dttm INDEX IO2 ON #t1 (end_date)

	COMMIT WORK

	UPDATE FORECAST_Base_Sample AS a
	SET a.Churn_type = b.Churn_type
	FROM FORECAST_Base_Sample AS a
	INNER JOIN #t1 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date

	DROP TABLE #t1

	COMMIT WORK

	--		select * from FORECAST_Base_Sample where bb_status_code='BCRQ'
	UPDATE FORECAST_Base_Sample AS a
	SET a.Churn_type = CASE WHEN bb_status_code IN ('PC', 'BCRQ') THEN 'CusCan' ELSE 'SysCan' END
	FROM FORECAST_Base_Sample AS a
	WHERE a.churn_type IS NULL AND bb_status_code IN ('PC', 'BCRQ', 'AB')

	COMMIT WORK 
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Missing churn_types for pipeline entries filled with defaults: ' || @@rowcount TO client

	UPDATE FORECAST_Base_Sample AS a
	SET a.BB_offer_rem_and_end = b.BB_offer_rem_and_end
		, a.BB_tenure = b.BB_tenure
		, a.my_sky_login_3m = b.my_sky_login_3m
		, a.BB_all_calls_1m = b.BB_all_calls_1m
		, a.node_SA = b.node_SA
		, a.segment_SA = b.segment_SA
	FROM FORECAST_Base_Sample AS a
	INNER JOIN pitteloudj.DTV_FCAST_WEEKLY_BASE_2 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - First Update FORECAST_Base_Sample completed: ' || @@rowcount TO client

	---????update this?
	UPDATE FORECAST_Base_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
												AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
												AND MoR.event_dt <= sample.end_date 
												AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC' AND sample.PL_Future_Sub_Effective_Dt IS NULL 
	
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - PC future sub effective dt set ' || @@rowcount TO client

	UPDATE FORECAST_Base_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE BB_Status_Code = 'PC' AND PL_Future_Sub_Effective_Dt IS NULL 
		---????update this?
		------------==================================++++++++++++++++++++++++++++++++==========================================---------------
		------------==================================++++++++++++++++++++++++++++++++==========================================---------------
		------------==================================++++++++++++++++++++++++++++++++==========================================---------------
		message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base - UPDATE AB_Future_Sub_Effective_Dt Begin' TO client

	SELECT a.account_number
		, b.event_dt
		, a.BB_status_code
		, a.end_date
		, 'rankk' = RANK() OVER (PARTITION BY a.account_number ORDER BY b.event_dt DESC)
	INTO #AB_BCRQ_2
	FROM FORECAST_Base_Sample AS a
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND b.event_dt <= a.end_date
	WHERE a.BB_status_code IN ('AB', 'BCRQ') 
		AND PL_Future_Sub_Effective_Dt IS NULL 
		AND b.enter_syscan = 1

	DELETE FROM #AB_BCRQ_2 WHERE rankk > 1 ---- LATEST PL

	SELECT a.account_number
		, a.event_dt
		, a.end_date
		, a.BB_status_code
		, 'next_cancel_status' = b.status_code
		, 'next_cancel_dt' = b.effective_from_dt
		, 'rankk' = RANK() OVER (PARTITION BY a.account_number ORDER BY b.effective_from_dt ASC, b.cb_row_id ASC)
	INTO #AB_BCRQ_3
	FROM #AB_BCRQ_2 AS a
	INNER JOIN cust_subs_hist AS b ON a.account_number = b.account_number AND a.event_dt <= b.effective_from_dt
	WHERE b.subscription_sub_type = 'Broadband DSL Line' 
			AND b.status_code_changed = 'Y' 
			AND b.effective_from_dt <> b.effective_to_dt 
			AND b.prev_status_code IN ('BCRQ') 
			AND b.status_code IN ('CN', 'SC', 'PO')

	DELETE FROM #AB_BCRQ_3 WHERE rankk > 1 

	CREATE hg INDEX id1 ON #AB_BCRQ_3 (account_number)

	UPDATE FORECAST_Base_Sample AS a
	SET PL_Future_Sub_Effective_Dt = next_cancel_dt
	FROM FORECAST_Base_Sample AS a
	INNER JOIN #AB_BCRQ_3 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date

	DROP TABLE #AB_BCRQ_3 message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 1/2' TO client

	----------------------------------------------------------------
	------------- Accounts in the pipeline 
	----------------------------------------------------------------
	SELECT a.account_number
		, a.end_date
		, b.event_dt
		, 'randx' = RAND(convert(INT, RIGHT(a.account_number, 6)) * DATEPART(ms, GETDATE()))
		, 'rankk' = RANK() OVER (PARTITION BY a.account_number ORDER BY b.event_dt DESC)
	INTO #AB_2
	FROM FORECAST_Base_Sample AS a
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND b.event_dt <= a.end_date
	WHERE PL_Future_Sub_Effective_Dt IS NULL 
		AND a.BB_status_code IN ('AB') 
		AND b.enter_syscan = 1

	DELETE FROM #AB_2 WHERE rankk > 1

	SELECT *
		, 'next_cancel_dt' = CASE 	WHEN randx <= .25 THEN DATEADD(day, 15, event_dt) 
									WHEN randx BETWEEN .25 AND .79 THEN DATEADD(day, 60, event_dt) 
									WHEN randx >= .79 THEN DATEADD(day, 65, event_dt) ELSE event_dt END
	INTO #AB_3
	FROM #AB_2

	
	DROP TABLE #AB_2
	
	UPDATE FORECAST_Base_Sample AS a
	SET PL_Future_Sub_Effective_Dt = next_cancel_dt
	FROM FORECAST_Base_Sample AS a
	INNER JOIN #AB_3 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 2/2' TO client

	DROP TABLE #AB_3
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	----------------------------------------
	UPDATE FORECAST_Base_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
													AND MoR.BCRQ_Future_Sub_Effective_Dt > sample.end_date 
													AND MoR.event_dt <= sample.end_date 
													AND (MoR.BCRQ_effective_to_dt > sample.end_date OR MoR.BCRQ_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ' AND sample.PL_Future_Sub_Effective_Dt IS NULL
		
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - BCRQ future sub effective dt set ' || @@rowcount TO client

	UPDATE FORECAST_Base_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE BB_Status_Code = 'BCRQ' AND PL_Future_Sub_Effective_Dt IS NULL

	--sample to speed up processing
	UPDATE FORECAST_Base_Sample
	SET sample = CASE WHEN rand_sample < @sample_pct THEN 'A' ELSE 'B' END
		
END

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON SABB_Forecast_Create_Opening_Base TO CITeam , vespa_group_low_security
GO



CREATE OR REPLACE PROCEDURE SABB_my_sky_login_prob AS

BEGIN
	message convert(TIMESTAMP, now()) || ' | SABB_my_sky_login_prob - Initialization begin ' TO client

	DECLARE @mx_dt DATE

	SET @mx_dt = (SELECT max(end_date) FROM FORECAST_Base_Sample )

	IF EXISTS (SELECT tname FROM syscatalog WHERE creator = USER_NAME() AND UPPER(tname) = UPPER('SABB_my_sky_login_prob_TABLE') AND tabletype = 'TABLE' )
	DROP TABLE SABB_my_sky_login_prob_TABLE

	SELECT account_number
		, 'segment_sa' 	= convert(VARCHAR(20), NULL)
		, 'curr_count' 	= MAX(CASE WHEN end_date = @mx_dt THEN my_sky_login_3m_raw ELSE 0 END)
		, 'count_1w' 	= MAX(CASE WHEN end_date = DATEADD(day, - 7, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'Calls_LW' 	= CASE WHEN curr_count < count_1w THEN 0 ELSE curr_count - count_1w END
		, 'L_12' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 12, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'L_9' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 9, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'L_6' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 6, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'L_3' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 3, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'Prob_Group' 	= CASE WHEN L_12 = 0 THEN 1 WHEN L_9 = 0 THEN 2 WHEN L_6 = 0 THEN 3 WHEN L_3 = 0 THEN 4 ELSE 5 -- Picking the longest Group
			END
	INTO #mysky_prob_1
	FROM pitteloudj.cust_fcast_weekly_base_2
	WHERE bb_active = 1 AND dtv_active = 0 
			-- AND account_number = ANY (SELECT DISTINCT account_number FROM FORECAST_Base_Sample) 
			AND BB_latest_act_dt IS NOT NULL
	GROUP BY account_number

	COMMIT WORK
	CREATE hg INDEX id1 ON #mysky_prob_1 (account_number)
	COMMIT WORK

	UPDATE #mysky_prob_1 AS a
	SET a.segment_sa = b.segment_sa
	FROM #mysky_prob_1 AS a
	INNER JOIN pitteloudj.DTV_fcast_weekly_base_2 AS b ON a.account_number = b.account_number AND b.end_Date = @mx_dt

	COMMIT WORK

	SELECT 'Prob_Group' = 1
		, Calls_LW
		, segment_sa
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW ASC)
	INTO #mysky_prob_2
	FROM #mysky_prob_1
	WHERE L_12 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 2
		, Calls_LW
		, segment_sa
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW ASC)
	FROM #mysky_prob_1
	WHERE L_9 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 3
		, Calls_LW
		, segment_sa
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW ASC)
	FROM #mysky_prob_1
	WHERE L_6 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 4
		, Calls_LW
		, segment_sa
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW ASC)
	FROM #mysky_prob_1
	WHERE L_3 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 5
		, Calls_LW
		, segment_sa
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW ASC )
	FROM #mysky_prob_1
	WHERE Prob_Group = 5
	GROUP BY Calls_LW
		, segment_sa

	SELECT *
		, 'UPPER_LIMIT' = SUM(prob) OVER ( PARTITION BY Prob_Group , segment_sa ORDER BY RANK_ID ASC ) 
	INTO #mysky_prob_3
	FROM #mysky_prob_2

	SELECT a.Prob_Group
		, a.Calls_LW
		, a.segment_sa
		, 'Lower_limit' = COALESCE(b.UPPER_LIMIT, 0)
		, a.UPPER_LIMIT
	INTO SABB_my_sky_login_prob_TABLE
	FROM #mysky_prob_3 AS a
	LEFT JOIN #mysky_prob_3 AS b ON a.segment_sa = b.segment_sa AND a.Prob_Group = b.Prob_Group AND a.rank_id - 1 = b.rank_id 
	
	message convert(TIMESTAMP, now()) || ' | SABB_my_sky_login_prob_TABLE - COMPLETED: ' || @@rowcount TO client

	COMMIT 
	CREATE lf INDEX id1 ON SABB_my_sky_login_prob_TABLE (Prob_Group)
	CREATE lf INDEX id2 ON SABB_my_sky_login_prob_TABLE (Calls_LW)
	CREATE hg INDEX id3 ON SABB_my_sky_login_prob_TABLE (Lower_limit)
	CREATE hg INDEX id4 ON SABB_my_sky_login_prob_TABLE (UPPER_LIMIT)
	
	GRANT SELECT ON SABB_my_sky_login_prob_TABLE TO citeam , vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | SABB_my_sky_login_prob - COMPLETED ' TO client
END
GO

CREATE OR REPLACE PROCEDURE SABB_BB_Calls_prob AS

BEGIN
	message convert(TIMESTAMP, now()) || ' | SABB_BB_Calls_prob - Initialization begin ' TO client
	DECLARE @mx_dt DATE
	SET @mx_dt = (SELECT max(end_date) FROM pitteloudj.cust_fcast_weekly_base_2)

	IF EXISTS (SELECT tname FROM syscatalog WHERE creator = USER_NAME() AND UPPER(tname) = UPPER('SABB_BB_Calls_prob_TABLE') AND tabletype = 'TABLE')
		DROP TABLE SABB_BB_Calls_prob_TABLE

	SELECT account_number
		, 'segment_sa' 	= convert(VARCHAR(20), NULL)
		, 'curr_count' 	= MAX(CASE WHEN end_date = @mx_dt THEN BB_all_calls_1m_raw ELSE 0 END)
		, 'count_1w'	= MAX(CASE WHEN end_date = DATEADD(day, - 7, @mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END)
		, 'Calls_LW'	= CASE WHEN curr_count < count_1w THEN 0 ELSE curr_count - count_1w END
		, 'L_12'		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 12, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END)
		, 'L_9' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 9, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END)
		, 'L_6' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 6, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END)
		, 'L_3' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 3, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END)
		, 'Prob_Group' 	= CASE WHEN L_12 = 0 THEN 1 WHEN L_9 = 0 THEN 2 WHEN L_6 = 0 THEN 3 WHEN L_3 = 0 THEN 4 ELSE 5 -- Picking the longest Group
			END
	INTO #BB_CALLS_prob_1
	FROM pitteloudj.cust_fcast_weekly_base_2
	WHERE bb_active = 1 AND dtv_active = 0 
		AND BB_latest_act_dt IS NOT NULL
	GROUP BY account_number

	COMMIT WORK

	CREATE hg INDEX id1 ON #BB_CALLS_prob_1 (account_number)

	COMMIT WORK

	UPDATE #BB_CALLS_prob_1 AS a
	SET a.segment_sa = b.segment_sa
	FROM #BB_CALLS_prob_1 AS a
	INNER JOIN DTV_fcast_weekly_base_2 AS b ON a.account_number = b.account_number AND b.end_Date = @mx_dt

	COMMIT WORK

	SELECT 'Prob_Group' = 1
		, Calls_LW
		, segment_sa
		, 'hits' = COUNT(1)
		, 't_segment' = SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' = CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' = RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC)
	INTO #BB_calls_prob_2
	FROM #BB_CALLS_prob_1
	WHERE L_12 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 2
		, Calls_LW
		, segment_sa
		, 'hits' = COUNT(1)
		, 't_segment' = SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' = CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' = RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC)
	FROM #BB_CALLS_prob_1
	WHERE L_9 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 3
		, Calls_LW
		, segment_sa
		, 'hits' = COUNT(1)
		, 't_segment' = SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' = CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' = RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC)
	FROM #BB_CALLS_prob_1
	WHERE L_6 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 4
		, Calls_LW
		, segment_sa
		, 'hits' = COUNT(1)
		, 't_segment' = SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' = CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' = RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC)
	FROM #BB_CALLS_prob_1
	WHERE L_3 = 0
	GROUP BY Calls_LW
		, segment_sa
	
	UNION
	
	SELECT 'Prob_Group' = 5
		, Calls_LW
		, segment_sa
		, 'hits' = COUNT(1)
		, 't_segment' = SUM(hits) OVER (PARTITION BY segment_sa)
		, 'prob' = CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' = RANK() OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC)
	FROM #BB_CALLS_prob_1
	WHERE Prob_Group = 5
	GROUP BY Calls_LW
		, segment_sa

	SELECT *
		, 'UPPER_LIMIT' = SUM(prob) OVER (PARTITION BY Prob_Group, segment_sa ORDER BY RANK_ID ASC)
	INTO #BB_CALLS_prob_3
	FROM #BB_calls_prob_2

	SELECT a.Prob_Group
		, a.Calls_LW
		, a.segment_sa
		, 'Lower_limit' = COALESCE(b.UPPER_LIMIT, 0)
		, a.UPPER_LIMIT
	INTO SABB_BB_Calls_prob_TABLE
	FROM #BB_CALLS_prob_3 AS a
	LEFT JOIN #BB_CALLS_prob_3 AS b ON a.segment_sa = b.segment_sa AND a.Prob_Group = b.Prob_Group AND a.rank_id - 1 = b.rank_id 
	
	message convert(TIMESTAMP, now()) || ' | SABB_BB_Calls_prob_TABLE - COMPLETED: ' || @@rowcount TO client

	COMMIT WORK
	CREATE lf INDEX id1 ON SABB_BB_Calls_prob_TABLE (Prob_Group)
	CREATE lf INDEX id2 ON SABB_BB_Calls_prob_TABLE (Calls_LW)
	CREATE hg INDEX id3 ON SABB_BB_Calls_prob_TABLE (Lower_limit)
	CREATE hg INDEX id4 ON SABB_BB_Calls_prob_TABLE (UPPER_LIMIT)

	GRANT SELECT ON SABB_BB_Calls_prob_TABLE TO citeam , vespa_group_low_security

	COMMIT WORK 
	
	message convert(TIMESTAMP, now()) || ' | SABB_BB_Calls_prob - COMPLETED ' TO client
END
GO

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
	FROM CITeam.Subs_Calendar(@ForeCAST_Start_Wk / 100, @ForeCAST_End_Wk / 100)

	UPDATE ForeCAST_Loop_Table AS a
	SET subs_week_and_year = sc.subs_week_and_year
		, subs_week_of_year = sc.subs_week_of_year
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

CREATE OR REPLACE PROCEDURE Forecast_SABB_Loop_Table_2_Actions (IN Counter INT, IN Rate_Multiplier REAL) sql security invoker

BEGIN
	DECLARE multiplier BIGINT;
	DECLARE multiplier_2 BIGINT;

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Initializing' TO client;

	SET multiplier = DATEPART(millisecond, now()) + 1;
	SET multiplier_2 = DATEPART(millisecond, now()) + 2;

	DROP TABLE IF EXISTS intraweek_movements;
	DROP TABLE IF EXISTS weekly_movements;
		--------------------------------------------------------------------------------------------------------------
		-- Predicted rates -------------------------------------------------------------------------------------------
		--------------------------------------------------------------------------------------------------------------
		--- rates ----
		UPDATE Forecast_Loop_Table_2 AS a
		SET pred_bb_enter_SysCan_rate = Coalesce(b.pred_SysCan_rate, 0)
			, pred_bb_enter_CusCan_rate = Coalesce(b.pred_CusCan_rate, 0)
			, pred_bb_enter_HM_rate = Coalesce(b.pred_HM_rate, 0)
			, pred_bb_enter_3rd_party_rate = Coalesce(b.pred_3rd_party_rate, 0)
			, pred_BB_Offer_Applied_rate = Coalesce(b.pred_BB_Offer_Applied_rate, 0)
		FROM Forecast_Loop_Table_2 AS a
		LEFT JOIN SABB_predicted_values AS b ON (a.subs_week_of_year = b.subs_week OR (a.subs_week_of_year = 53 AND b.subs_week = 52)) AND a.sabb_forecast_segment = b.sabb_forecast_segment;

	------ SysCan trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_SysCan_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'SysCan Entry';

	------ SysCan cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_SysCan_rate = CASE WHEN pred_bb_enter_SysCan_rate + pred_bb_enter_SysCan_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_SysCan_rate + pred_bb_enter_SysCan_YoY_Trend END;

	------ CusCan trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_CusCan_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'CusCan Entry';

	------ CusCan cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_CusCan_rate = CASE WHEN pred_bb_enter_CusCan_rate + pred_bb_enter_CusCan_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_CusCan_rate + pred_bb_enter_CusCan_YoY_Trend END;

	------ HM trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_HM_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'HM Entry';

	------ HM cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_HM_rate = CASE WHEN pred_bb_enter_HM_rate + pred_bb_enter_HM_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_HM_rate + pred_bb_enter_HM_YoY_Trend END;

	------ 3rd party trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_3rd_party_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = '3rd Party Entry';

	------ 3rd party cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_3rd_party_rate = CASE WHEN pred_bb_enter_3rd_party_rate + pred_bb_enter_3rd_party_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_3rd_party_rate + pred_bb_enter_3rd_party_YoY_Trend END;

	------ BB offer applied trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_BB_Offer_Applied_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'BB Offer Applied';

	------ BB offer applied cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_BB_Offer_Applied_rate = CASE WHEN pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend <= 0 THEN 0 ELSE pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend END;

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 1 ' TO client;

	UPDATE Forecast_Loop_Table_2
	SET rand_action_Pipeline = CASE WHEN BB_status_code IN ('AB', 'BCRQ', 'PC') THEN 1 
			ELSE NULL 
			END;

	UPDATE Forecast_Loop_Table_2
	SET rand_action_Pipeline = rand(number() * multiplier + 4)
	WHERE rand_action_Pipeline IS NULL;

	DROP TABLE

	IF EXISTS #Pipeline_Rank;
		SELECT account_number
			, rand_action_Pipeline
			, count() OVER (PARTITION BY SABB_Forecast_segment) AS Total_Cust_In_SABB_Segment
			, cast(rank() OVER (PARTITION BY SABB_Forecast_segment ORDER BY rand_action_Pipeline ASC) AS REAL) AS SABB_Group_rank
			, CASE WHEN rand_action_Pipeline <= cum_bb_enter_SysCan_rate THEN 1 ELSE 0 END AS BB_SysCan
			, CASE WHEN rand_action_Pipeline > cum_bb_enter_SysCan_rate AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate) THEN 1 ELSE 0 END AS BB_CusCan
			, CASE WHEN rand_action_Pipeline > (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate) THEN 1 ELSE 0 END AS BB_HM
			, CASE WHEN rand_action_Pipeline > (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate + cum_bb_enter_3rd_party_rate) THEN 1 ELSE 0 END AS BB_3rd_Party
		INTO #Pipeline_Rank
		FROM Forecast_Loop_Table_2;

	COMMIT WORK;

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 2 ' TO client;

	CREATE hg INDEX idx_1 ON #Pipeline_Rank (account_number);

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_SysCan = 1
		, churn_type = 'SysCan'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_SysCan = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_CusCan = 1
		, churn_type = 'CusCan'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_CusCan = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_HM = 1
		, churn_type = 'HM'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_HM = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_3rd_Party = 1
		, churn_type = '3rd Party'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_3rd_Party = 1;

	DROP TABLE #Pipeline_Rank;
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 3 ' TO client;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_Offer_Applied = 1
	WHERE bb_status_code = 'AC' AND bb_syscan = 0 AND BB_CusCan = 0 AND BB_HM = 0 AND BB_3rd_party = 0 AND rand_BB_Offer_Applied <= pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend AND end_date BETWEEN curr_offer_start_date_bb AND curr_offer_end_date_intended_bb;

	---??? I believe this should hold the offer BB_Offer_Applied_rate for statuses that stay at AC - therefore need to alter the definition in the pipeline rate proc to capture the BB_Offer_Applied only for these
	---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
	--- but this will give the intraweek percentiles in a format we can use:
	---??? code starts here:
	SELECT churn_type
		, CASE WHEN status_code IN ('AB', 'BCRQ', 'PC') THEN 'PL' ELSE status_code END AS pseudo_status
		, CASE WHEN next_status_code IN ('AB', 'PC', 'BCRQ') THEN 'PL' ELSE next_status_code END AS next_pseudo_status
		, AB_ReAC_offer_applied
		, sum(ABs) AS Cnt
		, Row_number() OVER (PARTITION BY Churn_type ORDER BY pseudo_status ASC) AS Row_ID
	INTO #im
	FROM (SELECT *
		FROM Intrawk_AB_Pct
		WHERE next_status_code NOT IN ('AP')
				UNION
		SELECT *
		FROM Intrawk_PC_Pct
		WHERE next_status_code NOT IN ('AP')
				UNION
		SELECT *
		FROM Intrawk_BCRQ_Pct
		WHERE next_status_code NOT IN ('AP')
		) AS x
	GROUP BY churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied
	ORDER BY churn_type ASC
		, pseudo_status ASC
		, next_pseudo_status ASC
		, AB_ReAC_offer_applied ASC;

	SELECT Row_ID
		, churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied
		, cnt
		, SUM(cnt) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC) AS acum_abs
		, SUM(cnt) OVER (PARTITION BY churn_type) AS acum_abs1
		, cast(acum_abs AS REAL) / acum_abs1 AS prob
	INTO #t1
	FROM #im;
	
	DROP TABLE #im;

	SELECT t1.churn_type
		, t1.pseudo_status
		, t1.next_pseudo_status
		, t1.AB_ReAC_offer_applied AS ReAC_Offer_Applied
		, t1.cnt
		, t1.acum_abs
		, t1.acum_abs1
		, COALESCE(t2.prob, 0) AS Lower
		, t1.prob AS UPPER
	INTO intraweek_movements
	FROM #t1 AS t1
	LEFT JOIN #t1 AS t2 ON t1.row_id = t2.row_id + 1 AND t1.Churn_type = t2.Churn_type;

	DROP TABLE #t1; 
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 4 ' TO client;

		UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM intraweek_movements
		WHERE churn_type = 'SysCan'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_SysCan > 0;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM intraweek_movements
		WHERE churn_type = 'CusCan'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_CusCan > 0;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM intraweek_movements
		WHERE churn_type = 'HM'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_HM > 0;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM intraweek_movements
		WHERE churn_type = '3rd Party'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_3rd_party > 0;

	--?? final update is that if eow status code is set to PL then set the eow status to a real pipeline status (doesn't really matter which one ) - signifying that they are still in the pipeline
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = CASE WHEN BB_SysCan = 1 THEN 'AB' ELSE 'PC' END
	FROM Forecast_Loop_Table_2 AS base
	WHERE base.BB_status_code_EoW = 'PL';

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 5 ' TO client;

	---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
	--- but this will give the churn week status movement percentiles in a format we can use:
	---??? code starts here:
	SELECT churn_type
		, Wks_to_intended_churn
		, CASE WHEN Status_Code_EoW IN ('AB', 'PC', 'BCRQ') THEN 'PL' ELSE Status_Code_EoW END AS next_pseudo_status_EoW
		, ReAC_offer_applied
		, sum(Cnt) AS Cnt
		, Row_number() OVER (PARTITION BY Churn_type, Wks_to_intended_churn ORDER BY next_pseudo_status_EoW ASC , ReAC_offer_applied ASC) AS Row_ID
	INTO #wm
	FROM (
		SELECT * FROM PC_PL_Status_Change_Dist
		UNION
		SELECT * FROM AB_PL_Status_Change_Dist
		) AS x
	--??? extend this bad status filter?
	WHERE (status_code_eow NOT IN ('PA') 
			OR (Wks_to_intended_churn = 'Churn in next 1 wks' AND status_code_eow NOT IN ('AB', 'PC', 'BCRQ')))
	GROUP BY churn_type
		, next_pseudo_status_EoW
		, ReAC_offer_applied
		, Wks_to_intended_churn
	ORDER BY churn_type ASC
		, Wks_to_intended_churn ASC
		, next_pseudo_status_EoW ASC
		, ReAC_offer_applied ASC;

	SELECT Row_ID
		, churn_type
		, Wks_to_intended_churn
		, next_pseudo_status_EoW
		, ReAC_offer_applied
		, cnt
		, SUM(cnt) OVER (PARTITION BY churn_type, Wks_to_intended_churn ORDER BY Row_ID ASC) AS acum_abs
		, SUM(cnt) OVER (PARTITION BY churn_type, Wks_to_intended_churn) AS acum_abs1
		, cast(acum_abs AS REAL) / acum_abs1 AS prob
	INTO #t2
	FROM #wm;

	DROP TABLE #wm;
	
	SELECT t1.churn_type
		, t1.Wks_to_intended_churn
		, t1.next_pseudo_status_EoW
		, t1.ReAC_offer_applied
		, t1.cnt
		, t1.acum_abs
		, t1.acum_abs1
		, COALESCE(t2.prob, 0) AS Lower_
		, t1.prob AS UPPER_
	INTO weekly_movements
	FROM #t2 AS t1
	LEFT JOIN #t2 AS t2 ON t1.row_id = t2.row_id + 1 AND t1.Churn_type = t2.Churn_type AND t1.Wks_to_intended_churn = t2.Wks_to_intended_churn;
	
	DROP TABLE #t2;
	---??? code ends here
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 6 ' TO client;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = PC.next_pseudo_status_EoW
		, BB_Offer_Applied = ReAC_offer_applied
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN weekly_movements AS PC ON base.rand_BB_pipeline_Status_Change BETWEEN PC.lower_ AND PC.upper_ 
														AND trim(base.churn_type) = pc.churn_type 
														AND CASE 	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 0 THEN 'Churn in next 1 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 1 THEN 'Churn in next 2 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 2 THEN 'Churn in next 3 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 3 THEN 'Churn in next 4 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 4 THEN 'Churn in next 5 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 >= 5 THEN 'Churn in next 6+ wks' END = PC.Wks_To_Intended_Churn
	WHERE BB_Status_Code IN ('PC', 'BCRQ', 'AB') 
		AND bb_syscan = 0 AND BB_CusCan = 0 AND BB_HM = 0 AND BB_3rd_party = 0 AND trim(base.churn_type) IN ('CusCan', 'HM', '3rd Party');

	-- ??? does this make sense to change like this?  -- is this correct?
	---??? need a join to make the cuscan, syscan, 3rd party, HM all join to the right sections.  If we do ths we can get rid of the section below
	---??? check all the names in the code above!
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.next_pseudo_status_EoW
		, BB_Offer_Applied = AB.ReAC_offer_applied
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN weekly_movements AS AB ---??? obviously need this table
		ON base.rand_BB_pipeline_Status_Change BETWEEN AB.lower_ AND AB.upper_ AND trim(base.churn_type) = ab.churn_type 
														AND CASE 	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 0 THEN 'Churn in next 1 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 1 THEN 'Churn in next 2 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 2 THEN 'Churn in next 3 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 3 THEN 'Churn in next 4 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 4 THEN 'Churn in next 4 wks'
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 5 THEN 'Churn in next 6 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 6 THEN 'Churn in next 7 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 7 THEN 'Churn in next 8 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 8 THEN 'Churn in next 9 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 >= 9 THEN 'Churn in next 10+ wks' END = AB.Wks_To_Intended_Churn
	WHERE BB_Status_Code IN ('PC', 'BCRQ', 'AB') 
			AND trim(base.churn_type) IN ('SysCan') 
			AND bb_syscan = 0 
			AND BB_CusCan = 0 
			AND BB_HM = 0 
			AND BB_3rd_party = 0;

	--?? final update is that if eow status code is set to PL then set the eow status to the current status (i.e. nothing has changed)
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = CASE WHEN BB_SysCan = 1 THEN 'AB' WHEN BB_CusCan = 1 OR BB_HM = 1 OR BB_3rd_Party = 1 THEN 'PC' ELSE BB_Status_Code END
	FROM Forecast_Loop_Table_2 AS base
	WHERE base.BB_status_code_EoW = 'PL';

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 7 ' TO client;

	---??? check all the names in the code above!
	UPDATE Forecast_Loop_Table_2 AS base
	SET CusCan = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = 'CusCan';

	--	AND BB_CusCan > 0;
	UPDATE Forecast_Loop_Table_2 AS base
	SET SysCan = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = 'SysCan';

	--	AND BB_SysCan > 0;
	UPDATE Forecast_Loop_Table_2 AS base
	SET HM = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = 'HM';

	--AND BB_HM > 0;
	UPDATE Forecast_Loop_Table_2 AS base
	SET _3rd_Party = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = '3rd Party'
		--	AND BB_3rd_party > 0;
		/*xx*/
END
GO


CREATE OR REPLACE PROCEDURE SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 (
	IN Forecast_Start_Wk INT
	, IN Forecast_End_Wk INT
	, IN True_Sample_Rate REAL
	)

BEGIN
	DECLARE @new_cust_end_date DATE;
	DECLARE @new_cust_subs_week_and_year INT;
	DECLARE @new_cust_subs_week_of_year INT;
	DECLARE @new_cust_subs_year INT;
	DECLARE @multiplier BIGINT;

	message cast(now() AS TIMESTAMP) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - Initialization begin ' TO client;

	SET @multiplier = DATEPART(millisecond, now()) + 2631;

	SELECT * INTO #Sky_Calendar FROM CITeam.subs_Calendar(Forecast_Start_Wk / 100, Forecast_End_Wk / 100);

	SET @new_cust_end_date = (SELECT max(end_date + 7) FROM Forecast_Loop_Table_2 );
	SET @new_cust_subs_week_and_year = (SELECT max(subs_week_and_year) FROM #sky_calendar  WHERE calendar_date = @new_cust_end_date);
	SET @new_cust_subs_week_of_year = (SELECT max(subs_week_of_year) FROM #sky_calendar WHERE calendar_date = @new_cust_end_date );
	SET @new_cust_subs_year = (SELECT max(subs_year) FROM #sky_calendar WHERE calendar_date = @new_cust_end_date);

	DROP TABLE IF EXISTS #new_customers_last_2Yrs_2;
	
		SELECT *
			, rand(number() * @multiplier + 163456) AS rand_sample2
			, rand(number() * @multiplier + 1) AS e1
			, rand(number() * @multiplier + 2) AS e2
			, rand(number() * @multiplier + 3) AS e3
			, rand(number() * @multiplier + 4) AS e4
			, rand(number() * @multiplier + 5) AS e5
			, rand(number() * @multiplier + 6) AS e6
			, rand(number() * @multiplier + 7) AS e7
		INTO #new_customers_last_2Yrs_2
		FROM Forecast_New_Cust_Sample;

	DROP TABLE IF EXISTS #new_customers_last_2Yrs_3;
	
	SELECT *
		, row_number() OVER (ORDER BY rand_sample2 ASC) AS Rand_Rnk
	INTO #new_customers_last_2Yrs_3
	FROM #new_customers_last_2Yrs_2;

	DROP TABLE #new_customers_last_2Yrs_2;
	
	DELETE
	FROM #new_customers_last_2Yrs_3 AS new_cust
	FROM #new_customers_last_2Yrs_3 AS new_cust
	INNER JOIN Activation_Vols AS act ON new_cust.Rand_Rnk > act.New_Customers * true_sample_rate AND act.subs_week_of_year = @new_cust_subs_week_of_year;

	message cast(now() AS TIMESTAMP) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - Table insert begin ' TO client;

	INSERT INTO Forecast_Loop_Table_2 (
		account_number
		, end_date
		, subs_week_and_year
		, subs_year
		, subs_week_of_year
		, weekid
		, BB_Status_Code
		, churn_type
		, BB_Status_Code_EoW
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
		, sabb_forecast_segment
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
		, rand_action_Pipeline
		, rand_BB_Offer_Applied
		, rand_Intrawk_BB_NotSysCan
		, rand_Intrawk_BB_SysCan
		, rand_BB_Pipeline_Status_Change
		, rand_New_Off_Dur
		, rand_BB_NotSysCan_Duration
		, SABB_forecast_segment_COUNT
		, SABB_Group_rank
		, pct_SABB_COUNT
		, SABB_Churn
		, BB_offer_applied
		, DTV_AB
		, cum_BB_Offer_Applied_rate
		, pred_bb_enter_SysCan_rate
		, pred_bb_enter_SysCan_YoY_Trend
		, cum_bb_enter_SysCan_rate
		, pred_bb_enter_CusCan_rate
		, pred_bb_enter_CusCan_YoY_Trend
		, cum_bb_enter_CusCan_rate
		, pred_bb_enter_HM_rate
		, pred_bb_enter_HM_YoY_Trend
		, cum_bb_enter_HM_rate
		, pred_bb_enter_3rd_party_rate
		, pred_bb_enter_3rd_party_YoY_Trend
		, cum_bb_enter_3rd_party_rate
		, pred_BB_Offer_Applied_rate
		, pred_BB_Offer_Applied_YoY_Trend
		, CusCan
		, SysCan
		, HM
		, _3rd_Party
		, calls_LW
		, my_sky_login_LW
		, BB_SysCan
		, BB_CusCan
		, BB_HM
		, BB_3rd_Party
		)
	SELECT replicate(CHAR(65 + remainder((counter - 1), 53)), (counter - 1) / 53 + 1) || a.account_number AS account_number
		, @new_cust_end_date - 7 AS end_date
		, @new_cust_subs_week_and_year AS subs_week_and_year
		, @new_cust_subs_year
		, @new_cust_subs_week_of_year AS subs_week_of_year
		, (year(@new_cust_end_date) - 2010) * 52 + @new_cust_subs_week_of_year AS weekid
		, BB_Status_Code
		, churn_type
		, BB_Status_Code_EoW
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
		, cast(node_SA AS VARCHAR)
		, segment_SA
		, PL_Future_Sub_Effective_Dt
		, DTV_Activation_Type
		, Curr_Offer_start_Date_BB
		, Curr_offer_end_date_Intended_BB
		, Prev_offer_end_date_BB
		, Future_offer_Start_dt
		, Future_end_Start_dt
		, BB_first_act_dt
		, rand_sample2
		, sample
		, SABB_flag
		, e1
		, e2
		, e3
		, e4
		, e5
		, e6
		, e7
		, COUNT() OVER (PARTITION BY node_SA) AS y1
		, cast(row_number() OVER (PARTITION BY node_SA ORDER BY e1 ASC) AS REAL) AS y2
		, y2 / y1
		, cast(0 AS TINYINT)
		, 0, 0, 0, 0, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0	, 0
	FROM #new_customers_last_2Yrs_3 AS a;

	DROP TABLE #new_customers_last_2Yrs_3;
	
	message cast(now() AS TIMESTAMP) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - Forecast_Loop_Table_2 insert done: ' || @@rowcount TO client;

	COMMIT WORK;
	message cast(now() AS TIMESTAMP) || ' | SABB_Forecast_Insert_New_Custs_Into_Loop_Table_2 - COMPLETED ' TO client
END

GO


CREATE OR REPLACE PROCEDURE SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk AS

BEGIN
	--------------------------------------------------------------------------
	-- Update table for start of next loop -----------------------------------
	--------------------------------------------------------------------------
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Initializing' TO client

	-- set the expected churn date for non-syscan pipeline entries based on previous experience 	
	UPDATE Forecast_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, base.end_date + dur.Days_To_churn)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN DTV_PC_Duration_Dist AS dur ON rand_BB_NotSysCan_Duration BETWEEN dur.PC_Days_Lower_Prcntl AND dur.PC_Days_Upper_Prcntl
	WHERE (BB_3rd_Party > 0 OR BB_CusCan > 0 OR BB_HM > 0) AND base.BB_Status_Code_EoW = 'PC' 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for non-syscan PL entries ' || @@rowcount TO client

	-- set the expected churn date to be 50 days on for SysCan 	  
	UPDATE Forecast_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, base.end_date + 50)
	FROM Forecast_Loop_Table_2 AS base
	WHERE BB_SysCan > 0 AND base.BB_Status_Code_EoW IN ('AB', 'BCRQ') 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for syscan PL entries ' || @@rowcount TO client

	--- Setting next curr_offer_start_date_BB
	UPDATE Forecast_Loop_Table_2 AS base
	SET curr_offer_start_date_BB = end_date + 3
		, Curr_Offer_end_Date_Intended_BB = dateadd(month, Total_Offer_Duration_Mth, end_date + 3)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN Offer_Applied_Dur_Dist AS offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound AND Offer_Segment = 'Other' ---??? check where we get this table from
	WHERE BB_Offer_Applied = 1 AND NOT (BB_Status_Code IN ('AB', 'PC', 'BCRQ') AND BB_Status_Code_EoW = 'AC') 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Active: Updating curr_offer_start_date_BB DONE ' || @@rowcount TO client

	--- Setting next curr_offer_start_date_BB
	UPDATE Forecast_Loop_Table_2 AS base
	SET curr_offer_start_date_BB = end_date + 3
		, Curr_Offer_end_Date_Intended_BB = dateadd(month, Total_Offer_Duration_Mth, end_date + 3)
	FROM -- Default 10m offer
		Forecast_Loop_Table_2 AS base
	INNER JOIN Offer_Applied_Dur_Dist AS offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound AND Offer_Segment = 'Reactivations' ---??? check where we get this table from
	WHERE BB_Offer_Applied = 1 AND BB_Status_Code IN ('AB', 'PC', 'BCRQ') 
				AND BB_Status_Code_EoW = 'AC' 
				
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Reactivations: Updating curr_offer_start_date_BB DONE ' || @@rowcount TO client 
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating curr_offer_start_date_BB DONE' TO client

	--- Setting next BB_status code 
	UPDATE Forecast_Loop_Table_2
	SET BB_Status_Code = Coalesce(BB_Status_Code_EoW, BB_Status_Code) 
	
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating status code ' || @@rowcount TO client

	--- Clearing not pipeline accounts Future effective dt
	UPDATE Forecast_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = NULL ---?? note changed name here
	WHERE BB_Status_Code NOT IN ('PC', 'AB', 'BCRQ') 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Clear the churn date for non PL ' || @@rowcount TO client

	--- Clearing the pipeline entry status codes
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_SysCan = 0
		, BB_CusCan = 0
		, BB_HM = 0
		, BB_3rd_Party = 0

	--	BB_offer_applied = 0   -- set BB offer applied back to zero after triggering
	--- Updating organic growth variables
	UPDATE Forecast_Loop_Table_2
	SET end_date = end_date + 7
		, BB_tenure_raw = BB_tenure_raw + 7
		, DTV_Activation_Type = NULL
		, weekid = weekid + 1

	--- Setting offer end date when expiration date happen in the previous week
	UPDATE Forecast_Loop_Table_2
	SET Prev_offer_end_date_BB = Curr_Offer_end_Date_Intended_BB
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 1/3' TO client

	--- Clearing Offer end date when curr offer ended on the previous week
	UPDATE Forecast_Loop_Table_2
	SET Curr_Offer_end_Date_Intended_BB = NULL
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date

	UPDATE Forecast_Loop_Table_2
	SET BB_offer_applied = 0
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date

	--- 
	UPDATE Forecast_Loop_Table_2
	SET Prev_offer_end_date_BB = NULL
	WHERE Prev_offer_end_date_BB < (end_date) - 53 * 7 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 2/3' TO client

	--- Updating Offer Remaining days
	UPDATE Forecast_Loop_Table_2
	SET BB_offer_rem_and_end_raw = CASE WHEN BB_Offer_Applied = 1 THEN DATEDIFF(day, end_date, Curr_Offer_end_Date_Intended_BB) ELSE BB_offer_rem_and_end_raw - 7 END
		
		
		
		-----======== PLACEHOLDERS FOR CALLS AND MY SKY LOGIN
		--- Updating my_sky_login_3m_raw 
		
			DECLARE @dt DATE 
			SET @dt = (SELECT max(end_date) FROM Forecast_Loop_Table_2) 
		
		message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating my_sky_login_3m_raw ' TO client

	SELECT base.account_number
		, base.end_Date
		, 'visit_days' = SUM(visit)
	INTO #days_visited_3m_2
	FROM (SELECT account_number
			, visit_date
			, 'visit' = 1
		FROM vespa_shared.mysky_daily_usage
		WHERE visit_date <= @dt
		UNION
		SELECT account_number
			, end_date
			, my_sky_login_LW
		FROM FORECAST_Looped_Sim_Output_Platform
		) AS v
	INNER JOIN Forecast_Loop_Table_2 AS base ON BASE.account_number = v.account_number
	WHERE visit_date BETWEEN DATEADD(wk, 1, DATEADD(mm, - 3, end_date)) AND end_date
	GROUP BY base.account_number
		, base.end_date 
		
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - days_visited_3m_2:  ' || @@rowcount TO client

	----------------------------------------------------------------------	
	COMMIT WORK
	CREATE hg INDEX ID1 ON #days_visited_3m_2 (account_number)
	CREATE dttm INDEX ID2 ON #days_visited_3m_2 (end_date)
	CREATE lf INDEX ID3 ON #days_visited_3m_2 (visit_days)
	COMMIT WORK

	--- Updating CALLS					
	SELECT base.account_number
		, 'call_count' = SUM(calls)
		, end_date
	INTO #BBCalls_Temp_1m_2
	FROM (SELECT account_number
			, call_date
			, 'calls' = COUNT(1)
		FROM cust_inbound_calls AS a
		INNER JOIN (SELECT 'min_dt' = DATEADD(month, - 13, MIN(end_date)) FROM Forecast_Loop_Table_2) AS b ON a.call_date >= b.min_dt 
		WHERE 	call_date <= @dt
				AND contact_activity = 'Inbound' 
				AND service_call_type IN (
				'SCT_CUSSER_BBusage', 'SCT_SALOLY_EOODirect_SABB', 'SCT_SALRET_BB_Campaign2', 'SCT_SALRET_BB_Churn', 'SCT_SALRET_BB_MAC', 'SCT_SALRET_BB_Online', 'SCT_SALRET_BB_PIPELINE', 'SCT_SALRET_BB_TA', 'SCT_SALRET_BB_TA_Xfer'
				, 'SCT_SALRET_BB_TVWinback', 'SCT_SALRET_BB_Value', 'SCT_SALRET_BB_Value_SA', 'SCT_SALRET_BB_Value_SA_Xfer', 'SCT_SALRET_BB_Value_Xfer', 'SCT_SALRET_BB_Value2', 'SCT_SALRET_ELP_BB', 'SCT_SALTRN_BB_TA_Xfer', 'SCT_SALRET_BB_Campaign1'
				, 'SCT_SALRET_BB_HighChurn', 'SCT_SALRET_BB_Value_D&G', 'SCT_SALRET_BB_HighChurn_Xfer', 'SCT_CUSSER_BBusage', 'SCT_SALOLY_EOODirect_SABB', 'SCT_SALRET_BB_Campaign2', 'SCT_SALRET_BB_Churn', 'SCT_SALRET_BB_MAC', 'SCT_SALRET_BB_Online'
				, 'SCT_SALRET_BB_PIPELINE', 'SCT_SALRET_BB_TA', 'SCT_SALRET_BB_TA_Xfer', 'SCT_SALRET_BB_TVWinback', 'SCT_SALRET_BB_Value', 'SCT_SALRET_BB_Value_SA', 'SCT_SALRET_BB_Value_SA_Xfer', 'SCT_SALRET_BB_Value_Xfer', 'SCT_SALRET_BB_Value2'
				, 'SCT_SALRET_ELP_BB', 'SCT_SALTRN_BB_TA_Xfer', 'SCT_SALRET_BB_Campaign1', 'SCT_SALRET_BB_HighChurn', 'SCT_SALRET_BB_Value_D&G', 'SCT_SALRET_BB_HighChurn_Xfer', 'SCT_HLPALL_NowTV_Cancel_Xfer', 'SCT_SALRET_ELP_Xfer', 'SCT_SALTRN_BB_TA_Xfer'
				, 'SCT_SALRET_BB_Value_SA_Xfer', 'SCT_SALVAL_BB_Syscan', 'SCT_SALRET_BB_Campaign3', 'SCT_HLPTV__PriceTalk_AVS', 'SCT_HLPTV__PriceTalk_TO', 'SCT_OTHCTT_DN1', 'SCT_SALRET_PriceTalk', 'Support Broadband and Talk', 'SCT_WELBBT_Fibre', 'SCT_WELBBT_Fibre_Engineer'
				, 'SCT_WELBBT_Fibre_NL', 'SCT_WELBBT_Fibre_Staff', 'SCT_WELBBT_Fibre_Staff_Xfer', 'SCT_WELBBT_Fibre_Xfer', 'SCT_WELBBT_IncompleteJob', 'SCT_WELBBT_LinePlant_Xfer', 'SCT_WELBBT_MoveHome_Xfer', 'SCT_WELBBT_Nuisance_Xfer', 'SCT_WELBBT_Order', 'SCT_WELBBT_OrderRecovery_Direct'
				, 'SCT_WELBBT_OrderRecovery_Xfer', 'SCT_WELBBT_Order_Engineer', 'SCT_WELBBT_Order_NL', 'SCT_WELBBT_Order_Xfer', 'SCT_WELBBT_Slamming_Direct', 'SCT_WELBBT_Staff_Order', 'SCT_WELBBT_Staff_Order_Xfer', 'SCT_WELBBT_Support_Xfer', 'SCT_WELBBT_TalkTechnical'
				, 'SCT_WELBBT_Technical', 'SCT_WELBBT_Tech_TO', 'SCT_SUPBBT_Case_Broadband', 'SCT_SUPBBT_Case_Broadband_NL', 'SCT_SUPBBT_Case_Talk', 'SCT_SUPBBT_Case_Talk_NL'
				, 'Broadband (One Service)'
				, 'Broadband Escalation (One Service)'
				, 'Complaints Broadband'
				, 'Complaints Broadband (ROI)'
				, 'Complaints Broadband and Talk (MYSKY)'
				, 'Escalation Broadband'
				, 'Escalation Broadband (ROI)'
				, 'EST Broadband and Talk'
				, 'Fibre Broadband'
				, 'General Pool for 16 Olympus Retention'
				, 'General Pool for 17 Pro Broadband'
				, 'Help and Troubleshooting (Broadband)'
				, 'Help and Troubleshooting Broadband / Talk (ROI)'
				, 'Moving Home Talk / Broadband (ROI)'
				, 'Pro Broadband'
				, 'Product Information Broadband / Talk (ROI)'
				, 'Product Missold Broadband and Talk'
				, 'SCT_CUSDBT_BBTech'
				, 'SCT_CUSDBT_Spin_BBTech'
				, 'SCT_CUSSER_BBusage'
				, 'SCT_DIALLER_CAM_DIGEXP_BBT'
				, 'SCT_DIALLER_CAM_ONEEXP_BBT'
				, 'SCT_DIALLER_CAM_ONEEXP_BBTPlus'
				, 'SCT_DIALLER_CAM_OSSEXP_BBT_Help'
				, 'SCT_DIALLER_CAM_OSSEXP_BBT_Welcome'
				, 'SCT_DIALLER_CAM_OSSEXP_HM_BBT'
				, 'SCT_DIGEXP_BBT_Fibre_Xfer'
				, 'SCT_DIGEXP_BBT_Xfer'
				, 'SCT_ESCCOM_Escalation_BBT_Xfer'
				, 'SCT_ESCCOM_LeaderSupport_BBT_Xfer'
				, 'SCT_HLPBBT_Alarm'
				, 'SCT_HLPBBT_BB_Engineer'
				, 'SCT_HLPBBT_BB_Engineer_NL'
				, 'SCT_HLPBBT_BB_Online'
				, 'SCT_HLPBBT_BB_Online_NL'
				, 'SCT_HLPBBT_BB_Router'
				, 'SCT_HLPBBT_BB_Router_NL'
				, 'SCT_HLPBBT_BB_Technical'
				, 'SCT_HLPBBT_BB_Technical_HSS'
				, 'SCT_HLPBBT_BB_Technical_NL'
				, 'SCT_HLPBBT_BB_Technical_TO'
				, 'SCT_HLPBBT_BB_Tech_HSS_TO'
				, 'SCT_HLPBBT_BB_Tech_Xfer'
				, 'SCT_HLPBBT_ClosedOutage'
				, 'SCT_HLPBBT_Fibre_D&G'
				, 'SCT_HLPBBT_Fibre_Xfer'
				, 'SCT_HLPBBT_Fix_Xfer'
				, 'SCT_HLPBBT_Main_TO'
				, 'SCT_HLPBBT_PDS_Xfer'
				, 'SCT_HLPBBT_Pro_Case'
				, 'SCT_HLPBBT_Pro_Tech_BB'
				, 'SCT_HLPBBT_Pro_Tech_Comb'
				, 'SCT_HLPBBT_Pro_Tech_Talk'
				, 'SCT_HLPBBT_Pro_Tech_Xfer'
				, 'SCT_HLPBBT_Pro_Upg_BB'
				, 'SCT_HLPBBT_Pro_Upg_BB_TO'
				, 'SCT_HLPBBT_Pro_Upg_Talk'
				, 'SCT_HLPBBT_Pro_WebHost'
				, 'SCT_HLPBBT_ST_Tech_Xfer'
				, 'SCT_HLPBBT_TalkTechnical'
				, 'SCT_HLPBBT_Talk_Engineer'
				, 'SCT_HLPBBT_Talk_Tarriff'
				, 'SCT_HLPBBT_Talk_Tarriff_NL'
				, 'SCT_HLPBBT_Talk_Technical'
				, 'SCT_HLPBBT_Talk_Technical_HSS'
				, 'SCT_HLPBBT_Talk_Technical_NL'
				, 'SCT_HLPBBT_Talk_Tech_HSS_TO'
				, 'SCT_HLPBBT_Talk_Tech_TO'
				, 'SCT_HLPBBT_Technical'
				, 'SCT_HLPBBT_Tech_Connect'
				, 'SCT_HLPBBT_Tech_Connect_NL'
				, 'SCT_HLPBBT_Tech_Fibre'
				, 'SCT_HLPBBT_Tech_Fibre_NL'
				, 'SCT_HLPBBT_Tech_NL_FB'
				, 'SCT_HLPBBT_Tech_TO'
				, 'SCT_ONEEXP_BBT'
				, 'SCT_ONEEXP_BBTPlus_Xfer'
				, 'SCT_ONEEXP_BBT_Xfer'
				, 'SCT_OSSEXP_BBT'
				, 'SCT_OSSEXP_BBT_APP'
				, 'SCT_OSSEXP_BBT_Help'
				, 'SCT_OSSEXP_BBT_Help_Xfer'
				, 'SCT_OSSEXP_BBT_Welcome_Xfer'
				, 'SCT_OSSEXP_HM_BBT_Xfer'
				, 'SCT_REPEXR_BBST'
				, 'SCT_REPEXR_BBST_Order'
				, 'SCT_REPEXR_BBST_Order_TO'
				, 'SCT_REPEXR_BBST_TO'
				, 'SCT_REPHLP_BBST'
				, 'SCT_REPHLP_BBST_Direct'
				, 'SCT_REPHLP_BBST_TO'
				, 'SCT_REPHLP_BBST_Xfer'
				, 'SCT_REPHLP_Fibre'
				, 'SCT_REPWEL_BBST'
				, 'SCT_REPWEL_BBST_TO'
				, 'SCT_REPWEL_Fibre'
				, 'SCT_SALATT_Olympus_Direct'
				, 'SCT_SALATT_Olympus_Redirect'
				, 'SCT_SALATT_Olympus_Xfer   '
				, 'SCT_SALEXC_BB'
				, 'SCT_SALEXC_BBFF'
				, 'SCT_SALEXC_BBMAC'
				, 'SCT_SALEXC_BBMAC_Xfer'
				, 'SCT_SALEXC_BBNLP'
				, 'SCT_SALEXC_BBNoLR'
				, 'SCT_SALEXC_BBPreActive'
				, 'SCT_SALEXC_BB_Xfer'
				, 'SCT_SALEXC_Fibre'
				, 'SCT_SALEXC_Olympus'
				, 'SCT_SALEXC_ROI_BBT_Upgrades'
				, 'SCT_SALEXC_ROI_SwitcherBB'
				, 'SCT_SALOLY_EOODDR_CAN_SABB'
				, 'SCT_SALOLY_EOODDR_DGBT_SABB'
				, 'SCT_SALOLY_EOODirect_SABB   '
				, 'SCT_SALOLY_Olympus_Xfer'
				, 'SCT_SALPAT_ROI_BB'
				, 'SCT_SALPAT_ROI_BB_Xfer'
				, 'SCT_SALPAT_ROI_Fibre_Direct '
				, 'SCT_SALRET_BB_Campaign1'
				, 'SCT_SALRET_BB_Campaign2'
				, 'SCT_SALRET_BB_Campaign3'
				, 'SCT_SALRET_BB_Churn'
				, 'SCT_SALRET_BB_HighChurn'
				, 'SCT_SALRET_BB_HighChurn_Xfer'
				, 'SCT_SALRET_BB_MAC'
				, 'SCT_SALRET_BB_Online'
				, 'SCT_SALRET_BB_PIPELINE'
				, 'SCT_SALRET_BB_TA'
				, 'SCT_SALRET_BB_TA_Xfer'
				, 'SCT_SALRET_BB_TVWinback'
				, 'SCT_SALRET_BB_Value'
				, 'SCT_SALRET_BB_Value2'
				, 'SCT_SALRET_BB_ValueBill'
				, 'SCT_SALRET_BB_ValueBill_TO'
				, 'SCT_SALRET_BB_Value_D&G'
				, 'SCT_SALRET_BB_Value_SA'
				, 'SCT_SALRET_BB_Value_SA_Xfer'
				, 'SCT_SALRET_BB_Value_Xfer'
				, 'SCT_SALRET_ELP_BB'
				, 'SCT_SALRTM_BBINFO'
				, 'SCT_SALRTM_SHMS_Olympus'
				, 'SCT_SALTRN_BB_HighChurn'
				, 'SCT_SALTRN_BB_HighChurn_Xfer'
				, 'SCT_SALTRN_BB_TA_Xfer'
				, 'SCT_SALVAL_BB_Syscan'
				)
		GROUP BY account_number
			, call_date
		
		UNION
		
		SELECT account_number
			, end_date
			, calls_LW
		FROM FORECAST_Looped_Sim_Output_Platform
		WHERE calls_LW > 0
		) AS TEMP
	INNER JOIN Forecast_Loop_Table_2 AS base ON base.account_number = TEMP.account_number
	WHERE call_date BETWEEN DATEADD(week, 1, DATEADD(mm, - 1, end_date)) AND end_date
	GROUP BY base.account_number
		, end_date message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - BBCalls_Temp_1m_2:  ' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX ID1 ON #BBCalls_Temp_1m_2 (account_number)
	CREATE dttm INDEX ID2 ON #BBCalls_Temp_1m_2 (end_date)
	CREATE lf INDEX ID3 ON #BBCalls_Temp_1m_2 (call_count)
	COMMIT WORK

	SELECT b.account_number
		, b.segment_sa
		, 'L_12' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 12, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END)		-- Max Login in the past 12 month
		, 'L_9' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 9, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END) -- Max Login in the past 9 month
		, 'L_6' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 6, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END) -- Max Login in the past 6 month
		, 'L_3' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 3, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END)
		, 'C_12' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 12, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) -- Max Login in the past 12 month
		, 'C_9' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 9, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) -- Max Login in the past 9 month
		, 'C_6' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 6, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) -- Max Login in the past 6 month
		, 'C_3' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 3, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END)
		, 'Login_group' = CASE WHEN L_12 = 0 THEN 1 WHEN L_9 = 0 THEN 2 WHEN L_6 = 0 THEN 3 WHEN L_3 = 0 THEN 4 ELSE 5 END
		, 'Call_group' = CASE WHEN C_12 = 0 THEN 1 WHEN C_9 = 0 THEN 2 WHEN C_6 = 0 THEN 3 WHEN C_3 = 0 THEN 4 ELSE 5 END
		, 'Rand_Login' = convert(REAL, NULL)
		, 'Rand_call' = convert(REAL, NULL)
	INTO #t_prob
	FROM Forecast_Loop_Table_2 AS b
	INNER JOIN pitteloudj.cust_fcast_weekly_base_2 AS a ON a.account_number = b.account_number
	GROUP BY b.account_number
		, b.segment_sa

	COMMIT WORK

	UPDATE #t_prob
	SET Rand_LOGIN = RAND((convert(REAL, account_number)) * DATEPART(ms, GETDATE()))
		, Rand_call = RAND((convert(REAL, account_number) * 10) * DATEPART(ms, GETDATE()))

	CREATE hg INDEX ID1 ON #t_prob (account_number)
	CREATE lf INDEX ID2 ON #t_prob (segment_sa)
	CREATE lf INDEX ID3 ON #t_prob (Login_group)
	CREATE lf INDEX ID4 ON #t_prob (Call_group) 
	
	
	message convert (TIMESTAMP , now() ) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - t_prob:  ' || @@rowcount TO client

	UPDATE Forecast_Loop_Table_2 AS a
	SET a.my_sky_login_3m_raw = COALESCE(c.Calls_LW, 0) + COALESCE(d.visit_days, 0)
		, a.my_sky_login_LW = COALESCE(c.Calls_LW, 0)
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #t_prob AS b ON a.account_number = b.account_number
	LEFT JOIN #days_visited_3m_2 AS d ON a.account_number = d.account_number
	LEFT JOIN SABB_my_sky_login_prob_TABLE AS c ON b.Login_group = c.Prob_Group AND b.segment_sa = c.segment_sa AND Rand_login BETWEEN Lower_limit AND UPPER_LIMIT 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing Forecast_Loop_Table_2/1:  ' || @@rowcount TO client

	UPDATE Forecast_Loop_Table_2 AS a
	SET a.BB_all_calls_1m_raw = COALESCE(c.Calls_LW, 0) + COALESCE(d.call_count, 0)
		, a.Calls_LW = COALESCE(c.Calls_LW, 0)
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #t_prob AS b ON a.account_number = b.account_number
	LEFT JOIN #BBCalls_Temp_1m_2 AS d ON a.account_number = d.account_number
	LEFT JOIN SABB_BB_Calls_prob_TABLE AS c ON b.Login_group = c.Prob_Group AND b.segment_sa = c.segment_sa AND Rand_call BETWEEN Lower_limit AND UPPER_LIMIT 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing Forecast_Loop_Table_2/2:  ' || @@rowcount TO client message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 3/3' TO client

	--- Refreshing binned variables
	UPDATE Forecast_Loop_Table_2
	SET my_sky_login_3m = CASE WHEN my_sky_login_3m_raw > 2 THEN 3 ELSE my_sky_login_3m_raw END
		, BB_all_calls_1m = CASE WHEN BB_all_calls_1m_raw = 0 THEN 0 ELSE 1 END
		, BB_offer_rem_and_end = CASE 	WHEN BB_offer_rem_and_end_raw BETWEEN - 9998 AND - 1015 THEN - 3 
										WHEN BB_offer_rem_and_end_raw BETWEEN - 1015 AND - 215 THEN - 2 
										WHEN BB_offer_rem_and_end_raw BETWEEN - 215 AND - 75 THEN - 1 
										WHEN BB_offer_rem_and_end_raw BETWEEN - 74 AND - 0 THEN 0 
										WHEN BB_offer_rem_and_end_raw BETWEEN 1 AND 62 THEN 1 
										WHEN BB_offer_rem_and_end_raw BETWEEN 63 AND 162 THEN 2 
										WHEN BB_offer_rem_and_end_raw BETWEEN 163 AND 271 THEN 3 
										WHEN BB_offer_rem_and_end_raw > 271 THEN 4 ELSE - 9999 END
		, BB_tenure = CASE 	WHEN BB_tenure_raw <= 118 THEN 1 
							WHEN BB_tenure_raw BETWEEN 119 AND 231 THEN 2 
							WHEN BB_tenure_raw BETWEEN 231 AND 329 THEN 3 
							WHEN BB_tenure_raw BETWEEN 329 AND 391 THEN 4 
							WHEN BB_tenure_raw BETWEEN 392 AND 499 THEN 5 
							WHEN BB_tenure_raw BETWEEN 499 AND 641 THEN 6 
							WHEN BB_tenure_raw BETWEEN 641 AND 1593 THEN 7 
							WHEN BB_tenure_raw > 1593 THEN 8 ELSE - 1 END

	--- Refreshing nodes and segments
	UPDATE Forecast_Loop_Table_2 AS a
	SET SABB_forecast_segment = convert(VARCHAR(4), node)
		, segment_sa = segment
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN pitteloudj.BB_SABB_Churn_segments_lookup AS b ON a.BB_offer_rem_and_end = b.BB_offer_rem_and_end 
								AND a.BB_tenure = b.BB_tenure 
								AND a.my_sky_login_3m = b.my_sky_login_3m 
								AND a.talk_type = b.talk_type 
								AND a.home_owner_status = b.home_owner_status 
								AND a.BB_all_calls_1m = b.BB_all_calls_1m

	DROP TABLE #days_visited_3m_2
	DROP TABLE #BBCalls_Temp_1m_2
	DROP TABLE #t_prob
END
GO
-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk TO CITeam
GO

CREATE OR REPLACE PROCEDURE SABB_Forecast_Create_New_Forecast_Loop_Table ()

BEGIN
	DROP TABLE

	IF EXISTS Forecast_Loop_Table;
		SELECT account_number
			, end_date
			, subs_week_and_year
			, subs_year
			, subs_week_of_year
			, weekid
			, BB_Status_Code
			, churn_type
			, BB_Status_Code_EoW
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
			, SABB_forecast_segment
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
			, BB_SysCan
			, BB_CusCan
			, BB_HM
			, BB_3rd_Party
			, rand_action_Pipeline
			, rand_BB_Offer_Applied
			, rand_Intrawk_BB_NotSysCan
			, rand_Intrawk_BB_SysCan
			, rand_BB_Pipeline_Status_Change
			, rand_New_Off_Dur
			, rand_BB_NotSysCan_Duration
			, CusCan
			, SysCan
			, HM
			, _3rd_Party
			, calls_LW
			, my_sky_login_LW
		INTO Forecast_Loop_Table
		FROM Forecast_Loop_Table_2
END

GO

