CREATE OR REPLACE PROCEDURE TP_Regression_Coefficient (IN LV INT, IN Regression_Yrs TINYINT) 
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

	DROP TABLE IF EXISTS #TP_weekly_agg;
	
	SELECT subs_year
			, subs_week_of_year AS subs_week
			, cast(subs_week_and_year AS INT) AS subs_week_and_year
			, node_TP AS TP_forecast_segment
			, Count(*) 								AS n
			, cast(sum(CASE WHEN bb_offer_rem_and_end = - 9999 THEN 0 ELSE 1 END) AS REAL) AS BB_Offer_Applied 	-- TODO: Check this definition of offer applied
			, cast(sum(bb_enter_SysCan) AS REAL) 	AS bb_enter_SysCan
			, cast(sum(bb_enter_CusCan) AS REAL) 	AS bb_enter_CusCan
			, cast(sum(bb_enter_HM) AS REAL) 		AS bb_enter_HM
			, cast(sum(bb_enter_3rd_party) AS REAL) AS bb_enter_3rd_party
			, dense_rank() OVER (ORDER BY subs_week_and_year DESC	) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, (week_id / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) AS Max_Subs_Week
		INTO #TP_weekly_agg
		FROM jcartwright.DTV_Fcast_Weekly_Base_2 AS agg ---??? update this source later
		WHERE subs_week_and_year BETWEEN Y1W01 AND Y3W52 AND subs_week <> 53
		GROUP BY subs_year
			, subs_week
			, subs_week_and_year
			, node_TP;-- segment

	-----------------------------------------------------------------------------------------------------------
	----------  Pipeline entry events -----------------------------------------------------------------
	-----------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #Regr_inputs;
		SELECT quarter_id
			, agg.TP_forecast_segment
			, row_number() OVER (PARTITION BY agg.TP_forecast_segment ORDER BY quarter_id DESC ) AS x
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
		FROM #TP_weekly_agg AS agg
		GROUP BY quarter_id
			, agg.TP_forecast_segment;

	DROP TABLE IF EXISTS #Regr_coeff;
	
	SELECT TP_forecast_segment
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
		GROUP BY TP_forecast_segment
		HAVING n > 1000;

	UPDATE #Regr_coeff
	SET 	b1_BB_enter_CusCan 	= (sum_x_BB_enter_CusCan 	- (sum_BB_enter_CusCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);
		, 	b1_BB_enter_SysCan 	= (sum_x_BB_enter_SysCan 	- (sum_BB_enter_SysCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);
		, 	b1_BB_enter_HM 		= (sum_x_BB_enter_HM 		- (sum_BB_enter_HM * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);
		, 	b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party - (sum_BB_enter_3rd_party * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);
		,   b1_BB_Offer_Applied = (sum_x_BB_Offer_Applied - (sum_BB_Offer_Applied * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);
	
	UPDATE #Regr_coeff
	SET  	b0_BB_enter_CusCan 	= sum_BB_enter_CusCan / n - b1_BB_enter_CusCan * sum_x / n;
		, 	b0_BB_enter_SysCan 	= sum_BB_enter_SysCan / n - b1_BB_enter_SysCan * sum_x / n;
		, 	b0_BB_enter_HM 		= sum_BB_enter_HM / n - b1_BB_enter_HM * sum_x / n;
		, 	b0_BB_enter_3rd_party = sum_BB_enter_3rd_party / n - b1_BB_enter_3rd_party * sum_x / n;
		, 	b0_BB_Offer_Applied = sum_BB_Offer_Applied / n - b1_BB_Offer_Applied * sum_x / n;

	---------------------------------------------------------------------------------------------------
	-- Set proc outputs -------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------------------
	SELECT LV
		, 'CusCan Entry' AS Metric
		, TP_forecast_segment AS forecast_segment
		, b1_BB_enter_CusCan
		, b0_BB_enter_CusCan
	FROM #Regr_coeff
	UNION ALL
	SELECT LV
		, 'SysCan Entry' AS Metric
		, TP_forecast_segment AS forecast_segment
		, b1_BB_enter_SysCan
		, b0_BB_enter_SysCan
	FROM #Regr_coeff
	UNION ALL
	SELECT LV
		, 'HM Entry' AS Metric
		, TP_forecast_segment AS forecast_segment
		, b1_BB_enter_HM
		, b0_BB_enter_HM
	FROM #Regr_coeff
	UNION ALL
	SELECT LV
		, '3rd Party Entry' AS Metric
		, TP_forecast_segment AS forecast_segment
		, b1_BB_enter_3rd_party
		, b0_BB_enter_3rd_party
	FROM #Regr_coeff
	UNION ALL
	SELECT LV
		, 'BB Offer Applied' AS Metric
		, TP_forecast_segment AS forecast_segment
		, b1_BB_Offer_Applied
		, b0_BB_Offer_Applied
	FROM #Regr_coeff
END
GO

