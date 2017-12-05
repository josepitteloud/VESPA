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
	DECLARE End_date DATE;
	
	SELECT * INTO #Sky_Calendar FROM /*CITeam.*/Subs_Calendar(LV / 100 - Regression_Yrs - 1, LV / 100);
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
	SET End_date = (SELECT max(calendar_date) FROM #Sky_Calendar WHERE Subs_week_and_year = Y3W52);

	DROP TABLE IF EXISTS #SABB_weekly_agg;
	
	--------------------------------------------------------------------------------------------------------------------------------------------		

	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
		WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	End_date BETWEEN effective_from_dt AND effective_to_dt ;
	-------------------------------------------------------------------------------------------------------------------------------------------		
		message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - #skyplus' TO client;
	
		DROP TABLE IF EXISTS #t_acct;
	SELECT CAST(LEFT(agg.subs_week_and_year, 4) AS INT) 		AS subs_year
			, CAST(RIGHT(agg.subs_week_and_year, 2) AS INT)	AS subs_week
			, cast(agg.subs_week_and_year AS INT) AS subs_week_and_year
			, dense_rank() OVER (ORDER BY agg.subs_week_and_year DESC	) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, ((week_id-1) / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) AS Max_Subs_Week
			, MAX(bb_enter_SysCan) 		bb_enter_SysCan
			, MAX(x.cuscan) 		bb_enter_CusCan
			, MAX(x.HM) 			bb_enter_HM
			, MAX(bb_enter_3rd_party) bb_enter_3rd_party
			, agg.end_date 
			, agg.account_number
	INTO #t_acct
	FROM citeam.CUST_Fcast_Weekly_Base AS agg 
	LEFT JOIN HM_fix AS x ON agg.account_number = x.account_number AND x.Subs_Week_and_Year = agg.Subs_Week_and_Year
	LEFT JOIN #skyplus AS b ON b.account_number = agg.account_number 
	LEFT JOIN citeam.nowtv_accounts_ents     AS c ON c.account_number = agg.account_number AND End_date BETWEEN period_start_date AND period_end_date
	WHERE subs_week_and_year BETWEEN Y1W01 AND Y3W52 AND subs_week <> 53
			AND (b.account_number IS NULL OR c.account_number IS NULL )
			AND agg.bb_active = 1 And agg.dtv_active =0 
	GROUP BY  subs_year
			, subs_week
			, agg.subs_week_and_year
			, agg.end_date 
			, agg.account_number
			;
	
	--CREATE HG INDEX id1 ON t_acct(account_number);
	--CREATE HG INDEX id2 ON t_acct(end_date);
		
	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - #t_acct' TO client;
	
	SELECT 	  a.subs_year
			, a.subs_week
			, a.subs_week_and_year
			, a.week_id
			, a.week_position
			, a.last_quarter
			, a.quarter_id
			, a.Max_Subs_Week
			, cast(sum(Offer_Applied_BB) AS REAL) AS BB_Offer_Applied	 	-- TODO: Check this definition of offer applied
			, cast(sum(bb_enter_SysCan) AS REAL) 	AS bb_enter_SysCan
			, cast(sum(bb_enter_CusCan) AS REAL) 	AS bb_enter_CusCan
			, cast(sum(bb_enter_HM) AS REAL) 		AS bb_enter_HM
			, cast(sum(bb_enter_3rd_party) AS REAL) AS bb_enter_3rd_party
			,  CAST(CASE 	WHEN node IN (22, 46, 49, 70, 75, 71) THEN 1
													WHEN node IN ( 83, 53, 43, 82, 73, 57) THEN 2
													WHEN node IN ( 63, 47, 68, 42, 62, 12, 39, 11, 35) THEN 3
													WHEN node IN ( 21, 74, 72) THEN 4
													WHEN node IN ( 40, 36, 66, 60, 65) THEN 5
													WHEN node IN ( 77, 31, 84, 56, 76) THEN 6
													WHEN node IN ( 10, 41, 67) THEN 7
													WHEN node IN ( 61, 51, 64, 24, 50) THEN 8
													WHEN node IN ( 27, 55, 85, 81, 79, 80, 54) THEN 9
													WHEN node IN ( 9) THEN 10
													ELSE 0 END AS VARCHAR(4))		AS SABB_forecast_segment
			, Count(*) 								AS n
	INTO #SABB_weekly_agg
	FROM #t_acct		AS a 
	JOIN pitteloudj.DTV_FCAST_WEEKLY_BASE AS b ON a.account_number  = b.account_number AND a.end_date = b.end_date 
	LEFT JOIN BB_SABB_Churn_segments_lookup AS c  ON b.BB_offer_rem_and_end = c.BB_offer_rem_and_end
												AND b.BB_tenure 			= c.BB_tenure 
												AND b.my_sky_login_3m 		= c.my_sky_login_3m
												AND b.talk_type 			= c.talk_type
												AND b.home_owner_status 	= c.home_owner_status
												AND b.BB_all_calls_1m 		= c.BB_all_calls_1m
	GROUP BY a.subs_year
			, a.subs_week
			, a.subs_week_and_year
			, a.week_id
			, a.week_position
			, a.last_quarter
			, a.quarter_id
			, a.Max_Subs_Week
			, SABB_forecast_segment;
	
	DROP TABLE #t_acct;
	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - #SABB_weekly_agg' TO client	;
		
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

