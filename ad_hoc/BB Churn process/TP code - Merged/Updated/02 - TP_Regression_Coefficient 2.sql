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
	DECLARE End_date DATE;

	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;
	
	message cast(now() AS TIMESTAMP) || ' | TP_Regression_Coefficient - Initialization begin ' TO client;																							

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

	message cast(now() AS TIMESTAMP) || ' | TP_Regression_Coefficient - y1w01:'||Y1W01 TO client;
	message cast(now() AS TIMESTAMP) || ' | TP_Regression_Coefficient - y3w52:'||Y3W52 TO client;
	message cast(now() AS TIMESTAMP) || ' | TP_Regression_Coefficient - end_date:'||End_date TO client;
	DROP TABLE IF EXISTS #TP_weekly_agg;
	

	--------------------------------------------------------------------------------------------------------------------------------------------		
/*
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
		WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	End_date BETWEEN effective_from_dt AND effective_to_dt ;
		
*/
	-------------------------------------------------------------------------------------------------------------------------------------------		
		message cast(now() AS TIMESTAMP) || ' | TP_Regression_Coefficient - #skyplus' TO client;
	
	DROP TABLE IF EXISTS #t_acct;
	SELECT 
			  agg.end_date 
			, CAST (x.Subs_Year 			AS INT) AS Subs_Year
			, CAST (RIGHT(CAST(x.Subs_Week_And_Year 	AS VARCHAR) ,2) 	AS INT)	AS Subs_week
			, CAST (x.Subs_Week_And_Year 	AS INT) AS Subs_Week_And_Year
			
			, sum(CAST(agg.Offer_Applied_BB AS REAL)) 	AS BB_Offer_Applied	 	-- TODO: Check this definition of offer applied
			, sum(CAST(b.enter_SysCan AS REAL)) 		AS bb_enter_SysCan
			, sum(CAST(b.enter_CusCan AS REAL)) 		AS bb_enter_CusCan
			, sum(CAST(b.enter_HM AS REAL)) 			AS bb_enter_HM
			, sum(CAST(b.enter_3rd_party AS REAL)) 		AS bb_enter_3rd_party
			, CAST(sub_segment AS VARCHAR(4)) 					AS TP_forecast_segment
			, Count(*) 									AS n
	
	INTO #TP_weekly_agg1
	FROM citeam.DTV_Fcast_Weekly_Base AS agg ---??? update this source later
	JOIN #sky_calendar AS x ON agg.end_date = x.calendar_date 
	LEFT JOIN CITeam.Broadband_Comms_Pipeline AS b ON agg.account_number  = b.account_number 
													AND x.Subs_Week_And_Year = b.Subs_Week_And_Year 
													AND b.BB_Cust_Type = 'Triple Play'
													AND b.ProdPlat_Churn_Type = 'Product'
	LEFT JOIN my_sky AS y ON agg.account_number = y.account_number AND y.end_date = agg.end_date																												
	LEFT JOIN BB_TP_Product_Churn_segments_lookup AS c  ON  agg.DTV_TA_calls_1m 	= c.DTV_TA_calls_1m
														AND COALESCE(agg.RTM , 'UNKNOWN')	= c.RTM 
														AND y.mysky				= c.my_sky_login_3m
														AND agg.Talk_tenure 	= c.Talk_tenure
														AND CASE 	WHEN trim(agg.simple_segment) IN ('1 Secure') THEN '1 Secure' 
															WHEN trim(agg.simple_segment) IN ('2 Start') THEN '2 Start' 
															WHEN trim(agg.simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
															WHEN trim(agg.simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
															WHEN trim(agg.simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
															WHEN trim(agg.simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
															ELSE 'UNKNOWN' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
															END		 		= c.Simple_Segment
														AND agg.BB_all_calls_1m 	= c.BB_all_calls_1m
	--LEFT JOIN #skyplus AS b ON b.account_number = agg.account_number 
	--LEFT JOIN NOW_TV_SUBS_HIST   AS c ON c.account_number = agg.account_number AND End_date BETWEEN effective_from_dt AND effective_to_dt
	WHERE subs_week_and_year BETWEEN Y1W01 AND Y3W52 AND subs_week <> 53
			--AND (b.account_number IS NULL OR c.account_number IS NULL )
			AND agg.bb_active = 1 And agg.dtv_active =1
	GROUP BY subs_year
			, subs_week
			, subs_week_and_year
			, TP_forecast_segment
			, agg.end_date ;
	
	SELECT *
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) AS Max_Subs_Week
			, dense_rank() OVER (ORDER BY subs_week_and_year DESC) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, ((week_id-1) / 13) + 1 AS quarter_id
	INTO #TP_weekly_agg
	FROM #TP_weekly_agg1;
	
	DROP TABLE #TP_weekly_agg1;
	message cast(now() AS TIMESTAMP) || ' | TP_Regression_Coefficient - #TP_weekly_agg: '||@@rowcount TO client	;
		
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

