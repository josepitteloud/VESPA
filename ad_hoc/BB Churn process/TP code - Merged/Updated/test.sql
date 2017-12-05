CREATE OR REPLACE PROCEDURE Forecast_TP_Rates (IN Forecast_Start_Wk INT) result (
	Subs_Week SMALLINT
	, TP_forecast_segment VARCHAR(50)
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
	message cast(now() AS TIMESTAMP) || ' | Forecast_TP_Rates - Initialization begin ' TO client;
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

	DROP TABLE IF EXISTS #TP_weekly_agg;
	message cast(now() AS TIMESTAMP) || ' | Forecast_TP_Rates - checkpoint 1 ' TO client;	
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
		AND     	var_End_date BETWEEN effective_from_dt AND effective_to_dt ;
*/	
	--------------------------------------------------------------------------------------------------------------------------------------------	
		
	message cast(now() AS TIMESTAMP) || ' | Forecast_TP_Rates - checkpoint 2 ' TO client;	
	
		SELECT cast(LEFT(z.subs_week_and_year,4)  AS INT)  AS subs_year
			, cast(RIGHT(z.subs_week_and_year,2)  AS INT) as Subs_week
			, cast(z.subs_week_and_year AS INT) AS subs_week_and_year
			, CAST(d.node AS VARCHAR(4))	 AS TP_forecast_segment
			, Count() AS n
			, cast(sum(agg.Offer_Applied_BB) AS REAL) AS BB_Offer_Applied				-- TODO: Affected by the bb_offerapplied
			, cast(sum(z.bb_enter_SysCan) AS REAL) AS bb_enter_SysCan
			, cast(sum(z.bb_enter_CusCan) AS REAL) AS bb_enter_CusCan
			, cast(sum(z.bb_enter_HM) AS REAL) AS bb_enter_HM
			, cast(sum(z.bb_enter_3rd_party) AS REAL) AS bb_enter_3rd_party
			, dense_rank() OVER (ORDER BY z.subs_week_and_year DESC) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, ((week_id-1) / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) AS Max_Subs_Week
			,cast(0 as smallint) as Max_week_id
		INTO #TP_weekly_agg
		FROM pitteloudj.DTV_Fcast_Weekly_Base AS agg
		JOIN citeam.CUST_Fcast_Weekly_Base AS z ON agg.account_number = z.account_number AND agg.end_date = z.end_date
		LEFT JOIN BB_TP_Product_Churn_segments_lookup AS d  ON agg.DTV_TA_calls_1m 	= d.DTV_TA_calls_1m
													 AND agg.RTM 				= d.RTM 
													 AND agg.my_sky_login_3m 		= d.my_sky_login_3m
													 AND agg.Talk_tenure 				= d.Talk_tenure
													 AND agg.Simple_Segment 		= d.Simple_Segment
													 AND agg.BB_all_calls_1m 		= d.BB_all_calls_1m 
		--LEFT JOIN #skyplus AS b ON agg.account_number = b.account_number 
		--LEFT JOIN NOW_TV_SUBS_HIST AS c ON agg.account_number = c.account_number AND var_End_date BETWEEN effective_from_dt AND effective_to_dt
		WHERE subs_week_and_year BETWEEN _1st_Wk1 AND _Lst_Wk
			AND agg.bb_active = 1 And agg.dtv_active =1 and subs_week<>53 
		--	AND (b.account_number IS NULL  OR c.account_number IS NULL )
		GROUP BY subs_year
			, subs_week
			, z.subs_week_and_year
			, TP_forecast_segment
			;
		message cast(now() AS TIMESTAMP) || ' | Forecast_TP_Rates - checkpoint 3 ' TO client;	
		
	UPDATE #TP_weekly_agg
	SET week_position = CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' 
							WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END
		, last_quarter = CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END
		, quarter_id = ((week_id - 1) / 13) + 1;

	UPDATE #TP_weekly_agg
	SET subs_week = subs_week - 1
	WHERE Max_Subs_Week = 53;

	UPDATE #TP_weekly_agg
	SET Subs_Week_And_Year = Subs_Year * 100 + subs_week;

	DELETE
	FROM #TP_weekly_agg
	WHERE subs_week = 0;


	--Update #TP_weekly_agg Set subs_week = subs_week - 1 where Max_Subs_Week = 53;

	--Update #TP_weekly_agg Set Subs_Week_And_Year = Subs_Year*100 + subs_week;

	DROP TABLE IF EXISTS #TP_forecast_summary_1;
	SELECT subs_week
			, TP_forecast_segment
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
		INTO #TP_forecast_summary_1
		FROM #TP_weekly_agg AS agg
		GROUP BY subs_week
			, TP_forecast_segment;

	DROP TABLE

	IF EXISTS #cuscan_forecast_summary_LQ;
		SELECT TP_forecast_segment
			, sum(n) AS LQ_n
			, sum(BB_offer_applied) AS LQ_BB_Offer
		INTO #cuscan_forecast_summary_LQ
		FROM #TP_weekly_agg
		WHERE last_quarter = 'Y'
		GROUP BY TP_forecast_segment;

	message cast(now() AS TIMESTAMP) || ' | 7' TO client;

	UPDATE #TP_forecast_summary_1 AS a
	SET a.LQ_n = b.LQ_n
		, a.LQ_BB_Offer = b.LQ_BB_Offer
	FROM #TP_forecast_summary_1 AS a
	LEFT JOIN #cuscan_forecast_summary_LQ AS b ON a.TP_forecast_segment = b.TP_forecast_segment;

	DROP TABLE

	IF EXISTS #TP_forecast_summary_2;
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
		INTO #TP_forecast_summary_2
		FROM #TP_forecast_summary_1;

	DROP TABLE IF EXISTS #TP_forecast_summary_3;
	
	SELECT *
			, curr_bb_enter_SysCan_rate AS pred_bb_enter_SysCan_rate
			, curr_bb_enter_CusCan_rate AS pred_bb_enter_CusCan_rate
			, curr_bb_enter_HM_rate AS pred_bb_enter_HM_rate
			, curr_bb_enter_3rd_party_rate AS pred_bb_enter_3rd_party_rate
			, curr_BB_Offer_Applied_rate AS pred_BB_Offer_Applied_rate
		INTO #TP_forecast_summary_3
		FROM #TP_forecast_summary_2;

	message cast(now() AS TIMESTAMP) || ' | Forecast_TP_Rates - END' TO client;		
	SELECT subs_week
		, TP_forecast_segment
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
	FROM #TP_forecast_summary_3 AS a
END
GO

