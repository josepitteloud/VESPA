-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE Forecast_SABB_Rates (IN Forecast_Start_Wk INT) --(In Y1W01 integer,In Y3W52 integer)
		Result (Subs_Week SMALLINT
		, SABB_forecast_segment VARCHAR(50) ---- replacement for ,Cuscan_forecast_segment varchar(50) - this is the SABB segment (low level not high level)
		, pred_SysCan_rate FLOAT
		, pred_CusCan_rate FLOAT
		, pred_HM_rate FLOAT
		, pred_3rd_party_rate FLOAT
		, pred_BB_Offer_Applied_rate FLOAT
		)

BEGIN
	DECLARE @var_End_date DATE;
	DECLARE @_1st_Wk1 INT;
	DECLARE @_Lst_Wk INT;
		MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Rates - Initialization begin ' TO CLIENT;

	SELECT * INTO #sky_calendar FROM subs_calendar(Forecast_Start_Wk / 100 - 3, Forecast_Start_Wk / 100);

	SET @var_End_date 	= (SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = Forecast_Start_Wk );
	SET @_Lst_Wk 		= (SELECT max(subs_week_and_year) FROM #sky_calendar WHERE calendar_date = @var_End_date );
	SET @_1st_Wk1 = CASE WHEN Forecast_Start_Wk % 100 < 52 THEN (Forecast_Start_Wk / 100 - 3) * 100 + Forecast_Start_Wk % 100 ELSE (Forecast_Start_Wk / 100 - 2) * 100 + 1 END;
	-- Select _1st_Wk1,_Lst_Wk;
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Rates - Initialization DONE' TO CLIENT;
	--select last 3 years, flag last year and year prior to that
	DROP TABLE IF EXISTS #SABB_weekly_agg;
		SELECT subs_year
			, subs_week_of_year AS subs_week
			, cast(subs_week_and_year AS INT) AS subs_week_and_year
			, node_sa AS SABB_forecast_segment
			, Count(*) AS n
			, BB_Offer_Applied ---??? need to change this to the right field when we have it
			, Cast(sum(bb_enter_SysCan) AS FLOAT) AS bb_enter_SysCan
			, Cast(sum(bb_enter_CusCan) AS FLOAT) AS bb_enter_CusCan
			, Cast(sum(bb_enter_HM) AS FLOAT) AS bb_enter_HM
			, Cast(sum(bb_enter_3rd_party) AS FLOAT) AS bb_enter_3rd_party
			, dense_rank() OVER (
				ORDER BY subs_week_and_year DESC
				) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, (week_id / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY Subs_Year) Max_Subs_Week
		INTO #SABB_weekly_agg
		--from CITeam.DTV_Fcast_Weekly_Base agg
		FROM pitteloudj.DTV_Fcast_Weekly_Base_2 agg ---??? update this source later
		WHERE subs_week_and_year BETWEEN @_1st_Wk1 AND @_Lst_Wk
		--         and subs_week != 53
		--   and Downgrade_View = 'Actuals'    --??? add this back in when we have donwgrade_view re-added to the data
		GROUP BY subs_year
			, subs_week
			, subs_week_and_year
			, node_sa -- segment
			;
	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Rates - Creating Table: #SABB_weekly_agg DONE: '||@@rowcount TO CLIENT;
	
	UPDATE #SABB_weekly_agg
	SET subs_week = subs_week - 1
	WHERE Max_Subs_Week = 53;

	UPDATE #SABB_weekly_agg
	SET Subs_Week_And_Year = Subs_Year * 100 + subs_week;

	DELETE 	FROM #SABB_weekly_agg 	WHERE subs_week = 0;

	--for each customer segment and week, action counts for current and previous year
	DROP TABLE IF EXISTS #SABB_forecast_summary_1;
		SELECT subs_week
			, SABB_forecast_segment
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
		FROM #SABB_weekly_agg agg
		GROUP BY subs_week
			, SABB_forecast_segment;
	
	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Rates - Creating Table: #SABB_forecast_summary_1 DONE: '||@@rowcount TO CLIENT;
	
	--for each customer segment (but not week), no action new offer counts for last quarter
	DROP TABLE

	IF EXISTS #cuscan_forecast_summary_LQ;
		SELECT SABB_forecast_segment
			, sum(n) AS LQ_n
			, sum(BB_offer_applied) AS LQ_BB_Offer
		INTO #cuscan_forecast_summary_LQ
		FROM #SABB_weekly_agg
		WHERE last_quarter = 'Y'
		GROUP BY SABB_forecast_segment;

	--add LQ volumes onto previous summary table
	UPDATE #SABB_forecast_summary_1
	SET a.LQ_n = b.LQ_n
		, a.LQ_BB_Offer = b.LQ_BB_Offer
	FROM #SABB_forecast_summary_1 AS a
	LEFT JOIN #cuscan_forecast_summary_LQ AS b ON a.SABB_forecast_segment = b.SABB_forecast_segment;

	--create rates from action counts and cell size
	DROP TABLE

	IF EXISTS #SABB_forecast_summary_2;
		SELECT *
			-- Curr Years Weekly Rates
			, CASE WHEN curr_n != 0 THEN (cast(curr_bb_enter_SysCan AS FLOAT) / cast(curr_n AS FLOAT)) ELSE 0 END AS curr_bb_enter_SysCan_rate
			, CASE WHEN curr_n != 0 THEN (cast(curr_bb_enter_CusCan AS FLOAT) / cast(curr_n AS FLOAT)) ELSE 0 END AS curr_bb_enter_CusCan_rate
			, CASE WHEN curr_n != 0 THEN (cast(curr_bb_enter_HM AS FLOAT) / cast(curr_n AS FLOAT)) ELSE 0 END AS curr_bb_enter_HM_rate
			, CASE WHEN curr_n != 0 THEN (cast(curr_bb_enter_3rd_party AS FLOAT) / cast(curr_n AS FLOAT)) ELSE 0 END AS curr_bb_enter_3rd_party_rate
			, CASE WHEN curr_n != 0 THEN (cast(curr_BB_Offer_Applied AS FLOAT) / cast(curr_n AS FLOAT)) ELSE 0 END AS curr_BB_Offer_Applied_rate
			-- Prev Years Weekly Rates
			, CASE WHEN prev_n != 0 THEN (cast(prev_bb_enter_SysCan AS FLOAT) / cast(prev_n AS FLOAT)) ELSE 0 END AS prev_bb_enter_SysCan_rate
			, CASE WHEN prev_n != 0 THEN (cast(prev_bb_enter_CusCan AS FLOAT) / cast(prev_n AS FLOAT)) ELSE 0 END AS prev_bb_enter_CusCan_rate
			, CASE WHEN prev_n != 0 THEN (cast(prev_bb_enter_HM AS FLOAT) / cast(prev_n AS FLOAT)) ELSE 0 END AS prev_bb_enter_HM_rate
			, CASE WHEN prev_n != 0 THEN (cast(prev_bb_enter_3rd_party AS FLOAT) / cast(prev_n AS FLOAT)) ELSE 0 END AS prev_bb_enter_3rd_party_rate
			, CASE WHEN prev_n != 0 THEN (cast(prev_BB_Offer_Applied AS FLOAT) / cast(prev_n AS FLOAT)) ELSE 0 END AS prev_BB_Offer_Applied_rate
			-- Last Quarters Weekly Rates
			, cast(LQ_BB_Offer AS FLOAT) / cast(LQ_n AS FLOAT) AS LQ_DTV_Offer_rate
			, cast(curr_n AS FLOAT) / (cast(curr_n AS FLOAT) + cast(prev_n AS FLOAT)) AS curr_share
			, 1 - curr_share AS prev_share
		INTO #SABB_forecast_summary_2
		FROM #SABB_forecast_summary_1;

	DROP TABLE IF EXISTS #SABB_forecast_summary_3;
		SELECT *
			, /*(curr_share **/ curr_bb_enter_SysCan /*) + (prev_share *prev_TA_Call_cnt_rate     )*/ AS pred_bb_enter_SysCan_rate
			, /*(curr_share **/ curr_bb_enter_CusCan /*) + (prev_share *prev_TA_Call_Cust_rate    )*/ AS pred_bb_enter_CusCan_rate
			, /*(curr_share **/ curr_bb_enter_HM /*) + (prev_share *prev_Web_Chat_TA_Cnt_rate )*/ AS pred_bb_enter_HM_rate
			, /*(curr_share **/ curr_bb_enter_3rd_party /*) + (prev_share *prev_Web_Chat_TA_Cust_rate)*/ AS pred_bb_enter_3rd_party_rate
			, /*(curr_share **/ curr_BB_Offer_Applied /*) + (prev_share *prev_NonTA_DTV_Offer_Applied_rate  )*/ AS pred_BB_Offer_Applied_rate
		INTO #SABB_forecast_summary_3
		FROM #SABB_forecast_summary_2;

	--final output for use in forecasting
	SELECT subs_week
		, SABB_forecast_segment
		, pred_bb_enter_SysCan_rate
		, pred_bb_enter_CusCan_rate
		, pred_bb_enter_HM_rate
		, pred_bb_enter_3rd_party_rate
		, pred_BB_Offer_Applied_rate
	FROM #SABB_forecast_summary_3 AS a;
	
	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Rates - COMPLETED: '||@@rowcount TO CLIENT;
END;

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON Forecast_SABB_Rates TO CITeam;





/*  changing this so that it holds the movements to the four pipeline states that we wish to model.
TA movements are out of scope so we will not include those, though there may need to be some tracking 

CREATE variable var_End_date DATE;
CREATE variable _1st_Wk1 INT;
CREATE variable _Lst_Wk INT;
CREATE variable Forecast_Start_Wk INT;
SET Forecast_Start_Wk = 201553;

-- First you need to impersonate CITeam
-- Setuser CITeam;
*/
-- assumption is that the following fields exist on dtv_fcast_weekly_base:
-- node
-- offer_applied_BB
-- bb_enter_SysCan
-- bb_enter_CusCan
-- bb_enter_HM
-- bb_enter_3rd_party
-- bb_pc ??? what is this  use dtv_pc for now but look into what we need here, if anything at all