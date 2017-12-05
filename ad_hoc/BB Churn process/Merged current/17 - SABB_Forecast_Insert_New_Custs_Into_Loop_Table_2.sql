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

	SELECT * INTO #Sky_Calendar FROM /*CITeam.*/subs_Calendar(Forecast_Start_Wk / 100, Forecast_End_Wk / 100);

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
		, Sky_plus  	
		, nowtv_flag 
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
		, Sky_plus  	
		, nowtv_flag 
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


