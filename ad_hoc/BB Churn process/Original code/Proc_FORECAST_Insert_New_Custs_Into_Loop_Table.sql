-- First you need to impersonate CITeam
SETUSER CITeam;

-- Drop procedure if exists CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2;
CREATE PROCEDURE CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2 (
	IN Forecast_Start_Wk INT
	, IN Forecast_End_Wk INT
	, IN True_Sample_Rate FLOAT
	) SQL Security INVOKER

BEGIN
	DECLARE new_cust_end_date DATE;
	DECLARE new_cust_subs_week_and_year INT;
	DECLARE new_cust_subs_week_of_year INT;
	DECLARE multiplier BIGINT;

	SET multiplier = DATEPART(millisecond, now()) + 2631;
	SELECT * INTO #Sky_Calendar FROM CITeam.subs_Calendar(Forecast_Start_Wk / 100, Forecast_End_Wk / 100);

	---------------------------------------------------------------
	--INSERT NEW CUSTOMERS
	----------------------------------------------------------------
	SET new_cust_end_date = (SELECT max(end_date + 7) FROM Forecast_Loop_Table_2);
	SET new_cust_subs_week_and_year = (SELECT max(subs_week_and_year) FROM #sky_calendar WHERE calendar_date = new_cust_end_date);
	SET new_cust_subs_week_of_year = (SELECT max(subs_week_of_year) FROM #sky_calendar WHERE calendar_date = new_cust_end_date);

	-- select new_end_date, new_subs_week_and_year, new_subs_week_of_year;
	DROP TABLE IF EXISTS #new_customers_last_2Yrs_2;

	SELECT * , rand(number(*) * multiplier + 163456) AS rand_sample
	INTO #new_customers_last_2Yrs_2
	FROM CITeam.Forecast_New_Cust_Sample;

	DROP TABLE IF EXISTS #new_customers_last_2Yrs_3;

	SELECT * , row_number() OVER (ORDER BY rand_sample) Rand_Rnk
	INTO #new_customers_last_2Yrs_3
	FROM #new_customers_last_2Yrs_2;

	DELETE #new_customers_last_2Yrs_3 new_cust
	FROM #new_customers_last_2Yrs_3 new_cust
	INNER JOIN Activation_Vols act ON new_cust.Rand_Rnk > act.New_Customers * true_sample_rate 
				AND act.subs_week_of_year = new_cust_subs_week_of_year;

	INSERT INTO Forecast_Loop_Table_2 (
		account_number
		, end_date
		, subs_week_and_year
		, subs_week_of_year
		, weekid
		, DTV_Status_Code_EoW
		, DTV_PC_Future_Sub_Effective_Dt
		, DTV_AB_Future_Sub_Effective_Dt
		, BB_segment
		, prem_segment
		, Simple_Segments
		, country
		, Affluence
		, package_desc
		, offer_length_DTV
		, HD_Segment
		, Curr_Offer_end_Date_Intended_DTV
		, curr_offer_start_date_DTV
		, Prev_offer_end_date_DTV
		, Time_To_Offer_End_DTV
		, Curr_Offer_end_Date_Intended_BB
		, curr_offer_start_date_BB
		, Prev_offer_end_date_BB
		, Time_To_Offer_End_BB
		, Curr_Offer_end_Date_Intended_LR
		, curr_offer_start_date_LR
		, Prev_offer_end_date_LR
		, Time_To_Offer_End_LR
		, DTV_BB_LR_offer_end_dt
		, Time_To_Offer_End
		, DTV_Tenure
		, dtv_act_date
		, Time_Since_Last_TA_call
		, Last_TA_Call_dt
		, Time_Since_Last_AB
		, Last_AB_Dt
		, Previous_AB_Count
		, Previous_Abs
		, CusCan_Forecast_Segment
		, SysCan_Forecast_Segment
		, DTV_Activation_Type --,new_customer
		, TA_Call_Cust
		, TA_Call_Count
		, TA_Saves
		, TA_Non_Saves
		, WC_Call_Cust
		, WC_Call_Count
		, WC_Saves
		, WC_Non_Saves
		, DTV_AB
		, Cuscan_segment_count
		, Syscan_segment_count
		, CusCan
		, SysCan
		, pred_TA_Call_Cust_rate
		, pred_TA_Call_Cust_YoY_Trend
		, cum_TA_Call_Cust_rate
		, pred_Web_Chat_TA_Cust_rate
		, pred_Web_Chat_TA_Cust_YoY_Trend
		, cum_Web_Chat_TA_Cust_rate
		, cum_Web_Chat_TA_Cust_Trend_rate
		, pred_DTV_AB_rate
		, pred_DTV_YoY_Trend
		, cum_DTV_AB_rate
		, cum_DTV_AB_Trend_rate
		, pred_NonTA_DTV_Offer_Applied_rate
		, pred_NonTA_DTV_Offer_Applied_YoY_Trend
		, pred_TA_DTV_Offer_Applied_rate
		, pred_TA_DTV_Offer_Applied_YoY_Trend
		, DTV_Offer_Applied
		, pred_TA_DTV_PC_rate
		, pred_TA_Sky_Plus_Save_rate
		, cum_TA_DTV_PC_rate
		, pred_WC_DTV_PC_rate
		, pred_WC_Sky_Plus_Save_rate
		, cum_WC_DTV_PC_rate
		, pred_Other_DTV_PC_rate
		, TA_DTV_PC
		, WC_DTV_PC
		, TA_Sky_Plus_Save
		, WC_Sky_Plus_Save
		, Other_DTV_PC
		)
	SELECT --top 10000
		replicate(CHAR(65 + (counter - 1) % 53), (counter - 1) / 53 + 1) || a.account_number AS account_number
		, new_cust_end_date - 7 AS end_date
		, new_cust_subs_week_and_year AS subs_week_and_year
		, new_cust_subs_week_of_year AS subs_week_of_year
		, (year(new_cust_end_date) - 2010) * 52 + new_cust_subs_week_of_year AS weekid
		, DTV_Status_Code
		, DTV_PC_Future_Sub_Effective_Dt + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS DTV_PC_Future_Sub_Effective_Dt
		, DTV_AB_Future_Sub_Effective_Dt + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS DTV_AB_Future_Sub_Effective_Dt
		, BB_Segment
		, prem_segment
		, Simple_Segments
		, country
		, Affluence
		, package_desc
		, offer_length_DTV
		, HD_Segment
		, Curr_Offer_end_Date_Intended_DTV + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS Curr_Offer_end_Date_Intended_DTV
		, curr_offer_start_date_DTV + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS curr_offer_start_date_DTV
		, Prev_offer_end_date_DTV + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS Prev_offer_end_date_DTV
		, Time_To_Offer_End_DTV
		, Curr_Offer_end_Date_Intended_BB + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS Curr_Offer_end_Date_Intended_BB
		, curr_offer_start_date_BB + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS curr_offer_start_date_BB
		, Prev_offer_end_date_BB + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS Prev_offer_end_date_BB
		, Time_To_Offer_End_BB
		, Curr_Offer_end_Date_Intended_LR + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS Curr_Offer_end_Date_Intended_LR
		, curr_offer_start_date_LR + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS curr_offer_start_date_LR
		, Prev_offer_end_date_LR + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS Prev_offer_end_date_LR
		, Time_To_Offer_End_LR
		, DTV_BB_LR_offer_end_dt + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS DTV_BB_LR_offer_end_dt
		, Time_To_Offer_End
		, DTV_Tenure
		, a.dtv_act_date + Cast(new_cust_end_date AS INT) - Cast(a.end_date AS INT) AS dtv_act_date
		, Time_Since_Last_TA_call
		, Last_TA_Call_dt + Cast(new_cust_end_date AS INT) - Cast(end_date AS INT) AS Last_TA_Call_dt
		, Time_Since_Last_AB
		, Last_AB_Dt + Cast(new_cust_end_date AS INT) - Cast(end_date AS INT) AS Last_AB_Dt
		, Previous_AB_Count
		, Previous_Abs
		-- segments
		, CusCan_Forecast_Segment
		, SysCan_Forecast_Segment
		, DTV_Activation_Type
		-- new customers
		--,1 as new_customer -- is this the flag to use for inserting the new customers
		-- actions
		, Cast(0 AS TINYINT) AS TA_Call_Cust
		, Cast(0 AS TINYINT) AS TA_Call_Count
		, Cast(0 AS TINYINT) AS TA_Saves
		, Cast(0 AS TINYINT) AS TA_Non_Saves
		, Cast(0 AS TINYINT) AS WC_Call_Cust
		, Cast(0 AS TINYINT) AS WC_Call_Count
		, Cast(0 AS TINYINT) AS WC_Saves
		, Cast(0 AS TINYINT) AS WC_Non_Saves
		, Cast(0 AS TINYINT) AS DTV_AB
		, Cast(0 AS TINYINT) AS Cuscan_segment_count
		, Cast(0 AS TINYINT) AS Syscan_segment_count
		, Cast(0 AS TINYINT) AS CusCan
		, Cast(0 AS TINYINT) AS SysCan
		, Cast(0 AS FLOAT) AS pred_TA_Call_Cust_rate
		, Cast(0 AS FLOAT) AS pred_TA_Call_Cust_YoY_Trend
		, Cast(0 AS FLOAT) AS cum_TA_Call_Cust_rate
		, Cast(0 AS FLOAT) AS pred_Web_Chat_TA_Cust_rate
		, Cast(0 AS FLOAT) AS pred_Web_Chat_TA_Cust_YoY_Trend
		, Cast(0 AS FLOAT) AS cum_Web_Chat_TA_Cust_rate
		, Cast(0 AS FLOAT) AS cum_Web_Chat_TA_Cust_Trend_rate
		, Cast(0 AS FLOAT) AS pred_DTV_AB_rate
		, Cast(0 AS FLOAT) AS pred_DTV_YoY_Trend
		, Cast(0 AS FLOAT) AS cum_DTV_AB_rate
		, cast(0 AS FLOAT) AS cum_DTV_AB_Trend_rate
		, Cast(0 AS FLOAT) AS pred_NonTA_DTV_Offer_Applied_rate
		, Cast(0 AS FLOAT) AS pred_NonTA_DTV_Offer_Applied_YoY_Trend
		, Cast(0 AS FLOAT) AS pred_TA_DTV_Offer_Applied_rate
		, Cast(0 AS FLOAT) AS pred_TA_DTV_Offer_Applied_YoY_Trend
		, cast(0 AS TINYINT) AS DTV_Offer_Applied
		, cast(0 AS FLOAT) AS pred_TA_DTV_PC_rate
		, cast(0 AS FLOAT) AS pred_TA_Sky_Plus_Save_rate
		, Cast(0 AS FLOAT) AS cum_TA_DTV_PC_rate
		, cast(0 AS FLOAT) AS pred_WC_DTV_PC_rate
		, cast(0 AS FLOAT) AS pred_WC_Sky_Plus_Save_rate
		, Cast(0 AS FLOAT) AS cum_WC_DTV_PC_rate
		, Cast(0 AS FLOAT) AS pred_Other_DTV_PC_rate
		, cast(0 AS TINYINT) AS TA_DTV_PC
		, cast(0 AS TINYINT) AS WC_DTV_PC
		, cast(0 AS TINYINT) AS TA_Sky_Plus_Save
		, cast(0 AS TINYINT) AS WC_Sky_Plus_Save
		, cast(0 AS TINYINT) AS Other_DTV_PC
	FROM #new_customers_last_2Yrs_3 AS a;
END;

-- Grant execute rights to the members of CITeam
GRANT EXECUTE
	ON CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2
	TO CITeam;

-- Change back to your account
SETUSER;

-- Test it
Call CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2(201601, 201652);
