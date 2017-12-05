CREATE OR REPLACE PROCEDURE TP_Forecast_Create_New_Forecast_Loop_Table ()

BEGIN
	DROP TABLE

	IF EXISTS TP_ForeCAST_Loop_Table;
		SELECT account_number
			, end_date
			, subs_week_and_year
			, subs_year
			, subs_week_of_year
			
			, weekid
			, BB_Status_Code
			, BB_Status_Code_EoW
			, BB_Segment
			, country
			, BB_package
			
			, churn_type
			, DTV_TA_calls_1m_raw
			, DTV_TA_calls_1m
			, RTM
			, Talk_tenure_raw
			
			, Talk_tenure
			, my_sky_login_3m_raw
			, my_sky_login_3m
			, BB_all_calls_1m_raw
			, BB_all_calls_1m
			
			, Simple_Segments
			, TP_forecast_segment
			, segment_TP
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
			, TP_flag
			
			, Sky_plus  	
			, nowtv_flag 
			
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
			, DTV_LW
		INTO TP_ForeCAST_Loop_Table
		FROM TP_ForeCAST_Loop_Table_2
END

GO

