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
		INTO Forecast_Loop_Table
		FROM Forecast_Loop_Table_2
END

GO

