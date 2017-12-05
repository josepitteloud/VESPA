-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Create_New_Forecast_Loop_Table;

CREATE OR REPLACE PROCEDURE SABB_Forecast_Create_New_Forecast_Loop_Table () 
SQL Security INVOKER
BEGIN


Drop table if exists Forecast_Loop_Table;

Select 

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
	, SABB_forecast_segment
	, segment_SA
	, PL_Future_Sub_Effective_Dt
--	, PC_Future_Sub_Effective_Dt
--	, AB_Future_Sub_Effective_Dt
--	, BCRQ_Future_Sub_Effective_Dt
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

into Forecast_Loop_Table
from Forecast_Loop_Table_2;

END;


-- Grant execute rights to the members of CITeam
grant execute on SABB_Forecast_Create_New_Forecast_Loop_Table to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call SABB_Forecast_Create_New_Forecast_Loop_Table;


