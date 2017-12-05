-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Create_New_Forecast_Loop_Table;

Create procedure CITeam.Forecast_Create_New_Forecast_Loop_Table()
SQL Security INVOKER
BEGIN

Drop table if exists Forecast_Loop_Table;
Select account_number,
end_date,
subs_week_and_year,
subs_week_of_year,
weekid,
DTV_Status_Code,
DTV_PC_Future_Sub_Effective_Dt,
DTV_AB_Future_Sub_Effective_Dt,
DTV_Status_Code_EoW,
BB_Segment,
prem_segment,
Simple_Segments,
country,
Affluence,
package_desc,
offer_length_DTV,
curr_offer_start_date_DTV,
Curr_Offer_end_Date_Intended_DTV,
Prev_offer_end_date_DTV,
Time_To_Offer_End_DTV,
curr_offer_start_date_BB,
Curr_Offer_end_Date_Intended_BB,
Prev_offer_end_date_BB,
Time_To_Offer_End_BB,
curr_offer_start_date_LR,
Curr_Offer_end_Date_Intended_LR,
Prev_offer_end_date_LR,
Time_To_Offer_End_LR,
DTV_BB_LR_offer_end_dt,
Time_To_Offer_End,
DTV_Tenure,
dtv_act_date,
Time_Since_Last_TA_call,
Last_TA_Call_dt,
Time_Since_Last_AB,
Last_AB_Dt,
Previous_AB_Count,
Previous_Abs,
CusCan_Forecast_Segment,
SysCan_Forecast_Segment,
DTV_Activation_Type,
dtv_latest_act_date,
dtv_first_act_date,
HD_segment,
rand_action_Cuscan,
rand_action_Syscan,
rand_TA_Vol,
rand_WC_Vol,
rand_TA_Save_Vol,
rand_WC_Save_Vol,
rand_TA_DTV_Offer_Applied,
rand_NonTA_DTV_Offer_Applied,
rand_TA_DTV_PC_Vol,
rand_WC_DTV_PC_Vol,
rand_Other_DTV_PC_Vol,
rand_Intrawk_DTV_PC,
rand_DTV_PC_Duration,
rand_DTV_PC_Status_Change,
rand_New_Off_Dur,
rand_Intrawk_DTV_AB

into Forecast_Loop_Table
from Forecast_Loop_Table_2;

END;


-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_Create_New_Forecast_Loop_Table to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Create_New_Forecast_Loop_Table;


