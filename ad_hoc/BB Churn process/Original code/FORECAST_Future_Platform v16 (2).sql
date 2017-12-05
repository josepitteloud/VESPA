/*----------------------------------------------------------------------------------------------------------*/
-------------------------------- Forecast Model Development Log ----------------------------------------------
/*----------------------------------------------------------------------------------------------------------*/
/*
V5  -- Initial 5Yr Plan and Q2 F'cast
V6  -- Update default rentention offer length from 6months to 10 months
V7  -- A year after rolling off an offer if a customer hasn't taken a new offer they move back into the lower risk No Offer segment
    -- Last Time since last TA updated so customers forecast to call TA will move into higher risk segments that have recently called TA
V8  -- Predicted rates and trends for Sky Q  customers applied to account for there being no history of Q customer events
V9  -- Logic added for Pending Cancels
V10 -- HD segment added to simulation table
    -- All figures updated with new TA definition (i.e. all Cancellation Attempts with a TA Save or Non-Save outcome
    -- Historic base update to use lookup table to assign customer cuscan/syscan segments
V11 -- Correction to churn fix so churn removes customers with TA
V12 -- CusCan Forecast Segment updated to use Time to Offer End across DTV,BB and LR instead of just DTV
    -- CusCan/SysCan Weekly Agg tables removed and replaced with CITeam.DTV_Fcast_Weekly_Base to speed up query
    -- Status code used as forecast segment in CITeam.DTV_Fcast_Weekly_Base
    -- DTV_BB_LR_offer_end_dt added to CITeam.Cust_Fcast_Weekly_Base
    -- cuscan_weekly_agg updated to aggregate CITeam.DTV_Fcast_Weekly_Base
    -- Code for cuscan_align_mXX and Syscan_align_mXXtables removed
    -- CITeam.Cust_Fcast_Weekly_Base used for churn vol in to Pred_Rates in place of CITeam.Weekly_Agg_Actions_CusCan
V13 -- Code corrected for sampling last 6 Wks of Acquisition
V14 -- Pending Cancels logic added to forecast
V15 -- Large blocks of code replaced by procedures
    -- Regression trend updated to remove oscillations in trend caused by starting and eding with different quarters, issue with null rates also corrected
    -- Logic for Cust_Fcast_Weekly_Base offers applied corrected so it uses whole offer start instead of individual offer legs
    -- Logic added for PC entries, durations and rectivations
    -- Phasing for CusCan rates table shift back 1Wk to account for Wk 53 where appropriate -- SysCan Rates TBC
    -- Offer duration for new offers based on distribution from last 6 Wks rather than fixed 10m duration
V16 -- New Segment added for customers in 1st of Tenure
    -- ROI Segments added
    -- Rate multiplier added to a buffer into forecast and prevent underforecasting
    -- All rate procs updated to only use actuals from DTV_Fcast_Weekly_Base table
*/

/*----------------------------------------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------------------------------------*/
-- Select * from sky_calendar where calendar_date = today()

----------------------------------------------------------------------------------------------------------------
-- PART 0: Test if forecast should run -------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
Drop variable if exists Nxt_sim_Dt; Create variable Nxt_sim_Dt date;
Set Nxt_sim_Dt = (Select Min(Misc_dt_1) from CITeam.SQL_Automation_Execution_Flags
                where Automation_Script = 'FORECAST_Tableau_Tables' and Automation_Variable = 'Cuscan_Fcast_Table_Status');

If today() < Nxt_sim_Dt then return End If;




SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;


-----------------------------------------------------------------------------------------------
----PART I: Create Variables and Set Forecast Parameters --------------------------------------
-----------------------------------------------------------------------------------------------
-- Select Date for forecast to start from
Drop variable if exists Forecast_Start_Wk; Create variable Forecast_Start_Wk integer; --1st Wk of Forecast
Set Forecast_Start_Wk = 201624;
-- (Select max(subs_week_and_year) from sky_calendar where calendar_date = (Select max(end_date + 7) from citeam.cust_fcast_weekly_base));
-- Select Forecast_Start_Wk;

-- Set end date of forecast
Drop variable if exists Forecast_End_Wk; Create variable Forecast_End_Wk integer; --Last Wk of Forecast
Set Forecast_End_Wk = 201752;

-- Set number of weeks forecast should run for
Drop variable if exists n_weeks_to_forecast; create variable n_weeks_to_forecast integer;
Drop table if exists #Sky_Calendar;
Select *
into #Sky_Calendar
from CITeam.Subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);

set n_weeks_to_forecast = (Select count(distinct subs_week_and_year) from #sky_calendar where Cast(subs_week_and_year as integer) between Forecast_Start_Wk and Forecast_End_Wk );

-- Sampling variables
Drop variable if exists sample_pct; create variable sample_pct decimal(7,4); set sample_pct=0.25;
Drop variable if exists true_sample_rate; create variable true_sample_rate float;

Drop variable if exists counter; create variable counter integer;

-- Dates for historic rates/trends
Drop variable if exists run_rate_weeks; create variable run_rate_weeks integer; set run_rate_weeks =13;
Drop variable if exists Y3W52; create variable Y3W52 integer;
Drop variable if exists Y3W40; create variable Y3W40 integer;
Drop variable if exists Y3W01; create variable Y3W01 integer;
Drop variable if exists Y2W01; create variable Y2W01 integer;
Drop variable if exists Y1W01; create variable Y1W01 integer;

--3 year window preceding the forecast, weeks are the week of action

Set Y1W01 = Case when ((Cast(Forecast_Start_Wk as float)/100) % 1)*100 = 53
                      then (Forecast_Start_Wk/100-2)*100 + 1
                 else Forecast_Start_Wk - 300
            end;
Set Y2W01 = Y1W01 + 100;
Set Y3W01 = Y1W01 + 200;
Set Y3W52 = Case when ((Cast(Forecast_Start_Wk as float)/10) % 1)*10 = 1
                      then (Forecast_Start_Wk/100 - 1)*100 +  52
                 else Forecast_Start_Wk - 1
            end;
Set Y3W40 = Case when ((Cast(Y3W52 as float)/100) % 1)*100  <= 12
                      then (Y3W52/100 - 1)*100 + (52-12) + ((Cast(Y3W52 as float)/100) % 1)*100
                 when Y3W52 = 53
                      then Y3W52 - 13
                 else Y3W52 - 12
            end
            ;

-- Select Forecast_Start_Wk,Y1W01,Y2W01,Y3W01,Y3W40,Y3W52;


-----------------------------------------------------------------------------------------------
----PART II: Calculate historic rates and trends for forecast
-----------------------------------------------------------------------------------------------

-- CusCan PL Rates
drop table if exists Cuscan_predicted_values;
Select * into Cuscan_predicted_values from CITeam.Forecast_CusCan_Rates(Forecast_Start_Wk);

-- SysCan PL Rates
drop table if exists Syscan_predicted_values;
Select * into Syscan_predicted_values from CITeam.Forecast_SysCan_Rates(Y1W01,Y3W52);

-- Part 2.3: TA CALLS - Volumes Saved / Non-Saved
Drop table if exists TA_Call_Dist;
Select * into TA_Call_Dist from CITeam.Forecast_TA_Vol_Dist(Forecast_Start_Wk,6);

-- WC TAs - Volumes Saved / Non-Saved
Drop table if exists WC_Dist;
Select * into WC_Dist from CITeam.Forecast_WC_Vol_Dist(Y2W01,Y3W52);

-- CONVERSION RATES TA/WC/Other -> PC
Drop table if exists TA_DTV_PC_Vol;
Select * into TA_DTV_PC_Vol from CITeam.Forecast_PC_Conversion_Rates(Y3W52);

-- Trend coefficients
drop table if exists Fcast_Regr_Coeffs;
Select * into Fcast_Regr_Coeffs from CITeam.Regression_Coefficient(Forecast_Start_Wk,2);
-- Select * from Fcast_Regr_Coeffs where Metric = 'TA_Call_Customers'

--Intraweek PC probablilities
Drop table if exists IntraWk_PC_Pct;
Select * into IntraWk_PC_Pct from CITeam.Intraweek_PCs_Dist(Forecast_Start_Wk);

--Intraweek PC probablilities
Drop table if exists IntraWk_AB_Pct;
Select * into IntraWk_AB_Pct from CITeam.Intraweek_ABs_Dist(Forecast_Start_Wk);

-- Intended PC Duration Distribution
Drop table if exists DTV_PC_Duration_Dist;
Select * into DTV_PC_Duration_Dist from CITeam.PC_Duration_Dist(Forecast_Start_Wk);

-- PC Reactivation, PC ABs
Drop table if exists PC_PL_Status_Change_Dist;
Select * into PC_PL_Status_Change_Dist from CITeam.PC_Status_Movement_Probabilities(Forecast_Start_Wk);

-- AB Reactivation
Drop table if exists AB_PL_Status_Change_Dist;
Select * into AB_PL_Status_Change_Dist from CITeam.AB_Status_Movement_Probabilities(Forecast_Start_Wk);


-- Offer Duration Distribution
Drop table if exists Offer_Applied_Dur_Dist;
Select * into Offer_Applied_Dur_Dist from CITeam.Offer_Applied_Duration_Dist(Forecast_Start_Wk,6)


----------------------------------------------------------------------------------------------------------------
----PART III: NEW CUSTOMERS ------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
-- Last 6 Wks activations to sample in simulation
Call CITeam.Build_Forecast_New_Cust_Sample(Forecast_Start_Wk);
-- Select top 100 * from CITeam.FORECAST_New_Cust_Sample

-- Weekly Activation Volumes
drop table if exists Activation_Vols;
Select * into Activation_Vols from CITeam.Forecast_Activation_Vols(Y2W01,Y3W52);


----------------------------------------------------------------------------------------------------------------
---PART IV - CREATE OPENING BASE FOR SIMULATION ----------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
Call CITeam.Forecast_Create_Opening_Base(Forecast_Start_Wk,sample_pct);

Drop table if exists Forecast_Loop_Table;
Select account_number,end_date,subs_week_and_year,subs_week_of_year,weekid
,DTV_Status_Code,DTV_PC_Future_Sub_Effective_Dt,DTV_AB_Future_Sub_Effective_Dt
,Cast(null as varchar(2)) as DTV_Status_Code_EoW
,BB_Segment,prem_segment,Simple_Segments,country,Affluence,package_desc
,offer_length_DTV,curr_offer_start_date_DTV,Curr_Offer_end_Date_Intended_DTV,Prev_offer_end_date_DTV,Time_To_Offer_End_DTV
,curr_offer_start_date_BB,Curr_Offer_end_Date_Intended_BB,Prev_offer_end_date_BB,Time_To_Offer_End_BB
,curr_offer_start_date_LR,Curr_Offer_end_Date_Intended_LR,Prev_offer_end_date_LR,Time_To_Offer_End_LR
,DTV_BB_LR_offer_end_dt,Time_To_Offer_End,DTV_Tenure,dtv_act_date
,Time_Since_Last_TA_call,Last_TA_Call_dt,Time_Since_Last_AB,Last_AB_Dt,Previous_AB_Count,Previous_Abs
,CusCan_Forecast_Segment,SysCan_Forecast_Segment,DTV_Activation_Type,dtv_latest_act_date,dtv_first_act_date,HD_segment

,Cast(null as float)  as rand_action_Cuscan
,Cast(null as float)  as rand_action_Syscan
,Cast(null as float)  as rand_TA_Vol
,Cast(null as float)  as rand_WC_Vol
,Cast(null as float)  as rand_TA_Save_Vol
,Cast(null as float)  as rand_WC_Save_Vol
,Cast(null as float)  as rand_TA_DTV_Offer_Applied
,Cast(null as float)  as rand_NonTA_DTV_Offer_Applied

,Cast(null as float)  as rand_TA_DTV_PC_Vol
,Cast(null as float)  as rand_WC_DTV_PC_Vol
,Cast(null as float)  as rand_Other_DTV_PC_Vol

,Cast(null as float)  as rand_Intrawk_DTV_PC
,Cast(null as float)  as rand_DTV_PC_Duration
,Cast(null as float)  as rand_DTV_PC_Status_Change
,Cast(null as float)  as rand_New_Off_Dur

,Cast(null as float)  as rand_Intrawk_DTV_AB

into Forecast_Loop_Table
from CITeam.FORECAST_Base_Sample
where sample = 'A';


----------------------------------------------------------------------------------------------------------------
-- PART V - RUN SIMULATION LOOP --------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
set temporary option query_temp_space_limit = 0;

Drop table if exists #Sky_Calendar;
Select * into #Sky_Calendar from CITeam.Subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);

commit;

set counter = 1;
Set True_Sample_Rate = (Select Cast(sum(Case when sample = 'A' then 1 else 0 end) as float)/count(*) from CITeam.FORECAST_Base_Sample);
-- Select True_Sample_Rate


Delete from FORECAST_Looped_Sim_Output_Platform;

-- Start Loop
While Counter <= n_weeks_to_forecast LOOP

-- Create Forecast Loop Table 2
Call CITeam.Forecast_Create_Forecast_Loop_Table_2(Forecast_Start_Wk,Forecast_End_Wk,True_Sample_Rate);

--
Call CITeam.Forecast_Loop_Table_2_Actions(Counter,1.02);

Call CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2(Forecast_Start_Wk,Forecast_End_Wk,True_Sample_Rate);

/* ---- Temporary code to remove churned customers ----*/
-- Call CITeam.Forecast_Temp_Loop_Table_2_Churn_Custs;
-- Insert table into Output table
Insert into FORECAST_Looped_Sim_Output_Platform  Select * from Forecast_Loop_Table_2;
-- drop table FORECAST_Looped_Sim_Output_Platform;        Select * into FORECAST_Looped_Sim_Output_Platform from Forecast_Loop_Table_2;


-- Remove churned customers
Delete from Forecast_Loop_Table_2 where CusCan= 1;
Delete Forecast_Loop_Table_2 where SysCan = 1;

set counter = counter+1;

-- Update Forecast_Loop_Table_2 fields for next week
Call CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk();

-- Create new forecast loop table for start of next week's loop
Call CITeam.Forecast_Create_New_Forecast_Loop_Table;


END Loop;

Call CITeam.INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE('LV 201624 V16',0.25,201624);



Select * into FORECAST_Looped_Sim_Output_Platform_201601_V16 from FORECAST_Looped_Sim_Output_Platform

/*
grant select on menziesm.FORECAST_Looped_Sim_Output_Platform to hga08

Select a.*,b.model_segment,b.new_segment1
into FORECAST_Looped_Sim_Output_Platform_REVISED_REDDY_Q3
from FORECAST_Looped_Sim_Output_Platform a
     left join
     simmonsr. REVISED_REDDY_q3_forecast_base b
     on a.account_number = b.account_number

select subs_week_and_year,count(*)*4 Customers,sum(TA_Call_Count)*4 TA_Events,sum(TA_DTV_PC)*4 TA_DTV_PC,sum(WC_DTV_PC)*4 WC_DTV_PC,sum(Other_DTV_PC)*4 Other_DTV_PC
from FORECAST_Looped_Sim_Output_Platform
where dtv_activation_type is null
group by subs_week_and_year

Select subs_week_and_year,sum(PC_Pending_Cancellations),sum(Same_Day_Cancels),Sum(Same_Day_PC_Reactivations)
from citeam.master_of_retention
where subs_year = 2016
group by subs_week_and_year

Select cuscan_forecast_segment,subs_week_and_year,pred_TA_Call_Cust_rate,pred_TA_Call_Cust_YoY_Trend,cum_TA_Call_Cust_rate
,count(*)*4 as Fcast_Customers
,sum(TA_Call_Cust)*4 Fcast_TA_Call_Cust
,cast(Fcast_TA_Call_Cust as float)/Fcast_Customers as TA_Call_Cust_Rate
from FORECAST_Looped_Sim_Output_Platform
where subs_week_and_year between 201601 and 201613
    and dtv_activation_type is null
group by cuscan_forecast_segment,subs_week_and_year,pred_TA_Call_Cust_rate,pred_TA_Call_Cust_YoY_Trend,cum_TA_Call_Cust_rate

grant select on FORECAST_Looped_Sim_Output_Platform to simmonsr
*/

GO
