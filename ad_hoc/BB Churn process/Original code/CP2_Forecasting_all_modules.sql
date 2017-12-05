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
-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
Create variable var_End_date date;
Create variable _1st_Wk1 integer;
Create variable _Lst_Wk integer;
Create variable Forecast_Start_Wk integer;

Set Forecast_Start_Wk = 201553

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_CusCan_Rates;

Create procedure CITeam.Forecast_CusCan_Rates(In Forecast_Start_Wk integer) --(In Y1W01 integer,In Y3W52 integer)
Result(Subs_Week smallint,Cuscan_forecast_segment varchar(50)
,pred_TA_Call_cnt_rate float
,pred_TA_Call_Cust_rate float
,pred_TA_Not_Saved_rate float
,pred_TA_Saved_rate float
,pred_Web_Chat_TA_Cnt_rate float
,pred_Web_Chat_TA_Cust_rate float
,pred_Web_Chat_TA_Not_Saved_rate float
,pred_Web_Chat_TA_Saved_rate float
,pred_NonTA_DTV_Offer_Applied_rate float
,pred_TA_DTV_Offer_Applied_rate float

,pred_TA_DTV_PC_rate float
,pred_WC_DTV_PC_rate float
,pred_Other_PC_rate float
)
BEGIN

Declare var_End_date date;
Declare _1st_Wk1 integer;
Declare _Lst_Wk integer;

Select * into #sky_calendar from citeam.subs_calendar(Forecast_Start_Wk/100 - 3,Forecast_Start_Wk/100);

Set var_End_date = (Select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk);
Set _Lst_Wk = (Select max(subs_week_and_year) from #sky_calendar where calendar_date = var_End_date);
Set _1st_Wk1 = Case when Forecast_Start_Wk % 100 < 52 then (Forecast_Start_Wk/100 - 3)*100 + Forecast_Start_Wk % 100
                    else (Forecast_Start_Wk/100 - 2)*100 + 1
               end;

-- Select _1st_Wk1,_Lst_Wk;

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

--select last 3 years, flag last year and year prior to that
drop table if exists #cuscan_weekly_agg;
select
  subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment
,Count(*) as n
,Cast(sum(TA_Event_Count) as float) as TA_Call_cnt
,Cast(sum(Unique_TA_Caller) as float) as TA_Call_Customers
,Cast(sum(TA_Non_Save_Count) as float) as TA_Not_Saved
,Cast(sum(TA_Save_Count) as float) as TA_Saved
,Cast(sum(Web_Chat_TA_Cnt) as float) as Web_Chat_TA_Cnt
,Cast(sum(Web_Chat_TA_Customers) as float) as Web_Chat_TA_Customers
,Cast(sum(Web_Chat_TA_Not_Saved) as float) as Web_Chat_TA_Not_Saved
,Cast(sum(Web_Chat_TA_Saved) as float) as Web_Chat_TA_Saved
,Cast(sum(Offer_Applied_DTV) as float) as DTV_Offer_Applied
,Cast(sum(TA_DTV_Offer_Applied) as float) as TA_DTV_Offer_Applied
,Cast(DTV_Offer_Applied - TA_DTV_Offer_Applied        as float) as NonTA_DTV_Offer_Applied

,Cast(sum(TA_DTV_PC)                as float) as TA_DTV_PC
,Cast(sum(WC_DTV_PC)                as float) as WC_DTV_PC
,Cast(sum(Accessibility_DTV_PC) + sum(Min_Term_PC) + sum(Other_PC) as float) as Other_PC

,dense_rank() over(order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null
 end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,(week_id/13)+1 as quarter_id
,Max(Subs_Week) over(partition by Subs_Year) Max_Subs_Week
into #cuscan_weekly_agg
from CITeam.DTV_Fcast_Weekly_Base agg
where subs_week_and_year between _1st_Wk1 and _Lst_Wk
--         and subs_week != 53
    and Downgrade_View = 'Actuals'
group by subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment
;

Update #cuscan_weekly_agg Set subs_week = subs_week - 1 where Max_Subs_Week = 53;
Update #cuscan_weekly_agg Set Subs_Week_And_Year = Subs_Year*100 + subs_week;
Delete from #cuscan_weekly_agg where subs_week = 0;


--for each customer segment and week, action counts for current and previous year
drop table if exists #cuscan_forecast_summary_1;
select
   subs_week
  ,cuscan_forecast_segment

  ,sum(case when Week_Position = 'Prev' then n                   else 0 end)    as prev_n
  ,sum(case when Week_Position = 'Prev' then TA_Call_cnt         else 0 end)    as prev_TA_Call_cnt
  ,sum(case when Week_Position = 'Prev' then TA_Call_Customers   else 0 end)    as prev_TA_Call_Cust
  ,sum(case when Week_Position = 'Prev' then TA_Not_Saved        else 0 end)    as prev_TA_Not_Saved
  ,sum(case when Week_Position = 'Prev' then TA_Saved            else 0 end)    as prev_TA_Saved
  ,sum(case when Week_Position = 'Prev' then Web_Chat_TA_Cnt     else 0 end)    as prev_Web_Chat_TA_Cnt
  ,sum(case when Week_Position = 'Prev' then Web_Chat_TA_Customers  else 0 end)    as prev_Web_Chat_TA_Cust
  ,sum(case when Week_Position = 'Prev' then Web_Chat_TA_Not_Saved  else 0 end)    as prev_Web_Chat_TA_Not_Saved
  ,sum(case when Week_Position = 'Prev' then Web_Chat_TA_Saved   else 0 end)    as prev_Web_Chat_TA_Saved
  ,sum(case when Week_Position = 'Prev' then agg.DTV_Offer_Applied - agg.TA_DTV_Offer_Applied   else 0 end)    as prev_NonTA_DTV_Offer_Applied
  ,sum(case when Week_Position = 'Prev' then agg.TA_DTV_Offer_Applied   else 0 end)    as prev_TA_DTV_Offer_Applied

  ,sum(case when Week_Position = 'Prev' then TA_DTV_PC           else 0 end)    as prev_TA_DTV_PC
  ,sum(case when Week_Position = 'Prev' then WC_DTV_PC           else 0 end)    as prev_WC_DTV_PC
  ,sum(case when Week_Position = 'Prev' then Other_PC            else 0 end)    as prev_Other_PC


  ,sum(case when Week_Position = 'Curr' then n                     else 0 end)    as curr_n
  ,sum(case when Week_Position = 'Curr' then TA_Call_cnt           else 0 end)    as curr_TA_Call_cnt
  ,sum(case when Week_Position = 'Curr' then TA_Call_Customers     else 0 end)    as curr_TA_Call_Cust
  ,sum(case when Week_Position = 'Curr' then TA_Not_Saved          else 0 end)    as curr_TA_Not_Saved
  ,sum(case when Week_Position = 'Curr' then TA_Saved              else 0 end)    as curr_TA_Saved
  ,sum(case when Week_Position = 'Curr' then Web_Chat_TA_Cnt       else 0 end)    as curr_Web_Chat_TA_Cnt
  ,sum(case when Week_Position = 'Curr' then Web_Chat_TA_Customers else 0 end)    as curr_Web_Chat_TA_Cust
  ,sum(case when Week_Position = 'Curr' then Web_Chat_TA_Not_Saved else 0 end)    as curr_Web_Chat_TA_Not_Saved
  ,sum(case when Week_Position = 'Curr' then Web_Chat_TA_Saved   else 0 end)    as curr_Web_Chat_TA_Saved
  ,sum(case when Week_Position = 'Curr' then agg.DTV_Offer_Applied - agg.TA_DTV_Offer_Applied   else 0 end)    as curr_NonTA_DTV_Offer_Applied
  ,sum(case when Week_Position = 'Curr' then TA_DTV_Offer_Applied   else 0 end)    as curr_TA_DTV_Offer_Applied

  ,sum(case when Week_Position = 'Curr' then TA_DTV_PC             else 0 end)    as curr_TA_DTV_PC
  ,sum(case when Week_Position = 'Curr' then WC_DTV_PC             else 0 end)    as curr_WC_DTV_PC
  ,sum(case when Week_Position = 'Curr' then Other_PC              else 0 end)    as curr_Other_PC
  ,sum(0)                                                                       as LQ_n
  ,sum(0)                                                                       as LQ_DTV_Offer

into #cuscan_forecast_summary_1
from #cuscan_weekly_agg agg
group by
subs_week
,cuscan_forecast_segment
;


--for each customer segment (but not week), no action new offer counts for last quarter
drop table if exists #cuscan_forecast_summary_LQ;
select
cuscan_forecast_segment
,sum(n)       as LQ_n
,sum(dtv_offer_applied)    as LQ_DTV_Offer
into #cuscan_forecast_summary_LQ
from #cuscan_weekly_agg
where last_quarter='Y'
group by cuscan_forecast_segment;


--add LQ volumes onto previous summary table
update #cuscan_forecast_summary_1
set a.LQ_n = b.LQ_n
   ,a.LQ_DTV_Offer = b.LQ_DTV_Offer
from #cuscan_forecast_summary_1 as a
     left join
     #cuscan_forecast_summary_LQ as b
     on a.cuscan_forecast_segment = b.cuscan_forecast_segment
;




--create rates from action counts and cell size

drop table if exists #cuscan_forecast_summary_2;
select
   *
-- Curr Years Weekly Rates
  ,case when curr_n !=0 then (cast(curr_TA_Call_cnt          as float)/ cast(curr_n as float)) else 0 end  as curr_TA_Call_cnt_rate
  ,case when curr_n !=0 then (cast(curr_TA_Call_Cust         as float)/ cast(curr_n as float)) else 0 end  as curr_TA_Call_Cust_rate
  ,case when curr_n !=0 then (cast(curr_TA_Not_Saved         as float)/ cast(curr_n as float)) else 0 end  as curr_TA_Not_Saved_rate
  ,case when curr_n !=0 then (cast(curr_TA_Saved             as float)/ cast(curr_n as float)) else 0 end as curr_TA_Saved_rate
  ,case when curr_n !=0 then (cast(curr_Web_Chat_TA_Cnt      as float)/ cast(curr_n as float)) else 0 end as curr_Web_Chat_TA_Cnt_rate
  ,case when curr_n !=0 then (cast(curr_Web_Chat_TA_Cust     as float)/ cast(curr_n as float)) else 0 end as curr_Web_Chat_TA_Cust_rate
  ,case when curr_n !=0 then (cast(curr_Web_Chat_TA_Not_Saved  as float)/ cast(curr_n as float)) else 0 end as curr_Web_Chat_TA_Not_Saved_rate
  ,case when curr_n !=0 then (cast(curr_Web_Chat_TA_Saved    as float)/ cast(curr_n as float)) else 0 end as curr_Web_Chat_TA_Saved_rate
  ,case when curr_n !=0 then (cast(curr_NonTA_DTV_Offer_Applied    as float)/ cast(curr_n as float)) else 0 end as curr_NonTA_DTV_Offer_Applied_rate
  ,case when curr_TA_Call_Cust !=0 then (cast(curr_TA_DTV_Offer_Applied    as float)/ cast(curr_TA_Call_Cust as float)) else 0 end as curr_TA_DTV_Offer_Applied_rate

  ,case when curr_TA_Call_Cust !=0 then (cast(curr_TA_DTV_PC            as float)/ cast(curr_TA_Call_Cust     as float)) else 0 end as curr_TA_DTV_PC_rate
  ,case when curr_Web_Chat_TA_Cust !=0 then (cast(curr_WC_DTV_PC        as float)/ cast(curr_Web_Chat_TA_Cust as float)) else 0 end as curr_WC_DTV_PC_rate
  ,case when curr_n !=0 then (cast(curr_Other_PC             as float)/ (cast(curr_n as float) - cast(curr_TA_Call_Cust as float) -  cast(curr_Web_Chat_TA_Cust as float))) else 0 end as curr_Other_PC_rate

-- Prev Years Weekly Rates
  ,case when prev_n !=0 then (cast(prev_TA_Call_cnt          as float)/ cast(prev_n as float)) else 0 end as prev_TA_Call_cnt_rate
  ,case when prev_n !=0 then (cast(prev_TA_Call_Cust         as float)/ cast(prev_n as float)) else 0 end as prev_TA_Call_Cust_rate
  ,case when prev_n !=0 then (cast(prev_TA_Not_Saved         as float)/ cast(prev_n as float)) else 0 end as prev_TA_Not_Saved_rate
  ,case when prev_n !=0 then (cast(prev_TA_Saved             as float)/ cast(prev_n as float)) else 0 end as prev_TA_Saved_rate
  ,case when prev_n !=0 then (cast(prev_Web_Chat_TA_Cnt      as float)/ cast(prev_n as float)) else 0 end as prev_Web_Chat_TA_Cnt_rate
  ,case when prev_n !=0 then (cast(prev_Web_Chat_TA_Cust as float)/ cast(prev_n as float)) else 0 end as prev_Web_Chat_TA_Cust_rate
  ,case when prev_n !=0 then (cast(prev_Web_Chat_TA_Not_Saved as float)/ cast(prev_n as float)) else 0 end as prev_Web_Chat_TA_Not_Saved_rate
  ,case when prev_n !=0 then (cast(prev_Web_Chat_TA_Saved   as float)/ cast(prev_n as float)) else 0 end as prev_Web_Chat_TA_Saved_rate
  ,case when prev_n !=0 then (cast(prev_NonTA_DTV_Offer_Applied as float)/ cast(prev_n as float)) else 0 end as prev_NonTA_DTV_Offer_Applied_rate
  ,case when prev_TA_Call_Cust !=0 then (cast(prev_TA_DTV_Offer_Applied   as float)/ cast(prev_TA_Call_Cust as float)) else 0 end as prev_TA_DTV_Offer_Applied_rate

  ,case when prev_TA_Call_Cust !=0 then (cast(prev_TA_DTV_PC            as float)/ cast(prev_TA_Call_Cust     as float)) else 0 end as prev_TA_DTV_PC_rate
  ,case when prev_Web_Chat_TA_Cust !=0 then (cast(prev_WC_DTV_PC        as float)/ cast(prev_Web_Chat_TA_Cust as float)) else 0 end as prev_WC_DTV_PC_rate
  ,case when prev_n !=0 then (cast(prev_Other_PC             as float)/ (cast(prev_n as float) - cast(prev_TA_Call_Cust as float) - cast(prev_Web_Chat_TA_Cust as float))) else 0 end as prev_Other_PC_rate


-- Last Quarters Weekly Rates
  ,cast(LQ_DTV_Offer   as float)/ cast(LQ_n as float)  as LQ_DTV_Offer_rate

  ,cast(curr_n as float)/ (cast(curr_n as float) + cast(prev_n as float)) as curr_share
  ,1 - curr_share as prev_share
into #cuscan_forecast_summary_2
from #cuscan_forecast_summary_1
;



drop table if exists #cuscan_forecast_summary_3;
select
*
,/*(curr_share **/curr_TA_Call_cnt_rate         /*) + (prev_share *prev_TA_Call_cnt_rate     )*/ as pred_TA_Call_cnt_rate
,/*(curr_share **/curr_TA_Call_Cust_rate        /*) + (prev_share *prev_TA_Call_Cust_rate    )*/ as pred_TA_Call_Cust_rate
,(curr_share *curr_TA_Not_Saved_rate        ) + (prev_share *prev_TA_Not_Saved_rate    ) as pred_TA_Not_Saved_rate -- not used
,(curr_share *curr_TA_Saved_rate            ) + (prev_share *prev_TA_Saved_rate        ) as pred_TA_Saved_rate -- not used
,/*(curr_share **/curr_Web_Chat_TA_Cnt_rate     /*) + (prev_share *prev_Web_Chat_TA_Cnt_rate )*/ as pred_Web_Chat_TA_Cnt_rate
,/*(curr_share **/curr_Web_Chat_TA_Cust_rate    /*) + (prev_share *prev_Web_Chat_TA_Cust_rate)*/ as pred_Web_Chat_TA_Cust_rate
,/*(curr_share **/curr_Web_Chat_TA_Not_Saved_rate /*) + (prev_share *prev_Web_Chat_TA_Not_Saved_rate )*/ as pred_Web_Chat_TA_Not_Saved_rate
,/*(curr_share **/curr_Web_Chat_TA_Saved_rate   /*) + (prev_share *prev_Web_Chat_TA_Saved_rate  )*/ as pred_Web_Chat_TA_Saved_rate
,/*(curr_share **/curr_NonTA_DTV_Offer_Applied_rate   /*) + (prev_share *prev_NonTA_DTV_Offer_Applied_rate  )*/ as pred_NonTA_DTV_Offer_Applied_rate
,/*(curr_share **/curr_TA_DTV_Offer_Applied_rate  /*) + (prev_share *prev_TA_DTV_Offer_Applied_rate  )*/ as pred_TA_DTV_Offer_Applied_rate

,/*(curr_share **/curr_TA_DTV_PC_rate   /*)        + (prev_share *prev_TA_DTV_PC_rate  ) */             as pred_TA_DTV_PC_rate
,/*(curr_share **/curr_WC_DTV_PC_rate   /*)        + (prev_share *prev_WC_DTV_PC_rate  ) */             as pred_WC_DTV_PC_rate
,/*(curr_share **/curr_Other_PC_rate    /*)        + (prev_share *prev_Other_PC_rate   ) */             as pred_Other_PC_rate
into #cuscan_forecast_summary_3
from #cuscan_forecast_summary_2;


--final output for use in forecasting
select subs_week,cuscan_forecast_segment
,pred_TA_Call_cnt_rate
,pred_TA_Call_Cust_rate
,pred_TA_Not_Saved_rate
,pred_TA_Saved_rate
,pred_Web_Chat_TA_Cnt_rate
,pred_Web_Chat_TA_Cust_rate
,pred_Web_Chat_TA_Not_Saved_rate
,pred_Web_Chat_TA_Saved_rate
,pred_NonTA_DTV_Offer_Applied_rate
,pred_TA_DTV_Offer_Applied_rate
,pred_TA_DTV_PC_rate
,pred_WC_DTV_PC_rate
,pred_Other_PC_rate
from #cuscan_forecast_summary_3 as a
;

END;




-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_CusCan_Rates to CITeam;

-- Change back to your account
Setuser;

-- Test it
Select top 10000 * from CITeam.Forecast_CusCan_Rates(201301,201552);


-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
Create variable Y1W01 integer;
Create variable Y3W52 integer;

Set Y1W01 = 201301;
Set Y3W52 = 201552;

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_SysCan_Rates;

Create procedure CITeam.Forecast_SysCan_Rates(In Y1W01 integer,In Y3W52 integer)
Result(Subs_Week smallint,Syscan_forecast_segment varchar(50),pred_dtv_ab_rate float)
BEGIN

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

drop table if exists #syscan_weekly_agg;
select
  subs_year
,subs_week
,subs_week_and_year
,syscan_forecast_segment

-- ,offer_length_dtv
-- ,dtv_offer_applied
-- ,new_customer
-- ,activations_acquisitions
-- ,activations_reinstates

,Count(*) as n
,cast(sum(dtv_ab)                   as float) as dtv_ab

,dense_rank() over(order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,(week_id/13)+1 as quarter_id
into #syscan_weekly_agg
from CITeam.DTV_FCast_Weekly_Base
where subs_week_and_year between Y1W01 and Y3W52
            and subs_week !=53
            and Downgrade_View = 'Actuals'
group by subs_year
,subs_week
,subs_week_and_year
,syscan_forecast_segment;



-- select * from syscan_align;

--for each customer segment and week, action counts for current and previous year
 drop table if exists #syscan_forecast_summary_1;
select
   subs_week
  ,syscan_forecast_segment

  ,sum(case when Week_Position = 'Prev' then n        else 0 end)    as prev_n
  ,sum(case when Week_Position = 'Prev' then dtv_ab   else 0 end)    as prev_dtv_ab

  ,sum(case when Week_Position = 'Curr' then n        else 0 end)    as curr_n
  ,sum(case when Week_Position = 'Curr' then dtv_ab   else 0 end)    as curr_dtv_ab


--   ,sum(case when Week_Position = 'Prev' and        new_customer=1 then n else 0 end) as prev_new_customer
--   ,sum(case when Week_Position = 'Curr' and        new_customer=1 then n else 0 end) as curr_new_customer
  -- can we make sure the below condition is correct
--   ,sum(case when Week_Position = 'Prev' and activations_reinstates>=1 then n else 0 end) as prev_reinstated_customer
--   ,sum(case when Week_Position = 'Curr' and activations_reinstates>=1 then n else 0 end) as curr_reinstated_customer
--
  ,sum(0)   as LQ_n
  ,sum(0)   as LQ_DTV_Offer

into #syscan_forecast_summary_1
from #syscan_weekly_agg
group by
subs_week
,syscan_forecast_segment
;


--for each customer segment (but not week), no action new offer counts for last quarter
drop table if exists #syscan_forecast_summary_LQ;
select
syscan_forecast_segment
,sum(n)       as LQ_n
-- ,sum(dtv_offer_applied)    as LQ_DTV_Offer
into #syscan_forecast_summary_LQ
from #syscan_weekly_agg
where last_quarter='Y'
group by syscan_forecast_segment;



--create rates from action counts and cell size

drop table if exists #syscan_forecast_summary_2;
select
   *
-- Curr Years Weekly Rates
  ,case when curr_n !=0 then (cast(curr_dtv_ab    as float)/ cast(curr_n as float)) else 0 end as curr_dtv_ab_rate

-- Prev Years Weekly Rates
  ,case when prev_n !=0 then (cast(prev_dtv_ab    as float)/ cast(prev_n as float)) else 0 end as prev_dtv_ab_rate

-- Last Quarters Weekly Rates
--   ,case when LQ_n!=0 then cast(LQ_DTV_Offer   as float)/ cast(LQ_n as float) else 0 end as LQ_DTV_Offer_rate

  ,cast(curr_n as float)/ (cast(curr_n as float) + cast(prev_n as float)) as curr_share
  ,1 - curr_share as prev_share
into #syscan_forecast_summary_2
from #syscan_forecast_summary_1
;

-- pred rates (except for No Action New Offer which uses the last quarters average)

drop table if exists #syscan_forecast_summary_3;
select
*
,/*(curr_share **/ curr_dtv_ab_rate /*) + (prev_share * prev_dtv_ab_rate)*/ as pred_dtv_ab_rate
into #syscan_forecast_summary_3
from #syscan_forecast_summary_2;


--final output for use in forecasting
select Subs_Week,Syscan_forecast_segment,pred_dtv_ab_rate
from #syscan_forecast_summary_3 as a
;


END;




-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_SysCan_Rates to CITeam;

-- Change back to your account
Setuser;

-- Test it
Select top 10000 * from CITeam.Forecast_SysCan_Rates(201301,201552);


Create variable Forecast_Start_Wk integer; Set Forecast_Start_Wk = 201601;
Create variable Num_Wks integer; Set Num_Wks = 6;

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_TA_Vol_Dist;

Create procedure CITeam.Forecast_TA_Vol_Dist(In Forecast_Start_Wk integer,In Num_Wks integer)
Result(CusCan_Forecast_Segment varchar(50),Total_Calls smallint,TA_Saved smallint,TA_DTV_Offer_Applied tinyint, TA_Customers Integer,
TA_Lower_Pctl float,TA_Upper_Pctl float)
-- Result(Subs_Week integer,Total_Calls Integer,TA_Saved Integer,TA_Customers Integer,
-- TA_Vol_Percentile float,Prev_TA_Vol_Percentile float,Cum_TA_Saves integer,Total_TA_Saves Integer,
-- TA_Save_Vol_Percentile float,Prev_TA_Save_Vol_Percentile float
-- )
BEGIN

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Select * into #Sky_Calendar from CITeam.Subs_Calendar(Forecast_Start_Wk/100 - 1,Forecast_Start_Wk/100);

Select *
,event_dt - datepart(weekday,event_dt+2) as event_end_dt
,Cast(0 as tinyint) as TA_DTV_Offer_Applied
,Cast(null as varchar(50)) as CusCan_Forecast_Segment
into #crr
from citeam.combined_retention_report crr
where event_dt between (select max(calendar_date - 7 - 6*7 + 1) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk) -- Last 6 Wk PC conversions
                        and (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
        and TA_Channel = 'Voice'
        and Turnaround_Saved + Turnaround_Not_Saved > 0;

Update  #crr
Set TA_DTV_Offer_Applied = 1
from #crr
     inner join
     CITeam.offer_usage_all oua
     on oua.account_number = #crr.account_number
        and oua.offer_start_dt_Actual = #crr.event_dt
        and oua.offer_start_dt_Actual = oua.Whole_offer_start_dt_Actual
        and oua.offer_end_dt_Actual > oua.offer_start_dt_Actual
        and oua.subs_type = 'DTV Primary Viewing'
        and lower(oua.offer_dim_description) not like '%price protection%';

Update #crr crr
Set CusCan_ForeCast_Segment = base.CusCan_ForeCast_Segment
from #crr crr
     inner join
     CITeam.DTV_Fcast_Weekly_Base base
     on crr.account_number = base.account_number
        and crr.event_end_dt = base.end_date;

select Cast(event_dt - datepart(weekday,event_dt+2) as date) as end_date
,CusCan_ForeCast_Segment
,account_number
,sum(Turnaround_Saved + Turnaround_Not_Saved)         as TA_Event_Count
,Sum(Turnaround_Saved)           as TA_Saved_Count
,max(TA_DTV_Offer_Applied) as TA_DTV_Offer_Applied
into #Acc_TA_Call_Vol
from #crr
where CusCan_ForeCast_Segment is not null
group by end_date,account_number,CusCan_ForeCast_Segment
;


Select Row_Number() over(partition by CusCan_ForeCast_Segment order by TA_Custs desc) TA_Dist_Rnk,
       CusCan_ForeCast_Segment,TA_Event_Count,TA_Saved_Count,TA_DTV_Offer_Applied,count(*) TA_Custs
into #TA_Call_Vol
from #Acc_TA_Call_Vol
group by CusCan_ForeCast_Segment,TA_Event_Count,TA_Saved_Count,TA_DTV_Offer_Applied;

Select CusCan_ForeCast_Segment,TA_Dist_Rnk,TA_Event_Count,TA_Saved_Count,TA_DTV_Offer_Applied,TA_Custs,
       Sum(TA_Custs) over(Partition by CusCan_ForeCast_Segment) Total_TA_Custs,
       Sum(TA_Custs) over(Partition by CusCan_ForeCast_Segment order by TA_Dist_Rnk) Cum_TA_Custs,
       Cast(Cum_TA_Custs as float)/Total_TA_Custs as TA_Dist_Upper_Pctl
into #TA_Call_Vol_2
from #TA_Call_Vol;

Select TA1.CusCan_ForeCast_Segment,TA1.TA_Event_Count,TA1.TA_Saved_Count,TA1.TA_DTV_Offer_Applied,TA1.TA_Custs,
        Coalesce(TA2.TA_Dist_Upper_Pctl,0) as TA_Dist_Lower_Pctl,
        TA1.TA_Dist_Upper_Pctl
from #TA_Call_Vol_2 TA1
     left join
     #TA_Call_Vol_2 TA2
     on TA2.CusCan_ForeCast_Segment = TA1.CusCan_ForeCast_Segment
        and TA2.TA_Dist_Rnk = TA1.TA_Dist_Rnk - 1;

END;



-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_TA_Vol_Dist to CITeam;

-- Change back to your account
Setuser;

-- Test it
Select * from CITeam.Forecast_TA_Vol_Dist(201601,6);


/*
Create variable Y2W01 integer;
Create variable Y3W52 integer;

Set Y2W01 = 201401;
Set Y3W52 = 201552;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
 Drop procedure if exists Forecast_WC_Vol_Dist;

Create procedure Forecast_WC_Vol_Dist(In Y2W01 integer,In Y3W52 integer)
Result(Subs_Week integer,Total_WCs Integer,WebChat_TA_Saved Integer,WC_Customers Integer,
WC_Vol_Percentile float,Prev_WC_Vol_Percentile float,Cum_WC_Saves integer,Total_WC_Saves Integer,
WC_Save_Vol_Percentile float,Prev_WC_Save_Vol_Percentile float
)
BEGIN

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Select *
into #Sky_Calendar
from Subs_Calendar(Y2W01/100 -1 ,Y3W52/100 + 1);

Drop table if exists #WC_Call_Vol;
select /*cuscan_forecast_segment,*/ end_date
,Cast(null as integer) subs_week
,Webchat_ta_Saved + Webchat_ta_not_saved as total_WCs
,Webchat_TA_Saved
,count(*) as Total_Customers
into #WC_Call_Vol
from citeam.cust_fcast_weekly_base
where end_date between (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Y2W01)
        and (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Y3W52)
        and Webchat_ta_Saved + Webchat_ta_not_saved > 0
group by /*cuscan_forecast_segment,*/ end_date
,total_WCs
,Webchat_TA_Saved;


Drop table if exists WC_Dist;
Select subs_week_of_year as subs_week /*cuscan_forecast_segment,*/
        ,total_WCs
        ,Webchat_TA_Saved
        ,sum(WCs.Total_Customers) WC_Customers
        ,Cast(null as float) as WC_Vol_Percentile
        ,Cast(null as float) as Prev_WC_Vol_Percentile

        ,sum(WC_Customers) over(partition by subs_week,total_WCs order by Webchat_TA_Saved) as Cum_WC_Saves
        ,sum(WC_Customers) over(partition by subs_week,total_WCs)                           as Total_WC_Saves
        ,Cast(Cum_WC_Saves as float)/Total_WC_Saves                                         as WC_Save_Vol_Percentile
        ,Cast(null as float)                                                                as Prev_WC_Save_Vol_Percentile
into #WC_Dist
from #WC_Call_Vol as WCs
     inner join
     #sky_calendar sc
     on sc.calendar_date = WCs.end_date+7
where subs_week_of_year != 53
--         and total_calls <= 4
group by
subs_week
,total_wcs /*cuscan_forecast_segment,*/
,Webchat_TA_Saved
;



Update #WC_Dist a
Set Prev_WC_Save_Vol_Percentile = Coalesce(b.WC_Save_Vol_Percentile,0)
from #WC_Dist as a
     left join
     #WC_Dist as b
     on a.subs_week = b.subs_week
        and a.total_wcs = b.total_wcs
        and a.Webchat_TA_Saved -1 = b.Webchat_TA_Saved;



Drop table if exists #WC_Vol_Dist;
Select subs_week
        ,total_WCs
       ,sum(WC_Customers) as WC_Customers
       ,Sum(WC_Customers) over(partition by subs_week order by total_WCs) as Cum_WC_Customers
       ,Sum(WC_Customers) over(partition by subs_week)                    as Total_WC_Customers
       ,Cast(Cum_WC_Customers as float)/Total_WC_Customers                as Percentile
into #WC_Vol_Dist
from #WC_Dist
group by subs_week
,total_WCs;

-- upper bound
Update #WC_Dist cd
Set WC_Vol_Percentile = Percentile
from #WC_Dist as cd
     left join
     #WC_Vol_Dist as  vd
     on vd.subs_week = cd.subs_week
        and vd.total_WCs = cd.total_WCs;


-- lower  bound
Update #WC_Dist cd
Set Prev_WC_Vol_Percentile = Coalesce(vd.Percentile,0)
from #WC_Dist cd
     left join
     #WC_Vol_Dist vd
     on vd.subs_week = cd.subs_week
        and vd.total_WCs = cd.total_WCs - 1;

Select * from #WC_Dist;

END;




-- Grant execute rights to the members of CITeam
grant execute on Forecast_WC_Vol_Dist to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select top 10000 * from CITeam.Forecast_WC_Vol_Dist(201401,201552);
*/


/*Create variable Y3W52 integer;

Set Y3W52 = 201624;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
Drop procedure if exists Forecast_PC_Conversion_Rates;

Create procedure Forecast_PC_Conversion_Rates(In Y3W52 integer)
Result(CusCan_Forecast_Segment varchar(50)
,Total_TA_DTV_PC integer,Total_TA_SkyPlus_Save integer
,Total_WC_DTV_PC integer,Total_WC_SkyPlus_Save integer
,Total_Other_PC integer
,Total_TA_Cust integer,Total_WC_Cust integer,Total_Other_Cust integer
,TA_DTV_PC_Conv_rate float,TA_SkyPlus_Save_rate float
,WC_DTV_PC_Conv_rate float,WC_SkyPlus_Save_rate float
,Other_DTV_PC_Conv_rate float
)
BEGIN

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Select *
into #Sky_Calendar
from Subs_Calendar(Y3W52/100 -1 ,Y3W52/100);

Drop table if exists #TA_DTV_PC_Vol;
select Cuscan_forecast_segment
,sum(TA_DTV_PC)                                    as Total_TA_DTV_PC
,sum(TA_Sky_Plus_Save)                             as Total_TA_SkyPlus_Save
,sum(WC_DTV_PC)                                    as Total_WC_DTV_PC
,sum(WC_Sky_Plus_Save)                             as Total_WC_SkyPlus_Save
,sum(Other_PC )                                    as Total_Other_PC

,sum(Unique_TA_Caller)                            as Total_TA_Cust
,sum(Web_Chat_TA_Customers)                        as Total_WC_Cust
,Count(*) - Total_TA_Cust - Total_WC_Cust            as Total_Other_Cust

,case when Total_TA_Cust!=0    then cast(Total_TA_DTV_PC as float) / Total_TA_Cust    else 0 end  as TA_DTV_PC_Conv_Rate
,case when Total_TA_Cust!=0    then cast(Total_TA_SkyPlus_Save as float) / Total_TA_Cust    else 0 end  as TA_SkyPlus_Save_Rate

,case when Total_WC_Cust!=0    then cast(Total_WC_DTV_PC as float) / Total_WC_Cust    else 0 end  as WC_DTV_PC_Conv_Rate
,case when Total_WC_Cust!=0    then cast(Total_WC_SkyPlus_Save as float) / Total_WC_Cust    else 0 end  as WC_SkyPlus_Save_Rate

,case when Total_Other_Cust!=0 then cast(Total_Other_PC  as float) / Total_Other_Cust else 0 end  as Other_DTV_PC_Conv_Rate

into #TA_DTV_PC_Vol
from DTV_Fcast_Weekly_Base -- Replace with DTV_FCast_Weekly_Base
where  end_date between (select max(calendar_date - 7 - 6*7) from #sky_calendar where subs_week_and_year = Y3W52) -- Last 6 Wk PC conversions
        and (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Y3W52)
        and Downgrade_View = 'Actuals'
group by
cuscan_forecast_segment
;

Select * from #TA_DTV_PC_Vol;

END;




-- Grant execute rights to the members of CITeam
grant execute on Forecast_PC_Conversion_Rates to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select top 10000 * from CITeam.Forecast_PC_Conversion_Rates(201552);

Select * into TA_DTV_PC_Vol from CITeam.Forecast_PC_Conversion_Rates(Y3W52);
Select sum(Total_TA_DTV_PC) ,sum(Total_TA_Cust),Cast(sum(Total_TA_DTV_PC) as float)/sum(Total_TA_Cust)  from CITeam.Forecast_PC_Conversion_Rates(201624);
*/-- Drop procedure if exists CITeam.Regression_Coefficient;
/*
Setuser CITeam;

Create variable LV integer; Set LV = 201614;
Create variable Regression_Yrs smallint; Set Regression_Yrs = 2;

Create variable  Y3W52 integer;
Create variable  Y1W01 integer;
*/
Drop procedure if exists Regression_Coefficient;

CREATE PROCEDURE Regression_Coefficient(IN LV integer,IN Regression_Yrs tinyint)
RESULT(LV integer,Metric varchar(30),Fcast_Segment varchar(100),Grad_Coeff float,Intercept_Coeff float)
BEGIN

Declare Dynamic_SQL varchar(1000);
Declare Y3W52 integer;
Declare Y1W01 integer;

-- Create variable Dynamic_SQL varchar(1000);

-- Create variable LV integer; Set LV = 201601;

-----------------------------------------------------------------------------------------------------------
-- Create aggregates table from which to calc trends ------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
Select * into #Sky_Calendar from CITeam.Subs_Calendar(LV/100-Regression_Yrs-1,LV/100);

Drop table if exists #Regr_Wks;
Select Cast(sc.subs_week_and_year as integer) Subs_week_and_year
       ,Row_Number() over(order by Subs_week_and_year desc) Wk_Rnk
into #Regr_Wks
from #sky_calendar sc
where Cast(sc.subs_Week_and_year as integer) < LV
    and Subs_Week_of_year != 53
group by Subs_week_and_year;

Delete from #Regr_Wks where Wk_Rnk > Regression_Yrs *52 + 13;

Set Y1W01 = (Select min(Subs_week_and_year) from #Regr_Wks);

-- Case when ((Cast(LV as float)/100) % 1)*100 = 53
--                       then (LV/100-2)*100 + 1
--                  else LV - 300
--             end;
Set Y3W52 = (Select max(Subs_week_and_year) from #Regr_Wks);
-- Case when ((Cast(LV as float)/10) % 1)*10 = 1
--                       then (LV/100 - 1)*100 +  52
--                  else LV - 1
--             end;


drop table if exists #cuscan_weekly_agg;
select
  subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment

--,offer_length_dtv
-- ,dtv_offer_applied
--,new_customer
--,activations_acquisitions
--,activations_reinstates

,Count(*) as n
,Cast(sum(TA_Event_Count) as float) as TA_Call_cnt
,Cast(sum(Unique_TA_Caller) as float) as TA_Call_Customers
,Cast(sum(TA_Non_Save_Count) as float) as TA_Not_Saved
,Cast(sum(TA_Save_Count) as float) as TA_Saved
,Cast(sum(Web_Chat_TA_Cnt) as float) as Web_Chat_TA_Cnt
,Cast(sum(Web_Chat_TA_Customers) as float) as Web_Chat_TA_Customers
,Cast(sum(Web_Chat_TA_Not_Saved) as float) as Web_Chat_TA_Not_Saved
,Cast(sum(Web_Chat_TA_Saved) as float) as Web_Chat_TA_Saved
,Cast(sum(Offer_Applied_DTV) as float) as DTV_Offer_Applied
,Cast(sum(TA_DTV_Offer_Applied) as float) as TA_DTV_Offer_Applied
,Cast(DTV_Offer_Applied - TA_DTV_Offer_Applied        as float) as NonTA_DTV_Offer_Applied

,Cast(sum(TA_DTV_PC)                as float) as TA_DTV_PC
,Cast(sum(WC_DTV_PC)                as float) as WC_DTV_PC
,Cast(sum(Accessibility_DTV_PC) + sum(Min_Term_PC) + sum(Other_PC) as float) as Other_PC

,row_number() over(partition by cuscan_forecast_segment order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,((week_id-1)/13)+1 as quarter_id
into #cuscan_weekly_agg
from CITeam.DTV_Fcast_Weekly_Base agg
where subs_week_and_year between Y1W01 and Y3W52
        and subs_week != 53
        and Downgrade_View = 'Actuals'
group by subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment
;


-----------------------------------------------------------------------------------------------------------
----------  Turnaround and Webchat Events -----------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
Drop table if exists #Regr_inputs;
select   quarter_id
        ,agg.cuscan_forecast_segment
        ,row_number() over(partition by agg.cuscan_forecast_segment order by quarter_id desc)     as x
        ,sum(cast(TA_Call_Customers     as float))/sum(n)        as TA_cust
        ,sum(cast(Web_Chat_TA_Customers as float))/sum(n)        as WC_cust
        ,Case when sum(TA_Call_Customers) = 0 then 0 else sum(cast(TA_DTV_Offer_Applied as float))/sum(TA_Call_Customers) end       as TA_DTV_Offer_Applied_cust
        ,sum(cast(NonTA_DTV_Offer_Applied as float))/(sum(n)-sum(TA_Call_Customers))       as NonTA_DTV_Offer_Applied_cust

        ,case when sum(TA_Call_Customers) = 0 then 0 else sum(cast(TA_DTV_PC as float))/sum(TA_Call_Customers)  end   as TA_DTV_PC_Cust
        ,case when sum(Web_Chat_TA_Customers) = 0 then 0 else sum(cast(WC_DTV_PC as float))/sum(Web_Chat_TA_Customers) end as WC_DTV_PC_Cust
        ,sum(cast(Other_PC as float))/(sum(n)-sum(TA_Call_Customers)-sum(Web_Chat_TA_Customers)) as Other_PC_Cust

        ,x*x                                                    as xx
        ,x*TA_cust                                              as x_TA_cust
        ,x*WC_cust                                              as x_WC_cust
        ,x*TA_DTV_Offer_Applied_cust                            as x_TA_DTV_Offer_Applied_cust
        ,x*NonTA_DTV_Offer_Applied_cust                         as x_NonTA_DTV_Offer_Applied_cust
        ,x*TA_DTV_PC_Cust                                       as x_TA_DTV_PC_Cust
        ,x*WC_DTV_PC_Cust                                       as x_WC_DTV_PC_Cust
        ,x*Other_PC_Cust                                        as x_Other_PC_Cust


        ,Sum(n)                                                as cell_n
        ,cast(null as float)                                    as TA_regression
        ,cast(null as float)                                    as WC_regression
        ,cast(null as float)                                    as TA_DTV_Offer_Applied_regression
        ,cast(null as float)                                    as NonTA_DTV_Offer_Applied_regression
        ,cast(null as float)                                    as TA_DTV_PC_regression
        ,cast(null as float)                                    as WC_DTV_PC_regression
        ,cast(null as float)                                    as Other_PC_regression

into #Regr_inputs
from #Cuscan_weekly_agg as agg
group by  quarter_id
         ,agg.cuscan_forecast_segment
;


-- Select * from #Regr_inputs;

--weighted univariate regression (weeks is the independent variable, each weekly data point has cell_n observations)
Drop table if exists #Regr_coeff;
select cuscan_forecast_segment
        ,sum(cell_n)             as n
        ,sum(Cast(cell_n as bigint)*x)          as sum_x
        ,sum(Cast(cell_n as bigint)*xx)         as sum_xx

        ,sum(cell_n*TA_cust)       as sum_TA_cust
        ,sum(cell_n*WC_cust)       as sum_WC_cust
        ,sum(cell_n*TA_DTV_Offer_Applied_cust)       as sum_TA_DTV_Offer_Applied_cust
        ,sum(cell_n*NonTA_DTV_Offer_Applied_cust)    as sum_NonTA_DTV_Offer_Applied_cust
        ,sum(cell_n*TA_DTV_PC_Cust)   as sum_TA_DTV_PC_Cust
        ,sum(cell_n*WC_DTV_PC_Cust)   as sum_WC_DTV_PC_Cust
        ,sum(cell_n*Other_PC_Cust)    as sum_Other_PC_Cust

        ,sum(cell_n*x_TA_cust)     as sum_x_TA_cust
        ,sum(cell_n*x_WC_cust)     as sum_x_WC_cust
        ,sum(cell_n*x_TA_DTV_Offer_Applied_cust)     as sum_x_TA_DTV_Offer_Applied_cust
        ,sum(cell_n*x_NonTA_DTV_Offer_Applied_cust)  as sum_x_NonTA_DTV_Offer_Applied_cust
        ,sum(cell_n*x_TA_DTV_PC_Cust)  as sum_x_TA_DTV_PC_Cust
        ,sum(cell_n*x_WC_DTV_PC_Cust)  as sum_x_WC_DTV_PC_Cust
        ,sum(cell_n*x_Other_PC_Cust)   as sum_x_Other_PC_Cust

        ,cast(null as float)     as b0_TA_cust
        ,cast(null as float)     as b0_WC_cust
        ,cast(null as float)     as b0_TA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b0_NonTA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b0_TA_DTV_PC_Cust
        ,cast(null as float)     as b0_WC_DTV_PC_Cust
        ,cast(null as float)     as b0_Other_PC_Cust

        ,cast(null as float)     as b1_TA_cust
        ,cast(null as float)     as b1_WC_cust
        ,cast(null as float)     as b1_TA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b1_NonTA_DTV_Offer_Applied_cust
        ,cast(null as float)     as b1_TA_DTV_PC_Cust
        ,cast(null as float)     as b1_WC_DTV_PC_Cust
        ,cast(null as float)     as b1_Other_PC_Cust


into #Regr_coeff
from #Regr_inputs
group by cuscan_forecast_segment
having n > 1000
;



update #Regr_coeff set b1_TA_cust        = (sum_x_TA_cust       - (sum_TA_cust       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_WC_cust        = (sum_x_WC_cust       - (sum_WC_cust       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_TA_DTV_Offer_Applied_cust        = (sum_x_TA_DTV_Offer_Applied_cust       - (sum_TA_DTV_Offer_Applied_cust       *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff set b1_NonTA_DTV_Offer_Applied_cust     = (sum_x_NonTA_DTV_Offer_Applied_cust    - (sum_NonTA_DTV_Offer_Applied_cust    *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);

update #Regr_coeff set b1_TA_DTV_PC_Cust    = case when sum_TA_cust=0 then 0 else (sum_x_TA_DTV_PC_Cust   - (sum_TA_DTV_PC_Cust   *sum_x)/sum_TA_cust)/(sum_xx -(sum_x*sum_x)/sum_TA_cust) end;
update #Regr_coeff set b1_WC_DTV_PC_Cust    = case when sum_WC_cust=0 then 0 else (sum_x_WC_DTV_PC_Cust   - (sum_WC_DTV_PC_Cust   *sum_x)/sum_WC_cust)/(sum_xx -(sum_x*sum_x)/sum_WC_cust) end;
update #Regr_coeff set b1_Other_PC_Cust     = (sum_x_Other_PC_Cust    - (sum_Other_PC_Cust    *sum_x)/(n-sum_TA_cust-sum_WC_cust))/(sum_xx -(sum_x*sum_x)/(n-sum_TA_cust-sum_WC_cust));

update #Regr_coeff set b0_TA_cust        = sum_TA_cust       /n      - b1_TA_cust        *sum_x/n;
update #Regr_coeff set b0_WC_cust        = sum_WC_cust       /n      - b1_WC_cust        *sum_x/n;
update #Regr_coeff set b0_TA_DTV_Offer_Applied_cust        = sum_TA_DTV_Offer_Applied_cust       /n      - b1_TA_DTV_Offer_Applied_cust        *sum_x/n;
update #Regr_coeff set b0_NonTA_DTV_Offer_Applied_cust     = sum_NonTA_DTV_Offer_Applied_cust    /n      - b1_NonTA_DTV_Offer_Applied_cust     *sum_x/n;

update #Regr_coeff set b0_TA_DTV_PC_Cust   = case when sum_TA_cust=0 then 0 else sum_TA_DTV_PC_Cust   /sum_TA_cust      - b1_TA_DTV_PC_Cust    *sum_x/sum_TA_cust end;
update #Regr_coeff set b0_WC_DTV_PC_Cust   = case when sum_WC_cust=0 then 0 else sum_WC_DTV_PC_Cust   /sum_WC_cust      - b1_WC_DTV_PC_Cust    *sum_x/sum_WC_cust end;
update #Regr_coeff set b0_Other_PC_Cust    = sum_Other_PC_Cust    /(n-sum_TA_cust-sum_WC_cust)      - b1_Other_PC_Cust     *sum_x/(n-sum_TA_cust-sum_WC_cust);


-- Select * from Regr_coeff




-----------------------------------------------------------------------------------------------------------
----------  Active Blocks ---------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
drop table if exists #Syscan_weekly_agg;
select
  subs_year
,subs_week
,subs_week_and_year
,Syscan_forecast_segment

,Count(*) as n
,Cast(sum(DTV_AB) as float) as DTV_AB


,row_number() over(partition by Syscan_forecast_segment order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,((week_id-1)/13)+1 as quarter_id
into #Syscan_weekly_agg
from CITeam.DTV_Fcast_Weekly_Base agg
where subs_week_and_year between Y1W01 and Y3W52
        and subs_week != 53
        and Downgrade_View = 'Actuals'
group by subs_year
,subs_week
,subs_week_and_year
,Syscan_forecast_segment
;


--regression inputs
Drop table if exists #Regr_inputs_2;
select quarter_id
        ,agg.Syscan_forecast_segment
        ,row_number() over(partition by agg.Syscan_forecast_segment  order by quarter_id)     as x
        ,sum(cast(DTV_AB     as float))/sum(n)                  as DTV_AB

        ,x*x                                                    as xx
        ,x*DTV_AB                                               as x_DTV_AB

        ,sum(n)                                                 as cell_n
        ,cast(null as float)                                    as DTV_AB_regression

into #Regr_inputs_2
from #syscan_weekly_agg as agg
group by quarter_id
        ,agg.Syscan_forecast_segment
;

--weighted univariate regression (weeks is the independent variable, each weekly data point has cell_n observations)
Drop table if exists #Regr_coeff_2;
select Syscan_forecast_segment
        ,sum(cell_n)             as n
        ,sum(Cast(cell_n as bigint)*x)          as sum_x
        ,sum(Cast(cell_n as bigint)*xx)         as sum_xx

        ,sum(cell_n*DTV_AB)       as sum_DTV_AB
        ,sum(cell_n*x_DTV_AB)     as sum_x_DTV_AB

        ,cast(null as float)     as b0_DTV_AB
        ,cast(null as float)     as b1_DTV_AB

into #Regr_coeff_2
from #Regr_inputs_2
group by Syscan_forecast_segment
having n > 1000
;


update #Regr_coeff_2 set b1_DTV_AB        = (sum_x_DTV_AB        - (sum_DTV_AB        *sum_x)/n)/(sum_xx -(sum_x*sum_x)/n);
update #Regr_coeff_2 set b0_DTV_AB        = sum_DTV_AB       /n      - b1_DTV_AB         *sum_x/n;




---------------------------------------------------------------------------------------------------
-- Set proc outputs -------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
Select LV,'TA_Call_Customers' as Metric,cuscan_forecast_segment as forecast_segment,b1_TA_cust,b0_TA_cust
from #Regr_coeff
union all
Select LV,'Web_Chat_TA_Customers'  as Metric,cuscan_forecast_segment as forecast_segment,b1_WC_cust,b0_WC_cust
from #Regr_coeff
union all
Select LV,'TA_DTV_Offer_Applied' as Metric,cuscan_forecast_segment as forecast_segment,b1_TA_DTV_Offer_Applied_cust,b0_TA_DTV_Offer_Applied_cust
from #Regr_coeff
union all
Select LV,'NonTA_DTV_Offer_Applied' as Metric,cuscan_forecast_segment as forecast_segment,b1_NonTA_DTV_Offer_Applied_cust,b0_NonTA_DTV_Offer_Applied_cust
from #Regr_coeff
union all
Select LV,'DTV_AB' as Metric,Syscan_forecast_segment as forecast_segment,b1_DTV_AB,b0_DTV_AB
from #Regr_coeff_2
union all
Select LV,'TA_DTV_PC' as Metric,cuscan_forecast_segment as forecast_segment,b1_TA_DTV_PC_Cust,b0_TA_DTV_PC_Cust
from #Regr_coeff
union all
Select LV,'WC_DTV_PC' as Metric,cuscan_forecast_segment as forecast_segment,b1_WC_DTV_PC_Cust,b0_WC_DTV_PC_Cust
from #Regr_coeff
union all
Select LV,'Other_PC' as Metric,cuscan_forecast_segment as forecast_segment,b1_Other_PC_Cust,b0_Other_PC_Cust
from #Regr_coeff
;


END;

Grant Execute on Regression_Coefficient to CITeam;

RETURN;
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

/*
Call Regression_Coefficient(201601);

Select *
-- into #Regression_Coefficient_Test
from citeam.Regression_Coefficient(201601,2);

Select * from #Regression_Coefficient_Test
*/









------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of PCs will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------
Setuser citeam;

create variable Forecast_Start_Week integer; Set Forecast_Start_Week = 201601;

drop procedure if exists CITeam.Intraweek_PCs_Dist;

create procedure CITeam.Intraweek_PCs_Dist(IN Forecast_Start_Week integer)
RESULT
(
  Next_Status_Code varchar(2),
  PC_ReAC_Offer_Applied  tinyint,
  PCs integer,
  IntaWk_PC_Lower_Pctl float,
  IntaWk_PC_Upper_Pctl float
)
BEGIN

Select * into #Sky_Calendar from Citeam.subs_calendar(Forecast_Start_Week/100 -1,Forecast_Start_Week/100);

drop table if exists #Acc_PC_Events_Same_Week;

select subs_week_and_year
,event_dt
,event_dt - datepart(weekday,event_dt+2) PC_Event_End_Dt
,PC_Effective_To_Dt
,PC_Effective_To_Dt - datepart(weekday,PC_Effective_To_Dt+2) PC_Effective_To_End_Dt
,mor.account_number
-- ,csh.status_code Next_Status_Code1
,MoR.PC_Next_Status_Code Next_Status_Code
,Case when MoR.PC_Reactivation_Offer_Id is not null then 1 else 0 end PC_ReAC_Offer_Applied

into #Acc_PC_Events_Same_Week
from CITeam.Master_Of_Retention as MoR
where event_dt between (select max(calendar_date - 6 - 5*7) from #sky_calendar where subs_week_and_year = Forecast_Start_Week) -- Last 6 Wk PC conversions
                        and (select max(calendar_date) from #sky_calendar where subs_week_and_year = Forecast_Start_Week)
                        and (Same_Day_Cancels > 0 or PC_Pending_Cancellations > 0 or Same_Day_PC_Reactivations > 0);


Select Coalesce(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt then MoR.Next_Status_Code else null end,'PC') Next_Status_Code,
       Cast(Case Next_Status_Code
            when 'AC' then 1
            when 'PO' then 2
            when 'AB' then 3
            else 0
       end as integer) Next_Status_Code_Rnk,
       Cast(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt then MoR.PC_ReAC_Offer_Applied else 0 end as integer) PC_ReAC_Offer_Applied,
       Row_number() over(order by Next_Status_Code_Rnk,PC_ReAC_Offer_Applied) Row_ID,
--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'PO' then 1 else 0 end) as Intraweek_Churn,
--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'AC' then 1 else 0 end) as Intraweek_PC_Reactivation,
       count(*) as PCs--,
--        sum(PCs) over() Total_PCs,
--        sum(PCs) over(order by Row_ID) Cum_PCs,
--        Cast(Cum_PCs as float)/Total_PCs as IntaWk_PC_Upper_Pctl
--        Cast(Intraweek_Churn as float)/PCs as Pct_Intraweek_Churn,
--        Cast(Intraweek_PC_Reactivation as float)/PCs as Pct_Intraweek_Reactivation
into #PC_Events_Same_Week
from #Acc_PC_Events_Same_Week MoR
group by Next_Status_Code,PC_ReAC_Offer_Applied;


Select Row_ID,Next_Status_Code,PC_ReAC_Offer_Applied,PCs,
       sum(PCs) over(order by Row_ID) Cum_PCs,
       sum(PCs) over() Total_PCs,
       Cast(Cum_PCs as float)/Total_PCs as IntaWk_PC_Upper_Pctl
into #PC_Events
from #PC_Events_Same_Week pc1
group by Row_ID,Next_Status_Code,PC_ReAC_Offer_Applied,PCs;


Select pc1.Next_Status_Code,pc1.PC_ReAC_Offer_Applied,pc1.PCs
        ,Coalesce(pc2.IntaWk_PC_Upper_Pctl,0) IntaWk_PC_Lower_Pctl
        ,pc1.IntaWk_PC_Upper_Pctl
from #PC_Events pc1
     left join
     #PC_Events pc2
     on pc2.row_id = pc1.row_id - 1;



END;

Grant execute on CITeam.Intraweek_PCs_Dist to CITeam;

Setuser;

Select * from CITeam.Intraweek_PCs_Dist(201601);













------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc duration between PC and intended churn date ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
Setuser citeam;

create variable Forecast_Start_Week integer; Set Forecast_Start_Week = 201601;

drop procedure if exists CITeam.PC_Duration_Dist;

create procedure CITeam.PC_Duration_Dist(IN Forecast_Start_Week integer)
RESULT
(
  Days_To_churn integer,
  PCs integer,
  Total_PCs integer,
  PC_Days_Lower_Prcntl float,
  PC_Days_Upper_Prcntl float
)
BEGIN

Select * into #Sky_Calendar from Citeam.subs_calendar(Forecast_Start_Week/100 -1,Forecast_Start_Week/100);


Select
 event_dt - datepart(weekday,event_dt+2) PC_Event_End_Dt
,PC_Effective_To_Dt - datepart(weekday,PC_Effective_To_Dt+2) PC_Effective_To_End_Dt
,PC_Future_Sub_Effective_Dt - datepart(weekday,PC_Future_Sub_Effective_Dt+2) as PC_Future_Sub_End_Dt
,PC_Future_Sub_Effective_Dt - PC_Event_End_Dt as Days_To_churn
-- ,Count(*) PC_Pipeline_Cancellations
into #PC_Events_Days_To_Intended_Churn
from citeam.Master_of_retention -- from MoR
where event_dt between (select max(calendar_date - 6*7 + 1) from #sky_calendar where subs_week_and_year = Forecast_Start_Week) -- Last 6 Wk PC conversions
        and (select max(calendar_date) from #sky_calendar where subs_week_and_year = Forecast_Start_Week)
        and (Same_Day_Cancels > 0 or PC_Pending_Cancellations > 0 or Same_Day_PC_Reactivations > 0)
        and PC_Event_End_Dt != PC_Effective_To_End_Dt
        and PC_Event_End_Dt != PC_Future_Sub_End_Dt
        and PC_Future_Sub_Effective_Dt > event_dt
-- group by PC_Event_End_Dt,PC_Effective_To_End_Dt,Days_To_churn
-- order by Days_To_churn
;

Select  Days_To_churn
       ,Row_number() over(order by Days_To_churn) Row_ID
       ,count(*) as PCs
       ,sum(PCs) over()  Total_PCs
       ,sum(PCs) over(order by Days_To_churn)  Cum_PCs
       ,Cast(PCs as float)/Total_PCs as Pct_PCs
       ,Cast(null as float) as PC_Days_Lower_Prcntl
       ,Cast(Cum_PCs as float)/Total_PCs as PC_Days_Upper_Prcntl
into #PC_Days_Prcntl
from #PC_Events_Days_To_Intended_Churn
group by Days_To_churn
order by Days_To_churn;

Update #PC_Days_Prcntl pc1
Set pc1.PC_Days_Lower_Prcntl = Coalesce(pc2.PC_Days_Upper_Prcntl,0)
from #PC_Days_Prcntl pc1
     left join
     #PC_Days_Prcntl pc2
     on pc2.Row_ID = pc1.Row_ID - 1;

Select Days_To_churn,PCs,Total_PCs,PC_Days_Lower_Prcntl,PC_Days_Upper_Prcntl
from #PC_Days_Prcntl;

END;

Grant execute on CITeam.PC_Duration_Dist to CITeam;

Setuser;

Select * from CITeam.PC_Duration_Dist(201601);













------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from PC to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------

Setuser citeam;

create variable Forecast_Start_Week integer; Set Forecast_Start_Week = 201601;

drop procedure if exists CITeam.PC_Status_Movement_Probabilities;

create procedure CITeam.PC_Status_Movement_Probabilities(IN Forecast_Start_Week integer)
RESULT
(
  Wks_To_Intended_Churn varchar(20),
  Status_Code_EoW varchar(2),
  Status_Code_EoW_Rnk integer,
  PC_ReAC_Offer_Applied  tinyint,
  PCs integer,
  Cum_Total_Cohort_PCs integer,
  Total_Cohort_PCs integer,
  PC_Percentile_Lower_Bound float,
  PC_Percentile_Upper_Bound float
)
BEGIN

Select * into #Sky_Calendar from CITeam.Subs_Calendar(Forecast_Start_Week/100 -1,Forecast_Start_Week/100);

drop table if exists #PC_Intended_Churn;
select account_number,
       event_dt,
--        Cast(event_dt - datepart(weekday,event_dt+2) + 7 as date) event_dt_End_Dt,
       PC_Future_Sub_Effective_Dt,
       Cast(PC_Future_Sub_Effective_Dt - datepart(weekday,PC_Future_Sub_Effective_Dt+2) + 7 as date) PC_Future_Sub_Effective_Dt_End_Dt,
       PC_Effective_To_Dt,

--        Cast(PC_Effective_To_Dt - datepart(weekday,PC_Effective_To_Dt+2)+7 as date) PC_Effective_To_Dt_End_Dt,
       PC_Next_status_code Next_status_code,
       Case when PC_Reactivation_Offer_Id is not null then 1 else 0 end PC_ReAC_Offer_Applied

into #PC_Intended_Churn
from CITeam.Master_of_Retention
where PC_Future_Sub_Effective_Dt between (select min(calendar_date - 6*7) from #sky_calendar where subs_week_and_year = Forecast_Start_Week) -- Last 6 Wk PC conversions
                        and (select min(calendar_date - 1) from #sky_calendar where subs_week_and_year = Forecast_Start_Week)
        and (PC_Pending_Cancellations > 0) -- the next 7 days
        and PC_Future_Sub_Effective_Dt is not null and Next_status_code is not null
        and PC_Effective_To_Dt <= PC_Future_Sub_Effective_Dt
;

Select PCs.*,
       Case when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=0 then 'Churn in next 1 wks'
          when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=1 then 'Churn in next 2 wks'
          when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=2 then 'Churn in next 3 wks'
          when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=3 then 'Churn in next 4 wks'
          when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=4 then 'Churn in next 5 wks'
          when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>=5 then 'Churn in next 6+ wks'
--           when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>5 then '6+_Wks_To_Churn'
     end as  Wks_To_Intended_Churn,
       sc.Calendar_date End_date,
       Case when sc.calendar_date + 7 between event_dt and PC_Effective_To_Dt then 'PC'
            when sc.calendar_date + 7 between PC_Effective_To_Dt and PC_Future_Sub_Effective_Dt_End_Dt then Next_Status_Code
       end Status_Code_EoW,
     Case when sc.calendar_date + 7 between PC_Effective_To_Dt and PC_Future_Sub_Effective_Dt_End_Dt
                and Status_Code_EoW = 'AC'
                                                then PCs.PC_ReAC_Offer_Applied
                                                else 0
     end PC_ReAC_Offer_Applied_EoW,
     (Case Status_Code_EoW when 'AC' then 1
                            when 'AB' then 2
                            when 'PO' then 3
                            when 'PC' then 4
     end) - PC_ReAC_Offer_Applied_EoW  as Status_Code_EoW_Rnk
into #PC_PL_Status
from #PC_Intended_Churn PCs
     inner join
     #sky_calendar sc
     on sc.calendar_date between PCs.event_dt and PCs.PC_Effective_To_Dt - 1
        and sc.subs_last_day_of_week = 'Y'
;

-- Select top 100 * from #PC_PL_Status where Wks_To_Intended_Churn = '0_Wks_To_Churn' and Status_Code_EoW is null

Select Wks_To_Intended_Churn,Status_Code_EoW,Status_Code_EoW_Rnk,PC_ReAC_Offer_Applied_EoW,
       count(*) PCs,
       Sum(PCs) over(partition by Wks_To_Intended_Churn order by Status_Code_EoW_Rnk) Cum_Total_Cohort_PCs,
       Sum(PCs) over(partition by Wks_To_Intended_Churn) Total_Cohort_PCs,
       Cast(null as float) as PC_Percentile_Lower_Bound,
       Cast(Cum_Total_Cohort_PCs as float)/Total_Cohort_PCs as PC_Percentile_Upper_Bound
into #PC_Percentiles
from #PC_PL_Status
group by Wks_To_Intended_Churn,Status_Code_EoW_Rnk,Status_Code_EoW,PC_ReAC_Offer_Applied_EoW
order by Wks_To_Intended_Churn,Status_Code_EoW_Rnk,Status_Code_EoW,PC_ReAC_Offer_Applied_EoW;

Update #PC_Percentiles pcp
Set PC_Percentile_Lower_Bound = Cast(Coalesce(pcp2.PC_Percentile_Upper_Bound,0) as float)
from #PC_Percentiles pcp
     left join
     #PC_Percentiles pcp2
     on pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn
        and pcp2.Status_Code_EoW_Rnk = pcp.Status_Code_EoW_Rnk - 1;

Select * from #PC_Percentiles;

END;

grant execute on CITeam.PC_Status_Movement_Probabilities to CITeam;

Setuser;

Select * from CITeam.PC_Status_Movement_Probabilities(201601);



























------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of ABs will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------
Setuser citeam;

create variable Forecast_Start_Week integer; Set Forecast_Start_Week = 201601;

drop procedure if exists CITeam.Intraweek_ABs_Dist;

create procedure CITeam.Intraweek_ABs_Dist(IN Forecast_Start_Week integer)
RESULT
(
  Next_Status_Code varchar(2),
  AB_ReAC_Offer_Applied  tinyint,
  ABs integer,
  IntaWk_AB_Lower_Pctl float,
  IntaWk_AB_Upper_Pctl float
)
BEGIN

Select * into #Sky_Calendar from Citeam.subs_calendar(Forecast_Start_Week/100 -1,Forecast_Start_Week/100);

drop table if exists #Acc_AB_Events_Same_Week;

select subs_week_and_year
,event_dt
,event_dt - datepart(weekday,event_dt+2) AB_Event_End_Dt
,AB_Effective_To_Dt
,AB_Effective_To_Dt - datepart(weekday,AB_Effective_To_Dt+2) AB_Effective_To_End_Dt
,mor.account_number
-- ,csh.status_code Next_Status_Code1
,MoR.AB_Next_Status_Code Next_Status_Code
,Case when MoR.AB_Reactivation_Offer_Id is not null then 1 else 0 end AB_ReAC_Offer_Applied

into #Acc_AB_Events_Same_Week
from CITeam.Master_Of_Retention as MoR
where event_dt between (select max(calendar_date - 6 - 5*7) from #sky_calendar where subs_week_and_year = Forecast_Start_Week) -- Last 6 Wk PC conversions
                        and (select max(calendar_date) from #sky_calendar where subs_week_and_year = Forecast_Start_Week)
                        and AB_Pending_Terminations > 0;


Select Coalesce(Case when AB_Effective_To_End_Dt = AB_Event_End_Dt then MoR.Next_Status_Code else null end,'AB') Next_Status_Code,
       Cast(Case Next_Status_Code
            when 'AC' then 1
            when 'SC' then 2
            when 'PC' then 3
            when 'PO' then 4
            else 0
       end as integer) Next_Status_Code_Rnk,
       Cast(Case when AB_Effective_To_End_Dt = AB_Event_End_Dt then MoR.AB_ReAC_Offer_Applied else 0 end as integer) AB_ReAC_Offer_Applied,
       Row_number() over(order by Next_Status_Code_Rnk,AB_ReAC_Offer_Applied) Row_ID,
--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'PO' then 1 else 0 end) as Intraweek_Churn,
--        sum(Case when PC_Effective_To_End_Dt = PC_Event_End_Dt and Next_Status_Code = 'AC' then 1 else 0 end) as Intraweek_PC_Reactivation,
       count(*) as ABs--,
--        sum(PCs) over() Total_PCs,
--        sum(PCs) over(order by Row_ID) Cum_PCs,
--        Cast(Cum_PCs as float)/Total_PCs as IntaWk_PC_Upper_Pctl
--        Cast(Intraweek_Churn as float)/PCs as Pct_Intraweek_Churn,
--        Cast(Intraweek_PC_Reactivation as float)/PCs as Pct_Intraweek_Reactivation
into #AB_Events_Same_Week
from #Acc_AB_Events_Same_Week MoR
group by Next_Status_Code,AB_ReAC_Offer_Applied;


Select Row_ID,Next_Status_Code,AB_ReAC_Offer_Applied,ABs,
       sum(ABs) over(order by Row_ID) Cum_ABs,
       sum(ABs) over() Total_ABs,
       Cast(Cum_ABs as float)/Total_ABs as IntaWk_PC_Upper_Pctl
into #AB_Events
from #AB_Events_Same_Week pc1
group by Row_ID,Next_Status_Code,AB_ReAC_Offer_Applied,ABs;


Select pc1.Next_Status_Code,pc1.AB_ReAC_Offer_Applied,pc1.ABs
        ,Coalesce(pc2.IntaWk_PC_Upper_Pctl,0) IntaWk_PC_Lower_Pctl
        ,pc1.IntaWk_PC_Upper_Pctl
from #AB_Events pc1
     left join
     #AB_Events pc2
     on pc2.row_id = pc1.row_id - 1;



END;

Grant execute on CITeam.Intraweek_ABs_Dist to CITeam;

Setuser;

Select * from CITeam.Intraweek_PCs_Dist(201601);
















------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from AB to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------

Setuser citeam;

create variable Forecast_Start_Week integer; Set Forecast_Start_Week = 201601;

drop procedure if exists CITeam.AB_Status_Movement_Probabilities;

create procedure CITeam.AB_Status_Movement_Probabilities(IN Forecast_Start_Week integer)
RESULT
(
  Wks_To_Intended_Churn varchar(20),
  Status_Code_EoW varchar(2),
--   Status_Code_EoW_Rnk integer,
  AB_ReAC_Offer_Applied  tinyint,
  ABs integer,
  Cum_Total_Cohort_ABs integer,
  Total_Cohort_ABs integer,
  AB_Percentile_Lower_Bound float,
  AB_Percentile_Upper_Bound float
)
BEGIN

Select * into #Sky_Calendar from CITeam.Subs_Calendar(Forecast_Start_Week/100 -1,Forecast_Start_Week/100);

drop table if exists #AB_Intended_Churn;
select account_number,
       event_dt,
       AB_Future_Sub_Effective_Dt,
       Cast(AB_Future_Sub_Effective_Dt - datepart(weekday,AB_Future_Sub_Effective_Dt+2) + 7 as date) AB_Future_Sub_Effective_Dt_End_Dt,
       AB_Effective_To_Dt,
       AB_Next_status_code Next_status_code,
       Case when AB_Reactivation_Offer_Id is not null then 1 else 0 end as AB_ReAC_Offer_Applied

into #AB_Intended_Churn
from CITeam.Master_of_Retention
where AB_Future_Sub_Effective_Dt between (select min(calendar_date - 6*7) from #sky_calendar where subs_week_and_year = Forecast_Start_Week) -- Last 6 Wk PC conversions
                        and (select min(calendar_date - 1) from #sky_calendar where subs_week_and_year = Forecast_Start_Week)
        and (AB_Pending_Terminations > 0) -- the next 7 days
        and AB_Future_Sub_Effective_Dt is not null and AB_Next_status_code is not null
        and AB_Effective_To_Dt <= AB_Future_Sub_Effective_Dt
;

Select ABs.*,
       Case when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=0 then 'Churn in next 1 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=1 then 'Churn in next 2 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=2 then 'Churn in next 3 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=3 then 'Churn in next 4 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=4 then 'Churn in next 5 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=5 then 'Churn in next 6 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=6 then 'Churn in next 7 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=7 then 'Churn in next 8 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7=8 then 'Churn in next 9 wks'
          when (cast(AB_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>=9 then 'Churn in next 10+ wks'
     end as  Wks_To_Intended_Churn,
       sc.Calendar_date End_date,
       Case when sc.calendar_date + 7 between event_dt and AB_Effective_To_Dt then 'AB'
            when sc.calendar_date + 7 between AB_Effective_To_Dt and AB_Future_Sub_Effective_Dt_End_Dt then Next_Status_Code
       end Status_Code_EoW,
     Case when sc.calendar_date + 7 = AB_Effective_To_Dt - datepart(weekday,AB_Effective_To_Dt+2) + 7
                and Status_Code_EoW = 'AC'
                                                then ABs.AB_ReAC_Offer_Applied
                                                else 0
     end AB_ReAC_Offer_Applied_EoW,
     (Case Status_Code_EoW when 'AC' then 1
                           when 'AB' then 2
                           when 'SC' then 3
                           when 'PC' then 4
                           when 'PO' then 5
     end) - AB_ReAC_Offer_Applied_EoW  as Status_Code_EoW_Rnk
into #AB_PL_Status
from #AB_Intended_Churn ABs
     inner join
     #sky_calendar sc
     on sc.calendar_date between ABs.event_dt and ABs.AB_Effective_To_Dt - 1
        and sc.subs_last_day_of_week = 'Y'
;

-- Select top 100 * from #PC_PL_Status where Wks_To_Intended_Churn = '0_Wks_To_Churn' and Status_Code_EoW is null

Select Wks_To_Intended_Churn,Status_Code_EoW,Status_Code_EoW_Rnk,AB_ReAC_Offer_Applied_EoW,
       count(*) ABs,
       Sum(ABs) over(partition by Wks_To_Intended_Churn order by Status_Code_EoW_Rnk) Cum_Total_Cohort_ABs,
       Sum(ABs) over(partition by Wks_To_Intended_Churn) Total_Cohort_ABs,
       Cast(null as float) as AB_Percentile_Lower_Bound,
       Cast(Cum_Total_Cohort_ABs as float)/Total_Cohort_ABs as AB_Percentile_Upper_Bound,
       Row_Number() over(partition by Wks_To_Intended_Churn order by Status_Code_EoW_Rnk) Row_ID
into #AB_Percentiles
from #AB_PL_Status
group by Wks_To_Intended_Churn,Status_Code_EoW_Rnk,Status_Code_EoW,AB_ReAC_Offer_Applied_EoW
order by Wks_To_Intended_Churn,Status_Code_EoW_Rnk,Status_Code_EoW,AB_ReAC_Offer_Applied_EoW;

Update #AB_Percentiles pcp
Set AB_Percentile_Lower_Bound = Cast(Coalesce(pcp2.AB_Percentile_Upper_Bound,0) as float)
from #AB_Percentiles pcp
     left join
     #AB_Percentiles pcp2
     on pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn
        and pcp2.Row_ID = pcp.Row_ID - 1;

Select Wks_To_Intended_Churn,Status_Code_EoW,AB_ReAC_Offer_Applied_EoW,
       ABs,Cum_Total_Cohort_ABs,Total_Cohort_ABs,AB_Percentile_Lower_Bound,AB_Percentile_Upper_Bound
from #AB_Percentiles;

END;

grant execute on CITeam.AB_Status_Movement_Probabilities to CITeam;

Setuser;

Select * from CITeam.AB_Status_Movement_Probabilities(201601);


/*
Select top 1000 * from master_of_retention where PC_effective_to_dt >= '2016-07-01' and Next_status_code = 'AC'


Create variable Forecast_Start_Wk integer; Set Forecast_Start_Wk = 201601;
Create variable Num_Wks integer; Set Num_Wks = 6;

Setuser CITeam;
*/
Drop procedure if exists Offer_Applied_Duration_Dist;

Create Procedure Offer_Applied_Duration_Dist(IN Forecast_Start_Wk integer,Num_Wks integer)
RESULT(Offer_segment varchar(30),
       Total_Offer_Duration_Mth integer,
       Weekly_Avg_New_Offers integer,
       Total_New_Offers integer,
       Cum_New_Offers integer,
       Dur_Pctl_Lower_Bound float,
       Dur_Pctl_Upper_Bound float
       )
BEGIN

Select * into #Sky_Calendar from subs_calendar(Forecast_Start_Wk/100-1,Forecast_Start_Wk/100);

Select
--     Case overall_offer_segment_grouped_1
--          when 'Winback' then 'Activations'
--          when 'PAT(Value)&CoE' then 'Other'
--          when 'Other-Unknown' then 'Other'
--          when 'ReInstate' then 'Activations'
--          when 'Price Protection' then 'Price Protection'
--          when 'NonRetention Offers' then 'Other'
--          when 'BB Acquisition/Upgrade' then 'Other'
--          when 'Other Retention' then 'TA'
--          when 'DTH Acquisition' then 'Activations'
--          when 'TA' then 'TA'
--          when 'Package Changes/Upgrades' then 'Other'
--     end overall_offer_segment_grouped_1,
--     overall_offer_segment,
    Case overall_offer_segment
        when '1.(DTH)A1.Acquisition' then 'Activations'
        when '1.(DTH)B1.Winback' then 'Activations'
        when '1.(DTH)B2.TA' then 'TA'
        when '1.(DTH)B3.CoE' then 'Other'
        when '1.(DTH)B4.PAT' then 'Other'
        when '1.(DTH)B5.Pipeline ReInstate' then 'Reactivations'
        when '1.(DTH)B6.Other Retention' then 'TA'
        when '1.(DTH)C1.DTV Package Movement' then 'Other'
        when '1.(DTH)D1.Offer On Call' then 'Other'
        when '1.(DTH)D3.Other' then 'Other'
        when '2.(BB)A1.Acquisition/Upgrade' then 'Activations'
        when '2.(BB)B1.TA' then 'TA'
        when '2.(BB)B2.CoE' then 'Other'
        when '2.(BB)B3.PAT' then 'Other'
        when '2.(BB)B4.Pipeline ReInstate' then 'Reactivations'
        when '2.(BB)B5.Other Retention' then 'Reactivations'
        when '2.(BB)C1.BB Package Movement' then 'Other'
        when '2.(BB)C2.Offer On Call' then 'Other'
        when '2.(BB)C4.Other' then 'Other'
    end overall_offer_segment,
    Total_Offer_Duration_Mth,
    count(*)/Num_Wks Weekly_Avg_New_Offers,
    Sum(Weekly_Avg_New_Offers) over(partition by overall_offer_segment) Total_New_Offers,
    Sum(Weekly_Avg_New_Offers) over(partition by overall_offer_segment order by Total_Offer_Duration_Mth) Cum_New_Offers,
    Cast(Cum_New_Offers as float)/Total_New_Offers as Pctl_New_Offers,
    Row_Number() over(partition by overall_offer_segment order by Total_Offer_Duration_Mth) Dur_Rnk
into #Offer_Dur
from citeam.offer_usage_all oua
where offer_start_dt_Actual between (Select max(calendar_date - 7 - Num_Wks*7 + 1) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
                                and (Select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
        and Total_Offer_Duration_Mth <= 36
        and offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual
        and Subs_Type = 'DTV Primary Viewing'
        and lower(offer_dim_description) not like '%price protection%'
        and oua.overall_offer_segment_grouped_1 != 'Price Protection'
group by overall_offer_segment,Total_Offer_Duration_Mth--,overall_offer_segment_grouped_1,x_overall_offer_segment
;


Select
--     dur1.overall_offer_segment_grouped_1,
    dur1.overall_offer_segment,
--     dur1.x_overall_offer_segment,
    dur1.Total_Offer_Duration_Mth,
    dur1.Weekly_Avg_New_Offers,
    dur1.Total_New_Offers,
    dur1.Cum_New_Offers,
    Coalesce(dur2.Pctl_New_Offers,0) Dur_Pctl_Lower_Bound,
    dur1.Pctl_New_Offers Dur_Pctl_Upper_Bound
from #Offer_Dur dur1
     left join
     #Offer_Dur dur2
     on dur2.overall_offer_segment = dur1.overall_offer_segment
        and dur2.Dur_Rnk = dur1.Dur_Rnk - 1

END;

Grant execute on Offer_Applied_Duration_Dist to CITeam;
/*
Setuser;

Select * from CITeam.Offer_Applied_Duration_Dist(201601,6)

*/

/*
dba.sp_drop_table 'CITeam','FORECAST_New_Cust_Sample'
dba.sp_create_table 'CITeam','FORECAST_New_Cust_Sample',
   'end_date date default null, '
|| 'year integer default null,'
|| 'week integer default null,'
|| 'year_week integer default null,'
|| 'account_number varchar(20) default null,'
|| 'dtv_status_code varchar(2) default null,'
|| 'DTV_PC_Future_Sub_Effective_Dt date default null, '
|| 'DTV_AB_Future_Sub_Effective_Dt date default null, '
|| 'BB_Segment varchar(30) default null,'
|| 'prem_segment varchar(7) default null,'
|| 'Simple_Segments varchar(13) default null,'
|| 'country varchar(3) default null,'
|| 'Affluence varchar(10) default null,'
|| 'package_desc varchar(50) default null,'
|| 'offer_length_DTV varchar(18) default null,'
|| 'curr_offer_start_date_DTV date default null,'
|| 'Curr_Offer_end_Date_Intended_DTV date default null,'
|| 'Prev_offer_end_date_DTV date default null,'
|| 'Time_To_Offer_End_DTV varchar(28) default null,'
|| 'curr_offer_start_date_BB date default null,'
|| 'Curr_Offer_end_Date_Intended_BB date default null,'
|| 'Prev_offer_end_date_BB date default null,'
|| 'Time_To_Offer_End_BB varchar(28) default null,'
|| 'curr_offer_start_date_LR date default null,'
|| 'Curr_Offer_end_Date_Intended_LR date default null,'
|| 'Prev_offer_end_date_LR date default null,'
|| 'Time_To_Offer_End_LR varchar(28) default null,'
|| 'DTV_BB_LR_offer_end_dt date default null,'
|| 'Time_To_Offer_End varchar(28) default null,'
|| 'DTV_Tenure varchar(5) default null,'
|| 'dtv_act_date date default null,'
|| 'Time_Since_Last_TA_call varchar(28) default null,'
|| 'Last_TA_Call_dt date default null,'
|| 'Time_Since_Last_AB varchar(24) default null,'
|| 'Last_AB_Dt date default null,'
|| 'Previous_AB_Count varchar(18) default null,'
|| 'Previous_Abs smallint default null,'
|| 'CusCan_Forecast_Segment varchar(100) default null,'
|| 'SysCan_Forecast_Segment varchar(100) default null,'
|| 'DTV_Activation_Type varchar(11) default null,'
|| 'HD_segment varchar(70) default null'

Select top 100 * from CITeam.FORECAST_New_Cust_Sample;
*/

Create variable LV integer;

Set LV = 201601;

Create variable Obs_Dt date;

-- First you need to impersonate CITeam
Setuser CITeam;




-- Drop procedure if exists CITeam.Build_Forecast_New_Cust_Sample;

Create procedure CITeam.Build_Forecast_New_Cust_Sample(In LV integer)
BEGIN

Declare Obs_Dt date;

Delete from CITeam.FORECAST_New_Cust_Sample;

Set Obs_Dt = (Select max(calendar_date) from citeam.subs_calendar(LV/100 -1,LV/100) where Subs_Week_And_Year < LV);

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Insert into CITeam.FORECAST_New_Cust_Sample
select
end_date
,cast(null as integer)  as year
,Cast(null as integer) as week
,Cast(null as integer) as year_week
,account_number
,dtv_status_code
,Cast(null as date) DTV_PC_Future_Sub_Effective_Dt
,Cast(null as date) DTV_AB_Future_Sub_Effective_Dt
,Case when BB_Active > 0 then 'BB' else 'Non BB' end BB_Segment
,Case when sports > 0 and movies > 0 then 'TopTier'
      when sports > 0                then 'Sports'
      when movies > 0                then 'Movies'
      when DTV_Active = 1            then 'Basic'
end prem_segment
,case
        when trim(simple_segment) in ('1 Secure')       then '1 Secure'
        when trim(simple_segment) in ('2 Start', '3 Stimulate','2 Stimulate')  then '2 Stimulate'
        when trim(simple_segment) in ('4 Support','3 Support')      then '3 Support'
        when trim(simple_segment) in ('5 Stabilise','4 Stabilise')    then '4 Stabilise'
        else 'Other/Unknown'
end as Simple_Segments
-- ,simple_segment
,Case when ROI > 0 then 'ROI' else 'UK' end as country
,Affluence_Bands as Affluence
,Case when trim(package_desc) in ('Variety','Kids,Mix,World') or package_desc is null then 'Variety'
      when package_desc is null then 'Original'
      when package_desc = 'Other' then 'Original'
      else package_desc
end package_desc
, case
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3  then 'Offer Length 3M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >3) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) then 'Offer Length 6M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >6) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) then 'Offer Length 9M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >9) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) then 'Offer Length 12M'
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12  then 'Offer Length 12M +'
        when Curr_Offer_end_Date_Intended_DTV is null then 'No Offer'
  end as offer_length_DTV
,curr_offer_start_date_DTV
,Curr_Offer_end_Date_Intended_DTV
,Prev_offer_end_date_DTV
,case
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_DTV between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_DTV between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End DTV'
  end as Time_To_Offer_End_DTV

,curr_offer_start_date_BB
,Curr_Offer_end_Date_Intended_BB
,Prev_offer_end_date_BB
,case
    when Curr_Offer_end_Date_intended_BB between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 43) and (end_date + 49) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 50) and (end_date + 56) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 57) and (end_date + 63) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 64) and (end_date + 70) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 71) and (end_date + 77) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 78) and (end_date + 84) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 85) and (end_date + 91) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB >= (end_date + 92)                          then 'Offer Ending in 7+ Wks'


    when Prev_offer_end_Date_BB between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_Date_BB between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 49) and (end_date - 43) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 56) and (end_date - 50) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 63) and (end_date - 57) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 70) and (end_date - 64) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 77) and (end_date - 71) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 84) and (end_date - 78) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 91) and (end_date - 85) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB <= (end_date - 92)                        then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB is null then 'Null'
    when Curr_Offer_end_Date_intended_BB is null then 'Null'
    else 'No Offer End BB'
end as Time_To_Offer_End_BB

,curr_offer_start_date_LR
,Curr_Offer_end_Date_Intended_LR
,Prev_offer_end_date_LR
  ,case
    when Curr_Offer_end_Date_Intended_LR between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_LR between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_LR between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer LR'
  end as Time_To_Offer_End_LR

,DTV_BB_LR_offer_end_dt
,case
    when DTV_BB_LR_offer_end_dt between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when DTV_BB_LR_offer_end_dt between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer'
  end as Time_To_Offer_End
,case when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)   then 'M10'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
      else 'YNone'
end as DTV_Tenure
,dtv_act_date
,Case when Last_TA_Call_dt is null then 'No Prev TA Calls'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7  = 0 then '0 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 1 then '01 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 2 and 5 then '02-05 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 6 and 35 then '06-35 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 36 and 41 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 42 and 46 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 47 then '47 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 48 and 52 then '48-52 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 53 and 60 then '53-60 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 > 60 then '61+ Wks since last TA Call'
--      when Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer) >= 52*2*7 then 'Last TA > 2 Yrs Ago'
     Else ''
--      (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 || ' Wks since last TA Call'
End Time_Since_Last_TA_call
,Last_TA_Call_dt
,Case when  Last_AB_Dt  is null then 'No Prev AB Calls'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 0 then '0 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 1 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 2 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 3 then '3 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 4 then '4 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 5 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 6 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 7 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 8 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 9 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 10 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 11 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 12 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 > 12 then '12+ Mnths since last AB'
     Else ''
end as  Time_Since_Last_AB
,Last_AB_Dt
,case
        when Previous_Abs = 0 then '0 Previous_Abs'
        when Previous_Abs = 1 then '1 Previous_Abs'
        when Previous_Abs = 2 then '2 Previous_Abs'
        when Previous_Abs = 3 then '3 Previous_Abs'
        when Previous_Abs = 4 then '4-7 Previous_Abs'
        when Previous_Abs = 5 then '4-7 Previous_Abs'
        when Previous_Abs = 6 then '4-7 Previous_Abs'
        when Previous_Abs = 7 then '4-7 Previous_Abs'
        when Previous_Abs = 8 then '8-10 Previous_Abs'
        when Previous_Abs = 9 then '8-10 Previous_Abs'
        when Previous_Abs = 10 then '8-10 Previous_Abs'
        when Previous_Abs = 11 then '11-15 Previous_Abs'
        when Previous_Abs = 12 then '11-15 Previous_Abs'
        when Previous_Abs = 13 then '11-15 Previous_Abs'
        when Previous_Abs = 14 then '11-15 Previous_Abs'
        when Previous_Abs = 15 then '11-15 Previous_Abs'
        when Previous_Abs >= 16 then '16 + Previous_Abs'
  else ''
end as Previous_AB_Count
,Previous_Abs
,Cast(null as varchar(100)) as CusCan_Forecast_Segment
,Cast(null as varchar(100)) as SysCan_Forecast_Segment

,Case when dtv_latest_act_date between (end_date-6) and end_date and dtv_first_act_date < dtv_latest_act_date then 'Reinstate'
      when dtv_latest_act_date between (end_date-6) and end_date and (dtv_first_act_date = dtv_latest_act_date) then 'Acquisition'
End as DTV_Activation_Type
,HD_segment
from citeam.cust_fcast_weekly_base
where end_date between Obs_Dt - 5* 7 and Obs_Dt
    and dtv_active =1
    and dtv_latest_act_date between (end_date-6) and end_date -- New customers
    and DTV_Activation_Type is not null
;

Update CITeam.FORECAST_New_Cust_Sample sample
Set DTV_PC_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
from CITeam.FORECAST_New_Cust_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.PC_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.PC_effective_to_dt >sample.end_date or MoR.PC_effective_to_dt is null)
where sample.DTV_Status_Code = 'PC';

Update CITeam.FORECAST_New_Cust_Sample sample
Set DTV_AB_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
from CITeam.FORECAST_New_Cust_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.AB_effective_to_dt >sample.end_date or MoR.AB_effective_to_dt is null)
where sample.DTV_Status_Code = 'AB';

Update CITeam.FORECAST_New_Cust_Sample sample
Set DTV_Status_Code = 'AC'
where DTV_AB_Future_Sub_Effective_Dt is null and DTV_PC_Future_Sub_Effective_Dt is null;

-- sp_columns 'new_customers_sample'

END;



-- Grant execute rights to the members of CITeam
grant execute on CITeam.Build_Forecast_New_Cust_Sample to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Build_Forecast_New_Cust_Sample(201601);

-- Select top 10000 * from CITeam.Build_Forecast_New_Cust_Sample(201601)


/*

dba.sp_drop_table 'CITeam','FORECAST_Base_Sample'
dba.sp_create_table 'CITeam','FORECAST_Base_Sample',
   'account_number varchar(20),'
|| 'end_date date,'
|| 'subs_week_and_year integer,'
|| 'subs_week_of_year tinyint,'
|| 'weekid bigint,'
|| 'DTV_Status_Code varchar(2),'
|| 'BB_Segment varchar(30) default null, '
|| 'prem_segment varchar(7),'
|| 'Simple_Segments varchar(13),'
|| 'country char(3),'
|| 'Affluence varchar(10),'
|| 'package_desc varchar(50),'
|| 'offer_length_DTV varchar(18),'
|| 'curr_offer_start_date_DTV date,'
|| 'Curr_Offer_end_Date_Intended_DTV date,'
|| 'Prev_offer_end_date_DTV date,'
|| 'Time_To_Offer_End_DTV char(28),'
|| 'curr_offer_start_date_BB date,'
|| 'Curr_Offer_end_Date_Intended_BB date,'
|| 'Prev_offer_end_date_BB date,'
|| 'Time_To_Offer_End_BB varchar(28),'
|| 'curr_offer_start_date_LR date,'
|| 'Curr_Offer_end_Date_Intended_LR date,'
|| 'Prev_offer_end_date_LR date,'
|| 'Time_To_Offer_End_LR varchar(28),'
|| 'DTV_BB_LR_offer_end_dt date,'
|| 'Time_To_Offer_End varchar(28),'
|| 'DTV_Tenure varchar(5),'
|| 'dtv_act_date date,'
|| 'Time_Since_Last_TA_call varchar(28),'
|| 'Last_TA_Call_dt date,'
|| 'Time_Since_Last_AB varchar(24),'
|| 'Last_AB_Dt date,'
|| 'Previous_AB_Count varchar(18),'
|| 'Previous_Abs smallint,'
|| 'DTV_PC_Future_Sub_Effective_Dt date default null, '
|| 'DTV_AB_Future_Sub_Effective_Dt date default null, '
|| 'CusCan_Forecast_Segment varchar(100),'
|| 'SysCan_Forecast_Segment varchar(100),'
|| 'DTV_Activation_Type varchar(100),'
|| 'dtv_latest_act_date date,'
|| 'dtv_first_act_date date,'
|| 'HD_segment varchar(70),'

|| 'rand_sample float, '
|| 'sample varchar(10) '





|| 'rand_action_Cuscan float,'
|| 'rand_action_Syscan decimal(20,18),'
|| 'rand_TA_Vol float,'
|| 'rand_WC_Vol float,'
|| 'rand_TA_Save_Vol float,'
|| 'rand_WC_Save_Vol float,'
|| 'rand_TA_DTV_Offer_Applied float,'
|| 'rand_NonTA_DTV_Offer_Applied float,'
|| 'rand_TA_DTV_PC_Vol float,'
|| 'rand_WC_DTV_PC_Vol float,'
|| 'rand_Other_DTV_PC_Vol float'

Select top 1000 * from CITeam.FORECAST_Base_Sample
*/


/*

Create variable Forecast_Start_Wk integer;
Set Forecast_Start_Wk = 201601;
Create variable base_date date;
create variable true_sample_rate float ;
Create Variable multiplier bigint ;
Create variable sample_pct float;

Set sample_pct = 0.25;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
Drop procedure if exists Forecast_Create_Opening_Base;

Create procedure Forecast_Create_Opening_Base(In Forecast_Start_Wk integer,In sample_pct float)
BEGIN

Declare base_date date;
Declare true_sample_rate float;
Declare multiplier  bigint;
Set multiplier = DATEPART(millisecond,now())+738;

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;


-- create the base week
Select * into #Sky_Calendar from Subs_Calendar(Forecast_Start_Wk/100-1,Forecast_Start_Wk/100);
set base_date = (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk);
-- select base_date;



-- 2.1 Base To Be Simulated
Set multiplier = DATEPART(millisecond,now())+1;

-- drop table if exists #base_sample;
Delete from FORECAST_Base_Sample;

Insert into FORECAST_Base_Sample
select
 account_number
,end_date
,cast(subs_week_and_year as integer)
,subs_week_of_year
,(subs_year-2010)*52+subs_week_of_year as weekid
,DTV_Status_Code
,Case when BB_Active > 0 then 'BB' else 'Non BB' end BB_Segment
,Case when sports > 0 and movies > 0 then 'TopTier'
      when sports > 0                then 'Sports'
      when movies > 0                then 'Movies'
      when DTV_Active = 1            then 'Basic'
end as prem_segment
,case
        when trim(simple_segment) in ('1 Secure')       then '1 Secure'
        when trim(simple_segment) in ('2 Start', '3 Stimulate','2 Stimulate')  then '2 Stimulate'
        when trim(simple_segment) in ('4 Support','3 Support')      then '3 Support'
        when trim(simple_segment) in ('5 Stabilise','4 Stabilise')    then '4 Stabilise'
        else 'Other/Unknown'
end as Simple_Segments
,Case when ROI > 0 then 'ROI' else 'UK' end as country
,Affluence_Bands as Affluence
,Case when trim(package_desc) in ('Variety','Kids,Mix,World') or package_desc is null then 'Variety'
      when package_desc is null then 'Original'
      when package_desc = 'Other' then 'Original'
      else package_desc
end package_desc
, case
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3  then 'Offer Length 3M'
       when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >3) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) then 'Offer Length 6M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >6) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) then 'Offer Length 9M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >9) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) then 'Offer Length 12M'
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12  then 'Offer Length 12M +'
        when Curr_Offer_end_Date_Intended_DTV is null then 'No Offer'
  end as offer_length_DTV


,curr_offer_start_date_DTV
,Curr_Offer_end_Date_Intended_DTV
,Prev_offer_end_date_DTV
,case
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_DTV between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_DTV between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End DTV'
  end as Time_To_Offer_End_DTV

,curr_offer_start_date_BB
,Curr_Offer_end_Date_Intended_BB
,Prev_offer_end_date_BB
,case
    when Curr_Offer_end_Date_intended_BB between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 43) and (end_date + 49) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 50) and (end_date + 56) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 57) and (end_date + 63) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 64) and (end_date + 70) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 71) and (end_date + 77) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 78) and (end_date + 84) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 85) and (end_date + 91) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB >= (end_date + 92)                          then 'Offer Ending in 7+ Wks'


    when Prev_offer_end_Date_BB between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_Date_BB between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 49) and (end_date - 43) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 56) and (end_date - 50) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 63) and (end_date - 57) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 70) and (end_date - 64) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 77) and (end_date - 71) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 84) and (end_date - 78) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 91) and (end_date - 85) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB <= (end_date - 92)                        then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB is null then 'Null'
    when Curr_Offer_end_Date_intended_BB is null then 'Null'
    else 'No Offer End BB'
end as Time_To_Offer_End_BB

,curr_offer_start_date_LR
,Curr_Offer_end_Date_Intended_LR
,Prev_offer_end_date_LR
  ,case
    when Curr_Offer_end_Date_Intended_LR between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_LR between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_LR between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer LR'
  end as Time_To_Offer_End_LR

,DTV_BB_LR_offer_end_dt
,case
    when DTV_BB_LR_offer_end_dt between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when DTV_BB_LR_offer_end_dt between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer'
  end as Time_To_Offer_End

--,Cast(null as varchar(100)) as Time_To_Offer_End_BB

,case
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
end as DTV_Tenure
,dtv_act_date
,Case when Last_TA_Call_dt is null then 'No Prev TA Calls'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7  = 0 then '0 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 1 then '01 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 2 and 5 then '02-05 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 6 and 35 then '06-35 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 36 and 41 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 42 and 46 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 47 then '47 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 48 and 52 then '48-52 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 53 and 60 then '53-60 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 > 60 then '61+ Wks since last TA Call'
--      when Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer) >= 52*2*7 then 'Last TA > 2 Yrs Ago'
     Else ''
--      (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 || ' Wks since last TA Call'
End Time_Since_Last_TA_call
,Last_TA_Call_dt
,Case when  Last_AB_Dt  is null then 'No Prev AB Calls'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 0 then '0 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 1 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 2 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 3 then '3 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 4 then '4 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 5 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 6 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 7 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 8 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 9 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 10 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 11 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 12 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 > 12 then '12+ Mnths since last AB'
     Else ''
end as  Time_Since_Last_AB
,Last_AB_Dt
,case
        when Previous_Abs = 0 then '0 Previous_Abs'
        when Previous_Abs = 1 then '1 Previous_Abs'
        when Previous_Abs = 2 then '2 Previous_Abs'
        when Previous_Abs = 3 then '3 Previous_Abs'
        when Previous_Abs = 4 then '4-7 Previous_Abs'
        when Previous_Abs = 5 then '4-7 Previous_Abs'
        when Previous_Abs = 6 then '4-7 Previous_Abs'
        when Previous_Abs = 7 then '4-7 Previous_Abs'
        when Previous_Abs = 8 then '8-10 Previous_Abs'
        when Previous_Abs = 9 then '8-10 Previous_Abs'
        when Previous_Abs = 10 then '8-10 Previous_Abs'
        when Previous_Abs = 11 then '11-15 Previous_Abs'
        when Previous_Abs = 12 then '11-15 Previous_Abs'
        when Previous_Abs = 13 then '11-15 Previous_Abs'
        when Previous_Abs = 14 then '11-15 Previous_Abs'
        when Previous_Abs = 15 then '11-15 Previous_Abs'
        when Previous_Abs >= 16 then '16 + Previous_Abs'
  else ''
end as Previous_AB_Count
,Previous_Abs
,Cast(null as date) as DTV_PC_Future_Sub_Effective_Dt
,Cast(null as date) as DTV_AB_Future_Sub_Effective_Dt
,Cast(null as varchar(100)) as CusCan_Forecast_Segment
,Cast(null as varchar(100)) as SysCan_Forecast_Segment
,Cast(null as varchar(100)) as DTV_Activation_Type
-- ,Case when dtv_latest_act_date between (end_date-6) and end_date and dtv_first_act_date < dtv_latest_act_date then 'Reinstate'
--       when dtv_latest_act_date between (end_date-6) and end_date and (dtv_first_act_date = dtv_latest_act_date) then 'Acquisition'
-- End as DTV_Activation_Type
,dtv_latest_act_date
,dtv_first_act_date
,HD_segment
,rand(number(*)*multiplier) as rand_sample
-- ,Cast(null as float) as sample_rnk_prctl
,cast(null as Varchar(10)) as sample

-- into #base_sample
from citeam.cust_fcast_weekly_base --_2015Q4
where end_date = base_date
      and dtv_active =1
      and dtv_act_date is not null
;


Update FORECAST_Base_Sample sample
Set DTV_PC_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
from FORECAST_Base_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.PC_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.PC_effective_to_dt >sample.end_date or MoR.PC_effective_to_dt is null)
where sample.DTV_Status_Code = 'PC';

Update FORECAST_Base_Sample sample
Set DTV_Status_Code = 'AC'
where DTV_Status_Code = 'PC' and DTV_PC_Future_Sub_Effective_Dt is null;

Update FORECAST_Base_Sample sample
Set DTV_AB_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
from FORECAST_Base_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.AB_effective_to_dt >sample.end_date or MoR.AB_effective_to_dt is null)
where sample.DTV_Status_Code = 'AB';

Update FORECAST_Base_Sample sample
Set DTV_Status_Code = 'AC'
where DTV_Status_Code = 'AB' and DTV_AB_Future_Sub_Effective_Dt is null;

Update FORECAST_Base_Sample
Set CusCan_Forecast_Segment = csl.cuscan_forecast_segment
from FORECAST_Base_Sample flt
     inner join
     CITeam.CusCan_Segment_Lookup csl
     on csl.dtv_tenure = flt.dtv_tenure
        and csl.Time_Since_Last_TA_Call = flt.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = flt.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = flt.Time_To_Offer_End_DTV
        and csl.package_desc = flt.package_desc;


Update FORECAST_Base_Sample flt
Set SysCan_Forecast_Segment = ssl.SysCan_Forecast_Segment
from FORECAST_Base_Sample flt
     inner join
     SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = flt.Time_Since_Last_AB
        and ssl.dtv_tenure = flt.dtv_tenure
        and ssl.Affluence = flt.Affluence
        and ssl.simple_segments = flt.simple_segments
        and ssl.Previous_AB_Count = flt.Previous_AB_Count;


--sample to speed up processing
update FORECAST_Base_Sample
set sample = case when rand_sample < sample_pct then 'A' else 'B' end;


-- Select subs_week_and_year, count(*) as n, count(distinct account_number) as d, n-d as dups from Forecast_Loop_Table group by subs_week_and_year;
-- set true_sample_rate = (select sum(case when sample='A' then cast(1 as float) else 0 end)/count(*) from #base_sample);

END;




-- Grant execute rights to the members of CITeam
grant execute on Forecast_Create_Opening_Base to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select top 1000 * from CITeam.Forecast_Create_Opening_Base(201601,0.25);

-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
Create variable Y2W01 integer;
Create variable Y3W52 integer;

Set Y2W01 = 201401;
Set Y3W52 = 201552;

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Create_Forecast_Loop_Table_2;

Create procedure CITeam.Forecast_Create_Forecast_Loop_Table_2(In Forecast_Start_Wk integer,In Forecast_End_Wk integer,IN true_sample_rate float)
SQL Security INVOKER
BEGIN

Declare multiplier bigint;
Set multiplier = DATEPART(millisecond,now())+1;

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

-- update the dates first
Drop table if exists #Loop_Sky_Calendar;
Select * into #Loop_Sky_Calendar from CITeam.Subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);

Update Forecast_Loop_Table a
Set subs_week_and_year = sc.subs_week_and_year,
      subs_week_of_year = sc.subs_week_of_year
from Forecast_Loop_Table a
     inner join
     #Loop_Sky_Calendar sc
     on sc.calendar_date = a.end_date + 7;

-- update the segments
Update Forecast_Loop_Table
Set CusCan_Forecast_Segment = Case when DTV_status_code in ('AB','PC') then DTV_status_code else csl.cuscan_forecast_segment end
from Forecast_Loop_Table flt
     inner join
     CITeam.CusCan_Segment_Lookup csl
     on csl.dtv_tenure = flt.dtv_tenure
        and csl.Time_Since_Last_TA_Call = flt.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = flt.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = flt.Time_To_Offer_End_DTV
        and csl.package_desc = flt.package_desc
        and csl.Country = flt.Country;


Update Forecast_Loop_Table flt
Set SysCan_Forecast_Segment = Case when DTV_status_code in ('AB','PC') then DTV_status_code else ssl.SysCan_Forecast_Segment end
from Forecast_Loop_Table flt
     inner join
     CITeam.SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = flt.Time_Since_Last_AB
        and ssl.dtv_tenure = flt.dtv_tenure
        and ssl.Affluence = flt.Affluence
        and ssl.simple_segments = flt.simple_segments
        and ssl.Previous_AB_Count = flt.Previous_AB_Count;


Update Forecast_Loop_Table
Set rand_action_Cuscan = rand(number(*)*multiplier+1)
   ,rand_action_Syscan = Cast(null as float)
   ,rand_TA_Vol = rand(number(*)*multiplier+2)
   ,rand_WC_Vol = rand(number(*)*multiplier+3)
   ,rand_TA_Save_Vol = rand(number(*)*multiplier+4)
   ,rand_WC_Save_Vol = rand(number(*)*multiplier+5)
   ,rand_TA_DTV_Offer_Applied = rand(number(*)*multiplier+6)
   ,rand_NonTA_DTV_Offer_Applied = rand(number(*)*multiplier+7)

   ,rand_TA_DTV_PC_Vol = rand(number(*)*multiplier+8)
   ,rand_WC_DTV_PC_Vol = rand(number(*)*multiplier+9)
   ,rand_Other_DTV_PC_Vol = rand(number(*)*multiplier+10)

   ,rand_Intrawk_DTV_PC = rand(number(*)*multiplier+2134)
   ,rand_DTV_PC_Duration = rand(number(*)*multiplier+234)
   ,rand_DTV_PC_Status_Change = rand(number(*)*multiplier+8323)
   ,rand_New_Off_Dur = rand(number(*)*multiplier+3043)

   ,rand_Intrawk_DTV_AB = rand(number(*)*multiplier+3383)

   ;




-- 3.02 Add Random Number and Segment Size for random event allocations later --

DROP TABLE if exists Pred_Rates;

select
 a.*
,b.Cuscan * true_sample_rate as CusCan_Churn
,c.Syscan * true_sample_rate as SysCan_Churn
into Pred_Rates
from Forecast_Loop_Table as a
/*-------------------------------------------------------------*/
-- this can removed once cuscan and syscan are forecasted - ----
     left join
        (select Cuscan_forecast_segment, sum(PO_Pipeline_Cancellations) + sum(Same_Day_Cancels) as Cuscan
          from citeam.DTV_Fcast_Weekly_Base
          where Subs_year = 2015
                and subs_week = (Select max(subs_week_of_year) from Forecast_Loop_Table)
          group by Cuscan_forecast_segment
          ) as b
        on a.Cuscan_forecast_segment = b.Cuscan_forecast_segment
     left join
     (select Syscan_forecast_segment, sum(SC_Gross_Terminations) as Syscan
          from citeam.DTV_Fcast_Weekly_Base
          where Subs_year = 2015
                and subs_week = (Select max(subs_week_of_year) from Forecast_Loop_Table)
          group by Syscan_forecast_segment
          ) as c
        on a.syscan_forecast_segment = c.syscan_forecast_segment
/*--------------------------------------------------------------*/
;



-- 3.04 Calculate Proportions for random event allocation and bring in event rates --
-- we have calculated above the distributions for TA_Calls and WC_Calls
--     we need to treat somehow the overlapping customers - that go in PC and AB
-- we calculate first the cuscan and then we exclude the cuscan in order to caluclate the syscan
-- we set syscan rank as null


Drop table if exists Forecast_Loop_Table_2;
select
 a.* --account_number
-- ,a.end_date
,count(*) over(partition by a.Cuscan_forecast_segment) as Cuscan_segment_count
,count(*) over(partition by a.Syscan_forecast_segment) as Syscan_segment_count
,cast(row_number() over(partition by a.Cuscan_Forecast_segment  order by rand_action_Cuscan) as float) as CusCan_Group_rank
,Cast(null as float) as SysCan_Group_rank
,CusCan_Group_rank/Cuscan_segment_count as pct_cuscan_count
,cast(null as float) as pct_syscan_count

,cast(0 as tinyint) as CusCan
,cast(0 as tinyint) as SysCan

-- cuscan
,Cast(0 as float) as pred_TA_Call_Cust_rate
,Cast(0 as float) as pred_TA_Call_Cust_YoY_Trend
,Cast(0 as float) as cum_TA_Call_Cust_rate

,Cast(0 as float) as pred_Web_Chat_TA_Cust_rate
,Cast(0 as float) as pred_Web_Chat_TA_Cust_YoY_Trend
,Cast(0 as float) as cum_Web_Chat_TA_Cust_rate
,Cast(0 as float) as cum_Web_Chat_TA_Cust_Trend_rate

-- ,Cast(0 as float) /*b.DTV_Offer_Applied_Rate*/ as pred_DTV_Offer_Applied_Rate

--syscan
,Cast(0 as float) as pred_DTV_AB_rate
,Cast(0 as float) as pred_DTV_YoY_Trend
,Cast(0 as float) as cum_DTV_AB_rate
,cast(0 as float) as cum_DTV_AB_Trend_rate

,Cast(0 as float) as pred_NonTA_DTV_Offer_Applied_rate
,Cast(0 as float) as pred_NonTA_DTV_Offer_Applied_YoY_Trend
,Cast(0 as float) as pred_TA_DTV_Offer_Applied_rate
,Cast(0 as float) as pred_TA_DTV_Offer_Applied_YoY_Trend

-- calculate actions and use cumulative to keep the relationship
-- ,cum_dtv_ab_rate + b.DTV_Offer_Applied_Rate as cum_DTV_Offer_Applied_Rate

,cast(0 as tinyint) as TA_Call_Cust
,cast(0 as tinyint) as TA_Call_Count
,cast(0 as tinyint) as TA_Saves
,cast(0 as tinyint) as TA_Non_Saves

,cast(0 as tinyint) as WC_Call_Cust
,cast(0 as tinyint) as WC_Call_Count
,cast(0 as tinyint) as WC_Saves
,cast(0 as tinyint) as WC_Non_Saves

,cast(0 as tinyint) as DTV_AB

,cast(0 as tinyint) as DTV_Offer_Applied
-- ,cast(0 as tinyint) as DTV_Offer_Applied


---------- TA -> PC
-- rates
,cast(0 as float) as pred_TA_DTV_PC_rate
,cast(0 as float) as pred_TA_Sky_Plus_Save_rate
,Cast(0 as float) as cum_TA_DTV_PC_rate

,cast(0 as float) as pred_WC_DTV_PC_rate
,cast(0 as float) as pred_WC_Sky_Plus_Save_rate
,Cast(0 as float) as cum_WC_DTV_PC_rate

,Cast(0 as float) as pred_Other_DTV_PC_rate

----------- PC
--- flag
,cast(0 as tinyint) as TA_DTV_PC
,cast(0 as tinyint) as WC_DTV_PC
,cast(0 as tinyint)  as TA_Sky_Plus_Save
,cast(0 as tinyint)  as WC_Sky_Plus_Save
,cast(0 as tinyint) as Other_DTV_PC

into Forecast_Loop_Table_2
From Pred_Rates  as a
;


END;


-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_Create_Forecast_Loop_Table_2 to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Create_Forecast_Loop_Table_2(201601,201652);


Create variable Counter integer; Set Counter = 1;
Create variable  multiplier  bigint;
Create variable  multiplier_2  bigint;

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Loop_Table_2_Actions;

Create procedure CITeam.Forecast_Loop_Table_2_Actions(IN Counter integer,IN Rate_Multiplier float)
SQL Security INVOKER
BEGIN
Declare multiplier  bigint;
Declare multiplier_2  bigint;
Set multiplier = DATEPART(millisecond,now())+1;
Set multiplier_2 = DATEPART(millisecond,now())+2;

--------------------------------------------------------------------------------------------------------------
-- Predicted rates -------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

--- cuscan rates ----
update Forecast_Loop_Table_2 as a
set  pred_TA_Call_Cust_rate      = Coalesce(b.pred_TA_Call_Cust_rate,0)
    ,pred_Web_Chat_TA_Cust_rate = Coalesce(b.pred_Web_Chat_TA_Cust_rate,0)
    ,pred_TA_DTV_Offer_Applied_rate = Coalesce(b.pred_TA_DTV_Offer_Applied_rate,0)
    ,pred_NonTA_DTV_Offer_Applied_rate = Coalesce(b.pred_NonTA_DTV_Offer_Applied_rate,0)
from Forecast_Loop_Table_2 as a
     left join
     cuscan_predicted_values as b
        on (a.subs_week_of_year       = b.subs_week or (a.subs_week_of_year = 53 and b.subs_week = 52))
        and replace(a.cuscan_forecast_segment,'_SkyQ','_Original') = b.cuscan_forecast_segment;

------ TA trend ------
update Forecast_Loop_Table_2 as a
set pred_TA_Call_Cust_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'TA_Call_Customers';

------ TA cum ----
update Forecast_Loop_Table_2 as a
Set cum_TA_Call_Cust_rate = pred_TA_Call_Cust_rate + pred_TA_Call_Cust_YoY_Trend;

------ WC cum-----
update Forecast_Loop_Table_2 as a
Set cum_Web_Chat_TA_Cust_rate  = cum_TA_Call_Cust_rate + pred_Web_Chat_TA_Cust_rate;

------ WC trend ------
update Forecast_Loop_Table_2 as a
set pred_Web_Chat_TA_Cust_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'Web_Chat_TA_Customers';
-------WC cum ------
update Forecast_Loop_Table_2 as a
Set cum_Web_Chat_TA_Cust_Trend_rate = cum_Web_Chat_TA_Cust_rate + pred_Web_Chat_TA_Cust_YoY_Trend ;

------ DTV Offer trend ------
update Forecast_Loop_Table_2 as a
set pred_TA_DTV_Offer_Applied_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'TA_DTV_Offer_Applied';

------ DTV Offer trend ------
update Forecast_Loop_Table_2 as a
set pred_NonTA_DTV_Offer_Applied_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on a.Cuscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'NonTA_DTV_Offer_Applied';


-- syscan rates -----
update Forecast_Loop_Table_2 as a
set pred_DTV_AB_rate  = Coalesce(c.pred_DTV_AB_rate,0)
from Forecast_Loop_Table_2 as a
     left join
     syscan_predicted_values as c
     on (a.subs_week_of_year       = c.subs_week or (a.subs_week_of_year = 53 and c.subs_week = 52))
        and a.syscan_forecast_segment              = c.syscan_forecast_segment;

------ AB_DTV trend ------
update Forecast_Loop_Table_2 as a
set pred_DTV_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     Fcast_Regr_Coeffs as d
        on a.syscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'DTV_AB';

---- AB cum ------
update Forecast_Loop_Table_2 as a
set cum_DTV_AB_rate  = pred_DTV_AB_rate  ;

update Forecast_Loop_Table_2 as a
set cum_DTV_AB_trend_rate = cum_DTV_AB_rate + pred_dtv_YoY_Trend;





--------------------------------------------------------------------------------------------------------------
-- TA/WC Volumes, Saves & Offers Applied  --------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------

-- 3.06 Allocate customers randomly based on rates --
update Forecast_Loop_Table_2 as a
set
 TA_Call_Cust   = case when rand_action_Cuscan /*pct_cuscan_count*/ <= cum_TA_Call_Cust_rate * Rate_Multiplier
                       then 1
                       else 0
                  end
,WC_Call_Cust   = case when rand_action_Cuscan /*pct_cuscan_count*/ > cum_TA_Call_Cust_rate * Rate_Multiplier
                            and rand_action_Cuscan /*pct_cuscan_count*/ <= cum_Web_Chat_TA_Cust_rate * Rate_Multiplier
                       then 1
                       else 0
                  end
;

-- TA
update Forecast_Loop_Table_2 as a
set  TA_Call_Count  = b.total_calls
    ,TA_Saves      = b.TA_Saved
    ,DTV_Offer_Applied = b.TA_DTV_Offer_Applied
--     ,TA_Non_Saves   = case when pct_cuscan_count <= pred_TA_Not_Saved_rate        then 1 else 0 end
from Forecast_Loop_Table_2 as a
     inner join
     TA_Call_Dist as b
     on a.CusCan_Forecast_Segment = b.CusCan_Forecast_Segment
        and a.rand_TA_Vol between b.TA_Lower_Pctl and b.TA_Upper_Pctl
where TA_Call_Cust > 0;


-- WebChat

update Forecast_Loop_Table_2 as a
set
     WC_Call_Count  = b.total_WCs
    ,WC_Saves       = b.Webchat_TA_Saved
-- select count(*)
from Forecast_Loop_Table_2 as a
     inner join
     WC_Dist as b
     on b.Subs_week = a.subs_week_of_year
        and Prev_WC_Vol_Percentile <= rand_WC_Vol
        and rand_WC_Vol <= WC_Vol_Percentile
        and Prev_WC_Save_Vol_Percentile <= rand_WC_Save_Vol
        and rand_WC_Save_Vol <= WC_Save_Vol_Percentile
where WC_Call_Cust > 0;























--------------------------------------------------------------------------------------------------------------
-- Pending Cancels -------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--- pred DTV_PC ----
Update Forecast_Loop_Table_2 as a
Set pred_TA_DTV_PC_rate = b.TA_DTV_PC_Conv_Rate
   ,pred_TA_Sky_Plus_Save_rate = b.TA_SkyPlus_Save_Rate
   ,pred_WC_DTV_PC_rate = b.WC_DTV_PC_Conv_Rate
   ,pred_WC_Sky_Plus_Save_rate = b.WC_SkyPlus_Save_Rate
   ,pred_Other_DTV_PC_rate = Coalesce(b.Other_DTV_PC_Conv_Rate,0)
from Forecast_Loop_Table_2 as a
     inner join
     TA_DTV_PC_Vol as b
     on a.cuscan_forecast_segment = b.cuscan_forecast_segment;


--- cum DTV_PC ----
update Forecast_Loop_Table_2 as a
set cum_TA_DTV_PC_rate = pred_TA_DTV_PC_rate + pred_TA_Sky_Plus_Save_rate
   ,cum_WC_DTV_PC_rate = pred_WC_DTV_PC_rate + pred_WC_Sky_Plus_Save_rate
  ;

---- DTV_PC counts
Drop table if exists #TA_PC_Pctl;
Select cuscan_forecast_segment,account_number,TA_Call_Cust,WC_Call_Cust,TA_Saves,WC_Saves,DTV_Offer_Applied,
Row_number() over(partition by cuscan_forecast_segment order by TA_Saves,DTV_Offer_Applied, WC_Saves) Segment_Rnk,
Count(*) over(partition by cuscan_forecast_segment) Total_Accs,
Cast(Segment_Rnk as float)/Total_Accs as CusCan_Segment_Pctl
into #TA_PC_Pctl
from Forecast_Loop_Table_2
where TA_Call_Cust > 0 and DTV_Status_Code not in ('AB','PC')
;

update Forecast_Loop_Table_2 as a
set
TA_DTV_PC = case when CusCan_Segment_Pctl <= pred_TA_DTV_PC_rate and a.TA_Call_Cust > 0
                then 1
                else 0
            end
,TA_Sky_Plus_Save = case when CusCan_Segment_Pctl > pred_TA_DTV_PC_rate
                              and CusCan_Segment_Pctl < cum_TA_DTV_PC_rate
                              and a.TA_Call_Cust >0
                then 1
                else 0
            end
from Forecast_Loop_Table_2 as a
     inner join
     #TA_PC_Pctl pc
     on pc.account_number = a.account_number;


Drop table if exists #TA_PC_Pctl;
Select cuscan_forecast_segment,account_number,TA_Call_Cust,WC_Call_Cust,TA_Saves,WC_Saves,DTV_Offer_Applied,
Row_number() over(partition by cuscan_forecast_segment order by TA_Saves,WC_Saves,DTV_Offer_Applied) Segment_Rnk,
Count(*) over(partition by cuscan_forecast_segment) Total_Accs,
Cast(Segment_Rnk as float)/Total_Accs as CusCan_Segment_Pctl
into #WC_PC_Pctl
from Forecast_Loop_Table_2
where WC_Call_Cust > 0 and DTV_Status_Code not in ('AB','PC')
;

update Forecast_Loop_Table_2 as a
set WC_DTV_PC = case when rand_WC_DTV_PC_Vol <= pred_WC_DTV_PC_rate and a.WC_Call_Cust > 0
                then 1
                else 0
            end
    ,WC_Sky_Plus_Save = case when rand_WC_DTV_PC_Vol > pred_WC_DTV_PC_rate
                              and rand_WC_DTV_PC_Vol < cum_WC_DTV_PC_rate
                              and a.WC_Call_Cust >0
                then 1
                else 0
            end
from Forecast_Loop_Table_2 as a
     inner join
     #WC_PC_Pctl pc
     on pc.account_number = a.account_number;


update Forecast_Loop_Table_2 as a
set Other_DTV_PC = 1
where a.TA_Call_Cust = 0 and a.WC_Call_Cust = 0 and a.DTV_Offer_Applied = 0
        and rand_Other_DTV_PC_Vol <= pred_Other_DTV_PC_rate
;




Update Forecast_Loop_Table_2
Set rand_action_Syscan = Case when TA_Call_Cust + WC_Call_Cust > 0 then 1
                              else null
                         end;

Update Forecast_Loop_Table_2
Set rand_action_Syscan = rand(number(*)*multiplier+4)
where rand_action_Syscan is null;

-- the low ranking customers will be the ones with no TA / WC
Drop table if exists #SysCan_Rank;
Select
account_number
,rand_action_Syscan
,sum(TA_Call_Cust+WC_Call_Cust) over(partition by Syscan_Forecast_segment) SysCan_Seg_CusCan_Actions
,count(*) over(partition by Syscan_Forecast_segment) Total_Cust_In_SysCan_Segment
,cast(rank() over(partition by Syscan_Forecast_segment  order by rand_action_Syscan) as float)                               as SysCan_Group_rank
,cast(rank() over(partition by Syscan_Forecast_segment order by rand_action_Syscan) as float)/cast(Syscan_segment_count as float) as pct_syscan_count
,case when TA_Call_Cust+WC_Call_Cust = 0
           and rand_action_Syscan <= pred_dtv_AB_rate*Total_Cust_In_SysCan_Segment/(Total_Cust_In_SysCan_Segment-SysCan_Seg_CusCan_Actions)
      then 1
      else 0
 end as DTV_AB
into #SysCan_Rank
from Forecast_Loop_Table_2;

commit;
create hg index idx_1 on #SysCan_Rank(account_number);

-- Update Forecast_Loop_Table_2
-- Set SysCan_Group_rank = b.SysCan_Group_rank,
--     pct_syscan_count = b.pct_syscan_count
-- from Forecast_Loop_Table_2 a
--      inner join
--      #SysCan_Rank b
--      on a.account_number = b.account_number;


update Forecast_Loop_Table_2 as a
set DTV_AB         = 1
from Forecast_Loop_Table_2 as a
     inner join
     #SysCan_Rank b
     on b.account_number = a.account_number
        and b.DTV_AB = 1
;












Update Forecast_Loop_Table_2 as a
Set DTV_Offer_Applied = 1
where --TA_Call_Cust = 1 and rand_TA_DTV_Offer_Applied <= pred_TA_DTV_Offer_Applied_rate + pred_TA_DTV_Offer_Applied_YoY_Trend
      --or
      TA_Call_Cust = 0 and rand_NonTA_DTV_Offer_Applied <= pred_NonTA_DTV_Offer_Applied_rate + pred_NonTA_DTV_Offer_Applied_YoY_Trend
      ;




Update Forecast_Loop_Table_2
Set DTV_Status_Code_EoW = AB.Next_Status_Code,
    DTV_Offer_Applied = AB.AB_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     IntraWk_AB_Pct AB
     on base.rand_Intrawk_DTV_AB between AB.IntaWk_AB_Lower_Pctl and AB.IntaWk_AB_Upper_Pctl
where DTV_AB > 0;

Update Forecast_Loop_Table_2 base
Set DTV_Status_Code_EoW = PC.Next_Status_Code,
    DTV_Offer_Applied = PC.PC_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     IntraWk_PC_Pct PC
     on base.rand_Intrawk_DTV_PC between PC.IntaWk_PC_Lower_Pctl and PC.IntaWk_PC_Upper_Pctl
where TA_DTV_PC > 0
      or
      WC_DTV_PC > 0
      or
      TA_Sky_Plus_Save > 0
      or
      WC_Sky_Plus_Save > 0
      or
      Other_DTV_PC > 0;



Update Forecast_Loop_Table_2 base
Set DTV_Status_Code_EoW = PC.Status_Code_EoW,
    DTV_Offer_Applied = PC.PC_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     PC_PL_Status_Change_Dist PC
     on base.rand_DTV_PC_Status_Change between PC.PC_Percentile_Lower_Bound and PC.PC_Percentile_Upper_Bound
        and Case when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=0 then 'Churn in next 1 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=1 then 'Churn in next 2 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=2 then 'Churn in next 3 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=3 then 'Churn in next 4 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=4 then 'Churn in next 5 wks'
                 when (cast(base.DTV_PC_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7>=5 then 'Churn in next 6+ wks'
--           when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>5 then '6+_Wks_To_Churn'
            end = PC.Wks_To_Intended_Churn
where DTV_Status_Code = 'PC'
        and
        (TA_DTV_PC = 0
      and
      WC_DTV_PC = 0
      and
      TA_Sky_Plus_Save = 0
      and
      WC_Sky_Plus_Save = 0
      and
      Other_DTV_PC = 0)
;


Update Forecast_Loop_Table_2 base
Set DTV_Status_Code_EoW = AB.Status_Code_EoW,
    DTV_Offer_Applied = AB.AB_ReAC_Offer_Applied
from Forecast_Loop_Table_2 base
     inner join
     AB_PL_Status_Change_Dist AB
     on base.rand_DTV_PC_Status_Change between AB.AB_Percentile_Lower_Bound and AB.AB_Percentile_Upper_Bound
        and Case when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=0 then 'Churn in next 1 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=1 then 'Churn in next 2 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=2 then 'Churn in next 3 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=3 then 'Churn in next 4 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=4 then 'Churn in next 4 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=5 then 'Churn in next 6 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=6 then 'Churn in next 7 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=7 then 'Churn in next 8 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7=8 then 'Churn in next 9 wks'
                 when (cast(base.DTV_AB_Future_Sub_Effective_Dt as integer) - cast(base.End_Date as integer))/7>=9 then 'Churn in next 10+ wks'
--           when (cast(PC_Future_Sub_Effective_Dt as integer) - cast(End_Date as integer))/7>5 then '6+_Wks_To_Churn'
            end = AB.Wks_To_Intended_Churn
where DTV_Status_Code = 'AB' and DTV_AB = 0;

Update Forecast_Loop_Table_2 base
Set CusCan = 1
where DTV_Status_Code_EoW = 'PO';

Update Forecast_Loop_Table_2 base
Set SysCan = 1
where DTV_Status_Code_EoW = 'SC';






END;


-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_Loop_Table_2_Actions to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Loop_Table_2_Actions(10);



-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2;

Create procedure CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2(In Forecast_Start_Wk integer,In Forecast_End_Wk integer,IN True_Sample_Rate float)
SQL Security INVOKER
BEGIN
Declare new_cust_end_date date;
Declare new_cust_subs_week_and_year integer;
Declare new_cust_subs_week_of_year integer;
Declare multiplier bigint;

Set multiplier = DATEPART(millisecond,now())+2631;

Select * into #Sky_Calendar from CITeam.subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);

---------------------------------------------------------------
--INSERT NEW CUSTOMERS
----------------------------------------------------------------
set new_cust_end_date           = (select max(end_date + 7) from Forecast_Loop_Table_2);
set new_cust_subs_week_and_year = (select max(subs_week_and_year) from #sky_calendar where calendar_date = new_cust_end_date);
set new_cust_subs_week_of_year  = (select max(subs_week_of_year) from #sky_calendar where calendar_date = new_cust_end_date);


-- select new_end_date, new_subs_week_and_year, new_subs_week_of_year;
drop table if exists #new_customers_last_2Yrs_2;
Select *,rand(number(*)*multiplier+163456) as rand_sample
into #new_customers_last_2Yrs_2
from CITeam.Forecast_New_Cust_Sample;

drop table if exists #new_customers_last_2Yrs_3;
Select *,row_number() over(order by rand_sample) Rand_Rnk
into #new_customers_last_2Yrs_3
from #new_customers_last_2Yrs_2;

Delete #new_customers_last_2Yrs_3 new_cust
from #new_customers_last_2Yrs_3 new_cust
     inner join
     Activation_Vols act
     on new_cust.Rand_Rnk > act.New_Customers * true_sample_rate
        and act.subs_week_of_year = new_cust_subs_week_of_year;


insert into Forecast_Loop_Table_2
(account_number,end_date,subs_week_and_year,subs_week_of_year,weekid,DTV_Status_Code_EoW
,DTV_PC_Future_Sub_Effective_Dt
,DTV_AB_Future_Sub_Effective_Dt
,BB_segment,prem_segment,Simple_Segments,country,Affluence,package_desc,offer_length_DTV,HD_Segment


,Curr_Offer_end_Date_Intended_DTV,curr_offer_start_date_DTV,Prev_offer_end_date_DTV,Time_To_Offer_End_DTV
,Curr_Offer_end_Date_Intended_BB,curr_offer_start_date_BB,Prev_offer_end_date_BB,Time_To_Offer_End_BB
,Curr_Offer_end_Date_Intended_LR,curr_offer_start_date_LR,Prev_offer_end_date_LR,Time_To_Offer_End_LR
, DTV_BB_LR_offer_end_dt, Time_To_Offer_End

,DTV_Tenure,dtv_act_date,Time_Since_Last_TA_call
,Last_TA_Call_dt,Time_Since_Last_AB,Last_AB_Dt,Previous_AB_Count,Previous_Abs
,CusCan_Forecast_Segment,SysCan_Forecast_Segment
,DTV_Activation_Type--,new_customer
,TA_Call_Cust,TA_Call_Count,TA_Saves,TA_Non_Saves
,WC_Call_Cust,WC_Call_Count,WC_Saves,WC_Non_Saves,DTV_AB
,Cuscan_segment_count,Syscan_segment_count
,CusCan,SysCan

,pred_TA_Call_Cust_rate
,pred_TA_Call_Cust_YoY_Trend
,cum_TA_Call_Cust_rate
,pred_Web_Chat_TA_Cust_rate
,pred_Web_Chat_TA_Cust_YoY_Trend
,cum_Web_Chat_TA_Cust_rate
,cum_Web_Chat_TA_Cust_Trend_rate
,pred_DTV_AB_rate
,pred_DTV_YoY_Trend
,cum_DTV_AB_rate
,cum_DTV_AB_Trend_rate
,pred_NonTA_DTV_Offer_Applied_rate
,pred_NonTA_DTV_Offer_Applied_YoY_Trend
,pred_TA_DTV_Offer_Applied_rate
,pred_TA_DTV_Offer_Applied_YoY_Trend
,DTV_Offer_Applied
,pred_TA_DTV_PC_rate
,pred_TA_Sky_Plus_Save_rate
,cum_TA_DTV_PC_rate
,pred_WC_DTV_PC_rate
,pred_WC_Sky_Plus_Save_rate
,cum_WC_DTV_PC_rate
,pred_Other_DTV_PC_rate
,TA_DTV_PC
,WC_DTV_PC
,TA_Sky_Plus_Save
,WC_Sky_Plus_Save
,Other_DTV_PC


)
select --top 10000
 replicate(char(65 + (counter - 1)%53), (counter-1)/53 + 1) || a.account_number as account_number
,new_cust_end_date - 7 as end_date
,new_cust_subs_week_and_year as subs_week_and_year
,new_cust_subs_week_of_year as subs_week_of_year
,(year(new_cust_end_date)-2010)*52+new_cust_subs_week_of_year as weekid
,DTV_Status_Code
,DTV_PC_Future_Sub_Effective_Dt + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as DTV_PC_Future_Sub_Effective_Dt
,DTV_AB_Future_Sub_Effective_Dt + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as DTV_AB_Future_Sub_Effective_Dt
,BB_Segment
,prem_segment
,Simple_Segments
,country
,Affluence
,package_desc
,offer_length_DTV
,HD_Segment

,Curr_Offer_end_Date_Intended_DTV + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as Curr_Offer_end_Date_Intended_DTV
,curr_offer_start_date_DTV        + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as curr_offer_start_date_DTV
,Prev_offer_end_date_DTV          + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as Prev_offer_end_date_DTV
,Time_To_Offer_End_DTV

,Curr_Offer_end_Date_Intended_BB + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as Curr_Offer_end_Date_Intended_BB
,curr_offer_start_date_BB        + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as curr_offer_start_date_BB
,Prev_offer_end_date_BB          + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as Prev_offer_end_date_BB
,Time_To_Offer_End_BB

,Curr_Offer_end_Date_Intended_LR + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as Curr_Offer_end_Date_Intended_LR
,curr_offer_start_date_LR        + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as curr_offer_start_date_LR
,Prev_offer_end_date_LR          + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as Prev_offer_end_date_LR
,Time_To_Offer_End_LR

,DTV_BB_LR_offer_end_dt          + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as DTV_BB_LR_offer_end_dt
,Time_To_Offer_End

,DTV_Tenure
,a.dtv_act_date + Cast(new_cust_end_date as integer)-Cast(a.end_date as integer) as dtv_act_date

,Time_Since_Last_TA_call
,Last_TA_Call_dt + Cast(new_cust_end_date as integer)-Cast(end_date as integer) as Last_TA_Call_dt
,Time_Since_Last_AB
,Last_AB_Dt + Cast(new_cust_end_date as integer)-Cast(end_date as integer) as Last_AB_Dt
,Previous_AB_Count
,Previous_Abs

-- segments
,CusCan_Forecast_Segment
,SysCan_Forecast_Segment
,DTV_Activation_Type
-- new customers
--,1 as new_customer -- is this the flag to use for inserting the new customers

-- actions
,Cast(0 as tinyint) as TA_Call_Cust
,Cast(0 as tinyint) as TA_Call_Count
,Cast(0 as tinyint) as TA_Saves
,Cast(0 as tinyint) as TA_Non_Saves
,Cast(0 as tinyint) as WC_Call_Cust
,Cast(0 as tinyint) as WC_Call_Count
,Cast(0 as tinyint) as WC_Saves
,Cast(0 as tinyint) as WC_Non_Saves
,Cast(0 as tinyint) as DTV_AB
,Cast(0 as tinyint) as Cuscan_segment_count
,Cast(0 as tinyint) as Syscan_segment_count
,Cast(0 as tinyint) as CusCan
,Cast(0 as tinyint) as SysCan

,Cast(0 as float) as pred_TA_Call_Cust_rate
,Cast(0 as float) as pred_TA_Call_Cust_YoY_Trend
,Cast(0 as float) as cum_TA_Call_Cust_rate
,Cast(0 as float) as pred_Web_Chat_TA_Cust_rate
,Cast(0 as float) as pred_Web_Chat_TA_Cust_YoY_Trend
,Cast(0 as float) as cum_Web_Chat_TA_Cust_rate
,Cast(0 as float) as cum_Web_Chat_TA_Cust_Trend_rate
,Cast(0 as float) as pred_DTV_AB_rate
,Cast(0 as float) as pred_DTV_YoY_Trend
,Cast(0 as float) as cum_DTV_AB_rate
,cast(0 as float) as cum_DTV_AB_Trend_rate
,Cast(0 as float) as pred_NonTA_DTV_Offer_Applied_rate
,Cast(0 as float) as pred_NonTA_DTV_Offer_Applied_YoY_Trend
,Cast(0 as float) as pred_TA_DTV_Offer_Applied_rate
,Cast(0 as float) as pred_TA_DTV_Offer_Applied_YoY_Trend
,cast(0 as tinyint) as DTV_Offer_Applied
,cast(0 as float) as pred_TA_DTV_PC_rate
,cast(0 as float) as pred_TA_Sky_Plus_Save_rate
,Cast(0 as float) as cum_TA_DTV_PC_rate
,cast(0 as float) as pred_WC_DTV_PC_rate
,cast(0 as float) as pred_WC_Sky_Plus_Save_rate
,Cast(0 as float) as cum_WC_DTV_PC_rate
,Cast(0 as float) as pred_Other_DTV_PC_rate
,cast(0 as tinyint) as TA_DTV_PC
,cast(0 as tinyint) as WC_DTV_PC
,cast(0 as tinyint)  as TA_Sky_Plus_Save
,cast(0 as tinyint)  as WC_Sky_Plus_Save
,cast(0 as tinyint) as Other_DTV_PC

from #new_customers_last_2Yrs_3 as a
;

END;


-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2 to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Insert_New_Custs_Into_Loop_Table_2(201601,201652);

-- First you need to impersonate CITeam
Setuser CITeam;

-- Drop procedure if exists CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk;

Create procedure CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk()
SQL Security INVOKER
BEGIN

--------------------------------------------------------------------------
-- Update table for start of next loop -----------------------------------
--------------------------------------------------------------------------
Update Forecast_Loop_Table_2 base
Set DTV_PC_Future_Sub_Effective_Dt  = Cast(base.end_date + dur.Days_To_churn as date)
from Forecast_Loop_Table_2 base
     inner join
     DTV_PC_Duration_Dist dur
     on rand_DTV_PC_Duration between dur.PC_Days_Lower_Prcntl and dur.PC_Days_Upper_Prcntl
where (TA_DTV_PC > 0
      or
      WC_DTV_PC > 0
      or
      TA_Sky_Plus_Save > 0
      or
      WC_Sky_Plus_Save > 0
      or
      Other_DTV_PC > 0
      or base.DTV_Status_Code = 'AB')
      and base.DTV_Status_Code_EoW = 'PC';

Update Forecast_Loop_Table_2 base
Set DTV_AB_Future_Sub_Effective_Dt  = Cast(base.end_date + 50 as date)
from Forecast_Loop_Table_2 base
where DTV_AB > 0
      and base.DTV_Status_Code_EoW = 'AB';


Update Forecast_Loop_Table_2 base
Set  curr_offer_start_date_DTV = end_date + 3
    ,Curr_Offer_end_Date_Intended_DTV = dateadd(month,Total_Offer_Duration_Mth,end_date + 3) -- Default 10m offer
from Forecast_Loop_Table_2 base
     inner join
     Offer_Applied_Dur_Dist offer
     on base.rand_New_Off_Dur between offer.Dur_Pctl_Lower_Bound and offer.Dur_Pctl_Upper_Bound
        and Offer_Segment = 'TA'
where DTV_Offer_Applied = 1 and TA_Call_Cust > 0;

Update Forecast_Loop_Table_2 base
Set  curr_offer_start_date_DTV = end_date + 3
    ,Curr_Offer_end_Date_Intended_DTV = dateadd(month,Total_Offer_Duration_Mth,end_date + 3) -- Default 10m offer
from Forecast_Loop_Table_2 base
     inner join
     Offer_Applied_Dur_Dist offer
     on base.rand_New_Off_Dur between offer.Dur_Pctl_Lower_Bound and offer.Dur_Pctl_Upper_Bound
        and Offer_Segment = 'Other'
where DTV_Offer_Applied = 1 and TA_Call_Cust = 0;

Update Forecast_Loop_Table_2 base
Set  curr_offer_start_date_DTV = end_date + 3
    ,Curr_Offer_end_Date_Intended_DTV = dateadd(month,Total_Offer_Duration_Mth,end_date + 3) -- Default 10m offer
from Forecast_Loop_Table_2 base
     inner join
     Offer_Applied_Dur_Dist offer
     on base.rand_New_Off_Dur between offer.Dur_Pctl_Lower_Bound and offer.Dur_Pctl_Upper_Bound
        and Offer_Segment = 'Reactivations'
where DTV_Offer_Applied = 1
        and (
            (DTV_Status_Code = 'PC' and DTV_Status_Code_EoW = 'AC')
            or
            ((TA_DTV_PC > 0 or WC_DTV_PC > 0 or TA_Sky_Plus_Save > 0 or WC_Sky_Plus_Save > 0 or Other_DTV_PC > 0) and DTV_Status_Code_EoW = 'AC')
            )
        ;


Update Forecast_Loop_Table_2
Set DTV_Status_Code = Coalesce(DTV_Status_Code_EoW,DTV_Status_Code);

Update Forecast_Loop_Table_2 base
Set DTV_PC_Future_Sub_Effective_Dt  = null
where base.DTV_Status_Code != 'PC';

Update Forecast_Loop_Table_2 base
Set DTV_AB_Future_Sub_Effective_Dt  = null
where base.DTV_Status_Code != 'AB';





Update Forecast_Loop_Table_2
set end_date = end_date + 7;

Update Forecast_Loop_Table_2
Set Prev_offer_end_date_DTV = Curr_Offer_end_Date_Intended_DTV
where Curr_Offer_end_Date_Intended_DTV <= end_date;

Update Forecast_Loop_Table_2
Set Curr_Offer_end_Date_Intended_DTV = null
where Curr_Offer_end_Date_Intended_DTV <= end_date;

Update Forecast_Loop_Table_2
Set Prev_offer_end_date_DTV = null
where Prev_offer_end_date_DTV < (end_date) - 53*7;

Update Forecast_Loop_Table_2
Set DTV_BB_LR_Offer_End_Dt =
Case when Coalesce(Curr_Offer_end_Date_intended_DTV,Prev_offer_end_date_DTV) is not null
                and ABS(Coalesce(Curr_Offer_end_Date_intended_DTV,Prev_offer_end_date_DTV) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_BB,Prev_offer_end_date_BB,Cast('9999-09-09' as date)) - End_Date)
                and ABS(Coalesce(Curr_Offer_end_Date_intended_DTV,Prev_offer_end_date_DTV) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_LR,Prev_offer_end_date_LR,Cast('9999-09-09' as date)) - End_Date)
      then Coalesce(Curr_Offer_end_Date_intended_DTV,Prev_offer_end_date_DTV)  -- DTV Offer End Dt
      when Coalesce(Curr_Offer_end_Date_intended_BB,Prev_offer_end_date_BB) is not null
                and ABS(Coalesce(Curr_Offer_end_Date_intended_BB,Prev_offer_end_date_BB) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_DTV,Prev_offer_end_date_DTV,Cast('9999-09-09' as date)) - End_Date)
                and ABS(Coalesce(Curr_Offer_end_Date_intended_BB,Prev_offer_end_date_BB) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_LR,Prev_offer_end_date_LR,Cast('9999-09-09' as date)) - End_Date)
      then Coalesce(Curr_Offer_end_Date_intended_BB,Prev_offer_end_date_BB)  -- BB Offer End Dt
      when Coalesce(Curr_Offer_end_Date_intended_LR,Prev_offer_end_date_LR) is not null
                and ABS(Coalesce(Curr_Offer_end_Date_intended_LR,Prev_offer_end_date_LR) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_DTV,Prev_offer_end_date_DTV,Cast('9999-09-09' as date)) - End_Date)
                and ABS(Coalesce(Curr_Offer_end_Date_intended_LR,Prev_offer_end_date_LR) - End_Date) <= ABS(Coalesce(Curr_Offer_end_Date_intended_BB,Prev_offer_end_date_BB,Cast('9999-09-09' as date)) - End_Date)
      then Coalesce(Curr_Offer_end_Date_intended_LR,Prev_offer_end_date_LR)  -- LR Offer End Dt
End;


Update Forecast_Loop_Table_2
Set  Last_TA_Call_dt = Case when TA_Call_Cust >0 then end_date - 3 else Last_TA_Call_dt end
    ,Last_AB_Dt = Case when DTV_AB > 0 then end_date - 3 else Last_AB_Dt end;

Update Forecast_Loop_Table_2
set
--   end_date = end_date + 7
--  ,
 weekid = weekid+1
--  ,dtv_status_code
, offer_length_DTV = case
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3  then 'Offer Length 3M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >3) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) then 'Offer Length 6M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >6) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) then 'Offer Length 9M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >9) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) then 'Offer Length 12M'
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12  then 'Offer Length 12M +'
        when Curr_Offer_end_Date_Intended_DTV is null then 'No Offer'
  end
-- ,Curr_Offer_end_Date_Intended_DTV
-- ,curr_offer_start_date_DTV
,Time_To_Offer_End_DTV = case
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_DTV between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_DTV between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End DTV'
  end
--   ,Prev_offer_end_date_DTV
,Time_To_Offer_End_BB = case
    when Curr_Offer_end_Date_intended_BB between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 43) and (end_date + 49) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 50) and (end_date + 56) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 57) and (end_date + 63) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 64) and (end_date + 70) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 71) and (end_date + 77) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 78) and (end_date + 84) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 85) and (end_date + 91) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB >= (end_date + 92)                          then 'Offer Ending in 7+ Wks'


    when Prev_offer_end_Date_BB between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_Date_BB between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 49) and (end_date - 43) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 56) and (end_date - 50) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 63) and (end_date - 57) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 70) and (end_date - 64) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 77) and (end_date - 71) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 84) and (end_date - 78) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 91) and (end_date - 85) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB <= (end_date - 92)                        then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB is null then 'Null'
    when Curr_Offer_end_Date_intended_BB is null then 'Null'
    else 'No Offer End BB'
end
,Time_To_Offer_End_LR = case
    when Curr_Offer_end_Date_Intended_LR between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_LR between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_LR between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End LR'
  end
,Time_To_Offer_End = case
    when DTV_BB_LR_offer_end_dt between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when DTV_BB_LR_offer_end_dt between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer'
end
-- ,Curr_Offer_end_Date_intended_BB
-- ,Prev_offer_end_Date_BB
,DTV_Tenure = case
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
end
-- ,dtv_act_date

,Previous_Abs = Previous_Abs + Case when DTV_AB > 0 then 1 else 0 end

,DTV_Activation_Type = null;

Update Forecast_Loop_Table_2
Set  Time_Since_Last_TA_call = Case when Last_TA_Call_dt is null then 'No Prev TA Calls'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7  = 0 then '0 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 1 then '01 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 2 and 5 then '02-05 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 6 and 35 then '06-35 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 36 and 41 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 42 and 46 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 47 then '47 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 48 and 52 then '48-52 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 53 and 60 then '53-60 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 > 60 then '61+ Wks since last TA Call'
--      when Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer) >= 52*2*7 then 'Last TA > 2 Yrs Ago'
     Else ''
--      (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 || ' Wks since last TA Call'
End
    ,Time_Since_Last_AB = Case when  Last_AB_Dt  is null then 'No Prev AB Calls'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 0 then '0 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 1 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 2 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 3 then '3 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 4 then '4 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 5 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 6 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 7 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 8 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 9 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 10 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 11 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 12 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 > 12 then '12+ Mnths since last AB'
     Else ''
end
,Previous_AB_Count = case
        when Previous_Abs = 0 then '0 Previous_Abs'
        when Previous_Abs = 1 then '1 Previous_Abs'
        when Previous_Abs = 2 then '2 Previous_Abs'
        when Previous_Abs = 3 then '3 Previous_Abs'
        when Previous_Abs = 4 then '4-7 Previous_Abs'
        when Previous_Abs = 5 then '4-7 Previous_Abs'
        when Previous_Abs = 6 then '4-7 Previous_Abs'
        when Previous_Abs = 7 then '4-7 Previous_Abs'
        when Previous_Abs = 8 then '8-10 Previous_Abs'
        when Previous_Abs = 9 then '8-10 Previous_Abs'
        when Previous_Abs = 10 then '8-10 Previous_Abs'
        when Previous_Abs = 11 then '11-15 Previous_Abs'
        when Previous_Abs = 12 then '11-15 Previous_Abs'
        when Previous_Abs = 13 then '11-15 Previous_Abs'
        when Previous_Abs = 14 then '11-15 Previous_Abs'
        when Previous_Abs = 15 then '11-15 Previous_Abs'
        when Previous_Abs >= 16 then '16 + Previous_Abs'
  else ''
end;



END;


-- Grant execute rights to the members of CITeam
grant execute on CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Forecast_Loop_Table_2_Update_For_Nxt_Wk(10);


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


/*

Select top 1000 account_number,end_date,27 as payment_due_day_of_month,DTV_AB,
Case when day(end_date) < payment_due_day_of_month
          then Cast('' || year(dateadd(month,-1,end_date)) || '-' || month(dateadd(month,-1,end_date)) || '-' || payment_due_day_of_month as date)
     when day(end_date) >= payment_due_day_of_month
          then Cast('' || year(end_date) || '-' || month(end_date) || '-' || payment_due_day_of_month as date)
end as Last_Payment_Dt,
Cast(end_date-Case when day(end_date) < payment_due_day_of_month
          then Cast('' || year(dateadd(month,-1,end_date)) || '-' || month(dateadd(month,-1,end_date)) || '-' || payment_due_day_of_month as date)
     when day(end_date) >= payment_due_day_of_month
          then Cast('' || year(end_date) || '-' || month(end_date) || '-' || payment_due_day_of_month as date)
end as integer) as Days_Since_Last_Payment_Dt
from citeam.cust_fcast_weekly_base
where end_date >= '2016-06-30' --payment_due_day_of_month is not null

Select
Cast(end_date-Case when day(end_date) < payment_due_day_of_month
          then Cast('' || year(dateadd(month,-1,end_date)) || '-' || month(dateadd(month,-1,end_date)) || '-' || payment_due_day_of_month as date)
     when day(end_date) >= payment_due_day_of_month
          then Cast('' || year(end_date) || '-' || month(end_date) || '-' || payment_due_day_of_month as date)
end as integer) as Days_Since_Last_Payment_Dt,
count(*) Customers,
sum(DTV_AB) ABs,
Cast(ABs as float)/Customers as DTV_AB_Rate
from citeam.cust_fcast_weekly_base
where end_date >= '2016-06-30' --payment_due_day_of_month is not null
        and dtv_active  > 0
group by Days_Since_Last_Payment_Dt;

/*
dba.sp_drop_table 'CITeam','DTV_FCAST_WEEKLY_BASE'
dba.sp_create_table 'CITeam','DTV_FCAST_WEEKLY_BASE',
   'Downgrade_View varchar(20) default null, '
|| 'Subs_Year smallint default null,'
|| 'Subs_Quarter tinyint default null,'
|| 'Subs_Week smallint default null,'
|| 'Subs_Week_And_Year integer default null,'
|| 'End_Date date default null, '
|| 'Account_Number varchar(20) default null, '
|| 'DTV_Tenure varchar(20) default null, '
|| 'Package_Desc varchar(50) default null, '
|| 'Prem_Segment varchar(30), '
|| 'HD_segment varchar(50) default null, '
|| 'Sports_Tenure varchar(20) default null, '
|| 'Movies_Tenure varchar(20) default null, '
|| 'Simple_Segment varchar(50) default null, '

|| 'Time_Since_Last_TA_call varchar(30) default null, '
|| 'Time_Since_Last_AB varchar(30) default null, '
|| 'Previous_ABs varchar(20) default null, '
|| 'Affluence varchar(10) default null, '
|| 'Country varchar(3) default null, '
|| 'Offer_Length_DTV varchar(30) default null, '
|| 'Time_To_Offer_End_DTV varchar(30) default null, '
|| 'Time_To_Offer_End_BB varchar(30) default null, '
|| 'Time_To_Offer_End_LR varchar(30) default null, '
|| 'Time_To_Offer_End varchar(30) default null, '
|| 'DTV_Status_Code     varchar(2) default null, '
|| 'New_Customer bit default 0, '
|| 'CusCan_Forecast_Segment varchar(50) default null, '
|| 'SysCan_Forecast_Segment varchar(50) default null, '
|| 'Placeholder_1 varchar(50) default null, '
|| 'Placeholder_2 varchar(50) default null, '


|| 'DTV_Active float default 0, '
|| 'BB_Active float default 0, '
|| 'DTV_AB float default 0, '
|| 'TA_Event_Count float default 0, '
|| 'Unique_TA_Caller float default 0, '
|| 'TA_Save_Count float default 0, '
|| 'TA_Non_Save_Count float default 0, '
|| 'Offer_Applied_DTV float default 0, '
|| 'Web_Chat_TA_Cnt float default 0, '
|| 'Web_Chat_TA_Customers float default 0, '
|| 'Web_Chat_TA_Not_Saved float default 0, '
|| 'Web_Chat_TA_Saved float default 0, '

|| 'TA_DTV_Offer_Applied float default 0, '
|| 'TA_DTV_PC float default 0, '
|| 'WC_DTV_PC float default 0, '
|| 'Accessibility_DTV_PC float default 0, '
|| 'Min_Term_PC float default 0, '
|| 'Other_PC float default 0, '
|| 'DTV_PC float default 0, '

|| 'PO_Pipeline_Cancellations float default 0, '
|| 'Same_Day_Cancels float default 0, '
|| 'SC_Gross_Terminations float default 0, '
|| 'TA_Sky_Plus_Save float default 0,'
|| 'WC_Sky_Plus_Save float default 0,'
|| 'DTV_Status_Code_EoW varchar(2) default null '




create hg index idx on CITeam.DTV_FCAST_WEEKLY_BASE(Account_Number);
create lf index idx_1 on CITeam.DTV_FCAST_WEEKLY_BASE(Downgrade_View);
create lf index idx_2 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Year);
create lf index idx_3 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Quarter);
create lf index idx_4 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Week);
create lf index idx_5 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Week_And_Year);

create lf index idx_6 on CITeam.DTV_FCAST_WEEKLY_BASE(DTV_Tenure);
create lf index idx_7 on CITeam.DTV_FCAST_WEEKLY_BASE(Package_Desc);
create lf index idx_8 on CITeam.DTV_FCAST_WEEKLY_BASE(Prem_Segment);
create lf index idx_9 on CITeam.DTV_FCAST_WEEKLY_BASE(HD_segment);
create lf index idx_10 on CITeam.DTV_FCAST_WEEKLY_BASE(Simple_Segment);
create lf index idx_11 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_Since_Last_TA_call);
create lf index idx_12 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_Since_Last_AB);

create lf index idx_13 on CITeam.DTV_FCAST_WEEKLY_BASE(Country);
create lf index idx_14 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End_DTV);
create lf index idx_15 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End_BB);
create lf index idx_16 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End_LR);
create lf index idx_17 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End);
create lf index idx_18 on CITeam.DTV_FCAST_WEEKLY_BASE(Previous_ABs);
create lf index idx_19 on CITeam.DTV_FCAST_WEEKLY_BASE(CusCan_Forecast_Segment);
create lf index idx_20 on CITeam.DTV_FCAST_WEEKLY_BASE(SysCan_Forecast_Segment);

create lf index idx_21 on CITeam.DTV_FCAST_WEEKLY_BASE(End_Date);
create lf index idx_22 on CITeam.DTV_FCAST_WEEKLY_BASE(Placeholder_1);
create lf index idx_23 on CITeam.DTV_FCAST_WEEKLY_BASE(Placeholder_2);

create lf index idx_24 on CITeam.DTV_FCAST_WEEKLY_BASE(_24MF_BB_Offer);
create lf index idx_25 on CITeam.DTV_FCAST_WEEKLY_BASE(Had_Offer_In_Last_Year);

Select top 100 * from CITeam.DTV_FCAST_WEEKLY_BASE
*/
/*
Alter table CITeam.DTV_FCAST_WEEKLY_BASE
Add (DTV_Status_Code_EoW varchar(2) default null);

Had_Offer_In_Last_Year varchar(10) default 'No'
);

Update CITeam.DTV_FCAST_WEEKLY_BASE
Set _24MF_BB_Offer = 'Not On Offer';

Create variable @loop_dt date;
Set @Loop_Dt = (Select max(end_date) from citeam.cust_fcast_weekly_base);
-- Select @Loop_Dt

While @Loop_Dt >= (Select min(end_date) from citeam.cust_fcast_weekly_base) Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = Case when source.curr_offer_end_date_Intended_BB >= source.end_date then 'On 24MF BB Offer'
                          when source.Prev_offer_end_date_BB > source.end_date - 7*7 then '24MF BB Ended Lst 7W'
                     end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and source.Offer_ID_BB in
(
81069,
81082,
81083,
81084,
81248,
81249,
81250,
81266,
81277,
81286,
81338,
81471,
81692,
82732,
82793
)
    and base.downgrade_view = 'Actuals';

-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt - 7;

End Loop




Set @Loop_Dt = (Select max(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16');
-- Select @Loop_Dt


While @Loop_Dt >= (Select min(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16') Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = 'Not On Offer'
where end_date = @Loop_dt;

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = Case when source.curr_offer_end_date_Intended_BB >= base.end_date then 'On 24MF BB Offer'
                          when source.Prev_offer_end_date_BB > base.end_date - 7*7 then '24MF BB Ended Lst 7W'
                          else 'Not On Offer'
                     end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = '2016-06-30'
where   base.end_date = @Loop_dt
    and source.end_date = '2016-06-30'
    and source.Offer_ID_BB in
(
81069,
81082,
81083,
81084,
81248,
81249,
81250,
81266,
81277,
81286,
81338,
81471,
81692,
82732,
82793
)
    and base.downgrade_view = 'LV 201601 V16';

-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt - 7;

End Loop;

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = 'Not On Offer'
where _24MF_BB_Offer is NULL;


81069   Broadband Unlimited Free for 24 Months With Sky Sports - Existing UK Customers
81082   Broadband Unlimited Free for 24 Months - Existing UK Customers
81083   Broadband Unlimited Free for 24 Months With Sports - Existing UK Customers
81084   Broadband Unlimited Free for 24 Months with Sports and Movies - Existing UK Customers
81248   Broadband Unlimited Free for 24 Months with Sports and Line Rental (18M New Min Term)
81249   Broadband Unlimited Free for 24 Months with Sports and Line Rental - UK New Customers
81250   Broadband Unlimited Free for 24 Months with Sports and Line Rental (18M New Min Term)
-- 81251        Broadband Connect at 10GBP for 24 Months with Sports and Line Rental (12M New Min Term)
81266   Broadband Unlimited Free for 24 Months with Sports and Line Rental - UK Existing Customers
-- 81267        Broadband Connect at 10GBP for 24 Months with Sports and Line Rental - UK New Customers
81277   Broadband Unlimited Free for 24 Months (12M New Min Term)
81286   Broadband Unlimited free for 24 Months with Sports (18M New Min Term)
-- 81315        Broadband Connect at 10GBP for 24 Months with Sports and Line Rental - UK Existing Customers
81338   Sky Broadband Unlimited Free for 24 Months with Sports and Line Rental - Existing UK Customers
-- 81340        Sky Broadband Connect at 10 GBP for 24 Months with Sports and Line Rental - Existing UK Customers
-- 81434        Broadband Connect at 10 GBP for 24 Months with Sky Sports - Existing UK Customers
81471   Broadband Unlimited Free for 24 Months with Sports (18M New Min Term)
81692   Broadband Unlimited Free for 24 Months with Sky Sports - Existing UK Customers
82732   Broadband Unlimited Free for 24 Months with Line Rental and Sports (18M New Min Term)
82793   Broadband Unlimited Free for 24 Months with Line Rental and Sports (24M New Min Term)










Set @Loop_Dt = (Select max(end_date) from citeam.cust_fcast_weekly_base);

While @Loop_Dt >= (Select min(end_date) from citeam.cust_fcast_weekly_base) Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'No'
where   base.end_date = @Loop_dt

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'Yes'
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
        and (source.Prev_offer_end_date_DTV >= source.end_date - 365
            or source.Prev_offer_end_date_BB >= source.end_date - 365
            or source.Prev_offer_end_date_LR >= source.end_date - 365)
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and base.downgrade_view = 'Actuals';

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'On Offer'
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
        and (source.curr_offer_end_date_Intended_DTV is not null
            or source.curr_offer_end_date_Intended_BB is not null
            or source.curr_offer_end_date_Intended_LR is not null)
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and base.downgrade_view = 'Actuals';


-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt - 7;

End Loop



Set @Loop_Dt = (Select min(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16');
-- Select @Loop_Dt


While @Loop_Dt <= (Select max(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16')
Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'No'
where   base.end_date = @Loop_dt
        and base.downgrade_view = 'LV 201601 V16';

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year =
        Case when source.curr_offer_end_date_Intended_DTV > base.end_date
                    or source.curr_offer_end_date_Intended_BB > base.end_date
                    or source.curr_offer_end_date_Intended_LR > base.end_date
              then 'On Offer'
             when source.Prev_offer_end_date_DTV >= base.end_date - 365
                    or source.Prev_offer_end_date_BB >= base.end_date - 365
                    or source.Prev_offer_end_date_LR >= base.end_date - 365
              then 'Yes'
              else 'No'
        end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     left join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = '2016-06-30'
where   base.end_date = @Loop_dt
    and source.end_date = '2016-06-30'
    and base.downgrade_view = 'LV 201601 V16';

commit;

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year =
        Case when source.curr_offer_end_date_Intended_DTV > source.end_date and base.Had_Offer_In_Last_Year in ('Yes','No') then 'On Offer'
             when source.Prev_offer_end_date_DTV >= source.end_date - 365 and base.Had_Offer_In_Last_Year in ('No') then 'Yes'
             else base.Had_Offer_In_Last_Year
        end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     menziesm.FORECAST_Looped_Sim_Output_Platform_201601_V15 source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and base.downgrade_view = 'LV 201601 V16'
    ;


-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt + 7;

End Loop


Select subs_week_and_year,count(*)*4 Customers
from menziesm.FORECAST_Looped_Sim_Output_Platform_201601_V15 source
where curr_offer_end_date_Intended_DTV > end_date
group by subs_week_and_year


Select top 1000 * from citeam.dtv_fcast_weekly_base where end_date = '2016-11-24' and downgrade_view = 'Actuals'

Select Downgrade_View,Subs_Year,Subs_Quarter,subs_week_and_year,Prem_Segment,Case when BB_Active > 0 then 1 else 0 end as BB_Active,Had_Offer_In_Last_Year,_24MF_BB_Offer,Cast(null as varchar(40)) as Karl_Segment
,sum(DTV_Active) --count(*)* Case when Downgrade_View = 'Actuals' then 1 else 4 end
    as Customers
,sum(TA_Event_Count)TA_Events
into --drop table
Karl_TA_Events
from citeam.dtv_fcast_weekly_base
-- where subs_week_and_year between 201601 and 201652
group by Downgrade_View,Subs_Year,Subs_Quarter,subs_week_and_year,Prem_Segment,BB_Active,Had_Offer_In_Last_Year,_24MF_BB_Offer;


Update Karl_TA_Events
Set _24MF_BB_Offer = 'Not On Offer'
where _24MF_BB_Offer is NULL;


Update Karl_TA_Events
Set Karl_Segment = Case when Had_Offer_In_Last_Year = 'On Offer' and _24MF_BB_Offer in ('On 24MF BB Offer','24MF BB Ended Lst 7W')   then '24 MF Offer'
                        when Had_Offer_In_Last_Year = 'On Offer'                                                                     then 'On Non 24MF BB Offer'
                        when Had_Offer_In_Last_Year = 'Yes'                                                                           then 'Had Offer in Last Yr'
                        when Had_Offer_In_Last_Year = 'No' and Prem_Segment in ('Sports','TopTier') and BB_Active = 1                then 'Sports & BB'
                        when Had_Offer_In_Last_Year = 'No' and Prem_Segment in ('Basic') and BB_Active = 1                           then 'Basic Trple Play'
                        else 'Other - Not on offer'
                    end;


Select *
-- ,Count(distinct Subs_week_and_year) over(partition by subs_year,subs_quarter_of_year) Wks_In_Qtr
from Karl_TA_Events
where downgrade_view = 'LV 201601 V15'
        or (Downgrade_View = 'Actuals' and subs_week_and_year <= 201616)














*/
/*
Call CITEAM.UPDATE_DTV_FCAST_WEEKLY_BASE('2016-11-03','2016-12-08')
---------------------------------------------------------------------------------------------
-- Create Procedure to insert actuals into DTV fcast base table -----------------------------
---------------------------------------------------------------------------------------------
-- First you need to impersonate CITeam
Setuser CITeam;


*/


Drop Procedure if exists UPDATE_DTV_FCAST_WEEKLY_BASE;
Create procedure UPDATE_DTV_FCAST_WEEKLY_BASE(IN Start_End_Dt date,IN End_End_Date date)
SQL SECURITY DEFINER


BEGIN
SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Delete from DTV_FCAST_WEEKLY_BASE
where end_date between Start_End_Dt and End_End_Date
-- in (Select distinct end_date from citeam.cust_fcast_weekly_update)
        and Downgrade_View = 'Actuals';

Insert into DTV_FCAST_WEEKLY_BASE(Downgrade_View,End_Date,Account_Number,Subs_Year,Subs_Quarter,Subs_Week,Subs_Week_And_Year
,DTV_Status_Code
,Affluence
,HD_Segment
,Prem_Segment
,sports_tenure
,movies_tenure
,Simple_Segment
,Package_Desc
,Country
,Offer_Length_DTV
,Time_To_Offer_End_DTV
,Time_To_Offer_End_BB
,Time_To_Offer_End_LR
,Time_To_Offer_End
,DTV_Tenure
,Time_Since_Last_TA_call
,Time_Since_Last_AB
,Previous_ABs

,New_Customer,CusCan_Forecast_Segment,SysCan_Forecast_Segment
,DTV_Active
,BB_Active
,DTV_AB
,TA_Event_Count
,Unique_TA_Caller
,TA_Save_Count
,TA_Non_Save_Count
,Offer_Applied_DTV

,Web_Chat_TA_Cnt
,Web_Chat_TA_Customers
,Web_Chat_TA_Not_Saved
,Web_Chat_TA_Saved

,TA_DTV_Offer_Applied
,TA_DTV_PC
,WC_DTV_PC
,Accessibility_DTV_PC
,Min_Term_PC
,Other_PC
,DTV_PC
,PO_Pipeline_Cancellations
,Same_Day_Cancels
,SC_Gross_Terminations

)
Select

'Actuals' as Downgrade_View
,End_Date
,Account_Number
,null as Subs_Year
,null as Subs_Quarter
,null as Subs_Week
,null as Subs_Week_And_Year

,DTV_Status_Code
,affluence_bands as Affluence
,HD_Segment
,Case when sports > 0 and movies > 0 then 'TopTier'
      when sports > 0                then 'Sports'
      when movies > 0                then 'Movies'
      when DTV_Active = 1            then 'Basic'
 end as Prem_Segment

,Case when (end_date - sports_act_date) <=  730 then 'A.<2 Yrs'
      when (end_date - sports_act_date) <= 1825 then 'B.<5 Yrs'
      when (end_date - sports_act_date) <= 3650 then 'C.<10 Yrs'
      when ( end_date- sports_act_date) >  3650 then 'D.10+ Yrs'
      else null
 end as sports_tenure
,Case when (end_date - movies_act_date) <=  730 then 'A.<2 Yrs'
      when (end_date - movies_act_date) <= 1825 then 'B.<5 Yrs'
      when (end_date - movies_act_date) <= 3650 then 'C.<10 Yrs'
      when ( end_date- movies_act_date) >  3650 then 'D.10+ Yrs'
      else null
 end as Movies_tenure

 ,case
    when trim(simple_segment) in ('1 Secure')       then '1 Secure'
    when trim(simple_segment) in ('2 Start', '3 Stimulate','2 Stimulate')  then '2 Stimulate'
    when trim(simple_segment) in ('4 Support','3 Support')      then '3 Support'
    when trim(simple_segment) in ('5 Stabilise','4 Stabilise')    then '4 Stabilise'
    else 'Other/Unknown'
 end as Simple_Segment

,Case when trim(package_desc) in ('Variety','Kids,Mix,World') or package_desc is null then 'Variety'
      when package_desc is null then 'Original'
      when package_desc = 'Other' then 'Original'
      else package_desc
 end Package_Desc

,Case when ROI > 0 then 'ROI' else 'UK' end as Country

, case
    when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3  then 'Offer Length 3M'
    when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >3) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) then 'Offer Length 6M'
    when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >6) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) then 'Offer Length 9M'
    when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >9) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) then 'Offer Length 12M'
    when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12  then 'Offer Length 12M +'
    when Curr_Offer_end_Date_Intended_DTV is null then 'No Offer'
    when curr_offer_start_date is null then 'No Offer'
 end as Offer_Length_DTV

,case
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV > (end_date + 42)                           then 'Offer Ending in 7+ Wks'
    when Prev_offer_end_date_DTV between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_DTV between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End DTV'
 end as Time_To_Offer_End_DTV

,case
when Curr_Offer_end_Date_intended_BB between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 43) and (end_date + 49) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 50) and (end_date + 56) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 57) and (end_date + 63) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 64) and (end_date + 70) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 71) and (end_date + 77) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 78) and (end_date + 84) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB between (end_date + 85) and (end_date + 91) then 'Offer Ending in 7+ Wks'
when Curr_Offer_end_Date_intended_BB >= (end_date + 92)                          then 'Offer Ending in 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
when Prev_offer_end_Date_BB between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
when Prev_offer_end_Date_BB between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
when Prev_offer_end_Date_BB between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
when Prev_offer_end_Date_BB between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
when Prev_offer_end_Date_BB between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
when Prev_offer_end_Date_BB between (end_date - 49) and (end_date - 43) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 56) and (end_date - 50) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 63) and (end_date - 57) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 70) and (end_date - 64) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 77) and (end_date - 71) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 84) and (end_date - 78) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB between (end_date - 91) and (end_date - 85) then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB <= (end_date - 92)                        then 'Offer Ended 7+ Wks'
when Prev_offer_end_Date_BB is null then 'Null'
when Curr_Offer_end_Date_intended_BB is null then 'Null'
else 'No Offer End BB'
 end as Time_To_Offer_End_BB

,case
    when Curr_Offer_end_Date_Intended_LR between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_LR between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_LR between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End LR'
  end as Time_To_Offer_End_LR

,case
    when DTV_BB_LR_offer_end_dt between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when DTV_BB_LR_offer_end_dt between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer'
end as Time_To_Offer_End
,case
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
end as DTV_Tenure
,Case when Last_TA_Call_dt is null then 'No Prev TA Calls'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7  = 0 then '0 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 1 then '01 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 2 and 5 then '02-05 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 6 and 35 then '06-35 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 36 and 41 then '36-46 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 42 and 46 then '36-46 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 47 then '47 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 48 and 52 then '48-52 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 53 and 60 then '53-60 Wks since last TA Call'
 when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 > 60 then '61+ Wks since last TA Call'
 Else ''
End Time_Since_Last_TA_call

,Case when  Last_AB_Dt  is null then 'No Prev AB Calls'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 0 then '0 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 1 then '1-2 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 2 then '1-2 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 3 then '3 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 4 then '4 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 5 then '5-7 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 6 then '5-7 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 7 then '5-7 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 8 then '8-12 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 9 then '8-12 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 10 then '8-12 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 11 then '8-12 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 12 then '8-12 Mnths since last AB'
 when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 > 12 then '12+ Mnths since last AB'
 Else ''
end as  Time_Since_Last_AB

,case
    when base.Previous_Abs = 0 then '0 Previous_Abs'
    when base.Previous_Abs = 1 then '1 Previous_Abs'
    when base.Previous_Abs = 2 then '2 Previous_Abs'
    when base.Previous_Abs = 3 then '3 Previous_Abs'
    when base.Previous_Abs = 4 then '4-7 Previous_Abs'
    when base.Previous_Abs = 5 then '4-7 Previous_Abs'
    when base.Previous_Abs = 6 then '4-7 Previous_Abs'
    when base.Previous_Abs = 7 then '4-7 Previous_Abs'
    when base.Previous_Abs = 8 then '8-10 Previous_Abs'
    when base.Previous_Abs = 9 then '8-10 Previous_Abs'
    when base.Previous_Abs = 10 then '8-10 Previous_Abs'
    when base.Previous_Abs = 11 then '11-15 Previous_Abs'
    when base.Previous_Abs = 12 then '11-15 Previous_Abs'
    when base.Previous_Abs = 13 then '11-15 Previous_Abs'
    when base.Previous_Abs = 14 then '11-15 Previous_Abs'
    when base.Previous_Abs = 15 then '11-15 Previous_Abs'
    when base.Previous_Abs >= 16 then '16 + Previous_Abs'
  else ''
end as Previous_ABs


,case when dtv_act_date between (end_date-6) and end_date then 1 else 0 end as New_Customer
,null as CusCan_Forecast_Segment
,null as SysCan_Forecast_Segment

,DTV_Active
,BB_Active
,DTV_AB
,TA_Call_Count as TA_Event_Count
,TA_Call_Flag as Unique_TA_Caller
,TA_Save_Count
,TA_Call_Count - base.TA_Save_Count as TA_Non_Save_Count
,Cast(0 as tinyint) Offer_Applied_DTV
-- case when base.offer_applied_DTV > 0
--                 and base.dtv_active=1
--                 and base.DTV_PC = 0
--                 and base.DTV_AB = 0
--                 and base.SC_Gross_Terminations = 0
--                 and base.PO_Pipeline_Cancellations = 0
--       then 1
--       else 0
-- end as Offer_Applied_DTV

,Coalesce(WebChat_TA_Saved,0) + Coalesce(WebChat_TA_Not_Saved,0) Web_Chat_TA_Cnt
,Case when Coalesce(WebChat_TA_Saved,0) > 0  or  Coalesce(WebChat_TA_Not_Saved,0) > 0 then 1 else 0 end as Web_Chat_TA_Customers
,Coalesce(WebChat_TA_Not_Saved,0) as Web_Chat_TA_Not_Saved
,Coalesce(WebChat_TA_Saved,0) as Web_Chat_TA_Saved

-- ,case when base.offer_applied_DTV > 0
--                 and base.dtv_active=1
--                 and base.DTV_PC = 0
--                 and base.DTV_AB = 0
--                 and base.SC_Gross_Terminations = 0
--                 and base.PO_Pipeline_Cancellations = 0
--                 and base.ta_call_flag > 0
--       then 1
--       else 0
-- end as
,Cast(0 as tinyint) as TA_DTV_Offer_Applied
,TA_DTV_PC
,WC_DTV_PC
,Accessibility_DTV_PC
,Min_Term_PC
,Other_PC
,DTV_PC
,PO_Pipeline_Cancellations
,Same_Day_Cancels
,SC_Gross_Terminations
from citeam.cust_fcast_weekly_base base
where end_date between Start_End_Dt and End_End_Date
-- in (Select distinct end_date from citeam.cust_fcast_weekly_update)
        and end_date < today()-datepart(weekday,today()+2)
        and DTV_Active = 1
        and dtv_act_date is not null;

-- Update CITEAM.DTV_FCAST_WEEKLY_BASE base
-- Set TA_DTV_Offer_Applied = 1
-- from CITEAM.DTV_FCAST_WEEKLY_BASE base
--      inner join
--      CITeam.Combined_Retention_Report crr
--      on crr.account_number = base.account_number
--         and crr.event_dt between base.end_date + 1 and base.end_date + 7
-- where base.end_date in (Select distinct end_date from citeam.cust_fcast_weekly_update)

Update DTV_FCAST_WEEKLY_BASE base
Set --Offer_Applied_DTV = 0,
    TA_DTV_Offer_Applied = 0
where --base.end_date between Start_End_Dt and End_End_Date
        --and
downgrade_view = 'Actuals';


-- Update CITEAM.DTV_FCAST_WEEKLY_BASE base
-- Set Offer_Applied_DTV = 1
-- from CITEAM.DTV_FCAST_WEEKLY_BASE base
--      inner join
--      CITeam.offer_usage_all oua
--      on oua.account_number = base.account_number
--         and oua.offer_start_dt_Actual between base.end_date + 1 and base.end_date + 7
--         and oua.offer_start_dt_Actual = oua.Whole_offer_start_dt_Actual
--         and oua.offer_end_dt_Actual > oua.offer_start_dt_Actual
--         and oua.subs_type = 'DTV Primary Viewing'
--         and lower(oua.offer_dim_description) not like '%price protection%'
-- where base.end_date between Start_End_Dt and End_End_Date
--         and
-- downgrade_view = 'Actuals';
-- 


drop table if exists #TA_DTV_Offer_Applied;
Select crr.account_number,
       crr.event_dt - datepart(weekday,crr.event_dt+2) end_date
into #TA_DTV_Offer_Applied
from CITeam.Combined_Retention_Report crr
     inner join
     CITeam.offer_usage_all oua
     on oua.account_number = crr.account_number
        and oua.offer_start_dt_Actual = crr.event_dt
        and oua.offer_start_dt_Actual = oua.Whole_offer_start_dt_Actual
        and oua.offer_end_dt_Actual > oua.offer_start_dt_Actual
        and oua.subs_type = 'DTV Primary Viewing'
        and lower(oua.offer_dim_description) not like '%price protection%'
        and crr.TA_Channel = 'Voice'
-- where crr.event_dt > Start_End_Dt
group by crr.account_number,
         end_date
;

commit;

Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set TA_DTV_Offer_Applied = 1
from CITEAM.DTV_FCAST_WEEKLY_BASE base
     inner join
     #TA_DTV_Offer_Applied oua
     on oua.account_number = base.account_number
        and oua.end_date = base.end_date
where downgrade_view = 'Actuals';


Select *
into #Subs_Calendar
from CITeam.Subs_Calendar(2012,2020);

commit;

Create date index idx_1 on #Subs_Calendar(calendar_date);
-- Create lf index idx_2 on #Proc_Sky_Calendar(Subs_Year);
-- Create lf index idx_3 on #Proc_Sky_Calendar(Subs_Week_of_Year);
-- Create lf index idx_4 on #Proc_Sky_Calendar(Subs_Quarter_of_Year);
-- Create lf index idx_5 on #Proc_Sky_Calendar(Subs_Last_Day_Of_Week);

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set CusCan_Forecast_Segment = case when Outer_Base_Table.DTV_status_code = 'AC' then csl.cuscan_forecast_segment else Outer_Base_Table.DTV_status_code end
from CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
    inner join
    CITeam.CusCan_Segment_Lookup csl
    on csl.dtv_tenure = Outer_Base_Table.DTV_tenure
        and csl.Time_Since_Last_TA_Call = Outer_Base_Table.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = Outer_Base_Table.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = Outer_Base_Table.Time_To_Offer_End_DTV
        and csl.package_desc = Outer_Base_Table.package_desc
        and csl.Country = Outer_Base_Table.Country
where Outer_Base_Table.CusCan_Forecast_Segment is null
        and Downgrade_View = 'Actuals'
;

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set SysCan_Forecast_Segment = case when Outer_Base_Table.DTV_status_code = 'AC' then ssl.Syscan_forecast_segment else Outer_Base_Table.DTV_status_code end
from CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
     inner join
     CITeam.SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = Outer_Base_Table.Time_Since_Last_AB
        and ssl.dtv_tenure = Outer_Base_Table.DTV_Tenure
        and ssl.Affluence = Outer_Base_Table.Affluence
        and ssl.simple_segments = Outer_Base_Table.simple_segment
        and ssl.Previous_AB_Count = Outer_Base_Table.Previous_ABs
where Outer_Base_Table.SysCan_Forecast_Segment is null
        and Downgrade_View = 'Actuals';

Select * into #Segment_Base from Simmonsr. REVISED_REDDY_q3_v2_forecast_base; -- simmonsr.REVISED_REDDY_q3_forecast_base;

Update CITeam.DTV_FCAST_WEEKLY_BASE a
Set Placeholder_1 = b.new_segment1
from CITeam.DTV_FCAST_WEEKLY_BASE a
     inner join
     #Segment_Base b
     on a.account_number = b.account_number
where a.subs_week_and_year >= 201614
        and a.end_date between Start_End_Dt and End_End_Date
        and Downgrade_View = 'Actuals';


--------------------------------------------------------------
--- Events from customers not in the base at start of week ---
--------------------------------------------------------------
Insert into CITEAM.DTV_FCAST_WEEKLY_BASE
(
Downgrade_View,Subs_Year,Subs_Quarter,Subs_Week,Subs_Week_And_Year,
New_Customer,
TA_Event_Count,Unique_TA_Caller,TA_Save_Count,TA_Non_Save_Count
)

Select
'Actuals' as Downgrade_View,
crr.subs_year,
Case when crr.subs_week_and_year % 100 between  1 and 13 then 1
     when crr.subs_week_and_year % 100 between 14 and 26 then 2
     when crr.subs_week_and_year % 100 between 27 and 39 then 3
     when crr.subs_week_and_year % 100 between 40 and 53 then 4
end as Subs_Quarter,
crr.subs_week_and_year % 100 as Subs_Week,
crr.subs_week_and_year,
1 as New_Customer,
sum(turnaround_saved+turnaround_not_saved) TA_Event_Count,
Count(distinct crr.account_number) Unique_TA_Caller,
sum(turnaround_saved) TA_Save_Count,
sum(turnaround_not_saved) TA_Non_Save_Count
from citeam.combined_retention_report crr
     left join
     CITeam.DTV_Fcast_Weekly_Base base
     on base.account_number = crr.account_number
        and Cast(crr.event_dt - datepart(weekday,event_dt+2) as date) = base.end_date
        and base.downgrade_view = 'Actuals'
where crr.event_dt - datepart(weekday,event_dt+2) between Start_End_Dt and Start_End_Dt
    and crr.TA_channel = 'Voice'
    and base.account_number is null
group by crr.subs_year,crr.subs_week_and_year
;

-- Delete from CITEAM.DTV_FCAST_WEEKLY_BASE where downgrade_view = 'Actuals' and end_date is null

-- Select * into #Subs_Calendar from citeam.subs_calendar (2012,2016);
--
Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set Subs_Year = Cast(sc.Subs_year as integer)
,Subs_Quarter = Cast(sc.Subs_quarter_of_year as integer)
,Subs_Week = Cast(sc.Subs_week_of_year as integer)
,Subs_Week_And_Year = Cast(sc.Subs_Week_and_year as integer)
from CITEAM.DTV_FCAST_WEEKLY_BASE base
     inner join
     #Subs_Calendar as sc
     on sc.calendar_date = base.end_date + 7
where base.Subs_Year is null
;


END;



-- Grant execute rights to the members of CITeam
grant execute on UPDATE_DTV_FCAST_WEEKLY_BASE to CITeam;
/*
-- Test it
     Execute CITeam.UPDATE_DTV_FCAST_WEEKLY_BASE;

-- Change back to your account
     Setuser;
	 














-----------------------------------------------------------------------------
-- Create Procedure to insert forecast into DTV fcast base table ------------
-----------------------------------------------------------------------------
-- First you need to impersonate CITeam
Setuser CITeam;

Create variable var_Downgrade_View varchar(20); Set var_Downgrade_View = 'LV 201601 V15';
Create variable Sample_Rate float; Set  Sample_Rate = 0.25;
Create variable  Forecast_Start_Wk integer; Set Forecast_Start_Wk = 201601;
	 */
 Drop Procedure if exists INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE;
Create procedure INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE(IN var_Downgrade_View varchar(20),IN Sample_Rate float,IN Forecast_Start_Wk integer)
SQL SECURITY INVOKER

BEGIN

If var_Downgrade_View = 'Actuals' then return end if;

-- Declare var_Downgrade_View as varchar(20));
-- Set var_Downgrade_View = 'LV 201601 V13'
-- sp_columns 'DTV_FCAST_WEEKLY_BASE'


Delete from DTV_FCAST_WEEKLY_BASE
where trim(Downgrade_View) = var_Downgrade_View;

Insert into DTV_FCAST_WEEKLY_BASE
(Downgrade_View
,End_Date
,Account_Number
,Subs_Year
,Subs_Quarter
,Subs_Week
,Subs_Week_And_Year
,DTV_Status_Code
,Affluence
,HD_Segment
,Prem_Segment
,sports_tenure
,movies_tenure
,Simple_Segment
,Package_Desc
,Country
,Offer_Length_DTV
,Time_To_Offer_End_DTV
,Time_To_Offer_End_BB
,Time_To_Offer_End_LR
,Time_To_Offer_End
,DTV_Tenure
,Time_Since_Last_TA_call
,Time_Since_Last_AB
,Previous_ABs

,New_Customer
,CusCan_Forecast_Segment
,SysCan_Forecast_Segment
,DTV_Active
,BB_Active
,DTV_AB
,TA_Event_Count
,Unique_TA_Caller
,TA_Save_Count
,TA_Non_Save_Count
,Offer_Applied_DTV

,Web_Chat_TA_Cnt
,Web_Chat_TA_Customers
,Web_Chat_TA_Not_Saved
,Web_Chat_TA_Saved

,TA_DTV_Offer_Applied
,TA_DTV_PC
,WC_DTV_PC
,Accessibility_DTV_PC
,Min_Term_PC
,Other_PC
,DTV_PC
,PO_Pipeline_Cancellations
,Same_Day_Cancels
,SC_Gross_Terminations
,DTV_Status_Code_EoW
)

Select
 var_Downgrade_View as Downgrade_View
,End_Date
,Account_Number
,Subs_Week_And_Year/100 as Subs_Year
,Case when subs_week_of_year between 1  and 13 then 1
      when subs_week_of_year between 14 and 26 then 2
      when subs_week_of_year between 27 and 39 then 3
      when subs_week_of_year between 40 and 53 then 4
 end Subs_Quarter
,subs_week_of_year as Subs_Week
,Subs_Week_And_Year

,null as DTV_Status_Code
,Affluence
,HD_Segment
,Prem_Segment
,null as sports_tenure
,null as Movies_tenure
,Simple_Segments as Simple_Segment
,Package_Desc
,Country
,Offer_Length_DTV

,Time_To_Offer_End_DTV
,null as Time_To_Offer_End_BB
,null as Time_To_Offer_End_LR
,null as Time_To_Offer_End
,DTV_Tenure
,Time_Since_Last_TA_call
,Time_Since_Last_AB
,Previous_AB_Count as Previous_ABs

,case when dtv_latest_act_date between end_date + 1 and end_date + 7 then 1 else 0 end as New_Customer
,CusCan_Forecast_Segment
,SysCan_Forecast_Segment

,Cast(Case when DTV_Activation_Type is null then 1 else 0 end as float)/Sample_Rate as DTV_Active
,Cast(Case when BB_Segment = 'BB' then 1 else 0 end as float)/Sample_Rate as BB_Active
,Cast(base.DTV_AB as float)/Sample_Rate as DTV_AB
,Cast(base.TA_Call_Count as float)/Sample_Rate as TA_Event_Count
,Cast(base.TA_Call_Cust as float)/Sample_Rate as Unique_TA_Caller
,Cast(base.TA_Saves as float)/Sample_Rate as TA_Save_Count
,Cast(base.TA_Call_Count - base.TA_Saves as float)/Sample_Rate as TA_Non_Save_Count
,Cast(base.DTV_Offer_Applied as float)/Sample_Rate as Offer_Applied_DTV

,Cast(base.WC_Call_Count as float)/Sample_Rate as Web_Chat_TA_Cnt
,Cast(base.WC_Call_Cust as float)/Sample_Rate as Web_Chat_TA_Customers
,Cast(base.WC_Call_Count - base.WC_Call_Cust as float)/Sample_Rate as Web_Chat_TA_Not_Saved
,Cast(base.WC_Saves as float)/Sample_Rate as  Web_Chat_TA_Saved

,Cast(case when base.DTV_Offer_Applied > 0
--                 and base.dtv_active=1
                and base.TA_DTV_PC = 0
                and base.WC_DTV_PC = 0
--                 and base.TA_Sky_Plus_Save = 0
--                 and base.WC_Sky_Plus_Save = 0
--                 and base.Other_PC > 0
                and base.DTV_AB = 0
                and base.SysCan = 0
                and base.CusCan = 0
                and base.TA_Call_Cust > 0
      then 1
      else 0
end as float)/Sample_Rate as TA_DTV_Offer_Applied
,Cast(base.TA_DTV_PC as float)/Sample_Rate
,Cast(base.WC_DTV_PC as float)/Sample_Rate
,Cast(null as float)/Sample_Rate as Accessibility_DTV_PC
,Cast(null as float)/Sample_Rate as Min_Term_PC
,Cast(base.Other_DTV_PC as float)/Sample_Rate as Other_DTV_PC
,Cast(null as float)/Sample_Rate as DTV_PC
,Cast(base.CusCan as float)/Sample_Rate as CusCan
,Cast(null as float)/Sample_Rate as Same_Day_Cancels
,Cast(base.SysCan as float)/Sample_Rate as SysCan
,DTV_Status_Code_EoW

from /*citeam.*/ FORECAST_Looped_Sim_Output_Platform base
;





Drop table if exists #Fcast_New_Cust_TA_Events;
Select
end_date,
subs_year,
Subs_Quarter,
Subs_Week,
subs_week_and_year,
New_Customer,
TA_Event_Count,
Unique_TA_Caller,
TA_Save_Count,
TA_Non_Save_Count,
Dense_Rank() over(partition by Subs_Week order by Subs_Year desc) Week_Rnk
into #Fcast_New_Cust_TA_Events
from DTV_FCAST_WEEKLY_BASE
where account_number is null and downgrade_view = 'Actuals' and New_Customer = 1
        and Subs_Week_And_Year < Forecast_Start_Wk;

Drop table if exists #Wks;
Select distinct subs_year,Subs_Quarter,Subs_Week,subs_week_and_year
into #Wks
from DTV_FCAST_WEEKLY_BASE
where downgrade_view = var_Downgrade_View;

Insert into DTV_FCAST_WEEKLY_BASE
(
Downgrade_View,Subs_Year,Subs_Quarter,Subs_Week,Subs_Week_And_Year,
New_Customer,
TA_Event_Count,Unique_TA_Caller,TA_Save_Count,TA_Non_Save_Count
)
Select
var_Downgrade_View as Downgrade_View,
base.subs_year,
base.Subs_Quarter,
base.Subs_Week,
base.subs_week_and_year,
New_Cust.New_Customer,
New_Cust.TA_Event_Count,
New_Cust.Unique_TA_Caller,
New_Cust.TA_Save_Count,
New_Cust.TA_Non_Save_Count
from #Fcast_New_Cust_TA_Events New_Cust
     inner join
     #Wks base
     on (New_Cust.Subs_Week = base.Subs_Week or (New_Cust.Subs_Week = 52 and base.Subs_Week = 53))
            and Week_Rnk = 1
;

END;

-- Grant execute rights to the members of CITeam
grant execute on INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE to CITeam;
/*
-- Change back to your account
     Setuser;

-- Test it
     Call CITeam.INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE('LV201601 V16',0.25,201601);

Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set DTV_Tenure = null;

commit;
Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set DTV_Tenure =
case
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
end
from CITEAM.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITEAM.cust_FCAST_WEEKLY_BASE source
     on source.account_number = base.account_number
        and source.end_date = base.end_date;

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set CusCan_Forecast_Segment = null
where Downgrade_View = 'Actuals';

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set CusCan_Forecast_Segment = case when Outer_Base_Table.DTV_status_code = 'AC' then csl.cuscan_forecast_segment else Outer_Base_Table.DTV_status_code end
from CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
    inner join
    CITeam.CusCan_Segment_Lookup csl
    on csl.dtv_tenure = Outer_Base_Table.DTV_tenure
        and csl.Time_Since_Last_TA_Call = Outer_Base_Table.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = Outer_Base_Table.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = Outer_Base_Table.Time_To_Offer_End_DTV
        and csl.package_desc = Outer_Base_Table.package_desc
        and csl.Country = Outer_Base_Table.Country
where Downgrade_View = 'Actuals';


Delete from CITeam.DTV_Fcast_Weekly_Base
where downgrade_view = 'LV 201601 V11';

Update CITeam.DTV_Fcast_Weekly_Base
Set downgrade_view = 'LV 201601 V11'
where downgrade_view = 'LVX201601 V11';
*//*
Create variable Start_Year integer; Set Start_Year = 2015;
Create variable End_Year integer; Set End_Year = 2017;

Create variable cal_date date;
Create variable min_cal_date date;
Create variable max_cal_date date;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
 Drop procedure if exists Subs_Calendar;

Create procedure Subs_Calendar(In Start_Year integer,In End_Year integer)
Result( Calendar_date date,subs_year integer, subs_week_of_year integer, Subs_Week_And_Year integer,Subs_quarter_of_year integer,Subs_Last_Day_Of_Week char(1))
-- SQL Security Invoker
BEGIN

Declare cal_date date;
Declare min_cal_date date;
Declare max_cal_date date;


-- Calculate start of Start_Year
Set min_cal_date = Cast(Start_Year || '-07-01' as date);
Set min_cal_date = min_cal_date -  datepart(weekday,min_cal_date + 2) + 1;
-- select min_cal_date;

Set max_cal_date = Cast(End_Year + 1 || '-07-01' as date);
Set max_cal_date = max_cal_date -  datepart(weekday,max_cal_date + 2);

CREATE TABLE #Cal_Dates(
                        Row_ID numeric(5) IDENTITY,
                        Calendar_Date date default null,
                        New_Subs_Year bit default 0,
                        New_Subs_Week bit default 0
                        );

Insert into #Cal_Dates(Calendar_Date)
Select top 10000 Cast(null as date) as Calendar_Date
from CITeam.Cust_Fcast_Weekly_Base;

Update #Cal_Dates
Set Calendar_Date = Cast(min_cal_date + Row_ID - 1 as date);

Delete from #Cal_Dates where Calendar_Date > max_cal_date;

Update #Cal_Dates
Set New_Subs_Year = Case when (
                                (month(calendar_date) = 7 and day(calendar_date) = 1)
                                or
                                (month(calendar_date) = 6 and day(calendar_date) between 25 and 30)
                               )
                         then 1
                         else 0
                    end
    ,New_Subs_Week = 1
where datepart(weekday,Calendar_Date+2) = 1
       ;

Select *,sum(New_Subs_Year) over(order by Cast(Calendar_Date as integer)) Subs_Year_ID
into #Cal_Dates_1
from #Cal_Dates;

Select *
,Start_Year + Subs_Year_ID - 1 as Subs_Year
,sum(New_Subs_Week) over(partition by Subs_Year_ID order by Cast(Calendar_Date as integer)) Subs_Week_Of_Year
,Case when datepart(weekday,Calendar_Date + 2) = 7 then 'Y' else 'N' end as Subs_Last_Day_Of_Week
into #Cal_Dates_2
from #Cal_Dates_1;

Select
Calendar_date,
Subs_Year,
Subs_Week_Of_Year,
Subs_Year*100 + Subs_Week_Of_Year as Subs_Week_And_Year,
Case when Subs_Week_Of_Year between 1 and 13 then 1
            when Subs_Week_Of_Year between 14 and 26 then 2
            when Subs_Week_Of_Year between 27 and 39 then 3
            when Subs_Week_Of_Year between 40 and 53 then 4
end Subs_quarter_of_year,
Subs_Last_Day_Of_Week
from #Cal_Dates_2;

END;

-- Grant execute rights to the members of CITeam
grant execute on SUBS_CALENDAR to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select * from  CITeam.SUBS_CALENDAR(2015,2016);

*/







/* Test proc replicates subs calendar
Select *
from CITeam.Subs_Calendar(2007,2016) a
     full outer join
     sky_calendar b
     on a.calendar_date = b.calendar_date
        and a.subs_year = b.subs_year
        and a.subs_week_of_year = b.subs_week_of_year
        and a.Subs_Last_Day_of_Week = b.Subs_Last_Day_of_Week
where coalesce(a.subs_year,b.subs_year) between 2010 and 2015
*/

