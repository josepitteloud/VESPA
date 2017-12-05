/*----------------------------------------------------------------------------------------------------------*/
-------------------------------- Forecast Model Development Log ----------------------------------------------
/*----------------------------------------------------------------------------------------------------------*/
/*

V5  -- Initial 5Yr Plan and Q2 F'cast
V6  -- Update default rentention offer length from 6months to 10 months
V7  -- A year after rolling off an offer if a customer hasn�t taken a new offer they move back into the lower risk �No Offer� segment
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


*/
/*----------------------------------------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------------------------------------*/








----------------------------------------------------------------------------------------------------------------
--- Create Variables and Set Forecast Parameters ---------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
-- Base date for start of forecast
Drop variable if exists base_date;
create variable base_date date;

-- Sampling variables
Drop variable if exists sample_pct;
create variable sample_pct decimal(7,4);
set sample_pct=0.25;

-- Random number variables
Drop variable if exists multiplier;
Create Variable multiplier  bigint ;
Set multiplier = DATEPART(millisecond,now())+1;
Drop variable if exists multiplier_2;
Create Variable multiplier_2  bigint ;
Set multiplier_2 = DATEPART(millisecond,now())+2;


-- Regression variables
Drop variable if exists Regr_Grad_Coeff;
Create variable Regr_Grad_Coeff float;
Drop variable if exists Regr_Intercept_Coeff;
Create variable Regr_Intercept_Coeff float;
Drop variable if exists Fcast_Intecpt_Adj;
Create variable Fcast_Intecpt_Adj float;

Drop variable if exists Nxt_sim_Dt;
Create variable Nxt_sim_Dt date;
Set Nxt_sim_Dt = (Select Min(Misc_dt_1) from CITeam.SQL_Automation_Execution_Flags
                where Automation_Script = 'FORECAST_Tableau_Tables' and Automation_Variable = 'Cuscan_Fcast_Table_Status');
-- Select Nxt_sim_Dt
Drop variable if exists Forecast_Start_Wk;
Create variable Forecast_Start_Wk integer; --1st Wk of Forecast
Set Forecast_Start_Wk = 201617;
-- (Select max(subs_week_and_year) from sky_calendar where calendar_date = (Select max(end_date + 7) from citeam.cust_fcast_weekly_base));
-- Select Forecast_Start_Wk;

Drop variable if exists Forecast_End_Wk;
Create variable Forecast_End_Wk integer; --Last Wk of Forecast
Set Forecast_End_Wk = 201652;

Drop table if exists #Sky_Calendar;
Select *
into #Sky_Calendar
from CITeam.Subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);

Drop variable if exists n_weeks_to_forecast;
create variable n_weeks_to_forecast     integer;
set n_weeks_to_forecast = (Select count(distinct subs_week_and_year) from #sky_calendar where Cast(subs_week_and_year as integer) between Forecast_Start_Wk and Forecast_End_Wk );
-- select n_weeks_to_forecast;


Drop variable if exists new_cust_end_date;
create variable new_cust_end_date date;
Drop variable if exists new_cust_subs_week_and_year;
create variable new_cust_subs_week_and_year integer;
Drop variable if exists new_cust_subs_week_of_year;
create variable new_cust_subs_week_of_year integer;

Drop variable if exists counter;
create variable counter integer;
-- (Select max(misc_int_1) from  CITeam.SQL_Automation_Execution_Flags
--                        where Automation_Script = 'FORECAST_Tableau_Tables' and Automation_Variable = 'MS_Fcast_Table_Status');
-- Select Nxt_sim_Dt,Forecast_Start_Wk,Forecast_End_Wk;

If today() < Nxt_sim_Dt then return End If;

-- select Forecast_Start_Wk,Forecast_End_Wk;


-----------------------------------------------------------------------------------------------
----PART I: CUSCAN RATES    -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

create variable run_rate_weeks integer;
create variable Y3W52 integer;
create variable Y3W40 integer;
create variable Y3W01 integer;
create variable Y2W01 integer;
create variable Y1W01 integer;

--3 year window preceding the forecast, weeks are the week of action

Set Y1W01 = Case when ((Cast(Forecast_Start_Wk as float)/100) % 1)*100 = 53
                      then (Forecast_Start_Wk/100-2)*100 + 1
                 else Forecast_Start_Wk - 300
            end;--  - 300; --201242;
Set Y2W01 = Y1W01 + 100; --201342;
Set Y3W01 = Y1W01 + 200;
Set Y3W52 = Case when ((Cast(Forecast_Start_Wk as float)/10) % 1)*10 = 1
                      then (Forecast_Start_Wk/100 - 1)*100 +  52
                 else Forecast_Start_Wk - 1
            end; --201541;
Set Y3W40 = Case when ((Cast(Y3W52 as float)/100) % 1)*100  <= 12
                      then (Y3W52/100 - 1)*100 + (52-12) + ((Cast(Y3W52 as float)/100) % 1)*100
                 when Y3W52 = 53
                      then Y3W52 - 13
                 else Y3W52 - 12
            end
            ; --201529;

-- Select Forecast_Start_Wk,Y1W01,Y2W01,Y3W01,Y3W40,Y3W52;


set  run_rate_weeks =13;

--select last 3 years, flag last year and year prior to that
drop table if exists cuscan_weekly_agg;
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

,dense_rank() over(order by subs_week_and_year desc) as week_id
,case
  when week_id between  1 and  52 then 'Curr'
  when week_id between 53 and 104 then 'Prev'
  else null end as week_position
,case when week_id between 1 and 13 then 'Y' else 'N' end as last_quarter
,(week_id/13)+1 as quarter_id
into cuscan_weekly_agg
from CITeam.DTV_Fcast_Weekly_Base agg
where subs_week_and_year between Y1W01 and Y3W52
        and subs_week != 53
group by subs_year
,subs_week
,subs_week_and_year
,cuscan_forecast_segment
;



--for each customer segment and week, action counts for current and previous year
drop table if exists cuscan_forecast_summary_1;
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

--   ,sum(case when Week_Position = 'Prev' and        new_customer=1 then n else 0 end) as prev_new_customer
--   ,sum(case when Week_Position = 'Curr' and        new_customer=1 then n else 0 end) as curr_new_customer
  -- can we make sure the below condition is correct
--   ,sum(case when Week_Position = 'Prev' and activations_reinstates>=1 then n else 0 end) as prev_reinstated_customer
--   ,sum(case when Week_Position = 'Curr' and activations_reinstates>=1 then n else 0 end) as curr_reinstated_customer

  ,sum(0)                                                                       as LQ_n
  ,sum(0)                                                                       as LQ_DTV_Offer

into cuscan_forecast_summary_1
from cuscan_weekly_agg agg
group by
subs_week
,cuscan_forecast_segment
;


--for each customer segment (but not week), no action new offer counts for last quarter
drop table if exists cuscan_forecast_summary_LQ;
select
cuscan_forecast_segment
,sum(n)       as LQ_n
,sum(dtv_offer_applied)    as LQ_DTV_Offer
into cuscan_forecast_summary_LQ
from cuscan_weekly_agg
where last_quarter='Y'
group by cuscan_forecast_segment;


--add LQ volumes onto previous summary table
update cuscan_forecast_summary_1
set a.LQ_n = b.LQ_n
   ,a.LQ_DTV_Offer = b.LQ_DTV_Offer
from cuscan_forecast_summary_1 as a
     left join
     cuscan_forecast_summary_LQ as b
on a.cuscan_forecast_segment = b.cuscan_forecast_segment
;




--create rates from action counts and cell size

drop table if exists cuscan_forecast_summary_2;
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
into cuscan_forecast_summary_2
from cuscan_forecast_summary_1
;


-- pred rates (except for No Action New Offer which uses the last quarters average)

drop table if exists cuscan_forecast_summary_3;
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


--,LQ_nano as pred_nano_rate
-- ,cast((prev_new_customer        + curr_new_customer       )/2 as integer) as pred_new_customer
-- ,cast((prev_reinstated_customer + curr_reinstated_customer)/2 as integer) as pred_reinstated_customer

into cuscan_forecast_summary_3
from cuscan_forecast_summary_2;


--final output for use in forecasting
drop table if exists cuscan_predicted_values;
select
a.*
-- ,coalesce(     TA_Call_cnt_rate_trend, 1) as      TA_Call_cnt_rate_trend
-- ,coalesce(    TA_Call_Cust_rate_trend, 1) as      TA_Call_Cust_rate_trend
-- ,coalesce(    TA_Not_Saved_rate_trend, 1) as      TA_Not_Saved_rate_trend
-- ,coalesce(        TA_Saved_rate_trend, 1) as      TA_Saved_rate_trend
-- ,coalesce( Web_Chat_TA_Cnt_rate_trend, 1) as      Web_Chat_TA_Cnt_rate_trend
-- ,coalesce( Web_Chat_TA_Cust_rate_trend, 1) as   Web_Chat_TA_Cust_rate_trend
-- ,coalesce( Web_Chat_TA_Not_Saved_rate_trend, 1) as  Web_Chat_TA_Not_Saved_rate_trend
-- ,coalesce(Web_Chat_TA_Saved_rate_trend, 1) as     Web_Chat_TA_Saved_rate_trend
-- ,coalesce(1, 1) as     NonTA_DTV_Offer_Applied_rate_trend -- not used
-- ,coalesce(TA_DTV_Offer_Applied_rate_trend, 1) as     TA_DTV_Offer_Applied_rate_trend
--
-- ,coalesce(       TA_DTV_PC_rate_trend, 1) as     TA_DTV_PC_rate_trend
-- ,coalesce(       WC_DTV_PC_rate_trend, 1) as     WC_DTV_PC_rate_trend
-- ,coalesce(        Other_PC_rate_trend, 1) as     Other_PC_rate_trend

into cuscan_predicted_values
from cuscan_forecast_summary_3 as a
--      left join
--      cuscan_align as b
--      on a.cuscan_forecast_segment  = b.cuscan_forecast_segment
;

/*
Select top 1000 * from cuscan_predicted_values;
Select top 1000 * from cuscan_align;
*/

-----------------------------------------------------------------------------------------------
----PART II: SYSCAN RATES -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------


drop table if exists syscan_weekly_agg;
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
into syscan_weekly_agg
from CITeam.DTV_FCast_Weekly_Base
where subs_week_and_year between Y1W01 and Y3W52
            and subs_week !=53
group by subs_year
,subs_week
,subs_week_and_year
,syscan_forecast_segment;



-- select * from syscan_align;

--for each customer segment and week, action counts for current and previous year
 drop table if exists syscan_forecast_summary_1;
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

into syscan_forecast_summary_1
from syscan_weekly_agg
group by
subs_week
,syscan_forecast_segment
;


--for each customer segment (but not week), no action new offer counts for last quarter
drop table if exists syscan_forecast_summary_LQ;
select
syscan_forecast_segment
,sum(n)       as LQ_n
-- ,sum(dtv_offer_applied)    as LQ_DTV_Offer
into syscan_forecast_summary_LQ
from syscan_weekly_agg
where last_quarter='Y'
group by syscan_forecast_segment;



--create rates from action counts and cell size

drop table if exists syscan_forecast_summary_2;
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
into syscan_forecast_summary_2
from syscan_forecast_summary_1
;

-- pred rates (except for No Action New Offer which uses the last quarters average)

drop table if exists syscan_forecast_summary_3;
select
*
,/*(curr_share **/ curr_dtv_ab_rate /*) + (prev_share * prev_dtv_ab_rate)*/ as pred_dtv_ab_rate
into syscan_forecast_summary_3
from syscan_forecast_summary_2;


--final output for use in forecasting
drop table if exists syscan_predicted_values;
select
a.*
-- ,coalesce(dtv_ab_rate_trend, 1) as      dtv_ab_rate_trend

into syscan_predicted_values
from syscan_forecast_summary_3 as a
-- left join syscan_align as b
-- on a.syscan_forecast_segment  = b.syscan_forecast_segment

;


-----------------------------------------------------------------------------------------------
----PART III: NEW CUSTOMERS -------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
drop table if exists new_customers_last_2Yrs;

select
end_date
,cast(null as integer)  as year
,Cast(null as integer) as week
,Cast(null as integer) as year_week
,account_number
,dtv_status_code
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
-- ,rand(number(*)*multiplier) as rand_sample
--  ,row_number() over(partition by DTV_Activation_Type order by rand_sample) Sample_Rank
-- ,count(*) as n
into new_customers_last_2Yrs
from citeam.cust_fcast_weekly_base
where dtv_active =1
    and end_date between
--     (Select max(calendar_date) from sky_calendar where Cast(subs_week_and_year as integer) = Y2W01 and subs_last_day_of_week = 'Y')
                                        (Select max(calendar_date - 5 * 7) from sky_calendar where Cast(subs_week_and_year as integer) = Y3W52 and subs_last_day_of_week = 'Y')
                                    and (Select max(calendar_date) from sky_calendar where Cast(subs_week_and_year as integer) = Y3W52 and subs_last_day_of_week = 'Y')
    and dtv_latest_act_date between (end_date-6) and end_date -- New customers
    and DTV_Activation_Type is not null
;

--------------------------------------------------------------------------------------------------------------------------------
-------------------------- UPDATE THE SEGMENTS ----------------------------------------------------------------------------------

Update new_customers_last_2Yrs flt
Set CusCan_Forecast_Segment = replace(csl.cuscan_forecast_segment,'_SkyQ','_Original')
from new_customers_last_2Yrs flt
     inner join
     CITeam.CusCan_Segment_Lookup csl
     on csl.dtv_tenure = flt.dtv_tenure
        and csl.Time_Since_Last_TA_Call = flt.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = flt.Offer_Length_DTV
        and csl.Time_To_Offer_End = flt.Time_To_Offer_End
        and csl.package_desc = flt.package_desc;


Update new_customers_last_2Yrs flt
Set SysCan_Forecast_Segment = ssl.SysCan_Forecast_Segment
from new_customers_last_2Yrs flt
     inner join
     SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = flt.Time_Since_Last_AB
        and ssl.dtv_tenure = flt.dtv_tenure
        and ssl.Affluence = flt.Affluence
        and ssl.simple_segments = flt.simple_segments
        and ssl.Previous_AB_Count = flt.Previous_AB_Count;










-----------------------------------------------------------------------------------------------------------------------
-- Create the distributions for the numbers for Calls and WebChat ------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------
-- TA CALLS -- CALCULATE ALSO THE DISTRIBUTION FOR SAVED / NOT SAVED CALLS
----------------------------------------------------------------------


Drop table if exists TA_Call_Vol;
select /*cuscan_forecast_segment,*/ end_date
,Cast(null as integer) subs_week
,ta_call_count          as total_calls
,TA_Save_Count           as TA_Saved
-- ,total_calls - TA_Saved as TA_Not_Saved
,count(*) as Total_Customers
into TA_Call_Vol
from citeam.cust_fcast_weekly_base
where end_date between (select max(calendar_date - 7) from sky_calendar where subs_week_and_year = Y2W01)
        and (select max(calendar_date - 7) from sky_calendar where subs_week_and_year = Y3W52)
        and ta_call_flag = 1
group by /*cuscan_forecast_segment,*/ end_date
,total_calls
,TA_saved
-- ,TA_not_saved
;

commit;
create date index idx_1 on TA_Call_Vol(end_date);

Drop table if exists TA_Call_Dist;
Select subs_week_of_year as subs_week, /*cuscan_forecast_segment,*/
--        1 as TA_Call_Cust,
        total_calls,
        TA_Saved,
        sum(TAs.Total_Customers) TA_Customers,
        Cast(null as float) as TA_Vol_Percentile,
        Cast(null as float) as Prev_TA_Vol_Percentile,


        sum(TA_Customers) over(partition by subs_week,total_calls order by TA_Saved) Cum_TA_Saves,
        sum(TA_Customers) over(partition by subs_week,total_calls) Total_TA_Saves,
        Cast(Cum_TA_Saves as float)/Total_TA_Saves as TA_Save_Vol_Percentile,
        Cast(null as float) as Prev_TA_Save_Vol_Percentile
into TA_Call_Dist
from TA_Call_Vol TAs
     inner join
     sky_calendar sc
     on sc.calendar_date = TAs.end_date+7
where subs_week_of_year != 53
        and total_calls <= 4
group by
        subs_week
        ,total_calls
        ,TA_Saved /*cuscan_forecast_segment,*/
        ;

-- join the table back - where in the distibutionthe customer sits
Update TA_Call_Dist a
Set Prev_TA_Save_Vol_Percentile = Coalesce(b.TA_Save_Vol_Percentile,0)
from  TA_Call_Dist a
      left join
       TA_Call_Dist b
       on a.subs_week = b.subs_week
          and a.total_calls = b.total_calls
          and a.TA_Saved - 1 = b.TA_Saved;

Drop table if exists TA_Call_Vol_Dist;
Select subs_week,total_calls,
       sum(TA_Customers) as TA_Customers,
       Sum(TA_Customers) over(partition by subs_week order by total_calls) Cum_TA_Customers,
       Sum(TA_Customers) over(partition by subs_week) Total_TA_Customers,
       Cast(Cum_TA_Customers as float)/Total_TA_Customers as Percentile
into TA_Call_Vol_Dist
from TA_Call_Dist
group by subs_week,total_calls;

-- upper bound
Update TA_Call_Dist cd
Set TA_Vol_Percentile = Percentile
from TA_Call_Dist cd
     left join
     TA_Call_Vol_Dist vd
     on vd.subs_week = cd.subs_week
        and vd.total_calls = cd.total_calls;

-- lower  bound
Update TA_Call_Dist cd
Set Prev_TA_Vol_Percentile = Coalesce(vd.Percentile,0)
from TA_Call_Dist cd
     left join
     TA_Call_Vol_Dist vd
     on vd.subs_week = cd.subs_week
        and vd.total_calls = cd.total_calls - 1;


-- Select * from TA_Call_Vol_Dist order by subs_week,total_calls;
-- Select * from TA_Call_Dist  order by subs_week,total_calls,TA_Saved;


commit;

-------------------------------------------------------
-- WEBCHAT  --
------------------------------------------------------
Drop table if exists WC_Call_Vol;
select /*cuscan_forecast_segment,*/ end_date
,Cast(null as integer) subs_week
,Webchat_ta_Saved + Webchat_ta_not_saved as total_WCs
,Webchat_TA_Saved
,count(*) as Total_Customers
into WC_Call_Vol
from citeam.cust_fcast_weekly_base
where end_date between (select max(calendar_date - 7) from sky_calendar where subs_week_and_year = Y2W01)
        and (select max(calendar_date - 7) from sky_calendar where subs_week_and_year = Y3W52)
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
into WC_Dist
from WC_Call_Vol as WCs
     inner join
     sky_calendar sc
     on sc.calendar_date = WCs.end_date+7
where subs_week_of_year != 53
--         and total_calls <= 4
group by
subs_week
,total_wcs /*cuscan_forecast_segment,*/
,Webchat_TA_Saved
;



Update WC_Dist a
Set Prev_WC_Save_Vol_Percentile = Coalesce(b.WC_Save_Vol_Percentile,0)
from WC_Dist as a
     left join
     WC_Dist as b
     on a.subs_week = b.subs_week
        and a.total_wcs = b.total_wcs
        and a.Webchat_TA_Saved -1 = b.Webchat_TA_Saved;



Drop table if exists WC_Vol_Dist;
Select subs_week
        ,total_WCs
       ,sum(WC_Customers) as WC_Customers
       ,Sum(WC_Customers) over(partition by subs_week order by total_WCs) as Cum_WC_Customers
       ,Sum(WC_Customers) over(partition by subs_week)                    as Total_WC_Customers
       ,Cast(Cum_WC_Customers as float)/Total_WC_Customers                as Percentile
into WC_Vol_Dist
from WC_Dist
group by subs_week
,total_WCs;

-- upper bound
Update WC_Dist cd
Set WC_Vol_Percentile = Percentile
from WC_Dist as cd
     left join
     WC_Vol_Dist as  vd
     on vd.subs_week = cd.subs_week
        and vd.total_WCs = cd.total_WCs;


-- lower  bound
Update WC_Dist cd
Set Prev_WC_Vol_Percentile = Coalesce(vd.Percentile,0)
from WC_Dist cd
     left join
     WC_Vol_Dist vd
     on vd.subs_week = cd.subs_week
        and vd.total_WCs = cd.total_WCs - 1;


-- Select * from TA_Call_Vol_Dist order by subs_week,total_calls;
-- Select * from TA_Call_Dist  order by subs_week,total_calls,TA_Saved;
--
-- Select * from WC_Vol_Dist order by subs_week,total_WCs;
-- Select * from WC_Dist  order by subs_week,total_WCs,Webchat_TA_Saved;

----------------------------------------------------------------------------------------------------------------------------------------------
--- CALCULATE THE CONVERSION RATES TA -> PC -----------------
Drop table if exists TA_DTV_PC_Vol;
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

into TA_DTV_PC_Vol
from citeam.DTV_Fcast_Weekly_Base -- Replace with DTV_FCast_Weekly_Base
where  end_date between (select max(calendar_date - 7 - 5*7) from sky_calendar where subs_week_and_year = Y3W52) -- Last 6 Wk PC conversions
        and (select max(calendar_date - 7) from sky_calendar where subs_week_and_year = Y3W52)
group by
cuscan_forecast_segment
;

-- sp_columns 'DTV_FCAST_WEEKLY_BASE'




----------------------------------------------------------------------------------------------------------------
---PART IV - SIMULATION-----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------
--1.0 CREATE THE OPENING BASE

set temporary option query_temp_space_limit = 0;

-- create the base week
set base_date = (select max(calendar_date - 7) from sky_calendar where subs_week_and_year = Forecast_Start_Wk);
-- select base_date;


-- Note: all segment related fields align with the date fields (action fields are from the following week)

drop table if exists base_sample;

select
  account_number
,end_date
,cast(subs_week_and_year as integer)
,subs_week_of_year
,(subs_year-2010)*52+subs_week_of_year as weekid
,DTV_Status_Code
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

into base_sample
from citeam.cust_fcast_weekly_base
where end_date = base_date
      and dtv_active =1
      and dtv_act_date is not null
;


Update base_sample
Set CusCan_Forecast_Segment = csl.cuscan_forecast_segment
from base_sample flt
     inner join
     CITeam.CusCan_Segment_Lookup csl
     on csl.dtv_tenure = flt.dtv_tenure
        and csl.Time_Since_Last_TA_Call = flt.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = flt.Offer_Length_DTV
        and csl.Time_To_Offer_End = flt.Time_To_Offer_End
        and csl.package_desc = flt.package_desc;


Update base_sample flt
Set SysCan_Forecast_Segment = ssl.SysCan_Forecast_Segment
from base_sample flt
     inner join
     SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = flt.Time_Since_Last_AB
        and ssl.dtv_tenure = flt.dtv_tenure
        and ssl.Affluence = flt.Affluence
        and ssl.simple_segments = flt.simple_segments
        and ssl.Previous_AB_Count = flt.Previous_AB_Count;



--sample to speed up processing
update base_sample set sample = case when rand_sample < sample_pct then 'A' else 'B' end;
-- select sample, count(*) from base_sample group by sample;


-- 2.1 Base To Be Simulated
--      variables renamed to Post_ to align with the looping process
--      likewise Base2 tablename used (Looping reads base2 into base1 then creates base2)

DROP TABLE if exists Forecast_Loop_Table;
Select *
,rand(number(*)*multiplier+1) as rand_action_Cuscan
,Cast(null as float)          as rand_action_Syscan
,rand(number(*)*multiplier+2) as rand_TA_Vol
,rand(number(*)*multiplier+3) as rand_WC_Vol
,rand(number(*)*multiplier+4) as rand_TA_Save_Vol
,rand(number(*)*multiplier+5) as rand_WC_Save_Vol
,rand(number(*)*multiplier+6) as rand_TA_DTV_Offer_Applied
,rand(number(*)*multiplier+7) as rand_NonTA_DTV_Offer_Applied

,rand(number(*)*multiplier+8) as rand_TA_DTV_PC_Vol
,rand(number(*)*multiplier+9) as rand_WC_DTV_PC_Vol
,rand(number(*)*multiplier+10) as rand_Other_DTV_PC_Vol

into Forecast_Loop_Table
from base_sample
where sample = 'A';

-- select top 10 * from Forecast_Loop_Table;


-- Select subs_week_and_year, count(*) as n, count(distinct account_number) as d, n-d as dups from Forecast_Loop_Table group by subs_week_and_year;
Drop variable if exists true_sample_rate;
create variable true_sample_rate float ;
set true_sample_rate = (select sum(case when sample='A' then cast(1 as float) else 0 end)/count(*) from base_sample);

-- select true_sample_rate;

alter table Forecast_Loop_Table drop rand_sample;
alter table Forecast_Loop_Table drop sample;






------------------------------------------------------------------------------------------------------------------------------------------------
-- 3.0 LOOPING ---------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------
set temporary option query_temp_space_limit = 0;

---------------------------------------------------------------------------------------------------------------------------------------------------
------------- recalculate the new segments based on the  volumes ----------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------
Select Cast(sc.subs_week_of_year as integer) subs_week_of_year
    ,sum(Case when dtv_latest_act_date between (end_date-6) and end_date and dtv_first_act_date < dtv_latest_act_date then 1 else 0 end)/2 Reinstates
    ,sum(Case when dtv_latest_act_date between (end_date-6) and end_date and (dtv_first_act_date = dtv_latest_act_date) then 1 else 0 end)/2 Acquisitions
    ,Reinstates + Acquisitions  as New_Customers
into #Activation_Vols
from citeam.cust_fcast_weekly_Base base
     inner join
     sky_calendar sc
     on sc.calendar_date = base.end_date
where base.end_date between (Select max(calendar_date) from sky_calendar where Cast(subs_week_and_year as integer) = Y2W01 and subs_last_day_of_week = 'Y')
      and (Select max(calendar_date) from sky_calendar where Cast(subs_week_and_year as integer) = Y3W52 and subs_last_day_of_week = 'Y')
      and sc.subs_week_of_year != 53
group by sc.subs_week_of_year ;



Drop table if exists #Sky_Calendar;
Select *
into #Sky_Calendar
from CITeam.Subs_Calendar(Forecast_Start_Wk/100,Forecast_End_Wk/100);


set counter = 1;

Delete from FORECAST_Looped_Sim_Output_Platform;


drop table if exists #Fcast_Regr_Coeffs;
Select *
into #Fcast_Regr_Coeffs
from menziesm.Regression_Coefficient(201601 /*Forecast_Start_Wk*/);
-- Select * from #Fcast_Regr_Coeffs
-- Select Regr_Grad_Coeff;
-- Select Fcast_Intecpt_Adj;

-- Select * into FORECAST_Looped_Sim_Output_Platform_Trend from FORECAST_Looped_Sim_Output_Platform
While Counter <= n_weeks_to_forecast LOOP


-- update the dates first
Update Forecast_Loop_Table a
Set subs_week_and_year = sc.subs_week_and_year,
      subs_week_of_year = sc.subs_week_of_year
from Forecast_Loop_Table a
     inner join
     #sky_calendar sc
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
        and csl.Time_To_Offer_End = flt.Time_To_Offer_End
        and csl.package_desc = flt.package_desc;


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

   ;




-- 3.02 Add Random Number and Segment Size for random event allocations later --

DROP TABLE if exists Pred_Rates;

select
 a.*
-- ,rand(number(*)*multiplier+1) as rand_action
-- ,rand(number(*)*multiplier+2) as rand_Ta_Vol
-- ,rand(number(*)*multiplier+3) as rand_WC_Vol
-- ,count(*) over(partition by Cuscan_Segment) as Cuscan_segment_count
-- ,count(*) over(partition by Syscan_Segment) as Syscan_segment_count
-- ,sum(b.Cuscan) CusCan_Churn
-- ,sum(c.Syscan) SysCan_Churn
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

commit;



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
     #Fcast_Regr_Coeffs as d
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
     #Fcast_Regr_Coeffs as d
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
     #Fcast_Regr_Coeffs as d
        on replace(a.cuscan_forecast_segment,'_SkyQ','_Original')  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'TA_DTV_Offer_Applied';

------ DTV Offer trend ------
update Forecast_Loop_Table_2 as a
set pred_NonTA_DTV_Offer_Applied_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
from Forecast_Loop_Table_2 as a
     left join
     #Fcast_Regr_Coeffs as d
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
     #Fcast_Regr_Coeffs as d
        on a.syscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
        and d.Metric = 'DTV_AB';

---- AB cum ------
update Forecast_Loop_Table_2 as a
set cum_DTV_AB_rate  = pred_DTV_AB_rate  ;

update Forecast_Loop_Table_2 as a
set cum_DTV_AB_trend_rate = cum_DTV_AB_rate + pred_dtv_YoY_Trend;







-- ------ TA_DTV_PC trend ------
-- update Forecast_Loop_Table_2 as a
-- set pred_TA_DTV_PC_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
-- from Forecast_Loop_Table_2 as a
--      left join
--      #Fcast_Regr_Coeffs as d
--         on a.cuscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
--         and d.Metric = 'TA_DTV_PC';
--
--
-- ------ WC_DTV_PC trend ------
-- update Forecast_Loop_Table_2 as a
-- set pred_WC_DTV_PC_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
-- from Forecast_Loop_Table_2 as a
--      left join
--      #Fcast_Regr_Coeffs as d
--         on a.cuscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
--         and d.Metric = 'WC_DTV_PC';
--
-- ------ Other_PC trend ------
-- update Forecast_Loop_Table_2 as a
-- set pred_Other_PC_YoY_Trend  = Coalesce(d.Grad_Coeff * 4 * (Cast(counter-1 as integer)/52+1),0)
-- from Forecast_Loop_Table_2 as a
--      left join
--      #Fcast_Regr_Coeffs as d
--         on a.cuscan_forecast_segment  = d.fcast_segment
--         and d.LV = Forecast_Start_Wk
--         and d.Metric = 'Other_PC';





-- 3.06 Allocate customers randomly based on rates --
update Forecast_Loop_Table_2 as a
set
 TA_Call_Cust   = case when pct_cuscan_count <= cum_TA_Call_Cust_rate
                       then 1
                       else 0
                  end
,WC_Call_Cust   = case when pct_cuscan_count > cum_TA_Call_Cust_rate
                            and pct_cuscan_count <= cum_Web_Chat_TA_Cust_rate
                       then 1
                       else 0
                  end
;

Update Forecast_Loop_Table_2 as a
Set DTV_Offer_Applied = 1
where TA_Call_Cust = 1 and rand_TA_DTV_Offer_Applied <= pred_TA_DTV_Offer_Applied_rate + pred_TA_DTV_Offer_Applied_YoY_Trend
      or
      TA_Call_Cust = 0 and rand_NonTA_DTV_Offer_Applied <= pred_NonTA_DTV_Offer_Applied_rate + pred_NonTA_DTV_Offer_Applied_YoY_Trend
      ;


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

update Forecast_Loop_Table_2 as a
set
TA_DTV_PC = case when rand_TA_DTV_PC_Vol <= pred_TA_DTV_PC_rate and TA_Call_Cust > 0
                then 1
                else 0
            end
,TA_Sky_Plus_Save = case when rand_TA_DTV_PC_Vol > pred_TA_DTV_PC_rate
                              and rand_TA_DTV_PC_Vol < cum_TA_DTV_PC_rate
                              and TA_Call_Cust >0
                then 1
                else 0
            end

,WC_DTV_PC = case when rand_WC_DTV_PC_Vol <= pred_WC_DTV_PC_rate and WC_Call_Cust > 0
                then 1
                else 0
            end
,WC_Sky_Plus_Save = case when rand_WC_DTV_PC_Vol > pred_WC_DTV_PC_rate
                              and rand_WC_DTV_PC_Vol < cum_WC_DTV_PC_rate
                              and WC_Call_Cust >0
                then 1
                else 0
            end
,Other_DTV_PC = Case when TA_Call_Cust = 0 and WC_Call_Cust = 0
                          and rand_Other_DTV_PC_Vol <= pred_Other_DTV_PC_rate
                     then 1 else 0
            end
from Forecast_Loop_Table_2 as a
-- left join #TA_PC_Vols as b
--  on (a.subs_week_of_year       = b.subs_week or (a.subs_week_of_year = 53 and b.subs_week = 52))
--         and a.cuscan_forecast_segment = b.cuscan_forecast_segment
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
,cast(rank() over(partition by Syscan_Forecast_segment  order by rand_action_Syscan) as float)                               as SysCan_Group_rank
,cast(rank() over(partition by Syscan_Forecast_segment order by rand_action_Syscan) as float)/cast(Syscan_segment_count as float) as pct_syscan_count
into #SysCan_Rank
from Forecast_Loop_Table_2;

commit;
create hg index idx_1 on #SysCan_Rank(account_number);

Update Forecast_Loop_Table_2
Set SysCan_Group_rank = b.SysCan_Group_rank,
    pct_syscan_count = b.pct_syscan_count
from Forecast_Loop_Table_2 a
     inner join
     #SysCan_Rank b
     on a.account_number = b.account_number;


update Forecast_Loop_Table_2 as a
set DTV_AB         = case when pct_syscan_count <= pred_dtv_AB_rate              then 1 else 0 end
;


-- TA
update Forecast_Loop_Table_2 as a
set  TA_Call_Count  = b.total_calls
    ,TA_Saves      = b.TA_Saved
--     ,TA_Non_Saves   = case when pct_cuscan_count <= pred_TA_Not_Saved_rate        then 1 else 0 end
from Forecast_Loop_Table_2 as a
     inner join
     TA_Call_Dist as b
     on b.Subs_week = a.subs_week_of_year
        and Prev_TA_Vol_Percentile <= rand_TA_Vol
        and rand_TA_Vol <= TA_Vol_Percentile
        and Prev_TA_Save_Vol_Percentile <= rand_TA_Save_Vol
        and rand_TA_Save_Vol <= TA_Save_Vol_Percentile
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
--
-- Update Forecast_Loop_Table_2 as a
-- Set TA_DTV_PC = 1
-- where TA_Call_Count - TA_Saves > 0;

-- Update Forecast_Loop_Table_2 as a
-- Set WC_DTV_PC = 1
-- where WC_Call_Count - WC_Saves > 0;

------------------------- To be completed
/*
Update Forecast_Loop_Table_2 as a
Set Other_PC = 1
where
*/
------------------------------------------




/* Validation
Select end_date,TA_Call_Cust,WC_Call_Cust,DTV_AB,count(*)*4 Customers,sum(TA_Call_Count)*4 TA_Events
from Forecast_Loop_Table_2
group by end_date,TA_Call_Cust,WC_Call_Cust,DTV_AB;

Select subs_week_and_year, sum(mor.AB_Pending_Terminations) AB_Pending_Terminations
from citeam.master_of_retention mor
where subs_week_and_year = 201501 and mor.AB_Pending_Terminations >0
group by subs_week_and_year;

Select subs_week_and_year,TA_Channel,sum(Turnaround_Saved+Turnaround_Not_Saved)
-- Select top 100 *
from CITeam.Combined_Retention_Report
where subs_week_and_year = 201501
group by subs_week_and_year,TA_Channel;
*/

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
from new_customers_last_2Yrs;

drop table if exists #new_customers_last_2Yrs_3;
Select *,row_number() over(order by rand_sample) Rand_Rnk
into #new_customers_last_2Yrs_3
from #new_customers_last_2Yrs_2;

Delete #new_customers_last_2Yrs_3 new_cust
from #new_customers_last_2Yrs_3 new_cust
     inner join
     #Activation_Vols act
     on new_cust.Rand_Rnk > act.New_Customers * true_sample_rate
        and act.subs_week_of_year = new_cust_subs_week_of_year;


insert into Forecast_Loop_Table_2
(account_number,end_date,subs_week_and_year,subs_week_of_year,weekid,DTV_Status_Code
,prem_segment,Simple_Segments,country,Affluence,package_desc,offer_length_DTV,HD_Segment


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


---------------------------------------------------------
/* ---- Temporary code to remove churned customers ----*/
---------------------------------------------------------
Drop table if exists #Churn_Custs;
Select account_number,DTV_Activation_Type,Time_Since_Last_TA_Call,CusCan_Forecast_Segment,SysCan_Forecast_Segment,CusCan_Churn,SysCan_Churn,
rand(number(*)*multiplier+7) as rand_Cuscan_churn,
rand(number(*)*multiplier+8) as rand_Syscan_churn
into #Churn_Custs
from Forecast_Loop_Table_2
where DTV_activation_type is null;

Drop table if exists #Churn_Custs_2;
Select *,
Case Time_Since_Last_TA_Call
when 'No Prev TA Calls' then 99999
when '0 Wks since last TA Call' then 2
when '48-52 Wks since last TA Call' then 6
when '53-60 Wks since last TA Call' then 7
when '61+ Wks since last TA Call' then 8
when '06-35 Wks since last TA Call' then 4
when '36-46 Wks since last TA Call' then 5
when '02-05 Wks since last TA Call' then 1
when '01 Wks since last TA Call' then 3
when '47 Wks since last TA Call' then 9
end Time_Since_Last_TA_Call_Rnk
,row_number() over(partition by CusCan_Forecast_Segment order by Time_Since_Last_TA_Call_Rnk, rand_Cuscan_churn) as CusCan_Churn_Rnk
,row_number() over(partition by SysCan_Forecast_Segment order by rand_Cuscan_churn desc,rand_Syscan_churn) as SysCan_Churn_Rnk
into #Churn_Custs_2
from #Churn_Custs;

commit;

-- Select *
-- from #Churn_Custs_2
-- where CusCan_Churn_Rnk <= 1000
-- order by CusCan_Forecast_Segment,DTV_Activation_Type desc,Time_Since_Last_TA_Call_Rnk, rand_Cuscan_churn;

Update Forecast_Loop_Table_2
Set CusCan = 1
from Forecast_Loop_Table_2 a
     inner join
     #Churn_Custs_2 b
     on a.account_number = b.account_number
where b.CusCan_Churn_Rnk < b.CusCan_Churn;


Update Forecast_Loop_Table_2
Set SysCan = 1
from Forecast_Loop_Table_2 a
     inner join
     #Churn_Custs_2 b
     on a.account_number = b.account_number
where b.SysCan_Churn_Rnk < b.SysCan_Churn;

---------------------------------------------------------
/* --------- Insert table into Output table -----------*/
---------------------------------------------------------
-- insert into output table
Insert into FORECAST_Looped_Sim_Output_Platform
Select *
-- into
-- drop table
-- FORECAST_Looped_Sim_Output_Platform
from Forecast_Loop_Table_2;


---------------------------------------------------------
/* ---- Temporary code to remove churned customers ----*/
---------------------------------------------------------
Delete Forecast_Loop_Table_2
where CusCan= 1;


Delete Forecast_Loop_Table_2
where SysCan = 1;


--------------------------------------------------------------------------
-- Update table for start of next loop -----------------------------------
--------------------------------------------------------------------------
Update Forecast_Loop_Table_2
set end_date = end_date + 7;

Update Forecast_Loop_Table_2
Set DTV_Status_Code = 'AB'
where DTV_AB > 0;

Update Forecast_Loop_Table_2
Set DTV_Status_Code = 'PC'
where TA_DTV_PC > 0
      or
      WC_DTV_PC > 0
      or
      TA_Sky_Plus_Save > 0
      or
      WC_Sky_Plus_Save > 0
      or
      Other_DTV_PC > 0;

set counter = counter+1;

Update Forecast_Loop_Table_2
Set Prev_offer_end_date_DTV = Curr_Offer_end_Date_Intended_DTV
where Curr_Offer_end_Date_Intended_DTV <= end_date;

Update Forecast_Loop_Table_2
Set Prev_offer_end_date_DTV = null
where Prev_offer_end_date_DTV < (end_date) - 53*7;


Update Forecast_Loop_Table_2
Set  curr_offer_start_date_DTV = end_date - 3
    ,Curr_Offer_end_Date_Intended_DTV = dateadd(month,10,end_date - 3) -- Default 10m offer
where DTV_Offer_Applied = 1;

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
,DTV_Tenure = case when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)   then 'M10'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
      else 'YNone'
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

Update Forecast_Loop_Table_2
Set DTV_Status_Code = 'AC'
where DTV_Status_Code = 'AB'
      and end_date - Last_AB_Dt > 51/2 -- /2 temp fix to account for reactivation before reaching end of PL
      and DTV_AB = 0;

Update Forecast_Loop_Table_2
Set DTV_Status_Code = 'AC'
where DTV_Status_Code = 'PC'
      and end_date - Last_TA_Call_Dt > 31/2 -- /2 temp fix to account for reactivation before reaching end of PL
      and TA_DTV_PC = 0
      and WC_DTV_PC = 0
      and Other_DTV_PC = 0;

Drop table if exists Forecast_Loop_Table;
Select account_number
,end_date
,subs_week_and_year
,subs_week_of_year
,weekid
,DTV_Status_Code
,prem_segment
,Simple_Segments
,country
,Affluence
,package_desc
,offer_length_DTV

,curr_offer_start_date_DTV
,Curr_Offer_end_Date_Intended_DTV
,Prev_offer_end_date_DTV
,Time_To_Offer_End_DTV

,curr_offer_start_date_BB
,Curr_Offer_end_Date_Intended_BB
,Prev_offer_end_date_BB
,Time_To_Offer_End_BB

,curr_offer_start_date_LR
,Curr_Offer_end_Date_Intended_LR
,Prev_offer_end_date_LR
,Time_To_Offer_End_LR

,DTV_BB_LR_offer_end_dt
,Time_To_Offer_End
,DTV_Tenure
,dtv_act_date
,Time_Since_Last_TA_call
,Last_TA_Call_dt
,Time_Since_Last_AB
,Last_AB_Dt
,Previous_AB_Count
,Previous_Abs
,CusCan_Forecast_Segment
,SysCan_Forecast_Segment
,DTV_Activation_Type
,dtv_latest_act_date
,dtv_first_act_date
,HD_segment

,rand_action_Cuscan
,rand_action_Syscan
,rand_TA_Vol
,rand_WC_Vol
,rand_TA_Save_Vol
,rand_WC_Save_Vol
,rand_TA_DTV_Offer_Applied
,rand_NonTA_DTV_Offer_Applied
,rand_TA_DTV_PC_Vol
,rand_WC_DTV_PC_Vol
,rand_Other_DTV_PC_Vol

into Forecast_Loop_Table
from Forecast_Loop_Table_2;

END Loop;

RETURN;
-- Select * into FORECAST_Looped_Sim_Output_Platform_201501_V13 from FORECAST_Looped_Sim_Output_Platform;

-- grant select on FORECAST_Looped_Sim_Output_Platform_201601_V11 to public

Select top 10000 *
from menziesm.FORECAST_Looped_Sim_Output_Platform
where subs_week_and_year = 201604
        and TA_Call_Cust = 1
        and DTV_Status_Code = 'AC'
order by CusCan_Forecast_Segment,TA_Call_Cust,rand_TA_DTV_PC_Vol

-- Select top 100 * from FORECAST_Looped_Sim_Output_Platform
-- Select top 100 * from simmonsr. REVISED_REDDY_q3_forecast_base

Select a.*,b.model_segment,b.new_segment1
into FORECAST_Looped_Sim_Output_Platform_REVISED_REDDY_Q3
from FORECAST_Looped_Sim_Output_Platform a
     left join
     simmonsr. REVISED_REDDY_q3_forecast_base b
     on a.account_number = b.account_number


/*
sp_columns 'FORECAST_Looped_Sim_Output_Platform'

select subs_week_and_year,count(*)*4 Customers,sum(TA_Call_Count)*4 TA_Events
from FORECAST_Looped_Sim_Output_Platform
where dtv_activation is null
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
