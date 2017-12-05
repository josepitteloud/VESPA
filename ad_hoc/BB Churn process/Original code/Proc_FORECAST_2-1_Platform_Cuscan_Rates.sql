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


