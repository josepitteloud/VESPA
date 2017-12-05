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


