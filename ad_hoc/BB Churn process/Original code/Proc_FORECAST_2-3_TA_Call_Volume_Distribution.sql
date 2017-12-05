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


