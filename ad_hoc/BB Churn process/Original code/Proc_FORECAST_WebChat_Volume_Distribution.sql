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


