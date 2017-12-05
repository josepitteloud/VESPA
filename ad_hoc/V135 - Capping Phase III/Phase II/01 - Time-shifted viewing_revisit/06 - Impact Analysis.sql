--****Impact Analysis - Comparing the Current Capping to the New one and checking the difference between the two--*****

--23rd of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130923)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130923
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130923)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130923
group by Timeshifting
order by Total_Viewing desc

--24th of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130924)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130924
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130924)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130924
group by Timeshifting
order by Total_Viewing desc

--25th of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130925)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130925
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130925)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130925
group by Timeshifting
order by Total_Viewing desc

--26th of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130926)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130926
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130926)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130926
group by Timeshifting
order by Total_Viewing desc

--27th of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130927)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130927
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130927)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130927
group by Timeshifting
order by Total_Viewing desc

--28th of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130928)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130928
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130928)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130928
group by Timeshifting
order by Total_Viewing desc

--29th of September
--Current
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130929)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130929
group by Timeshifting
order by Total_Viewing desc

--New
declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130929)

select Timeshifting
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130929
group by Timeshifting
order by Total_Viewing desc

---------------Looking at the distribution of total duration based on the Viewing Start Hour for Live Events
--23rd of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130923)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130923
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130923)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130923
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--24th of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130924)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130924
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130924)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130924
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--25th of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130925)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130925
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130925)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130925
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--26th of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130926)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130926
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130926)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130926
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--27th of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130927)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130927
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130927)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130927
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--28th of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130928)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130928
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130928)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130928
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--29th of September
--Current

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from current_vespa_daily_augs_20130929)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from current_vespa_daily_augs_20130929
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour

--New

declare @Distinct_Sub_Ids int
set @Distinct_Sub_Ids = (
select count(distinct Subscriber_Id)from New_Vespa_Daily_Augs_20130929)

select hour(Viewing_Starts) as Viewing_hour
      ,sum(Viewing_Duration)as Total_Viewing
      ,(Total_Viewing*1.0)/3600/@Distinct_Sub_Ids as Viewing_Avg_Hours
from New_Vespa_Daily_Augs_20130929
where Timeshifting = 'LIVE_Events'
group by Viewing_hour
order by Viewing_hour


