/*
Analyse difference in viewing events duration for:

Playback (Vx > Tx)
vs.
VOSDAL (Vx = Tx)

Author: Claudio Lima
Date: 30/11/2012

*/

-- Create a week worth of data to work with
-- if object_id('VEA_sample_5_12Nov_Time_Shifted_Events_24h') is not null drop table VEA_sample_5_12Nov_Time_Shifted_Events_24h
select subscriber_id
        ,EVENT_START_DATE_TIME_UTC
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
        ,EVENT_END_DATE_TIME_UTC
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,min(BROADCAST_START_DATE_TIME_UTC) as BROADCAST_START_DATE_TIME_UTC_MIN
       ,dateformat(min(BROADCAST_START_DATE_TIME_UTC),'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(min(BROADCAST_START_DATE_TIME_UTC),'HH:MM:SS') as BROADCAST_START_TIME
       ,Duration
       ,case
                when BROADCAST_START_DATE = EVENT_START_DATE
                then 'VOSDAL'
                else 'Playback'
        end as Viewing_Event_Type
        ,case
                when datepart(weekday,EVENT_START_DATE)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE)=7 then 'Sat'
        end as EVENT_START_DOW
        ,case
                when datepart(weekday,BROADCAST_START_DATE)=1 then 'Sun'
                when datepart(weekday,BROADCAST_START_DATE)=2 then 'Mon'
                when datepart(weekday,BROADCAST_START_DATE)=3 then 'Tue'
                when datepart(weekday,BROADCAST_START_DATE)=4 then 'Wed'
                when datepart(weekday,BROADCAST_START_DATE)=5 then 'Thu'
                when datepart(weekday,BROADCAST_START_DATE)=6 then 'Fri'
                when datepart(weekday,BROADCAST_START_DATE)=7 then 'Sat'
        end as BROADCAST_START_DOW
        ,hour(EVENT_START_TIME) as EVENT_START_HOUR
        ,case
                when EVENT_START_HOUR between 4  and 14 then '04-14'
                when EVENT_START_HOUR between 15 and 19 then '15-19'
                when EVENT_START_HOUR between 20 and 22 then '20-22'
                when EVENT_START_HOUR in (23,0,1,2,3)   then '23-03'
        end as EVENT_START_PERIOD
        ,hour(BROADCAST_START_TIME) as BROADCAST_START_HOUR
        ,case
                when BROADCAST_START_HOUR between 4  and 14 then '04-14'
                when BROADCAST_START_HOUR between 15 and 19 then '15-19'
                when BROADCAST_START_HOUR between 20 and 22 then '20-22'
                when BROADCAST_START_HOUR in (23,0,1,2,3)   then '23-03'
        end as BROADCAST_START_PERIOD
into VEA_sample_5_11Nov_Time_Shifted_Events
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event = 'Sky+ time-shifted viewing event'
and subscriber_id is not null
group by subscriber_id
        ,EVENT_START_DATE_TIME_UTC
        ,EVENT_END_DATE_TIME_UTC
        ,Duration
-- 51,657,334 Row(s) affected

grant all on VEA_sample_5_11Nov_Time_Shifted_Events to igonorp
commit;
select top 10* from limac.VEA_sample_5_11Nov_Time_Shifted_Events
--------------------------------------------------
-- Profile viewing data used on analysis
--------------------------------------------------
select EVENT_START_DATE,count(*)
from VEA_sample_5_11Nov_Time_Shifted_Events
group by EVENT_START_DATE
order by EVENT_START_DATE
/*
2012-11-05      7108331
2012-11-06      6922999
2012-11-07      6645547
2012-11-08      6729919
2012-11-09      7067000
2012-11-10      7970762
2012-11-11      9212776
*/

select BROADCAST_START_DATE,count(*)
from VEA_sample_5_11Nov_Time_Shifted_Events
group by BROADCAST_START_DATE
order by BROADCAST_START_DATE
/*
                542392
2012-05-26      3005
2012-05-27      3608
...
2012-11-05      5755950
2012-11-06      5721891
2012-11-07      5449804
2012-11-08      5259983
2012-11-09      4915738
2012-11-10      5692813
2012-11-11      3539870
*/

-- Profile events by duration
select Viewing_Event_Type
        ,case
                when Duration between 0 and 6 then '0-6'
                else '>6'
        end as Short_long_Duration
        ,count(*)
from VEA_sample_5_11Nov_Time_Shifted_Events
group by Viewing_Event_Type
        ,case
                when Duration between 0 and 6 then '0-6'
                else '>6'
        end
order by Viewing_Event_Type
        ,case
                when Duration between 0 and 6 then '0-6'
                else '>6'
        end
/*
Playback        0-6     4792935
Playback        >6      26996342
VOSDAL          0-6     2401119
VOSDAL          >6      17466938
*/

-- when air time is later than viewing time
select Viewing_Event_Type,count(*)
from VEA_sample_5_11Nov_Time_Shifted_Events
where BROADCAST_START_DATE_TIME_UTC_MIN > EVENT_START_DATE_TIME_UTC
group by Viewing_Event_Type
/*
VOSDAL          232
Playback        1
*/

-- Breakdown by difference of days between air and view dates
select datediff(day,BROADCAST_START_DATE,EVENT_START_DATE) as air_view_delta_days
        ,count(*) as Num_Events
from VEA_sample_5_11Nov_Time_Shifted_Events
where BROADCAST_START_DATE is not null
and air_view_delta_days >= 1
group by air_view_delta_days
order by air_view_delta_days

-- Breakdown by day of the week
select Viewing_Event_Type
        ,EVENT_START_DOW
        ,count(*) 'Num_Events'
from VEA_sample_5_11Nov_Time_Shifted_Events
group by Viewing_Event_Type
        ,EVENT_START_DOW
order by Viewing_Event_Type
        ,EVENT_START_DOW

-- Breakdown by viewing start hour
select Viewing_Event_Type
        ,EVENT_START_HOUR
        ,count(*) 'Num_Events'
from VEA_sample_5_11Nov_Time_Shifted_Events
group by Viewing_Event_Type
        ,EVENT_START_HOUR
order by Viewing_Event_Type
        ,EVENT_START_HOUR

--------------------------------------------------
-- Profile event duration for Playback vs. VOSDAL
--------------------------------------------------

-- Let's makes this faster
create hg index idx1_hg on VEA_sample_5_11Nov_Time_Shifted_Events(  Viewing_Event_Type
                                                                        ,EVENT_START_DOW
                                                                        ,EVENT_START_HOUR
                                                                        ,Duration)

select  Viewing_Event_Type
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,ntiles
        ,median(Duration)/60.0 'Duration_Median_min'
from (
select Viewing_Event_Type
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,Duration
        ,ntile(200) over (partition by Viewing_Event_Type
                                       ,EVENT_START_DOW
                                       ,EVENT_START_HOUR
                         order by Duration) as ntiles
from VEA_sample_5_11Nov_Time_Shifted_Events
) t
group by Viewing_Event_Type
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,ntiles
order by Viewing_Event_Type
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,ntiles

-- Profile VOSDAL events based on the difference between air and viewing time
select air_view_delta
        ,ntiles
        ,median(Duration)/60.0 'Duration_Median_min'
from
(
select air_view_delta
        ,Duration
        ,ntile(200) over (partition by air_view_delta order by Duration) as ntiles
from    (
        select cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int) as air_view_delta
                ,Duration
        from VEA_sample_5_11Nov_Time_Shifted_Events
        where Viewing_Event_Type = 'VOSDAL'
        ) t1
where air_view_delta >= 0
) t2
group by air_view_delta
        ,ntiles
order by air_view_delta
        ,ntiles

------------------------------------------------------------------
-- For VOSDAL look at the difference between air and viewing time
------------------------------------------------------------------

-- difference in hours
select air_view_delta
        ,count(*) as Num_Events
from (
select cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int) as air_view_delta
from VEA_sample_5_11Nov_Time_Shifted_Events
where Viewing_Event_Type = 'VOSDAL'
) t
group by air_view_delta
order by air_view_delta

-- difference in minutes for the first hour
select air_view_delta
        ,count(*) as Num_Events
from (
select cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/60.0 as int) as air_view_delta
from VEA_sample_5_11Nov_Time_Shifted_Events
where Viewing_Event_Type = 'VOSDAL'
and cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
) t
group by air_view_delta
order by air_view_delta

------------------------------------------------
-- DOW/hour relation between broadcast and view
------------------------------------------------

-- broadcast/viewing day of the week distribution
select BROADCAST_START_DOW
        ,EVENT_START_DOW
        ,count(*) as Num_Events
from VEA_sample_5_11Nov_Time_Shifted_Events
where BROADCAST_START_DOW is not null
and Viewing_Event_Type = 'Playback'
group by BROADCAST_START_DOW
        ,EVENT_START_DOW
order by BROADCAST_START_DOW
        ,EVENT_START_DOW

-- broadcast/viewing day of the week distribution for Playback only
select BROADCAST_START_DOW
        ,EVENT_START_DOW
        ,count(*) as Num_Events
from VEA_sample_5_11Nov_Time_Shifted_Events
where BROADCAST_START_DOW is not null
group by BROADCAST_START_DOW
        ,EVENT_START_DOW
order by BROADCAST_START_DOW
        ,EVENT_START_DOW

-- broadcast/viewing hour distribution
select BROADCAST_START_HOUR
        ,EVENT_START_HOUR
        ,count(*) as Num_Events
from VEA_sample_5_11Nov_Time_Shifted_Events
where BROADCAST_START_HOUR is not null
group by BROADCAST_START_HOUR
        ,EVENT_START_HOUR
order by BROADCAST_START_HOUR
        ,EVENT_START_HOUR

-- broadcast/viewing hour distribution for VOSDAL only
select BROADCAST_START_HOUR
        ,EVENT_START_HOUR
        ,count(*) as Num_Events
from VEA_sample_5_11Nov_Time_Shifted_Events
where BROADCAST_START_HOUR is not null
and Viewing_Event_Type = 'VOSDAL'
group by BROADCAST_START_HOUR
        ,EVENT_START_HOUR
order by BROADCAST_START_HOUR
        ,EVENT_START_HOUR

-- Look at number od subscribers with long time-shifted events
select   count(distinct subscriber_id) as Total
        ,count (distinct (case when duration >  1*24*60*60 then subscriber_id else null end)) as _1Day_ON
        ,count (distinct (case when duration >  2*24*60*60 then subscriber_id else null end)) as _2Days_ON
        ,count (distinct (case when duration >  3*24*60*60 then subscriber_id else null end)) as _3Days_ON
        ,count (distinct (case when duration >  7*24*60*60 then subscriber_id else null end)) as _1Week_ON
        ,count (distinct (case when duration > 14*24*60*60 then subscriber_id else null end)) as _2Weeks_ON
        ,count (distinct (case when duration > 28*24*60*60 then subscriber_id else null end)) as _4Weeks_ON
from VEA_sample_5_11Nov_Time_Shifted_Events

-------------------------------------------------------
-- Breakdown split of time-shifted events
-------------------------------------------------------

select top 1000 * from VEA_sample_5_11Nov_Time_Shifted_Events

-- cut-off at 12AM
select case
            when date(EVENT_START_DATE_TIME_UTC) = date(BROADCAST_START_DATE_TIME_UTC_MIN) 
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                    then 'VOSDAL <1h'
                    else 'VOSDAL 1-24h'
                 end
            else 'Playback (+1 day)'
        end as Viewing_Type_Detailed_24h
        ,EVENT_START_HOUR
        ,EVENT_START_DOW
        ,BROADCAST_START_HOUR
        ,BROADCAST_START_DOW
        ,count(*) as Number_Events
        ,sum(Duration)*1.0/3600 as Viewing_Hours    
from VEA_sample_5_11Nov_Time_Shifted_Events
where duration > 6
group by case
            when date(EVENT_START_DATE_TIME_UTC) = date(BROADCAST_START_DATE_TIME_UTC_MIN) 
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                    then 'VOSDAL <1h'
                    else 'VOSDAL 1-24h'
                 end
            else 'Playback (+1 day)'
        end
        ,EVENT_START_HOUR
        ,EVENT_START_DOW
        ,BROADCAST_START_HOUR
        ,BROADCAST_START_DOW

-- cut-off at 6AM
select case
            when date(dateadd(hh, -6, BROADCAST_START_DATE_TIME_UTC_MIN)) = date(dateadd(hh, -6, EVENT_START_DATE_TIME_UTC)) 
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                    then 'VOSDAL <1h'
                    else 'VOSDAL 1-24h'
                 end
            else 'Playback (+1 day)'
        end as Viewing_Type_Detailed_06h
        ,EVENT_START_HOUR
        ,EVENT_START_DOW
        ,BROADCAST_START_HOUR
        ,BROADCAST_START_DOW
        ,count(*)  
        ,sum(Duration)*1.0/3600 as Viewing_Hours        
from VEA_sample_5_11Nov_Time_Shifted_Events
where duration > 6
group by case
            when date(dateadd(hh, -6, BROADCAST_START_DATE_TIME_UTC_MIN)) = date(dateadd(hh, -6, EVENT_START_DATE_TIME_UTC)) 
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_END_DATE_TIME_UTC)/(60.0*60.0) as int) = 0
                    then 'VOSDAL <1h'
                    else 'VOSDAL 1-24h'
                 end
            else 'Playback (+1 day)'
        end
        ,EVENT_START_HOUR
        ,EVENT_START_DOW
        ,BROADCAST_START_HOUR
        ,BROADCAST_START_DOW



select count(*) from VEA_sample_5_11Nov_Time_Shifted_Events --  51,657,334
select count(*) from VEA_sample_5_11Nov_Live_Viewing_Events -- 110,614,599
select count(*) from VEA_sample_5_11Nov_Time_Shifted_Events where duration > 6 -- 44,463,280
select count(*) from VEA_sample_5_11Nov_Live_Viewing_Events where duration > 6 -- 101,204,427
select sum(duration)*1.0/3600 from VEA_sample_5_11Nov_Time_Shifted_Events where duration > 6 --  6,073,911
select sum(duration)*1.0/3600 from VEA_sample_5_11Nov_Live_Viewing_Events where duration > 6 -- 42,912,261
