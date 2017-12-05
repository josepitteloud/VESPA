/*********************************************************************************************************************
Analysis of the Refinement of capping for Time-Shifted events
(Live-Pause,
 ***VOSDAL,
    ***Timeshifted,
      *** and Showcase)


Analyst: Patrick Igonor
Lead Analyst: Claudio Lima
Date: 06/11/2013 (Revised)

***********************************************************************************************************************/

-- Create a 2 weeks worth of data to execute the Project
-- if object_id('Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events') is not null drop table VEA_sample_23rdSept_06thOct_Viewing_Event
if object_id('Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New') is not null drop table Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New
select  subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,EVENT_END_DATE_TIME_UTC
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,live_recorded
       ,duration
       ,channel_name
       ,genre_description
       ,sub_genre_description
       ,type_of_viewing_event
       ,case
                when datepart(weekday,EVENT_START_DATE)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE)=7 then 'Sat'
        end as EVENT_START_DOW
        ,hour(EVENT_START_TIME) as EVENT_START_HOUR
        ,case
                when EVENT_START_HOUR between 4  and 14 then '04-14'
                when EVENT_START_HOUR between 15 and 19 then '15-19'
                when EVENT_START_HOUR between 20 and 22 then '20-22'
                when EVENT_START_HOUR in (23,0,1,2,3)   then '23-03'
        end as EVENT_START_PERIOD
       ,min(BROADCAST_START_DATE_TIME_UTC) over (partition by Subscriber_Id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC) as BROADCAST_START_DATE_TIME_UTC_min
       ,case
            when live_recorded = 'RECORDED' and service_key in (4094,4095,4096,4097,4098) then 4
            when live_recorded = 'LIVE' then 0
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) = date(BROADCAST_START_DATE_TIME_UTC_min)
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_min,EVENT_START_DATE_TIME_UTC)/3600.0 as int) = 0
                    then 1
                    else 2 end
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) <> date(BROADCAST_START_DATE_TIME_UTC_min) then 3
        end as live_timeshifted_events
       ,account_number
       ,playback_speed
       ,playback_type
       ,reported_playback_speed
       ,service_key
into Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New
from sk_prod.vespa_dp_prog_VIEWED_201309
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-09-23 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2013-09-30 23:59:59'
and subscriber_id is not null
and account_number is not null
and duration > 6

union all

select  subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,EVENT_END_DATE_TIME_UTC
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,live_recorded
       ,duration
       ,channel_name
       ,genre_description
       ,sub_genre_description
       ,type_of_viewing_event
       ,case
                when datepart(weekday,EVENT_START_DATE)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE)=7 then 'Sat'
        end as EVENT_START_DOW
        ,hour(EVENT_START_TIME) as EVENT_START_HOUR
        ,case
                when EVENT_START_HOUR between 4  and 14 then '04-14'
                when EVENT_START_HOUR between 15 and 19 then '15-19'
                when EVENT_START_HOUR between 20 and 22 then '20-22'
                when EVENT_START_HOUR in (23,0,1,2,3)   then '23-03'
        end as EVENT_START_PERIOD
       ,min(BROADCAST_START_DATE_TIME_UTC) over (partition by Subscriber_Id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC) as BROADCAST_START_DATE_TIME_UTC_min
       ,case
            when live_recorded = 'RECORDED' and service_key in (4094,4095,4096,4097,4098) then 4
            when live_recorded = 'LIVE' then 0
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) = date(BROADCAST_START_DATE_TIME_UTC_min)
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_min,EVENT_START_DATE_TIME_UTC)/3600.0 as int) = 0
                    then 1
                    else 2 end
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) <> date(BROADCAST_START_DATE_TIME_UTC_min) then 3
        end as live_timeshifted_events
       ,account_number
       ,playback_speed
       ,playback_type
       ,reported_playback_speed
       ,service_key
from sk_prod.vespa_dp_prog_VIEWED_current
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-10-01 00:00:00'
and EVENT_START_DATE_TIME_UTC   <  '2013-10-07 00:00:00'
and subscriber_id is not null
and account_number is not null
and duration > 6
--379,377,151 Row(s) affected

--Granting Priviledges --
 grant all on Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New to limac;
 commit;


--Creating indexes to speed things up--
create hg index idx1_hg on Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New(live_timeshifted_events,EVENT_START_DOW,EVENT_START_HOUR,duration);

-- 2a: Calculate ntiles for Viewing_Event_Type and also by DOW and start hours--

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   (  select duration
        ,live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Hour order by duration) as ntiles
        from Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New
        ) VEA_VA
where live_timeshifted_events <> 0
group by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour, ntiles
order by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour, ntiles


--2b: Calculate ntiles for Viewing_Event_Type and also by DOW and start period--(Not really necessary)

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   (  select duration
        ,live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Period order by duration) as ntiles
        from Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New
        ) VEA_VA
where live_timeshifted_events <> 0
group by  live_timeshifted_events, Event_Start_DOW, Event_Start_Period, ntiles
order by  live_timeshifted_events, Event_Start_DOW, Event_Start_Period, ntiles


--2c: Calculate ntiles for live_timeshifted_events alone WITHOUT PARTITIONING BY DOW AND EVENT START HOUR

select   live_timeshifted_events
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   (  select duration
        ,live_timeshifted_events
        ,ntile(200) over (partition by live_timeshifted_events order by duration) as ntiles
        from Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New
        ) VEA_VA
where live_timeshifted_events <> 0
group by  live_timeshifted_events, ntiles
order by  live_timeshifted_events, ntiles

--Threshold Determination ----

--Pulling the partition of viewing events type, Event start DOW and Period into a table (VEA_sample_23Sept_06Oct_Time_Shifted_Events)--

select   duration
        ,live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Period order by duration) as ntiles
into VEA_sample_23Sept_06Oct_Time_Shifted_Events_New_ntiles
from Vespa_dp_prog_VIEWED_23Sept_06Oct_Viewing_Events_New
where live_timeshifted_events <> 0

----------------------------------------------------------------

--*****Ntiles selection based on Viewing_Event_Time, Day of the week and Start Period
--Ntile selection at 55 minutes----

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_55min
        ,'55mins'
from VEA_sample_23Sept_06Oct_Time_Shifted_Events_New_ntiles
where duration >=55*60
group by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
order by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period

-----------------------------------------------------------
--Ntile selection at 60 minutes----

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_60min
        ,'60mins'
from VEA_sample_23Sept_06Oct_Time_Shifted_Events_New_ntiles
where duration >=60*60
group by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
order by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period

-------------------------------------------------------------
--Ntile selection at 65 minutes----
select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_65min
        ,'65mins'
from VEA_sample_23Sept_06Oct_Time_Shifted_Events_New_ntiles
where duration >=65*60
group by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
order by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period

--------------------------------------------------------------
--Ntile selection at 70 minutes--

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_70min
        ,'70mins'
from VEA_sample_23Sept_06Oct_Time_Shifted_Events_New_ntiles
where duration >=70*60
group by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period
order by live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Period

