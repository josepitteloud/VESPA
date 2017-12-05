/*

--- Determine events when STB goes into standby ---

S01. Look at a small number of subscribers for their weekly viewing
S02. Get all events for a day with duration similar to auto standby times
s03. Look at distribution of type of viewing event and duration for a day

Author: Claudio Lima

v0.1 - 30/11/2012

*/

-----------------------------------------------------------------------
-- S01. Look at a small number of subscribers for their weekly viewing
-----------------------------------------------------------------------

-- Look at a number of subscribers for a week period
select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,Duration
       ,channel_name
       ,next_channel_name
into VEA_sample_12sub_1week
from sk_prod.VESPA_EVENTS_ALL
where subscriber_id in (31019,105090,138222,266193,394215,630042
                        ,758777,1259949,1728422,1956063,2574278,2866728)
and panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 04:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-12 03:59:59'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration
-- 9549 Row(s) affected

-- Look at a sample of subscribers that have events around 243 min
select top 10 subscriber_id
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 04:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-06 03:59:59'
and Duration between 243*60-180 and 243*60+60

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
into VEA_sample_10sub_1day_243min
from sk_prod.VESPA_EVENTS_ALL
where subscriber_id in (6140460,437891,1013425,305232,6580450
                       ,442879,3049353,914929,1313051,1592021)
and panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 04:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-06 03:59:59'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration
-- 931

-----------------------------------------------------------------------------
-- S02. Get all events for a day with duration similar to auto standby times
-----------------------------------------------------------------------------
select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,case
                when Duration between 122*60-180 and 122*60+180
                then 122
                when Duration between 183*60-180 and 183*60+180
                then 183
                when Duration between 243*60-180 and 243*60+180
                then 243
                else 0
       end as Duration_Slot
into VEA_sample_1day_autostandby_periods
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 04:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-06 03:59:59'
and     (
        Duration between 122*60-180 and 122*60+180 -- 122 +/- 3 min
        OR
        Duration between 183*60-180 and 183*60+180 -- 183 +/- 3 min
        OR
        Duration between 243*60-180 and 243*60+180 -- 243 +/- 3 min
        )
-- 1236238 Row(s) affected

-- Look at distribution of type of viewing event wo duplicates
select type_of_viewing_event,count(*)
from (  select distinct subscriber_id
                        ,type_of_viewing_event
                        ,EVENT_START_DATE
                        ,EVENT_START_Time
                        ,Duration
        from VEA_sample_1day_autostandby_periods
) t
group by type_of_viewing_event
order by count(*) desc
/*
TV Channel Viewing              178355
HD Viewing Event                39624
Non viewing event               22236
Digital Radio Viewing           2508
                                1917
Sky+ time-shifted viewing event 1621
Other Service Viewing Event     72
*/


-----------------------------------------------------------------------------
-- s03. Look at distribution of type of viewing event and duration for a day
-----------------------------------------------------------------------------

select  subscriber_id
       ,type_of_viewing_event
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,Duration
into VEA_sample_5Nov_EventStart_Duration
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 04:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-06 03:59:59'
-- 42,312,856

create hg index idx_hg_subscriber_id on VEA_sample_5Nov_EventStart_Duration(subscriber_id)
create lf index idx_lf_type_of_viewing_event on VEA_sample_5Nov_EventStart_Duration(type_of_viewing_event)

-- with duplicates
select type_of_viewing_event
       ,round(Duration/60.0,0)
       ,count(*)
from VEA_sample_5Nov_EventStart_Duration
group by type_of_viewing_event,round(Duration/60.0,0)
order by type_of_viewing_event,round(Duration/60.0,0)

-- without duplicates
select type_of_viewing_event
       ,round(Duration/60.0,0)
       ,count(*)
from    (
        select distinct subscriber_id
       ,type_of_viewing_event
       ,EVENT_START_DATE
       ,EVENT_START_TIME
       ,EVENT_END_DATE
       ,EVENT_END_TIME
       ,Duration
        from VEA_sample_5Nov_EventStart_Duration
        ) t
group by type_of_viewing_event,round(Duration/60.0,0)
order by type_of_viewing_event,round(Duration/60.0,0)

-- including event start hour
select type_of_viewing_event
        ,hour(EVENT_START_TIME) as event_start_hour
       ,round(Duration/60.0,0) as duration_min
       ,count(*)
from    (
        select distinct subscriber_id
       ,type_of_viewing_event
       ,EVENT_START_DATE
       ,EVENT_START_TIME
       ,EVENT_END_DATE
       ,EVENT_END_TIME
       ,Duration
        from VEA_sample_5Nov_EventStart_Duration
        ) t
group by type_of_viewing_event,event_start_hour,duration_min
order by type_of_viewing_event,event_start_hour,duration_min


