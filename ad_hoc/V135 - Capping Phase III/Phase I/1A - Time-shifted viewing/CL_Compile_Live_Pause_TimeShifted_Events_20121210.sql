/*


Playback speed information for time-shifted viewing events
Looking at live -> pause -> time-shifted events

Author: Claudio Lima
Date: 10/12/2012

*/

-- Look at a sample of data of time shifted events
select top 10000 *
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event = 'Sky+ time-shifted viewing event'

-- breakdown by playback speed
select playback_speed
        ,playback_type
        ,reported_playback_speed
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event = 'Sky+ time-shifted viewing event'
group by playback_speed
        ,playback_type
        ,reported_playback_speed
order by playback_speed
        ,playback_type
        ,reported_playback_speed
/*
playback_speed  playback_type   reported_playback_speed
1.00            normal          2
*/

-- breakdown by playback speed for non viewing events
-- here lies what we're looking for
select playback_speed
        ,playback_type
        ,reported_playback_speed
        ,count(*)
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event = 'Non viewing event'
group by playback_speed
        ,playback_type
        ,reported_playback_speed
order by playback_speed
        ,playback_type
        ,reported_playback_speed
/*
playback_speed  playback_type   reported_playback_speed count()
-30.00  rewind playback         -60     1183618
-18.00  rewind playback         -36     32
-12.00  rewind playback         -24     839435
-7.50   rewind playback         -15     27
-6.00   rewind playback         -12     1489574
-2.50   rewind playback         -5      15
-2.00   rewind playback         -4      2327048
0.00    paused                  0       12547635
.50     slow motion playback    1       29472
1.00    live                            34803495
2.00    fast forward playbac    4       1835382
6.00    fast forward playbac    12      4218138
12.00   fast forward playbac    24      5379945
14.00   fast forward playbac    28      8
15.00   fast forward playbac    30      81
18.00   fast forward playbac    36      152
30.00   fast forward playbac    60      14422211
*/

-- create a table with non viewing events
select subscriber_id
        ,EVENT_START_DATE_TIME_UTC
        ,EVENT_END_DATE_TIME_UTC
       ,playback_type
       ,Duration
into VEA_sample_5_11Nov_NonViewing_Events
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event = 'Non viewing event'
and playback_type in ('paused','rewind playback')
and subscriber_id is not null
group by subscriber_id
        ,EVENT_START_DATE_TIME_UTC
        ,EVENT_END_DATE_TIME_UTC
        ,Duration
        ,playback_type
-- 17,095,129

commit

-- created indexes to speedup join query below
create hg index idx1 on VEA_sample_5_11Nov_NonViewing_Events(   subscriber_id
                                                                ,EVENT_END_DATE_TIME_UTC
                                                                ,playback_type)

create hg index idx1 on VEA_sample_5_11Nov_Time_Shifted_Events(  subscriber_id
                                                                ,EVENT_START_DATE_TIME_UTC)

-- create flag to indicate which time-shifted events start after a pause/rewind
alter table VEA_sample_5_11Nov_Time_Shifted_Events add After_Pause tinyint

-- update flag
update VEA_sample_5_11Nov_Time_Shifted_Events
set After_Pause = 1
from VEA_sample_5_11Nov_Time_Shifted_Events TSE
inner join VEA_sample_5_11Nov_NonViewing_Events NVE
on TSE.subscriber_id = NVE.subscriber_id
and TSE.EVENT_START_DATE_TIME_UTC = NVE.EVENT_END_DATE_TIME_UTC
where NVE.playback_type in ('paused','rewind playback')
and TSE.viewing_event_type = 'VOSDAL'

select count(*) from VEA_sample_5_11Nov_Time_Shifted_Events where After_Pause = 1 -- 5,880,171

-- create a table with live viewing events
select subscriber_id
        ,EVENT_START_DATE_TIME_UTC
        ,EVENT_END_DATE_TIME_UTC
       ,Duration
into VEA_sample_5_11Nov_Live_Viewing_Events
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 00:00:00'
and EVENT_START_DATE_TIME_UTC <= '2012-11-11 23:59:59'
and type_of_viewing_event not in ('Sky+ time-shifted viewing event','Non viewing event')
and subscriber_id is not null
group by subscriber_id
        ,EVENT_START_DATE_TIME_UTC
        ,EVENT_END_DATE_TIME_UTC
        ,Duration
-- 110,614,599 Row(s) affected

commit

-- create index to speedup join query below
create hg index idx1 on VEA_sample_5_11Nov_Live_Viewing_Events(  subscriber_id
                                                                ,EVENT_END_DATE_TIME_UTC)

create hg index idx1 on VEA_sample_5_11Nov_NonViewing_Events(   subscriber_id
                                                                ,EVENT_START_DATE_TIME_UTC
                                                                ,playback_type)

-- add flag variable to indicate which pauses come after live events
alter table VEA_sample_5_11Nov_NonViewing_Events add After_Live tinyint

-- update the flag (one day at the time otherwise query will exceed temp memory)
update VEA_sample_5_11Nov_NonViewing_Events
set After_Live = 1
from VEA_sample_5_11Nov_NonViewing_Events NVE
inner join VEA_sample_5_11Nov_Live_Viewing_Events LVE
on NVE.subscriber_id = LVE.subscriber_id
and NVE.EVENT_START_DATE_TIME_UTC = LVE.EVENT_END_DATE_TIME_UTC
--where day(NVE.EVENT_START_DATE_TIME_UTC) = 11
-- 491658+996630+1503970+2014397+2539250+3163625+3806985

select count(*) from VEA_sample_5_11Nov_NonViewing_Events where  After_Live = 1 -- 3,806,985

drop index VEA_sample_5_11Nov_NonViewing_Events.idx3

create hg index idx3 on VEA_sample_5_11Nov_NonViewing_Events(   subscriber_id
                                                                ,EVENT_END_DATE_TIME_UTC
                                                                ,After_Live)

-- add flag to indicate live -> pause/rewind -> time-shifted events
alter table VEA_sample_5_11Nov_Time_Shifted_Events add After_Live_Pause tinyint

-- update flag
update VEA_sample_5_11Nov_Time_Shifted_Events
set After_Live_Pause = 1
from VEA_sample_5_11Nov_Time_Shifted_Events TSE
inner join VEA_sample_5_11Nov_NonViewing_Events NVE
on TSE.subscriber_id = NVE.subscriber_id
and TSE.EVENT_START_DATE_TIME_UTC = NVE.EVENT_END_DATE_TIME_UTC
where TSE.viewing_event_type = 'VOSDAL'
and NVE.After_Live = 1
and TSE.After_Pause = 1

select count(*) from VEA_sample_5_11Nov_Time_Shifted_Events where After_Live_Pause = 1 -- 1,429,393



--------------------------------------------
-- Profile live->pause->time-shifted events
--------------------------------------------

-- Number of events and percentage of VOSDAL for each air-view delta
select air_view_delta
       ,sum(case when After_Live_Pause = 1 then 1 else 0 end) as Num_Events_After_Live_Pause
       ,count(*) as Num_Events
       ,Num_Events_After_Live_Pause*1.0/Num_Events as Percentage_Events_After_Live_Pause
from    (
        select cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int) as air_view_delta
                ,After_live_pause
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        where Viewing_Event_Type = 'VOSDAL'
        ) t
where air_view_delta >= 0
group by air_view_delta
order by air_view_delta






