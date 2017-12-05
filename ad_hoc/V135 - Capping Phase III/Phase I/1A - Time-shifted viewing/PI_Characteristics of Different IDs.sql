/*

Look at specific examples

*/


--
select top 5 *
from limac.VEA_sample_5_11Nov_Time_Shifted_Events
where duration between 400*60 and 700*60
and cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int)=0
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 5244527
and EVENT_START_DATE_TIME_UTC >= '2012-11-08 21:38:00.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-09 11:17:57.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration




-----------------------------------------------------------------------------------
select top 5 *
from limac.VEA_sample_5_11Nov_Time_Shifted_Events
where duration between 600*60 and 1000*60
and cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int)=23
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 7820111
and EVENT_START_DATE_TIME_UTC >= '2012-11-07 22:49:43.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-08 17:22:02.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration




---------------------------------------------------------------------------


-- 2 more examples for Playback, 23-3h, duration >10,000 min

select distinct subscriber_id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC,Duration
from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
where duration > 10000*60
and Event_Start_Hour in (23,0,1,2,3)
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 20841434
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 22:50:48.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-29 00:09:22.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration





-- 2 more examples for Playback, 23-3h, duration 500-700 min
select distinct subscriber_id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC,Duration
from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
where duration between 500*60 and 700*60
and Event_Start_Hour in (23,0,1,2,3)
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 3055572
and EVENT_START_DATE_TIME_UTC >= '2012-11-09 00:00:54.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-09 13:40:48.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration


----------------------------------------------------------------------------------------------------------------------------------------
----New Id's-----

select top 5 *
from limac.VEA_sample_5_11Nov_Time_Shifted_Events
where duration between 400*60 and 700*60
and cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int)=0
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,Live_recorded
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 3055572
and EVENT_START_DATE_TIME_UTC >= '2012-11-08 21:38:00.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-09 11:17:57.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration




-----------------------------------------------------------------------------------
select top 5 *
from limac.VEA_sample_5_11Nov_Time_Shifted_Events
where duration between 600*60 and 1000*60
and cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int)=23
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,Live_recorded
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 27300518
and EVENT_START_DATE_TIME_UTC >= '2012-11-07 22:49:43.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-08 17:22:02.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration




---------------------------------------------------------------------------


-- 2 more examples for Playback, 23-3h, duration >10,000 min

select distinct subscriber_id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC,Duration
from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
where duration > 10000*60
and Event_Start_Hour in (23,0,1,2,3)
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,Live_recorded
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 15837431
and EVENT_START_DATE_TIME_UTC >= '2012-11-05 22:16:23.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-27 10:18:40.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration





-- 2 more examples for Playback, 23-3h, duration 500-700 min
select distinct subscriber_id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC,Duration
from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
where duration between 500*60 and 700*60
and Event_Start_Hour in (23,0,1,2,3)
order by duration desc

select pk_viewing_prog_instance_fact
       ,subscriber_id
       ,type_of_viewing_event
       ,Playback_Speed
       ,Playback_type
       ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
       ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,dateformat(EVENT_END_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_END_DATE
       ,dateformat(EVENT_END_DATE_TIME_UTC,'HH:MM:SS') as EVENT_END_TIME
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_START_DATE
       ,dateformat(BROADCAST_START_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_START_TIME
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'YYYY-MM-DD') as BROADCAST_END_DATE
       ,dateformat(BROADCAST_END_DATE_TIME_UTC,'HH:MM:SS') as BROADCAST_END_TIME
       ,Duration
       ,programme_name
       ,channel_name
       ,next_channel_name
       ,genre_description
       ,sub_genre_description
from sk_prod.VESPA_EVENTS_ALL
where panel_id = 12
and subscriber_id = 9004671
and EVENT_START_DATE_TIME_UTC >= '2012-11-07 22:48:33.000000'
and EVENT_START_DATE_TIME_UTC <= '2012-11-08 12:28:16.000000'
order by subscriber_id,EVENT_START_DATE_TIME_UTC,Duration



