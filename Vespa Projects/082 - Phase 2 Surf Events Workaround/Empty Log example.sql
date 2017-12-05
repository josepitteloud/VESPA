select account_number
,subscriber_id
,Event_Type
,x_type_of_viewing_event
,video_playing_flag
,play_back_speed
,Adjusted_Event_Start_Time
,X_Adjusted_Event_End_Time 
,x_event_duration
into #event_details_phase_1_20120728
from sk_prod.VESPA_STB_PROG_EVENTS_20120728
where event_type='evEmptyLog'
;

select top 100 * from #event_details_phase_1_20120728


account_number,subscriber_id,Event_Type,x_type_of_viewing_event,video_playing_flag,play_back_speed,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time,x_event_duration
'210023904027',937199,'evEmptyLog','Non viewing event',,,'2012-07-28 00:50:09.000','2012-07-28 00:50:09.000',0


select subscriber_id 
, account_number
,event_start_date_time_utc 
, event_end_date_time_utc
, instance_start_date_time_utc
,log_received_start_date_time_utc
,video_playing_flag
,playback_speed
,channel_name
,service_type_description
,type_of_viewing_event
,instance_start_date_time_utc

--into #event_details_phase_2_20120728
 from sk_prod.vespa_events_all where
--type_of_viewing_event ='Non viewing event'
--and cast(event_start_date_time_utc as date ) in ( '2012-07-28')
--and event_start_date_time_utc between '2012-07-27 00:00:00' and '2012-07-29 23:59:59'
--and
 subscriber_id = 2463630
order by event_start_date_time_utc
;
commit;



select account_number
,subscriber_id
,Event_Type
,x_type_of_viewing_event
,video_playing_flag
,play_back_speed
,Adjusted_Event_Start_Time
,X_Adjusted_Event_End_Time 
,x_event_duration
,x_channel_name
,x_epg_title
,x_viewing_start_time
,x_viewing_end_time

from sk_prod.VESPA_STB_PROG_EVENTS_20120801
where  subscriber_id = 2463630
order by adjusted_event_start_time,x_viewing_start_time
;
commit;