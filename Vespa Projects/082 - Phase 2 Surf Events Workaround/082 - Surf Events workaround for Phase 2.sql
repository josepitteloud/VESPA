
---Project 082-----

----Surf Events Workaround

--As the field Event_Type is not available of Phase 2 data, we need to try and find a way of identifying Surf events from what fields are available on Phase 2 data


----Part 1 - Count of number of events by type for Phase 1 and Phase 2 Data

----Phase 1 Video Playing_flag_combinations

select Event_Type
,x_type_of_viewing_event
,video_playing_flag
,case when play_back_speed is null then 'Null' else 'Non-Null' end as play_back_speed_type
,count(*) as records
, sum(case when Adjusted_Event_Start_Time=X_Adjusted_Event_End_Time then 1 else 0 end) as same_time
from sk_prod.VESPA_STB_PROG_EVENTS_20120728
group by Event_Type
,x_type_of_viewing_event
,video_playing_flag
,play_back_speed_type
order by Event_Type
,x_type_of_viewing_event
,video_playing_flag
,play_back_speed_type
;

----Phase 2 Event Types----
select type_of_viewing_event
,video_playing_flag
,case when playback_speed is null then 'Null' else 'Non-Null' end as play_back_speed_type
,count(*) as records
from sk_prod.vespa_events_all where cast(event_start_date_time_utc as date ) in ( '2012-07-28')
group by type_of_viewing_event
,video_playing_flag
,play_back_speed_type
order by type_of_viewing_event
,video_playing_flag
,play_back_speed_type
;


-----Creating a set of criteria to identify evSurf events---

---create a list of all non-viewing events from both Phase 1 and Phase 2 data for a single day---

---All Phase 1 events (include all to be able to quantify how accurate attribution is---
--drop table #event_details_phase_1_20120728;
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
where x_type_of_viewing_event= 'Non viewing event'
;

commit;
create hg index idx1 on #event_details_phase_1_20120728 (subscriber_id);
create hg index idx2 on #event_details_phase_1_20120728 (Adjusted_Event_Start_Time);
commit;

--select * from sk_prod.VESPA_STB_PROG_EVENTS_20120728 where subscriber_id = 4401662 order by Adjusted_Event_Start_Time
--,X_Adjusted_Event_End_Time ;
--select top 500 * from #event_details_phase_1_20120728;
--select top 500 * from #event_details_phase_1_20120728 where  Event_Type='evStandbyOut';
--select top 500 * from #event_details_phase_1_20120728 where  Event_Type='evStandbyIn';
---All Phase 2 Non Viewing Events---
select *
/*subscriber_id 
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
*/
into #event_details_phase_2_20120728
 from sk_prod.vespa_events_all where
type_of_viewing_event ='Non viewing event'
--and cast(event_start_date_time_utc as date ) in ( '2012-07-28')
and event_start_date_time_utc between '2012-07-28 00:00:00' and '2012-07-28 23:59:59'

;
commit;
---Not all Phase 2 activity will be in Phase 1 data, but all Phase 1 events should be represented in Phase 2

--select count(*) from #event_details_phase_2_20120728;
select * into dbarnett.event_details_phase_2_20120728 from #event_details_phase_2_20120728; commit;

commit;
create hg index idx1 on dbarnett.event_details_phase_2_20120728 (subscriber_id);
create hg index idx2 on dbarnett.event_details_phase_2_20120728 (event_start_date_time_utc);
commit;


---Match Phase 2 data to phase 1---

select a.account_number as account_number_phase_1
,a.subscriber_id as subscriber_id_phase_1
,a.Event_Type as Event_Type_phase_1
,a.x_type_of_viewing_event as x_type_of_viewing_event_phase_1
,a.video_playing_flag as video_playing_flag_phase_1
,a.play_back_speed as play_back_speed_phase_1
,a.Adjusted_Event_Start_Time as Adjusted_Event_Start_Time_phase_1
,a.X_Adjusted_Event_End_Time as X_Adjusted_Event_End_Time_phase_1
,b.*
into dbarnett.non_viewing_phase_1_phase_2_comparison
from #event_details_phase_1_20120728 as a
left outer join dbarnett.event_details_phase_2_20120728 as b
on a.subscriber_id = b.subscriber_id and a.Adjusted_Event_Start_Time=b.event_start_date_time_utc
;
commit;

--select count(*) from dbarnett.non_viewing_phase_1_phase_2_comparison
--select count(*) from #event_details_phase_1_20120728

select count(*) from dbarnett.non_viewing_phase_1_phase_2_comparison where event_type_phase_1 = 'evSurf' and duration>0;
select count(*) from #event_details_phase_1_20120728 where event_type = 'evSurf' and x_event_duration>0;

select event_type_phase_1 , count(*), sum(case when video_playing_flag=1 and type_of_viewing_event='Non viewing event' and live_recorded='LIVE' then 1 else 0 end) as surf_events 
from dbarnett.non_viewing_phase_1_phase_2_comparison
where  duration>0
group by event_type_phase_1
order by event_type_phase_1
;

commit;


select case when duration>=300 then 300 else duration end as duration_length
, sum(case when event_type_phase_1 ='evChangeView' then 1 else 0 end) as change_view
, sum(case when event_type_phase_1 ='evEmptyLog' then 1 else 0 end) as empty_log
, sum(case when event_type_phase_1 ='evStandbyIn' then 1 else 0 end) as standby_in
, sum(case when event_type_phase_1 ='evStandbyOut' then 1 else 0 end) as standby_out
, sum(case when event_type_phase_1 ='evSurf' then 1 else 0 end) as surf
from dbarnett.non_viewing_phase_1_phase_2_comparison
where  duration>0 and video_playing_flag=1 and type_of_viewing_event='Non viewing event' and live_recorded='LIVE'
group by duration_length
order by duration_length
;
commit;
--select top 500 * from 

---Test of Classification---

--select count(*) from sk_prod.vespa_events_all where video_playing_flag=1 and type_of_viewing_event='Non viewing event' and live_recorded='LIVE' and duration between 1 and 15;


event_type_phase_1,count(),surf_events
'evChangeView',6110060,65865
'evEmptyLog',592,163
'evStandbyIn',692654,323002
'evStandbyOut',101259,68655
'evSurf',558193,553200


/*
count()
558193
count()
560403
*/




-- where event_type_phase_1 = 'evSurf';

--select top 500 * from  dbarnett.non_viewing_phase_1_phase_2_comparison where video_playing_flag=1 and type_of_viewing_event='Non viewing event' and live_recorded='LIVE';

--select top 500 * from  dbarnett.non_viewing_phase_1_phase_2_comparison where event_type_phase_1 = 'evSurf';

count()
568030



select count(*) , sum(case when cb_change_date is null then 1 else 0 end) as miss from dbarnett.non_viewing_phase_1_phase_2_comparison where event_type_phase_1 = 'evEmptyLog';


--------------------Test Code-----------------------------

insert 
     select distinct account_number, service_instance_id
    into #raw_logs_dump
     from sk_prod.vespa_events_all
     where event_start_date_time_utc between dateadd(hour, 6, @scaling_day) and dateadd(hour, 30, @scaling_day)
     and panel_id in (4,12)


select top 500 * from sk_prod.vespa_events_all where ;

commit;

select count(*) , sum(case when account_number is null then 1 else 0 end) as missing from sk_prod.vespa_events_all


select datediff(second, Adjusted_Event_Start_Time, X_Adjusted_Event_End_Time) as Duration,
  count(*) as Cnt
  from sk_prod.VESPA_STB_PROG_EVENTS_20120728
where Event_Type = 'evSurf'
group by Duration
order by Duration


select top 100 subscriber_id , account_number , stb_log_creation_date,document_creation_date,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time
  from sk_prod.VESPA_STB_PROG_EVENTS_20120728
where Event_Type = 'evSurf'



-------------------------Figures---------------------------------------------

-----Phase 1----
select  subscriber_id , account_number , stb_log_creation_date,document_creation_date,Adjusted_Event_Start_Time
,X_Adjusted_Event_End_Time,x_viewing_start_time, x_viewing_end_time,Event_Type,x_type_of_viewing_event,x_channel_name,stb_log_creation_date,video_playing_flag
  from sk_prod.VESPA_STB_PROG_EVENTS_20120728
where subscriber_id in 
(1285540) 
order by Adjusted_Event_Start_Time , X_Adjusted_Event_End_Time , x_viewing_start_time, x_viewing_end_time
;

----Phase 1 Video Playing_flag_combinations

select Event_Type
,x_type_of_viewing_event
,video_playing_flag
,case when play_back_speed is null then 'Null' else 'Non-Null' end as play_back_speed_type
,count(*) as records
, sum(case when Adjusted_Event_Start_Time=X_Adjusted_Event_End_Time then 1 else 0 end) as same_time
from sk_prod.VESPA_STB_PROG_EVENTS_20120728
group by Event_Type
,x_type_of_viewing_event
,video_playing_flag
,play_back_speed_type
order by Event_Type
,x_type_of_viewing_event
,video_playing_flag
,play_back_speed_type
;

commit;

-----Phase 2-----
select subscriber_id , account_number
,event_start_date_time_utc , event_end_date_time_utc, instance_start_date_time_utc
,log_received_start_date_time_utc
,video_playing_flag
,playback_speed
,channel_name
,service_type_description
,type_of_viewing_event
 from sk_prod.vespa_events_all where subscriber_id in 
(1285540)
and cast(event_start_date_time_utc as date ) in ( '2012-07-28')
order by event_start_date_time_utc , event_end_date_time_utc, instance_start_date_time_utc
;

select type_of_viewing_event
,video_playing_flag
,case when playback_speed is null then 'Null' else 'Non-Null' end as play_back_speed_type
,count(*) as records
from sk_prod.vespa_events_all where cast(event_start_date_time_utc as date ) in ( '2012-07-28')
group by type_of_viewing_event
,video_playing_flag
,play_back_speed_type
order by type_of_viewing_event
,video_playing_flag
,play_back_speed_type
;

commit;
---Identify Surf events----













----------------------------------------------------------

commit;

1297876,
1548129,
1557910)
1559908,
1715574,
2365850,
2758734,
2930206,
4401662,
4800794,
4884828,
6103997,
6858071,
7097933,
7341118,
8000444,
8037366,
8209966,
8308347,
8593681,
9868576,
11296700,
11758783,
12232169,
12281749)



select top 100 subscriber_id , account_number , stb_log_creation_date,document_creation_date,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time
  from sk_prod.VESPA_STB_PROG_EVENTS_20120728
where Event_Type = 'evEmptyLog'

select event_type, subscriber_id , account_number , stb_log_creation_date,document_creation_date,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time
  from sk_prod.VESPA_STB_PROG_EVENTS_20120727
where subscriber_id = 24200653
order by Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time


subscriber_id,account_number,stb_log_creation_date,document_creation_date,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time
937199,'210023904027','2012-07-28 00:50:09.000','2012-07-28 01:21:37.000','2012-07-28 00:50:09.000','2012-07-28 00:50:09.000'

select *
from sk_prod.vespa_events_all 
--
where 
cast(event_start_date_time_utc as date ) in ( '2012-07-27', '2012-07-28')
and 
subscriber_id = 24200653 order by event_start_date_time_utc
;

select Event_Type, count(*) as records , sum(case when Adjusted_Event_Start_Time=X_Adjusted_Event_End_Time) then 1 else 0 end) as same_time
  from sk_prod.VESPA_STB_PROG_EVENTS_20120728
group by 
--where Event_Type = 'evEmptyLog'
