

--drop table #uncapped_and_phase_1b_capping;
---Evaluation of 1b and 2 capping-----
select subscriber_id
--,event_type
,channel_name_inc_hd
,epg_title
,tx_start_datetime_utc
,tx_end_datetime_utc
,adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local
,uncapped_x_viewing_end_time_local
,capped_x_viewing_end_time_local
into #uncapped_and_phase_1b_capping
from dbarnett.project072_all_viewing
where subscriber_id = 52043
order by adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local
;


select subscriber_id 
,channel_name_inc_hd
,viewing_starts_local
,viewing_stops_local
into #phase_2_capping
from dbarnett.project072_capping_phase_2_feb05_18
where subscriber_id = 52043
order by viewing_starts_local
,viewing_stops_local
;

select a.*
,b.viewing_stops_local as viewing_ends_capped_v2
into #both_capping_details
from #uncapped_and_phase_1b_capping as a
left outer join #phase_2_capping as b
on a.subscriber_id=b.subscriber_id
and a.viewing_record_start_time_local=b.viewing_starts_local
;
commit;



--------Second example-----


--drop table #uncapped_and_phase_1b_capping;
---Evaluation of 1b and 2 capping-----
select subscriber_id
--,event_type
,channel_name_inc_hd
,epg_title
,tx_start_datetime_utc
,tx_end_datetime_utc
,adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local
,uncapped_x_viewing_end_time_local
,capped_x_viewing_end_time_local
into #uncapped_and_phase_1b_capping_107158
from dbarnett.project072_all_viewing
where subscriber_id = 107158
order by adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local
;


select subscriber_id 
,channel_name_inc_hd
,viewing_starts_local
,viewing_stops_local
into #phase_2_capping_107158
from dbarnett.project072_capping_phase_2_feb05_18
where subscriber_id = 107158
order by viewing_starts_local
,viewing_stops_local
;

select a.*
,b.viewing_stops_local as viewing_ends_capped_v2
into #both_capping_details_107158
from #uncapped_and_phase_1b_capping_107158 as a
left outer join #phase_2_capping_107158 as b
on a.subscriber_id=b.subscriber_id
and a.viewing_record_start_time_local=b.viewing_starts_local
;
commit;

select * from #both_capping_details_107158;




--------Third example-----


--drop table #uncapped_and_phase_1b_capping;
---Evaluation of 1b and 2 capping-----
select subscriber_id
--,event_type
,channel_name_inc_hd
,epg_title
,tx_start_datetime_utc
,tx_end_datetime_utc
,adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local
,uncapped_x_viewing_end_time_local
,capped_x_viewing_end_time_local
into #uncapped_and_phase_1b_capping_1027233
from dbarnett.project072_all_viewing
where subscriber_id = 1027233
order by adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local
;


select subscriber_id 
,channel_name_inc_hd
,viewing_starts_local
,viewing_stops_local
into #phase_2_capping_1027233
from dbarnett.project072_capping_phase_2_feb05_18
where subscriber_id = 1027233
order by viewing_starts_local
,viewing_stops_local
;

select a.*
,b.viewing_stops_local as viewing_ends_capped_v2
into #both_capping_details_1027233
from #uncapped_and_phase_1b_capping_1027233 as a
left outer join #phase_2_capping_1027233 as b
on a.subscriber_id=b.subscriber_id
and a.viewing_record_start_time_local=b.viewing_starts_local
;
commit;

select * from #both_capping_details_1027233;

--select * from vespa_analysts.VESPA_DAILY_AUGS_20120205 where subscriber_id = 1027233 order by viewing_starts

--

select subscriber_id
,adjusted_event_start_time
,x_adjusted_event_end_time
,x_viewing_start_time
,x_viewing_end_time
,x_programme_viewed_duration
into #uncapped_viewing
from sk_prod.vespa_stb_prog_events_20120205
where programme_trans_sk = 201202060000014737 
order by adjusted_event_start_time
,x_adjusted_event_end_time
;


select * into #capped_v2_viewing from vespa_analysts.VESPA_DAILY_AUGS_20120205 where programme_trans_sk = 201202060000014737 order by viewing_starts

select top 500 * from #capped_v2_viewing

select a.*
,b.viewing_starts
,b.viewing_stops
,b.viewing_duration
,b.capped_flag
,b.capped_event_end_time
into #add_capping_v2
from #uncapped_viewing as a
left outer join #capped_v2_viewing as b
on a.subscriber_id = b.subscriber_id
and a.x_viewing_start_time = b.viewing_starts
;

select top 1000 * from #add_capping_v2;

select * from  vespa_analysts.VESPA_DAILY_AUGS_20120205 where subscriber_id = 15986 order by viewing_starts


commit;


201202060000014737
--
select * from  #both_capping_details order by adjusted_event_start_time
,x_adjusted_event_end_time
,viewing_record_start_time_local;



select top 5000 * from dbarnett.project072_all_viewing where channel_name_inc_hd = 'Sky Sports 1' and  right(cast(subscriber_id as varchar),2)='33'

from dbarnett.project072_capping_phase_2_feb05_18

from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel



---commit;
select top 500 * from vespa_analysts.CP2_calculated_viewing_caps where live=1;





