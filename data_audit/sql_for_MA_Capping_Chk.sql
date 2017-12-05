---netezza query to pull counts of events, capped events and ma_events from 
---Mar 1st 2013

SELECT substr(cast(dk_event_start_datehour_dim as varchar(20)),1,8) event_date, count(1) total_count
,sum(case when dk_capped_event_end_time_datehour_dim is not null and dk_capped_event_end_time_datehour_dim > 0 then 1 else 0 end) capped_end_datehour_populated
,sum(case when dk_capped_event_end_time_datehour_dim is null then 1 else 0 end) capped_end_datehour_null
,sum(case when dk_capped_event_end_time_datehour_dim is not null and dk_capped_event_end_time_datehour_dim < 0 then 1 else 0 end) capped_end_datehour_minus
,sum(case when dk_capped_event_end_time_dim is not null and dk_capped_event_end_time_dim > 0 then 1 else 0 end) capped_end_time_populated
,sum(case when dk_capped_event_end_time_dim is null then 1 else 0 end) capped_end_time_null
,sum(case when dk_capped_event_end_time_dim is not null and dk_capped_event_end_time_dim < 0 then 1 else 0 end) capped_end_datehour_minus
,sum(case when dk_barb_min_start_datehour_dim is not null and dk_barb_min_start_datehour_dim > 0 then 1 else 0 end) barb_start_datehour_populated
,sum(case when dk_barb_min_start_datehour_dim is null then 1 else 0 end) barb_start_datehour_null
,sum(case when dk_barb_min_start_datehour_dim is not null and dk_barb_min_start_datehour_dim < 0 then 1 else 0 end) barb_start_datehour_minus
,sum(case when dk_barb_min_end_datehour_dim is not null and dk_barb_min_end_datehour_dim > 0 then 1 else 0 end) barb_end_datehour_populated
,sum(case when dk_barb_min_end_datehour_dim is null then 1 else 0 end) barb_end_datehour_null
,sum(case when dk_barb_min_end_datehour_dim is not null and dk_barb_min_end_datehour_dim < 0 then 1 else 0 end) barb_end_datehour_minus
,sum(case when dk_barb_min_start_time_dim is not null and dk_barb_min_start_time_dim > 0 then 1 else 0 end) barb_start_time_populated
,sum(case when dk_barb_min_start_time_dim is null then 1 else 0 end) barb_start_time_null
,sum(case when dk_barb_min_start_time_dim is not null and dk_barb_min_start_time_dim < 0 then 1 else 0 end) barb_start_time_minus
,sum(case when dk_barb_min_end_time_dim is not null and dk_barb_min_end_time_dim > 0 then 1 else 0 end) barb_end_time_populated
,sum(case when dk_barb_min_end_time_dim is null then 1 else 0 end) barb_end_time_null
,sum(case when dk_barb_min_end_time_dim is not null and dk_barb_min_end_time_dim < 0 then 1 else 0 end) barb_end_time_minus
  FROM SMI_ACCESS.SMI_ETL.V_VIEWING_PROGRAMME_INSTANCE_FACT
  where substr(cast(dk_event_start_datehour_dim as varchar(20)),1,8) > '20130228'
group by substr(cast(dk_event_start_datehour_dim as varchar(20)),1,8) 
 
