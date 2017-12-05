---CBI UAT MA CHECK

select 
c.panel_id,
substr(dk_event_start_datehour_dim,1,8) programme_event_dt,
sum(case when dk_barb_min_start_datehour_dim > 0 then 1 else 0 end) barb_start_notnull,
sum(case when dk_barb_min_start_datehour_dim < 0 then 1 else 0 end) barb_start_null,
sum(case when dk_barb_min_end_datehour_dim > 0 then 1 else 0 end) barb_end_notnull,
sum(case when dk_barb_min_end_datehour_dim < 0 then 1 else 0 end) barb_end_null,
b.type_of_viewing_event
from tstiq_smi_export..VIEWING_PROGRAMME_INSTANCE_FACT a
inner join
tstiq_smi_dw..playback_dim b
on a.dk_playback_dim = b.pk_playback_dim
inner join
tstiq_smi_dw..viewing_event_dim c
on
a.dk_viewing_event_dim = c.pk_viewing_event_dim
--where a.dk_event_start_datehour_dim > 2012112423
group by c.panel_id,substr(a.dk_event_start_datehour_dim,1,8),
b.type_of_viewing_event


---CBI PROD MA CHECK

select 
c.panel_id,
substr(dk_event_start_datehour_dim,1,8) programme_event_dt,
sum(case when dk_barb_min_start_datehour_dim > 0 then 1 else 0 end) barb_start_notnull,
sum(case when dk_barb_min_start_datehour_dim < 0 then 1 else 0 end) barb_start_null,
sum(case when dk_barb_min_end_datehour_dim > 0 then 1 else 0 end) barb_end_notnull,
sum(case when dk_barb_min_end_datehour_dim < 0 then 1 else 0 end) barb_end_null,
b.type_of_viewing_event
from smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT a
inner join
smi_dw..playback_dim b
on a.dk_playback_dim = b.pk_playback_dim
inner join
smi_dw..viewing_event_dim c
on
a.dk_viewing_event_dim = c.pk_viewing_event_dim
where a.dk_event_start_datehour_dim > 2012112423
group by c.panel_id,substr(a.dk_event_start_datehour_dim,1,8),
b.type_of_viewing_event
order by 2


---OLIVE PROD CHECK

select cb_change_date,type_of_viewing_event,
dateformat(event_start_date_time_utc,'MM/DD/YYYY')  event_date,
sum(case when barb_min_start_date_time_utc is not null then 1 else 0 end) barb_start_total,
sum(case when barb_min_start_date_time_utc is null then 1 else 0 end) barb_start_null_total,
sum(case when barb_min_end_date_time_utc is not null then 1 else 0 end) barb_end_total,
sum(case when barb_min_end_date_time_utc is null then 1 else 0 end) barb_end_null_total
from sk_prod.vespa_events_all
--from sk_prod.vespa_events_viewed_all
where cb_change_date > '2012-12-05'
group by cb_change_date,
type_of_viewing_event,dateformat(event_start_date_time_utc,'MM/DD/YYYY')