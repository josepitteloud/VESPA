if object_id('data_quality_dp_data_to_analyze') is not null drop table data_quality_dp_data_to_analyze
if object_id('data_quality_dp_data_audit') is not null drop table data_quality_dp_data_audit
commit

declare @analysis_date date
declare @analysis_min_date date
declare @analysis_max_date date
declare @analysis_date_current date

set @analysis_date = (select max(cb_change_date) from sk_prod.vespa_dp_prog_viewed_current)

select * into #analysis_event_dates from
--select * into analysis_event_dates from
(select distinct date(event_start_date_time_utc) event_date 
from sk_prod.vespa_dp_prog_viewed_current
where cb_change_date = @analysis_date) t

select top 1 * into #analysis_event_dates_final from
--select top 7 * into analysis_event_dates_final from
(select event_date from #analysis_event_dates) t
order by 1 desc

set @analysis_min_date  = (select min(event_date) from #analysis_event_dates_final)

set @analysis_max_date  = (select max(event_date) from #analysis_event_dates_final)

set @analysis_date_current = @analysis_min_date 

while @analysis_date_current <= @analysis_max_date 
begin

delete from data_quality_dp_data_audit

insert into data_quality_dp_data_to_analyze
select pk_viewing_prog_instance_fact,cb_change_date,dk_barb_min_start_datehour_dim,dk_barb_min_start_time_dim,
dk_barb_min_end_datehour_dim,dk_barb_min_end_time_dim,dk_channel_dim, dk_event_start_datehour_dim,dk_event_start_time_dim,
dk_event_end_datehour_dim,dk_event_end_time_dim,dk_instance_start_datehour_dim,dk_instance_start_time_dim,
dk_instance_end_datehour_dim,dk_instance_end_time_dim,dk_programme_dim, dk_programme_instance_dim, dk_viewing_event_dim,
genre_description, sub_genre_description,service_type,service_type_description, type_of_viewing_event, account_number,panel_id,
live_recorded,barb_min_start_date_time_utc,barb_min_end_date_time_utc,event_start_date_time_utc,event_end_date_time_utc,
instance_start_date_time_utc,instance_end_date_time_utc,dk_capping_end_datehour_dim,dk_capping_end_time_dim,capping_end_date_time_utc,
log_start_date_time_utc
from sk_prod.vespa_dp_prog_viewed_current vdpvc
where date(vdpvc.event_start_date_time_utc) = @analysis_date_current

commit

insert into data_quality_dp_data_audit
select @analysis_date_current, pk_viewing_prog_instance_fact,cb_change_date,dk_barb_min_start_datehour_dim,dk_barb_min_start_time_dim,
dk_barb_min_end_datehour_dim,dk_barb_min_end_time_dim,dk_channel_dim, dk_event_start_datehour_dim,dk_event_start_time_dim,
dk_event_end_datehour_dim,dk_event_end_time_dim,dk_instance_start_datehour_dim,dk_instance_start_time_dim,
dk_instance_end_datehour_dim,dk_instance_end_time_dim,dk_programme_dim, dk_programme_instance_dim, dk_viewing_event_dim,
genre_description, sub_genre_description,service_type,service_type_description, type_of_viewing_event, account_number,panel_id,
live_recorded,barb_min_start_date_time_utc,barb_min_end_date_time_utc,event_start_date_time_utc,event_end_date_time_utc,
instance_start_date_time_utc,instance_end_date_time_utc,dk_capping_end_datehour_dim,dk_capping_end_time_dim,capping_end_date_time_utc,
log_start_date_time_utc
from sk_prod.vespa_dp_prog_viewed_current vdpvc
where date(vdpvc.event_start_date_time_utc) = @analysis_date_current

commit


set @analysis_date_current = @analysis_date_current + 1

end

