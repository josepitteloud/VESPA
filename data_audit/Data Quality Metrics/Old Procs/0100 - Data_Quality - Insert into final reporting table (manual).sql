
declare @analysis_date_current date

set @analysis_date_current = '2013-05-16'

truncate table data_quality_vespa_repository_reporting

insert into data_quality_vespa_repository_reporting
select a.dq_vr_id, a.dq_run_id, @analysis_date_current, a.dq_vm_id, a.metric_result,
a.metric_tolerance_amber, a.metric_tolerance_red, a.metric_rag, a.load_timestamp,a.modified_date from data_quality_vespa_repository a,
data_quality_vespa_metrics b
where a.dq_vm_id = b.dq_vm_id
and a.dq_run_id = 81
and a.viewing_data_date = @analysis_date_current
and b.current_flag = 1
and lower(b.metric_short_name) not like 'tdq%'
union 
select a.dq_vr_id, a.dq_run_id, @analysis_date_current, a.dq_vm_id, a.metric_result,
a.metric_tolerance_amber, a.metric_tolerance_red, a.metric_rag, a.load_timestamp,a.modified_date from data_quality_vespa_repository a,
data_quality_vespa_metrics b
where a.dq_vm_id = b.dq_vm_id
and a.dq_run_id = 81
and b.current_flag = 1
and lower(b.metric_short_name) like 'tdq%'


commit