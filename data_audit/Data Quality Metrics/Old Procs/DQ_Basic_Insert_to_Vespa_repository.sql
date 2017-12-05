
insert into data_quality_vespa_repository
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red,metric_rag, load_timestamp, modified_date)
select dqr.logger_id,dqr.data_date,dq_vm.dq_vm_id,result,dq_chk_det.metric_tolerance_amber, dq_chk_det.metric_tolerance_red,
dqr.rag_status,dqr.load_timestamp, dqr.modified_date
from data_quality_results dqr,
data_quality_check_details dq_chk_det,
data_quality_vespa_metrics dq_vm
where dqr.dq_check_detail_id = dq_chk_det.dq_check_detail_id
and dq_chk_det.metric_short_name = dq_vm.metric_short_name

commit
