----Select most recent list of Olive results

select dq_col.creator,
dq_col.table_name, 
dq_col.column_name, 
dq_chk_typ.dq_check_type,
dk_chk_det.dq_check_detail_id,
dk_chk_det.metric_short_name,
dq_res.sql_processed,
dq_res.rag_status,
coalesce(cast(dq_res.result as varchar),dq_res.result_text) result_value,
dk_chk_det.metric_benchmark,
dk_chk_det.metric_tolerance_amber,
dk_chk_det.metric_tolerance_red,
dq_res.load_timestamp
from data_quality_columns dq_col, 
data_quality_check_type dq_chk_typ, 
data_quality_results dq_res,
data_quality_check_details dk_chk_det,
data_quality_run_group dq_run_grp,
(select dq_check_detail_id, max(dq_res_id) dq_res_id
from data_quality_results
group by dq_check_detail_id) dq_res_max
where dk_chk_det.dq_col_id = dq_col.dq_col_id
and dq_chk_typ.dq_check_type_id = dk_chk_det.dq_check_type_id 
and dq_res.dq_check_detail_id = dk_chk_det.dq_check_detail_id
and dq_run_grp.dq_run_id = dq_sched_run_id
AND dq_res.dq_res_id = dq_res_max.dq_res_id
and dq_run_grp.run_type = 'OLIVE_BASIC_CHECKS'
