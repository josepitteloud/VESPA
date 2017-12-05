insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,
unknown_value, load_timestamp,metric_short_name)
values
(149, 1,4,0,10,50,'undefined',getdate(),'vdq_data_quality_dp_data_audit_genre_description_undefined_unknown_check')

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,
unknown_value, load_timestamp,metric_short_name)
values
(150, 1,4,0,10,50,'undefined',getdate(),'vdq_data_quality_dp_data_audit_sub_genre_description_undefined_unknown_check')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,
metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
values
('vdq_data_quality_dp_data_audit_genre_description_undefined_unknown_check',
'A test to check for the number of Undefined values within the genre description field on any given viewing day',
0, 10, 50, getdate(), 'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,
metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
values
('vdq_data_quality_dp_data_audit_sub_genre_description_undefined_unknown_check',
'A test to check for the number of Undefined values within the sub-genre description field on any given viewing day',
0, 10, 50, getdate(), 'data_integrity',1)

commit


insert into data_quality_columns
(creator, table_name, column_name, column_type, column_length, load_timestamp)
values
('kinnairt','data_quality_scaling_accounts','account_number','varchar',20,getdate())

commit

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp,metric_short_name)
values
(942, 1,6,0,10,50,getdate(),'vdq_data_quality_scaling_accounts_account_number_primary_key_check')

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,
metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
values
('vdq_data_quality_scaling_accounts_account_number_primary_key_check',
'A primary key check on the VIQ Scaling Weights table so that for each viewing day, the list of accounts is unique',
0, 10, 50, getdate(), 'data_integrity',1)

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,
metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
values
('mtc_viq_programme_sched_no_programme',
'A check for the percentage of programme instances for a viewing day where there are no corresponding programme information',
5, 10, 50, getdate(), 'data_integrity',1)

commit


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,
metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
values
('mtc_viq_programme_sched_no_channel',
'A check for the percentage of channel keys for a viewing day where there are no corresponding programme information',
5, 10, 50, getdate(), 'data_integrity',1)

commit



