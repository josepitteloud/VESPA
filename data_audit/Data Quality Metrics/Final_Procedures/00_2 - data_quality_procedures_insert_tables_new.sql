


----normal insert----------------------------------

insert into DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT_TOTAL
select * from kinnairt.DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT_TOTAL

insert into DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT
select * from kinnairt.DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT

insert into  DATA_QUALITY_SLOT_DATA_AUDIT
select * from kinnairt.DATA_QUALITY_SLOT_DATA_AUDIT

insert into  data_quality_adsmart_campaign_data_audit
select * from kinnairt.data_quality_adsmart_campaign_data_audit

insert into data_quality_adsmart_hh_data_audit
select * from kinnairt.data_quality_adsmart_hh_data_audit

insert into data_quality_adsmart_segment_data_audit
insert into data_quality_channel_issues_list
select * from kinnairt.data_quality_channel_issues_list

insert into data_quality_dp_data_audit
select * from kinnairt.data_quality_dp_data_audit

insert into data_quality_homebase_channels
select * from kinnairt.data_quality_homebase_channels

insert into data_quality_scaling_accounts
select * from kinnairt.data_quality_scaling_accounts

insert into data_quality_scaling_table_checks
select * from kinnairt.data_quality_scaling_table_checks


insert into data_quality_sky_base_upscale
select * from kinnairt.data_quality_sky_base_upscale


insert into data_quality_vespa_repository_reporting
select * from kinnairt.data_quality_vespa_repository_reporting

commit


--------------------------special select for tables that have pk_ids-------------------------------------

SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_channel_check'

insert into data_quality_channel_check
select * from kinnairt.data_quality_channel_check;

commit;


SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_channel_service_key_list';
insert into data_quality_channel_service_key_list
select * from kinnairt.data_quality_channel_service_key_list;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_check_details';
insert into data_quality_check_details
select * from kinnairt.data_quality_check_details;

commit;




SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_check_type';
insert into data_quality_check_type
select * from kinnairt.data_quality_check_type;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_columns';
insert into data_quality_columns
select * from kinnairt.data_quality_columns;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_regression_reports_repository';
insert into data_quality_regression_reports_repository
select * from kinnairt.data_quality_regression_reports_repository;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_regression_thresholds';
insert into data_quality_regression_thresholds
select * from kinnairt.data_quality_regression_thresholds;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_results';
insert into data_quality_results
select * from kinnairt.data_quality_results;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_run_group';
insert into data_quality_run_group
select * from kinnairt.data_quality_run_group;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_slots_daily_reporting';
insert into data_quality_slots_daily_reporting
select * from kinnairt.data_quality_slots_daily_reporting;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_vespa_metrics';
insert into data_quality_vespa_metrics
select * from kinnairt.data_quality_vespa_metrics;

commit;

SET TEMPORARY OPTION IDENTITY_INSERT = '';
SET TEMPORARY OPTION IDENTITY_INSERT = 'data_quality_vespa_repository';
insert into data_quality_vespa_repository
select * from kinnairt.data_quality_vespa_repository;

commit;






