if object_id('DATA_QUALITY_SLOT_DATA_AUDIT') is not null then drop table DATA_QUALITY_SLOT_DATA_AUDIT end if;

create table DATA_QUALITY_SLOT_DATA_AUDIT
(SLOT_DATA_KEY	bigint	Not Null,
VIEWED_START_DATE_KEY	integer	,
IMPACTS	smallint	,
RECORD_DATE	timestamp	,
HOUSEHOLD_KEY	bigint	,
IMPACT_DAY	timestamp Not Null,
slot_instance_key	bigint	,
channel_key	integer	,
slot_start_date_key	integer	,
slot_start_time_key	integer	,
slot_end_date_key	integer	,
slot_end_time_key	integer	,
previous_programme_key	bigint	,
next_programme_key	bigint	,
prev_prog_schedule_key	bigint	,
next_prog_schedule_key	bigint	,
prev_broadcast_start_date	date,
next_broadcast_start_date	date,
prev_broadcast_start_time	time,
next_broadcast_start_time	time,
slot_start_date	date,
slot_end_date	date,
slot_start_time	time,
slot_end_time	time,
scaling_factor	double,
prev_broadcast_end_time	time,
next_broadcast_end_time	time,
prev_broadcast_end_date	date,
next_broadcast_end_date	date,
slot_key bigint,
viewed_duration decimal(15),
viewed_start_time_key int,
time_shift_key int);

commit;
go

if object_id('data_quality_check_details') is not null then drop table data_quality_check_details end if;

create table data_quality_check_details
(dq_check_detail_id	bigint	not null identity,
dq_col_id	bigint	Not Null,
dq_sched_run_id	bigint Not Null,
dq_check_type_Id bigint	Not Null,
expected_value	varchar	(20),
metric_benchmark	decimal	(16,3),
metric_tolerance_amber	decimal	(6,3),
metric_tolerance_red	decimal	(6,3),
unknown_value	varchar	(20),
load_timestamp	timestamp,
modified_date	timestamp default timestamp,
metric_short_name	varchar	(200),
exception_value varchar(255),
notnull_col_checks varchar(1000));


commit;
go


if object_id('data_quality_check_type') is not null then drop table data_quality_check_type end if;

create table data_quality_check_type
(dq_check_type_Id	bigint	not null identity,
dq_check_type		varchar	(200),
load_timestamp	timestamp,
modified_date	timestamp default timestamp);


commit;
go


if object_id('data_quality_columns') is not null then drop table data_quality_columns end if;

create table data_quality_columns
(dq_col_id	bigint	not null identity,
creator	varchar	(50),
table_name	varchar	(200) Not Null,
column_name	varchar	(200) Not Null,
column_type	varchar	(50)  Not Null,
column_length	integer Not Null,
load_timestamp	timestamp,
modified_date	timestamp default timestamp);

ALTER TABLE data_quality_columns
ADD UNIQUE (table_name,column_name);

commit;
go

if object_id('data_quality_dp_data_audit') is not null then drop table data_quality_dp_data_audit end if;


create table data_quality_dp_data_audit
(viewing_date	date,
pk_viewing_prog_instance_fact	bigint,
cb_change_date	date,
dk_barb_min_start_datehour_dim	integer,
dk_barb_min_start_time_dim	integer,
dk_barb_min_end_datehour_dim	integer,
dk_barb_min_end_time_dim	integer,
dk_channel_dim	integer,
dk_event_start_datehour_dim	integer,
dk_event_start_time_dim	integer,
dk_event_end_datehour_dim	integer,
dk_event_end_time_dim	integer,
dk_instance_start_datehour_dim	integer,
dk_instance_start_time_dim	integer,
dk_instance_end_datehour_dim	integer,
dk_instance_end_time_dim	integer,
dk_programme_dim	bigint,
dk_programme_instance_dim	bigint,
dk_viewing_event_dim	bigint,
genre_description	varchar	(20),
sub_genre_description	varchar	(20),
service_type	bigint,
service_type_description	varchar	(40),
type_of_viewing_event	varchar	(40),
account_number	varchar	(20),
panel_id	tinyint	,
live_recorded	varchar	(8),
barb_min_start_date_time_utc	timestamp,
barb_min_end_date_time_utc	timestamp,
event_start_date_time_utc	timestamp,
event_end_date_time_utc	timestamp,
instance_start_date_time_utc	timestamp,
instance_end_date_time_utc	timestamp,
dk_capping_end_datehour_dim	integer,
dk_capping_end_time_dim	integer,
capping_end_date_time_utc	timestamp,
log_start_date_time_utc	timestamp,
duration	integer,
subscriber_id	decimal	(9,0),
log_received_start_date_time_utc	timestamp,
capped_full_flag	bit		not null,
capped_partial_flag	bit		not null);

commit;
go

if object_id('data_quality_results') is not null then drop table data_quality_results end if;

create table data_quality_results
(dq_res_id	bigint	identity,
dq_check_detail_id	bigint Not Null,
dq_run_id	bigint Not Null,
result	bigint,
RAG_STATUS	varchar	(5),
sql_processed	varchar	(8000),
date_period	date,
data_total	bigint,
logger_id	bigint,
data_date	date,
load_timestamp	timestamp,
modified_date	timestamp default timestamp,
result_text varchar(255));

commit;
go


if object_id('data_quality_run_group') is not null then drop table data_quality_run_group end if;

create table data_quality_run_group
(dq_run_id	bigint	not null identity,
run_type	varchar	(100),
load_timestamp	timestamp,
modified_date	timestamp default timestamp);

commit;
go

if object_id('data_quality_sky_base_upscale') is not null then drop table data_quality_sky_base_upscale end if;

create table data_quality_sky_base_upscale
(sky_base_upscale_total	bigint,
event_date date);

commit;
go

if object_id('data_quality_vespa_metrics') is not null then drop table data_quality_vespa_metrics end if;

create table data_quality_vespa_metrics
(dq_vm_id	integer	not null,
metric_short_name	varchar	(200),
metric_description	varchar	(1000),
metric_benchmark	decimal	(16,3),
metric_tolerance_amber	decimal	(6,3),
metric_tolerance_red	decimal	(6,3),
load_timestamp	timestamp,
modified_date	timestamp default timestamp,
metric_grouping	varchar	(30),
current_flag	integer	 default 1);


commit;
go

if object_id('data_quality_vespa_repository') is not null then drop table data_quality_vespa_repository end if;

create table data_quality_vespa_repository
(dq_vr_id	bigint	identity,
dq_run_id	bigint	Not Null,
viewing_data_date	date,
dq_vm_id	bigint	Not Null,
metric_result	decimal	(16,3),
metric_tolerance_amber	decimal	(6,3),
metric_tolerance_red	decimal	(6,3),
metric_rag	varchar	(8),
load_timestamp	timestamp,
modified_date	timestamp default timestamp);


commit;
go

if object_id('data_quality_vespa_repository_reporting') is not null then drop table data_quality_vespa_repository_reporting end if;

create table data_quality_vespa_repository_reporting
(dq_vr_id	bigint,
dq_run_id	bigint Not Null,
viewing_data_date date,
dq_vm_id	bigint	Not Null,
metric_result	decimal	(16,3),
metric_tolerance_amber	decimal	(6,3),
metric_tolerance_red	decimal	(6,3),
metric_rag	varchar	(8),
load_timestamp	timestamp,
modified_date	timestamp);


commit;
go

if object_id('SC2_scaling_weekly_sample_viq_dq') is not null then drop table SC2_scaling_weekly_sample_viq_dq end if;

create table SC2_scaling_weekly_sample_viq_dq
(account_number varchar(20),
cb_key_household bigint,
cb_key_individual bigint,
consumerview_cb_row_id bigint,
universe varchar (20),
isba_tv_region varchar(20),
hhcomposition varchar (2),
tenure  varchar (15),
num_mix integer,
mix_pack varchar (20),
package varchar(20),
boxtype varchar(35),
scaling_segment_id integer,
mr_boxes integer);


commit;
go


IF object_id('scaling_cbi_panel') IS NOT NULL then DROP table scaling_cbi_panel end if;

create table scaling_cbi_panel
(account_number varchar (20),
cb_key_household bigint,
cb_key_individual bigint,
consumerview_cb_row_id bigint ,
universe varchar (20),
isba_tv_region varchar(20),
hhcomposition varchar (2),
tenure varchar (15),
num_mix integer,
mix_pack varchar (20),
package varchar (20),
boxtype varchar (35),
scaling_segment_id  integer,
mr_boxes integer ,
adjusted_event_start_date_vespa date ,
calculated_scaling_weight double );


commit;
go



IF object_id('segmentation_index_viq_dq') IS NOT NULL then DROP table segmentation_index_viq_dq  end if;

create table segmentation_index_viq_dq
(metric_id char (3),
segment varchar(35),
sky_base numeric ,
sky_base_actual integer,
vespa_base integer,
coverage double);


commit;
go


IF object_id('scaling_variables_viq_dq') IS NOT NULL then DROP table scaling_variables_viq_dq  end if;


create table scaling_variables_viq_dq
(metric_id char (3),
segment varchar (35),
sky_base numeric,
sky_base_actual integer,
vespa_base integer,
coverage double,
index_var numeric ,
metric_desc char (13));


commit;
go



IF object_id('data_quality_scaling_accounts') IS NOT NULL then DROP table data_quality_scaling_accounts  end if;

create table data_quality_scaling_accounts
(account_number varchar(20));


commit;
go
