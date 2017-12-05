 if object_id('data_quality_columns') is not null drop TABLE data_quality_columns;

COMMIT;
GO

create table data_quality_columns
(dq_col_id bigint identity
,creator varchar(50)
,table_name varchar(200) not null
,column_name varchar(200) not null
,column_type varchar(50) not null
,column_length int not null
,load_timestamp datetime
,modified_date datetime default timestamp);

ALTER TABLE data_quality_columns
ADD UNIQUE (table_name,column_name)

 if object_id('data_quality_check_type') is not null drop TABLE data_quality_check_type;

COMMIT;
GO

create table data_quality_check_type
(dq_check_type_Id bigint identity
,dq_check_type varchar(200)
,load_timestamp datetime
,modified_date datetime default timestamp);

declare @load_date datetime

set @load_date = getdate()

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('COLUMN_TYPE_LENGTH_CHECK',@load_date)

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('ISNULL_CHECK',@load_date)

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('MAX_LENGTH_CHECK',@load_date)

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('DISTINCT_COUNT_CHECK',@load_date)

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('UNKNOWN_CHECK',@load_date)

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('PRIMARY_KEY',@load_date)

insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('FOREIGN_KEY',@load_date)


insert into data_quality_check_type
(dq_check_type, load_Timestamp)
VALUES
('EXPERIAN_PROPENSITY',@load_date)


COMMIT

 if object_id('data_quality_run_group') is not null drop TABLE data_quality_run_group;

create table data_quality_run_group
(dq_run_id bigint identity
,run_type varchar(100)
,load_timestamp timestamp
,modified_date timestamp default timestamp)

insert into data_quality_run_group
(run_type, load_timestamp)
values
('VESPA_DATA_QUALITY',GETDATE())

insert into data_quality_columns
(creator, table_name ,column_name ,column_type ,column_length, load_timestamp)
VALUES
('sk_prod','EXPERIAN_CONSUMERVIEW','cb_key_household','varchar',20,getdate())

commit

 if object_id('data_quality_check_details') is not null drop TABLE data_quality_check_details;

CREATE TABLE data_quality_check_details
(dq_check_detail_id bigint identity
, dq_col_id bigint not null
, dq_run_id bigint not null
, dq_check_type_Id bigint not null
, current_values varchar(1000)
, expected_value varchar(10)
, percentage_tolerance decimal (4,2)
,unknown_value varchar(20)
,metric_short_name varchar(200)
,load_timestamp datetime
,modified_date datetime default timestamp);



declare @load_date datetime

set @load_date = getdate()

insert into data_quality_check_details
(dq_col_Id, dq_check_type_id, load_timestamp)
select dq_col.dq_col_Id,
dq_check.dq_check_type_id, @load_date from 
data_quality_columns dq_col, data_quality_check_type dq_check
where lower(dq_col.column_name) = 'dk_programme_instance_dim'
and lower(dq_col.table_name) = 'vespa_dp_prog_viewed_current'

commit

 if object_id('data_quality_results') is not null drop TABLE data_quality_results;


create table data_quality_results
(dq_res_id bigint identity,
dq_check_detail_id bigint not null,
dq_run_id bigint not null,
result bigint,
pass_fail varchar(4),
sql_processed varchar(8000),
date_period date,
data_total bigint,
logger_id bigint,
data_date date,
load_timestamp timestamp,
modified_date timestamp default timestamp)


 if object_id('data_quality_foreign_keys_mapping') is not null drop TABLE data_quality_foreign_keys_mapping;

CREATE TABLE data_quality_foreign_keys_mapping
(dq_fk_map_id bigint identity
, dq_col_id_pk bigint not null
, dq_col_id_fk bigint not null
,load_timestamp datetime
,modified_date datetime default timestamp);



------additions to review

if object_id('data_quality_results') is not null drop TABLE data_quality_results;

create table data_quality_results
(dq_res_id bigint identity 
dq_check_detail_id bigint not null 
dq_run_id bigint not null 
result bigint 
pass_fail varchar(4) 
sql_processed varchar(8000) 
load_timestamp timestamp 
modified_date timestamp default timestamp)


 if object_id(data_quality_run_group) is not null drop TABLE data_quality_run_group;

create table data_quality_run_group
(dq_run_id bigint identity
 run_type varchar(100)
 load_timestamp timestamp
 modified_date timestamp default timestamp)

insert into data_quality_run_group
(run_type , load_timestamp)
values
('VESPA_DATA_QUALITY', GETDATE())


insert into data_quality_run_group
(run_type, load_timestamp)
values
('EXPERIAN_DATA', getdate())
commit

----------------------------------------------------------------

if object_id('data_quality_dp_data_to_analyze') is not null drop table data_quality_dp_data_to_analyze
if object_id('data_quality_dp_data_audit') is not null drop table data_quality_dp_data_audit
commit


create table data_quality_dp_data_audit
(viewing_date date ,
pk_viewing_prog_instance_fact bigint ,
cb_change_date date ,
dk_barb_min_start_datehour_dim integer ,
dk_barb_min_start_time_dim integer ,
dk_barb_min_end_datehour_dim integer ,
dk_barb_min_end_time_dim integer ,
dk_channel_dim integer ,
dk_event_start_datehour_dim integer ,
dk_event_start_time_dim integer ,
dk_event_end_datehour_dim integer ,
dk_event_end_time_dim integer ,
dk_instance_start_datehour_dim integer ,
dk_instance_start_time_dim integer ,
dk_instance_end_datehour_dim integer ,
dk_instance_end_time_dim integer ,
dk_programme_dim bigint ,
dk_programme_instance_dim bigint ,
dk_viewing_event_dim bigint ,
genre_description varchar (20),
sub_genre_description varchar (20),
service_type bigint ,
service_type_description varchar (40),
type_of_viewing_event varchar (40),
account_number varchar (20),
panel_id tinyint ,
live_recorded varchar (8),
barb_min_start_date_time_utc timestamp ,
barb_min_end_date_time_utc timestamp ,
event_start_date_time_utc timestamp ,
event_end_date_time_utc timestamp ,
instance_start_date_time_utc timestamp ,
instance_end_date_time_utc timestamp ,
dk_capping_end_datehour_dim integer ,
dk_capping_end_time_dim integer ,
capping_end_date_time_utc timestamp ,
log_start_date_time_utc timestamp ,
duration integer,
subscriber_id decimal (9,0) ,
log_received_start_date_time_utc timestamp,
capped_full_flag bit,
capped_partial_flag bit)


create table data_quality_dp_data_to_analyze
(viewing_date date ,
pk_viewing_prog_instance_fact bigint ,
cb_change_date date ,
dk_barb_min_start_datehour_dim integer ,
dk_barb_min_start_time_dim integer ,
dk_barb_min_end_datehour_dim integer ,
dk_barb_min_end_time_dim integer ,
dk_channel_dim integer ,
dk_event_start_datehour_dim integer ,
dk_event_start_time_dim integer ,
dk_event_end_datehour_dim integer ,
dk_event_end_time_dim integer ,
dk_instance_start_datehour_dim integer ,
dk_instance_start_time_dim integer ,
dk_instance_end_datehour_dim integer ,
dk_instance_end_time_dim integer ,
dk_programme_dim bigint ,
dk_programme_instance_dim bigint ,
dk_viewing_event_dim bigint ,
genre_description varchar (20),
sub_genre_description varchar (20),
service_type bigint ,
service_type_description varchar (40),
type_of_viewing_event varchar (40),
account_number varchar (20),
panel_id tinyint ,
live_recorded varchar (8),
barb_min_start_date_time_utc timestamp ,
barb_min_end_date_time_utc timestamp ,
event_start_date_time_utc timestamp ,
event_end_date_time_utc timestamp ,
instance_start_date_time_utc timestamp ,
instance_end_date_time_utc timestamp ,
dk_capping_end_datehour_dim integer ,
dk_capping_end_time_dim integer ,
capping_end_date_time_utc timestamp ,
log_start_date_time_utc timestamp ,
duration integer,
subscriber_id decimal (9,0) )

---------------------------------------------------------------------------------------------

--create the data quality vespa metrics table you will be storing all of these in.

if object_id('data_quality_vespa_metrics') is not null drop table data_quality_vespa_metrics
commit

create table data_quality_vespa_metrics
(dq_vm_id int identity,
metric_short_name varchar(200),
metric_description varchar(1000),
metric_benchmark decimal (16,3),
metric_tolerance_amber decimal (6,3),
metric_tolerance_red decimal (6,3),
load_timestamp timestamp ,
modified_date timestamp default timestamp)

----------------insert the metrics you want to start measuring against with some default benchmark values



delete from data_quality_vespa_metrics
----

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_hh_avg_precapped_live','03) - To collect the Average Daily Panel Viewing per Household per Viewing Day for Live Events (Pre-Capping/Scaling/Minute Attribution)',
9, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_hh_avg_precapped_rec','03) - To collect the Average Daily Panel Viewing per Household per Viewing Day for Recorded Events (Pre-Capping/Scaling/Minute Attribution)',
3, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_hh_avg_precapped_total','03) - To collect the Average Daily Panel Viewing per Household per Viewing Day for All Events (Pre-Capping/Scaling/Minute Attribution)',
10, 10, 50, getdate())
-----

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_hh_avg_postcapped_live','03) - To collect the Average Daily Panel Viewing per Household per Viewing Day for Live Events (Post-Capping/Scaling/Minute Attribution)',
5, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_hh_avg_postcapped_rec','03) - To collect the Average Daily Panel Viewing per Household per Viewing Day for Recorded Events (Post-Capping/Scaling/Minute Attribution)',
3, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_hh_avg_postcapped_total','03) - To collect the Average Daily Panel Viewing per Household per Viewing Day for All Events (Post-Capping/Scaling/Minute Attribution)',
6, 10, 50, getdate())

commit


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_stb_avg_precapped_live','To collect the Average Daily Panel Viewing per stb per Viewing Day for Live Events (Pre-Capping/Scaling/Minute Attribution)',
9, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_stb_avg_precapped_rec','To collect the Average Daily Panel Viewing per stb per Viewing Day for Recorded Events (Pre-Capping/Scaling/Minute Attribution)',
3, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_stb_avg_precapped_total','To collect the Average Daily Panel Viewing per stb per Viewing Day for All Events (Pre-Capping/Scaling/Minute Attribution)',
10, 10, 50, getdate())
-----

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_stb_avg_postcapped_live','To collect the Average Daily Panel Viewing per stb per Viewing Day for Live Events (Post-Capping/Scaling/Minute Attribution)',
5, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_stb_avg_postcapped_rec','To collect the Average Daily Panel Viewing per stb per Viewing Day for Recorded Events (Post-Capping/Scaling/Minute Attribution)',
3, 10, 50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_stb_avg_postcapped_total','To collect the Average Daily Panel Viewing per stb per Viewing Day for All Events (Post-Capping/Scaling/Minute Attribution)',
6, 10, 50, getdate())

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_total_no_of_events','To collect the total number of daily panel events per viewing day',
17000000, 10, 50, getdate())

commit


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('daily_viewing_total_no_of_instances','To collect the total number of daily panel instances per viewing day',
25000000, 10, 50, getdate())

commit


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('total_impacts_per_viewing_day','To collect the Total Impacts per Household per Impact Day(Post-Capping/Scaling/Minute Attribution)',
10000000, 10, 50, getdate())

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('average_impacts_per_viewing_day','To collect the Average Impacts per Household per Impact Day(Post-Capping/Scaling/Minute Attribution)',
5, 10, 50, getdate())

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('average_events_per_hh_per_viewing_day','10) - To collect the Avg of Daily Panel Viewing events per Household for each viewing day',
5, 10, 50, getdate())

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp)
values
('average_instances_per_hh_per_viewing_day','10) - To collect the Avg of Daily Panel Viewing instances per Household for each viewing day',
5, 10, 50, getdate())

commit


INSERT INTO data_quality_vespa_metrics
(METRIC_SHORT_NAME, METRIC_BENCHMARK,METRIC_TOLERANCE_AMBER, METRIC_TOLERANCE_RED, LOAD_TIMESTAMP)
(select LOWER('VDQ'||'_'||c.table_name||'_'||c.column_name||'_'||dq_check_type||'')metric_short_name 
, METRIC_BENCHMARK, METRIC_TOLERANCE_AMBER, METRIC_TOLERANCE_RED,GETDATE()
from data_quality_check_details a,
(select dq_check_type_id, dq_check_type from data_quality_check_type) b,
(select dq_col_id, column_name, table_name, creator from data_quality_columns) c,
data_quality_run_group d
where a.dq_check_type_id = b.dq_check_type_id
and a.dq_col_id = c.dq_col_id
and a.dq_sched_run_id = d.dq_run_id)

COMMIT

select * into  #tmp_ins
from
(select dq_check_detail_id, LOWER('VDQ'||'_'||c.table_name||'_'||c.column_name||'_'||dq_check_type||'')metric_short_name 
, METRIC_BENCHMARK, METRIC_TOLERANCE_AMBER, METRIC_TOLERANCE_RED,GETDATE() load_timestamp
from data_quality_check_details a,
(select dq_check_type_id, dq_check_type from data_quality_check_type) b,
(select dq_col_id, column_name, table_name, creator from data_quality_columns) c,
data_quality_run_group d
where a.dq_check_type_id = b.dq_check_type_id
and a.dq_col_id = c.dq_col_id
and a.dq_sched_run_id = d.dq_run_id) t


update data_quality_check_details a
set metric_short_name =
(select metric_short_name from #tmp_ins b
where a.dq_check_detail_id = b.dq_check_detail_id)

commit

insert into data_quality_check_details
(dq_col_Id, dq_SCHED_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,unknown_value, load_timestamp)
select distinct dq_col.dq_col_Id,1,
dq_check.dq_check_type_id, 0,10,50, '-1', @load_date from
data_quality_columns dq_col, data_quality_check_type dq_check
where lower(dq_col.column_name) like '%_dim'
and lower(dq_col.table_name) = 'data_quality_dp_data_audit'
and dq_check_type = 'UNKNOWN_CHECK'

commit

declare @load_date datetime

set @load_date = getdate()

insert into data_quality_check_details
(dq_col_Id, dq_SCHED_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,unknown_value, load_timestamp)
select distinct dq_col.dq_col_Id,1,
dq_check.dq_check_type_id, 0,10,50, '-1', @load_date from
data_quality_columns dq_col, data_quality_check_type dq_check
where lower(dq_col.table_name) = 'data_quality_dp_data_audit'
and dq_check_type = 'ISNULL_CHECK'

COMMIT


declare @load_date datetime

set @load_date = getdate()

insert into data_quality_check_details
(dq_col_Id, dq_SCHED_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,unknown_value, load_timestamp)
select distinct dq_col.dq_col_Id,1,
dq_check.dq_check_type_id, 0,10,50, '-1', @load_date from
data_quality_columns dq_col, data_quality_check_type dq_check
where lower(dq_col.table_name) = 'data_quality_dp_data_audit'
and upper(dq_col.column_name) like 'PK%'
and dq_check_type = 'PRIMARY_KEY_CHECK'

commit


-------------------------------------------------------------------------------------------

CREATE TABLE VIQ_ALL_Boxes_viq_dq
	(account_status varchar(50)
  , account_number varchar (20)
	, region	varchar(70)
	, current_package	varchar(50)
	, tenure	varchar(20)
	, box_type	varchar(30)
	, h_lifestage	varchar(50)
	, HH_Box_Comp varchar(50)
	, value_seg varchar(15)
	, Net_Status int
	, in_Panel int not null DEFAULT 0
	);


CREATE HG INDEX idx1_viq_dq ON VIQ_ALL_Boxes_viq_dq(account_number)
COMMIT


CREATE TABLE VIQ_Indexes_viq_dq
( Metric_ID int, 
  Metric_Desc varchar(20),
  Panel int,
  Metric_Label varchar(100),
  Metric_Value int, 
  Date_Created Datetime  
  );
-----------------------------------------------------------------------


alter table data_quality_vespa_metrics
add(metric_grouping varchar(30))

select * from data_quality_vespa_metrics

update data_quality_vespa_metrics
set metric_grouping = null

commit

update data_quality_vespa_metrics
set metric_grouping = 'data_integrity'
where metric_short_name like 'vdq%'

commit

update data_quality_vespa_metrics
set metric_grouping = 'household_metrics'
where metric_short_name like 'sca%'

commit

update data_quality_vespa_metrics
set metric_grouping = 'individual_metrics'
where metric_short_name like '%stb_avg%'

commit

update data_quality_vespa_metrics
set metric_grouping = 'data_totals'
where metric_grouping is null

commit

-----------------------------------------------------------------------


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,metric_tolerance_red, load_timestamp, metric_grouping)
SELECT distinct 
replace(replace('sca_viq_data_quality_'||metric_desc||'_'||coalesce(metric_label,'null')||'_sky','&',''),' ','') ,
replace(replace('sca_viq_data_quality_'||metric_desc||'_'||coalesce(metric_label,'null')||'_sky','&',''),' ','') ,
100000,10,50,today(), 'household_metrics'
from VIQ_Indexes_viq_dq
where metric_id = 337

commit
--------------------------------------------------------------------


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,metric_tolerance_red, load_timestamp, metric_grouping)
SELECT distinct 
replace(replace('sca_viq_data_quality_'||metric_desc||'_12_'||coalesce(metric_label,'null')||'','&',''),' ','') ,
replace(replace('sca_viq_data_quality_'||metric_desc||'_12_'||coalesce(metric_label,'null')||'','&',''),' ','') ,
100000,10,50,today(), 'household_metrics'
from VIQ_Indexes_viq_dq
where metric_id = 337

commit


-------------------------------------------UAT process - create table in own schema---------------------------------------------------------


select * into DATA_QUALITY_SLOT_DATA_AUDIT from kinnairt.DATA_QUALITY_SLOT_DATA_AUDIT
select * into data_quality_dp_data_audit from kinnairt.data_quality_dp_data_audit
select * into data_quality_check_type from kinnairt.data_quality_check_type
select * into data_quality_columns from kinnairt.data_quality_columns
select * into data_quality_run_group from kinnairt.data_quality_run_group
select * into data_quality_check_details from kinnairt.data_quality_check_details
select * into data_quality_vespa_repository_reporting from kinnairt.data_quality_vespa_repository_reporting
select * into data_quality_results from kinnairt.data_quality_results
select * into data_quality_vespa_metrics from kinnairt.data_quality_vespa_metrics
select * into data_quality_vespa_repository from kinnairt.data_quality_vespa_repository
select * into data_quality_dp_data_to_analyze from kinnairt.data_quality_dp_data_to_analyze
select * into viq_indexes_index_viq_dq from kinnairt.viq_indexes_index_viq_dq
select * into viq_indexes_indextotal_viq_dq from kinnairt.viq_indexes_indextotal_viq_dq
select * into VIQ_Indexes_viq_dq from kinnairt.VIQ_Indexes_viq_dq
select * into VIQ_ALL_Boxes_viq_dq from kinnairt.VIQ_ALL_Boxes_viq_dq

---------------------------------------------------------------------------------------------------


---------additional metrics added for STB on 14/05/2013

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_precapped_primary_live','To collect the Average Daily Panel Viewing per stb per Viewing Day for Live Events (Pre-Capping/Scaling/Minute Attribution) on Primary Boxes',
10,10,50,getdate(), 'individual_metrics')


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_precapped_primary_recorded','To collect the Average Daily Panel Viewing per stb per Viewing Day for Recorded Events (Pre-Capping/Scaling/Minute Attribution) on Primary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_precapped_primary_total','To collect the Average Daily Panel Viewing per stb per Viewing Day for All Events (Pre-Capping/Scaling/Minute Attribution) on Primary Boxes',
10,10,50,getdate(), 'individual_metrics')


insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_precapped_secondary_live','To collect the Average Daily Panel Viewing per stb per Viewing Day for Live Events (Pre-Capping/Scaling/Minute Attribution) on Secondary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_precapped_secondary_recorded','To collect the Average Daily Panel Viewing per stb per Viewing Day for Recorded Events (Pre-Capping/Scaling/Minute Attribution) on Secondary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_precapped_secondary_total','To collect the Average Daily Panel Viewing per stb per Viewing Day for All Events (Pre-Capping/Scaling/Minute Attribution) on Secondary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_postcapped_primary_live','To collect the Average Daily Panel Viewing per stb per Viewing Day for Live Events (Post-Capping/Scaling/Minute Attribution) on Primary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_postcapped_primary_recorded','To collect the Average Daily Panel Viewing per stb per Viewing Day for Recorded Events (Post-Capping/Scaling/Minute Attribution) on Primary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_postcapped_primary_total','To collect the Average Daily Panel Viewing per stb per Viewing Day for Total Events (Post-Capping/Scaling/Minute Attribution) on Primary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_postcapped_secondary_live','To collect the Average Daily Panel Viewing per stb per Viewing Day for Live Events (Post-Capping/Scaling/Minute Attribution) on Secondary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_postcapped_secondary_recorded','To collect the Average Daily Panel Viewing per stb per Viewing Day for Recorded Events (Post-Capping/Scaling/Minute Attribution) on Secondary Boxes',
10,10,50,getdate(), 'individual_metrics')

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber,
metric_tolerance_red, load_timestamp, metric_grouping)
values
('daily_viewing_stb_avg_postcapped_secondary_total','To collect the Average Daily Panel Viewing per stb per Viewing Day for Total Events (Post-Capping/Scaling/Minute Attribution) on Secondary Boxes',
10,10,50,getdate(), 'individual_metrics')

commit

-------------------------------------------------------------------------------------------------------------------------------------

--upscale table for sky base

create table data_quality_sky_base_upscale
(sky_base_upscale_total bigint,
event_date date)

insert into data_quality_sky_base_upscale
select distinct 10100000, broadcast_day_date
from sk_prod.viq_date
where broadcast_day_date between '2012-12-01' and today() + 70


-------------------------------------------------------------------------------------------------------------------------------------

----------------------new scaling metrics insert into vespa metrics table------------------------------------------------------------

-------------------------------------------------coverage insert metrics---------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp,metric_grouping, current_flag)
select 
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'coverage'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'coverage'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_desc,
b.metric_value, 10,50,getdate(), 'household_totals',1
from
(select metric_desc,segment,coverage metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'coverage'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
--------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------INDEX INSERT METRICS-----------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp,metric_grouping, current_flag)
select 
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'index'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'index'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_desc,
b.metric_value, 10,50,getdate(), 'household_totals',1
from
(select metric_desc,segment,index_var metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'index'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b


---------------------------------------------------------------sky_actual daily-----------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp,metric_grouping, current_flag)
select 
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky_actual'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_short_name,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky_actual'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_desc,
b.metric_value, 10,50,getdate(), 'household_totals',1
from
(select metric_desc,segment,sky_base_actual metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky_actual'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_short_name
from scaling_variables_viq_dq) b

---------------------------------------------------------------------------------------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp,metric_grouping, current_flag)
select 
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_short_name,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_desc,
b.metric_value, 10,50,getdate(), 'household_totals',1
from
(select metric_desc,segment,sky_base metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_short_name
from scaling_variables_viq_dq) b


--------------------------------------------------------------------------------------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp,metric_grouping, current_flag)
select 
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||12||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||12||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_desc,
b.metric_value, 10,50,getdate(), 'household_totals',1
from
(select metric_desc,segment,vespa_base metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||12||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')  metric_short_name
from scaling_variables_viq_dq) b



commit

update data_quality_vespa_metrics
set current_flag = 0
where dq_vm_id in (406,461,516,571,626)

commit


-------------------------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_capped_end_greater_event_start','To check if there are any instances where capped end time is greater than the event start time.',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_vespa_dp_events_duration_negative','To check if there are any instances where event duration is null or negative.',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_tmp_capped_end_time_date_chk','To check that the capping end date also has a time flag.',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_capped_flag_consistency_chk','To check that the capping flags are consistent within the events data.',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_capped_flag_endtime_consistency_chk','To check that the capping flag is consistent with the population of the capping end time field.',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_barb_start_end_consistency_chk','To check the consistency of the barb start and end date and time fields.',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_barb_start_end_overlap_chk','To check if there are instances where the barb min start end has overlapped for a subscriber within an event day.  This should not happen generally. ',
0, 10, 50, getdate(),'data_integrity',1)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark,metric_tolerance_amber,metric_tolerance_red, load_timestamp,metric_grouping, current_flag)
values
('mtc_barb_start_end_time_date_consistency_chk','To check the consistency of the barb start date and time along with the barb end date and time fields.',
0, 10, 50, getdate(),'data_integrity',1)

commit
