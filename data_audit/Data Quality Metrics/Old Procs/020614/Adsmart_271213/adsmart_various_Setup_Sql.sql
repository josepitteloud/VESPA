
insert into data_quality_columns
(creator, table_name, column_name, column_type, column_length, load_timestamp)
select creator, tname, cname, coltype, length, getdate() from sys.syscolumns
where lower(tname) in
('data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_slot_data_audit')
and lower(creator) = 'kinnairt'
order by tname, colno

commit

commit
select top 100 * from z_logger_events
order by 1 desc
commit


select * from data_quality_columns
where lower(table_name) in
('data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_slot_data_audit')
and lower(creator) = 'kinnairt'


---------------------------------------------------------primary key checks------------------------------------------------------------------

select top 1000 * from data_quality_vespa_metrics
where metric_short_name like '%primary_key%'

insert into data_quality_vespa_metrics
(metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_grouping,current_flag)
values
('adsm_data_quality_adsmart_slot_data_audit_fact_viewing_slot_instance_key_primary_key_check','Primary Key check for pk_viewing_slot_instance_fact', 0, 10, 50, getdate(), 'data_integrity',1)

commit

select * from data_quality_vespa_metrics
order by 1 desc

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red,load_timestamp, metric_short_name)
select dq_col.dq_col_id, dq_run_grp.dq_run_id, dq_check_type_id, 0,10,50,getdate(),'adsm_data_quality_adsmart_slot_data_audit_fact_viewing_slot_instance_key_primary_key_check' metric_short_name
from
data_quality_columns dq_col,
data_quality_run_group dq_run_grp,
data_quality_check_type dq_chk_typ,
data_quality_columns fk_dq_col
where dq_run_grp.RUN_TYPE = 'ADSMART_DATA_QUALITY'
AND lower(dq_col.TABLE_NAME) = 'data_quality_adsmart_slot_data_audit'
and dq_col.column_name = 'fact_viewing_slot_instance_key'
and dq_chk_typ.dq_check_type  = 'PRIMARY_KEY_CHECK'
and fk_dq_col.column_name 

select * from data_quality_check_details
order by 1 desc


-------------------------------------------------------foreign key check---------------------------------------------------------------------------


select top 1000 * from data_quality_vespa_metrics
where metric_short_name like '%foreign_key%'

select top 10 * from sk_prod.dim_agency
------------------------------------------------------------------------------------------------------------------------------------




select top 10 * from data_quality_check_details
where metric_short_name like '%foreign_key%'

select top 10 * from data_quality_vespa_metrics

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name, 'FOREIGN KEY check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where creator = 'kinnairt'
and lower(table_name) in 
(
'data_quality_adsmart_slot_data_audit')
and lower(column_name) not like 'cb%'
and lower(column_name) like '%key'
and lower(column_name) not like '%viewing_slot_instance%'

commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%date_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('viq_date')
and lower(column_name) = 'pk_datehour_dim') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit
--done
data_quality_adsmart_slot_data_audit - sk_prod.viq_date
broadcast_start_date_key - pk_datehour_dim
data_quality_adsmart_slot_data_audit -  sk_prod.viq_date
viewed_start_date_key - pk_datehour_dim
data_quality_adsmart_slot_data_audit -  sk_prod.viq_date
preceding_programme_broadcast_start_date_key - pk_datehour_dim
data_quality_adsmart_slot_data_audit -  sk_prod.viq_date
preceding_programme_broadcast_end_date_key - pk_datehour_dim
data_quality_adsmart_slot_data_audit -  sk_prod.viq_date
succ_programme_broadcast_start_date_key - pk_datehour_dim
data_quality_adsmart_slot_data_audit -  sk_prod.viq_date
succ_programme_broadcast_end_date_key - pk_datehour_dim
---------------------


---------------time

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%time_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('viq_time')
and lower(column_name) = 'pk_time_dim') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-----time done 
data_quality_adsmart_slot_data_audit - sk_prod.viq_time
broadcast_start_time_key - pk_time_dim

data_quality_adsmart_slot_data_audit -  sk_prod.viq_time
viewed_start_time_key - pk_time_dim

data_quality_adsmart_slot_data_audit -  sk_prod.viq_time
preceding_programme_broadcast_start_time_key - pk_time_dim

data_quality_adsmart_slot_data_audit -  sk_prod.viq_time
preceding_programme_broadcast_end_time_key - pk_time_dim


data_quality_adsmart_slot_data_audit -  sk_prod.viq_time
succ_programme_broadcast_start_time_key - pk_time_dim

data_quality_adsmart_slot_data_audit -  sk_prod.viq_time
succ_programme_broadcast_end_time_key - pk_time_dim

----------------------------------------------------------------------------


---------------programme schedule----------------------------------------

---------------time

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%programme_schedule_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_broadcast_programme_schedule')
and lower(column_name) = 'broadcast_programme_schedule_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-------------------------------done--------------------------------------
data_quality_adsmart_slot_data_audit - sk_prod.dim_broadcast_programme_Schedule
preceding_programme_schedule_key - broadcast_programme_schedule_key

data_quality_adsmart_slot_data_audit - sk_prod.dim_broadcast_programme_Schedule
succeding_programme_schedule_key - broadcast_programme_schedule_key
---------------------------------------------------------------------------

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%adsmart_campaign_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_adsmart_campaign')
and lower(column_name) = 'adsmart_campaign_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

data_quality_adsmart_slot_data_audit - sk_prod.dim_adsmart_campaign
adsmart_campaign_key - adsmart_campaign_key

-----------------------------------------------------------------------------------------------------------
commit

drop table #tmp_cols_1

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%agency_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_agency')
and lower(column_name) = 'agency_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

---------------------------------------------------------------------------
data_quality_adsmart_slot_data_audit - sk_prod.dim_agency
agency_key - agency_key
----------------------------------------------------------------------------

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%broadcast_channel_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_broadcast_channel')
and lower(column_name) = 'broadcast_channel_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

------------------------------------------------------------------------------
data_quality_adsmart_slot_data_audit - sk_prod.dim_broadcast_channel
broadcast_channel_key - broadcast_channel_key
------------------------------------------------------------------------------


select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%segment_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_segment')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'segment_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-----------------------------------------------------------------------------------
data_quality_adsmart_slot_data_audit - sk_prod.dim_segment
segment_key - segment_key
-----------------------------------------------------------------------------------



select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%slot_copy_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_slot_copy')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'slot_copy_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-------------------------------------------------------------------------------
data_quality_adsmart_slot_data_audit - sk_prod.dim_slot_copy
slot_copy_key - slot_copy_key
--------------------------------------------------------------------------------


select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%slot_reference_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_slot_reference')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'slot_reference_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

--------------------------------------------------------------------------------
data_quality_adsmart_slot_data_audit - sk_prod.dim_slot_reference
slot_reference_key - slot_reference_key
--------------------------------------------------------------------------------



select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) like '%time_shift_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('viq_time_shift')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'pk_timeshift_dim') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit
--------------------------------------------------------------------------------

data_quality_adsmart_slot_data_audit -  sk_prod.viq_time_shift
time_shift_key - pk_timeshift_dim
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------------------------------------------------------------------------------------

select top 10 * from data_quality_adsmart_hh_data_audit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name, 'FOREIGN KEY check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where creator = 'kinnairt'
and lower(table_name) in 
(
'data_quality_adsmart_hh_data_audit')
and lower(column_name) not like 'cb%'
and lower(column_name) like '%key'
and lower(column_name) not like '%date_key'
and lower(column_name) not like '%viewing_slot_instance%'

commit


select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_hh_data_audit')
and lower(column_name) like '%household_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('viq_household')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'household_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-------------------------------------------------------------------------------------------------------------------
data_quality_adsmart_hh_data_audit -  sk_prod.viq_household
household_key - household_key
-------------------------------------------------------------------------------------------------------------------



select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_hh_data_audit')
and lower(column_name) like '%segment_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_segment')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'segment_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit
-------------------------------------------------------------------------------------------------------------

data_quality_adsmart_hh_data_audit - sk_prod.dim_segment
segment_key -  segment_key
--------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------

select top 10 * from data_quality_adsmart_campaign_data_audit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_campaign_data_audit')
and lower(column_name) like '%segment_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_segment')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'segment_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

data_quality_adsmart_campaign_data_audit - sk_prod.dim_segment
segment_key - segment_key

---------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name, 'FOREIGN KEY check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where creator = 'kinnairt'
and lower(table_name) in 
(
'data_quality_adsmart_campaign_data_audit')
and lower(column_name) not like 'cb%'
and lower(column_name) like '%key'
--and lower(column_name) not like '%date_key'
and lower(column_name) not like '%viewing_slot_instance%'

commit



select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_campaign_data_audit')
and lower(column_name) like '%adsmart_campaign_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_adsmart_campaign')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'adsmart_campaign_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_campaign_data_audit')
and lower(column_name) like '%agency_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_agency')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'agency_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-----------------------------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name, 'FOREIGN KEY check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where creator = 'kinnairt'
and lower(table_name) in 
(
'data_quality_adsmart_segment_data_audit')
and lower(column_name) not like 'cb%'
and lower(column_name) like '%key'
and lower(column_name) not like '%date_key'
and lower(column_name) not like '%viewing_slot_instance%'

commit



select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit')
and lower(column_name) like '%segment_key') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'FOREIGN_KEY_CHECK') C,
(select dq_col_id fk_dq_col_id from data_quality_columns
where lower(table_name) in ('dim_segment')
and lower(creator) = 'sk_prod'
and lower(column_name) = 'segment_key') d)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,fk_dq_col_id, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_foreign_key_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

------------------------------------------------------------------------------------------------------------------------------------------

--UNKNOWN VALUE -1

begin
insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_-1'metric_short_name, '-1 unknown check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where creator = 'kinnairt'
and lower(table_name) in 
('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')

commit

select top 100 * from data_quality_vespa_metrics
order by 1 desc

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp, '-1' unknown_value from data_quality_check_type
where dq_check_type = 'UNKNOWN_CHECK') C)


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value, metric_short_name)
select distinct a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_-1' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

--UNKNOWN VALUE (-99)

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_-99'metric_short_name, '-99 unknown check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp, '-99' unknown_value from data_quality_check_type
where dq_check_type = 'UNKNOWN_CHECK') C)


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_-99' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit
-----------------------------------------------IS NULL CHECK

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_isnull_check'metric_short_name, 'ISNULL check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')
commit


select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'ISNULL_CHECK') C)


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_isnull_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit


------------------------------------------------------unknown specific checks--------------------------------------------------------

sales_house_name, sales_house_short_name, adsmart_action
'n/a'


delete from data_quality_vespa_metrics
where dq_vm_id > 1683

commit

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_na'metric_short_name, 'NA unknown check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('sales_house_name', 'sales_house_short_name', 'adsmart_action')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('sales_house_name', 'sales_house_short_name', 'adsmart_action')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp, 'n/a' unknown_value from data_quality_check_type
where dq_check_type = 'UNKNOWN_CHECK') C)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_na' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

 from data_quality_check_details
order by 1 desc

delete from 

wheer unk


----------------------------------------------------------------------------------------------------------------------------------

media_adsmart_status
'(n/a)'


insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_na'metric_short_name, 'NA unknown check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('media_adsmart_status')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('media_adsmart_status')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp, '(n/a)' unknown_value from data_quality_check_type
where dq_check_type = 'UNKNOWN_CHECK') C)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_na' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

---------------------------------------------------------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_unknown'metric_short_name, 'Unknown unknown check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('buyer_name', 'advertiser_name', 'advertiser_code')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('buyer_name', 'advertiser_name', 'advertiser_code')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp, 'Unknown' unknown_value from data_quality_check_type
where dq_check_type = 'UNKNOWN_CHECK') C)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_unknown' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit
----------------------------------------------------------------------------------------------------------------------------------------

insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_othertv'metric_short_name, 'Other TV unknown check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('vespa_channel_name')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('vespa_channel_name')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp, 'Other TV' unknown_value from data_quality_check_type
where dq_check_type = 'UNKNOWN_CHECK') C)

insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value, metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_unknown_check_other_tv' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit

-----------------------------------------------------------------
select top 100 * from data_quality_adsmart_segment_data_audit

select top 100 * from data_quality_adsmart_campaign_data_audit

select top 100 * from data_quality_adsmart_hh_data_audit


----------------------------------------------------------------------------------------------------------------------------------------------------

--TABLE COUNTS

insert into data_quality_columns
(creator, table_name, column_name, column_type, column_length, load_timestamp)
select distinct 'kinnairt' creator,lower(table_name) table_name, '1' column_name, 'varchar' column_type, 1 column_length, getdate() load_timestamp from 
data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')

commit


select * from data_quality_check_details
where lower(metric_short_name) like ('%count%')
-------------------------------------------------------------------------------


insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_table_count_check'metric_short_name, 
'Table Count check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_integrity',1
from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('1')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_segment_data_audit',
'data_quality_adsmart_campaign_data_audit',
'data_quality_adsmart_hh_data_audit',
'data_quality_adsmart_slot_data_audit')
and column_name = '1') a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,60000000 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'TABLE_COUNT_CHECK') C)

select top 10 * from data_quality_check_details
order by 1 desc


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_table_count_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit
--------------------------------------------------------------------------------------------------------------------------------------------------------


insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_distinct_count_check'metric_short_name, 'Distinct Count check for '||table_name||'.'||column_name'' , 0 metric_benchmark,10,50,getdate(), 'data_totals',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('broadcast_channel_key')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('broadcast_channel_key')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,0 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'DISTINCT_COUNT_CHECK') C)


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_distinct_count_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit



insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_distinct_count_check'metric_short_name, 'Distinct Count check for '||table_name||'.'||column_name'' , 40 metric_benchmark,10,50,getdate(), 'data_totals',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('adsmart_campaign_key')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('adsmart_campaign_key')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,40 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'DISTINCT_COUNT_CHECK') C)


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_distinct_count_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit



insert into data_quality_vespa_metrics
(metric_short_name, metric_description,metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp, metric_grouping, current_flag)
select 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_distinct_count_check'metric_short_name, 'Distinct Count check for '||table_name||'.'||column_name'' , 40 metric_benchmark,10,50,getdate(), 'data_totals',1
from data_quality_columns
where lower(table_name) in (
'data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('segment_key')
commit

select * into #tmp_cols_1 from
((select dq_col_id from data_quality_columns
where lower(table_name) in ('data_quality_adsmart_slot_data_audit')
and lower(column_name) in 
('segment_key')) a,
(select dq_run_id from data_quality_run_group
where run_type = 'ADSMART_DATA_QUALITY') b,
(select dq_check_type_id,40 metric_benchmark,10 amber,50 red,GETDATE() load_timestamp from data_quality_check_type
where dq_check_type = 'DISTINCT_COUNT_CHECK') C)


insert into data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_short_name)
select a.*, b.metric_short_name from
#tmp_cols_1 a,
(select dq_col_id dq_id, 'adsm_'||lower(table_name)||'_'||lower(column_name)||'_distinct_count_check' metric_short_name
from data_quality_columns) b
where a.dq_col_id = b.dq_id

commit




select a.account_number, min(b.adjusted_event_start_date_vespa) min_report_date,
max(b.adjusted_event_start_date_vespa) max_report_date
into tst_panel_rep_2
from
(select account_number from sk_prod.vespa_panel_status
where panel_no = 12) a
left outer join
sk_prod.viq_viewing_data_Scaling b
on a.account_number = b.account_number
where b.adjusted_event_start_date_vespa between '2013-10-30' and '2013-12-08'
group by a.account_number

commit
--metrics insert into data_quality_vespa_metrics table

insert into data_quality_vespa_metrics
(metric_short_name,metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp)
values
('mtc_adsmart_slot_zero_duration_per_viewing_day',' Slots which have a zero or less duration per Broadcast Day', 0,10,50, getdate())

insert into data_quality_vespa_metrics
(metric_short_name,metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp)
values
('mtc_adsmart_slot_events_weight_assigned_per_viewing_day',' Slots which have a zero or less weight attached per Broadcast Day', 0,10,50, getdate())

commit

insert into data_quality_run_group
(run_type, load_timestamp)
values
('ADSMART_DATA_QUALITY',getdate())

commit


select metric_short_name, metric_description, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,metric_grouping,




-------------------------------------------------------------------------------------------------------------------------------------------------
/*
select 
--adsmart slots
adsmart_slots.fact_viewing_slot_instance_key, 
adsmart_slots.adsmart_campaign_key, adsmart_slots.agency_key, adsmart_slots.broadcast_channel_key, adsmart_slots.broadcast_start_date_key,
adsmart_slots.broadcast_start_time_key, adsmart_slots.preceding_programme_schedule_key, 
adsmart_slots.succeeding_programme_schedule_key,adsmart_slots.segment_key, 
adsmart_slots.slot_copy_key, adsmart_slots.slot_reference_key,adsmart_slots.time_shift_key,adsmart_slots.viewed_start_date_key, 
adsmart_slots.viewed_start_time_key, 
adsmart_slots.actual_impacts, adsmart_slots.actual_impressions, adsmart_slots.actual_impressions_day_one_weighted, adsmart_slots.actual_serves,
adsmart_slots.actual_weight, adsmart_slots.sample_impressions,
--time_shift
time_shift.viewed_duration,
time_shift.viewing_time, time_shift.timeshift_band,time_shift.elapsed_days,
time_shift.elapsed_hours,time_shift.elapsed_hours_total,
--agency
agency.advertiser_code, agency.advertiser_name, agency.agency_key, 
agency.barb_sales_house_id, agency.buyer_code, agency.sales_house_name, agency.sales_house_short_name,
agency.buyer_name,
--broadcast_channel
 broadcast_channel.channel_format,broadcast_channel.media_adsmart_status, broadcast_channel.vespa_channel_name,
 --preceding_broadcast_programme_schedule
 preceding_broadcast_programme.preceding_programme_broadcast_start_date_key,
 preceding_broadcast_programme.preceding_programme_broadcast_start_time_key,  
 preceding_broadcast_programme.preceding_programme_broadcast_end_date_key, 
 preceding_broadcast_programme.preceding_programme_broadcast_end_time_key,
 --succ_broadcast_programme_schedule
succ_broadcast_programme.succ_programme_broadcast_start_date_key,
succ_broadcast_programme.succ_programme_broadcast_start_time_key,  
succ_broadcast_programme.succ_programme_broadcast_end_date_key, 
succ_broadcast_programme.succ_programme_broadcast_end_time_key,
--segment
segment.segment_id, segment.segment_name, segment.segment_status, segment.segment_description,
--slot_copy
slot_copy.slot_copy_duration_seconds, slot_copy.slot_type, slot_copy.product_code, slot_copy.product_name,
--slot_reference
slot_reference.slot_reference_slot_type, slot_reference.slot_sub_type, slot_reference.slot_duration_seconds,slot_reference.slot_duration_reported_Seconds,
slot_reference.spot_position_in_break,slot_reference.slot_type_position, 
slot_reference.slot_type_total_position, 
slot_reference.break_position, 
slot_reference.adsmart_action, 
slot_reference.adsmart_priority, 
slot_reference.adsmart_status, slot_reference.adsmart_total_priority,
--broadcast start time
broadcast_start_time.broadcast_start_utc_time, broadcast_start_time.start_broadcast_time, broadcast_start_time.start_spot_standard_daypart_uk,
-- broadcast end time
broadcast_end_time.broadcast_end_utc_time, broadcast_end_time.end_broadcast_time, broadcast_end_time.end_spot_standard_daypart_uk,
--viewing start time
viewing_start_time.viewing_start_utc_time, viewing_start_time.viewing_start_broadcast_time, viewing_start_time.viewing_start_spot_standard_daypart_uk,
--broadcast start date
broadcast_start_date.broadcast_start_datehour_utc,  broadcast_start_date.broadcast_start_day_date,  
broadcast_start_date.broadcast_start_weekday,broadcast_start_date.broadcast_start_day_in_month, 
 broadcast_start_date.broadcast_start_day_in_week, broadcast_start_date.broadcast_start_day_long,
 broadcast_start_date.utc_start_day_date,  
 broadcast_start_date.utc_start_weekday, 
broadcast_start_date.utc_start_day_in_month,  broadcast_start_date.utc_start_day_in_week,
broadcast_start_date.utc_start_day_long 
--broadcast end date
 broadcast_end_date.broadcast_end_datehour_utc, broadcast_end_date.broadcast_end_day_date, 
broadcast_end_date.broadcast_end_weekday, broadcast_end_date.broadcast_end_day_in_month, 
 broadcast_end_date.broadcast_end_day_in_week, broadcast_end_date.broadcast_end_day_long,
 broadcast_end_date.utc_end_day_date,  broadcast_end_date.utc_end_weekday, 
 broadcast_end_date.utc_end_day_in_month,  broadcast_end_date.utc_end_day_in_week,
 broadcast_end_date.utc_end_day_long,
 --viewing start date
 viewing_start_date.viewing_start_datehour_utc,  viewing_start_date.viewing_start_day_date, 
viewing_start_date.viewing_start_weekday, viewing_start_date.viewing_start_day_in_month, 
viewing_start_date.viewing_start_day_in_week, viewing_start_date.viewing_start_day_long,
 viewing_start_date.utc_viewing_start_day_date, viewing_start_date.utc_viewing_start_weekday, 
viewing_start_date.utc_viewing_start_day_in_month,  viewing_start_date.utc_viewing_start_day_in_week,
viewing_start_date.utc_viewing_start_day_long
  from #adsmart_slots adsmart_slots
left outer join
(select time_shift_key, viewing_time, timeshift_band,elapsed_days,elapsed_hours,
elapsed_hours_total from sk_prod.viq_time_shift) time_shift
on adsmart_slots.time_shift_key = time_shift.time_shift_key
left outer join
(select  advertiser_code, advertiser_name, agency_key, barb_sales_house_id, buyer_code, sales_house_name, sales_house_short_name,
buyer_name from sk_prod.DIM_AGENCY) agency
on adsmart_slots.agency_key = agency.agency_key
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key preceding_programme_broadcast_start_date_key,
broadcast_start_time_key preceding_programme_broadcast_start_time_key,  
broadcast_end_date_key preceding_programme_broadcast_end_date_key, 
broadcast_end_time_key preceding_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) preceding_broadcast_programme
on
adsmart_slots.preceding_programme_schedule_key = preceding_broadcast_programme.broadcast_programme_schedule_key
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key succ_programme_broadcast_start_date_key,
broadcast_start_time_key succ_programme_broadcast_start_time_key,  
broadcast_end_date_key succ_programme_broadcast_end_date_key, 
broadcast_end_time_key succ_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) succ_broadcast_programme
on
adsmart_slots.succeeding_programme_schedule_key = succ_broadcast_programme.broadcast_programme_schedule_key
left outer join
(select segment_key, segment_id, segment_name, 
segment_status, segment_description from sk_prod.DIM_SEGMENT) segment
on
adsmart_slots.segment_key = segment.segment_key
left outer join
(select slot_copy_key,slot_copy_duration_seconds, slot_type, 
product_code, product_name  from sk_prod.DIM_SLOT_COPY) slot_copy
on
adsmart_slots.slot_copy_key = slot_copy.slot_copy_key
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
adsmart_slots.slot_reference_key = slot_reference.slot_reference_key
left outer join
(select  pk_time_dim, utc_time_minute broadcast_start_utc_time, 
broadcast_time start_broadcast_time, spot_standard_daypart_uk start_spot_standard_daypart_uk
from sk_prod.viq_time) broadcast_start_time
on
adsmart_slots.broadcast_start_time_key = broadcast_start_time.pk_time_dim
left outer join
(select  pk_time_dim, utc_time_minute broadcast_end_utc_time, 
broadcast_time end_broadcast_time, spot_standard_daypart_uk end_spot_standard_daypart_uk
from sk_prod.viq_time) broadcast_end_time
on
adsmart_slots.broadcast_end_time_key = broadcast_end_time.pk_time_dim
left outer join
(select pk_time_dim, utc_time_minute viewing_start_utc_time, 
broadcast_time viewing_start_broadcast_time, spot_standard_daypart_uk  
viewing_start_spot_standard_daypart_uk
from sk_prod.viq_time) viewing_start_time
on
adsmart_slots.viewing_start_time_key = viewing_start_time.pk_time_dim
left outer join
(select pk_datehour_dim, utc_datehour broadcast_start_datehour_utc, broadcast_day_date broadcast_start_day_date, 
broadcast_weekday broadcast_start_weekday,broadcast_day_in_month broadcast_start_day_in_month, 
broadcast_day_in_week broadcast_start_day_in_week,broadcast_day_long broadcast_start_day_long,
utc_day_date utc_start_day_date, utc_weekday utc_start_weekday, 
utc_day_in_month utc_start_day_in_month, utc_day_in_week utc_start_day_in_week,
 utc_day_long utc_start_day_long from sk_prod.viq_date) broadcast_start_date
on
 adsmart_slots.broadcast_start_date_key = broadcast_start_date.pk_datehour_dim
(select pk_datehour_dim, utc_datehour broadcast_end_datehour_utc, broadcast_day_date broadcast_end_day_date, 
broadcast_weekday broadcast_end_weekday,broadcast_day_in_month broadcast_end_day_in_month, 
broadcast_day_in_week broadcast_end_day_in_week,broadcast_day_long broadcast_end_day_long,
utc_day_date utc_end_day_date, utc_weekday utc_end_weekday, 
utc_day_in_month utc_end_day_in_month, utc_day_in_week utc_end_day_in_week,
 utc_day_long utc_end_day_long from sk_prod.viq_date) broadcast_end_date
on
 adsmart_slots.broadcast_end_date_key = broadcast_end_date.pk_datehour_dim
left outer join 
(select pk_datehour_dim, utc_datehour viewing_start_datehour_utc, broadcast_day_date viewing_start_day_date, 
broadcast_weekday viewing_start_weekday,broadcast_day_in_month viewing_start_day_in_month, 
broadcast_day_in_week viewing_start_day_in_week,broadcast_day_long viewing_start_day_long,
utc_day_date utc_viewing_start_day_date, utc_weekday utc_viewing_start_weekday, 
utc_day_in_month utc_viewing_start_day_in_month, utc_day_in_week utc_viewing_start_day_in_week,
 utc_day_long utc_viewing_start_day_long from sk_prod.viq_date) viewing_start_date
on
 adsmart_slots.viewing_start_date_key = viewing_start_date.pk_datehour_dim*/
-------------------------------------------------------------------------------------------------------------------------------------


truncate table DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT

commit

insert into DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT
(--adsmart slots
fact_viewing_slot_instance_key, 
adsmart_campaign_key, agency_key, broadcast_channel_key, broadcast_start_date_key,
broadcast_start_time_key, preceding_programme_schedule_key, 
succeeding_programme_schedule_key,segment_key, 
slot_copy_key, slot_reference_key,time_shift_key,viewed_start_date_key, 
viewed_start_time_key, 
actual_impacts, actual_impressions, actual_impressions_day_one_weighted, actual_serves,
actual_weight, sample_impressions,
viewed_duration,
--time_shift
viewing_type, timeshift_band,elapsed_days,
elapsed_hours,elapsed_hours_total,
--agency
advertiser_code, advertiser_name,  
barb_sales_house_id, buyer_code, sales_house_name, sales_house_short_name,
buyer_name,
--broadcast_channel
 channel_format,media_adsmart_status, vespa_channel_name,
 --preceding_broadcast_programme_schedule
 preceding_programme_broadcast_start_date_key,
 preceding_programme_broadcast_start_time_key,  
 preceding_programme_broadcast_end_date_key, 
 preceding_programme_broadcast_end_time_key,
 --succ_broadcast_programme_schedule
succ_programme_broadcast_start_date_key,
succ_programme_broadcast_start_time_key,  
succ_programme_broadcast_end_date_key, 
succ_programme_broadcast_end_time_key,
--segment
segment_id, segment_name, segment_status, segment_description,
--slot_copy
slot_copy_duration_seconds, slot_type, product_code, product_name,
--slot_reference
slot_reference_slot_type, slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds,
spot_position_in_break,slot_type_position, 
slot_type_total_position, 
break_position, 
adsmart_action, 
adsmart_priority, 
adsmart_status, adsmart_total_priority,
--broadcast start time
broadcast_start_utc_time, start_broadcast_time, start_spot_standard_daypart_uk,
--viewing start time
viewing_start_utc_time, viewing_start_broadcast_time, viewing_start_spot_standard_daypart_uk,
--broadcast start date
broadcast_start_datehour_utc,  broadcast_start_day_date,  
broadcast_start_weekday,broadcast_start_day_in_month, 
 broadcast_start_day_in_week, broadcast_start_day_long,
 utc_start_day_date,  
 utc_start_weekday, 
utc_start_day_in_month,  utc_start_day_in_week,
utc_start_day_long ,
 --viewing start date
 viewing_start_datehour_utc,  viewing_start_day_date, 
viewing_start_weekday, viewing_start_day_in_month, 
viewing_start_day_in_week, viewing_start_day_long,
 utc_viewing_start_day_date, utc_viewing_start_weekday, 
utc_viewing_start_day_in_month,  utc_viewing_start_day_in_week,
utc_viewing_start_day_long)
select 
--adsmart slots
adsmart_slots.fact_viewing_slot_instance_key, 
adsmart_slots.adsmart_campaign_key, adsmart_slots.agency_key, adsmart_slots.broadcast_channel_key, adsmart_slots.broadcast_start_date_key,
adsmart_slots.broadcast_start_time_key, adsmart_slots.preceding_programme_schedule_key, 
adsmart_slots.succeeding_programme_schedule_key,adsmart_slots.segment_key, 
adsmart_slots.slot_copy_key, adsmart_slots.slot_reference_key,adsmart_slots.time_shift_key,adsmart_slots.viewed_start_date_key, 
adsmart_slots.viewed_start_time_key, 
adsmart_slots.actual_impacts, adsmart_slots.actual_impressions, adsmart_slots.actual_impressions_day_one_weighted, adsmart_slots.actual_serves,
adsmart_slots.actual_weight, adsmart_slots.sample_impressions,
adsmart_slots.viewed_duration,
--time_shift
time_shift.viewing_type, time_shift.timeshift_band,time_shift.elapsed_days,
time_shift.elapsed_hours,time_shift.elapsed_hours_total,
--agency
agency.advertiser_code, agency.advertiser_name,  
agency.barb_sales_house_id, agency.buyer_code, agency.sales_house_name, agency.sales_house_short_name,
agency.buyer_name,
--broadcast_channel
 broadcast_channel.channel_format,broadcast_channel.media_adsmart_status, broadcast_channel.vespa_channel_name,
 --preceding_broadcast_programme_schedule
 preceding_broadcast_programme.preceding_programme_broadcast_start_date_key,
 preceding_broadcast_programme.preceding_programme_broadcast_start_time_key,  
 preceding_broadcast_programme.preceding_programme_broadcast_end_date_key, 
 preceding_broadcast_programme.preceding_programme_broadcast_end_time_key,
 --succ_broadcast_programme_schedule
succ_broadcast_programme.succ_programme_broadcast_start_date_key,
succ_broadcast_programme.succ_programme_broadcast_start_time_key,  
succ_broadcast_programme.succ_programme_broadcast_end_date_key, 
succ_broadcast_programme.succ_programme_broadcast_end_time_key,
--segment
segment.segment_id, segment.segment_name, segment.segment_status, segment.segment_description,
--slot_copy
slot_copy.slot_copy_duration_seconds, slot_copy.slot_type, slot_copy.product_code, slot_copy.product_name,
--slot_reference
slot_reference.slot_reference_slot_type, slot_reference.slot_sub_type, slot_reference.slot_duration_seconds,slot_reference.slot_duration_reported_Seconds,
slot_reference.spot_position_in_break,slot_reference.slot_type_position, 
slot_reference.slot_type_total_position, 
slot_reference.break_position, 
slot_reference.adsmart_action, 
slot_reference.adsmart_priority, 
slot_reference.adsmart_status, slot_reference.adsmart_total_priority,
--broadcast start time
broadcast_start_time.broadcast_start_utc_time, broadcast_start_time.start_broadcast_time, broadcast_start_time.start_spot_standard_daypart_uk,
--viewing start time
viewing_start_time.viewing_start_utc_time, viewing_start_time.viewing_start_broadcast_time, viewing_start_time.viewing_start_spot_standard_daypart_uk,
--broadcast start date
broadcast_start_date.broadcast_start_datehour_utc,  broadcast_start_date.broadcast_start_day_date,  
broadcast_start_date.broadcast_start_weekday,broadcast_start_date.broadcast_start_day_in_month, 
 broadcast_start_date.broadcast_start_day_in_week, broadcast_start_date.broadcast_start_day_long,
 broadcast_start_date.utc_start_day_date,  
 broadcast_start_date.utc_start_weekday, 
broadcast_start_date.utc_start_day_in_month,  broadcast_start_date.utc_start_day_in_week,
broadcast_start_date.utc_start_day_long ,
 --viewing start date
 viewing_start_date.viewing_start_datehour_utc,  viewing_start_date.viewing_start_day_date, 
viewing_start_date.viewing_start_weekday, viewing_start_date.viewing_start_day_in_month, 
viewing_start_date.viewing_start_day_in_week, viewing_start_date.viewing_start_day_long,
 viewing_start_date.utc_viewing_start_day_date, viewing_start_date.utc_viewing_start_weekday, 
viewing_start_date.utc_viewing_start_day_in_month,  viewing_start_date.utc_viewing_start_day_in_week,
viewing_start_date.utc_viewing_start_day_long
  from adsmart_slots adsmart_slots
left outer join
(select pk_timeshift_dim, viewing_type, timeshift_band,elapsed_days,elapsed_hours,
elapsed_hours_total from sk_prod.viq_time_shift) time_shift
on adsmart_slots.time_shift_key = time_shift.pk_timeshift_dim
left outer join
(select  advertiser_code, advertiser_name, agency_key, barb_sales_house_id, buyer_code, sales_house_name, sales_house_short_name,
buyer_name from sk_prod.DIM_AGENCY) agency
on adsmart_slots.agency_key = agency.agency_key
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key preceding_programme_broadcast_start_date_key,
broadcast_start_time_key preceding_programme_broadcast_start_time_key,  
broadcast_end_date_key preceding_programme_broadcast_end_date_key, 
broadcast_end_time_key preceding_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) preceding_broadcast_programme
on
adsmart_slots.preceding_programme_schedule_key = preceding_broadcast_programme.broadcast_programme_schedule_key
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key succ_programme_broadcast_start_date_key,
broadcast_start_time_key succ_programme_broadcast_start_time_key,  
broadcast_end_date_key succ_programme_broadcast_end_date_key, 
broadcast_end_time_key succ_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) succ_broadcast_programme
on
adsmart_slots.succeeding_programme_schedule_key = succ_broadcast_programme.broadcast_programme_schedule_key
left outer join
(select segment_key, segment_id, segment_name, 
segment_status, segment_description from sk_prod.DIM_SEGMENT) segment
on
adsmart_slots.segment_key = segment.segment_key
left outer join
(select slot_copy_key,slot_copy_duration_seconds, slot_type, 
product_code, product_name  from sk_prod.DIM_SLOT_COPY) slot_copy
on
adsmart_slots.slot_copy_key = slot_copy.slot_copy_key
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
adsmart_slots.slot_reference_key = slot_reference.slot_reference_key
left outer join
(select  pk_time_dim, utc_time_minute broadcast_start_utc_time, 
broadcast_time start_broadcast_time, spot_standard_daypart_uk start_spot_standard_daypart_uk
from sk_prod.viq_time) broadcast_start_time
on
adsmart_slots.broadcast_start_time_key = broadcast_start_time.pk_time_dim
left outer join
(select pk_time_dim, utc_time_minute viewing_start_utc_time, 
broadcast_time viewing_start_broadcast_time, spot_standard_daypart_uk  
viewing_start_spot_standard_daypart_uk
from sk_prod.viq_time) viewing_start_time
on
adsmart_slots.viewed_start_time_key = viewing_start_time.pk_time_dim
left outer join
(select pk_datehour_dim, utc_datehour broadcast_start_datehour_utc, broadcast_day_date broadcast_start_day_date, 
broadcast_weekday broadcast_start_weekday,broadcast_day_in_month broadcast_start_day_in_month, 
broadcast_day_in_week broadcast_start_day_in_week,broadcast_day_long broadcast_start_day_long,
utc_day_date utc_start_day_date, utc_weekday utc_start_weekday, 
utc_day_in_month utc_start_day_in_month, utc_day_in_week utc_start_day_in_week,
 utc_day_long utc_start_day_long from sk_prod.viq_date) broadcast_start_date
on
 adsmart_slots.broadcast_start_date_key = broadcast_start_date.pk_datehour_dim
left outer join 
(select pk_datehour_dim, utc_datehour viewing_start_datehour_utc, broadcast_day_date viewing_start_day_date, 
broadcast_weekday viewing_start_weekday,broadcast_day_in_month viewing_start_day_in_month, 
broadcast_day_in_week viewing_start_day_in_week,broadcast_day_long viewing_start_day_long,
utc_day_date utc_viewing_start_day_date, utc_weekday utc_viewing_start_weekday, 
utc_day_in_month utc_viewing_start_day_in_month, utc_day_in_week utc_viewing_start_day_in_week,
 utc_day_long utc_viewing_start_day_long from sk_prod.viq_date) viewing_start_date
on
 adsmart_slots.viewed_start_date_key = viewing_start_date.pk_datehour_dim

commit





truncate table data_quality_adsmart_campaign_data_audit

commit

select ADSMART_CAMPAIGN_KEY, AGENCY_KEY, SEGMENT_KEY, 
campaign_actual_impressions, campaign_actual_impressions_day_one_weighted,
campaign_actual_serves, campaign_days_run, campaign_percentage_campaign_run, campaign_percentage_target_achieved,
campaign_Sample_impressions, campaign_sample_serves, campaign_segment_measurement_panel,
campaign_Segment_universe_size_day_one_weighted, campaign_target_impressions, campaign_tracking_index
into #fact_adsmart_campaign 
FROM sk_prod.FACT_ADSMART_CAMPAIGN

insert into data_quality_adsmart_campaign_data_audit
(ADSMART_CAMPAIGN_KEY, AGENCY_KEY, SEGMENT_KEY, 
campaign_actual_impressions, campaign_actual_impressions_day_one_weighted,
campaign_actual_serves, campaign_days_run, campaign_percentage_campaign_run, campaign_percentage_target_achieved,
campaign_Sample_impressions, campaign_sample_serves, campaign_segment_measurement_panel,
campaign_Segment_universe_size_day_one_weighted,campaign_target_impressions,campaign_tracking_index,
adsmart_campaign_code,adsmart_campaign_active_length, adsmart_campaign_budget, 
adsmart_campaign_start_date, adsmart_campaign_end_date,adsmart_campaign_status, 
break_spacing, daily_business_pvr_cap, daily_tech_pvr_cap, 
total_business_pvr_cap, total_tech_pvr_cap,
segment_status, segment_description)
select fact.ADSMART_CAMPAIGN_KEY, fact.AGENCY_KEY, fact.SEGMENT_KEY, 
fact.campaign_actual_impressions, fact.campaign_actual_impressions_day_one_weighted,
fact.campaign_actual_serves, fact.campaign_days_run, fact.campaign_percentage_campaign_run, fact.campaign_percentage_target_achieved,
fact.campaign_Sample_impressions, fact.campaign_sample_serves, fact.campaign_segment_measurement_panel,
fact.campaign_Segment_universe_size_day_one_weighted, fact.campaign_target_impressions, fact.campaign_tracking_index,
dim_campaign.adsmart_campaign_code,dim_campaign.adsmart_campaign_active_length, dim_campaign.adsmart_campaign_budget, 
dim_campaign.adsmart_campaign_start_date, dim_campaign.adsmart_campaign_end_date,dim_campaign.adsmart_campaign_status, 
dim_campaign.break_spacing, dim_campaign.daily_business_pvr_cap, dim_campaign.daily_tech_pvr_cap, 
dim_campaign.total_business_pvr_cap, dim_campaign.total_tech_pvr_cap,
dim_segment.segment_status, dim_segment.segment_description 
into data_quality_adsmart_campaign_data_audit
from #fact_adsmart_campaign fact
left outer join
(select  adsmart_campaign_key, adsmart_campaign_code,adsmart_campaign_active_length, adsmart_campaign_budget, 
adsmart_campaign_start_date, adsmart_campaign_end_date,adsmart_campaign_status, 
break_spacing, daily_business_pvr_cap, daily_tech_pvr_cap, total_business_pvr_cap, total_tech_pvr_cap
from sk_prod.dim_adsmart_campaign) dim_campaign
on fact.adsmart_campaign_key = dim_campaign.adsmart_campaign_key
left outer join
(select segment_id, segment_key, segment_status, segment_description 
from sk_prod.dim_segment) dim_segment
on
fact.segment_key = dim_Segment.segment_key

commit

-------------------------------------------------------------------------------------------------------------------------------------------------

truncate table data_quality_adsmart_segment_data_audit

commit

select segment_key, segment_date_key, measurement_panel_billable_customer_accounts, 
measurement_panel_dth_active_viewing_cards, universe_size_billable_customer_accounts,
universe_size_dth_active_viewing_cards,
universe_size_weighted_billable_customer_accounts,
universe_size_weighted_dth_active_viewing_cards
into #fact_adsmart_segment
from 
sk_prod.FACT_ADSMART_SEGMENT


insert into data_quality_adsmart_segment_data_audit
(segment_key, segment_date_key, measurement_panel_billable_customer_accounts, 
measurement_panel_dth_active_viewing_cards, universe_size_billable_customer_accounts,
universe_size_dth_active_viewing_cards,
universe_size_weighted_billable_customer_accounts,
universe_size_weighted_dth_active_viewing_cards,
segment_status, segment_description)
select fact.segment_key, fact.segment_date_key, fact.measurement_panel_billable_customer_accounts, 
fact.measurement_panel_dth_active_viewing_cards, fact.universe_size_billable_customer_accounts,
fact.universe_size_dth_active_viewing_cards,
fact.universe_size_weighted_billable_customer_accounts,
fact.universe_size_weighted_dth_active_viewing_cards,
dim_segment.segment_status, dim_segment.segment_description
from 
#fact_adsmart_segment fact
left outer join
(select segment_id, segment_key, segment_status, segment_description 
from sk_prod.dim_segment) dim_segment
on
fact.segment_key = dim_segment.segment_key

commit
----------------------------------------------------------------------------------------------------------------------------------------------------------------

truncate table data_quality_adsmart_hh_data_audit

commit

select account_number, household_key, segment_date_key, segment_key
into #fact_household_segment_date
from sk_prod.FACT_HOUSEHOLD_SEGMENT
where segment_date_key = 

insert into data_quality_adsmart_hh_data_audit
(account_number, household_key, segment_date_key, segment_key,
segment_status, segment_description,
hh_has_adsmart_stb, no_of_adsmart_stb)
select fact.account_number, fact.household_key, fact.segment_date_key, fact.segment_key,
dim_segment.segment_status, dim_segment.segment_description,
hh.hh_has_adsmart_stb, hh.no_of_adsmart_stb 
from 
#fact_household_segment_date fact
left outer join
(select segment_id, segment_key, segment_status, segment_description 
from sk_prod.dim_segment) dim_segment
on fact.segment_key = dim_Segment.segment_key
left outer join
(select household_key, hh_has_adsmart_stb, no_of_adsmart_stb from sk_prod.viq_household) hh
on
fact.household_key = hh.household_key

commit

----------------------------------------------------------------------------------------------------------------------------------------------------------------