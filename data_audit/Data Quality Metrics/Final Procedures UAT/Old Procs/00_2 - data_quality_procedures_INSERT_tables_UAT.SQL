TRUNCATE TABLE data_quality_check_type


insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('ISNULL_CHECK',getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('MAX_LENGTH_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('DISTINCT_COUNT_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('UNKNOWN_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('COLUMN_TYPE_LENGTH_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('PRIMARY_KEY_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('EXPERIAN_PROPENSITY', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('TABLE_COUNT_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('MIN_VALUE_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('MAX_VALUE_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('MATCH_RATES_SKYBASE_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('TABLE_COLUMNS_COUNT_CHECK', getdate());

insert into data_quality_check_type
( dq_check_type, load_timestamp)
values
('MATCH_RATES_VESPA_CHECK', getdate());

commit;

----------------------------------------------------------------------------------------------------------

----columns insert

TRUNCATE TABLE data_quality_columns;

insert into data_quality_columns
(creator,
table_name,
column_name,
column_type,
column_length,
load_timestamp)
select creator, tname, cname, coltype, length, getdate()
from sys.syscolumns
where lower(creator) = 'sk_uat'
and upper(tname) in
('FACT_ADSMART_SLOT_INSTANCE',
'FACT_HOUSEHOLD_SEGMENT',
'FACT_ADSMART_SEGMENT',
'FACT_ADSMART_CAMPAIGN',
'DIM_SEGMENT',
'DIM_ADSMART_CAMPAIGN',
'DIM_LINEAR_CAMPAIGN',
'DIM_AGENCY',
'DIM_SLOT_COPY',
'DIM_SLOT_REFERENCE',
'DIM_BROADCAST_CHANNEL',
'DIM_BROADCAST_PROGRAMME_SCHEDULE',
'VIQ_VIEWING_DATA_SCALING',
'VIQ_TIME',
'VIQ_DATE');

commit;

insert into data_quality_columns
(creator,
table_name,
column_name,
column_type,
column_length,
load_timestamp)
select distinct creator, tname, 1, 'int', 4, getdate()
from sys.syscolumns
where lower(creator) = 'sk_uat'
and upper(tname) in
('FACT_ADSMART_SLOT_INSTANCE',
'FACT_HOUSEHOLD_SEGMENT',
'FACT_ADSMART_SEGMENT',
'FACT_ADSMART_CAMPAIGN',
'DIM_SEGMENT',
'DIM_ADSMART_CAMPAIGN',
'DIM_LINEAR_CAMPAIGN',
'DIM_AGENCY',
'DIM_SLOT_COPY',
'DIM_SLOT_REFERENCE',
'DIM_BROADCAST_CHANNEL',
'DIM_BROADCAST_PROGRAMME_SCHEDULE',
'VIQ_VIEWING_DATA_SCALING',
'VIQ_TIME',
'VIQ_DATE');

commit;

-------------------------------------------------------------------------------------------------------------------------------------------------

TRUNCATE TABLE data_quality_run_group;

insert into data_quality_run_group
(run_type, load_timestamp)
values
('ADSMART_BASIC_CHECKS',today());

commit;

------------------------------------------------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------primary key--------------------------------------------------------


TRUNCATE TABLE data_quality_check_details;

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where (upper(substring(a.column_name,1,2)) = 'PK'
OR lower(a.column_name) = 'fact_viewing_slot_instance_key'
or (lower(a.column_name) = 'adsmart_campaign_key' and CHARINDEX('ADSMART_CAMPAIGN', a.table_name) > 0)
or (lower(a.column_name) = 'agency_key' and CHARINDEX('AGENCY', a.table_name) > 0)
or (lower(a.column_name) = 'broadcast_channel_key' and CHARINDEX('BROADCAST_CHANNEL', a.table_name) > 0)
or (lower(a.column_name) = 'broadcast_programme_schedule_key' and CHARINDEX('BROADCAST_PROGRAMME_SCHEDULE', a.table_name) > 0)
or (lower(a.column_name) = 'linear_campaign_key' and CHARINDEX('LINEAR_CAMPAIGN', a.table_name) > 0)
or (lower(a.column_name) = 'segment_key' and CHARINDEX('DIM_SEGMENT', a.table_name) > 0)
or (lower(a.column_name) = 'slot_copy_key' and CHARINDEX('SLOT_COPY', a.table_name) > 0)
or (lower(a.column_name) = 'slot_reference_key' and CHARINDEX('SLOT_REFERENCE', a.table_name) > 0)
or (lower(a.column_name) = 'adsmart_segment_key' and CHARINDEX('ADSMART_SEGMENT', a.table_name) > 0)
or (lower(a.column_name) = 'household_key' and CHARINDEX('HOUSEHOLD', a.table_name) > 0)
)
AND UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'PRIMARY_KEY_CHECK';


COMMIT;

-------------------------------------------------------------------------------UNKNOWN------------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
unknown_value,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,'-1',getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where CHARINDEX('_key',a.column_name) > 0
AND UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'UNKNOWN_CHECK';


COMMIT;


---------------------------------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------ISNULL--------------------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'ISNULL_CHECK';


COMMIT;

-------------------------------------------------------------------------------UNKNOWN 99------------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
unknown_value,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,'-99',getdate(), 'ADSMART_-99'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where CHARINDEX('_key',a.column_name) > 0
AND UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'UNKNOWN_CHECK';


COMMIT;

---------------------------------------------------------------------

---------------------------------------------------------------------------COLUMN_TYPE_LENGTH_CHECK-------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'COLUMN_TYPE_LENGTH_CHECK';

COMMIT;

--------------------------------------------------------------TABLE_COUNT_CHECK-----------------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where a.column_name = '1'
and UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'TABLE_COUNT_CHECK';

COMMIT;


---------------------------------------------------------------MIN_VALUE_CHECK----------------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'MIN_VALUE_CHECK';

COMMIT;


-------------------------------------------------------------------------MAX VALUE CHECK----------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'MAX_VALUE_CHECK';

COMMIT;


---------------------------------------------------------------------TABLE COLUMNS COUNT CHECK--------------------------------------------

--------------------------------------------------------------TABLE_COUNT_CHECK-----------------------------------------------------------------

insert into data_quality_check_details
(dq_col_id,
dq_sched_run_id,
dq_check_type_Id,
metric_benchmark,
metric_tolerance_amber,
metric_tolerance_red,
load_timestamp,
metric_short_name)
select a.dq_col_id, b.dq_run_id, c.dq_check_type_id, 0,10,50,getdate(), 'ADSMART_'||a.table_name||'_'||a.column_name||'_'||c.dq_check_type
from data_quality_columns a,
data_quality_run_group b,
data_quality_check_type c
where a.column_name = '1'
and UPPER(B.RUN_TYPE) = 'ADSMART_BASIC_CHECKS'
AND UPPER(C.DQ_CHECK_TYPE) = 'TABLE_COLUMNS_COUNT_CHECK';


COMMIT;


-----------------------------------------------------------------------------------------------------------------------------------------------


