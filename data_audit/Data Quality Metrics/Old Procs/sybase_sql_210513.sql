--------------------------------------------------insert vespa results into repository--------------------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select vespa_base  metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||12||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit
---------------------------------D02 - Insert into sky metrics into the repository table------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select sky_base metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

--------------------------------Insert Sky Base Actual------------------------------------------------------------

---------------------------------D02 - Insert into sky metrics into the repository table------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select sky_base_actual metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'sky_actual'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

-----------------------------------------------------------------------------------------------------------------

-------------------------------D03 - Insert indexes into the repository table-------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select index_var metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'index'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_')metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit

--------------------------------Insert Vespa  Base Coverage------------------------------------------------------------

insert into data_quality_vespa_REPOSITORY
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red, metric_rag, load_timestamp)
select @CP2_build_ID,@target_date,a.dq_vm_id, b.metric_value, a.metric_tolerance_amber, a.metric_tolerance_red,
metric_benchmark_check(metric_value, metric_benchmark, metric_tolerance_amber,metric_tolerance_red) metric_rag,
getdate()
from
data_quality_vespa_metrics a,
(select coverage metric_value,
replace(replace('sca_viq_data_quality_'||lower(metric_desc)||'_'||'coverage'||'_'||replace(replace(replace(replace(replace(replace(replace(replace(coalesce(lower(segment),'null'),' ','_'),'(',''),')',''),'.',''),'&',''),',',''),'-',''),'+','')||'',' ',''),'__','_') metric_short_name
from scaling_variables_viq_dq) b
where lower(a.metric_short_name) = lower(b.metric_short_name)
and a.current_flag = 1

commit




select * from sys.syscolumns
where tname = 'data_quality_sky_base_upscale'

create table data_quality_sky_base_upscale
(sky_base_upscale_total bigint,
event_date date)

insert into data_quality_sky_base_upscale
select distinct 10100000, broadcast_day_date
from sk_prod.viq_date
where broadcast_day_date between '2012-12-01' and today() + 70

select top 10 * from data_quality_vespa_metrics

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

select * from data_quality_vespa_metrics
where metric_grouping = 'household_metrics'
and lower(metric_short_name) like  '%unk%'


-------------------------------------------------------------------------------------------------------------

select * from data_quality_vespa_metrics
where metric_grouping = 'data_integrity'

select * from data_quality_check_details
where dq_col_id in
(select dq_col_id from data_quality_columns
where upper(table_name) in 
('VIQ_VIEWING_DATA',
'VIQ_PROG_SCHED_PROPERTIES',
'VIQ_DATE',
'VIQ_TIME',
'VIQ_CHANNEL',
'VIQ_PROGRAMME',
'VIQ_PLATFORM_SERVICE',
'VIQ_HOUSEHOLD',
'VIQ_PROGRAMME_SCHEDULE',
'VIQ_TIME_SHIFT',
'VIQ_TIME_MINUTE',
'VIQ_INTERVAL_HOUR_MINUTES',
'VIQ_VIEWING_DATA_SCALING',
'VIQ_ALL_INTERVALS',
'ADVERTISER',
'BUYER',
'SALES_HOUSE',
'SLOT',
'SLOT_DATA',
'SPOT_POSITION',
'SLOT_INSTANCE'))


select * from data_quality_columns


-------------------------------------------------------------------------------4) insert the testing group that you want to use------------------------------------------------------------
---CONFIGURABLE TO WHAT TYPE OF CHECK YOU WANT TO USE IT FOR


commit

----------------------------------------------------------------------------6) INSERT THE COLUMNS YOU WANT TO CHECK (HAS BE CONFIGURED BASED ON INDIVIDUAL NEEDS-----------------------------------------------

---isnull checks insert



INSERT INTO data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp)
SELECT col.dq_col_id, sched.dq_run_id,
check1.dq_check_type_id, 0, 10,50,getdate()
from (select b.* from sys.syscolumns a,
data_quality_columns b
where a.creator = b.creator
and a.tname = b.table_name
and a.cname = b.column_name
and a.colno = 1
and b.column_name not in ('cb_source_cd','interval_key')) col,
data_quality_run_group sched,
data_quality_check_type check1
where lower(col.creator) = 'sk_uat'
and check1.dq_check_type = 'PRIMARY_KEY_CHECK'

commit


INSERT INTO data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp)
SELECT col.dq_col_id, sched.dq_run_id,
check1.dq_check_type_id, 0, 10,50,getdate()
from (select b.* from sys.syscolumns a,
data_quality_columns b
where a.creator = b.creator
and a.tname = b.table_name
and a.cname = b.column_name
and b.column_name in ('cb_row_id')) col,
data_quality_run_group sched,
data_quality_check_type check1
where lower(col.creator) = 'sk_uat'
and check1.dq_check_type = 'PRIMARY_KEY_CHECK'

commit


------------------------------------------------------default check----------------------------------------------------------------------

INSERT INTO data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp,unknown_value)
SELECT col.dq_col_id, sched.dq_run_id,
check1.dq_check_type_id, 0, 10,50,getdate(),'-1'
from(select b.* from sys.syscolumns a,
data_quality_columns b
where a.creator = b.creator
and a.tname = b.table_name
and a.cname = b.column_name
and lower(b.column_name) like '%_key'
and lower(b.column_type) != 'varchar') col,
data_quality_run_group sched,
data_quality_check_type check1
where lower(col.creator) = 'sk_uat'
and lower(col.table_name) != 'viq_viewing_data'
and lower(col.column_name) like '%_key'
and lower(col.column_type) != 'varchar'
and check1.dq_check_type = 'UNKNOWN_CHECK'

commit


----------------------------------------------------------------------------7) PROC FOR VESPA UAT BASIC CHECKS (AGAIN CONFIGURABLE TO WHAT YOU WANT TO DO)-----------------------------------------------
--COPY IN GIT REPOSITORY VESPA/DATA_AUDIT/DATA QUALITY METRICS/FINAL_PROCEDURES

----------------------------------------------------------------------------------------------------------------------------------------------

--CODE FOR BASIC CHECKING

-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Vespa_UAT_Checks
**
** The control proc which runs the basic checks routine to get execute the vespa basic checks for the
** data quality report
**
**
** Refer also to:
**
**
** Code sections:
**      Part A:
**		A01 - get information from the Vespa Basic Checks tables
**		A02 - Loop creation to execute each metric at a time
**
**      Part B:
**              B01 - call basic checks procedure for each metric affected
**
** Things done:
**
**
******************************************************************************/



if object_id('Data_Quality_UAT_Basic_Checks') is not null drop procedure Data_Quality_UAT_Basic_Checks
commit

go

create procedure Data_Quality_UAT_Basic_Checks
    @run_type varchar(40) = 'VESPA_UAT_TESTING'
    ,@target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

--declare @run_type varchar(40)
declare @creator varchar(40)
declare @column_name varchar(200)
declare @table_name varchar(200)
declare @dq_check_type varchar(200)
declare @dq_check_detail_id int
declare @dq_run_id bigint


EXECUTE logger_add_event @RunID , 3,'Data Quality Basic Checks Start for Date '||cast (@target_date as varchar(20))

------------------------------------------A01 - get information from the Vespa Basic Checks tables-----------------------------------------

COMMIT
select dq_chk_det.dq_check_detail_id,dq_col.creator, dq_col.column_name, dq_col.table_name, dq_chk_type.dq_check_type,
dq_run_grp.dq_run_id
into #data_quality_run_process
from data_quality_columns dq_col,data_quality_check_type dq_chk_type,
data_quality_check_details dq_chk_det, data_quality_run_group dq_run_grp
where dq_chk_det.dq_col_id = dq_col.dq_col_id
and dq_chk_det.dq_check_type_id = dq_chk_type.dq_check_type_id
and dq_chk_det.dq_sched_run_id = dq_run_grp.dq_run_id
--AND DQ_RUN_GRP.RUN_TYPE = 'VESPA_UAT_TESTING'
and dq_run_grp.run_type = @run_type

 -- this is the type unique index on the table you're updating

-- Copy out the unique ids of the rows you want to update to a temporary table
select dq_check_detail_id into #temp from #data_quality_run_process -- you can use a where condition here
order by 1

------------------------------------------A02 - Loop creation to execute each metric at a time-----------------------------------------

-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_check_detail_id  = dq_check_detail_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_check_detail_id = @dq_check_detail_id -- delete that uid from the temp table

  -- Do something with the uid you have

set @creator = (select creator from  #data_quality_run_process
where dq_check_detail_id = @dq_check_detail_id)

set @column_name = (select column_name from  #data_quality_run_process
where dq_check_detail_id = @dq_check_detail_id)

set @table_name = (select table_name from  #data_quality_run_process
where dq_check_detail_id = @dq_check_detail_id)

set @dq_check_type = (select dq_check_type from  #data_quality_run_process
where dq_check_detail_id = @dq_check_detail_id)

set @dq_run_id = (select dq_run_id from  #data_quality_run_process
where dq_check_detail_id = @dq_check_detail_id)


------------------------------------------B01 - call basic checks procedure for each metric affected-----------------------------------------


execute dq_basic_checks @table_name,@column_name,@creator,@dq_check_type,@dq_run_id,@target_date,@CP2_build_ID

end

EXECUTE logger_add_event @RunID , 3,'Data Quality Basic Checks End for Date '||cast (@target_date as varchar(20))


end

----------------------------------------------------------------------------8) DATA QUALITY CHECKS PROCEDURE-----------------------------------------------
--COPY IN GIT REPOSITORY VESPA/DATA_AUDIT/DATA QUALITY METRICS/FINAL_PROCEDURES

---------------------------------------------------------------------------------------------------------------------------------------------



-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Basic_Checks
**
** This is a generic procedure which has its own control systems and tables and can be run
** independently of any other process.
**
**
** Refer also to:
**
**
** Code sections:
**      Part A: A01 - Collect basic information from the vespa metrics tables
**		A02 - Assign values from temp table to each of the variables
**		A03 - Loop creation to execute each metric at a time
**
**      Part B:
**              B01 - Isnull Check
**		B02 - Metric benchmark for Isnull
**		B03 - Insert into Data Quality Results table
**
**      Part C:
**              C01 - Max Length Check
**		C02 - Metric benchmark for max length
**		C03 - Insert into Data Quality Results table
**
**      Part D:
**		D01 - Distinct Count Check
**		D02 - Metric benchmark for distinct count
**		D03 - Insert into Data Quality Results table
**
**	Part E:
**
**		E01 - Unknown Check
**		E02 - Metric benchmark for unknown check
**		E03 - Insert into Data Quality Results table
**
**	Part F:
**
**		F01 - Column Type Check
**		F02 - Metric benchmark for Column Type check
**		F03 - Insert into Data Quality Results table
**
**	Part G:
**
**		F01 - Primary Key Check
**		F02 - Metric benchmark for Primary Key check
**		F03 - Insert into Data Quality Results table
**
** Things done:
**
**
******************************************************************************/


if object_id('DQ_Basic_Checks') is not null drop procedure DQ_Basic_Checks
commit

go

create procedure DQ_Basic_Checks
    @table_name     varchar(200) --tablename you want to run the basic checks on
    ,@column_name    varchar(200)
    ,@creator      varchar(200)
    ,@check_type    varchar(200)
    ,@dq_run_id	    int
    ,@target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)

as
begin

declare @var_sql varchar(8000)
declare @count_isnull int
declare @col_type_check int
declare @max_length_chk int
declare @dist_cnt_chk int
declare @unknown_chk int
declare @COL_TYPE_CHK int
declare @load_date datetime
declare @dq_det_id int
declare @res_final varchar(200)
declare @column_type varchar(30)
declare @unknown_value varchar(20)

declare @table_row_count int
declare @column_row_count int
declare @pass_fail varchar(4)
declare @expected_value varchar(40)
declare @dq_check_detail_id bigint
declare @metric_benchmark decimal (16,3)
declare @metric_tolerance_amber decimal (16,3)
declare @metric_tolerance_red decimal (16,3)
declare @metric_rag varchar(5)
declare @pk_result int
set @load_date = getdate()

-------------------------------A01 - Collect basic information from the vespa metrics tables---------------------------------

select dq_chk_det.unknown_value unknown_value, dq_chk_det.dq_check_detail_id dq_det_id,
dq_chk_det.expected_value expected_value,
dq_col.column_type column_type, dq_run_type.dq_run_id dq_run_id,
dq_chk_det.metric_benchmark metric_benchmark,
dq_chk_det.metric_tolerance_amber metric_tolerance_amber,
dq_chk_det.metric_tolerance_red metric_tolerance_red
into #tmp_dq_values
from
data_quality_check_type dq_chk_type
,data_quality_columns dq_col
,data_quality_check_details dq_chk_det
,data_quality_run_group dq_run_type
where dq_chk_det.dq_col_Id = dq_col.dq_col_Id
and dq_chk_det.dq_check_type_id = dq_chk_type.dq_check_type_id
and dq_chk_det.dq_sched_run_id = dq_run_type.dq_run_id
and UPPER(dq_chk_type.dq_check_type) = UPPER(@check_type)
and UPPER(dq_col.creator) = UPPER(@creator)
and UPPER(dq_col.table_name) = UPPER(@table_name)
and UPPER(dq_col.column_name) = UPPER(@column_name)
and DQ_RUN_TYPE.dq_run_id = @dq_run_id

-------------------------------A02 - Assign values from temp table to each of the variables---------------------------------


set @dq_det_id = (select dq_det_id from #tmp_dq_values)
set @unknown_value = (select unknown_value from #tmp_dq_values)
set @column_type = (select column_type from #tmp_dq_values)
set @expected_value = (select expected_value from #tmp_dq_values)
set @metric_benchmark = (select metric_benchmark from #tmp_dq_values)
set @metric_tolerance_amber = (select metric_tolerance_amber from #tmp_dq_values)
set @metric_tolerance_red = (select metric_tolerance_red from #tmp_dq_values)


-------------------------------B01 - Isnull Check---------------------------------


if @check_type = 'ISNULL_CHECK'
begin
set @var_sql = 'select count(1) INTO @count_isnull from '||@creator||'.'||@table_name||' where '||@column_name||' is null'

execute (@var_sql)
commit

-------------------------------B02 - Metric benchmark for Isnull---------------------------------


set @metric_rag = metric_benchmark_check(@count_isnull, @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ ISNULL check for '||@table_name||'.'||@column_name||''


-------------------------------B03 - Insert into Data Quality Results table---------------------------------



insert into data_quality_results
(dq_check_detail_id, dq_run_id, result, rag_status, sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id, @count_isnull,@metric_rag, @var_sql,@load_date, @CP2_build_ID,@target_date)
commit

end
-----------------------------------------------------------------------------------------------------------------


-------------------------------C01 - Max Length Check---------------------------------


if @check_type = 'MAX_LENGTH_CHECK'
begin

if UPPER(@column_type) LIKE '%CHAR%'
begin
set @var_sql = 'select max('||@column_name||') INTO @max_length_chk from '||@creator||'.'||@table_name||''

execute (@var_sql)
commit

end

if isnull(@var_sql,'<NULL>') = '<NULL>'
begin
set @var_sql = 'column type not appropriate for processing'

end

----------------------------------------------------C02 - Metric benchmark for max length--------------------------------------------


set @metric_rag = metric_benchmark_check(@max_length_chk, @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Max Length Chk for '||@table_name||'.'||@column_name||''

--------------------------------------------------C03 - Insert into Data Quality Results table---------------------------------


insert into data_quality_results
(dq_check_detail_id, dq_run_id, result, rag_status, sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id, @max_length_chk,@metric_rag, @var_sql,@load_date, @CP2_build_ID,@target_date)
commit

end

------------------------------------------------------------------------------------------------------------------------------

-------------------------------D01 - Distinct Count Check-------------------------------------------------------------



if @check_type = 'DISTINCT_COUNT_CHECK'
begin
set @var_sql = 'select count (distinct '||@column_name||') INTO @dist_cnt_chk from '||@creator||'.'||@table_name||''

execute (@var_sql)
commit

------------------------------------D02 - Metric benchmark for distinct count------------------------------------------

set @metric_rag = metric_benchmark_check(@dist_cnt_chk , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Distinct Cnt for '||@table_name||'.'||@column_name||''


--------------------------------------------------D03 - Insert into Data Quality Results table---------------------------------


insert into data_quality_results
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@dist_cnt_chk,@var_sql,@metric_rag,@load_date, @CP2_build_ID, @target_date)
commit
end

-----------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------E01 - Unknown Check----------------------------------------------------

if @check_type = 'UNKNOWN_CHECK'
begin

IF isnumeric(@unknown_value) = 1
begin
set @var_sql = 'select count (1) INTO @unknown_chk from '||@creator||'.'||@table_name||' WHERE '||@column_name||' = convert(int, ('||@unknown_value||'))'

commit

end

IF isnumeric(@unknown_value) = 0
begin
set @var_sql = 'select count (1) INTO @unknown_chk from '||@creator||'.'||@table_name||' WHERE UPPER('||@column_name||') = UPPER('||@unknown_value||')'

commit

end

execute (@var_sql)
commit

--------------------------------------------E02 - Metric benchmark for unknown check----------------------------------------


set @metric_rag = metric_benchmark_check(@unknown_chk , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Unknown Chk for '||@table_name||'.'||@column_name||''


--------------------------------------------------E03 - Insert into Data Quality Results table---------------------------------


insert into data_quality_results
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@unknown_chk,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit

end

------------------------------------------------------------------------------------------------------------------------------------------------------


-------------------------------------------------F01 - Column Type Check------------------------------------------------------


if @check_type = 'COLUMN_TYPE_LENGTH_CHECK'

begin
set @var_sql = 'select count (*) INTO @COL_TYPE_CHK from
(select coltype, length from sys.syscolumns where upper(creator) = '''||@creator||'''
AND UPPER(TNAME) = '''||@table_name||''' AND UPPER(CNAME) = '''||@column_name||'''
 UNION  SELECT COLUMN_TYPE, COLUMN_LENGTH FROM DATA_QUALITY_COLUMNS WHERE UPPER(CREATOR) = '''||@creator||'''
 AND UPPER(TABLE_NAME) = '''||@table_name||''' AND UPPER(COLUMN_NAME) = '''||@column_name||''') T'

execute (@var_sql)
commit

--------------------------------------------E02 - Metric benchmark for column type check----------------------------------------


set @metric_rag = metric_benchmark_check(@COL_TYPE_CHK  , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Col Type Chk for '||@table_name||'.'||@column_name||''


--------------------------------------------------F03 - Insert into Data Quality Results table---------------------------------


insert into data_quality_results
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@COL_TYPE_CHK,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit
end

------------------------------------------------------------------------------------------------------------------------------------------------------

if @check_type = 'PRIMARY_KEY_CHECK'

-------------------------------------------------G01 - Primary Key Check------------------------------------------------------


begin
set @var_sql = 'select count (1) INTO @table_row_count from '||@creator||'.'||@table_name||''

execute (@var_sql)

commit

set @var_sql = 'select count (distinct '||@column_name||') INTO @column_row_count from '||@creator||'.'||@table_name||''

execute (@var_sql)

commit

set @pk_result = @table_row_count - @column_row_count

--------------------------------------------G02 - Metric benchmark for primary Key check----------------------------------------


set @metric_rag = metric_benchmark_check(@pk_result , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ PK Chk for '||@table_name||'.'||@column_name||''


--------------------------------------------------G03 - Insert into Data Quality Results table---------------------------------


insert into data_quality_results
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@pk_result,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit

end

/*

if @check_type = 'EXPERIAN_PROPENSITY'
begin


--using experian household key as we want to check that, by their data, it is doing what it should

SET @VAR_SQL = 'SELECT exp_cb_key_household,'||@column_name||' into #tst_cash_percentile FROM sk_prod.PERSON_PROPENSITIES_GRID_NEW pp JOIN sk_prod.EXPERIAN_CONSUMERVIEW cv  ON pp.ppixel2011 = cv.p_pixel_v2 and pp.mosaic_uk_2009_type = cv.Pc_mosaic_uk_type'

EXECUTE (@VAR_SQL)

SET @VAR_SQL = ''

SET @VAR_SQL = 'SELECT '||@column_name||',(cast (count(1) as float) / cast((select count(1) from #tst_cash_percentile) as float)) * 100 percentage into #tst_csh_percentile_chk
from
#tst_cash_percentile
group by have_a_cash_isa_percentile

select case when sum(case when percentage > 1.5 then 1 else 0 end) > 0 then 1 else 0 end chk
from #tst_csh_percentile_chk
*/

end

----------------------------------------------------------------------------9) METRICS BENCHMARK FUNCTION (CALCULATES THE DIFFERENCES BETWEEN RESULT AND BENCHMARK)-----------------------------------------------
--COPY IN GIT REPOSITORY VESPA/DATA_AUDIT/DATA QUALITY METRICS/FINAL_PROCEDURES



-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Metric Benchmark Function
**
** This script has the function which we use to compare result to the metric amber and metric red
** benchmark values so we can assign a red, amber or green status.
**
** Refer also to:
**
**
**      Part A: Function details
**		A01 - Housekeeping
**		A02 - Mathematical function to gain results
**		A03 - Logic to decide what the RAG status will be
**		A04 - return result
**
**
** Things done:
**
**
******************************************************************************/


if object_id('metric_benchmark_check') is not null drop function metric_benchmark_check
commit

go

CREATE FUNCTION metric_benchmark_check
(metric_result decimal
,metric_benchmark decimal
,metric_tolerance_amber decimal
,metric_tolerance_red decimal )
returns varchar(8)
--returns decimal (6,3)
as
begin

declare @benchmark_result varchar(8)
declare @result_benchmark decimal (16,3)
declare @metric_benchmark_divide decimal (16,3)


-----------------------------------------------------------A01 - Housekeeping-----------------------------------------------------

if metric_benchmark = 0
begin
set @metric_benchmark_divide = 1
end

if metric_benchmark > 0
begin
set @metric_benchmark_divide = metric_benchmark
end

-----------------------------------------------------------A02 - Mathematical function to gain results--------------------------------------

set @result_benchmark = abs(1.0 * (metric_result - metric_benchmark) / @metric_benchmark_divide) * 100

-----------------------------------------------------------A03 - Logic to decide what the RAG status will be--------------------------------------


    if (@result_benchmark <= metric_tolerance_amber)
        begin
            set @benchmark_result = 'GREEN'
        end

    if (@result_benchmark between (metric_tolerance_amber + 0.001) and (metric_tolerance_red - 0.001))
        begin
            set @benchmark_result = 'AMBER'
        end

    if (@result_benchmark >= metric_tolerance_red)
        begin
            set @benchmark_result = 'RED'
        end

-----------------------------------------------------------A04 - return result--------------------------------------


return @benchmark_result
--return @result_benchmark
end

--------------------------------------------------------------------------------------------

------------------------------------------------------------------------------10) script to run the procedure (using vespa_uat_testing as the default here)-------

create variable @RunID bigint
exec logger_create_run 'Data_Quality_UAT_BASIC_Checks', 'Latest Run', @RunID output
exec Data_Quality_UAT_Basic_Checks 'VESPA_UAT_TESTING',today(),@RunID

---------------------------------------------------------------------------------------------


------------------------------------------------------------------------------11) Gives results of most recent run for each metric-------

----select statement to view the most recent results

select b.creator, b.table_name, b.column_name, c.dq_check_type,
d.result metric_result, d.rag_status, a.metric_benchmark, a.metric_tolerance_amber, a.metric_tolerance_red
from data_quality_check_details a,
data_quality_check_type c,
data_quality_columns b,
(select a.* from data_quality_results a,
(select dq_check_detail_id, max(dq_res_id) dq_res_id from data_quality_results
where dq_run_id = (select dq_run_id from data_quality_RUN_GROUP where RUN_TYPE = 'VESPA_UAT_TESTING')
group by dq_check_detail_id) b
where a.dq_res_id = b.dq_RES_ID) d
where c.dq_check_type_id = a.dq_check_type_id
and a.dq_col_id = b.dq_col_id
and a.dq_check_detail_id = d.dq_check_detail_id
and a.dq_sched_run_id = (select dq_run_id from data_quality_RUN_GROUP where RUN_TYPE = 'VESPA_UAT_TESTING')
order by 1,2,3, 4


------------------------------------------------------------------------------------------------------------------------------------------------


select top 10 * from sk_prod.slot_timetable

select top 10 a.*, date(load_timestamp) from data_quality_vespa_metrics a
order by 1 desc

select today()

select 

INSERT INTO data_quality_vespa_metrics
(METRIC_SHORT_NAME, METRIC_DESCRIPTION,METRIC_BENCHMARK,METRIC_TOLERANCE_AMBER, METRIC_TOLERANCE_RED, LOAD_TIMESTAMP)
(select LOWER('VDQ'||'_'||c.table_name||'_'||c.column_name||'_'||dq_check_type||'')metric_short_name,
LOWER('VDQ'||'_'||c.table_name||'_'||c.column_name||'_'||dq_check_type||'') metric_description
, METRIC_BENCHMARK, METRIC_TOLERANCE_AMBER, METRIC_TOLERANCE_RED,GETDATE()
from data_quality_check_details a,
(select dq_check_type_id, dq_check_type from data_quality_check_type) b,
(select dq_col_id, column_name, table_name, creator from data_quality_columns) c,
data_quality_run_group d
where a.dq_check_type_id = b.dq_check_type_id
and a.dq_col_id = c.dq_col_id
and a.dq_sched_run_id = d.dq_run_id
and not exists 
(select 1 from data_quality_vespa_metrics e
where LOWER('VDQ'||'_'||c.table_name||'_'||c.column_name||'_'||dq_check_type||'') = e.metric_short_name))

COMMIT

SELECT TOP 10 * FROM data_quality_check_details


select * from data_quality_check_details
order by 1 desc

where date(load_timestamp) = today()



select * into data_quality_check_details_cp
from data_quality_check_details


declare @dq_check_detail_id int
declare @metric_short_name varchar(200)


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
and a.dq_sched_run_id = d.dq_run_id
and a.metric_short_name is null)t

SELECT dq_check_detail_id into #temp FROM #tmp_ins
--------------------------------------------A02 - Define the loop which will be cycled through to execute the Analytical procedure-------------------------
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_check_detail_id = dq_check_detail_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_check_detail_id = @dq_check_detail_id -- delete that uid from the temp table

set @metric_short_name = (select metric_short_name from #tmp_ins where dq_check_detail_id = @dq_check_detail_id)

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

update data_quality_check_details a
set metric_short_name = @metric_short_name
where dq_check_detail_id = @dq_check_detail_id

commit

end

update data_quality_vespa_metrics
set metric_grouping = 'data_integrity'
where date(load_timestamp) = today()
and metric_grouping is null

commit

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

declare @dq_vm_id int
declare @metric_short_name_new varchar(200)

select * into  #tmp_ins
from
(select a.*, 't'||substr(metric_short_name,2)metric_short_name_new from data_quality_vespa_metrics a
where date(load_timestamp) = today())t

SELECT dq_vm_id into #temp FROM #tmp_ins
--------------------------------------------A02 - Define the loop which will be cycled through to execute the Analytical procedure-------------------------
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_vm_id = dq_vm_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_vm_id = @dq_vm_id -- delete that uid from the temp table

set @metric_short_name_new = (select metric_short_name_new from #tmp_ins where dq_vm_id = @dq_vm_id)

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

update data_quality_vespa_metrics a
set metric_short_name = @metric_short_name_new
where dq_vm_id = @dq_vm_id

commit

end

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

declare @dq_check_detail_id int
declare @metric_short_name_new varchar(200)

select * into  #tmp_ins
from
(select a.*, 't'||substr(metric_short_name,2)metric_short_name_new
from data_quality_check_details a
where date(modified_date) = today())t

SELECT dq_check_detail_id into #temp FROM #tmp_ins
--------------------------------------------A02 - Define the loop which will be cycled through to execute the Analytical procedure-------------------------
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_check_detail_id = dq_check_detail_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_check_detail_id = @dq_check_detail_id -- delete that uid from the temp table

set @metric_short_name_new = (select metric_short_name_new from #tmp_ins where dq_check_detail_id = @dq_check_detail_id)

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

update data_quality_check_details a
set metric_short_name = @metric_short_name_new
where dq_check_detail_id = @dq_check_detail_id

commit

end


-- Copy out the unique ids of the rows you want to update to a temporary table
select dq_check_detail_id into #temp from #data_quality_run_process -- you can use a where condition here


select * from data_quality_check_details

select * from data_quality_run_group

update data_quality_check_details
set metric_short_name = 'vdq_data_quality_dp_data_audit_account_number_distinct_count_check'
where metric_short_name = 'tdq_data_quality_dp_data_audit_account_number_distinct_count_check'

update data_quality_check_details
set metric_short_name = 'vdq_data_quality_dp_data_audit_dk_channel_dim_distinct_count_check'
where metric_short_name = 'tdq_data_quality_dp_data_audit_dk_channel_dim_distinct_count_check'

update data_quality_check_details
set metric_short_name = 'vdq_data_quality_dp_data_audit_dk_programme_dim_distinct_count_check'
where metric_short_name = 'tdq_data_quality_dp_data_audit_dk_programme_dim_distinct_count_check'

commit



select * from data_quality_vespa_metrics
where lower(metric_short_name) like 'tdq%'
and metric_short_name not in
(select metric_short_name from data_quality_check_details
where lower(metric_short_name) like 'tdq%')



-----------------------------------------------------------------------------------------------------------------------------------------

select dq_vm_id, metric_short_name 
into #tmp_vesp_metric
from data_quality_vespa_metrics
where UPPER(metric_short_name) like 'TDQ%'

------------------------------------------A02 - get information from the Vespa Basic Checks tables-----------------------------------------


select dq_chk_det.dq_check_detail_id,dq_col.creator, dq_col.column_name, dq_col.table_name, dq_chk_type.dq_check_type,
dq_run_grp.dq_run_id 
into #data_quality_run_process
from data_quality_columns dq_col,data_quality_check_type dq_chk_type,
data_quality_check_details dq_chk_det, data_quality_run_group dq_run_grp,
#tmp_vesp_metric vesp_metric
where dq_chk_det.dq_col_id = dq_col.dq_col_id
and dq_chk_det.dq_check_type_id = dq_chk_type.dq_check_type_id
and dq_chk_det.dq_sched_run_id = dq_run_grp.dq_run_id
and dq_run_grp.run_type = 'VESPA_UAT_TESTING'
and upper(vesp_metric.metric_short_name) = upper(dq_chk_det.metric_short_name)
 -- this is the type unique index on the table you're updating

SELECT * FROM #data_quality_run_process



--------------------------------------------------------------------------------------------------------------------------------------------

create variable @RunID bigint;
execute logger_create_run 'Data_Quality_Metrics_Collection', 'Latest Run', @RunID output;
execute data_quality_data_processing_date null,@RunID,'2013-05-16';



create procedure data_quality_data_processing_date
@run_type varchar(20)
,@CP2_build_ID     bigint = NULL
,@build_date date


9380168

 select dq_vm_id, metric_short_name
into #tmp_dqvm
 from data_quality_vespa_metrics
 where upper(metric_short_name) not like 'VDQ%'
and upper(metric_short_name) not like 'SCA%'
and upper(metric_short_name) not like 'TDQ%'
and upper(metric_short_name) not like '%STB%'
and current_flag = 1


select * from #tmp_dqvm




-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Basic_Checks
**
** This is the control procedure to execute the Vespa Analytical Metrics procedure where we present
** metrics names to the procedure and it locates the relevant code and returns the results
**
**  
** Refer also to:
**
**
** Code sections:
**      Part A: 
**		A01 - Select the metrics that you want to look through the Analytical Procedure for
**		A02 - Define the loop which will be cycled through to execute the Analytical procedure
**		A03 - Execute Analytical Metric Procedure for the metric short name within the loop
**
** Things done:
**
**
******************************************************************************/



if object_id('Data_Quality_Vespa_Metric_Run') is not null drop procedure Data_Quality_Vespa_Metric_Run
commit

go

create procedure Data_Quality_Vespa_Metric_Run
     @load_date       date = NULL     -- Date of events being analyzed
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue))

as
begin


EXECUTE logger_add_event @RunID , 3,'Analytical Metrics Process Start for Date : '||cast (@analysis_date_current as varchar(20))

declare @metric_short_name varchar(200)
declare @dq_vm_id bigint


--------------------------------------------A01 - Select the metrics that you want to look through the Analytical Procedure for-------------------------


 select dq_vm_id, metric_short_name
into #tmp_dqvm
 from data_quality_vespa_metrics
 where upper(metric_short_name) not like 'VDQ%'
and upper(metric_short_name) not like 'SCA%'
and upper(metric_short_name) not like 'TDQ%'
and upper(metric_short_name) not like '%STB%'
and current_flag = 1


 -- this is the type unique index on the table you're updating

-- Copy out the unique ids of the rows you want to update to a temporary table
SELECT dq_vm_id into #temp FROM #tmp_dqvm

--------------------------------------------A02 - Define the loop which will be cycled through to execute the Analytical procedure-------------------------


-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_vm_id  = dq_vm_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_vm_id = @dq_vm_id  -- delete that uid from the temp table

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

set @metric_short_name = (select metric_short_name from #tmp_dqvm
where dq_vm_id = @dq_vm_id)

EXECUTE logger_add_event @RunID , 3,'Metrics Process Start for '||@metric_short_name||''

--------------------------------------------A03 - Execute Analytical Metric Procedure for the metric short name within the loop-------------------------


execute Data_Quality_Metrics_Collection @load_date,@CP2_build_ID,@metric_short_name

EXECUTE logger_add_event @RunID , 3,'Metrics Process End for '||@metric_short_name||''

end

EXECUTE logger_add_event @RunID , 3,'Analytical Metrics Process End for Date : '||cast (@analysis_date_current as varchar(20))

end


begin
declare @data_count int
declare @analysis_date_current date
declare @CP2_build_ID    bigint

set @analysis_date_current = '2013-05-16'
set @CP2_build_ID = 81


execute Data_Quality_Vespa_Metric_Run @analysis_date_current, @CP2_build_ID     
---now move onto the next day

set @data_count = null

set @data_count = (select count(1) from data_quality_vespa_repository where dq_run_id = @CP2_build_ID 
and viewing_data_date = @analysis_date_current)

EXECUTE logger_add_event @RunID , 3,'Count of metrics collected so far for '||cast (@analysis_date_current as varchar(20)),@data_count

set @analysis_date_current = @analysis_date_current + 1

EXECUTE logger_add_event @RunID , 3,'Data Quality Process End for Date '||cast (@analysis_date_current as varchar(20))

EXECUTE logger_add_event @RunID , 3,'Data Quality Process End', @CP2_build_ID

end

SELECT * FROM DATA_QUALITY_VESPA_REPOSITORY
ORDER BY 1 DESC

select * into data_quality_vespa_metrics_cp from data_quality_vespa_metrics

select * from data_quality_vespa_metrics_cp

commit


----------------------------------------------------------------------------------------------------------------------------------------------

commit

select * from z_logger_events
where run_id = 81
order by 1 desc

commit

commit

select dq_vm_id from data_quality_vespa_repository
where dq_run_id = 81
group by dq_vm_id
having count(*) > 1


order by 1 desc

commit

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

declare @dq_vr_id int
declare @metric_res decimal (16,3)

select a.dq_vr_id, a.metric_tolerance_amber, a.metric_tolerance_red, a.metric_rag, a.load_timestamp,a.modified_date,a.metric_result,
c.dq_col_id, d.table_name, d.column_name, e.dq_check_type,b.metric_short_name
into #tmp_metrics
from data_quality_vespa_repository_reporting a,
data_quality_vespa_metrics b,
data_quality_check_details c,
data_quality_columns d,
data_quality_check_type e
where a.dq_vm_id = b.dq_vm_id
and b.metric_short_name = c.metric_short_name
and c.dq_col_id = d.dq_col_id
and c.dq_check_type_id = e.dq_check_type_id
and a.dq_run_id = 81
and b.current_flag = 1
and lower(b.metric_short_name) like 'tdq%'
create table #tmp_metrics_final
(dq_vr_id int,
table_name varchar(100),
column_name varchar(100),
default_value int,
isnull_check int,
primary_key_check int,
metric_short_name varchar(200))

insert into #tmp_metrics_final
(dq_vr_id, table_name, column_name, metric_short_name)
select dq_vr_id,table_name, column_name, metric_short_name from 
#tmp_metrics

SELECT dq_vr_id into #temp FROM #tmp_metrics
where dq_check_type = 'UNKNOWN_CHECK'
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_vr_id  = dq_vr_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_vr_id = @dq_vr_id  -- delete that uid from the temp table

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

set @metric_res = (select metric_result from #tmp_metrics
where dq_vr_id = @dq_vr_id)

update #tmp_metrics_final
set default_value = @metric_res
where dq_vr_id = @dq_vr_id

end

DROP TABLE #temp

SELECT dq_vr_id into #temp FROM #tmp_metrics
where dq_check_type = 'PRIMARY_KEY_CHECK'
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_vr_id  = dq_vr_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_vr_id = @dq_vr_id  -- delete that uid from the temp table

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

set @metric_res = (select metric_result from #tmp_metrics
where dq_vr_id = @dq_vr_id)

update #tmp_metrics_final
set PRIMARY_KEY_CHECK = @metric_res
where dq_vr_id = @dq_vr_id

end

DROP TABLE #temp

SELECT dq_vr_id into #temp FROM #tmp_metrics
where dq_check_type = 'ISNULL_CHECK'
-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_vr_id  = dq_vr_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_vr_id = @dq_vr_id  -- delete that uid from the temp table

  -- Do something with the uid you have
--  update customers set name = 'Joe Shmoe' where uid = @uid

set @metric_res = (select metric_result from #tmp_metrics
where dq_vr_id = @dq_vr_id)

update #tmp_metrics_final
set ISNULL_CHECK = @metric_res
where dq_vr_id = @dq_vr_id

end

select * from #tmp_metrics_final


select distinct 'grant select on '||creator||'.'||tname||' to bednaszs ' from 
sys.syscolumns
where creator = 'kinnairt'
and lower(tname) like 'data_qual%'

select * from data_quality_results
where logger_id = 81
order by 1 desc

commit



'grant select on ' ||  u.user_name || '.' || tab.table_name || ' to bednaszs '
grant select on kinnairt.DATA_QUALITY_SLOT_DATA_AUDIT to bednaszs 
grant select on kinnairt.data_quality_dp_data_audit to bednaszs 
grant select on kinnairt.data_quality_check_type to bednaszs 
grant select on kinnairt.data_quality_columns to bednaszs 
grant select on kinnairt.data_quality_sky_base_upscale to bednaszs 
grant select on kinnairt.data_quality_check_details_cp to bednaszs 
grant select on kinnairt.data_quality_run_group to bednaszs 
grant select on kinnairt.data_quality_check_details to bednaszs 
grant select on kinnairt.data_quality_vespa_repository_reporting to bednaszs 
grant select on kinnairt.data_quality_results to bednaszs 
grant select on kinnairt.data_quality_vespa_metrics to bednaszs 
grant select on kinnairt.data_quality_vespa_repository to bednaszs 
grant select on kinnairt.data_quality_dp_data_to_analyze to bednaszs 


--------------------------------------------------------------------------------------------------------------------------------------------