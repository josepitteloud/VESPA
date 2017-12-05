
-----------------------------------------------1) drop tables-------------------------------------------------------------

drop table data_quality_check_details
drop table data_quality_check_type
drop table data_quality_columns
drop table data_quality_run_group
drop table data_quality_results

-----------------------------------------------2) create tables ----------------------------------------------------------


create table data_quality_results
(dq_res_id bigint identity,
dq_check_detail_id bigint,
dq_run_id bigint,
result bigint,
RAG_STATUS varchar (5),
sql_processed varchar (8000),
date_period date,
data_total bigint,
logger_id bigint,
data_date date,
load_timestamp timestamp,
modified_date timestamp default timestamp)

create table data_quality_check_details
(dq_check_detail_id	bigint	identity,
dq_col_id	bigint	,
dq_sched_run_id	bigint	,
dq_check_type_Id	bigint	,
expected_value	varchar	(20),
metric_benchmark	decimal	(16,3),
metric_tolerance_amber	decimal	(6,3),
metric_tolerance_red	decimal	(6,3),
unknown_value	varchar	(20),
load_timestamp	timestamp	,
modified_date	timestamp	 default timestamp,
metric_short_name	varchar	(200))

create table data_quality_check_type
(dq_check_type_Id	bigint	identity,
dq_check_type	varchar	(200),
load_timestamp	timestamp	,
modified_date	timestamp	default timestamp)

create table data_quality_columns
(dq_col_id	bigint	identity,
creator	varchar	(30),
table_name	varchar	(200),
column_name	varchar	(200),
column_type	varchar	(50),
column_length	integer	,
load_timestamp	timestamp	,
modified_date	timestamp	 default timestamp)

create table data_quality_run_group
(dq_run_id	bigint identity,
run_type	varchar	(100),
load_timestamp	timestamp,
modified_date	timestamp default timestamp)


-----------------------------------------------------------------3) insert the columns that you want to use-------------------------------------------------------------

insert into data_quality_columns
(creator	,
table_name	,
column_name	,
column_type	,
column_length	,
load_timestamp	)
select creator, tname, cname, coltype,length, getdate()
from sys.syscolumns
where upper(tname) in
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
'SLOT_INSTANCE')
and lower(creator) = 'sk_uat'

commit

-------------------------------------------------------------------------------4) insert the testing group that you want to use------------------------------------------------------------
---CONFIGURABLE TO WHAT TYPE OF CHECK YOU WANT TO USE IT FOR

insert into data_quality_run_group
(run_type	,load_timestamp)
values
('VESPA_UAT_TESTING', getdate())

commit


--------------------------------------------------------------------------------5) INsert the types of checks that you can do (relate to the dq_basic_checks proc---------------------------------------

insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('ISNULL_CHECK', getdate())

insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('UNKNOWN_CHECK',getdate())

insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('PRIMARY_KEY_CHECK',getdate())


insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('MAX_LENGTH_CHECK',getdate())

insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('DISTINCT_COUNT_CHECK',getdate())


insert into data_quality_check_type
(dq_check_type, load_timestamp)
values
('COLUMN_TYPE_LENGTH_CHECK',getdate())


commit



----------------------------------------------------------------------------6) INSERT THE COLUMNS YOU WANT TO CHECK (HAS BE CONFIGURED BASED ON INDIVIDUAL NEEDS-----------------------------------------------

---isnull checks insert

INSERT INTO data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp)
SELECT col.dq_col_id, sched.dq_run_id,
check1.dq_check_type_id, 0, 10,50,getdate()
from data_quality_columns col,
data_quality_run_group sched,
data_quality_check_type check1
where col.creator = 'sk_uat'
and upper(col.table_name) in
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
'SLOT_INSTANCE')
and check1.dq_check_type = 'ISNULL_CHECK'

commit


------------primary key INSERTS-----------------------------


INSERT INTO data_quality_check_details
(dq_col_id, dq_sched_run_id, dq_check_type_id, metric_benchmark, metric_tolerance_amber, metric_tolerance_red, load_timestamp)
SELECT col.dq_col_id, sched.dq_run_id,
check1.dq_check_type_id, 0, 10,50,getdate()
from (select b.* from sys.syscolumns a,
data_quality_columns b
where a.creator = b.creator
and a.tname = b.table_name
and a.cname = b.column_name
and a.colno = 1) col,
data_quality_run_group sched,
data_quality_check_type check1
where lower(col.creator) = 'sk_uat'
and lower(col.table_name) = 'viq_viewing_data'
and col.column_name = 'viewing_data_id'
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



