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
*
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
declare @min_value varchar(255)
declare @max_value varchar(255)
declare @var_sql_key varchar(50)

declare @pass_fail varchar(4)
declare @expected_value varchar(40)
declare @dq_check_detail_id bigint
declare @metric_benchmark decimal (16,3)
declare @metric_tolerance_amber decimal (16,3)
declare @metric_tolerance_red decimal (16,3)
declare @metric_rag varchar(5)
declare @exception_value varchar(255)
declare @notnull_col_checks varchar(1000)
declare @pk_result int
declare @cb_match_value int
declare @var_sql_null varchar(2000)
declare @match_check_table_name varchar(200)
declare @notnull_col_checks_loop varchar(8000)
declare @table_sql_where_clause varchar(2000)
set @load_date = getdate()

-------------------------------A01 - Collect basic information from the vespa metrics tables---------------------------------

--essentially for the table, column, check type etc get the necessary information to understand how this is to run through.

--unknown value (essentially this can define what value for the unknown that the specific check will look for)
--dq_det_id (an identifier to pull out the specific check that this is referring to)
--expected_value (for some of the checks there is an expected value that we are looking to compare our result against)
--column_type (as we store the table and column detail in our own table to check for scenarios when this has been changed at source)
--dq_run_id (a unique identifier relating to the run group e.g. VESPA_DATA_QUALITY, ADSMART_DATA_QUALITY etc)
--metric_benchmark (a value which the metric in question should be seen as relating to)
--metric_benchmark_amber (a tolerance threshold outside of which the result to metric_benchmark comparison should be flagged as AMBER)
--metric_benchmark_red (a tolerance threshold outside of which the result to metric_benchmark comparison should be flagged as RED)
--exception_value (with some of the basic checks (min and max mainly) it was asked that we exclude certain values from the analysis and this field allows this to happen)
--notnull_col_checks (allows addition in where clause for including that specific columns must be not null as part of the overall basic check)
--sql_where_clause (the not null col checks where not enough and there was a requirement to make the where clause more complex in the exclusion on these tables)


select dq_chk_det.unknown_value unknown_value, dq_chk_det.dq_check_detail_id dq_det_id,
dq_chk_det.expected_value expected_value,
dq_col.column_type column_type, dq_run_type.dq_run_id dq_run_id,
dq_chk_det.metric_benchmark metric_benchmark, 
dq_chk_det.metric_tolerance_amber metric_tolerance_amber, 
dq_chk_det.metric_tolerance_red metric_tolerance_red,
dq_chk_det.exception_value, dq_chk_det.notnull_col_checks,
dq_chk_det.sql_where_clause
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
set @exception_value = (select exception_value from #tmp_dq_values)
set @notnull_col_checks = (select notnull_col_checks from #tmp_dq_values)
set @table_sql_where_clause = (select sql_where_clause from #tmp_dq_values)



-------------------------------B01 - Isnull Check---------------------------------


--does exactly what you would expect checks whether the column in the creator:table concerned has any values that are null
--split into those that do not have a complex where clause and those that do.


if @check_type = 'ISNULL_CHECK' 
begin

if @table_sql_where_clause = null
begin
set @var_sql = 'select count(1) INTO @count_isnull from '||@creator||'.'||@table_name||' where '||@column_name||' is null'


end


if @table_sql_where_clause != null
begin
set @var_sql = 'select count(1) INTO @count_isnull from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||' and '||@column_name||' is null'

end

execute (@var_sql)
commit

-------------------------------B02 - Metric benchmark for Isnull---------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@count_isnull, @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ ISNULL check for '||@table_name||'.'||@column_name||''


-------------------------------B03 - Insert into Data Quality Results table---------------------------------


--inserts results from above query and benchmark check into the data_quality_results table

insert into data_quality_results 
(dq_check_detail_id, dq_run_id, result, rag_status, sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id, @count_isnull,@metric_rag, @var_sql,@load_date, @CP2_build_ID,@target_date)
commit

end
-----------------------------------------------------------------------------------------------------------------


-------------------------------C01 - Max Length Check---------------------------------


--does exactly what you would expect checks for the Maximum value for the column in the creator:table concerned 
--it is split into where the column is like a character and where it is a number and also 
--if there is anything to be added to the where clause


if @check_type = 'MAX_LENGTH_CHECK' 
begin


if (UPPER(@column_type) LIKE '%CHAR%' and @table_sql_where_clause = null)
begin
set @var_sql = 'select max('||@column_name||') INTO @max_length_chk from '||@creator||'.'||@table_name||''

execute (@var_sql)
commit

end

if (UPPER(@column_type) LIKE '%CHAR%' and @table_sql_where_clause != null)
begin
set @var_sql = 'select max('||@column_name||') INTO @max_length_chk from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''

execute (@var_sql)
commit

end

if isnull(@var_sql,'<NULL>') = '<NULL>'
begin
set @var_sql = 'column type not appropriate for processing'

end

----------------------------------------------------C02 - Metric benchmark for max length--------------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@max_length_chk, @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Max Length Chk for '||@table_name||'.'||@column_name||''

--------------------------------------------------C03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id, result, rag_status, sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id, @max_length_chk,@metric_rag, @var_sql,@load_date, @CP2_build_ID,@target_date)
commit

end

------------------------------------------------------------------------------------------------------------------------------

-------------------------------D01 - Distinct Count Check-------------------------------------------------------------

--does exactly what you would expect checks for the count of the distinct values in the creator:table concerned 
--it is split into if there is anything to be added to the where clause


if @check_type = 'DISTINCT_COUNT_CHECK' 
begin

if @table_sql_where_clause = null
begin
set @var_sql = 'select count (distinct '||@column_name||') INTO @dist_cnt_chk from '||@creator||'.'||@table_name||''
end

if @table_sql_where_clause != null
begin
set @var_sql = 'select count (distinct '||@column_name||') INTO @dist_cnt_chk from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''
end

execute (@var_sql)
commit

------------------------------------D02 - Metric benchmark for distinct count------------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@dist_cnt_chk , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Distinct Cnt for '||@table_name||'.'||@column_name||''


--------------------------------------------------D03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@dist_cnt_chk,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit
end

-----------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------E01 - Unknown Check----------------------------------------------------

--does exactly what you would expect checks for the how many rows we have in the creator:table where there are specific unknown values
--unknown value being searched for is defined by the value that is included to be checked in the data_quality_check_details table
--it is split into if there is anything to be added to the where clause and also if it is a numeric or non numeric column.


if @check_type = 'UNKNOWN_CHECK' 
begin

IF (isnumeric(@unknown_value) = 1 and @table_sql_where_clause = null)
begin
set @var_sql = 'select count (1) INTO @unknown_chk from '||@creator||'.'||@table_name||' WHERE '||@column_name||' = convert(int, ('||@unknown_value||'))'

commit

end

IF (isnumeric(@unknown_value) = 1 and @table_sql_where_clause != null)
begin
set @var_sql = 'select count (1) INTO @unknown_chk from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||' and '||@column_name||' = convert(int, ('||@unknown_value||'))'

commit

end


IF (isnumeric(@unknown_value) = 0 and @table_sql_where_clause = null)
begin
set @var_sql = 'select count (1) INTO @unknown_chk from '||@creator||'.'||@table_name||' WHERE UPPER('||@column_name||') = UPPER('''||@unknown_value||''')'

commit

end

IF (isnumeric(@unknown_value) = 0 and @table_sql_where_clause != null)
begin
set @var_sql = 'select count (1) INTO @unknown_chk from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||' and UPPER('||@column_name||') = UPPER('''||@unknown_value||''')'

commit

end


execute (@var_sql)
commit

--------------------------------------------E02 - Metric benchmark for unknown check----------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@unknown_chk , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Unknown Chk for '||@table_name||'.'||@column_name||''


--------------------------------------------------E03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@unknown_chk,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit

end

------------------------------------------------------------------------------------------------------------------------------------------------------


-------------------------------------------------F01 - Column Type Check------------------------------------------------------

--does exactly what you would expect checks to see if there is any differences between the creator:table value in the system tables against
--the values that we have for this column in our data_quality_columns table
--it is split into if there is anything to be added to the where clause.


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

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@COL_TYPE_CHK  , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Col Type Chk for '||@table_name||'.'||@column_name||''


--------------------------------------------------F03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@COL_TYPE_CHK,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit
end

------------------------------------------------------------------------------------------------------------------------------------------------------

if @check_type = 'PRIMARY_KEY_CHECK' 

-------------------------------------------------G01 - Primary Key Check------------------------------------------------------

--does exactly what you would expect checks to see if if the creator||table||column combo that is being checked is a primary key.


begin
set @var_sql = 'select count (1) INTO @table_row_count from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''

execute (@var_sql)

commit

set @var_sql = 'select count (distinct '||@column_name||') INTO @column_row_count from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''

execute (@var_sql)

commit

set @pk_result = @table_row_count - @column_row_count

--------------------------------------------G02 - Metric benchmark for primary Key check----------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@pk_result , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ PK Chk for '||@table_name||'.'||@column_name||''


--------------------------------------------------G03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@pk_result,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit

end


----------------------------------------------TABLE COUNT CHECK--------------------------------------------------------------

--does exactly what you would expect checks to see the row count in any creator||table combo


if @check_type = 'TABLE_COUNT_CHECK' 
begin
set @var_sql = 'select count (1) INTO @table_row_count from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''

execute (@var_sql)
commit

------------------------------------H02 - Metric benchmark for distinct count------------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@table_row_count, @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ TABLE_COUNT_CHECK Cnt for '||@creator||'.'||@table_name||''


--------------------------------------------------H03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@table_row_count,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit
end


----------------------------------------------MIN VALUE--------------------------------------------------------------

--does exactly what you would expect checks to the min value for the creator||table||column combo that is being checked.
--split by what is in the exception values column along with if there is anything else in the where clause



if @check_type = 'MIN_VALUE_CHECK' 
begin

if @exception_value is null
begin

set @var_sql = 'select min('||@column_name||') INTO @min_value from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''

end

if @exception_value is not null
begin

if @table_sql_where_clause = null
begin
set @var_sql = 'select min('||@column_name||') INTO @min_value from '||@creator||'.'||@table_name||' where '||@column_name||' not in ('''||@exception_value||''')'
end

if @table_sql_where_clause != null
begin
set @var_sql = 'select min('||@column_name||') INTO @min_value from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||' and '||@column_name||' not in ('''||@exception_value||''')'
end

end

execute (@var_sql)
commit


EXECUTE logger_add_event @CP2_build_ID , 3,'DQ MIN_VALUE_CHECK Cnt for '||@creator||'.'||@table_name||'.||@column_name||'


--------------------------------------------------J03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result_text, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@min_value,null,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit
end
----------------------------------------------------------------------------------------------------------------------------

--does exactly what you would expect checks to the min value for the creator||table||column combo that is being checked.
--split by what is in the exception values column along with if there is anything else in the where clause

if @check_type = 'MAX_VALUE_CHECK' 
begin


if @exception_value is null
begin

set @var_sql = 'select max('||@column_name||') INTO @max_value from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||''

end

if @exception_value is not null
begin

if @table_sql_where_clause = null
begin
set @var_sql = 'select max('||@column_name||') INTO @max_value from '||@creator||'.'||@table_name||' where '||@column_name||' not in ('''||@exception_value||''')'
end

if @table_sql_where_clause != null
begin
set @var_sql = 'select max('||@column_name||') INTO @max_value from '||@creator||'.'||@table_name||' '||@table_sql_where_clause||' and '||@column_name||' not in ('''||@exception_value||''')'
end

end


execute (@var_sql)
commit


EXECUTE logger_add_event @CP2_build_ID , 3,'DQ MAX_VALUE_CHECK Cnt for '||@creator||'.'||@table_name||'.'||@column_name||''

--------------------------------------------------K03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result_text, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@max_value,null,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit
end



------------obsolete now as was going to be used for experian work but other work superseded this

-------------SKYBASE CHECK

/*

if @check_type = 'MATCH_RATES_SKYBASE_CHECK' 
begin
--------------------------------------------L02 - Metric benchmark for unknown check----------------------------------------

if lower(@column_name) = 'cb_key_individual' 
begin
set @var_sql_key = 'individual_key'
end

if lower(@column_name) = 'cb_key_household' 
begin
set @var_sql_key = 'household_key'
end


if lower(@column_name) = 'cb_address_postcode' 
begin
set @var_sql_key = 'cb_address_postcode'

end

----------------------------------------checks on individual or household key attributes--------------------------------------------------------

if @var_sql_key in ('individual_key','household_key')
begin
if @notnull_col_checks is null 
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_Skybase as s 
                inner join '||@creator||'.'||@table_name||' as e on e.'||@column_name||' = s.'||@var_sql_key||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0 

begin

set @var_sql_null = 'where e.'||@notnull_col_checks||' is not null '

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_Skybase as s 
                inner join '||@creator||'.'||@table_name||' as e on e.'||@column_name||' = s.'||@var_sql_key||' ' ||@var_sql_null||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0 

begin

set @notnull_col_checks_loop = @notnull_col_checks

set @var_sql_null = ' where '

commit

while charindex(',',@notnull_col_checks_loop) > 0
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_Skybase as s 
                inner join '||@creator||'.'||@table_name||' as e on e.'||@column_name||' = s.'||@var_sql_key||''

set @var_sql_null = @var_sql_null + 'e.' + ' ' + substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop) - 1) + ' is not null and '

set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',', @notnull_col_checks_loop) + 1)

end 

set @var_sql_null = @var_sql_null + ' e.' + @notnull_col_checks_loop + ' is not null'

set @var_sql = @var_sql + @var_sql_null

end

execute (@var_sql)
end

----------------------------------------checks on postcode attributes--------------------------------------------------------

if @var_sql_key = 'cb_address_postcode'
begin

if @notnull_col_checks is null 
begin

--inner join '||@creator||'.'||@table_name||' as e on TRIM(REPLACE(e.'||@column_name||',' ','')) = s.'||@var_sql_key||''

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_SkyPostcode as s 
inner join '||@creator||'.'||@table_name||' as e on trim(replace(e.'||@column_name||','' '','''')) = s.'||@var_sql_key||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0 

begin

set @var_sql_null = 'where e.'||@notnull_col_checks||' is not null '


set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_SkyPostcode as s 
                inner join '||@creator||'.'||@table_name||' as e on trim(replace(e.'||@column_name||','' '','''')) = s.'||@var_sql_key||' ' ||@var_sql_null||''
                
end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0 

begin

set @notnull_col_checks_loop = @notnull_col_checks

set @var_sql_null = ' where '

commit

while charindex(',',@notnull_col_checks_loop) > 0
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_SkyPostcode as s 
                inner join '||@creator||'.'||@table_name||' as e on trim(replace(e.'||@column_name||','' '','''')) = s.'||@var_sql_key||''

set @var_sql_null = @var_sql_null + 'e.' + ' ' + substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop) - 1) + ' is not null and '

set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',', @notnull_col_checks_loop) + 1)

end 

set @var_sql_null = @var_sql_null + ' e.' + @notnull_col_checks_loop + ' is not null'

set @var_sql = @var_sql + @var_sql_null


end

execute (@var_sql)

end



--------------------------------------------L02 - Metric benchmark for unknown check----------------------------------------


--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@cb_match_value  , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ SkyBase Match Check for '||@table_name||'.'||@column_name||''

--------------------------------------------------E03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@cb_match_value,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit


end


---------------------------------------------------------------------SKYBASE FINISHED--------------------------------------------------------

if @check_type = 'MATCH_RATES_VESPA_CHECK' 
begin
--------------------------------------------M02 - Metric benchmark for unknown check----------------------------------------

if lower(@column_name) = 'cb_key_individual' 
begin
set @var_sql_key = 'individual_key'

end

if lower(@column_name) = 'cb_key_household' 
begin
set @var_sql_key = 'household_key'
end

if lower(@column_name) = 'cb_address_postcode' 
begin
set @var_sql_key = 'cb_address_postcode'

end

----------------------------------------checks on individual or household key attributes--------------------------------------------------------

if @var_sql_key in ('individual_key','household_key') 
begin

if @notnull_col_checks is null 
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_Vespa as s 
                inner join '||@creator||'.'||@table_name||' as e on e.'||@column_name||' = s.'||@var_sql_key||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0 

begin

set @var_sql_null = 'where e.'||@notnull_col_checks||' is not null '


set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_Vespa as s 
                inner join '||@creator||'.'||@table_name||' as e on e.'||@column_name||' = s.'||@var_sql_key||' ' ||@var_sql_null||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0 

begin

set @notnull_col_checks_loop = @notnull_col_checks

set @var_sql_null = ' where '

commit

while charindex(',',@notnull_col_checks_loop) > 0
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_Vespa as s 
                inner join '||@creator||'.'||@table_name||' as e on e.'||@column_name||' = s.'||@var_sql_key||''

set @var_sql_null = @var_sql_null + 'e.' + ' ' + substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop) - 1) + ' is not null and '

set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',', @notnull_col_checks_loop) + 1)

end 

set @var_sql_null = @var_sql_null + ' e.' + @notnull_col_checks_loop + ' is not null'

set @var_sql = @var_sql + @var_sql_null


end


execute (@var_sql)


end

------------------------------------------checks on individual or household key ended---------------------------------------------------------------

----------------------------------------checks on postcode attributes--------------------------------------------------------

if @var_sql_key = 'cb_address_postcode' 
begin

if @notnull_col_checks is null 
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_VespaPostcode as s 
                inner join '||@creator||'.'||@table_name||' as e on trim(replace(e.'||@column_name||','' '','''')) = s.'||@var_sql_key||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) = 0 

begin

set @var_sql_null = 'where e.'||@notnull_col_checks||' is not null '

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_VespaPostcode as s 
                inner join '||@creator||'.'||@table_name||' as e on trim(replace(e.'||@column_name||','' '','''')) = s.'||@var_sql_key||' ' ||@var_sql_null||''

end

if @notnull_col_checks is not null and charindex(',',@notnull_col_checks) > 0 

begin

set @notnull_col_checks_loop = @notnull_col_checks

set @var_sql_null = ' where '

commit

while charindex(',',@notnull_col_checks_loop) > 0
begin

set @var_sql = 'select count(distinct e.'||@column_name||') INTO @cb_match_value from Data_Quality_Match_Rates_VespaPostcode as s 
                inner join '||@creator||'.'||@table_name||' as e on trim(replace(e.'||@column_name||','' '','''')) = s.'||@var_sql_key||''

set @var_sql_null = @var_sql_null + 'e.' + ' ' + substring(@notnull_col_checks_loop,1,charindex(',',@notnull_col_checks_loop) - 1) + ' is not null and '

set @notnull_col_checks_loop = substring(@notnull_col_checks_loop,charindex(',', @notnull_col_checks_loop) + 1)

end 

set @var_sql_null = @var_sql_null + ' e.' + @notnull_col_checks_loop + ' is not null'

set @var_sql = @var_sql + @var_sql_null


end

execute (@var_sql)

end


-------------------------------------------check on postcode level ended-----------------------------------------------------------------------------------


--------------------------------------------M02 - Metric benchmark for unknown check----------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@cb_match_value  , @metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Vespa Match Check for '||@table_name||'.'||@column_name||''

--------------------------------------------------M03 - Insert into Data Quality Results table---------------------------------


--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@cb_match_value,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit


end

*/


---------------------------------------------------------TABLE COLUMNS COUNT-----------------------------------------------------

if @check_type = 'TABLE_COLUMNS_COUNT_CHECK' 

-------------------------------------------------N01 - Table Columns Count------------------------------------------------------


--check the systems table for the table you want to check to see how many columns are in the table.  To highlight if anything is added without prior knowledge


begin

set @var_sql = 'select count ('||@column_name||') INTO @column_row_count from sys.syscolumns where upper(creator) = upper('''||@creator||''') and upper(tname) = upper('''||@table_name||''')'

execute (@var_sql)

commit

--------------------------------------------N02 - Metric benchmark for Table Columns Count check----------------------------------------

--presents the result of the query to the metric benchmark check function, along with the expected
--threshold and tolerance percetages so that we can derive if it is GREEN, AMBER or RED.


set @metric_rag = metric_benchmark_check(@column_row_count,@metric_benchmark, @metric_tolerance_amber,@metric_tolerance_red)

commit

EXECUTE logger_add_event @CP2_build_ID , 3,'DQ Table COlumn Count Check for '||@table_name||'.'||@column_name||''


--------------------------------------------------N03 - Insert into Data Quality Results table---------------------------------

--inserts results from above query and benchmark check into the data_quality_results table


insert into data_quality_results 
(dq_check_detail_id, dq_run_id,result, rag_status,sql_processed,load_timestamp, logger_id, data_date)
values
(@DQ_DET_ID,@dq_run_id,@column_row_count,@metric_rag,@var_sql,@load_date, @CP2_build_ID, @target_date)
commit

end


end

go

grant execute on DQ_Basic_Checks to vespa_group_low_security, sk_prodreg, buxceys,kinnairt
