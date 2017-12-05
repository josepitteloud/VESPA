
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Olive_Basic_Checks
**
** The control proc which runs the basic checks routine to get execute the Olive basic checks for the
** data quality report
**
**  
** Refer also to:
**
**
** Code sections:
**      Part A: A01 - SELECT METRICS THAT WILL BE EXECUTED USING THE BASIC ROUTINE
**		A02 - get information from the Olive Basic Checks tables
**		A03 - Loop creation to execute each metric at a time
**
**      Part B:       
**              B01 - call basic checks procedure for each metric affected
**
** Things done:
**
**
******************************************************************************/



if object_id('Data_Quality_Olive_Basic_Checks') is not null drop procedure Data_Quality_Olive_Basic_Checks
commit

go

create procedure Data_Quality_Olive_Basic_Checks
    @run_type varchar(40) = 'OLIVE_BASIC_CHECKS'
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


EXECUTE kinnairt.logger_add_event @RunID , 3,'Data Quality Basic Checks Start for Date '||cast (@target_date as varchar(20))

------------------------------------------A01 - SELECT METRICS THAT WILL BE EXECUTED USING THE BASIC ROUTINE-----------------------------------------


select a.dq_check_detail_id, a.metric_short_name 
into #tmp_olive_metric
from data_quality_check_details a,
data_quality_RUN_GROUP b
where a.dq_SCHED_run_id = b.dq_run_id
and b.run_type = @run_type


------------------------------------------A02 - get information from the Vespa Basic Checks tables-----------------------------------------


select dq_chk_det.dq_check_detail_id,dq_col.creator, dq_col.column_name, dq_col.table_name, dq_chk_type.dq_check_type,
dq_run_grp.dq_run_id 
into #data_quality_run_process
from data_quality_columns dq_col,data_quality_check_type dq_chk_type,
data_quality_check_details dq_chk_det, data_quality_run_group dq_run_grp,
#tmp_olive_metric olive_metric
where dq_chk_det.dq_col_id = dq_col.dq_col_id
and dq_chk_det.dq_check_type_id = dq_chk_type.dq_check_type_id
and dq_chk_det.dq_sched_run_id = dq_run_grp.dq_run_id
and dq_run_grp.run_type = @run_type
and dq_chk_det.dq_check_detail_id = olive_metric.dq_check_detail_id

 -- this is the type unique index on the table you're updating

-- Copy out the unique ids of the rows you want to update to a temporary table
select dq_check_detail_id into #temp from #data_quality_run_process -- you can use a where condition here


------------------------------------------A03 - Loop creation to execute each metric at a time-----------------------------------------

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

EXECUTE kinnairt.logger_add_event @RunID , 3,'Data Quality Basic Checks End for Date '||cast (@target_date as varchar(20))


end

go

grant execute on Data_Quality_Olive_Basic_Checks to vespa_group_low_security, sk_prodreg