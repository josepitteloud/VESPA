
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data_Quality_Linear_Slot_Metric_Checks
**
** This is the control procedure to execute the Adsmart Analytical Metrics procedure where we present
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



if object_id('Data_Quality_Linear_Slot_Metric_Run') is not null drop procedure Data_Quality_Linear_Slot_Metric_Run
commit

go

create procedure Data_Quality_Linear_Slot_Metric_Run
     @load_date       date = NULL     -- Date of events being analyzed
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue))

as
begin


EXECUTE logger_add_event @RunID , 3,' Linear Slot Analytical Metrics Process Start for Date : '||cast (@analysis_date_current as varchar(20))

declare @metric_short_name varchar(200)
declare @dq_vm_id bigint


--------------------------------------------A01 - Select the metrics that you want to look through the Analytical Procedure for-------------------------


 select dq_vm_id, metric_short_name
into #tmp_dqvm
 from data_quality_vespa_metrics
 where upper(metric_short_name) like 'MTC%LINEAR%'
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

EXECUTE logger_add_event @RunID , 3,'Linear Slot Analytical Metrics Process End for Date : '||cast (@analysis_date_current as varchar(20))

end

go

grant execute on Data_Quality_Linear_Slot_Metric_Run to vespa_group_low_security, sk_prodreg, sawkinss, kinnairt