
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


EXECUTE logger_add_event @CP2_build_ID , 3,'Analytical Metrics Process Start for Date : '||cast (@analysis_date_current as varchar(20))

declare @metric_short_name varchar(200)
declare @dq_vm_id bigint


--------------------------------------------A01 - Select the metrics that you want to look through the Analytical Procedure for-------------------------

--dq_vm_id (unique identifier to get the metrics that you want to be run as part of this process)
--metric_short_name (unique name for each metric that you want to run)

--select pulls back those metrics that you want to run for this VESPA run and excluded any other type of metrics involved.
--a flaw with the current infrastructure is that each time a new bunch of metrics is added to the data_quality_vespa_metrics and 
--data_quality_check_details tables we need to take this into account to exclude from this batch as the naming convention
--was not locked down as this was part of the initial batch.  Subsequent control files are specific so do not need altering

 select dq_vm_id, metric_short_name
into #tmp_dqvm
 from data_quality_vespa_metrics
 where upper(metric_short_name) not like 'VDQ%'
and upper(metric_short_name) not like 'SCA%'
and upper(metric_short_name) not like 'TDQ%'
and upper(metric_short_name) not like 'ADQ%'
and upper(metric_short_name) not like 'ADSM%'
and upper(metric_short_name) not like 'MTC_ADSMART%'
and upper(metric_short_name) not like 'LSDQ%'
and upper(metric_short_name) not like 'DEMO_HH%'
and current_flag = 1

---------------old run when looking to run subset of metrics

----------testing---------------------------

/*
 select dq_vm_id, metric_short_name
into #tmp_dqvm
 from data_quality_vespa_metrics
 where upper(metric_short_name) not like 'VDQ%'
and upper(metric_short_name) not like 'SCA%'
and upper(metric_short_name) not like 'TDQ%'
--and upper(metric_short_name) not like '%STB%'
and current_flag = 1
and lower(metric_short_name) in 
('daily_viewing_stb_avg_precapped_live',
'daily_viewing_stb_avg_precapped_rec',
'daily_viewing_stb_avg_precapped_total',
'daily_viewing_stb_avg_postcapped_total',
'daily_viewing_stb_avg_postcapped_live',
'daily_viewing_stb_avg_postcapped_rec',
'daily_viewing_hh_avg_precapped_live',
'daily_viewing_hh_avg_precapped_rec',
'daily_viewing_hh_avg_precapped_total',
'daily_viewing_hh_avg_postcapped_total',
'daily_viewing_hh_avg_postcapped_live',
'daily_viewing_hh_avg_postcapped_rec')
*/


 -- this is the type unique index on the table you're updating

-- Copy out the unique ids of the rows you want to update to a temporary table
SELECT dq_vm_id into #temp FROM #tmp_dqvm

--------------------------------------------A02 - Define the loop which will be cycled through to execute the Analytical procedure-------------------------

--loop through the metrics that the above select returns.  For each one this will be getting presented to the Data Quality Metrics Collection 
--procedure so that it will action the metric concerned and return the results to the data_quality_vespa_repository table

while exists (select 1 from #temp)
begin
  set rowcount 1
  select @dq_vm_id  = dq_vm_id from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where dq_vm_id = @dq_vm_id  -- delete that uid from the temp table

set @metric_short_name = (select metric_short_name from #tmp_dqvm
where dq_vm_id = @dq_vm_id)

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics Process Start for '||@metric_short_name||''

--------------------------------------------A03 - Execute Analytical Metric Procedure for the metric short name within the loop-------------------------

execute Data_Quality_Metrics_Collection @load_date,@CP2_build_ID,@metric_short_name

EXECUTE logger_add_event @CP2_build_ID , 3,'Metrics Process End for '||@metric_short_name||''

end

EXECUTE logger_add_event @CP2_build_ID , 3,'Analytical Metrics Process End for Date : '||cast (@analysis_date_current as varchar(20))

end

go

grant execute on Data_Quality_Vespa_Metric_Run to vespa_group_low_security, sk_prodreg, buxceys,kinnairt