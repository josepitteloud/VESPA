
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data Quality Data Processing Month
**
** This is the overriding data process procedure which will be run as part of the data quality metrics
** piece.  All metrics that are run as part of the Data quality process should be referenced in this
** procedure.  This procedure will look at each Month and select the correct event data from that month
**
** Refer also to:
**
** Data Quality Vespa Scaling Checks
** Data Quality Vespa Basic Checks
** Data Quality Basic Checks
** Data Quality Vespa Metric Run
** Data Quality Metrics Collection
** Metric Benchmark function
** Data_Quality_Vespa_STB_Checks
**
** Code sections:
**      Part A: A01 - RunType (Month)
**
**      Part B:       Data capture
**              B01 - Select most recent date loaded
**              B02 - Collect all days where events were loaded on that load date
**              B03 - collect the event days you want to process
**
**      Part C:       Scaling Process
**              C01 - Vespa Scaling Process
**
**      Part D:       Data Collection
**              D01 - Data Collection
**              D02 - get data for programmes
**              D03 - get data for slots
**
**      Part E:       Vespa Basic Checks
**
**		E01 - Execute the Vespa Basic Checks procedure
**		E02 - insert relevant metrics into the metrics repository table
**
**      Part F:       Vespa Analytical Checks
**
**		F01 - Vespa Analytical Metrics
**
**
**
**
** Things done:
**
**
******************************************************************************/


if object_id('data_quality_household_metrics') is not null drop procedure data_quality_household_metrics
commit

go

create procedure data_quality_household_metrics
@CP2_build_ID     bigint = NULL

as
begin

declare @analysis_date date
declare @analysis_min_date date
declare @analysis_max_date date
declare @analysis_date_current date
declare @data_count int
declare @data_days_analyze int
declare @sql_stmt varchar(8000)
declare @run_stmt varchar(200)
declare @slot_date int
declare @build_date date

set @build_date = today()

EXECUTE logger_add_event @RunID , 3,'Data Quality Household Metrics Start', @CP2_build_ID

select @build_date event_date into #analysis_event_dates_final 

---lets see how many days worth of data we are running for

set @data_count = null

set @data_count = (select count(1) from #analysis_event_dates_final)

--ok time to start with the 1st date we have on our list of dates to examine--

set @analysis_date_current = (select event_date from #analysis_event_dates_final)

----------------------------------D01 - Data collection-----------------------------------------------

----------------------------------E01 - Execute the Vespa Basic Checks procedure-----------------------------------------------

----need to now execute all your procedures on the collected audit data here...--------------

---lets get the basic checks done first

execute Data_Quality_Household_Basic_Checks 'HOUSEHOLD_DIMENSION_CHECKS',today(),@CP2_build_ID

---as some person had a crazy notion of capturing generic checks more widely, we need to go off
---to thar table to get the relevant metrics that have been run and bring them into a Vespa-specific
---repository.  Nuts!!

----------------------------------E02 - insert relevant metrics into the metrics repository table-----------------------------------------------
insert into data_quality_vespa_repository
(dq_run_id, viewing_data_date, dq_vm_id, metric_result, metric_tolerance_amber, metric_tolerance_red,metric_rag, load_timestamp, modified_date)
select dqr.logger_id,dqr.data_date,dq_vm.dq_vm_id,result,dq_chk_det.metric_tolerance_amber, dq_chk_det.metric_tolerance_red,
dqr.rag_status,dqr.load_timestamp, dqr.modified_date
from data_quality_results dqr,
data_quality_check_details dq_chk_det,
data_quality_vespa_metrics dq_vm
where dqr.dq_check_detail_id = dq_chk_det.dq_check_detail_id
and dq_chk_det.metric_short_name = dq_vm.metric_short_name
and dqr.logger_id = @CP2_build_ID
and dqr.data_date = @analysis_date_current

commit

set @data_count = null

set @data_count = (select count(1) from data_quality_vespa_repository where dq_run_id = @CP2_build_ID 
and viewing_data_date = @analysis_date_current)

EXECUTE logger_add_event @RunID , 3,'Count of metrics collected so far for '||cast (@analysis_date_current as varchar(20)),@data_count

----------------------------------F01 - Vespa Analytical Metrics-----------------------------------------------

----now lets do some Vespa specific metrics 
execute data_quality_household_metrics_run @CP2_build_ID     ,@analysis_date_current
---now move onto the next day

set @data_count = null

EXECUTE logger_add_event @RunID , 3,'Data Quality Household Metrics End', @CP2_build_ID

commit

end


go

grant execute on data_quality_household_metrics to vespa_group_low_security, sk_prodreg, buxceys, kinnairt
