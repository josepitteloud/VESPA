
--------------------------------------------------------------------------------------------------------------------------------------------------------------




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


if object_id('data_quality_data_processing_month') is not null drop procedure data_quality_data_processing_month 
commit

go

create procedure data_quality_data_processing_month
@run_type varchar(20)
,@CP2_build_ID     bigint = NULL
,@year_month varchar(6)
,@build_date date
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


EXECUTE logger_add_event @CP2_build_ID , 3,'Data Quality Process Start Month', @CP2_build_ID

----------------------------------B03 - collect the event days you want to process--------------------------------------------------


select @build_date event_date into #analysis_event_dates_final 

---lets see how many days worth of data we are running for

delete from #analysis_event_dates_final where event_date = today()

set @data_count = null

set @data_count = (select count(1) from #analysis_event_dates_final)

EXECUTE logger_add_event @CP2_build_ID , 3,'Data Quality running for count of days',@data_count

--ok time to start with the 1st date we have on our list of dates to examine--

set @analysis_min_date  = (select min(event_date) from #analysis_event_dates_final)

set @analysis_max_date  = (select max(event_date) from #analysis_event_dates_final)

set @analysis_date_current = @analysis_min_date 

EXECUTE logger_add_event @CP2_build_ID , 3,'Data Quality Process Start for Date '||cast (@analysis_date_current as varchar(20))

--comment out tdq checks as need to run this once in a process not once for each day------------------------------------

--execute Data_Quality_Vespa_Basic_Checks_tdq 'VESPA_UAT_TESTING',@analysis_date_current,@CP2_build_ID

----------------------------------D01 - Data collection-----------------------------------------------

while @analysis_date_current <= @analysis_max_date 
begin

set @slot_date = cast(replace(@analysis_date_current,'-','') as int)

--programme data staging table

delete from data_quality_dp_data_audit

delete from DATA_QUALITY_SLOT_DATA_AUDIT

--scaling accounts staging table

delete from data_quality_scaling_accounts

----------------------------------D02 - get data for programmes-----------------------------------------------

set @sql_stmt = 'insert into data_quality_dp_data_audit
select '''||@analysis_date_current||''', pk_viewing_prog_instance_fact,cb_change_date,dk_barb_min_start_datehour_dim,dk_barb_min_start_time_dim,
dk_barb_min_end_datehour_dim,dk_barb_min_end_time_dim,dk_channel_dim, dk_event_start_datehour_dim,dk_event_start_time_dim,
dk_event_end_datehour_dim,dk_event_end_time_dim,dk_instance_start_datehour_dim,dk_instance_start_time_dim,
dk_instance_end_datehour_dim,dk_instance_end_time_dim,dk_programme_dim, dk_programme_instance_dim, dk_viewing_event_dim,
genre_description, sub_genre_description,service_type,service_type_description, type_of_viewing_event, account_number,panel_id,
live_recorded,barb_min_start_date_time_utc,barb_min_end_date_time_utc,event_start_date_time_utc,event_end_date_time_utc,
instance_start_date_time_utc,instance_end_date_time_utc,dk_capping_end_datehour_dim,dk_capping_end_time_dim,capping_end_date_time_utc,
log_start_date_time_utc, duration, subscriber_id, log_received_start_date_time_utc,capped_full_flag,capped_partial_flag, service_key
from sk_prod.vespa_dp_prog_viewed_'||@year_month||' vdpvc where date(vdpvc.event_start_date_time_utc) = '''||@analysis_date_current||''' '

--execute sql statement to populate the programme viewing table

execute (@sql_stmt)

commit

set @data_count = null

set @data_count = (select count(1) from data_quality_dp_data_audit)

if @data_count = 0
begin
EXECUTE logger_add_event @CP2_build_ID ,2,'Refresh of Data Audit table not worked',@data_count

end

if @data_count > 0
begin
EXECUTE logger_add_event @CP2_build_ID , 3,'Count of records in data_quality_dp_data_audit table',@data_count
end

----------------------------------D03 - get data for slots-----------------------------------------------

--discontinued as superseded by other procs

/*
insert into DATA_QUALITY_SLOT_DATA_AUDIT
SELECT  SLOT_DATA_KEY, VIEWED_START_DATE_KEY, IMPACTS, RECORD_DATE, HOUSEHOLD_KEY, UTC_DATEHOUR IMPACT_DAY,
slot_inst_data.slot_instance_key, 
slot_inst_data.channel_key, 
slot_inst_data.slot_start_date_key, 
slot_inst_data.slot_start_time_key, 
slot_inst_data.slot_end_date_key, 
slot_inst_data.slot_end_time_key,
slot_inst_data.previous_programme_key,
slot_inst_data.next_programme_key,
slot_inst_data.prev_prog_schedule_key, 
slot_inst_data.next_prog_schedule_key, 
slot_inst_data.prev_broadcast_start_date, 
slot_inst_data.next_broadcast_start_date, 
slot_inst_data.prev_broadcast_start_time, 
slot_inst_data.next_broadcast_start_time,
slot_inst_data.slot_start_date, 
slot_inst_data.slot_end_date, 
slot_inst_data.slot_start_time, 
slot_inst_data.slot_end_time,
slot.scaling_factor,
slot_inst_data.prev_broadcast_end_time, 
slot_inst_data.next_broadcast_end_time,
slot_inst_data.prev_broadcast_end_date, 
slot_inst_data.next_broadcast_end_date,
slot.slot_key,
slot.viewed_duration,
slot.viewed_start_time_key,
slot.time_shift_key
FROM SK_PROD.SLOT_DATA slot
inner join
sk_prod.viq_date viq_date
on
slot.broadcast_start_date_key = viq_date.pk_datehour_dim
left outer join
(select slot_instance_key, channel_key, slot_start_date_key, slot_start_time_key, slot_end_date_key, slot_end_time_key,
previous_programme_key,prev_prog_schedule_key, next_programme_key,next_prog_schedule_key, 
prev_prog_start_date.local_day_date prev_broadcast_start_date, 
prev_prog_end_date.local_day_date prev_broadcast_end_date, 
next_prog_start_date.local_day_date next_broadcast_start_date, 
next_prog_end_date.local_day_date next_broadcast_end_date, 
prev_prog_start_time.local_time_minute prev_broadcast_start_time, 
prev_prog_end_time.local_time_minute prev_broadcast_end_time,
next_prog_start_time.local_time_minute next_broadcast_start_time,next_prog_end_time.local_time_minute next_broadcast_end_time,
slot_start_date.local_day_date slot_start_date, 
slot_end_date.local_day_date slot_end_date, 
slot_start_time.local_time_minute slot_start_time, 
slot_end_time.local_time_minute slot_end_time
from sk_prod.slot_instance slot_inst
left outer join
sk_prod.viq_programme_schedule viq_prev_prog_sched
on
slot_inst.prev_prog_schedule_key = viq_prev_prog_sched.programme_instance_id
left outer join
sk_prod.viq_programme_schedule viq_next_prog_sched
on
slot_inst.next_prog_schedule_key = viq_next_prog_sched.programme_instance_id
left outer join
sk_prod.viq_date prev_prog_start_date
on 
viq_prev_prog_sched.dk_start_datehour = prev_prog_start_date.pk_datehour_dim
left outer join
sk_prod.viq_date prev_prog_end_date
on 
viq_prev_prog_sched.dk_end_datehour = prev_prog_end_date.pk_datehour_dim
left outer join
sk_prod.viq_date next_prog_start_date
on
viq_next_prog_sched.dk_start_datehour = next_prog_start_date.pk_datehour_dim
left outer join
sk_prod.viq_date next_prog_end_date
on
viq_next_prog_sched.dk_end_datehour = next_prog_end_date.pk_datehour_dim
left outer join
sk_prod.viq_time prev_prog_start_time
on
viq_prev_prog_sched.dk_start_time = prev_prog_start_time.pk_time_dim
left outer join
sk_prod.viq_time prev_prog_end_time
on
viq_prev_prog_sched.dk_end_time = prev_prog_end_time.pk_time_dim
left outer join
sk_prod.viq_time next_prog_start_time
on
viq_next_prog_sched.dk_start_time = next_prog_start_time.pk_time_dim
left outer join
sk_prod.viq_time next_prog_end_time
on
viq_next_prog_sched.dk_end_time = next_prog_end_time.pk_time_dim
left outer join
sk_prod.viq_date slot_start_date
on
slot_inst.slot_start_date_key = slot_start_date.pk_datehour_dim
left outer join
sk_prod.viq_date slot_end_date
on
slot_inst.slot_end_date_key = slot_end_date.pk_datehour_dim
left outer join
sk_prod.viq_time slot_start_time
on
slot_inst.slot_start_time_key = slot_start_time.pk_time_dim
left outer join
sk_prod.viq_time slot_end_time
on
slot_inst.slot_end_time_key = slot_end_time.pk_time_dim) slot_inst_data
on
slot.slot_instance_key = slot_inst_data.slot_instance_key
where broadcast_start_date_key/100 = @slot_date

commit

*/

set @data_count = null


/*

set @data_count = (select count(1) from DATA_QUALITY_SLOT_DATA_AUDIT)

if @data_count = 0
begin
EXECUTE logger_add_event @CP2_build_ID ,2,'Refresh of Data Slot table not worked',@data_count

end

if @data_count > 0
begin
EXECUTE logger_add_event @CP2_build_ID , 3,'Count of records in DATA_QUALITY_SLOT_DATA_AUDIT table',@data_count
end

*/

----------------------------------C01 - Vespa Channel Watching--------------------------------------------------


--call the channel watch procedure so we can continue to capture the viewing by channel to allow for analysis
--of any major drop offs

execute Data_Quality_Channel_Watching @analysis_date_current,@CP2_build_ID 


----------------------------------D01 - Vespa Scaling Process--------------------------------------------------


--As some of the metrics need to know about what records we have for scaling for the event day in question
--we pull these accounts into a separate dq table so that we can audit how many we have for that given day


insert into data_quality_scaling_accounts
select account_number from sk_prod.viq_viewing_data_scaling
where adjusted_event_start_date_vespa = @analysis_date_current

commit

--call the scaling checks procedure

--commented out 16/12/2013 as not being used and will save processing times.
--execute Data_Quality_Vespa_Scaling_Checks @analysis_date_current,@CP2_build_ID 

----------------------------------E01 - Execute the Vespa Basic Checks procedure-----------------------------------------------


----need to now execute all your procedures on the collected audit data here...--------------

---lets get the basic checks done first

execute Data_Quality_Vespa_Basic_Checks 'VESPA_DATA_QUALITY',@analysis_date_current,@CP2_build_ID

---as some person had a crazy notion of capturing generic checks more widely, we need to go off
---to that table to get the relevant metrics that have been run and bring them into a Vespa-specific
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

EXECUTE logger_add_event @CP2_build_ID , 3,'Count of metrics collected so far for '||cast (@analysis_date_current as varchar(20)),@data_count

----------------------------------F01 - Vespa Analytical Metrics-----------------------------------------------

--execute separate procedure which is calculating the STB checks (may be the way to go going forward
--so that you can keep better control over the size of procedures that can be run)

--decomissioned for reprocessing due to the differences in vespa panellists between date of event and current view of panel

--execute Data_Quality_Vespa_STB_Checks @analysis_date_current, @CP2_build_ID     

----now lets do some Vespa specific metrics 
----these are more analytical based metrics which had to be coded specifically for this process

execute Data_Quality_Vespa_Metric_Run @analysis_date_current, @CP2_build_ID     

set @data_count = null

set @data_count = (select count(1) from data_quality_vespa_repository where dq_run_id = @CP2_build_ID 
and viewing_data_date = @analysis_date_current)

EXECUTE logger_add_event @CP2_build_ID , 3,'Count of metrics collected so far for '||cast (@analysis_date_current as varchar(20)),@data_count

set @analysis_date_current = @analysis_date_current + 1

EXECUTE logger_add_event @CP2_build_ID , 3,'Data Quality Process End for Date '||cast (@analysis_date_current as varchar(20))

end

EXECUTE logger_add_event @CP2_build_ID , 3,'Data Quality Process End', @CP2_build_ID

commit

end


go

grant execute on data_quality_data_processing_month to vespa_group_low_security, sk_prodreg, buxceys, kinnairt

