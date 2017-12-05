
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


if object_id('data_quality_slot_day_processing') is not null drop procedure data_quality_slot_day_processing;
commit;

go


create procedure data_quality_slot_day_processing
@run_type varchar(20)
,@CP2_build_ID     bigint = NULL
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
declare @slot_cnt int
declare @hh_cnt int
declare @seg_cnt int
declare @camp_cnt int
declare @min_broadcast_date_hour int
declare @max_broadcast_date_hour int
declare @fact_tbl_name varchar(8000)
declare @var_sql varchar(8000)

select @build_date event_date into #analysis_event_dates_final 

set @analysis_date_current = @build_date

set @analysis_max_date  = (select max(event_date) from #analysis_event_dates_final)

select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
into #tmp_date_hours
from 
sk_uat.viq_date
where broadcast_day_date = @analysis_date_current

set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)

set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)

set @slot_date = cast(replace(@analysis_date_current,'-','') as int)


SELECT COUNT(1) total, 1 CNT INTO #tmp_cnt_inst
from sk_uat.SLOT_DATA_HISTORY
where BROADCAST_START_DATE_KEY/100 = @slot_date

select total @slot_cnt from #tmp_cnt_inst

if @slot_cnt > 0 
begin
set @fact_tbl_name = 'sk_uat.SLOT_DATA_HISTORY'
end

if @fact_tbl_name is null 
begin
set @fact_tbl_name = 'sk_uat.SLOT_DATA'
END

EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart running on'||@fact_tbl_name||'',@data_count

---lets see how many days worth of data we are running for

delete from #analysis_event_dates_final where event_date = today()

set @data_count = null

set @data_count = (select count(1) from #analysis_event_dates_final)


set @var_sql = 
'select SLOT_DATA_KEY, VIEWED_START_DATE_KEY, IMPACTS, RECORD_DATE, HOUSEHOLD_KEY, 
scaling_factor,slot_key,viewed_duration,viewed_start_time_key,slot_instance_key,time_shift_key,broadcast_start_date_key
into #linear_slots
from '||@fact_tbl_name||' WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour '

execute (@var_sql)


EXECUTE logger_add_event @RunID , 3,'Data Quality Linear Slot running on'||@fact_tbl_name||'',@data_count

---lets see how many days worth of data we are running for

delete from #analysis_event_dates_final where event_date = today()

set @data_count = null

set @data_count = (select count(1) from #analysis_event_dates_final)

EXECUTE logger_add_event @RunID , 3,'Data Quality Linear Slot running for count of days',@data_count

--ok time to start with the 1st date we have on our list of dates to examine--

EXECUTE logger_add_event @RunID , 3,'Data Quality Linear Slot Process Start for Date '||cast (@analysis_date_current as varchar(20))

----------------------------------D01 - Data collection-----------------------------------------------

--while @analysis_date_current <= @analysis_max_date 
begin

set @slot_date = cast(replace(@analysis_date_current,'-','') as int)

truncate table DATA_QUALITY_SLOT_DATA_AUDIT

commit

insert into DATA_QUALITY_SLOT_DATA_AUDIT
(SLOT_DATA_KEY, VIEWED_START_DATE_KEY, IMPACTS, RECORD_DATE, HOUSEHOLD_KEY, IMPACT_DAY,
slot_instance_key, 
channel_key, 
slot_start_date_key, 
slot_start_time_key, 
slot_end_date_key, 
slot_end_time_key,
previous_programme_key,
next_programme_key,
prev_prog_schedule_key, 
next_prog_schedule_key, 
prev_broadcast_start_date, 
next_broadcast_start_date, 
prev_broadcast_start_time, 
next_broadcast_start_time,
slot_start_date, 
slot_end_date, 
slot_start_time, 
slot_end_time,
scaling_factor,
prev_broadcast_end_time, 
next_broadcast_end_time,
prev_broadcast_end_date, 
next_broadcast_end_date,
slot_key,
viewed_duration,
viewed_start_time_key,
time_shift_key,
advertiser_code, 
advertiser_name,
buyer_code, 
buyer_name, 
buyer_source, 
buyer_type,
barb_sales_house_id, 
media_sales_house_name, 
media_sales_house_short_name,
break_position, 
spot_position_in_break,
spot_type)
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
slot.time_shift_key,
slot_inst_data.advertiser_code, 
slot_inst_data.advertiser_name,
slot_inst_data.buyer_code, 
slot_inst_data.buyer_name, 
slot_inst_data.buyer_source, 
slot_inst_data.buyer_type,
slot_inst_data.barb_sales_house_id, 
slot_inst_data.media_sales_house_name, 
slot_inst_data.media_sales_house_short_name,
slot_inst_data.break_position, 
slot_inst_data.spot_position_in_break,
slot_inst_data.spot_type
FROM #linear_slots slot
inner join
sk_uat.viq_date viq_date
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
slot_end_time.local_time_minute slot_end_time,
advert.advertiser_code, advert.advertiser_name,
buyer.buyer_code, buyer.buyer_source, buyer.buyer_name, buyer.buyer_type,
spos.spot_type, spos.break_position, spos.spot_position_in_break,
sales.barb_sales_house_id, sales.media_sales_house_name, sales.media_sales_house_short_name
from sk_uat.slot_instance slot_inst
left outer join
sk_uat.viq_programme_schedule viq_prev_prog_sched
on
slot_inst.prev_prog_schedule_key = viq_prev_prog_sched.programme_instance_id
left outer join
sk_uat.viq_programme_schedule viq_next_prog_sched
on
slot_inst.next_prog_schedule_key = viq_next_prog_sched.programme_instance_id
left outer join
sk_uat.viq_date prev_prog_start_date
on 
viq_prev_prog_sched.dk_start_datehour = prev_prog_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date prev_prog_end_date
on 
viq_prev_prog_sched.dk_end_datehour = prev_prog_end_date.pk_datehour_dim
left outer join
sk_uat.viq_date next_prog_start_date
on
viq_next_prog_sched.dk_start_datehour = next_prog_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date next_prog_end_date
on
viq_next_prog_sched.dk_end_datehour = next_prog_end_date.pk_datehour_dim
left outer join
sk_uat.viq_time prev_prog_start_time
on
viq_prev_prog_sched.dk_start_time = prev_prog_start_time.pk_time_dim
left outer join
sk_uat.viq_time prev_prog_end_time
on
viq_prev_prog_sched.dk_end_time = prev_prog_end_time.pk_time_dim
left outer join
sk_uat.viq_time next_prog_start_time
on
viq_next_prog_sched.dk_start_time = next_prog_start_time.pk_time_dim
left outer join
sk_uat.viq_time next_prog_end_time
on
viq_next_prog_sched.dk_end_time = next_prog_end_time.pk_time_dim
left outer join
sk_uat.viq_date slot_start_date
on
slot_inst.slot_start_date_key = slot_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date slot_end_date
on
slot_inst.slot_end_date_key = slot_end_date.pk_datehour_dim
left outer join
sk_uat.viq_time slot_start_time
on
slot_inst.slot_start_time_key = slot_start_time.pk_time_dim
left outer join
sk_uat.viq_time slot_end_time
on
slot_inst.slot_end_time_key = slot_end_time.pk_time_dim
left outer join
sk_uat.advertiser advert
on
slot_inst.advertiser_key = advert.advertiser_key
left outer join
sk_uat.buyer buyer
on
slot_inst.buyer_key = buyer.buyer_key
left outer join
sk_uat.spot_position spos
on
slot_inst.spot_position_key = spos.spot_position_key
left outer join
sk_uat.sales_house sales
on
slot_inst.sales_house_key = sales.sales_house_key) slot_inst_data
on
slot.slot_instance_key = slot_inst_data.slot_instance_key
where broadcast_start_date_key between @min_broadcast_date_hour and @max_broadcast_date_hour


set @slot_cnt = (select count(1) from  DATA_QUALITY_SLOT_DATA_AUDIT)

if @slot_cnt = 0
begin
EXECUTE logger_add_event @RunID ,2,'Refresh of Linear Slot Data Audit table not worked',@slot_cnt

end

if @slot_cnt > 0
begin
EXECUTE logger_add_event @RunID , 3,'Count of records in Linear Slot Data Audit table',@slot_cnt
end

------------------------------------LINEAR SLOT CAMPAIGNS TABLE FOR ANALYSIS--------------------------------------------

truncate table DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT


insert into DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT
select slot_inst.slot_instance_key, slot_inst.channel_key, slot_inst.slot_start_date_key, slot_inst.slot_start_time_key, 
slot_inst.slot_end_date_key, slot_inst.slot_end_time_key,
slot_inst.slot_instance_position, slot_inst.slot_instance_total_position, slot_inst.slot_type_position, slot_inst.slot_type_total_position, 
slot_inst.slot_key, slot_inst.buyer_key, slot_inst.advertiser_key, slot_inst.sales_house_key, slot_inst.spot_position_key,
slot.media_code, slot.slot_type, slot.slot_name, slot.slot_duration, slot.clearcast_commercial_no,slot.product_code, slot.product_name,
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
slot_end_time.local_time_minute slot_end_time,
advert.advertiser_code, advert.advertiser_name,
buyer.buyer_code, buyer.buyer_source, buyer.buyer_name, buyer.buyer_type,
spos.spot_type, spos.break_position, spos.spot_position_in_break,
sales.barb_sales_house_id, sales.media_sales_house_name, sales.media_sales_house_short_name
into DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT
from sk_uat.slot_instance slot_inst
left outer join sk_uat.slot slot
on
slot_inst.slot_key = slot.slot_key
left outer join
sk_uat.viq_programme_schedule viq_prev_prog_sched
on
slot_inst.prev_prog_schedule_key = viq_prev_prog_sched.programme_instance_id
left outer join
sk_uat.viq_programme_schedule viq_next_prog_sched
on
slot_inst.next_prog_schedule_key = viq_next_prog_sched.programme_instance_id
left outer join
sk_uat.viq_date prev_prog_start_date
on 
viq_prev_prog_sched.dk_start_datehour = prev_prog_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date prev_prog_end_date
on 
viq_prev_prog_sched.dk_end_datehour = prev_prog_end_date.pk_datehour_dim
left outer join
sk_uat.viq_date next_prog_start_date
on
viq_next_prog_sched.dk_start_datehour = next_prog_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date next_prog_end_date
on
viq_next_prog_sched.dk_end_datehour = next_prog_end_date.pk_datehour_dim
left outer join
sk_uat.viq_time prev_prog_start_time
on
viq_prev_prog_sched.dk_start_time = prev_prog_start_time.pk_time_dim
left outer join
sk_uat.viq_time prev_prog_end_time
on
viq_prev_prog_sched.dk_end_time = prev_prog_end_time.pk_time_dim
left outer join
sk_uat.viq_time next_prog_start_time
on
viq_next_prog_sched.dk_start_time = next_prog_start_time.pk_time_dim
left outer join
sk_uat.viq_time next_prog_end_time
on
viq_next_prog_sched.dk_end_time = next_prog_end_time.pk_time_dim
left outer join
sk_uat.viq_date slot_start_date
on
slot_inst.slot_start_date_key = slot_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date slot_end_date
on
slot_inst.slot_end_date_key = slot_end_date.pk_datehour_dim
left outer join
sk_uat.viq_time slot_start_time
on
slot_inst.slot_start_time_key = slot_start_time.pk_time_dim
left outer join
sk_uat.viq_time slot_end_time
on
slot_inst.slot_end_time_key = slot_end_time.pk_time_dim
left outer join
sk_uat.advertiser advert
on
slot_inst.advertiser_key = advert.advertiser_key
left outer join
sk_uat.buyer buyer
on
slot_inst.buyer_key = buyer.buyer_key
left outer join
sk_uat.spot_position spos
on
slot_inst.spot_position_key = spos.spot_position_key
left outer join
sk_uat.sales_house sales
on
slot_inst.sales_house_key = sales.sales_house_key
where slot_inst.slot_start_date_key between @min_broadcast_date_hour and @max_broadcast_date_hour


set @slot_cnt = 0

set @slot_cnt = (select count(1) from  DATA_QUALITY_LINEAR_SLOT_CAMPAIGN_DATA_AUDIT)

if @slot_cnt = 0
begin
EXECUTE logger_add_event @RunID ,2,'Refresh of Linear Campaign Data Audit table not worked',@slot_cnt

end

if @slot_cnt > 0
begin
EXECUTE logger_add_event @RunID , 3,'Count of records in Linear Campaign Data Audit table',@slot_cnt
end


----------------------------------C01 - Linear Slot Basic Checks--------------------------------------------------

execute Data_Quality_Linear_Slot_Basic_Checks 'LINEAR_SLOT_DATA_QUALITY',@analysis_date_current,@CP2_build_ID
   
	
------------------------------------ADSMART BASIC CHECKS END----------------------------------------------------	

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

EXECUTE logger_add_event @RunID , 3,'Count of Linear Slot metrics collected so far for '||cast (@analysis_date_current as varchar(20)),@data_count
	

execute Data_Quality_Linear_Slot_Metric_Run @analysis_date_current,@CP2_build_ID

	
set @analysis_date_current = @analysis_date_current + 1


EXECUTE logger_add_event @RunID , 3,'Data Quality Linear Slot running on'||@fact_tbl_name||'',@data_count


EXECUTE logger_add_event @RunID , 3,'Data Quality Linear Slot Process End for Date '||cast (@analysis_date_current as varchar(20))

end

commit

end


go

grant execute on data_quality_slot_day_processing to vespa_group_low_security, sk_uatreg
