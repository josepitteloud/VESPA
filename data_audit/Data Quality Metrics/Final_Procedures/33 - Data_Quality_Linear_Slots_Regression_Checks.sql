-------------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------------------------




-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Data Quality Linear Regression Checks
**
** This is the sql that contains both the logic to derive the necessary records and also the 
** metrics sql for each of the metrics involved and places tham in a repository table for storage
** purposes
**
**
** Refer also to:
**
**
**
**
**
** Things done:
**
**
******************************************************************************/

if object_id('data_quality_linear_slot_regression_checks') is not null drop procedure data_quality_linear_slot_regression_checks;
commit;

go

create procedure data_quality_linear_slot_regression_checks
@date_type varchar(8)
,@analysis_date_start date
,@analysis_date_end date
as
begin

declare @analysis_date date
declare @analysis_min_date int
declare @analysis_max_date int
declare @analysis_date_current date
declare @data_count int
declare @data_days_analyze int
declare @sql_stmt varchar(8000)
declare @run_stmt varchar(200)
declare @slot_date int
declare @slot_cnt int
declare @slot_cnt_vol int
declare @hh_cnt int
declare @seg_cnt int
declare @camp_cnt int
declare @min_broadcast_date_hour int
declare @max_broadcast_date_hour int
declare @fact_tbl_name varchar(50)
declare @var_sql varchar(8000)
declare @fact_tbl_name_additional varchar(50)
declare @build_date date
declare @RunID     bigint 
declare @date_column varchar(25)


/*
declare @date_type varchar(8)
declare @analysis_date_start date
declare @analysis_date_end date

set @date_type = 'LOCAL'
set @analysis_date_start = '2014-05-07'
set @analysis_date_end = '2014-05-09'
*/


set @RunID = dq_respository_Seq.nextval

set @build_date = today()

create table #tmp_date_hours
(min_broadcast_date_hour int,
max_broadcast_date_hour int)

if @date_type = 'LOCAL' 
begin

set @date_column = 'local_day_date'

insert into #tmp_date_hours
select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
from 
sk_prod.viq_date
where local_day_date between @analysis_date_start and @analysis_date_end

end


if @date_type = 'BROADCAST' 

begin

set @date_column = 'broadcast_day_date'

insert into #tmp_date_hours
select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
from 
sk_prod.viq_date
where broadcast_day_date between @analysis_date_start and @analysis_date_end

end

set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)

set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)

SELECT COUNT(1) total, 1 CNT INTO #tmp_cnt_inst_hist
from sk_prod.SLOT_DATA_HISTORY
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour and @max_broadcast_date_hour

SELECT COUNT(1) total_vol, 1 CNT INTO #tmp_cnt_inst
from sk_prod.SLOT_DATA
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour and @max_broadcast_date_hour

select distinct pk_datehour_dim, local_day_date, broadcast_day_date dates_analysed into #analysis_event_dates_final
from sk_prod.viq_date
where pk_datehour_dim between @min_broadcast_date_hour and @max_broadcast_date_hour

set @analysis_min_date = (select min(pk_datehour_dim/100) from sk_prod.viq_date where local_day_date = (select min(local_day_date) from #analysis_event_dates_final))
set @analysis_max_date = (select max(pk_datehour_dim/100) from sk_prod.viq_date where local_day_date = (select max(local_day_date) from #analysis_event_dates_final))

set @slot_cnt = (select total from #tmp_cnt_inst_hist)
set @slot_cnt_vol = (select total_vol from #tmp_cnt_inst)

if @slot_cnt > 0 and @slot_cnt_vol > 0
begin
set @fact_tbl_name = 'sk_prod.SLOT_DATA_HISTORY'
set @fact_tbl_name_additional = 'sk_prod.SLOT_DATA'
end

if @slot_cnt > 0 and @slot_cnt_vol = 0
begin
set @fact_tbl_name = 'sk_prod.SLOT_DATA_HISTORY'
END

if @slot_cnt = 0 and @slot_cnt_vol > 0
begin
set @fact_tbl_name = 'sk_prod.SLOT_DATA'
END

--EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart running on'||@fact_tbl_name||'',@data_count

---lets see how many days worth of data we are running for

set @data_count = null

set @data_count = (select count(1) from #analysis_event_dates_final)

--EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart running for count of days',@data_count

--ok time to start with the 1st date we have on our list of dates to examine--

--EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart Process Start for Dates '||cast (@min_broadcast_date_hour as varchar(20)) || 'to'||cast (@max_broadcast_date_hour as varchar(20))

----------------------------------D01 - Data collection-----------------------------------------------

--begin

--set @slot_date = cast(replace(@analysis_date_current,'-','') as int)

create table #linear_slots
(slot_key bigint
, viewed_start_date_key int
, broadcast_start_date_key int
, viewed_start_time_key int
, slot_instance_key bigint
, household_key bigint
, time_shift_key int
, broadcast_duration decimal
, viewed_duration decimal
, impacts smallint
, scaling_factor double
, local_day_date date)

if @fact_tbl_name is not null AND @fact_tbl_name_additional is null
begin
set @var_sql = 
'insert into #linear_slots
(slot_key 
, viewed_start_date_key 
, broadcast_start_date_key 
, viewed_start_time_key 
, slot_instance_key 
, household_key 
, time_shift_key 
, broadcast_duration 
, viewed_duration 
, impacts 
, scaling_factor 
, local_day_date)
select slot_key 
, viewed_start_date_key 
, broadcast_start_date_key 
, viewed_start_time_key 
, slot_instance_key 
, household_key 
, time_shift_key 
, broadcast_duration 
, viewed_duration 
, impacts 
, scaling_factor 
, b.'||@date_column||' 
from '||@fact_tbl_name||' a, #analysis_event_dates_final b
 WHERE a.BROADCAST_START_DATE_KEY  = b.pk_datehour_dim
and a.BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour'

execute (@var_sql)

end

if @fact_tbl_name is not null AND @fact_tbl_name_additional is not null
begin
set @var_sql = 
'insert into #linear_slots
(slot_key 
, viewed_start_date_key 
, broadcast_start_date_key 
, viewed_start_time_key 
, slot_instance_key 
, household_key 
, time_shift_key 
, broadcast_duration 
, viewed_duration 
, impacts 
, scaling_factor 
, local_day_date)
select slot_key 
, viewed_start_date_key 
, broadcast_start_date_key 
, viewed_start_time_key 
, slot_instance_key 
, household_key 
, time_shift_key 
, broadcast_duration 
, viewed_duration 
, impacts 
, scaling_factor 
, b.'||@date_column||' 
from '||@fact_tbl_name||' a, #analysis_event_dates_final b
 WHERE a.BROADCAST_START_DATE_KEY  = b.pk_datehour_dim
and a.BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour'

execute (@var_sql)

set @var_sql = 
'insert into #linear_slots
(slot_key 
, viewed_start_date_key 
, broadcast_start_date_key 
, viewed_start_time_key 
, slot_instance_key 
, household_key 
, time_shift_key 
, broadcast_duration 
, viewed_duration 
, impacts 
, scaling_factor 
, local_day_date)
select slot_key 
, viewed_start_date_key 
, broadcast_start_date_key 
, viewed_start_time_key 
, slot_instance_key 
, household_key 
, time_shift_key 
, broadcast_duration 
, viewed_duration 
, impacts 
, scaling_factor 
, b.'||@date_column||' 
from '||@fact_tbl_name_additional||' a, #analysis_event_dates_final b
 WHERE a.BROADCAST_START_DATE_KEY  = b.pk_datehour_dim
and a.BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour'

execute (@var_sql)

end

create table #linear_regression_metric_results
(REPORT_TYPE    varchar(50),
metric_id varchar(10),
report_date     date,
metric_result   decimal (16,2))

--METRIC I1

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I1',local_day_date,
(1.0 * (sum(case when slot_instance.slot_instance_key is not null then 1 else 0 end))/count(1) * 100) metric_result
from #linear_slots linear_slots 
left outer join
sk_prod.slot_instance slot_instance
on
linear_slots.slot_instance_key = slot_instance.slot_instance_key
group by local_day_date


--METRIC I1 DONE

--METRIC I2

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I2',local_day_date,
(1.0 * (sum(case when slot.slot_key is not null then 1 else 0 end))/count(1) * 100) metric_result
from #linear_slots linear_slots 
left outer join
(select slot_key from sk_prod.SLOT) slot
on
linear_slots.slot_key = slot.slot_key
group by local_day_date


--METRIC I2 DONE

--METRIC I2
/*
insert into #LINEAR_regression_metric_results
select 'LINEAR', 'I2',local_day_date,sum(case when slot_reference.slot_reference_key is null then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
LINEAR_slots.slot_reference_key = slot_reference.slot_reference_key
group by local_day_date
*/

--METRIC I2 DONE

--METRIC I3 NOT APPLICABLE AS NOT IN OLIVE

--DK_PRECEDING_PROGRAMME_INSTANCE_DIM

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I3',local_day_date,
(1.0 * (sum(case when prev_prog.prev_prog_schedule_key is not null then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.programme_instance_id is not null then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance prev_prog
on
linear_slots.slot_instance_key = prev_prog.slot_instance_key
left outer join
sk_prod.viq_programme_schedule prog_sched
on
prev_prog.prev_prog_schedule_key = prog_sched.programme_instance_id
group by local_day_date


--METRIC I3 DONE

--METRIC I4 

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I4',local_day_date,
(1.0 * (sum(case when prev_prog.previous_programme_key is not null then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.pk_programme_dim is not null then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance prev_prog
on
linear_slots.slot_instance_key = prev_prog.slot_instance_key
left outer join
sk_prod.viq_programme prog_sched
on
prev_prog.previous_programme_key = prog_sched.pk_programme_dim
group by local_day_date


--METRIC I4 DONE

--METRIC I5 NOT APPLICABLE AS NOT IN OLIVE


--DK_succeeding_PROGRAMME_INSTANCE_DIM

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I5',local_day_date,
(1.0 * (sum(case when next_prog.next_prog_schedule_key is not null then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.programme_instance_id is not null then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance next_prog
on
linear_slots.slot_instance_key = next_prog.slot_instance_key
left outer join
sk_prod.viq_programme_schedule prog_sched
on
next_prog.next_prog_schedule_key = prog_sched.programme_instance_id
group by local_day_date

--METRIC I5 DONE

--METRIC I6 

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I6',local_day_date,
(1.0 * (sum(case when next_prog.next_programme_key is not null then 1 when next_prog.next_programme_key < 0 then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.pk_programme_dim is not null then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance next_prog
on
linear_slots.slot_instance_key = next_prog.slot_instance_key
left outer join
sk_prod.viq_programme prog_sched
on
next_prog.next_programme_key = prog_sched.pk_programme_dim
group by local_day_date

--METRIC I6 DONE

--METRIC I7 

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I7',local_day_date,
(1.0 * (sum(case when broadcast_channel.channel_key is not null then 1 else 0 end))/count(1) * 100) metric_result
from #linear_slots LINEAR_slots 
left outer join
sk_Prod.slot_instance broadcast_channel
on LINEAR_slots.slot_instance_key = broadcast_channel.slot_instance_key
group by local_day_date

--I7 METRIC DONE


--METRIC I8-1 

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I8-1',local_day_date,
(1.0 * (sum(case when prev_prog.previous_programme_key is null then 1 when prev_prog.previous_programme_key < 0 then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.pk_programme_dim is null then 1 when prog_sched.pk_programme_dim < 0 then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance prev_prog
on
linear_slots.slot_instance_key = prev_prog.slot_instance_key
left outer join
sk_prod.viq_programme prog_sched
on
prev_prog.previous_programme_key = prog_sched.pk_programme_dim
group by local_day_date

--METRIC I8-1 DONE

--METRIC I8-2 

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I8-2',local_day_date,
(1.0 * (sum(case when prev_prog.prev_prog_schedule_key is null then 1 when prev_prog.prev_prog_schedule_key < 0 then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.programme_instance_id is null then 1 when prog_sched.programme_instance_id < 0 then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance prev_prog
on
linear_slots.slot_instance_key = prev_prog.slot_instance_key
left outer join
sk_prod.viq_programme_schedule prog_sched
on
prev_prog.prev_prog_schedule_key = prog_sched.programme_instance_id
group by local_day_date


--METRIC I8-2 DONE

--METRIC I8-3 

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I8-3',local_day_date,
(1.0 * (sum(case when next_prog.next_programme_key is null then 1 when next_prog.next_programme_key < 0 then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.pk_programme_dim is null then 1 when prog_sched.pk_programme_dim < 0 then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance next_prog
on
linear_slots.slot_instance_key = next_prog.slot_instance_key
left outer join
sk_prod.viq_programme prog_sched
on
next_prog.next_programme_key = prog_sched.pk_programme_dim
group by local_day_date


--METRIC I8-3 DONE

--METRIC I8-4 
insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I8-4',local_day_date,
(1.0 * (sum(case when succ_prog.next_prog_schedule_key is null then 1 when succ_prog.next_prog_schedule_key < 0 then 1 else 0 end))/count(1) * 100) metric_result
--,(1.0 * (sum(case when prog_sched.programme_instance_id is null then 1 when prog_sched.programme_instance_id < 0 then 1 else 0 end))/count(1) * 100) metric_result2
from #linear_slots linear_slots 
left outer join
sk_prod.SLOT_instance succ_prog
on
linear_slots.slot_instance_key = succ_prog.slot_instance_key
left outer join
sk_prod.viq_programme_schedule prog_sched
on
succ_prog.next_prog_schedule_key = prog_sched.programme_instance_id
group by local_day_date


--METRIC I8-4 DONE

--METRIC I9
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I9',local_day_date,sum(case when VIEWED_DURATION IS NULL THEN 1
                                                when viewed_duration < 1 then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--I9 METRIC DONE


--METRIC I10 not in Olive

--change to zero
/*
insert into #LINEAR_regression_metric_results
select 'LINEAR', 'I10',local_day_date,
(1.0 * (count(distinct fact_viewing_slot_instance_key)/count(1) * 100)) metric_result
--count(1) - count(distinct fact_viewing_slot_instance_key) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date
*/
--I10 METRIC DONE

--METRIC I11 NOT APPLICABLE AS NOT IN OLIVE

select distinct 
--linear_slot
linear_slots.local_day_date,
linear_slots.slot_instance_key,
--slot_programme_key
slot_inst.prev_prog_schedule_key, 
--slot date and time
cast(date_start.utc_day_date ||' '||time_start.utc_time_minute as datetime) slot_start_date_time,
cast(date_end.utc_day_date ||' '||time_end.utc_time_minute as datetime) slot_end_date_time,
--programme start and end time
cast(prog_date_start.utc_day_date ||' '||prog_time_start.utc_time_minute as datetime) prog_start_date_time,
cast(prog_date_end.utc_day_date ||' '||prog_time_end.utc_time_minute as datetime) prog_end_date_time
into #I11_slots
from #linear_slots linear_slots
inner join
sk_prod.slot_instance slot_inst
on
(linear_slots.slot_instance_key = slot_inst.slot_instance_key)
inner join
sk_prod.viq_date date_start
on
(slot_inst.slot_start_date_key = date_start.pk_datehour_dim)
inner join
sk_prod.viq_time time_start
on
(slot_inst.slot_start_time_key = time_start.pk_time_dim)
inner join
sk_prod.viq_date date_end
on
(slot_inst.slot_end_date_key = date_end.pk_datehour_dim)
inner join
sk_prod.viq_time time_end
on
(slot_inst.slot_end_time_key = time_end.pk_time_dim)
inner join
sk_prod.viq_programme_schedule prog_sched
on
(slot_inst.prev_prog_schedule_key = prog_sched.programme_instance_id)
inner join
sk_prod.viq_date prog_date_start
on
(prog_sched.dk_start_datehour = prog_date_start.pk_datehour_dim)
inner join
sk_prod.viq_time prog_time_start
on
(prog_sched.dk_start_time = prog_time_start.pk_time_dim)
inner join
sk_prod.viq_date prog_date_end
on
(prog_sched.dk_end_datehour = prog_date_end.pk_datehour_dim)
inner join
sk_prod.viq_time prog_time_end
on
(prog_sched.dk_end_time = prog_time_end.pk_time_dim)
where prev_prog_schedule_key = next_prog_schedule_key

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I11',local_day_date, 
(1.0 * (sum(case when slot_start_date_time < prog_start_date_time then 1 
when slot_end_date_time > prog_end_date_time then 1 
when slot_start_date_time > prog_end_date_time then 1 
when slot_end_date_time < prog_start_date_time then 1 else 0 end)/count(1) * 100)) metric_result
from #I11_slots
group by local_day_date

--METRIC I11 DONE

--METRIC I12 

select distinct 
--linear_slot
linear_slots.local_day_date,
linear_slots.slot_instance_key,
--slot_prev_programme_key
slot_inst.prev_prog_schedule_key, 
--slot_next_programme_key
slot_inst.next_prog_schedule_key, 
--slot date and time
cast(date_start.utc_day_date ||' '||time_start.utc_time_minute as datetime) slot_start_date_time,
cast(date_end.utc_day_date ||' '||time_end.utc_time_minute as datetime) slot_end_date_time,
--programme start and end time
cast(prev_prog_date_start.utc_day_date ||' '||prev_prog_time_start.utc_time_minute as datetime) prev_prog_start_date_time,
cast(prev_prog_date_end.utc_day_date ||' '||prev_prog_time_end.utc_time_minute as datetime) prev_prog_end_date_time,
cast(next_prog_date_start.utc_day_date ||' '||next_prog_time_start.utc_time_minute as datetime) next_prog_start_date_time,
cast(next_prog_date_end.utc_day_date ||' '||next_prog_time_end.utc_time_minute as datetime) next_prog_end_date_time
into #I12_slots
from #linear_slots linear_slots
inner join
sk_prod.slot_instance slot_inst
on
(linear_slots.slot_instance_key = slot_inst.slot_instance_key)
inner join
sk_prod.viq_date date_start
on
(slot_inst.slot_start_date_key = date_start.pk_datehour_dim)
inner join
sk_prod.viq_time time_start
on
(slot_inst.slot_start_time_key = time_start.pk_time_dim)
inner join
sk_prod.viq_date date_end
on
(slot_inst.slot_end_date_key = date_end.pk_datehour_dim)
inner join
sk_prod.viq_time time_end
on
(slot_inst.slot_end_time_key = time_end.pk_time_dim)
--get details for previous programme
left join
sk_prod.viq_programme_schedule prev_prog_sched_start
on
(slot_inst.prev_prog_schedule_key = prev_prog_sched_start.programme_instance_id)
inner join
sk_prod.viq_date prev_prog_date_start
on
(prev_prog_sched_start.dk_start_datehour = prev_prog_date_start.pk_datehour_dim)
inner join
sk_prod.viq_time prev_prog_time_start
on
(prev_prog_sched_start.dk_start_time = prev_prog_time_start.pk_time_dim)
inner join
sk_prod.viq_date prev_prog_date_end
on
(prev_prog_sched_start.dk_end_datehour = prev_prog_date_end.pk_datehour_dim)
inner join
sk_prod.viq_time prev_prog_time_end
on
(prev_prog_sched_start.dk_end_time = prev_prog_time_end.pk_time_dim)
--get details for next programme
left join
sk_prod.viq_programme_schedule next_prog_sched
on
(slot_inst.next_prog_schedule_key = next_prog_sched.programme_instance_id)
inner join
sk_prod.viq_date next_prog_date_start
on
(next_prog_sched.dk_start_datehour = next_prog_date_start.pk_datehour_dim)
inner join
sk_prod.viq_time next_prog_time_start
on
(next_prog_sched.dk_start_time = next_prog_time_start.pk_time_dim)
inner join
sk_prod.viq_date next_prog_date_end
on
(next_prog_sched.dk_end_datehour = next_prog_date_end.pk_datehour_dim)
inner join
sk_prod.viq_time next_prog_time_end
on
(next_prog_sched.dk_end_time = next_prog_time_end.pk_time_dim)
where prev_prog_schedule_key != next_prog_schedule_key

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I12',local_day_date, 
(1.0 * (sum(case when slot_start_date_time < prev_prog_start_date_time then 1 
when slot_end_date_time > next_prog_end_date_time then 1 
when slot_start_date_time > prev_prog_end_date_time then 1 
when slot_end_date_time < next_prog_start_date_time then 1 else 0 end)/count(1) * 100)) metric_result
from #I12_slots
group by local_day_date

--METRIC I12 DONE

--METRIC I13 

select distinct 
--linear_slot
linear_slots.local_day_date,
linear_slots.slot_instance_key,
--slot_prev_programme_key
slot_inst.prev_prog_schedule_key, 
--slot_next_programme_key
slot_inst.next_prog_schedule_key, 
--slot date and time
cast(date_start.utc_day_date ||' '||time_start.utc_time_minute as datetime) slot_start_date_time,
cast(date_end.utc_day_date ||' '||time_end.utc_time_minute as datetime) slot_end_date_time,
--programme start and end time
cast(prev_prog_date_start.utc_day_date ||' '||prev_prog_time_start.utc_time_minute as datetime) prev_prog_start_date_time,
cast(prev_prog_date_end.utc_day_date ||' '||prev_prog_time_end.utc_time_minute as datetime) prev_prog_end_date_time,
cast(next_prog_date_start.utc_day_date ||' '||next_prog_time_start.utc_time_minute as datetime) next_prog_start_date_time,
cast(next_prog_date_end.utc_day_date ||' '||next_prog_time_end.utc_time_minute as datetime) next_prog_end_date_time
into #I1314_slots
from #linear_slots linear_slots
inner join
sk_prod.slot_instance slot_inst
on
(linear_slots.slot_instance_key = slot_inst.slot_instance_key)
inner join
sk_prod.viq_date date_start
on
(slot_inst.slot_start_date_key = date_start.pk_datehour_dim)
inner join
sk_prod.viq_time time_start
on
(slot_inst.slot_start_time_key = time_start.pk_time_dim)
inner join
sk_prod.viq_date date_end
on
(slot_inst.slot_end_date_key = date_end.pk_datehour_dim)
inner join
sk_prod.viq_time time_end
on
(slot_inst.slot_end_time_key = time_end.pk_time_dim)
--get details for previous programme
left join
sk_prod.viq_programme_schedule prev_prog_sched_start
on
(slot_inst.prev_prog_schedule_key = prev_prog_sched_start.programme_instance_id)
inner join
sk_prod.viq_date prev_prog_date_start
on
(prev_prog_sched_start.dk_start_datehour = prev_prog_date_start.pk_datehour_dim)
inner join
sk_prod.viq_time prev_prog_time_start
on
(prev_prog_sched_start.dk_start_time = prev_prog_time_start.pk_time_dim)
inner join
sk_prod.viq_date prev_prog_date_end
on
(prev_prog_sched_start.dk_end_datehour = prev_prog_date_end.pk_datehour_dim)
inner join
sk_prod.viq_time prev_prog_time_end
on
(prev_prog_sched_start.dk_end_time = prev_prog_time_end.pk_time_dim)
--get details for next programme
left join
sk_prod.viq_programme_schedule next_prog_sched
on
(slot_inst.next_prog_schedule_key = next_prog_sched.programme_instance_id)
inner join
sk_prod.viq_date next_prog_date_start
on
(next_prog_sched.dk_start_datehour = next_prog_date_start.pk_datehour_dim)
inner join
sk_prod.viq_time next_prog_time_start
on
(next_prog_sched.dk_start_time = next_prog_time_start.pk_time_dim)
inner join
sk_prod.viq_date next_prog_date_end
on
(next_prog_sched.dk_end_datehour = next_prog_date_end.pk_datehour_dim)
inner join
sk_prod.viq_time next_prog_time_end
on
(next_prog_sched.dk_end_time = next_prog_time_end.pk_time_dim)

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I13',local_day_date, 
(1.0 * (sum(case when prev_prog_schedule_key = next_prog_schedule_key and (prev_prog_start_date_time != next_prog_start_date_time) then 1 
                when prev_prog_schedule_key = next_prog_schedule_key and (prev_prog_end_date_time != next_prog_end_date_time) then 1 else 0 end)/count(1) * 100)) metric_result
from #I1314_slots
group by local_day_date

--METRIC I13 DONE

--METRIC I14 

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'I14',local_day_date, 
(1.0 * (sum(case when prev_prog_schedule_key != next_prog_schedule_key and (prev_prog_start_date_time = next_prog_start_date_time) then 1 
                when prev_prog_schedule_key != next_prog_schedule_key and (prev_prog_end_date_time = next_prog_end_date_time) then 1 else 0 end)/count(1) * 100)) metric_result
from #I1314_slots
group by local_day_date

--METRIC I14 DONE

--METRIC C1 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C1 DONE

--METRIC C2 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C2 DONE

--METRIC C3 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C3 DONE

--METRIC C4 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C4 DONE

--METRIC C5-1 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C5-1 DONE

--METRIC C5-2 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C5-2 DONE

--METRIC C6-1 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C6-1 DONE

--METRIC C6-2 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C6-2 DONE

--METRIC C7-1 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C7-1 DONE

--METRIC C7-2
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'C7-2',local_day_date,sum(viewed_duration) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--METRIC C7-2 DONE

--METRIC C8-2
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'C8-2',local_day_date,
(1.0 * (sum(viewed_duration)/60 )/ SUM(DISTINCT HOUSEHOLD_KEY) * 100) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--METRIC C8-2

--METRIC C9 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C9 

--METRIC C10 NOT APPLICABLE AS NOT IN OLIVE

--METRIC C10 

--METRIC M2 

--METRIC M2 NOT APPLICABLE AS NOT IN OLIVE

--METRIC M3

--METRIC M3 NOT APPLICABLE AS NOT IN OLIVE

--METRIC M4-1

--METRIC M4-1 NOT APPLICABLE AS NOT IN OLIVE

--METRIC M4-2

--METRIC M4-2 NOT APPLICABLE AS NOT IN OLIVE

--METRIC M5

--METRIC M5 NOT APPLICABLE AS NOT IN OLIVE

--METRIC M7-1

--METRIC M7-2 NOT APPLICABLE AS NOT IN OLIVE

--S1-1	Proportion of events with weight assigned
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'S1-1',local_day_date,
round((1.0 * (SUM(CASE WHEN scaling_factor > 0 THEN 1 ELSE 0 END))/count(1)),2) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--S1-2	Number of events with weight assigned
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'S1-2',local_day_date,
SUM(CASE WHEN scaling_factor > 0 THEN 1 ELSE 0 END) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--S1-2	done

--S2	Check no difference between weights in fact and source
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'S2',local_day_date,sum(case when scaling_factor != calculated_scaling_weight then 1 else 0 end) metric_result 
from 
(select distinct household_key, scaling_factor, local_day_date
from #linear_slots LINEAR_slots ) a,
(select adjusted_event_start_date_vespa, household_key, calculated_scaling_weight from sk_prod.viq_viewing_data_scaling) b
where a.household_key = b.household_key
and a.local_day_date = b.adjusted_event_start_date_vespa
group by a.local_day_date

--S2	Done

--S3	Check all records matchign scaling source have been attributed - cannot be done in Olive

--S3    Done

--S4 Check we have a weight asigned where the flag = 1 - cannot be done in Olive

--S4 Done

--S5	Check the flag = 1 when weight is asigned - cannot be done in Olive

--S5 Done

--CH1-1	Proportion of channels matching with the dimension
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'CH1-1',local_day_date,
(1.0 * sum(case when broadcast_channel.pk_channel_dim is not null then 1 else 0 end)/count(slot_inst.channel_key)) metric_result
from #linear_slots LINEAR_slots 
inner join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
left outer join
(select pk_channel_dim, service_key
from sk_prod.viq_channel) broadcast_channel
on slot_inst.channel_key = broadcast_channel.pk_channel_dim
group by local_day_date

--CH1-1	Done

--CH1-2	Number of channels matching with the dimension
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'CH1-2',local_day_date,
count(distinct case when broadcast_channel.pk_channel_dim is not null then broadcast_channel.pk_channel_dim else null end) metric_result
from #linear_slots LINEAR_slots 
inner join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
left outer join
(select pk_channel_dim, service_key
from sk_prod.viq_channel) broadcast_channel
on slot_inst.channel_key = broadcast_channel.pk_channel_dim
group by local_day_date


--CH1-2	Done

--CH2	Checking all channel names are in place (null or -1 count)
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'CH2',local_day_date,
sum(distinct case when broadcast_channel.channel_name is null then 1 
when broadcast_channel.channel_name = '(blank)' then 1 
when broadcast_channel.channel_name = '(unknown)' then 1 
when LOWER(broadcast_channel.channel_name) = 'unknown' then 1 
when broadcast_channel.channel_name = '-1' then 1 
when broadcast_channel.channel_name = '-99' then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
inner join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
left outer join
(select pk_channel_dim, service_key, channel_name
from sk_prod.viq_channel) broadcast_channel
on slot_inst.channel_key = broadcast_channel.pk_channel_dim
group by local_day_date

--CH2	Done

--CH3	Checking all Channel Genre are in place (null or -1 count)
--channel genre not located in viq_channel table
/*
insert into #LINEAR_regression_metric_results
select 'LINEAR', 'CH3',local_day_date,
sum(distinct case when broadcast_channel.channel_genre is null then 1 
when broadcast_channel.channel_genre = 'unknown' then 1 
when broadcast_channel.channel_genre = 'N/a' then 1 
when broadcast_channel.channel_genre = '(unknown)' then 1 
when broadcast_channel.channel_genre = '-1' then 1 
when broadcast_channel.channel_genre = '-99' then 1 else 0 end) metric_result
from linear_slots LINEAR_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on LINEAR_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date
*/
--CH3	Done

--CH4	Checking all service keys are in place (null or -1 count)

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'CH4',local_day_date,
sum(distinct case when broadcast_channel.service_key  is null then 1 
when broadcast_channel.service_key < 0 then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
inner join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
left outer join
(select pk_channel_dim, service_key, channel_name
from sk_prod.viq_channel) broadcast_channel
on slot_inst.channel_key = broadcast_channel.pk_channel_dim
group by local_day_date

--CH4 DONE

--D1	Check how many records have we got per day
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'D1',local_day_date,
COUNT(1) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM - Not valid for Olive

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'D2',local_day_date,
sum(distinct case when slot_inst.prev_prog_schedule_key is null then 1 
when slot_inst.next_prog_schedule_key is null then 1 
when slot_inst.prev_prog_schedule_key < 0 then 1 
when slot_inst.next_prog_schedule_key < 0 then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
left outer join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
group by local_day_date

--D2 Done

--D3	Check proportion of records without a DK_PROGRAMME_DIM -- Not valid for Olive

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'D2',local_day_date,
sum(distinct case when slot_inst.previous_programme_key is null then 1 
when slot_inst.next_programme_key is null then 1 
when slot_inst.previous_programme_key < 0 then 1 
when slot_inst.previous_programme_key < 0 then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
left outer join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
group by local_day_date

--D3 Done

--D4	Check proportion of records without a DK_CHANNEL_DIM
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'D4',local_day_date,
sum(case when broadcast_channel.pk_channel_dim is null then 1 else 0 end) metric_result
from #linear_slots LINEAR_slots 
inner join
sk_prod.slot_instance slot_inst
on
linear_slots.slot_instance_key = slot_inst.slot_instance_key
left outer join
(select pk_channel_dim, service_key, channel_name
from sk_prod.viq_channel) broadcast_channel
on slot_inst.channel_key = broadcast_channel.pk_channel_dim
group by local_day_date
--D4 Done

--D4	D5	Check number of accounts per day
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'LINEAR', 'D5',local_day_date,
count(distinct household_key) metric_result
from #linear_slots LINEAR_slots 
group by local_day_date

--D5 Done

--D6	Check number of boxes per day - Not valid for Olive

--D6 Done

--insert results into regression repository table

insert into data_quality_regression_reports_repository
(run_id,report_type ,metric_id ,metric_result ,rag_status, report_date,metric_threshold,metric_tolerance_amber,metric_tolerance_red)
SELECT @RunID, b.report_type, b.metric_id, a.metric_result, 
metric_benchmark_check (a.metric_result, b.metric_threshold, b.metric_tolerance_amber, b.metric_tolerance_red) RAG_STATUS ,
a.report_date, b.metric_threshold,b.metric_tolerance_amber,b.metric_tolerance_red
FROM #linear_regression_metric_results a
right outer join
(select * from data_quality_regression_thresholds 
where report_type = 'LINEAR' )b
on (UPPER(a.report_type) = UPPER(b.report_type)
and a.metric_id = b.metric_id)

commit

end

go

grant execute on data_quality_linear_slot_regression_checks to vespa_group_low_security, sk_prodreg, buxceys, kinnairt