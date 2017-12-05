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
** Project Vespa: Data Quality Programme Regression Checks
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


--exec data_quality_programme_regression_checks '2014-03-01','2014-03-07';


if object_id('data_quality_programme_regression_checks') is not null drop procedure data_quality_programme_regression_checks;
commit;

go

create procedure data_quality_programme_regression_checks
@analysis_date_start date
,@analysis_date_end date
as

begin

declare @analysis_date date
declare @analysis_min_date date
declare @analysis_max_date date
declare @analysis_hour_min bigint
declare @analysis_hour_max bigint
declare @analysis_date_current date
declare @data_count int
declare @data_days_analyze int
declare @sql_stmt varchar(9000)
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
declare @year_month varchar(6)
/*
declare @analysis_date_start date
declare @analysis_date_end date

set @analysis_date_start = '2014-04-29'
set @analysis_date_end = '2014-04-30'
*/
--set the runID (means you get a unique id to enter into the result set that is not dependant on the z_logger_runs routine)

set @RunID = dq_respository_Seq.nextval

--set the build date for the run so you know when it was run

set @build_date = today()

--create temp table for the dates you are analyzing

create table #tmp_date_hours
(min_broadcast_date_hour int,
max_broadcast_date_hour int)

--insert the min and max broadcast dates that are being analysed by the procedure.  Using UTC date for Programme as it is consistent over the year.

insert into #tmp_date_hours
select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
from 
sk_prod.viq_date
where utc_day_date between @analysis_date_start and @analysis_date_end

--set the min value of the dates you are analyzing

set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)

--set the max value of the dates you are analyzing

set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)

--collect the dates

select distinct pk_datehour_dim, utc_day_date, utc_year_month into #analysis_event_dates_final
from sk_prod.viq_date
where pk_datehour_dim between @min_broadcast_date_hour and @max_broadcast_date_hour

--set the min and max analysis dates

set @analysis_min_date = (select min(utc_day_date) from #analysis_event_dates_final)
set @analysis_max_date = (select max(utc_day_date) from #analysis_event_dates_final)

--set the analysis date current to be the analysis_min_date

set @analysis_date_current = @analysis_min_date

--create temporary table to collect the data for the analysis dates you want to use

create table #programme_viewing
(viewing_date date,
pk_viewing_prog_instance_fact bigint,
cb_change_date date,
dk_barb_min_start_datehour_dim int,
dk_barb_min_start_time_dim int,
dk_barb_min_end_datehour_dim int,
dk_barb_min_end_time_dim int,
dk_channel_dim int,
dk_event_start_datehour_dim int,
dk_event_start_time_dim int,
dk_event_end_datehour_dim int,
dk_event_end_time_dim int,
dk_instance_start_datehour_dim int,
dk_instance_start_time_dim int,
dk_instance_end_datehour_dim int,
dk_instance_end_time_dim int,
dk_programme_dim bigint,
dk_programme_instance_dim bigint,
dk_viewing_event_dim bigint,
genre_description varchar (20),
sub_genre_description varchar (20),
service_type bigint,
service_type_description varchar (40),
type_of_viewing_event varchar (40),
account_number varchar(20),
panel_id tinyint,
live_recorded varchar (8),
barb_min_start_date_time_utc timestamp,
barb_min_end_date_time_utc timestamp,
event_start_date_time_utc timestamp,
event_end_date_time_utc timestamp,
instance_start_date_time_utc timestamp,
instance_end_date_time_utc timestamp,
dk_capping_end_datehour_dim int,
dk_capping_end_time_dim int,
capping_end_date_time_utc timestamp,
log_start_date_time_utc timestamp,
duration int,
subscriber_id decimal(9,0),
log_received_start_date_time_utc timestamp,
capped_full_flag bit,
capped_partial_flag bit,
service_key int)

--loop around for the event dates that you want to measure in this analysis

while @analysis_date_current <= @analysis_max_date 
begin

--get the year_month value to add to the relevant dp_prog_viewed table

set @analysis_hour_min = (select min(pk_datehour_dim) from #analysis_event_dates_final where utc_day_date = @analysis_date_current)
set @analysis_hour_max = (select max(pk_datehour_dim) from #analysis_event_dates_final where utc_day_date = @analysis_date_current)
set @year_month = (select distinct cast(utc_year_month as varchar(6)) utc_year_month from #analysis_event_dates_final where utc_day_date = @analysis_date_current)

--get the viewing that you want to add to the staging table to analyze as part of the analysis


set @sql_stmt = 'insert into #programme_viewing
select '''||@analysis_date_current||''', pk_viewing_prog_instance_fact,cb_change_date,dk_barb_min_start_datehour_dim,dk_barb_min_start_time_dim,
dk_barb_min_end_datehour_dim,dk_barb_min_end_time_dim,dk_channel_dim, dk_event_start_datehour_dim,dk_event_start_time_dim,
dk_event_end_datehour_dim,dk_event_end_time_dim,dk_instance_start_datehour_dim,dk_instance_start_time_dim,
dk_instance_end_datehour_dim,dk_instance_end_time_dim,dk_programme_dim, dk_programme_instance_dim, dk_viewing_event_dim,
genre_description, sub_genre_description,service_type,service_type_description, type_of_viewing_event, account_number,panel_id,
live_recorded,barb_min_start_date_time_utc,barb_min_end_date_time_utc,event_start_date_time_utc,event_end_date_time_utc,
instance_start_date_time_utc,instance_end_date_time_utc,dk_capping_end_datehour_dim,dk_capping_end_time_dim,capping_end_date_time_utc,
log_start_date_time_utc, duration, subscriber_id, log_received_start_date_time_utc,capped_full_flag,capped_partial_flag, service_key
from sk_prod.vespa_dp_prog_viewed_'||@year_month||' vdpvc where dk_event_start_datehour_dim between '||@analysis_hour_min||' and '||@analysis_hour_max||'  '


--look to refactor for dimension key for performance

--select  @analysis_hour_min, @analysis_hour_max ,@year_month ,@sql_stmt

execute (@sql_stmt)

commit

set @analysis_date_current = @analysis_date_current + 1

end

--select * into programme_viewing from #programme_viewing



commit

----------------------------------D01 - Data collection-----------------------------------------------

--create temp table to store all the metrics results within

create table #linear_regression_metric_results
(REPORT_TYPE    varchar(50),
metric_id varchar(10),
report_date     date,
metric_result   decimal (16,2))

--METRIC I1 number of programme instances not in the dimension

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'I1',viewing_date,
(1.0 * (sum(case when prog_inst.programme_instance_id is not null then 1 else 0 end))/count(1) ) metric_result
from #programme_viewing programme_viewing 
left outer join
sk_prod.viq_programme_schedule prog_inst
on
programme_viewing.dk_programme_instance_dim = prog_inst.programme_instance_id
group by viewing_date

--METRIC I1 DONE

--METRIC I2 Number of Programmes not in  the Dimension

insert into #linear_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'I2',viewing_date,
(1.0 * (sum(case when prog.pk_programme_dim is not null then 1 else 0 end))/count(1)) metric_result
from #programme_viewing programme_viewing 
left outer join
(select pk_programme_dim from sk_prod.viq_programme) prog
on
programme_viewing.dk_programme_dim = prog.pk_programme_dim
group by viewing_date


--METRIC I2 DONE

--METRIC I3 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I3 DONE

--METRIC I4 NOT APPLICABLE AS NOT IN OLIVE


--METRIC I4 DONE

--METRIC I5 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I5 DONE

--METRIC I6 

--METRIC I6 DONE

--METRIC I7 Num of Channels not in the viq_channel dimension

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'I7',viewing_date,
(1.0 * (sum(case when broadcast_channel.pk_channel_dim is not null then 1 else 0 end))/count(1)) metric_result
from #programme_viewing programme_viewing 
left outer join
sk_Prod.viq_channel broadcast_channel
on programme_viewing.dk_channel_dim = broadcast_channel.pk_channel_dim
group by viewing_date

--I7 METRIC DONE


--METRIC I8-1 not applicable

--METRIC I8-1 DONE 

--METRIC I8-2 not applicable

--METRIC I8-2 DONE

--METRIC I8-3 Not Applicable


--METRIC I8-3 DONE

--METRIC I8-4 Not Applicable

--METRIC I8-4 DONE

--METRIC I9
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'I9',viewing_date,sum(case when DURATION IS NULL THEN 1
                                                when duration < 0 then 1 else 0 end) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--I9 METRIC DONE


--METRIC I10 - check that pk_id for viewing is unique

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'I10',viewing_date,
(1.0 * (count(distinct pk_viewing_prog_instance_fact)/count(1) * 100)) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--I10 METRIC DONE

--METRIC I11 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I11 DONE

--METRIC I12 NOt Applicable in Olive

--METRIC I12 DONE

--METRIC I13 Not Applicable in Olive

--METRIC I13 DONE

--METRIC I14 Not Applicable in Olive

--METRIC I14 DONE

--METRIC C1 Discontinued as the other Capping (C) metrics give what is required

--METRIC C1 DONE

--METRIC C2 check capped end time is after event start time

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C2',viewing_date,
(1.0 * (sum(case when capping_end_date_time_utc is not null and (capping_end_date_time_utc > event_start_date_time_utc) then 1 else 0 end)/ 
(sum(case when capping_end_date_time_utc is not null then 1 else 0 end)) * 100)) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--METRIC C2 DONE

--METRIC C3 check capped end time is less than or equal to event end time

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C3',viewing_date,
(1.0 * (sum(case when capping_end_date_time_utc is not null and (capping_end_date_time_utc <= event_end_date_time_utc) then 1 else 0 end)/ 
(sum(case when capping_end_date_time_utc is not null then 1 else 0 end)) * 100)) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--METRIC C3 DONE

--METRIC C4 Duplicate of C2

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C4',viewing_date,
(1.0 * (sum(case when capping_end_date_time_utc is not null and (capping_end_date_time_utc > event_start_date_time_utc) then 1 else 0 end)/ 
(sum(case when capping_end_date_time_utc is not null then 1 else 0 end)) * 100)) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--METRIC C4 DONE

--C5-1	Check we don't have capped date and not time

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C5-1',viewing_date,
(1.0 * (sum(case when dk_capping_end_datehour_dim > 0 and dk_capping_end_time_dim <= 0 then 1 else 0 end)/ 
count(1) * 100)) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--METRIC C5-1 DONE

--C5-2	Check we don't have capped time and not date

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C5-2',viewing_date,
(1.0 * (sum(case when dk_capping_end_time_dim > 0 and dk_capping_end_datehour_dim <= 0 then 1 else 0 end)/ 
count(1) * 100)) metric_result
--count(1) - count(distinct fact_viewing_slot_instance_key) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--METRIC C5-2 DONE

--C6-1	Check we have values for the full flag available in the fact

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C6-1',viewing_date,
(1.0 * (sum(case when capped_full_flag > 0 then 1 else 0 end))/ count(1) * 100) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--METRIC C6-1 DONE


--C6-2	Check we have values for the partial flag available in the fact

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C6-2',viewing_date,
(1.0 * (sum(case when capped_partial_flag > 0 then 1 else 0 end))/ count(1) * 100) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--METRIC C6-2 DONE

--C7-1	Check pre capped duration for programmes

select distinct viewing_date, subscriber_id, event_start_date_time_utc, event_end_date_time_utc, datediff(ss,event_start_date_time_utc, event_end_date_time_utc) diff
into #tmp_C7_1
from #programme_viewing
where subscriber_id > 0

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C7-1',viewing_date, sum(diff)/60 metric_result
from #tmp_C7_1
group by viewing_date

--METRIC C7_1 DONE

--C7-2	Check post capped duration for programmes

select distinct viewing_date, subscriber_id, event_start_date_time_utc, 
(case when capping_end_date_time_utc is not null then capping_end_date_time_utc else event_end_date_time_utc end) event_end_date , 
datediff(ss,event_start_date_time_utc, event_end_date) diff
into #tmp_C7_2
from #programme_viewing
where subscriber_id > 0

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C7-2',viewing_date,sum(diff)/60 metric_result
from #tmp_C7_2
group by viewing_date

--METRIC C7_2 DONE

--C8-2	Check hours viewed per box post capped duration for both programmes/slots

select distinct viewing_date, subscriber_id, event_start_date_time_utc, 
(case when capping_end_date_time_utc is not null then capping_end_date_time_utc else event_end_date_time_utc end) event_end_date , 
datediff(ss,event_start_date_time_utc, event_end_date) diff
into #tmp_C8_2
from #programme_viewing
where subscriber_id > 0

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C8-2',viewing_date,
(1.0 * (sum(diff)/60/60)) / (count(distinct subscriber_id) ) metric_result
from #tmp_C8_2
group by viewing_date

--METRIC C8-2 DONE

--C9	Check no overlap between partial or full capped flag

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C9',viewing_date,
sum(case when (capped_full_flag > 0 and capped_partial_flag > 0 ) then 1 else 0 end) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--METRIC C9 DONE

--C10	Check capped date/time not null if capped flag is 1

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'C10',viewing_date,
sum(case when (capped_full_flag > 0 and capping_end_date_time_utc is null) then 1 
when (capped_partial_flag > 0 and capping_end_date_time_utc is null) then 1 else 0 end) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--METRIC C10 DONE


--M2	Check programmes are been attributed

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M2',viewing_date,
(1.0 * (sum(case when (dk_barb_min_start_datehour_dim > 0 and dk_barb_min_end_datehour_dim > 0 
and dk_barb_min_start_time_dim > 0 and dk_barb_min_end_time_dim > 0) then 1 else 0 end)) /count(1)) * 100
metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--M2 Done

--M3	Check for attributted programmes we have barb start minute and barb end minute


insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M3',viewing_date,
1.0 * (sum(case when (dk_barb_min_start_datehour_dim > 0 and dk_barb_min_start_time_dim <= 0) then 1 
when (dk_barb_min_end_datehour_dim > 0 and dk_barb_min_end_time_dim <= 0) then 1 else 0 end)) /
(sum(case when (dk_barb_min_start_datehour_dim > 0 and dk_barb_min_end_datehour_dim > 0 
and dk_barb_min_start_time_dim > 0 and dk_barb_min_end_time_dim > 0) then 1 else 0 end))
metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--M3 Done

--M4-1	Check for cases with barb start but no end minute

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M4-1',viewing_date,
sum(case when (dk_barb_min_start_time_dim > 0 and dk_barb_min_end_time_dim <= 0) then 1 else 0 end)
metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--M4-1 Done

--M4-2	Check for cases with barb end but no start minute

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M4-2',viewing_date,
sum(case when (dk_barb_min_start_time_dim <= 0 and dk_barb_min_end_time_dim > 0) then 1 else 0 end)
metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--M4-2 Done

--M5	Count slots/programmes are clamining the same minute (should be 0)

--only looking at Live events as there is the scenario where recorded instances can get the same minute

select viewing_date, subscriber_id,pk_viewing_prog_instance_fact, instance_start_date_time_utc,live_recorded,
LAG (pk_viewing_prog_instance_fact) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_pk_viewing_prog_instance_fact,
LAG (dk_barb_min_start_time_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_start_time,
LAG (dk_barb_min_end_time_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_end_time,
LAG (dk_barb_min_start_datehour_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_start_date,
LAG (dk_barb_min_end_datehour_dim) OVER (PARTITION BY subscriber_id ORDER BY instance_start_date_time_utc) prev_barb_end_date,
dk_barb_min_start_datehour_dim, dk_barb_min_start_time_dim,
dk_barb_min_end_datehour_dim, dk_barb_min_end_time_dim into #tmp_data_quality_ma_overlap
from #programme_viewing
where subscriber_id > 0
and UPPER(live_recorded) = 'LIVE'


insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M5',viewing_date,
count(1) metric_result
from #tmp_data_quality_ma_overlap
where dk_barb_min_start_datehour_dim = prev_barb_start_date
and dk_barb_min_start_time_dim <= prev_barb_end_time
and dk_barb_min_start_time_dim > 0
group by viewing_date

--M5 Done

--M7-1	Check proportion of records been attributed

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M2',viewing_date,
(1.0 * (sum(case when (dk_barb_min_start_datehour_dim > 0 and dk_barb_min_end_datehour_dim > 0 
and dk_barb_min_start_time_dim > 0 and dk_barb_min_end_time_dim > 0) then 1 else 0 end)) /count(1)) * 100
metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--M7-1 Done

--M7-2	Check number of records been attributed

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'M7-2',viewing_date,
sum(case when (dk_barb_min_start_datehour_dim > 0 and dk_barb_min_end_datehour_dim > 0 
and dk_barb_min_start_time_dim > 0 and dk_barb_min_end_time_dim > 0) then 1 else 0 end)
metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--M7-2 Done

--S1-1	Proportion of events with weight assigned - Not valid for Programme in Olive

--S1-2	Number of events with weight assigned - Not valid for Programme in Olive

--S1-2	done

--S2	Check no difference between weights in fact and source  - Not valid for Programme in Olive

--S2	Done

--S3	Check all records matching scaling source have been attributed  - Not valid for Programme in Olive

--S3    Done

--S4 Check we have a weight asigned where the flag = 1 - Not valid for Programme in Olive

--S4 Done

--S5	Check the flag = 1 when weight is asigned - Not valid for Programme in Olive

--S5 Done

--CH1-1	Proportion of channels matching with the dimension
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'CH1-1',viewing_date,
(1.0 * sum(case when broadcast_channel.pk_channel_dim is not null then 1 else 0 end)/count(1)) metric_result
from #programme_viewing programme_viewing 
left outer join
(select pk_channel_dim, service_key
from sk_prod.viq_channel) broadcast_channel
on programme_viewing.dk_channel_dim = broadcast_channel.pk_channel_dim
group by viewing_date

--CH1-1	Done

--CH1-2	Number of channels matching with the dimension
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'CH1-2',viewing_date,
count(distinct broadcast_channel.service_key) metric_result
from #programme_viewing programme_viewing 
left outer join
(select pk_channel_dim, service_key
from sk_prod.viq_channel) broadcast_channel
on programme_viewing.dk_channel_dim = broadcast_channel.pk_channel_dim
group by viewing_date


--CH1-2	Done

--CH2	Checking all channel names are in place (null or -1 count)
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'CH2',viewing_date,
sum(distinct case when broadcast_channel.channel_name is null then 1 
when broadcast_channel.channel_name = '(blank)' then 1 
when broadcast_channel.channel_name = '(unknown)' then 1 
when LOWER(broadcast_channel.channel_name) = 'unknown' then 1 
when broadcast_channel.channel_name = '-1' then 1 
when broadcast_channel.channel_name = '-99' then 1 else 0 end) metric_result
from #programme_viewing programme_viewing 
left outer join
(select pk_channel_dim, service_key, channel_name
from sk_prod.viq_channel) broadcast_channel
on programme_viewing.dk_channel_dim = broadcast_channel.pk_channel_dim
group by viewing_date

--CH2	Done

--CH3	Checking all Channel Genre are in place (null or -1 count)
--channel genre not located in viq_channel table

--CH3	Done

--CH4	Checking all service keys are in place (null or -1 count)

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'CH4',viewing_date,
sum(case when broadcast_channel.service_key  is null then 1 
when broadcast_channel.service_key < 0 then 1 else 0 end) metric_result
from #programme_viewing programme_viewing 
left outer join
(select pk_channel_dim, service_key, channel_name
from sk_prod.viq_channel) broadcast_channel
on programme_viewing.dk_channel_dim = broadcast_channel.pk_channel_dim
group by viewing_date

--CH4 DONE

--D1	Check how many records have we got per day
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'D1',viewing_date,
COUNT(1) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM - Not valid for Olive

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'D2',viewing_date,
1.0 * (sum(case when dk_programme_instance_dim is null then 1 
when dk_programme_instance_dim < 0 then 1 else 0 end))/count(1) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--D2 Done

--D3	Check proportion of records without a DK_PROGRAMME_DIM -- Not valid for Olive

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'D3',viewing_date,
1.0 * (sum(case when dk_programme_dim is null then 1 
when dk_programme_dim < 0 then 1 else 0 end))/count(1) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--D3 Done

--D4	Check proportion of records without a DK_CHANNEL_DIM

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'D4',viewing_date,
1.0 * (sum(case when dk_channel_dim is null then 1 
when dk_channel_dim < 0 then 1 else 0 end))/count(1) metric_result
from #programme_viewing programme_viewing 
group by viewing_date



--D4 Done

--D4	D5	Check number of accounts per day
insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'D5',viewing_date,
count(distinct Account_number) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--D5 Done

--D6	Check number of boxes per day - Not valid for Olive

insert into #LINEAR_regression_metric_results
(REPORT_TYPE    ,
metric_id ,
report_date     ,
metric_result   )
select 'PROGRAMME', 'D6',viewing_date,
count(distinct subscriber_id) metric_result
from #programme_viewing programme_viewing 
group by viewing_date


--D6 Done

--B2	Primary Key Check on Programme Fact Key

insert into #LINEAR_regression_metric_results
select 'PROGRAMME', 'B2',viewing_date,
(1.0 * (count(distinct pk_viewing_prog_instance_fact)/count(1) * 100)) metric_result
from #programme_viewing programme_viewing 
group by viewing_date

--B2 Done

--insert results into regression repository table

insert into data_quality_regression_reports_repository
(run_id,report_type ,metric_id ,metric_result ,rag_status, report_date,metric_threshold,metric_tolerance_amber,metric_tolerance_red)
SELECT @RunID, b.report_type, b.metric_id, a.metric_result, 
metric_benchmark_check (a.metric_result, b.metric_threshold, b.metric_tolerance_amber, b.metric_tolerance_red) RAG_STATUS ,
a.report_date, b.metric_threshold,b.metric_tolerance_amber,b.metric_tolerance_red
FROM #linear_regression_metric_results a
right outer join
(select * from data_quality_regression_thresholds 
where report_type = 'PROGRAMME' )b
on (UPPER(a.report_type) = UPPER(b.report_type)
and a.metric_id = b.metric_id)

commit

end

go
grant execute on data_quality_programme_regression_checks to vespa_group_low_security, sk_prodreg, buxceys, kinnairt
