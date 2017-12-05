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
** Project Vespa: Data Quality Adsmart Regression Checks
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


--exec data_quality_adsmart_regression_checks 'LOCAL','2014-03-01','2014-03-07';


if object_id('data_quality_adsmart_regression_checks') is not null drop procedure data_quality_adsmart_regression_checks;
commit;

go

create procedure data_quality_adsmart_regression_checks
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
from sk_prod.FACT_ADSMART_SLOT_INSTANCE_HISTORY
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour and @max_broadcast_date_hour

SELECT COUNT(1) total_vol, 1 CNT INTO #tmp_cnt_inst
from sk_prod.FACT_ADSMART_SLOT_INSTANCE
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour and @max_broadcast_date_hour

select distinct pk_datehour_dim, local_day_date, broadcast_day_date into #analysis_event_dates_final
from sk_prod.viq_date
where pk_datehour_dim between @min_broadcast_date_hour and @max_broadcast_date_hour

set @analysis_min_date = (select min(pk_datehour_dim/100) from sk_prod.viq_date where local_day_date = (select min(local_day_date) from #analysis_event_dates_final))
set @analysis_max_date = (select max(pk_datehour_dim/100) from sk_prod.viq_date where local_day_date = (select max(local_day_date) from #analysis_event_dates_final))

set @slot_cnt = (select total from #tmp_cnt_inst_hist)
set @slot_cnt_vol = (select total_vol from #tmp_cnt_inst)

if @slot_cnt > 0 and @slot_cnt_vol > 0
begin
set @fact_tbl_name = 'sk_prod.FACT_ADSMART_SLOT_INSTANCE_HISTORY'
set @fact_tbl_name_additional = 'sk_prod.FACT_ADSMART_SLOT_INSTANCE'
end

if @slot_cnt > 0 and @slot_cnt_vol = 0
begin
set @fact_tbl_name = 'sk_prod.FACT_ADSMART_SLOT_INSTANCE_HISTORY'
END

if @slot_cnt = 0 and @slot_cnt_vol > 0
begin
set @fact_tbl_name = 'sk_prod.FACT_ADSMART_SLOT_INSTANCE'
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

create table #adsmart_slots
(fact_viewing_slot_instance_key bigint, 
adsmart_campaign_key bigint, 
agency_key bigint, 
broadcast_channel_key bigint, 
broadcast_start_date_key integer,
broadcast_start_time_key integer, 
preceding_programme_schedule_key bigint, 
succeeding_programme_schedule_key bigint,
segment_key bigint, 
slot_copy_key bigint, 
slot_reference_key bigint,
time_shift_key smallint,
viewed_start_date_key integer, 
viewed_start_time_key integer, 
actual_impacts smallint, 
actual_impressions decimal, 
actual_impressions_day_one_weighted decimal , 
actual_serves integer,
actual_weight decimal , 
sample_impressions integer,
viewed_duration integer, 
household_key bigint,
local_day_date date)

if @fact_tbl_name is not null AND @fact_tbl_name_additional is null
begin
set @var_sql = 
'insert into #adsmart_slots
select fact_viewing_slot_instance_key, 
adsmart_campaign_key, agency_key, broadcast_channel_key, broadcast_start_date_key,
broadcast_start_time_key, preceding_programme_schedule_key, 
succeeding_programme_schedule_key,segment_key, 
slot_copy_key, slot_reference_key,time_shift_key,viewed_start_date_key, viewed_start_time_key, 
actual_impacts, actual_impressions, actual_impressions_day_one_weighted, actual_serves,
actual_weight, sample_impressions,viewed_duration, household_key, b.'||@date_column||' 
from '||@fact_tbl_name||' a, #analysis_event_dates_final b
 WHERE a.BROADCAST_START_DATE_KEY  = b.pk_datehour_dim
and a.BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0'

execute (@var_sql)

end

if @fact_tbl_name is not null AND @fact_tbl_name_additional is not null
begin
set @var_sql = 
'insert into #adsmart_slots
select fact_viewing_slot_instance_key, 
adsmart_campaign_key, agency_key, broadcast_channel_key, broadcast_start_date_key,
broadcast_start_time_key, preceding_programme_schedule_key, 
succeeding_programme_schedule_key,segment_key, 
slot_copy_key, slot_reference_key,time_shift_key,viewed_start_date_key, viewed_start_time_key, 
actual_impacts, actual_impressions, actual_impressions_day_one_weighted, actual_serves,
actual_weight, sample_impressions,viewed_duration, household_key, b.'||@date_column||' 
from '||@fact_tbl_name||' a, #analysis_event_dates_final b
 WHERE a.BROADCAST_START_DATE_KEY  = b.pk_datehour_dim
and a.BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0'

execute (@var_sql)

set @var_sql = 
'insert into #adsmart_slots 
select fact_viewing_slot_instance_key, 
adsmart_campaign_key, agency_key, broadcast_channel_key, broadcast_start_date_key,
broadcast_start_time_key, preceding_programme_schedule_key, 
succeeding_programme_schedule_key,segment_key, 
slot_copy_key, slot_reference_key,time_shift_key,viewed_start_date_key, viewed_start_time_key, 
actual_impacts, actual_impressions, actual_impressions_day_one_weighted, actual_serves,
actual_weight, sample_impressions,viewed_duration, household_key, b.'||@date_column||'
from '||@fact_tbl_name_additional||' a, #analysis_event_dates_final b
 WHERE a.BROADCAST_START_DATE_KEY  = b.pk_datehour_dim
and a.BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0'

execute (@var_sql)

end



create table #adsmart_regression_metric_results
(REPORT_TYPE    varchar(50),
metric_id varchar(10),
report_date     date,
metric_result   decimal (16,2))

--METRIC I1

insert into #adsmart_regression_metric_results
select 'ADSMART', 'I1',local_day_date,
(1.0 * (sum(case when slot_copy.slot_copy_key is not null then 1 else 0 end))/count(1) * 100) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select slot_copy_key,slot_copy_duration_seconds, slot_type, 
product_code, product_name  from sk_prod.DIM_SLOT_COPY) slot_copy
on
adsmart_slots.slot_copy_key = slot_copy.slot_copy_key
group by local_day_date

--METRIC I1 DONE

--METRIC I2

insert into #adsmart_regression_metric_results
select 'ADSMART', 'I2',local_day_date,
(1.0 * (sum(case when slot_reference.slot_reference_key is not null then 1 else 0 end))/count(1) * 100) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
adsmart_slots.slot_reference_key = slot_reference.slot_reference_key
group by local_day_date


--METRIC I2 DONE

--METRIC I2
/*
insert into #adsmart_regression_metric_results
select 'ADSMART', 'I2',local_day_date,sum(case when slot_reference.slot_reference_key is null then 1 else 0 end) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
adsmart_slots.slot_reference_key = slot_reference.slot_reference_key
group by local_day_date
*/

--METRIC I2 DONE

--METRIC I3 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I3 DONE

--METRIC I4 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I4 DONE

--METRIC I5 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I5 DONE

--METRIC I6 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I6 DONE

--METRIC I7 

insert into #adsmart_regression_metric_results
select 'ADSMART', 'I7',local_day_date,
(1.0 * (sum(case when broadcast_channel.broadcast_channel_key is not null then 1 else 0 end))/count(1) * 100) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--I7 METRIC DONE


--METRIC I8-1 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I8-1 DONE

--METRIC I8-2 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I8-2 DONE

--METRIC I8-3 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I8-3 DONE

--METRIC I8-4 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I8-4 DONE

--METRIC I9
insert into #adsmart_regression_metric_results
select 'ADSMART', 'I9',local_day_date,sum(case when VIEWED_DURATION IS NULL THEN 1
                                                when viewed_duration < 1 then 1 else 0 end) metric_result
from #adsmart_slots adsmart_slots 
group by local_day_date

--I9 METRIC DONE


--METRIC I10

--change to zer

insert into #adsmart_regression_metric_results
select 'ADSMART', 'I10',local_day_date,
(1.0 * (count(distinct fact_viewing_slot_instance_key)/count(1) * 100)) metric_result
--count(1) - count(distinct fact_viewing_slot_instance_key) metric_result
from #adsmart_slots adsmart_slots 
group by local_day_date

--I10 METRIC DONE

--METRIC I11 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I11 DONE

--METRIC I12 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I12 DONE

--METRIC I13 NOT APPLICABLE AS NOT IN OLIVE

--METRIC I13 DONE

--METRIC I14 NOT APPLICABLE AS NOT IN OLIVE

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
insert into #adsmart_regression_metric_results
select 'ADSMART', 'C7-2',local_day_date,sum(viewed_duration) metric_result
from #adsmart_slots adsmart_slots 
group by local_day_date

--METRIC C7-2 DONE

--METRIC C8-2
insert into #adsmart_regression_metric_results
select 'ADSMART', 'C8-2',local_day_date,
(1.0 * (sum(viewed_duration)/60 )/ SUM(DISTINCT HOUSEHOLD_KEY) * 100) metric_result
from #adsmart_slots adsmart_slots 
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
insert into #adsmart_regression_metric_results
select 'ADSMART', 'S1-1',local_day_date,
round((1.0 * (SUM(CASE WHEN ACTUAL_WEIGHT > 0 THEN 1 ELSE 0 END))/count(1)),2) metric_result
from #adsmart_slots adsmart_slots 
group by local_day_date

--S1-2	Number of events with weight assigned
insert into #adsmart_regression_metric_results
select 'ADSMART', 'S1-2',local_day_date,
SUM(CASE WHEN ACTUAL_WEIGHT > 0 THEN 1 ELSE 0 END) metric_result
from #adsmart_slots adsmart_slots 
group by local_day_date

--S1-2	done

--S2	Check no difference between weights in fact and source
insert into #adsmart_regression_metric_results
select 'ADSMART', 'S2',local_day_date,sum(case when actual_weight != calculated_scaling_weight then 1 else 0 end) metric_result --
from 
(select distinct household_key, actual_weight, local_day_date
from #adsmart_slots adsmart_slots ) a,
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
insert into #adsmart_regression_metric_results
select 'ADSMART', 'CH1-1',local_day_date,
(1.0 * sum(case when broadcast_channel.broadcast_channel_key is not null then 1 else 0 end)/count(adsmart_slots.broadcast_channel_key)) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--CH1-1	Done

--CH1-2	Number of channels matching with the dimension
insert into #adsmart_regression_metric_results
select 'ADSMART', 'CH1-2',local_day_date,
count(distinct case when broadcast_channel.broadcast_channel_key is not null then broadcast_channel.broadcast_channel_key else null end) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--CH1-2	Done

--CH2	Checking all channel names are in place (null or -1 count)
insert into #adsmart_regression_metric_results
select 'ADSMART', 'CH2',local_day_date,
sum(distinct case when broadcast_channel.vespa_channel_name is null then 1 
when broadcast_channel.vespa_channel_name = '(blank)' then 1 
when broadcast_channel.vespa_channel_name = '(unknown)' then 1 
when broadcast_channel.vespa_channel_name = '-1' then 1 
when broadcast_channel.vespa_channel_name = '-99' then 1 else 0 end) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--CH2	Done

--CH3	Checking all Channel Genre are in place (null or -1 count)
insert into #adsmart_regression_metric_results
select 'ADSMART', 'CH3',local_day_date,
sum(distinct case when broadcast_channel.channel_genre is null then 1 
when broadcast_channel.channel_genre = 'unknown' then 1 
when broadcast_channel.channel_genre = 'N/a' then 1 
when broadcast_channel.channel_genre = '(unknown)' then 1 
when broadcast_channel.channel_genre = '-1' then 1 
when broadcast_channel.channel_genre = '-99' then 1 else 0 end) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--CH3	Done

--CH4	Checking all service keys are in place (null or -1 count)
insert into #adsmart_regression_metric_results
select 'ADSMART', 'CH3',local_day_date,
sum(distinct case when broadcast_channel.service_key is null then 1 
when broadcast_channel.service_key < 0 then 1 else 0 end) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--CH4 DONE

--D1	Check how many records have we got per day
insert into #adsmart_regression_metric_results
select 'ADSMART', 'D1',local_day_date,
COUNT(1) metric_result
from #adsmart_slots adsmart_slots 
group by local_day_date

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM - Not valid for Olive

--D2 Done


--D3	Check proportion of records without a DK_PROGRAMME_DIM -- Not valid for Olive


--D3 Done

--D4	Check proportion of records without a DK_CHANNEL_DIM
insert into #adsmart_regression_metric_results
select 'ADSMART', 'D4',local_day_date,
sum(case when broadcast_channel.broadcast_channel_key is null then 1 else 0 end) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--D4 Done

--D4	D5	Check number of accounts per day
insert into #adsmart_regression_metric_results
select 'ADSMART', 'D5',local_day_date,
count(distinct household_key) metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name, channel_genre, service_key
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--D5 Done

--D6	Check number of boxes per day - Not valid for Olive

--D6 Done

--AD1	Check proportion of records without a DK_agency_DIM
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD1',local_day_date,
(1.0 * sum(case when agency.agency_key is null then 1 else 0 end)/count(1) )metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select  advertiser_code, advertiser_name, agency_key, barb_sales_house_id, buyer_code, sales_house_name, sales_house_short_name,
buyer_name from sk_prod.DIM_AGENCY) agency
on adsmart_slots.agency_key = agency.agency_key
group by local_day_date

--AD1 Done

--AD2	Check proportion of records without a DK_campaign_DIM (both for Linear and Adsmart campaign)
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD2',local_day_date,
(1.0 * sum(case when campaign.adsmart_campaign_key is null then 1 else 0 end)/count(1) )metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select  adsmart_campaign_key from sk_prod.dim_adsmart_campaign) campaign
on adsmart_slots.adsmart_campaign_key = campaign.adsmart_campaign_key
group by local_day_date

--AD2	Done

--AD3	Check proportion of records without a DK_agency_DIM - Duplicate


--AD3   DONE

--AD4	Check proportion of records without a DK_segment_DIM at Segment Fact
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD4',local_day_date,
(1.0 * sum(case when SEGMENT.segment_key is null then 1 
                when SEGMENT.segment_key < 0 then 1 else 0 end)/count(1))metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select segment_key, segment_id, segment_name, 
segment_status, segment_description from sk_prod.DIM_SEGMENT) segment
on
adsmart_slots.segment_key = segment.segment_key
group by local_day_date

--AD4 done

--AD5	Check proportion of records without a DK_broadcast_channel_DIM at VSIF_volatile
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD5',local_day_date,
(1.0 * sum(case when broadcast_channel.broadcast_channel_key is null then 1 
                when broadcast_channel.broadcast_channel_key < 0 then 1 else 0 end)/count(1) )metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
group by local_day_date

--AD5 done

--AD6	Check proportion of records without a DK_broadcast_channel_DIM at VSIF_volatile
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD6',local_day_date,
(1.0 * sum(case when preceding_broadcast_programme.broadcast_programme_schedule_key is null then 1 
                when preceding_broadcast_programme.broadcast_programme_schedule_key < 0 then 1 else 0 end)/count(1))metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key preceding_programme_broadcast_start_date_key,
broadcast_start_time_key preceding_programme_broadcast_start_time_key,  
broadcast_end_date_key preceding_programme_broadcast_end_date_key, 
broadcast_end_time_key preceding_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) preceding_broadcast_programme
on
adsmart_slots.preceding_programme_schedule_key = preceding_broadcast_programme.broadcast_programme_schedule_key
group by local_day_date

--AD6 done

--AD7	Check proportion of records without a DK_PROGRAMME_SCHEDULE_DIM at VSIF_volatile (for Succeeding prog)
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD7',local_day_date,
(1.0 * sum(case when succ_broadcast_programme.broadcast_programme_schedule_key is null then 1 
                when succ_broadcast_programme.broadcast_programme_schedule_key < 0 then 1 else 0 end)/count(1) )metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key succ_programme_broadcast_start_date_key,
broadcast_start_time_key succ_programme_broadcast_start_time_key,  
broadcast_end_date_key succ_programme_broadcast_end_date_key, 
broadcast_end_time_key succ_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) succ_broadcast_programme
on
adsmart_slots.succeeding_programme_schedule_key = succ_broadcast_programme.broadcast_programme_schedule_key
group by local_day_date

--AD7 Done

--AD8	Check proportion of records without a DK_SLOT_REFERENCE_DIM at VSIF_volatile
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD8',local_day_date,
(1.0 * sum(case when slot_reference.slot_reference_key is null then 1 
                when slot_reference.slot_reference_key < 0 then 1 else 0 end)/count(1))metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
adsmart_slots.slot_reference_key = slot_reference.slot_reference_key
group by local_day_date

--AD8 DONE

--AD9	Check proportion of records without a DK_SLOT_COPY_DIM at VSIF_volatile
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD9',local_day_date,
(1.0 * sum(case when slot_copy.slot_copy_key is null then 1 
                when slot_copy.slot_copy_key < 0 then 1 else 0 end)/count(1))metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select slot_copy_key,slot_copy_duration_seconds, slot_type, 
product_code, product_name  from sk_prod.DIM_SLOT_COPY) slot_copy
on
adsmart_slots.slot_copy_key = slot_copy.slot_copy_key
group by local_day_date

--AD9 DONE

--AD10	Check proportion of records without a DK_segment_DIM at VSIF_volatile

--AD10	Done

--AD11	Check proportion of records without a DK_ADSMART_MEDIA_CAMPAIGN_DIM at VSIF_volatile
insert into #adsmart_regression_metric_results
select 'ADSMART', 'AD11',local_day_date,
(1.0 * sum(case when campaign.adsmart_campaign_key is null then 1 else 0 end)/count(1) )metric_result
from #adsmart_slots adsmart_slots 
left outer join
(select  adsmart_campaign_key from sk_prod.dim_adsmart_campaign) campaign
on adsmart_slots.adsmart_campaign_key = campaign.adsmart_campaign_key
group by local_day_date

--AD11 DONE

--AD12	Check proportion of records without a DK_agency_DIM at VSIF_volatile - DUPLICATE OF AD1


--AD13	Check Adsmart campaign are not duplicated -?????

--insert results into regression repository table

insert into data_quality_regression_reports_repository
(run_id,report_type ,metric_id ,metric_result ,rag_status, report_date,metric_threshold,metric_tolerance_amber,metric_tolerance_red)
SELECT @RunID, b.report_type, b.metric_id, a.metric_result, 
metric_benchmark_check (a.metric_result, b.metric_threshold, b.metric_tolerance_amber, b.metric_tolerance_red) RAG_STATUS ,
a.report_date, b.metric_threshold,b.metric_tolerance_amber,b.metric_tolerance_red
FROM #adsmart_regression_metric_results a
right outer join
(select * from data_quality_regression_thresholds 
where report_type = 'ADSMART' ) b
on (UPPER(a.report_type) = UPPER(b.report_type)
and a.metric_id = b.metric_id)
commit

end

grant execute on data_quality_adsmart_regression_checks to vespa_group_low_security, sk_prodreg, buxceys, kinnairt
