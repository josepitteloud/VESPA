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


if object_id('data_quality_adsmart_rolling_day_processing') is not null drop procedure data_quality_adsmart_rolling_day_processing;
commit;

go


create procedure data_quality_adsmart_rolling_day_processing
@date_type varchar(8)
,@CP2_build_ID     bigint = NULL
,@BUILD_date date
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

/*
declare @date_type varchar(8)
declare @CP2_build_ID     INT
declare @BUILD_date date
declare @analysis_date_start date
declare @analysis_date_end date
declare @RunID     INT

set @RunID  = 99999
set @date_type = 'LOCAL'
set @CP2_build_ID     = 99999
set @BUILD_date = '2014-04-23'
set @analysis_date_start = '2014-04-01'
set @analysis_date_end = '2014-04-02'

*/

create table #tmp_date_hours
(min_broadcast_date_hour int,
max_broadcast_date_hour int)

if @date_type = 'LOCAL' 
begin

insert into #tmp_date_hours
select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
from 
sk_prod.viq_date
where local_day_date between @analysis_date_start and @analysis_date_end

end


if @date_type = 'BROADCAST' 

begin

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

select distinct local_day_date, broadcast_day_date dates_analysed into #analysis_event_dates_final
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

EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart running on'||@fact_tbl_name||'',@data_count

---lets see how many days worth of data we are running for

set @data_count = null

set @data_count = (select count(1) from #analysis_event_dates_final)

EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart running for count of days',@data_count

--ok time to start with the 1st date we have on our list of dates to examine--

EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart Process Start for Dates '||cast (@min_broadcast_date_hour as varchar(20)) || 'to'||cast (@max_broadcast_date_hour as varchar(20))

----------------------------------D01 - Data collection-----------------------------------------------

begin

--set @slot_date = cast(replace(@analysis_date_current,'-','') as int)

truncate table DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT

commit

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
household_key bigint)

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
actual_weight, sample_impressions,viewed_duration, household_key
from '||@fact_tbl_name||' WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
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
actual_weight, sample_impressions,viewed_duration, household_key
from '||@fact_tbl_name||' WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
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
actual_weight, sample_impressions,viewed_duration, household_key
from '||@fact_tbl_name_additional||' WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0 '

execute (@var_sql)
end

insert into DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT
(--adsmart slots
fact_viewing_slot_instance_key, 
adsmart_campaign_key, agency_key, broadcast_channel_key, broadcast_start_date_key,
broadcast_start_time_key, preceding_programme_schedule_key, 
succeeding_programme_schedule_key,segment_key, 
slot_copy_key, slot_reference_key,time_shift_key,viewed_start_date_key, 
viewed_start_time_key, 
actual_impacts, actual_impressions, actual_impressions_day_one_weighted, actual_serves,
actual_weight, sample_impressions,
viewed_duration,household_key,
--time_shift
viewing_type, timeshift_band,elapsed_days,
elapsed_hours,elapsed_hours_total,
--agency
advertiser_code, advertiser_name,  
barb_sales_house_id, buyer_code, sales_house_name, sales_house_short_name,
buyer_name,
--broadcast_channel
 channel_format,media_adsmart_status, vespa_channel_name,
 --preceding_broadcast_programme_schedule
 preceding_programme_broadcast_start_date_key,
 preceding_programme_broadcast_start_time_key,  
 preceding_programme_broadcast_end_date_key, 
 preceding_programme_broadcast_end_time_key,
 --succ_broadcast_programme_schedule
succ_programme_broadcast_start_date_key,
succ_programme_broadcast_start_time_key,  
succ_programme_broadcast_end_date_key, 
succ_programme_broadcast_end_time_key,
--segment
segment_id, segment_name, segment_status, segment_description,
--slot_copy
slot_copy_duration_seconds, slot_type, product_code, product_name,
--slot_reference
slot_reference_slot_type, slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds,
spot_position_in_break,slot_type_position, 
slot_type_total_position, 
break_position, 
adsmart_action, 
adsmart_priority, 
adsmart_status, adsmart_total_priority,
--broadcast start time
broadcast_start_utc_time, start_broadcast_time, start_spot_standard_daypart_uk,
--viewing start time
viewing_start_utc_time, viewing_start_broadcast_time, viewing_start_spot_standard_daypart_uk,
--broadcast start date
broadcast_start_datehour_utc,  broadcast_start_day_date,  
broadcast_start_weekday,broadcast_start_day_in_month, 
 broadcast_start_day_in_week, broadcast_start_day_long,
 utc_start_day_date,  
 utc_start_weekday, 
utc_start_day_in_month,  utc_start_day_in_week,
utc_start_day_long ,
 --viewing start date
 viewing_start_datehour_utc,  viewing_start_day_date, 
viewing_start_weekday, viewing_start_day_in_month, 
viewing_start_day_in_week, viewing_start_day_long,
 utc_viewing_start_day_date, utc_viewing_start_weekday, 
utc_viewing_start_day_in_month,  utc_viewing_start_day_in_week,
utc_viewing_start_day_long,
--local start date
 local_start_day_date, local_start_weekday, 
local_start_day_in_month,  local_start_day_in_week,
local_start_day_long
)
select 
--adsmart slots
adsmart_slots.fact_viewing_slot_instance_key, 
adsmart_slots.adsmart_campaign_key, adsmart_slots.agency_key, adsmart_slots.broadcast_channel_key, adsmart_slots.broadcast_start_date_key,
adsmart_slots.broadcast_start_time_key, adsmart_slots.preceding_programme_schedule_key, 
adsmart_slots.succeeding_programme_schedule_key,adsmart_slots.segment_key, 
adsmart_slots.slot_copy_key, adsmart_slots.slot_reference_key,adsmart_slots.time_shift_key,adsmart_slots.viewed_start_date_key, 
adsmart_slots.viewed_start_time_key, 
adsmart_slots.actual_impacts, adsmart_slots.actual_impressions, adsmart_slots.actual_impressions_day_one_weighted, adsmart_slots.actual_serves,
adsmart_slots.actual_weight, adsmart_slots.sample_impressions,
adsmart_slots.viewed_duration,adsmart_slots.household_key,
--time_shift
time_shift.viewing_type, time_shift.timeshift_band,time_shift.elapsed_days,
time_shift.elapsed_hours,time_shift.elapsed_hours_total,
--agency
agency.advertiser_code, agency.advertiser_name,  
agency.barb_sales_house_id, agency.buyer_code, agency.sales_house_name, agency.sales_house_short_name,
agency.buyer_name,
--broadcast_channel
 broadcast_channel.channel_format,broadcast_channel.media_adsmart_status, broadcast_channel.vespa_channel_name,
 --preceding_broadcast_programme_schedule
 preceding_broadcast_programme.preceding_programme_broadcast_start_date_key,
 preceding_broadcast_programme.preceding_programme_broadcast_start_time_key,  
 preceding_broadcast_programme.preceding_programme_broadcast_end_date_key, 
 preceding_broadcast_programme.preceding_programme_broadcast_end_time_key,
 --succ_broadcast_programme_schedule
succ_broadcast_programme.succ_programme_broadcast_start_date_key,
succ_broadcast_programme.succ_programme_broadcast_start_time_key,  
succ_broadcast_programme.succ_programme_broadcast_end_date_key, 
succ_broadcast_programme.succ_programme_broadcast_end_time_key,
--segment
segment.segment_id, segment.segment_name, segment.segment_status, segment.segment_description,
--slot_copy
slot_copy.slot_copy_duration_seconds, slot_copy.slot_type, slot_copy.product_code, slot_copy.product_name,
--slot_reference
slot_reference.slot_reference_slot_type, slot_reference.slot_sub_type, slot_reference.slot_duration_seconds,slot_reference.slot_duration_reported_Seconds,
slot_reference.spot_position_in_break,slot_reference.slot_type_position, 
slot_reference.slot_type_total_position, 
slot_reference.break_position, 
slot_reference.adsmart_action, 
slot_reference.adsmart_priority, 
slot_reference.adsmart_status, slot_reference.adsmart_total_priority,
--broadcast start time
broadcast_start_time.broadcast_start_utc_time, broadcast_start_time.start_broadcast_time, broadcast_start_time.start_spot_standard_daypart_uk,
--viewing start time
viewing_start_time.viewing_start_utc_time, viewing_start_time.viewing_start_broadcast_time, viewing_start_time.viewing_start_spot_standard_daypart_uk,
--broadcast start date
broadcast_start_date.broadcast_start_datehour_utc,  broadcast_start_date.broadcast_start_day_date,  
broadcast_start_date.broadcast_start_weekday,broadcast_start_date.broadcast_start_day_in_month, 
 broadcast_start_date.broadcast_start_day_in_week, broadcast_start_date.broadcast_start_day_long,
 broadcast_start_date.utc_start_day_date,  
 broadcast_start_date.utc_start_weekday, 
broadcast_start_date.utc_start_day_in_month,  broadcast_start_date.utc_start_day_in_week,
broadcast_start_date.utc_start_day_long ,
 --viewing start date
 viewing_start_date.viewing_start_datehour_utc,  viewing_start_date.viewing_start_day_date, 
viewing_start_date.viewing_start_weekday, viewing_start_date.viewing_start_day_in_month, 
viewing_start_date.viewing_start_day_in_week, viewing_start_date.viewing_start_day_long,
 viewing_start_date.utc_viewing_start_day_date, viewing_start_date.utc_viewing_start_weekday, 
viewing_start_date.utc_viewing_start_day_in_month,  viewing_start_date.utc_viewing_start_day_in_week,
viewing_start_date.utc_viewing_start_day_long,
 --local start date
  local_start_date.local_start_day_date, local_start_date.local_start_weekday, 
local_start_date.local_start_day_in_month, local_start_date.local_start_day_in_week,
local_start_date.local_start_day_long
  from #adsmart_slots adsmart_slots
left outer join
(select pk_timeshift_dim, viewing_type, timeshift_band,elapsed_days,elapsed_hours,
elapsed_hours_total from sk_prod.viq_time_shift) time_shift
on adsmart_slots.time_shift_key = time_shift.pk_timeshift_dim
left outer join
(select  advertiser_code, advertiser_name, agency_key, barb_sales_house_id, buyer_code, sales_house_name, sales_house_short_name,
buyer_name from sk_prod.DIM_AGENCY) agency
on adsmart_slots.agency_key = agency.agency_key
left outer join
(select broadcast_channel_key, channel_format,media_adsmart_status, vespa_channel_name
from sk_prod.DIM_BROADCAST_CHANNEL) broadcast_channel
on adsmart_slots.broadcast_channel_key = broadcast_channel.broadcast_channel_key
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key preceding_programme_broadcast_start_date_key,
broadcast_start_time_key preceding_programme_broadcast_start_time_key,  
broadcast_end_date_key preceding_programme_broadcast_end_date_key, 
broadcast_end_time_key preceding_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) preceding_broadcast_programme
on
adsmart_slots.preceding_programme_schedule_key = preceding_broadcast_programme.broadcast_programme_schedule_key
left outer join
(select  broadcast_programme_schedule_key, 
broadcast_start_date_key succ_programme_broadcast_start_date_key,
broadcast_start_time_key succ_programme_broadcast_start_time_key,  
broadcast_end_date_key succ_programme_broadcast_end_date_key, 
broadcast_end_time_key succ_programme_broadcast_end_time_key
from sk_prod.DIM_BROADCAST_PROGRAMME_SCHEDULE) succ_broadcast_programme
on
adsmart_slots.succeeding_programme_schedule_key = succ_broadcast_programme.broadcast_programme_schedule_key
left outer join
(select segment_key, segment_id, segment_name, 
segment_status, segment_description from sk_prod.DIM_SEGMENT) segment
on
adsmart_slots.segment_key = segment.segment_key
left outer join
(select slot_copy_key,slot_copy_duration_seconds, slot_type, 
product_code, product_name  from sk_prod.DIM_SLOT_COPY) slot_copy
on
adsmart_slots.slot_copy_key = slot_copy.slot_copy_key
left outer join
(select slot_reference_key, slot_type slot_reference_slot_type, 
slot_sub_type, slot_duration_seconds,slot_duration_reported_Seconds, spot_position_in_break,
slot_type_position, slot_type_total_position, break_position, adsmart_action, adsmart_priority, 
adsmart_status, adsmart_total_priority from sk_prod.DIM_SLOT_REFERENCE) slot_reference
on
adsmart_slots.slot_reference_key = slot_reference.slot_reference_key
left outer join
(select  pk_time_dim, utc_time_minute broadcast_start_utc_time, 
broadcast_time start_broadcast_time, spot_standard_daypart_uk start_spot_standard_daypart_uk
from sk_prod.viq_time) broadcast_start_time
on
adsmart_slots.broadcast_start_time_key = broadcast_start_time.pk_time_dim
left outer join
(select pk_time_dim, utc_time_minute viewing_start_utc_time, 
broadcast_time viewing_start_broadcast_time, spot_standard_daypart_uk  
viewing_start_spot_standard_daypart_uk
from sk_prod.viq_time) viewing_start_time
on
adsmart_slots.viewed_start_time_key = viewing_start_time.pk_time_dim
left outer join
(select pk_datehour_dim, utc_datehour broadcast_start_datehour_utc, broadcast_day_date broadcast_start_day_date, 
broadcast_weekday broadcast_start_weekday,broadcast_day_in_month broadcast_start_day_in_month, 
broadcast_day_in_week broadcast_start_day_in_week,broadcast_day_long broadcast_start_day_long,
utc_day_date utc_start_day_date, utc_weekday utc_start_weekday, 
utc_day_in_month utc_start_day_in_month, utc_day_in_week utc_start_day_in_week,
 utc_day_long utc_start_day_long from sk_prod.viq_date) broadcast_start_date
on
 adsmart_slots.broadcast_start_date_key = broadcast_start_date.pk_datehour_dim
left outer join 
(select pk_datehour_dim, utc_datehour viewing_start_datehour_utc, broadcast_day_date viewing_start_day_date, 
broadcast_weekday viewing_start_weekday,broadcast_day_in_month viewing_start_day_in_month, 
broadcast_day_in_week viewing_start_day_in_week,broadcast_day_long viewing_start_day_long,
utc_day_date utc_viewing_start_day_date, utc_weekday utc_viewing_start_weekday, 
utc_day_in_month utc_viewing_start_day_in_month, utc_day_in_week utc_viewing_start_day_in_week,
 utc_day_long utc_viewing_start_day_long from sk_prod.viq_date) viewing_start_date
on
 adsmart_slots.viewed_start_date_key = viewing_start_date.pk_datehour_dim
left outer join 
(select pk_datehour_dim,   local_day_date local_start_day_date, local_weekday local_start_weekday, 
local_day_in_month local_start_day_in_month, local_day_in_week local_start_day_in_week,
local_day_long local_start_day_long from sk_prod.viq_date) local_start_date
on
  adsmart_slots.broadcast_start_date_key = local_start_date.pk_datehour_dim

commit

set @slot_cnt = null

set @slot_cnt = (select count(1) from  DATA_QUALITY_ADSMART_SLOT_DATA_AUDIT)

if @slot_cnt = 0
begin
EXECUTE logger_add_event @RunID ,2,'Refresh of Adsmart Slot Data Audit table not worked',@slot_cnt

end

if @slot_cnt > 0
begin
EXECUTE logger_add_event @RunID , 3,'Count of records in Adsmart Slot Data Audit table',@slot_cnt
end


-----------------------add records for campaign---------------------------------------

truncate table data_quality_adsmart_campaign_data_audit

commit

select ADSMART_CAMPAIGN_KEY, AGENCY_KEY, SEGMENT_KEY, 
campaign_actual_impressions, campaign_actual_impressions_day_one_weighted,
campaign_actual_serves, campaign_days_run, campaign_percentage_campaign_run, campaign_percentage_target_achieved,
campaign_Sample_impressions, campaign_sample_serves, campaign_segment_measurement_panel,
campaign_Segment_universe_size_day_one_weighted, campaign_target_impressions, campaign_tracking_index
into #fact_adsmart_campaign 
FROM sk_prod.FACT_ADSMART_CAMPAIGN

insert into data_quality_adsmart_campaign_data_audit
(ADSMART_CAMPAIGN_KEY, AGENCY_KEY, SEGMENT_KEY, 
campaign_actual_impressions, campaign_actual_impressions_day_one_weighted,
campaign_actual_serves, campaign_days_run, campaign_percentage_campaign_run, campaign_percentage_target_achieved,
campaign_Sample_impressions, campaign_sample_serves, campaign_segment_measurement_panel,
campaign_Segment_universe_size_day_one_weighted,campaign_target_impressions,campaign_tracking_index,
adsmart_campaign_code,adsmart_campaign_active_length, adsmart_campaign_budget, 
adsmart_campaign_start_date, adsmart_campaign_end_date,adsmart_campaign_status, 
break_spacing, daily_business_pvr_cap, daily_tech_pvr_cap, 
total_business_pvr_cap, total_tech_pvr_cap,
segment_status, segment_description)
select fact.ADSMART_CAMPAIGN_KEY, fact.AGENCY_KEY, fact.SEGMENT_KEY, 
fact.campaign_actual_impressions, fact.campaign_actual_impressions_day_one_weighted,
fact.campaign_actual_serves, fact.campaign_days_run, fact.campaign_percentage_campaign_run, fact.campaign_percentage_target_achieved,
fact.campaign_Sample_impressions, fact.campaign_sample_serves, fact.campaign_segment_measurement_panel,
fact.campaign_Segment_universe_size_day_one_weighted, fact.campaign_target_impressions, fact.campaign_tracking_index,
dim_campaign.adsmart_campaign_code,dim_campaign.adsmart_campaign_active_length, dim_campaign.adsmart_campaign_budget, 
dim_campaign.adsmart_campaign_start_date, dim_campaign.adsmart_campaign_end_date,dim_campaign.adsmart_campaign_status, 
dim_campaign.break_spacing, dim_campaign.daily_business_pvr_cap, dim_campaign.daily_tech_pvr_cap, 
dim_campaign.total_business_pvr_cap, dim_campaign.total_tech_pvr_cap,
dim_segment.segment_status, dim_segment.segment_description 
from #fact_adsmart_campaign fact
left outer join
(select  adsmart_campaign_key, adsmart_campaign_code,adsmart_campaign_active_length, adsmart_campaign_budget, 
adsmart_campaign_start_date, adsmart_campaign_end_date,adsmart_campaign_status, 
break_spacing, daily_business_pvr_cap, daily_tech_pvr_cap, total_business_pvr_cap, total_tech_pvr_cap
from sk_prod.dim_adsmart_campaign) dim_campaign
on fact.adsmart_campaign_key = dim_campaign.adsmart_campaign_key
left outer join
(select segment_id, segment_key, segment_status, segment_description 
from sk_prod.dim_segment) dim_segment
on
fact.segment_key = dim_Segment.segment_key

commit

set @camp_cnt = null

set @camp_cnt = (select count(1) from  data_quality_adsmart_campaign_data_audit)

if @camp_cnt = 0
begin
EXECUTE logger_add_event @RunID ,2,'Refresh of Adsmart Campaign Data Audit table not worked',@camp_cnt

end

if @camp_cnt > 0
begin
EXECUTE logger_add_event @RunID , 3,'Count of records in Adsmart Campaign Data Audit table',@camp_cnt
end

-----------------------------------------------------------------------------------------------------------------------------

-----------------------add records for segment---------------------------------------

truncate table data_quality_adsmart_segment_data_audit

commit

select segment_key, segment_date_key, measurement_panel_billable_customer_accounts, 
measurement_panel_dth_active_viewing_cards, universe_size_billable_customer_accounts,
universe_size_dth_active_viewing_cards,
universe_size_weighted_billable_customer_accounts,
universe_size_weighted_dth_active_viewing_cards
into #fact_adsmart_segment
from 
sk_prod.FACT_ADSMART_SEGMENT

insert into data_quality_adsmart_segment_data_audit
(segment_key, segment_date_key, measurement_panel_billable_customer_accounts, 
measurement_panel_dth_active_viewing_cards, universe_size_billable_customer_accounts,
universe_size_dth_active_viewing_cards,
universe_size_weighted_billable_customer_accounts,
universe_size_weighted_dth_active_viewing_cards,
segment_status, segment_description)
select fact.segment_key, fact.segment_date_key, fact.measurement_panel_billable_customer_accounts, 
fact.measurement_panel_dth_active_viewing_cards, fact.universe_size_billable_customer_accounts,
fact.universe_size_dth_active_viewing_cards,
fact.universe_size_weighted_billable_customer_accounts,
fact.universe_size_weighted_dth_active_viewing_cards,
dim_segment.segment_status, dim_segment.segment_description
from 
#fact_adsmart_segment fact
left outer join
(select segment_id, segment_key, segment_status, segment_description 
from sk_prod.dim_segment) dim_segment
on
fact.segment_key = dim_segment.segment_key

commit

set @seg_cnt = null

set @seg_cnt = (select count(1) from  data_quality_adsmart_segment_data_audit)

if @seg_cnt = 0
begin
EXECUTE logger_add_event @RunID ,2,'Refresh of Adsmart Segment Data Audit table not worked',@seg_cnt

end

if @seg_cnt > 0
begin
EXECUTE logger_add_event @RunID , 3,'Count of records in Adsmart Segment Data Audit table',@seg_cnt
end

---------------------------------------------------------------------------------------------------------------------------

----------------------------------------load in household table------------------------------------------------------------

truncate table data_quality_adsmart_hh_data_audit

commit

select account_number, household_key, segment_date_key, segment_key
into #fact_household_segment_date
from sk_prod.FACT_HOUSEHOLD_SEGMENT
where segment_date_key between @analysis_min_date and @analysis_max_date
and household_key > 0


insert into data_quality_adsmart_hh_data_audit
(account_number, household_key, segment_date_key, segment_key,
segment_status, segment_description,
hh_has_adsmart_stb, no_of_adsmart_stb)
select fact.account_number, fact.household_key, fact.segment_date_key, fact.segment_key,
dim_segment.segment_status, dim_segment.segment_description,
hh.hh_has_adsmart_stb, hh.no_of_adsmart_stb 
from 
#fact_household_segment_date fact
left outer join
(select segment_id, segment_key, segment_status, segment_description 
from sk_prod.dim_segment) dim_segment
on fact.segment_key = dim_Segment.segment_key
left outer join
(select household_key, hh_has_adsmart_stb, no_of_adsmart_stb from sk_prod.viq_household) hh
on
fact.household_key = hh.household_key

commit

set @hh_cnt = null

set @hh_cnt = (select count(1) from  data_quality_adsmart_hh_data_audit)

if @hh_cnt = 0
begin
EXECUTE logger_add_event @RunID ,2,'Refresh of Adsmart Household Data Audit table not worked',@hh_cnt

end

if @hh_cnt > 0
begin
EXECUTE logger_add_event @RunID , 3,'Count of records in Adsmart Segment Data Audit table',@hh_cnt
end

/*
----------------------------------C01 - Adsmart Basic Checks--------------------------------------------------

execute Data_Quality_Adsmart_Basic_Checks 'ADSMART_DATA_QUALITY',@analysis_date_current,@CP2_build_ID
    
	
-----------_------------------------ADSMART BASIC CHECKS END----------------------------------------------------	

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

EXECUTE logger_add_event @RunID , 3,'Count of Adsmart metrics collected so far for '||cast (@analysis_date_current as varchar(20)),@data_count
	

execute Data_Quality_Adsmart_Metric_Run @analysis_date_current,@CP2_build_ID

	
set @analysis_date_current = @analysis_date_current + 1


EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart running on'||@fact_tbl_name||'',@data_count


EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart Process End for Date '||cast (@analysis_date_current as varchar(20))

*/

end

--EXECUTE logger_add_event @RunID , 3,'Data Quality Adsmart Process End', @CP2_build_ID

commit


end


go

grant execute on data_quality_adsmart_rolling_day_processing to vespa_group_low_security, sk_prodreg, buxceys, kinnairt
