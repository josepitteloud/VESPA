
declare @date_val date
declare @min_broadcast_date_hour bigint
declare @max_broadcast_date_hour bigint
declare @slot_date int
declare @analysis_date_current date
declare @batch_date date


--create batch date to go into analysis table

set @batch_date = (select a.local_day_date from sk_prod.viq_date a,
(select max(broadcast_start_date_key) start_date_key from sk_prod.fact_adsmart_slot_instance) b
where a.pk_datehour_dim = b.start_date_key)

--create temp table to store results by day in

create table #tmp_results
(source varchar(25),
broadcast_date date,
data_area varchar(40),
data_count int,
records_with_impression int,
records_with_no_impression int,
ACTUAL_IMPRESSIONS_SUM decimal)

--select the dates that you want to use to produce the results

select distinct 'date' val, broadcast_day_date date_val
into #tmp_dqvm
from sk_prod.viq_date
where broadcast_day_date between '2013-12-18' and today()

SELECT date_val into #temp FROM #tmp_dqvm


-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @date_val   = date_val  from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where date_val  = @date_val   -- delete that uid from the temp table

set @analysis_date_current = @date_val

---get min and max broadcast start date hours for each utc_date

select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
into #tmp_date_hours
from 
sk_prod.viq_date
where local_day_date = @analysis_date_current

set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)
set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)

set @slot_date = cast(replace(@analysis_date_current,'-','') as int)

-----get slots from both instance and instance history tables

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day, 'adsmart_slots' chk, count(1) total, 
sum(case when actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(actual_impressions) Actal_impressions_sum
from sk_prod.fact_adsmart_Slot_instance
WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select 'slot_instance_history',@analysis_date_current broadcast_day, 'adsmart_slots' chk, count(1) total, 
sum(case when actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(actual_impressions) Actal_impressions_sum
from sk_prod.fact_adsmart_Slot_instance_history
WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0

--2) count of segments

--get segments from both instance and instance_history tables

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_Segments' chk, count(distinct segment_key) total,0 TOTAL1,0 total2,0 total3 
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select 'slot_instance_history',@analysis_date_current broadcast_day,'adsmart_Segments' chk, count(distinct segment_key) total,0 TOTAL1,0 total2,0 total3 
from sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0

----get campaigns from both instance and instance_history tables


--3) count of campaigns

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_campaigns' chk, count(distinct adsmart_campaign_key) total,0 TOTAL1,0 total2,0 total3
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select 'slot_instance_history',@analysis_date_current broadcast_day,'adsmart_campaigns' chk, count(distinct adsmart_campaign_key) total,0 TOTAL1,0 total2,0 total3
from sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0

---get households count for relevant date

--4 count of households

insert into #tmp_results
select 'households',@analysis_date_current broadcast_day,'adsmart_households' chk, sum(case when household_key > 0 then 1 else 0 end) total, count(1) TOTAL1,0 total2,0 total3
from sk_prod.FACT_HOUSEHOLD_SEGMENT
where segment_date_key = @slot_date

end


insert into data_quality_slots_daily_reporting
(date_type ,batch_date,date_value,slots_totals,actual_impressions ,segments_totals, 
households_totals,campaigns_totals ,load_timestamp )
select distinct 'local_date',@batch_date, date1.broadcast_date,a.slots_totals,a.actual_impressions,
b.segments_totals, c.households_totals,
d.campaigns_totals, getdate()  from #tmp_results date1
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_slots' then data_count else 0 end) slots_totals,
sum(case when data_area = 'adsmart_slots' then actual_impressions_sum else 0 end) actual_impressions from 
#tmp_results
group by broadcast_date, data_area) a
where slots_totals > 0) a
on date1.broadcast_date = a.broadcast_date
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_Segments' then data_count else 0 end) segments_totals
from #tmp_results
group by broadcast_date, data_area) b
where segments_totals > 0) b
on date1.broadcast_date = b.broadcast_date
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_households' then data_count else 0 end) households_totals
from #tmp_results
group by broadcast_date, data_area) c
where households_totals > 0) c
on date1.broadcast_date = c.broadcast_date
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_campaigns' then data_count else 0 end) campaigns_totals
from #tmp_results
group by broadcast_date, data_area) d
where campaigns_totals > 0) d
on date1.broadcast_date = d.broadcast_date
order by date1.broadcast_date

commit

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


declare @date_val date
declare @min_broadcast_date_hour bigint
declare @max_broadcast_date_hour bigint
declare @slot_date int
declare @analysis_date_current date
declare @batch_date date


--create batch date to go into analysis table

set @batch_date = (select a.local_day_date from sk_prod.viq_date a,
(select max(broadcast_start_date_key) start_date_key from sk_prod.fact_adsmart_slot_instance) b
where a.pk_datehour_dim = b.start_date_key)

--create temp table to store results by day in

create table #tmp_results
(source varchar(25),
broadcast_date date,
data_area varchar(40),
data_count int,
records_with_impression int,
records_with_no_impression int,
ACTUAL_IMPRESSIONS_SUM decimal)

--select the dates that you want to use to produce the results

select distinct 'date' val, broadcast_day_date date_val
into #tmp_dqvm
from sk_prod.viq_date
where broadcast_day_date between '2013-12-18' and today()

SELECT date_val into #temp FROM #tmp_dqvm


-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @date_val   = date_val  from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where date_val  = @date_val   -- delete that uid from the temp table

set @analysis_date_current = @date_val

---get min and max broadcast start date hours for each utc_date

select min(pk_datehour_dim)min_broadcast_date_hour, max(pk_datehour_dim) max_broadcast_date_hour 
into #tmp_date_hours
from 
sk_prod.viq_date
where broadcast_day_date = @analysis_date_current

set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)
set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)

set @slot_date = cast(replace(@analysis_date_current,'-','') as int)

-----get slots from both instance and instance history tables

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day, 'adsmart_slots' chk, count(1) total, 
sum(case when actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(actual_impressions) Actal_impressions_sum
from sk_prod.fact_adsmart_Slot_instance
WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select 'slot_instance_history',@analysis_date_current broadcast_day, 'adsmart_slots' chk, count(1) total, 
sum(case when actual_impressions > 0 then 1 else 0 end) records_with_impression,
sum(case when actual_impressions <= 0 then 1 else 0 end) records_with_no_impression,
sum(actual_impressions) Actal_impressions_sum
from sk_prod.fact_adsmart_Slot_instance_history
WHERE BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0

--2) count of segments

--get segments from both instance and instance_history tables

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_Segments' chk, count(distinct segment_key) total,0 TOTAL1,0 total2,0 total3 
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select 'slot_instance_history',@analysis_date_current broadcast_day,'adsmart_Segments' chk, count(distinct segment_key) total,0 TOTAL1,0 total2,0 total3 
from sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0

----get campaigns from both instance and instance_history tables


--3) count of campaigns

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_campaigns' chk, count(distinct adsmart_campaign_key) total,0 TOTAL1,0 total2,0 total3
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select 'slot_instance_history',@analysis_date_current broadcast_day,'adsmart_campaigns' chk, count(distinct adsmart_campaign_key) total,0 TOTAL1,0 total2,0 total3
from sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0

---get households count for relevant date

--4 count of households

insert into #tmp_results
select 'households',@analysis_date_current broadcast_day,'adsmart_households' chk, sum(case when household_key > 0 then 1 else 0 end) total, count(1) TOTAL1,0 total2,0 total3
from sk_prod.FACT_HOUSEHOLD_SEGMENT
where segment_date_key = @slot_date

end

insert into data_quality_slots_daily_reporting
(date_type ,batch_date,date_value,slots_totals,actual_impressions ,segments_totals, 
households_totals,campaigns_totals ,load_timestamp )
select distinct 'broadcast_date',@batch_date, date1.broadcast_date,a.slots_totals,a.actual_impressions,
b.segments_totals, c.households_totals,
d.campaigns_totals, getdate()  from #tmp_results date1
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_slots' then data_count else 0 end) slots_totals,
sum(case when data_area = 'adsmart_slots' then actual_impressions_sum else 0 end) actual_impressions from 
#tmp_results
group by broadcast_date, data_area) a
where slots_totals > 0) a
on date1.broadcast_date = a.broadcast_date
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_Segments' then data_count else 0 end) segments_totals
from #tmp_results
group by broadcast_date, data_area) b
where segments_totals > 0) b
on date1.broadcast_date = b.broadcast_date
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_households' then data_count else 0 end) households_totals
from #tmp_results
group by broadcast_date, data_area) c
where households_totals > 0) c
on date1.broadcast_date = c.broadcast_date
left outer join
(select * from 
(select broadcast_date, sum(case when data_area = 'adsmart_campaigns' then data_count else 0 end) campaigns_totals
from #tmp_results
group by broadcast_date, data_area) d
where campaigns_totals > 0) d
on date1.broadcast_date = d.broadcast_date
order by date1.broadcast_date

commit


