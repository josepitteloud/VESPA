if object_id('data_quality_BI_checks_reporting') is not null drop procedure data_quality_BI_checks_reporting 
commit

go

create procedure kinnairt.data_quality_BI_checks_reporting
as
begin
  declare @date_val date
  declare @min_broadcast_date_hour bigint
  declare @max_broadcast_date_hour bigint
  declare @slot_date integer
  declare @analysis_date_current date
  declare @batch_date date
  set @batch_date = (select a.local_day_date from sk_prod.viq_date as a
        ,(select start_date_key=max(broadcast_start_date_key) from sk_prod.fact_adsmart_slot_instance) as b
      where a.pk_datehour_dim = b.start_date_key)
  create table #tmp_results(
    source varchar(25) null,
    broadcast_date date null,
    data_area varchar(40) null,
    data_count integer null,
    records_with_impression integer null,
    records_with_no_impression integer null,
    ACTUAL_IMPRESSIONS_SUM decimal null,
    )
  select distinct val='date',date_val=broadcast_day_date
    into #tmp_dqvm
    from sk_prod.viq_date
--    where broadcast_day_date between '2013-12-18' and today()
	where broadcast_day_date between '2014-01-19' and today()
  select date_val into #temp from #tmp_dqvm
  while exists(select 1 from #temp)
    begin
      set rowcount 1
      select @date_val = date_val from #temp
      set rowcount 0
      delete from #temp where date_val = @date_val
      set @analysis_date_current = @date_val
      select min_broadcast_date_hour=min(pk_datehour_dim),max_broadcast_date_hour=max(pk_datehour_dim)
        into #tmp_date_hours
        from sk_prod.viq_date
        where local_day_date = @analysis_date_current
      set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)
      set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)
      set @slot_date = convert(integer,replace(@analysis_date_current,'-',''))
      insert into #tmp_results
        select 'slot_instance',broadcast_day=@analysis_date_current,chk='adsmart_slots',total=count(1),
          records_with_impression=sum(case when actual_impressions > 0 then 1 else 0 end),
          records_with_no_impression=sum(case when actual_impressions <= 0 then 1 else 0 end),
          Actal_impressions_sum=sum(actual_impressions)
          from sk_prod.fact_adsmart_Slot_instance
          where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
          and @max_broadcast_date_hour and adsmart_campaign_key > 0 union all
        select 'slot_instance_history',broadcast_day=@analysis_date_current,chk='adsmart_slots',total=count(1),
          records_with_impression=sum(case when actual_impressions > 0 then 1 else 0 end),
          records_with_no_impression=sum(case when actual_impressions <= 0 then 1 else 0 end),
          Actal_impressions_sum=sum(actual_impressions)
          from sk_prod.fact_adsmart_Slot_instance_history
          where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
          and @max_broadcast_date_hour and adsmart_campaign_key > 0

--get segments from both instance and instance_history tables

select distinct segment_key into #tmp_segments
from
(select distinct segment_key 
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select distinct segment_key from
sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0) a

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_Segments' chk, count(distinct segment_key) total,0 TOTAL1,0 total2,0 total3 
from 
#tmp_segments


----get campaigns from both instance and instance_history tables


--3) count of campaigns

select distinct adsmart_campaign_key into #tmp_campaigns
from
(select distinct adsmart_campaign_key
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select distinct adsmart_campaign_key from
sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0) a


insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_campaigns' chk, count(distinct adsmart_campaign_key) total,0 TOTAL1,0 total2,0 total3
from #tmp_campaigns





      insert into #tmp_results
        select 'households',broadcast_day=@analysis_date_current,chk='adsmart_households',total=sum(case when household_key > 0 then 1 else 0 end),TOTAL1=count(1),total2=0,total3=0
          from sk_prod.FACT_HOUSEHOLD_SEGMENT
          where segment_date_key = @slot_date
    end
  insert into data_quality_slots_daily_reporting
    ( date_type,batch_date,date_value,slots_totals,actual_impressions,segments_totals,
    households_totals,campaigns_totals,load_timestamp ) 
    select distinct 'local_date',@batch_date,date1.broadcast_date,a.slots_totals,a.actual_impressions,
      b.segments_totals,c.households_totals,
      d.campaigns_totals,getdate() from #tmp_results as date1
        left outer join(select *
          from(select broadcast_date,slots_totals=sum(case when data_area = 'adsmart_slots' then data_count else 0 end),
              actual_impressions=sum(case when data_area = 'adsmart_slots' then actual_impressions_sum else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as a
          where slots_totals > 0) as a
        on date1.broadcast_date = a.broadcast_date
        left outer join(select *
          from(select broadcast_date,segments_totals=sum(case when data_area = 'adsmart_Segments' then data_count else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as b
          where segments_totals > 0) as b
        on date1.broadcast_date = b.broadcast_date
        left outer join(select *
          from(select broadcast_date,households_totals=sum(case when data_area = 'adsmart_households' then data_count else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as c
          where households_totals > 0) as c
        on date1.broadcast_date = c.broadcast_date
        left outer join(select *
          from(select broadcast_date,campaigns_totals=sum(case when data_area = 'adsmart_campaigns' then data_count else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as d
          where campaigns_totals > 0) as d
        on date1.broadcast_date = d.broadcast_date
      order by date1.broadcast_date asc
  commit work
  set @batch_date = (select a.local_day_date from sk_prod.viq_date as a
        ,(select start_date_key=max(broadcast_start_date_key) from sk_prod.fact_adsmart_slot_instance) as b
      where a.pk_datehour_dim = b.start_date_key)
  drop table #tmp_results
  create table #tmp_results(
    source varchar(25) null,
    broadcast_date date null,
    data_area varchar(40) null,
    data_count integer null,
    records_with_impression integer null,
    records_with_no_impression integer null,
    ACTUAL_IMPRESSIONS_SUM decimal null,
    )
  drop table #tmp_dqvm
  drop table #temp
  select distinct val='date',date_val=broadcast_day_date
    into #tmp_dqvm
    from sk_prod.viq_date
--    where broadcast_day_date between '2013-12-18' and today()
	where broadcast_day_date between '2014-01-19' and today()

  select date_val into #temp from #tmp_dqvm
  while exists(select 1 from #temp)
    begin
      set rowcount 1
      select @date_val = date_val from #temp
      set rowcount 0
      delete from #temp where date_val = @date_val
      set @analysis_date_current = @date_val
      select min_broadcast_date_hour=min(pk_datehour_dim),max_broadcast_date_hour=max(pk_datehour_dim)
        into #tmp_date_hours
        from sk_prod.viq_date
        where broadcast_day_date = @analysis_date_current
      set @min_broadcast_date_hour = (select min_broadcast_date_hour from #tmp_date_hours)
      set @max_broadcast_date_hour = (select max_broadcast_date_hour from #tmp_date_hours)
      set @slot_date = convert(integer,replace(@analysis_date_current,'-',''))
      insert into #tmp_results
        select 'slot_instance',broadcast_day=@analysis_date_current,chk='adsmart_slots',total=count(1),
          records_with_impression=sum(case when actual_impressions > 0 then 1 else 0 end),
          records_with_no_impression=sum(case when actual_impressions <= 0 then 1 else 0 end),
          Actal_impressions_sum=sum(actual_impressions)
          from sk_prod.fact_adsmart_Slot_instance
          where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
          and @max_broadcast_date_hour and adsmart_campaign_key > 0 union all
        select 'slot_instance_history',broadcast_day=@analysis_date_current,chk='adsmart_slots',total=count(1),
          records_with_impression=sum(case when actual_impressions > 0 then 1 else 0 end),
          records_with_no_impression=sum(case when actual_impressions <= 0 then 1 else 0 end),
          Actal_impressions_sum=sum(actual_impressions)
          from sk_prod.fact_adsmart_Slot_instance_history
          where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
          and @max_broadcast_date_hour and adsmart_campaign_key > 0

--get segments from both instance and instance_history tables

select distinct segment_key into #tmp_segments
from
(select distinct segment_key 
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select distinct segment_key from
sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0) a

insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_Segments' chk, count(distinct segment_key) total,0 TOTAL1,0 total2,0 total3 
from 
#tmp_segments


----get campaigns from both instance and instance_history tables


--3) count of campaigns

select distinct adsmart_campaign_key into #tmp_campaigns
from
(select distinct adsmart_campaign_key
from sk_prod.fact_adsmart_Slot_instance
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0
union all
select distinct adsmart_campaign_key from
sk_prod.fact_adsmart_Slot_instance_history
where BROADCAST_START_DATE_KEY between @min_broadcast_date_hour
 and @max_broadcast_date_hour and adsmart_campaign_key > 0) a


insert into #tmp_results
select 'slot_instance',@analysis_date_current broadcast_day,'adsmart_campaigns' chk, count(distinct adsmart_campaign_key) total,0 TOTAL1,0 total2,0 total3
from #tmp_campaigns




      insert into #tmp_results
        select 'households',broadcast_day=@analysis_date_current,chk='adsmart_households',total=sum(case when household_key > 0 then 1 else 0 end),TOTAL1=count(1),total2=0,total3=0
          from sk_prod.FACT_HOUSEHOLD_SEGMENT
          where segment_date_key = @slot_date
    end
  insert into data_quality_slots_daily_reporting
    ( date_type,batch_date,date_value,slots_totals,actual_impressions,segments_totals,
    households_totals,campaigns_totals,load_timestamp ) 
    select distinct 'broadcast_date',@batch_date,date1.broadcast_date,a.slots_totals,a.actual_impressions,
      b.segments_totals,c.households_totals,
      d.campaigns_totals,getdate() from #tmp_results as date1
        left outer join(select *
          from(select broadcast_date,slots_totals=sum(case when data_area = 'adsmart_slots' then data_count else 0 end),
              actual_impressions=sum(case when data_area = 'adsmart_slots' then actual_impressions_sum else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as a
          where slots_totals > 0) as a
        on date1.broadcast_date = a.broadcast_date
        left outer join(select *
          from(select broadcast_date,segments_totals=sum(case when data_area = 'adsmart_Segments' then data_count else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as b
          where segments_totals > 0) as b
        on date1.broadcast_date = b.broadcast_date
        left outer join(select *
          from(select broadcast_date,households_totals=sum(case when data_area = 'adsmart_households' then data_count else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as c
          where households_totals > 0) as c
        on date1.broadcast_date = c.broadcast_date
        left outer join(select *
          from(select broadcast_date,campaigns_totals=sum(case when data_area = 'adsmart_campaigns' then data_count else 0 end)
              from #tmp_results
              group by broadcast_date,data_area) as d
          where campaigns_totals > 0) as d
        on date1.broadcast_date = d.broadcast_date
      order by date1.broadcast_date asc
  commit work
end


go

grant execute on data_quality_BI_checks_reporting to vespa_group_low_security, sk_prodreg
