/******************************************************************************
**
** Project Vespa: Data Quality BI Checks Reporting
**
** This is the procedure to collect the basic Adsmart metrics which are reported against...
**
**  
** Refer also to:
**
**
** Code sections:
**
**
**1)	Gather Metrics (except Household Metric See 5) below) by two different dates 
**	a.	Local Date – defined as midnight to 23.59.59 on any given date.
**	b.	Broadcast Date – defined as 06.00 on date 1 to 5.59.59 on date 2 so a 
**	broadcast date of 6th May 2014 starts at 6.00am on 06/05/2014 and ends at 05.59.59 on 07/05/2014.
**2)	Collect the total number of Adsmart only slots and the sum of Impressions against the Volatile and Static Adsmart data 
**	only back to an agreed date.  
**	Adsmart only defined as where an Adsmart Campaign Key > 0.  
**	There are some other calculations which are in this query but are not used as it was decided that they were not as crucial.
**3)	Collect the total number of Adsmart specific segments against the Volatile and Static Adsmart data only back to an agreed date.  
**	Adsmart only defined as where an Adsmart Campaign Key > 0.
**4)	Collect the total number of Adsmart specific campaigns against the Volatile and Static Adsmart data only back to an agreed date.  
**	Adsmart only defined as where an Adsmart Campaign Key > 0.
**5)	Collect the total number of Adsmart households against the fact_household_segment back to an agreed date.  
**	Only looks at those Household Segments which are matched to an 
**	Olive Household Key as the way these segments work there are a vast amount which do not get matched 
**	and the BI team are only interested in those that are matched (FACT_HOUSEHOLD_SEGMENT  where household_key > 0).
**
**
**      
**
**
******************************************************************************/


if object_id('data_quality_BI_checks_reporting') is not null drop procedure data_quality_BI_checks_reporting
commit

go

create procedure "data_quality_BI_checks_reporting"
as
begin
  declare @date_val date
  declare @min_broadcast_date_hour bigint
  declare @max_broadcast_date_hour bigint
  declare @slot_date integer
  declare @analysis_date_current date
  declare @batch_date date
  set @batch_date = (select "a"."local_day_date" from "sk_prod"."viq_date" as "a"
        ,(select 'start_date_key'="max"("broadcast_start_date_key") from "sk_prod"."fact_adsmart_slot_instance") as "b"
      where "a"."pk_datehour_dim" = "b"."start_date_key")
  create table #tmp_results(
    "source" varchar(25) null,
    "broadcast_date" date null,
    "data_area" varchar(40) null,
    "data_count" integer null,
    "records_with_impression" integer null,
    "records_with_no_impression" integer null,
    "ACTUAL_IMPRESSIONS_SUM" decimal null,
    )
  select distinct 'val'='date','date_val'="broadcast_day_date"
    into #tmp_dqvm
    from "sk_prod"."viq_date"
    where "broadcast_day_date" between '2014-06-01' and "today"()
  ---gather metrics by Local Date
  select 'min_broadcast_date_hour'="min"("pk_datehour_dim"),'max_broadcast_date_hour'="max"("pk_datehour_dim")
    into #tmp_date_hours
    from "sk_prod"."viq_date" as "a"
      ,#tmp_dqvm as "b"
    where "a"."local_day_date" = "b"."date_val"
  set @min_broadcast_date_hour = (select "min_broadcast_date_hour" from #tmp_date_hours)
  set @max_broadcast_date_hour = (select "max_broadcast_date_hour" from #tmp_date_hours)
  ---gather slots totals and sum of impressions by local_date
  insert into #tmp_results
    select 'slot_instance','broadcast_day'="b"."local_day_date",'chk'='adsmart_slots','total'="count"(1),
      'records_with_impression'="sum"(case when "actual_impressions" > 0 then 1 else 0 end),
      'records_with_no_impression'="sum"(case when "actual_impressions" <= 0 then 1 else 0 end),
      'Actal_impressions_sum'="sum"("actual_impressions")
      from "sk_prod"."viq_date" as "b"
        left outer join "sk_prod"."fact_adsmart_Slot_instance" as "a"
        on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
      where "a"."BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
      and @max_broadcast_date_hour and "adsmart_campaign_key" > 0
      group by "b"."local_day_date" union all
    select 'slot_instance_history','broadcast_day'="b"."local_day_date",'chk'='adsmart_slots','total'="count"(1),
      'records_with_impression'="sum"(case when "actual_impressions" > 0 then 1 else 0 end),
      'records_with_no_impression'="sum"(case when "actual_impressions" <= 0 then 1 else 0 end),
      'Actal_impressions_sum'="sum"("actual_impressions")
      from "sk_prod"."viq_date" as "b"
        left outer join "sk_prod"."fact_adsmart_Slot_instance_history" as "a"
        on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
      where "a"."BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
      and @max_broadcast_date_hour and "adsmart_campaign_key" > 0
      group by "b"."local_day_date"
  --get segments from both instance and instance_history tables
  select distinct "local_day_date","segment_key" into #tmp_segments
    from(select "b"."local_day_date","a"."segment_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0 union
      select "b"."local_day_date","a"."segment_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance_history" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0) as "a"
  insert into #tmp_results
    select 'slot_instance','broadcast_day'="local_day_date",'chk'='adsmart_Segments','total'="count"(distinct "segment_key"),'TOTAL1'=0,'total2'=0,'total3'=0
      from #tmp_segments
      group by "local_day_date"
  ----get campaigns from both instance and instance_history tables
  --3) count of campaigns
  select distinct "local_day_date","adsmart_campaign_key" into #tmp_campaigns
    from(select "b"."local_day_date","a"."adsmart_campaign_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0 union
      select "b"."local_day_date","a"."adsmart_campaign_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance_history" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0) as "a"
  insert into #tmp_results
    select 'slot_instance','broadcast_day'="local_day_date",'chk'='adsmart_campaigns','total'="count"(distinct "adsmart_campaign_key"),'TOTAL1'=0,'total2'=0,'total3'=0
      from #tmp_campaigns
      group by "local_day_date"
  insert into #tmp_results
    select 'households','broadcast_day'="b"."local_day_date",'chk'='adsmart_households',"count"("a"."cb_row_id"),'TOTAL1'="count"(1),'total2'=0,'total3'=0
      from(select distinct "local_day_date",'date_value'=convert(integer,convert(varchar(8),"local_day_date",112)) from "sk_prod"."viq_date") as "b"
        left outer join(select "segment_date_key","cb_row_id" from "sk_prod"."FACT_HOUSEHOLD_SEGMENT" where "household_key" > 0) as "a"
        on "b"."date_value" = "a"."segment_date_key"
        join #tmp_dqvm as "c"
        on "b"."local_day_date" = "c"."date_val"
      group by "b"."local_day_date"


  ---organise the data into the reporting table so that it can be stored and reported against in a consistent format
  insert into "data_quality_slots_daily_reporting"
    ( "date_type","batch_date","date_value","slots_totals","actual_impressions","segments_totals",
    "households_totals","campaigns_totals","load_timestamp" )
    select distinct 'local_date',@batch_date,"date1"."broadcast_date","a"."slots_totals","a"."actual_impressions",
      "b"."segments_totals","c"."households_totals",
      "d"."campaigns_totals","getdate"() from #tmp_results as "date1"
        left outer join(select *
          from(select "broadcast_date",'slots_totals'="sum"(case when "data_area" = 'adsmart_slots' then "data_count" else 0 end),
              'actual_impressions'="sum"(case when "data_area" = 'adsmart_slots' then "actual_impressions_sum" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "a"
          where "slots_totals" > 0) as "a"
        on "date1"."broadcast_date" = "a"."broadcast_date"
        left outer join(select *
          from(select "broadcast_date",'segments_totals'="sum"(case when "data_area" = 'adsmart_Segments' then "data_count" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "b"
          where "segments_totals" > 0) as "b"
        on "date1"."broadcast_date" = "b"."broadcast_date"
        left outer join(select *
          from(select "broadcast_date",'households_totals'="sum"(case when "data_area" = 'adsmart_households' then "data_count" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "c"
          where "households_totals" > 0) as "c"
        on "date1"."broadcast_date" = "c"."broadcast_date"
        left outer join(select *
          from(select "broadcast_date",'campaigns_totals'="sum"(case when "data_area" = 'adsmart_campaigns' then "data_count" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "d"
          where "campaigns_totals" > 0) as "d"
        on "date1"."broadcast_date" = "d"."broadcast_date"
      order by "date1"."broadcast_date" asc
  commit work
  set @batch_date = (select "a"."local_day_date" from "sk_prod"."viq_date" as "a"
        ,(select 'start_date_key'="max"("broadcast_start_date_key") from "sk_prod"."fact_adsmart_slot_instance") as "b"
      where "a"."pk_datehour_dim" = "b"."start_date_key")


  drop table #tmp_results


  create table #tmp_results(
    "source" varchar(25) null,
    "broadcast_date" date null,
    "data_area" varchar(40) null,
    "data_count" integer null,
    "records_with_impression" integer null,
    "records_with_no_impression" integer null,
    "ACTUAL_IMPRESSIONS_SUM" decimal null,
    )
  drop table #tmp_dqvm
  --drop table #temp
  ---gather metrics by Broadcast Date
  select distinct 'val'='date','date_val'="broadcast_day_date"
    into #tmp_dqvm
    from "sk_prod"."viq_date"
    where "broadcast_day_date" between '2014-06-01' and "today"()
  select 'min_broadcast_date_hour'="min"("pk_datehour_dim"),'max_broadcast_date_hour'="max"("pk_datehour_dim")
    into #tmp_date_hours
    from "sk_prod"."viq_date" as "a"
      ,#tmp_dqvm as "b"
    where "a"."broadcast_day_date" = "b"."date_val"
  set @min_broadcast_date_hour = (select "min_broadcast_date_hour" from #tmp_date_hours)
  set @max_broadcast_date_hour = (select "max_broadcast_date_hour" from #tmp_date_hours)
  ---gather slots totals and sum of impressions by local_date
  insert into #tmp_results
    select 'slot_instance','broadcast_day'="b"."broadcast_day_date",'chk'='adsmart_slots','total'="count"(1),
      'records_with_impression'="sum"(case when "actual_impressions" > 0 then 1 else 0 end),
      'records_with_no_impression'="sum"(case when "actual_impressions" <= 0 then 1 else 0 end),
      'Actal_impressions_sum'="sum"("actual_impressions")
      from "sk_prod"."viq_date" as "b"
        left outer join "sk_prod"."fact_adsmart_Slot_instance" as "a"
        on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
      where "a"."BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
      and @max_broadcast_date_hour and "adsmart_campaign_key" > 0
      group by "b"."broadcast_day_date" union all
    select 'slot_instance_history','broadcast_day'="b"."broadcast_day_date",'chk'='adsmart_slots','total'="count"(1),
      'records_with_impression'="sum"(case when "actual_impressions" > 0 then 1 else 0 end),
      'records_with_no_impression'="sum"(case when "actual_impressions" <= 0 then 1 else 0 end),
      'Actal_impressions_sum'="sum"("actual_impressions")
      from "sk_prod"."viq_date" as "b"
        left outer join "sk_prod"."fact_adsmart_Slot_instance_history" as "a"
        on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
      where "a"."BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
      and @max_broadcast_date_hour and "adsmart_campaign_key" > 0
      group by "b"."broadcast_day_date"
  --get segments from both instance and instance_history tables
  --2) get segments for broadcast_date
  drop table #tmp_segments
  select distinct "broadcast_day_date","segment_key" into #tmp_segments
    from(select "b"."broadcast_day_date","a"."segment_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0 union
      select "b"."broadcast_day_date","a"."segment_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance_history" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0) as "a"
  insert into #tmp_results
    select 'slot_instance','broadcast_day'="broadcast_day_date",'chk'='adsmart_Segments','total'="count"(distinct "segment_key"),'TOTAL1'=0,'total2'=0,'total3'=0
      from #tmp_segments
      group by "broadcast_day_date"
  ----get campaigns from both instance and instance_history tables
  --3) count of campaigns for broadcast_date
  drop table #tmp_campaigns
  select distinct "broadcast_day_date","adsmart_campaign_key" into #tmp_campaigns
    from(select "b"."broadcast_day_date","a"."adsmart_campaign_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0 union
      select "b"."broadcast_day_date","a"."adsmart_campaign_key"
        from "sk_prod"."viq_date" as "b"
          left outer join "sk_prod"."fact_adsmart_Slot_instance_history" as "a"
          on "a"."broadcast_start_date_key" = "b"."pk_datehour_dim"
        where "BROADCAST_START_DATE_KEY" between @min_broadcast_date_hour
        and @max_broadcast_date_hour and "adsmart_campaign_key" > 0) as "a"
  insert into #tmp_results
    select 'slot_instance','broadcast_day'="broadcast_day_date",'chk'='adsmart_campaigns','total'="count"(distinct "adsmart_campaign_key"),'TOTAL1'=0,'total2'=0,'total3'=0
      from #tmp_campaigns
      group by "broadcast_day_date"
  --households counts for broadcast_date
  insert into #tmp_results
    select 'households','broadcast_day'="b"."local_day_date",'chk'='adsmart_households',"count"("a"."cb_row_id"),'TOTAL1'="count"(1),'total2'=0,'total3'=0
      from(select distinct "local_day_date",'date_value'=convert(integer,convert(varchar(8),"local_day_date",112)) from "sk_prod"."viq_date") as "b"
        left outer join(select "segment_date_key","cb_row_id" from "sk_prod"."FACT_HOUSEHOLD_SEGMENT" where "household_key" > 0) as "a"
        on "b"."date_value" = "a"."segment_date_key"
        join #tmp_dqvm as "c"
        on "b"."local_day_date" = "c"."date_val"
      group by "b"."local_day_date"
  ---organise the data into the reporting table so that it can be stored and reported against in a consistent format
  insert into "data_quality_slots_daily_reporting"
    ( "date_type","batch_date","date_value","slots_totals","actual_impressions","segments_totals",
    "households_totals","campaigns_totals","load_timestamp" )
    select distinct 'broadcast_date',@batch_date,"date1"."broadcast_date","a"."slots_totals","a"."actual_impressions",
      "b"."segments_totals","c"."households_totals",
      "d"."campaigns_totals","getdate"() from #tmp_results as "date1"
        left outer join(select *
          from(select "broadcast_date",'slots_totals'="sum"(case when "data_area" = 'adsmart_slots' then "data_count" else 0 end),
              'actual_impressions'="sum"(case when "data_area" = 'adsmart_slots' then "actual_impressions_sum" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "a"
          where "slots_totals" > 0) as "a"
        on "date1"."broadcast_date" = "a"."broadcast_date"
        left outer join(select *
          from(select "broadcast_date",'segments_totals'="sum"(case when "data_area" = 'adsmart_Segments' then "data_count" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "b"
          where "segments_totals" > 0) as "b"
        on "date1"."broadcast_date" = "b"."broadcast_date"
        left outer join(select *
          from(select "broadcast_date",'households_totals'="sum"(case when "data_area" = 'adsmart_households' then "data_count" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "c"
          where "households_totals" > 0) as "c"
        on "date1"."broadcast_date" = "c"."broadcast_date"
        left outer join(select *
          from(select "broadcast_date",'campaigns_totals'="sum"(case when "data_area" = 'adsmart_campaigns' then "data_count" else 0 end)
              from #tmp_results
              group by "broadcast_date","data_area") as "d"
          where "campaigns_totals" > 0) as "d"
        on "date1"."broadcast_date" = "d"."broadcast_date"
      order by "date1"."broadcast_date" asc
  commit work
end

go

grant execute on data_quality_BI_checks_reporting to vespa_group_low_security, sk_prodreg, buxceys, kinnairt