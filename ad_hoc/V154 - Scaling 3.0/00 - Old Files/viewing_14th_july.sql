--Take all viewing data from 14th july
begin

if       object_id('V154_viewing_14th_july') is not null drop table V154_viewing_14th_july
SELECT
         pk_viewing_prog_instance_fact
        ,account_number
        ,subscriber_id
        ,programme_name
        ,channel_name
        ,live_recorded
        ,broadcast_start_date_time_utc
        ,broadcast_end_date_time_utc
        ,event_start_date_time_utc
        ,event_end_date_time_utc
        ,instance_start_date_time_utc
        ,case when (capping_end_date_time_utc is null or instance_end_date_time_utc < capping_end_date_time_utc)
            then instance_end_date_time_utc else capping_end_date_time_utc end as instance_end_date_time_utc
        ,datediff(second, instance_start_date_time_utc, instance_end_date_time_utc) as duration
  INTO        V154_viewing_14th_july
        FROM  sk_prod.VESPA_DP_PROG_VIEWED_201307
        WHERE panel_id = 12
          and day(broadcast_start_date_time_utc) = 14
          AND type_of_viewing_event             <> 'Non viewing event'
         AND (capping_end_date_time_utc IS NULL
            OR capping_end_date_time_utc > broadcast_start_date_time_utc)
         and (lower(channel_name) like '%bbc%' or lower(channel_name) like '%itv%' or lower(channel_name) like '%sky%')
         and duration            > 6
         AND subscriber_id       IS NOT NULL
         AND account_number      IS NOT NULL
         AND account_number      NOT IN (SELECT account_number
                                FROM vespa_analysts.accounts_to_exclude)
commit
end

--Find popular shows, look at viewers and viewing duration
if           object_id('V154_progs_14th_july') is not null drop table V154_progs_14th_july
select       programme_name
            ,sum(datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)) as duration
            ,count(distinct subscriber_id) as viewers
        into V154_progs_14th_july
        from V154_viewing_14th_july
    group by programme_name
    order by duration desc
commit

select top 20 *, 1.0*duration/viewers as ave_time from V154_progs_14th_july order by viewers desc
commit

--As we want to look at spot data select 'Law  Order UK'
--Using TechEdge we pick the first two spots in the second ad break of the show
--The times are as follows:
--20:36:43 - 20:37:13
--20:37:13 - 20:37:43
-- Check minute attribution metric at mid-point of The White Queen
declare @spot_time1        datetime
declare @spot_time2        datetime
declare @spot_time3        datetime
set     @spot_time1        = '2013-07-14 20:36:43.000000'
set     @spot_time2        = '2013-07-14 20:37:13.000000'
set     @spot_time3        = '2013-07-14 20:37:43.000000'
commit

--Create table with all viewers of 'Law Order UK' for first two spots
if           object_id('V154_law_order_spot1') is not null drop table V154_law_order_spot1
select *
         into V154_law_order_spot1
         from V154_viewing_14th_july
        where (programme_name like '%Order%' and programme_name like 'Law%' and programme_name like '%UK')
          and (event_start_date_time_utc <= @spot_time1 and event_end_date_time_utc >= @spot_time2)
          and  live_recorded = 'LIVE'
commit
if           object_id('V154_law_order_spot2') is not null drop table V154_law_order_spot2
select *
         into V154_law_order_spot2
         from V154_viewing_14th_july
        where (programme_name like '%Order%' and programme_name like 'Law%' and programme_name like '%UK')
          and (event_start_date_time_utc <= @spot_time2 and event_end_date_time_utc >= @spot_time3)
          and  live_recorded = 'LIVE'
commit

--Add scaling segment id's and scaling variables to V154_law_order_spot1 and V154_law_order_spot2
if           object_id('V154_law_order_spot1_updated') is not null drop table V154_law_order_spot1_updated
select a.*, isba_tv_region, hhcomposition, package, tenure, boxtype, sky_base_universe, updated_scaling_segment
          into  V154_law_order_spot1_updated
          from  V154_law_order_spot1 a
    inner join  (
        select  a.account_number, b.isba_tv_region, b.hhcomposition, b.package, b.tenure, b.boxtype, a.weighting_universe as sky_base_universe, b.updated_scaling_segment
                from V154_accounts_proxy_consent a
          inner join V154_accounts_aggregated b
                  on a.isba_tv_region = b.isba_tv_region
                 and a.hhcomposition  = b.hhcomposition
                 and a.package        = b.package
                 and a.tenure         = b.tenure
                 and a.boxtype        = b.boxtype
                 and a.weighting_universe = b.sky_base_universe) as sub
           on    a.account_number = sub.account_number
commit
if           object_id('V154_law_order_spot2_updated') is not null drop table V154_law_order_spot2_updated
select a.*, isba_tv_region, hhcomposition, package, tenure, boxtype, sky_base_universe, updated_scaling_segment
          into  V154_law_order_spot2_updated
          from  V154_law_order_spot2 a
    inner join  (
        select  a.account_number, b.isba_tv_region, b.hhcomposition, b.package, b.tenure, b.boxtype, a.weighting_universe as sky_base_universe, b.updated_scaling_segment
                from V154_accounts_proxy_consent a
          inner join V154_accounts_aggregated b
                  on a.isba_tv_region = b.isba_tv_region
                 and a.hhcomposition  = b.hhcomposition
                 and a.package        = b.package
                 and a.tenure         = b.tenure
                 and a.boxtype        = b.boxtype
                 and a.weighting_universe = b.sky_base_universe) as sub
           on    a.account_number = sub.account_number
commit

--Add information relating to updated weights and old weights
alter table V154_law_order_spot1_updated
        add(
             updated_segment_weight real
            ,old_scaling_segment_id real
            ,old_segment_weight real)
alter table V154_law_order_spot2_updated
        add(
             updated_segment_weight real
            ,old_scaling_segment_id real
            ,old_segment_weight real)
update      V154_law_order_spot1_updated a
        set a.updated_segment_weight = b.segment_weight
       from SC3_weighting_working_table b
      where a.updated_scaling_segment = b.scaling_segment_id
update      V154_law_order_spot2_updated a
        set a.updated_segment_weight = b.segment_weight
       from SC3_weighting_working_table b
      where a.updated_scaling_segment = b.scaling_segment_id
commit

--Try to find old scaling segment ids.
--Need to create table containing old segments as we need to ensure that universe is correct
select *
       into #temp_segments_lookup
       from vespa_analysts.SC2_Segments_Lookup_v2_1
      where universe like 'A)%' and boxtype like 'A)%'
         or universe like 'A)%' and boxtype like 'B)%'
         or universe like 'A)%' and boxtype like 'C)%'
         or universe like 'A)%' and boxtype like 'D)%'
         or universe like 'B)%' and boxtype like 'E)%'
         or universe like 'B)%' and boxtype like 'F)%'
         or universe like 'B)%' and boxtype like 'G)%'
         or universe like 'B)%' and boxtype like 'H)%'
         or universe like 'B)%' and boxtype like 'I)%'
         or universe like 'B)%' and boxtype like 'J)%'
         or universe like 'B)%' and boxtype like 'K)%'
         or universe like 'B)%' and boxtype like 'L)%'
         or universe like 'B)%' and boxtype like 'M)%'

update      V154_law_order_spot1_updated a
        set a.old_scaling_segment_id = b.scaling_segment_id
       from #temp_segments_lookup b
      where a.isba_tv_region = b.isba_tv_region
        and a.hhcomposition  = b.hhcomposition
        and a.package        = b.package
        and a.tenure         = b.tenure
        and a.boxtype        = b.boxtype
update      V154_law_order_spot2_updated a
        set a.old_scaling_segment_id = b.scaling_segment_id
       from #temp_segments_lookup b
      where a.isba_tv_region = b.isba_tv_region
        and a.hhcomposition  = b.hhcomposition
        and a.package        = b.package
        and a.tenure         = b.tenure
        and a.boxtype        = b.boxtype
commit
--Link old scaling segment ids with the weights for the day in question
declare @spot_time1        datetime
set     @spot_time1        = '2013-07-14 20:36:43.000000'
declare @scaling_date      date
set     @scaling_date      = date(@spot_time1)

--Variables calculated automatically from above values
update      V154_law_order_spot1_updated a
        set a.old_segment_weight = b.weighting
       from vespa_analysts.SC2_Weightings b
      where a.old_scaling_segment_id = b.scaling_segment_id
        and scaling_day = @scaling_date
update      V154_law_order_spot2_updated a
        set a.old_segment_weight = b.weighting
       from vespa_analysts.SC2_Weightings b
      where a.old_scaling_segment_id = b.scaling_segment_id
        and scaling_day = @scaling_date
commit

--Find viewing figures for the show, split by adsmartable universe
select sky_base_universe, sum(updated_segment_weight) as updated_viewing_figures, sum(old_segment_weight) as old_viewing_figures
        from V154_law_order_spot1_updated
    group by sky_base_universe
commit
select sky_base_universe, sum(updated_segment_weight) as updated_viewing_figures, sum(old_segment_weight) as old_viewing_figures
        from V154_law_order_spot2_updated
    group by sky_base_universe
commit

end

-- select * into old_V154_viewing_14th_july from V154_viewing_14th_july
-- select * into old_V154_progs_14th_july from V154_progs_14th_july
-- select * into old_V154_law_order_spot1 from V154_law_order_spot1
-- select * into old_V154_law_order_spot2 from V154_law_order_spot2
-- select * into old_V154_law_order_spot1_updated from V154_law_order_spot1_updated
-- select * into old_V154_law_order_spot2_updated from V154_law_order_spot2_updated

