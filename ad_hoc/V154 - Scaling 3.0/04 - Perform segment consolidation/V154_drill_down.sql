/*
**The purpose of this code is to drill down from the updated SC4 scaling segments
**and try to see if the aggregation has had a serious effect.
*/
begin

if object_id('V154_drill_down_region') is not null drop table V154_drill_down_region
select  sub1.*, reg.isba_tv_region as old_region
        into V154_drill_down_region
        from (select  a.scaling_segment_id
                     ,a.sky_base_accounts as aggregated_sky_base_accounts
                     ,a.vespa_panel as aggregated_vespa_panel
                     ,sum_of_weights
                     ,segment_weight
                     ,b.sky_base_universe
                     ,b.isba_tv_region
                     ,b.hhcomposition
                     ,b.tenure
                     ,b.package
                     ,b.no_of_stbs
                     ,b.hd_subscription
                     ,b.pvr
             from V154_weighting_working_table a
       inner join V154_segment_lookup_v1_1 b
               on a.scaling_segment_id = b.updated_scaling_segment_id) as sub1
  right join igonorp.V154_isba_tv_region_v2 reg
          on sub1.isba_tv_region = reg.isba_tv_region_v2
commit

alter table V154_drill_down_region
        add (sky_base_accounts  integer default 0
            ,vespa_panel        integer default 0)
commit

if object_id('temp_region_count') is not null drop table temp_region_count
select   isba_tv_region
        ,sky_base_universe
        ,hhcomposition_v2
        ,tenure_v2
        ,package_v2
        ,no_of_stbs
        ,hd_subscription
        ,pvr
        ,count(distinct account_number) as sky_base_accounts
    into temp_region_count
    from V154_account_numbers_variables_v2
group by isba_tv_region
        ,sky_base_universe
        ,hhcomposition_v2
        ,tenure_v2
        ,package_v2
        ,no_of_stbs
        ,hd_subscription
        ,pvr
commit
if object_id('temp_region_vespa_count') is not null drop table temp_region_vespa_count
select   isba_tv_region
        ,weighting_universe
        ,hhcomposition_v2
        ,tenure_v2
        ,package_v2
        ,no_of_stbs
        ,hd_subscription
        ,pvr
        ,sum(vespa_indicator) as vespa_panel
    into temp_region_vespa_count
    from V154_account_numbers_variables_v2
group by isba_tv_region
        ,weighting_universe
        ,hhcomposition_v2
        ,tenure_v2
        ,package_v2
        ,no_of_stbs
        ,hd_subscription
        ,pvr
commit
update      V154_drill_down_region a
        set a.sky_base_accounts = b.sky_base_accounts
       from temp_region_count b
      where a.old_region = b.isba_tv_region
        and a.sky_base_universe = b.sky_base_universe
        and a.hhcomposition = b.hhcomposition_v2
        and a.tenure = b.tenure_v2
        and a.package = b.package_v2
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr
commit
update      V154_drill_down_region a
        set a.vespa_panel = b.vespa_panel
       from temp_region_vespa_count b
      where a.old_region = b.isba_tv_region
        and a.sky_base_universe = b.weighting_universe
        and a.hhcomposition = b.hhcomposition_v2
        and a.tenure = b.tenure_v2
        and a.package = b.package_v2
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr
commit

update      V154_drill_down_region
        set aggregated_vespa_panel = 0
      where aggregated_vespa_panel = 0.000001
commit
end


--Tests to make sure aggregates are accurate
--Check the aggregated counts - sum of counts witrhin each scaling segment
select top 20   scaling_segment_id
                ,max(aggregated_sky_base_accounts) - sum(sky_base_accounts) as diff
        from V154_drill_down_region
        group by scaling_segment_id
        order by abs(diff) desc
select top 20   scaling_segment_id
                ,max(aggregated_vespa_panel) - sum(vespa_panel) as diff
        from V154_drill_down_region
        group by scaling_segment_id
        order by abs(diff) desc

--Sum of squared differences
select sqrt(sum(diff)) from (
        select distinct scaling_segment_id, POWER(aggregated_sky_base_accounts - sum_of_weights, 2) as diff
                from V154_drill_down_region
        ) as sub1
select sqrt(SUM(POWER(sky_base_accounts - vespa_panel*segment_weight, 2))) as squared_diff
        from V154_drill_down_region

select isba_tv_region, sqrt(sum(diff)) from (
        select distinct scaling_segment_id, isba_tv_region, POWER(aggregated_sky_base_accounts - sum_of_weights, 2) as diff
                from V154_drill_down_region
        ) as sub1
group by isba_tv_region
order by isba_tv_region
select isba_tv_region, sqrt(SUM(POWER(sky_base_accounts - vespa_panel*segment_weight, 2))) as squared_diff
        from V154_drill_down_region
group by isba_tv_region
order by isba_tv_region


-- --Old code used when comparing boxtype
-- --However boxtype has been replaced by the variables no_of_stbs, hd_subscription, pvr
-- --so this code is likely to be redundant.
-- --Do the same for boxtype
-- if object_id('V154_drill_down_boxtype') is not null drop table V154_drill_down_boxtype
-- select  sub1.*, reg.boxtype as old_boxtype
--         into V154_drill_down_boxtype
--         from (select  a.scaling_segment_id
--                      ,a.sky_base_accounts as aggregated_sky_base_accounts
--                      ,a.vespa_panel as aggregated_vespa_panel
--                      ,sum_of_weights
--                      ,segment_weight
--                      ,b.sky_base_universe
--                      ,b.isba_tv_region
--                      ,b.hhcomposition
--                      ,b.tenure
--                      ,b.package
--                      ,b.boxtype
--              from V154_weighting_working_table a
--        inner join V154_segment_lookup_v1_1 b
--                on a.scaling_segment_id = b.updated_scaling_segment_id) as sub1
--   right join igonorp.V154_boxtype_v3 reg
--           on sub1.boxtype = reg.boxtype_v3
-- commit
-- 
-- alter table V154_drill_down_boxtype
--         add (sky_base_accounts  integer default 0
--             ,vespa_panel        integer default 0)
-- commit
-- 
-- if object_id('temp_boxtype_count') is not null drop table temp_boxtype_count
-- select   boxtype
--         ,sky_base_universe
--         ,hhcomposition_v2
--         ,tenure_v2
--         ,package_v2
--         ,isba_tv_region_v2
--         ,count(distinct account_number) as sky_base_accounts
--     into temp_boxtype_count
--     from V154_account_numbers_variables_v2
-- group by boxtype
--         ,sky_base_universe
--         ,hhcomposition_v2
--         ,tenure_v2
--         ,package_v2
--         ,isba_tv_region_v2
-- commit
-- if object_id('temp_boxtype_vespa_count') is not null drop table temp_boxtype_vespa_count
-- select   boxtype
--         ,weighting_universe
--         ,hhcomposition_v2
--         ,tenure_v2
--         ,package_v2
--         ,isba_tv_region_v2
--         ,sum(vespa_indicator) as vespa_panel
--     into temp_boxtype_vespa_count
--     from V154_account_numbers_variables_v2
-- group by boxtype
--         ,weighting_universe
--         ,hhcomposition_v2
--         ,tenure_v2
--         ,package_v2
--         ,isba_tv_region_v2
-- commit
-- 
-- update      V154_drill_down_boxtype a
--         set a.sky_base_accounts = b.sky_base_accounts
--        from temp_boxtype_count b
--       where a.old_boxtype = b.boxtype
--         and a.sky_base_universe = b.sky_base_universe
--         and a.hhcomposition = b.hhcomposition_v2
--         and a.tenure = b.tenure_v2
--         and a.package = b.package_v2
--         and a.isba_tv_region = b.isba_tv_region_v2
-- commit
-- update      V154_drill_down_boxtype a
--         set a.vespa_panel = b.vespa_panel
--        from temp_boxtype_vespa_count b
--       where a.old_boxtype = b.boxtype
--         and a.sky_base_universe = b.weighting_universe
--         and a.hhcomposition = b.hhcomposition_v2
--         and a.tenure = b.tenure_v2
--         and a.package = b.package_v2
--         and a.isba_tv_region = b.isba_tv_region_v2
-- commit
-- 
-- select top 20   scaling_segment_id
--                 ,max(aggregated_sky_base_accounts) - sum(sky_base_accounts) as diff
--         from V154_drill_down_boxtype
--         group by scaling_segment_id
--         order by abs(diff) desc
-- select top 20   scaling_segment_id
--                 ,max(aggregated_vespa_panel) - sum(vespa_panel) as diff
--         from V154_drill_down_boxtype
--         group by scaling_segment_id
--         order by abs(diff) desc
--
