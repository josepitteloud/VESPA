--Main Pivot - Panel - Enabled - Quality - Adsmart

select distinct sbv.account_number
          ,case when sbv.panel = 'VESPA' and sbv.status_vespa = 'Enabled' then 1 else 0 end as DP_enabled
          ,case when sbv.panel is not null and sbv.status_vespa = 'Enabled' then 1 else 0 end as AP_enabled
          ,case when vp1 = 1 and sbv.status_vespa = 'Enabled' then 1 else 0 end as VP_enabled
          ,case when Num_logs_sent_30d > 0 then 1 else 0 end as Returning
          ,avg (reporting_quality)
          ,sav.Adsmart_flag
          ,viewing_consent_flag
                  ,weight
                  ,Num_adsmartable_boxes
                  ,Num_boxes 
                  ,case when Num_boxes > 1 and Num_adsmartable_boxes = Num_boxes then 1 else 0 end as multi_all_adsmart
                  ,case when Num_boxes > 1 and Num_adsmartable_boxes < Num_boxes and Num_adsmartable_boxes > 0 then 1 else 0 end as multi_not_adsmart
from vespa_analysts.SIG_SINGLE_BOX_VIEW as SBV
inner join vespa_analysts.SIG_SINGLE_ACCOUNT_VIEW as SAV
on sbv.account_number = sav.account_number
left join  vespa_analysts.vespa_broadcast_reporting_vp_map as VPMAP
on sbv.account_number = vpmap.account_number
group by sbv.account_number, DP_enabled, AP_enabled, VP_enabled, Returning, sav.Adsmart_flag, viewing_consent_flag, weight, Num_adsmartable_boxes, Num_boxes, multi_all_adsmart, multi_not_adsmart 



--TA Call Coverage

select   round((cast((sum(case when enabled = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as enable_tacoverage
                ,round((cast((sum(case when ret = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as returning_tacoverage
                ,round((cast((sum(case when r50 = 1 then sumprop else 1 end)) as float) / cast((sum(sumprop)) as float)),4) as ret_50_tacoverage
                from    (
                                                                select  case when sbv.panel is not null and SBV.status_vespa = 'Enabled' then 1 else 0 end   as enabled
                                                                                                                                                ,case when sbv.panel is not null and SBV.status_vespa = 'Enabled' and Num_logs_sent_30d >0 then 1 else 0 end   as ret
                                                                                                                                                ,case when sbv.panel is not null and SBV.status_vespa = 'Enabled' and Num_logs_sent_7d > 0 and reporting_quality >=0.5 then 1 else 0 end   as r50
                                                                                ,round(sum(round(ta.TA_Propensity,2)),0)            as sumprop
                                                                from    limac.VESPA_TA_CALLERS_201307_SCORED_19Nov  as ta
                                                                left join   vespa_analysts.SIG_SINGLE_BOX_VIEW   as sbv
                                                                on  ta.account_number = sbv.account_number
                                                                group   by  enabled, ret, rr
                )   as n


--Panel Balance

select  lights.panel
        ,lights.variable_name
        ,lights.vespa_imbalance
        ,measures.Cat_convergence
        ,measures.Convergence_std
from    (
            select   panel
                    ,variable_name
                    ,min(sequencer)                                                     as _sequencer
                    ,sum(case when panel = 'VESPA' then imbalance_rating when panel = 'VESPA11' then imbalance_rating else 0 end)    as vespa_imbalance
            from    vespa_traffic_lights_hist
            where   sequencer < 7
            and   (panel = 'VESPA' or panel = 'VESPA11')
            and weekending = case when datepart(weekday,GETDATE()) = 7 then GETDATE()
                                                                         else (GETDATE() - datepart(weekday,GETDATE())) end
            group   by  variable_name, panel
        ) as lights
        inner join  (
                        select  1 /*UNIVERSE*/      as sequencer
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
                                                                                        ,sum(weights.vespa_accounts * weights.weighting)    as convergence_
                                                                                        ,sky_base - convergence_                            as diff
                                                                        from    (
                                                                                                select  distinct scaling_segment_id
                                                                                                from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                                                where   (panel = 'VESPA' or panel = 'VESPA11')
                                                                                                and status_vespa = 'Enabled'
                                                                                        )                                                       as ssp
                                                                                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as ssl
                                                                                        on  ssp.scaling_segment_ID = ssl.scaling_segment_ID
                                                                                        inner join vespa_analysts.sc2_weightings        as weights
                                                                                        on  ssp.scaling_segment_id = weights.scaling_segment_id
                                                                                        and weights.scaling_day =   (
                                                                                                                                                        select max(scaling_date) as thedate
                                                                                                                                                        from vespa_analysts.SC2_Metrics
                                                                                                                                                )
                                    group   by  ssl.universe
                                ) as base
                        union
                        select  2 /*REGION*/        as sequencer
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
                                                                                        ,sum(weights.vespa_accounts * weights.weighting)    as convergence_
                                                                                        ,sky_base - convergence_                            as diff
                                                                        from    (
                                                                                                select  distinct scaling_segment_id
                                                                                                from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                                                where   (panel = 'VESPA' or panel = 'VESPA11')
                                                                                                and status_vespa = 'Enabled'
                                                                                        )                                                       as ssp
                                                                                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as ssl
                                                                                        on  ssp.scaling_segment_ID = ssl.scaling_segment_ID
                                                                                        inner join vespa_analysts.sc2_weightings        as weights
                                                                                        on  ssp.scaling_segment_id = weights.scaling_segment_id
                                                                                        and weights.scaling_day =   (
                                                                                                                                                        select max(scaling_date) as thedate
                                                                                                                                                        from vespa_analysts.SC2_Metrics
                                                                                                                                                )
                                    group   by  ssl.isba_tv_region
                                ) as base
                         union
                         select  3 /*HHComposition*/ as sequencer
                                 ,sum(abs(diff))     as Cat_convergence
                                 ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
                                                                                        ,sum(weights.vespa_accounts * weights.weighting)    as convergence_
                                                                                        ,sky_base - convergence_                            as diff
                                                                        from    (
                                                                                                select  distinct scaling_segment_id
                                                                                                from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                                                where   (panel = 'VESPA' or panel = 'VESPA11')
                                                                                                and status_vespa = 'Enabled'
                                                                                        )                                                       as ssp
                                                                                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as ssl
                                                                                        on  ssp.scaling_segment_ID = ssl.scaling_segment_ID
                                                                                        inner join vespa_analysts.sc2_weightings        as weights
                                                                                        on  ssp.scaling_segment_id = weights.scaling_segment_id
                                                                                        and weights.scaling_day =   (
                                                                                                                                                        select max(scaling_date) as thedate
                                                                                                                                                        from vespa_analysts.SC2_Metrics
                                                                                                                                                )
                                    group   by  ssl.hhcomposition
                                ) as base
                        union
                        select  4 /*PACKAGE*/       as sequencer
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
                                                                                        ,sum(weights.vespa_accounts * weights.weighting)    as convergence_
                                                                                        ,sky_base - convergence_                            as diff
                                                                        from    (
                                                                                                select  distinct scaling_segment_id
                                                                                                from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                                                where   (panel = 'VESPA' or panel = 'VESPA11')
                                                                                                and status_vespa = 'Enabled'
                                                                                        )                                                       as ssp
                                                                                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as ssl
                                                                                        on  ssp.scaling_segment_ID = ssl.scaling_segment_ID
                                                                                        inner join vespa_analysts.sc2_weightings        as weights
                                                                                        on  ssp.scaling_segment_id = weights.scaling_segment_id
                                                                                        and weights.scaling_day =   (
                                                                                                                                                        select max(scaling_date) as thedate
                                                                                                                                                        from vespa_analysts.SC2_Metrics
                                                                                                                                                )
                                    group   by  ssl.package
                                ) as base
                        union
                        select  5 /*TENURE*/        as sequencer
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
                                                                                        ,sum(weights.vespa_accounts * weights.weighting)    as convergence_
                                                                                        ,sky_base - convergence_                            as diff
                                                                        from    (
                                                                                                select  distinct scaling_segment_id
                                                                                                from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                                                where   (panel = 'VESPA' or panel = 'VESPA11')
                                                                                                and status_vespa = 'Enabled'
                                                                                        )                                                       as ssp
                                                                                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as ssl
                                                                                        on  ssp.scaling_segment_ID = ssl.scaling_segment_ID
                                                                                        inner join vespa_analysts.sc2_weightings        as weights
                                                                                        on  ssp.scaling_segment_id = weights.scaling_segment_id
                                                                                        and weights.scaling_day =   (
                                                                                                                                                        select max(scaling_date) as thedate
                                                                                                                                                        from vespa_analysts.SC2_Metrics
                                                                                                                                                )
                                    group   by  ssl.tenure
                                ) as base
                        union
                        select  6 /*BOX TYPE*/      as sequencer
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
                                                                                        ,sum(weights.vespa_accounts * weights.weighting)    as convergence_
                                                                                        ,sky_base - convergence_                            as diff
                                                                        from    (
                                                                                                select  distinct scaling_segment_id
                                                                                                from    vespa_analysts.SIG_SINGLE_BOX_VIEW
                                                                                                where   (panel = 'VESPA' or panel = 'VESPA11')
                                                                                                and status_vespa = 'Enabled'
                                                                                        )                                                       as ssp
                                                                                        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as ssl
                                                                                        on  ssp.scaling_segment_ID = ssl.scaling_segment_ID
                                                                                        inner join vespa_analysts.sc2_weightings        as weights
                                                                                        on  ssp.scaling_segment_id = weights.scaling_segment_id
                                                                                        and weights.scaling_day =   (
                                                                                                                                                        select max(scaling_date) as thedate
                                                                                                                                                        from vespa_analysts.SC2_Metrics
                                                                                                                                                )
                                    group   by  ssl.boxtype
                                ) as base
                    )   as measures
        on  lights._sequencer = measures.sequencer
order   by  _sequencer


--Panel Representation

declare @totalsegments float

        select  @totalsegments = count(1)
        from    vespa_analysts.SC2_Segments_Lookup_v2_1

    select  base.sky_week
                        ,min( case when base.ranking = 1 then coalesce(base.sky_base,0) end)    as sky_base
                        ,avg( base.vespa_panel )                                                as convergence
                        ,round((max(sc2hist.PopulationCoverage)/convergence),3)                 as Pop_coverage
                        ,round((max(sc2hist.SegmentCoverage)/@totalsegments),3)                 as Seg_coverage
        into    #tempshelf
        from    (
                                select  metrics.weekending
                                                ,left(calendar.subs_week_and_year,4) || '-' || right(calendar.subs_week_and_year,2)  as sky_week
                                                ,metrics.sky_base
                                                ,metrics.vespa_panel
                                                ,rank() over    (
                                                                                        partition by    sky_week
                                                                                        order by        scaling_date desc
                                                                                ) as ranking
                                from    (
                                                        select  scaling_date
                                                                        ,datepart(weekday,scaling_date) as theday
                                                                        ,case   when theday = 7
                                                                                        then scaling_date
                                                                                        else cast((scaling_date + (7 - theday)) as date)
                                                                         end    as weekending
                                                                        ,sky_base
                                                                        ,sum_of_weights as vespa_panel
                                                        from    vespa_analysts.sc2_metrics      as metrics
                                                        where   metrics.scaling_date >= (
                                                                                                                                select max(scaling_date) -27
                                                                                                                                from    vespa_analysts.sc2_metrics
                                                                                                                        )
                                                )   as metrics
                                                inner join sk_prod.sky_calendar as calendar
                                                on  metrics.weekending = calendar.calendar_date
                                 --order  by  metrics.weekending desc
                         ) as base
                         left join  (
                                                        select  weekending
                                                                        ,left(calendar.subs_week_and_year,4) || '-' || right(calendar.subs_week_and_year,2)  as sky_week
                                                                        ,cast(avg(PopCoverage) as integer) as PopulationCoverage
                                                                        ,cast(avg(segCoverage) as integer) as SegmentCoverage
                                                        from    (
                                                                                select  scaling_day
                                                                                                ,sum(sky_base_accounts)             as PopCoverage
                                                                                                ,count(distinct scaling_segment_id) as segCoverage
                                                                                                ,datepart(weekday,scaling_day) as theday
                                                                                                ,case   when theday = 7
                                                                                                                then scaling_day
                                                                                                                else cast((scaling_day + (7 - theday)) as date)
                                                                                                end as weekending
                                                                                from    vespa_analysts.sc2_weightings
                                                                                where   scaling_day >=  (
                                                                                                                                        select  cast((max(scaling_day) - 37) as date)
                                                                                                                                        from    vespa_analysts.sc2_weightings
                                                                                                                                )
                                                                                and     vespa_accounts > 0
                                                                                group   by  scaling_day
                                                                        ) as Base
                                                                        inner join sk_prod.sky_calendar as calendar
                                                                        on  base.weekending = calendar.calendar_date
                                                        group   by  weekending
                                                                                ,sky_week
                                                ) as sc2hist
                                                on base.sky_week = sc2hist.sky_week
        group   by  base.sky_week
        order   by  base.sky_week    asc

        select * from #tempshelf
