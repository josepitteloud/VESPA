
--------------------
-- PANEL PERFORMANCE
--------------------

select  case    when (ssav.viewing_consent_flag in('?','N') or ssav.viewing_consent_flag is null)   then 'N'
                else 'Y'
        end     as personalisation_flag
        ,case   when ssav.panel in ('VESPA','VESPA11')      then 'DP'
                when ssav.panel in ('ALT5','ALT6','ALT7')   then 'AP'
                else null
        end     as thepanel
        ,case   when ssav.status_vespa = 'Enabled'  then ssav.status_vespa
                else 'Disabled'
        end     as panel_status
        ,ssav.return_data_30d                   as datareturn_flag
        ,ssav.reporting_performance
        ,case   when ssav.min_reporting_quality >=0.5   then 1 
                else 0
        end     as rq_ge_50
        ,ssav.adsmart_flag
        ,case   when vp.account_number is not null  then 1
                else 0
        end     as in_vp
        ,count(distinct ssav.account_number)    as naccounts
        ,avg(ssav.avg_reporting_quality)        as avg_rq
        ,sum(ta.ta_propensity)                  as sum_prop
from    sig_single_account_view                                     as ssav
        left join vespa_analysts.SkyBase_TA_scores                  as ta
        on  ssav.account_number = ta.account_number
        left join vespa_analysts.vespa_broadcast_reporting_vp_map   as vp
        on  ssav.account_number = vp.account_number
        and vp.vp1 = 1
group   by  personalisation_flag
            ,thepanel
            ,panel_status
            ,datareturn_flag
            ,ssav.reporting_performance
            ,rq_ge_50
            ,ssav.adsmart_flag
            ,in_vp
			
			
----------------
-- PANEL BALANCE
----------------

-- Panel Balance 1
select	* 
from 	v_masvg_traffic_lights

/*	View Definition as follow...

create view v_masvg_traffic_lights
as 
select  lights.variable_name
        ,lights.imbalance_rating
        ,measures.Cat_convergence
        ,measures.Convergence_std
from    (
            select  variable_name
                    ,sequencer
                    ,imbalance_rating 
            from    vespa_traffic_lights_hist 
            where   weekending = (select max(Weekending) from vespa_traffic_lights_hist)
            and     panel = 'DP'
            and     sequencer <7
        ) as lights
        inner join  (
                        select  1       			as sequencer	--UNIVERSE
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
											,sum(weights.vespa_accounts * weights.weighting)    as convergence_
											,sky_base - convergence_                            as diff
									from    (
												select  distinct scaling_segment_id
												from    Vespa_Scaling_Segment_Profiling 
												where   panel = 'DP'
											)                                               	as ssp
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
                        select  2         			as sequencer		--REGION
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
											,sum(weights.vespa_accounts * weights.weighting)    as convergence_
											,sky_base - convergence_                            as diff
									from    (
												select  distinct scaling_segment_id
												from    Vespa_Scaling_Segment_Profiling 
												where   panel = 'DP'
											)                                               	as ssp
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
                        select  3 			 		as sequencer		--HHCOMPOSITION
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
											,sum(weights.vespa_accounts * weights.weighting)    as convergence_
											,sky_base - convergence_                            as diff
									from    (
												select  distinct scaling_segment_id
												from    Vespa_Scaling_Segment_Profiling 
												where   panel = 'DP'
											)                                               	as ssp
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
                        select  4        			as sequencer		--PACKAGE
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
											,sum(weights.vespa_accounts * weights.weighting)    as convergence_
											,sky_base - convergence_                            as diff
									from    (
												select  distinct scaling_segment_id
												from    Vespa_Scaling_Segment_Profiling 
												where   panel = 'DP'
											)                                               	as ssp
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
                        select  5         			as sequencer		--TENURE
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
											,sum(weights.vespa_accounts * weights.weighting)    as convergence_
											,sky_base - convergence_                            as diff
									from    (
												select  distinct scaling_segment_id
												from    Vespa_Scaling_Segment_Profiling 
												where   panel = 'DP'
											)                                               	as ssp
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
                        select  6       			as sequencer		--BOXTYPE
                                ,sum(abs(diff))     as Cat_convergence
                                ,stddev(diff)       as Convergence_std
                        from    (
                                    select  sum(weights.sky_base_accounts)                      as Sky_base
											,sum(weights.vespa_accounts * weights.weighting)    as convergence_
											,sky_base - convergence_                            as diff
									from    (
												select  distinct scaling_segment_id
												from    Vespa_Scaling_Segment_Profiling 
												where   panel = 'DP'
											)                                               	as ssp
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
        on  lights.sequencer = measures.sequencer
order   by  lights.sequencer


commit
 */

-- Panel Balance 2
select  base.sky_week
		,min( case when base.ranking = 1 then coalesce(base.sky_base,0) end)    as sky_base
		,round(avg( base.vespa_panel),3)                                        as convergence
		,round((max(sc2hist.PopulationCoverage)/convergence),3)                 as Pop_coverage
		,round((max(sc2hist.SegmentCoverage)/cast((select count(1) from vespa_analysts.SC2_Segments_Lookup_v2_1 ) as float)),3)                 as Seg_coverage
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

--------------------
-- PANEL COMPOSITION
--------------------

select  adsmart_flag
        ,case   when num_boxes = 1 and adsmart_flag = 1 then 1
                else 0
        end     as ad1b1a
        ,case   when num_boxes > 1 and (num_adsmartable_boxes >0 and num_adsmartable_boxes < num_boxes)   then 1
                else 0
        end     as admbna
        ,case   when num_boxes > 1 and num_adsmartable_boxes = num_boxes    then 1
                else 0
        end     as adallads
        ,count(distinct account_number) as naccounts
from    sig_single_account_view
group   by  adsmart_flag
            ,ad1b1a
            ,admbna
            ,adallads
