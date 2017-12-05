 /*
--------------------------------------------------------------------------------------------------------------
**Project Name:                                         Vespa Executive Dashboard
**Analysts:                                                     Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                                                      Jose Loureda
**Stakeholder:                                          Vespa Directors / Managers.
**Due Date:                                                     22/02/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                            http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fRegular%20reports
                                                                        %2fMeta%2fExecutive%20Dashboard&FolderCTID=&View={95B15B22-959B-4B62-809A-AD43E02001BD}
                                                                        
**Business Brief:

Module responsible for extracting a history of the latest 5 weeks from Scaling results to display trends on
the Vespa Panel Balance...

**Module's Sections:

M09: Vespa Panel Balance Extractor
                M09.0 - Initialising environment
                M09.1 - Deriving Metric(s)
                M09.2 - QAing results
                M09.3 - Returning results
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------------
/* M09.0 - Initialising environment */
--------------------------------------
create or replace procedure vespa_sp_xdash_m09_vespaPanelBalance
as begin

        declare @totalsegments float

        select  @totalsegments = count(1)
        from    vespa_analysts.SC2_Segments_Lookup_v2_1 -- 304201

--------------------------------
/* M09.1 - Deriving Metric(s) */
--------------------------------

	insert  into vespa_analysts.vespa_xdash_o5_vespaPanelBalance
    select  base.sky_week
			,min( case when base.ranking = 1 then coalesce(base.sky_base,0) end)    as sky_base
			,round(avg( base.vespa_panel),3)                                        as convergence
			,round((max(sc2hist.PopulationCoverage)/convergence),3)                 as Pop_coverage
			,round((max(sc2hist.SegmentCoverage)/@totalsegments),3)                 as Seg_coverage
	--into    #tempshelf
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


---------------------------
/* M09.2 - QAing results */
---------------------------
-- NIP...

-------------------------------
/* M09.3 - Returning results */
-------------------------------

     --   select  *
     --   from    #tempshelf cortb commented out (22-04-2014) 


end;

commit;
------------------------------------------------------ THE END...
