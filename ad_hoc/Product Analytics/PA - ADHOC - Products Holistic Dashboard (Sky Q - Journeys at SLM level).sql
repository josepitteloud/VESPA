/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ 
             ?$$$,      I$$$ $$$$. $$$$  $$$= 
              $$$$$$$$= I$$$$$$$    $$$$.$$$  
                  :$$$$~I$$$ $$$$    $$$$$$   
               ,.   $$$+I$$$  $$$$    $$$$=   
              $$$$$$$$$ I$$$   $$$$   .$$$    
                                      $$$     
                                     $$$      
                                    $$$?

            CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							PRODUCTS HOLISTIC DASHBOARD
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Products Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        it was requested to pull a dashboard together that compares performance (defined in group discussions) across multiple Sky products (Q vs + vs Go vs Now TV vs Kids vs App)
		
		There are many sections within the holistic dashboard, but this script focus on Sky Q specifically for Journey metrics.
		
**Sections:

		A - Drafting Sky Q Journey foundational measures at SLM		
			A00 - Initialisation
			A01 - Generating Metrics
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/


-----------------------
-- A00 - Initialisation
-----------------------

truncate table ref_home_start;commit;

insert	into ref_home_start
select	*
--into	ref_home_start
from	(
			select	*
					,max(origin) over	(
											partition by	date_
															,dk_serial_number
											order by		start_
											rows between	1 preceding and 1 preceding
										)	as origin_2
			from	(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl3_session
								,target
								,start_
								,max(target) over	(
														partition by	date_
																		,dk_serial_number
														order by		start_
														rows between	1 preceding and 1 preceding
													)	as origin
						from	(
									select	date_
											,dk_serial_number
											,gn_lvl2_session
											,gn_lvl3_session
											,gn_lvl3_session_grain as target
											,min(index_)	as start_
									from 	z_pa_events_fact
									where	date_ between '2016-07-01 00:00:00' and '2016-09-30 00:00:00' --> Parameter
									--where	date_ = '2016-06-20 00:00:00' --> Parameter
									group	by	date_
												,dk_serial_number
												,gn_lvl2_session
												,gn_lvl3_session
												,gn_lvl3_session_grain
									order	by	date_
												,dk_serial_number
												,start_
								)	as base
					)	as base2
		)	as base3
where	gn_lvl2_session in	(
								'TV Guide'
								,'Catch Up'
								,'Recordings'
								,'My Q'
							)
and		lower(origin_2) like 'home%';

commit;


---------------------------
-- A01 - Generating Metrics
---------------------------

with	ref_conv as	(
						select	date_
								,dk_serial_number
								,gn_lvl3_session_grain
								,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
						from	z_pa_events_fact
						where	date_ between '2016-07-01 00:00:00' and '2016-07-30 00:00:00' --> Parameter
						--where	date_ = '2016-06-20 00:00:00' --> Parameter
						group	by	date_
									,dk_serial_number
									,gn_lvl3_session_grain
					)
		,ref_daily as	(
							select	--date_
									--,stb_type
									count(distinct dk_serial_number) as stb_daily_sample
							from	z_pa_events_fact
							where	date_ between '2016-07-01 00:00:00' and '2016-07-30 00:00:00' --> Parameter
							--where	date_ = '2016-06-20 00:00:00' --> Parameter
							--group	by	date_
										--,stb_type
						)
select	--case when extract(dow from base.date_) in (1,7) then 'Weekend' else 'Weekdays' end 						as week_part
		--,base.stb_type
		base.gn_lvl3_session
		,count(distinct base.dk_serial_number) 																	as reach
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.dk_serial_number
											else null 
									end
								)
				)	as reach_converted_old_version
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) and  ref_home_start.target is not null then base.dk_serial_number
											else null 
									end
								)
				)	as reach_converted
		,max(ref_daily.stb_daily_sample)																		as stb_daily_population
		,cast(reach as float) / cast(stb_daily_population as float)												as prop_reach
		,cast(reach_converted as float) / cast(stb_daily_population as float)									as prop_reach_converted
		,cast(reach_converted as float) / cast(reach as float)													as prop_reach_conv_over_visitors
		,count	(
					distinct	(
									case	when ref_home_start.target is not null then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl3_session_grain 
											else null
									end
								)
				)	as n_journeys
		,count(distinct	base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl3_session_grain)				as n_journeys_old
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) and ref_home_start.target is not null then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl3_session_grain
											else null 
									end
								)
				)	as n_converted_journeys
		
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl3_session_grain
											else null 
									end
								)
				)	as n_converted_journeys_old
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl3_session_grain)	as converted_checksum
		,cast(n_journeys as float) / cast((sum(n_journeys) over (partition by 1)) as float)						as prop_journeys
		,cast(n_converted_journeys as float) / cast(n_journeys as float)										as conversion_rate
		,sum(case when ref_home_start.target is not null then ss_elapsed_next_action else null end)												as n_secs_spent
		,sum(ss_elapsed_next_action) 																			as n_secs_spent_OLD
		,sum( case when ref_conv.gn_lvl3_session_grain is not null and base.INDEX_ < ref_conv.x then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_convertion_OLD
		,sum( case when ref_conv.gn_lvl3_session_grain is not null and base.INDEX_ < ref_conv.x and ref_home_start.target is not null then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_convertion
		,cast(n_secs_spent as float) / cast(reach as float) 													as avg_secs_x_stb
		,cast(n_secs_spent as float) / cast(n_journeys as float) 												as avg_secs_x_journey
		,cast(n_journeys as float) / cast(reach as float) 														as avg_journeys_x_stb
		,cast(sces_to_convertion as float) / cast(reach_converted as float) 									as avg_secs_x_stb_conv
		,cast(sces_to_convertion as float) / cast(n_converted_journeys as float) 								as avg_secs_x_journey_conv
		,cast(n_converted_journeys as float) / cast(reach_converted as float) 									as avg_journeys_x_stb_conv
from	z_pa_events_fact as base
		left join ref_home_start
		on	base.date_					= ref_home_start.date_
		and	base.dk_serial_number		= ref_home_start.dk_Serial_number
		and	base.gn_lvl3_session_grain	= ref_home_start.target
		inner join ref_daily
		on	1=1
		--on	base.date_					= ref_daily.date_
		--and	base.stb_type 				= ref_daily.stb_type
		left join	ref_conv
		on	base.date_	= ref_conv.date_
		and	base.dk_Serial_number 		= ref_conv.dk_serial_number
		and	base.gn_lvl3_session_grain	= ref_conv.gn_lvl3_session_grain
		and	ref_conv.x is not null
where	base.date_ between '2016-07-01 00:00:00' and '2016-07-30 00:00:00' --> Parameter
--where	base.date_ = '2016-06-20 00:00:00' --> Parameter
and		base.gn_lvl2_session in	(
									'TV Guide'
									,'Catch Up'
									,'Recordings'
									,'My Q'
								)
group	by	--week_part
			--,base.stb_type
			base.gn_lvl3_session