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

		A - Drafting Sky Q Journey foundational measures			
			A00 - Initialisation
			A01 - Generating Metrics
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/


-----------------------
-- A00 - Initialisation
-----------------------
/*
	Here I'm identifying all journeys that began from home...
	
	The way I'm doing this is simple, every session has a preceding one and a succeeding one.
	It is a natural characteristic that any journey (session) into any TLM offered in the homepage
	will have a preceding Home Session (for example, if you navigated into Sky Store from Home then there is no other preceding
	session that sky Store could have than a home one)
	
*/
truncate table ref_home_start;commit;

insert	into ref_home_start
select	*
-- into	ref_home_start
from	(
			select	date_
					,dk_serial_number
					,gn_lvl2_session
					,target
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
								,gn_lvl2_session_grain as target
								,min(index_)	as start_
						from 	z_pa_events_fact
						where	date_ between '2016-11-01' and '2016-11-30' --> Parameter
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
						order	by	date_
									,dk_serial_number
									,start_
					)	as base
		)	as base2
where	lower(origin) like 'home%'
and		gn_lvl2_session in	(
								'TV Guide'
								,'Catch Up'
								,'Recordings'
								,'My Q'
								,'Top Picks'
								,'Sky Box Sets'
								,'Sky Movies'
								,'Sky Store'
								,'Sports'
								,'Kids'
								,'Music'
								,'Online Videos'
								,'Search'
							);

commit;


---------------------------
-- A01 - Generating Metrics
---------------------------

with	ref_conv as	(

						/*
							Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							to use that then as a flag to derive time to conversion (this is, how many seconds since the
							beginning of the journey until the very first converting action)
						*/

						select	date_
								,dk_serial_number
								,gn_lvl2_session_grain
								,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
						from	z_pa_events_fact
						where	date_ between '2016-11-01' and '2016-11-30' --> Parameter
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session_grain
					)
		,ref_daily as	(
							
							-- here just finding the total number of STBs within the timeframe I'm evaluating
							-- this will actually determine the size of my Active Population...
							
							select	count(distinct dk_serial_number) as stb_daily_sample
							from	z_pa_events_fact
							where	date_ between '2016-11-01' and '2016-11-30' --> Parameter
						)
select	base.gn_lvl2_session
		,count(distinct base.dk_serial_number) 																	as reach
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.dk_serial_number
											else null 
									end
								)
				)	as reach_converted
		,max(ref_daily.stb_daily_sample)																		as stb_daily_population
		,cast(reach as float) / cast(stb_daily_population as float)												as prop_reach
		,cast(reach_converted as float) / cast(stb_daily_population as float)									as prop_reach_converted
		,cast(reach_converted as float) / cast(reach as float)													as prop_reach_conv_over_visitors
		,count(distinct	base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain)				as n_journeys
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain
											else null 
									end
								)
				)	as n_converted_journeys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_grain)	as converted_checksum
		,cast(n_journeys as float) / cast((sum(n_journeys) over (partition by 1)) as float)						as prop_journeys
		,cast(n_converted_journeys as float) / cast(n_journeys as float)										as conversion_rate
		,sum(ss_elapsed_next_action) 																			as n_secs_spent
		,sum( case when ref_conv.gn_lvl2_session_grain is not null and base.INDEX_ < ref_conv.x then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_conversion
from	z_pa_events_fact as base
		inner join ref_home_start
		on	base.date_					= ref_home_start.date_
		and	base.dk_serial_number		= ref_home_start.dk_Serial_number
		and	base.gn_lvl2_session_grain	= ref_home_start.target
		inner join ref_daily
		on	1=1
		left join	ref_conv
		on	base.date_	= ref_conv.date_
		and	base.dk_Serial_number 		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		and	ref_conv.x is not null
where	base.date_ between '2016-11-01' and '2016-11-30' --> Parameter
and		base.gn_lvl2_session in	(
									'TV Guide'
									,'Catch Up'
									,'Recordings'
									,'My Q'
									,'Top Picks'
									,'Sky Box Sets'
									,'Sky Movies'
									,'Sky Store'
									,'Sports'
									,'Kids'
									,'Music'
									,'Online Videos'
									--,'Search' --> is not a TLM
								)
group	by	base.gn_lvl2_session