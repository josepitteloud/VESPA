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

30 Mins for 4 weeks

--------------------------------------------------------------------------------------------------------------

*/


-----------------------
-- A00 - Initialisation
-----------------------

/*
	Z_PA_EVENTS_FACT is the table that holds the data which is ready to be used for analysis.
	
	Given that this table is filled after many ETL processes, at this check we are simply extracting
	the all Sky Weeks fully available in Z_PA_EVENTS_FACT.
	
	Any full Sky Week (7 days worth of data) simply means we have enough evidence to analyse behaviour for that week
	
*/

truncate table z_ref_weekly;commit;

insert	into z_ref_weekly
select	base.*
--into	z_ref_weekly
from	(
			select	date_dim.week_sky_in_year			as sky_week
					,count(distinct ground.date_)		as ndays
					,min(ground.date_) 					as thestart
					,max(ground.date_) 					as theend
					,count(distinct dk_serial_number)	as stb_weekly_pop
			from	z_pa_events_fact			as ground
					inner join pa_date_dim		as date_dim
					on	ground.date_ = date_dim.day_date
			where	date_ >= '2016-11-25' -- > Parameter
			group	by	sky_week
			having	ndays = 7
		)	as base
		inner join	(
						select 	max(sky_Week)+1	as next_week 
						from 	z_sq_hd_weekly 
					)	as ref_
		on	base.sky_week >= ref_.next_week;
commit;


/*
	Given that the context of the weekly snapshot aims to check a Home Page Performance
	We then need to only consider those journeys that started from Home and nowhere else.
	For example, If you navigate from Fullscreen straight into Recordings (possible by the Sky Button on the remote)
	that journey has to be discarded because it was not initiated from home.
	
*/

truncate table ref_home_start_2;commit;

insert	into ref_home_start_2
select	*
-- into	ref_home_start_2
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
								,gn_lvl2_session_grain	as target
								,min(index_)			as start_
						from 	z_pa_events_fact
						where	date_ >= (select min(thestart) as the_Start from z_ref_weekly)
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

insert	into z_sq_hd_weekly
with	ref_conv as	(
						select	date_
								,dk_serial_number
								,gn_lvl2_session_grain
								,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
						from	z_pa_events_fact
						where	date_ >= (select min(thestart) as the_Start from z_ref_weekly)
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session_grain
					)
		--,ref_weekly as	(
							--select	date_dim.week_sky_in_year			as sky_week
									--,count(distinct dk_serial_number)	as stb_weekly_pop
							--from	z_pa_events_fact			as ground
									--inner join pa_date_dim		as date_dim
									--on	ground.date_ = date_dim.day_date
							--where	date_ between '2016-11-11' and '2016-11-24' --> Parameter
							--group	by	sky_week
						--)
select	base.Sky_week
		,base.gn_lvl2_session
		,count(distinct base.dk_serial_number) 																	as reach
		,count	(distinct	(
								case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) and  ref_home_start_2.target is not null then base.dk_serial_number
										else null 
								end
							)
				)	as reach_converted
		,max(ref_weekly.stb_weekly_pop)																		as stb_daily_population
		,count	(distinct	(
								case	when ref_home_start_2.target is not null then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain 
										else null
								end
							)
				)	as n_journeys
		,count	(distinct	(
								case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) and ref_home_start_2.target is not null then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain
											else null 
								end
							)
				)	as n_converted_journeys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_grain)	as converted_checksum
		,sum(base.ss_elapsed_next_action)																		as n_secs_spent
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.INDEX_ < ref_conv.x and ref_home_start_2.target is not null then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_convertion
--into	z_sq_hd_weekly
from	(

			-- One of pre-sampling for performance...
			
			select	date_dim.sky_week
					,date_
					,dk_serial_number
					,gn_lvl2_session
					,gn_lvl2_session_grain
					,index_
					,dk_action_id
					,SS_ELAPSED_NEXT_ACTION
			from	z_pa_events_fact		as ground
					inner join z_ref_weekly	as date_dim
					on	ground.date_ between date_dim.thestart and date_dim.theend
			where	date_ >= (select min(thestart) as the_Start from z_ref_weekly)
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
										)
		)	as base
		inner join ref_home_start_2
		on	base.date_					= ref_home_start_2.date_
		and	base.dk_serial_number		= ref_home_start_2.dk_Serial_number
		and	base.gn_lvl2_session_grain	= ref_home_start_2.target
		inner join z_ref_weekly			as ref_weekly
		on	base.sky_week 				= ref_weekly.sky_week
		left join	ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_Serial_number 		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		and	ref_conv.x is not null
group	by	base.Sky_week
			,base.gn_lvl2_session;
			
commit;
			
			
			
			
			
			
			

		
		
		
-----------
-- Original
-----------

/*

from	z_pa_events_fact as base
		left join ref_home_start_2
		on	base.date_					= ref_home_start_2.date_
		and	base.dk_serial_number		= ref_home_start_2.dk_Serial_number
		and	base.gn_lvl2_session_grain	= ref_home_start_2.target
		inner join ref_weekly
		on	1=1
		left join	ref_conv
		on	base.date_	= ref_conv.date_
		and	base.dk_Serial_number 		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		and	ref_conv.x is not null
where	base.date_ between '2016-11-04' and '2016-11-10' --> Parameter
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
								)
								
*/