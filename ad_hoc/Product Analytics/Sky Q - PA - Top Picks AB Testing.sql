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

truncate table ref_home_start;commit;

insert	into ref_home_start
select	*
--into	ref_home_start
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
						where	date_ between '2016-11-23' and '2017-01-31' --> Parameter
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
							);

commit;


---------------------------
-- A01 - Generating Metrics
---------------------------

with	ref_daily as	(
							select	base.date_
									,count(distinct base.dk_serial_number) as stb_daily_sample
							from	z_pa_events_fact 	as base
									inner join z_ab_tp	as ref
									on	base.dk_SErial_number = ref.dk_Serial_number
							where	base.date_ between '2016-11-23' and '2017-01-31' --> Parameter
							group	by	date_
						)
select	case	when base.DATE_ between '2016-11-23' and '2016-12-08' then 1
				when base.DATE_ between '2016-12-09' and '2016-12-20' then 2
				when base.DATE_ between '2016-12-21' and '2017-01-31' then 3
				else	0
		end		as tests_cases
		,case	tests_Cases
				when 1 then ref.group_1
				when 2 then ref.group_2
				when 3 then ref.group_3
				else 'unknown'
		end		as ab_group
		,base.date_
		,base.gn_lvl2_session
		,count(distinct base.dk_serial_number) 																	as reach
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.dk_serial_number
											else null 
									end
								)
				)	as reach_converted
		,max(ref_daily.stb_daily_sample)																		as stb_daily_population
		,count(distinct	base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain)				as n_journeys
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain
											else null 
									end
								)
				)	as n_converted_journeys
		,count(distinct (case when base.dk_Action_id in (02400) then base.DATE_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_Session_Grain else null end)) as nconv_journeys_d
		,count(distinct (case when base.dk_Action_id in (03000) then base.DATE_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_Session_Grain else null end)) as nconv_journeys_p
		,count(distinct (case when base.dk_Action_id in (00001) then base.DATE_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_Session_Grain else null end)) as nconv_journeys_ch
from	z_pa_events_fact as base
		inner join z_ab_tp	as ref
		on	base.dk_SErial_number = ref.dk_Serial_number
		inner join ref_home_start
		on	base.date_					= ref_home_start.date_
		and	base.dk_serial_number		= ref_home_start.dk_Serial_number
		and	base.gn_lvl2_session_grain	= ref_home_start.target
		inner join ref_daily
		on	base.date_ = ref_daily.date_
where	base.date_ between '2016-11-23' and '2017-01-31' --> Parameter
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
group	by	tests_cases
			,ab_group
			,base.date_
			,base.gn_lvl2_session