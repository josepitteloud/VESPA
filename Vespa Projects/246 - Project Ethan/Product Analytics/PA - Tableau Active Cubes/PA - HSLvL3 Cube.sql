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
**Project Name:							PRODUCTS ANALYTICS (PA)
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          Product Team
**Due Date:                             05/02/2016
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        Pool of scripts written to verify PA's logics functioning, meant to work as part of a QA suit
		to check the health of PRODUCTS ANALYTICS

**Sections:

		A - Cube for Home Sessions LvL 3
			A00 - MASTER LvL 3
			
	
--------------------------------------------------------------------------------------------------------------

*/

---------------------
-- A00 - MASTER LvL 3
---------------------

truncate table z_pa_cube_hslvl3_master;commit;

Insert	into z_pa_cube_hslvl3_master
with	base as		(
						select	index_
								,asset_uuid
								,(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp	as dt
								,extract(epoch from	dt -	(
																min(dt) over	(
																					partition by	dk_serial_number
																					order by		index_
																					rows between	1 preceding and 1 preceding
																				)
															))
								as ss_elapsed_next_action
								,dk_serial_number
								,stb_type
								,dk_action_id
								,acts.action_name
								,dk_previous
								,dk_current
								,dk_referrer_id
								,dk_trigger_id
								,last_value(stage1 ignore nulls) over	(
																			partition by	dk_serial_number
																			order by 		index_
																			rows between	200 preceding and current row
																		)							as gn_lvl3_session_grain
								,substr(gn_lvl3_session_grain,1,instr(gn_lvl3_session_grain,'-')-1)	as gn_lvl3_session
								,last_value(stage2 ignore nulls) over	(
																			partition by	dk_serial_number
																			order by 		index_
																			rows between	200 preceding and current row
																		)							as gn_lvl2_session_grain
								,substr(gn_lvl2_session_grain,1,instr(gn_lvl2_session_grain,'-')-1) as gn_lvl2_session
						from	z_gslvl23_step_withtime				as obs
								left join pa_action_dim	as acts
								on	obs.dk_action_id	= acts.pk_action_id
								inner join pa_time_dim as thetime
								on	obs.dk_time			= thetime.pk_time_dim
								inner join pa_date_dim as thedate
								on	obs.dk_Date			= thedate.date_pk
						order	by	index_
					)
		,totstb_x_type	as	(
								select	date(dt)	as date_
										,stb_type
										,count(distinct dk_serial_number) as total_stb_x_type
								from 	base
								group	by	date_
											,stb_type
							)
		,totstb as	(
						select	date(dt)	as date_
								,count(distinct dk_serial_number) as total_stb
						from 	base
						group	by	date_
					)
		,totjour as	(
						select	datetime(to_char(dt,'YYYY-MM-DD HH24:00:00'))					as datehour_
								,count(distinct dk_serial_number||'-'||gn_lvl3_session_grain)	as tot_journeys
								,count((case when dk_Action_id in(02400,03000,02000,04500,03001,02100) then dk_serial_number||'-'||gn_lvl3_session_grain else null end))	as tot_actioned_journey
						from	base
						group	by	datehour_
					)
		,tenure as	(
						select	dk_serial_number
								,thedate.day_Date	as pa_start_dt
								,date(now()) - date(pa_Start_dt)	as thedif
								,case 	
										when thedif between 0 and 7			then '1 Week'
										when thedif between 8 and 15		then '2 Week'
										when thedif between 16 and 23		then '3 Week'
										when thedif between 24 and 31		then '4 Week'
										when thedif between 32 and 39		then '5 Week'
										when thedif between 40 and 47		then '6 Week'
										when thedif between 48 and 55		then '7 Week'
										when thedif between 56 and 63		then '8 Week'
										when thedif between 64 and 71		then '9 Week'
										when thedif between 72 and 79		then '10 Week'
										when thedif between 80 and 87		then '11 Week'
										when thedif between 88 and 95		then '12 Week'
										when thedif between 96 and 180		then '3-6 Month'
										when thedif between 181 and 365 	then '6-12 Month'
										when thedif between 366 and 730 	then '1-2 Years'
										when thedif between 731 and 1460	then '2-4 Years'
										when thedif >1460 					then '4+ Years'
										else 'Unknown'
								end 	as months_old
						from	(
									select	dk_serial_number
											,min(dk_Date) as dt_Start
									from	pa_events_Fact
									group	by	dk_Serial_number
								)	as base
								inner join pa_date_dim 	as thedate
								on	base.dt_Start = thedate.date_pk
					)
select	step1.datehour_
		,step1.stb_type
		,tenure.months_old
		,step1.gn_lvl3_session
		,step1.part_of_Day
		,step1.dk_action_id
		,step1.asset_uuid
		,max(totstb.total_stb)														as total_stbs
		,max(totstb_x_type.total_stb_x_type)										as total_stbs_x_type
		,count(1) 																	as freq
		,count(distinct step1.dk_serial_number) 									as nboxes
		,sum(step1.ss_elapsed_next_action)											as sum_ss_spent_in_session
		,count(distinct step1.dk_serial_number||'-'||step1.gn_lvl3_session_grain) 	as nsessions		
--into	z_pa_cube_hslvl3_master
from	(
			select	dk_action_id
					,action_name
					,asset_uuid
					,datetime(to_char(dt,'YYYY-MM-DD HH24:00:00'))	as datehour_
					,date(dt)										as date_
					,case	when cast(dt as time) between '00:00:00.000' and '05:59:59.000' then 'night'
							when cast(dt as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
							when cast(dt as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
							when cast(dt as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
							when cast(dt as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
							when cast(dt as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
							when cast(dt as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
					end		as part_of_day
					,dk_serial_number
					,stb_type
					,gn_lvl2_session_grain
					,gn_lvl2_session
					,gn_lvl3_session_grain
					,gn_lvl3_session
					,ss_elapsed_next_action
			from	base
		)	as step1
		inner join	totstb
		on 	step1.date_		= totstb.date_
		inner join totstb_x_type
		on 	step1.date_				= totstb_x_type.date_
		and	step1.stb_type			= totstb_x_type.stb_type
		inner join tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
		inner join totjour
		on	step1.datehour_	=	totjour.datehour_
group	by	step1.datehour_
			,step1.stb_type
			,tenure.months_old
			,step1.gn_lvl3_session
			,step1.part_of_Day
			,step1.dk_action_id
			,step1.asset_uuid;
			
commit;
