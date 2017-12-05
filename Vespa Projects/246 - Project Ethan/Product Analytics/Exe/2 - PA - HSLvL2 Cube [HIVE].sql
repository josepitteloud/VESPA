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

		A - Cube for Home Sessions LvL 2
			A00 - MASTER LvL 2 Sessions
			A01 - MASTER LvL 2 Interaction
			
	
--------------------------------------------------------------------------------------------------------------

*/

------------------------------
-- A00 - MASTER LvL 2 Sessions
------------------------------

truncate table z_pa_cube_hslvl2_Sessions;commit;

Insert	into z_pa_cube_hslvl2_Sessions
with	base as		(
						select	dk_action_id
								,action_name
								,asset_uuid
								,remote_type
								,datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))	as datehour_
								,date((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp)										as date_
								,case	when cast(datehour_ as time) between '00:00:00.000' and '05:59:59.000' then 'night'
										when cast(datehour_ as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
										when cast(datehour_ as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
										when cast(datehour_ as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
										when cast(datehour_ as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
										when cast(datehour_ as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
										when cast(datehour_ as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
								end		as part_of_day
								,obs.dk_serial_number
								,case	when substr(obs.dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(obs.dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(obs.dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as stb_type
								,home_session_lvl2_grain
								,home_session_lvl2_id
								,home_session_lvl3_grain
								,home_session_lvl3_id
--								,extract(
--											epoch from	date_ -	(
--																	min(date_) over	(
--																						partition by	obs.dk_serial_number
--																						order by		timems
--																						rows between	1 preceding and 1 preceding
--																					)
--																)
--										)	as ss_elapsed_next_action
						from	pa_events_Fact as obs
								left join pa_action_dim	as acts
								on	obs.dk_action_id	= acts.pk_action_id
								inner join pa_time_dim as thetime
								on	obs.dk_time			= thetime.pk_time_dim
								inner join pa_date_dim as thedate
								on	obs.dk_Date			= thedate.date_pk
								inner join z_pa_reliable_boxes as focus
								on	obs.dk_serial_number = focus.dk_serial_number
						where	dk_date >= 20160416 -- [TEMP]
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
		,totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	base
						group	by	date_
					)
select	step1.datehour_
		,step1.part_of_Day
		,step1.stb_type
		,tenure.months_old
		,step1.home_Session_lvl2_id
		,step1.remote_type
		,max(totstb.total_stb)					as tot_boxes
		,count(distinct step1.dk_serial_number)	as nboxes
		,count(distinct	(
							case 	when length(step1.home_Session_lvl2_id) <2 then null 
									else step1.dk_serial_number||'-'||step1.home_session_lvl2_id||'-'||step1.home_session_lvl2_grain
							end
						))	as tot_journeys
		,count	(distinct
					(
						case	when step1.dk_Action_id in(02400,03000,02000,04500,03001,02100) then step1.dk_serial_number||'-'||step1.home_session_lvl2_id||'-'||step1.home_session_lvl2_grain 
								else null 
						end
					)
				)	as tot_actioned_journey
--		,sum(step1.ss_elapsed_next_action)	as sum_ss_spent_in_session
--into	z_pa_cube_hslvl2_Sessions
from	base	as step1
		inner join tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
		inner join totstb
		on	step1.date_	= totstb.date_
where	step1.home_Session_lvl2_id <> ''
and		step1.home_Session_lvl2_id is not null
group	by	step1.date_
			,step1.datehour_
			,step1.part_of_Day
			,step1.stb_type
			,tenure.months_old
			,step1.home_Session_lvl2_id
			,step1.remote_type;
			
commit;



---------------------------------
-- A01 - MASTER LvL 2 Interaction
---------------------------------

truncate table z_pa_cube_hslvl2_Interaction;commit;

Insert	into z_pa_cube_hslvl2_Interaction
with	base as		(
						select	dk_action_id
								,action_name
								,asset_uuid
								,remote_type
								,datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))	as datehour_
								,date((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp)										as date_
								,case	when cast(datehour_ as time) between '00:00:00.000' and '05:59:59.000' then 'night'
										when cast(datehour_ as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
										when cast(datehour_ as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
										when cast(datehour_ as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
										when cast(datehour_ as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
										when cast(datehour_ as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
										when cast(datehour_ as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
								end		as part_of_day
								,obs.dk_serial_number
								,case	when substr(obs.dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(obs.dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(obs.dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as stb_type
								,home_session_lvl2_grain
								,home_session_lvl2_id
								,home_session_lvl3_grain
								,home_session_lvl3_id
								,extract(
											epoch from	date_ -	(
																	min(date_) over	(
																						partition by	obs.dk_serial_number
																						order by		timems
																						rows between	1 preceding and 1 preceding
																					)
																)
										)	as ss_elapsed_next_action
						from	pa_events_Fact as obs
								left join pa_action_dim	as acts
								on	obs.dk_action_id	= acts.pk_action_id
								inner join pa_time_dim as thetime
								on	obs.dk_time			= thetime.pk_time_dim
								inner join pa_date_dim as thedate
								on	obs.dk_Date			= thedate.date_pk
								inner join z_pa_reliable_boxes as focus
								on	obs.dk_serial_number = focus.dk_serial_number
						where	dk_Date >= 20160416 -- [TEMP]
					)
		,totstb_x_type	as	(
								select	date_
										,stb_type
										,count(distinct dk_serial_number) as total_stb_x_type
								from 	base
								group	by	date_
											,stb_type
							)
		,totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	base
						group	by	date_
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
		,step1.home_session_lvl2_id
		,step1.part_of_Day
		,step1.remote_type
		,step1.dk_action_id
		,step1.asset_uuid
		,max(totstb.total_stb)																							as total_stbs
		,max(totstb_x_type.total_stb_x_type)																			as total_stbs_x_type
		,count(1) 																										as freq
		,count(distinct step1.dk_serial_number) 																		as nboxes
		,sum(step1.ss_elapsed_next_action)																				as sum_ss_spent_in_session
		,count(distinct step1.dk_serial_number||'-'||step1.home_session_lvl2_id||'-'||step1.home_session_lvl2_grain) 	as nsessions		
--into	z_pa_cube_hslvl2_Interaction
from	base	as step1
		inner join	totstb
		on 	step1.date_		= totstb.date_
		inner join totstb_x_type
		on 	step1.date_				= totstb_x_type.date_
		and	step1.stb_type			= totstb_x_type.stb_type
		inner join tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
where	step1.home_Session_lvl2_id <> ''
and		step1.home_Session_lvl2_id is not null
group	by	step1.datehour_
			,step1.stb_type
			,tenure.months_old
			,step1.home_session_lvl2_id
			,step1.part_of_Day
			,step1.remote_type
			,step1.dk_action_id
			,step1.asset_uuid;
			
commit;
