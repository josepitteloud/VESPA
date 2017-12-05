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
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Product Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        A safety net in case the work stream on hadoop is not completed or still needs fixing on Home Sessions
		
		This follows up the work around logic placed at the beginning of the project but now processing data
		on daily basis. New days will be appended to the cubes to refresh the timeframe available.
		
	->	NOTE: 	Because we can't create SPs in Netezza neither declare variables, before running the script
				make sure you search for "<= PARAMETER" and update to the date you want to process.
				
				If that date exists already in the cubes, the output will be overwritten.

**Sections:

		A - Semi-Automating Cubes LvL 2 build
			A00 - Initialisation
			A01 - Find Starting points for all sessions
			A02 - Bag actions into their relevant sessions
			A03 - MASTER LvL 2 Sessions
			A04 - MASTER LvL 2 Interaction
			A05 - MASTER LvL 3 Sessions
			A06 - MASTER LvL 3 Interaction
			A07 - Inserting Records into Cubes
			A99 - HouseKeeping
			
**Running Time:

		40 Minutes
				
--------------------------------------------------------------------------------------------------------------

*/

-----------------------
-- A00 - Initialisation
-----------------------

-- PRE-REQUISIT:

/*
	if below query doesn't equals 1 then the rest should not be executed
*/

select cast(count(distinct dk_datehour) as float) / cast(24 as float)
from	z_pa_events_fact
where	dk_date =	(
						select	to_char (date (min(x)+1), 'YYYYMMDD') as proc_date
						from	(
									select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Interaction_N union
									select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Sessions_N union
									select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Interaction_N union
									select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Sessions_N
								)	as base
					)

-- END OF PRE-REQUISIT
					
truncate table z_pa_step_0;
truncate table z_pa_step_1;
truncate table z_pa_step_2_1; -- z_pa_cube_hslvl2_Sessions
truncate table z_pa_step_2_2; -- z_pa_cube_hslvl2_Interaction
truncate table z_pa_step_2_3; -- z_pa_cube_hslvl3_Sessions
truncate table z_pa_step_2_4; -- z_pa_cube_hslvl3_Interaction
truncate table z_pa_tenure;
commit;

insert	into z_pa_tenure
select	dk_serial_number
		,date(b.proc_date) - date(pa_Start_dt)	as thedif
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
--into	z_pa_tenure
from	z_pa_stb_tenure	as a
		inner join	(
						select	date (min(x)+1)	as proc_date
						from	(
									select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Interaction_N union
									select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Sessions_N union
									select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Interaction_N union
									select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Sessions_N
								)	as base
					)	as b
		on	1 = 1;

commit;

					
----------------------------------------------
-- A01 - Find Starting points for all sessions
----------------------------------------------

insert	into z_pa_step_0
select	index_
		,dk_asset_id
		,global_session_id
		,dk_date
		,dk_time
		,dk_serial_number
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,asset_uuid
		,stage1
		,remote_type
		,case	when	TLM = (max(TLM) over	( 
													partition by	dk_serial_number
													order by 		index_ 
													rows between 	1 preceding and 1 preceding
												)
							) then null
				else TLM||'-'|| dense_rank() over	(
														partition by	dk_date
																		,dk_serial_number
																		,TLM
														order by 		index_
													)
		end		as stage2
--into	z_pa_step_0
from	(

			-- step 3 - Carving the sessions (level 3)
			
			/*
				here what we are aiming to achieve is to bag together all actions into a single and relevant
				session, at each level (3 in this case).
				
				Hence stripping (making nulls) then associate sessions values for any actions right below
				the session start point.
			*/
			select	index_
					,dk_asset_id
					,global_session_id
					,dk_date
					,dk_time
					,dk_serial_number
					,dk_action_id
					,dk_previous
					,dk_current
					,dk_referrer_id
					,dk_trigger_id
					,asset_uuid
					,SLM
					,remote_type
					,case	when SLM = (max(SLM) over	( 
																	partition by	dk_serial_number
																	order by 		index_ 
																	rows between 	1 preceding and 1 preceding
																)
											) then null
							else SLM||'-'|| dense_rank() over	(
																		partition by	dk_date
																						,dk_serial_number
																						,SLM
																		order by 		index_
																	)
					end		stage1
					,screen_parent
					,case 	when	stage1 is not null	then	(
																	case	when screen_parent is null then substr(stage1,1,instr(stage1,'-')-1)
																			else screen_parent 
																	end
																) 
							else null 
					end		as TLM
			from	(
			
						-- step 2 - Identifying sessions starts
						
						/*
							by linking Step 1 output with pa_session_config, we then lookup (through thelinkage field)
							what are the URIs we have parametrised to make up the sessions (and the relevant session's metadata)
						*/
						select	index_
								,dk_asset_id
								,a.global_session_id
								,b.session_type	as SLM
								,a.thelinkage
								,a.dk_action_id
								,theprevious	as dk_previous
								,a.DK_current 	-- destination
								,a.DK_REFERRER_ID
								,a.dk_trigger_id
								,a.asset_uuid
								,a.dk_serial_number
								,a.dk_date
								,a.dk_time
								,b.screen_parent
								,a.remote_type
						from	(
						
									-- Step 1 -> Sampling & preparing the data for merging with the Session Config table
									/*
										Sampling: 	
										
										in principle, given that this is just a POC we don't need the whole data
										but instead one day of a lab box which we knows has the latest SI build installed
													
										Preparing the data:
										
										given that we compiled for every action, its preceding (dk_previous) and its
										succeeding (dk_current) actions done by the user. Every interactive action (that is,
										any dk_action_id <> 01400) we state that the dk_previous is always = dk_referrer_id
										Doing so means that such dk_previous = the place where the action took place
										
										the field below named thelinkage should be a redundant validation for above.
										
									*/
									select	row_number() over	(order by timems)	as index_
											,dk_asset_id
											,timems
											,dk_date
											,dk_time
											,dk_serial_number
											,dk_action_id
											,global_session_id
											,case	when dk_previous in ('01400','N/A') then	dk_referrer_id 
													else dk_previous
											end		as theprevious
											,case	when (dk_referrer_id is not null and dk_referrer_id <> 'N/A')	then dk_referrer_id
													when dk_action_id in ('01001','01002') 							then 'Mini guide'
													-- below one, to capture cases for Jsons without ref field as a bug
													when dk_previous like '0%'										then	dk_referrer_id 
													when instr(dk_previous,'"',1) > 0 								then translate(substr (dk_previous,instr(dk_previous,'"',1)),'"','')
													else dk_previous
											end		as thelinkage
											,dk_current
											,dk_trigger_id
											,dk_referrer_id
											,asset_uuid
											,remote_type
									from	pa_events_fact as a
									where	dk_date =	(
															select	to_char (date (min(x)+1), 'YYYYMMDD') as proc_date
															from	(
																		select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Interaction_N union
																		select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Sessions_N union
																		select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Interaction_N union
																		select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Sessions_N
																	)	as base
														)
								)	as a
								left join z_pa_screen_dim_v2 as b -- pa_screen_dim
								on	a.thelinkage = b.PK_SCREEN_ID
								and b.pk_screen_id not like '0%'
					)	as base
			group	by	index_
						,dk_asset_id
						,global_session_id
						,dk_date
						,dk_time
						,dk_serial_number
						,dk_action_id
						,dk_previous
						,dk_current
						,dk_trigger_id
						,dk_referrer_id
						,asset_uuid
						,SLM
						,screen_parent
						,remote_type
		)	as base2;
		
commit;


-------------------------------------------------
-- A02 - Bag actions into their relevant sessions
-------------------------------------------------

/*
	- Propagating sessions tags (both levels 2 and 3)
	
	once we have carved the sessions and identified where each of them start and ends
	we simply propagate the correspondent tags (sessions names and ids) to the 
	associated actions (rows)
*/

insert	into z_pa_step_1
select	index_
		,dk_asset_id
		,(TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp												as dt
		,date((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp)										as date_
		,datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))	as datehour_
		,case	when cast(datehour_ as time) between '00:00:00.000' and '05:59:59.000' then 'night'
				when cast(datehour_ as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
				when cast(datehour_ as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
				when cast(datehour_ as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
				when cast(datehour_ as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
				when cast(datehour_ as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
				when cast(datehour_ as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
		end		as part_of_day
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
		,asset_uuid
		,remote_type
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
--into	z_pa_step_1
from	z_pa_step_0				as obs
		left join pa_action_dim	as acts
		on	obs.dk_action_id	= acts.pk_action_id
		inner join pa_time_dim as thetime
		on	obs.dk_time			= thetime.pk_time_dim
		inner join pa_date_dim as thedate
		on	obs.dk_Date			= thedate.date_pk
order	by	index_;

commit;

------------------------------
-- A03 - MASTER LvL 2 Sessions
------------------------------

Insert	into z_pa_step_2_1
with	totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_step_1
						group	by	date_
					)
select	step1.datehour_
		,step1.part_of_Day
		,step1.stb_type
		,tenure.months_old
		,step1.gn_lvl2_session
		,step1.remote_type
		,max(totstb.total_stb)					as tot_boxes
		,count(distinct step1.dk_serial_number)	as nboxes
		,count(distinct	(
							case 	when length(step1.gn_lvl2_session) <2 then null 
									else step1.dk_serial_number||'-'||step1.gn_lvl2_session_grain
							end
						))	as tot_journeys
		,count	(distinct
					(
						case	when step1.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then step1.dk_serial_number||'-'||step1.gn_lvl2_session_grain
								else null 
						end
					)
				)	as tot_actioned_journey
		,sum(step1.ss_elapsed_next_action)	as sum_ss_spent_in_session
--into	z_pa_step_2_1
from	z_pa_step_1	as step1
		inner join z_pa_tenure as tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
		inner join totstb
		on	step1.date_	= totstb.date_
where	step1.gn_lvl2_session <> ''
and		step1.gn_lvl2_session is not null
group	by	step1.date_
			,step1.datehour_
			,step1.part_of_Day
			,step1.stb_type
			,tenure.months_old
			,step1.gn_lvl2_session
			,step1.remote_type;
			
commit;


---------------------------------
-- A04 - MASTER LvL 2 Interaction
---------------------------------

Insert	into z_pa_step_2_2
with	totstb_x_type	as	(
								select	date_
										,stb_type
										,count(distinct dk_serial_number) as total_stb_x_type
								from 	z_pa_step_1
								group	by	date_
											,stb_type
							)
		,totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_step_1
						group	by	date_
					)
select	step1.datehour_
		,step1.stb_type
		,tenure.months_old
		,step1.gn_lvl2_session
		,step1.part_of_Day
		,step1.remote_type
		,step1.dk_action_id
		,step1.asset_uuid
		,max(totstb.total_stb)														as total_stbs
		,max(totstb_x_type.total_stb_x_type)										as total_stbs_x_type
		,count(1) 																	as freq
		,count(distinct step1.dk_serial_number) 									as nboxes
		,sum(step1.ss_elapsed_next_action)											as sum_ss_spent_in_session
		,count(distinct step1.dk_serial_number||'-'||step1.gn_lvl2_session_grain)	as nsessions		
--into	z_pa_step_2_2
from	z_pa_step_1	as step1
		inner join	totstb
		on 	step1.date_		= totstb.date_
		inner join totstb_x_type
		on 	step1.date_				= totstb_x_type.date_
		and	step1.stb_type			= totstb_x_type.stb_type
		inner join z_pa_tenure as tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
where	step1.gn_lvl2_session <> ''
and		step1.gn_lvl2_session is not null
group	by	step1.datehour_
			,step1.stb_type
			,tenure.months_old
			,step1.gn_lvl2_session
			,step1.part_of_Day
			,step1.remote_type
			,step1.dk_action_id
			,step1.asset_uuid;
			
commit;


------------------------------
-- A05 - MASTER LvL 3 Sessions
------------------------------

Insert	into z_pa_step_2_3
with	totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_step_1
						group	by	date_
					)
select	step1.datehour_
		,step1.part_of_Day
		,step1.stb_type
		,tenure.months_old
		,step1.gn_lvl3_session
		,step1.remote_type
		,max(totstb.total_stb)					as tot_boxes
		,count(distinct step1.dk_serial_number)	as nboxes
		,count(distinct	(
							case 	when length(step1.gn_lvl3_session) <2 then null 
									else step1.dk_serial_number||'-'||step1.gn_lvl3_session_grain
							end
						))	as tot_journeys
		,count	(distinct
					(
						case	when step1.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then step1.dk_serial_number||'-'||step1.gn_lvl3_session_grain
								else null 
						end
					)
				)	as tot_actioned_journey
		,sum(step1.ss_elapsed_next_action)	as sum_ss_spent_in_session
--into	z_pa_step_2_3
from	z_pa_step_1	as step1
		inner join z_pa_tenure as tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
		inner join totstb
		on	step1.date_	= totstb.date_
where	step1.gn_lvl3_session <> ''
and		step1.gn_lvl3_session is not null
group	by	step1.date_
			,step1.datehour_
			,step1.part_of_Day
			,step1.stb_type
			,tenure.months_old
			,step1.gn_lvl3_session
			,step1.remote_type;
			
commit;

---------------------------------
-- A06 - MASTER LvL 3 Interaction
---------------------------------

Insert	into z_pa_step_2_4
with	totstb_x_type	as	(
								select	date_
										,stb_type
										,count(distinct dk_serial_number) as total_stb_x_type
								from 	z_pa_step_1
								group	by	date_
											,stb_type
							)
		,totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_step_1
						group	by	date_
					)
select	step1.datehour_
		,step1.stb_type
		,tenure.months_old
		,step1.gn_lvl3_session
		,step1.part_of_Day
		,step1.remote_type
		,step1.dk_action_id
		,step1.asset_uuid
		,max(totstb.total_stb)														as total_stbs
		,max(totstb_x_type.total_stb_x_type)										as total_stbs_x_type
		,count(1) 																	as freq
		,count(distinct step1.dk_serial_number) 									as nboxes
		,sum(step1.ss_elapsed_next_action)											as sum_ss_spent_in_session
		,count(distinct step1.dk_serial_number||'-'||step1.gn_lvl3_session_grain)	as nsessions		
--into	z_pa_step_2_4
from	z_pa_step_1	as step1
		inner join	totstb
		on 	step1.date_		= totstb.date_
		inner join totstb_x_type
		on 	step1.date_				= totstb_x_type.date_
		and	step1.stb_type			= totstb_x_type.stb_type
		inner join z_pa_tenure as tenure
		on	step1.dk_Serial_number	= tenure.dk_Serial_number
where	step1.gn_lvl3_session <> ''
and		step1.gn_lvl3_session is not null
group	by	step1.datehour_
			,step1.stb_type
			,tenure.months_old
			,step1.gn_lvl3_session
			,step1.part_of_Day
			,step1.remote_type
			,step1.dk_action_id
			,step1.asset_uuid;
			
commit;

-------------------------------------
-- A07 - Inserting Records into Cubes
-------------------------------------

insert	into z_pa_cube_hslvl2_Sessions_N
select	*
from	z_pa_step_2_1;

commit;

insert	into z_pa_cube_hslvl2_Interaction_N
select	*
from	z_pa_step_2_2;

commit;

insert	into z_pa_cube_hslvl3_Sessions_N
select	*
from	z_pa_step_2_3;

commit;

insert	into z_pa_cube_hslvl3_Interaction_N
select	*
from	z_pa_step_2_4;

commit;

---------------------
-- A99 - HouseKeeping
---------------------

truncate table z_pa_step_0;
truncate table z_pa_step_1;
truncate table z_pa_step_2_1;
truncate table z_pa_step_2_2;
truncate table z_pa_step_2_3;
truncate table z_pa_step_2_4;
truncate table z_pa_tenure;

commit;