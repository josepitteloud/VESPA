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

/*
select date_
		,cast(count(distinct datehour_) as float) / cast(24 as float)
from	z_pa_events_fact
where	date_=	(
					select	min(x)+1	as proc_date
					from	(
								select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Interaction_N union
								select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Sessions_N union
								select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Interaction_N union
								select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Sessions_N
							)	as base
				)
group	by	date_
*/


-- END OF PRE-REQUISIT

truncate table z_pa_cube_step_1;
truncate table z_pa_step_2_1; -- z_pa_cube_hslvl2_Sessions_N
truncate table z_pa_step_2_2; -- z_pa_cube_hslvl2_Interaction_N
truncate table z_pa_step_2_3; -- z_pa_cube_hslvl3_Sessions_N
truncate table z_pa_step_2_4; -- z_pa_cube_hslvl3_Interaction_N
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


-- The NEW

truncate table z_pa_cube_step_1;commit;

insert	into z_pa_cube_step_1
select	*
from	z_pa_events_Fact
where	date_ =	(
					select	min(x)+1 as proc_date
					from	(
								select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Interaction_N union
								select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Sessions_N union
								select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Interaction_N union
								select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Sessions_N
							)	as base
				);
					
commit;

------------------------------
-- A03 - MASTER LvL 2 Sessions
------------------------------

Insert	into z_pa_step_2_1
with	totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_cube_step_1
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
from	z_pa_cube_step_1	as step1
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
								from 	z_pa_cube_step_1
								group	by	date_
											,stb_type
							)
		,totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_cube_step_1
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
from	z_pa_cube_step_1	as step1
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
						from 	z_pa_cube_step_1
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
from	z_pa_cube_step_1	as step1
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
								from 	z_pa_cube_step_1
								group	by	date_
											,stb_type
							)
		,totstb as	(
						select	date_
								,count(distinct dk_serial_number) as total_stb
						from 	z_pa_cube_step_1
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
from	z_pa_cube_step_1	as step1
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

truncate table z_pa_cube_step_1;
truncate table z_pa_step_2_1;
truncate table z_pa_step_2_2;
truncate table z_pa_step_2_3;
truncate table z_pa_step_2_4;
truncate table z_pa_tenure;

commit;