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

        Identifying how old the STBs are in the Q Panel for PA.

**Sections:

		A - Calculating Tenure
			A00 - Checking if there are new STBs
				A01 - If so, Finding minimum reporting date to set as tenure
				A02 - Updating Foundational Table
			
			
**Running Time:

		
				
--------------------------------------------------------------------------------------------------------------

*/

---------------------------------------
-- A00 - Checking if there are new STBs
---------------------------------------
truncate table z_tenure_step_0;commit;


insert	into z_tenure_step_0
select	*
--into	z_tenure_step_0
from	(
			select	distinct substr(dk_serial_number,1,16) the_serial
			from	z_pa_events_fact
			where	date_=	(
								select	min(x)+1 as proc_date
								from	(
											select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Interaction_N union
											select	max(date(datehour_)) as x from z_pa_cube_hslvl2_Sessions_N union
											select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Interaction_N union
											select	max(date(datehour_)) as x from z_pa_cube_hslvl3_Sessions_N
										)	as base
							)
		)	as base
		left join z_pa_stb_tenure as ref
		on	base.the_serial = ref.dk_serial_number
where	ref.dk_serial_number is null;

commit;
		
-- select count(1), count(distinct the_serial) from z_tenure_step_0 -- The table must have entries to proceed with the rest of the script
		
---------------------------------------------------------------
-- A01 - If so, Finding minimum reporting date to set as tenure
---------------------------------------------------------------

truncate table z_tenure_step_1;commit;

insert	into z_tenure_step_1
select	dk_serial_number
		,thedate.day_Date	as pa_start_dt
--into	z_tenure_step_1
from	(
			select	base.dk_serial_number
					,min(dk_Date) as dt_Start
			from	pa_events_Fact as base
					inner join z_tenure_step_0 as ref_
					on	base.dk_serial_number	= ref_.the_serial
			group	by	base.dk_Serial_number
		)	as base
		inner join pa_date_dim 	as thedate
		on	base.dt_Start = thedate.date_pk;

commit;

truncate table z_tenure_step_0; commit;

------------------------------------
-- A02 - Updating Foundational Table
------------------------------------

insert	into z_pa_stb_tenure
select	*
		,0 as x
		,'' as y
from	z_tenure_step_1;

commit;

truncate table z_tenure_step_1; commit;