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
**Project Name:							PA - ADHOC - Clash Analysis
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Products Team
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:
	
	This piece is to grasp some basic knowledge around Clashes for Sky Q STBs.

	We mainly need to understand whether allowing 5 recordings in parallel is sufficient enough for customers or do we need to enhance this feature
		
**Sections:

		A - Drafting Clash Measures
			
			A01 - Daily Clash Activity per STB type
			A02 - Analysing distribution of STBs per number of Clashes
			A03 - Matrix for number of STBs clashing on N days across N months
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/

------------------------------------------
-- A01 - Daily Clash Activity per STB type
------------------------------------------

select	date_
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as the_stb_type
		,count(distinct dk_serial_number) 														as n_stbs
		,count(distinct (case when dk_action_id = 02004 then dk_serial_number else null end)) 	as n_stbs_clashing
		,sum(case when dk_action_id = 02004 then 1 else 0 end) 									as nclashes
		,round((cast(n_stbs_clashing as float) / cast(n_stbs as float)),4)						as prop_stbs_clashing
		--,round((cast(nclashes as float) / cast(n_stbs_clashing as float)),4)					as clashing_ratio_x_stb
from	z_pa_events_fact
where	date_ >= '2016-06-01'
group	by	date_
			,the_stb_type
			
			
-------------------------------------------------------------
-- A02 - Analysing distribution of STBs per number of Clashes
-------------------------------------------------------------
			
select	nclashes
		,the_stb_type
		,count(distinct dk_serial_number)	as n_boxes
from	(
			select	date_
					,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as the_stb_type
					,dk_serial_number
					,count(1)	as nclashes
			from	z_pa_events_fact
			where	date_ >= '2016-06-01'
			and		dk_action_id = 02004
			group	by	date_
						,the_stb_type
						,dk_serial_number
		)	as base
group	by	nclashes
			,the_stb_type
			
			
			
			
---------------------------------------------------------------------
-- A03 - Matrix for number of STBs clashing on N days across N months
---------------------------------------------------------------------

select	the_stb_type
		,n_days
		,n_months
		,count(distinct dk_serial_number) as nboxes
from	(
			select	the_stb_type
					,dk_serial_number
					,count(distinct date_)	as n_days
					,count(distinct (extract(month from date_))) as n_months
			from	(
						select	date_
								,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as the_stb_type
								,dk_serial_number
						from	z_pa_events_fact
						where	date_ >= '2016-06-01'
						and		dk_action_id = 02004
						group	by	date_
									,the_stb_type
									,dk_serial_number
					)	as base
			group	by	the_stb_type
						,dk_serial_number
		)	as base2
group	by	the_stb_type
			,n_days
			,n_months


