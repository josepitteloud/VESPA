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

        Measuring Green Button engagement. not a deep dive...
		
**Sections:

		A - Data Analysis
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/

--------------------
-- A - Data Analysis
--------------------


--

select	extract(month from date_) 			as the_month
		,stb_type
		,count(distinct dk_Serial_number)															as nboxes
		,count(distinct(case when dk_Action_id = 07001 then dk_serial_number else null end))		as reach
		,count(1) 																					as nactions
		,sum(case when dk_Action_id = 07001 then 1 else 0 end)										as ngreen_actions
		,sum(case when gn_lvl2_Session = 'Fullscreen' then 1 else 0 end) 							as nfullscreen_Actions
		,sum(case when gn_lvl2_session = 'Fullscreen' and dk_Action_id = '07001' then 1 else 0 end)	as ngreen_screen
from	(
			select	date_
					,dk_Serial_number
					,dk_action_id
					,gn_lvl2_session
					,case 	substr(dk_serial_number,3,1)
							when 'B'	then 'Gateway'
							when 'C'	then 'Gateway'
							when 'D'	then 'MR'
							else 'unknown'
					end		as stb_type
			from	z_pa_events_fact
			where	date_ >= '2016-11-01'
		)	as base
group	by	the_month
			,stb_type

--

select	extract(month from date_)	as the_month
		,dk_serial_number
		,stb_type
		,count(distinct date_) 														as nactive_days
		,count(distinct (case when dk_Action_id = 07001 then date_ else null end))	as ngreen_days
		,count(1) 																	as nactions
		,sum(case when dk_Action_id = 07001 then 1 else 0 end) 						as ngreen_actions
from	(
			select	date_
					,dk_Serial_number
					,dk_action_id
					,case 	substr(dk_serial_number,3,1)
							when 'B'	then 'Gateway'
							when 'C'	then 'Gateway'
							when 'D'	then 'MR'
							else 'unknown'
					end		as stb_type
			from	z_pa_events_fact
		)	as base
where	date_ >= '2016-11-01'
group	by	the_month
			,dk_serial_number
			,stb_type
			
--

 -- NO POINT ON RUNNING THIS SINCE ALL 07001 HAPPEN IN FULLSCREEN

select	extract(month from date_)			as the_month
		,gn_lvl2_session
		,count(1) 							as nactions
		,count(distinct dk_Serial_number)	as reach
from	z_pa_events_fact
where	date_ >= '2016-11-01'
and		dk_action_id = 07001
group	by	the_month
			,gn_lvl2_Session