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

        The intention of this Script is to monitor on very basic but fundamental aspects 
		for Product Analytics

**Sections:

		A - Global Monitor
			
--------------------------------------------------------------------------------------------------------------

*/

---------------------
-- A - Global Monitor
---------------------

select	datetime(to_char((TO_CHAR(thedate.day_Date,'YYYY-MM-DD')||' '||thetime.UTC_TIME)::timestamp,'YYYY-MM-DD HH24:00:00'))
		--	as dt
		,count(1) as nactions
		,round((cast(sum(case when global_session_id <> '' then 1 else 0 end)as float)/cast(nactions as float)),4) as prop_with_gs
		,count(distinct dk_serial_number) as nboxes
from	pa_events_Fact	as base
		inner join pa_time_dim as thetime
		on	base.dk_time	= thetime.pk_time_dim
		inner join pa_date_dim as thedate
		on	base.dk_Date	= thedate.date_pk
where	dk_date >= 20160401
group	by	dk_Datehour