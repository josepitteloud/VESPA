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
**Analysts:                             Angel Donnarumma	(angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma    (angel.donnarumma@sky.uk)
**Stakeholder:                          Oliver Bartlett
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

       Adhoc Request to measure performance of "For You" areas in Sky Q relevant to content recomendation
				
--------------------------------------------------------------------------------------------------------------

*/



select	gn_lvl3_session
		,count(distinct dk_serial_number) as nboxes
		,count	(distinct
					(
						case	when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then dk_serial_number
								else null 
						end
					)
				)	as nboxes_converted
		,count(distinct	(
							case 	when length(gn_lvl3_session) <2 then null 
									else date_||'-'||dk_serial_number||'-'||gn_lvl3_session_grain
							end
						))	as tot_journeys
		,count	(distinct
					(
						case	when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then date_||'-'||dk_serial_number||'-'||gn_lvl3_session_grain
								else null 
						end
					)
				)	as tot_actioned_journey
		,count(distinct asset_uuid) as n_assets
from	z_pa_events_fact
where	lower(gn_lvl3_session) like '%for%you%'
and		date_ between '2016-06-01 00:00:00' and '2016-06-30 00:00:00'
group	by	gn_lvl3_session