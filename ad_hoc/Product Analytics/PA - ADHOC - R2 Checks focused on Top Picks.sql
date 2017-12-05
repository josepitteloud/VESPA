 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	Pilot to demonstrate usefulness of A/B testing through PA Cohorts
		
**Sections:

--------------------------------------------------------------------------------------------------------------
*/



select	case when b.DK_SERIAL_NUMBER is not null then 'R2' else 'Exc. R2' end	as the_sample
		,gn_lvl2_session
		,count(distinct	(
							case 	when length(a.gn_lvl2_session) <2 then null 
									else date_||'-'||a.dk_serial_number||'-'||a.gn_lvl2_session_grain
							end
						))	as tot_journeys
		,count	(distinct
					(
						case	when a.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then date_||'-'||a.dk_serial_number||'-'||a.gn_lvl2_session_grain
								else null 
						end
					)
				)	as tot_actioned_journey
		--,sum(x) over (partition by The_sample) as the_total
		--,cast(x as float) / cast(the_total as float) as the_prop
from	z_pa_events_fact	as a
		left join	z_pa_focus_group_x as b
		on	a.dk_serial_number = b.dk_serial_number
where	date_ between '2016-06-01 00:00:00' and '2016-06-30 00:00:00'
group	by	the_sample
			,gn_lvl2_session





