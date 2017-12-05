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

        Understanding Home Journeys fluctuation for a cohort of STBs Pre/Post R11 Launch and Sky Store Changes.
		
**Sections:

			
**Running Time:


--------------------------------------------------------------------------------------------------------------

*/



--For Q:

/*
	Description:
	
	
	
*/



select	count(distinct date_||dk_serial_number||target) as n_TLMs_from_home
		,count(distinct dk_serial_number) as nboxes
from	(
			select	date_
					,dk_serial_number
					,gn_lvl2_session
					,target
					,max(target) over	(
											partition by	date_
															,dk_serial_number
											order by		start_
											rows between	1 preceding and 1 preceding
										)	as origin
			from	(
						select	base.date_
								,base.dk_serial_number
								,base.gn_lvl2_session
								,base.gn_lvl2_session_grain	as target
								,min(base.index_)			as start_
						from 	z_pa_events_fact	as base
								inner join	(
												select	substr(dk_serial_number,3,1)	as stb_type
														,dk_serial_number
														,count(distinct date_)			as n_dates
												from	z_pa_events_fact
												where	date_ between '2016-06-01' and '2016-06-30'
												group	by	stb_type
															,dk_serial_number
												having	n_dates > 14
											)	as ref
								on	base.dk_serial_number	= ref.dk_serial_number
						--where	base.date_ between '2016-07-01 00:00:00' and '2016-07-19 00:00:00' --> Pre
						where	date_ between '2016-08-16 00:00:00' and '2016-09-03 00:00:00' --> Post
						group	by	base.date_
									,base.dk_serial_number
									,base.gn_lvl2_session
									,base.gn_lvl2_session_grain
--						order	by	date_
--									,dk_serial_number
--									,start_
					)	as base
		)	as base2
where	lower(origin) like 'home%'
and		gn_lvl2_session in	(
								'TV Guide'
								,'Catch Up'
								,'Recordings'
								,'My Q'
								,'Top Picks'
								,'Sky Box Sets'
								,'Sky Movies'
								,'Sky Store'
								,'Sports'
								,'Kids'
								,'Music'
								,'Online Videos'
								,'Search'
							)
			

			
--For Plus:

/*
	Description:
	
	
	
*/

select	count(distinct concat(string(thedate),string(viewing_card),string(sky_plus_session_grain)))	as n_TLMs_from_home
		,count(distinct viewing_card)																as nboxes
from    (
			select  thedate
					,viewing_card
					,sky_plus_session
					,sky_plus_session_grain
					,min(sky_plus_session_grain) over (
														PARTITION BY  thedate
																	  ,viewing_card
														ORDER BY      start_
														rows between  1 preceding and 1 preceding
													  ) as origin
			from    (
						SELECT  thedate
								,viewing_card
								,sky_plus_session
								,sky_plus_session_grain
								,min(integer(concat(string(sessionid),string(actions_sequence)))) as start_
						from	table_query(Q_PA_Stage,"table_id contains 'z_final_201507'") -- Pre
						from 	table_query(Q_PA_Stage,"table_id contains 'z_final_' and (table_id contains '201508' or table_id contains '201509')") -- Post
						group   by  thedate
									,viewing_card
									,sky_plus_session
									,sky_plus_session_grain
					)   as base
		)   as base2
where   lower(origin) like 'home%'
and     sky_plus_session in (
							  'TV Guide'
							  ,'Catch Up TV'
							  ,'Recordings'
							  ,'Top Picks'
							  ,'Sky Box Sets'
							  ,'Sky Cinema'
							  ,'Sky Movies'
							  ,'Sky Store'
							  ,'Sports'
							  ,'Kids'
							  ,'Music'
							  ,'Online Videos'
							  ,'Search'
							)