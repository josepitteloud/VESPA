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

        This request aims to give some foundational evidence of customers engagement with UHD content through channel tunnings.
		
		The goal is to understand what Q Session is driving most of the access to UHD content.
		
**Assumptions:

		I've noticed there is one particular value for channel tune-ins actions on their dk_referrer_id that doesn't allow the Sessions Logic to
		identify Fullscreen sessions. this value is 'referrer', this value should not be in place but after checking the data is safe to place the condition
		saying that for all tune-in activity where the dk_referrer_id = 'referrer',  that's activity that has to be attributed to fullscreen.
		
**Sections:

		A - Drafting Sky Q Journey foundational measures			
			A00 - Pre-Requisites
			A01 - Tuning UHD channels identification
			A02 - Generating Metrics
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/

-----------------------
-- A00 - Pre-Requisites
-----------------------

	/*
		An alternative table to Z_PA_EVENTS_FACT has had to be created for the time-frame comprised from the 6th of August onwards.
		
		The reason been is that Z_PA_EVENTS_FACT does not currently surface -> dk_channel_id <-
		
		This Pre-Requisites' output is = z_pa_step_1_b
	*/

	
-------------------------------------------
-- A01 - Tuning UHD channels identification
-------------------------------------------	

select	datehour_
		,case	when dk_referrer_id = 'referrer'		then 'Fullscreen'
				--when dk_referrer_id = 'liveTrickPlay'	then 'Mini Guide' --->> PROBABLY
				when gn_lvl3_session = 'Prompt' 		then gn_lvl3_session
				else gn_lvl2_session 
		end 	Sky_Q_Sessions
		,substr(dk_channel_id,3,4)																		as channel
		,count(Distinct dk_serial_number) 																as n_boxes
		,count(distinct (case when dk_channel_id like 'Â¿900%' then dk_serial_number else null end))	as n_boxes_tuning_uhd
		,sum(case when dk_channel_id like 'Â¿900%' then 1 else 0 end)									as n_uhd_tunes
		,sum(case when dk_action_id = 00300 then 1 else 0 end)											as prompt_seen
from 	z_pa_step_1_b --z_uhd_day
where	dk_channel_id like 'Â¿900%'
--and		date_ = '2016-08-14'--> TEMP
group	by	datehour_
			,Sky_Q_Sessions
			,channel
LIMIT	100


-- GET this in place to identify journeys in which a red button was pressed and then count accordingly at journey level in the first query atop

select	base.date_
		,base.dk_serial_number
		,base.gn_lvl2_session_grain
		,sum(case when base.dk_action_id = 04002 then 1 else 0 end) as theflag	-- Red Button
		,sum(case when base.dk_action_id = 00300 then 1 else 0 end) as theflag2 -- Prompt
		,sum(case when base.dk_action_id = 01000 then 1 else 0 end) as theflag3 -- Miniguide
		,count(1) as n_actions
from	z_pa_step_1_b as base
		inner join	(
						-- 0) for all journeys that show an UHD tuning on Fullscreen...
						select	distinct
								date_
								,dk_serial_number
								,gn_lvl2_session_grain
						from	z_pa_step_1_b
						where	gn_lvl2_session = 'Fullscreen'
						and		dk_channel_id like 'Â¿900%'
						and		dk_referrer_id = 'referrer'
						and		date_ = '2016-08-19'
					)	as ref
		on	base.dk_serial_number		= ref.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
		and	base.date_					= ref.date_
where	base.date_ = '2016-08-19'
and		base.gn_lvl2_session = 'Fullscreen'
group	by	base.date_
			,base.dk_serial_number
			,base.gn_lvl2_session_grain
having	theflag <=0
and		theflag2 <=0
and		theflag3 <=0
limit	100

---------------------------
-- A02 - Generating Metrics
---------------------------

select	base2.datehour_
		,case	--when dk_referrer_id = 'referrer'		then 'Fullscreen' 
				when ref2.red_button >0 					then 'Fullscreen - Red Button'
				when ref2.prompt > 0 						then 'Prompt'
				when ref2.miniguide > 0 					then 'Mini Guide'
				when base2.gn_lvl3_session = 'Prompt' 		then gn_lvl3_session
				when ref2.gn_lvl2_session_grain is not null	or base2.GN_LVL2_SESSION = 'Fullscreen' then 'Fullscreen - unidentified'
				when base2.dk_referrer_id = 'liveTrickPlay'	then 'Mini Guide'
				else base2.gn_lvl2_session 
		end 	as Sky_Q_Sessions
		,substr(base2.dk_channel_id,3,4)																			as channel
		,count(Distinct base2.dk_serial_number) 																	as n_boxes
		,count(distinct (case when base2.dk_channel_id like 'Â¿900%' then base2.dk_serial_number else null end))	as n_boxes_tuning_uhd
		,sum(case when base2.dk_channel_id like 'Â¿900%' then 1 else 0 end)											as n_uhd_tunes
		,sum(case when base2.dk_action_id = 00300 then 1 else 0 end)												as prompt_seen
from 	z_pa_step_1_b	as base2
		left join	(
						select	base.date_
								,base.dk_serial_number
								,base.gn_lvl2_session_grain
								,sum(case when base.dk_action_id = 04002 then 1 else 0 end) as red_button	-- Red Button
								,sum(case when base.dk_action_id = 00300 then 1 else 0 end) as Prompt -- Prompt
								,sum(case when base.dk_action_id = 01000 then 1 else 0 end) as Miniguide -- Miniguide
								,count(1) as n_actions
						from	z_pa_step_1_b as base
								inner join	(
												-- 0) for all journeys that show an UHD tuning on Fullscreen...
												select	distinct
														date_
														,dk_serial_number
														,gn_lvl2_session_grain
												from	z_pa_step_1_b
												where	gn_lvl2_session = 'Fullscreen'
												and		dk_channel_id like 'Â¿900%'
												and		dk_referrer_id = 'referrer'
												--and		date_ = '2016-08-19'
											)	as ref
								on	base.dk_serial_number		= ref.dk_serial_number
								and	base.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
								and	base.date_					= ref.date_
						--where	base.date_ = '2016-08-19'
						and		base.gn_lvl2_session = 'Fullscreen'
						group	by	base.date_
									,base.dk_serial_number
									,base.gn_lvl2_session_grain
						--having	theflag <=0
						--and		theflag2 <=0
						--and		theflag3 <=0
					)	as ref2
		on	base2.DATE_ 				= ref2.date_
		and	base2.DK_SERIAL_NUMBER		= ref2.dk_serial_number
		and	base2.GN_LVL2_SESSION_grain	= ref2.gn_lvl2_session_grain
where	base2.dk_channel_id like 'Â¿900%'
--and		base2.date_ = '2016-08-19'
group	by	base2.datehour_
			,Sky_Q_Sessions
			,channel