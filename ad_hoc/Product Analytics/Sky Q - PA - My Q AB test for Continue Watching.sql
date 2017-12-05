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
**Project Name:							PRODUCTS ANALYTICS 	(PA)
**Done by:                             	Angel Donnarumma	(angel.donnarumma@sky.uk)
**Stakeholder:                          Daniel Chronnell

**Business Brief:

        On the original version deployed within R4, My Q has its first signpost to be Continue Watching. 
		However, there is an alternative hypothesis that holding Continue Watching as the first signpost 
		could cause users not to explore on other assets underneath this category (and been true, then 
		limiting the amount of assets users are exposed to)

		A test was designed to validate on this hypothesis were a branched R4 has Continue watching after 
		a couple of other signposts (within My Q)
	
		Can we measure which version drives more value to customers?
		
**Considerations:

		Better Value is currently been considered as:
			+ More Converted journeys per STB
			+ More asset consumption per STB
			+ Above spread across a range of Signpost (not just Continue watching)
			+ My Q therefore increasing its Home Page Share of converted journeys across the rest of TLMs
		
**Sections:

		A - Data Evaluation
			1 - how long the test and sample of STBs is been running from
			
		B - Data Preparation
			
		C - Data Analysis (Queries)
			2 - Verifying Home Page Performance for A/B Groups...
			3 - Verifying on SLM (signposts) performance in My Q on R4 for A/B Groups
			
**Pre-Requisits:

	- z_pa_events_fact_ (labeled as z_pa_events_fact_YYYYMM, in this specific case z_pa_events_fact_201704)
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/

----------------------------------------------------------------
-- 1 - how long the test and sample of STBs is been running from
----------------------------------------------------------------

/*
	Using PA_EVENTS_FACT to flag the STBs (dk_Serial_number) in each group over targeted timeframe
*/

select	dk_date
		,trial
		,count(distinct dk_SErial_number) as nboxes
from	pa_events_Fact
where	dk_Date between 20170323 and 20170420
and		trial <> ''
group	by	dk_date
			,trial
			
--------------------------------------------------------
-- 2 - Verifying Home Page Performance for A/B Groups...
--------------------------------------------------------

with	ground as 			(
								select	*
								from	z_pa_events_fact_YYYYMM
								where	trial in ('SKYQ_TRIAL_A','SKYQ_TRIAL_B')
								and		date_ between ? and  ?
								union
								select	*
								from	z_pa_events_fact_YYYYMM
								where	trial in ('SKYQ_TRIAL_A','SKYQ_TRIAL_B')
								and		date_ between ? and  ?
								.
								.
								.
							)
		,ref_ses_conv as	(
							-- Measuring for converted/exploring sessions
							-- this is used to measure Time_To... used by our current capping criteria
							select	date_
									,dk_serial_number
									,session_grain
									,min(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end)	as x
							from	ground
							where	session = 'Home'
							and		dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,session_grain
						)
		,ref_ses as		(
							-- Identifying Home Sessions we are considering valid for analysis
							select	date_
									,dk_serial_number
									,session_grain
									,case when time_to_conv is not null then 1 else 0 end	as conv_flag
									,coalesce(time_to_conv,time_spent)						as time_to_
							from	(
										select	base.date_
												,base.dk_serial_number
												,base.session_grain
												,sum(base.ss_elapsed_next_action)																						as time_spent
												,sum(case when ref.session_grain is not null and base.index_ < ref.x then base.ss_elapsed_next_action else null end)	as time_to_conv
										from	ground		as base
												left join ref_ses_conv	as ref
												on	base.date_				= ref.date_
												and	base.dk_serial_number	= ref.dk_serial_number
												and	base.session_grain		= ref.session_grain
										where	base.session = 'Home'
										group	by	base.date_
													,base.dk_serial_number
													,base.session_grain
									)	as final_ 
							where	(	
										(conv_flag = 1 and time_to_conv between 0 and 600) -- 95% of all converted journeys
										or
										(conv_flag = 0 and time_spent between 0 and 1000) -- 90% of all exploratory journeys
									)
						)
		,base as		(
							-- extracting the base data for:
								-- All valid Home Sessions
								-- and from those, isolating journeys that started from the Home Page only
							select	a.*
							from	ground	as a
									inner join ref_ses	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_grain			= b.session_grain
									inner join ref_home_start_	as ref_home
									on	a.date_					= ref_home.date_
									and	a.dk_Serial_number		= ref_home.dk_serial_number
									and a.gn_lvl2_session_grain	= ref_home.target
						)
		,ref_conv as	(
							-- Flagging exact point of conversions for journeys at SLM lvl that did so
							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							where	dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
		,base_size as	(
							-- counting the total number of STBs we are looking at in this exercise
							select	trial
									,count(distinct dk_serial_number) as nactive_boxes
							from	base
							group	by	trial
						)
select	base.trial
		,case when base.gn_lvl2_session = 'Top Picks' then 'My Q' else base.gn_lvl2_session end 												as gn_lvl2_session_
		,max(base_size.nactive_boxes)																											as active_boxes
		,count(distinct base.dk_serial_number)																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.index_ < ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
		,sum(case when base.dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)										as nconv_actions
from	base
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join base_size
		on base.trial	= base_size.trial
group	by	base.trial
			,gn_lvl2_session_
			
----------------------------------------------------------------------------
-- 3 - Verifying on SLM (signposts) performance in My Q on R4 for A/B Groups
----------------------------------------------------------------------------

with	ground as 		(
							select	*
							from	z_pa_events_fact_YYYYMM
							where	trial in ('SKYQ_TRIAL_A','SKYQ_TRIAL_B')
							and		date_ between ? and  ?
							union
							select	*
							from	z_pa_events_fact_YYYYMM
							where	trial in ('SKYQ_TRIAL_A','SKYQ_TRIAL_B')
							and		date_ between ? and  ?
							.
							.
							.
						)
		,base as		(
							select	*
									,last_value(x1 ignore nulls) over	(
																			partition by	date_
																							,dk_serial_number
																							,gn_lvl3_Session_Grain 
																			order by 		index_
																			rows between 	80 preceding and current row
																		)							as signpost_grain
									,substr(signpost_grain,1,instr(signpost_grain,'-')-1)			as signpost
							from	(
										select	a.*
												,b.signpost as ff
												,case	when b.signpost is null and a.dk_previous like '%/EVOD%'					then gn_lvl3_Session_grain
														when b.signpost is not null 												then b.signpost
														else null
												end		as x
												,x||'-'||dense_rank()	over	(
																					partition by	a.date_
																									,a.dk_serial_number
																									,a.gn_lvl3_Session_Grain
																									,x
																					order by 		a.index_
																				)	as x1
										from	ground 							as a
												left join pa_signpost_events	as b
												on	a.dk_serial_number	= b.dk_Serial_number
												and	a.timems			= b.timems
										--and		a.gn_lvl2_session = 'Top Picks'
										--and		a.dk_serial_number in ('32B0630488567908','32B0570488077074','32B0570488092681','32B0570488121818')
										--and		a.dk_serial_number = '32B0570488121818'
									)	as base
							--order	by	index_
						)
		,base_size	as	(
							select	trial
									,count(distinct dk_serial_number)	as active_STBs
							from	base
							group	by	trial
						)
select	base.trial
		,signpost
		,max(base_size.active_STBs)																										as Active_STBs
		,count(distinct dk_Serial_number) 																								as reach
		,count(distinct (case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then dk_Serial_number else null end))	as conv_reach
		,count(distinct date_||'-'||gn_lvl3_session_grain||'-'||signpost_grain||'-'||dk_serial_number)									as njourneys
		,count(distinct	(
							case	when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)	then date_||'-'||gn_lvl3_session_grain||'-'||signpost_grain||'-'||dk_serial_number
									else null
							end
						))	as nconv_journeys
		,sum(case when dk_Action_id  = 02400 then 1 else 0 end)							as ndownloads
		,sum(case when dk_Action_id  = 03000 then 1 else 0 end)							as nplaybacks
		,sum(case when dk_Action_id  = 00001 then 1 else 0 end)							as ntunings
		,sum(case when dk_Action_id  in (02000,02010,02002,02005) then 1 else 0 end)	as nbookings
from	base
		inner join 	base_size
		on	base.trial	= base_size.trial
where	signpost <> 'Top Picks'
group	by	base.trial
			,signpost