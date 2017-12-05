
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
**Project Name:		PRODUCTS ANALYTICS 	(PA)
**Done by:          Angel Donnarumma
**Stakeholder:      Daniel Chronnell, Daniel Delieu, Simon Waldman
**Due Date:        

**Business Brief:

        Measuring the impact R4 brings to the platform  (using R3 as benchmark)
		
**Considerations:

		
**Sections:

		A - Data Evaluation
			
		B - Data Preparation
			
		C - Data Analysis (Queries)
		
		D - Vault
			
**Pre-requisites:

		- z_pa_events_fact_v2
			This holds Sessions (Home/Fullscreen). a just in case measure + maintaining context with what shown before
			
		- ref_home_start
			Collection of all journeys that started from the Home Page
			
		- z_pa_kpi_def_lvl1
			a list of valid sessions we are interested on
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/

-----------------------
-- B - DATA PREPARATION
-----------------------

--drop table	z_pa_events_fact_v2;commit;
--truncate table	z_pa_events_fact_v2;commit;

insert	into z_pa_events_fact_v2
select	index_
		,date_
		,dt	
		,dk_serial_number
		,extract(epoch from (min(dt) over (partition by date_,dk_serial_number order by index_ rows between 1 following and 1 following))- dt) 	as ss_before_next_action
		,extract(epoch from	dt - ( min(dt) over	(partition by date_,dk_serial_number order by	index_ rows between	1 preceding and 1 preceding)))		as ss_elapsed_next_action	-- time difference between current action and preceding action in seconds
		,last_value(w0 ignore nulls) over	(
												partition by	date_
																,dk_serial_number
												order by		index_
											)	as Session_type -- as w
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,gn_lvl2_session
		,gn_lvl2_session_grain
--into	z_pa_events_fact_v2
from	(
			select	*
					,x1||'-'||dense_rank() over (partition by date_,dk_serial_number,x1 order by index_) as w0
			from	(
						select	*
								,max(y0)  over	(
													partition by 	date_
																	,dk_serial_number
													order by		index_
													rows between	1 preceding and 1 preceding
												)	as z
								,case	when (z is null or z<>y0)	then x
										else null
								end		x1
						from	(
									select	index_
											,date_
											,dt
											,dk_serial_number
											,gn_lvl2_session
											,gn_lvl2_session_grain
											,dk_action_id
											,dk_previous
											,dk_current
											,dk_referrer_id
											,dk_trigger_id
											,case	when gn_lvl2_session in ('Home','Fullscreen')	then gn_lvl2_session 
													when dk_action_id = 00003 						then 'Stand By Out'
													when dk_action_id = 00004						then 'Reboot'
													else null end	as x
											,last_value(x ignore nulls) over	(
																					partition by	date_
																									,dk_serial_number
																					order by 		index_
																					rows between	200 preceding and current row
																				) as  y0
									from	z_pa_events_fact
									where	date_ between '2017-03-08' and '2017-03-11' --> Parameter
									--where	date_ = '2016-11-04 00:00:00'
									--and		dk_serial_number = '32B0580488179819' -- bingo!
									--order	by	index_
									--limit	200
								)	as base
					)	as	step_1
		)	as	step2;
--order	by	index_
commit;


--drop table	ref_home_start;commit;
--truncate table	ref_home_start;commit;


insert	into ref_home_start
select	*
-- into	ref_home_start
from	(
			select	date_
					,dk_serial_number
					,gn_lvl2_session
					,target
					,n_globnav_clicks
					,max(target) over	(
											partition by	date_
															,dk_serial_number
											order by		start_
											rows between	1 preceding and 1 preceding
										)	as origin
			from	(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain 																	as target
								,min(index_)																			as start_
								,sum(case when dk_action_id = 01400 and dk_trigger_id <> 'system-' then 1 else 0 end)	as n_globnav_clicks
						from 	z_pa_events_fact_v2
						where	date_ between '2017-03-08' and '2017-03-11' --> Parameter
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
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
							);

commit;



--drop table	z_pa_kpi_def_lvl1;commit;
--truncate table	z_pa_kpi_def_lvl1;commit;

insert	into z_pa_kpi_def_lvl1
with	ref_conv as	(

						/*
							Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							to use that then as a flag to derive time to conversion (this is, how many seconds since the
							beginning of the Session until the very first converting action)
						*/

						select	date_
								,dk_serial_number
								,session_type
								,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) 	as x
								--,max(index_)																							as end_
						from	z_pa_events_fact_v2
						where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
						and		date_ between '2017-03-08' and '2017-03-11' --> Parameter
						--and		date_ = '2016-10-04 00:00:00'
						--and		dk_serial_number = '32B0560488008521'
						--and		session_type = 'Home-9'
						group	by	date_
									,dk_serial_number
									,session_type
					)
select	*
--into	z_pa_kpi_def_lvl1
from	(
			select	*
					,min(session_type) over	(
												partition by	date_
																,dk_Serial_number
												order by		the_index
												rows between	1 following and 1 following
											)	as x
			from	(
						select	extract(month from a.date_)																		as the_month
								,a.date_
								,a.dk_serial_number
								,a.session_type
								,max(
										case	when a.dk_action_id = 00002 and a.dk_trigger_id = 'timeOut-' 				then 'Stand By in - System Time out'
												when a.dk_action_id = 00002 and a.dk_trigger_id = 'userInput-powerButton'	then 'Stand By in - Manual Power Off'
										end		
									)	as	ending
								,max(case when a.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
								,min(a.index_)																					as the_index
								,sum(case when a.dk_trigger_id <> 'system-' then 1 else 0 end)									as nclicks 					-- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
								,sum(case when a.dk_trigger_id <> 'system-' and a.dk_action_id = '01400' then 1 else 0 end)		as n_ses_globnav_clicks 	-- Likewise above, although this time only interested in navigation clicks
								,sum( case when c.target is not null and a.dk_action_id = '01400' then 1 else 0 end )			as n_jour_globnav_clicks 	-- Then here only counting those global nav / not-system clicks done within journeys (subset of above)
								,count(distinct	(
													case	when a.gn_lvl2_session in	(
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
																						)	then a.gn_lvl2_session
															else 	null
													end
												))	as ntlms_visited
								,count(distinct (c.date_||'-'||c.dk_serial_number||'-'||c.target))								as ntlms_journeys
								,sum(a.ss_elapsed_next_action)																	as session_length_ss
								,sum( case when a.index_ <= b.x 	then a.ss_elapsed_next_action else null end)				as time_to_conv
								,sum( case when a.dk_action_id = 04002 then 1 else 0 end) 										as n_applaunches
								,sum( case when a.dk_action_id = 03000 then 1 else 0 end)										as n_playbacks
								,sum( case when a.dk_action_id = 02400 then 1 else 0 end)										as n_downloads
								,sum( case when a.dk_action_id = 00001 then 1 else 0 end)										as n_tunings
								,sum( case when a.dk_action_id in (02000,02010,02002,02005) then 1 else 0 end)					as n_bookings
						from	z_pa_events_fact_v2 	as a
								left join	ref_conv	as b
								on	a.date_				= b.date_
								and	a.dk_serial_number	= b.dk_serial_number
								and	a.session_type		= b.session_type
								left join	ref_home_start	as c
								on	a.date_					= c.date_
								and	a.dk_serial_number		= c.dk_serial_number
								and	a.gn_lvl2_session_grain	= c.target
						where	a.date_ between '2017-03-08' and '2017-03-11' --> Parameter
						--where	a.date_ = '2016-10-04 00:00:00'
						--and		a.dk_serial_number = '32B0560488008521'
						--and		a.session_type = 'Home-9'
						group	by	the_month
									,a.date_
									,a.dk_serial_number
									,a.session_type
					)	as base
		)	as	step1
where	session_type like 'Home%'
and		(
			ending <> ''
			or
			x like 'Fullscreen%'
		);
commit;


------------------------------
-- C - Data Analysis (Queries)
------------------------------

-- Sky Button Presses Pre and Post...

with	r4 as	(
					-- This is the list of STBs that make up the trialist group
					-- 100K STBs who got R4 before launch
					
					/*
						A couple of days this would have thrown the following number [119017]
						
						Today (2017-03-23) is giving [107558] 10% less...
					*/
					
					select	distinct
							SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER				
							,date(dt_effective_from)			as date_
					from	pa_system_information
					where	DT_EFFECTIVE_TO	=	'9999-09-09'
					and		date(dt_effective_from) between '2017-03-09' and '2017-03-20'
					AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' 
				)
select	count(distinct base.date_)				as ndates
		,count(1)								as nsky_buttons
		,count(distinct base.dk_Serial_number)	as reach
from	z_pa_events_fact as base
		-- For R3
				left join r4
				on	base.dk_serial_number		= r4.dk_serial_number
where	r4.dk_serial_number is null
		
		-- For R4
--				inner join r4
--				on	base.dk_serial_number	= r4.dk_serial_number
--				and	base.date_ 				>= r4.date_
 -- Pre
--where	(
--			base.date_ between '2017-02-23' and '2017-02-24'	--> I Parameter
--			or
--			base.date_ between '2017-03-01' and '2017-03-06'	--> II Parameter 
--		)
-- Post
and		(
			base.date_ between '2017-03-09' and '2017-03-10'	--> I Parameter
			or
			base.date_ between '2017-03-15' and '2017-03-20'	--> II Parameter 
		)
and		base.dk_trigger_id in	(
									'userInput-KeyEvent:Key_SkyKeyPressed'
									,'userInput-KeyEvent:Key_SkyKeyReleased'
								)





-- A VERSION OF ABOVE BUT WITH SESSIONS IN PLACE (THIS IS WHAT WE WANT)...

-- LvL 2
with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
									left join	(
													-- Exclusion list: Sessions we don't want to consider for this exercise due either been bugs or
													-- represent a behaviour that is not in line with concious actions performed by the user
													-- we don't currently treat time-out as such...
													select	date_
															,dk_serial_number
															,session_type
													FROM	z_pa_kpi_def_lvl1
													where	(
																(ending <> '' and x like 'Fullscree%') 							-- <1%
																or
																(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))	-- <1%
																or
																session_length_ss <= 0											-- <1%
																or
																ending = 'Stand By in - System Time out'
															) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
												)	as excl_list
									on	base.date_				= excl_list.date_
									and	base.dk_Serial_number	= excl_list.dk_serial_number
									and	base.session_type		= excl_list.session_type
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							/* -- Before R4
							and		(
										base.date_ between '2017-02-23' and '2017-02-24'	--> I Parameter
										or
										base.date_ between '2017-03-01' and '2017-03-06'	--> II Parameter 
									) */
							/* -- After R4
							and		(
										base.date_ between '2017-03-09' and '2017-03-10'	--> I Parameter
										or
										base.date_ between '2017-03-15' and '2017-03-20'	--> II Parameter 
									) */
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,r4 as			(
							-- This is the list of STBs that make up the trialist group
							-- 100K STBs who got R4 before launch
							
							/*
								A couple of days this would have thrown the following number [119017]
								
								Today (2017-03-23) is giving [107558] 10% less...
							*/
							
							select	distinct
									SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER				
									,date(dt_effective_from)			as date_
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							and		date(dt_effective_from) between '2017-03-09' and '2017-03-21'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' 
						)
		,base as		(
							select	a.*
							from	z_pa_events_fact_v2		as a
									inner join ses_sample 	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_type			= b.session_type
							/* -- For R3
									left join r4
									on	a.dk_serial_number		= r4.dk_serial_number
							where	r4.dk_serial_number is null */
							
							/* -- For R4
									inner join r4
									on	a.dk_serial_number		= r4.dk_serial_number
									and	a.date_ 				>= r4.date_ */
						)
		,ref_conv as	(
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
							select	count(distinct dk_serial_number) as nactive_boxes
							from	base
						)				
select	base.gn_lvl2_session
		/* -- For R4
		case when base.gn_lvl2_session = 'Top Picks' then 'My Q' else base.gn_lvl2_session end 													as gn_lvl2_session_ */
		,max(base_size.nactive_boxes)																											as active_boxes
		,count(distinct base.dk_serial_number)																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_Session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_Grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.index_< ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
from	base
		inner join ref_home_start	as ref_home
		on	base.date_					= ref_home.date_
		and	base.dk_Serial_number		= ref_home.dk_serial_number
		and base.gn_lvl2_session_grain	= ref_home.target
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join base_size
		on 1=1
/* -- For R3
group	by	base.gn_lvl2_session*/
/* -- For R4
group	by	gn_lvl2_session_*/
			
			
-- LvL 3 (Not tested yet)

 with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
									left join	(
													-- Exclusion list: Sessions we don't want to consider for this exercise due either been bugs or
													-- represent a behaviour that is not in line with concious actions performed by the user
													-- we don't currently treat time-out as such...
													select	date_
															,dk_serial_number
															,session_type
													FROM	z_pa_kpi_def_lvl1
													where	(
																(ending <> '' and x like 'Fullscree%') 							-- <1%
																or
																(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))	-- <1%
																or
																session_length_ss <= 0											-- <1%
																or
																ending = 'Stand By in - System Time out'
															) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
												)	as excl_list
									on	base.date_				= excl_list.date_
									and	base.dk_Serial_number	= excl_list.dk_serial_number
									and	base.session_type		= excl_list.session_type
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							/* -- Before R4
							and		(
										base.date_ between '2017-02-23' and '2017-02-24'	--> I Parameter
										or
										base.date_ between '2017-03-01' and '2017-03-06'	--> II Parameter 
									) */
							/* -- After R4
							and		(
										base.date_ between '2017-03-09' and '2017-03-10'	--> I Parameter
										or
										base.date_ between '2017-03-15' and '2017-03-20'	--> II Parameter 
									) */
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,r4 as			(
							-- This is the list of STBs that make up the trialist group
							-- 100K STBs who got R4 before launch
							
							/*
								A couple of days this would have thrown the following number [119017]
								
								Today (2017-03-23) is giving [107558] 10% less...
							*/
							
							select	distinct
									SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER				
									,date(dt_effective_from)			as date_
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							and		date(dt_effective_from) between '2017-03-09' and '2017-03-21'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' 
						)
		,base as		(
							select	a.*
							from	z_pa_events_fact_v3		as a
									inner join ses_sample 	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_type			= b.session_type
							/* -- For R3
									left join r4
									on	a.dk_serial_number		= r4.dk_serial_number
							where	r4.dk_serial_number is null */
							
							/* -- For R4
									inner join r4
									on	a.dk_serial_number		= r4.dk_serial_number
									and	a.date_ 				>= r4.date_ */
						)
		,ref_conv as	(
							select	date_
									,dk_serial_number
									,gn_lvl3_session_grain
									,min(case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							where	dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,gn_lvl3_session_grain
						)
		,base_size as	(
							select	count(distinct dk_serial_number) as nactive_boxes
							from	base
						)
select	base.gn_lvl2_session
		/* -- For R4
		case when base.gn_lvl2_session = 'Top Picks' then 'My Q' else base.gn_lvl2_session end 													as gn_lvl2_session_ */
		,base.gn_lvl3_session
		,max(base_size.nactive_boxes)																											as active_boxes
		,count(distinct base.dk_serial_number)																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl3_session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl3_session_grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent
		,sum(case when ref_conv.gn_lvl3_session_grain is not null and base.index_ < ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
		,sum(case when base.dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)										as nconv_actions
from	base
		inner join ref_home_start	as ref_home
		on	base.date_					= ref_home.date_
		and	base.dk_Serial_number		= ref_home.dk_serial_number
		and base.gn_lvl2_session_grain	= ref_home.target
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl3_session_grain	= ref_conv.gn_lvl3_session_grain
		inner join base_size
		on 1=1
/* -- For R3
where	base.gn_lvl2_session <> base.gn_lvl3_session 
group	by	base.gn_lvl2_session
			,base.gn_lvl3_session*/
/* -- For R4
where	gn_lvl2_session_ <> base.gn_lvl3_session
group	by	gn_lvl2_session_
			,base.gn_lvl3_session*/
			
------------------------------------------------------- POTENTIAL SUBSTITUTION OF ABOVE DUE TV GUIDE ALL CHANNELS SITUATION

with	ref_ses_conv as	(
							-- Measuring for converted/exploring sessions
							-- this is used to measure Time_To... used by our current capping criteria
							select	date_
									,dk_serial_number
									,session_grain
									,min(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end)	as x
							from	z_pa_events_fact_
							where	session = 'Home'
							and		dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							and		date_ >= '2017-03-09' --> Parameter
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
										from	z_pa_events_fact_		as base
												left join ref_ses_conv	as ref
												on	base.date_				= ref.date_
												and	base.dk_serial_number	= ref.dk_serial_number
												and	base.session_grain		= ref.session_grain
										where	base.date_ >= '2017-03-09' --> Parameter
										and		base.session = 'Home'
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
		,r4 as			(
							-- This is the list of STBs that make up the trialist group
							-- 100K STBs who got R4 before launch							
							select	distinct
									SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER				
									,date(dt_effective_from)			as date_
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							and		date(dt_effective_from) between '2017-03-09' and '2017-03-21'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' 
						)
		,base as		(
							-- extracting the base data for:
								-- All valid Home Sessions
								-- and from those, isolating journeys that started from the Home Page only
								-- for PRE / POST R4 Period & STBs
							select	a.*
							from	z_pa_events_fact_	as a
									inner join ref_ses	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_grain			= b.session_grain
									inner join ref_home_start_	as ref_home
									on	a.date_					= ref_home.date_
									and	a.dk_Serial_number		= ref_home.dk_serial_number
									and a.gn_lvl2_session_grain	= ref_home.target
							/* -- For R3
									left join r4
									on	a.dk_serial_number		= r4.dk_serial_number
							where	r4.dk_serial_number is null */
							
							/* -- For R4
									inner join r4
									on	a.dk_serial_number		= r4.dk_serial_number
									and	a.date_ 				>= r4.date_*/
							/*-- Pre
							where	(
										a.date_ between '2017-02-23' and '2017-02-24'	--> I Parameter
										or
										a.date_ between '2017-03-01' and '2017-03-06'	--> II Parameter 
									)*/
							/*-- Post
							where	(
										a.date_ between '2017-03-09' and '2017-03-10'	--> I Parameter
										or
										a.date_ between '2017-03-15' and '2017-03-20'	--> II Parameter 
									)*/
						)
		,ref_conv as	(
							-- Flagging exact point of conversions for journeys at SLM lvl that did so
							select	date_
									,dk_serial_number
									,gn_lvl3_session_grain
									,min(case when dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							where	dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,gn_lvl3_session_grain
						)
		,base_size as	(
							-- counting the total number of STBs we are looking at in this exercise
							select	count(distinct dk_serial_number) as nactive_boxes
							from	base
						)
select	--base.gn_lvl2_session
		-- For R4
		case when base.gn_lvl2_session = 'Top Picks' then 'My Q' else base.gn_lvl2_session end 													as gn_lvl2_session_
		,base.gn_lvl3_session
		,max(base_size.nactive_boxes)																											as active_boxes
		,count(distinct base.dk_serial_number)																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl3_session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl3_session_grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent
		,sum(case when ref_conv.gn_lvl3_session_grain is not null and base.index_ < ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
		,sum(case when base.dk_Action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)										as nconv_actions
from	base
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl3_session_grain	= ref_conv.gn_lvl3_session_grain
		inner join base_size
		on 1=1
/* -- For R3
where	base.gn_lvl2_session <> base.gn_lvl3_session 
group	by	base.gn_lvl2_session
			,base.gn_lvl3_session*/
/*-- For R4
where	gn_lvl2_session_ <> base.gn_lvl3_session
group	by	gn_lvl2_session_
			,base.gn_lvl3_session*/






-------------------------------------------------------

--  A VERSION OF ALL WITHOUT HAVING SESSIONS INVOLVED...

with	r4 as			(
							-- This is the list of STBs that make up the trialist group
							-- 100K STBs who got R4 before launch
							
							/*
								A couple of days this would have thrown the following number [119017]
								
								Today (2017-03-23) is giving [107558] 10% less...
							*/
							
							select	distinct
									SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER				
									,date(dt_effective_from)			as date_
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							and		date(dt_effective_from)<= '2017-03-21 00:00:00'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' 
						)
		,base as		(
							-- Trimming for base data to be analysed...
							select	x.date_
									,x.dk_serial_number
									,x.index_
									,x.gn_lvl2_session
									,x.gn_lvl2_session_grain
									,x.ss_elapsed_next_action
									,x.dk_trigger_id
									,x.dk_action_id
									,case	when r4.dk_serial_number is null then 'R3'
											else 'R4'
									end		as groups
									,case	when r4.dk_serial_number is not null and x.gn_lvl2_session = 'Top Picks' then 'My Q' 
											else x.gn_lvl2_session 
									end 	as Sky_Q_sections
							from	z_pa_events_fact as x
									--inner join r4
									left join r4
									on	x.dk_serial_number = r4.dk_serial_number
									and	x.date_				= r4.date_
									-- Carving for Journeys starting at Home Page...
									inner join ref_home_start	as ref_home
									on	x.date_					= ref_home.date_
									and	x.dk_serial_number		= ref_home.dk_serial_number
									and	x.gn_lvl2_session_grain	= ref_home.target
							where	(
										x.date_ between '2017-03-08' and '2017-03-10'	--> I Parameter
										or
										x.date_ between '2017-03-15' and '2017-03-21'	--> II Parameter
									)
							and		r4.dk_serial_number is null
						)
		,ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	base
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		dk_Action_id in (02400,03000,00001,02000,02010,02002,02005)
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
		,base_size as	(
							select	count(distinct x.dk_serial_number) as nactive_base
							from	z_pa_events_fact as x
									--inner join r4
									left join r4
									on	x.dk_serial_number 	= r4.dk_serial_number
									and	x.date_				= r4.date_
							where	(
										x.date_ between '2017-03-08' and '2017-03-10'	--> I Parameter
										or
										x.date_ between '2017-03-15' and '2017-03-21'	--> II Parameter 
									)
							and		r4.dk_serial_number is null
						)
select	base.groups
		,base.Sky_Q_sections
		,max(base_size.nactive_base)																											as tot_active_base
		,count(distinct base.dk_serial_number) 																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent_secs
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.index_ <= ref_conv.x then base.ss_elapsed_next_action else null end) as sces_to_conversion						
from	base
		-- For converted journeys, getting index_ of conversion...
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		-- just adding in a reference to # Active STBs base over the whole timeframe...
		inner join base_size
		on	1=1
group	by	base.groups
			,base.sky_q_sections
			
			
			
-- A VERSION OF ABOVE BUT WITH SESSIONS IN PLACE (THIS IS WHAT WE WANT)...
with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
									left join	(
													-- Exclusion list: Sessions we don't want to consider for this exercise due either been bugs or
													-- represent a behaviour that is not in line with concious actions performed by the user
													-- we don't currently treat time-out as such...
													select	date_
															,dk_serial_number
															,session_type
													FROM	z_pa_kpi_def_lvl1
													where	(
																(ending <> '' and x like 'Fullscree%') 							-- <1%
																or
																(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))	-- <1%
																or
																session_length_ss <= 0											-- <1%
																or
																ending = 'Stand By in - System Time out'
															) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
												)	as excl_list
									on	base.date_				= excl_list.date_
									and	base.dk_Serial_number	= excl_list.dk_serial_number
									and	base.session_type		= excl_list.session_type
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							--and		base.date_ = '2017-03-09' --> PARAMETER
							and		(
										base.date_ between '2017-02-23' and '2017-02-24'	--> I Parameter
										or
										base.date_ between '2017-03-01' and '2017-03-06'	--> II Parameter 
									)
							--and		base.dk_serial_number = '32B0560488000232'
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,r4 as			(
							-- This is the list of STBs that make up the trialist group
							-- 100K STBs who got R4 before launch
							
							/*
								A couple of days this would have thrown the following number [119017]
								
								Today (2017-03-23) is giving [107558] 10% less...
							*/
							
							select	distinct
									SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER				
									,date(dt_effective_from)			as date_
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							and		date(dt_effective_from)<= '2017-03-21 00:00:00'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' 
						)
		,base as		(
							select	a.*
							from	z_pa_events_fact_v2		as a
									inner join ses_sample 	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_type			= b.session_type
									left join r4
									on	a.dk_serial_number		= r4.dk_serial_number
							where	r4.dk_serial_number is null
						)
		,ref_conv as	(
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
							select	count(distinct dk_serial_number) as nactive_boxes
							from	base
						)				
select	base.gn_lvl2_session
		--case when base.gn_lvl2_session = 'Top Picks' then 'My Q' else base.gn_lvl2_session end 													as gn_lvl2_session
		,max(base_size.nactive_boxes)																											as active_boxes
		,count(distinct base.dk_serial_number)																									as reach
		,count(distinct ref_conv.dk_serial_number)																								as conv_reach
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_Session_grain) 												as njourneys
		,count(distinct ref_conv.date_||'-'||ref_conv.dk_serial_number||'-'||ref_conv.gn_lvl2_session_Grain)									as nconv_journeys
		,sum(base.ss_elapsed_next_action)																										as time_spent
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and base.index_< ref_conv.x then base.ss_elapsed_next_action else null end)	as time_to_conv
from	base
		inner join ref_home_start	as ref_home
		on	base.date_					= ref_home.date_
		and	base.dk_Serial_number		= ref_home.dk_serial_number
		and base.gn_lvl2_session_grain	= ref_home.target
		left join ref_conv
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join base_size
		on 1=1
group	by	base.gn_lvl2_session



------------
-- D - Vault
------------
/* 
--  from here downwards we have everything we need for querying the data out...


with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
									left join	(
													-- Exclusion list: Sessions we don't want to consider for this exercise due either been bugs or
													-- represent a behaviour that is not in line with concious actions performed by the user
													-- we don't currently treat time-out as such...
													select	date_
															,dk_serial_number
															,session_type
													FROM	z_pa_kpi_def_lvl1
													where	(
																(ending <> '' and x like 'Fullscree%') 							-- <1%
																or
																(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))	-- <1%
																or
																session_length_ss <= 0											-- <1%
																or
																ending = 'Stand By in - System Time out'
															) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
												)	as excl_list
									on	base.date_				= excl_list.date_
									and	base.dk_Serial_number	= excl_list.dk_serial_number
									and	base.session_type		= excl_list.session_type
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							and		base.date_ between '2017-03-09' and '2017-03-10' --> Parameter
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,base_size as	(
							select	the_month
									,count(distinct dk_serial_number) as nactive_boxes
							from	z_pa_kpi_def_lvl1
							where	date_ between '2017-03-09' and '2017-03-10' --> Parameter
							--and		substr(dk_serial_number,3,1) in ('B','C')	-- to isolate Gateways only without having to include stb_type, quick fix...
							group	by	the_month
										--,Stb_type
						)
		,ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	z_pa_events_fact_v2
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		date_ between '2017-03-09' and '2017-03-10' --> Parameter
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
		,r4 as			(
							select	DISTINCT SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' -- 119017
						)
select	base.groups
		,base.Sky_Q_sections
		,max(base_size.nactive_boxes)																			as monthly_active_base
		,count(distinct base.dk_serial_number) 																	as reach
		,count(distinct	base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain)				as n_journeys
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain
											else null 
									end
								)
				)	as n_converted_journeys
		,sum(ss_elapsed_next_action) 																			as n_secs_spent
		,sum( case when ref_conv.gn_lvl2_session_grain is not null and base.INDEX_ < ref_conv.x then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_conversion						
		,sum( case when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end) as nconv_actions
from	(
			select	x.*
					,case	when r4.dk_serial_number is null then 'R3'
							else 'R4'
					end		as groups
					,case when r4.dk_serial_number is not null and x.gn_lvl2_session = 'Top Picks' then 'My Q' else x.gn_lvl2_session end as Sky_Q_sections
			from	z_pa_events_fact_v2 as x
					left join	r4
					on	x.dk_serial_number		= r4.dk_serial_number
			where	x.date_ between '2017-03-09' and '2017-03-10' --> Parameter
		)	as base -- Transactional data with Sessions and Journeys...
		left join	ref_conv 			-- Converted journeys reference
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join	ses_sample	as sessions -- Home Sessions reference
		on	base.date_					= sessions.date_
		and	base.dk_serial_number		= sessions.dk_Serial_number
		and	base.session_type			= sessions.session_type
		inner join 	base_size
		on	sessions.the_month			= base_size.the_month
		inner join	ref_home_start	as c
		on	base.date_					= c.date_
		and	base.dk_serial_number		= c.dk_serial_number
		and	base.gn_lvl2_session_grain	= c.target
group	by	base.groups
			,base.Sky_Q_sections
			
			
			
			
			
-- Dist of journeys by time to...


with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
									left join	(
													-- Exclusion list: Sessions we don't want to consider for this exercise due either been bugs or
													-- represent a behaviour that is not in line with concious actions performed by the user
													-- we don't currently treat time-out as such...
													select	date_
															,dk_serial_number
															,session_type
													FROM	z_pa_kpi_def_lvl1
													where	(
																(ending <> '' and x like 'Fullscree%') 							-- <1%
																or
																(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))	-- <1%
																or
																session_length_ss <= 0											-- <1%
																or
																ending = 'Stand By in - System Time out'
															) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
												)	as excl_list
									on	base.date_				= excl_list.date_
									and	base.dk_Serial_number	= excl_list.dk_serial_number
									and	base.session_type		= excl_list.session_type
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							and		base.date_ between '2017-03-08' and '2017-03-11' --> Parameter
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	z_pa_events_fact_v2
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		date_ between '2017-03-08' and '2017-03-11' --> Parameter
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
		,r4 as			(
							select	DISTINCT SUBSTR("source/serialNumber",1,16)	AS	DK_SERIAL_NUMBER
							from	pa_system_information
							where	DT_EFFECTIVE_TO	=	'9999-09-09'
							AND		SOFTWARE_VERSION LIKE 'Q004.001.55.19L%' -- 119017
						)
select	case	when r4.dk_serial_number is null then 'R3'
				else 'R4'
		end		as groups
		,case when r4.dk_serial_number is not null then 'My Q' else base.gn_lvl2_session end as Sky_Q_sections
		,base.date_
		,base.dk_serial_number
		,base.gn_lvl2_session_grain
		,sum(base.ss_elapsed_next_action) 																											as n_secs_spent
		,sum( case when ref_conv.gn_lvl2_session_grain is not null and base.INDEX_ < ref_conv.x then base.SS_ELAPSED_NEXT_ACTION else null end)	as secs_to_conv
		,coalesce(secs_to_conv,n_secs_spent) as x
from 	z_pa_events_fact_v2	as base
		left join	r4
		on	base.dk_serial_number		= r4.dk_serial_number
		left join	ref_conv 			-- Converted journeys reference
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join	ses_sample	as sessions -- Home Sessions reference to those who we consider valid...
		on	base.date_				= sessions.date_
		and	base.dk_serial_number	= sessions.dk_Serial_number
		and	base.session_type		= sessions.session_type
		inner join	ref_home_start	as c --> Carving only for journeys that started from Home...
		on	base.date_					= c.date_
		and	base.dk_serial_number		= c.dk_serial_number
		and	base.gn_lvl2_session_grain	= c.target
where	base.gn_lvl2_session in ('Top Picks','My Q')
group	by	groups
			,base.date_
			,Sky_Q_sections
			,base.dk_serial_number
			,base.gn_lvl2_session_grain */