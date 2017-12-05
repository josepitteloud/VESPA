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
							from	z_pa_events_fact_	as a
									inner join ref_ses	as b
									on	a.date_					= b.date_
									and	a.dk_serial_number		= b.dk_serial_number
									and	a.session_grain			= b.session_grain
									
									/* Uncomment if you require Home Page Performance (journeys that started from Home) */
									--inner join ref_home_start_	as ref_home
									--on	a.date_					= ref_home.date_
									--and	a.dk_Serial_number		= ref_home.dk_serial_number
									--and a.gn_lvl2_session_grain	= ref_home.target
						)
