
 -- VOICE AND TEXT SEARCH TREATMENT...

select	date_
		,dt
		,timems
		,dk_serial_number
		,gn_lvl2_session
		,gn_lvl2_session_grain
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,query
		,error_msg
		,last_value(x ignore nulls)	over	(
												partition by	date_
																,dk_serial_number
																,gn_lvl2_session_grain
												order by		timems
												rows between 	50 preceding and current row
											)	as input
from	(
			select	a.date_
					,a.dt
					,a.timems
					,a.dk_serial_number
					,a.gn_lvl2_session
					,a.gn_lvl2_session_grain
					,a.dk_action_id
					,a.dk_previous
					,a.dk_current
					,a.dk_referrer_id
					,b.query
					,b.error_msg
					,case	when gn_lvl2_Session = 'Voice Search' and dk_action_id = 01605 	then dense_rank() over(partition by a.date_,a.dk_serial_number,a.gn_lvl2_session_grain order by timems)
							when gn_lvl2_Session = 'Search' and dk_previous<>dk_action_id and dk_action_id ='01605' then dense_rank() over(partition by date_,a.dk_serial_number,gn_lvl2_session_grain order by a.timems) 
							else null 
					end 	as x
			from	z_pa_events_fact_201703 			as a
					left join pa_voice_search_events	as b
					on	a.dk_serial_number	= b.dk_serial_number
					and	a.timems			= b.timems
			where	a.date_ = '2017-03-06'
			and		a.gn_lvl2_session = 'Voice Search'
			and		a.dk_serial_number = '32B0550480010071'
			and		a.gn_lvl2_session_grain = 'Voice Search-9'
		)	as base
order	by	timems
