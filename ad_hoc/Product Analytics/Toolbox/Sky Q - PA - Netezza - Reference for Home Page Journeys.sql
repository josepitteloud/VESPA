insert	into ref_home_start_
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
						from 	z_pa_events_fact_YYYYMM
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