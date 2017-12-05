
with	ref as	(
					select	date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as conv_index
					from	z_pa_events_fact
					where	date_ = '2016-11-04'
					and		gn_lvl2_session = 'Top Picks'
					group	by	datE_
								,dk_serial_number
								,gn_lvl2_session_grain
					having	conv_index is not null
				)
select	base.gn_lvl2_session
		,count(distinct base.dk_serial_number) 														as n_stbs_visiting
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain)	as njourneys
		,count(distinct (case when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_Grain else null end)) as nconv_journeys
		--
		,cast(nconv_journeys as float)/cast(njourneys as float)		as CR
		,cast(njourneys as float)/cast(n_stbs_visiting as float)	as the_avg_visit_x_STB
		,sum(case when ref.gn_lvl2_session_grain is not null and base.index_ <= ref.conv_index then base.ss_elapsed_next_action else null end)	as time_to_conv
from	z_pa_events_fact	as base
		left join ref
		on	base.date_					= ref.date_
		and	base.dk_serial_number		= ref.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
where	base.date_ = '2016-11-04'
and		base.gn_lvl2_session = 'Top Picks'
group	by	base.gn_lvl2_session



