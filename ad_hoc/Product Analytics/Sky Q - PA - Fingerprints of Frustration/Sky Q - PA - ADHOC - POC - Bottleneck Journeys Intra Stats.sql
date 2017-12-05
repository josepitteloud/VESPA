truncate table z_journey_intrastas;commit;

insert	into z_journey_intrastas
select	conv_flag
		,thedate
		,dk_serial_number
		,gn_lvl2_session
		,gn_lvl2_session_grain
		,count(1) 																					as n_globnav_clicks 	-- they are all 01400
		,sum(case when next_action in(02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as n_interactive_clicks -- which action came next inter / glob?
		,sum(time_to_action)																		as length_globnav_time	-- this is not the length of the journey
		,sum(case when ref.screen_levels = 1 	then 1 else 0 end) 									as n_tlm_clicks
		,sum(case when ref.screen_levels = 2 	then 1 else 0 end) 									as n_slm_clicks
		,sum(case when ref.screen_levels = 3 	then 1 else 0 end) 									as n_cat_clicks
		,sum(case when ref.screen_levels = 99	then 1 else 0 end) 									as n_asset_clicks
		,count(distinct (case when ref.screen_levels = 1 	then dk_previous else null end)) 		as n_tlm_visited
		,count(distinct (case when ref.screen_levels = 2 	then dk_previous else null end)) 		as n_slm_visited
		,count(distinct (case when ref.screen_levels = 3 	then dk_previous else null end)) 		as n_cat_visited
		,count(distinct (case when ref.screen_levels = 99	then dk_previous else null end)) 		as n_asset_visited
--into	z_journey_intrastas
from	z_journey_globnav			as base
		left join z_journey_uri_dim	as ref
		on	base.dk_previous = ref.the_uri
group	by	conv_flag
			,thedate
			,dk_serial_number
			,gn_lvl2_session
			,gn_lvl2_session_grain;
commit;