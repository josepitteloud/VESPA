select	gn_lvl2_session
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain) as nsession
		,count(distinct (case when conv_flag>0 then  date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain else null end)) as nsession_conv
from	(
			select	date_
					,dk_serial_number
					,gn_lvl2_session
					,gn_lvl2_session_grain
					,min	(
								case	when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1
										else 0
								end
							)	as conv_flag
			from	z_pa_events_fact
			where	date_ between '2016-08-01' and '2016-08-31'
			and		(
						dk_previous in ('guide://programme-details/interim','guide://programme-details/store-interim')
						or
						dk_referrer_id in ('guide://programme-details/interim','guide://programme-details/store-interim','Programme Details')
					)
			group	by	date_
						,dk_serial_number
						,gn_lvl2_session
						,gn_lvl2_session_grain
		)	as base
group	by	gn_lvl2_session