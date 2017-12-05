-- Monthly View
select	dk_serial_number
		,case	substr(dk_serial_number,3,1)
				when 'B' then 'Gateway'
				when 'C' then 'Gateway'
				when 'D' then 'MR'
		end		as Stb_type
		,count(distinct( case when dk_action_id in (02000,02010,02002,02005) then date_ else null end))	as returning
		,sum(case when dk_action_id in (02000,02010,02002,02005) then 1 else 0 end)						as freq
from	z_pa_Events_fact
where	date_ between '2017-01-01' and '2017-01-31'
group	by	dk_serial_number
			,Stb_type
			
			
			
select	extract(month from date_)																															as the_month
		,gn_lvl2_session
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_Session_grain)																			as njourneys
		,count(distinct dk_serial_number)																													as nSTBs
		,count(distinct (case when dk_action_id in (02000,02010,02002,02005) then date_||'-'||dk_serial_number||'-'||gn_lvl2_Session_grain else null end))	as nbooked_journeys
		,count(distinct (case when dk_action_id in (02000,02010,02002,02005) then dk_serial_number else null end))											as nSTBs_booking
		,sum(case when dk_action_id in (02000,02010,02002,02005) then 1 else 0 end)																			as freq
from	z_pa_Events_fact
where	date_ between '2017-01-01' and '2017-01-31'
group	by	the_month
			,gn_lvl2_session			

-- Checking in Plus

select sky_plus_Session
       ,count(distinct viewing_Card) as nboxes
       ,count(Distinct(case when action_category = 'LinearAction' and action like 'LINEAR%_RECORD%' then viewing_Card else null end)) as nboxes_booking
       ,count(distinct concat(string(thedate),viewing_Card,sky_plus_Session_grain)) as njourneys
       ,count(distinct	(
							case 	when action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%' 
										then concat(string(thedate),viewing_Card,sky_plus_Session_grain)
									else 
										null
							end
						))	as njourneys_booking
		,sum(case when action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%' then 1 else 0 end) as nbookings
FROM  	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-01'),timestamp('2016-11-30'))	
group	by	sky_plus_session