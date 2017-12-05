
---------------------------------------------------------------------------------------------
-- Distribution of boxes based on time booting (as in days) and distance between booting days
---------------------------------------------------------------------------------------------
select	the_stb_type
		,booting_days_distance
		,freq
		,sum(the_prop)	as target
from	(
			select	dk_serial_number
					,the_stb_type
					,booting_days_distance
					,count(1) as freq
					,cast(freq as float) / cast((sum(freq)over(partition by dk_serial_number))as float) as the_prop
			from	(
						select	dk_serial_number
								,the_stb_type
								,date_	as booting_date
								,max(date_) over	(
														partition by	dk_serial_number
														order by		date_
														rows between	1 following and 1 following
													)	as next_booting_date
								,case 	when next_booting_date is null then 0
										else extract(epoch from next_booting_date - booting_date)
								end		as booting_days_distance
						from	(
									select	date_
											,dk_serial_number
											,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
													when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
													when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
											end		as the_stb_type
											,sum(case when dk_action_id = 00004 then 1 else 0 end)	as n_boots
									from	z_pa_events_fact
									where	extract(hour from datehour_) > 5
									group	by	date_
												,dk_serial_number
												,the_stb_type
								)	as ref0
						where	n_boots > 0
						group	by	dk_serial_number
									,the_stb_type
									,booting_date
					)	as base
			group	by	dk_serial_number
						,the_stb_type
						,booting_days_distance
		)	as base2
group	by	the_stb_type
			,booting_days_distance
			,freq
			
			
			
-------------------------
-- Weekly Rebooting Trend
-------------------------
select	thestb_type
		,the_week
		,n_booting_days
		,count(distinct dk_serial_number) as n_boxes
from	(
			select	dk_serial_number
					,thestb_type
					,extract(week from date_) as the_week
					,count(distinct (case when n_boots>0 then date_ else null end)) as n_booting_days
			from	(
						select	date_
								,dk_serial_number
								,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as thestb_type
								,sum(case when dk_action_id = 00004 then 1 else 0 end)	as n_boots
						from	z_pa_events_fact
						where	extract(hour from datehour_) > 5
						group	by	date_
									,dk_serial_number
									,thestb_type
					)	as ref0
			group	by	dk_serial_number
						,thestb_type
						,the_week
		)	as base
group	by	thestb_type
			,the_week
			,n_booting_days
			
			
			
-----------------------------------
-- Rebooting Reach on Monthly Basis
-----------------------------------
select	extract(year from date_)															as the_year
		,extract(month from date_)															as the_month
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as thestb_type
		,count(distinct dk_serial_number)													as n_stbs
		,count(distinct (case when dk_action_id=00004 then dk_Serial_number else null end))	as n_booting_stbs
		,sum(case when dk_action_id = 00004 then 1 else 0 end)								as n_boots
from	z_pa_events_fact	as base
where	extract(hour from datehour_) > 5
group	by	the_year
			,the_month
			,thestb_type
			
			
--------------------
-- Rebooting Profile
--------------------
select	thestb_type	as stb_type
		,case	when the_ratio	>= 1 then '1'
				when the_ratio >=0.9 and the_ratio < 1 then '0.9'
				when the_ratio >=0.8 and the_ratio < 0.9 then '0.8'
				when the_ratio >=0.7 and the_ratio < 0.8 then '0.7'
				when the_ratio >=0.6 and the_ratio < 0.7 then '0.6'
				when the_ratio >=0.5 and the_ratio < 0.6 then '0.5'
				when the_ratio >=0.4 and the_ratio < 0.5 then '0.4'
				when the_ratio >=0.3 and the_ratio < 0.4 then '0.3'
				when the_ratio >=0.2 and the_ratio < 0.3 then '0.2'
				when the_ratio >=0.1 and the_ratio < 0.2 then '0.1'
				when the_ratio < 0.1 then '0'
		end		as booting_ratio
		,count(distinct dk_serial_number) as n_stbs
from	(		
			select	dk_serial_number
					,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as thestb_type
					,count(distinct (case when dk_action_id = 00004 then extract(month from date_) else null end))	as n_booting_months
					,count(distinct date_)																			as n_active_days
					,count(distinct (case when dk_action_id = 00004 then date_ else null end))						as n_booting_days
					,sum(case when dk_action_id = 00004 then 1 else 0 end)											as n_bootings
					,round(cast(n_booting_days as float)/cast(n_active_days as float),3)							as the_ratio
			from	z_pa_events_fact
			where	extract(hour from datehour_) > 5
			group	by	dk_serial_number
			having	n_bootings > 0
		)	as base
where	n_active_days >14 -- at least 15 days interacting with Q
group	by	stb_type
			,booting_ratio