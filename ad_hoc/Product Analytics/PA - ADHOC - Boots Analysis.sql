
-- Data Source 1
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
			
			
-- Data Source 2
select	extract(month from date_)			as the_month
		,thestb_type						as stb_type
		,n_boots
		,count(distinct dk_serial_number)	as n_booting_stbs
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
where	n_boots between 1 and 10
group	by	the_month
			,stb_type
			,n_boots
			
-- Data source 3 
select	date_
		,stb_type
		,count(distinct dk_serial_number)	as n_stbs
		,count(distinct (case when dk_action_id = 00004 then dk_serial_number else null end))	as n_booting_stbs
		,sum(case when dk_action_id = 00004 then 1 else 0 end)	as n_boots
from	z_pa_events_fact
where	extract(hour from datehour_) > 5
group	by	date_
			,stb_type
			
			
-- heat map high - level
select	the_month
		,stb_type
		,n_boots
		,sum(n_dates*n_stbs_booting)/sum(n_stbs_booting) as target_
from	(
			select	the_month
					,stb_type
					,n_boots
					,n_dates
					,count(distinct dk_serial_number) as n_stbs_booting
			from	(
						select	extract(month from date_)	as the_month
								,dk_serial_number
								,thestb_type				as stb_type
								,n_boots
								,count(distinct date_)		as n_dates
						from	(
									select	date_
											,dk_serial_number
											,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
													when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
													when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
											end		as thestb_type
											,sum(case when dk_action_id = 00004 then 1 else 0 end)	as n_boots
									from	z_pa_events_fact
									--where	dk_serial_number = '32B0620488473618'
									group	by	date_
												,dk_serial_number
												,thestb_type
								)	as base
						where	n_boots between 1 and 10
						group	by	the_month
									,dk_serial_number
									,stb_type
									,n_boots
					)	as base2
			group	by	the_month
						,stb_type
						,n_boots
						,n_dates
		)	as base3
group	by	the_month
			,stb_type
			,n_boots
			
-- Heath map Details
select	the_month
		,stb_type
		,n_boots
		,n_dates
		,count(distinct dk_serial_number) as n_stbs_booting
from	(
			select	extract(month from date_)	as the_month
					,dk_serial_number
					,thestb_type				as stb_type
					,n_boots
					,count(distinct date_)		as n_dates
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
						--where	dk_serial_number = '32B0620488473618'
						group	by	date_
									,dk_serial_number
									,thestb_type
					)	as base
			where	n_boots between 1 and 10
			group	by	the_month
						,dk_serial_number
						,stb_type
						,n_boots
		)	as base2
group	by	the_month
			,stb_type
			,n_boots
			,n_dates
			
			
-- Data Source 5 (how many days STBs reboot per month)

select	the_month
		,n_days_booting
		,count(distinct dk_serial_number)	as n_stbs_booting
from	(
			select	extract(month from date_)	as the_month
					,dk_serial_number
					,count(distinct (case when dk_action_id = 00004 then date_ else null end))		as n_days_booting
			from	z_pa_events_fact
			where	extract(hour from datehour_) > 5
			group	by	the_month
						,dk_serial_number
		)	as base
group	by	the_month
			,n_days_booting
order	by	the_month
			,n_days_booting
			
			
			
-- Data source  6 

select	extract(month from date_) 	as the_month
		,stb_type
		,avg(prop_booting_stbs)		as avg_prop_booting_stbs
		,avg(booting_ratio_per_stb)	as avg_bootin_ratio_per_stb
from	(
			select	date_
					,stb_type
					,count(distinct dk_serial_number)														as n_stbs
					,count(distinct (case when dk_action_id = 00004 then dk_serial_number else null end))	as n_booting_stbs
					,sum(case when dk_action_id = 00004 then 1 else 0 end)									as n_boots
					,cast(n_booting_stbs as float)/cast(n_stbs as float)									as prop_booting_stbs
					,cast(n_boots as float)/cast(n_booting_stbs as float)									as booting_ratio_per_stb
			from	z_pa_events_fact
			where	stb_type is not null
			group	by	date_
						,stb_type
		)	as base2
group	by	the_month
			,stb_type

			
			
			
--  Hourly booting activity per month -- Data Source 7
/*
	This is to justify the hour from which we are excluding 00004
*/
select	extract(month from datehour_)														as the_month
		,extract(hour from datehour_)														as the_hours
		,avg(n_active_stbs_across_month)													as avg_active_stbs_across_month
		,avg(n_booting_stbs_across_month)													as avg_booting_stbs_across_month
		,avg(the_prop)																		as avg_prop
		,avg(n_bootings_actions)															as avg_bootings_actions
		,cast(avg_bootings_actions as float) / cast(avg_booting_stbs_across_month as float)	as ratio_boots_x_stb
from	(
			select 	datehour_
					,stb_type
					,count(distinct dk_serial_number)														as n_active_stbs_across_month
					,count(distinct (case when dk_action_id = 00004 then dk_serial_number else null end)) 	as n_booting_stbs_across_month
					,cast(n_booting_stbs_across_month as float)/cast(n_active_stbs_across_month as float)	as the_prop
					,sum(case when dk_action_id = 00004 then 1 else 0 end)									as n_bootings_actions
			from	z_pa_events_fact	as base
			--where	date_ between '2016-08-01' and '2016-08-31'
			group	by	datehour_
						,stb_type
		)	as base
group	by	the_month
			,the_hours
			
			
			
			
-- Data source 8
/*
	which STBs are rebooting frequently
*/

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
			
			
			
-- Data Source 9 

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
									--and		dk_serial_number in ('32B0560488004549','32B0560488008255')--,'32B0560488009337','32B0560488009754')
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