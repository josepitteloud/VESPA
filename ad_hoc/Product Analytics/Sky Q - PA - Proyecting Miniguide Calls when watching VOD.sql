------------------
-- High-level View
------------------

select	base.*
		,ref.avg_length_usage_days
from	(
			select	extract(month from date_)																as the_month
					,count(distinct dk_serial_number) 														as active_base
					,count(distinct (case when dk_action_id = 01000 then dk_serial_number else null end)) 	as active_base_mini
					,cast( active_base_mini as float) / cast( active_base as float)							as monthly_reach
					,sum(case when dk_action_id = 01000 then 1 else 0 end)									as month_minicalls
					,cast(month_minicalls as float ) / cast(active_base_mini as float)						as monthly_avg_mini_calls
					,monthly_avg_mini_calls/cast(count(distinct date_) as float)							as daily_avg_mini_calls
			from	z_pa_events_fact
			where	date_ between '2016-09-01' and '2016-11-30'
			group	by	the_month
		)	as base
		inner join	(
						select	the_month
								,avg(length_usage)	as avg_length_usage_days
						from	(
									select	extract(month from date_)	as the_month
											,dk_serial_number
											,count(distinct date_)		as length_usage
									from	z_pa_events_fact
									where	date_ between '2016-09-01' and '2016-11-30'
									and		dk_action_id = 01000
									group	by	the_month
												,dk_serial_number
								)	as base
						group	by	the_month
					)	as ref
		on	base.the_month	= ref.the_month

---------------------------------------------
-- AVG daily usage in a month (Sept-Nov 2016)
---------------------------------------------

select	the_month
		,avg_miniguide
		,count(distinct dk_serial_number) 	as nboxes
from	(
			-- what's each box avg Mini-guide calls in a month
			select	the_month
					,dk_serial_number
					,round(avg(miniguide_hits),0)	as avg_miniguide
			from	(
						-- how many Mini-guide calls for each box on each day...
						select	extract(month from date_)	as the_month
								,date_
								,dk_serial_number
								,count(1) as miniguide_hits
						from	z_pa_events_fact
						where	date_ between '2016-09-01' and '2016-11-30'
						and		dk_action_id = 01000
						group	by	the_month
									,date_
									,dk_serial_number
					)	as base
			group	by	the_month
						,dk_serial_number
		)	as thefinal
group	by	the_month
			,avg_miniguide
			
			
-----------------------------------------
-- Most common frequency of usage per STB
-----------------------------------------

select	miniguide_hits
		,count(distinct dk_serial_number) as nboxes
from	(
			select	date_
					,dk_serial_number
					,count(1) as miniguide_hits
			from	z_pa_events_fact
			where	date_ between '2016-09-01' and '2016-09-30'
			and		dk_action_id = 01000
			group	by	date_
						,dk_serial_number
		)	as base
group	by	miniguide_hits


--------------------------------------------
-- Monthly Length of Usage STBs distribution
--------------------------------------------

select	the_month
		,length_usage
		,count(distinct dk_serial_number)	as nboxes
from	(
			select	extract(month from date_)	as the_month
					,dk_serial_number
					,count(distinct date_)		as length_usage
			from	z_pa_events_fact
			where	date_ between '2016-09-01' and '2016-11-30'
			and		dk_action_id = 01000
			group	by	the_month
						,dk_serial_number
		)	as base
group	by	the_month
			,length_usage
			
			
			
------------------------------------
-- High-level Monthly Length of Usage
------------------------------------

select	the_month
		,avg(length_usage)	as avg_length_usage_days
from	(
			select	extract(month from date_)	as the_month
					,dk_serial_number
					,count(distinct date_)		as length_usage
			from	z_pa_events_fact
			where	date_ between '2016-09-01' and '2016-11-30'
			and		dk_action_id = 01000
			group	by	the_month
						,dk_serial_number
		)	as base
group	by	the_month