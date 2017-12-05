------------------------------------
-- Measuring Apps Engagement in 2017
------------------------------------

select	substr(dk_date,1,4)																as year_
		,case	substr(dk_date,5,2)
				when 01 then '01 (January)'
				when 02 then '02 (February)'
				when 03 then '03 (March)'
				when 04 then '04 (April)'
				when 05 then '05 (May)'
				when 06 then '06 (June)'
				when 07 then '07 (July)'
				when 08 then '08 (August)'
				when 09 then '09 (September)'
				when 10 then '10 (October)'
				when 11 then '11 (November)'
				when 12 then '12 (December)'
				else	'unknown'
		end		as month_
		,dk_serial_number
		,app_name
		,sum(Case when dk_Action_id = 04002 then 1 else 0 end)							as app_launches
		,count(distinct (case when dk_Action_id = 04002 then dk_Date else null end))	as nfreq
from	pa_events_fact
where	dk_date between 20170101 and 20171231
group	by	year_
			,month_
			,dk_serial_number
			,app_name

	
--------------------------------
-- High Level: Apps Reach
--------------------------------

select	date_dim.WEEK_SKY_IN_YEAR 																							as sky_week
			,count(distinct base.date_)																								as checksum
			,count(distinct dk_serial_number)																						as active_base
			,count(distinct(case when dk_action_id = 04002 then dk_serial_number else null end))	as apps_reach
from	(
				select	date(cast(dk_Date as varchar(10)))	as date_
							,dk_serial_number
							,dk_action_id
							,app_name
							,timems
				from	pa_events_fact
				where	dk_date >= 20161001
			)	as base
			inner join pa_date_dim	as date_dim
			on	base.date_ = date_dim.day_date
group	by	sky_week
having	checksum = 7


-------------------------------------------------------------
-- Granular level: Reach per Apps [Weekly View]
-------------------------------------------------------------

select	date_dim.WEEK_SKY_IN_YEAR 								as sky_week
		,min(base.date_)										as sky_week_from
		,max(base.date_)										as sky_week_to
		,count(distinct base.date_) 							as checksum
		,base.app_name
		,count(distinct dk_serial_number)						as app_reach
		,sum(Case when dk_action_id = 04002 then 1 else 0 end)	as app_launches
from	(
			select	date(cast(dk_Date as varchar(10)))	as date_
					,dk_serial_number
					,dk_action_id
					,app_name
					,timems
			from	pa_events_fact
			where	dk_date >= 20161001
		)	as base
		inner join pa_date_dim	as date_dim
		on	base.date_ = date_dim.day_date
where	base.app_name <> ''
group	by	sky_week
			,base.app_name
having		checksum = 7




-------------------------------------------------------------
-- Granular level: Reach per Apps [Monthly View]
-------------------------------------------------------------

select	extract(month from date_)	as the_month
			,base.app_name
			,count(distinct dk_serial_number)	as app_reach
from	(
				select	date(cast(dk_Date as varchar(10)))	as date_
							,dk_serial_number
							,dk_action_id
							,app_name
							,timems
				from	pa_events_fact
				where	dk_date >= 20161001
			)	as base
where	base.app_name <> ''
group	by	the_month
					,base.app_name