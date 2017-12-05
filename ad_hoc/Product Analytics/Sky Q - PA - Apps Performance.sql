with 	ref as		(
						-- Monthly Apps Base Ref...
						select	extract(year from date(cast(dk_date as varchar(8))))									as year_
								,extract(month from date(cast(dk_date as varchar(8))))									as month_
								,count(distinct dk_serial_number)														as active_base
								,count(distinct(case when dk_Action_id = 04002 then dk_serial_number else null end))	as overall_apps_reach
						from	pa_events_fact
						where	dk_date between 20170601 and 20170630
						group	by	year_
									,month_
					)
		,ground as	(
						-- Monthly Apps performance...
						select	extract(year from date(cast(dk_date as varchar(8))))									as year_
								,extract(month from date(cast(dk_date as varchar(8))))									as month_
								,app_name
								,sum(case when dk_action_id = 04002 then 1 else 0 end)									as nlaunches
								,count(distinct(case when dk_Action_id = 04002 then dk_serial_number else null end))	as apps_reach
						from	pa_events_fact
						where	dk_date between 20170601 and 20170630
						and		app_name <> ''
						group	by	year_
									,month_
									,app_name
					)
select	ground.*
		,ref.active_base
		,ref.overall_apps_reach
from 	ground
		inner join ref
		on	ground.year_	= ref.year_
		and	ground.month_	= ref.month_

		
with	ref as		(			
						-- Daily Apps Base Ref...			
						select	extract(year from date(cast(dk_date as varchar(8))))									as year_
								,extract(month from date(cast(dk_date as varchar(8))))									as month_
								,date(cast(dk_date as varchar(8)))														as date_
								,count(distinct dk_serial_number)														as active_base
								,count(distinct(case when dk_Action_id = 04002 then dk_serial_number else null end))	as overall_apps_reach
						from	pa_events_fact
						where	dk_date between 20170601 and 20170630
						group	by	year_
									,month_
									,date_
					)
		,ground as	(
						-- Daily Apps performance...
						select	extract(year from date(cast(dk_date as varchar(8))))									as year_
								,extract(month from date(cast(dk_date as varchar(8))))									as month_
								,date(cast(dk_date as varchar(8)))														as date_
								,app_name
								,sum(case when dk_action_id = 04002 then 1 else 0 end)									as nlaunches
								,count(distinct(case when dk_Action_id = 04002 then dk_serial_number else null end))	as apps_reach
						from	pa_events_fact
						where	dk_date between 20170601 and 20170630
						and		app_name <> ''
						group	by	year_
									,month_
									,date_
									,app_name
					)
select	ground.*
		,ref.active_base
		,ref.overall_apps_reach
from	ground
		inner join ref
		on	ground.year_	= ref.year_
		and	ground.month_	= ref.month_
		and	ground.date_	= ref.date_