select	base.the_month
		,base.gn_lvl2_session
		,base.n_journeys
		,base.n_boxes
		,cast(base.n_boxes as float) / cast(ref.tot_month_stbs as float)	as monthly_prop_stbs
from	(
			select	extract ( month from date_) as the_month
					,gn_lvl2_session
					,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)	as n_journeys
					,count(distinct dk_serial_number)											as n_boxes
			from	z_pa_events_fact
			where	lower(gn_lvl2_session) like '%app%'
			group	by	the_month
						,gn_lvl2_session
		)	as base
		inner join	(
						select 	extract(month from date_)	as the_month
								,count(distinct dk_serial_number)	as tot_month_stbs
						from	z_pa_events_fact
						group	by	the_month
					)	as ref
		on	base.the_month	= ref.the_month
		
		
select 	extract(month from date_)			as the_month
		,count(distinct dk_serial_number)	as n_stbs
		,count(distinct (case when gn_lvl2_session in ('Apps Tray','Apps List') then dk_serial_number else null end))	as n_stbs_in_app_area
		--,count(distinct (case when lower(gn_lvl2_session) like '%app%' and gn_lvl2_session not in ('Apps Tray','Apps List') then dk_serial_number else null end))	as n_stbs_on_app
		,count(distinct (case when dk_action_id = 04002 then dk_serial_number else null end))	as n_stbs_on_app2
		,sum(case when dk_action_id = 04002 then 1 else 0 end) as n_app_launches
from	z_pa_events_fact
--where	date_ between ' 2016-08-01' and '2016-08-31'
group	by	the_month






with 	base as 	(
						select	extract(year from date_) 													as the_year
								,extract(month from date_) 													as the_month
								,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as thestb_type
								,gn_lvl2_session
								,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)	as njourneys
								,count(distinct dk_serial_number)											as nstbs
						from	z_pa_events_fact	as base
						where	gn_lvl2_session is not null
						group	by	the_year
									,the_month
									,thestb_type
									,gn_lvl2_session
					)
		,ref as		(
						select	extract(year from date_) 													as the_year
								,extract(month from date_) 													as the_month
								,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as thestb_type
								,count(distinct dk_serial_number)	as n_monthly_stbs
								,count(distinct (case when dk_action_id = 04002 then dk_serial_number else null end))	as n_monthly_stbs_app_laucnh
								,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)	as n_monthly_journeys
						from	z_pa_events_fact
						where	gn_lvl2_session is not null
						group	by	the_year
									,the_month
									,thestb_type
					)
		,app_launch	as	(
							select	extract(month from date_)	as the_month
									,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
											when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
											when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
									end		as thestb_type
									,count(distinct dk_serial_number)	as n_active_stbs
									,count(distinct (case when dk_Action_id = 04002 then dk_serial_number else null end))	as n_stbs_launching_app
							from	z_pa_events_fact
							group	by	the_month
										,thestb_type
						)
select	base.*
		,ref.n_monthly_stbs
		,ref.n_monthly_stbs_app_laucnh
		,ref.n_monthly_journeys
		,app_launch.n_active_stbs
		,app_launch.n_stbs_launching_app
from	base
		inner join ref
		on	base.the_month		= ref.the_month
		and	base.thestb_type	= ref.thestb_type
		inner join app_launch
		on	base.the_month		= app_launch.the_month
		and	base.thestb_type	= app_launch.thestb_type
		
		

		
-- As studied, the Watch flag isolates Red Button Launches = Sports App via Red Button. Additionally we are only interested by this query to compare as well all app launches that come from the Apps Tray
select	extract(month from date_)	as the_month
		,case	when watch > 0 then 'Sports App'
				else gn_lvl2_session
		end		as Sky_q_session
		,case	when watch > 0 then 'Red Button'
				else 'Apps Tray'
		end		as the_trigger
		,count(distinct date_||'-'||dk_serial_number||'-'||target) 	as freq
		,count(distinct dk_serial_number)							as reach
from	(
			-- this cut here helps identifying the starting point of a session and fitting everything into the right sequence of occurence...
			select	date_
					,dk_serial_number
					,gn_lvl2_session
					,target
					,watch
					,start_
					,max(target) over	(
											partition by	date_
															,dk_serial_number
											order by		start_
											rows between	1 preceding and 1 preceding
										)	as origin
			from	(
						-- Identifying Red Button app Launches that are wrongly assigned to other sessions because of a miss spelt URI for fullscreen... all Red Buttons are happening in fullscreen hence the criteria below to say if an app was launch in full screen then that's the sports app as is the only app up until today that we can launch from full screen
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain as target
								,min(index_)	as start_
								,sum(case when dk_action_id = 04002 and dk_referrer_id in ('guide://fullScreen','guide://fullscreen') then 1 else 0 end) as watch
						from 	z_pa_events_fact
						--where	date_ between '2016-08-01' and '2016-08-07'
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
					)	as base
		)	as checkgin
--where	(
			--(lower(gn_lvl2_session) like '%app' and lower(origin) like 'apps tray%') or
			--watch > 0
		--)
group	by	the_month
			,Sky_q_session
			,the_trigger

			
			
----------------			
-- newer version
----------------

-- As studied, the Watch flag isolates Red Button Launches = Sports App via Red Button. Additionally we are only interested by this query to compare as well all app launches that come from the Apps Tray
select	base.the_month
		,base.the_stb_type															as stb_type
		,case	when base.watch > 0 then 'Sports App'
				else base.gn_lvl2_session
		end		as Sky_q_session
		,case	when base.watch > 0 then 'Red Button'
				else 'Apps Tray'
		end		as the_trigger
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.target)	as freq
		,count(distinct base.dk_serial_number)										as reach
		,min(ref.n_monthly_stbs) 													as total_monthly_stbs
		,min(ref.n_monthly_stbs_app_laucnh) 										as total_monthly_stbs_app_launch
from	(
			select	date_
					,extract(month from date_)																									as the_month
					,dk_serial_number
					,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as the_stb_type
					,gn_lvl2_session
					,gn_lvl2_session_grain 																										as target
					,min(index_)																												as start_
					,sum(case when dk_action_id = 04002 and dk_referrer_id in ('guide://fullScreen','guide://fullscreen') then 1 else 0 end)	as watch
			from 	z_pa_events_fact
			where	gn_lvl2_session is not null
			--and		date_ between '2016-08-01' and '2016-08-07'
			group	by	date_
						,the_month
						,dk_serial_number
						,the_stb_type
						,gn_lvl2_session
						,gn_lvl2_session_grain
		)	as base
		inner join	(
						select	extract(month from date_) 																as the_month
								,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
										when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
										when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
								end		as thestb_type
								,count(distinct dk_serial_number)	as n_monthly_stbs
								,count(distinct (case when dk_action_id = 04002 then dk_serial_number else null end))	as n_monthly_stbs_app_laucnh
								,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)				as n_monthly_journeys
						from	z_pa_events_fact
						where	gn_lvl2_session is not null
						group	by	the_month
									,thestb_type
					)	as ref
		on	base.the_month	= ref.the_month
		and	base.the_stb_type	= ref.thestb_type 
group	by	base.the_month
			,stb_type
			,Sky_q_session
			,the_trigger

			
			
			
-- We now have in PA_EVENTS_FACT visibility on a new field named APP_NAME showing, for all app launches 04002, what is the actual name of the app that was launched by any given user

with 	base as	(
					select	extract(week from (cast(cast(dk_date as varchar(8)) as date)))	as the_week
							,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
									when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
									when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
							end		as the_stb_type
							,dk_trigger_id						as remote_button
							,app_name
							,count(1) 							as n_app_launches
							,count(distinct dk_serial_number)	as reach
					from	pa_events_fact
					where	dk_Date >= 20161007
					and		app_name <> ''
					group	by	the_week
								,the_stb_type
								,remote_button
								,app_name
				) 
		,ref as	(
					select	extract(week from (cast(cast(dk_date as varchar(8)) as date)))	as the_week
							,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
									when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
									when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
							end		as the_stb_type
							,count(distinct dk_serial_number) as n_boxes
							,count(distinct (case when dk_action_id = 04002 then dk_serial_number else null end)) as n_boxes_engaging
					from	pa_events_fact
					where	dk_date >= 20161007
					group	by	the_week
								,the_stb_type
				)
select	base.*
		,ref.n_boxes			as n_daily_active_boxes
		,ref.n_boxes_engaging	as n_daily_active_boxes_using_apps
from	base
		inner join ref
		on	base.the_week 		= ref.the_week
		and	base.the_stb_type	= ref.the_stb_type
		
		
-- A complement around the new capability (App Name)

select	substr(dk_datehour,9)	as the_hour
		,case	substr(dk_serial_number,3,1)
			when 'B' then 'Sky Q Silver'
			when 'C' then 'Sky Q Box'
			when 'D' then 'Sky Q Mini'
		end		as	stb_type
		,case 	when extract(dow from cast(cast(dk_date as varchar(8)) as date)) in (1,7)	then 'Weekend'
				else 'Weekday'
		end		as week_part
		,app_name
		,count(1)	as n_launches
       	,count(distinct dk_serial_number) as nboxes
from	pa_events_fact
where	dk_datehour >= 2016100609
and		dk_action_id = 04002
group	by	the_hour
			,stb_type
			,week_part
			,app_name