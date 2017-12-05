--------
-- Reach
--------
select	extract(year from date_)	as the_year
		,extract(month from date_)	as the_month
		,case	substr(dk_serial_number,3,1) 
				when 'B' then 'Sky Q Silver'
				when 'C' then 'Sky Q Box'
				when 'D' then 'Sky Q Mini'
				else substr(dk_serial_number,3,1)
		end		as the_stb_type
		,count(distinct dk_serial_number) as reach
		,count(distinct (case when dk_action_id = 04002 then dk_serial_number else null end))	as app_reach
		,count(distinct (Case when gn_lvl2_session not in ('Account App','DSU App','Help App') and lower(gn_lvl2_session) like '%app' then dk_Serial_number else null end)) as commercialapps_reach
		,count(distinct	(
							case	when	(
												(gn_lvl2_session in ('Apps List','Vevo Menu') and dk_Action_id = '04002')
												or
												(lower(gn_lvl2_session) like '%app' and gn_lvl2_session not in ('Account App','DSU App','Help App'))
											)
									then 	dk_serial_number 
									else 	null 
							end
						))	as x
from	z_pa_events_fact
--where	date_ = '2016-11-01' -- > JUST FOR ANALYSING
group	by	the_year
			,the_month
			,the_stb_type

---------------------
-- Menu Effectiveness
---------------------

/* !!!!!!!!!!! OBSOLETE !!!!!!!!!!!!!

select	extract(year from date_)	as the_year
		,extract(month from date_)	as the_month
		,case	substr(dk_serial_number,3,1) 
				when 'B' then 'Sky Q Silver'
				when 'C' then 'Sky Q Box'
				when 'D' then 'Sky Q Mini'
				else substr(dk_serial_number,3,1)
		end		as the_stb_type
		,gn_lvl2_session
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_Session_grain)														as njourneys
		,count(distinct (case when dk_Action_id = 04002 then date_||'-'||dk_serial_number||'-'||gn_lvl2_Session_grain else null end))	as n_app_journeys
from	z_pa_events_fact
where	gn_lvl2_session in ('Apps Tray','Apps List','Fullscreen')
group	by	the_year
			,the_month
			,the_stb_type
			,gn_lvl2_session
*/

select	case 	when dk_previous = 'guide://fullscreen?targetGadget=appsTray&sceneTransition=immediate' then 'Apps Tray'
				else gn_lvl2_session
		end		as sky_Q_Apps_Area
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain) 														as njourneys
		--,count(distinct(case when dk_action_id = 04002 then date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain else null end))	as conversion
		,count(distinct (
							case 	when	(
												(gn_lvl2_session = 'Apps List' and app_name in ('YouTube','com.bskyb.vevo')) or
												(gn_lvl2_session = 'Apps Tray' and app_name in ('com.bskyb.photos','com.bskyb.news','com.bskyb.sports','com.bskyb.weather')) or
												(gn_lvl2_session = 'Vevo Menu' and app_name in ('com.bskyb.vevo'))
											)	then date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain
									else	null
							end
						))	as checksum
from	z_pa_events_fact_102016
where	date_ >= '2016-10-07'
and		(
			gn_lvl2_session in	(
									'Apps Tray'
									,'Apps List'
									,'Vevo Menu'
								)
			or
			dk_previous = 'guide://fullscreen?targetGadget=appsTray&sceneTransition=immediate'
		)
group	by	sky_Q_Apps_Area
			
			
			
--------------
-- Frequencies
--------------
select	extract(year from date_)	as the_year
		,extract(month from date_) 	as the_month
		,case 	gn_lvl2_session	
				when 'Fullscreen'	then 'All Red Button'
				else gn_lvl2_session
		end		as the_apps
		,case	substr(dk_serial_number,3,1) 
				when 'B' then 'Sky Q Silver'
				when 'C' then 'Sky Q Box'
				when 'D' then 'Sky Q Mini'
				else substr(dk_serial_number,3,1)
		end		as the_stb_type
		,case 	gn_lvl2_session	
				when 'Fullscreen'	then 'Red Button'
				else 'Select'
		end		as remote_button
		,count(distinct date_||'-'||dk_Serial_number||'-'||gn_lvl2_session_grain)	as freq
		,count(distinct dk_serial_number)											as reach
from	z_pa_events_fact
where	(gn_lvl2_session in	(
								'Sports App'
								,'Vevo App'
								,'Weather App'
								,'Help App'
								,'Account App'
								,'News App'
								,'DSU App'
								,'Photos App'
							)
		or
		(gn_lvl2_session = 'Fullscreen' and lower (dk_trigger_id) like '%redbutton%'))
and		date_ < '2016-10-01'
group	by	the_year
			,the_month
			,the_apps
			,the_stb_type
			,remote_button
union
select 	substr(dk_Date,1,4)		as the_year
		,substr(dk_date,5,2)	as the_month
		,case 	app_name
				when 'com.bskyb.sports'		then 'Sports App'
				when 'com.bskyb.vevo'		then 'Vevo App'
				when 'com.bskyb.weather'	then 'Weather App'
				when 'com.bskyb.help'		then 'Help App'
				when 'com.bskyb.accman'		then 'Account App'
				when 'com.bskyb.news'		then 'News App'
				when 'com.bskyb.dsu'		then 'DSU App'
				when 'com.bskyb.photos'		then 'Photos App'
				else app_name
		end		as the_apps
		,case	substr(dk_serial_number,3,1) 
				when 'B' then 'Sky Q Silver'
				when 'C' then 'Sky Q Box'
				when 'D' then 'Sky Q Mini'
				else substr(dk_serial_number,3,1)
		end		as the_stb_type
		,dk_trigger_id						as remote_button
		,count(1)							as Freq
		,count(distinct dk_serial_number)	as reach
from	pa_events_fact
where	dk_date >= 20161001
and		app_name <> ''
and		dk_action_id = 04002
group	by	the_year
			,the_month
			,the_apps
			,the_stb_type
			,remote_button

			
			
----------------------
-- Apps trend (Cohort)
----------------------
/*
	Cohort:

		STBs with at least 15 days of activity since June 2016 that have engaged with apps at least once
		
		Made out of: 131K STBs
*/		

select	dk_serial_number
from	(
			select	dk_serial_number
					,count(distinct date_)	as activity
					,max((case when dk_Action_id = 04002 then 1 else 0 end))	as app_launched
			from	z_pa_events_fact
			where	date_ between '2016-06-01' and '2016-06-30'
			group	by	dk_serial_number
			having	activity >= 15
		)	as base
where	app_launch > 0



------------------------------------
-- App Launches per Sky Q Apps Areas
------------------------------------

select	gn_lvl2_session
		,app_name
		,count(1)	as n_launches
from	z_pa_events_fact_102016
where	date_ >= '2016-10-07'
and		dk_Action_id = 04002
and		(
			(gn_lvl2_session = 'Apps List' and app_name in ('YouTube','com.bskyb.vevo')) or
			(gn_lvl2_session = 'Apps Tray' and app_name in ('com.bskyb.photos','com.bskyb.news','com.bskyb.sports','com.bskyb.weather')) or
			(gn_lvl2_session = 'Vevo Menu' and app_name in ('com.bskyb.vevo'))
		)
group	by	gn_lvl2_session
			,app_name