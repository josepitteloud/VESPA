
-- CRITICAL TO BEAR IN MIND:

/*
	This analysis feeds from:
	C:\Users\and36\Documents\GIT\Vespa\ad_hoc\Product Analytics\Sky Q - PA - ETL - Z_PA_EVENTS_FACT_V2.sql
	
		from tables:
		z_pa_events_fact_v2
		z_pa_kpi_def_lvl1
		z_pa_kpi_def_r4
		
		
	THIS WAS DONE FOR ONLY 11K STBS WHICH ARE NOT THE SAME SAMPLE WE LOOKED AT ON THE SPRINT...
		
*/

------------------
-- Trimming for R4
------------------

/*
	'2017-01-26' and '2017-02-01'	then 'Pre'
	'2017-02-02' and '2017-02-08'	then 'Post'
*/

 -- 1

select	case	when base.date_ between '2017-01-26' and '2017-02-01'	then 'Pre'
				when base.date_ between '2017-02-02' and '2017-02-08'	then 'Post'
		end		as period_
		,base.dk_serial_number
		,count(distinct base.date_||'-'||base.session_type) 													as nsessions
		,count(distinct (case when base.conv_flag = 1 then base.date_||'-'||base.session_type else null end))	as nconv_sessions
		,cast(nconv_sessions as float) / cast(nsessions as float)												as ses_conversion_rate
		,ntile(4) over (order by ses_conversion_rate)															as Qtile
		,sum(session_length_ss) 																				as total_time_spent
		,sum(time_to_conv)																						as total_time_to_conv
		,sum(n_applaunches)																						as n_applaunches
		,sum(n_playbacks)																						as n_playbacks
		,sum(n_downloads)																						as n_downloads
		,sum(n_tunings)																							as n_tunings
		,sum(n_bookings)																						as n_bookings
--into	checking
from	z_pa_kpi_def_lvl1 as base
		left join	(
						-- Exclusion List
						select	date_
								,dk_serial_number
								,session_type
						FROM	z_pa_kpi_def_lvl1
						where	(
									(ending <> '' and x like 'Fullscree%') -- 203443
									or
									(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))
									or
									session_length_ss <= 0
									or
									ending = 'Stand By in - System Time out'
								) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
					)	as excl_list
		on	base.date_				= excl_list.date_
		and	base.dk_Serial_number	= excl_list.dk_serial_number
		and	base.session_type		= excl_list.session_type
		inner join z_pa_kpi_def_r4	as ref
		on	base.DK_SERIAL_NUMBER	= ref.DK_SERIAL_NUMBER
where	excl_list.dk_serial_number is null
and		substr(base.dk_serial_number,3,1) in ('B','C')
and		base.date_ between '2017-01-26' and '2017-02-08'
group	by	period_
			,base.dk_serial_number
having	nconv_sessions > 0



-- 2


select	case	when base.date_ between '2017-01-26' and '2017-02-01'	then 'Pre'
				when base.date_ between '2017-02-02' and '2017-02-08'	then 'Post'
		end		as period_
		,base.conv_flag
		,floor (cast(coalesce(time_to_conv,session_length_ss) as float)/10) * 10	as Time_bands
		,coalesce(time_to_conv,session_length_ss)									as time_
		,count(1) as nsessions
from	z_pa_kpi_def_lvl1			as base
		inner join z_pa_kpi_def_r4	as ref
		on	base.dk_Serial_number	= ref.dk_serial_number
		left join	(
						select	date_
								,dk_serial_number
								,session_type
						FROM	z_pa_kpi_def_lvl1
						where	(
									(ending <> '' and x like 'Fullscree%') -- 203443
									or
									(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))
									or
									session_length_ss <= 0
									or
									ending = 'Stand By in - System Time out'
								) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
					)	as excl_list
		on	base.date_				= excl_list.date_
		and	base.dk_Serial_number	= excl_list.dk_serial_number
		and	base.session_type		= excl_list.session_type
where	excl_list.dk_serial_number is null
and		(	
			(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
			or
			(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
		)
and		base.date_ between '2017-01-26' and '2017-02-08'
group	by	period_
			,base.conv_flag
			,time_bands
			,time_
			
			
-- 3


with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	case	when base.date_ between '2017-01-26' and '2017-02-01'	then 'Pre'
											when base.date_ between '2017-02-02' and '2017-02-08'	then 'Post'
									end		as period_
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1			as base
									inner join z_pa_kpi_def_r4	as ref	-- TRIMMING FOR STBS HOLDING R4 
									on	base.dk_Serial_number	= ref.dk_serial_number
									left join	(
													-- Exclusion list: Sessions we don't want to consider for this exercise due either been bugs or
													-- represent a behaviour that is not in line with concious actions performed by the user
													-- we don't currently treat time-out as such...
													select	date_
															,dk_serial_number
															,session_type
													FROM	z_pa_kpi_def_lvl1
													where	(
																(ending <> '' and x like 'Fullscree%') 							-- <1%
																or
																(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))	-- <1%
																or
																session_length_ss <= 0											-- <1%
																or
																ending = 'Stand By in - System Time out'
															) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
												)	as excl_list
									on	base.date_				= excl_list.date_
									and	base.dk_Serial_number	= excl_list.dk_serial_number
									and	base.session_type		= excl_list.session_type
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							and		base.date_ between '2017-01-26' and '2017-02-08' --> Parameter
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,base_size as	(
							select	case	when base.date_ between '2017-01-26' and '2017-02-01'	then 'Pre'
											when base.date_ between '2017-02-02' and '2017-02-08'	then 'Post'
									end		as period_
									,count(distinct base.dk_serial_number) as nactive_boxes
							from	z_pa_kpi_def_lvl1 			as base
									inner join z_pa_kpi_def_r4	as ref	-- TRIMMING FOR STBS HOLDING R4 
									on	base.dk_Serial_number	= ref.dk_serial_number
							where	date_ between '2017-01-26' and '2017-02-08' --> Parameter
							group	by	period_
										--,Stb_type
						)
		,ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,base.dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	z_pa_events_fact_v2			as base
									inner join z_pa_kpi_def_r4	as ref	-- TRIMMING FOR STBS HOLDING R4 
									on	base.dk_Serial_number	= ref.dk_serial_number
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		date_ between '2017-01-26' and '2017-02-08' --> Parameter
							group	by	date_
										,base.dk_serial_number
										,gn_lvl2_session_grain
						)
select	sessions.period_
		,base.gn_lvl2_session
		,max(base_size.nactive_boxes)																			as monthly_active_base
		,count(distinct base.dk_serial_number) 																	as reach
		,count(distinct	base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain)				as n_journeys
		,count	(	distinct	(
									case	when base.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain
											else null 
									end
								)
				)	as n_converted_journeys
		,sum(ss_elapsed_next_action) 																			as n_secs_spent
		,sum( case when ref_conv.gn_lvl2_session_grain is not null and base.INDEX_ < ref_conv.x then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_conversion						
from	z_pa_events_fact_v2 	as base -- Transactional data with Sessions and Journeys...
		left join	ref_conv 			-- Converted journeys reference
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join	ses_sample	as sessions -- Home Sessions reference
		on	base.date_				= sessions.date_
		and	base.dk_serial_number	= sessions.dk_Serial_number
		and	base.session_type		= sessions.session_type
		inner join 	base_size
		on	sessions.period_		= base_size.period_
		inner join	ref_home_start	as c
		on	base.date_					= c.date_
		and	base.dk_serial_number		= c.dk_serial_number
		and	base.gn_lvl2_session_grain	= c.target
where	base.date_ between '2017-01-26' and '2017-02-08' --> Parameter
group	by	sessions.period_
			,base.gn_lvl2_session
			


-- 4

select 	base.the_month
		,base.Sky_Q_Feature
		,qtiles.qtile
		,base.stb_type
		,count(distinct base.dk_serial_number) 													as reach
		,count(distinct(case when base.conv_flag = 1 then base.dk_serial_number else null end))	as conv_reach
		,count(1)																				as njourneys
		,sum(base.conv_flag)																	as nconverted_journeys
		,sum(base.length_secs)																	as time_spent
		,sum(coalesce(base.secs_to_conv,base.length_secs))										as Time_to_conv
from 	(
			select	-- extract(year from date_)||'-'||extract(month from date_)	as the_month
					extract(month from date_)	as the_month
					,'Search'					as Sky_Q_Feature
					,*
			from	z_search_journeys
		)	as base
		inner join z_pa_kpi_def_qtiles2 as qtiles
		on	base.the_month			= qtiles.the_month
		and	base.dk_Serial_number	= qtiles.dk_serial_number
group	by	base.the_month
			,base.Sky_Q_Feature
			,qtiles.qtile
			,base.stb_type
			
			
			
-- 5 

select 	base.the_month
		,base.Sky_Q_Feature
		,qtiles.qtile
		,base.stb_type
		,count(distinct base.dk_serial_number) 													as reach
		,count(distinct(case when base.conv_flag = 1 then base.dk_serial_number else null end))	as conv_reach
		,count(1)																				as njourneys
		,sum(base.conv_flag)																	as nconverted_journeys
		,sum(base.length_secs)																	as time_spent
		,sum(coalesce(base.secs_to_conv,base.length_secs))										as Time_to_conv
from 	(
			select	-- extract(year from date_)||'-'||extract(month from date_)	as the_month
					extract(month from date_)	as the_month
					,'Search'					as Sky_Q_Feature
					,*
			from	z_search_journeys
		)	as base
		inner join z_pa_kpi_def_qtiles2 as qtiles
		on	base.the_month			= qtiles.the_month
		and	base.dk_Serial_number	= qtiles.dk_serial_number
group	by	base.the_month
			,base.Sky_Q_Feature
			,qtiles.qtile
			,base.stb_type
			
