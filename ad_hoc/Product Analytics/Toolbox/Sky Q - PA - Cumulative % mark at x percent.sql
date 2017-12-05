
---------------------------------------------------------
-- Cumulative % of a KPI - Exact value at specific % mark
---------------------------------------------------------

/*
	This example shows the avg time to conversion reached by 90% of STBs.
	
	What matters here is the line where we are doing the cumulative cut (identified with the following comment: "-- % cut")
	
*/

with	ref_home as		(
							select	date_
									,dk_serial_number
									,session_Grain
									,min(case when gn_lvl2_session = 'Home' then start_time else null end) 		as x
							from	z_pa_cap_journeys_201704
							where	session = 'Home'
							and		date_ = '2017-04-01'
							group	by	date_
										,dk_Serial_number
										,session_Grain
						)
		,ref_conv as	(
							select	x.date_
									,x.dk_serial_number
									,x.session_grain
									,y.x
									,min(case when x.time_to_conv is not null then x.start_time else null end) 		as x_conv
									,min(case when x.time_to_playback is not null then x.start_time else null end)	as x_play
							from	z_pa_cap_journeys_201704	as x
									inner join ref_home			as y
									on	x.date_				= y.date_
									and	x.dk_serial_number	= y.dk_serial_number
									and	x.session_grain		= y.session_grain
							where	x.session = 'Home'
							and		x.date_ = '2017-04-01'
							and		x.start_time >= y.x
							group	by	x.date_
										,x.dk_serial_number
										,x.session_grain
										,y.x
						)
		,base as		(
							select	a.date_
									,a.stb_type
									,a.dk_serial_number
									,session
									,a.session_grain
									,gn_lvl2_session
									,gn_lvl2_session_Grain
									,start_time
									,time_spent
									,time_to_conv
									,time_to_playback
									,case when start_time = b.x then gn_lvl2_session else null end as x1
									-- time to conversion
									,sum(case when start_time >= b.x then coalesce(time_to_conv,time_spent) else null end) over (partition by a.date_,a.dk_serial_number,a.session_grain order by start_time rows between unbounded preceding and current row)		as cum_time_to_conv
									,case when start_time = b.x_conv then gn_lvl2_session else null end as conv_session
									-- time to playback
									--,sum(case when start_time >= b.x then coalesce(time_to_playback,time_spent) else null end) over (partition by a.date_,a.dk_serial_number,a.session_grain order by start_time rows between unbounded preceding and current row)	as cum_time_to_play
									--,case when start_time = b.x_play then gn_lvl2_session else null end as play_session
							from	z_pa_cap_journeys_201704	as a
									inner join	ref_conv		as b
									on	a.date_				= b.date_
									and	a.dk_serial_number	= b.dk_serial_number
									and	a.session_Grain		= b.session_grain
							where	a.session = 'Home'
							and		a.date_ = '2017-04-01'
						)
		,etl1 as 		(
							select	extract(year from date_)								as year_
									,extract(month from date_)								as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'			
											else stb_type			
									end														as stb_type_
									,dk_serial_number
									,conv_session
									,cum_time_to_conv
									,start_time
									,row_number() over	(
															partition by	dk_serial_number
																			,conv_session
															order by		cum_time_to_conv, start_time
														)	as nrow
									,row_number() over	(
															partition by	dk_serial_number
																			,conv_session
															order by		cum_time_to_conv desc, start_time desc
														)	as nrowx
									,nrow-nrowx				as delta --<- ANSWER
							from	base
							where	conv_session in	(
														'TV Guide'
														,'Catch Up'
														,'Recordings'
														,'My Q'
														,'Top Picks'
														,'Sky Box Sets'
														,'Sky Movies'
														,'Sky Store'
														,'Sports'
														,'Kids'
														,'Music'
														,'Online Videos'
													)
							and		cum_time_to_conv <= 1050 -- Capping...
						)
		,etl2 as		(
							select	year_
									,month_
									,stb_type_
									,dk_serial_number
									,case when month_ > 2 and conv_session = 'Top Picks' then 'My Q' else conv_session end	as gn_lvl2_session_
									,round(avg(cum_time_to_conv),2) 														as avg_time_to_conv
							from	etl1
							where	delta between -1 and 0
							group	by	year_
										,month_
										,stb_type_
										,dk_serial_number
										,gn_lvl2_session_
						)
select	year_
		,month_
		,stb_type_
		,gn_lvl2_session_
		,max(avg_time_to_conv) as x
from	(
			select	*
					,sum(prop) over	(
										partition by	year_
														,month_
														,stb_type_
														,gn_lvl2_session_
										order by 		avg_time_to_conv
										rows between 	unbounded preceding and current row
									)	as cum
			from	(
						select	year_
								,month_
								,stb_type_
								,gn_lvl2_session_
								,avg_time_to_conv
								,count(distinct dk_serial_number) as nstbs
								,cast(nstbs as float)/cast((sum(nstbs)over (partition by year_,month_,stb_type_,gn_lvl2_session_)) as float)	as prop
						from	etl2
						group	by	year_
									,month_
									,stb_type_
									,gn_lvl2_session_
									,avg_time_to_conv
					)	as f
			where	stb_type_ = 'Gateways'
			and		gn_lvl2_session_ = 'Online Videos'
		)	as etll
where	cum between .89 and 0.9001 -- % Cut
group	by	year_
			,month_
			,stb_type_
			,gn_lvl2_session_