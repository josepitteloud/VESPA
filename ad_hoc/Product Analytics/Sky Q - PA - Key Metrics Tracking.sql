/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ 
             ?$$$,      I$$$ $$$$. $$$$  $$$= 
              $$$$$$$$= I$$$$$$$    $$$$.$$$  
                  :$$$$~I$$$ $$$$    $$$$$$   
               ,.   $$$+I$$$  $$$$    $$$$=   
              $$$$$$$$$ I$$$   $$$$   .$$$    
                                      $$$     
                                     $$$      
                                    $$$?

            CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							PRODUCTS ANALYTICS 	(PA)
**Done by:                             	Angel Donnarumma	(angel.donnarumma@sky.uk)
**Stakeholder:                          Daniel Chronnell
**Trello Card:							122 - Sky Q - Key Metrics Tracking
										https://trello.com/c/mgzE8uGJ/125-122-sky-q-key-metrics-tracking

**Business Brief:

        Establishing a Standard set of KPIs to mearsures performance of products (CE Devices, STBs)
		
**Considerations:

		+ R4 will be assumed for the base since 2017-03-09
		
**Sections:

		A - Data Evaluation
			
		B - Data Preparation
			
		C - Data Analysis (Queries)
			
			
**Pre-Requisits:

	
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/



----------------------
-- A - Data Evaluation
----------------------

-- Where to cap (cutting of outliers/tail)

with base as	(
					select	date_
							,dk_serial_number
							,session
							,session_grain
							,sum(time_spent)	as x
					from	z_pa_journeys_201701
					group	by	date_
								,dk_serial_number
								,session
								,session_grain
					union
					select	date_
							,dk_serial_number
							,session
							,session_grain
							,sum(time_spent)	as x
					from	z_pa_journeys_201702
					group	by	date_
								,dk_serial_number
								,session
								,session_grain
					union
					select	date_
							,dk_serial_number
							,session
							,session_grain
							,sum(time_spent)	as x
					from	z_pa_journeys_201703
					group	by	date_
								,dk_serial_number
								,session
								,session_grain
					union
					select	date_
							,dk_serial_number
							,session
							,session_grain
							,sum(time_spent)	as x
					from	z_pa_journeys_201704
					group	by	date_
								,dk_serial_number
								,session
								,session_grain
				)
select	extract(month from date_)	as month_
		,x
		,session
		,count(1) as njourneys
from	base
group	by	month_
			,x
			,session
			
/*
	Data shows that we are safe to cut at 95% threshold where tails begin. behaviour is constant throughout the 4 months
	for Home sessions 			= 6750 seconds long
	for Full-screen sessions	= 14400 seconds long
*/		
		
----------------------
--B - Data Preparation
----------------------
--> (10 min running time...)

-- Compressing at Journey level...
drop table z_pa_journeys_YYYYMM;commit;

with	ref_conv as		(
							select	date_
									,dk_Serial_number
									,gn_lvl2_Session_Grain
									,min(index_)																as x_
									,min(case when dk_action_id in (00001,03000) then index_  else null end)	as x_play
							from	z_pa_events_fact_YYYYMM
							where	dk_action_id in (02400,00001,03000,02000,02010,02002,02005)
							and		dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-')
							group	by	date_
										,dk_Serial_number
										,gn_lvl2_Session_Grain
						)
		,base as		(	
							select	*
									,min(ss_elapsed_next_action) over 	(
																			partition by	date_
																							,dk_serial_number
																							,gn_lvl2_session_Grain
																			order by		timems
																			rows between 	1 following and 1 following
																		)	as time_spent_in_action
									,dense_rank() over	(
															partition by	date_
																			,dk_serial_number
																			,gn_lvl2_session_grain
															order by 		index_
														)	as sequence_
							from	z_pa_events_fact_YYYYMM
						)
		,full_conv_ as	(
							select	date_
									,dk_serial_number
									,session
									,session_grain
									,gn_lvl2_session
									,gn_lvl2_session_grain
									,min(
											case	when sequence_ = 1 and ss_elapsed_next_action >= 120	then index_
													when time_spent_in_action >= 120						then index_
													else null
											end
										)	as x_
									--,min(index_) as x_
							from	base
							where	gn_lvl2_session = 'Fullscreen'
							group	by	date_
										,dk_serial_number
										,session
										,session_grain
										,gn_lvl2_session
										,gn_lvl2_session_grain
						)
select	g.date_
		,g.dk_serial_number
		,g.stb_type
		,g.session
		,g.session_grain
		,g.gn_lvl2_session
		,g.gn_lvl2_session_grain
		
		-- Time
		,min(g.dt)																																			as start_
		,min(g.timems)																																		as start_time
		,min(g.index_)																																		as start_index
		,max(g.dt)																																			as end_
		,max(g.timems)																																		as end_time
		,max(g.index_)																																		as end_index
		,sum(g.ss_elapsed_next_action)																														as time_spent
		
		-- Activity
		,count(1)																																			as nactions
		,sum(case when g.dk_action_id = 01400 then 1 else 0 end)																							as n_gnavs
		,sum(case when g.dk_action_id = 01002 and g.dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)						as n_minibrowse
		,sum(case when g.dk_action_id = 02400 and g.dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)						as n_downloads
		,sum(case when g.dk_action_id = 03000 and g.dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)						as n_playbacks
		,sum(case when g.dk_action_id = 00001 and g.dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)						as n_tunings
		,sum(case when g.dk_action_id in (02000,02010,02002,02005) and g.dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)	as n_bookings
		,sum(case when g.dk_action_id = 04002 and g.dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)						as n_launches
		
		--Full-screen Activity
		,sum(case when g.gn_lvl2_session = 'Fullscreen' and g.dk_action_id = 00001 and g.time_spent_in_action <120 then 1 else 0 end)									as browsing
		,sum(case when g.gn_lvl2_session = 'Fullscreen' and g.dk_Action_id = 00001 and g.time_spent_in_action >= 120 then 1 else 0 end) 								as Converting_2m
		--,sum(case when g.gn_lvl2_session = 'Fullscreen' and g.dk_Action_id = 00001 and g.time_spent_in_action >= 420 then 1 else 0 end)									as Converting_7m
		,sum(case when g.gn_lvl2_session = 'Fullscreen' and g.sequence_ = 1 and (g.time_spent_in_action >= 120 or g.ss_elapsed_next_Action >= 120) then 1 else 0 end)	as entry_conv_2m
--		,sum(case when g.gn_lvl2_session = 'Fullscreen' and g.sequence_ = 1 and (g.time_spent_in_action >= 420 or g.ss_elapsed_next_Action >= 420) then 1 else 0 end)	as entry_conv_7m
		,sum(case when g.gn_lvl2_session = 'Fullscreen' and b.x_ is not null and  g.index_ <= b.x_ then  g.ss_elapsed_next_action else null end)							as time_to_consume
		
		-- Effectiveness
		,sum(case when ref_conv.x_ is not null and g.index_ <= ref_conv.x_ then g.ss_elapsed_next_action else null end)										as time_to_conv
		,sum(
				case	when b.x_ is not null and g.index_ <= b.x_ then g.ss_elapsed_next_action		-- for Fullscreen Playbacks
						when ref_conv.x_play is not null and g.index_ <= ref_conv.x_play then g.ss_elapsed_next_action 	-- for UI Playbacks
						else null 
				end
			)	as time_to_playback
		
into	z_pa_journeys_YYYYMM
from	base						as g
		left join ref_conv
		on	g.date_					= ref_conv.date_
		and	g.dk_Serial_number		= ref_conv.dk_Serial_number
		and	g.gn_lvl2_Session_Grain	= ref_conv.gn_lvl2_Session_Grain
		left join full_conv_		as b
		on	g.date_					= b.date_
		and	g.dk_Serial_number		= b.dk_Serial_number
		and	g.gn_lvl2_Session_Grain	= b.gn_lvl2_Session_Grain
group	by	g.date_
			,g.dk_serial_number
			,g.stb_type
			,g.session
			,g.session_grain
			,g.gn_lvl2_session
			,g.gn_lvl2_session_grain;
commit;
			
			
-- Capping Sessions...

/*
	I should have changed the actual name of the table since could be misleading...
*/
drop table z_pa_cap_journeys_YYYYMM;commit;

with ref as	(
				select	date_
						,dk_serial_number
						,stb_type
						,session
						,session_grain
						,sum(time_spent)	as x
						,case 	when session = 'Home' and x <= 6750 then 1			-- < Sessions Capping Rule (1.9 hours just in UI)
								when session = 'Fullscreen' and x <= 14400 then 1	-- < Sessions Capping Rule (4 hours just in Full screen)
								else 0
						end		as valid_
				from	z_pa_journeys_YYYYMM
				where	session in ('Home','Fullscreen')
				and		session_grain is not null
				group	by	date_
							,dk_serial_number
							,stb_type
							,session
							,session_grain
				having	valid_ = 1
			)
select	x.*
into	z_pa_cap_journeys_YYYYMM
from	z_pa_journeys_YYYYMM	as x
		inner join ref
		on	x.date_				= ref.date_
		and	x.dk_serial_number	= ref.dk_Serial_number
		and	x.session_grain		= ref.session_grain;
commit;


-- Housekeeping
drop table z_pa_journeys_YYYYMM;commit;


-- Signposting
drop table z_pa_cap_signpost_YYYYMM;commit;

select	*
		,last_value(x1 ignore nulls) over	(
												partition by	date_
																,dk_serial_number
																,gn_lvl3_Session_Grain 
												order by 		index_
												rows between 	80 preceding and current row
											)							as signpost_grain
		,substr(signpost_grain,1,instr(signpost_grain,'-')-1)			as signpost
into	z_pa_cap_signpost_YYYYMM
from	(
			select	a.*
					,b.signpost as ff
					,case	when b.signpost is null and a.dk_previous like '%/EVOD%'	then gn_lvl3_Session_grain
							when b.signpost is not null 								then b.signpost
							else null
					end		as x
					,x||'-'||dense_rank()	over	(
														partition by	a.date_
																		,a.dk_serial_number
																		,a.gn_lvl3_Session_Grain
																		,x
														order by 		a.index_
													)	as x1
			from	z_pa_events_fact_YYYYMM				as a
					inner join z_pa_cap_journeys_YYYYMM as trim_
					on	a.date_					= trim_.date_
					and	a.dk_serial_number		= trim_.dk_serial_number
					and	a.gn_lvl2_session_grain	= trim_.gn_lvl2_session_grain
					left join pa_signpost_events	as b
					on	a.dk_serial_number	= b.dk_Serial_number
					and	a.timems			= b.timems
			where	trim_.gn_lvl2_session = 'Top Picks'
		)	as etl1;
commit;
		

-- Finding Conversion indexes...
drop table z_pa_timeto_base_YYYYMM;commit;

with	ref_home as		(
							select	date_
									,dk_serial_number
									,session_Grain
									,min(case when gn_lvl2_session = 'Home' then start_time else null end) 		as x
							from	z_pa_cap_journeys_YYYYMM
							where	session = 'Home'
							/* -- Uncomment for working example...
							and		dk_Serial_number in ('32B0570488114031','32B0550480005140')
							and		date_ = '2017-04-01'
							*/
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
							from	z_pa_cap_journeys_YYYYMM	as x
									inner join ref_home			as y
									on	x.date_				= y.date_
									and	x.dk_serial_number	= y.dk_serial_number
									and	x.session_grain		= y.session_grain
							where	x.session = 'Home'
							/* -- Uncomment for working example...
							and		x.dk_Serial_number in ('32B0570488114031','32B0550480005140')
							and		x.date_ = '2017-04-01'
							*/
							and		x.start_time >= y.x
							group	by	x.date_
										,x.dk_serial_number
										,x.session_grain
										,y.x
						)
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
		,sum(case when start_time >= b.x then coalesce(time_to_playback,time_spent) else null end) over (partition by a.date_,a.dk_serial_number,a.session_grain order by start_time rows between unbounded preceding and current row)	as cum_time_to_play
		,case when start_time = b.x_play then gn_lvl2_session else null end as play_session
into	z_pa_timeto_base_YYYYMM
from	z_pa_cap_journeys_YYYYMM	as a
		inner join	ref_conv		as b
		on	a.date_				= b.date_
		and	a.dk_serial_number	= b.dk_serial_number
		and	a.session_Grain		= b.session_grain
where	a.session = 'Home';
/* -- Uncomment for working example...
and		a.dk_Serial_number in ('32B0570488114031','32B0550480005140')
and		a.date_ = '2017-04-01'
order	by	start_time
*/
commit;

------------------------------			
-- C - Data Analysis (Queries)
------------------------------


-- Total Actions from personalised services

-- + Total Actions per Device on TLMs
--drop table 		z_pa_kpi_q1; commit;
--truncate table	z_pa_kpi_q1; commit;

--insert	into z_pa_kpi_q1
with	base as			(	-- How many Conv Actions each STB did in the Month...
							select	extract(year from x.date_)														as year_
									,extract(month from x.date_)													as month_
									,case	when x.stb_type in ('Silver','Q') then 'Gateways'
											else x.stb_type
									end																				as stb_type_
									,x.dk_Serial_number
									,case	when month_ >2 and x.gn_lvl2_session = 'Top Picks'	then 'My Q'
											when x.gn_lvl2_session = 'Sky Movies' 				then 'Sky Cinema'
											else x.gn_lvl2_session
									end																				as gn_lvl2_session_
									,sum(case when y.target is not null then n_downloads else 0 end)				as downloads
									,sum(case when y.target is not null then n_playbacks else 0 end)				as playbacks
									,sum(case when y.target is not null then n_tunings else 0 end)					as tunings
									,sum(case when y.target is not null then n_bookings else 0 end)					as bookings
									,downloads+playbacks+tunings+bookings											as tot_conv_actions
									,count(distinct x.date_||'-'||x.dk_serial_number||'-'||x.gn_lvl2_session_grain)	as njourneys
									,count(distinct(case when x.time_to_conv is not null then x.date_||'-'||x.dk_serial_number||'-'||x.gn_lvl2_session_grain else null end))	as nconv_journeys
							from	z_pa_cap_journeys_201704	as x
									inner join ref_home_start_	as y -- Home Page Performance 
									on	x.date_					= y.date_
									and	x.dk_Serial_number		= y.dk_serial_number
									and	x.gn_lvl2_session_grain	= y.target
							where	x.gn_lvl2_session in	(
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
							and		x.date_ >= '2017-04-03' -- UNCOMMENT FOR WEEKLY SNAPSHOTS ONLY!!!!!!!
							--and		x.dk_serial_number = '32B0560488001205'
							group	by	year_
										,month_
										,stb_type_
										,x.dk_serial_number
										,gn_lvl2_session_
						)
		,base_act as	(	-- How many days each STB reported data...
							select	extract(year from date_)														as year_
									,extract(month from date_)														as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end																				as stb_type_
									,dk_serial_number
									,count(distinct date_)															as nactive_days
									,count(distinct (((extract(epoch from date_ - date('2017-04-03')))/7) + 1 ))	as nactive_weeks
							from	z_pa_cap_journeys_201704
							where	date_ >= '2017-04-03' -- UNCOMMENT FOR WEEKLY SNAPSHOTS ONLY!!!!!!!
							--and		dk_serial_number = '32B0560488001205'
							group	by	year_
										,month_
										,stb_type_
										,dk_serial_number
						)
		,base_day as	(	-- What is the daily ratio of Conv Actions on TLMs for each STB...
							select	a.year_
									,a.month_
									,a.stb_type_
									,a.dk_serial_number
									,a.gn_lvl2_session_
									,a.downloads+a.bookings														as watch_later
									,a.playbacks+a.tunings														as watch_now
									,a.tot_conv_actions
									,b.nactive_days
									-- Daily
									,cast(a.tot_conv_actions as float) 	/ cast(b.nactive_days as float)			as r_day_conv_act
									,cast(watch_later as float) 		/ cast(b.nactive_days as float)			as r_watch_later
									,cast(watch_now as float) 			/ cast(b.nactive_days as float)			as r_watch_now
									,cast(njourneys as float)			/ cast(b.nactive_days as float)			as r_journeys
									,cast(nconv_journeys as float)		/ cast(b.nactive_days as float)			as r_conv_journeys
									
									-- Weekly
									,cast(a.tot_conv_actions as float) 	/ cast(b.nactive_weeks as float)		as r_week_conv_act
									,cast(watch_later as float) 		/ cast(b.nactive_weeks as float)		as r_watch_later_week
									,cast(watch_now as float) 			/ cast(b.nactive_weeks as float)		as r_watch_now_week
									,cast(njourneys as float)			/ cast(b.nactive_weeks as float)		as r_journeys_week
									,cast(nconv_journeys as float)		/ cast(b.nactive_weeks as float)		as r_conv_journeys_week
									,case when nconv_journeys > 0 then cast(tot_conv_actions as float)	/ cast(nconv_journeys as float) else 0 end	as r_acts_per_conv_j
							from	base				as a
									inner join base_act as b
									on	a.dk_serial_number	= b.dk_serial_number
						)
		,base_size as	(	-- how many STBs where active in the month, overall...
							select	extract(year from date_)			as year_
									,extract(month from date_)			as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end									as stb_type_
									,count(distinct dk_serial_number) 	as tot_active_stbs
							from	z_pa_cap_journeys_201704
							--where	dk_serial_number = '32B0560488001205'
							group	by	year_
										,month_
										,stb_type_
						)
select	x.year_
		,x.month_
		,x.stb_type_
		,x.gn_lvl2_session_
		,max(y.tot_active_stbs)												as active_base
		
		-- daily
		,sum(x.r_day_conv_act)												as tot_stb_convact_day
		,sum(x.r_watch_later)												as tot_watch_later
		,sum(x.r_watch_now)													as tot_watch_now
		,sum(x.r_journeys)													as tot_journeys
		,sum(x.r_conv_journeys)												as tot_conv_journeys
		,cast(tot_stb_convact_day as float)  / cast(active_base as float)	as measure
		,cast(tot_watch_later as float) / cast(active_base as float)		as n_watch_later
		,cast(tot_watch_now as float) / cast(active_base as float)			as n_watch_now
		,cast(tot_journeys as float) / cast(active_base as float)			as njourneys
		,cast(tot_conv_journeys as float) / cast(active_base as float)		as nconv_journeys
		
		-- weekly
		,sum(x.r_week_conv_act)												as tot_stb_convact_week
		,sum(x.r_watch_later_week)											as tot_watch_later_week
		,sum(x.r_watch_now_week)											as tot_watch_now_week
		,sum(x.r_journeys_week)												as tot_journeys_week
		,sum(x.r_conv_journeys_week)										as tot_conv_journeys_week
		,cast(tot_stb_convact_week as float)  / cast(active_base as float)	as measure_week
		,cast(tot_watch_later_week as float) / cast(active_base as float)	as n_watch_later_week
		,cast(tot_watch_now_week as float) / cast(active_base as float)		as n_watch_now_week
		,cast(tot_journeys_week as float) / cast(active_base as float)		as njourneys_week
		,cast(tot_conv_journeys_week as float) / cast(active_base as float)	as nconv_journeys_week
		,cast((sum(r_acts_per_conv_j)) as float) / cast(active_base as float) as x
		,avg(r_acts_per_conv_j) as y
		,stddev(r_acts_per_conv_j) as z
--into	z_pa_kpi_q1
from	base_day 				as x
		inner join base_size	as y
		on	x.year_		= y.year_
		and	x.month_	= y.month_
		and	x.stb_type_	= y.stb_type_
group	by	x.year_
			,x.month_
			,x.stb_type_
			,x.gn_lvl2_session_;
commit;
			

-- + Total Actions per Device on SLMs (My Q)
--drop table 		z_pa_kpi_q2; commit;
--truncate table	z_pa_kpi_q2; commit;

insert	into z_pa_kpi_q2
with	base as			(
							select	extract(year from date_) 																															as year_
									,extract(month from  date_)																															as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end																																					as stb_type_
									,dk_serial_number
									,signpost
									,sum(case when dk_Action_id  = 02400 and dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)							as ndownloads
									,sum(case when dk_Action_id  = 03000 and dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)							as nplaybacks
									,sum(case when dk_Action_id  = 00001 and dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)							as ntunings
									,sum(case when dk_Action_id  in (02000,02010,02002,02005) and dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-') then 1 else 0 end)	as nbookings
									,ndownloads+nplaybacks+ntunings+nbookings																											as tot_conv_actions
							from	z_pa_cap_signpost_201704
							where	signpost <> 'Top Picks'
							group	by	year_
										,month_
										,stb_type_
										,dk_serial_number
										,signpost
						)
		,base_act as	(	-- How many days each STB was active overall...
							select	extract(year from date_)			as year_
									,extract(month from date_)			as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end									as stb_type_
									,dk_serial_number
									,count(distinct date_)				as nactive_days
							from	z_pa_cap_journeys_201704
							group	by	year_
										,month_
										,stb_type_
										,dk_serial_number
						)
		,base_day as	(	-- What is the daily ratio of Conv Actions on TLMs for each STB...
							select	a.year_
									,a.month_
									,a.stb_type_
									,a.dk_serial_number
									,a.signpost
									,a.tot_conv_actions
									,b.nactive_days
									,cast(a.tot_conv_actions as float) / cast(b.nactive_days as float)	as r_day_conv_act
							from	base				as a
									inner join base_act as b
									on	a.dk_serial_number	= b.dk_serial_number
						)
		,base_size as	(	-- how many STBs where active in the month, overall...
							select	extract(year from date_)			as year_
									,extract(month from date_)			as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end									as stb_type_
									,count(distinct dk_serial_number) 	as tot_active_stbs
							from	z_pa_cap_journeys_201704
							group	by	year_
										,month_
										,stb_type_
						)
select	x.year_
		,x.month_
		,x.stb_type_
		,x.signpost
		,sum(x.r_day_conv_act)												as tot_stb_convact_day
		,max(y.tot_active_stbs)												as active_base
		,cast(tot_stb_convact_day as float)  / cast(active_base as float)	as measure
--into	z_pa_kpi_q2
from	base_day 				as x
		inner join base_size	as y
		on	x.year_		= y.year_
		and	x.month_	= y.month_
		and	x.stb_type_	= y.stb_type_
group	by	x.year_
			,x.month_
			,x.stb_type_
			,x.signpost;
commit;



-- Tail analysis for Time to... distributions
with	ref_home as		(
							select	date_
									,dk_serial_number
									,session_Grain
									,min(case when gn_lvl2_session = 'Home' then start_time else null end) 		as x
							from	z_pa_cap_journeys_201704
							where	session = 'Home'
							--and		dk_Serial_number in ('32B0570488114031','32B0550480005140') -- uncomment to see a working example
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
							--and		x.dk_Serial_number in ('32B0570488114031','32B0550480005140') -- uncomment to see a working example
							and		x.date_ = '2017-04-01'
							and		x.start_time >= y.x
							group	by	x.date_
										,x.dk_serial_number
										,x.session_grain
										,y.x
						)
		,base as		(
							select	a.date_
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
									,sum(case when start_time >= b.x then coalesce(time_to_conv,time_spent) else null end) over (partition by a.date_,a.dk_serial_number,a.session_grain order by start_time rows between unbounded preceding and current row) as cum_time_to_conv
									,case when start_time = b.x_conv then gn_lvl2_session else null end as conv_session
									-- time to playback
--									,sum(case when start_time >= b.x then coalesce(time_to_playback,time_spent) else null end) over (partition by a.date_,a.dk_serial_number,a.session_grain order by start_time rows between unbounded preceding and current row) as cum_time_to_play
--									,case when start_time = b.x_play then gn_lvl2_session else null end as play_session
							from	z_pa_cap_journeys_201704	as a
									inner join	ref_conv		as b
									on	a.date_				= b.date_
									and	a.dk_serial_number	= b.dk_serial_number
									and	a.session_Grain		= b.session_grain
							where	a.session = 'Home'
							--and		a.dk_Serial_number in ('32B0570488114031','32B0550480005140') -- uncomment to see a working example
							and		a.date_ = '2017-04-01'
							--order	by	start_time -- uncomment to see a working example
						)
select	*
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
			
-- Distribution of STBs on Time to Conversion
/*
	Q3 looks at TLM levels
	Q6 looks at UI Level
	Q8 Percentiles Performance based on Time to Play for 90% cut
*/
--drop table 		z_pa_kpi_q3; commit;
--truncate table	z_pa_kpi_q3; commit;
--drop table 		z_pa_kpi_q6; commit;
--truncate table	z_pa_kpi_q6; commit;
--drop table 		z_pa_kpi_q8; commit;
--truncate table	z_pa_kpi_q8; commit;

--insert	into z_pa_kpi_q3
--insert	into z_pa_kpi_q6
--insert	into z_pa_kpi_q8
with	etl1 as 	(
						select	extract(year from date_)								as year_
								,extract(month from date_)								as month_
								,case	when stb_type in ('Silver','Q') then 'Gateways'			
										else stb_type			
								end														as stb_type_
								,dk_serial_number
								,session
								,conv_session
								,cum_time_to_conv
								,start_time
								,row_number() over	(
														partition by	dk_serial_number
																		,conv_session
														order by		cum_time_to_conv	asc
																		,start_time			asc
													)	as nrow
								,row_number() over	(
														partition by	dk_serial_number
																		,conv_session
														order by		cum_time_to_conv 	desc
																		,start_time 		desc
													)	as nrowx
								,nrow-nrowx				as delta --<- ANSWER
						from	z_pa_timeto_base_YYYYMM
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
		,etl2 as	(
						select	year_
								,month_
								,stb_type_
								,dk_serial_number
								,case when month_ > 2 and conv_session = 'Top Picks' then 'My Q' else conv_session end as gn_lvl2_session_ -- Comment for z_pa_kpi_q6
								,round(avg(cum_time_to_conv),2) as avg_time_to_conv
						from	etl1
						where	delta between -1 and 1
						group	by	year_
									,month_
									,stb_type_
									,dk_serial_number
									,gn_lvl2_session_
					)

-- Query for : z_pa_kpi_q3
select	year_
		,month_
		,stb_type_
		,gn_lvl2_session_
		,avg_time_to_conv
		,count(distinct dk_serial_number) as nstbs
--into	z_pa_kpi_q3
from	etl2
group	by	year_
			,month_
			,stb_type_
			,gn_lvl2_session_
			,avg_time_to_conv;
			
-- query for : z_pa_kpi_q6
select	year_
		,month_
		,stb_type_
		,time_to_conv						as avg_time_to_conv
		,count(distinct dk_Serial_number) 	as nstbs
--into	z_pa_kpi_q6
from	(
			select	year_
					,month_
					,stb_type_
					,dk_serial_number
					,avg(avg_time_to_conv)	as time_to_conv
			from	etl2
			group	by	year_
						,month_
						,stb_type_
						,dk_serial_number
		)	as base
group	by	year_
			,month_
			,stb_type_
			,avg_time_to_conv;
			
-- query for: z_pa_kpi_q8
select	year_
		,month_
		,stb_type_
		,dk_serial_number
		,time_to_conv
		,ntile(10) over	(
							partition by	year_
											,month_
											,stb_type_
							order by		time_to_conv
						)	as percentiles
--into	z_pa_kpi_q8
from	(
			select	year_
					,month_
					,stb_type_
					,dk_serial_number
					,avg(avg_time_to_conv)	as time_to_conv
			from	etl2
			group	by	year_
						,month_
						,stb_type_
						,dk_serial_number
		)	as base
where	(
			stb_type_ = 'Gateways' and time_to_conv <= 112	-- 90% cut for Gateways Time to Conv on Sessions
			or
			stb_type_ = 'MR' and time_to_conv <= 117		-- 90% cut for MR Time to Conv on Sessions
		);

commit;
			
			
-- Distribution of STBs on Time to Playback
/*
	Q4 looks at TLM levels
	Q7 looks at UI Level
	Q9 Percentiles Performance based on Time to Play for 90% cut
*/
--drop table 		z_pa_kpi_q4; commit;
--truncate table	z_pa_kpi_q4; commit;
--drop table 		z_pa_kpi_q7; commit;
--truncate table	z_pa_kpi_q7; commit;
--drop table 		z_pa_kpi_q9; commit;
--truncate table	z_pa_kpi_q9; commit;

--insert	into z_pa_kpi_q4
--insert	into z_pa_kpi_q7
--insert	into z_pa_kpi_q9
with	etl1 as 	(
						select	extract(year from date_)								as year_
								,extract(month from date_)								as month_
								,case	when stb_type in ('Silver','Q') then 'Gateways'			
										else stb_type			
								end														as stb_type_
								,dk_serial_number
								,play_session
								,cum_time_to_play
								,start_time
								,row_number() over	(
														partition by	dk_serial_number
																		,play_session
														order by		cum_time_to_play	asc
																		,start_time			asc
													)	as nrow
								,row_number() over	(
														partition by	dk_serial_number
																		,play_session
														order by		cum_time_to_play	desc
																		,start_time 		desc
													)	as nrowx
								,nrow-nrowx				as delta --<- ANSWER
						from	z_pa_timeto_base_YYYYMM
						where	play_session in	(
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
						and		cum_time_to_play <= 1050 -- Capping...
					)
		,etl2 as	(
						select	year_
								,month_
								,stb_type_
								,dk_serial_number
								,case when month_ > 2 and play_session = 'Top Picks' then 'My Q' else play_session end	as gn_lvl2_session_ -- Comment for z_pa_kpi_q7
								,round(avg(cum_time_to_play),2) 														as avg_time_to_play
						from	etl1
						where	delta between -1 and 1
						group	by	year_
									,month_
									,stb_type_
									,dk_serial_number
									,gn_lvl2_session_
					)

-- Query for : z_pa_kpi_q4
select	year_
		,month_
		,stb_type_
		,gn_lvl2_session_
		,avg_time_to_play
		,count(distinct dk_serial_number) as nstbs
--into	z_pa_kpi_q4
from	etl2
group	by	year_
			,month_
			,stb_type_
			,gn_lvl2_session_
			,avg_time_to_play;
			
-- query for : z_pa_kpi_q7
select	year_
		,month_
		,stb_type_
		,time_to_play						as avg_time_to_play
		,count(distinct dk_Serial_number) 	as nstbs
--into	z_pa_kpi_q7
from	(
			select	year_
					,month_
					,stb_type_
					,dk_serial_number
					,avg(avg_time_to_play)	as time_to_play
			from	etl2
			group	by	year_
						,month_
						,stb_type_
						,dk_serial_number
		)	as base
group	by	year_
			,month_
			,stb_type_
			,avg_time_to_play;
commit;

-- query for: z_pa_kpi_q9
select	year_
		,month_
		,stb_type_
		,dk_serial_number
		,time_to_play
		,ntile(10) over	(
							partition by	year_
											,month_
											,stb_type_
							order by		time_to_play
						)	as percentiles
--into	z_pa_kpi_q9
from	(
			select	year_
					,month_
					,stb_type_
					,dk_serial_number
					,avg(avg_time_to_play)	as time_to_play
			from	etl2
			group	by	year_
						,month_
						,stb_type_
						,dk_serial_number
		)	as base
where	(
			stb_type_ = 'Gateways' and time_to_play <= 122	-- 90% cut for Gateways
			or
			stb_type_ = 'MR' and time_to_play <= 125		-- 90% cut for MR
		);

commit;



-- Total Usage of UI
--drop table 		z_pa_kpi_q5; commit;
--truncate table	z_pa_kpi_q5; commit;

insert	into z_pa_kpi_q5
with	base as			(	-- Compressing at Session Level...
							select	extract(year from date_)																	as year_
									,extract(month from date_)																	as month_
									,date_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end																							as stb_type_
									,dk_serial_number
									,case when gn_lvl2_session = 'Mini Guide' then gn_lvl2_session else session end				as session_
									,case when gn_lvl2_session = 'Mini Guide' then gn_lvl2_session_grain else session_grain end	as session_grain_
									,min(start_time)																			as start_time_
									,max(end_time)																				as end_time_
									,count	(distinct(
												case	when 	gn_lvl2_Session in (
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
																then date_||'-'||dk_serial_number||'-'||gn_lvl2_session_Grain
														else 	null 
												end
											))																					as ntlms_journeys
									,sum(n_minibrowse)																			as mini_browse
									,sum(n_downloads+n_bookings)																as watch_later
									,sum(n_playbacks+n_tunings)																	as watch_now
									,case	when ntlms_journeys+mini_browse+watch_later+watch_now = 0			then 'Abandoned'
											when ntlms_journeys+mini_browse > 0 and watch_later+watch_now <1	then 'Exploring'
											when watch_later >0 and watch_now <1 								then 'Watching Later'
											when watch_now > 0 													then 'Watching Now'
											else 'uncategorised'
									end		as classification
							from	z_pa_cap_journeys_201704
							where	session in ('Home','Fullscreen')
							and		stb_type_ is not null
							and		date_ >= '2017-04-03' -- UNCOMMENT FOR WEEKLY SNAPSHOTS ONLY!!!!!!!
							group	by	year_
										,month_
										,date_
										,stb_type_
										,dk_serial_number
										,session_
										,session_grain_
						)
		,base_act as	(	-- How many days each STB reported data...
							select	extract(year from date_)			as year_
									,extract(month from date_)			as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end									as stb_type_
									,dk_serial_number
									,count(distinct date_)				as nactive_days
									,count(distinct (((extract(epoch from date_ - date('2017-04-03')))/7) + 1 )) as nactive_weeks
							from	z_pa_cap_journeys_201704
							where	date_ >= '2017-04-03' -- UNCOMMENT FOR WEEKLY SNAPSHOTS ONLY!!!!!!!
							group	by	year_
										,month_
										,stb_type_
										,dk_serial_number
						)
		,base_day as	(	-- Compressing to a daily activity for each STB...
							select	a.year_
									,a.month_
									,a.stb_type_
									,a.dk_serial_number
									,a.session_
									,a.classification
									,count(distinct a.date_||'-'||a.dk_serial_number||'-'||a.session_grain_) 	as nsessions
									,max(b.nactive_days)														as nactive_days_
									,max(b.nactive_weeks)														as nactive_weeks_
									,cast(nsessions as float) / cast(nactive_days_ as float)					as r_day_ses
									,cast(nsessions as float) / cast(nactive_weeks_ as float)					as r_week_ses
							from	base				as a
									inner join base_act	as b
									on	a.dk_serial_number = b.dk_serial_number
							group	by	a.year_
										,a.month_
										,a.stb_type_
										,a.dk_serial_number
										,a.session_
										,a.classification
						)
		,base_size as	(	-- how many STBs where active in the month, overall...
							select	extract(year from date_)			as year_
									,extract(month from date_)			as month_
									,case	when stb_type in ('Silver','Q') then 'Gateways'
											else stb_type
									end									as stb_type_
									,count(distinct dk_serial_number) 	as tot_active_stbs
							from	z_pa_cap_journeys_201704
							group	by	year_
										,month_
										,stb_type_
						)
select	x.year_
		,x.month_
		,x.stb_type_
		,x.session_
		,x.classification
		,sum(x.r_day_ses)												as tot_stb_ses_day
		,sum(x.r_week_ses)												as tot_stb_ses_week
		,max(y.tot_active_stbs)											as active_base
		-- Daily
		,cast(tot_stb_ses_day as float)  / cast(active_base as float)	as measure
		-- weekly
		,cast(tot_stb_ses_week as float)  / cast(active_base as float)	as measure_week
--into	z_pa_kpi_q5
from	base_day 				as x
		inner join base_size	as y
		on	x.year_		= y.year_
		and	x.month_	= y.month_
		and	x.stb_type_	= y.stb_type_
group	by	x.year_
			,x.month_
			,x.stb_type_
			,x.session_
			,x.classification;
commit;


-- AVG Daily Active STBs

/*
	for active we need to consider how much activity they have had rather than just returning data out of echo mode...
	16% of boxes on the 1st of April did up to 3 actions only (this cannot be treated as active)
*/
select	extract(year from date_)	as year_
		,extract(month from date_)	as month_
		,date_
		,stb_type_
		,count(distinct dk_Serial_number) as nactive_stbs
from	(
			select	date_
					,case	when stb_type in ('Silver','Q') then 'Gateways'
							else stb_type
					end		as stb_type_
					,dk_serial_number
					,count(1) as nactions
			from	z_pa_events_fact_201704
			where	dk_trigger_id not in ('userInput-unknown','system-','UNKNOWN-')
			and		stb_type is not null
			group	by	date_
						,stb_type_
						,dk_serial_number
			having	nactions > 4 -- > a box must have done over 4 actions to be counted as legitimate active on a given day...
		)	as base
group	by	year_
			,month_
			,date_
			,stb_type_
			

			
-- Creating Quartiles based on Time To Conversion
-- at Session Level
-- at TLM Level










