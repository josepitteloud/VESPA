
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
**Stakeholder:                          Product Team
**Due Date:                             
**Sharepoint Folder:

**Business Brief:

        KPI Definitions
		
**Considerations:

		
		
**Sections:

		A - Data Evaluation
			
			5 - Evaluating Distributions
				- 5.1) AVg Sessions per STB
					5.1.1) AVG Sessions per STB split by Conversion Flag (conv_flag)
				- 5.2) AVG TLMs Visited
				- 5.3) AVG # Journeys Done per SESSIONS_PER_USER
				- 5.4) AVG # Journeys per Sessions
				- 5.5) Sessions Length for Converted (Time to conversion)/Abandoned(Full session Length) journeys...
				- 5.6) Session Length (Seconds) Overall
				- 5..7) Time to abandon to Power OFFLINE
				
			7 - Phase 2: Sample of STBs that converted on monthly basis
			
		B - Data Preparation
			1 - Incorporating Master Sessions (Sessions LvL 1)
			2 - Compacting to Session Level
			3 - Now proceeding to aggregate at Session Level
			8 - Phase 2: Quartile Segmentation based on converted sessions [TABLEAU VIS]
				- 8.1) Phase 2: same approach as above but only Gateways this time
			9 - Phase 2: R4 Trial group
			
		C - Data Analysis (Queries)
			4 - Generating KPIs At Session LeveL
			6 - Generating KPIs At Journey LeveL
				- 6.1) Distributions (Time per TLM)
			
			
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/


-----------------------
-- B - Data Preparation
-----------------------

-- 1 - Incorporating Master Sessions (Sessions LvL 1)


--drop table	z_pa_events_fact_v2;commit;
--truncate table	z_pa_events_fact_v2;commit;

insert	into z_pa_events_fact_v2
select	index_
		,date_
		,dt	
		,dk_serial_number
		,extract(epoch from (min(dt) over (partition by date_,dk_serial_number order by index_ rows between 1 following and 1 following))- dt) 	as ss_before_next_action
		,extract(epoch from	dt - ( min(dt) over	(partition by date_,dk_serial_number order by	index_ rows between	1 preceding and 1 preceding)))		as ss_elapsed_next_action	-- time difference between current action and preceding action in seconds
		,last_value(w0 ignore nulls) over	(
												partition by	date_
																,dk_serial_number
												order by		index_
											)	as Session_type -- as w
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,gn_lvl2_session
		,gn_lvl2_session_grain
--into	z_pa_events_fact_v2
from	(
			select	*
					,x1||'-'||dense_rank() over (partition by date_,dk_serial_number,x1 order by index_) as w0
			from	(
						select	*
								,max(y0)  over	(
													partition by 	date_
																	,dk_serial_number
													order by		index_
													rows between	1 preceding and 1 preceding
												)	as z
								,case	when (z is null or z<>y0)	then x
										else null
								end		x1
						from	(
									select	index_
											,date_
											,dt
											,dk_serial_number
											,gn_lvl2_session
											,gn_lvl2_session_grain
											,dk_action_id
											,dk_previous
											,dk_current
											,dk_referrer_id
											,dk_trigger_id
											,case	when gn_lvl2_session in ('Home','Fullscreen')	then gn_lvl2_session 
													when dk_action_id = 00003 						then 'Stand By Out'
													when dk_action_id = 00004						then 'Reboot'
													else null end	as x
											,last_value(x ignore nulls) over	(
																					partition by	date_
																									,dk_serial_number
																					order by 		index_
																					rows between	200 preceding and current row
																				) as  y0
									from	z_pa_events_fact
									where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
									--where	date_ = '2016-11-04 00:00:00'
									--and		dk_serial_number = '32B0580488179819' -- bingo!
									--order	by	index_
									--limit	200
								)	as base
					)	as	step_1
		)	as	step2;
--order	by	index_
commit;



-- 2 - Compacting to Session Level

/*
	Here I'm identifying all journeys that began from home...
	
	The way I'm doing this is simple, every session has a preceding one and a succeeding one.
	It is a natural characteristic that any journey (session) into any TLM offered in the homepage
	will have a preceding Home Session (for example, if you navigated into Sky Store from Home then there is no other preceding
	session that sky Store could have than a home one)
	
*/
--drop table ref_home_start;commit;
--truncate table ref_home_start;commit;

insert	into ref_home_start
select	*
-- into	ref_home_start
from	(
			select	date_
					,dk_serial_number
					,gn_lvl2_session
					,target
					,n_globnav_clicks
					,max(target) over	(
											partition by	date_
															,dk_serial_number
											order by		start_
											rows between	1 preceding and 1 preceding
										)	as origin
			from	(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain 																	as target
								,min(index_)																			as start_
								,sum(case when dk_action_id = 01400 and dk_trigger_id <> 'system-' then 1 else 0 end)	as n_globnav_clicks
						from 	z_pa_events_fact_v2
						where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
					)	as base
		)	as base2
where	lower(origin) like 'home%'
and		gn_lvl2_session in	(
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
							);

commit;


-- 3 - Now proceeding to aggregate at Session Level

--drop table		z_pa_kpi_def_lvl1;commit;
--truncate table	z_pa_kpi_def_lvl1;commit;

insert	into z_pa_kpi_def_lvl1
with	ref_conv as	(

						/*
							Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							to use that then as a flag to derive time to conversion (this is, how many seconds since the
							beginning of the Session until the very first converting action)
						*/

						select	date_
								,dk_serial_number
								,session_type
								,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) 	as x
								--,max(index_)																							as end_
						from	z_pa_events_fact_v2
						where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
						and		date_ between '2016-10-01' and '2016-10-31' --> Parameter
						--and		date_ = '2016-10-04 00:00:00'
						--and		dk_serial_number = '32B0560488008521'
						--and		session_type = 'Home-9'
						group	by	date_
									,dk_serial_number
									,session_type
					)
select	*
--into	z_pa_kpi_def_lvl1
from	(
			select	*
					,min(session_type) over	(
												partition by	date_
																,dk_Serial_number
												order by		the_index
												rows between	1 following and 1 following
											)	as x
			from	(
						select	extract(month from a.date_)																		as the_month
								,a.date_
								,a.dk_serial_number
								,a.session_type
								,max(
										case	when a.dk_action_id = 00002 and a.dk_trigger_id = 'timeOut-' 				then 'Stand By in - System Time out'
												when a.dk_action_id = 00002 and a.dk_trigger_id = 'userInput-powerButton'	then 'Stand By in - Manual Power Off'
										end		
									)	as	ending
								,max(case when a.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
								,min(a.index_)																					as the_index
								,sum(case when a.dk_trigger_id <> 'system-' then 1 else 0 end)									as nclicks 					-- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
								,sum(case when a.dk_trigger_id <> 'system-' and a.dk_action_id = '01400' then 1 else 0 end)		as n_ses_globnav_clicks 	-- Likewise above, although this time only interested in navigation clicks
								,sum( case when c.target is not null and a.dk_action_id = '01400' then 1 else 0 end )			as n_jour_globnav_clicks 	-- Then here only counting those global nav / not-system clicks done within journeys (subset of above)
								,count(distinct	(
													case	when a.gn_lvl2_session in	(
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
																						)	then a.gn_lvl2_session
															else 	null
													end
												))	as ntlms_visited
								,count(distinct (c.date_||'-'||c.dk_serial_number||'-'||c.target))								as ntlms_journeys
								,sum(a.ss_elapsed_next_action)																	as session_length_ss
								,sum( case when a.index_ <= b.x 	then a.ss_elapsed_next_action else null end)				as time_to_conv
								,sum( case when a.dk_action_id = 04002 then 1 else 0 end) 										as n_applaunches
								,sum( case when a.dk_action_id = 03000 then 1 else 0 end)										as n_playbacks
								,sum( case when a.dk_action_id = 02400 then 1 else 0 end)										as n_downloads
								,sum( case when a.dk_action_id = 00001 then 1 else 0 end)										as n_tunings
								,sum( case when a.dk_action_id in (02000,02010,02002,02005) then 1 else 0 end)					as n_bookings
						from	z_pa_events_fact_v2 	as a
								left join	ref_conv	as b
								on	a.date_				= b.date_
								and	a.dk_serial_number	= b.dk_serial_number
								and	a.session_type		= b.session_type
								left join	ref_home_start	as c
								on	a.date_					= c.date_
								and	a.dk_serial_number		= c.dk_serial_number
								and	a.gn_lvl2_session_grain	= c.target
						where	a.date_ between '2016-10-01' and '2016-10-31' --> Parameter
						--where	a.date_ = '2016-10-04 00:00:00'
						--and		a.dk_serial_number = '32B0560488008521'
						--and		a.session_type = 'Home-9'
						group	by	the_month
									,a.date_
									,a.dk_serial_number
									,a.session_type
					)	as base
		)	as	step1
where	session_type like 'Home%'
and		(
			ending <> ''
			or
			x like 'Fullscreen%'
		);
commit;


-- 8 - Phase 2: Quartile Segmentation based on converted sessions

-- Basing subsequent scripts on the monthly 97% or so STBs converting...

--drop table z_pa_kpi_def_qtiles;commit;
--truncate table z_pa_kpi_def_qtiles;commit;

insert	into z_pa_kpi_def_qtiles
select	base.the_month
		,case	substr(base.dk_serial_number,3,1)
				when 'B' then 'Gateway'
				when 'C' then 'Gateway'
				when 'D' then 'MR'
		end		as Stb_type
		,base.dk_serial_number
		,count(distinct base.date_||'-'||base.session_type) 													as nsessions
		,count(distinct (case when base.conv_flag = 1 then base.date_||'-'||base.session_type else null end))	as nconv_sessions
		,cast(nconv_sessions as float) / cast(nsessions as float)												as ses_conversion_rate
		,ntile(4) over (order by ses_conversion_rate)															as Qtile
--into	z_pa_kpi_def_qtiles
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
						and		date_ between '2016-10-01' and '2016-10-31' --> Parameter
					)	as excl_list
		on	base.date_				= excl_list.date_
		and	base.dk_Serial_number	= excl_list.dk_serial_number
		and	base.session_type		= excl_list.session_type
where	excl_list.dk_serial_number is null
and		base.date_ between '2016-10-01' and '2016-10-31' --> Parameter
group	by	base.the_month
			,Stb_type
			,base.dk_serial_number
having	nconv_sessions > 0;

commit;


-- 8.1) Phase 2: same approach as above but only Gateways this time

--drop table z_pa_kpi_def_qtiles2;commit;
--truncate table z_pa_kpi_def_qtiles2;commit;

insert	into z_pa_kpi_def_qtiles2
select	base.the_month
		,case	substr(base.dk_serial_number,3,1)
				when 'B' then 'Gateway'
				when 'C' then 'Gateway'
				when 'D' then 'MR'
		end		as Stb_type
		,base.dk_serial_number
		,count(distinct base.date_||'-'||base.session_type) 													as nsessions
		,count(distinct (case when base.conv_flag = 1 then base.date_||'-'||base.session_type else null end))	as nconv_sessions
		,cast(nconv_sessions as float) / cast(nsessions as float)												as ses_conversion_rate
		,ntile(4) over (order by ses_conversion_rate)															as Qtile
--into	z_pa_kpi_def_qtiles2
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
						and		date_ between '2016-10-01' and '2016-10-31' --> Parameter
					)	as excl_list
		on	base.date_				= excl_list.date_
		and	base.dk_Serial_number	= excl_list.dk_serial_number
		and	base.session_type		= excl_list.session_type
where	excl_list.dk_serial_number is null
and		base.date_ between '2016-10-01' and '2016-10-31' --> Parameter
group	by	base.the_month
			,Stb_type
			,base.dk_serial_number
having	nconv_sessions > 0;

commit;



-- 9 - Phase 2: R4 Trial group

--drop table z_pa_kpi_def_r4;commit;

select	*
into	z_pa_kpi_def_r4
from	(
			-- The list of Serial Numbers is of 11K STBs, it's way to extensive like to place it here
			-- the list of Serial Numbers is at: 
			-- >>>>> G:\RTCI\Sky Projects\Vespa\Products\Analysis - Excel <<<<<
		)	as base;
		
commit;

------------------------------
-- C - Data Analysis (Queries)
------------------------------
		
-- 4 - Generating KPIs At Session LeveL

/*
	NOTE: Uncomment line for enabling KPIs broken-down by STB Type or Quartiles
*/

with	base_size as	(
							select	the_month
									--,case	substr(dk_serial_number,3,1)
											--when 'B' then 'Gateway'
											--when 'C' then 'Gateway'
											--when 'D' then 'MR'
									--end		as Stb_type
									,count(distinct dk_serial_number) as nactive_boxes
							from	z_pa_kpi_def_lvl1
							where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
							group	by	the_month
										--,Stb_type
						)
select	base.the_month
		--,qtiles.qtile
		--,base.stb_type
		,base_size.nactive_boxes
		,case	when base.x like 'Fullscreen%' then 'To Fullscreen'
				when base.ending <> '' then base.ending
				else 'weirdcase'
		end		as split
		,base.conv_flag
		,count(distinct base.dk_serial_number)											as nboxes
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.session_type) as n_sessions
		,avg(base.nclicks) 																as avg_clicks_per_session
		,avg(base.ntlms_visited)														as avg_tlms_visited_per_session
		,avg(base.ntlms_journeys)														as avg_tlms_journeys_per_session
		,avg(base.session_length_ss)													as avg_session_length_ss	
		,avg(base.time_to_conv)															as avg_time_to_conv_per_session
		,avg	( 
					case	when (ntlms_journeys <=0 or ntlms_journeys is null)	then null
							else cast(n_jour_globnav_clicks as float)/cast(ntlms_journeys as float)
					end
				)	as avg_nav_clicks_per_journey
from	z_pa_kpi_def_lvl1 as base 
		--(
			--select	*
					--,case	substr(dk_serial_number,3,1)
							--when 'B' then 'Gateway'
							--when 'C' then 'Gateway'
							--when 'D' then 'MR'
					--end		as Stb_type
			--from	z_pa_kpi_def_lvl1
			--where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
		--)	as base
		--inner join z_pa_kpi_def_qtiles as qtiles
		--on	base.the_month			= qtiles.the_month
		--and	base.dk_Serial_number	= qtiles.dk_serial_number
		--and	base.stb_type			= qtiles.stb_type
		inner join	base_size
		on	base.the_month	= base_size.the_month
		--and	base.stb_type	= base_size.stb_type
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
where	excl_list.dk_serial_number is null
and		base.date_ between '2016-10-01' and '2016-10-31' --> Parameter
and		(	
			(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- >80% of all converted sessions
			or
			(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- >80% of all abandoned sessions
		)
group	by	base.the_month
			--,qtiles.qtile
			--,base.stb_type
			,base_size.nactive_boxes
			,split
			,base.conv_flag
			
			
			

-- 5 - Evaluating Distributions


-- 5.1) AVg Sessions per STB


select	(nsessions/10)*10	as n10_sessions
		,count(distinct dk_serial_number)	as nboxes
from	(
			select	base.dk_serial_number
					,count(distinct base.DATE_||'-'||base.session_type)	as nsessions
			from	z_pa_kpi_def_lvl1	as base
					left join	(
									select	date_
											,dk_serial_number
											,session_type
									FROM	z_pa_kpi_def_lvl1
									where	(
												(ending <> '' and x like 'Fullscree%') -- 203443
												or
												(conv_flag = 1 and (time_to_conv is null or time_to_conv <=0))
											) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
								)	as excl_list
					on	base.date_				= excl_list.date_
					and	base.dk_Serial_number	= excl_list.dk_serial_number
					and	base.session_type		= excl_list.session_type
			where	excl_list.dk_serial_number is null
			group	by	base.dk_serial_number
		)	as step1
group	by	n10_sessions


-- 5.1.1) AVG Sessions per STB split by Conversion Flag (conv_flag)

select	conv_flag
		,(nsessions/10)*10	as n10_sessions
		,count(distinct dk_serial_number)	as nboxes
from	(
			select	base.dk_serial_number
					,base.conv_flag
					,count(distinct base.DATE_||'-'||base.session_type)	as nsessions
			from	z_pa_kpi_def_lvl1	as base
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
			where	excl_list.dk_serial_number is null
			group	by	base.dk_serial_number
						,base.conv_flag
		)	as step1
group	by	conv_flag
			,n10_sessions
			
			
-- 5.2) AVG TLMs Visited

select	base.the_month
		,base.CONV_FLAG
		,base.NTLMS_VISITED
		,count(1) as hits
from	z_pa_kpi_def_lvl1	as base
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
group	by	1,2,3


-- 5.3) AVG # Journeys Done per SESSIONS_PER_USER

select	base.CONV_FLAG
		,base.ntlms_journeys
		,count(1) as hits
from	z_pa_kpi_def_lvl1	as base
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
group	by	1,2


-- 5.4) AVG # Journeys per Sessions

select	base.the_month
		--,qtiles.qtile
		--,case	substr(base.dk_serial_number,3,1)
				--when 'B' then 'Gateway'
				--when 'C' then 'Gateway'
				--when 'D' then 'MR'
		--end		as Stb_type
		,base.conv_flag
		,base.ntlms_journeys
		,count(1) as nsessions
from	z_pa_kpi_def_lvl1	as base
		--inner join z_pa_kpi_def_qtiles as qtiles
		--on	base.the_month			= qtiles.the_month
		--and	base.dk_Serial_number	= qtiles.dk_serial_number
		--and	base.stb_type			= qtiles.stb_type
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
group	by	1,2,3 --,4 --5,


-- 5.5) Sessions Length for Converted (Time to conversion)/Abandoned(Full session Length) journeys...

select	base.the_month
		--,qtiles.qtile
		--,case	substr(base.dk_serial_number,3,1)
				--when 'B' then 'Gateway'
				--when 'C' then 'Gateway'
				--when 'D' then 'MR'
		--end		as Stb_type
		,conv_flag
		,floor (cast(coalesce(time_to_conv,session_length_ss) as float)/10) * 10	as Time_bands
		,count(1) as nsessions
from	z_pa_kpi_def_lvl1	as base
		--inner join z_pa_kpi_def_qtiles as qtiles
		--on	base.the_month			= qtiles.the_month
		--and	base.dk_Serial_number	= qtiles.dk_serial_number
		--and	base.stb_type			= qtiles.stb_type
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
group	by	1,2,3 --,4 --,5


-- 5.6) Session Length (Seconds) Overall

select	base.SESSION_LENGTH_SS
		,count(1) as nsessions
from 	z_pa_kpi_def_lvl1	as base
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
								) -- there are session that move from Home to fullscreen and then to timeout-StandBy, but this is not properly notified, hence having to exclude these (0.3% out of 100%)
					)	as excl_list
		on	base.date_				= excl_list.date_
		and	base.dk_Serial_number	= excl_list.dk_serial_number
		and	base.session_type		= excl_list.session_type
where	excl_list.dk_serial_number is null
group	by	1


-- 5..7) Time to abandon to Power OFFLINE

select	*
from	(
			select	floor (cast(session_length_ss as float)/10) * 10	as session_length_ss_band
					,count(1) as nsessions
			from 	z_pa_kpi_def_lvl1
			where	conv_flag = 0
			and		ending = 'Stand By in - Manual Power Off'
			group	by	session_length_ss_band
		)	as d
where	session_length_ss_band > 0 and session_length_ss_band is not null





-- 6 - Generating KPIs At Journey LeveL


with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
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
							and		base.date_ between '2016-10-01' and '2016-10-31' --> Parameter
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,base_size as	(
							select	the_month
									--,case	substr(dk_serial_number,3,1)
											--when 'B' then 'Gateway'
											--when 'C' then 'Gateway'
											--when 'D' then 'MR'
									--end		as Stb_type
									,count(distinct dk_serial_number) as nactive_boxes
							from	z_pa_kpi_def_lvl1
							where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
							--and		substr(dk_serial_number,3,1) in ('B','C')	-- to isolate Gateways only without having to include stb_type, quick fix...
							group	by	the_month
										--,Stb_type
						)
		,ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	z_pa_events_fact_v2
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		date_ between '2016-10-01' and '2016-10-31' --> Parameter
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
select	sessions.the_month
		--,qtiles.qtile
		--,base.Stb_type
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
		--(
			--select	*
					--,case	substr(dk_serial_number,3,1)
							--when 'B' then 'Gateway'
							--when 'C' then 'Gateway'
							--when 'D' then 'MR'
					--end		as Stb_type
			--from	z_pa_events_fact_v2
			--where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
		--)	as base
		left join	ref_conv 			-- Converted journeys reference
		on	base.date_					= ref_conv.date_
		and	base.dk_serial_number		= ref_conv.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
		inner join	ses_sample	as sessions -- Home Sessions reference
		on	base.date_				= sessions.date_
		and	base.dk_serial_number	= sessions.dk_Serial_number
		and	base.session_type		= sessions.session_type
		--inner join z_pa_kpi_def_qtiles as qtiles
		--on	sessions.the_month			= qtiles.the_month
		--and	sessions.dk_Serial_number	= qtiles.dk_serial_number
		inner join 	base_size
		on	sessions.the_month		= base_size.the_month
		--and	base.stb_type			= base_size.stb_type
		inner join	ref_home_start	as c
		on	base.date_					= c.date_
		and	base.dk_serial_number		= c.dk_serial_number
		and	base.gn_lvl2_session_grain	= c.target
where	base.date_ between '2016-10-01' and '2016-10-31' --> Parameter
group	by	sessions.the_month
			--,qtiles.qtile
			--,base.Stb_type
			,base.gn_lvl2_session
			
			
-- 6.1) Distributions (Time per TLM)


with	ses_sample as	(
							--	Generating Slicer to carve the exact same Sessions we use to generate before KPIs...
							--	The idea is to see TLM behaviour for the same group of sessions we are analysing

							select	base.the_month
									--,qtiles.qtile
									,base.DATE_
									,base.DK_SERIAL_NUMBER
									,base.SESSION_TYPE
							from	z_pa_kpi_def_lvl1	as base
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
									--inner join z_pa_kpi_def_qtiles as qtiles
									----inner join z_pa_kpi_def_qtiles2 as qtiles -- For only Gateways
									--on	base.the_month			= qtiles.the_month
									--and	base.dk_Serial_number	= qtiles.dk_serial_number
							where	excl_list.dk_serial_number is null -- this is here to really make the exclusion happen...
							and		(	
										(base.conv_flag = 1 and base.time_to_conv between 0 and 600) -- 80% of all converted sessions
										or
										(base.conv_flag = 0 and base.session_length_ss between 0 and 1000) -- 80% of all abandoned sessions
									)
						)
		,base_size as	(
							select	the_month
									--,case	substr(dk_serial_number,3,1)
											--when 'B' then 'Gateway'
											--when 'C' then 'Gateway'
											--when 'D' then 'MR'
									--end		as Stb_type
									,count(distinct dk_serial_number) as nactive_boxes
							from	z_pa_kpi_def_lvl1
							where	substr(dk_serial_number,3,1) in ('B','C')	-- to isolate Gateways only without having to include stb_type, quick fix...
							group	by	the_month
										--,stb_type
						)
		,ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
							from	z_pa_events_fact_v2
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		date_ between '2016-10-01' and '2016-11-30' --> Parameter
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
select	the_month
		--,qtile
		--,stb_type
		,gn_lvl2_session
		,case when sces_to_conversion is not null then 1 else 0 end 					as conv_flag
		,floor (cast(coalesce(sces_to_conversion,n_secs_spent) as float)/10) * 10		as session_length_ss_band
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)		as njourneys
from	(
			select	sessions.the_month
					--,sessions.qtile
					,base.date_
					,case	substr(base.dk_serial_number,3,1)
							when 'B' then 'Gateway'
							when 'C' then 'Gateway'
							when 'D' then 'MR'
					end		as Stb_type
					,base.dk_serial_number
					,base.gn_lvl2_session
					,base.gn_lvl2_session_grain
					,sum(ss_elapsed_next_action) 																											as n_secs_spent
					,sum( case when ref_conv.gn_lvl2_session_grain is not null and base.INDEX_ < ref_conv.x then base.SS_ELAPSED_NEXT_ACTION else null end) as sces_to_conversion
			from	z_pa_events_fact_v2 	as base -- Transactional data with Sessions and Journeys...
					left join	ref_conv 	-- Converted journeys reference
					on	base.date_					= ref_conv.date_
					and	base.dk_serial_number		= ref_conv.dk_serial_number
					and	base.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
					inner join	ses_sample	as sessions
					on	base.date_				= sessions.date_
					and	base.dk_serial_number	= sessions.dk_Serial_number
					and	base.session_type		= sessions.session_type
					inner join 	base_size
					on	sessions.the_month		= base_size.the_month
					inner join	ref_home_start	as c
					on	base.date_					= c.date_
					and	base.dk_serial_number		= c.dk_serial_number
					and	base.gn_lvl2_session_grain	= c.target
			where	base.date_ between '2016-10-01' and '2016-11-30' --> Parameter
			group	by	sessions.the_month
						--,sessions.qtile
						,base.date_
						,Stb_type
						,base.dk_serial_number
						,base.gn_lvl2_session
						,base.gn_lvl2_session_grain
		)	as step1
group	by	the_month
			--,qtile
			--,stb_type
			,gn_lvl2_session
			,conv_flag
			,session_length_ss_band
			
	
-- 7 - Phase 2: Sample of STBs that converted on monthly basis

/*----------------------------------------------------------------------------

--> 97% of STBs convert at least once monthly, remaining 3% abandon entirely

THE_MONTH	NBOXES	CONV_BOX	THE_PROP
	10		403028	393738		0.976949492342964
	11		401665	391750		0.975315250270748
*/----------------------------------------------------------------------------

select	the_month
		,count(distinct dk_serial_number) as nboxes
		,count(distinct (case when conv_flag >0 then dk_serial_number else null end)) as conv_box
		,cast(conv_box as float) / cast(nboxes as float)	as the_prop
from	(
			select	base.*
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
			where	excl_list.dk_serial_number is null
		)	as x
group	by	the_month
	
			
