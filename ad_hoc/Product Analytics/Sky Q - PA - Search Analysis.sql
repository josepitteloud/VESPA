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

        Analysing Search performance in Sky Q, is there anything we can recommend to enhance users experience
		with the feature.
		
		At the moment the hypothesis of Personalising the results will bring great value...
		
		is this true?
		how much value will that bring?
		is there any other aspect to improve about Search?
		
**Considerations:

		[logged by]: 2017-02-23
		-- This round is only looking at Text search (Voice search is developed already but has not role out to
		customers)
		
**Sections:

		A - Data Evaluation
			1 - Capping Criteria
			2 - What are the buttons commonly pressed while in search
			3 -Sight seen of some numbuttons press on Search Journeys
			
		B - Data Preparation
			11 - Generating Search Sections Split
			4 - Extracting legitimate Search Journeys for analysis
			12 - Quartiles for Boxes getting the best of search
			
		C - Data Analysis (Queries)
			10 - Ground Analysis (Basic understanding of Search Performance)
			5 - Phase 1: Search Performance (High-level view)
			6 - Phase 1: Complement, performance of 5 given cohorts (new joiners, Exp Q users)
			7 - Phase 2: Exit mechanism while on Programme Detail Page & Search re-start mechanism while on Search Page
			9 - Phase 4: On Converted Journeys, what type of content users look for (VOD, Linear, etc.)
			13 - Search Distances
			
			
			
			
**Running Time:

30 Mins

--------------------------------------------------------------------------------------------------------------

*/

----------------------
-- A - Data Evaluation
----------------------


-- 1 -Capping Criteria (finding what is legitimate to analyse)...

	/*
		Chopping here for the long tail which is due to buggy Searches
	*/

select	date_
		,dk_serial_number
		,gn_lvl2_session_grain
		,sum(ss_elapsed_next_action)	as length_secs
from 	z_pa_events_fact_v2
where	date_ between '2016-10-01' and '2016-10-31'
and		gn_lvl2_session = 'Search'
group	by	date_
			,dk_serial_number
			,gn_lvl2_session_grain
having	length_secs between 0 and 1000 -- Capping Threshold for Search 82% of all Journeys...



-- 2 - What are the buttons commonly pressed while in search

with	cap as	(
					-- Capping Criteria (finding what is legitimate to analyse)...
					select	date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,sum(ss_elapsed_next_action)	as length_secs
					from 	z_pa_events_fact_v2
					where	date_ between '2016-10-01' and '2016-10-31'
					and		gn_lvl2_session = 'Search'
					group	by	date_
								,dk_serial_number
								,gn_lvl2_session_grain
					having	length_secs between 0 and 1000 -- Capping Threshold for Search 82% of all Journeys...
				)
select	*
		,sum(the_prop) over (partition by 1 order by hits desc rows between unbounded preceding and current row )	as cumu
from	(
			select	base.dk_trigger_id
					,count(1) as hits
					,cast(hits as float)/cast((sum(hits)over(partition by 1)) as float) as the_prop
			from	z_pa_events_fact	as base
					inner join cap
					on	base.date_					= cap.date_
					and	base.dk_serial_number 		= cap.dk_serial_number
					and	base.gn_lvl2_session_grain	= cap.gn_lvl2_session_grain
			where	base.gn_lvl2_session = 'Search'
			and		base.date_ between '2016-10-01' and '2016-10-31'
			group	by	base.dk_trigger_id
		)	as ground
order	by	hits desc


-- 3 - Sight seen of some numbuttons press on Search Journeys... what do they look like...

/*
	Best answer by 20170216:
	
	when looking at Oct 2016 this is true for 0.2% (7022) journeys into search... Query [IMPACT]
	
	PA doesn't track as thought text entered via numeric buttons on the search area. However, is thought that
	these journeys containing these button presses are because the user is downloading/playing assets that
	require the verification PiN, after imputing it you're redirected to Full-screen to start consuming...
	
	Another subset to these journeys I believe is made of bugs within the EPG, making boxes to send an unexpected
	global Nav message (01400) with trigger ID as any of bellow...
	
	Recommendations: Discard these journeys...
	
*/
with	cap as	(
					-- Capping Criteria (finding what is legitimate to analyse)...
					select	date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,sum(ss_elapsed_next_action)	as length_secs
					from 	z_pa_events_fact_v2
					where	date_ between '2016-10-01' and '2016-10-31'
					and		gn_lvl2_session = 'Search'
					group	by	date_
								,dk_serial_number
								,gn_lvl2_session_grain
					having	length_secs between 0 and 1000 -- Capping Threshold for Search 82% of all Journeys...
				)
select	*
from	z_pa_events_fact	as base
		inner join	(
						-- SAMPLING...
						select	distinct
								a.date_
								,a.dk_serial_number
								,a.gn_lvl2_session_grain
						from	z_pa_events_fact as a
								inner join cap
								on	a.date_					= cap.date_
								and	a.dk_serial_number		= cap.dk_serial_number
								and	a.gn_lvl2_session_grain	= cap.gn_lvl2_session_grain
						where	a.gn_lvl2_session = 'Search'
						and		a.date_ between '2016-10-01' and '2016-10-31'
						and		a.dk_trigger_id in	(
														'userInput-KeyEvent:Key_0KeyReleased'
														,'userInput-KeyEvent:Key_1KeyReleased'
														,'userInput-KeyEvent:Key_2KeyReleased'
														,'userInput-KeyEvent:Key_3KeyReleased'
														,'userInput-KeyEvent:Key_4KeyReleased'
														,'userInput-KeyEvent:Key_5KeyReleased'
														,'userInput-KeyEvent:Key_6KeyReleased'
														,'userInput-KeyEvent:Key_7KeyReleased'
														,'userInput-KeyEvent:Key_8KeyReleased'
														,'userInput-KeyEvent:Key_9KeyReleased'
														,'userInput-KeyEvent:Key_F10KeyReleased'
														,'userInput-KeyEvent:Key_F11KeyReleased'
														,'userInput-KeyEvent:Key_F12KeyReleased'
													)
						limit	200
					)	as ref
		on	base.date_					= ref.date_
		and	base.dk_serial_number		= ref.dk_serial_number
		and	base.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
where	base.gn_lvl2_session = 'Search'
and		base.date_ between '2016-10-01' and '2016-10-31'
order	by	index_

-- [IMPACT of 3]

/*
	in summary... these journeys are 0.2% out of 100% of Search Journeys
*/

with	cap as	(
					-- Capping Criteria (finding what is legitimate to analyse)...
					select	date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,sum(ss_elapsed_next_action)	as length_secs
					from 	z_pa_events_fact_v2
					where	date_ between '2016-10-01' and '2016-10-31'
					and		gn_lvl2_session = 'Search'
					group	by	date_
								,dk_serial_number
								,gn_lvl2_session_grain
					having	length_secs between 0 and 1000 -- Capping Threshold for Search 82% of all Journeys...
				)
select	count(distinct a.date_||'-'||a.dk_serial_number||'-'||a.gn_lvl2_session_grain) as n_search_journeys
		,count(distinct	(
							case	when a.dk_trigger_id in	(
																'userInput-KeyEvent:Key_0KeyReleased'
																,'userInput-KeyEvent:Key_1KeyReleased'
																,'userInput-KeyEvent:Key_2KeyReleased'
																,'userInput-KeyEvent:Key_3KeyReleased'
																,'userInput-KeyEvent:Key_4KeyReleased'
																,'userInput-KeyEvent:Key_5KeyReleased'
																,'userInput-KeyEvent:Key_6KeyReleased'
																,'userInput-KeyEvent:Key_7KeyReleased'
																,'userInput-KeyEvent:Key_8KeyReleased'
																,'userInput-KeyEvent:Key_9KeyReleased'
																,'userInput-KeyEvent:Key_F10KeyReleased'
																,'userInput-KeyEvent:Key_F11KeyReleased'
																,'userInput-KeyEvent:Key_F12KeyReleased'
															)	then a.date_||'-'||a.dk_serial_number||'-'||a.gn_lvl2_session_grain
									else null
							end
						))	as n_search_journeys_num
		,cast(n_search_journeys_num as float) / cast(n_search_journeys as float)	as the_impact
from	z_pa_events_fact as a
		inner join cap
		on	a.date_					= cap.date_
		and	a.dk_serial_number		= cap.dk_serial_number
		and	a.gn_lvl2_session_grain	= cap.gn_lvl2_session_grain
where	a.gn_lvl2_session = 'Search'
and		a.date_ between '2016-10-01' and '2016-10-31'


-----------------------
-- B - Data Preparation
-----------------------

-- 11 - Generating Search Sections Split

--drop table z_search_trans;commit;
--truncate table z_search_trans;commit;

insert	into z_search_trans
with	ref as	(
					-- Identifying Legitimate Searches...
					
					select	date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,sum(ss_elapsed_next_action)	as length_secs
					from 	z_pa_events_fact
					where	date_ between '2016-10-01' and '2016-10-31' --> Parameter
					and		gn_lvl2_session = 'Search'
					group	by	date_
								,dk_serial_number
								,gn_lvl2_session_grain
					having	length_secs between 0 and 1000 -- Capping Threshold for Search 82% of all Journeys...
				)
select	index_
		,date_
		,dt
		,dk_serial_number
		,ss_elapsed_next_action
		,'session_type' as session_type
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,gn_lvl2_session
		,gn_lvl2_session_grain
		,last_value(search_section ignore nulls) over	(
															partition by	date_
																			,dk_Serial_number
																			,gn_lvl2_Session_grain
															order by		index_
															rows between	200 preceding and current row
														)	as search_section
--into	z_search_trans
from	(
			select	*
					,page_Start||'-'||dense_rank()over(partition by date_,dk_serial_number,page_start order by index_) as search_section
			from	(
						select	*
								,max(runner) over (partition by date_,dk_serial_number,gn_lvl2_session_grain order by index_ rows between 1 preceding and 1 preceding) as prev_page
								,case	when (prev_page is null or runner <> prev_page) then pages else null end as page_start
						from	(
									selecT	base.*
											,case 	when base.dk_previous = 'guide://search' 					then 'Search Page'
													when base.dk_previous = 'guide://programme-details/interim'	then 'PG Page'
													else null
											end		pages
											,last_value(pages ignore nulls) over	(
																						partition by	base.date_
																										,base.dk_serial_number
																										,base.gn_lvl2_Session_grain
																						order by		base.index_
																						rows between	200 preceding and current row
																					)	as runner
									from	z_pa_events_Fact	as base
											inner join ref
											on	base.date_					= ref.date_
											and	base.dk_serial_number		= ref.dk_serial_number
											and	base.gn_lvl2_session_grain	=	ref.gn_lvl2_Session_grain
									where	base.date_ between '2016-11-01' and '2016-11-30' --> Parameter
								)	as step1
					)	as step2
		)	as final_;
commit;

-- 4 - Extracting legitimate Search Journeys for analysis

--drop table z_search_journeys;commit;
--truncate table z_search_journeys;commit;

insert	into z_search_journeys
with	ref_conv as	(
							--	Here I'm flagging the very first CONVERTING action (see list of action ids for reference)
							--	to use that as a flag to derive time to conversion (this is, how many seconds since the
							--	beginning of the journey until the very first converting action)

							select	date_
									,dk_serial_number
									,gn_lvl2_session_grain
									,min(case when dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then index_ else null end) as x
									,min(case when dk_Action_id = 01400 then index_ else null end) as y -- PILA CON ESTO PARA CALCULAR TIMEPO EN SEARCH PAGE
									,max(index_)	as eoj -- End Of Journey
							from	z_search_trans		as x
							where	dk_trigger_id <> 'system-' -- I'm removing actions done by the system as we are rather interested on conscious actions done by the users
							and		date_ between '2016-10-01' and '2016-10-31' --> Parameter
							group	by	date_
										,dk_serial_number
										,gn_lvl2_session_grain
						)
select	ground.date_
		,case	substr(ground.dk_serial_number,3,1)
				when 'B' then 'Gateway'
				when 'C' then 'Gateway'
				when 'D' then 'MR'
		end		as Stb_type
		,ground.dk_serial_number
		,case when ref_conv.x is not null then 1 else 0 end as conv_flag
		,ground.gn_lvl2_session
		,ground.gn_lvl2_session_grain
		,ground.search_section
		,sum(case when ground.dk_trigger_id <> 'system-' then 1 else 0 end)			as nclicks
		,sum(case when dk_Action_id = 01605 then 1 else 0 end)						as n_typing
		,sum(case when dk_action_id = 01400 then 1 else 0 end)						as n_nav_clicks
		,sum(case when dk_action_id = 02400 then 1 else 0 end)						as n_downloads
		,sum(case when dk_action_id = 03000 then 1 else 0 end)						as n_playbacks
		,sum(case when dk_Action_id in (02000,02010,02002,02005) then 1 else 0 end)	as n_bookings
		,sum(ss_elapsed_next_action)												as length_secs
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and ground.INDEX_ < ref_conv.x then ground.SS_ELAPSED_NEXT_ACTION else null end)	as secs_to_conv
		,max(case when ref_conv.gn_lvl2_session_grain is not null and ground.index_ = ref_conv.eoj then dk_previous else null end)					as End_of_search
		,max(case when ref_conv.gn_lvl2_session_grain is not null and ground.index_ = ref_conv.eoj then dk_trigger_id else null end)				as End_reason
		,sum(case when ref_conv.gn_lvl2_session_grain is not null and ground.INDEX_ < ref_conv.y then ground.SS_ELAPSED_NEXT_ACTION else null end)	as secs_to_suggest
--into	z_search_journeys
from 	z_search_trans	as ground
		left join ref_conv
		on	ground.date_					= ref_conv.date_
		and	ground.dk_serial_number			= ref_conv.dk_serial_number
		and	ground.gn_lvl2_session_grain	= ref_conv.gn_lvl2_session_grain
group	by	ground.date_
			,Stb_type
			,ground.dk_serial_number
			,conv_flag
			,ground.gn_lvl2_session
			,ground.gn_lvl2_session_grain
			,ground.search_section;
commit;


-- 12 - Quartiles for Boxes getting the best of search

/*
	the intention here is to split the sample into Quartiles given how effective is search for them.
	
	Effective defined as:
		+ Conversion Rate 											[Conv_rate]
		+ Ratio of Searches per day 								[search_daily_ratio]
		+ Return to Search Frequency (How many days using search) 	[search_return_rate]
	
*/

--drop table z_search_tiles;commit;
--truncate table z_search_tiles;commit;
--
--insert	into	z_search_tiles
with 	ref as	(
					select	base.dk_serial_number
							,count(distinct date_)	as ndates
					from 	z_pa_events_fact_v2	as base
							inner join	(
											select	distinct dk_serial_number
											from	z_search_trans
										)	as ref
							on	base.dk_serial_number	= ref.dk_serial_number
					where	date_ between '2016-10-01' and '2016-11-30'
					group	by	base.dk_Serial_number
				)
		,base as	(
						select	target.dk_serial_number
								,count(distinct target.date_||'-'||target.gn_lvl2_Session_grain)	as nsearches
								,count(distinct	(
													case 	when target.dk_Action_id in(02400,03000,00001,02000,02010,02002,02005) then target.date_||'-'||target.gn_lvl2_Session_grain
															else null
													end
												))	as nconv_searches
								,count(distinct target.date_)	as ndays_searching
								,max(ref.ndates)				as ndays_active
								,cast(nconv_searches as float) / cast(nsearches as float)		as conv_rate
								,cast(nsearches as float) / cast(ndays_searching as float) 		as search_daily_ratio
								,cast(ndays_searching as float) / cast(ndays_active as float)	as search_return_rate
						from	z_search_trans	as target
								inner join ref
								on	target.dk_serial_number	= ref.dk_serial_number
						group	by	target.dk_serial_number
					)
select	dk_Serial_number
		,conv_rate
		,search_return_rate
		,search_daily_ratio
		,ntile(4) over	(
							order by	search_return_rate	desc
										,conv_rate 			desc
										,search_daily_ratio desc
						)	as qtiles
		,ndays_searching
		,ndays_active
into	z_search_tiles
from	base;

commit;

------------------------------
-- C - Data Analysis (Queries)
------------------------------

-- 10 - Ground Analysis (Basic understanding of Search Performance)

-- I
select	extract(month from date_)	as the_month
		,stb_type
		,conv_flag
		,case when n_typing > 0 then 'Input' else 'No Input' end					as Searching_flag
		,count(distinct dk_serial_number)											as reach
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain) 	as njourneys
		,sum(nclicks)																as t_clicks
		,sum(n_nav_clicks) 															as t_nav_clicks
		,sum(n_downloads)															as t_downloads
		,sum(n_playbacks)															as t_playbacks
		,sum(n_bookings)															as t_bookings
		,sum(n_typing)																as t_typing
		,sum(coalesce(secs_to_conv,length_secs))									as time_to_
from	(
			select	date_
					,dk_serial_number
					,stb_type
					,conv_flag
					,gn_lvl2_session_Grain
					,sum(n_typing)		as n_typing
					,sum(nclicks)		as nclicks
					,sum(secs_to_conv)	as secs_to_conv
					,sum(length_secs)	as length_secs
					,sum(n_downloads)	as n_downloads
					,sum(n_playbacks)	as n_playbacks
					,sum(n_bookings)	as n_bookings
					,sum(n_nav_clicks)	as n_nav_clicks
			from	z_search_journeys
			group	by	1,2,3,4,5
		)	as base
group	by	the_month
			,stb_type
			,conv_flag
			,Searching_flag

-- II
select	extract(month from date_)	as the_month
		,stb_type
		,count(distinct dk_serial_number)	as nboxes
		,count(distinct (case when secs_to_conv is not null then dk_serial_number else null end)) as nconv_boxes
from	z_search_journeys
group	by	the_month
			,stb_type

-- III
select	extract(month from date_)	as the_month
		,case	substr(dk_Serial_number,3,1)
				when 'B' then 'Gateway'
				when 'C' then 'Gateway'
				when 'D' then 'MR'
				else 'unknown'
		end		as stb_type
		,count(distinct dk_serial_number)	as month_boxes
from	z_pa_events_fact_v2
where	date_ between '2016-10-01' and '2016-11-30'
group	by	the_month
			,stb_type

-- IV
select	extract(month from date_)	as the_month
		,stb_type
		,conv_flag
		,case when n_typing > 0 then 'Input' else 'No Input' end					as Searching_flag
		,coalesce(secs_to_conv,length_secs)	as time_to_
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)	as njourneys
from	(
			select	date_
					,dk_serial_number
					,stb_type
					,conv_flag
					,gn_lvl2_session_Grain
					,sum(n_typing)		as n_typing
					,sum(nclicks)		as nclicks
					,sum(secs_to_conv)	as secs_to_conv
					,sum(length_secs)	as length_secs
			from	z_search_journeys
			group	by	1,2,3,4,5
		)	as base
group	by	the_month
			,stb_type
			,conv_flag
			,Searching_flag
			,time_to_


-- V
select	extract(month from date_)	as the_month
		,stb_type
		,conv_flag
		,case when n_typing > 0 then 'Input' else 'No Input' end					as Searching_flag
		,nclicks
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)	as njourneys
from	(
			select	date_
					,dk_serial_number
					,stb_type
					,conv_flag
					,gn_lvl2_session_Grain
					,sum(n_typing)	as n_typing
					,sum(nclicks)	as nclicks
			from	z_search_journeys
			group	by	1,2,3,4,5
		)	as base
group	by	the_month
			,stb_type
			,conv_flag
			,Searching_flag
			,nclicks

			
-- VI
select	*
from	(
			select	*
					,sum(the_prop) over(partition by conv_flag order by nsearches desc rows between unbounded preceding and current row) as cum
			from	(
						select	conv_flag
								,substr(search_section,1,instr(search_section,'-')-1)	as section
								,end_of_search
								,end_reason
								,count(distinct date_||'-'||dk_Serial_number||'-'||gn_lvl2_session_grain) as nsearches
								,cast(nsearches as float) / cast((sum(nsearches)over(partition by conv_flag)) as float)	as the_prop
						from 	z_search_journeys
						where	end_of_search is not null
						and		search_section is not null
						group	by	conv_flag
									,section
									,end_of_search
									,end_reason
					)	as base
		)	as final_
where	cum <= 0.91

-- 5 - Phase 1: Search Performance (High-level view)

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
		inner join z_pa_kpi_def_qtiles as qtiles
		on	base.the_month			= qtiles.the_month
		and	base.dk_Serial_number	= qtiles.dk_serial_number
group	by	base.the_month
			,base.Sky_Q_Feature
			,qtiles.qtile
			,base.stb_type
			
			
-- 6 - Phase 1: Complement, performance of 5 given cohorts (new joiners, Exp Q users)

-- 7 - Phase 2: Exit mechanism while on Programme Detail Page & Search re-start mechanism while on Search Page
select	*
from	(
			select	*
					,sum(the_prop) over(partition by conv_flag order by nsearches desc rows between unbounded preceding and current row) as cum
			from	(
						select	conv_flag
								,substr(search_section,1,instr(search_section,'-')-1)	as section
								,end_of_search
								,end_reason
								,count(distinct date_||'-'||dk_Serial_number||'-'||gn_lvl2_session_grain) as nsearches
								,cast(nsearches as float) / cast((sum(nsearches)over(partition by conv_flag)) as float)	as the_prop
						from 	z_search_journeys
						where	end_of_search is not null
						and		search_section is not null
						group	by	conv_flag
									,section
									,end_of_search
									,end_reason
					)	as base
		)	as final_
where	cum <= 0.91



-- 9 - Phase 4: On Converted Journeys, what type of content users look for (VOD, Linear, etc.)



-- 13 - Search Distances

with	base as		(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
								,min(index_)																					as seq
								,min(dt)																						as start_
								,max(dt)																						as end_
								,sum(ss_elapsed_next_action)																	as time_spent
						from	z_pa_events_fact_v2
						where	date_ between '2016-10-01' and '2016-11-30'
						--and		dk_serial_number = '32D0030487556346'
						and		gn_lvl2_session <> 'Home'
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
					)
		,etl1 as	(
						select	*
								,max(gn_lvl2_session) over	(
																partition by	dk_serial_number
																order by		start_
																rows between 	1 following and 1 following
															)	as next_section
								
						from	base
					)
		,etl2 as	(
						select	*
								,max(start_) over	(
														partition by	dk_serial_number
														order by		start_
														rows between	1 following and 1 following
													)	as next_start
								,extract (epoch from next_start - start_)	as search_distance
						from	etl1
						where	gn_lvl2_session = 'Search'
					)
select	conv_flag
		,search_distance
		,count(1)			as nsearches
from	etl2
group	by	conv_flag
			,search_distance

-- 14 - Search (where to Next)


with	base as		(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
								,min(index_)																					as seq
								,min(dt)																						as start_
								,max(dt)																						as end_
								,sum(ss_elapsed_next_action)																	as time_spent
						from	z_pa_events_fact_v2
						where	date_ between '2016-10-01' and '2016-11-30'
						--and		dk_serial_number = '32D0030487556346'
						and		gn_lvl2_session <> 'Home'
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
					)
		,etl1 as	(
						select	*
								,max(gn_lvl2_session) over	(
																partition by	dk_serial_number
																order by		start_
																rows between 	1 following and 1 following
															)	as next_section
								
						from	base
					)
		,etl2 as	(
						select	*
								,max(start_) over	(
														partition by	dk_serial_number
														order by		start_
														rows between	1 following and 1 following
													)	as next_start
								,extract (epoch from next_start - start_)	as search_distance
						from	etl1
						where	gn_lvl2_session = 'Search'
					)
select	next_section
		,count(1)			as nsearches
from	etl2
where	search_distance <60
and		conv_flag = 0
group	by	next_section


-- 15 - Where do people find value after not converting in search within the next 60 Secs...

with	base as		(
						select	date_
								,dk_serial_number
								,gn_lvl2_session
								,gn_lvl2_session_grain
								,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
								,min(index_)																					as seq
								,min(dt)																						as start_
								,max(dt)																						as end_
								,sum(ss_elapsed_next_action)																	as time_spent
						from	z_pa_events_fact_v2
						where	date_ between '2016-10-01' and '2016-11-30'
						and 	gn_lvl2_session not in ('Fullscreen','Home','Vevo Menu','My Account','Settings')
						and		lower(gn_lvl2_session) not like '%app%'
						group	by	date_
									,dk_serial_number
									,gn_lvl2_session
									,gn_lvl2_session_grain
					)
		,etl1 as	(
						select	*
								,max(gn_lvl2_session) over	(
																partition by	dk_serial_number
																order by		start_
																rows between 	1 following and 1 following
															)	as next_section
								,max(conv_flag) over	(
															partition by	dk_serial_number
															order by		start_
															rows between	1 following and 1 following
														)	as next_section_conv_flag
								,max(start_) over	(
														partition by 	dk_Serial_number
														order by		start_
														rows between 	1 following and 1 following
													)	as next_section_start_
								,extract(epoch from next_section_start_ - end_)	as time_distance
						from	base
					)
select	gn_lvl2_session	as from_
		,conv_flag
		,next_section
		,next_section_conv_flag
		,count(1)	as freq
from	etl1
where	gn_lvl2_session = 'Search'
and		conv_flag = 0
and		time_distance <60
and		( next_section_conv_flag = 1 or next_section = 'Stand By' )
group	by	from_
			,conv_flag
			,next_section
			,next_section_conv_flag
			


-- 16 - Searches by Remote Type

with	coh as		(
						select	distinct
								dk_serial_number
						from	z_pa_Events_fact
						where	date_ between '2016-10-01' and '2016-10-31'
						and		gn_lvl2_session = 'Search'
					)
		,base as	(
						select	x.*
						from	z_search_journeys	as x
								inner join coh
								on	x.dk_serial_number = coh.dk_serial_number
						where	x.date_ between '2016-10-01' and '2017-03-31'
					)
		,ref_rem as	(	
						select	x.date_
								,x.dk_serial_number
								,x.gn_lvl2_session_grain
								,count(distinct x.remote_type)	as nremotes
						from	z_pa_events_fact	as x
								inner join base
								on	x.date_					= base.date_
								and	x.dk_serial_number		= base.dk_Serial_number
								and	x.gn_lvl2_session_grain	= base.gn_lvl2_session_grain
						where	x.date_ between '2016-10-01' and '2017-03-31'
						and		x.remote_type in ('IR','BT')
						group	by	x.date_
									,x.dk_serial_number
									,x.gn_lvl2_session_grain
					)
select	extract(month from base.date_)																												as the_month
		,case when ref_rem.nremotes > 1 then 'Both' else ref.remote_type end 																		as remotes_
		,count(distinct base.dk_serial_number) 																										as nstbs_searching
		,count(distinct base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_grain)													as nsearches
		,count(distinct case when base.conv_flag = 1 then base.date_||'-'||base.dk_serial_number||'-'||base.gn_lvl2_session_Grain else null end)	as nconv_searches
from	base
		inner join z_pa_events_fact as ref
		on	base.date_					= ref.date_
		and	base.dk_serial_number		= ref.dk_Serial_number
		and	base.gn_lvl2_session_grain	= ref.gn_lvl2_session_grain
		inner join ref_rem
		on	base.date_					= ref_rem.date_
		and	base.dk_serial_number		= ref_rem.dk_Serial_number
		and	base.gn_lvl2_session_grain	= ref_rem.gn_lvl2_session_grain
where	base.date_ between '2016-10-01' and '2017-03-31'
group	by	the_month
			,remotes_