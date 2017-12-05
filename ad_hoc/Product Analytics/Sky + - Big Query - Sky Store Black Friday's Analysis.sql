
-- Final query
select  base.thedate
		,base.sky_plus_session
        ,count	(distinct	concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))	as njourneys
        ,count(distinct base.viewing_card) 																				as reach
		,count	(distinct	(
								case 	when base.slots_ is not null then (concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))
										else null
								end	
							)
				)	as njourneys_slots
		,count	(distinct	(
								case 	when base.slots_ is not null then base.viewing_card
										else null
								end
							)
				)	as reach_slots
from    (
			/*
				Creating a subset of records from the base tables for only STBs with R11 and for a
				specific set of sessions...
			*/
			select  *
					,case 	when  	hour(timestamp(timestamp_)) between 7 and 11 and date(timestamp_) in (date('2016-11-18'),date('2016-11-25'))	then 'f slot'
							when	hour(timestamp(timestamp_)) = 10 and date(timestamp_) in (date('2016-11-19'),date('2016-11-26')) 	then 's slot'
							else	null
					end		as slots_
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-08-18'),timestamp('2016-11-26'))
			where	(software_version like 'R11%' or software_version like 'R12%')
			and	   sky_plus_session in	(
											'TV Guide'
											,'Catch Up TV'
											,'Recordings'
											,'Top Picks'
											,'Sky Box Sets'
											,'Sky Cinema'
											,'Sky Store'
											,'Sports'
											,'Kids'
											,'Music'
											,'Online Videos'
										)
        )	as base
		inner join	(
						/*
							Identifying only sessions that began at home...
						*/
						select  *
						from    (
									select  thedate
											,viewing_card
											,sky_plus_session
											,sky_plus_session_grain
											,min(sky_plus_session_grain) over	(
																					PARTITION BY  thedate
																								  ,viewing_card
																					ORDER BY      start_
																					rows between  1 preceding and 1 preceding
																				)	as origin
									from    (
												SELECT  thedate
														,viewing_card
														,sky_plus_session
														,sky_plus_session_grain
														,min(integer(concat(string(sessionid),string(actions_sequence)))) as start_
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-08-18'),timestamp('2016-11-26'))
												where 	(software_version like 'R11%' or software_version like 'R12%')
												group   by  thedate
															,viewing_card
															,sky_plus_session
															,sky_plus_session_grain
											)   as base
								)   as base2
						where   lower(origin) like 'home%'
						and     sky_plus_session in (
														'TV Guide'
														,'Catch Up TV'
														,'Recordings'
														,'Top Picks'
														,'Sky Box Sets'
														,'Sky Cinema'
														,'Sky Store'
														,'Sports'
														,'Kids'
														,'Music'
														,'Online Videos'
													)
					)	as ref_home_start
		on	base.thedate				= ref_home_start.thedate
		and	base.viewing_card			= ref_home_start.viewing_card
		and	base.sky_plus_session_grain	= ref_home_start.sky_plus_session_grain
where	base.sky_plus_session = 'Top Picks'
and		base.thedate in (date('2016-11-25'),date('2016-11-26'),date('2016-11-18'),date('2016-11-19'))
group   by	base.thedate
			,base.sky_plus_session
			
			
			
			
------------------------------------------------------------------
-- A02 - Sky Cinema entries from different areas of the UI [PROXY] 
------------------------------------------------------------------

select 	base.thedate
		,base.sky_plus_session
        ,count	(distinct	concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))	as njourneys
        ,count(distinct base.viewing_card) 																				as reach
		,count	(distinct	(
								case 	when base.slots_ is not null then (concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))
										else null
								end	
							)
				)	as njourneys_slots
		,count	(distinct	(
								case 	when base.slots_ is not null then base.viewing_card
										else null
								end
							)
				)	as reach_slots
from	(
			-- extracting data from table data range function and trimming to only what needed...
			select  *
					,case 	when  	hour(timestamp(timestamp_)) between 7 and 11 and date(timestamp_) in (date('2016-11-18'),date('2016-11-25'))	then 'f slot'
							when	hour(timestamp(timestamp_)) = 10 and date(timestamp_) in (date('2016-11-19'),date('2016-11-26')) 	then 's slot'
							else	null
					end		as slots_
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-08-18'),timestamp('2016-11-26'))
			where	(software_version like 'R11%' or software_version like 'R12%')
			and		sky_plus_session in	(
											'TV Guide'
											,'Catch Up TV'
											,'Recordings'
											,'Top Picks'
											,'Sky Box Sets'
											,'Sky Cinema'
											,'Sky Store'
											,'Sports'
											,'Kids'
											,'Music'
											,'Online Videos'
										)
		)	as base 
		inner join	(
						-- Identifying only sessions that began at home...
						select  *
						from    (
									select  thedate
											,viewing_card
											,sky_plus_session
											,sky_plus_session_grain
											,min(sky_plus_session_grain) over	(
																					PARTITION BY 	thedate
																									,viewing_card
																					ORDER BY      	start_
																					rows between  	1 preceding and 1 preceding
																				) 	as origin
									from    (
												SELECT  thedate
														,viewing_card
														,sky_plus_session
														,sky_plus_session_grain
														,min(integer(concat(string(sessionid),string(actions_sequence)))) as start_
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-08-18'),timestamp('2016-11-26'))
												where 	(software_version like 'R11%' or software_version like 'R12%')
												group   by  thedate
															,viewing_card
															,sky_plus_session
															,sky_plus_session_grain
											)   as base
								)   as base2
						where   lower(origin) like 'home%'
						and     sky_plus_session in	(
														'TV Guide'
														,'Catch Up TV'
														,'Recordings'
														,'Top Picks'
														,'Sky Box Sets'
														,'Sky Cinema'
														,'Sky Store'
														,'Sports'
														,'Kids'
														,'Music'
														,'Online Videos'
													)
					)	as ref_home_start
		on	base.thedate				= ref_home_start.thedate
		and	base.viewing_card			= ref_home_start.viewing_card
		and	base.sky_plus_session_grain	= ref_home_start.sky_plus_session_grain -- 454,406,485
where	base.thedate in (date('2016-11-25'),date('2016-11-26'),date('2016-11-18'),date('2016-11-19'))
and		(
			-- Identifying sessions that began from home and went into Sky Store through TLM/SLMs...
			lower(base.screen) like '%sky%store%'			
			or
			-- Navigation into Sky Store through Top Picks' mosaic from Home page...
			(
				base.action_category = 'HomePageLinkJump' 
				and	(
						lower(base.action) 		like '%MFCG%'
						OR lower(base.action)	like '%featured%'
						OR lower(base.action) 	like '%new to rent%'
						OR lower(base.action) 	like '%new to buy%'
						OR lower(base.action)	like '%black friday%'
					)
			)
		)
group   by	base.thedate
			,base.sky_plus_session
