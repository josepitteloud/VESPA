

-- Final query
select  base.Sky_week
		,case 	when	base.sky_plus_session = 'Catch Up TV' then 'Catch Up'
				else	base.sky_plus_session 
		end		as gn_lvl2_session
        ,count	(distinct	concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))	as njourneys
        ,count(distinct (
							case  	when	(
												(lower(base.screen) like '/tv/%' and base.sky_plus_session = 'TV Guide')		or
												lower(base.screen) like '/tv/live/%' 								            or
												lower(base.screen) like '/playback/%' 								            or
												lower(base.eventlabel) like '%not_booked%'										or
												base.action_category = 'LinearAction' and base.action like 'LINEAR%_RECORD_%'	
											)	then concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain))
									else null
							 end
                        ))  as njourneys_converted
		,max(pop_ref.n_stbs_pop)			as tot_stb_pop
        ,count(distinct base.viewing_card)	as reach
        ,count(distinct (
							case  	when	(
												(lower(base.screen) like '/tv/%' and base.sky_plus_session = 'TV Guide')		or
												lower(base.screen) like '/tv/live/%' 								     		or
												lower(base.screen) like '/playback/%' 							            	or
												lower(base.eventlabel) like '%not_booked%'										or
												base.action_category = 'LinearAction' and base.action like 'LINEAR%_RECORD_%'
											) 	then base.viewing_card
                                    else null
                            end
						))	as conversion_reach
        ,sum(base.secs_to_next_action )	as n_secs_in_session
        ,sum	(
					case	when	integer(concat(string(base.sessionid),string(base.actions_sequence)))<=ref.conv_flag then base.secs_to_next_action 
							else 	null 
					end
				)	as n_secs_to_conv
from    (
			/*
				Creating a subset of records from the base tables for only STBs with R11 and for a
				specific set of sessions...
			*/
			select  *
					,integer(ceil((datediff(timestamp(thedate),timestamp('2017-01-02'))+1)/7)) as Sky_week
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-05-28'))
			where	sky_plus_session in	(
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
						-- Total Population Selected for Focus Groups...
						select  integer(ceil((datediff(timestamp(thedate),timestamp('2017-01-02'))+1)/7)) 	as Sky_week
								,count(distinct viewing_card) 												as n_stbs_pop
						from    table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-05-28'))
						group   by  Sky_week
					)	as pop_ref
		on	base.Sky_week = pop_ref.Sky_week
        LEFT JOIN	(
						/*
							Identifying the first action in the journey related to conversion. this will be use above
							to measure the length in seconds from the start of the each session that converted until 
							the this first action considered for conversion... resulting in the measure named
							"n_secs_to_conv"
						*/ 
						select  thedate
								,viewing_card
								,sky_plus_session_grain
								,min(
										case	when	(
															(lower(screen) like '/tv/%' and sky_plus_session = 'TV Guide')		or
															lower(screen) like '/tv/live/%' 									or
															lower(screen) like '/playback/%' 									or
															lower(eventlabel) like '%not_booked%'								or
															action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%'
														)	then integer(concat(string(sessionid),string(actions_sequence)))
												else null
										end 
									)	as conv_flag
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-05-28'))
						group	by 	thedate
									,viewing_card
									,sky_plus_session_grain
					)	as ref
          on  base.thedate 					= ref.thedate
          and base.viewing_card 			= ref.viewing_card
          and base.sky_plus_session_grain	= ref.sky_plus_session_grain
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
													from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-05-28'))
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
group   by	base.Sky_week
			,gn_lvl2_session