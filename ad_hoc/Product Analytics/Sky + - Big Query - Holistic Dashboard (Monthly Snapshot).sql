

-- Final query
select  base.the_month
		,case 	when	base.sky_plus_session = 'Catch Up TV' then 'Catch Up'
				else	base.sky_plus_session 
		end		as sky_plus_tlms
		,max(ref_act_base.nactive_boxes)	as active_boxes
        ,count	(distinct	concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))	as njourneys
        ,count(distinct (
                          case  when	(
											(lower(base.screen) like '/tv/%' and base.sky_plus_session = 'TV Guide')	or
											lower(base.screen) like '/tv/live/%' 								        or
											lower(base.screen) like '/playback/%' 								        or
											lower(base.eventlabel) like '%not_booked%'									or
											(action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%')
										) 	then concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain))
                                else null
                          end
                        ))  as njourneys_converted
        ,count(distinct base.viewing_card) as reach
        ,count(distinct (
                              case  when	(
												(lower(base.screen) like '/tv/%' and base.sky_plus_session = 'TV Guide')	or
												lower(base.screen) like '/tv/live/%' 								     	or
												lower(base.screen) like '/playback/%' 							            or
												lower(base.eventlabel) like '%not_booked%'									or
												(action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%')
											) then base.viewing_card
                                    else null
                              end
						))	as conversion_reach
        ,sum(base.secs_to_next_action )	as n_secs_in_session
        ,sum(
				case	when	integer(concat(string(base.sessionid),string(base.actions_sequence)))<=ref.conv_flag then base.secs_to_next_action 
						else 	null 
				end
			)	as n_secs_to_conv
from	(
			/*
				Base data
			*/
			select	*
					,strftime_utc_usec(timestamp(thedate),"%Y-%m")	as the_month
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-11-30')) --> Parameter
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
											--,'Search'
										)
		)	as base
		left join	(
						/*
							Reference for Conversions:
							
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
															(lower(screen) like '/tv/%' and sky_plus_session = 'TV Guide')			or
															lower(screen) like '/tv/live/%' 										or
															lower(screen) like '/playback/%' 										or
															lower(eventlabel) like '%not_booked%'									or
															(action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%')
														)	then integer(concat(string(sessionid),string(actions_sequence)))
												else null
										end 
									)	as conv_flag
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-11-30')) --> Parameter
						group	by 	thedate
									,viewing_card
									,sky_plus_session_grain
					)	as ref
		on  base.thedate 				= ref.thedate
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
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-11-30')) --> Parameter
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
		inner join	(
						/*
							Determining total number of active STBs returning data on monthly basis
						*/
						select	strftime_utc_usec(timestamp(thedate),"%Y-%m")	as the_month
								,count(distinct viewing_card)					as nactive_boxes
						where	sky_plus_session in	(
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-11-30')) --> Parameter
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
														--,'Search'
													)
						group	by	1
					)	as ref_act_base
		on	base.the_month	= ref_act_base.the_month
group	by	base.the_month
			,sky_plus_tlms