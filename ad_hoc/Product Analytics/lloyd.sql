
-- The list of Viewing cards we want to look at...

select	vcid
		,panel
--into	Q_PA_Stage.z_r12
from	(
			select	base.vcid	as vcid
					,base.panel	as panel
			from	(
						SELECT	vcid
								,panel
						from 	PanelManagement.R12H_Panels 
						where 	(
									panel = 'A' and label = 'Standard Customer' 
									or
									Panel in ('B','C') -- D Group is the control group hence not reporting layout (The_layout = null)
								)
						and   	dupe = 'Valid'
						group	by 1,2
					)	as base
					inner join	(
									select	vcid
											,the_layout
									FROM	FLATTEN	(
														(
															SELECT	MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN HITS	AS VCID
																	,MAX(IF(hits.customDimensions.index=24, hits.customDimensions.value, NULL)) WITHIN HITS	AS the_layout
																	,TIMESTAMP(INTEGER(visitStartTime*1000000 + hits.time*1000))							AS HitTime
																	,hits.appInfo.appVersion 																AS software_version
															FROM	TABLE_DATE_RANGE([78413818.ga_sessions_],TIMESTAMP('2017-02-09'),  TIMESTAMP('2017-02-17')) -- < PARAMETER
														)
														,VCID
													)
									where  	hitTime between TIMESTAMP('2017-02-09') and TIMESTAMP('2017-02-17') -- < PARAMETER
									group by 1,2
								) 	as ref
					on	base.vcid	= ref.vcid
					and	base.panel	= ref.the_layout
		)
		,(
			SELECT	vcid
					,panel
			from 	PanelManagement.R12H_Panels 
			where 	Panel = 'D'
			and   	dupe = 'Valid'
			group	by 1,2
		)
		
-- Checking how much we end up loosing...
select	base.panel
		,count(distinct base.vcid) as nboxes
from	(
			SELECT	vcid
					,panel
			from 	PanelManagement.R12H_Panels 
			where 	(
						panel = 'A' and label = 'Standard Customer' 
						or
						Panel in ('B','C','D')
					)
			and   	dupe = 'Valid'
			group	by 1,2
		)	as base
		inner join	(
						select	vcid
								,the_layout
						FROM	FLATTEN	(
											(
												SELECT	MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN HITS	AS VCID
														,MAX(IF(hits.customDimensions.index=24, hits.customDimensions.value, NULL)) WITHIN HITS	AS the_layout
														,TIMESTAMP(INTEGER(visitStartTime*1000000 + hits.time*1000))							AS HitTime
														,hits.appInfo.appVersion 																AS software_version
												FROM	TABLE_DATE_RANGE([78413818.ga_sessions_],TIMESTAMP('2017-02-09'),   TIMESTAMP('2017-02-17')) -- < PARAMETER
											)
											,VCID
										)
						where  	hitTime between TIMESTAMP('2017-02-09') and TIMESTAMP('2017-02-17') -- < PARAMETER
						group by 1,2
					) 	as ref
		on	base.vcid	= ref.vcid
		and	base.panel	= ref.the_layout
group	by	base.panel


--

-- Final query
select  base.panel
		,case 	when	base.sky_plus_session = 'Catch Up TV' then 'Catch Up'
				else	base.sky_plus_session 
		end		as gn_lvl2_session
        ,count	(distinct	concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))	as njourneys
        ,count(distinct (
                          case  when  (
										(lower(base.screen) like '/tv/%' and base.sky_plus_session = 'TV Guide')	or
                                        lower(base.screen) like '/tv/live/%' 								                      		or
                                        lower(base.screen) like '/playback/%' 								                    		or
                                        lower(base.eventlabel) like '%not_booked%'
                                      ) then concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain))
                                else null
                          end
                        ))  as njourneys_converted
		--,max(pop_ref.n_stbs_pop)					as tot_stb_pop
        ,count(distinct base.viewing_card) as reach
        ,count(distinct (
                              case  when  (
                                            (lower(base.screen) like '/tv/%' and base.sky_plus_session = 'TV Guide')	or
                                            lower(base.screen) like '/tv/live/%' 								     	or
                                            lower(base.screen) like '/playback/%' 							            or
                                            --lower(base.eventlabel) like '%not_booked%'
                                          ) then base.viewing_card
                                    else null
                              end
						))	as conversion_reach
        ,sum(base.secs_to_next_action )	as n_secs_in_session
--        ,sum	(
					--case	when	integer(concat(string(base.sessionid),string(base.actions_sequence)))<=ref.conv_flag then base.secs_to_next_action 
							--else 	null 
					--end
				--)	as n_secs_to_conv
from    (
			/*
				Creating a subset of records from the base tables for only STBs with R11 and for a
				specific set of sessions...
			*/
			select  a.*
					,b.panel
					--,integer(15+ceil((datediff(timestamp(thedate),timestamp('2016-10-14'))+1)/7)) as Sky_week
			from	(select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-24'),timestamp('2017-01-30'))) as a
					inner join	Q_PA_Stage.z_r12 as b
					on	a.viewing_card = b.vcid
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
		--inner join	(
						--select  integer(15+ceil((datediff(timestamp(thedate),timestamp('2016-10-14'))+1)/7)) as Sky_week
								--,count(distinct viewing_card) as n_stbs_pop
						--from    table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-24'),timestamp('2017-01-30'))
						--group   by  Sky_week
					--)	as pop_ref
		--on	base.Sky_week = pop_ref.Sky_week
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
															(lower(screen) like '/tv/%' and sky_plus_session = 'TV Guide')	or
															lower(screen) like '/tv/live/%' 								or
															lower(screen) like '/playback/%' 								or
															lower(eventlabel) like '%not_booked%'
														)	then integer(concat(string(sessionid),string(actions_sequence)))
												else null
										end 
									)	as conv_flag
						from	(select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-24'),timestamp('2017-01-30'))) as a
								inner join Q_PA_Stage.z_r12 as b
								on a.viewing_card = b.vcid
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
													from	(select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-24'),timestamp('2017-01-30'))) as a
															inner join Q_PA_Stage.z_r12 as b
															on	a.viewing_card = b.vcid
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
group   by	base.panel
			--,base.A_B
			,gn_lvl2_session
