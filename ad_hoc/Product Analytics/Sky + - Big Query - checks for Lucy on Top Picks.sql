select	base.thedate
		,base.A_B
		,count(distinct base.viewing_Card) as nvcs
from    (
			/*
				Creating a subset of records from the base tables for only STBs with R11 and for a
				specific set of sessions...
			*/
			select  *
					,case	when software_version like '%.64.00%' then 'R11' 
							else 'Other' 
					end 	as A_B
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-09-01'),timestamp('2016-11-23'))
			--where	software_version like 'R11%'
        )	as base
		INNER JOIN	PanelManagement.R11_1_AB_Test_Panels	as PM
		on	base.viewing_card = PM.vcid
group	by	base.thedate
			,base.A_B
			
			
			
			
--

select	ndays
		,count(distinct viewing_card)	as ncards
from	(
			select	base.viewing_Card
					,count(distinct (case when base.sky_plus_session = 'Top Picks' then base.thedate else null end))	as ndays
			from    (
						/*
							Creating a subset of records from the base tables for only STBs with R11 and for a
							specific set of sessions...
						*/
						select  *
								,case	when software_version like '%.64.00%' then 'R11' 
										else 'Other' 
								end 	as A_B
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-01'),timestamp('2016-11-23'))
						where	software_version like 'R11%'
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
														--,'Search'
													)
					)	as base
					INNER JOIN	PanelManagement.R11_1_AB_Test_Panels	as PM
					on	base.viewing_card = PM.vcid
					inner join	(
									select	case	when software_version like '%.64.00%' then 'R11' 
													else 'Other' 
											end 	as A_B
											,count(distinct x.viewing_card)	as n_stbs_pop
									from	(
												select	thedate
														,viewing_card
														,software_version
												from    table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-01'),timestamp('2016-11-23')) as base
												group	by	thedate
															,viewing_card
															,software_version
											)	as x
											INNER JOIN	PanelManagement.R11_1_AB_Test_Panels	as y
											on	x.viewing_card = y.vcid
									group	by	A_B
								)	as pop_ref
					on	base.A_B = pop_ref.A_B
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
														,min(sky_plus_session_grain) over (
																							PARTITION BY  thedate
																										  ,viewing_card
																							ORDER BY      start_
																							rows between  1 preceding and 1 preceding
																						  ) as origin
												from    (
															SELECT  thedate
																	,viewing_card
																	,sky_plus_session
																	,sky_plus_session_grain
																	,min(integer(concat(string(sessionid),string(actions_sequence)))) as start_
															from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-01'),timestamp('2016-11-23'))
															where 	software_version like 'R11%'
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
																	--,'Search'
																)
								)	as ref_home_start
					on	base.thedate				= ref_home_start.thedate
					and	base.viewing_card			= ref_home_start.viewing_card
					and	base.sky_plus_session_grain	= ref_home_start.sky_plus_session_grain
			where	base.A_B = 'R11'
			and   PM.panel_ref = 'Panel_A'
			group	by	1
		)	as final
group	by	ndays