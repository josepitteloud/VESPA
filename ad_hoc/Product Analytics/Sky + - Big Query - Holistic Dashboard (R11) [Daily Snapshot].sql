
-- Final query
select  base.thedate
		,base.soft_version
		,case 	when	base.sky_plus_session = 'Catch Up TV' then 'Catch Up'
				else	base.sky_plus_session 
		end		as gn_lvl2_session
        ,count	(distinct	concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain)))	as njourneys
        ,count(distinct base.viewing_card) 																				as reach
from    (
			/*
				Creating a subset of records from the base tables for only STBs with R11 and for a
				specific set of sessions...
			*/
			select  *
					,case	when software_version like '%.65.00%' then 'R11.1' 
							when software_version like '%.64.00%' then 'R11' 
							else 'Other' 
					end 	as soft_version
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-08-17'),timestamp('2016-11-22'))
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
													from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-08-17'),timestamp('2016-11-22'))
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
														)
						)	as ref_home_start
		on	base.thedate				= ref_home_start.thedate
		and	base.viewing_card			= ref_home_start.viewing_card
		and	base.sky_plus_session_grain	= ref_home_start.sky_plus_session_grain
group   by	base.thedate
			,base.soft_version
			,gn_lvl2_session
