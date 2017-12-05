
------------------------------------------------------------------------
-- A01 - Sky Cinema entries from different areas of the UI
------------------------------------------------------------------------

select 	base.A_B
		,base.sky_plus_session
		,count(distinct concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain))) as njourneys
from	(
			-- extracting data from table data range function and trimming to only what needed...
			select  *
					,case	when software_version like '%.65.00%' then 'R11.1' 
							when software_version like '%.64.00%' then 'R11' 
							else 'Other' 
					end		as A_B
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-13'),timestamp('2016-11-06'))
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
											,'Search'
										)
		)	as base -- 454,406,485
		INNER JOIN	PanelManagement.R11_1_AB_Test_Panels	as PM
		on	base.viewing_card = PM.vcid
		left join	(
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
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-13'),timestamp('2016-11-06'))
												where 	software_version like 'R11%'
												group   by  thedate
															,viewing_card
															,sky_plus_session
															,sky_plus_session_grain
											)   as base
									)   as base2
						where   lower(origin) like 'home%'
						and     	sky_plus_session in	(
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
															,'Search'
														)
					)	as ref_home_start
		on	base.thedate				= ref_home_start.thedate
		and	base.viewing_card			= ref_home_start.viewing_card
		and	base.sky_plus_session_grain	= ref_home_start.sky_plus_session_grain -- 454,406,485
where	(
			-- Identifying sessions that began from home and went into Sky Store through TLM/SLMs...
			(ref_home_start.sky_plus_session_grain is not null and lower(base.screen) like '%sky%store%') 
			or
			-- Navigation into Sky Store through Top Picks' mosaic from Home page...
			(
				base.action_category = 'HomePageLinkJump' 
				and	(
						lower(base.action) like '%MFCG%'
						OR lower(base.action) like '%featured%'
						OR lower(base.action) like '%new to rent%'
						OR lower(base.action) like '%new to buy%'
					)
			)
		) -- 490,337
group	by	base.A_B
			,base.sky_plus_session


					

---------------------------------------------					
-- B01 - How I got to the conclusion
---------------------------------------------

----------------
-- SAMPLING
----------------

-- Identifying sessions with screen referring to Sky STORE

select  viewing_card
        ,sky_plus_session_grain
from    skyplus.skyplus_sessions_20161109
where   lower(screen) like '%/sky store%'
group   by  viewing_card
            ,sky_plus_session_grain
limit   100

/*

Row 	viewing_card 	sky_plus_session_grain 	 
1 			598007359		Sky Store-5 	 
2 			576357602		Sky Cinema-1 	 
3 			463091694		Sky Store-1 	 
4 			440506863		Sky Cinema-3 	 
5 			519413686		Sky Cinema-3 	 
6 			421863432		Sky Store-1 	 
7 			729177857		Sky Store-3 	 
8 			569181217		Sky Store-1 	 
9 			541331617		Sky Cinema-3 	 
36 	409128139	Sky Box Sets-1 	 
37 	418559522	Sky Box Sets-1 	 
40 	575410493	Mini Guide-6
54 	445033228	Mini Guide-271
59 	434656443	Mini Guide-118
*/



-- For cinema this patch looks ok but not for Top Picks... in a whole month (October) we only have 6 entries into Sky Store identified... this doesnt sound right

select	thedate
			,viewing_card
			,sky_plus_session_grain
from    table_Date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-10-31'))
where	sky_plus_session = 'Top Picks'
and     	lower(screen) like '%sky%store%'
group 	by	thedate
					,viewing_card
					,sky_plus_session_grain

/*
1 	2016-10-07	414734269	Top Picks-1 	 
2 	2016-10-22	467989661	Top Picks-2 	 
3 	2016-10-15	581974706	Top Picks-1 	 
4 	2016-10-19	454884511	Top Picks-3 	 
5 	2016-10-06	538201831	Top Picks-1 	 
6 	2016-10-05	487304792	Top Picks-1 	 
*/
-------------------------------------------------------------------------
-- Identifying sessions with screen referring to Sky STORE
-------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------
-- inspecting sessions to verify we are not chopping them with a Sky Store sessions when this reference occurs
---------------------------------------------------------------------------------------------------------------------------------------------


/*------------------------------------------------------------------------------------------------------
		if we are not, that means we can then use the '%sky%store%' wildcard on 
		journeys from Top Picks and Sky Cinema to find out who went into SKY STORE 
		through these mean
*/------------------------------------------------------------------------------------------------------




select  *
from    skyplus.skyplus_sessions_20161109
where   viewing_card = '576357602'
and     sky_plus_session_grain = 'Sky Cinema-3'
order   by sessionid, actions_sequence



select *
from    table_Date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-10-31'))
where	  sky_plus_session = 'Top Picks'
and     action_category = 'HomePageLinkJump'
and     lower(action) like '%store%'
limit   100




1 	2016-10-31	444879779	Home-15 	 
2 	2016-10-31	414494633	Home-5 	 
3 	2016-10-31	704734896	Home-5 	 
4 	2016-10-31	703930487	Home-3 	 
5 	2016-10-31	502019045	Home-4 	 
6 	2016-10-31	577853310	Home-10 	 
7 	2016-10-31	386545420	Home-27 	 
8 	2016-10-31	617498837	Home-5 	 
9 	2016-10-31	569998511	Home-10 	 
10 	2016-10-31	506364504	Home-40 	 
11 	2016-10-31	577523335	Home-12 	 
12 	2016-10-31	409674959	Home-45 	 
13 	2016-10-31	541154456	Home-213 	 
14 	2016-10-31	580571883	Home-16 	 
15 	2016-10-31	366688190	Home-17 	 
16 	2016-10-31	565276375	Home-4 	 
17 	2016-10-31	604631697	Home-9 	 
18 	2016-10-31	577835341	Home-3 	 
19 	2016-10-29	424789113	Home-10 	 





--	This is how we flag Sky Stores navigations via Top Picks... ALL OF THOSE FROM HOME
	
	
select  *
--from    table_Date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2016-10-31'))
from    skyplus.skyplus_sessions_20161011
where   action_category = 'HomePageLinkJump'
and     lower(action) like '%store%'
order   by  viewing_card
            ,sessionid
            ,actions_sequence
	
	
	
	
	
	
	
/*----------------------------------------
		output from verifying on A01
*/----------------------------------------
					
					
					
-- ALL
			
Row 	base_sky_plus_session 	njourneys 	 
1 		Sky Box Sets			6783 	 
2 		TV Guide				4 	 
3 		Top Picks				35807 	 
4 		Sky Store				69294 	 


Row 	base_A_B 	base_sky_plus_session 	njourneys 	 
1 		Other		Sky Store				10 	 
2 		Other		Top Picks				1 	 
3 		R11			Sky Box Sets			1853 	 
4 		R11			Sky Store				16656 	 
5 		R11			Top Picks				9793 	 
6 		R11.1		Sky Box Sets			1679 	 
7 		R11.1		Sky Store				21120 	 
8 		R11.1		TV Guide				2 	 
9 		R11.1		Top Picks				9083 	 