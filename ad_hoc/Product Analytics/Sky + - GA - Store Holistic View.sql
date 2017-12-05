


not yet possible to isolate store in top picks

"STORE:" tag in GA is misleading.

The proxy for the rest of the areas is doable though.





        
--------------------
-- EXPLORATION PHASE
--------------------

select 	viewing_card
			,count(distinct sky_plus_session_grain) as njourneys
			,count(distinct (case when lower(screen) like '%sky%store%' then sky_plus_session_grain else null end)) as withstore
from    skyplus.skyplus_sessions_20170208
where	sky_plus_session = 'Sky Cinema' -- or = 'Sky Box Sets'
group	by  viewing_card
order   by  withstore desc

-- but for Top Picks this is what works...

select	viewing_card
        ,count(distinct sky_plus_session_grain) as njourneys
        ,count(distinct (case when action_category = 'HomePageLinkJump' and  lower(action) like '%store%' then sky_plus_session_grain else null end)) as withstore
from    skyplus.skyplus_sessions_20170208
where	sky_plus_session = 'Top Picks'
group	  by	viewing_card
order   by withstore desc


/* -- SAMPLE

For Sky Cinema
	Row 	viewing_card 	njourneys 	withstore 	 
	1 			542010608		26				9 	 
	2 			541154431		23				8 	 
	3 			541155693		25				8 	 
	4 			542010012		24				8 	 
	5 			542009584		25				7 	 
	6 			541154456		25				7 	 
	7 			505293753		24				7 	 
	8 			460450935		6				6 	 
	9 			479871113		7				6 	 

For Sky Box Sets
	Row 	viewing_card 	njourneys 	withstore 	 
	1 		441240801			14			7 	 
	2 		505290015			52			4 	 
	3 		542010418			52			4 	 
	4 		505293894			51			4 	 
	5 		505293878			51			4 	 
	6 		562520957			5			4 	 
	7 		403076789			51			4 	 
	8 		505295303			52			4 	 
	9 		542010459			52			4 	 
	10 		388861247			4			4 	 
	11 		403080849			51			4 	 


For Top Picks (Might have to use another approach...)
	Row 	viewing_card 	njourneys 	withstore 	 
	1 		429057433			3			1


	Row 	viewing_card 	njourneys 	withstore 	 
	1 		505288548			15			11 	 
	2 		542009584			9			9 	 
	3 		541154456			9			9 	 
	4 		542010608			9			9 	 
	5 		505293753			9			9 	 
	6 		542010012			9			9 	 
	7 		541154431			9			9 	 
	8 		542010434			9			8 	 
	9 		541155693			9			8 	 



*/



-- Evidence of Sky Store Access from Sky Cinema...
select  *
from    skyplus.skyplus_sessions_20170208
where	sky_plus_session = 'Sky Cinema'
and   viewing_card = '460450935'
order by sessionid, actions_sequence



-- Evidence of Sky Store Access from Sky Box Sets...
select  *
from    skyplus.skyplus_sessions_20170208
where	sky_plus_session = 'Sky Box Sets'
and   viewing_card = '388861247'
order by sessionid, actions_sequence

-- Evidence of Sky Store Access from Top Picks...
select  *
from    skyplus.skyplus_sessions_20170208
where	sky_plus_session = 'Top Picks'
and   viewing_card = '505293753'
order by sessionid, actions_sequence


-----------------------------------------------------------------------
-- Defining the rules to isolate journeys into Sky Store via other TLMs
-----------------------------------------------------------------------

/*
	The intention here is to get journeys and conversion rates for these subsets of the journeys
*/

-- For Store...

--	That is journeys from the home page


-- For Top Picks...


where	sky_plus_session 	= 'Top Picks'
and		action_category 	= 'HomePageLinkJump' 
and  	lower(action) 		like 'store:%'



-- For Sky Box Sets...
-- For Sky Cinema
select	*
		,case	when lower(regexp_replace(screen,' ','')) like '%skystore%' then 1 else 0  end as x
		,'New Store'
from    skyplus.skyplus_sessions_20170208
where	sky_plus_session in ('Sky Cinema','Sky Box Sets','Top Picks')
and   	viewing_card in ('460450935','388861247','505293753')
and     (length(screen) - length(regexp_replace(screen,'/',''))) > 1


-----------------
-- Applied Theory
-----------------


--> step 1

/*
	Isolating Sky Plus sessions required for the exercise...
*/

select	base.*
		,integer(15+ceil((datediff(timestamp(base.thedate),timestamp('2016-10-14'))+1)/7)) 			as Sky_week
		,case	when lower(regexp_replace(base.screen,' ','')) like '%skystore%' then 1 else 0  end	as x
		,max([x]) over	(
							partition by	base.viewing_card
											,base.sky_plus_session
							order by 		base.sessionid
											,base.actions_sequence 
							rows between 	1 preceding and 1 preceding
						) 	as y
--into	z_holisticstore_YYYYMMDD
from    (select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-11'),timestamp('2016-11-11'))) as base --> Parameter!	
		left join	(	-- Extracting Journeys into Top Picks that landed in Sky Store (Sky Store tile in Top Picks)...
						select	thedate
								,viewing_card
								,sky_plus_session_grain
						from 	(
									select	*
											,case	when action contains 'row=3;column=6' then 'row=3;column=6'
													when action contains 'row=3;column=3' then 'row=3;column=3'
													when action contains 'row=3;column=4' then 'row=3;column=4'
													when action contains 'row=3;column=5' then 'row=3;column=5'
													when action contains 'row=3;column=1' then 'row=3;column=1'
													when action contains 'row=4;column=4' then 'row=4;column=4'
													else null
											end		thelinkage
									from 	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-11'),timestamp('2016-11-11')) --> Parameter
									where 	sky_plus_session = 'Top Picks'
									and		action_category = 'HomePageLinkJump'
								) 	as base
								inner join Q_PA_Stage.z_bypass as ref
								on	base.thelinkage	= ref.grid
						where 	action contains ref.grid
						and		base.timestamp_ between ref.effective_from and ref.effective_to
						group 	by	1,2,3
					)	as ref2
		on	base.thedate				= ref2.thedate
		and	base.viewing_card			= ref2.viewing_card
		and	base.sky_plus_session_grain	= ref2.sky_plus_session_grain
		left join	(	-- Extracting Journeys into Store from HomePage...
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
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-11'),timestamp('2016-11-11')) --> Parameter!
												group   by  thedate
															,viewing_card
															,sky_plus_session
															,sky_plus_session_grain
											)   as base
								)   as base2
						where   lower(origin) like 'home%'
						and     sky_plus_session in (
													  'Sky Store'
													)
					)	as ref
		on	base.thedate				= ref.thedate
		and	base.viewing_card			= ref.viewing_card
		and	base.sky_plus_session_grain	= ref.sky_plus_session_grain
where	(
			(
				base.sky_plus_session in ('Sky Cinema','Sky Box Sets')	-- Choosing Sky Cinema/Box Sets
				and (length(base.screen) - length(regexp_replace(base.screen,'/',''))) > 1
			)
			or	ref2.sky_plus_session_grain is not null 				-- Choosing Top Picks journeys
			or	ref.sky_plus_session_grain is not null					-- Choosing Sky Store journeys
		)
--and   	base.viewing_card in ('460450935','388861247','505293753')



--> step_2

/*
	Given the criteria to select Sky Cinema / Box Sets journeys that touched Store
	at any point in time and extracting those "Nested Sub-Journeys". The intention is
	to measure them in a similar fashion done for Home Page performance but in this case
	focused on Store Only.
*/

select 	*
--into	z_holisticstore_ref
from	(
			select	base_thedate
					,base_viewing_card
					--,integer (concat(string(base_sessionid),string(base_actions_sequence))) as z
					,base_session_id
					,base_actions_sequence
					,base_sky_plus_session
					,x
					,max([z]-1) over	(
											partition by	base_viewing_card 
															,base_sky_plus_session
											order by 		base_sessionid
															,base_actions_sequence
											rows between 	1 following and 1 following
										)  	as the_end
			--from	Q_PA_Stage.z_step_1
			from	table_date_range(Q_PA_Stage.z_holisticstore_,timestamp('2016-07-15'),timestamp('2017-02-09')) --> Parameter!
			where   x<>y
			and		base_sky_plus_session in ('Sky Cinema','Sky Box Sets')
		) 	as step1
where 	x = 1



--> Output Showcase...

select	base.*
		,ref.z
from	(select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2016-11-11'),timestamp('2016-11-11'))) as base --> Parameter!	-- Population of journeys for analysis
		left join Q_PA_Stage.z_holisticstore_ref	as ref 				-- for Sky Cinema / Box Sets
		on	base.base_thedate			= ref.base_thedate
		and	base.base_viewing_card		= ref.base_viewing_card
		and	base.base_sky_plus_session	= ref.base_sky_plus_session
where	(
			integer(concat(string(base.base_sessionid),string(base.base_actions_sequence))) between ref.z and ref.the_end	-- Filtering for Cinema/Box Sets
			or base.base_sky_plus_session in ('Sky Store','Top Picks')														-- Persisting with Store and Top Picks
		)
and		base.base_viewing_card in ('460450935','388861247','505293753')
order	by	base_sessionid
			,base_actions_sequence
			

--------------
-- Aggregating
--------------

select	base.the_new_week
		,base.base_sky_plus_session
		,count(distinct(concat(string(base.base_thedate),string(base.base_viewing_card),string(base.base_sky_plus_session_grain))))	as njourneys
		,count(distinct (
							case  	when  	(
												(lower(base.base_screen) like '/tv/%' and base.base_sky_plus_session = 'TV Guide')	or
												lower(base.base_screen) like '/tv/live/%' 								                      		or
												lower(base.base_screen) like '/playback/%' 								                    		or
												lower(base.base_eventlabel) like '%not_booked%'
											) 	then concat(string(base.base_thedate),string(base.base_viewing_card),string(base.base_sky_plus_session_grain))
									else null
							end
						))  as njourneys_converted
		,count(distinct(base.base_viewing_card))	as nboxes
		,sum((
				case  	when  	(
									lower(base.base_eventlabel) like '%not_booked%' and base.base_action = 'SELECT'
								) 	then 1
						else 0
				end
			))  as n_downloads
from	(	
			select	* 
					,case	when base_thedate between '2016-10-31' and '2016-11-06' then 18
							when base_thedate between '2016-11-07' and '2016-11-13' then 19
							when base_thedate between '2016-11-14' and '2016-11-20' then 20
							when base_thedate between '2016-11-21' and '2016-11-27' then 21
							when base_thedate between '2016-11-28' and '2016-12-04' then 22
							when base_thedate between '2016-12-05' and '2016-12-11' then 23
							when base_thedate between '2016-12-12' and '2016-12-18' then 24
							when base_thedate between '2016-12-19' and '2016-12-25' then 25
							when base_thedate between '2016-12-26' and '2016-12-28' then 26
							  --when base_thedate between '2016-12-26' and '2016-12-28' then 27
							when base_thedate between '2017-01-09' and '2017-01-15' then 28
							when base_thedate between '2017-01-16' and '2017-01-22' then 29
							when base_thedate between '2017-01-23' and '2017-01-29' then 30
							when base_thedate between '2017-01-30' and '2017-02-05' then 31
							when base_thedate between '2017-02-06' and '2017-02-12' then 32
					end 	as the_new_week
			from 	table_date_range(Q_PA_Stage.z_holisticstore_,timestamp('2016-07-15'),timestamp('2017-02-09'))
		) 	as base --> Parameter!
		left join Q_PA_Stage.z_holisticstore_ref	as ref 				-- for Sky Cinema / Box Sets
		on	base.base_thedate			= ref.base_thedate
		and	base.base_viewing_card		= ref.base_viewing_card
		and	base.base_sky_plus_session	= ref.base_sky_plus_session
where	(
			integer(concat(string(base.base_sessionid),string(base.base_actions_sequence))) between ref.z and ref.the_end -- Filtering for Cinema/Box Sets
			or base.base_sky_plus_session in ('Sky Store','Top Picks')	-- Persisting with Store and Top Picks
		)
and		base.base_software_version like 'R11%'
group	by	base.the_new_week
			,base.base_sky_plus_session





-- Run now this for R11
-- Sky week 9 onwards... 29th of Augst
-- weekly snapshots...