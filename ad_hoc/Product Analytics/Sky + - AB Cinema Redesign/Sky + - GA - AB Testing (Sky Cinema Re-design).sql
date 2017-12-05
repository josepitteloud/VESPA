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
**Stakeholder:                          Daniel Chronnell

**Business Brief:

        There is a new UI version for + been propose (to look more like Q). 
		Hence, we want to understand what impact that will have over Sky Cinema.
		
**Considerations:

		->	This piece will require clarification on previous work done by DataTonic 
			(Sky Cinema conversion rate = 28% using Google Sessions vs 33% using Sky Journeys).
			
		->	True to date, since we cannot identify customers' subs we are making a proxy to
			accounts with Sky Cinema subs by only considering those who have converted at least
			once in this area of the UI.
		
		->	
		
**Sections:

		A - Data Evaluation
			1 - Defining targeted profile
			2 - How far back in time can we go with this population...
			3 - For a Holistic View for Sky Store activity for this group of STBs...
			4 - Standardising what is the expected behaviour in Sky cinema for the targeted profile...
			
		B - Data Preparation
			
		C - Data Analysis (Queries)
			
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/

--------------------------------
-- 1 - Defining targeted profile
--------------------------------

/*
	All Active accounts with Sky Cinema Subscriptions
	
	a proxy valid until (2017-04-28), since at the moment we don't have visibility on the type of subs an account holds in Google Analytics
	we will then be targeting accounts that have converted at least once in our timeframe in Sky cinema. The reasoning is that, because
	you cannot download anything unless you have the rights to do so + circa 80% of all accounts do consume cinema content; this then is a 
	safe proxy to begin with
	
					select	count(1) -- 103,982 STBs comply to this criteria
					from	(
								select	viewing_card
								from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-01'),timestamp('2017-01-31'))
								where	sky_plus_session = 'Sky Cinema'
								and		(
											(lower(screen) like '/tv/%' and sky_plus_session = 'TV Guide')	or
											lower(screen) like '/tv/live/%' 								or
											lower(screen) like '/playback/%' 							    or
											lower(eventlabel) like '%not_booked%'
										)
								group	by	viewing_card
							)	as ref_profile
	
	Timeframe:
		January 2017
		
	[2017-04-28] AD: we switched now to use Tom's way of identifying customers on Movie Packages which is what we originally intended
*/

		
select	the_date
		,viewing_card
from	(
			SELECT visitID
					,date(date) as the_date
					,MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) WITHIN hits AS PVOD
					,MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN hits AS viewing_card
			FROM	TABLE_DATE_RANGE([78413818.ga_sessions_], TIMESTAMP('2017-01-27'), TIMESTAMP('2017-01-27')) --> Parameter
		)	as base
where	pvod like 'SH_UK%MOVIES%'
group	by	1,2

	
-------------------------------------------------------------
-- 2 - How far back in time can we go with this population...
-------------------------------------------------------------
/*
	ANSWER:
	With above group of STBs we can only go back until October 2016 where we have 90% of them sending data
	before October 2016 only 14% of them did on monthly basis. Which is not ideal at all.
			
					select	strftime_utc_usec(base.thedate, "%y-%m")	as the_month
							,count(distinct base.viewing_card)			as nboxes
							,count(1)									as nactions
					from    (
								select	*
								from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-04-01'),timestamp('2017-03-31'))
							)	as base
							inner join	(
											select	viewing_card
											from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-01'),timestamp('2017-01-31'))
											where	sky_plus_session = 'Sky Cinema'
											and		(
														(lower(screen) like '/tv/%' and sky_plus_session = 'TV Guide')	or
														lower(screen) like '/tv/live/%' 								or
														lower(screen) like '/playback/%' 							    or
														lower(eventlabel) like '%not_booked%'
													)
											group	by	viewing_card
										)	as ref_profile
							on	base.viewing_card	= ref_profile.viewing_card
					group	by	1
	
	[2017-04-28] AD: We no longer need this check since the new approach mentioned above
	
*/

---------------------------------------------------------------------------
-- 3 - For a Holistic View for Sky Store activity for this group of STBs...
---------------------------------------------------------------------------

/*
	This exercise is an incremental work on top of what done previously for Sky Store team
	Therefore the following piece is referenced in this exercise:
	C:\Users\and36\Documents\GIT\Vespa\ad_hoc\Product Analytics\Sky + - GA - Store Holistic View.sql
	
	Making use of the know-how to generate the holistic view for Sky Store.
	
	This then will be used as a template to generate KPIs for our sample of STBs (complying with the
	targeted profile, defined above)
	
	PRE-REQUISITS for this approach:
	
	+ Q_PA_Stage.z_holisticstore_yyyymmdd
	+ Q_PA_Stage.z_holisticstore_ref
	
	OUTPUT CHECKS AT:
	G:\RTCI\Sky Projects\Vespa\Products\Analysis - Excel\Sky + - AB Cinema\Sky + - GA - Holistic Store activity for cinema subs STBs.xlsx
	
*/

select	base.base_sky_plus_session
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
			from 	table_date_range(Q_PA_Stage.z_holisticstore_,timestamp('2017-01-16'),timestamp('2017-01-23')) -- > Parameter
		) 	as base
		left join Q_PA_Stage.z_holisticstore_ref	as ref	-- for Sky Cinema / Box Sets
		on	base.base_thedate			= ref.base_thedate
		and	base.base_viewing_card		= ref.base_viewing_card
		and	base.base_sky_plus_session	= ref.base_sky_plus_session
		inner join	(
						-- Target Profile...
						select	the_date
								,viewing_card
						from	(
									SELECT visitID
											,date(date) as the_date
											,MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) WITHIN hits AS PVOD
											,MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN hits AS viewing_card
									FROM	TABLE_DATE_RANGE([78413818.ga_sessions_],timestamp('2017-01-16'),timestamp('2017-01-23')) -- > Parameter
								)	as base
						where	pvod like 'SH_UK%MOVIES%'
						group	by	1,2
					)	as ref_profile
		on	base.base_viewing_card	= ref_profile.viewing_card
		and	base.base_thedate		= ref_profile.the_date
where	(
			integer(concat(string(base.base_sessionid),string(base.base_actions_sequence))) between ref.z and ref.the_end	-- Filtering for Cinema/Box Sets
			or base.base_sky_plus_session in ('Sky Store','Top Picks')														-- Persisting with Store and Top Picks
		)
group	by	base.base_sky_plus_session


---------------------------------------------------------------------------------------------
-- 4 - Standardising what is the expected behaviour in Sky cinema for the targeted profile...
---------------------------------------------------------------------------------------------

/*
	The aim here is to measure what is the expected behaviour for the targeted profile through the standard set of KPIs we
	use to date (Conversion Rate, Reach, ratio of Exploratory/Converted Journeys, time to Exit/Convert)
	
	Snap-shooting these KPIs over the past 12th months (suggested by Chronnell) split on 10 days snapshots (suggested by Simon)
	
	OUTPUT CHECK AT:
	G:\RTCI\Sky Projects\Vespa\Products\Analysis - Excel\Sky + - AB Cinema\Sky + - GA - Sky Cinema Behaviour (Before Release).xlsx
	
*/

select  base.snapshot_10d
		--,base.A_B
		,case 	when	base.sky_plus_session = 'Catch Up TV' then 'Catch Up'
				else	base.sky_plus_session 
		end		as gn_lvl2_session
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
			select	*
					--,((extract(epoch from thedate - date('2016-10-01')))/10)+1	as snapshot_10d
					,floor(datediff(timestamp(thedate),timestamp('2016-10-01'))/10)+1 as snapshot_10d
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2017-03-31')) --> Parameter
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
															(lower(screen) like '/tv/%' and sky_plus_session = 'TV Guide')			or
															lower(screen) like '/tv/live/%' 										or
															lower(screen) like '/playback/%' 										or
															lower(eventlabel) like '%not_booked%'									or
															(action_category = 'LinearAction' and action like 'LINEAR%_RECORD_%')
														)	then integer(concat(string(sessionid),string(actions_sequence)))
												else null
										end 
									)	as conv_flag
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2017-03-31')) --> Parameter
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
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2017-03-31')) --> Parameter
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
						select	floor(datediff(timestamp(thedate),timestamp('2016-10-01'))/10)+1	as snapshot_10d
						,count(distinct viewing_card)												as nactive_boxes
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2016-10-01'),timestamp('2017-03-31')) --> Parameter
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
						group	by	1
					)	as ref_act_base
		on	base.snapshot_10d	= ref_act_base.snapshot_10d
		inner join	(
						-- Target Profile...
						select	the_date
								,viewing_card
						from	(
									SELECT visitID
											,date(date) as the_date
											,MAX(IF(hits.customDimensions.index=1, hits.customDimensions.value, NULL)) WITHIN hits AS PVOD
											,MAX(IF(hits.customDimensions.index=3, hits.customDimensions.value, NULL)) WITHIN hits AS viewing_card
									FROM	TABLE_DATE_RANGE([78413818.ga_sessions_],timestamp('2016-10-01'),timestamp('2017-03-31')) --> Parameter
								)	as base
						where	pvod like 'SH_UK%MOVIES%'
						group	by	1,2
					)	as ref_profile
		on	base.viewing_card	= ref_profile.viewing_card
		and	base.thedate		= ref_profile.the_date
group	by	base.snapshot_10d
			,gn_lvl2_session