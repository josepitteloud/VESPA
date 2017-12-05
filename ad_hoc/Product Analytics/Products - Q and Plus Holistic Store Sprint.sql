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

        A Sprint done to measure Sky Store Home Page performance against all other TLMs (Specifically On Demand areas) 
		and also to measure performance of all Store Entry Points in the UI. Done for Sky Q and Plus.
		
**Considerations:

		-> Existing Gaps on identifying all journeys for all entry points in both Sky Plus and Q
		-> # of Downloads in Sky Plus is a proxy since we infer a download took place
		
**Sections: (TBC)

		A - Data Evaluation
			
		B - Data Preparation
				
		C - Data Analysis (Queries)
			
**Running Time:

???

--------------------------------------------------------------------------------------------------------------

*/


--------
-- Sky Q
--------


-- Sky Q - PA - Downloads for Sky Store WoW...
with	base as	(
					select	extract(year from date_)	as year_
							,extract(month from date_)	as month_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1	as nweek_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') then 'Sky Store'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,dk_action_id
					from	z_pa_events_fact_201701
					where	dk_Action_id in (05300,05350)
					and		date_ > '2017-01-01'
					and		gn_lvl2_session in	(
													'Catch Up'
													,'Kids'
													,'Mini Guide'
													,'Search'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Top Picks'
													,'Voice Search'
													,'Sky Store'
													,'Home'
												)
					union
					select	extract(year from date_)	as year_
							,extract(month from date_)	as month_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1	as nweek_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') then 'Sky Store'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,dk_action_id
					from	z_pa_events_fact_201702
					where	dk_Action_id in (05300,05350)
					and		gn_lvl2_session in	(
													'Catch Up'
													,'Kids'
													,'Mini Guide'
													,'Search'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Top Picks'
													,'Voice Search'
													,'Sky Store'
													,'Home'
												)
					union
					select	extract(year from date_)	as year_
							,extract(month from date_)	as month_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1	as nweek_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') 	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,dk_action_id
					from	z_pa_events_fact_201703
					where	dk_Action_id in (05300,05350)
					and		gn_lvl2_session in	(
													'Catch Up'
													,'Kids'
													,'Mini Guide'
													,'Search'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Top Picks'
													,'Voice Search'
													,'Sky Store'
													,'Home'
												)
					union
					select	extract(year from date_)	as year_
							,extract(month from date_)	as month_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1	as nweek_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store')	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,dk_action_id
					from	z_pa_events_fact_201704
					where	dk_Action_id in (05300,05350)
					and		gn_lvl2_session in	(
													'Catch Up'
													,'Kids'
													,'Mini Guide'
													,'Search'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Top Picks'
													,'Voice Search'
													,'Sky Store'
													,'Home'
												)
					union
					select	extract(year from date_)	as year_
							,extract(month from date_)	as month_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1	as nweek_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store')	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,dk_action_id
					from	z_pa_events_fact_201705
					where	dk_Action_id in (05300,05350)
					and		gn_lvl2_session in	(
													'Catch Up'
													,'Kids'
													,'Mini Guide'
													,'Search'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Top Picks'
													,'Voice Search'
													,'Sky Store'
													,'Home'
												)
					union
					select	extract(year from date_)	as year_
							,extract(month from date_)	as month_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1	as nweek_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store')	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,dk_action_id
					from	z_pa_events_fact_201706
					where	dk_Action_id in (05300,05350)
					and		gn_lvl2_session in	(
													'Catch Up'
													,'Kids'
													,'Mini Guide'
													,'Search'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Top Picks'
													,'Voice Search'
													,'Sky Store'
													,'Home'
												)
				)
select	base.year_
		,base.nweek_
		,count(distinct date_)														as ndays
		,min(date_) 																as from_
		,max(date_) 																as to_
		,gn_lvl2_session_
		,count(distinct dk_serial_number)											as reach
		,count(distinct date_||'-'||dk_serial_number||'-'||gn_lvl2_session_grain)	as njourneys
		,sum(case when dk_Action_id = 05300 then 1 else 0 end)						as n_rents
		,sum(case when dk_Action_id = 05350 then 1 else 0 end)						as n_buys
from	base
group	by	base.year_
			,base.nweek_
			,gn_lvl2_session_
having	ndays = 7
			
			
			
-- Sky Q - PA - Sky Store Home Page Performance WoW


-- 19 min

with	base as	(
					select	extract(year from date_)																		as year_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1											as week_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') then 'Sky Store'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
							,sum(case when dk_Action_id = 05300 then 1 else 0 end)											as n_rents
							,sum(case when dk_Action_id = 05350 then 1 else 0 end)											as n_buys
					from	z_pa_events_fact_201701
					where	date_ > '2017-01-01'
					and		gn_lvl2_session in	(
													'TV Guide'
													,'Catch Up'
													,'Recordings'
													,'My Q'
													,'Top Picks'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Sky Store'
													,'Sports'
													,'Kids'
													,'Music'
													,'Online Videos'
												)
					group	by	1,2,3,4,5,6
					union
					select	extract(year from date_)																		as year_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1											as week_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') then 'Sky Store'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
							,sum(case when dk_Action_id = 05300 then 1 else 0 end)											as n_rents
							,sum(case when dk_Action_id = 05350 then 1 else 0 end)											as n_buys
					from	z_pa_events_fact_201702
					where	gn_lvl2_session in	(
													'TV Guide'
													,'Catch Up'
													,'Recordings'
													,'My Q'
													,'Top Picks'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Sky Store'
													,'Sports'
													,'Kids'
													,'Music'
													,'Online Videos'
												)
					group	by	1,2,3,4,5,6
					union
					select	extract(year from date_)																		as year_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1											as week_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') 	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
							,sum(case when dk_Action_id = 05300 then 1 else 0 end)											as n_rents
							,sum(case when dk_Action_id = 05350 then 1 else 0 end)											as n_buys
					from	z_pa_events_fact_201703
					where	gn_lvl2_session in	(
													'TV Guide'
													,'Catch Up'
													,'Recordings'
													,'My Q'
													,'Top Picks'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Sky Store'
													,'Sports'
													,'Kids'
													,'Music'
													,'Online Videos'
												)
					group	by	1,2,3,4,5,6
					union
					select	extract(year from date_)																		as year_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1											as week_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') 	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
							,sum(case when dk_Action_id = 05300 then 1 else 0 end)											as n_rents
							,sum(case when dk_Action_id = 05350 then 1 else 0 end)											as n_buys
					from	z_pa_events_fact_201704
					where	gn_lvl2_session in	(
													'TV Guide'
													,'Catch Up'
													,'Recordings'
													,'My Q'
													,'Top Picks'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Sky Store'
													,'Sports'
													,'Kids'
													,'Music'
													,'Online Videos'
												)
					group	by	1,2,3,4,5,6
					union
					select	extract(year from date_)																		as year_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1											as week_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') 	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
							,sum(case when dk_Action_id = 05300 then 1 else 0 end)											as n_rents
							,sum(case when dk_Action_id = 05350 then 1 else 0 end)											as n_buys
					from	z_pa_events_fact_201705
					where	gn_lvl2_session in	(
													'TV Guide'
													,'Catch Up'
													,'Recordings'
													,'My Q'
													,'Top Picks'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Sky Store'
													,'Sports'
													,'Kids'
													,'Music'
													,'Online Videos'
												)
					group	by	1,2,3,4,5,6
					union
					select	extract(year from date_)																		as year_
							,(extract(epoch from date_ - date('2017-01-02'))/7)+1											as week_
							,case	when gn_lvl2_session in ('Home','Sky  Store','Sky Store') 	then 'Sky Store'
									when gn_lvl2_session = 'Top Picks'							then 'My Q'
									else gn_lvl2_session
							end		as gn_lvl2_session_
							,date_
							,dk_serial_number
							,gn_lvl2_session_grain
							,max(case when dk_action_id in (02400,03000,00001,02000,02010,02002,02005) then 1 else 0 end)	as conv_flag
							,sum(case when dk_Action_id = 05300 then 1 else 0 end)											as n_rents
							,sum(case when dk_Action_id = 05350 then 1 else 0 end)											as n_buys
					from	z_pa_events_fact_201706
					where	gn_lvl2_session in	(
													'TV Guide'
													,'Catch Up'
													,'Recordings'
													,'My Q'
													,'Top Picks'
													,'Sky Box Sets'
													,'Sky Movies'
													,'Sky Store'
													,'Sports'
													,'Kids'
													,'Music'
													,'Online Videos'
												)
					and		date_ = '2017-06-18'
					group	by	1,2,3,4,5,6
				)
		,base_act as	(
							select	year_
									,week_
									,count(distinct dk_serial_number) as nactive_Stbs
							from	base
							group	by	year_
										,week_
						)
select	year_
		,week_
		,gn_lvl2_session_
		,count(distinct x.date_) 																						as ndays
		,min(x.date_) 																									as from_
		,max(x.date_) 																									as to_
		,max(z.nactive_stbs) 																							as nactive_base
		,count(distinct x.dk_serial_number)																				as reach
		,count(distinct y.DATE_||'-'||y.dk_serial_number||'-'||y.target)  												as njourneys
		,count(distinct (case when x.conv_flag = 1 then y.DATE_||'-'||y.dk_serial_number||'-'||y.target else null end))	as nconv_journeys
from	base						as x
		inner join base_act 		as z
		on	x.year_	= z.year_
		and	x.week_	= z.week_
		left join ref_home_start_	as y -- Home Page Performance 
		on	x.date_					= y.date_
		and	x.dk_Serial_number		= y.dk_serial_number
		and	x.gn_lvl2_session_grain	= y.target
group	by	year_
			,week_
			,gn_lvl2_session_
having	ndays = 7


--------
-- Sky +
--------

-- Preparing base data for Holistic Store...

select	base.*
		,integer(ceil((datediff(timestamp(base.thedate),timestamp('2017-01-02'))+1)/7)) 			as Sky_week
		,case	when lower(regexp_replace(base.screen,' ','')) like '%skystore%' then 1 else 0  end	as x
		,max([x]) over	(
							partition by	base.viewing_card
											,base.sky_plus_session
							order by 		base.sessionid
											,base.actions_sequence 
							rows between 	1 preceding and 1 preceding
						) 	as y
--into	z_holisticstore
from    (select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18'))) as base --> Parameter!	
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
									from 	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18')) --> Parameter
									where 	sky_plus_session = 'Top Picks'
									and		action_category = 'HomePageLinkJump'
								) 	as base
								inner join Q_PA_Stage.z_bypass2	as ref
								on	base.thelinkage	= ref.grid
						where 	action contains ref.grid
						and		base.timestamp_ between timestamp(ref.effective_from) and timestamp(ref.effective_to)
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
												from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18')) --> Parameter!
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
		inner join	(
						select	* 
						from [eternal-trees-847:PanelManagement.CustomerPanels25112015]
						where Panel_ref in ('Customer_1', 'Customer_1_1', 'Customer_2')
					) 	as panel 
		on 	base.viewing_card = panel.vcid
where	(
			(
				base.sky_plus_session in ('Sky Cinema','Sky Box Sets')	-- Choosing Sky Cinema/Box Sets
				and (length(base.screen) - length(regexp_replace(base.screen,'/',''))) > 1
			)
			or	ref2.sky_plus_session_grain is not null 				-- Choosing Top Picks journeys
			or	ref.sky_plus_session_grain is not null					-- Choosing Sky Store journeys
		)
		
		

		
		
---	Generating Standard KPI for Holistic Store Entry Points Performance...

select	base.sky_week
		,base.base_sky_plus_session
		,count(distinct base.base_thedate)																							as ndays
		,min(base.base_thedate)																										as from_
		,max(base.base_thedate)																										as to_
		,count(distinct(concat(string(base.base_thedate),string(base.base_viewing_card),string(base.base_sky_plus_session_grain))))	as njourneys
		,count(distinct (
							case  	when  	(
												--(lower(base.base_screen) like '/tv/%' and base.base_sky_plus_session = 'TV Guide')	or
												--lower(base.base_screen) like '/tv/live/%' 								            or
												--lower(base.base_screen) like '/playback/%' 								            or
												lower(base.base_eventlabel) like '%not_booked%'										--or				
												--base.base_action_category = 'LinearAction' and base.base_action like 'LINEAR%_RECORD_%'
											) 	then concat(string(base.base_thedate),string(base.base_viewing_card),string(base.base_sky_plus_session_grain))
									else null
							end
						))  																										as njourneys_converted
		,count(distinct(base.base_viewing_card))																					as nboxes
		,sum((
				case  	when  	(
									lower(base.base_eventlabel) like '%not_booked%' --and base.base_action = 'SELECT'
								) 	then 1
						else 0
				end
			))  																													as n_downloads
from	Q_PA_Stage.z_holisticstore2					as base
		left join Q_PA_Stage.z_holisticstore_ref_	as ref 				-- for Sky Cinema / Box Sets
		on	base.base_thedate			= ref.base_thedate
		and	base.base_viewing_card		= ref.base_viewing_card
		and	base.base_sky_plus_session	= ref.base_sky_plus_session
		inner join	(	-- Carving for 50k sample...
						/*
							This is done since in Jan-Feb we have the Top Picks A/B testing which makes figures irregular and
							not fit with regular periods of activity. The 50K sample did not experienced the A/B test hence is in 
							right shape for a time series analysis...
						*/
						select	* 
						from [eternal-trees-847:PanelManagement.CustomerPanels25112015]
						where Panel_ref in ('Customer_1', 'Customer_1_1', 'Customer_2')
					) 	as panel 
		on 	base.base_viewing_card = panel.vcid
where	(
			integer(concat(string(base.base_sessionid),string(base.base_actions_sequence))) between ref.z and ref.the_end -- Filtering for Cinema/Box Sets
			or base.base_sky_plus_session in ('Sky Store','Top Picks')	-- Persisting with Store and Top Picks
		)
group	by	base.sky_week
			,base.base_sky_plus_session
			
			
--- Measuring volume of weekly active STBs reporting data...

select	base.sky_week
		,count(distinct(base.base_viewing_card))	as active_STBS
from	Q_PA_Stage.z_holisticstore2					as base
		left join Q_PA_Stage.z_holisticstore_ref_	as ref 				-- for Sky Cinema / Box Sets
		on	base.base_thedate			= ref.base_thedate
		and	base.base_viewing_card		= ref.base_viewing_card
		and	base.base_sky_plus_session	= ref.base_sky_plus_session
		inner join	(
						select	* 
						from [eternal-trees-847:PanelManagement.CustomerPanels25112015]
						where Panel_ref in ('Customer_1', 'Customer_1_1', 'Customer_2')
					) 	as panel 
		on 	base.base_viewing_card = panel.vcid
where	(
			integer(concat(string(base.base_sessionid),string(base.base_actions_sequence))) between ref.z and ref.the_end -- Filtering for Cinema/Box Sets
			or base.base_sky_plus_session in ('Sky Store','Top Picks')	-- Persisting with Store and Top Picks
		)
group	by	base.sky_week



--	Home Page Performance...

select  base.Sky_week
		,case 	when	base.sky_plus_session = 'Catch Up TV' then 'Catch Up'
				else	base.sky_plus_session 
		end																																				as gn_lvl2_session
		--,max(base_act.nactive_days)																														as nact_days
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
                        ))  																															as njourneys_converted
		,max(pop_ref.n_stbs_pop)																														as tot_stb_pop
        ,count(distinct base.viewing_card)																												as reach
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
						))																																as conversion_reach
        ,sum(base.secs_to_next_action )																													as n_secs_in_session
        ,sum	(
					case	when	integer(concat(string(base.sessionid),string(base.actions_sequence)))<=ref.conv_flag then base.secs_to_next_action 
							else 	null 
					end
				)	 																																	as n_secs_to_conv
from    (
			/*
				Extracting only the data we need for TLMs...
			*/
			select  *
					,integer(ceil((datediff(timestamp(thedate),timestamp('2017-01-02'))+1)/7)) as Sky_week
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18'))
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
						-- Calculating how many days each STB was active on the week...
						select	integer(ceil((datediff(timestamp(thedate),timestamp('2017-01-02'))+1)/7))	as Sky_week
								,viewing_card
								,count(distinct thedate)													as nactive_days
						from 	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18'))
						group	by	sky_week
									,viewing_card
					)	as base_act
		on	base.sky_week		= base_act.sky_week
		and	base.viewing_card	= base_act.viewing_card
		inner join	(
						-- Total Population Selected for Focus Groups...
						select  integer(ceil((datediff(timestamp(thedate),timestamp('2017-01-02'))+1)/7)) 	as Sky_week
								,count(distinct viewing_card) 												as n_stbs_pop
						from    (select thedate, viewing_card from table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18')) group by 1,2) as x
								inner join	(
												select	* 
												from [eternal-trees-847:PanelManagement.CustomerPanels25112015]
												where Panel_ref in ('Customer_1', 'Customer_1_1', 'Customer_2')
											) 	as panel 
								on 	x.viewing_card = panel.vcid
						group   by  Sky_week
					)	as pop_ref
		on	base.Sky_week = pop_ref.Sky_week
        LEFT JOIN	(
						
							--Identifying the first action in the journey related to conversion. this will be used above
							--to measure the length in seconds from the start of the each session that converted until 
							--the this first action considered for conversion... resulting in the measure named
							--"n_secs_to_conv"
						 
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
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18'))
						group	by 	thedate
									,viewing_card
									,sky_plus_session_grain
					)	as ref
          on  base.thedate 					= ref.thedate
          and base.viewing_card 			= ref.viewing_card
          and base.sky_plus_session_grain	= ref.sky_plus_session_grain
		  inner join	(
							
								--Identifying only sessions that began at home...
							
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
													from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-01-02'),timestamp('2017-06-18'))
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
						select	* 
						from [eternal-trees-847:PanelManagement.CustomerPanels25112015]
						where Panel_ref in ('Customer_1', 'Customer_1_1', 'Customer_2')
					) 	as panel 
		on 	base.viewing_card = panel.vcid 
group   by	base.Sky_week
			,gn_lvl2_session