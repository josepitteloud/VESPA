-- For Sky Cinema and Store

select	count(distinct viewing_card) 													as active_base
		,count(distinct (case when sky_package = 1 then viewing_card else null end)) 	as with_sky_package
		,avg(case when sky_package = 1 then actions_in_cinema else null end) 			as cinema_avg
		,stddev_samp( case when sky_package = 1 then actions_in_cinema else null end)	as cinema_stdev
		,avg(case when sky_package = 0 then actions_in_store else null end)				as store_avg
		,stddev_samp(case when sky_package = 0 then actions_in_store else null end)		as store_stdev
from	(
			SELECT	base.viewing_card
					,max(base.sky_package)													as sky_package
					,sum(base.cinema_Actions) 												as actions_in_cinema
					,sum(base.store_actions) 												as actions_in_store
			FROM	(
						select	thedate
								,viewing_card
								,max(case when pvod contains "MOVIES" then 1 else 0 end) as sky_package
								,SUM(	
										CASE	WHEN sky_plus_session = "Sky Cinema"
												and	(
														(eventlabel CONTAINS 'templatename=ViewOrRecordPVOD' OR lower(eventLabel) like '%play_pvod_event%')
												        or ((eventlabel CONTAINS 'State=NOT_BOOKED;templatename=MultiFormat' AND NOT sky_plus_session CONTAINS "Sky Store") OR lower(eventLabel) like '%start_pdl_event_download%')	
												        or ((eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') AND sky_plus_session = "Sky Cinema")
												        or (action_category CONTAINS 'AssetPurchase')
												        or (eventlabel CONTAINS 'UNHANDLED_EVENT_TYPE' AND screen CONTAINS "/buy_and_keep_purchase")
												        or (eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') 																					
													)	then 1
												else 0 
										end
									)	as Cinema_actions
								,SUM(	
										CASE	WHEN sky_plus_session = "Sky Store"
												and	(
														(eventlabel CONTAINS 'templatename=ViewOrRecordPVOD' OR lower(eventLabel) like '%play_pvod_event%')
												        or ((eventlabel CONTAINS 'State=NOT_BOOKED;templatename=MultiFormat' AND NOT sky_plus_session CONTAINS "Sky Store") OR lower(eventLabel) like '%start_pdl_event_download%')	
												        or ((eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') AND sky_plus_session = "Sky Cinema")
												        or (action_category CONTAINS 'AssetPurchase')
												        or (eventlabel CONTAINS 'UNHANDLED_EVENT_TYPE' AND screen CONTAINS "/buy_and_keep_purchase")
												        or (eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') 																					
													)	then 1
												else 0 
										end
									)	as Store_actions
						from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-06-01'),timestamp('2017-06-14'))	--> Parameter
						group	by	thedate
									,viewing_card
					)	as base
			group	by	base.viewing_card
		)	as ground

		
-- for Sky Store within Cinema (for Cinema Package)
		
select	count(distinct viewing_card)	as nboxes
		,avg(actions) 					as avg_actions
		,stddev_samp(actions) 			as stddev_actions
		,avg(njourneys) 				as avg_Journeys
		,stddev_samp(njourneys) 		as stddev_journeys
from	(
			select	base.viewing_card
					,count(distinct (
										CASE	WHEN sky_plus_session = "Sky Cinema"
												and	integer(concat(string(base.sessionid),string(base.actions_sequence))) between ref.z and ref.the_end -- Filtering for Cinema/Box Sets
												then concat(string(base.thedate),string(base.viewing_card),string(base.sky_plus_session_grain))
												else null
										end
									))	as njourneys
					,SUM(	
							CASE	WHEN sky_plus_session = "Sky Cinema"
									and	integer(concat(string(base.sessionid),string(base.actions_sequence))) between ref.z and ref.the_end -- Filtering for Cinema/Box Sets
									and	(
											(eventlabel CONTAINS 'templatename=ViewOrRecordPVOD' OR lower(eventLabel) like '%play_pvod_event%')
											or ((eventlabel CONTAINS 'State=NOT_BOOKED;templatename=MultiFormat' AND NOT base.sky_plus_session CONTAINS "Sky Store") OR lower(eventLabel) like '%start_pdl_event_download%')	
											or ((eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') AND base.sky_plus_session = "Sky Cinema")
											or (action_category CONTAINS 'AssetPurchase')
											or (eventlabel CONTAINS 'UNHANDLED_EVENT_TYPE' AND screen CONTAINS "/buy_and_keep_purchase")
											or (eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') 																					
										)	then 1
									else 0
							end
						)	as Actions
			from	(select * from table_date_range(skyplus.skyplus_sessions_,timestamp('2017-06-01'),timestamp('2017-06-14'))) as base	--> Parameter
					left join Q_PA_Stage.z_holisticstore_ref_	as ref 				-- for Sky Cinema / Box Sets
					on	base.thedate			= ref.base_thedate
					and	base.viewing_card		= ref.base_viewing_card
					and	base.sky_plus_session	= ref.base_sky_plus_session
					inner join	(	-- Target Profile: anyone with Sky Cinema Package
									select	thedate
											,viewing_card as vcid
									from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-06-01'),timestamp('2017-06-14'))	--> Parameter
									where	pvod contains 'MOVIES'
									group 	by	thedate
												,vcid
								) 	as panel 
					on 	base.thedate		= panel.thedate
					and	base.viewing_card	= panel.vcid
			group	by	base.viewing_card
		)	as ground

		
-- Original Query

SELECT	viewing_card
		,sky_plus_session
		,Actions
		,STDDEV_SAMP(Actions) OVER (PARTitION BY sky_plus_session) AS STDEV
FROM	(
			select	viewing_card 
					,sky_plus_session
					,SUM(	COUNT(DISTINCT	(
												--This cannot consider pVOD rental due to MFCG. Can only be true
												CASE	WHEN eventlabel CONTAINS 'templatename=ViewOrRecordPVOD' OR lower(eventLabel) like '%play_pvod_event%' THEN concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
														ELSE NULL
												END
											))
							+
							COUNT(DISTINCT	(
												CASE	WHEN (eventlabel CONTAINS 'State=NOT_BOOKED;templatename=MultiFormat' AND NOT sky_plus_session CONTAINS "Sky Store") OR lower(eventLabel) like '%start_pdl_event_download%' THEN concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
														ELSE NULL
												END
											))
							+
							COUNT(DISTINCT	(
												--These events are conversions by watching something you've already purchased.
												CASE 	WHEN (eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') AND sky_plus_session = "Sky Cinema" THEN concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
														ELSE NULL
												END
											))
							+
							COUNT(DISTINCT	(
												CASE	WHEN action_category CONTAINS 'AssetPurchase' THEN concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
														ELSE NULL
												END
											)) --CHECKED 
							+
							COUNT(DISTINCT	(
												--Adjusted this to use the WebItemEvent that occurs during a B&K purchase. This is more accurate.
												--This conversion is based on a purchase being compelted. CHECKED
												CASE	WHEN eventlabel CONTAINS 'UNHANDLED_EVENT_TYPE' AND screen CONTAINS "/buy_and_keep_purchase" THEN concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
														ELSE NULL
												END
											))
							+
							COUNT(DISTINCT	(
												--These events are conversions by watching something you've already purchased.
												CASE	WHEN (eventlabel CONTAINS 'templatename=Recorded' OR eventlabel CONTAINS 'PLAY_PDL_EVENT') THEN concat(string(thedate),string(viewing_card),string(sky_plus_session_grain))
														ELSE NULL
												END
											))
						)	AS Actions
			from	table_date_range(skyplus.skyplus_sessions_,timestamp('2017-06-21'),timestamp('2017-06-21'))
			where 	sky_plus_session IN ("Sky Cinema", "Sky Store") --Only return stuff from the session we are interested in
			AND 	PVOD CONTAINS "MOVIES"
			group	by	1,2
		)
WHERE	Actions > 0
ORDER 	BY 3,2 DESC
