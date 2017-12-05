---------------------------- [ this is the test Script for Dev (One-go version) ]

-- step 5 - Propagating sessions tags (both levels 2 and 3)

/*
	once we have carved the sessions and identified where each of them start and ends
	we simply propagate the correspondent tags (sessions names and ids) to the 
	associated actions (rows)
*/
select	index_
		,dk_asset_id
		,dk_date
		,dk_serial_number
		,dk_action_id
		,dk_previous
		,dk_current
		,dk_referrer_id
		,dk_trigger_id
		,last_value(stage1 ignore nulls) over	(
													partition by	dk_serial_number
													order by 		index_
													rows between	500 preceding and current row
												)							as gn_lvl3_session_grain
		,substr(gn_lvl3_session_grain,1,instr(gn_lvl3_session_grain,'-')-1)	as gn_lvl3_session
		,last_value(stage2 ignore nulls) over	(
													partition by	dk_serial_number
													order by 		index_
													rows between	500 preceding and current row
												)							as gn_lvl2_session_grain
		,substr(gn_lvl2_session_grain,1,instr(gn_lvl2_session_grain,'-')-1) as gn_lvl2_session
from	(

			-- step 4 - Carving the sessions (level 2)
						
			/*
				here what we are aiming to achieve is to bag together all actions into a single and relevant
				session, at each level (2 in this case).
				
				Hence stripping (making nulls) then associate sessions values for any actions right below
				the session start point.
			*/
			select	index_
					,dk_asset_id
					,dk_date
					,dk_serial_number
					,dk_action_id
					,dk_previous
					,dk_current
					,dk_referrer_id
					,dk_trigger_id
					,stage1
					,case	when	TLM = (max(TLM) over	( 
																partition by	dk_serial_number
																order by 		index_ 
																rows between 	1 preceding and 1 preceding
															)
										) then null
							else TLM||'-'|| dense_rank() over	(
																	partition by	dk_date
																					,dk_serial_number
																					,TLM
																	order by 		index_
																)
					end		as stage2
			from	(
			
						-- step 3 - Carving the sessions (level 3)
						
						/*
							here what we are aiming to achieve is to bag together all actions into a single and relevant
							session, at each level (3 in this case).
							
							Hence stripping (making nulls) then associate sessions values for any actions right below
							the session start point.
						*/
						select	index_
								,dk_asset_id
								,dk_date
								,dk_serial_number
								,dk_action_id
								,dk_previous
								,dk_current
								,dk_referrer_id
								,dk_trigger_id
								,SLM
								,case	when SLM = (max(SLM) over	( 
																				partition by	dk_serial_number
																				order by 		index_ 
																				rows between 	1 preceding and 1 preceding
																			)
														) then null
										else SLM||'-'|| dense_rank() over	(
																					partition by	dk_date
																									,dk_serial_number
																									,SLM
																					order by 		index_
																				)
								end		stage1
								,screen_parent
								,case 	when	stage1 is not null	then	(
																				case	when screen_parent is null then substr(stage1,1,instr(stage1,'-')-1)
																						else screen_parent 
																				end
																			) 
										else null 
								end		as TLM					
						from	(
						
									-- step 2 - Identifying sessions starts
									
									/*
										by linking Step 1 output with pa_session_config, we then lookup (through thelinkage field)
										what are the URIs we have parametrised to make up the sessions (and the relevant session's metadata)
									*/
									select	index_
											,dk_asset_id
											,b.screen_name as SLM
											,a.thelinkage
											,a.dk_action_id
											,theprevious	as dk_previous
											,a.DK_current 	-- destination
											,a.DK_REFERRER_ID
											,a.dk_trigger_id
											,dk_serial_number
											,dk_date
											,b.screen_parent
									from	(
									
												-- Step 1 -> Sampling & preparing the data for merging with the Session Config table
												/*
													Sampling: 	
													
													in principle, given that this is just a POC we don't need the whole data
													but instead one day of a lab box which we knows has the latest SI build installed
																
													Preparing the data:
													
													given that we compiled for every action, its preceding (dk_previous) and its
													succeeding (dk_current) actions done by the user. Every interactive action (that is,
													any dk_action_id <> 01400) we state that the dk_previous is always = dk_referrer_id
													Doing so means that such dk_previous = the place where the action took place
													
													the field below named thelinkage should be a redundant validation for above.
													
												*/
												select	row_number() over	(order by timems)	as index_
														,dk_asset_id
														,timems
														,dk_date
														,dk_serial_number
														,dk_action_id
														,case	when dk_previous in ('01400','N/A') then	dk_referrer_id 
																else dk_previous
														end		as theprevious
														,case	when (dk_referrer_id is not null and dk_referrer_id <> 'N/A')	then dk_referrer_id
																when dk_action_id in ('01001','01002') 							then 'Mini guide'
																-- below one, to capture cases for Jsons without ref field as a bug
																when dk_previous like '0%'										then	dk_referrer_id 
																else dk_previous
														end		as thelinkage
														,dk_current
														,dk_trigger_id
														,dk_referrer_id
												from	pa_events_fact
												where	dk_date = 20160121
												and		dk_serial_number = '32B0550480005157E'
											)	as a
											left join pa_session_config as b
											on	a.thelinkage = b.PK_SCREEN_ID
											and b.pk_screen_id not like '0%'
								)	as base
						group	by	index_
									,dk_asset_id
									,dk_date
									,dk_serial_number
									,dk_action_id
									,dk_previous
									,dk_current
									,dk_trigger_id
									,dk_referrer_id
									,SLM
									,screen_parent
					)	as base2
		)	as base3
order	by	index_

