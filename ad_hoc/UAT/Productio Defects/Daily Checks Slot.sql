/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES
					  
--------------------------------------------------------------------------------------------------------------

**Project Name: 					DAILY CHECKS (SLOT)
**Analysts:							Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
									Angai Maruthavanan	(Angai.Maruthavanan@SkyIQ.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						CBI

									
**Business Brief:

	To Provide Checks/counts we can then benchmark in order to measure the quality of results produced on each release

**Sections:
	
	A: 	GENERATING SLOTS CHECKS
		
		a01: Filtering/Preparing fields from fact table for easier queries(dk_dates to timestamps)...
		a02: Preparing start,end and capped end dk date times to timestamps for calculatin pre-post capping durations...
		a03: calculating/compacting pre-post capping duration at date level...
	
--------------------------------------------------------------------------------------------------------------
*/

--------------------------------
/* A: GENERATING SLOTS CHECKS */
--------------------------------
select	fact.DT
		,count(distinct fact.key_)																			as Records
		,count(distinct customer.account_number) 															as accounts
		,sum(case when fact.WEIGHT_SCALED >0 then 1 else 0 end) 											as accounts_with_weight
		,sum(case when fact.WEIGHT_SCALED <0 then 1 else 0 end) 											as accounts_with_invalid_weight
		,sum(case when fact.WEIGHT_SCALED <> scaling.WEIGHT_SCALED_VALUE then 1 else 0 end)					as accounts_with_weight_issues
		,count(distinct customer.household_key) 															as HHS
		,sum(case when fact.dk_slot_dim > 0 then 1 else 0 end) 												as records_with_prog_dim
		,count(distinct fact.dk_slot_dim) 																	as total_programme_dim
		,sum(case when fact.dk_slot_dim in (null,-1) then 1 else 0 end) 									as records_without_programme_dim
		,count(distinct fact.dk_channel_dim) 																as total_Channel_dim
		,sum(case when fact.dk_channel_dim in (null,-1) then 1 else 0 end) 									as no_channel_dim
		,sum(case when upper(trim(playback.LIVE_OR_RECORDED))= 'RECORDED' then 1 else 0 end) 				as Num_Recorded_Events
		,sum(case when upper(trim(playback.LIVE_OR_RECORDED))= 'LIVE' then 1 else 0 end) 					as Num_Live_Events
		,sum(case when upper(trim(playback.LIVE_OR_RECORDED)) not in ('LIVE','RECORDED') then 1 else 0 end)	as Num_playback_Unknown_Event
		,sum(case when fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM in (null,-1) then 1 else 0 end)					as Null_Billing_cust
		,sum(case when fact.DK_BARB_MIN_END_DATEHOUR_DIM > 0 then 1 else 0 end)								as Num_MA_records
		,sum(case when fact.DK_VIEWING_EVENT_DIM in (null,-1) then 1 else 0 end)							as Null_Viewing_event_id
		,sum(case when fact.duration < 0 then 1 else 0 end)													as Negative_duration
		,max(fact2.duration_pre_capped)																		as duration_pre_capped
		,max(fact2.duration_post_capped)																	as duration_post_capped
from	(

			-- a01: Filtering/Preparing fields from fact table for easier queries(dk_dates to timestamps)...
			
			select	dk_event_start_datehour_dim/100																	as DT
					,PK_VIEWING_SLOT_INSTANCE_FACT																	as key_
					,duration
					,to_timestamp(
									substring	(	
													cast(cast((dk_event_start_datehour_dim/100)as varchar(8)) as date),1,10) 
													||' '|| 
													cast(substring(cast(dk_event_start_time_dim as varchar(7)),2)	as time
												)
									,'yyyy-mm-dd hh:mi:ss'
								 )								as event_start
					,to_timestamp(
									substring	(	
													cast(cast((dk_event_end_datehour_dim/100)as varchar(8)) as date),1,10) 
													||' '|| 
													cast(substring(cast(dk_event_end_time_dim as varchar(7)),2) as time
												)
									,'yyyy-mm-dd hh:mi:ss'
								 )								as event_end
					,case 	when DK_CAPPED_EVENT_END_DATEHOUR_DIM <0 then null else DK_CAPPED_EVENT_END_DATEHOUR_DIM end	as thedate_capped
					,case 	when DK_CAPPED_EVENT_END_TIME_DIM <0 then null else DK_CAPPED_EVENT_END_TIME_DIM end 			as thetime_capped
					,case 	when thedate_capped>0 
							then
							to_timestamp(
											substring	(	
															cast(cast((thedate_capped/100)as varchar(8)) as date),1,10) 
															||' '|| 
															cast(substring(cast(thetime_capped as varchar(7)),2) as time
														)
											,'yyyy-mm-dd hh:mi:ss'
										)
					end 	as event_end_capped
					,ACTUAL_WEIGHT as WEIGHT_SCALED
					,dk_slot_dim
					,dk_channel_dim
					,DK_BILLING_CUSTOMER_ACCOUNT_DIM
					,DK_BARB_MIN_END_DATEHOUR_DIM
					,DK_VIEWING_EVENT_DIM
					,dk_playback_dim
			from	SMI_DW..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
			where	dk_event_start_datehour_dim between 2013050500 and 2013050523
		)																as fact 
		inner join (
						
						-- a03: calculating/compacting pre-post capping duration at date level...
						
						select	thedate
								,sum(round(((extract(epoch from event_end-event_start))/60.0),2))						as duration_pre_capped
								,sum(	case 	when event_end_capped is null
												then round(((extract(epoch from event_end-event_start))/60.0),2)
												else round(((extract(epoch from event_end_capped-event_start))/60.0),2)
										end
									)																					as duration_post_capped
						from	(
									
									-- a02: Preparing start,end and capped end dk date times to timestamps for calculatin pre-post capping durations...
									
									select	distinct
											DK_VIEWING_EVENT_DIM					as event_dim
											,DK_DTH_ACTIVE_VIEWING_CARD_DIM 		as box
											,to_timestamp(
															substring	(	
																			cast(cast((dk_event_start_datehour_dim/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(dk_event_start_time_dim as varchar(7)),2) as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)																														as event_start
											,to_timestamp(
															substring	(	
																			cast(cast((dk_event_end_datehour_dim/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(dk_event_end_time_dim as varchar(7)),2) as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)																														as event_end
											,case 	when DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM <0 then null else DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM end as thedate_capped
											,case 	when DK_CAPPED_EVENT_END_TIME_DIM <0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as thetime_capped
											,case 	when thedate_capped>0 
													then
													to_timestamp(
																	substring	(	
																					cast(cast((thedate_capped/100)as varchar(8)) as date),1,10) 
																					||' '|| 
																					cast(substring(cast(thetime_capped as varchar(7)),2) as time
																				)
																	,'yyyy-mm-dd hh:mi:ss'
																)
											end 																																as event_end_capped
											,dk_event_start_datehour_dim/100																									as thedate
									FROM	SMI_DW.SMI_ETL.VIEWING_PROGRAMME_INSTANCE_FACT
									where	dk_event_start_datehour_dim between 2013050500 and 2013050523
								) 	as base
						group	by	thedate
					)													as fact2
		on	fact.dt = fact2.thedate
		left join smi_access..V_BILLING_CUSTOMER_ACCOUNT_DIM			as billing
		on	fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM = billing.PK_BILLING_CUSTOMER_ACCOUNT_DIM
		left join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 			as scaling 
		on	billing.account_number = scaling.account_number
		and	scaling.event_start_date = '2013-05-05'
		left join DIS_PREPARE..TD_CUSTOMER_ATTRIBUTES 					as customer
		on	customer.scms_subscriber_id = scaling.scms_subscriber_id
		left join smi_dw..VIEWING_EVENT_DIM								as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
		left join smi_dw..playback_dim									as playback
		on	fact.dk_playback_dim = playback.PK_PLAYBACK_DIM
where	event.panel_id = 12
group	by	dt
order	by 	dt desc -- 60.786.316 -- 1,813,667,882

