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

**Project Name: 					PROGRAMME REGRESSION TESTS (SYBASE VERSION)
**Analysts:							Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
									Angai Maruthavanan	(Angai.Maruthavanan@SkyIQ.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						VESPA TEAM / CBI

									
**Business Brief:

	To Provide Checks/counts we can then benchmark in order to measure the quality of results produced on each release

**Sections:


*/

--------------------
/* INTO PROGRAMME */
--------------------

select	'I1' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct fact.DK_PROGRAMME_INSTANCE_DIM) as float)/cast(count(distinct slot_instance.PK_PROGRAMME_INSTANCE_DIM_ASOC)as float)	as slot_instance_coverage
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT 								as fact
		left join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC							as slot_instance
		on	fact.DK_PROGRAMME_INSTANCE_DIM = slot_instance.PK_PROGRAMME_INSTANCE_DIM_ASOC
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2013040100 and 2013040623 -- [DONE]
group	by	thedate
union	all
select	'I2' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct fact.DK_PROGRAMME_DIM)as float)/cast(count(distinct slot.PK_PROGRAMME_DIM)as float)	as slot_coverage
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT 								as fact
		left join smi_dw..PROGRAMME_DIM											as slot
		on	fact.DK_PROGRAMME_DIM = slot.PK_PROGRAMME_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2013040100 and 2013040623 -- [DONE]
group	by	thedate
union 	all
select	'I7' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct fact.DK_CHANNEL_DIM)as float)/cast(count(distinct channel.PK_CHANNEL_DIM)as float)	as channel_coverage
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT 								as fact
		left join smi_dw..CHANNEL_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2013040100 and 2013040623 -- [DONE]
group	by	thedate
union	all
-- Check duration of events not null
select	'I9' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(1)
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
and		(duration is null or duration < 0) -- [DONE]
group	by	thedate
union	all
-- Check if PK of SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC is duplicated
select	'I10' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct PK_VIEWING_PROGRAMME_INSTANCE_FACT)as float)/cast(count(1)as float) as slot_pk_numdup
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623 -- [DONE]
group	by	thedate
/* THIS COULD BE USEFULL IN THE FUTURE...
select	count(1)
from	(
			select	box
					,event_start
					,event_end
					,min(broadcast_start) 	as broadcast_lowend
					,max(broadcast_end)		as broadcast_highend
			from	(
						select	DK_DTH_ACTIVE_VIEWING_CARD_DIM																				as box
								,case when DK_EVENT_START_DATEHOUR_DIM < 0 			then null else DK_EVENT_START_DATEHOUR_DIM end 			as e1
								,case when DK_EVENT_START_TIME_DIM < 0 				then null else DK_EVENT_START_TIME_DIM end 				as e2
								,to_timestamp(
															substring	(	
																			cast(cast((e1/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(e2 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)																					as event_start
								,case when DK_EVENT_END_DATEHOUR_DIM < 0 			then null else DK_EVENT_END_DATEHOUR_DIM end 			as e3
								,case when DK_EVENT_END_TIME_DIM < 0 				then null else DK_EVENT_END_TIME_DIM end 				as e4
								,to_timestamp(
															substring	(	
																			cast(cast((e3/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(e4 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)																					as event_end
								,case when DK_BROADCAST_START_DATEHOUR_DIM < 0 	then null else DK_BROADCAST_START_DATEHOUR_DIM end 			as b1
								,case when DK_BROADCAST_START_TIME_DIM < 0 		then null else DK_BROADCAST_START_TIME_DIM end 				as b2
								,to_timestamp(
															substring	(	
																			cast(cast((b1/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(b2 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)																					as broadcast_start
								,case when DK_BROADCAST_END_DATEHOUR_DIM < 0 		then null else DK_BROADCAST_END_DATEHOUR_DIM end 		as b3
								,case when DK_BROADCAST_END_TIME_DIM < 0 			then null else DK_BROADCAST_END_TIME_DIM end 			as b4
								,to_timestamp(
															substring	(	
																			cast(cast((b3/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(b4 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)																					as broadcast_end
						from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
						where	DK_EVENT_START_DATEHOUR_DIM between 2013050500 and 2013050523
					) as Stage1
			group	by	box
						,event_start
						,event_end
			order	by	box
						,event_start
						,event_end
		) as stagef
where	broadcast_lowend < event_start
or		broadcast_highend > event_end



*/

-------------
/* CAPPING */
-------------

-- Check we have values for cap end date and time in the fact table

select	'C1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when (DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM > 0) then 1 else 0 end)as float) / cast(count(1)as float)	as capped_proportion
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union 	all
-- Check capped end time is greater thant event start time

select	'C2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when capped_end < event_start then 1 else 0 end)	as capped_before_start
from	(
			select	case when DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM end as c1
					,case when DK_CAPPED_EVENT_END_TIME_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as c2
					,to_timestamp(
												substring	(	
																cast(cast((c1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(c2 as varchar(7)),2)	as time
															)
												,'yyyy-mm-dd hh:mi:ss'
											)								as capped_end
					,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
					,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
					,to_timestamp(
												substring	(	
																cast(cast((e1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(e2 as varchar(7)),2)	as time
															)
												,'yyyy-mm-dd hh:mi:ss'
											)								as event_start
					,dk_event_start_datehour_dim
			from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
			where	dk_event_start_datehour_dim between 2013040100 and 2013040623
		) as base
group	by	thedate
union	all

-- Check capped end time is less than or equal event end time

select	'C3'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when capped_end > event_end then 1 else 0 end)	as capped_after_end
from	(
			select	case when DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM end as c1
					,case when DK_CAPPED_EVENT_END_TIME_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as c2
					,to_timestamp(
												substring	(	
																cast(cast((c1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(c2 as varchar(7)),2)	as time
															)
												,'yyyy-mm-dd hh:mi:ss'
											)								as capped_end
					,case when dk_event_end_datehour_dim < 0 then null else dk_event_end_datehour_dim end as e1
					,case when dk_event_end_time_dim < 0 then null else dk_event_end_time_dim end as e2
					,to_timestamp(
												substring	(	
																cast(cast((e1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(e2 as varchar(7)),2)	as time
															)
												,'yyyy-mm-dd hh:mi:ss'
											)								as event_end
					,dk_event_start_datehour_dim
			from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
			where	dk_event_start_datehour_dim between 2013040100 and 2013040623
		) as base
group	by	thedate
union	all

-- Check capped end time is less than or equal event end time

select	'C4'
		,dk_event_start_datehour_dim/100 	as thedate
		,sum(case when capped_end <= event_start then 1 else 0 end)	as capped_before_start
from	(
			select	case when DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM end as c1
					,case when DK_CAPPED_EVENT_END_TIME_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as c2
					,to_timestamp(
												substring	(	
																cast(cast((c1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(c2 as varchar(7)),2)	as time
															)
												,'yyyy-mm-dd hh:mi:ss'
											)								as capped_end
					,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
					,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
					,to_timestamp(
												substring	(	
																cast(cast((e1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(e2 as varchar(7)),2)	as time
															)
												,'yyyy-mm-dd hh:mi:ss'
											)								as event_start
					,dk_event_start_datehour_dim
			from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
			where	dk_event_start_datehour_dim between 2013042900 and 2013050423
		) as base
group	by	thedate
union	all


-- Check we don't have capped date and not time

select	'C5-1'
		,dk_event_start_datehour_dim/100 	as thedate
		,sum(case when (DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM in (null,-1) and DK_CAPPED_EVENT_END_TIME_DIM > 0)then 1 else 0 end) as capped_date_no_time
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check we don't have capped time and not date

select	'C5-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM > 0 and DK_CAPPED_EVENT_END_TIME_DIM in (null,-1))then 1 else 0 end )as capped_date_no_time
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check we have values for the full, partial flag available in the fact

select	'C6-1'
		,fact.dk_event_start_datehour_dim/100 	as thedate
		,count(1) as num_fullcapped_flag
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 	as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between 2013040100 and 2013040623
and		inst.CAPPED_FULL_FLAG > 0 -- 3,325,805
group	by 	thedate
union	all

select	'C6-2'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,count(1) as num_parcapped_flag
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 	as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between 2013040100 and 2013040623
and		inst.CAPPED_PARTIAL_FLAG > 0 -- 1,172,877
group	by	thedate
union	all


-- Check pre-post capped duration for programmes

select	'C7-1 pre'
		,thedate
		,sum(round(((extract(epoch from event_end-event_start))/60.0),2))						as duration_pre_capped
from	(
			
			-- Preparing start,end and capped end dk date times to timestamps for calculatin pre-post capping durations...
			
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
			where	dk_event_start_datehour_dim between 2013040100 and 2013040623
		) 	as base
group	by	thedate
union	all
select	'C7-2 post'
		,thedate
		,sum(	case 	when event_end_capped is null
						then round(((extract(epoch from event_end-event_start))/60.0),2)
						else round(((extract(epoch from event_end_capped-event_start))/60.0),2)
				end
			)																					as duration_post_capped
from	(
			
			-- Preparing start,end and capped end dk date times to timestamps for calculatin pre-post capping durations...
			
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
			where	dk_event_start_datehour_dim between 2013040100 and 2013040623
		) 	as base
group	by	thedate
union	all

-- Check hours viewed per box pre-post capped duration for both programmes/slots


select	'C8-1 pre'
		,thedate
		,(sum(round(((extract(epoch from event_end-event_start))/60.0),2))/60)/ count(distinct box)													as pre_capped_hours_viewed
from	(
			
			-- Preparing start,end and capped end dk date times to timestamps for calculatin pre-post capping durations...
			
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
			where	dk_event_start_datehour_dim between 2013040100 and 2013040623
		) 	as base
group	by	thedate
union	all
select	'C8-2 post'
		,thedate
		,(sum(	case 	when event_end_capped is null
						then round(((extract(epoch from event_end-event_start))/60.0),2)
						else round(((extract(epoch from event_end_capped-event_start))/60.0),2)
				end
			)/60)
		/ 
		count(distinct box) 													as post_capped_hours_viewed
from	(
			
			-- Preparing start,end and capped end dk date times to timestamps for calculatin pre-post capping durations...
			
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
			where	dk_event_start_datehour_dim between 2013040100 and 2013040623
		) 	as base
group	by	thedate
union	all

-- Check no overlap between partial or full capped flag

select	'C9'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when (inst.CAPPED_FULL_FLAG > 0 and inst.CAPPED_PARTIAL_FLAG > 0)then 1 else 0 end) as capped_flag_overlaps
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 	as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between 2013042900 and 2013050423
group	by	thedate
union	all

-- Check capped date/time not null if capped flag is 1

select	'C10'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						(inst.CAPPED_FULL_FLAG > 0 or inst.CAPPED_PARTIAL_FLAG > 0)
						and (fact.DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM in (null,-1) 
							 or fact.DK_CAPPED_EVENT_END_TIME_DIM in (null,-1)
							)
						) then 1 else 0 end
			) as capped_flag_no_datetime
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 	as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between 2013042900 and 2013050423
group	by	thedate


------------------------
/* MINUTE ATTRIBUTION */
------------------------

-- Checking no duplicated VIEWING_EVENT_ID on FINAL_MINUTE_ATTRIBUTION

select	'M1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(count(distinct b.VIEWING_EVENT_ID)as float)/cast(count(b.VIEWING_EVENT_ID)as float)	as unique_count
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT				as a
		inner join dis_prepare..FINAL_MINUTE_ATTRIBUTION 	as b
		on	a.DTH_VIEWING_EVENT_ID = b.VIEWING_EVENT_ID
where	a.dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check programmes are been attributed

select	'M2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when DK_BARB_MIN_END_DATEHOUR_DIM > 0 then 1 else 0 end)	as num_rec_ma
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check for attributted programmes we have barb start minute and barb end minute

select	'M3'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						(DK_BARB_MIN_END_DATEHOUR_DIM in (null,-1) and DK_BARB_MIN_END_TIME_DIM > 0)
						or	(DK_BARB_MIN_START_DATEHOUR_DIM in (null,-1) and DK_BARB_MIN_START_TIME_DIM > 0)
						or	(DK_BARB_MIN_END_DATEHOUR_DIM  > 0 and DK_BARB_MIN_END_TIME_DIM in (null,-1))
						or	(DK_BARB_MIN_START_DATEHOUR_DIM > 0 and DK_BARB_MIN_START_TIME_DIM in (null,-1))
						)
						then 1 else 0 end )	as barb_integrity_check
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all
		
-- Check for cases with barb start but no end minute
-- Check for cases with barb end but no start minute

select	'M4-1'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						DK_BARB_MIN_START_DATEHOUR_DIM > 0 
						and (DK_BARB_MIN_END_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_END_TIME_DIM in (null,-1))
						) 
						then 1 else 0 end )	as barb_start_no_end
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group 	by	thedate
union	all

select	'M4-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						DK_BARB_MIN_END_DATEHOUR_DIM > 0 
						and (DK_BARB_MIN_START_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_START_TIME_DIM in (null,-1))
						)
						then 1 else 0 end)	as barb_end_no_start
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check programmes are not clamining the same minute

select	'M5'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when barb_start = succ_barb_start then 1 else 0 end)	as hits
from	(
			select	box
					,dk_event_start_datehour_dim
					,event_start
					,event_end
					,barb_start
					,barb_end
					,min(barb_start) over 	(	partition by	box
																,event_start
												order by		barb_start
												rows between	1 following and 1 following
											) as succ_barb_start
			from	(
						select	DK_DTH_ACTIVE_VIEWING_CARD_DIM	as box
								,dk_event_start_datehour_dim
								,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
								,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
								,to_timestamp(
															substring	(	
																			cast(cast((e1/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(e2 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as event_start
								,case when dk_event_end_datehour_dim < 0 then null else dk_event_end_datehour_dim end as e3
								,case when dk_event_end_time_dim < 0 then null else dk_event_end_time_dim end as e4
								,to_timestamp(
															substring	(	
																			cast(cast((e3/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(e4 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as event_end
								,case when DK_BARB_MIN_START_DATEHOUR_DIM < 0 then null else DK_BARB_MIN_START_DATEHOUR_DIM end as b1
								,case when DK_BARB_MIN_START_TIME_DIM < 0 then null else DK_BARB_MIN_START_TIME_DIM end as b2
								,to_timestamp(
															substring	(	
																			cast(cast((b1/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(b2 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as barb_start
								,case when DK_BARB_MIN_END_DATEHOUR_DIM < 0 then null else DK_BARB_MIN_END_DATEHOUR_DIM end as b3
								,case when DK_BARB_MIN_END_TIME_DIM < 0 then null else DK_BARB_MIN_END_TIME_DIM end as b4
								,to_timestamp(
															substring	(	
																			cast(cast((b3/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(b4 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as barb_end
						from	(
									select	DK_DTH_ACTIVE_VIEWING_CARD_DIM
											,dk_event_start_datehour_dim
											,dk_event_start_time_dim
											,dk_event_end_datehour_dim
											,dk_event_end_time_dim
											,DK_BARB_MIN_START_DATEHOUR_DIM
											,DK_BARB_MIN_START_TIME_DIM
											,DK_BARB_MIN_END_DATEHOUR_DIM
											,DK_BARB_MIN_END_TIME_DIM
									from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
									where	dk_event_start_datehour_dim between 2013040100 and 2013040623 -- extracting the sample...
								) as stage1 -- manipulating the sample (Prepareing the dates)...
						order	by	box
									,dk_event_start_datehour_dim
									,event_start
									,event_end
									,barb_start
									,barb_end
					) as stage2 -- lead/lag function for barb_start to enable the check... 
			order	by	box
						,dk_event_start_datehour_dim
						,event_start
						,event_end
						,barb_start
						,barb_end
		) as stagef -- Counting cases...
group	by	thedate
union	all


/* -- Check duration of attribution is no greater than 1 min
select	'M6'
		,sum(case when round(((extract(epoch from barb_end-barb_start))/60.0),2) > 1 then 1 else 0 end) as MA_GT_1M
from	(
			select	box
					,event_start
					,event_end
					,barb_start
					,barb_end
					,min(barb_start) over 	(	partition by	box
																,event_start
												order by		barb_start
												rows between	1 following and 1 following
											) as succ_barb_start
			from	(
						select	DK_DTH_ACTIVE_VIEWING_CARD_DIM	as box
								,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
								,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
								,to_timestamp(
															substring	(	
																			cast(cast((e1/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(e2 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as event_start
								,case when dk_event_end_datehour_dim < 0 then null else dk_event_end_datehour_dim end as e3
								,case when dk_event_end_time_dim < 0 then null else dk_event_end_time_dim end as e4
								,to_timestamp(
															substring	(	
																			cast(cast((e3/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(e4 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as event_end
								,case when DK_BARB_MIN_START_DATEHOUR_DIM < 0 then null else DK_BARB_MIN_START_DATEHOUR_DIM end as b1
								,case when DK_BARB_MIN_START_TIME_DIM < 0 then null else DK_BARB_MIN_START_TIME_DIM end as b2
								,to_timestamp(
															substring	(	
																			cast(cast((b1/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(b2 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as barb_start
								,case when DK_BARB_MIN_END_DATEHOUR_DIM < 0 then null else DK_BARB_MIN_END_DATEHOUR_DIM end as b3
								,case when DK_BARB_MIN_END_TIME_DIM < 0 then null else DK_BARB_MIN_END_TIME_DIM end as b4
								,to_timestamp(
															substring	(	
																			cast(cast((b3/100)as varchar(8)) as date),1,10) 
																			||' '|| 
																			cast(substring(cast(b4 as varchar(7)),2)	as time
																		)
															,'yyyy-mm-dd hh:mi:ss'
														)								as barb_end
						from	(
									select	DK_DTH_ACTIVE_VIEWING_CARD_DIM
											,dk_event_start_datehour_dim
											,dk_event_start_time_dim
											,dk_event_end_datehour_dim
											,dk_event_end_time_dim
											,DK_BARB_MIN_START_DATEHOUR_DIM
											,DK_BARB_MIN_START_TIME_DIM
											,DK_BARB_MIN_END_DATEHOUR_DIM
											,DK_BARB_MIN_END_TIME_DIM
									from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
									where	dk_event_start_datehour_dim between 2013040100 and 2013040623 -- extracting the sample...
								) as stage1 -- manipulating the sample (Prepareing the dates)...
						order	by	box
									,event_start
									,event_end
									,barb_start
									,barb_end
					) as stage2 -- lead/lag function for barb_start to enable the check... 
			order	by	box
						,event_start
						,event_end
						,barb_start
						,barb_end
		) as stagef -- Counting cases...
*/	
	
		
-- Check number of records been attributed (Volumen and Proportion, compare this against threshold)

select	'M7-1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_BARB_MIN_START_DATEHOUR_DIM > 0 then 1 else 0 end )as float)/cast(count(1) as float) as prop_rec_Ma
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all
select	'M7-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when DK_BARB_MIN_START_DATEHOUR_DIM > 0 then 1 else 0 end) as num_Rec_MA
from 	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate


-------------
/* SCALING */
-------------

-- Number/ proportion of events with weight assigned

select	'S1-1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when weight_scaled > 0 then 1 else 0 end)as float)/cast(count(1)as float) as prop_rec_scaled
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all
select	'S1-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when WEIGHT_SCALED >0 then 1 else 0 end) as num_rec_scaled
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check no difference between weights in fact and source

select	'S2'
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when fact.WEIGHT_SCALED <> scaling.WEIGHT_SCALED_VALUE then 1 else 0 end ) as weight_diff
from	SMI_DW.SMI_ETL.VIEWING_PROGRAMME_INSTANCE_FACT	 				as fact 
		left join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 			as scaling 
		on	fact.dth_viewing_event_id = scaling.dth_viewing_event_id
		and	scaling.event_start_date between '2013-04-01' and '2013-04-06'
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2013040100 and 2013040623
group	by	thedate
union	all


-- Check all records matchign scaling source have been attributed

select	'S3'
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when fact.WEIGHT_SCALED > 0 then 1 else 0 end)as float)/cast(count(1)as float) as prop_rec_weightsourced
from	SMI_DW.SMI_ETL.VIEWING_PROGRAMME_INSTANCE_FACT	 				as fact 
		inner join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 			as scaling 
		on	fact.dth_viewing_event_id = scaling.dth_viewing_event_id
		and	scaling.event_start_date between '2013-04-01' and '2013-04-06'
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check we have a weight asigned where the flag = 1

select	'S4'
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when (WEIGHT_SAMPLE > 0 and WEIGHT_SCALED is null) then 1 else 0 end) num_scaled_weightless
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate
union	all

-- Check we have a weight asigned where the flag = 1

select	'S5'
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when (WEIGHT_SAMPLE = 0 and WEIGHT_SCALED > 0) then 1 else 0 end) num_weight_flagless
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013040100 and 2013040623
group	by	thedate