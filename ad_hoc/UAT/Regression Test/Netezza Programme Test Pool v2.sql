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
                                     $$$             ,

                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES
					  
--------------------------------------------------------------------------------------------------------------

**Project Name: 					PROGRAMME REGRESSION TESTS
**Analysts:							Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
									Jason Thompson
**Lead(s):							Jose Loureda
**Stakeholder:						VESPA TEAM / EDM

									
**Business Brief:

	To Provide Checks/counts we can then benchmark in order to measure the quality of results produced on each release

**Sections:

	A: INTO PROGRAMME
	B: CAPPING
	C: MINUTE ATTRIBUTION
	D: SCALING
	E: CHANNEL MAPPING
	F: DATA COMPLETENESS
	G: DATA INTEGRITY
	
*/
/*
		Daily Panel ID		Alternate Panel ID(s)
PSTN	12					6, 7
BB		11					5
*/
-----------------------
/* A: INTO PROGRAMME */
-----------------------

select	'I1' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct fact.DK_PROGRAMME_INSTANCE_DIM) as float)/cast(count(distinct slot_instance.PK_PROGRAMME_INSTANCE_DIM_ASOC)as float)	as slot_instance_coverage
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join SMI_ACCESS.ADMIN.V_PROGRAMME_INSTANCE_DIM							as slot_instance
		on	fact.DK_PROGRAMME_INSTANCE_DIM = slot_instance.PK_PROGRAMME_INSTANCE_DIM_ASOC
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023 -- [DONE]
and		event.panel_id in (12,11)
group	by	thedate
union	all
select	'I2' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct fact.DK_PROGRAMME_DIM)as float)/cast(count(distinct slot.PK_PROGRAMME_DIM)as float)	as slot_coverage
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join SMI_ACCESS.ADMIN.V_PROGRAMME_DIM											as slot
		on	fact.DK_PROGRAMME_DIM = slot.PK_PROGRAMME_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023 -- [DONE]
and		event.panel_id in (12,11)
group	by	thedate
union 	all
select	'I7' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct fact.DK_CHANNEL_DIM)as float)/cast(count(distinct channel.PK_CHANNEL_DIM)as float)	as channel_coverage
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join SMI_ACCESS.ADMIN.V_channel_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023 -- [DONE]
and		event.panel_id in (12,11)
group	by	thedate
union	all
-- Check duration of events not null
select	'I9' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(1)
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
and		(duration is null or duration < 0) -- [DONE]
group	by	thedate
union	all
-- Check if PK of SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC is duplicated
select	'I10' as index_
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct PK_VIEWING_PROGRAMME_INSTANCE_FACT)as float)/cast(count(1)as float) as slot_pk_numdup
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023 -- [DONE]
and		event.panel_id in (12,11)
group	by	thedate



----------------
/* B: CAPPING */
----------------


-- Check we have values for cap end date and time in the fact table

select	'C1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when (DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM > 0) then 1 else 0 end)as float) / cast(count(1)as float)	as capped_proportion
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
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
			from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
					left join smi_access..V_VIEWING_EVENT_DIM 										as event
					on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
			where	dk_event_start_datehour_dim between 2015020600 and 2015021023
			and		event.panel_id in (12,11)
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
			from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
					left join smi_access..V_VIEWING_EVENT_DIM 										as event
					on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
			where	dk_event_start_datehour_dim between 2015020600 and 2015021023
			and		event.panel_id in (12,11)
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
			from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
					left join smi_access..V_VIEWING_EVENT_DIM 										as event
					on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
			where	dk_event_start_datehour_dim between 2015020600 and 2015021023
			and		event.panel_id in (12,11)
		) as base
group	by	thedate
union	all


-- Check we don't have capped date and not time

select	'C5-1'
		,dk_event_start_datehour_dim/100 	as thedate
		,sum(case when (DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM in (null,-1) and DK_CAPPED_EVENT_END_TIME_DIM > 0)then 1 else 0 end) as capped_date_no_time
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- Check we don't have capped time and not date

select	'C5-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM > 0 and DK_CAPPED_EVENT_END_TIME_DIM in (null,-1))then 1 else 0 end )as capped_date_no_time
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- Check we have values for the full, partial flag available in the fact

select	'C6-1'
		,fact.dk_event_start_datehour_dim/100 	as thedate
		,count(1) as num_fullcapped_flag
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_VIEWING_INSTANCE_DIM									as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM		
where	fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
and		inst.CAPPED_FULL_FLAG = '1' -- 3,325,805
and		event.panel_id in (12,11)
group	by 	thedate
union	all

select	'C6-2'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,count(1) as num_parcapped_flag
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_VIEWING_INSTANCE_DIM									as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
and		inst.CAPPED_PARTIAL_FLAG = '1' -- 1,172,877
and		event.panel_id in (12,11)
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
			FROM	(
						select	DK_VIEWING_EVENT_DIM
								,DK_DTH_ACTIVE_VIEWING_CARD_DIM
								,dk_event_start_datehour_dim
								,dk_event_start_time_dim
								,dk_event_end_datehour_dim
								,dk_event_end_time_dim
								,DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM
								,DK_CAPPED_EVENT_END_TIME_DIM
						from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
								left join smi_access..V_VIEWING_EVENT_DIM 										as event
								on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
						where	dk_event_start_datehour_dim between 2015020600 and 2015021023
						and		event.panel_id in (12,11)
					)as stage1
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
			FROM	(
						select	DK_VIEWING_EVENT_DIM
								,DK_DTH_ACTIVE_VIEWING_CARD_DIM
								,dk_event_start_datehour_dim
								,dk_event_start_time_dim
								,dk_event_end_datehour_dim
								,dk_event_end_time_dim
								,DK_CAPPED_EVENT_END_TIME_DATEHOUR_DIM
								,DK_CAPPED_EVENT_END_TIME_DIM
						from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT  								as fact
								left join smi_access..V_VIEWING_EVENT_DIM 										as event
								on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
						where	dk_event_start_datehour_dim between 2015020600 and 2015021023
						and		event.panel_id in (12,11)
					)as stage1
		) 	as base
group	by	thedate
union	all


-- Check hours viewed per box pre-post capped duration for both programmes/slots
/* New Query*/
-- POST-CAPPED
select  'C8-2' as theindex
                ,thedate
                ,((sum(capdiff)/60)/60)/count(distinct box)
from    (
                        select  box
                                        ,to_timestamp   (
                                                                                substring(cast(cast((e1/100)as varchar(8)) as date),1,10)
                                                                                ||' '||
                                                                                cast(substring(cast(e2 as varchar(7)),2) as time)
                                                                                ,'yyyy-mm-dd hh:mi:ss'
                                                                        )   as e_start
                                        ,to_timestamp   (
                                                                                substring(cast(cast((e3/100)as varchar(8)) as date),1,10)
                                                                                ||' '||
                                                                                cast(substring(cast(e4 as varchar(7)),2) as time)
                                                                                ,'yyyy-mm-dd hh:mi:ss'
                                                                        )   as e_end
                                        ,to_timestamp   (
                                                                                substring(cast(cast((i1/100)as varchar(8)) as date),1,10)
                                                                                ||' '|| 
                                                                                cast(substring(cast(i2 as varchar(7)),2) as time)
                                                                                ,'yyyy-mm-dd hh:mi:ss'
                                                                        )   as i_start
                                        ,to_timestamp   (
                                                                                substring(cast(cast((i3/100)as varchar(8)) as date),1,10)
                                                                                ||' '|| 
                                                                                cast(substring(cast(i4 as varchar(7)),2) as time)
                                                                                ,'yyyy-mm-dd hh:mi:ss'
                                                                        )   as i_end
                                        ,case   when i_end < cap then extract(epoch from i_end-i_start)
                                                        when i_end = cap then extract(epoch from cap-i_start)
                                                        when i_end > cap then null
                                        end             as capdiff
                                        ,cap
                                        ,thedate
                        from    (
                                                select  fact.dk_event_start_datehour_dim/100    as thedate
                                                                ,fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM    as box
                                                                ,cap.EVENT_END_CAPPED_DATETIME                  as cap
                                                                ,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
                                                                ,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
                                                                ,case when dk_event_end_datehour_dim < 0 then null else dk_event_end_datehour_dim end as e3
                                                                ,case when dk_event_end_time_dim < 0 then null else dk_event_end_time_dim end as e4
                                                                ,case when dk_instance_start_datehour_dim < 0 then null else dk_instance_start_datehour_dim end as i1
                                                                ,case when dk_instance_start_time_dim < 0 then null else dk_instance_start_time_dim end as i2
                                                                ,case when dk_instance_end_datehour_dim < 0 then null else dk_instance_end_datehour_dim end as i3
                                                                ,case when dk_instance_end_time_dim < 0 then null else dk_instance_end_time_dim end as i4
                                                from    SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT                              as fact
                                                                left join DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY    as cap
                                on      fact.DTH_VIEWING_EVENT_ID = cap.DTH_VIEWING_EVENT_ID
                                where   fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
                                        )       as base
                )       as base2
group   by      thedate

union	all

-- Check no overlap between partial or full capped flag

select	'C9'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when (inst.CAPPED_FULL_FLAG > 0 and inst.CAPPED_PARTIAL_FLAG > 0)then 1 else 0 end) as capped_flag_overlaps
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_VIEWING_INSTANCE_DIM									as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
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
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_VIEWING_INSTANCE_DIM									as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate



---------------------------
/* C: MINUTE ATTRIBUTION */
---------------------------

-- Checking no duplicated VIEWING_EVENT_ID on FINAL_MINUTE_ATTRIBUTION

select 'M1'
,dk_event_start_datehour_dim/100 as thedate
                ,cast(count(distinct a.DTH_VIEWING_EVENT_ID)as float)/cast(count(distinct b.DTH_VIEWING_EVENT_ID)as float) as unique_count
from SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT as a
                inner join DIS_REFERENCE..FINAL_MINUTE_ATTRIBUTION_HISTORY as b
                on a.DTH_VIEWING_EVENT_ID = b.DTH_VIEWING_EVENT_ID
                left join smi_access..V_VIEWING_EVENT_DIM as event
                on a.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where a.dk_event_start_datehour_dim between 2015020600 and 2015021023
and event.panel_id in (12,11)
group by thedate

union	all

-- Check programmes are been attributed

select	'M2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when DK_BARB_MIN_END_DATEHOUR_DIM > 0 then 1 else 0 end)	as num_rec_ma
from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
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
from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
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
from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group 	by	thedate
union	all

select	'M4-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						DK_BARB_MIN_END_DATEHOUR_DIM > 0 
						and (DK_BARB_MIN_START_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_START_TIME_DIM in (null,-1))
						)
						then 1 else 0 end)	as barb_end_no_start
from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
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
									from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
											left join smi_access..V_VIEWING_EVENT_DIM 										as event
											on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
									where	dk_event_start_datehour_dim between 2015020600 and 2015021023 -- extracting the sample...
									and		event.panel_id in (12,11)
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

	
		
-- Check number of records been attributed (Volumen and Proportion, compare this against threshold)

select	'M7-1'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when fact.DK_BARB_MIN_START_DATEHOUR_DIM > 0 then 1 else 0 end )as float)/cast(count(1) as float) as prop_rec_Ma
from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_VIEWING_INSTANCE_DIM									as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
		left join SMI_ACCESS.ADMIN.V_PLAYBACK_DIM											as playback
		on	fact.dk_playback_dim = playback.PK_PLAYBACK_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
and		inst.capped_full_flag in (null,0)
and		playback.type_of_viewing_event in (
												'HD Viewing Event'
												,'Sky+ time-shifted viewing event '
												,'TV Channel Viewing'
											)
group	by	thedate
union	all
select	'M7-2'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when fact.DK_BARB_MIN_START_DATEHOUR_DIM > 0 then 1 else 0 end ) t_rec_ma
from 	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_VIEWING_INSTANCE_DIM									as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
		left join SMI_ACCESS.ADMIN.V_PLAYBACK_DIM											as playback
		on	fact.dk_playback_dim = playback.PK_PLAYBACK_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
and		inst.capped_full_flag in (null,0)
and		playback.type_of_viewing_event in (
												'HD Viewing Event'
												,'Sky+ time-shifted viewing event '
												,'TV Channel Viewing'
											)
group	by	thedate


----------------
/* D: SCALING */
----------------

-- Number/ proportion of events with weight assigned

select	'S1-1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when weight_scaled > 0 then 1 else 0 end)as float)/cast(count(1)as float) as prop_rec_scaled
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all
select	'S1-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when WEIGHT_SCALED >0 then 1 else 0 end) as num_rec_scaled
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- Check no difference between weights in fact and source

select	'S2'
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when fact.WEIGHT_SCALED <> scaling.WEIGHT_SCALED_VALUE then 1 else 0 end ) as weight_diff
from	SMI_DW..VIEWING_PROGRAMME_INSTANCE_FACT	 						as fact 
		left join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY  					as scaling 
		on	fact.dth_viewing_event_id = scaling.dth_viewing_event_id
		and	scaling.event_start_date between '2015-02-06 00:00:00' and '2015-02-10 23:59:59'
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Check all records matchign scaling source have been attributed

select	'S3'
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when fact.WEIGHT_SCALED > 0 then 1 else 0 end)as float)/cast(count(1)as float) as prop_rec_weightsourced
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 					as fact 
		inner join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY  					as scaling 
		on	fact.dth_viewing_event_id = scaling.dth_viewing_event_id
		and	scaling.event_start_date between '2015-02-06 00:00:00' and '2015-02-10 23:59:59'
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- Check we have a weight asigned where the flag = 1

select	'S4'
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when (WEIGHT_SAMPLE > 0 and WEIGHT_SCALED is null) then 1 else 0 end) num_scaled_weightless
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- Check we have a weight asigned where the flag = 1

select	'S5'
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when (WEIGHT_SAMPLE = 0 and WEIGHT_SCALED > 0) then 1 else 0 end) num_weight_flagless
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- S6 Comming Soon...


-- S7-1 Check Convergence Pre-Stage
select  'S7-1' as theindex
                ,substring(event_start_date,1,4)||substring(event_start_date,6,2)||substring(event_start_date,9,2) as thedate
                ,round(sum(thetotal),0) / min(meta.SKY_BASE) as thevalue
from    (
                        select  event_start_date,
                                         SCALING_ATTRIBUTE_01,
                                         SCALING_ATTRIBUTE_02,
                                         SCALING_ATTRIBUTE_03,
                                         SCALING_ATTRIBUTE_04,
                                         SCALING_ATTRIBUTE_05,
                                         SCALING_ATTRIBUTE_06,
                                         SCALING_ATTRIBUTE_07
                                        ,sum(WEIGHT_SCALED_PRE_UPLIFT_VALUE) as thetotal
                        FROM    DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY 
                        where   event_start_date between '2015-02-06 00:00:00' and '2015-02-10 23:59:59' --
                        group   by      event_start_date,
                                         SCALING_ATTRIBUTE_01,
                                         SCALING_ATTRIBUTE_02,
                                         SCALING_ATTRIBUTE_03,
                                         SCALING_ATTRIBUTE_04,
                                         SCALING_ATTRIBUTE_05,
                                         SCALING_ATTRIBUTE_06,
                                         SCALING_ATTRIBUTE_07
                ) as base
                left join dis_reference..SCALING_METADATA as meta
                on      base.event_Start_date between meta.effective_from and meta.effective_to
group   by      thedate
union all


select	'S7-2' as theindex
		,substring(event_start_date,1,4)||substring(event_start_date,6,2)||substring(event_start_date,9,2) as thedate
		,min(meta.SKY_BASE) - round(sum(thetotal),0) as thevalue
from	(
			select	event_start_date,
					 SCALING_ATTRIBUTE_01,
					 SCALING_ATTRIBUTE_02, 
					 SCALING_ATTRIBUTE_03, 
					 SCALING_ATTRIBUTE_04, 
					 SCALING_ATTRIBUTE_05,
					 SCALING_ATTRIBUTE_06,
					 SCALING_ATTRIBUTE_07
					,sum(WEIGHT_SCALED_VALUE) as thetotal
			FROM 	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY 
			where	event_start_date between '2015-02-06 00:00:00' and '2015-02-10 23:59:59' --
			group	by	event_start_date,
					 SCALING_ATTRIBUTE_01,
					 SCALING_ATTRIBUTE_02, 
					 SCALING_ATTRIBUTE_03, 
					 SCALING_ATTRIBUTE_04, 
					 SCALING_ATTRIBUTE_05,
					 SCALING_ATTRIBUTE_06,
					 SCALING_ATTRIBUTE_07
		) as base
		left join dis_reference..SCALING_METADATA as meta
		on	base.event_Start_date between meta.effective_from and meta.effective_to
group	by	thedate
order 	by	thedate;


------------------------
/* E: CHANNEL MAPPING */
------------------------


-- Proportion of channels matching with the dimension

select	'CH1-1' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct channel.PK_CHANNEL_DIM)as float)/cast(count(distinct fact.DK_CHANNEL_DIM)as float) as value_
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_channel_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all

-- Number of channels matching with the dimension


select	'CH1-2' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,count(distinct channel.PK_CHANNEL_DIM) as value_
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_channel_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Checking all channel names are in place


select	'CH2' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when channel.CHANNEL_NAME is null then 1 else 0 end)as float)/cast(count(1)as float) as value_
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		inner join SMI_ACCESS.ADMIN.V_channel_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Checking all Channel Genre are in place (null or -1 count)


select	'CH3' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when channel.CHANNEL_GENRE is null then 1 else 0 end)as float)/cast(count(1)as float) as value_
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		inner join SMI_ACCESS.ADMIN.V_channel_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Checking all service keys are in place (null or -1 count)


select	'CH4' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when channel.SERVICE_KEY is null then 1 else 0 end)as float)/cast(count(1)as float) as value_
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		inner join SMI_ACCESS.ADMIN.V_channel_DIM											as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate



--------------------------
/* F: DATA COMPLETENESS */
--------------------------

-- Check how many records have we got per day

select	'D1' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(1) as hits
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM

select	'D2' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_PROGRAMME_INSTANCE_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float) as value
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Check proportion of records without a DK_PROGRAMME_DIM

select	'D3' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_PROGRAMME_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float) as value
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Check proportion of records without a DK_CHANNEL_DIM

select	'D4' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_CHANNEL_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float) as value
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Check number of accounts per day

select	'D5' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(distinct cust.account_number) as value
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_BILLING_CUSTOMER_ACCOUNT_DIM								as billing
		on	fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM = billing.PK_BILLING_CUSTOMER_ACCOUNT_DIM
		left join dis_prepare..TD_CUSTOMER_ATTRIBUTES	as cust
		on	billing.ACCOUNT_NUMBER = cust.ACCOUNT_NUMBER
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate
union	all


-- Check number of boxes per day

select	'D6' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(distinct cust.SCMS_SUBSCRIBER_ID) as value
from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
		left join SMI_ACCESS.ADMIN.V_BILLING_CUSTOMER_ACCOUNT_DIM								as billing
		on	fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM = billing.PK_BILLING_CUSTOMER_ACCOUNT_DIM
		left join dis_prepare..TD_CUSTOMER_ATTRIBUTES							as cust
		on	billing.ACCOUNT_NUMBER = cust.ACCOUNT_NUMBER
		left join smi_access..V_VIEWING_EVENT_DIM 										as event
		on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
where	dk_event_start_datehour_dim between 2015020600 and 2015021023
and		event.panel_id in (12,11)
group	by	thedate



-----------------------
/* G: DATA INTEGRITY */
-----------------------


-- Checking after PKs been duplicated in the fact table...

select	'B1' 					as theindex
		,thedate
		,count(distinct thepk)	as thevalue
FROM	(
			select	dk_event_start_datehour_dim/100 as thedate
					,PK_VIEWING_PROGRAMME_INSTANCE_FACT as thepk
					,count(1) as hits
			from	SMI_dw..VIEWING_PROGRAMME_INSTANCE_FACT 									as fact
					left join smi_access..V_VIEWING_EVENT_DIM 										as event
					on	fact.DK_VIEWING_EVENT_DIM = event.PK_VIEWING_EVENT_DIM
			where	dk_event_start_datehour_dim between 2015020600 and 2015021023
			and		event.panel_id in (12,11)
			group	by	thedate
						,PK_VIEWING_PROGRAMME_INSTANCE_FACT
			having	count(1)> 1
		) as base
group	by	thedate
