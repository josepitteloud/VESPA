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

**Project Name: 					SLOT REGRESSION TESTS
**Analysts:							Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
									Angai Maruthavanan	(Angai.Maruthavanan@SkyIQ.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						VESPA TEAM / CBI

									
**Business Brief:

	To Provide Checks/counts we can then benchmark in order to measure the quality of results produced on each release

**Sections:

	A: INTO SLOT
	B: CAPPING	
	C: MINUTE ATTRIBUTION
	D: SCALING
	E: CHANNEL MAPPING
	F: DATA COMPLETENESS
	G: DATA INTEGRITY (SLOTS)

*/


------------------
/* A: INTO SLOT */
------------------


-- Check all values for DK_SLOT/PROG_INSTANCE_DIM are found in the Slot/prog Instance Dimension table

select	'I1' as index_
		,fact.dk_event_start_datehour_dim/100 as thedate
		,cast(count(distinct fact.DK_SLOT_INSTANCE_DIM) as float)/cast(count(distinct slot_instance.PK_SLOT_INSTANCE_DIM)as float)	as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE 								as fact
		left join smi_dw..SLOT_INSTANCE_DIM										as slot_instance
		on	fact.DK_SLOT_INSTANCE_DIM = slot_instance.PK_SLOT_INSTANCE_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check all values for DK_SLOT/PROG_DIM are found in the Slot/prog Dimension table

select	'I2' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100 as thedate
		,cast(count(distinct fact.DK_SLOT_DIM)as float)/cast(count(distinct slot.PK_SLOT_DIM)as float)	as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..SLOT_DIM					as slot
		on	fact.DK_SLOT_DIM = slot.PK_SLOT_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <FROM> and <TO>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union 	all

-- Check all values for DK_PRECEDING_PROGRAMME_INSTANCE_DIM are found in the Programmes Dimension table

select	'I3' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100 as thedate
		,cast(count(distinct fact.DK_PRECEDING_PROGRAMME_INSTANCE_DIM) as float)/ cast(count(distinct programme_inst.PK_PROGRAMME_INSTANCE_DIM_ASOC)as float)	as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE		as fact
		left join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as programme_inst
		on	fact.DK_PRECEDING_PROGRAMME_INSTANCE_DIM = programme_inst.PK_PROGRAMME_INSTANCE_DIM_ASOC
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

--Check all values for DK_PRECEDING_PROGRAMME_DIM are found in the Programme Instance Dimension table

select	'I4' as theindex
		,fact.DK_EVENT_START_DATEHOUR_DIM/100 as thedate
		,cast(count(distinct fact.DK_PRECEDING_PROGRAMME_DIM)as float)/cast(count(distinct programme.PK_PROGRAMME_DIM)as float)	as programme_coverage
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..PROGRAMME_DIM				as programme
		on	fact.DK_PRECEDING_PROGRAMME_DIM = programme.PK_PROGRAMME_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check all values for DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM are found in the Programme Instance Dimension table

select	'I5' as theindex
		,fact.DK_EVENT_START_DATEHOUR_DIM/100 as thedate
		,cast(count(distinct fact.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM) as float)/ cast(count(distinct programme_inst.PK_PROGRAMME_INSTANCE_DIM_ASOC)as float)	as prog_inst_coverage
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE		as fact
		left join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as programme_inst
		on	fact.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM = programme_inst.PK_PROGRAMME_INSTANCE_DIM_ASOC
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check all values for DK_SUCCEEDING_PROGRAMME_DIM are found in the Programmes Dimension table

select	'I6' as theindex
		,fact.DK_EVENT_START_DATEHOUR_DIM/100 as thedate
		,cast(count(distinct fact.DK_SUCCEEDING_PROGRAMME_DIM)as float)/cast(count(distinct programme.PK_PROGRAMME_DIM)as float)	as programme_coverage
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE 	as fact
		left join smi_dw..PROGRAMME_DIM				as programme
		on	fact.DK_SUCCEEDING_PROGRAMME_DIM = programme.PK_PROGRAMME_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check all values for DK_CHANNEL_DIM are found in the Channels Dimension table

select	'I7' as theindex
		,fact.DK_EVENT_START_DATEHOUR_DIM/100 as thedate
		,cast(count(distinct fact.DK_CHANNEL_DIM)as float)/cast(count(distinct channel.PK_CHANNEL_DIM)as float)	as channel_coverage
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE 	as fact
		left join smi_dw..CHANNEL_DIM				as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check after null or -1 for preceding programme dim

select	'I8-1' as theindex
		,dk_event_start_datehour_dim/100 as thedate
		,cast(sum(case when DK_PRECEDING_PROGRAMME_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float)	as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union 	all

-- Check after null or -1 for preceding programme Instance Dim

select	'I8-2' as theindex
		,dk_event_start_datehour_dim/100 as thedate
		,cast(sum(case when DK_PRECEDING_PROGRAMME_INSTANCE_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float)	as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check after null or -1 for succeeding programme dim

select	'I8-3' as theindex
		,dk_event_start_datehour_dim/100 as thedate
		,cast(sum(case when DK_SUCCEEDING_PROGRAMME_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float)	as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union 	all

-- Check after null or -1 for succeeding programme Instance Dim

select	'I8-4' as theindex	
		,dk_event_start_datehour_dim/100 as thedate
		,cast(sum(case when DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float)	as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union 	all

-- Count records with duration set to null or -1

select	'I9' as theindex
		,dk_event_start_datehour_dim/100 as thedate
		,sum(case when (SLOT_DURATION_VIEWED is null or SLOT_DURATION_VIEWED < 0) then 1 else 0 end) as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check if PK of SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_VOLATILE is unique

select	'I10' as theindex
		,dk_event_start_datehour_dim/100 as thedate
		,cast(count(distinct PK_VIEWING_SLOT_INSTANCE_FACT)as float)/cast(count(1)as float) as thevalue
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union 	all

-- Spots shown between same programme do not exceed broadcast time constraints

select	'I11' as theindex
		,event_start/100 as thedate
		,sum(case when	(
							prec_prog = succ_prog
							and
							(slot_date > succ_date or slot_date < prec_date)
						) then 1 else 0 end
			) as thevalue
from	(
			-- Reformating the dim dates to actual timestamps to handle them as needed...
			select	stage1.event_start
					,stage1.prec_prog
					,to_timestamp(
									substring	(cast(cast((stage1.prec_start/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.prec_stime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as prec_date
					,stage1.slot
					,to_timestamp(
									substring	(cast(cast((stage1.slot_start_date/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.slot_start_time as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as slot_date
					,stage1.succ_prog
					,to_timestamp(
									substring	(cast(cast((stage1.succ_end/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.succ_etime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as succ_date
			from	(
						-- Stiching in broadcast datetime/hours based on the broadcast instances to compare on level above...
						select	base.DK_EVENT_START_DATEHOUR_DIM			as event_start
								,base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM	as prec_prog
								,presc.DK_BROADCAST_START_DATEHOUR_DIM		as prec_start
								,presc.DK_BROADCAST_START_TIME_DIM			as prec_stime
								,base.DK_SLOT_INSTANCE_DIM					as slot
								,base.DK_BROADCAST_START_DATEHOUR_DIM		as slot_start_date
								,base.DK_BROADCAST_START_TIME_DIM			as slot_start_time
								,base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM	as succ_prog
								,succ.DK_BROADCAST_END_DATEHOUR_DIM			as succ_end
								,succ.DK_BROADCAST_END_TIME_DIM				as succ_etime
						from	(
									-- pulling out fields and the sample that I need from the fact table...
									select	PK_VIEWING_SLOT_INSTANCE_FACT
											,DK_PRECEDING_PROGRAMME_INSTANCE_DIM
											,DK_SLOT_INSTANCE_DIM
											,DK_BROADCAST_START_DATEHOUR_DIM
											,DK_BROADCAST_START_TIME_DIM
											,DK_INSTANCE_START_DATEHOUR_DIM
											,DK_INSTANCE_START_TIME_DIM
											,DK_EVENT_START_DATEHOUR_DIM
											,DK_EVENT_START_TIME_DIM
											,DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
									where 	dk_event_start_datehour_dim between <from> and <to>
									and		DK_PRECEDING_PROGRAMME_INSTANCE_DIM = DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									and		FILE_CYCLE_NUMBER = <batch>
								)												as base
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as presc
								on	base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM 	= presc.PK_PROGRAMME_INSTANCE_DIM_ASOC
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as succ
								on	base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM 	= succ.PK_PROGRAMME_INSTANCE_DIM_ASOC
					) as stage1
		) as finalStage
group	by	thedate
union 	all


-- Spots shown between different programmes do not exceed broadcast time constraints

select	'I12' as theindex
		,event_start/100 as thedate
		,sum(case when	(
							prec_prog <> succ_prog
							and
							(slot_date > succ_date or slot_date < prec_date)		
						) then 1 else 0 end
			) as thevalue
from	(
			-- Reformating the dim dates to actual timestamps to handle them as needed...
			select	stage1.event_start
					,stage1.prec_prog
					,to_timestamp(
									substring	(cast(cast((stage1.prec_start/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.prec_stime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as prec_date
					,stage1.slot
					,to_timestamp(
									substring	(cast(cast((stage1.slot_start_date/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.slot_start_time as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as slot_date
					,stage1.succ_prog
					,to_timestamp(
									substring	(cast(cast((stage1.succ_end/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.succ_etime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as succ_date
			from	(
						-- Stiching in broadcast datetime/hours based on the broadcast instances to compare on level above...
						select	base.DK_EVENT_START_DATEHOUR_DIM			as event_start
								,base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM	as prec_prog
								,presc.DK_BROADCAST_START_DATEHOUR_DIM		as prec_start
								,presc.DK_BROADCAST_START_TIME_DIM			as prec_stime
								,base.DK_SLOT_INSTANCE_DIM					as slot
								,base.DK_BROADCAST_START_DATEHOUR_DIM		as slot_start_date
								,base.DK_BROADCAST_START_TIME_DIM			as slot_start_time
								,base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM	as succ_prog
								,succ.DK_BROADCAST_END_DATEHOUR_DIM			as succ_end
								,succ.DK_BROADCAST_END_TIME_DIM				as succ_etime
						from	(
									-- pulling out fields and the sample that I need from the fact table...
									select	PK_VIEWING_SLOT_INSTANCE_FACT
											,DK_PRECEDING_PROGRAMME_INSTANCE_DIM
											,DK_SLOT_INSTANCE_DIM
											,DK_BROADCAST_START_DATEHOUR_DIM
											,DK_BROADCAST_START_TIME_DIM
											,DK_INSTANCE_START_DATEHOUR_DIM
											,DK_INSTANCE_START_TIME_DIM
											,DK_EVENT_START_DATEHOUR_DIM
											,DK_EVENT_START_TIME_DIM
											,DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
									where 	dk_event_start_datehour_dim between <from> and <to>
									and		DK_PRECEDING_PROGRAMME_INSTANCE_DIM = DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									and		FILE_CYCLE_NUMBER = <batch>
								)												as base
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as presc
								on	base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM 	= presc.PK_PROGRAMME_INSTANCE_DIM_ASOC
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as succ
								on	base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM 	= succ.PK_PROGRAMME_INSTANCE_DIM_ASOC
					) as stage1
		) as finalStage
group	by	thedate
union 	all


-- Check for preceding programme <> succeding programme where starting times are the same

select	'I13' as theindex
		,event_start/100 as thedate
		,sum(case when	(
							prec_prog <> succ_prog
							and
							prec_date = succ_date	
						) then 1 else 0 end
			) as thevalue
from	(
			-- Reformating the dim dates to actual timestamps to handle them as needed...
			select	stage1.event_start
					,stage1.prec_prog
					,to_timestamp(
									substring(cast(cast((stage1.prec_start/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.prec_stime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as prec_date
					,stage1.slot
					,to_timestamp(
									substring(cast(cast((stage1.slot_start_date/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.slot_start_time as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as slot_date
					,stage1.succ_prog
					,to_timestamp(
									substring(cast(cast((stage1.succ_start/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.succ_stime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as succ_date
			from	(
						-- Stiching in broadcast datetime/hours based on the broadcast instances to compare on level above...
						select	base.DK_EVENT_START_DATEHOUR_DIM			as event_start
								,base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM	as prec_prog
								,presc.DK_BROADCAST_START_DATEHOUR_DIM		as prec_start
								,presc.DK_BROADCAST_START_TIME_DIM			as prec_stime
								,base.DK_SLOT_INSTANCE_DIM					as slot
								,base.DK_BROADCAST_START_DATEHOUR_DIM		as slot_start_date
								,base.DK_BROADCAST_START_TIME_DIM			as slot_start_time
								,base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM	as succ_prog
								,succ.DK_BROADCAST_START_DATEHOUR_DIM		as succ_start
								,succ.DK_BROADCAST_START_TIME_DIM			as succ_stime
						from	(
									-- pulling out fields and the sample that I need from the fact table...
									select	PK_VIEWING_SLOT_INSTANCE_FACT
											,DK_PRECEDING_PROGRAMME_INSTANCE_DIM
											,DK_SLOT_INSTANCE_DIM
											,DK_BROADCAST_START_DATEHOUR_DIM
											,DK_BROADCAST_START_TIME_DIM
											,DK_INSTANCE_START_DATEHOUR_DIM
											,DK_INSTANCE_START_TIME_DIM
											,DK_EVENT_START_DATEHOUR_DIM
											,DK_EVENT_START_TIME_DIM
											,DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
									where 	dk_event_start_datehour_dim between <from> and <to>
									and		DK_PRECEDING_PROGRAMME_INSTANCE_DIM = DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									and		FILE_CYCLE_NUMBER = <batch>
								)												as base
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as presc
								on	base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM 	= presc.PK_PROGRAMME_INSTANCE_DIM_ASOC
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as succ
								on	base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM 	= succ.PK_PROGRAMME_INSTANCE_DIM_ASOC
					) as stage1
		) as finalStage
group	by	thedate
union 	all


-- Check for preceding programme = succeding programme where starting times are different

select	'I14' as theindex
		,event_start/100 as thedate
		,sum(case when	(
							prec_prog = succ_prog
							and
							prec_date <> succ_date
						) then 1 else 0 end
			) as thevalue
from	(
			-- Reformating the dim dates to actual timestamps to handle them as needed...
			select	stage1.event_start
					,stage1.prec_prog
					,to_timestamp(
									substring(cast(cast((stage1.prec_start/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.prec_stime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as prec_date
					,stage1.slot
					,to_timestamp(
									substring(cast(cast((stage1.slot_start_date/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.slot_start_time as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as slot_date
					,stage1.succ_prog
					,to_timestamp(
									substring(cast(cast((stage1.succ_start/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(stage1.succ_stime as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as succ_date
			from	(
						-- Stiching in broadcast datetime/hours based on the broadcast instances to compare on level above...
						select	base.DK_EVENT_START_DATEHOUR_DIM			as event_start
								,base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM	as prec_prog
								,presc.DK_BROADCAST_START_DATEHOUR_DIM		as prec_start
								,presc.DK_BROADCAST_START_TIME_DIM			as prec_stime
								,base.DK_SLOT_INSTANCE_DIM					as slot
								,base.DK_BROADCAST_START_DATEHOUR_DIM		as slot_start_date
								,base.DK_BROADCAST_START_TIME_DIM			as slot_start_time
								,base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM	as succ_prog
								,succ.DK_BROADCAST_START_DATEHOUR_DIM		as succ_start
								,succ.DK_BROADCAST_START_TIME_DIM			as succ_stime
						from	(
									-- pulling out fields and the sample that I need from the fact table...
									select	PK_VIEWING_SLOT_INSTANCE_FACT
											,DK_PRECEDING_PROGRAMME_INSTANCE_DIM
											,DK_SLOT_INSTANCE_DIM
											,DK_BROADCAST_START_DATEHOUR_DIM
											,DK_BROADCAST_START_TIME_DIM
											,DK_INSTANCE_START_DATEHOUR_DIM
											,DK_INSTANCE_START_TIME_DIM
											,DK_EVENT_START_DATEHOUR_DIM
											,DK_EVENT_START_TIME_DIM
											,DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
									where 	dk_event_start_datehour_dim between <from> and <to>
									and		DK_PRECEDING_PROGRAMME_INSTANCE_DIM = DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
									and		FILE_CYCLE_NUMBER = <batch>
								)												as base
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as presc
								on	base.DK_PRECEDING_PROGRAMME_INSTANCE_DIM 	= presc.PK_PROGRAMME_INSTANCE_DIM_ASOC
								LEFT join smi_dw..PROGRAMME_INSTANCE_DIM_ASOC	as succ
								on	base.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM 	= succ.PK_PROGRAMME_INSTANCE_DIM_ASOC
					) as stage1
		) as finalStage
group	by	thedate



----------------
/* B: CAPPING */
----------------


-- Check we have values for cap end date and time in the fact table

select	'C1' as theindex
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when (DK_CAPPED_EVENT_END_DATEHOUR_DIM > 0) then 1 else 0 end)as float) / cast(count(1)as float)	as capped_proportion
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union 	all


-- Check capped end time is greater thant event start time

select	'C2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when capped_end < event_start then 1 else 0 end)	as capped_before_start
from	(
			select	case when DK_CAPPED_EVENT_END_DATEHOUR_DIM < 0 then null else DK_CAPPED_EVENT_END_DATEHOUR_DIM end as c1
					,case when DK_CAPPED_EVENT_END_TIME_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as c2
					,to_timestamp(
									substring(cast(cast((c1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(c2 as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as capped_end
					,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
					,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
					,to_timestamp(
									substring(cast(cast((e1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(e2 as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as event_start
					,dk_event_start_datehour_dim
			from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
			where	dk_event_start_datehour_dim between <from> and <to>
			and		FILE_CYCLE_NUMBER = <batch>
		) as base
group	by	thedate
union	all


-- Check capped end time is less than or equal event end time

select	'C3'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when capped_end > event_end then 1 else 0 end)	as capped_after_end
from	(
			select	case when DK_CAPPED_EVENT_END_DATEHOUR_DIM < 0 then null else DK_CAPPED_EVENT_END_DATEHOUR_DIM end as c1
					,case when DK_CAPPED_EVENT_END_TIME_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as c2
					,to_timestamp(
									substring(cast(cast((c1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(c2 as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as capped_end
					,case when dk_event_end_datehour_dim < 0 then null else dk_event_end_datehour_dim end as e1
					,case when dk_event_end_time_dim < 0 then null else dk_event_end_time_dim end as e2
					,to_timestamp(
									substring	(cast(cast((e1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(e2 as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as event_end
					,dk_event_start_datehour_dim
			from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
			where	dk_event_start_datehour_dim between <from> and <to>
			and		FILE_CYCLE_NUMBER = <batch>
		) as base
group	by	thedate
union	all 


-- Check capped end time is greater thant event start time

select	'C4'
		,dk_event_start_datehour_dim/100 	as thedate
		,sum(case when capped_end <= event_start then 1 else 0 end)	as capped_before_start
from	(
			select	case when DK_CAPPED_EVENT_END_DATEHOUR_DIM < 0 then null else DK_CAPPED_EVENT_END_DATEHOUR_DIM end as c1
					,case when DK_CAPPED_EVENT_END_TIME_DIM < 0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as c2
					,to_timestamp(
									substring	(cast(cast((c1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(c2 as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as capped_end
					,case when dk_event_start_datehour_dim < 0 then null else dk_event_start_datehour_dim end as e1
					,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
					,to_timestamp(
									substring	(cast(cast((e1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(e2 as varchar(7)),2)	as time)
									,'yyyy-mm-dd hh:mi:ss'
								)								as event_start
					,dk_event_start_datehour_dim
			from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
			where	dk_event_start_datehour_dim between <from> and <to>
			and		FILE_CYCLE_NUMBER = <batch>
		) as base
group	by	thedate
union	all


-- Check we don't have capped date and not time

select	'C5-1'
		,dk_event_start_datehour_dim/100 	as thedate
		,sum(case when (DK_CAPPED_EVENT_END_DATEHOUR_DIM in (null,-1) and DK_CAPPED_EVENT_END_TIME_DIM > 0)then 1 else 0 end) as capped_date_no_time
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check we don't have capped time and not date

select	'C5-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (DK_CAPPED_EVENT_END_DATEHOUR_DIM > 0 and DK_CAPPED_EVENT_END_TIME_DIM in (null,-1))then 1 else 0 end )as capped_date_no_time
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check we have values for the full flag available in the fact
-- Check we have values for the partial flag available in the fact

select	'C6-1'
		,fact.dk_event_start_datehour_dim/100 	as thedate
		,sum(case when inst.CAPPED_FULL_FLAG > 0 then 1 else 0 end) as num_fullcapped_flag
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 	as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by 	thedate
union	all
select	'C6-2'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when inst.CAPPED_PARTIAL_FLAG > 0 then 1 else 0 end) as num_parcapped_flag
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 	as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
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
									substring(cast(cast((dk_event_start_datehour_dim/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(dk_event_start_time_dim as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)																														as event_start
					,event_end_datetime																													as event_end
					,dk_event_start_datehour_dim/100																									as thedate
			FROM	(
						select	fact.DK_VIEWING_EVENT_DIM
								,fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM
								,fact.dk_event_start_datehour_dim
								,fact.dk_event_start_time_dim
								,cap.event_end_datetime
						from	SMI_DW..VIEWING_SLOT_INSTANCE_FACT_VOLATILE				as fact
								left join DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY 	as cap
								on	fact.DTH_VIEWING_EVENT_ID = cap.DTH_VIEWING_EVENT_ID
						where	dk_event_start_datehour_dim between <from> and <to>
						and		FILE_CYCLE_NUMBER = <batch>
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
									substring(cast(cast((dk_event_start_datehour_dim/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(dk_event_start_time_dim as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)																														as event_start
					,to_timestamp(
									substring(cast(cast((dk_event_end_datehour_dim/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(dk_event_end_time_dim as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)																														as event_end
					,case 	when DK_CAPPED_EVENT_END_DATEHOUR_DIM <0 then null else DK_CAPPED_EVENT_END_DATEHOUR_DIM end as thedate_capped
					,case 	when DK_CAPPED_EVENT_END_TIME_DIM <0 then null else DK_CAPPED_EVENT_END_TIME_DIM end as thetime_capped
					,case 	when thedate_capped>0 
							then
							to_timestamp(
											substring(cast(cast((thedate_capped/100)as varchar(8)) as date),1,10) 
											||' '|| 
											cast(substring(cast(thetime_capped as varchar(7)),2) as time)
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
								,DK_CAPPED_EVENT_END_DATEHOUR_DIM
								,DK_CAPPED_EVENT_END_TIME_DIM
						from	SMI_DW..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
						where	dk_event_start_datehour_dim between <from> and <to>
						and		FILE_CYCLE_NUMBER = <batch>
					)as stage1
		) 	as base
group	by	thedate
union	all


-- Check hours viewed per box pre-post capped duration for both programmes/slots

select	'C8-1 pre'
		,thedate
		,sum(round(((extract(epoch from instance_end-instance_start))/60.0),2))/ count(distinct box)													as pre_capped_hours_viewed
from	(
			select	thedate
					,box
					,DTH_VIEWING_EVENT_ID
					,case when i1 < 0 then null else i1 end as ins1
					,case when i2 < 0 then null else i2 end as ins2
					,to_timestamp(
									substring(cast(cast((ins1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(ins2 as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)							as instance_start
					,case when i3 < 0 then null else i3 end as ins3
					,case when i4 < 0 then null else i4 end as ins4
					,to_timestamp(
									substring(cast(cast((ins3/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(ins4 as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)							as instance_end
			from	(					
						select	fact.dk_event_start_datehour_dim/100 	as thedate
								,dim.scms_subscriber_id					as box
								,fact.DTH_VIEWING_EVENT_ID
								,fact.DK_INSTANCE_START_DATEHOUR_DIM	as i1
								,fact.DK_INSTANCE_START_TIME_DIM		as i2
								,fact.DK_INSTANCE_END_DATEHOUR_DIM		as i3
								,fact.DK_INSTANCE_END_TIME_DIM			as i4
						from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
								left join mds..DTH_ACTIVE_VIEWING_CARD_DIM	as dim
								on	fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM = dim.PK_DTH_ACTIVE_VIEWING_CARD_DIM
						where	fact.dk_event_start_datehour_dim between <from> and <to>
						and		fact.SAMPLE_WEIGHT_VALUE = 1
						and		fact.FILE_CYCLE_NUMBER = <batch>
					) as Sampling
		) 	as base
group	by	thedate
union	all
select	'C8-2 post'
		,thedate
		,(sum(	case 	when (instance_end > event_end_capped) is null
						then round(((extract(epoch from event_end_capped-instance_start))/60.0),2)
						else round(((extract(epoch from instance_end-instance_start))/60.0),2)
				end
			)/60)
		/ 
		count(distinct box) 													as post_capped_hours_viewed
from	(
			select	distinct
					fact.dk_event_start_datehour_dim/100	as thedate
					,dim.SCMS_SUBSCRIBER_ID					as box
					,cap.EVENT_END_CAPPED_DATETIME 			as event_end_capped
					,to_timestamp(
									substring(cast(cast((i1/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(i2 as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)							as instance_start
					,to_timestamp(
									substring(cast(cast((i3/100)as varchar(8)) as date),1,10) 
									||' '|| 
									cast(substring(cast(i4 as varchar(7)),2) as time)
									,'yyyy-mm-dd hh:mi:ss'
								)							as instance_end
			from	(
						select	dk_event_start_datehour_dim
								,DK_DTH_ACTIVE_VIEWING_CARD_DIM
								,DTH_VIEWING_EVENT_ID
								,case when DK_INSTANCE_START_DATEHOUR_DIM < 0 then null else DK_INSTANCE_START_DATEHOUR_DIM end as i1
								,case when DK_INSTANCE_START_TIME_DIM < 0 then null else DK_INSTANCE_START_TIME_DIM end as i2
								,case when DK_INSTANCE_END_DATEHOUR_DIM < 0 then null else DK_EVENT_END_DATEHOUR_DIM end as i3
								,case when DK_INSTANCE_END_TIME_DIM < 0 then null else DK_EVENT_END_TIME_DIM end as i4
						from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
						where	dk_event_start_datehour_dim between <from> and <to>
						and		SAMPLE_WEIGHT_VALUE = 1
						and		FILE_CYCLE_NUMBER = <batch>
					)as fact
					left join DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY 	as cap
					on	fact.DTH_VIEWING_EVENT_ID = cap.DTH_VIEWING_EVENT_ID
					left join mds..DTH_ACTIVE_VIEWING_CARD_DIM				as dim
					on	fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM = dim.PK_DTH_ACTIVE_VIEWING_CARD_DIM
		) 	as base
group	by	thedate
union	all


-- Check no overlap between partial or full capped flag

select	'C9'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when (inst.CAPPED_FULL_FLAG > 0 and inst.CAPPED_PARTIAL_FLAG > 0)then 1 else 0 end) as capped_flag_overlaps
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 		as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check capped date/time not null if capped flag is 1

select	'C10'
		,fact.dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						(inst.CAPPED_FULL_FLAG > 0 or inst.CAPPED_PARTIAL_FLAG > 0)
						and (fact.DK_CAPPED_EVENT_END_DATEHOUR_DIM in (null,-1) 
							 or fact.DK_CAPPED_EVENT_END_TIME_DIM in (null,-1)
							)
						) then 1 else 0 end
			) as capped_flag_no_datetime
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..VIEWING_INSTANCE_DIM 		as inst 
		on	fact.dk_viewing_instance_dim = inst.pk_viewing_instance_dim
where	fact.dk_event_start_datehour_dim between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate


---------------------------
/* C: MINUTE ATTRIBUTION */
---------------------------

-- Checking no duplicated VIEWING_EVENT_ID on FINAL_MINUTE_ATTRIBUTION

select	'M1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(count(distinct b.VIEWING_EVENT_ID)as float)/cast(count(b.VIEWING_EVENT_ID)as float)	as unique_count
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE			as a
		inner join dis_prepare..FINAL_MINUTE_ATTRIBUTION 	as b
		on	a.DTH_VIEWING_EVENT_ID = b.VIEWING_EVENT_ID
where	a.dk_event_start_datehour_dim between <from> and <to>
and		a.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- Check programmes are been attributed

select	'M2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when DK_BARB_MIN_END_DATEHOUR_DIM > 0 then 1 else 0 end)	as num_rec_ma
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
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
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check for cases with barb end but no start minute

select	'M4-1'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						DK_BARB_MIN_START_DATEHOUR_DIM > 0 
						and (DK_BARB_MIN_END_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_END_TIME_DIM in (null,-1))
						) 
						then 1 else 0 end )	as barb_start_no_end
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group 	by	thedate
union	all
select	'M4-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when (
						DK_BARB_MIN_END_DATEHOUR_DIM > 0 
						and (DK_BARB_MIN_START_DATEHOUR_DIM in (null,-1) or DK_BARB_MIN_START_TIME_DIM in (null,-1))
						)
						then 1 else 0 end)	as barb_end_no_start
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check slots/programmes are not clamining the same minute

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
												substring(cast(cast((e1/100)as varchar(8)) as date),1,10) 
												||' '|| 
												cast(substring(cast(e2 as varchar(7)),2)	as time)
												,'yyyy-mm-dd hh:mi:ss'
											)								as event_start
								,case when dk_event_end_datehour_dim < 0 then null else dk_event_end_datehour_dim end as e3
								,case when dk_event_end_time_dim < 0 then null else dk_event_end_time_dim end as e4
								,to_timestamp(
												substring(cast(cast((e3/100)as varchar(8)) as date),1,10) 
												||' '|| 
												cast(substring(cast(e4 as varchar(7)),2)	as time)
												,'yyyy-mm-dd hh:mi:ss'
											)								as event_end
								,case when DK_BARB_MIN_START_DATEHOUR_DIM < 0 then null else DK_BARB_MIN_START_DATEHOUR_DIM end as b1
								,case when DK_BARB_MIN_START_TIME_DIM < 0 then null else DK_BARB_MIN_START_TIME_DIM end as b2
								,to_timestamp(
												substring(cast(cast((b1/100)as varchar(8)) as date),1,10) 
												||' '|| 
												cast(substring(cast(b2 as varchar(7)),2)	as time)
												,'yyyy-mm-dd hh:mi:ss'
											)								as barb_start
								,case when DK_BARB_MIN_END_DATEHOUR_DIM < 0 then null else DK_BARB_MIN_END_DATEHOUR_DIM end as b3
								,case when DK_BARB_MIN_END_TIME_DIM < 0 then null else DK_BARB_MIN_END_TIME_DIM end as b4
								,to_timestamp(
												substring(cast(cast((b3/100)as varchar(8)) as date),1,10) 
												||' '|| 
												cast(substring(cast(b4 as varchar(7)),2)	as time)
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
									from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
									where	dk_event_start_datehour_dim between <from> and <to> -- extracting the sample...
									and		FILE_CYCLE_NUMBER = <batch>
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
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_BARB_MIN_START_DATEHOUR_DIM > 0 then 1 else 0 end )as float)/cast(count(1) as float) as prop_rec_Ma
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all
select	'M7-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when DK_BARB_MIN_START_DATEHOUR_DIM > 0 then 1 else 0 end) as num_Rec_MA
from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate


----------------
/* D: SCALING */
----------------


-- volume/Proportion of events with weight assigned

select	'S1-1'
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when ACTUAL_WEIGHT_VALUE > 0 then 1 else 0 end)as float)/cast(count(1)as float) as prop_rec_scaled
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all
select	'S1-2'
		,dk_event_start_datehour_dim/100	as thedate
		,sum(case when ACTUAL_WEIGHT_VALUE >0 then 1 else 0 end) as num_rec_scaled
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check no difference between weights in fact and source

select	'S2'
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when fact.ACTUAL_WEIGHT_VALUE <> scaling.WEIGHT_SCALED_VALUE then 1 else 0 end ) as weight_diff
from	SMI_DW..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	 			as fact 
		left join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 	as scaling 
		on	fact.dth_viewing_event_id = scaling.dth_viewing_event_id
		and	scaling.event_start_date between '<from>' and '<to>'
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check all records matchign scaling source have been attributed

select	'S3'
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when fact.ACTUAL_WEIGHT_VALUE > 0 then 1 else 0 end)as float)/cast(count(1)as float) as prop_rec_weightsourced
from	SMI_DW..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	 			as fact 
		inner join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 	as scaling 
		on	fact.dth_viewing_event_id = scaling.dth_viewing_event_id
		and	scaling.event_start_date between '<from>' and '<to>'
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check we have a weight asigned where the flag = 1

select	'S4'
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when ( ACTUAL_WEIGHT_VALUE > 0 and SAMPLE_WEIGHT_VALUE is null) then 1 else 0 end) num_scaled_weightless
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check the flag = 1 when weight is asigned

select	'S5'
		,DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,sum(case when (SAMPLE_WEIGHT_VALUE = 0 and ACTUAL_WEIGHT_VALUE > 0) then 1 else 0 end) num_weight_flagless
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all

-- S6 Comming Soon...


-- S7-1 Check Convergence Pre-stage

select	'S7-1' as theindex
		,substring(base.event_start_date,1,4)||substring(base.event_start_date,6,2)||substring(base.event_start_date,9,2) as thedate
		,abs(
				(
					select	sum(weight_sample_value) as sky_target
					from	dis_reference..FINAL_SCALING_POPULATION_ATTRIBUTES_HISTORY
					where	audit_timestamp = '2013-06-08 23:28:43' -- awfull fix date...
				) - sum(base.HH_COMPOSITION_SCALING_VALUE)) as thevalue
from	dis_reference..FINAL_SCALING_RIM_WEIGHTING_HISTORY  as base
		inner join	(
						select	event_start_date
								,max(rim_weighting_iteration_number) as lap
						from	dis_reference..FINAL_SCALING_RIM_WEIGHTING_HISTORY
						where	event_start_date between '<FROM>' and '<TO>'
						group	by	event_start_date
					) as laps
		on	base.event_start_date = laps.event_start_date
		and	base.rim_weighting_iteration_number = laps.lap
group	by	thedate	
union	all

-- S7-2 Check Convergence Pos-stage

select	'S7-2' as theindex
		,substring(event_start_date,1,4)||substring(event_start_date,6,2)||substring(event_start_date,9,2) as thedate
		,min(meta.SKY_BASE) - round(sum(thetotal),0) as thevalue
from	(
			select	event_start_date 
					,HH_COMPOSITION
					,TV_REGION
					,BOX_TYPE
					,TENURE
					,SCALING_UNIVERSE_KEY
					,DTV_PACKAGE
					,sum(WEIGHT_SCALED_VALUE) as thetotal
			FROM 	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY
			where	event_start_date between '<FROM>' and '<TO>'
			group	by	event_start_date
						,HH_COMPOSITION
						,TV_REGION
						,BOX_TYPE
						,TENURE
						,SCALING_UNIVERSE_KEY
						,DTV_PACKAGE
		) as base
		left join dis_reference..SCALING_METADATA as meta
		on	base.event_Start_date between meta.effective_from and meta.effective_to
group	by	thedate
order 	by	thedate



------------------------
/* E: CHANNEL MAPPING */
------------------------

-- Proportion of channels matching with the dimension

select	'CH1-1' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(count(distinct channel.PK_CHANNEL_DIM)as float)/cast(count(distinct fact.DK_CHANNEL_DIM)as float) as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..CHANNEL_DIM				as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Number of channels matching with the dimension


select	'CH1-2' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,count(distinct channel.PK_CHANNEL_DIM) as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		left join smi_dw..CHANNEL_DIM				as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Checking all channel names are in place (null or -1 count)

select	'CH2' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when channel.CHANNEL_NAME is null then 1 else 0 end)as float)/cast(count(1)as float) as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		inner join smi_dw..CHANNEL_DIM				as channel
		on	fact.DK_CHANNEL_DIM =	 channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Checking all Channel Genre are in place (null or -1 count)

select	'CH3' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when channel.CHANNEL_GENRE is null then 1 else 0 end)as float)/cast(count(1)as float) as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		inner join smi_dw..CHANNEL_DIM				as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Checking all service keys are in place (null or -1 count)


select	'CH4' as index_
		,fact.DK_EVENT_START_DATEHOUR_DIM/100	as thedate
		,cast(sum(case when channel.SERVICE_KEY is null then 1 else 0 end)as float)/cast(count(1)as float) as value_
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE	as fact
		inner join smi_dw..CHANNEL_DIM				as channel
		on	fact.DK_CHANNEL_DIM = channel.PK_CHANNEL_DIM
where	fact.DK_EVENT_START_DATEHOUR_DIM between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate



--------------------------
/* F: DATA COMPLETENESS */
--------------------------


-- Check how many records have we got per day

select	'D1' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(1) as hits
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM

select	'D2' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_SLOT_INSTANCE_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float) as value
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check proportion of records without a DK_PROGRAMME_DIM

select	'D3' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_SLOT_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float) as value
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check proportion of records without a DK_CHANNEL_DIM

select	'D4' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,cast(sum(case when DK_CHANNEL_DIM in (null,-1) then 1 else 0 end)as float)/cast(count(1)as float) as value
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
where	dk_event_start_datehour_dim between <from> and <to>
and		FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check number of accounts per day

select	'D5' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(distinct cust.account_number) as value
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE		as fact
		left join mds..BILLING_CUSTOMER_ACCOUNT_DIM		as billing
		on	fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM = billing.PK_BILLING_CUSTOMER_ACCOUNT_DIM
		left join dis_prepare..TD_CUSTOMER_ATTRIBUTES	as cust
		on	billing.ACCOUNT_NUMBER = cust.ACCOUNT_NUMBER
where	dk_event_start_datehour_dim between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate
union	all


-- Check number of boxes per day

select	'D6' as index_
		,dk_event_start_datehour_dim/100	as thedate
		,count(distinct dim.SCMS_SUBSCRIBER_ID) as value
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE		as fact
		left join mds..DTH_ACTIVE_VIEWING_CARD_DIM		as dim
		on	fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM = dim.PK_DTH_ACTIVE_VIEWING_CARD_DIM
where	dk_event_start_datehour_dim between <from> and <to>
and		fact.FILE_CYCLE_NUMBER = <batch>
group	by	thedate



-------------------------------
/* G: DATA INTEGRITY (SLOTS) */ 
-------------------------------


-- Check for outstanding clearcast_commercial_number having >1 record in the output table that is because they have different buyer/advertiser details

select	'DI3'		as index_
		,count(1)	as thevalue
from	(
			select	clearcast_commercial_number as theid
					,BUYER_CODE
					,BUYER_name
					,ADVERTISER_CODE
					,ADVERTISER_NAME
					,count(1) as hits
			from	dis_reference..FINAL_SLOT_COPY
			where
			group	by	theid
						,BUYER_CODE
						,BUYER_name
						,ADVERTISER_CODE
						,ADVERTISER_NAME
			having	count(1)>1
		) as base
		
		
/*
TO ADD THIS CHECK IN THE FUTURE ON THE REGRESSION SHEET...

COUNTING PK DUPS
select	thedate
		,count(distinct thepk)
FROM	(
			select	dk_event_start_datehour_dim/100 as thedate
					,PK_VIEWING_PROGRAMME_INSTANCE_FACT as thepk
					,count(1) as hits
			from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT_MAR2013
			group	by	thedate
						,PK_VIEWING_PROGRAMME_INSTANCE_FACT
			having	count(1)> 1
		) as base
group	by	thedate
*/