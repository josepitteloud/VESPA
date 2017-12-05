
-- VOSDAL

select	timeslot
		,sum(theweight) 					as thereach
		,count(distinct account_number) as naccounts
from	(
			select	distinct
					scaling1.account_number
					,scaling1.weight_scaled_value as theweight
					,base.timeslot
			from	(
						select	fact.*
						from	(
									select	dth_viewing_event_id
											,DK_PLAYBACK_DIM
											,case when dk_broadcast_start_datehour_dim < 0 then null else dk_broadcast_start_datehour_dim end as b1
											,case when dk_broadcast_start_time_dim < 0 then null else dk_broadcast_start_time_dim end as b2
											,to_timestamp	(
																substring(cast(cast((b1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(b2 as varchar(7)),2)	as time)
																,'yyyy-mm-dd hh:mi:ss'
															)	as broadcast_start_dt
											,case when dk_instance_start_datehour_dim < 0 then null else dk_instance_start_datehour_dim end as i1
											,case when dk_instance_start_time_dim < 0 then null else dk_instance_start_time_dim end as i2
											,to_timestamp	(
																substring(cast(cast((i1/100)as varchar(8)) as date),1,10)
																||' '|| 
																cast(substring(cast(i2 as varchar(7)),2)	as time)
																,'yyyy-mm-dd hh:mi:ss'
															)	as instance_start_dt
											,case when	cast(broadcast_start_dt as time) > '06:00:00' 
															then cast((cast(cast(broadcast_start_dt+1 as date) as varchar(20)) || ' 06:00:00')as timestamp)
															else cast((cast(cast(broadcast_start_dt as date) as varchar(20)) || ' 06:00:00')as timestamp)
														end	as vosdal_cutoff
											,case when instance_start_dt <= vosdal_cutoff then 1 else 0 end as VOSDAL
											,case	when cast(instance_start_dt as time) between '00:00:00' and '04:00:00' then 'Midnight-4AM'
													when cast(instance_start_dt as time) between '04:00:01' and '06:00:00' then '4-6AM'
													when cast(instance_start_dt as time) between '06:00:01' and '10:00:00' then '6-10 AM'
													when cast(instance_start_dt as time) between '10:00:01' and '15:00:00' then '10AM-3PM'
													when cast(instance_start_dt as time) between '15:00:01' and '20:00:00' then '3-8PM'
													when cast(instance_start_dt as time) between '20:00:01' and '22:00:00' then '8-10PM'
													when cast(instance_start_dt as time) between '22:00:01' and '23:59:59' then '10pm-Midnight'
											end 	as timeslot
									from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
									where	dk_event_start_datehour_dim between 2013022700 and 2013022723
								)	as fact
								inner join smi_dw..PLAYBACK_DIM			as playback
								on	fact.DK_PLAYBACK_DIM = playback.PK_PLAYBACK_DIM
								and		playback.type_of_viewing_event = 'Sky+ time-shifted viewing event'
								and		playback.live_or_recorded = 'RECORDED'
						where	fact.broadcast_start_dt is not null
					)	as base
					left join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 			as scaling1 
					on	base.dth_viewing_event_id = scaling1.dth_viewing_event_id
					and	scaling1.event_start_date = '2013-02-27'
			where	VOSDAL = 1
		)	as stage1
group	by	timeslot	
	
	
	
-- LIVE

select	timeslot
		,sum(theweight) 					as thereach
		,count(distinct account_number) as naccounts
from	(
			select	distinct
					scaling1.account_number
					,scaling1.weight_scaled_value as theweight
					,base.timeslot
			from	(
						select	fact.*
						from	(
									select	dth_viewing_event_id
											,DK_PLAYBACK_DIM
											,case when dk_broadcast_start_datehour_dim < 0 then null else dk_broadcast_start_datehour_dim end as b1
											,case when dk_broadcast_start_time_dim < 0 then null else dk_broadcast_start_time_dim end as b2
											,to_timestamp	(
																substring(cast(cast((b1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(b2 as varchar(7)),2)	as time)
																,'yyyy-mm-dd hh:mi:ss'
															)	as broadcast_start_dt
											,case when dk_instance_start_datehour_dim < 0 then null else dk_instance_start_datehour_dim end as i1
											,case when dk_instance_start_time_dim < 0 then null else dk_instance_start_time_dim end as i2
											,to_timestamp	(
																substring(cast(cast((i1/100)as varchar(8)) as date),1,10)
																||' '|| 
																cast(substring(cast(i2 as varchar(7)),2)	as time)
																,'yyyy-mm-dd hh:mi:ss'
															)	as instance_start_dt
											,case when	cast(broadcast_start_dt as time) > '06:00:00' 
															then cast((cast(cast(broadcast_start_dt+1 as date) as varchar(20)) || ' 06:00:00')as timestamp)
															else cast((cast(cast(broadcast_start_dt as date) as varchar(20)) || ' 06:00:00')as timestamp)
														end	as vosdal_cutoff
											,case when instance_start_dt <= vosdal_cutoff then 1 else 0 end as VOSDAL
											,case	when cast(instance_start_dt as time) between '00:00:00' and '04:00:00' then 'Midnight-4AM'
													when cast(instance_start_dt as time) between '04:00:01' and '06:00:00' then '4-6AM'
													when cast(instance_start_dt as time) between '06:00:01' and '10:00:00' then '6-10 AM'
													when cast(instance_start_dt as time) between '10:00:01' and '15:00:00' then '10AM-3PM'
													when cast(instance_start_dt as time) between '15:00:01' and '20:00:00' then '3-8PM'
													when cast(instance_start_dt as time) between '20:00:01' and '22:00:00' then '8-10PM'
													when cast(instance_start_dt as time) between '22:00:01' and '23:59:59' then '10pm-Midnight'
											end 	as timeslot
									from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
									where	dk_event_start_datehour_dim between 2013022700 and 2013022723
								)	as fact
								inner join smi_dw..PLAYBACK_DIM			as playback
								on	fact.DK_PLAYBACK_DIM = playback.PK_PLAYBACK_DIM
								--and		playback.type_of_viewing_event = 'Sky+ time-shifted viewing event'
								and		playback.live_or_recorded = 'LIVE'
						where	fact.broadcast_start_dt is not null
					)	as base
					left join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 			as scaling1 
					on	base.dth_viewing_event_id = scaling1.dth_viewing_event_id
					and	scaling1.event_start_date = '2013-02-27'
			--where	VOSDAL = 1
		)	as stage1
group	by	timeslot	
	
	
	
