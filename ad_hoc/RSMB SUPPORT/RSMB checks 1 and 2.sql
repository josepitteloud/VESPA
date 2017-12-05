
-- 4) OUTPUT
select	type_of_viewing
		-- ,weekpart [UNCOMMENT FOR CHECKING AT 2]
		,count(1)
from	(
			-- 3) Compacting at type of viewing on a single event per box...
			select	distinct
					box
					,full_start_date
					,type_of_viewing
					--,weekpart		[UNCOMMENT FOR CHECKING AT 2]
			from	(
						-- 2) Adding playback and viewing event info
						select	dk_event_start_datehour_dim / 10000		as themonth
								,playback.live_or_recorded				as lr_flag
								,case	when (lr_flag = 'RECORDED' and fact.on_window = 1)
											then 'VOSDAL'
										when (lr_flag = 'RECORDED' and fact.on_window = 0)
											then 'REC'
										else 'linear'
								end	 	as type_of_viewing
								,fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM	as box
								,fact.dk_event_start_datehour_dim		as start_dt
								,fact.full_start_date
								,fact.weekpart
								,billing.account_number
						from	(
									-- 1) Sampling and manipulating dates
									select	DK_DTH_ACTIVE_VIEWING_CARD_DIM
											,dk_event_start_datehour_dim
											,DK_PLAYBACK_DIM
											,DK_VIEWING_EVENT_DIM
											,case when dk_event_start_datehour_dim <0 then null else dk_event_start_datehour_dim end as e1
											,case when dk_event_start_time_dim < 0 then null else dk_event_start_time_dim end as e2
											,to_timestamp	(
																substring	(cast(cast((e1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(e2 as varchar(7)),2) as time)
																,'yyyy-mm-dd hh:mi:ss'
															)	as full_start_date
											,case when dk_broadcast_start_datehour_dim <0 then null else dk_broadcast_start_datehour_dim end as b1
											,case when dk_broadcast_start_time_dim <0 then null else dk_broadcast_start_time_dim end as b2
											,to_timestamp	(
																substring	(cast(cast((b1/100)as varchar(8)) as date),1,10) 
																||' '|| 
																cast(substring(cast(b2 as varchar(7)),2) as time)
																,'yyyy-mm-dd hh:mi:ss'
															)	as broadcast_start_dt
											,case	when	EXTRACT(dow FROM full_start_date) in (1,7) then 'WE'
													else	'WD'
											end		as weekpart
											,extract (day from (broadcast_start_dt - full_start_date)) as sameday_flag
											,case	when broadcast_start_dt < cast((cast(cast(broadcast_start_dt as date) as varchar(10)) || ' 06:00:00') as timestamp)
													then cast((cast(cast((broadcast_start_dt - 1)as date) as varchar(10)) || ' 06:00:00') as timestamp)
													else cast((cast(cast(broadcast_start_dt as date) as varchar(10)) || ' 06:00:00') as timestamp)
											end		as window_starts
											,cast((cast(cast((window_starts + 1) as date) as varchar(10)) || ' 05:59:00') as timestamp) as window_ends
											,case	when full_start_date between window_starts and window_ends	
													then 1
													else 0
											end as on_window
											,DK_BILLING_CUSTOMER_ACCOUNT_DIM
									from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT.
									where	dk_event_start_datehour_dim between 2013022400 and 2013022423
								)	as fact
								inner join smi_dw..VIEWING_EVENT_DIM	as events
								on	fact.DK_VIEWING_EVENT_DIM = events.PK_VIEWING_EVENT_DIM
								inner join smi_dw..PLAYBACK_DIM			as playback
								on	fact.dk_playback_dim = playback.PK_PLAYBACK_DIM
								inner join BILLING_CUSTOMER_ACCOUNT_DIM as billing
								on	fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM = billing.PK_BILLING_CUSTOMER_ACCOUNT_DIM
						where	events.panel_id = 12
					) 	as stage0
		)	as stage1
group	by	type_of_viewing
			--,weekpart	[UNCOMMENT FOR CHECKING AT 2]