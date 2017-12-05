/**************************** SECTION 2 ********************************/

/*
Comparing the figures between cbi and vespa has to be on live as recorded is done ntiles per days on vespa and on cbi is done
ntile per hour... diff stuff coming out from that...
*/

--BEFORE CAPPING...

/*
This query is going to give you the results at a programme level... meaning you have the IntoProgramme perspective of the events
as the final outcome...

On the other hand, CBI doesn't have the IntoProgramme view over the events on the initial stage of the Capping process...
which honestly is a bit weird that both Vespa and CBI start with a diff base, I think it would have been easier starting at a agreed level
*/

select	ini.live
		,ini.event_start_hour
		,csi.ps_flag 						
		,ini.pack_grp
		,ini.genre
		,sum(ini.X_Event_Duration)/60	as _Umin
		,count(1)						as hits
from 	Capping2_01_Viewing_Records as ini
		inner join ( 
					select  convert(bigint,si_external_identifier)  as box
							,left(si_service_instance_type,1)       as ps_flag
					from    sk_prod.cust_service_instance
					where   si_service_instance_type like '%DTV%'
					and 	effective_to_dt = '9999-09-09'
					group   by  box
								,ps_flag
					) as csi
		on ini.subscriber_id = csi.box
group	by 	ini.live
			,ini.event_start_hour
			,csi.ps_flag 						
			,ini.pack_grp
			,ini.genre

			
			
-- AFTER CAPPING...
/*
So the Pen table is the table right before inputing everything into the Augments ones, this guy is NOT AT EVENT LEVEL... (but that's because I 
made it that way). However naturaly it is, but for UAT purposes I just brought it up to Event level to be able to compare results from both ends...
*/
select	cap.live																-- Live/Playback...
		,datepart(hh,cap.viewing_starts)            	as event_start_hour		-- Event start Hour...
		,csi.ps_flag										 					-- Primary/Secondary flag...
		,coalesce(cl.pack,'Other')						as pack					-- Channel pack
		,cap.genre																-- Genre...
		,sum(cap.viewing_duration)/60					as _Cmin				-- Minutes after Capping...
		,count(1)										as hits					-- Number of Events...
from	(	
			select	subscriber_id
					,adjusted_event_start_time
					,min(Programme_Trans_Sk)	as Programme_Trans_Sk
					,min(live)					as live
					,min(viewing_starts) 		as viewing_starts
					,min(genre)					as genre
					,sum(viewing_duration)		as viewing_duration
			from	CP2_capped_data_holding_pen
			group	by	subscriber_id
						,adjusted_event_start_time
		)	as cap
		left join ( 
			select  convert(bigint,si_external_identifier)  as box
					,left(si_service_instance_type,1)       as ps_flag
			from    sk_prod.cust_service_instance
			where   si_service_instance_type like '%DTV%'
			and 	effective_to_dt = '9999-09-09'
			group   by  box
						,ps_flag
		)							as csi
		on cap.subscriber_id = csi.box
		left join SK_PROD.VESPA_PROGRAMME_SCHEDULE as prog
        on cap.Programme_Trans_Sk = prog.dk_programme_instance_dim
        left join vespa_analysts.cp2_channel_lookup as cl
        on  upper(trim(prog.channel_name)) = upper(trim(cl.epg_channel))
group	by	live
			,event_start_hour
			,ps_flag
			,pack
			,genre
			
			
			
-- GROUPED...

/*
This is a compacted view, bringing the initial base into an event level just to compare figures from both ends' basis...
*/
select	ini.live
		,ini.event_start_hour
		,csi.ps_flag 						
		,ini.pack_grp
		,ini.genre
		,sum(ini.X_Event_Duration)/60	as _Umin
		,count(1)						as hits
from 	(	select  origin.*
					,cap.genre
					,cap.pack_grp
			from    (
						select  subscriber_id
                                ,min(live)              as live
								,min(cb_row_id)         as cb_row_id
								,min(event_start_hour)  as event_start_hour
								,sum(x_event_duration)  as x_event_duration
						from    Capping2_01_Viewing_Records
						group   by  subscriber_id
									,adjusted_event_start_time
					) as origin
					left join Capping2_01_Viewing_Records as cap
					on  origin.cb_row_id = cap.cb_row_id
		) as ini
		inner join ( 
			select  convert(bigint,si_external_identifier)  as box
					,left(si_service_instance_type,1)       as ps_flag
			from    sk_prod.cust_service_instance
			where   si_service_instance_type like '%DTV%'
			and 	effective_to_dt = '9999-09-09'
			group   by  box
						,ps_flag
		)							as csi
		on ini.subscriber_id = csi.box
group	by 	ini.live
			,ini.event_start_hour
			,csi.ps_flag 						
			,ini.pack_grp
			,ini.genre

/**************************** SECTION 3 ********************************/

						
						/* SUB-SECTION 3.1 */
						
/*
Counting up the events lasting less than 7 seconds... despite the fact that the table vespa_events_all is at a programme level, this sort of events
are naturaly also at event level as they don't last long enough to slide between two diff programs...
*/
declare @target_date			date
declare @varBroadcastMinDate  	int
declare @varBroadcastMaxDate  	int
declare @varEventStartHour    	int
declare @varEventEndHour      	int
set @target_date = ''

set @varBroadcastMinDate  = (dateformat(@target_date - 28, 'yyyymmdd00'))
set @varBroadcastMaxDate  = (dateformat(@target_date, 'yyyymmdd23'))          -- Broadcast to start no later than at 23:59 on the day
set @varEventStartHour    = (dateformat(@target_date - 1, 'yyyymmdd23'))      -- Event to start no earlier than at 23:00 on the previous day
set @varEventEndHour      = (dateformat(@target_date, 'yyyymmdd23'))          -- Event to start no later than at 23:59 on the next day
						
select	case when EA.REPORTED_PLAYBACK_SPEED is null then 1 else 0 end 							as live
		,datepart(hour,
                    case
                        when (EA.EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') 
							then EA.EVENT_START_DATE_TIME_UTC                      					-- prior Mar 12 - no change, consider UTC = local
                        when (EA.EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') 
							then dateadd(hour, 1, EA.EVENT_START_DATE_TIME_UTC)    					-- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
                        when (EA.EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') 
							then EA.EVENT_START_DATE_TIME_UTC                      					-- Oct 12-Mar 13 => UTC = Local
                        when (EA.EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') 
							then dateadd(hour, 1, EA.EVENT_START_DATE_TIME_UTC)    					-- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        when (EA.EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') 
							then EA.EVENT_START_DATE_TIME_UTC                      					-- Oct 13-Mar 14 => UTC = Local
                        else NULL                                                                   -- the scrippt will have to be updated past Mar 2014
                      end)                            											as event_start_hour
		,csi.ps_flag
		,coalesce(cl.pack,'Other')                                                              as pack
		,case when EA.Genre_Description is null then 'Unknown' else EA.Genre_Description end 	as genre
		,sum(EA.duration)/60 																	as _Min
		,count(1) 																				as hits
 from 	sk_prod.VESPA_EVENTS_ALL as EA
		left join ( 
			select  convert(bigint,si_external_identifier)  as box
					,left(si_service_instance_type,1)       as ps_flag
			from    sk_prod.cust_service_instance
			where   si_service_instance_type like '%DTV%'
			and 	effective_to_dt = '9999-09-09'
			group   by  box
						,ps_flag
		)							as csi
		on EA.subscriber_id = csi.box
		left join SK_PROD.VESPA_PROGRAMME_SCHEDULE as prog
        on EA.dk_programme_instance_dim = prog.dk_programme_instance_dim
        left join vespa_analysts.cp2_channel_lookup as cl
        on  upper(trim(prog.channel_name)) = upper(trim(cl.epg_channel))
where 	(EA.REPORTED_PLAYBACK_SPEED is null or EA.REPORTED_PLAYBACK_SPEED = 2)
  and 	EA.Duration <= 6
  and 	EA.Panel_id = 12
  and 	EA.type_of_viewing_event <> 'Non viewing event'					-- this bit wouldn't be necessary if daily panel wouldn't be deleted from events view all...
  and 	EA.DK_BROADCAST_START_DATEHOUR_DIM >= @varBroadcastMinDate
  and 	EA.DK_BROADCAST_START_DATEHOUR_DIM <= @varBroadcastMaxDate
  and 	EA.account_number is not null
  and 	EA.DK_EVENT_START_DATEHOUR_DIM >= @varEventStartHour         	-- Start with 2300 hours on the previous day to pick UTC records in DST time (DST = UTC + 1 between April & October)
  and 	EA.DK_EVENT_START_DATEHOUR_DIM <= @varEventEndHour         		-- End up with additional records for the next day, up to 04:00am
  and 	EA.subscriber_id is not null  
group   by  live
            ,event_start_hour
            ,ps_flag
            ,pack
            ,genre
			
			

					/* SUB-SECTION 3.2 */
						
select  live
        ,dateformat(adjusted_event_start_time,'yyyymmddhh') as event_start_hour
		,box_subscription
		,coalesce(pack,'Other')                             as pack
		,initial_genre
		,sum(event_dur_mins)                                as _min
        ,count(1)                                           as hits
from    CP2_event_listing
where   max_dur_mins = 20
group   by  live
            ,event_start_hour
            ,box_subscription
            ,pack
            ,initial_genre
			
			
			
					/* SUB-SECTION 3.3 - 3.4 */
					
-- Basically, CP2_capped_data_holding_pen holds all events going from the 9th at 23 hh until the 10th 23:59... I rather choose this one
-- instead as is more scalable? mmmm... do we want to scale?... well it doesn't affect query efficiency so, I think there's no harm on using
-- this one...

-- Now, with respcet to CP2_capped_events_with_endpoints... this is the table that tells us whether the event was thrown into the first
-- programme or ramdon end time selection capping rule...


select	case   when endp.capped_event_end_time is not null and endp.firstrow is null
					then 1 else 0
		end 												    as FPR
		,case   when endp.capped_event_end_time is not null and endp.firstrow is not null
					then 1 else 0
		end 												    as RPR  
		,cap.live
		,dateformat(cap.adjusted_event_start_time,'yyyymmddhh') as event_start_hour
		,coalesce(csi.ps_flag,'U') 							    as ps_flag
		,coalesce(cl.pack,'Other') 							    as pack
		,cap.genre
		,sum(cap.viewing_duration)/60						    as _min
		,count(1) 											    as hits
from	CP2_capped_data_holding_pen as cap
		inner join CP2_capped_events_with_endpoints	as endp
		on cap.subscriber_id = endp.subscriber_id
		and	endp.capped_event_end_time is not null
		and cap.adjusted_event_start_time = endp.adjusted_event_start_time
		left join ( 
			select  convert(bigint,si_external_identifier)  as box
					,left(si_service_instance_type,1)       as ps_flag
			from    sk_prod.cust_service_instance
			where   si_service_instance_type like '%DTV%'
			and 	effective_to_dt = '9999-09-09'
			group   by  box
						,ps_flag
		)											as csi
		on cap.subscriber_id = csi.box
		left join SK_PROD.VESPA_PROGRAMME_SCHEDULE 	as prog
        on cap.Programme_Trans_Sk = prog.dk_programme_instance_dim
        left join vespa_analysts.cp2_channel_lookup as cl
        on  upper(trim(prog.channel_name)) = upper(trim(cl.epg_channel))
group	by	fpr
			,rpr
			,live
			,event_start_hour
			,ps_flag
			,pack
			,genre


			
					/* SUB-SECTION 3.5 */

/*
Compacting up the Pen table to the event level to compare final outputs based on the Capped_flag field on both Vespa and CBI ends...
*/
					
select	cap.capped_flag
		,cap.live
		,dateformat(adjusted_event_start_time,'yyyymmddhh') 	as event_start_hour
		,coalesce(csi.ps_flag,'U') 								as ps_flag
		,coalesce(cl.pack,'Other')								as pack
		,cap.genre
		,sum(cap.viewing_duration)/60							as _min
		,count(1)												as hits
from 	(	
			select	subscriber_id
					,adjusted_event_start_time
                    ,max(capped_flag)           as capped_flag
					,min(Programme_Trans_Sk)	as Programme_Trans_Sk
					,min(live)					as live
					,min(viewing_starts) 		as viewing_starts
					,min(genre)					as genre
					,sum(viewing_duration)		as viewing_duration
			from	CP2_capped_data_holding_pen
			group	by	subscriber_id
						,adjusted_event_start_time
		)	as cap
		left join ( 
			select  convert(bigint,si_external_identifier)  as box
					,left(si_service_instance_type,1)       as ps_flag
			from    sk_prod.cust_service_instance
			where   si_service_instance_type like '%DTV%'
			and 	effective_to_dt = '9999-09-09'
			group   by  box
						,ps_flag
		)											as csi
		on cap.subscriber_id = csi.box
		left join SK_PROD.VESPA_PROGRAMME_SCHEDULE 	as prog
        on cap.Programme_Trans_Sk = prog.dk_programme_instance_dim
        left join vespa_analysts.cp2_channel_lookup as cl
        on  upper(trim(prog.channel_name)) = upper(trim(cl.epg_channel))
group	by	capped_flag
			,live
			,event_start_hour
			,ps_flag
			,pack
			,genre
			
			
			
					/* SUB-SECTION 3.6 */
					
/* killed with section 2... woohoo */



/**************************** SECTION 4 ********************************/



					/* SUB-SECTION 4.1 */
					
/* Number of events per ntile across each segment */

-- As CP2_ntiles_week is derived from CP2_Event_listing which holds the total population to be capped (grouped by box and event_start)
-- I think is save to check the Ntiles distribution on this table...

-- The fact here is that every row in CP2_ntiles_week refers to a single combination of box + event_star representing a viewing event,
-- hence rows = events per box or events population...

-- In this case I'm not going to create a pivot table because there are 200 ntiles that linked with the segments on the grouping command causes 
-- to have about 200k records which sybase can't pass into the clipboard... The upside is that this CP2_ntiles_week table holds all segments
-- hence extracting the figures won't be an issue... (actually never was, but is easier in this step)

 select 	live
		,event_start_hour
		,box_subscription
		,pack_grp
		,initial_genre
--		,ntile_lp
--		,ntile_1
--		,ntile_2
		,sum(x_event_duration)/60	as _min
		,count(1)               	as hits
from	CP2_ntiles_week
group	by 	live
			,event_start_hour
			,box_subscription
			,pack_grp
			,initial_genre
--			,ntile_lp
--			,ntile_1
--			,ntile_2
					


					/* SUB-SECTION 4.2 */
					
-- Segments' thresholds (in Minutes)...

		select  live
				,event_start_day
				,event_start_hour
				,pack_grp
				,initial_genre
				,cap_ntile
				,max(min_dur_mins) as min_threshold
		from    CP2_h23_3
		group   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile
		order   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile

		select  live
				,event_start_day
				,event_start_hour
				,pack_grp
				,initial_genre
				,cap_ntile
				,max(min_dur_mins) as min_threshold
		from    CP2_h20_22
		group   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile
		order   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile

		select  live
				,event_start_day
				,event_start_hour
				,pack_grp
				,initial_genre
				,cap_ntile
				,max(min_dur_mins) as min_threshold
		from    CP2_h15_19
		group   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile
		order   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile
		
		select  live
				,event_start_day
				,event_start_hour
				,pack_grp
				,initial_genre
				,cap_ntile
				,max(min_dur_mins) as min_threshold
		from    CP2_h4_14
		group   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile
		order   by 	live
					,event_start_day
					,event_start_hour
					,pack_grp
					,initial_genre
					,cap_ntile


					
					/* SUB-SECTION 4.3 */
					
-- Num of thresholds above and below the max and min...

	-- Out of aboves queries, what I've done is to merge the results into a single table called "cap"... then

		-- Number of thresholds below the minimum...
		select  count(1) as hits
		from    cap
		where   min_threshold <20 -- 203

		-- Number of thresholds above the maximum...
		select  count(1) as hits
		from    cap
		where   min_threshold>120 -- 1

		-- Total number of capping ntiles...
		select count(1) from cap -- 495