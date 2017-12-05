SELECT --cast(event.event_start_datetime as date) event_start_date,
          --event.start_hour,
          dth_viewing_event_id,
          event_action,
          lead_event_action as standby_lead_event_action ,--not only standby now, but the event following the 'Change View' event (if no restrictions in place)
		  case
		  when time(event_start_datetime)>= '03:30:00' and time(event_start_datetime) < '21:00:00'
				and duration_secs>=235*60					then 2
		  when time(event_start_datetime)>= '21:00:00' and time(event_start_datetime) < '23:00:00'
				and time(event_end_datetime) >= '00:55:00'					then 2		  
		  when (time(event_start_datetime)>= '23:00:00' or time(event_start_datetime) < '03:30:00')
				and duration_secs>=115*60					then 2		  
		  else	1
		  end as pre_standby_event_flag
		-- select count(1)
  FROM
       (
       select dth_viewing_event_id, dth_viewing_event_day, log_received_datetime, scms_subscriber_id, event_start_datetime, event_end_datetime, event_action,
                        DATE_PART('HOUR', event_start_datetime) as start_hour,
                        EXTRACT(epoch from event_end_datetime - event_start_datetime)     as duration_secs,
                     lead(event_action) over (partition by scms_subscriber_id order by EVENT_START_DATETIME) as lead_event_action,
                     lead(event_start_datetime) over (partition by scms_subscriber_id order by EVENT_START_DATETIME) as lead_event_start_datetime,
                     lead(dth_viewing_event_id) over (partition by scms_subscriber_id order by EVENT_START_DATETIME) as lead_dth_viewing_event_id,
                        lead(dth_viewing_event_day) over (partition by scms_subscriber_id order by EVENT_START_DATETIME) as lead_dth_viewing_event_day,
                        lead(log_received_datetime) over (partition by scms_subscriber_id order by EVENT_START_DATETIME) as lead_log_received_datetime
          from "DIS_REFERENCE"."ADMIN"."FINAL_DTH_VIEWING_EVENT_HISTORY"
         --where event_action =  'Change View'
           -- could put extra restriction in here to say, AND event_action != 'Change View'
          -- and EXTRACT(epoch from lead_event_start_datetime - event_end_datetime) between 0 and 1
           --and
           where 
		   (event_start_datetime between '2016-08-01 20:00:00' and '2016-08-04 05:00:00'  -- any day from 14 days from the 6th Nov incl
           or
		   event_start_datetime between '2016-08-08 20:00:00' and '2016-08-11 05:00:00')  -- any day from 14 days from the 6th Nov incl
		   AND panel_id_reported        in (11, 12)
         ) event
    --LEFT JOIN
    --   "DIS_REFERENCE"."ADMIN"."FINAL_CAPPED_EVENTS_HISTORY" as cap
    --   ON event.dth_viewing_event_id = cap.dth_viewing_event_id
    WHERE EXTRACT(epoch from event.lead_event_start_datetime - event.event_end_datetime) between 0 and 1
      --AND event_action =  'Change View'  --if this is commented out, then we are looking at all combinations of 1st event
      AND lead_event_action =  'Standby In'  --if this is commented out, then we are looking at all combinations of 2nd event