/* 4.3 Checks Against VESPA Programme Schedule
There is a migration process loading data into VPS. Check for the channels that have broadcast data but are not in Service_key_attributes or have broadcast data earlier than effective_from or later than effective_to dates.
Generally we are not interested in the data before 1/1/12.
*/

-- SERVICE KEY PROGRAMME SCHED CHECK

SELECT vps.service_key,
       vps.channel_name,
       min(cast(vps.broadcast_start_date_time_utc as date)) as min_date,
       max(cast(vps.broadcast_start_date_time_utc as date)) max_date,
       count(1) as no_progs
FROM SK_PROD.vespa_programme_schedule vps
        LEFT JOIN VESPA_ANALYSTS.channel_map_dev_service_key_attributes ska
        ON vps.service_key = ska.service_key and cast(vps.broadcast_start_date_time_utc as date) between cast(ska.effective_from as date) and ska.effective_to
where ska.service_key is null
and cast(vps.broadcast_start_date_time_utc as date) > '2012-01-01'
and vps.service_key < 65535
Group by vps.service_key, vps.channel_name
order by 1,3 –-disregard On Demand channels from this

-- code below is required for on demand channels due to no counter in SK

SELECT vps.service_key,
       vps.channel_name,
       min(cast(vps.broadcast_start_date_time_utc as date)) as min_date,
       max(cast(vps.broadcast_start_date_time_utc as date)) max_date,
       count(1) as no_progs
FROM SK_PROD.vespa_programme_schedule vps
        LEFT JOIN VESPA_ANALYSTS.channel_map_dev_service_key_attributes ska
        ON vps.service_key = cast(ska.notes as integer) and cast(vps.broadcast_start_date_time_utc as date) between cast(ska.effective_from as date) and ska.effective_to
where ska.notes is null
and     vps.service_key >65534 --< On Demand

--and ska.service_key >1000

and cast(vps.broadcast_start_date_time_utc as date) > '2012-01-01'
Group by vps.service_key, vps.channel_name
order by 1,3
