/* CHANGES IN BARB MASTER FILE

Check for changes to BARB reported channels that occurred since last BARB file parsing.
Previous file date is max of previous date
*/

SELECT filename
	, log_station_code
	, log_station_name
	, max(reporting_start_date) start_date
	, max(case when reporting_end_date is null then CAST('2999-12-31' as date) else reporting_end_date end) as end_date
FROM BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
GROUP BY filename
	, log_station_code
	, log_station_name
having (end_date >= '2013-10-01' and  end_date < '2999-12-31') OR (start_date >= '2013-10-01' and  start_date < '2999-12-31') 
order by 1,2


-- BARB Master SKB
-- Check for new BARB reported channels (these may already appear in the output of the previous query).

SELECT a.*
FROM (
        SELECT filename
			, log_station_code
			, log_station_name
			, max(reporting_start_date) start_date
			, max(case when reporting_end_date is null then CAST('2999-12-31' as date) else reporting_end_date end) as end_date
        from BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD
        group by filename, log_station_code, log_station_name
        ) a
LEFT JOIN (SELECT distinct log_station_code from VESPA_ANALYSTS.channel_map_dev_service_key_barb) b
        ON a.log_station_code = b.log_station_code
WHERE b.log_station_code is null

