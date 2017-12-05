/*	4.1 Creating a New Data Set
Populate the data by copying and pasting the result sets of the queries into each of 7 tabs.
SERVICE_KEY_ATTRIBUTES

*/

-- For On Demand channels
SELECT *
FROM VESPA_ANALYSTS.channel_map_prod_service_key_attributes
where notes not like '65535%'
order by service_key, effective_from

-- For Linear channels
SELECT *
FROM VESPA_ANALYSTS.channel_map_prod_service_key_attributes
where cast(notes as bigint ) >= 65535
order by cast(notes as bigint ) asc

-- SERVICE_KEY_BARB

SELECT *
FROM VESPA_ANALYSTS.channel_map_prod_service_key_barb
order by log_station_code,sti_code, service_key

-- SERVICE_KEY_LANDMARK

SELECT *
FROM VESPA_ANALYSTS.channel_map_prod_service_key_landmark
order by SARE_no, service_key, effective_from

-- LOG_STATION_PANEL

SELECT *
FROM VESPA_ANALYSTS.channel_map_dev_log_station_panel
order by log_station_code,sti_code
