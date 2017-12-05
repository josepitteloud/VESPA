
-------------SENSITIVE CHANNEL UAT IN NETEZZA


--CHECK THAT ALL BARB REPORTED CHANNELS ARE FLAGGED AS NONSENSITIVE

SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_CHANNEL_FLAG = 1
  LIMIT 100;

SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_viewing_event_FLAG = 1
  LIMIT 100;

SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_PROGRAMME_FLAG = 1
  LIMIT 100;
 
SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_VIEWING_EVENT_FLAG = 1
  LIMIT 100;

SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_MISSING_SK_MAPPING_FLAG = 1
  LIMIT 100;
 
SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_UNKNOWN_SK_MAPPING_FLAG = 1
  LIMIT 100;
 
SELECT COUNT(1)
  FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
  WHERE BARB_REPORTED_FLAG = 1 AND SENSITIVE_PROG_PERCENTAGE_FLAG = 1
  LIMIT 100;

-----------------------------------------------------------------------------------------------

--LIVE join 

ADJUSTED_EVENT_START_TIME >= TX_START_DATETIME_UTC 
ADJUSTED_EVENT_START_TIME < TX_END_DATETIME_UTC 
TRIPLET_SSP_NETWORK_ID = SSP_NETWORK_ID 
TRIPLET_TRANSPORT_ID = TRANSPORT_ID
TRIPLET_SERVICE_ID = SERVICE_ID 

--PLAYBACK join

select distinct servicekey, RECORDEDTIME
FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_programme_flag is null
and evchangeview_et is not null

RECORDEDTIME >= TX_START_DATETIME_UTC 
RECORDEDTIME < TX_END_DATETIME_UTC 
SERVICE_KEY = SERVICE_KEY 

select * from TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE


----------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------- 
 
--CHECK epg table to see what is being flagged as a sensitive programme (should be adult or religious in sub-genre)
--

SELECT distinct sub_genre_description, sensitive_programme_flag FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
WHERE (TX_DATE >= 20120802 AND TX_DATE < 20120809)

--check rolling 7 day percentage for this channel

select * from 
(SELECT SSP_NETWORK_ID,
TRANSPORT_ID,
SERVICE_ID ,channel_name,
TX_DATE,sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100) percentage_sensitive FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
--where (TX_DATE = 20120803 )
group by SSP_NETWORK_ID,
TRANSPORT_ID,
SERVICE_ID,channel_name,
TX_DATE) t
where percentage_sensitive >= 15

----------------

-- check all sensitive perc events are marked as sensitive events

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1 and sensitive_viewing_event_flag != 1

--0 rows
------------------check logic for 15% is identifying the correct channels for a particular day
------------------that should be flagged as insensitive

--events table

-------------------live

select distinct transportstreamid, siserviceid from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and adjusted_event_start_time like '2012-08-08%'
and servicekey is null

---final_epg_schedule

-- take events marked as sensitive based on the perc column on viewing events 9 for live

select distinct cast (transportstreamid as int) transport_id, cast(siserviceid as int) service_id from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and adjusted_event_start_time like '2012-08-08%'
and servicekey is null

----------------------calculate 15% on last 7 days for live

select distinct transport_id, service_id from 
(SELECT SSP_NETWORK_ID,
cast(transport_id as int) TRANSPORT_ID,
cast(service_id as int) SERVICE_ID,
sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100,1) percentage_sensitive 
FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
group by SSP_NETWORK_ID,
TRANSPORT_ID,
SERVICE_ID) t
where percentage_sensitive > 14.99

--intersect to see if all records in events table match to appropriate record in the epg table.

select distinct cast (transportstreamid as int) transport_id, cast(siserviceid as int) service_id from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and adjusted_event_start_time like '2012-08-08%'
and servicekey is null
intersect
select distinct transport_id, service_id from 
(SELECT SSP_NETWORK_ID,
cast(transport_id as int) TRANSPORT_ID,
cast(service_id as int) SERVICE_ID,
sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100,1) percentage_sensitive 
FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
group by SSP_NETWORK_ID,
TRANSPORT_ID,
SERVICE_ID) t
where percentage_sensitive > 14.99

---RECORDEDTIME

---- take events marked as sensitive based on the perc column on viewing events 9 for recorded

select distinct cast(servicekey as int) servicekey from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and recordedtime like '2012-08-08%'
and servicekey is not null

--92 rows
--------------------------------------------

----------------------calculate 15% on last 7 days for recorded

select distinct cast(service_key as int) service_key from 
(SELECT cast(service_key as int) service_key,
sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100,1) percentage_sensitive 
FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
and service_key is not null
group by service_key) t
where percentage_sensitive > 14.99

--102 rows

--intersect to see if all records in events table match to appropriate record in the epg table.

select distinct cast(servicekey as int) servicekey from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and recordedtime like '2012-08-08%'
and servicekey is not null
intersect
select distinct cast(service_key as int) service_key from 
(SELECT cast(service_key as int) service_key,
sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100,1) percentage_sensitive 
FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
and service_key is not null
group by service_key) t
where percentage_sensitive > 14.99

--92 rows

---------------------------------------------------

--check differences between what is in epg and what is in events

(select distinct cast(service_key as int) service_key from 
(SELECT cast(service_key as int) service_key,
sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100,1) percentage_sensitive 
FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
and service_key is not null
group by service_key) t
where percentage_sensitive > 14.99
minus
select distinct cast(servicekey as int) servicekey from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and recordedtime like '2012-08-08%'
and servicekey is not null)

---10 rows
------------------------------------------

--check to see if any records for these 10 rows for the day you are testing for...
select * from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where cast(servicekey as int) in 
(select distinct cast(service_key as int) service_key from 
(SELECT cast(service_key as int) service_key,
sum(sensitive_programme_flag) sum_sensitive_programmes,
count(1) total_programmes_per_Day,
round(sum(sensitive_programme_flag)/count(1) * 100,1) percentage_sensitive 
FROM TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE
where (TX_DATE >= 20120802 AND TX_DATE < 20120809)
and service_key is not null
group by service_key) t
where percentage_sensitive > 14.99
minus
select distinct cast(servicekey as int) servicekey from 
TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_prog_percentage_flag = 1
and recordedtime like '2012-08-08%'
and servicekey is not null)
and recordedtime like '2012-08-08%'

---0 rows

---------------------------------------------------------------------------------------------------------

---check for events being flagged as sensitive_programme_flag where percentage is under 15 %

--
--------------------------------------------------

--check data where sensitive percentage is 0 but sensitive_programme_flag = 1
--and ensure that sub-genre is either adult or religious for live

select distinct b.sub_genre_description 
--,a.* 
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE a,
TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE b
where a.originalnetworkid = b.ssp_network_id
and a.transportstreamid = b.transport_id
and a.siserviceid = b.service_id
and a.ADJUSTED_EVENT_START_TIME >= b.TX_START_DATETIME_UTC 
and a.ADJUSTED_EVENT_START_TIME < b.TX_END_DATETIME_UTC 
and a.sensitive_programme_flag = 1 and a.sensitive_prog_percentage_flag = 0
and a.adjusted_event_start_time like '2012-08-08%'

--check data where sensitive percentage is 0 but sensitive_programme_flag = 1
--and ensure that sub-genre is either adult or religious for recorded

select distinct b.sub_genre_description 
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE a,
TSTIQ_DIS_ETL.FINAL_EPG_SCHEDULE b
where a.servicekey = b.service_key
and a.RECORDEDTIME >= b.TX_START_DATETIME_UTC 
and a.RECORDEDTIME < b.TX_END_DATETIME_UTC 
and a.sensitive_programme_flag = 1 and a.sensitive_prog_percentage_flag = 0
and a.RECORDEDTIME like '2012-08-08%'

----------------------------------------------------------------------------------
'EVSURF'----------------------------------------------------------------------------------------

--analysis on nulls   

select * from tstiq_dis_prepare.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
limit 100

select event_type, SUM(case when recordedtime is not null then 1 else 0 end) RECORDED_EVENTS,
SUM(case when recordedtime is null then 1 else 0 end) LIVE_EVENTS,
MIN(ADJUSTED_EVENT_START_TIME) MIN_ADJUSTED_EVENT_START_TIME,MAX(ADJUSTED_EVENT_START_TIME) MAX_ADJUSTED_EVENT_START_TIME,
MIN(RECORDEDTIME) MIN_RECORDED_TIME, MAX(RECORDEDTIME) MAX_RECORDED_TIME,
count(1) total_events from tstiq_dis_prepare.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_viewing_event_flag is null
GROUP BY EVENT_TYPE

---analysis based on Changeviews and event start time

select event_type, 
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY-MM')
else TO_CHAR(adjusted_event_start_time,'YYYY-MM')end event_year_month,
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY')
else TO_CHAR(adjusted_event_start_time,'YYYY')end event_year,
SUM(case when recordedtime is not null then 1 else 0 end) RECORDED_EVENTS,
SUM(case when recordedtime is null then 1 else 0 end) LIVE_EVENTS,
count(1) total_events 
from tstiq_dis_prepare.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_viewing_event_flag is null
and event_type = 'EVCHANGEVIEW'
GROUP BY EVENT_TYPE,
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY-MM')
else TO_CHAR(adjusted_event_start_time,'YYYY-MM')end ,
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY')
else TO_CHAR(adjusted_event_start_time,'YYYY')end
order by 3,2

---------------------analysis based on all events and event start time----
select event_type, 
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY-MM')
else TO_CHAR(adjusted_event_start_time,'YYYY-MM')end event_year_month,
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY')
else TO_CHAR(adjusted_event_start_time,'YYYY')end event_year,
SUM(case when recordedtime is not null then 1 else 0 end) RECORDED_EVENTS,
SUM(case when recordedtime is null then 1 else 0 end) LIVE_EVENTS,
count(1) total_events 
from tstiq_dis_prepare.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
where sensitive_viewing_event_flag is null
GROUP BY EVENT_TYPE,
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY-MM')
else TO_CHAR(adjusted_event_start_time,'YYYY-MM')end ,
case when recordedtime is not null then TO_CHAR( recordedtime,'YYYY')
else TO_CHAR(adjusted_event_start_time,'YYYY')end
order by 1,3,2


------------------------check that relevant previous and future programme fields are -1 to stop
------------------------temporarily this being shown in VIQ

select distinct dk_previous_programme_instance_dim,dk_next_programme_instance_dim,
DK_PREVIOUS_PROGRAMME_DIM, 
DK_PREVIOUS_CHANNEL_DIM, 
DK_NEXT_PROGRAMME_DIM, 
DK_NEXT_CHANNEL_DIM
from TSTIQ_SMI_EXPORT.TSTIQ_SMI_ETL.VIEWING_PROGRAMME_INSTANCE_FACT
limit 100