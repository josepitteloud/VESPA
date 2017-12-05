----CBI TABLES UAT 



----uat 10082012

----------------------SECTION 1 INITIAL COUNTS--------------------

----------------------------------------------------------------------------------------------------------------

--Control table to include % null population or default population at event level for the following metrics:



--1.	Panel ID = null 

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where panelid is null

--0 rows

--2.	Account_number = null
--CBI process data at a subscriber level not account level

--3.	Subscriber_id = null or -1
select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where (subscriberid is null or subscriberid = -1)
and panelid = 12

--4.	programme instance dim = null
--CBI process data does not include programme_instance_dim


--5.	Event_type (Viewing / Channel Surf) = null

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_sensitive
where panelid = 12
and event_type is null

--0 rows


--6.	Event Start Time = null

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_TD1
where panelid = 12
and adjusted_event_start_time is null

--0 rows

--7.	Event End Time = null

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where x_adjusted_event_end_time is null
and panelid = 12

--0 rows

--8.	Viewing Start Time = null
--same as adjusted_event_start_time I believe as CBI have it compacted to the event and not at instance level
--at this stage

--9.	Viewing End Time = null
--same as adjusted_event_start_time I believe as CBI have it compacted to the event and not at instance level
--at this stage

--10.	stblogcreationdate <= 1970

SELECT count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
WHERE DATE_part('year',stblogcreationdate) < '1980'
and panelid = 12

--262 rows.  will these be filtered out of the next stage

SELECT COUNT(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_TD1
WHERE DATE_part('year',stblogcreationdate) < '1980'
AND PANELID = 12

--104 events where stblogcreationdate < 1980

--THIS IS A KNOWN ISSUE THAT HAS BEEN DELAYED TO BE REMEDIED UNTIL POST GO-LIVE

------------------------------------------------------------------------------------------------------

---recordedtime

SELECT count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
WHERE DATE_part('year',recordedtime) < '1980'
AND PANELID = 12

--575905 ROWS AT THE INITIAL STAGE

--WILL THESE BE FILTERED OUT OF THE SUBSEQUENT PROCESS

SELECT count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_TD1
WHERE DATE_part('year',recordedtime) < '1980'
AND PANELID = 12

--0 ROWS SO ALL FILTERED OUT BEFORE WE GO THROUGH THE PROCESS


-----------------------------------------------------------------------------------------
----------------------SECTION 2 COUNTS ON CONTROL TABLE--------------------

------------------------------capped delta-------------------------------------------

--1.	Live / Playback
--2.	Day of the week
--3.	Event Start hour
--4.	Primary / secondary box
--5.	Channel Pack 
--6.	Genre

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_box,silo_genre,silo_hour,silo_channel_pack, 
count(1)as hits,sum(event_duration_second)/60 event_duration,
sum(capped_event_duration_second)/60 capped_event_duration 
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_box,silo_genre,silo_hour,silo_channel_pack
order by 1


----------------------SECTION 3 COUNTS ON METRICS--------------------


-----------------------------events_delta metrics

---SHORT_DURATION
--1)

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,
silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
count(1) as hits, sum (short_duration_capped_flag) short_duration_capped,
round(sum(short_duration_capped_flag)/count(1) * 100,2) percent_short_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
GROUP BY silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

---DURATION AS % OF TOTAL PERCENTAGE OF MINS

2.	Duration (mins) / % of total viewing with a minimum cap applied

--
select 
silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,
silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
count(1) as hits, sum (min_cutoff_flag) min_duration_cnt,
round(sum(min_cutoff_flag)/count(1) * 100,2) percent_min_cutoff_flag,
sum(case when short_duration_capped_flag = 1 then capped_event_duration_second else 0 end)/60 short_duration_mins,
sum(capped_event_duration_second)/60 capped_duration_mins,
sum(event_duration_second)/60 uncapped_duration_mins,
sum (short_duration_capped_flag) short_duration_capped
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
GROUP BY silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1


--Long Duration

/* 1) where not assigned random row then either 
 	a) capped duration is adjusted_event_start_time to tx_broadcast_end_time
	b) or maximum cutoff
	
2) For random row assigned
	a) capped end time is derived from the end time of the random event.
 	b) duration is calculated from the adjusted_start_time and capped end time
*/

--3) LONG_DURATION CAPPED FLAG

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(long_duration_capped_flag) long_duration_events,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then 1 else 0 end) long_duration_random,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then 1 else 0 end) long_duration_max_cutoff,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then 1 else 0 end) long_duration_first_prog,
count(1) as events, 
round(sum(LONG_duration_capped_flag)/count(1) * 100,2) percent_long_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

------------------------------------------------------------------------------------------

------------------------------------------------CBI DATE OF EVENT-------------------------------------------------

select DATEOFEVENT,case when date_part('dow',DATEOFEVENT) = 1 then 'Sunday'
when date_part('dow',DATEOFEVENT) = 2 then 'Monday'
when date_part('dow',DATEOFEVENT) = 3 then 'Tuesday'
when date_part('dow',DATEOFEVENT) = 4 then 'Wednesday'
when date_part('dow',DATEOFEVENT) = 5 then 'Thursday'
when date_part('dow',DATEOFEVENT) = 6 then 'Friday'
when date_part('dow',DATEOFEVENT) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(long_duration_capped_flag) long_duration_events,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then 1 else 0 end) long_duration_random,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then 1 else 0 end) long_duration_max_cutoff,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then 1 else 0 end) long_duration_first_prog,
count(1) as events, 
round(sum(LONG_duration_capped_flag)/count(1) * 100,2) percent_long_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and DATEOFEVENT in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and DATEOFEVENT in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
group by DATEOFEVENT,date_part('dow',DATEOFEVENT),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1



--4) Long Duration events minutes

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(event_duration_second)/60  total_viewing_minutes,
sum(capped_event_duration_second)/60  total_capped_viewing_minutes,
sum(case when long_duration_capped_flag = 1 then capped_event_duration_second else 0 end)/60  total_long_dur_capped_minutes,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then capped_event_duration_second else 0 end)/ 60 long_duration_random_viewing_mins,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then capped_event_duration_second else 0 end)/ 60 long_duration_max_cutoff_viewing_mins,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then capped_event_duration_second else 0 end) / 60 long_duration_first_prog
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

--5) number % capped/uncapped events

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(case when short_duration_capped_flag = 1 then 1 else 0 end) short_duration_capped_count,
sum(case when long_duration_capped_flag = 1 then 1 else 0 end) long_duration_capped_count,
sum(case when long_duration_capped_flag = 0 and short_duration_capped_flag = 0 then 1 else 0 end) uncapped_events_count,
count(1) events_total
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

--6) duration (mins) of viewing before/after capping

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(event_duration_second)/ 60 total_minutes_viewed,
sum(capped_event_duration_second)/ 60 total_capped_minutes_viewed
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-08 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1


--------------------------------------------------------------------------------------------------------------

------------------------------------------4) NTILES WORK--------------------------------------------

--1.
SELECT dateofevent,case when date_part('dow',dateofevent) = 1 then 'Sunday'
when date_part('dow',dateofevent) = 2 then 'Monday'
when date_part('dow',dateofevent) = 3 then 'Tuesday'
when date_part('dow',dateofevent) = 4 then 'Wednesday'
when date_part('dow',dateofevent) = 5 then 'Thursday'
when date_part('dow',dateofevent) = 6 then 'Friday'
when date_part('dow',dateofevent) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
silo_key, ntile_number,
sum(case when ntile_exists_flag = 1 then 1 else 0 end) ntiles_existing,
sum(case when ntile_exists_flag = 0 then 1 else 0 end) ntiles_not_existing
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where panelid in (12)
and dateofevent in ('2012-11-08 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
group by dateofevent,date_part('dow',dateofevent),live_event_flag,silo_hour,SILO_CHANNEL_PACK,
SILO_GENRE,silo_box,silo_key, ntile_number
order by silo_key,ntile_number
limit 1000

--2.

--calculated thresholds by segment

select capped_threshold_key, capped_threshold_type live_event_flag,
capped_threshold_hour silo_hour,capped_threshold_channel_pack, capped_threshold_box, capped_threshold_genre,
capped_threshold_event_duration
from TSTIQ_DIS_ETL.CAPPED_THRESHOLD
order by 1 


--3.

--number /% of thresholds above/below max/min limits

SELECT CAPPING_THRESHOLD_TYPE, COUNT(1) FROM 
(select CAPPED_THRESHOLD_key, case when capped_threshold_event_duration = 20 then 'MIN_THRESHOLD'
						WHEN capped_threshold_event_duration = 120 then 'MAX_THRESHOLD'
						ELSE 'ACTUAL_CAPPED_VALUE' END CAPPING_THRESHOLD_TYPE
FROM TSTIQ_DIS_ETL.CAPPED_THRESHOLD) T
GROUP BY CAPPING_THRESHOLD_TYPE

---------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------
drop table tkd_viewing_mins_byaccount

create table tkd_viewing_mins_byaccount
as
(SELECT distinct d.subscriberid, c.account_number,
sum(event_duration_second)/60 uncapped_eventmin_duration,
sum(capped_event_duration_second)/60 capped_eventmin_duration
FROM TSTIQ_MDS..DTH_ACTIVE_VIEWING_CARD_DIM a
  inner join
tstiq_mds..customer_card_asoc b
on a.nk_dth_active_viewing_card_dim = b.nk_dth_active_viewing_card_dim
inner join
tstiq_mds..BILLING_CUSTOMER_ACCOUNT_DIM c
on 
b.nk_billing_customer_account_dim = c.nk_billing_customer_account_dim
right outer join
(SELECT * FROM 
 tstiq_dis_prepare..CAPPED_EVENTS_DELTA 
  WHERE panelid = 12
  and dateofevent in ('2012-11-08 00:00:00')) d
  on
  a.scms_subscriber_id = d.subscriberid
group by d.subscriberid, c.account_number)


--10/11/2012

drop table tkd_viewing_mins_byaccount_10112012_2

create table tkd_viewing_mins_byaccount_10112012_2
as
(SELECT distinct d.subscriberid, c.account_number,
sum(event_duration_second)/60 uncapped_eventmin_duration,
sum(capped_event_duration_second)/60 capped_eventmin_duration
FROM TSTIQ_MDS..DTH_ACTIVE_VIEWING_CARD_DIM a
  inner join
tstiq_mds..customer_card_asoc b
on a.nk_dth_active_viewing_card_dim = b.nk_dth_active_viewing_card_dim
inner join
tstiq_mds..BILLING_CUSTOMER_ACCOUNT_DIM c
on 
b.nk_billing_customer_account_dim = c.nk_billing_customer_account_dim
right outer join
(SELECT * FROM 
 tstiq_dis_prepare..CAPPED_EVENTS_DELTA 
  WHERE panelid = 12
  and dateofevent in ('2012-11-10 00:00:00')
  and event_duration_second > 6) d
  on
  a.scms_subscriber_id = d.subscriberid
group by d.subscriberid, c.account_number)

--10/11/2012


--initial checks

--1) check number of panel 12 tx+1 records that exist in the data being processed....

select * from tstiq_dis_prepare..CAPPED_EVENTS_TD2
where panelid = 12 
and (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 1
AND (CAST('2012-08-10' AS DATE))  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));
limit 100

-----------------------------------------------------------------------------------------------------------

----------------check counts of tx + 1 records for processing----------------------------------

select count(1) from 
--TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9
TSTIQ_DIS_PREPARE..capped_events_delta
where panelid in (12)
and DATEOFEVENT = '2012-11-08 00:00:00'
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
and     ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 0
AND (CAST('2012-08-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));


--2) check to ensure that proper fields used for different fields used in process

--live/tv

select distinct capped_threshold_type from capped_threshold

--silo_hour

select distinct capped_threshold_hour from capped_threshold

--channel pack

select distinct capped_threshold_channel_pack from capped_threshold

--genre

select distinct capped_threshold_genre from capped_threshold

--capped_threshold_box

select distinct capped_threshold_box from capped_threshold

--2.5-- identify those records you think are eligibe to be used in ntile process
--using new tx + 1 definition

-----------------------------------------------------------------------------------------------------------------

--3) examples of records being identified for capping

--------------------------------------------------------------------------------------------------------------------

--ntiles exist for everything except prime-time
--live

select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where (capped_threshold_hour not between 15 and 22
and capped_threshold_hour not between 6 and 9)
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= nvl(ntile.box_shut_down,0) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute

-------------------------------------------------------------------------------------

--prime time

select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000

-------------------------------------------------late afternoon-------------------------------------------------------

--------------------------------------------------------

--ntiles exist for late afternoon live

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , max(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 15 and 19
and capped_threshold_type = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


-----------------------------------------------------------------------------------------------------

--peak morning weekday ntile exists

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select capping_metadata_key,silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.capping_metadata_key, ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.capping_metadata_key,ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


---playback

--ntiles exist

select a.* from capped_threshold_ntile a,
(select capped_threshold_key from capped_threshold
where capped_threshold_type = 0) b
where a.silo_key = b.capped_threshold_key
order by 2,1,4

---------------------check short duration capped flag

--------------------check for segments with no panel12 events in there--------------------------------------------

select * from 
(select a.*, (case when identify > 0 then 1 else 0 end) identify_panel12 from 
(select  a.segment_key,a.silo_hour, a.segment_channel,
sum(case when panelid = 12 then 1 else 0 end) identify from TSTIQ_DIS_PREPARE..CAPPed_events_delta a,
TSTIQ_DIS_PREPARE..CAPPING_APPLY_UNCAPPED_EVENTS b
where a.random_row_id = b.random_row_id
and a.assigned_random_row_id = 0
and a.dateofevent = '2012-11-08 00:00:00'
group by a.segment_key,a.silo_hour, a.segment_channel)a) p
where identify_panel12 = 0

--686  rows with no panel12 involved

----------------------------------------------------------------------------------------------------------------------

---check that we are getting the correct data times for the tx definition through

SELECT min(DOCUMENTCREATIONDATE), max(DOCUMENTCREATIONDATE)
FROM TSTIQ_DIS_PREPARE..CAPPED_EVENTS
WHERE PANELID = 12
AND DATEOFEVENT = '2012-11-08 00:00:00'

----------------------------yep

  -------------------------------------------------------------------------------------------------------------------
  
  --09/11/2012 testing
  
  ----tst tables to extract 


--LIVE

--------------------------------------------------------------------------------------------------------------

delete from TKD_EVENTS_EXTRACT_LIVE;

commit;

insert into TKD_EVENTS_EXTRACT_LIVE
(select cap.viewing_event_id, cap.panelid, null ACCOUNT_NUMBER, cap.subscriberid,
cap.x_type_of_viewing_event,cap.adjusted_event_start_time event_start_date_time_utc,
cap.x_adjusted_event_end_time event_end_date_time_utc,
cap.adjusted_event_start_time x_viewing_start_time,
cap.x_adjusted_event_end_time x_viewing_end_time,
CAP.dummy1 tx_start_datetime_utc,cap.event_duration_second x_event_duration,
cap.event_duration_second x_programme_viewed_duration,
0 programme_trans_sk, date('2012-11-10') daily_table_date,
cap.live_event_flag live, cap.silo_genre genre, null sub_genre,
null epg_channel, tx.channel_name channel_name,
cap.dummy1 program_air_date, cap.dummy1 program_air_datetime,
DATE_PART('DAY',CAP.ADJUSTED_EVENT_START_TIME) event_start_day,
DATE_PART('HOUR',CAP.ADJUSTED_EVENT_START_TIME) EVENT_START_HOUR,
case when CAP.silo_box like 'P%' then 'P'
when CAP.silo_box like 'Se%' then 'S'
else 'U' end BOX_SUBSCRIPTION,
NULL INITIAL_GENRE,
NULL initial_channel_name, 
NULL pack,
NULL pack_grp,
NULL bucket_id
FROM TSTIQ_DIS_PREPARE..CAPPED_EVENTS CAP,
TSTIQ_DIS_REFERENCE..FINAL_EPG_SCHEDULE TX
WHERE CAP.ORIGINALNETWORKID = TX.SSP_NETWORK_ID
AND CAP.TRANSPORTSTREAMID = TX.TRANSPORT_ID
AND CAP.SISERVICEID = TX.SERVICE_ID
AND CAP.DUMMY1 = TX.TX_DATE_TIME_UTC
AND cap.live_event_flag = 1);

COMMIT;

-------------recorded

delete from TKD_EVENTS_EXTRACT_RECORDED;

commit;

INSERT INTO TKD_EVENTS_EXTRACT_RECORDED
(select cap.viewing_event_id, cap.panelid, null ACCOUNT_NUMBER, cap.subscriberid,
cap.x_type_of_viewing_event,cap.adjusted_event_start_time event_start_date_time_utc,
cap.x_adjusted_event_end_time event_end_date_time_utc,
cap.adjusted_event_start_time x_viewing_start_time,
cap.x_adjusted_event_end_time x_viewing_end_time,
CAP.dummy1 tx_start_datetime_utc,cap.event_duration_second x_event_duration,
cap.event_duration_second x_programme_viewed_duration,
0 programme_trans_sk, date('2012-11-10') daily_table_date,
cap.live_event_flag live, cap.silo_genre genre, null sub_genre,
null epg_channel, tx.channel_name channel_name,
cap.dummy1 program_air_date, cap.dummy1 program_air_datetime,
DATE_PART('DAY',CAP.ADJUSTED_EVENT_START_TIME) event_start_day,
DATE_PART('HOUR',CAP.ADJUSTED_EVENT_START_TIME) EVENT_START_HOUR,
case when CAP.silo_box like 'P%' then 'P'
when CAP.silo_box like 'Se%' then 'S'
else 'U' end BOX_SUBSCRIPTION,
NULL INITIAL_GENRE,
NULL initial_channel_name, 
NULL pack,
NULL pack_grp,
NULL bucket_id
FROM TSTIQ_DIS_PREPARE..CAPPED_EVENTS CAP,
TSTIQ_DIS_REFERENCE..FINAL_EPG_SCHEDULE TX
WHERE CAP.SERVICEKEY = TX.SERVICE_KEY
AND CAP.DUMMY1 = TX.TX_DATE_TIME_UTC
AND cap.live_event_flag = 0;

COMMIT;

---tst tables to extract end

----check on counts for 10th November

SELECT min(documentcreationdate), max(documentcreationdate) 
FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9_20121011
where panelid = 12
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
AND DATEOFEVENT = '2012-11-10 00:00:00'
and     ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-11-10' AS DATE) - 2
AND (CAST('2012-11-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-11-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)))
and DOCUMENTCREATIONDATE > 
(select max(documentcreationdate) from TSTIQ_DIS_PREPARE..capped_events_delta
where panelid = 12
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
AND DATEOFEVENT = '2012-11-10 00:00:00')

----check on counts for 11th August table for 10th August events 
----that we want to see in 


SELECT count(1)
FROM TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where panelid = 12
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
AND DATEOFEVENT = '2012-11-10 00:00:00'
and ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-11-10' AS DATE) - 0
AND (CAST('2012-11-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-11-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)))
and DOCUMENTCREATIONDATE > 
(select max(documentcreationdate) from TSTIQ_DIS_PREPARE..capped_events_delta
where panelid = 12
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
AND DATEOFEVENT = '2012-11-10 00:00:00')
limit 100

--check for short duration capped events

select min(event_duration_SECOND), max(event_duration_SECOND)
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
WHERE SHORT_DURATION_CAPPED_FLAG = 1



----uat 11102012

----------------------SECTION 1 INITIAL COUNTS--------------------

----------------------------------------------------------------------------------------------------------------

--Control table to include % null population or default population at event level for the following metrics:

--1.	Panel ID = null 

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where panelid is null

--0 rows

--2.	Account_number = null
--CBI process data at a subscriber level not account level

--3.	Subscriber_id = null or -1
select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where (subscriberid is null or subscriberid = -1)
and panelid = 12

--4.	programme instance dim = null
--CBI process data does not include programme_instance_dim


--5.	Event_type (Viewing / Channel Surf) = null

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_sensitive
where panelid = 12
and event_type is null

--0 rows


--6.	Event Start Time = null

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_TD1
where panelid = 12
and adjusted_event_start_time is null

--0 rows

--7.	Event End Time = null

select count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_9
where x_adjusted_event_end_time is null
and panelid = 12

--0 rows

--8.	Viewing Start Time = null
--same as adjusted_event_start_time I believe as CBI have it compacted to the event and not at instance level
--at this stage

--9.	Viewing End Time = null
--same as adjusted_event_start_time I believe as CBI have it compacted to the event and not at instance level
--at this stage

--10.	stblogcreationdate <= 1970

SELECT count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
WHERE DATE_part('year',stblogcreationdate) < '1980'
and panelid = 12

--25 rows.  will these be filtered out of the next stage

SELECT COUNT(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_TD1
WHERE DATE_part('year',stblogcreationdate) < '1980'
AND PANELID = 12

--13 events where stblogcreationdate < 1980

--THIS IS A KNOWN ISSUE THAT HAS BEEN DELAYED TO BE REMEDIED UNTIL POST GO-LIVE

------------------------------------------------------------------------------------------------------

---recordedtime

SELECT count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.VIEWING_EVENTS_09_SENSITIVE
WHERE DATE_part('year',recordedtime) < '1980'
AND PANELID = 12

--478924 ROWS AT THE INITIAL STAGE

--WILL THESE BE FILTERED OUT OF THE SUBSEQUENT PROCESS

SELECT count(1) from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_TD1
WHERE DATE_part('year',recordedtime) < '1980'
AND PANELID = 12

--0 ROWS SO ALL FILTERED OUT BEFORE WE GO THROUGH THE PROCESS


-----------------------------------------------------------------------------------------
----------------------SECTION 2 COUNTS ON CONTROL TABLE--------------------

------------------------------capped delta-------------------------------------------

--1.	Live / Playback
--2.	Day of the week
--3.	Event Start hour
--4.	Primary / secondary box
--5.	Channel Pack 
--6.	Genre

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_box,silo_genre,silo_hour,silo_channel_pack, 
count(1)as hits,sum(event_duration_second)/60 event_duration,
sum(capped_event_duration_second)/60 capped_event_duration 
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where panelid in (12)
and dateofevent in ('2012-11-10 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_box,silo_genre,silo_hour,silo_channel_pack
order by 1

----------------------SECTION 3 COUNTS ON METRICS--------------------


-----------------------------events_delta metrics

---SHORT_DURATION
--1)

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,
silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
count(1) as hits, sum (short_duration_capped_flag) short_duration_capped,
round(sum(short_duration_capped_flag)/count(1) * 100,2) percent_short_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-10 00:00:00')
GROUP BY silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

---DURATION AS % OF TOTAL PERCENTAGE OF MINS

2.	Duration (mins) / % of total viewing with a minimum cap applied

--
select 
silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,
silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
count(1) as hits, sum (min_cutoff_flag) min_duration_cnt,
round(sum(min_cutoff_flag)/count(1) * 100,2) percent_min_cutoff_flag,
sum(case when short_duration_capped_flag = 1 then capped_event_duration_second else 0 end)/60 short_duration_mins,
sum(capped_event_duration_second)/60 capped_duration_mins,
sum(event_duration_second)/60 uncapped_duration_mins,
sum (short_duration_capped_flag) short_duration_capped
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
and dateofevent in ('2012-11-10 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
GROUP BY silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1


--Long Duration

/* 1) where not assigned random row then either 
 	a) capped duration is adjusted_event_start_time to tx_broadcast_end_time
	b) or maximum cutoff
	
2) For random row assigned
	a) capped end time is derived from the end time of the random event.
 	b) duration is calculated from the adjusted_start_time and capped end time
*/

--3) LONG_DURATION CAPPED FLAG

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(long_duration_capped_flag) long_duration_events,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then 1 else 0 end) long_duration_random,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then 1 else 0 end) long_duration_max_cutoff,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then 1 else 0 end) long_duration_first_prog,
count(1) as events, 
round(sum(LONG_duration_capped_flag)/count(1) * 100,2) percent_long_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-10 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

------------------------------------------------------------------------------------------

------------------------------------------------CBI DATE OF EVENT-------------------------------------------------

--4) Long Duration events minutes
select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(event_duration_second)/60  total_viewing_minutes,
sum(capped_event_duration_second)/60  total_capped_viewing_minutes,
sum(case when long_duration_capped_flag = 1 then capped_event_duration_second else 0 end)/60  total_long_dur_capped_minutes,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then capped_event_duration_second else 0 end)/ 60 long_duration_random_viewing_mins,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then capped_event_duration_second else 0 end)/ 60 long_duration_max_cutoff_viewing_mins,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then capped_event_duration_second else 0 end) / 60 long_duration_first_prog
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
and dateofevent in ('2012-11-10 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1



---------------------------------------------------------------------------------------------------------------

--5) number % capped/uncapped events

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(case when short_duration_capped_flag = 1 then 1 else 0 end) short_duration_capped_count,
sum(case when long_duration_capped_flag = 1 then 1 else 0 end) long_duration_capped_count,
sum(case when long_duration_capped_flag = 0 and short_duration_capped_flag = 0 then 1 else 0 end) uncapped_events_count,
count(1) events_total
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-10 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

--6) duration (mins) of viewing before/after capping

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(event_duration_second)/ 60 total_minutes_viewed,
sum(capped_event_duration_second)/ 60 total_capped_minutes_viewed
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-10 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

--------------------------------------------------------------------------------------------------------------

------------------------------------------4) NTILES WORK--------------------------------------------

--1.

SELECT dateofevent,case when date_part('dow',dateofevent) = 1 then 'Sunday'
when date_part('dow',dateofevent) = 2 then 'Monday'
when date_part('dow',dateofevent) = 3 then 'Tuesday'
when date_part('dow',dateofevent) = 4 then 'Wednesday'
when date_part('dow',dateofevent) = 5 then 'Thursday'
when date_part('dow',dateofevent) = 6 then 'Friday'
when date_part('dow',dateofevent) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
silo_key, ntile_number,
sum(case when ntile_exists_flag = 1 then 1 else 0 end) ntiles_existing,
sum(case when ntile_exists_flag = 0 then 1 else 0 end) ntiles_not_existing
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where panelid in (12)
and dateofevent in ('2012-11-10 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
group by dateofevent,date_part('dow',dateofevent),live_event_flag,silo_hour,SILO_CHANNEL_PACK,
SILO_GENRE,silo_box,silo_key, ntile_number
order by silo_key,ntile_number
limit 1000

--2.

--calculated thresholds by segment

select capped_threshold_key, capped_threshold_type live_event_flag,
capped_threshold_hour silo_hour,capped_threshold_channel_pack, capped_threshold_box, capped_threshold_genre,
capped_threshold_event_duration
from TSTIQ_DIS_ETL.CAPPED_THRESHOLD
order by 1 

--3.

--number /% of thresholds above/below max/min limits

SELECT CAPPING_THRESHOLD_TYPE, COUNT(1) FROM 
(select CAPPED_THRESHOLD_key, case when capped_threshold_event_duration = 20 then 'MIN_THRESHOLD'
						WHEN capped_threshold_event_duration = 120 then 'MAX_THRESHOLD'
						ELSE 'ACTUAL_CAPPED_VALUE' END CAPPING_THRESHOLD_TYPE
FROM TSTIQ_DIS_ETL.CAPPED_THRESHOLD) T
GROUP BY CAPPING_THRESHOLD_TYPE

---------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------
-----viewing per account

drop table tkd_viewing_mins_byaccount

create table tkd_viewing_mins_byaccount
as
(SELECT distinct d.subscriberid, c.account_number,
sum(event_duration_second)/60 uncapped_eventmin_duration,
sum(capped_event_duration_second)/60 capped_eventmin_duration
FROM TSTIQ_MDS..DTH_ACTIVE_VIEWING_CARD_DIM a
  inner join
tstiq_mds..customer_card_asoc b
on a.nk_dth_active_viewing_card_dim = b.nk_dth_active_viewing_card_dim
inner join
tstiq_mds..BILLING_CUSTOMER_ACCOUNT_DIM c
on 
b.nk_billing_customer_account_dim = c.nk_billing_customer_account_dim
right outer join
(SELECT * FROM 
 tstiq_dis_prepare..CAPPED_EVENTS_DELTA 
  WHERE panelid = 12
  and dateofevent in ('2012-11-09 00:00:00')) d
  on
  a.scms_subscriber_id = d.subscriberid
group by d.subscriberid, c.account_number)

----------------------------------------------------------------


--initial checks

--1) check number of panel 12 tx+1 records that exist in the data being processed....

select * from tstiq_dis_prepare..CAPPED_EVENTS_TD2
where panelid = 12 
and (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 1
AND (CAST('2012-08-10' AS DATE))  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));
limit 100

-----------------------------------------------------------------------------------------------------------

----------------check counts of tx + 1 records for processing----------------------------------

select count(1) from 
--TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9
TSTIQ_DIS_PREPARE..capped_events_delta
where panelid in (12)
and DATEOFEVENT = '2012-11-09 00:00:00'
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
and     ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 0
AND (CAST('2012-08-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));


--2) check to ensure that proper fields used for different fields used in process

--live/tv

select distinct capped_threshold_type from capped_threshold

--silo_hour

select distinct capped_threshold_hour from capped_threshold

--channel pack

select distinct capped_threshold_channel_pack from capped_threshold

--genre

select distinct capped_threshold_genre from capped_threshold

--capped_threshold_box

select distinct capped_threshold_box from capped_threshold

--2.5-- identify those records you think are eligibe to be used in ntile process
--using new tx + 1 definition

-----------------------------------------------------------------------------------------------------------------

--3) examples of records being identified for capping

--------------------------------------------------------------------------------------------------------------------

--ntiles exist for everything except prime-time
--live

select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where (capped_threshold_hour not between 15 and 22
and capped_threshold_hour not between 6 and 9)
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= nvl(ntile.box_shut_down,0) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute

-------------------------------------------------------------------------------------

--prime time

select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000

-------------------------------------------------late afternoon-------------------------------------------------------

--------------------------------------------------------

--ntiles exist for late afternoon live

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , max(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 15 and 19
and capped_threshold_type = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000

-----------------------------------------------------------------------------------------------------

--peak morning weekday ntile exists

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select capping_metadata_key,silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.capping_metadata_key, ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.capping_metadata_key,ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000

---8pm weekend

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select capping_metadata_key,silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.capping_metadata_key, ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.capping_metadata_key,ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


---playback

--ntiles exist

select a.* from capped_threshold_ntile a,
(select capped_threshold_key from capped_threshold
where capped_threshold_type = 0) b
where a.silo_key = b.capped_threshold_key
order by 2,1,4


---------------------check short duration capped flag

select min(event_duration_second), max(event_duration_second) from tstiq_dis_prepare..capped_events_delta
where panelid = 12
and short_duration_capped_flag = 1

select min(event_duration_second), max(event_duration_second) from tstiq_dis_prepare..capped_events_delta
where panelid = 12
and short_duration_capped_flag = 0


--------------------check for segments with no panel12 events in there--------------------------------------------

select * from 
(select a.*, (case when identify > 0 then 1 else 0 end) identify_panel12 from 
(select  a.segment_key,a.silo_hour, a.segment_channel,
sum(case when panelid = 12 then 1 else 0 end) identify from TSTIQ_DIS_PREPARE..CAPPed_events_delta a,
TSTIQ_DIS_PREPARE..CAPPING_APPLY_UNCAPPED_EVENTS b
where a.random_row_id = b.random_row_id
and a.assigned_random_row_id = 0
and a.dateofevent = '2012-11-09 00:00:00'
group by a.segment_key,a.silo_hour, a.segment_channel)a) p
where identify_panel12 = 0

--686  rows with no panel12 involved


select * from 
(select a.*, (case when identify > 0 then 1 else 0 end) identify_panel12 from 
(select  a.segment_key,a.silo_hour, a.segment_channel,
sum(case when panelid = 12 then 1 else 0 end) identify from TSTIQ_DIS_PREPARE..CAPPed_events_delta a,
TSTIQ_DIS_PREPARE..CAPPING_APPLY_UNCAPPED_EVENTS b
where a.random_row_id = b.random_row_id
and a.assigned_random_row_id = 0
and a.dateofevent = '2012-11-09 00:00:00'
group by a.segment_key,a.silo_hour, a.segment_channel)a) p
where identify_panel12 = 1

--25112 rows with panel 12 involved

----------------------------------------------------------------------------------------------------------------------

---check that we are getting the correct data times for the tx definition through

SELECT min(DOCUMENTCREATIONDATE), max(DOCUMENTCREATIONDATE)
FROM TSTIQ_DIS_PREPARE..CAPPED_EVENTS
WHERE PANELID = 12
AND DATEOFEVENT = '2012-11-09 00:00:00'

----------------------------yep

--check the long duration > 2 hours events


  -------------------------------------------------------------------------------------------
  -------------------------------------------------counts on 08/11/2012 with revised data sets
  -------------------------------------------------------------------------------------------
  
  -----------------------------------------------------------------------------------------
----------------------SECTION 2 COUNTS ON CONTROL TABLE--------------------

------------------------------capped delta-------------------------------------------

--1.	Live / Playback
--2.	Day of the week
--3.	Event Start hour
--4.	Primary / secondary box
--5.	Channel Pack 
--6.	Genre

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_box,silo_genre,silo_hour,silo_channel_pack, 
count(1)as hits,sum(event_duration_second)/60 event_duration,
sum(capped_event_duration_second)/60 capped_event_duration 
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_full
where panelid in (12)
and dateofevent in ('2012-11-08 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_box,silo_genre,silo_hour,silo_channel_pack
order by 1

----------------------SECTION 3 COUNTS ON METRICS--------------------


-----------------------------events_delta metrics

---SHORT_DURATION
--1)

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,
silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
count(1) as hits, sum (short_duration_capped_flag) short_duration_capped,
round(sum(short_duration_capped_flag)/count(1) * 100,2) percent_short_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-08 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-08 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
GROUP BY silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

---DURATION AS % OF TOTAL PERCENTAGE OF MINS

2.	Duration (mins) / % of total viewing with a minimum cap applied

--
select 
silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,
silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
count(1) as hits, sum (min_cutoff_flag) min_duration_cnt,
round(sum(min_cutoff_flag)/count(1) * 100,2) percent_min_cutoff_flag,
sum(case when short_duration_capped_flag = 1 then capped_event_duration_second else 0 end)/60 short_duration_mins,
sum(capped_event_duration_second)/60 capped_duration_mins,
sum(event_duration_second)/60 uncapped_duration_mins,
sum (short_duration_capped_flag) short_duration_capped
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
GROUP BY silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1


--Long Duration

/* 1) where not assigned random row then either 
 	a) capped duration is adjusted_event_start_time to tx_broadcast_end_time
	b) or maximum cutoff
	
2) For random row assigned
	a) capped end time is derived from the end time of the random event.
 	b) duration is calculated from the adjusted_start_time and capped end time
*/

--3) LONG_DURATION CAPPED FLAG

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(long_duration_capped_flag) long_duration_events,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then 1 else 0 end) long_duration_random,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then 1 else 0 end) long_duration_max_cutoff,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then 1 else 0 end) long_duration_first_prog,
count(1) as events, 
round(sum(LONG_duration_capped_flag)/count(1) * 100,2) percent_long_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

------------------------------------------------------------------------------------------

------------------------------------------------CBI DATE OF EVENT-------------------------------------------------

select DATEOFEVENT,case when date_part('dow',DATEOFEVENT) = 1 then 'Sunday'
when date_part('dow',DATEOFEVENT) = 2 then 'Monday'
when date_part('dow',DATEOFEVENT) = 3 then 'Tuesday'
when date_part('dow',DATEOFEVENT) = 4 then 'Wednesday'
when date_part('dow',DATEOFEVENT) = 5 then 'Thursday'
when date_part('dow',DATEOFEVENT) = 6 then 'Friday'
when date_part('dow',DATEOFEVENT) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(long_duration_capped_flag) long_duration_events,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then 1 else 0 end) long_duration_random,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then 1 else 0 end) long_duration_max_cutoff,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then 1 else 0 end) long_duration_first_prog,
count(1) as events, 
round(sum(LONG_duration_capped_flag)/count(1) * 100,2) percent_long_duration
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and DATEOFEVENT in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and DATEOFEVENT in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
group by DATEOFEVENT,date_part('dow',DATEOFEVENT),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1



--4) Long Duration events minutes

select * from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where capped_event_duration_minute = 20
and long_duration_capped_flag = 1
and panelid = 12

select * from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where silo_key in 
(select capped_threshold_key from capped_threshold
where capped_threshold_event_duration = 20
and capped_threshold_min_cutoff_flag = 1)
and panelid = 12
and event_duration_minute > 20
and assigned_random_row_id = 0

(select * from capped_threshold
where capped_threshold_event_duration = 20
and capped_threshold_min_cutoff_flag = 1)

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(event_duration_second)/60  total_viewing_minutes,
sum(capped_event_duration_second)/60  total_capped_viewing_minutes,
sum(case when long_duration_capped_flag = 1 then capped_event_duration_second else 0 end)/60  total_long_dur_capped_minutes,
sum(case when long_duration_capped_flag = 1 and assigned_random_row_id > 0 then capped_event_duration_second else 0 end)/ 60 long_duration_random_viewing_mins,
sum(case when long_duration_capped_flag = 1 and max_cutoff_flag = 1 then capped_event_duration_second else 0 end)/ 60 long_duration_max_cutoff_viewing_mins,
sum(case when long_duration_capped_flag = 1 and segment_prog_flag = 1 and max_cutoff_flag = 0 then capped_event_duration_second else 0 end) / 60 long_duration_first_prog
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1


select date(recordedtime), count(1)
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where panelid in (12)
and dateofevent in ('2012-11-09 00:00:00')
and live_event_flag = 0
group by date(recordedtime)

SELECT DATE ('2012-11-08') - 28

---------------------------------------------------------------------------------------------------------------

--5) number % capped/uncapped events

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(case when short_duration_capped_flag = 1 then 1 else 0 end) short_duration_capped_count,
sum(case when long_duration_capped_flag = 1 then 1 else 0 end) long_duration_capped_count,
sum(case when long_duration_capped_flag = 0 and short_duration_capped_flag = 0 then 1 else 0 end) uncapped_events_count,
count(1) events_total
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1

---------------------------------------------------------------------------------------------------------------

--6) duration (mins) of viewing before/after capping

select silo_date,case when date_part('dow',silo_date) = 1 then 'Sunday'
when date_part('dow',silo_date) = 2 then 'Monday'
when date_part('dow',silo_date) = 3 then 'Tuesday'
when date_part('dow',silo_date) = 4 then 'Wednesday'
when date_part('dow',silo_date) = 5 then 'Thursday'
when date_part('dow',silo_date) = 6 then 'Friday'
when date_part('dow',silo_date) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
sum(event_duration_second)/ 60 total_minutes_viewed,
sum(capped_event_duration_second)/ 60 total_capped_minutes_viewed
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
--from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS
where panelid in (12)
--and silo_date in ('2012-11-09 00:00:00','2012-08-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
and dateofevent in ('2012-11-09 00:00:00')
group by silo_date,date_part('dow',silo_date),live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box
order by 1


--------------------------------------------------------------------------------------------------------------

------------------------------------------4) NTILES WORK--------------------------------------------

--1.

select * from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where event_type = 'EVCHANGEVIEW'
limit 1000

SELECT dateofevent,case when date_part('dow',dateofevent) = 1 then 'Sunday'
when date_part('dow',dateofevent) = 2 then 'Monday'
when date_part('dow',dateofevent) = 3 then 'Tuesday'
when date_part('dow',dateofevent) = 4 then 'Wednesday'
when date_part('dow',dateofevent) = 5 then 'Thursday'
when date_part('dow',dateofevent) = 6 then 'Friday'
when date_part('dow',dateofevent) = 7 then 'Saturday' else null end day_of_week,
live_event_flag,silo_hour,SILO_CHANNEL_PACK,SILO_GENRE,silo_box,
silo_key, ntile_number,
sum(case when ntile_exists_flag = 1 then 1 else 0 end) ntiles_existing,
sum(case when ntile_exists_flag = 0 then 1 else 0 end) ntiles_not_existing
from TSTIQ_DIS_PREPARE.TSTIQ_DIS_ETL.CAPPED_EVENTS_delta
where panelid in (12)
and dateofevent in ('2012-11-09 00:00:00')
--and silo_date in ('2012-11-09 00:00:00')
group by dateofevent,date_part('dow',dateofevent),live_event_flag,silo_hour,SILO_CHANNEL_PACK,
SILO_GENRE,silo_box,silo_key, ntile_number
order by silo_key,ntile_number
limit 1000

--2.

--calculated thresholds by segment

select capped_threshold_key, capped_threshold_type live_event_flag,
capped_threshold_hour silo_hour,capped_threshold_channel_pack, capped_threshold_box, capped_threshold_genre,
capped_threshold_event_duration
from TSTIQ_DIS_ETL.CAPPED_THRESHOLD
order by 1 


--3.

--number /% of thresholds above/below max/min limits

SELECT CAPPING_THRESHOLD_TYPE, COUNT(1) FROM 
(select CAPPED_THRESHOLD_key, case when capped_threshold_event_duration = 20 then 'MIN_THRESHOLD'
						WHEN capped_threshold_event_duration = 120 then 'MAX_THRESHOLD'
						ELSE 'ACTUAL_CAPPED_VALUE' END CAPPING_THRESHOLD_TYPE
FROM TSTIQ_DIS_ETL.CAPPED_THRESHOLD) T
GROUP BY CAPPING_THRESHOLD_TYPE

---------------------------------------------------------------------------------------------------------------

drop table tkd_viewing_mins_byaccount

create table tkd_viewing_mins_byaccount
as
(SELECT distinct d.subscriberid, c.account_number,
sum(event_duration_second)/60 uncapped_eventmin_duration,
sum(capped_event_duration_second)/60 capped_eventmin_duration
FROM TSTIQ_MDS..DTH_ACTIVE_VIEWING_CARD_DIM a
  inner join
tstiq_mds..customer_card_asoc b
on a.nk_dth_active_viewing_card_dim = b.nk_dth_active_viewing_card_dim
inner join
tstiq_mds..BILLING_CUSTOMER_ACCOUNT_DIM c
on 
b.nk_billing_customer_account_dim = c.nk_billing_customer_account_dim
right outer join
(SELECT * FROM 
 tstiq_dis_prepare..CAPPED_EVENTS_DELTA 
  WHERE panelid = 12
  and dateofevent in ('2012-11-09 00:00:00')) d
  on
  a.scms_subscriber_id = d.subscriberid
group by d.subscriberid, c.account_number)

SELECT * FROM 
 tstiq_dis_prepare..CAPPED_EVENTS_DELTA 
  WHERE panelid = 12
  and DATEOFEVENT = '2012-11-08 00:00'

----------------------------------------------------------------


--initial checks

--1) check number of panel 12 tx+1 records that exist in the data being processed....

select * from tstiq_dis_prepare..CAPPED_EVENTS_TD2
where panelid = 12 
and (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 1
AND (CAST('2012-08-10' AS DATE))  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));
limit 100

-----------------------------------------------------------------------------------------------------------

----------------check counts of tx + 1 records for processing----------------------------------

select count(1) from 
--TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9
TSTIQ_DIS_PREPARE..capped_events_delta
where panelid in (12)
and DATEOFEVENT = '2012-11-09 00:00:00'
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
and     ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 0
AND (CAST('2012-08-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));

--2) check to ensure that proper fields used for different fields used in process

--live/tv

select distinct capped_threshold_type from capped_threshold

--silo_hour

select distinct capped_threshold_hour from capped_threshold

--channel pack

select distinct capped_threshold_channel_pack from capped_threshold

--genre

select distinct capped_threshold_genre from capped_threshold

--capped_threshold_box

select distinct capped_threshold_box from capped_threshold

--2.5-- identify those records you think are eligibe to be used in ntile process
--using new tx + 1 definition

-----------------------------------------------------------------------------------------------------------------

--3) examples of records being identified for capping

--------------------------------------------------------------------------------------------------------------------

--ntiles exist for everything except prime-time
--live


select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where (capped_threshold_hour not between 15 and 22
and capped_threshold_hour not between 6 and 9)
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= nvl(ntile.box_shut_down,0) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute

-------------------------------------------------------------------------------------

--prime time

select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000

-------------------------------------------------late afternoon-------------------------------------------------------

--------------------------------------------------------

--ntiles exist for late afternoon live

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , max(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 15 and 19
and capped_threshold_type = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


-----------------------------------------------------------------------------------------------------

--peak morning weekday ntile exists

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select capping_metadata_key,silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.capping_metadata_key, ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.capping_metadata_key,ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


---playback

--ntiles exist

select a.* from capped_threshold_ntile a,
(select capped_threshold_key from capped_threshold
where capped_threshold_type = 0) b
where a.silo_key = b.capped_threshold_key
order by 2,1,4

---------------------check short duration capped flag

select min(event_duration_second), max(event_duration_second) from tstiq_dis_prepare..capped_events_delta
where panelid = 12
and short_duration_capped_flag = 1

select min(event_duration_second), max(event_duration_second) from tstiq_dis_prepare..capped_events_delta
where panelid = 12
and short_duration_capped_flag = 0


  ----------------------------------------------------------checks 22/11/2012-----------------------------------------------------------------
  
  select * FROM FINAL_EPG_SCHEDULE
  limit 100
  
  SELECT distinct tx_date_utc ,service_key,ssp_network_id, transport_id, 
  service_id,channel_name,genre_description,epg_channel, epg_group_name,
  tx_date_time_utc
  FROM FINAL_EPG_SCHEDULE
 where tx_date_utc in ('2012-11-10 00:00:00')
 order by tx_date_time_utc
 
 select service_key, full_name,channel_name,channel_genre,channel_pack,update_date 
 from TSTIQ_DIS_REFERENCE.TSTIQ_DIS_ETL.service_key_attributes
 where effective_to = '2999-12-31 00:00:00'
 
 select distinct originalnetworkid, transportstreamid, siserviceid, servicekey,silo_channel_pack, silo_genre from tstiq_dis_prepare..CAPPED_EVENTS_DELTA
 where panelid = 12
 
 select distinct tx_date_time_utc,genre_description,service_id, dk_channel_dim  from final_epg_schedule
 where tx_date_utc = '2012-11-09 00:00:00'
 and service_id = 4329

 select a.* from
( select distinct cast(dummy1 as varchar(20)) tx_date_time,silo_genre, cast(siserviceid  as varchar(20)) serviceid
 from tstiq_dis_prepare..capped_events_delta
 where panelid = 12
 and siserviceid is not null
 and dateofevent = '2012-11-09 00:00:00') a,
 (select distinct cast(tx_date_time_utc as varchar(20)) tx_date_time,genre_description,cast(service_id as varchar(20))service_id 
 from final_epg_schedule
 where tx_date_utc = '2012-11-09 00:00:00'
 and service_id is not null)  b
where  a.serviceid = b.service_id
 and a.tx_date_time = b.tx_date_time
 and upper(a.silo_genre) <> upper(b.genre_description)

-----------------------------------------------------------------------------------------------------------------------------------------------

select * from tstiq_dis_prepare..capped_events
limit 100



--479416 accounts

create table tkd_viewing_mins_byaccount_09122012
as
(SELECT distinct d.subscriberid, c.account_number,
sum(event_duration_second)/60 uncapped_eventmin_duration,
sum(capped_event_duration_second)/60 capped_eventmin_duration
FROM TSTIQ_MDS..DTH_ACTIVE_VIEWING_CARD_DIM a
  inner join
tstiq_mds..customer_card_asoc b
on a.nk_dth_active_viewing_card_dim = b.nk_dth_active_viewing_card_dim
inner join
tstiq_mds..BILLING_CUSTOMER_ACCOUNT_DIM c
on 
b.nk_billing_customer_account_dim = c.nk_billing_customer_account_dim
right outer join
(SELECT * FROM 
 tstiq_dis_prepare..CAPPED_EVENTS_DELTA 
  WHERE panelid = 12
  and dateofevent in ('2012-11-09 00:00:00')) d
  on
  a.scms_subscriber_id = d.subscriberid
group by d.subscriberid, c.account_number)


----------------------------------------------------------------

--initial checks

--1) check number of panel 12 tx+1 records that exist in the data being processed....

select * from tstiq_dis_prepare..CAPPED_EVENTS_TD2
where panelid = 12 
and (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 1
AND (CAST('2012-08-10' AS DATE))  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));
limit 100

((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 2
AND (CAST('2012-08-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));

select documentcreationdate, CAST('2012-08-10' AS DATE) + 1 + cast(2|| ' hours' as interval) date_gone
from 
TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9
limit 100

select CAST('2012-08-10' AS DATE) - 2,
CAST('2012-08-10' AS DATE) + 1 - cast('1 seconds' as interval)
from TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9_20120811
limit 100

-----------------------------------------------------------------------------------------------------------

----------------check counts of tx + 1 records for processing----------------------------------

select count(1) from 
--TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9
TSTIQ_DIS_PREPARE..capped_events_delta
where panelid in (12)
and DATEOFEVENT = '2012-11-08 00:00:00'
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
and     ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 0
AND (CAST('2012-08-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));


select count(1) from 
TSTIQ_DIS_PREPARE..VIEWING_EVENTS_9_20120811
where panelid in (12)
and DATEOFEVENT = '2012-11-08 00:00:00'
and x_type_of_viewing_event <> 'Non viewing event'
and event_type = 'EVCHANGEVIEW'
and     ((PANELID IN (12) AND (ADJUSTED_EVENT_START_TIME BETWEEN CAST('2012-08-10' AS DATE) - 0
AND (CAST('2012-08-10' AS DATE) + 1)  - cast('1 seconds' as interval))
AND DOCUMENTCREATIONDATE <= (CAST('2012-08-10' AS DATE) + 1)
+ cast(2|| ' hours' as interval)));

--2) check to ensure that proper fields used for different fields used in process

--live/tv

select distinct capped_threshold_type from capped_threshold

--silo_hour

select distinct capped_threshold_hour from capped_threshold

--channel pack

select distinct capped_threshold_channel_pack from capped_threshold

--genre

select distinct capped_threshold_genre from capped_threshold

--capped_threshold_box

select distinct capped_threshold_box from capped_threshold

--2.5-- identify those records you think are eligibe to be used in ntile process
--using new tx + 1 definition

-----------------------------------------------------------------------------------------------------------------

--3) examples of records being identified for capping

--------------------------------------------------------------------------------------------------------------------

--ntiles exist for everything except prime-time
--live


select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where (capped_threshold_hour not between 15 and 22
and capped_threshold_hour not between 6 and 9)
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= nvl(ntile.box_shut_down,0) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute


(select * from capped_threshold
where (capped_threshold_hour not between 15 and 22
and capped_threshold_hour not between 6 and 9)
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1)


-------------------------------------------------------------------------------------

--prime time

select a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000

select a.silo_key, a.silo_hour, max(ntile_number),max(median_duration) from capped_threshold_ntile a,
(select capped_threshold_key silo_key from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1) b
where a.silo_key = b.silo_key
group by a.silo_key,a.silo_hour


-------------------------------------------------late afternoon-------------------------------------------------------

--------------------------------------------------------

--ntiles exist for late afternoon live

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.silo_key,ntile.threshold_ntile , max(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 15 and 19
and capped_threshold_type = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
group by ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


-----------------------------------------------------------------------------------------------------

--peak morning weekday ntile exists

select a.capping_metadata_key,a.silo_key, a.event_duration_second,a.event_duration_minute, b.min_duration, 
case when b.min_duration < 20 then 20 
when b.min_duration > 120 then 120 else b.min_duration end capped_event_duration,
a.short_duration_capped_flag,
a.long_duration_capped_flag from TSTIQ_DIS_PREPARE..capped_events_delta a,
(select a.silo_key, a.min_duration from capped_threshold_ntile a,
(select capping_metadata_key,silo_key,ntile - threshold_ntile ntile_actual from 
(select ntile.capping_metadata_key, ntile.silo_key,ntile.threshold_ntile , min(ntile.ntile_number) ntile
from capped_threshold_ntile ntile,
(select * from capped_threshold
where capped_threshold_hour between 20 and 22
and capped_threshold_type = 1
and capped_threshold_ntileexist_flag = 1) threshold
where ntile.silo_key = threshold.capped_threshold_key
and ntile.median_duration >= (((hour_24_clock_last_hour) - silo_hour - 1) * hour_in_minutes + box_shut_down) --4 hour box shutdown
group by ntile.capping_metadata_key,ntile.silo_key, ntile.threshold_ntile) ntile_cho)b
where a.silo_key = b.silo_key
and a.ntile_number = b.ntile_actual ) b
where a.silo_key = b.silo_key
order by silo_key, event_duration_minute
limit 100000


---playback

--ntiles exist

select a.* from capped_threshold_ntile a,
(select capped_threshold_key from capped_threshold
where capped_threshold_type = 0) b
where a.silo_key = b.capped_threshold_key
order by 2,1,4

