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

**Project Name: 					SLOTS 2.7 TESTS (SYBASE VERSION)
**Analysts:							Tony Kinnaird
**									Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
**									Angai Maruthavanan	(Angai.Maruthavanan@SkyIQ.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						VESPA TEAM / CBI

									
**Business Brief:

	To Provide Checks/counts we can then benchmark in order to measure the quality of results produced on each release

**Sections:

	A: INTO PROGRAMME
	B: CAPPING
	C: MINUTE ATTRIBUTION
	D: SCALING
	E: CHANNEL MAPPING
	F: DATA COMPLETENESS
	G: DATA INTEGRITY
	
*/


--------------------
/* CREATE staging table to capture slots data */
--------------------


if object_id('DATA_QUALITY_SLOT_PROD') is not null then drop table DATA_QUALITY_SLOT_PROD end if;

create table DATA_QUALITY_SLOT_PROD
(SLOT_DATA_KEY	bigint	Not Null,
VIEWED_START_DATE_KEY	integer	,
IMPACTS	smallint	,
RECORD_DATE	timestamp	,
HOUSEHOLD_KEY	bigint	,
IMPACT_DAY	timestamp Not Null,
slot_instance_key	bigint	,
channel_key	integer	,
slot_start_date_key	integer	,
slot_start_time_key	integer	,
slot_end_date_key	integer	,
slot_end_time_key	integer	,
previous_programme_key	bigint	,
next_programme_key	bigint	,
prev_prog_schedule_key	bigint	,
next_prog_schedule_key	bigint	,
prev_broadcast_start_date	date,
next_broadcast_start_date	date,
prev_broadcast_start_time	time,
next_broadcast_start_time	time,
slot_start_date	date,
slot_end_date	date,
slot_start_time	time,
slot_end_time	time,
scaling_factor	double,
prev_broadcast_end_time	time,
next_broadcast_end_time	time,
prev_broadcast_end_date	date,
next_broadcast_end_date	date,
slot_key bigint,
viewed_duration decimal(15),
viewed_start_time_key int,
time_shift_key int);


--------------------
/* Grant Select rights to relevant user groups */
--------------------


grant select on DATA_QUALITY_SLOT_PROD to vespa_group_low_security, sk_prodreg;

commit;
go

------------------------------------------------------------------------------------------------------------------------------------------------

--------------------
/* Set dates you want to run this for in format yyyymmdd */
--------------------


declare @viewing_day_min int
declare @viewing_day_max int

set @viewing_day_min = 20130611
set @viewing_day_max = 20130612

--------------------
/* truncate staging table */
--------------------


truncate table DATA_QUALITY_SLOT_PROD


--------------------
/* insert relevant data into staging table from slot_data, slot_instance and left joining to get key info*/
--------------------


insert into DATA_QUALITY_SLOT_PROD
SELECT  SLOT_DATA_KEY, VIEWED_START_DATE_KEY, IMPACTS, RECORD_DATE, HOUSEHOLD_KEY, UTC_DATEHOUR IMPACT_DAY,
slot_inst_data.slot_instance_key,
slot_inst_data.channel_key,
slot_inst_data.slot_start_date_key,
slot_inst_data.slot_start_time_key,
slot_inst_data.slot_end_date_key,
slot_inst_data.slot_end_time_key,
slot_inst_data.previous_programme_key,
slot_inst_data.next_programme_key,
slot_inst_data.prev_prog_schedule_key,
slot_inst_data.next_prog_schedule_key,
slot_inst_data.prev_broadcast_start_date,
slot_inst_data.next_broadcast_start_date,
slot_inst_data.prev_broadcast_start_time,
slot_inst_data.next_broadcast_start_time,
slot_inst_data.slot_start_date,
slot_inst_data.slot_end_date,
slot_inst_data.slot_start_time,
slot_inst_data.slot_end_time,
slot.scaling_factor,
slot_inst_data.prev_broadcast_end_time,
slot_inst_data.next_broadcast_end_time,
slot_inst_data.prev_broadcast_end_date,
slot_inst_data.next_broadcast_end_date,
SLOT.SLOT_KEY,
slot.viewed_duration,
slot.viewed_start_time_key,
slot.time_shift_key
FROM SK_PROD.SLOT_DATA slot
inner join
sk_prod.viq_date viq_date
on
slot.viewed_start_date_key = viq_date.pk_datehour_dim
left outer join
(select slot_instance_key, channel_key, slot_start_date_key, slot_start_time_key, slot_end_date_key, slot_end_time_key,
previous_programme_key,prev_prog_schedule_key, next_programme_key,next_prog_schedule_key,
prev_prog_start_date.local_day_date prev_broadcast_start_date,
prev_prog_end_date.local_day_date prev_broadcast_end_date,
next_prog_start_date.local_day_date next_broadcast_start_date,
next_prog_end_date.local_day_date next_broadcast_end_date,
prev_prog_start_time.local_time_minute prev_broadcast_start_time,
prev_prog_end_time.local_time_minute prev_broadcast_end_time,
next_prog_start_time.local_time_minute next_broadcast_start_time,next_prog_end_time.local_time_minute next_broadcast_end_time,
slot_start_date.local_day_date slot_start_date,
slot_end_date.local_day_date slot_end_date,
slot_start_time.local_time_minute slot_start_time,
slot_end_time.local_time_minute slot_end_time
from sk_prod.slot_instance slot_inst
left outer join
sk_prod.viq_programme_schedule viq_prev_prog_sched
on
slot_inst.prev_prog_schedule_key = viq_prev_prog_sched.programme_instance_id
left outer join
sk_prod.viq_programme_schedule viq_next_prog_sched
on
slot_inst.next_prog_schedule_key = viq_next_prog_sched.programme_instance_id
left outer join
sk_prod.viq_date prev_prog_start_date
on
viq_prev_prog_sched.dk_start_datehour = prev_prog_start_date.pk_datehour_dim
left outer join
sk_prod.viq_date prev_prog_end_date
on
viq_prev_prog_sched.dk_end_datehour = prev_prog_end_date.pk_datehour_dim
left outer join
sk_prod.viq_date next_prog_start_date
on
viq_next_prog_sched.dk_start_datehour = next_prog_start_date.pk_datehour_dim
left outer join
sk_prod.viq_date next_prog_end_date
on
viq_next_prog_sched.dk_end_datehour = next_prog_end_date.pk_datehour_dim
left outer join
sk_prod.viq_time prev_prog_start_time
on
viq_prev_prog_sched.dk_start_time = prev_prog_start_time.pk_time_dim
left outer join
sk_prod.viq_time prev_prog_end_time
on
viq_prev_prog_sched.dk_end_time = prev_prog_end_time.pk_time_dim
left outer join
sk_prod.viq_time next_prog_start_time
on
viq_next_prog_sched.dk_start_time = next_prog_start_time.pk_time_dim
left outer join
sk_prod.viq_time next_prog_end_time
on
viq_next_prog_sched.dk_end_time = next_prog_end_time.pk_time_dim
left outer join
sk_prod.viq_date slot_start_date
on
slot_inst.slot_start_date_key = slot_start_date.pk_datehour_dim
left outer join
sk_prod.viq_date slot_end_date
on
slot_inst.slot_end_date_key = slot_end_date.pk_datehour_dim
left outer join
sk_prod.viq_time slot_start_time
on
slot_inst.slot_start_time_key = slot_start_time.pk_time_dim
left outer join
sk_prod.viq_time slot_end_time
on
slot_inst.slot_end_time_key = slot_end_time.pk_time_dim) slot_inst_data
on
slot.slot_instance_key = slot_inst_data.slot_instance_key
where slot.viewed_start_date_key/100  >= @viewing_day_min 
and slot.viewed_start_date_key/100 <= @viewing_day_max
commit


--------------------
/* Create temp table to store results in.  These indexs all follow the regression test suite created by Angel*/
--------------------


CREATE TABLE #TMP_OLIVE_SLOTS_RESULTS
(INDEX_RESULT VARCHAR(10),
viewing_date int,
RESULT FLOAT)

----------------------------------------------------------------------------------------------------------------------------------------------------------------------

--OLIVE EQUIVALENT

INSERT INTO #TMP_OLIVE_SLOTS_RESULTS

-----------------------
/* A: INTO PROGRAMME */
-----------------------

--I1	Check all values for DK_SLOT/PROG_INSTANCE_DIM are found in the Slot/prog Instance Dimension table

(
(SELECT 'I1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when SLOT_INSTANCE_KEY is null then 1 else 0 end) count FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100)

-- Check all values for DK_SLOT/PROG_DIM are found in the Slot/prog Dimension table
UNION ALL
--olive query

--I2	Check all values for DK_SLOT/PROG_DIM are found in the Slot/prog Dimension table


SELECT 'I2' as index_,
viewed_start_date_key/100 VIEWING_DAY,
COUNT(1) no_slot_prog_dimension
FROM DATA_QUALITY_SLOT_PROD fact
where not exists
(select 1 from sk_prod.slot slot
where fact.slot_key = slot.slot_key)
GROUP BY viewed_start_date_key/100

-- Check all values for DK_PRECEDING_PROGRAMME_INSTANCE_DIM are found in the Programmes Dimension table

UNION ALL

---olive equivalent------

--I3	Check all values for DK_PRECEDING_PROGRAMME_INSTANCE_DIM are found in the Programmes Dimension table

SELECT 'I3' as index_,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when prev_prog_schedule_key is null then 1 else 0 end) cnt FROM DATA_QUALITY_SLOT_PROD fact
GROUP BY viewed_start_date_key/100

--Check all values for DK_PRECEDING_PROGRAMME_DIM are found in the Programme Instance Dimension table
UNION ALL
---olive equivalent

--I4	Check all values for DK_PRECEDING_PROGRAMME_DIM are found in the Programme Instance Dimension table


SELECT 'I4' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when previous_programme_key is null then 1 else 0 end) cnt
FROM DATA_QUALITY_SLOT_PROD fact
GROUP BY viewed_start_date_key/100

UNION ALL

--I5	Check all values for DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM are found in the Programme Instance Dimension table


select	'I5' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_prog_schedule_key is null then 1 else 0 end) cnt
FROM DATA_QUALITY_SLOT_PROD fact
GROUP BY viewed_start_date_key/100

UNION ALL

---olive equivalent------

--I6	Check all values for DK_SUCCEEDING_PROGRAMME_DIM are found in the Programmes Dimension table


SELECT 'I6' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_programme_key is null then 1 else 0 end)cnt
FROM DATA_QUALITY_SLOT_PROD fact
GROUP BY viewed_start_date_key/100

-- Check all values for DK_CHANNEL_DIM are found in the Channels Dimension table
UNION ALL

----olive equivalent--------------------------------

--I7	Check all values for DK_CHANNEL_DIM are found in the Channels Dimension table


select a.*, isnull(b.cnt,0) cnt from 
(select distinct 'I7' as theindex,
viewed_start_date_key/100 viewing_day
from DATA_QUALITY_SLOT_PROD ) a
left outer join 
(select	'I7' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(distinct channel_key) cnt from DATA_QUALITY_SLOT_PROD a
where not exists
(select 1 from sk_prod.viq_channel b
where a.channel_key = b.pk_channel_dim)
and channel_key > 0
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day


UNION ALL

--I8-1	Check after null or -1 for preceding programme dim

select	'I8-1' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when previous_programme_key = -1 then 1 else 0 end) cnt from DATA_QUALITY_SLOT_PROD
where previous_programme_key = -1
GROUP BY viewed_start_date_key/100

UNION ALL

--------------------------------------olive equivalent-------------------------

--I8-2	Check after null or -1 for preceding programme Instance Dim

select	'I8-2' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum (case when prev_prog_schedule_key = -1 then 1 else 0 end )cnt from DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100

-- Check after null or -1 for succeeding programme dim

UNION ALL
--------------------------------------olive equivalent-------------------------

--I8-3	Check after null or -1 for succeeding programme dim


select	'I8-3' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_programme_key = -1 then 1 else 0 end) cnt from DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100

-- Check after null or -1 for succeeding programme Instance Dim

--------------------------------------olive equivalent-------------------------

UNION ALL

--I8-4	Check after null or -1 for succeeding programme Instance Dim



select	'I8-4' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_prog_schedule_key = -1 then 1 else 0 end) cnt from DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100

-- Count records with duration set to null or -1

UNION ALL
---olive equivalent-------------------------------

--I9	Count records with duration set to null or -1


select	'I9' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when viewed_duration is null or viewed_duration < 0 then 1 else 0 end) cnt
from DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100

UNION ALL
---olive equivalent-------------------------------


--I10	Check if PK of SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC is unique
select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'I10' as theindex,
viewed_start_date_key/100 viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'I10' as theindex,
VIEWING_DAY,
count(1) cnt from
(select viewed_start_date_key/100 viewing_day,slot_data_key from DATA_QUALITY_SLOT_PROD
group by viewed_start_date_key/100, slot_data_key
having count(*) > 1) t
group by viewing_day) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)


UNION ALL
---olive equivalent-------------------------------

-- I11 Spots shown between same programme do not exceed broadcast time constraints

select a.*, isnull(b.cnt, 0) cnt
from
((select distinct 'I11' as theindex,
viewed_start_date_key/100 VIEWING_DAY
from DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'I11' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_PROD
where prev_prog_schedule_key = next_prog_schedule_key
and prev_prog_schedule_key > 0
and next_prog_schedule_key > 0
and (slot_start_date ||' '|| slot_start_time < prev_broadcast_start_date || ' ' || prev_broadcast_start_time
--or slot_end_date ||' '|| slot_end_time > next_broadcast_end_date || ' ' || next_broadcast_end_time)
or slot_end_date ||' '|| slot_end_time > prev_broadcast_end_date || ' ' || prev_broadcast_end_time)
GROUP BY viewed_start_date_key/100) b
on
a.theindex = b.theindex
and a.viewing_day = b.viewing_day)


UNION ALL

--I12	Spots shown between different programmes do not exceed broadcast time constraints

select a.*, isnull(b.cnt, 0) cnt from
((select	distinct 'I12' as theindex,
viewed_start_date_key/100 viewing_day from
DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'I12' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_PROD
where prev_prog_schedule_key != next_prog_schedule_key
and prev_prog_schedule_key > 0
and next_prog_schedule_key > 0
and (slot_start_date ||' '|| slot_start_time < prev_broadcast_start_date || ' ' || prev_broadcast_start_time
or slot_end_date ||' '|| slot_end_time > next_broadcast_end_date || ' ' || next_broadcast_end_time)
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)
 
UNION ALL

--I13	Check for preceding programme <> succeding programme where starting times are the same


select a.*, isnull(b.cnt, 0) cnt from
((select	distinct 'I13' as theindex,
viewed_start_date_key/100 viewing_day from
DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'I13' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_PROD
where prev_prog_schedule_key != next_prog_schedule_key
and prev_prog_schedule_key > 0
and next_prog_schedule_key > 0
and prev_broadcast_start_date || ' ' || prev_broadcast_start_time = next_broadcast_start_date || ' ' || next_broadcast_start_time
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)


UNION ALL

--I14	Check for preceding programme = succeding programme where starting times are different


select a.*, isnull(b.cnt, 0) cnt from
((select	distinct 'I14' as theindex,
viewed_start_date_key/100 viewing_day from
DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'I14' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_PROD
where prev_prog_schedule_key = next_prog_schedule_key
and prev_prog_schedule_key > 0
and next_prog_schedule_key > 0
and prev_broadcast_start_date || ' ' || prev_broadcast_start_time != next_broadcast_start_date || ' ' || next_broadcast_start_time
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)


UNION ALL

----------------
/* B: CAPPING */
----------------

--C1 - Check we have values for cap end date and time in the fact table -not applicable

select distinct 'C1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C2 - Check capped end time is greater thant event start time

select distinct 'C2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C3 - Check capped end time is less than or equal event end time

select distinct 'C3' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD


union all

--C4 - Check capped end time is greater thant event start time

select distinct 'C4' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD


union all


--C5-1	Check we don't have capped date and not time


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'C5-1' as theindex,
viewed_start_date_key/100 viewing_day from
DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'C5-1' theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_PROD
where viewed_start_date_key > 0
and (viewed_start_time_key is null or viewed_start_time_key = -1)
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)


UNION ALL

--C5-2	Check we don't have capped time and not date

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'C5-2' as theindex,
viewed_start_date_key/100 viewing_day from
DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'C5-2' theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_PROD
where viewed_start_time_key > 0
and (viewed_start_date_key is null or viewed_start_date_key = -1)
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)

UNION ALL

--C6-1	Check we have values for the full flag available in the fact

select distinct 'C6-1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C6-2	Check we have values for the partial flag available in the fact

select distinct 'C6-2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C7-1	Check pre capped duration for programmes

select distinct 'C7-1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C7-2	Check post capped duration for programmes

select distinct 'C7-2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C8-1	Check hours viewed per box pre capped duration for both programmes/slots

select distinct 'C8-1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C8-2	Check hours viewed per box post capped duration for both programmes/slots

select distinct 'C8-2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD


UNION ALL

--C9	Check no overlap between partial or full capped flag

select distinct 'C9' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--C10	Check capped date/time not null if capped flag is 1

select distinct 'C10' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

---------------------------
/* C: MINUTE ATTRIBUTION */
---------------------------

--M1	Checking no duplicated VIEWING_EVENT_ID on FINAL_MINUTE_ATTRIBUTION

select distinct 'M1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M2	Check programmes are been attributed

select distinct 'M2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M3	Check for attributted programmes we have barb start minute and barb end minute

select distinct 'M3' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M4-1	Check for cases with barb end but no start minute

select distinct 'M4-1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M4-2	Check for cases with barb end but no start minute

select distinct 'M4-2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M5	Check slots/programmes are not clamining the same minute

select distinct 'M5' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M6	Check slots/programmes are not clamining the same minute

select distinct 'M6' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M7-1	Check proportion of records been attributed

select distinct 'M7-1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--M7-2	Check number of records been attributed

select distinct 'M7-1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--S1-1	Proportion of events with weight assigned

select	'S1-1',
viewed_start_date_key/100 VIEWING_DAY,
cast(sum(case when scaling_factor > 0 then 1 else 0 end) as float)/cast(count(1) as float) as prop_rec_scaled
from DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100

UNION ALL

--S1-2	Number of events with weight assigned

select distinct 'S1-2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--S3	Check all records matchign scaling source have been attributed

select distinct 'S3' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--S4	Check we have a weight asigned where the flag = 1

select distinct 'S4' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--S5	Check the flag = 1 when weight is asigned

select distinct 'S5' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--S6	Check what is the average dispersion of DP accounts across Scaling Variables

select distinct 'S6' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--S7	Check Convergence

select distinct 'S7' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--CH1-1	Proportion of channels matching with the dimension

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'CH1-1' as index_,
viewed_start_date_key/100 VIEWING_DAY 
from DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'CH1-1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
count(distinct channel_key) cnt
from DATA_QUALITY_SLOT_PROD a
where not exists
(select 1 from sk_prod.viq_channel b
where a.channel_key = b.pk_channel_dim)
and a.channel_key > 0
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

UNION ALL

--CH1-2	Checking all channel names are in place (null or -1 count)

select distinct 'CH1-2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--CH2	Checking all channel names are in place (null or -1 count)

select	'CH2' as index_,
99990909 viewing_day,
COUNT(1) FROM SK_PROD.VIQ_CHANNEL
WHERE CHANNEL_NAME IS NULL

UNION ALL

--CH3	Checking all Channel Genre are in place (null or -1 count)

select	'CH3' as index_,
99990909 viewing_day,
COUNT(1) FROM SK_PROD.VIQ_CHANNEL
WHERE CHANNEL_GENRE IS NULL

UNION ALL

--CH4	Checking all service keys are in place (null or -1 count)

select	'CH4' as index_,
99990909 viewing_day,
COUNT(1) FROM SK_PROD.VIQ_CHANNEL
WHERE SERVICE_KEY IS NULL

union all

--D1	Check how many records have we got per day

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D1' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
COUNT(1) cnt FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D2_1' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D2_1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (PREV_PROG_SCHEDULE_KEY IS NULL OR PREV_PROG_SCHEDULE_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D2_2' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D2_2' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (NEXT_PROG_SCHEDULE_KEY IS NULL OR NEXT_PROG_SCHEDULE_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D3	Check proportion of records without a DK_PROGRAMME_DIM

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D3_1' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D3_1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (PREVIOUS_PROGRAMME_KEY IS NULL OR PREVIOUS_PROGRAMME_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D3_2	Check proportion of records without a DK_PROGRAMME_DIM

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D3_2' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D3_2' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (NEXT_PROGRAMME_KEY IS NULL OR NEXT_PROGRAMME_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D4	Check proportion of records without a DK_CHANNEL_DIM

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D4' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D4' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (CHANNEL_KEY IS NULL OR CHANNEL_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_PROD
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D5	Check number of accounts per day

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D5' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(SELECT 'D5' as index_,
viewed_start_date_key/100 VIEWING_DAY,
COUNT(DISTINCT HOUSEHOLD_KEY) cnt FROM  DATA_QUALITY_SLOT_PROD
WHERE HOUSEHOLD_KEY > 0
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

UNION ALL

--D6	Check number of boxes per day

select distinct 'D6' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI1	Check all clearcast numbers have a reference to dis_manage..smi_parameters

select distinct 'DI1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI2	Check we are retaining only records with max parameter_integer for all matching clearcast_commercial_number

select distinct 'DI2' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI3	Check for outstanding clearcast_commercial_number having >1 record in the output table that is because they have different buyer/advertiser details

select distinct 'DI3' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI4	On dis_reference..FINAL_SLOT_COPY the buyer/advertiser name/code are the latest ones based on expiration date

select distinct 'DI4' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI5	Display number/proportion of clearcast_commercial_number matching on both dis_reference..FINAL_SLOT_COPY and dis_manage..DIS_PARAMETERS

select distinct 'DI5' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI6	Check no duplicate PKs in the FINAL_SLOT_COPY table

select distinct 'DI6' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI7	Display integrity checks (volume/proportion for null counts and "unknown") for relevant fields in Key table (dis_reference..FINAL_SLOT_COPY)

select distinct 'DI7' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI8	Display integrity checks (volume/proportion for null counts and "unknown") for Dimension tables (smi_dw..AGENCY_DIM, smi_dw..SLOT_REFERENCE_DIM, dis_reference..SLOT_TIMETABLE_HIST)

select distinct 'DI8' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL


--DI9	Display volume/proportion of preceding/succeeding programmes ids set to null or -1 @ dis_prepare..SLOT_TIMETABLE

select distinct 'DI9' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI10	Check no broadcast date/hour overlap across slots/breaks @ dis_prepare..SLOT_TIMETABLE

select distinct 'DI10' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI11	Check no SLOT_INSTANCE_POSITION overlaps across slots @ dis_prepare..SLOT_TIMETABLE

select distinct 'DI11' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI12	Check total number of slots is in line with break's capacity @ dis_prepare..SLOT_TIMETABLE

select distinct 'DI12' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI13	Display integrity checks (volume/proportion for null counts and "unknown") for relevant fields in dis_prepare..SLOT_TIMETABLE

select distinct 'DI13' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD

UNION ALL

--DI14	Display integrity checks (volume/proportion for null counts and "unknown") for relevant fields in dis_prepare..SLOT_TIMETABLE

select distinct 'DI14' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD


UNION ALL

--DI15	Sample and reconstruct the initial selection criteria to verify records have been updated with latest info provided based on the hierarchy

select distinct 'DI15' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD


UNION ALL

--B1	PKs duplicated on the Fact tables

select distinct 'B1' as theindex, viewed_start_date_key/100 VIEWING_DAY,-99999 cnt
FROM DATA_QUALITY_SLOT_PROD)

----------------------OLIVE EQUIVALENT-------------------------------------------------------

--------------------
/* scaling check a bit more convoluted so has to be done separately and results inserted into temp table*/
--------------------

--S2	Check no difference between weights in fact and source

select a.*, b.calculated_scaling_weight into #tmp_viewing_chk
from
(select a.slot_data_key, a.time_shift_key,cast(viewed_start_date_key/100 as varchar(12)) viewed_event_date , household_key, scaling_factor  from DATA_QUALITY_SLOT_PROD a
where household_key > 0) a,
(select household_key, replace(cast(adjusted_event_start_date_vespa as varchar(12)),'-','') viewed_event_date,
 calculated_scaling_weight from sk_prod.viq_viewing_data_scaling
where household_key > 0) b
where a.household_key = b.household_key
and a.viewed_event_date = b.viewed_event_date
and a.scaling_factor != b.calculated_scaling_weight

select a.*, B.CALCULATED_SCALING_WEIGHT into #tmp_viewing_chk_2
from
sk_prod.slot_data a,
#tmp_viewing_chk b
where A.SLOT_DATA_KEY = B.SLOT_DATA_KEY
and a.time_shift_key = 0

INSERT INTO #TMP_OLIVE_SLOTS_RESULTS
select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'S2' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_PROD) a
left outer join
(select	'S2' as index_,
viewed_start_date_key/100 viewing_day,
count(1) cnt from #tmp_viewing_chk_2
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)  

SELECT * FROM #TMP_OLIVE_SLOTS_RESULTS
ORDER BY 2, 1