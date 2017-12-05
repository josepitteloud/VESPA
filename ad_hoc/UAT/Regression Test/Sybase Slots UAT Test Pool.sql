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


*/

*/

--------------------
/* CREATE staging table to capture slots data */
--------------------


if object_id('DATA_QUALITY_SLOT_UAT') is not null then drop table DATA_QUALITY_SLOT_UAT end if;

create table DATA_QUALITY_SLOT_UAT
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


grant select on DATA_QUALITY_SLOT_UAT to sk_prodreg;

commit;
go

------------------------------------------------------------------------------------------------------------------------------------------------

--------------------
/* Set dates you want to run this for in format yyyymmdd */
--------------------


declare @viewing_day_min int
declare @viewing_day_max int

set @viewing_day_min = 20130521
set @viewing_day_max = 20130522

--------------------
/* truncate staging table */
--------------------


truncate table DATA_QUALITY_SLOT_UAT


--------------------
/* insert relevant data into staging table from slot_data, slot_instance and left joining to get key info*/
--------------------


insert into DATA_QUALITY_SLOT_UAT
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
FROM sk_uat.SLOT_DATA slot
inner join
sk_uat.viq_date viq_date
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
from sk_uat.slot_instance slot_inst
left outer join
sk_uat.viq_programme_schedule viq_prev_prog_sched
on
slot_inst.prev_prog_schedule_key = viq_prev_prog_sched.programme_instance_id
left outer join
sk_uat.viq_programme_schedule viq_next_prog_sched
on
slot_inst.next_prog_schedule_key = viq_next_prog_sched.programme_instance_id
left outer join
sk_uat.viq_date prev_prog_start_date
on
viq_prev_prog_sched.dk_start_datehour = prev_prog_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date prev_prog_end_date
on
viq_prev_prog_sched.dk_end_datehour = prev_prog_end_date.pk_datehour_dim
left outer join
sk_uat.viq_date next_prog_start_date
on
viq_next_prog_sched.dk_start_datehour = next_prog_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date next_prog_end_date
on
viq_next_prog_sched.dk_end_datehour = next_prog_end_date.pk_datehour_dim
left outer join
sk_uat.viq_time prev_prog_start_time
on
viq_prev_prog_sched.dk_start_time = prev_prog_start_time.pk_time_dim
left outer join
sk_uat.viq_time prev_prog_end_time
on
viq_prev_prog_sched.dk_end_time = prev_prog_end_time.pk_time_dim
left outer join
sk_uat.viq_time next_prog_start_time
on
viq_next_prog_sched.dk_start_time = next_prog_start_time.pk_time_dim
left outer join
sk_uat.viq_time next_prog_end_time
on
viq_next_prog_sched.dk_end_time = next_prog_end_time.pk_time_dim
left outer join
sk_uat.viq_date slot_start_date
on
slot_inst.slot_start_date_key = slot_start_date.pk_datehour_dim
left outer join
sk_uat.viq_date slot_end_date
on
slot_inst.slot_end_date_key = slot_end_date.pk_datehour_dim
left outer join
sk_uat.viq_time slot_start_time
on
slot_inst.slot_start_time_key = slot_start_time.pk_time_dim
left outer join
sk_uat.viq_time slot_end_time
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

--I1	Check all values for DK_SLOT/PROG_INSTANCE_DIM are found in the Slot/prog Instance Dimension table


(SELECT 'I1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when SLOT_INSTANCE_KEY is null then 1 else 0 end) count FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100)

-- Check all values for DK_SLOT/PROG_DIM are found in the Slot/prog Dimension table
UNION ALL
--olive query

--I2	Check all values for DK_SLOT/PROG_DIM are found in the Slot/prog Dimension table


SELECT 'I2' as index_,
viewed_start_date_key/100 VIEWING_DAY,
COUNT(1) no_slot_prog_dimension
FROM DATA_QUALITY_SLOT_UAT fact
where not exists
(select 1 from sk_uat.slot slot
where fact.slot_key = slot.slot_key)
GROUP BY viewed_start_date_key/100

-- Check all values for DK_PRECEDING_PROGRAMME_INSTANCE_DIM are found in the Programmes Dimension table

UNION ALL

---olive equivalent------

--I3	Check all values for DK_PRECEDING_PROGRAMME_INSTANCE_DIM are found in the Programmes Dimension table

SELECT 'I3' as index_,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when prev_prog_schedule_key is null then 1 else 0 end) cnt FROM DATA_QUALITY_SLOT_UAT fact
GROUP BY viewed_start_date_key/100

--Check all values for DK_PRECEDING_PROGRAMME_DIM are found in the Programme Instance Dimension table
UNION ALL
---olive equivalent

--I4	Check all values for DK_PRECEDING_PROGRAMME_DIM are found in the Programme Instance Dimension table


SELECT 'I4' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when previous_programme_key is null then 1 else 0 end) cnt
FROM DATA_QUALITY_SLOT_UAT fact
GROUP BY viewed_start_date_key/100

UNION ALL

--I5	Check all values for DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM are found in the Programme Instance Dimension table


select	'I5' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_prog_schedule_key is null then 1 else 0 end) cnt
FROM DATA_QUALITY_SLOT_UAT fact
GROUP BY viewed_start_date_key/100

UNION ALL

---olive equivalent------

--I6	Check all values for DK_SUCCEEDING_PROGRAMME_DIM are found in the Programmes Dimension table


SELECT 'I6' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_programme_key is null then 1 else 0 end)cnt
FROM DATA_QUALITY_SLOT_UAT fact
GROUP BY viewed_start_date_key/100

-- Check all values for DK_CHANNEL_DIM are found in the Channels Dimension table
UNION ALL

----olive equivalent--------------------------------

--I7	Check all values for DK_CHANNEL_DIM are found in the Channels Dimension table


select a.*, isnull(b.cnt,0) cnt from 
(select distinct 'I7' as theindex,
viewed_start_date_key/100 viewing_day
from DATA_QUALITY_SLOT_UAT ) a
left outer join 
(select	'I7' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(distinct channel_key) cnt from DATA_QUALITY_SLOT_UAT a
where not exists
(select 1 from sk_uat.viq_channel b
where a.channel_key = b.pk_channel_dim)
and channel_key > 0
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day


UNION ALL

--I8-1	Check after null or -1 for preceding programme dim

select	'I8-1' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when previous_programme_key = -1 then 1 else 0 end) cnt from DATA_QUALITY_SLOT_UAT
where previous_programme_key = -1
GROUP BY viewed_start_date_key/100

UNION ALL

--------------------------------------olive equivalent-------------------------

--I8-2	Check after null or -1 for preceding programme Instance Dim

select	'I8-2' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum (case when prev_prog_schedule_key = -1 then 1 else 0 end )cnt from DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100

-- Check after null or -1 for succeeding programme dim

UNION ALL
--------------------------------------olive equivalent-------------------------

--I8-3	Check after null or -1 for succeeding programme dim


select	'I8-3' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_programme_key = -1 then 1 else 0 end) cnt from DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100

-- Check after null or -1 for succeeding programme Instance Dim

--------------------------------------olive equivalent-------------------------

UNION ALL

--I8-4	Check after null or -1 for succeeding programme Instance Dim



select	'I8-4' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when next_prog_schedule_key = -1 then 1 else 0 end) cnt from DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100

-- Count records with duration set to null or -1

UNION ALL
---olive equivalent-------------------------------

--I9	Count records with duration set to null or -1


select	'I9' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
sum(case when viewed_duration is null or viewed_duration < 0 then 1 else 0 end) cnt
from DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100

UNION ALL
---olive equivalent-------------------------------


--I10	Check if PK of SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC is unique
select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'I10' as theindex,
viewed_start_date_key/100 viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'I10' as theindex,
VIEWING_DAY,
count(1) cnt from
(select viewed_start_date_key/100 viewing_day,slot_data_key from DATA_QUALITY_SLOT_UAT
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
from DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'I11' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_UAT
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
DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'I12' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_UAT
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
DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'I13' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_UAT
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
DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'I14' as theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_UAT
where prev_prog_schedule_key = next_prog_schedule_key
and prev_prog_schedule_key > 0
and next_prog_schedule_key > 0
and prev_broadcast_start_date || ' ' || prev_broadcast_start_time != next_broadcast_start_date || ' ' || next_broadcast_start_time
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)


UNION ALL

--C5-1	Check we don't have capped date and not time


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'C5-1' as theindex,
viewed_start_date_key/100 viewing_day from
DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'C5-1' theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_UAT
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
DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'C5-2' theindex,
viewed_start_date_key/100 VIEWING_DAY,
count(1) cnt from DATA_QUALITY_SLOT_UAT
where viewed_start_time_key > 0
and (viewed_start_date_key is null or viewed_start_date_key = -1)
GROUP BY viewed_start_date_key/100) b
on a.theindex = b.theindex
and a.viewing_day = b.viewing_day)

UNION ALL

--S1-1	Proportion of events with weight assigned

select	'S1-1',
viewed_start_date_key/100 VIEWING_DAY,
cast(sum(case when scaling_factor > 0 then 1 else 0 end) as float)/cast(count(1) as float) as prop_rec_scaled
from DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100

UNION ALL

--CH1-1	Proportion of channels matching with the dimension

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'CH1-1' as index_,
viewed_start_date_key/100 VIEWING_DAY 
from DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'CH1-1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
count(distinct channel_key) cnt
from DATA_QUALITY_SLOT_UAT a
where not exists
(select 1 from sk_uat.viq_channel b
where a.channel_key = b.pk_channel_dim)
and a.channel_key > 0
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)



UNION ALL

--CH2	Checking all channel names are in place (null or -1 count)

select	'CH2' as index_,
99990909 viewing_day,
COUNT(1) FROM sk_uat.VIQ_CHANNEL
WHERE CHANNEL_NAME IS NULL

UNION ALL

--CH3	Checking all Channel Genre are in place (null or -1 count)


select	'CH3' as index_,
99990909 viewing_day,
COUNT(1) FROM sk_uat.VIQ_CHANNEL
WHERE CHANNEL_GENRE IS NULL


UNION ALL

--CH4	Checking all service keys are in place (null or -1 count)


select	'CH4' as index_,
99990909 viewing_day,
COUNT(1) FROM sk_uat.VIQ_CHANNEL
WHERE SERVICE_KEY IS NULL

union all

select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D1' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
COUNT(1) cnt FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D2_1' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D2_1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (PREV_PROG_SCHEDULE_KEY IS NULL OR PREV_PROG_SCHEDULE_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D2	Check proportion of records without a DK_PROGRAMME_INSTANCE_DIM


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D2_2' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D2_2' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (NEXT_PROG_SCHEDULE_KEY IS NULL OR NEXT_PROG_SCHEDULE_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)


union all

--D3	Check proportion of records without a DK_PROGRAMME_DIM


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D3_1' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D3_1' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (PREVIOUS_PROGRAMME_KEY IS NULL OR PREVIOUS_PROGRAMME_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D3	Check proportion of records without a DK_PROGRAMME_DIM


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D3_2' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D3_2' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (NEXT_PROGRAMME_KEY IS NULL OR NEXT_PROGRAMME_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)


union all

--D4	Check proportion of records without a DK_CHANNEL_DIM


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D4' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D4' as index_,
viewed_start_date_key/100 VIEWING_DAY,
(1.0 * SUM(CASE WHEN (CHANNEL_KEY IS NULL OR CHANNEL_KEY = -1) THEN 1 ELSE 0 END)/count(1)) cnt
FROM DATA_QUALITY_SLOT_UAT
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

union all

--D5	Check number of accounts per day


select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'D5' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(SELECT 'D5' as index_,
viewed_start_date_key/100 VIEWING_DAY,
COUNT(DISTINCT HOUSEHOLD_KEY) cnt FROM  DATA_QUALITY_SLOT_UAT
WHERE HOUSEHOLD_KEY > 0
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)

----------------------OLIVE EQUIVALENT-------------------------------------------------------

--------------------
/* scaling check a bit more convoluted so has to be done separately and results inserted into temp table*/
--------------------

--S2	Check no difference between weights in fact and source

select a.*, b.calculated_scaling_weight into #tmp_viewing_chk
from
(select a.slot_data_key, a.time_shift_key,cast(viewed_start_date_key/100 as varchar(12)) viewed_event_date , household_key, scaling_factor  from DATA_QUALITY_SLOT_UAT a
where household_key > 0) a,
(select household_key, replace(cast(adjusted_event_start_date_vespa as varchar(12)),'-','') viewed_event_date,
 calculated_scaling_weight from sk_uat.viq_viewing_data_scaling
where household_key > 0) b
where a.household_key = b.household_key
and a.viewed_event_date = b.viewed_event_date
and a.scaling_factor != b.calculated_scaling_weight

select a.*, B.CALCULATED_SCALING_WEIGHT into #tmp_viewing_chk_2
from
sk_uat.slot_data a,
#tmp_viewing_chk b
where A.SLOT_DATA_KEY = B.SLOT_DATA_KEY
and a.time_shift_key = 0

INSERT INTO #TMP_OLIVE_SLOTS_RESULTS
select a.*, isnull(b.cnt, 0) cnt from
((select distinct 'S2' as index_,
viewed_start_date_key/100  viewing_day
from DATA_QUALITY_SLOT_UAT) a
left outer join
(select	'S2' as index_,
viewed_start_date_key/100 viewing_day,
count(1) cnt from #tmp_viewing_chk_2
GROUP BY viewed_start_date_key/100) b
on a.index_ = b.index_
and a.viewing_day = b.viewing_day)  

SELECT * FROM #TMP_OLIVE_SLOTS_RESULTS
ORDER BY 2, 1