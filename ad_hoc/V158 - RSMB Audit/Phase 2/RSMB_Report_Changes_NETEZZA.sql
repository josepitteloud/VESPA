/*
RSMB Report changes - Netezza side

Lead: Claudio Lima
Date: 06/01/2014
*/

---------------------------
-- Daily Response Rates [1]
---------------------------

select count(*) from TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED -- 536439

alter table TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED rename column_1 to SKY_DATE;
alter table TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED rename column_2 to CONTROL_CELL;
alter table TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED rename column_3 to NACCOUNTS;
alter table TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED rename column_4 to SKYBASE_TARGET;

select * from TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED limit 100

-- QA
select sky_date
		,count(*)
		,sum(naccounts)
		,sum(skybase_target)
		,min(naccounts)
		,avg(naccounts)
		,max(naccounts)
FROM TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED
group by sky_date
order by sky_date

-- All segment IDs have a control cell?
select count(*) 
FROM TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED a 
inner join TEMP..RSMB2_CONTROL_CELL_LOOKUP b
on a.control_cell = b.control_cell
-- 536439

-- Drop old table
drop table TEMP..RSMB2_DAILY_RESPONSE_RATES
-- Build new one
alter table TEMP..RSMB2_DAILY_RESPONSE_RATES_UPDATED rename to TEMP..RSMB2_DAILY_RESPONSE_RATES

select * from TEMP..RSMB2_DAILY_RESPONSE_RATES limit 100

------------------------------
-- Tenure of Drop-off Home [2]
------------------------------

select count(*) from RSMB2_TENURE_DROPOFF_HOMES_UPDATED -- 54868
select * from RSMB2_TENURE_DROPOFF_HOMES_UPDATED limit 100

alter table TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED rename column_1 to WEEK_OF_YEAR;
alter table TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED rename column_2 to CONTROL_CELL;
alter table TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED rename column_3 to DROPOFF_REASON;
alter table TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED rename column_4 to VESPA_RECENCY;
alter table TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED rename column_5 to NACCOUNTS;

 select week_of_year
 		,count(*)
		,sum(naccounts)
  FROM TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED
  group by week_of_year
  order by week_of_year
  
   select dropoff_reason
		,sum(naccounts)
  FROM TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED
  group by dropoff_reason
  order by dropoff_reason
  
  -- drop old table
  drop table TEMP..RSMB2_TENURE_DROPOFF_HOMES
  -- build new one
  alter table TEMP..RSMB2_TENURE_DROPOFF_HOMES_UPDATED rename to TEMP..RSMB2_TENURE_DROPOFF_HOMES
  
  select * from TEMP..RSMB2_TENURE_DROPOFF_HOMES limit 100
  
-------------------------------
-- Monthly Panel Continuity [3]
-------------------------------

-- Load tenure from 28 Feb (in weeks)
select * from TEMP..RSMB2_TENURE_FROM_28FEB_LOOKUP limit 100
select count(*) from TEMP..RSMB2_TENURE_FROM_28FEB_LOOKUP -- 460698

----------------------------
-- Sample 1: with RQ >= 0.9
----------------------------

-- Rename old table
alter table TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 
rename to TEMP.DONNARUA.OLD_RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1

-- Build new one
select	control_cell
		,frequency
		,count(distinct account_number) as hh_volume
into TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1
from	(		
			select	scaling.account_number -- UNIQUE AT ACCOUNT LEVEL...
					,lookup.CONTROL_CELL
					,round((count(distinct scaling.event_Start_date)/ 28.00),2)	as rq_february
					,count(distinct scaling.event_Start_date) rq
					,min(tenure.tenure_weeks) as frequency
			from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as scaling
					left join TEMP..RSMB2_CONTROL_CELL_LOOKUP		as lookup
					on	scaling.HH_COMPOSITION 			= lookup.HH_COMPOSITION
					and	scaling.TV_REGION 				= lookup.TV_REGION
					and	scaling.DTV_PACKAGE 			= lookup.DTV_PACKAGE
					and	scaling.BOX_TYPE 				= lookup.BOX_TYPE
					and	scaling.TENURE 					= lookup.TENURE
					and	scaling.SCALING_UNIVERSE_KEY	= lookup.SCALING_UNIVERSE_KEY
					left join TEMP..RSMB2_TENURE_FROM_28FEB_LOOKUP  as tenure
					on	scaling.account_number = tenure.ACCOUNT_NUMBER
			where	scaling.event_Start_date between '2013-02-01 00:00:00' and '2013-02-28 00:00:00'
			group	by	scaling.account_number
						,lookup.CONTROL_CELL
		)	as thebase
where	frequency is not null
and rq_february >= 0.9
group	by	control_cell
			,frequency
-- 39999

-- Compare previous/new results
select  'OLD'
		,count(*)
		,sum(hh_volume)
		,count(distinct control_cell)
		,min(frequency)
		,max(frequency)
from TEMP.DONNARUA.OLD_RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1
union
select  'NEW'
		,count(*)
		,sum(hh_volume)
		,count(distinct control_cell)
		,min(frequency)
		,max(frequency)
from TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1
-- NEW	39999	302244	14144	1	45
-- OLD	53664	291527	13985	5	70
-- Looks good!

select frequency,sum(hh_volume) 
from TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 
group by frequency
order by 1 desc

select * from TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 where control_cell is null -- 0

drop table TEMP.DONNARUA.OLD_RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1

--------------------------
-- Sample 2: all accounts
--------------------------

-- Rename old table
alter table TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2
rename to TEMP.DONNARUA.OLD_RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2

-- Build new one
select	control_cell
		,frequency
		,count(distinct account_number) as hh_volume
into 	TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2
from	(		
			select	scaling.account_number -- UNIQUE AT ACCOUNT LEVEL...
					,lookup.CONTROL_CELL
					,round((count(distinct scaling.event_Start_date)/ 28.00),2)	as rq_february
					,count(distinct scaling.event_Start_date) rq
					,min(tenure.tenure_weeks) as frequency
			from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as scaling
					left join TEMP..RSMB2_CONTROL_CELL_LOOKUP		as lookup
					on	scaling.HH_COMPOSITION 			= lookup.HH_COMPOSITION
					and	scaling.TV_REGION 				= lookup.TV_REGION
					and	scaling.DTV_PACKAGE 			= lookup.DTV_PACKAGE
					and	scaling.BOX_TYPE 				= lookup.BOX_TYPE
					and	scaling.TENURE 					= lookup.TENURE
					and	scaling.SCALING_UNIVERSE_KEY	= lookup.SCALING_UNIVERSE_KEY
					left join TEMP..RSMB2_TENURE_FROM_28FEB_LOOKUP  as tenure
					on	scaling.account_number = tenure.ACCOUNT_NUMBER
			where	scaling.event_Start_date ='2013-02-28 00:00:00'
			group	by	scaling.account_number
						,lookup.CONTROL_CELL
		)	as thebase
where	frequency is not null
group	by	control_cell
			,frequency
  
-- Compare previous/new results
select  'OLD'
		,count(*)
		,sum(hh_volume)
		,count(distinct control_cell)
		,min(frequency)
		,max(frequency)
from TEMP.DONNARUA.OLD_RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2
union
select  'NEW'
		,count(*)
		,sum(hh_volume)
		,count(distinct control_cell)
		,min(frequency)
		,max(frequency)
from TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2
-- NEW	51563	395260	17499	1	45
-- OLD	68358	381717	17313	1	70
-- Looks good!

select * from TEMP.DONNARUA.RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 where control_cell is null -- 0

drop table TEMP.DONNARUA.OLD_RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2