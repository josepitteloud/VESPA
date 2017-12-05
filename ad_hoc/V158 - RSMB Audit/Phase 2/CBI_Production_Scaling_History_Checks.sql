/*
Checking scaling weights history in CBI Production

*/

-----------------------------------
-- LIVE CBI server (10.105.15.11)
-----------------------------------

-- Take a look
SELECT *
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
--where scaling_segment_key = -1476321004239191264
LIMIT 100;
 
-- History from/to	 
select min(event_start_date),max(event_start_date)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
-- 2013-07-15 00:00:00	2014-01-08 00:00:00

-- How many scaling segment IDs?
select count(distinct scaling_segment_key)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
-- 2989240

-- Distinct IDs from a scaling segment
select count(distinct scaling_segment_key)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
where hh_composition = 'Single female'
and tv_region = 'Southern'
and dtv_package = 'Basic Entertainment'
and box_type = 'HDx & No_secondary_box'
and tenure = '2-10 Years'
and scaling_universe_key = 'Single Box Household Universe'
-- 178

-- Distinct dates for the same scaling segment
select count(distinct event_start_date)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
where hh_composition = 'Single female'
and tv_region = 'Southern'
and dtv_package = 'Basic Entertainment'
and box_type = 'HDx & No_secondary_box'
and tenure = '2-10 Years'
and scaling_universe_key = 'Single Box Household Universe'
-- 178

---------------------------------------
-- Historical CBI server (10.105.15.3)
---------------------------------------

select * FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY limit 100

-- History from/to	 
select min(event_start_date),max(event_start_date)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
-- 2013-05-01 00:00:00	2013-05-31 00:00:00

-- Distinct IDs from a scaling segment
select count(distinct scaling_segment_key)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
where hh_composition = 'Single female'
and tv_region = 'Southern'
and dtv_package = 'Basic Entertainment'
and box_type = 'HDx & No_secondary_box'
and tenure = '2-10 Years'
and scaling_universe_key = 'Single Box Household Universe'
-- 31

select * FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY_APR2013 limit 100

-- History from/to	 
select min(event_start_date),max(event_start_date)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY_APR2013
-- 2013-04-01 00:00:00	2013-04-30 00:00:00

-- Distinct IDs from a scaling segment
select count(distinct scaling_segment_key)
FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY_APR2013
where hh_composition = 'Single female'
and tv_region = 'Southern'
and dtv_package = 'Basic Entertainment'
and box_type = 'HDx & No_secondary_box'
and tenure = '2-10 Years'
and scaling_universe_key = 'Single Box Household Universe'
-- 178
