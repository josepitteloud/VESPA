/*
RSMB Reports QA

Claudio Lima
13/12/2013
*/

/*
Control cells
*/

select *
from TEMP.CLL08.RSMB_CONTROL_CELL_LOOKUP_R2
 LIMIT 100;

select count(*) 
from TEMP.CLL08.RSMB_CONTROL_CELL_LOOKUP_R2 -- 24680

/* New more compelte list of control cells */

select *
from TEMP..RSMB2_CONTROL_CELL_LOOKUP
 LIMIT 100;
 
select count(*) from TEMP..RSMB2_CONTROL_CELL_LOOKUP; -- 25105

/********************
Daily Response Rates
*********************/

SELECT *
  FROM TEMP..RSMB2_DAILY_RESPONSE_RATES
 LIMIT 100;
 
SELECT count(*) FROM TEMP..RSMB2_DAILY_RESPONSE_RATES -- 550,147
 
select sky_date
		,count(*)
		,sum(naccounts)
		,sum(skybase_target)
		,min(naccounts)
		,avg(naccounts)
		,max(naccounts)
FROM TEMP..RSMB2_DAILY_RESPONSE_RATES
group by sky_date
order by sky_date

select sky_date,count(*)
FROM TEMP..RSMB2_DAILY_RESPONSE_RATES
where naccounts=0
group by sky_date
order by sky_date
-- we should remove entries with 0 accounts returning

delete from temp.cll08.RSMB_DAILY_RESPONSE_RATES_R2 where naccounts=0
-- ERROR:  Cross Database Access not supported for this type of command

-- All segment IDs have a control cell?
select count(*) 
FROM TEMP.CLL08.RSMB_DAILY_RESPONSE_RATES_R2 a 
inner join TEMP.CLL08.RSMB_CONTROL_CELL_LOOKUP_R2 b
on a.control_cell = b.control_cell
--550,147

/***********************
Tenure of dropoff homes
************************/

SELECT count(*) FROM TEMP.CLL08.RSMB_TENURE_DROPOFF_HOMES_R2 -- 1973
SELECT count(*) FROM TEMP.CLL08.RSMB_TENURE_DROPOFF_HOMES_R2_V2 -- 25,049

SELECT *
  FROM TEMP.CLL08.RSMB_TENURE_DROPOFF_HOMES_R2_V2
--where week_of_year = 44
 LIMIT 100;
 
 select week_of_year
 		,count(*)
		,sum(naccounts)
  FROM TEMP.CLL08.RSMB_TENURE_DROPOFF_HOMES_R2_V2
  group by week_of_year
  order by week_of_year
  
   select dropoff_reason
		,sum(naccounts)
  FROM TEMP.CLL08.RSMB_TENURE_DROPOFF_HOMES_R2_V2
  group by dropoff_reason
  order by dropoff_reason
  
  select distinct dropoff_reason
  FROM TEMP.CLL08.RSMB_TENURE_DROPOFF_HOMES_R2
  
 /************************
 Monthly panel continuity
 *************************/
 
select * from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 limit 100
select * from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2 limit 100

select count(*),sum(hh_volume) from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 -- 53664, 291527
select count(*),sum(hh_volume) from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2 -- 68358, 381717 (77052, 445389)

-- tenure of accounts in panel on Feb 2013
select frequency/4,sum(hh_volume)
from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2
group by frequency/4
order by frequency/4

-- all entries in sample 1 have a scaling segment?
select count(*)
from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE1 a
inner join TEMP..RSMB2_CONTROL_CELL_LOOKUP b
on a.control_cell = b.control_cell
-- 53664 - Yes

-- all entries in sample 2 have a scaling segment?
select count(*)
from TEMP..RSMB2_MONTHLY_PANEL_CONTINUITY_SAMPLE2 a
inner join TEMP..RSMB2_CONTROL_CELL_LOOKUP b
on a.control_cell = b.control_cell
-- 68358 - Yes

-- number of accounts in scaling that we cannot find their scaling segment
			select  count(distinct scaling.account_number)
			from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as scaling
					left join TEMP..RSMB2_CONTROL_CELL_LOOKUP		as lookup
					on	scaling.HH_COMPOSITION 			= lookup.HH_COMPOSITION
					and	scaling.TV_REGION 				= lookup.TV_REGION
					and	scaling.DTV_PACKAGE 			= lookup.DTV_PACKAGE
					and	scaling.BOX_TYPE 				= lookup.BOX_TYPE
					and	scaling.TENURE 					= lookup.TENURE
					and	scaling.SCALING_UNIVERSE_KEY	= lookup.SCALING_UNIVERSE_KEY
			where	scaling.event_Start_date between '2013-02-01 00:00:00' and '2013-02-28 00:00:00'
			and lookup.CONTROL_CELL is null
			-- 0
			
-- number of accounts without tenure information
			select	count(distinct scaling.account_number)
			from	DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as scaling
					left join temp..RSMB2_TENURE_LOOKUP			as tenure
					on	scaling.account_number = tenure.ACCOUNT_NUMBER
			where	scaling.event_Start_date between '2013-02-01 00:00:00' and '2013-02-28 00:00:00'
			and tenure.dp_frequency is null
			-- 15921
			-- this seems to be accounts that get a weight in Netezza (empty logs) but do not get to Olive.
			
/**********************
Monthly Response Rates		
***********************/

SELECT *
  FROM TEMP..RSMB2_MONTHLY_RESPONSE_RATES
 LIMIT 100;
 
 select themonth
 		,count(*)
		,sum(response_rates)
		,sum(sky_target)
		,min(response_rates)
		,avg(response_rates)
		,max(response_rates)
 FROM TEMP..RSMB2_MONTHLY_RESPONSE_RATES
 group by themonth
 order by themonth
 
 -- all entries have a scaling segment?
select count(*)
from TEMP..RSMB2_MONTHLY_RESPONSE_RATES a
inner join TEMP..RSMB2_CONTROL_CELL_LOOKUP b
on a.control_cell = b.control_cell
-- 82518 - Yes
 
 select count(*)
 from (
 select	A.month				as themonth
		,B.control_cell		
		,A.hits				as response_rates
from	TEMP..z_3_beta								as A -- ref: netezza extraction for : z_3_beta
		left join temp..rsmb2_control_cell_lookup	as B
		on	A.HH_COMPOSITION	= B.HH_COMPOSITION
		and	A.TV_REGION			= B.TV_REGION
		and	A.DTV_PACKAGE		= B.DTV_PACKAGE
		and	A.BOX_TYPE			= B.BOX_TYPE
		and	A.TENURE			= B.TENURE
		and	A.UNIVERSE			= B.SCALING_UNIVERSE_KEY
order	by	A.month
			,B.control_cell
) t -- 68095

/*********************
Panel Composition
**********************/

select * from TEMP..RSMB2_PANEL_COMPOSITION_THURSDAYS LIMIT 100

select thursday
		,count(*)
		,sum(sky_base)
		,sum(vespa_panel)
		,sum(responsive)
from TEMP..RSMB2_PANEL_COMPOSITION_THURSDAYS
group by thursday
order by thursday

select count(*) from TEMP..RSMB2_PANEL_COMPOSITION_THURSDAYS where vespa_panel < responsive

 -- all entries have a scaling segment?
select count(*)
from TEMP..RSMB2_PANEL_COMPOSITION_THURSDAYS a
left join TEMP..RSMB2_CONTROL_CELL_LOOKUP b
on a.control_cell = b.control_cell
-- 500915 - Yes

select * from TEMP..RSMB2_PANEL_COMPOSITION_SATURDAY LIMIT 100

select saturday
		,count(*)
		,sum(sky_base)
		,sum(vespa_panel)
		,sum(responsive)
from TEMP..RSMB2_PANEL_COMPOSITION_SATURDAYS
group by saturday
order by saturday

select count(*) from TEMP..RSMB2_PANEL_COMPOSITION_Saturdays where vespa_panel < responsive

 -- all entries have a scaling segment?
select count(*)
from TEMP..RSMB2_PANEL_COMPOSITION_SATURDAYS a
left join TEMP..RSMB2_CONTROL_CELL_LOOKUP b
on a.control_cell = b.control_cell
-- 500915 - Yes

select count(distinct control_cell) from TEMP..RSMB2_PANEL_COMPOSITION_SATURDAYS -- 41045
select count(distinct control_cell) from TEMP..RSMB2_CONTROL_CELL_LOOKUP -- 44226

select count(*)
from TEMP..RSMB2_PANEL_COMPOSITION_THURSDAYS a
inner join TEMP..RSMB2_PANEL_COMPOSITION_SATURDAYS b
on a.control_cell = b.control_cell
and a.thursday+2 = b.saturday
-- 41045


