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
**Project Name:                     	Vespa Executive Dashboard
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          RSMB
**Due Date:                             31/08/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                        
                                                                        
**Business Brief:

Supporting RSMB Audit process by providing requested figures...

**Sections:

ID	Area					Table
0	Scaling					Region Consolidation
1	Panel Balance			Panel Composition
2	Panel Balance			Recruitment and Drop-off Rates
3	Panel Balance			Removal Reason Rates
4	Panel Balance			Migration Rates
5	Response Rates			Daily Response Rates
6	Response Rates			Monthly Response Rates
7	Panel Continuity		Monthly Panel Continuity
8	Panel Continuity		Tenure of Drop-off Home
9	Reporting Continuity	Overall Continuity
10	Reporting Continuity	All Year Continuity
11	Reporting Continuity	Tracking Continuity
12	Low Response Viewing	Low Response Viewing
13	Low Response Analysis	Low Response Analysis

--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------------
-- [0] OLIVE-NETEZZA REGION CONSOLIDATION 
-----------------------------------------

/*
The reason for this section is because at CBI the values on Region aren't matching
the ones we have at Olive... hence this is affecting the segment population, and segment id assigment...

The idea here is to tally up values under Region, compact the segments and re-distribute the segment ids
in order to be able to match with what CBI has...
*/

-- Copying Vespa segments lookup table...
select  *	
into    rsmb_consolidation
from    vespa_analysts.SC2_Segments_Lookup_v2_1

commit

-- Renaming Regions to what CBI has...
update  rsmb_consolidation
set     isba_tv_region =    case    when isba_tv_region in ('HTV Wales','HTV West') then 'Wales and West'
                                    when isba_tv_region = 'Meridian (exc. Chann' then 'Southern'
                                    when isba_tv_region = 'North West' then 'Lancashire'
                                    when isba_tv_region = 'Ulster' then 'Northern Ireland'
                                    else isba_tv_region
                            end

commit

/*

 QA Section 
 
select  isba_tv_region, min(scaling_segment_id) as thenewsegment
from    rsmb_consolidation
where   isba_tv_region in ('Wales and West', 'Southern','Lancashire','Northern Ireland')
group   by  isba_tv_region


select top 10 * from rsmb_consolidation

select '1',count(1) from rsmb_consolidation
union all
select '2',count(1) from (
select  universe, hhcomposition,tenure,package,boxtype,isba_tv_region, count(1) as hits, min(scaling_segment_id) as thenewsegment
from rsmb_consolidation group by universe, hhcomposition,tenure,package,boxtype,isba_tv_region 
) as base

*/

-- Re-distributing segment Ids...
select  universe
		,hhcomposition
		,tenure
		,package
		,boxtype
		,isba_tv_region
		,count(1) as hits
		,min(scaling_segment_id) as segment
		,max(scaling_segment_id) as ref
into    rsmb_segment_lookup -- drop table rsmb_segment_lookup
from    rsmb_consolidation 
group 	by	universe
			,hhcomposition
			,tenure
			,package
			,boxtype
			,isba_tv_region 

create hg index rsmbhg3 on rsmb_segment_lookup(segment);
commit;


-- QA
--select top 10 * from rsmb_segment_lookup



------------------------
-- [1] PANEL COMPOSITION  
------------------------


-- Extracting the period required only...

select  weights.scaling_day
        ,lookup.segment
        ,weights.vespa_accounts
        ,weights.sky_base_accounts 
        ,weights.scaling_segment_id as old_
into    weights1
from    vespa_analysts.SC2_Weightings   as weights
        left join   rsmb_segment_lookup as lookup
        on  weights.scaling_segment_id = lookup.segment
where   scaling_day between '2013-01-07' and '2013-03-24' -- 3364963 row(s) affected

commit

update  weights1    as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 216279 row(s) affected

commit

-- get this same view for Whole Panel, Single Box Households and Multi Box Households (RESULT)
select  calendar.theweek                                    as week
        ,weights.segment                                    as segment
        ,lookup.universe                                    as universe
        ,lookup.isba_tv_region                              as region
        ,lookup.tenure                                      as tenure
        ,lookup.package                                     as pack
        ,lookup.boxtype                                     as box
        ,lookup.hhcomposition                               as hh_comp
        ,cast(avg(weights.sky_base_accounts) as integer)    as skybase
        ,cast(avg(weights.vespa_accounts) as integer)       as dp
from    weights1 as weights                                             -- level: date + segment / control Cell
        inner join  rsmb_segment_lookup as lookup   -- level: segment / control Cell
        on  weights.segment = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                        where   utc_day_date between '2013-01-01' and '2013-08-31'
                    )   as calendar                                     -- level: date
        on  weights.scaling_day = calendar.thedate
group   by  week
            ,segment
            ,universe
            ,region
            ,tenure
            ,pack
            ,box
            ,hh_comp
            
			
			
------------------------------------
-- [2] RECRUITMENT AND DROPOFF RATES 
------------------------------------


-- Extracting the period requierd...
select  account_number
        ,profiling_date                         as thedate
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
        ,calendar.theweek                       as week
into    snap
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                    )   as calendar
        on  segment_snapshot.profiling_date = calendar.thedate
where   profiling_date between '2013-01-07' and '2013-03-24' -- 103651196 row(s) affected


create hg index snaphg1 on snap(segment)
commit

update  snap    as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 3289939 row(s) updated

commit


-- counting, within a week, how many times an account appear on which segment
/* 
	the rule implied here is that if an account appears >=4 times on a segment
	then we will consider such segment as the correspondent one for that account
	on the week of evaluation
*/
select  week
        ,base.account_number
        ,base.segment
        ,count(1)               as hits
into    rsmb_stage_1
from    snap as base                                                     --level: date / account_number
group   by  week
            ,base.account_number
            ,base.segment -- 103651196 row(s) affected
/*
    Everyone ended up having just one segment at a time on each week... sounds reasonable
    weekly balancing excercise...
*/

commit

create hg index rsmbhg1 on rsmb_stage_1(account_number)
create lf index rsmblf1 on rsmb_stage_1(week)

commit

-- (RESULT)
select  rsmb.week
        ,rsmb.segment
        ,sum(stage_2.subtot_recruited)  as tot_recruited
        ,sum(stage_2.subtot_dropoff)    as tot_dropoff
from    rsmb_stage_1    as rsmb
        inner join  (
        
                        select  account_number
                                ,week
                                ,sum(recruited) as subtot_recruited
                                ,sum(dropoff)   as subtot_dropoff
                        from    (
                                    select  account_number 
                                            ,case   when intprevstatus = 1 then 'Enabled'
                                                    when intprevstatus = 2 then 'Disabled'
                                                    when intprevstatus = 3 then 'Trumped'
                                                    else null 
                                            end     as prevstatus   
                                            ,min(intstatus) over   (
                                                                        partition by    account_number
                                                                        order by        base.thedate
                                                                        rows between    1 preceding and 1 preceding
                                                                    )   as intprevstatus
                                            ,status
                                            ,case   when status = 'Enabled'     then 1
                                                    when status = 'Disabled'    then 2
                                                    else 3 
                                            end     as intstatus
                                            ,min(base.thedate) over (
                                                                        partition by    account_number
                                                                        order by        base.thedate
                                                                        rows between    1 preceding and 1 preceding
                                                                    )   as prevday
                                            ,base.thedate
                                            ,min(base.thedate) over (   
                                                                        partition by    account_number
                                                                        order by        base.thedate
                                                                        rows between    1 following and 1 following
                                                                    )   as nextday
                                            , case when ((prevstatus in('Disabled','Trumped') or prevstatus is null) and status = 'Enabled') then 1 else 0 end  as recruited
                                            , case when (prevstatus = 'Enabled' and status in ('Disabled','Trumped')) then 1 else 0 end                         as dropoff
                                            ,calendar.theweek   as week
                                    from    (   
                                                -- Unifying at Account Level...
                                                select  distinct 
                                                        account_number
                                                        ,cast(coalesce(request_dt,modified_dt)as date)   as thedate
                                                        ,case   when lower(result) like '%enable%'  then 'Enabled'
                                                                when lower(result) like '%disable%' then 'Disabled'
                                                                else result
                                                        end     as status
                                                from    sk_prod.vespa_subscriber_status_hist                                                                    -- level: box / request_dt
                                                --where   account_number in ('200000235644','200000307153','200000846986')
                                            )   as base                                                                                                         -- level: account / date
                                            inner join  (   
                                                            -- Unifying view of vespa calendar into date level to get the week of year value...
                                                            select  distinct
                                                                    utc_day_date        as thedate
                                                                    ,utc_week_in_year   as theweek
                                                            from    sk_prod.VESPA_CALENDAR 
                                                        )   as calendar                                                                                         -- level: date
                                            on  base.thedate = calendar.thedate
                                )   as Stage_1
                        group   by  account_number
                                    ,week
                    )   as Stage_2
        on  rsmb.account_number = stage_2.account_number
        and rsmb.week = stage_2.week
group   by  rsmb.week
            ,rsmb.segment
			

			
---------------------------
-- [3] REMOVAL REASON RATES 
---------------------------

-- Extracting the period requierd...
select  account_number
        ,profiling_date                         as thedate
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
        ,calendar.theweek                       as week
into    snap
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                    )   as calendar
        on  segment_snapshot.profiling_date = calendar.thedate
where   profiling_date between '2013-01-07' and '2013-03-24' -- 103651196 row(s) affected


create hg index snaphg1 on snap(segment)
commit

update  snap    as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 3289939 row(s) updated

commit


-- (RESULT)

select  campaigns.week
        ,snap.segment
        ,campaigns.cell_name
        ,count(distinct campaigns.account_number) as hits
into    rsmb_removalerasons
from    (
            select  cust.account_number
                    ,calendar.theweek   as week
                    ,lookup.cell_name
            from    sk_prod.campaign_history_lookup_cust        as lookup
                    inner join sk_prod.CAMPAIGN_HISTORY_CUST    as cust
                    on  lookup.cell_id = cust.cell_id
                    inner join  (
                                    select  distinct
                                            utc_day_date        as thedate
                                            ,utc_week_in_year   as theweek
                                    from    sk_prod.VESPA_CALENDAR 
                                    where   utc_day_date between '2013-01-07' and '2013-03-24'
                                )   as calendar
                    on  cast(lookup.writeback_datetime as date) = calendar.thedate
            where   upper(lookup.campaign_name) like 'VESPA_DISABLEMENT_WEEKLY_%'
            and     cast(lookup.writeback_datetime as date) between '2013-01-07' and '2013-03-24'
            and     lookup.cell_name not in ('AnytimePlusEnablements & TransfersToPanel12')
        )   as campaigns
        inner join snap
        on  campaigns.account_number = snap.account_number
        and campaigns.week = snap.week
--where   snap.week in (2,3,4,6,9,10,12)
group   by  campaigns.week
            ,snap.segment
            ,campaigns.cell_name

commit	

			
			
----------------------
-- [4] MIGRATION RATES
----------------------

-- Extracting the period requierd...
select  account_number
        ,profiling_date                         as thedate
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
into    snap
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
where   profiling_date between '2013-01-07' and '2013-03-24' -- 103651196 row(s) affected

commit

create hg index snaphg1 on snap(segment)
commit

update  snap    as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 3289939 row(s) updated


-- counting, within a week, how many times an account appear on which segment
/* 
	the rule implied here is that if an account appears >=4 times on a segment
	then we will consider such segment as the correspondent one for that account
	on the week of evaluation
*/
select  calendar.theweek        as week
        ,base.account_number
        ,base.segment
        ,count(1)               as hits
into    rsmb_stage_1
from    snap as base                                                     --level: date / account_number
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                        where   utc_day_date between '2013-01-01' and '2013-08-31'
                    )   as calendar                                     -- level: date
        on  base.thedate = calendar.thedate
group   by  week
            ,base.account_number
            ,base.segment -- 103651196 row(s) affected

commit

select  * 
		,min(segment) over  (   
                                partition by    account_number
                                order by        week
                                rows between    1 preceding and 1 preceding
                            ) as came_from
        ,min(segment) over  (   
                                partition by    account_number
                                order by        week
                                rows between    1 following and 1 following
                            ) as went_to         
into    rsmb_stage_2
from    rsmb_stage_1

create hg index rsmbhg2 on rsmb_stage_2(account_number)

drop table rsmb_stage_1
drop table snap

commit

-- Generating Migration Rates... (RESULT)
select  week
        ,segment
        ,sum(case when (came_from <> segment and came_from is not null) then 1 else 0 end)  as joiners
        ,sum(case when (went_to <> segment and went_to is not null) then 1 else 0 end)      as leavers
from    rsmb_stage_2
group   by  week
            ,segment
order   by  week



---------------------------------
-- [5] DAILY RESPONSE RATES (VIQ)
---------------------------------

/*
	NOTE: the 24th of March has a natural data drop, in which we only have visibility over 62k of accounts interacting
	with the panel on such date...
*/

-- Extracting the period requierd...
select  viq.account_number
        ,calendar.thedate                       as thedate
        ,lookup.segment                         as segment
        ,intervals.scaling_segment_id    as old_
        ,calendar.theweek                       as week
into    snap2
from    vespa_analysts.sc2_intervals                as intervals
        inner join sk_prod.VIQ_VIEWING_DATA_SCALING as viq
        on  intervals.account_number = viq.account_number
        and viq.adjusted_event_start_date_vespa between intervals.reporting_starts and intervals.reporting_ends
        left join rsmb_segment_lookup               as lookup
        on intervals.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                        where   utc_day_date between '2013-01-07' and '2013-01-07'
                    )   as calendar
        on  calendar.thedate between intervals.reporting_starts and intervals.reporting_ends
--where   profiling_date between '2013-01-07' and '2013-03-24' -- 103651196 row(s) affected


create hg index snaphg1 on snap(segment)
commit

update  snap    as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 3289939 row(s) updated

commit


-- getting all accounts scaled on each day
select  calendar.thedate
        ,calendar.theweek
        ,intervals.account_number
into    tempshelf
from    (
            select  distinct
                    utc_day_date        as thedate
                    ,utc_week_in_year   as theweek
            from    sk_prod.VESPA_CALENDAR 
            where   utc_day_date between '2013-01-07' and '2013-03-24'
        )   as calendar            
        inner join  vespa_analysts.SC2_Intervals as intervals
        on  calendar.thedate between intervals.reporting_starts and intervals.reporting_ends

commit


-- Out of the skybase for each day lets sample for those accounts that we are actually interacting with the panel (Vespa DP)
select  *
into    target_snap
from    snap -- 103651196
where   account_number in ( select distinct account_number from tempshelf ) -- 5471310

commit

create hg index targetsnaphg1 on target_snap (account_number)
create lf index targetsnaplf1 on target_snap (week)

commit


-- (RESULT)

select  base.thedate    
        ,snap.segment   
        ,count(distinct base.account_number) as response_rate
from    tempshelf as base
        inner join  target_snap as snap
        on  base.account_number = snap.account_number
        and base.theweek = snap.week
group   by  base.thedate
            ,snap.segment
			

			
-----------------------------
-- [6] MONTHLY RESPONSE RATES
-----------------------------


-- Extracting the period requierd...
select  account_number
        ,profiling_date                         as thedate
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
        ,calendar.themonth                      as month
into    long_snap
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_month_num      as themonth
                        from    sk_prod.VESPA_CALENDAR 
                    )   as calendar
        on  segment_snapshot.profiling_date = calendar.thedate
where   profiling_date between '2012-12-13' and '2013-07-26' -- 329497048 row(s) affected


create hg index long_snapphg1 on long_snap(segment);
commit;

-- QA
-- select count(1) from long_snap where segment is null -- 10463493

update  long_snap   as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 10463493 row(s) updated



-- getting all accounts scaled on each day... and bringing the snapshot up to a month level...

select  calendar.themonth           as month
        ,intervals.account_number
into    rsmb_6_tempshelf_stage1
from    (
            select  distinct
                    utc_day_date        as thedate
                    ,utc_month_num      as themonth
            from    sk_prod.VESPA_CALENDAR 
            where   utc_day_date between '2012-12-13' and '2013-07-26'
        )   as calendar            
        inner join  vespa_analysts.SC2_Intervals as intervals
        on  calendar.thedate between intervals.reporting_starts and intervals.reporting_ends -- 87,257,074 row(s) affected

commit

select  distinct 
        month
        ,account_number 
into    rsmb_6_tempshelf_stage2 
from    rsmb_6_tempshelf_stage1  -- 3984785 row(s) affected


create hg index rsmbstage2hg on rsmb_6_tempshelf_stage2(account_number)

commit



-- (RESULT)

select  long_snap.segment
        ,base.month
        ,count(distinct base.account_number) as response_rate
into    rsmb_monthly_response_rates
from    rsmb_6_tempshelf_stage2     as base
        inner join long_snap        
        on  base.account_number = long_snap.account_number
        and base.month = long_snap.month
group   by  long_snap.segment
            ,base.month
having  count(distinct base.account_number) > 0

commit



--------------------------------
-- [7]	MONTHLY PANEL CONTINUITY
--------------------------------


-- getting all accounts scaled on each day... and bringing the snapshot up to a month level...
select  calendar.theyw             as yearweek
        ,calendar.thedate
        ,intervals.account_number
into    rsmb_7_tempshelf_stage1 -- drop table rsmb_7_tempshelf_stage1
from    (
            select  distinct
                    utc_day_date                                as thedate
                    ,(case when thedate = '2012-12-31' then cast(datepart(year,thedate)as integer)+1 else datepart(year,thedate) end) || (case when utc_week_in_year < 10 then ('0' || cast(utc_week_in_year as varchar(1))) else cast(utc_week_in_year as varchar(2)) end) as theyw
            from    sk_prod.VESPA_CALENDAR 
            where   utc_day_date between '2012-12-13' and '2013-07-26'
        )   as calendar            
        inner join  vespa_analysts.SC2_Intervals as intervals
        on  calendar.thedate between intervals.reporting_starts and intervals.reporting_ends -- 87,257,074 row(s) affected

commit

select  account_number
        ,min(yearweek)                      as start_dt
        ,max(yearweek)                      as end_dt
        ,case   when (cast(left(start_dt,4) as integer) = 2012 and cast(left(end_dt,4) as integer) = 2013)
                        then ((52 - cast(right(start_dt,2) as integer))  + cast(right(end_dt,2) as integer) )
                else (cast(right(end_dt,2) as integer) - cast(right(start_dt,2)as integer)) end + 1 as week_frequency
        ,cast(right(start_dt,2) as integer)  as weekstart
        ,cast(right(end_dt,2) as integer)    as weekend
into    rsmb_7_tempshelf_stage2 --  drop table rsmb_7_tempshelf_stage2
from    rsmb_7_tempshelf_stage1
group   by  account_number  -- 734,560 row(s) affected

create hg index rsmb7stage2hg on rsmb_7_tempshelf_stage2(account_number)

commit



-- Extracting the period requierd...
select  viq.account_number
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
        ,calendar.theweek                       as week
into    long_snap_pseudo
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                    )   as calendar
        on  segment_snapshot.profiling_date = calendar.thedate
		inner join sk_prod.VIQ_VIEWING_DATA_SCALING as viq
        on  segment_snapshot.account_number = viq.account_number
where   profiling_date between '2012-12-13' and '2013-07-26' -- 25,328,177 row(s) affected
and     account_number in   (
                                select distinct account_number
                                from    rsmb_7_tempshelf_stage2
                            )


create hg index long_snappseudophg1 on long_snap_pseudo(segment)
create hg index long_snappseudophg2 on long_snap_pseudo(account_number)
commit


-- QA
-- select count(1) from long_snap_pseudo where segment is null -- 882319

update  long_snap_pseudo   as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 10463493 row(s) updated


-- (RESULT 1)...
select  week
        ,segment
        ,overall_tenure
        ,count(distinct account_number) as num_housholds
from (
        select  distinct 
                snap.week
                ,snap.segment
                ,snap.account_number
                ,stats.week_frequency   as overall_tenure
                ,case   when (cast(left(stats.start_dt,4) as integer) = 2012 and cast(left(stats.end_dt,4) as integer) = 2013 and snap.week <50)
                        then ((52-stats.weekstart) + snap.week + 1)
                        else (snap.week - stats.weekstart)
                end as accumulated_tenure
        from    long_snap_pseudo                    as snap
                inner join rsmb_7_tempshelf_stage2  as stats
                on  snap.account_number = stats.account_number
                and ((snap.week >= stats.weekstart and snap.week <= stats.weekend) or stats.week_frequency >=30)
        where   accumulated_tenure >= 0
    )   as base
group   by  week
            ,segment
            ,overall_tenure

-- (RESULT 2)...
select  week
        ,segment
        ,account_number
        ,overall_tenure
        ,accumulated_tenure
from    (
            select  distinct 
                    snap.week
                    ,snap.segment
                    ,snap.account_number
                    ,stats.week_frequency   as overall_tenure
                    ,case   when (cast(left(stats.start_dt,4) as integer) = 2012 and cast(left(stats.end_dt,4) as integer) = 2013 and snap.week <50)
                            then ((52-stats.weekstart) + snap.week + 1)
                            else (snap.week - stats.weekstart)
                    end as accumulated_tenure
            from    long_snap_pseudo                    as snap
                    inner join rsmb_7_tempshelf_stage2  as stats
                    on  snap.account_number = stats.account_number
                    and ((snap.week >= stats.weekstart and snap.week <= stats.weekend) or stats.week_frequency >=30)
            where   accumulated_tenure >= 0
        )   as base
		
		
		

-----------------------------
-- [8] TENURE OF DROP-OFF HOME
-----------------------------


-- Extracting the period requierd...
select  account_number
        ,profiling_date                         as thedate
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
        ,calendar.theweek                       as week
into    snap
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_week_in_year   as theweek
                        from    sk_prod.VESPA_CALENDAR 
                    )   as calendar
        on  segment_snapshot.profiling_date = calendar.thedate
where   profiling_date between '2013-01-07' and '2013-03-24' -- 103651196 row(s) affected


create hg index snaphg1 on snap(segment)
commit

update  snap    as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 3289939 row(s) updated

commit

-- getting all accounts scaled on each day
select  calendar.thedate
        ,calendar.theweek
        ,intervals.account_number
into    drop table tempshelf_short
from    (
            select  distinct
                    utc_day_date        as thedate
                    ,utc_week_in_year   as theweek
            from    sk_prod.VESPA_CALENDAR 
            where   utc_day_date between '2013-01-07' and '2013-03-24'
        )   as calendar            
        inner join  vespa_analysts.SC2_Intervals as intervals
        on  calendar.thedate between intervals.reporting_starts and intervals.reporting_ends -- 30,566,006 row(s) affected

commit



-- Out of the skybase for each day lets sample for those accounts that we are actually interacting with the panel (Vespa DP)
select  *
into    target_snap
from    snap -- 103651196
where   account_number in ( select distinct account_number from tempshelf ) -- 5471310


-- (RESULT)...
select  segment
        ,removal_reason
        ,week_frequency
        ,count(distinct account_number) as hits
from    (
            select  base.account_number
                    ,min(campaigns.cell_name)                                                               as removal_reason
                    ,min(base.week) as start_at
                    ,max(case when campaigns.week is not null then (campaigns.week) else 0 end) - start_at  as week_frequency
                    ,min(case when campaigns.week is not null then base.segment else null end)              as segment
            from    (
                        select  distinct snap.segment
                                ,intervals.account_number
                                ,intervals.theweek  as week
                        from    tempshelf               as intervals
                                inner join target_snap  as snap
                                on  intervals.account_number = snap.account_number
                                and intervals.theweek = snap.week
								-- remove comments for running with a sample... testing purpose
                               -- and intervals.account_number in ('200001561394','200004454852','200007389766','210000351939','200003179914')
                    ) as base
                    left join  (
                                    select  cust.account_number
                                            ,calendar.theweek   as week
                                            ,lookup.cell_name
                                    from    sk_prod.campaign_history_lookup_cust        as lookup
                                            inner join sk_prod.CAMPAIGN_HISTORY_CUST    as cust
                                            on  lookup.cell_id = cust.cell_id
                                            inner join  (
                                                            select  distinct
                                                                    utc_day_date        as thedate
                                                                    ,utc_week_in_year   as theweek
                                                            from    sk_prod.VESPA_CALENDAR 
                                                            where   utc_day_date between '2013-01-07' and '2013-03-24'
                                                        )   as calendar
                                            on  cast(lookup.writeback_datetime as date) = calendar.thedate
                                    where   upper(lookup.campaign_name) like 'VESPA_DISABLEMENT_WEEKLY_%'
                                    and     cast(lookup.writeback_datetime as date) between '2013-01-07' and '2013-03-24'
                                    and     lookup.cell_name not in ('AnytimePlusEnablements & TransfersToPanel12')
									-- remove comments for running with a sample... testing purpose
                                   -- and     cust.account_number in ('200001561394','200004454852','200007389766','210000351939','200003179914')
                                )   as campaigns
                    on  base.account_number = campaigns.account_number
                    and base.week = campaigns.week
            group   by  base.account_number
            having  week_frequency >= 0
        )   as tenure_dropoff
group   by  segment
            ,removal_reason
            ,week_frequency



-------------------------
-- [9] OVERALL CONTINUITY
-------------------------

-- (RESULT)
select  segment
        ,sum(case when frequency = 0 then 1 else 0 end) as freq_0
        ,sum(case when frequency = 1 then 1 else 0 end) as freq_1
        ,sum(case when frequency = 2 then 1 else 0 end) as freq_2
        ,sum(case when frequency = 3 then 1 else 0 end) as freq_3
        ,sum(case when frequency = 4 then 1 else 0 end) as freq_4
        ,sum(case when frequency = 5 then 1 else 0 end) as freq_5
        ,sum(case when frequency = 6 then 1 else 0 end) as freq_6
        ,sum(case when frequency = 7 then 1 else 0 end) as freq_7
        ,sum(case when frequency = 8 then 1 else 0 end) as freq_8
from    (
            select  B.segment
                    ,A.account_number
                    ,count(distinct A.month) frequency
            from    rsmb_6_tempshelf_stage2 as A
                    inner join long_snap as B
                    on  A.account_number = B.account_number
                    and A.month = B.month
            group   by  B.segment
                        ,A.account_number
            --having  frequency = 8
        ) as rsmb_9
group   by  segment
			

			
			
--------------------------
--[10] ALL YEAR CONTINUITY			
--------------------------

-- (RESULT)
select  segment
        ,sum(case when frequency = 0 then 1 else 0 end) as freq_0
        ,sum(case when frequency = 1 then 1 else 0 end) as freq_1
        ,sum(case when frequency = 2 then 1 else 0 end) as freq_2
        ,sum(case when frequency = 3 then 1 else 0 end) as freq_3
        ,sum(case when frequency = 4 then 1 else 0 end) as freq_4
        ,sum(case when frequency = 5 then 1 else 0 end) as freq_5
        ,sum(case when frequency = 6 then 1 else 0 end) as freq_6
        ,sum(case when frequency = 7 then 1 else 0 end) as freq_7
        ,sum(case when frequency = 8 then 1 else 0 end) as freq_8
from    (
            select  B.segment
                    ,A.account_number
                    ,count(distinct A.month) frequency
            from    rsmb_6_tempshelf_stage2 as A
                    inner join long_snap as B
                    on  A.account_number = B.account_number
                    and A.month = B.month
            group   by  B.segment
                        ,A.account_number
            having  frequency = 8
        ) as rsmb_9
group   by  segment



---------------------------
-- [11]	TRACKING CONTINUITY
---------------------------


-- Extracting the period requierd...
select  account_number
        ,profiling_date                         as thedate
        ,lookup.segment                         as segment
        ,segment_snapshot.scaling_segment_id    as old_
        ,calendar.themonth                      as month
into    long_snap
from    vespa_analysts.SC2_Sky_base_segment_snapshots as segment_snapshot
        left join rsmb_segment_lookup as lookup
        on segment_snapshot.scaling_segment_id = lookup.segment
        inner join  (   
                        -- Unifying view of vespa calendar into date level to get the week of year value...
                        select  distinct
                                utc_day_date        as thedate
                                ,utc_month_num      as themonth
                        from    sk_prod.VESPA_CALENDAR 
                    )   as calendar
        on  segment_snapshot.profiling_date = calendar.thedate
where   profiling_date between '2012-12-13' and '2013-07-26' -- 329497048 row(s) affected


create hg index long_snapphg1 on long_snap(segment);
commit;

-- QA
-- select count(1) from long_snap where segment is null -- 10463493

update  long_snap   as a
set     a.segment = b.segment
from    rsmb_segment_lookup as b
where   a.old_ = b.ref
and     a.segment is null -- 10463493 row(s) updated


-- sampling for all households that were in the panel by the start of the 
-- evaluation period...(RESULT)


select  segment
        ,sum(case when month_frequency = 0 then 1 else 0 end ) as freq_0
        ,sum(case when month_frequency = 1 then 1 else 0 end ) as freq_1
        ,sum(case when month_frequency = 2 then 1 else 0 end ) as freq_2
        ,sum(case when month_frequency = 3 then 1 else 0 end ) as freq_3
        ,sum(case when month_frequency = 4 then 1 else 0 end ) as freq_4
        ,sum(case when month_frequency = 5 then 1 else 0 end ) as freq_5
        ,sum(case when month_frequency = 6 then 1 else 0 end ) as freq_6
        ,sum(case when month_frequency = 7 then 1 else 0 end ) as freq_7
        ,sum(case when month_frequency = 8 then 1 else 0 end ) as freq_8
from    (
            select  intervals.drop_month
                    ,intervals.month_frequency
                    ,lsnap.segment
            from    (
                        select  account_number
                                ,max(reporting_ends)                        as drop_date
                                ,datepart(month,drop_date)                  as drop_month
                                ,datediff(month, '2012-12-13', drop_date)   as month_frequency
                        from    vespa_analysts.SC2_Intervals
                        where   account_number in   (
                                                        select  distinct
                                                                account_number
                                                        from    vespa_analysts.SC2_Intervals
                                                        where   '2012-12-13' between reporting_starts and reporting_ends
                                                    )   
                        group   by  account_number
                    )   as intervals
                    left join long_snap as lsnap
                    on  intervals.account_number = lsnap.account_number
                    and lsnap.thedate = '2012-12-13' 
        )   as base
group   by  segment




