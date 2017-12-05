
-- Listing tables related to scaling
select table_name 
from sp_tables()
where table_owner = 'vespa_analysts'
and table_name like 'SC2%'
order by 1
/*
'SC2_Intervals'
'SC2_Segments_Lookup_v2_0'
'SC2_Segments_Lookup_v2_1'
'SC2_Sky_base_segment_snapshots'
'SC2_Todays_panel_members'
'SC2_Todays_segment_weights'
'SC2_Variables_Lookup_v2_0'
'SC2_Variables_Lookup_v2_1'
'SC2_Weightings'
'SC2_category_subtotals'
'SC2_category_working_table'
'SC2_metrics'
'SC2_non_convergences'
'SC2_scaling_weekly_sample'
'SC2_weighting_working_table'

*/

-- Inspect some of the tables
select top 100 * from vespa_analysts.SC2_Weightings -- scaling weights
select top 100 * from vespa_analysts.SC2_Intervals -- scaling ID for each HHs
select top 100 * from vespa_analysts.SC2_Segments_Lookup_v2_0 -- scaling segmentation variables/ID

-- How updated is this table?
select max(reporting_ends),max(reporting_Starts) 
from vespa_analysts.SC2_Intervals
-- 26 Dec
-- There's an issue with scaling convergence meaning there's no scaling data end December
-- Sebastian advised to use scaling data until 12 Dec

-- Let's proceed with the week 3-9 Dec
select count(distinct account_number)
from vespa_analysts.SC2_Intervals
where  ( reporting_starts >= '2012-12-03' and reporting_starts <= '2012-12-09' )
or ( reporting_ends >= '2012-12-03' and reporting_ends <= '2012-12-09' )
or ( reporting_starts < '2012-12-03' and reporting_ends > '2012-12-09' )
-- 484,365

-- Let's look at single box view
select panel_id_vespa,count(*) 
from vespa_analysts.vespa_single_box_view 
group by panel_id_vespa
order by panel_id_vespa
/*
NULL,   167260
6,      686592
7,      696370
12,     721956
*/

-- Breakdown by status
select status_vespa,count(*) 
from vespa_analysts.vespa_single_box_view 
group by status_vespa
order by status_vespa
/*
NULL,               167260
'DisablePending',   123149
'Disabled',         130112
'EnablePending',    39249
'Enabled',          1812408
*/

-- Are there account numbers being used in scaling that are not looged into single box view?
select count(distinct account_number)
from vespa_analysts.SC2_Intervals
where ( reporting_starts <= '2012-12-09' and reporting_starts >= '2012-12-03' )
or ( reporting_ends >= '2012-12-03' and reporting_ends <= '2012-12-09' )
or ( reporting_starts < '2012-12-03' and reporting_ends > '2012-12-09' ) -- 484,365
and account_number not in
    (
    select account_number
    from vespa_analysts.vespa_single_box_view
    where panel_id_vespa = 12
    --and status_vespa = 'Enabled'
    and enablement_date < '2012-12-03'
    )
-- 105,147
-- From looking at some reports with Angel around 10k customers / week request opt-out
-- Since 3 months have passed since the week of analysis, 100k seems a reasonable number

-- Obtain data return stats for HHs
select account_number
        ,min(reporting_starts) as Reporting_Started
        ,max(reporting_ends) as Reporting_Ended
        ,sum(Reported_Mon) as Reported_Mon
        ,sum(Reported_Tue) as Reported_Tue
        ,sum(Reported_Wed) as Reported_Wed
        ,sum(Reported_Thu) as Reported_Thu
        ,sum(Reported_Fri) as Reported_Fri
        ,sum(Reported_Sat) as Reported_Sat
        ,sum(Reported_Sun) as Reported_Sun
        ,Reported_Mon+Reported_Tue+Reported_Wed+Reported_Thu+Reported_Fri+Reported_Sat+Reported_Sun as Reported_Week
        ,sum(reporting_ends-reporting_starts+1) as Total_Reported
        ,Total_Reported*1.0/(max(reporting_ends)-min(reporting_starts)+1) as Reporting_Rate 
into HH_Data_Return_3_9_Dec
from (
select account_number
        ,reporting_starts
        ,reporting_ends
        ,case when reporting_starts <= '2012-12-03' and reporting_ends >= '2012-12-03' then 1 else 0 end as Reported_Mon
        ,case when reporting_starts <= '2012-12-04' and reporting_ends >= '2012-12-04' then 1 else 0 end as Reported_Tue
        ,case when reporting_starts <= '2012-12-05' and reporting_ends >= '2012-12-05' then 1 else 0 end as Reported_Wed
        ,case when reporting_starts <= '2012-12-06' and reporting_ends >= '2012-12-06' then 1 else 0 end as Reported_Thu
        ,case when reporting_starts <= '2012-12-07' and reporting_ends >= '2012-12-07' then 1 else 0 end as Reported_Fri
        ,case when reporting_starts <= '2012-12-08' and reporting_ends >= '2012-12-08' then 1 else 0 end as Reported_Sat
        ,case when reporting_starts <= '2012-12-09' and reporting_ends >= '2012-12-09' then 1 else 0 end as Reported_Sun
from vespa_analysts.SC2_Intervals
) t
group by account_number
having max(reporting_ends) >= '2012-12-09' and min(reporting_starts) <= '2012-12-03'
-- 489972 row(s) affected

-- Breakdown of HHs by number of days whare they have returned data
select Reported_Week,count(*)
from HH_Data_Return_3_9_Dec
group by Reported_Week
order by Reported_Week
/*
0,9934
1,5148
2,5462
3,6595
4,8883
5,14464
6,38137
7,401349
*/

select * 
from HH_Data_Return_3_9_Dec 
order by reporting_rate desc
        ,total_reported desc

-- Distribution of HHs by their overall reporting rate
select round(reporting_rate,1) as reporting_rate
        ,count(*)
from HH_Data_Return_3_9_Dec 
group by reporting_rate
order by reporting_rate
/*
0,163
0.10000000000,1992
0.20000000000,3273
0.30000000000,4546
0.40000000000,6105
0.50000000000,10457
0.60000000000,33766
0.70000000000,47256
0.80000000000,63914
0.90000000000,110966
1.00000000000,207534
*/

-- Distribution of HHs that have not returned data for week Dec 3-9 by their overall reporting rate
select round(reporting_rate,1) as reporting_rate
        ,count(*)
from HH_Data_Return_3_9_Dec
where Reported_week = 0
group by reporting_rate
order by reporting_rate
/*
0,139
0.10000000000,1215
0.20000000000,1138
0.30000000000,991
0.40000000000,901
0.50000000000,1024
0.60000000000,1182
0.70000000000,1251
0.80000000000,1508
0.90000000000,585
*/

-- Num of months in the panel for HHs not returning data
select datediff(month,reporting_started,date('2012-12-03')) as months_reporting
        ,count(*)
from HH_Data_Return_3_9_Dec
where Reported_week = 0
group by months_reporting
order by months_reporting
/*
0,2
1,56
2,261
3,3122
4,3684
6,583
7,230
9,12
10,66
11,82
12,455
13,1381
*/

-- Obtain most relevant Segment ID for HHs that reported during the week
select account_number
        ,scaling_segment_id
into #HHs_reporting_3_9_Dec
from (
select account_number
        ,scaling_segment_id
        ,rank() over(partition by account_number order by reporting_starts desc) Scaling_Order
from vespa_analysts.SC2_Intervals
where account_number in (select account_number from HH_Data_Return_3_9_Dec)
and (
( reporting_starts >= '2012-12-03' and reporting_starts <= '2012-12-09' )
or ( reporting_ends >= '2012-12-03' and reporting_ends <= '2012-12-09' )
or ( reporting_starts < '2012-12-03' and reporting_ends > '2012-12-09' )
)
) t
where Scaling_Order = 1
-- 480,038

-- Obtain most relevant Segment ID for HHs that reported during the week
select account_number
        ,scaling_segment_id
into #HHs_not_reporting_3_9_Dec
from (
select account_number
        ,scaling_segment_id
        ,rank() over(partition by account_number order by reporting_starts desc) Scaling_Order
from vespa_analysts.SC2_Intervals
where account_number in (select account_number from HH_Data_Return_3_9_Dec)
and account_number not in (select account_number from #HHs_reporting_3_9_Dec)
) t
where Scaling_Order = 1
-- 9,934

-- Aggregate Segment ID information
select *
into scaling_segments_3_9_Dec
from (
select * from #HHs_reporting_3_9_Dec
union all
select * from #HHs_not_reporting_3_9_Dec
) t
-- 489,972

-- Profile HHs by scaling variables
drop table HH_Data_Return_3_9_Dec_Profile
select DR.Reported_Week
        ,case 
            when DR.Reported_Week = 0 then 'No return'
            when DR.Reported_Week between 1 and 3 then 'Low return'
            when DR.Reported_Week between 4 and 7 then 'Acceptable return'
        end as Reporting_Quality
        ,SEG.universe
        ,SEG.isba_tv_region
        ,SEG.hhcomposition
        ,SEG.tenure
        ,SEG.package
        ,SEG.boxtype
        ,count(distinct DR.account_number) as Number_HHs
into HH_Data_Return_3_9_Dec_Profile
from HH_Data_Return_3_9_Dec DR
left join scaling_segments_3_9_Dec HH
on DR.account_number = HH.account_number
left join vespa_analysts.SC2_Segments_lookup SEG
on HH.scaling_segment_id = SEG.scaling_segment_id
group by DR.Reported_Week
        ,Reporting_Quality
        ,SEG.universe
        ,SEG.isba_tv_region
        ,SEG.hhcomposition
        ,SEG.tenure
        ,SEG.package
        ,SEG.boxtype
order by Number_HHs desc
-- 53916

-- sanity checks
select count(*) from HH_Data_Return_3_9_Dec -- 489972
select sum(number_hhs) from HH_Data_Return_3_9_Dec_Profile --  489972

select * from HH_Data_Return_3_9_Dec_Profile

-- Grant permissions
grant all on HH_Data_Return_3_9_Dec_Profile to igonorp;
grant all on scaling_segments_3_9_Dec to igonorp;
grant all on HH_Data_Return_3_9_Dec to igonorp;


