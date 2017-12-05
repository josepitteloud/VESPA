
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
select top 100 * from vespa_analysts.SC2_Segments_Lookup_v2_1 -- scaling segmentation variables/ID for 2013

-- How updated is this table?
select max(reporting_ends),max(reporting_Starts) 
from vespa_analysts.SC2_Intervals
-- 24 April

-- Let's proceed with the week 8-14 April
select count(distinct account_number)
from vespa_analysts.SC2_Intervals
where  ( reporting_starts >= '2013-04-08' and reporting_starts <= '2013-04-14' )
or ( reporting_ends >= '2013-04-08' and reporting_ends <= '2013-04-14' )
or ( reporting_starts < '2013-04-08' and reporting_ends > '2013-04-14' )
-- 440,116

-- Let's look at single box view
select top 100 * from vespa_analysts.vespa_single_box_view

select panel_id_vespa,count(*) 
from vespa_analysts.vespa_single_box_view 
group by panel_id_vespa
order by panel_id_vespa
/*
,168787
6,590320
7,601505
12,677489
*/

-- Breakdown by status
select status_vespa,count(*) 
from vespa_analysts.vespa_single_box_view 
group by status_vespa
order by status_vespa
/*
,168787
'DisablePending',4874
'Disabled',253789
'Enabled',1610651
*/

-- Are there account numbers being used in scaling that are not looged into single box view?
select count(distinct account_number)
from vespa_analysts.SC2_Intervals
where ( ( reporting_starts >= '2013-04-08' and reporting_starts <= '2013-04-14' )
or ( reporting_ends >= '2013-04-08' and reporting_ends <= '2013-04-14' )
or ( reporting_starts < '2013-04-08' and reporting_ends > '2013-04-14' ) )-- 440,116 
and account_number not in
    (
    select account_number
    from vespa_analysts.vespa_single_box_view
    where 1=1
    --and panel_id_vespa = 12
    --and status_vespa = 'Enabled'
    --and enablement_date < '2013-04-08'
    )
-- 0

-- Obtain data return stats for HHs
drop table HH_Data_Return_8_14_Apr
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
into HH_Data_Return_8_14_Apr
from (
select account_number
        ,reporting_starts
        ,reporting_ends
        ,case when reporting_starts <= '2013-04-08' and reporting_ends >= '2013-04-08' then 1 else 0 end as Reported_Mon
        ,case when reporting_starts <= '2013-04-09' and reporting_ends >= '2013-04-09' then 1 else 0 end as Reported_Tue
        ,case when reporting_starts <= '2013-04-10' and reporting_ends >= '2013-04-10' then 1 else 0 end as Reported_Wed
        ,case when reporting_starts <= '2013-04-11' and reporting_ends >= '2013-04-11' then 1 else 0 end as Reported_Thu
        ,case when reporting_starts <= '2013-04-12' and reporting_ends >= '2013-04-12' then 1 else 0 end as Reported_Fri
        ,case when reporting_starts <= '2013-04-13' and reporting_ends >= '2013-04-13' then 1 else 0 end as Reported_Sat
        ,case when reporting_starts <= '2013-04-14' and reporting_ends >= '2013-04-14' then 1 else 0 end as Reported_Sun
from vespa_analysts.SC2_Intervals
) t
group by account_number
having min(reporting_starts) < '2013-04-08' 
and max(reporting_ends) > '2013-04-14' 
-- 439,160 row(s) affected

-- Breakdown of HHs by number of days whare they have returned data
select Reported_Week,count(*)
from HH_Data_Return_8_14_Apr
group by Reported_Week
order by Reported_Week
/*
0,10115
1,6004
2,7180
3,8522
4,11354
5,16649
6,38072
7,341264
*/

-- Distribution of HHs by their overall reporting rate
select round(reporting_rate,1) as reporting_rate
        ,count(*)
from HH_Data_Return_8_14_Apr 
group by reporting_rate
order by reporting_rate
/*
0E-11,130
0.10000000000,2043
0.20000000000,6270
0.30000000000,4930
0.40000000000,5606
0.50000000000,9055
0.60000000000,16639
0.70000000000,42508
0.80000000000,76727
0.90000000000,155269
1.00000000000,119983
*/

-- Distribution of HHs that have not returned data for week Dec 3-9 by their overall reporting rate
select round(reporting_rate,1) as reporting_rate
        ,count(*)
from HH_Data_Return_8_14_Apr
where Reported_week = 0
group by reporting_rate
order by reporting_rate
/*
0E-11,94
0.10000000000,689
0.20000000000,794
0.30000000000,685
0.40000000000,608
0.50000000000,767
0.60000000000,1161
0.70000000000,1624
0.80000000000,2528
0.90000000000,1158
1.00000000000,7
*/

-- Num of months in the panel for HHs not returning data
select datediff(month,reporting_started,date('2013-04-08')) as months_reporting
        ,count(*)
from HH_Data_Return_8_14_Apr
where Reported_week = 0
group by months_reporting
order by months_reporting


-- Obtain most relevant Segment ID for HHs that reported during the week
select account_number
        ,scaling_segment_id
into #HHs_reporting_8_14_Apr
from (
select account_number
        ,scaling_segment_id
        ,rank() over(partition by account_number order by reporting_starts desc) Scaling_Order
from vespa_analysts.SC2_Intervals
where account_number in (select account_number from HH_Data_Return_8_14_Apr)
and (
( reporting_starts >= '2013-04-08' and reporting_starts <= '2013-04-14' )
or ( reporting_ends >= '2013-04-08' and reporting_ends <= '2013-04-14' )
or ( reporting_starts < '2013-04-08' and reporting_ends > '2013-04-14' )
)
) t
where Scaling_Order = 1
-- 429,045 row(s) affected

-- Obtain most relevant Segment ID for HHs that not reported during the week
select account_number
        ,scaling_segment_id
into #HHs_not_reporting_8_14_Apr
from (
select account_number
        ,scaling_segment_id
        ,rank() over(partition by account_number order by reporting_starts desc) Scaling_Order
from vespa_analysts.SC2_Intervals
where account_number in (select account_number from HH_Data_Return_8_14_Apr)
and account_number not in (select account_number from #HHs_reporting_8_14_Apr)
) t
where Scaling_Order = 1
-- 10,115 row(s) affected

-- Aggregate Segment ID information
select *
into scaling_segments_8_14_Apr
from (
select * from #HHs_reporting_8_14_Apr
union all
select * from #HHs_not_reporting_8_14_Apr
) t
-- 439,160 row(s) affected

-- Profile HHs by scaling variables
drop table HH_Data_Return_8_14_Apr_Profile
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
into HH_Data_Return_8_14_Apr_Profile
from HH_Data_Return_8_14_Apr DR
left join scaling_segments_8_14_Apr HH
on DR.account_number = HH.account_number
left join vespa_analysts.SC2_Segments_lookup_v2_1 SEG
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
-- 54,314 row(s) affected

-- sanity checks
select count(*) from HH_Data_Return_8_14_Apr -- 439160
select sum(number_hhs) from HH_Data_Return_8_14_Apr_Profile --  439160

select * from HH_Data_Return_8_14_Apr_Profile

-- Grant permissions
grant all on HH_Data_Return_8_14_Apr_Profile to igonorp;
grant all on scaling_segments_8_14_Apr to igonorp;
grant all on HH_Data_Return_8_14_Apr to igonorp;


