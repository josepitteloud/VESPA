/***********************************************************************************
** DATE : 08 / 08 / 2013
** PROJECT VESPA: SCALING 3.0
** ANALYST : Patrick Igonor
** LEAD ANALYST : CLAUDIO LIMA
** Analysis of the Distribution of Current Scaling Segments by their population/sample sizes
**
************************************************************************************/

--Tables of Interest
select top 10* from vespa_analysts.SC2_Segments_lookup_V2_1
select top 10* from vespa_analysts.SC2_Variables_Lookup_v2_1
select top 10* from vespa_analysts.SC2_Intervals
select top 10* from vespa_analysts.SC2_Weightings
select top 10* from vespa_analysts.SC2_Sky_base_segment_snapshots

select profiling_date, count(*) from vespa_analysts.SC2_Sky_base_segment_snapshots
group by profiling_date

select scaling_day, count(*) from vespa_analysts.SC2_Weightings
where scaling_day between '2013-07-08' and '2013-07-14'
group by scaling_day


--Quick Check
select count (distinct scaling_segment_ID) from vespa_analysts.SC2_Intervals
where reporting_starts <= '2013-07-14' and reporting_ends >= '2013-07-14'

select count (distinct scaling_segment_ID) from vespa_analysts.SC2_Sky_base_segment_snapshots
where reporting_starts <= '2013-07-14' and reporting_ends >= '2013-07-14'


****************************************************************

select count(*) from vespa_analysts.SC2_Segments_lookup_V2_1
--162,241
select distinct count(scaling_segment_ID) from vespa_analysts.SC2_Segments_lookup_V2_1
--162,241

select count(*) from vespa_analysts.SC2_Weightings
--count() 35,977,762

select distinct count(scaling_segment_ID) from vespa_analysts.SC2_Weightings
--count() 35,977,762

--Matching the vespa_accounts, sky_base_accounts and weightings with the scaling variables and segments in the SC2_Segments_lookup_V2_1 table.
select  SL.scaling_segment_ID
       ,SL.universe
       ,SL.isba_tv_region
       ,SL.hhcomposition
       ,SL.tenure
       ,SL.package
       ,SL.boxtype
       ,SL.scaling_segment_name
       ,WT.scaling_day
       ,WT.vespa_accounts
       ,WT.sky_base_accounts
       ,WT.weighting
into Segments_Weightings_Distr
from vespa_analysts.SC2_Segments_lookup_V2_1 SL
inner join vespa_analysts.SC2_Weightings WT
on SL.scaling_segment_ID = WT.scaling_segment_ID
and WT.scaling_day = '2013-07-14'
--42,916 Row(s) affected
select scaling_segment_ID,vespa_accounts,sky_base_accounts,weighting from igonorp.Segments_Weightings_Distr

--Granting access to Alex ---
grant all on Segments_Weightings_Distr to glasera;
grant all on account_level_segments to glasera;
commit;


**********************************************************************************

--Checking the different levels of each Scaling variable
select universe,count(*)
from Segments_Weightings_Distr
group by universe

select isba_tv_region,count(*)
from Segments_Weightings_Distr
group by isba_tv_region

select hhcomposition,count(*)
from Segments_Weightings_Distr
group by hhcomposition

select tenure,count(*)
from Segments_Weightings_Distr
group by tenure

select package,count(*)
from Segments_Weightings_Distr
group by package

select boxtype,count(*)
from Segments_Weightings_Distr
group by boxtype

--Checking the minimum and nmaximum values within the table
select min(sky_base_accounts)as min_acc
      ,max(sky_base_accounts) as max_acc
from Segments_Weightings_Distr
--min_acc   max_acc
    1       26,258

--Splitting each segment variables into buckets based on the sky base accounts

--Universe
select  universe,Number_HHs, count(*) as Number_Seg_Universe
into Universe_Buckets
from (
    select  scaling_segment_ID,universe,
            case
            when sky_base_accounts = 0 then '0'
            when sky_base_accounts between 1 and 9 then '1-9'
            when sky_base_accounts between 10 and 99 then '10-99'
            when sky_base_accounts between 100 and 999 then '100-999'
            when sky_base_accounts between 1000 and 9999 then '1000-9999'
            when sky_base_accounts between 10000 and 99999 then '10000-99999'
            when sky_base_accounts > 99999 then '99999+'
            else null
            end as Number_HHs
    from Segments_Weightings_Distr
    group by scaling_segment_ID,universe, sky_base_accounts
) as sub1
group by universe,Number_HHs
;

--isba_tv_region
select  isba_tv_region,Number_HHs, count(*) as Number_Seg_Region
into Region_buckets
from (
    select  scaling_segment_ID,isba_tv_region,
            case
            when sky_base_accounts =0 then '0'
            when sky_base_accounts between 1 and 9 then '1-9'
            when sky_base_accounts between 10 and 99 then '10-99'
            when sky_base_accounts between 100 and 999 then '100-999'
            when sky_base_accounts between 1000 and 9999 then '1000-9999'
            when sky_base_accounts between 10000 and 99999 then '10000-99999'
            when sky_base_accounts > 99999 then '99999+'
            else null
            end as Number_HHs
    from Segments_Weightings_Distr
    group by scaling_segment_ID,isba_tv_region, sky_base_accounts
) as sub1
group by isba_tv_region,Number_HHs
;
--hhcomposition
select  hhcomposition,Number_HHs, count(*) as Number_Seg_HHComp
into HHComposition_buckets
from (
    select  scaling_segment_ID,hhcomposition,
            case
            when sky_base_accounts =0 then '0'
            when sky_base_accounts between 1 and 9 then '1-9'
            when sky_base_accounts between 10 and 99 then '10-99'
            when sky_base_accounts between 100 and 999 then '100-999'
            when sky_base_accounts between 1000 and 9999 then '1000-9999'
            when sky_base_accounts between 10000 and 99999 then '10000-99999'
            when sky_base_accounts > 99999 then '99999+'
            else null
            end as Number_HHs
    from Segments_Weightings_Distr
    group by scaling_segment_ID,hhcomposition, sky_base_accounts
) as sub1
group by hhcomposition,Number_HHs

;
----Tenure
select  tenure,Number_HHs, count(*) as Number_Seg_tenure
into Tenure_buckets
from (
    select  scaling_segment_ID,tenure,
            case
            when sky_base_accounts =0 then '0'
            when sky_base_accounts between 1 and 9 then '1-9'
            when sky_base_accounts between 10 and 99 then '10-99'
            when sky_base_accounts between 100 and 999 then '100-999'
            when sky_base_accounts between 1000 and 9999 then '1000-9999'
            when sky_base_accounts between 10000 and 99999 then '10000-99999'
            when sky_base_accounts > 99999 then '99999+'
            else null
            end as Number_HHs
    from Segments_Weightings_Distr
    group by scaling_segment_ID,tenure, sky_base_accounts
) as sub1
group by tenure,Number_HHs
;
----Package
select  package,Number_HHs, count(*) as Number_Seg_package
into Package_buckets
from (
    select  package,
            case
            when sky_base_accounts =0 then '0'
            when sky_base_accounts between 1 and 9 then '1-9'
            when sky_base_accounts between 10 and 99 then '10-99'
            when sky_base_accounts between 100 and 999 then '100-999'
            when sky_base_accounts between 1000 and 9999 then '1000-9999'
            when sky_base_accounts between 10000 and 99999 then '10000-99999'
            when sky_base_accounts > 99999 then '99999+'
            else null
            end as Number_HHs
    from Segments_Weightings_Distr
    group by scaling_segment_ID,package, sky_base_accounts
) as sub1
group by package,Number_HHs
;
----Boxtype
select  boxtype,Number_HHs, count(*) as Number_Seg_boxtype
into Boxtype_buckets
from (
    select  scaling_segment_ID,boxtype,
            case
            when sky_base_accounts =0 then '0'
            when sky_base_accounts between 1 and 9 then '1-9'
            when sky_base_accounts between 10 and 99 then '10-99'
            when sky_base_accounts between 100 and 999 then '100-999'
            when sky_base_accounts between 1000 and 9999 then '1000-9999'
            when sky_base_accounts between 10000 and 99999 then '10000-99999'
            when sky_base_accounts > 99999 then '99999+'
            else null
            end as Number_HHs
    from Segments_Weightings_Distr
    group by scaling_segment_ID,boxtype, sky_base_accounts
) as sub1
group by boxtype,Number_HHs

*************************************************************************************

--Bringing all the scaling segment buckets together

select  Number_HHs,Number_Seg_Universe as Number_Segments,'universe' as Scaling_Variables, universe as Scaling_Sub_Variables into Combined_Scaling_Segments from Universe_Buckets
union all
select  Number_HHs,Number_Seg_Region as Number_Segments,'isba_tv_region' as Scaling_Variables, isba_tv_region as Scaling_Sub_Variables from Region_buckets
union all
select  Number_HHs,Number_Seg_HHComp as Number_Segments,'hhcomposition' as Scaling_Variables, hhcomposition as Scaling_Sub_Variables from HHComposition_buckets
union all
select  Number_HHs,Number_Seg_tenure as Number_Segments,'tenure' as Scaling_Variables, tenure as Scaling_Sub_Variables from Tenure_buckets
union all
select  Number_HHs,Number_Seg_package as Number_Segments,'package' as Scaling_Variables, package as Scaling_Sub_Variables from Package_buckets
union all
select  Number_HHs,Number_Seg_boxtype as Number_Segments,'boxtype' as Scaling_Variables, boxtype as Scaling_Sub_Variables from Boxtype_buckets


--Calculating the Sum of each scaling variable within each bucket ----
select  Number_HHs
       ,Scaling_Variables
       ,sum(Number_Segments)as Sum_Segments
into   #Sum_Scaling_Segments
from Combined_Scaling_Segments
group by Number_HHs,Scaling_Variables
--30 Row(s) affected

----Matching the sum obtained above to the Combined_Scaling_Segments table
select CSS.Number_HHs
      ,CSS.Scaling_Variables
      ,CSS.Scaling_Sub_Variables
      ,CSS.Number_Segments
      ,SSS.Sum_Segments
into Final_Combined_Scaling_segments
from Combined_Scaling_Segments CSS
inner join #Sum_Scaling_Segments SSS
on CSS.Number_HHs = SSS.Number_HHs
and CSS.Scaling_Variables = SSS.Scaling_Variables
--234 Row(s) affected

--Calculating the Percentages of each scaling varaiable within each bucket ----
select  Number_HHs
       ,Scaling_Variables
       ,Scaling_Sub_Variables
       ,Number_Segments
       ,Sum_Segments
       ,1.0*Number_Segments/Sum_Segments as Percentages
into Segment_Percentages
from Final_Combined_Scaling_segments
group by Number_HHs
       ,Scaling_Variables
       ,Scaling_Sub_Variables
       ,Number_Segments
       ,Sum_Segments
--234 Row(s) affected

--Selecting the most represented from the Scaling sub-variables (which is the maximum)
select   Number_HHs
        ,Sum_Segments
        ,Scaling_Variables
        ,Max(Percentages)as Max_Percentages
into #Max_Scaling_Variable
from Segment_Percentages
group by Number_HHs
        ,Sum_Segments
        ,Scaling_Variables
--30 Row(s) affected

----Matching this back to obtain the scaling sub-variables
select MSV.Number_HHs
      ,MSV.Sum_Segments
      ,MSV.Scaling_Variables
      ,SP.Scaling_Sub_Variables
      ,MSV.Max_Percentages
into Final_Most_Rep_Scaling_Var
from #Max_Scaling_Variable MSV
inner join Segment_Percentages SP
on MSV.Number_HHs = SP.Number_HHs
and MSV.Sum_Segments = SP.Sum_Segments
and MSV.Scaling_Variables = SP.Scaling_Variables
and MSV.Max_Percentages = SP.Percentages
order by MSV.Number_HHs

****************************************************************************************************************
--Ranking Scaling Segments by their representation in Vespa
select   scaling_segment_ID
        ,universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,boxtype
        ,scaling_segment_name
        ,scaling_day
        ,vespa_accounts
        ,sky_base_accounts
        ,weighting
        ,1.0*vespa_accounts / sky_base_accounts as Scaling_Segment_Rep
from Segments_Weightings_Distr
order by vespa_accounts asc,sky_base_accounts desc


******************************************************************************************************************
--Granting Privileges

grant all on Segments_Weightings_Distr to limac;
grant all on Universe_Buckets to limac;
grant all on Region_buckets to limac;
grant all on HHComposition_buckets to limac;
grant all on Tenure_buckets to limac;
grant all on Package_buckets to limac;
grant all on Boxtype_buckets to limac;
grant all on Combined_Scaling_Segments to limac;
grant all on Final_Combined_Scaling_segments to limac;
grant all on Segment_Percentages to limac;
grant all on Final_Most_Rep_Scaling_Var to limac;
grant all on account_level_segments to limac;
commit;

--creating an index on table Segments_Weightings_Distr--

create hg index idx1 on Segments_Weightings_Distr(scaling_segment_ID)

--Getting the data into an account level ---
select ss.account_number
      ,ss.profiling_date
      ,sw.*
into account_level_segments
from vespa_analysts.SC2_Sky_base_segment_snapshots ss
left join Segments_Weightings_Distr sw
on ss.scaling_segment_id = sw.scaling_segment_ID
where profiling_date = '2013-07-12'
--9,406,328 Row(s) affected

select count(distinct account_number) from account_level_segments
--9,406,328 Row(s) affected




select scaling_segment_id
      ,sky_base_accounts
      ,segment_weight
      ,case when vespa_panel = 0.000001 then 0 else vespa_panel end as Vespa_Accounts
from glasera.V154_weighting_working_table
--4,479 Row(s) affected


select scaling_segment_id
      ,sky_base_accounts
      ,vespa_accounts
from Segments_Weightings_Distr
--42,916 Row(s) affected

select top 10* from Segments_Weightings_Distr
grant all on Segments_Weightings_Distr to limac;
--Current
select count(scaling_segment_ID) as Num_Scaling_segment_ID
      ,case when weighting < 1 then '0'
            when weighting <= 10 then '1 - 10'
            when weighting <= 99 then '11 - 99'
            when weighting <= 999 then '100 - 999'
            when weighting > 999 then '999+'
      end as weighting_Intervals
from
group by weighting_Intervals
order by weighting_Intervals

--New
select case when segment_weight < 1 then '0'
            when segment_weight <= 10 then '1 - 10'
            when segment_weight <= 99 then '11 - 99'
            when segment_weight <= 999 then '100 - 999'
            when segment_weight > 999 then '999+'
      end as weighting_Intervals,
      count(scaling_segment_ID) as Num_Scaling_segment_ID
from glasera.V154_weighting_working_table
group by weighting_Intervals
order by weighting_Intervals




