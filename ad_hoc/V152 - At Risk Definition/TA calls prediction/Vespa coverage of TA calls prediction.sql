/*

TA calls prediction - Analyse Vespa coverage

Lead: Claudio Lima
Date: 2013-08-11

*/

select top 100 * from nicholm.TA_CALLERS_201307_V2_SCORED
select count(*) from nicholm.TA_CALLERS_201307_V2_SCORED  -- 9,346,993

select round(ta_propensity,2),count(*) 
from nicholm.TA_CALLERS_201307_V2_SCORED
where segment = '<10_Months'
group by round(ta_propensity,2)
order by round(ta_propensity,2)

-- Summary of TA call propensities
select segment
        ,round(ta_propensity,2) as ta_propensity
        ,count(*) as num_HHs
        ,num_HHs*1.0/sum(num_HHS) over(partition by segment) as percentage
from nicholm.TA_CALLERS_201307_V2_SCORED 
group by segment,ta_propensity
order by 1,2

-- Identify which accounts are in Vespa
select ta.*
        ,case 
            when sbv.account_number is not null
            then 1
            else 0
        end as Vespa
into VESPA_TA_CALLERS_201307_SCORED
from nicholm.TA_CALLERS_201307_V2_SCORED ta
left join (select distinct account_number from vespa_analysts.vespa_single_box_view where status_vespa = 'Enabled') sbv
on ta.account_number = sbv.account_number
-- 9346993 row(s) affected

select count(*) from VESPA_TA_CALLERS_201307_SCORED where vespa = 1 -- 2,201,700

-- Report num HHs for each TA propensity and Vespa coverage
select segment
        ,vespa
        ,round(ta_propensity,2) as ta_propensity
        ,count(*) as num_HHs
        ,sum(num_HHs) over(partition by segment,ta_propensity) as total_HHs
        ,num_HHs*1.0/total_HHs as percentage
from VESPA_TA_CALLERS_201307_SCORED
group by segment,vespa,ta_propensity
order by 1,2,3

-- Look at num of TA calls and proportion for which viewing data will be available
select segment
        ,vespa
        ,count(*) as num_HHs
        ,floor(sum(ta_propensity)) as num_ta_calls
from VESPA_TA_CALLERS_201307_SCORED
group by segment,vespa
order by 1,2

-- Number of TA calls covered by vespa in function of number of people added to the panel
select segment
        ,num_HHs
        ,num_ta_calls
from (
select segment
        ,ta_propensity
        ,count(*) over (partition by segment 
                                         order by ta_propensity desc
                                         rows between unbounded preceding 
                                         and current row) as num_HHs
        ,sum(ta_propensity) over (partition by segment 
                                         order by ta_propensity desc
                                         rows between unbounded preceding 
                                         and current row) as num_ta_calls
from VESPA_TA_CALLERS_201307_SCORED
where vespa = 0
group by segment,ta_propensity
) t
where num_hhs%100 = 0 -- sample for each 10 HHs added
order by 1,2
