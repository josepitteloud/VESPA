/*

Looking at history of capping thresholds

*/

select top 100 *
from vespa_analysts.CP2_QA_viewing_control_cap_distrib

-- Get proportion of thresholds at 20 and 120 min
select base.build_date
        ,base.Total_Capped_Instances
        ,t20.cap_instances*1.0/base.Total_Capped_Instances as Percentage_20min
        ,t120.cap_instances*1.0/base.Total_Capped_Instances as Percentage_120min
from (
select build_date
        ,sum(cap_instances) as Total_Capped_Instances
from vespa_analysts.CP2_QA_viewing_control_cap_distrib
group by build_date
) base
left join
(
select build_date
        ,cap_instances
from vespa_analysts.CP2_QA_viewing_control_cap_distrib
where max_dur_mins = 20
) t20
on base.build_date = t20.build_date
left join
(
select build_date
        ,cap_instances
from vespa_analysts.CP2_QA_viewing_control_cap_distrib
where max_dur_mins = 120
) t120
on base.build_date = t120.build_date
order by base.build_date


select * from vespa_analysts.CP2_calculated_viewing_caps

-------------------------------

select *,case 
            when min_duration > 7200 then 1 
            when min_duration < 1200 then -1
            else 0 
        end as Outside_Limits
from igonorp.Current_Threshold_Seg1

select case 
            when min_duration > 7200 then 1 
            when min_duration < 1200 then -1
            else 0 
        end as Outside_Limits
        ,count(*)
from igonorp.Current_Threshold_Seg2
group by case 
            when min_duration > 7200 then 1 
            when min_duration < 1200 then -1
            else 0 
        end 

select case 
            when min_duration > 7200 then 1 
            when min_duration < 1200 then -1
            else 0 
        end as Outside_Limits
        ,count(*)
from igonorp.Current_Threshold_Seg3
group by case 
            when min_duration > 7200 then 1 
            when min_duration < 1200 then -1
            else 0 
        end 

