/*

Comparison of new and current scaling weights

Lead: Claudio Lima

*/

-- Side by side comparison of weights
select 'Current' as Scaling_Version
        ,sum(vespa_accounts) as panel_size
        ,sum(weighting*vespa_accounts) as universe
        ,power(sum(weighting*vespa_accounts),2)/sum(power(weighting,2)*vespa_accounts) as effective_sample_size
        ,effective_sample_size/panel_size as relative_effective_sample_size 
        ,min(weighting) as min_weight
        ,sum(weighting*vespa_accounts)/panel_size as weighted_avg_weight
        ,avg(weighting) as avg_weight
        ,max(weighting) as max_weight
from vespa_analysts.SC2_weightings
where scaling_day = '2013-07-14'
and vespa_accounts > 0
union all
select 'New' as Scaling_Version
        ,sum(vespa_panel) as panel_size
        ,sum(segment_weight*vespa_panel) as universe
        ,power(sum(segment_weight*vespa_panel),2)/sum(power(segment_weight,2)*vespa_panel) as effective_sample_size
        ,effective_sample_size/panel_size as relative_effective_sample_size 
        ,min(segment_weight) as min_weight
        ,median()
        ,sum(segment_weight*vespa_panel)/panel_size as weighted_avg_weight
        ,avg(segment_weight) as avg_weight
        ,max(segment_weight) as max_weight
from glasera.V154_weighting_working_table
where vespa_panel > 0.1


-- Distribution of the weights
select coalesce(o.weight_group,n.weight_group) as weight_group
        ,o.num_segments as old_num_segments
        ,n.num_segments as new_num_segments
from(
select floor(weighting/10)*10 as weight_group,count(*) as num_segments
from vespa_analysts.SC2_weightings
where scaling_day = '2013-07-14'
group by floor(weighting/10)*10
) o
full join (
select floor(segment_weight/10)*10 as weight_group,count(*) as num_segments
from glasera.V154_weighting_working_table
group by floor(segment_weight/10)*10
) n
on o.weight_group = n.weight_group
order by 1

-- Look at segments with very high weight
select * from glasera.V154_weighting_working_table where segment_weight > 500 order by segment_weight desc

select top 100 * 
from glasera.V154_weighting_working_table 
order by abs(sum_of_weights-sky_base_accounts)/sky_base_accounts desc

select top 100 * 
from vespa_analysts.SC2_weightings 
where scaling_day = '2013-07-14' order by abs(sum_of_weights-sky_base_accounts)/sky_base_accounts desc

-- Look at the distribution of the difference between weights and sky base
select coalesce(o.base_weights_diff,n.base_weights_diff) as base_weights_diff
        ,o.num_segments as old_num_segments
        ,n.num_segments as new_num_segments
from (
select floor((sum_of_weights-sky_base_accounts)/sky_base_accounts) as base_weights_diff,count(*) as num_segments
from vespa_analysts.SC2_weightings
where scaling_day = '2013-07-14'
group by floor((sum_of_weights-sky_base_accounts)/sky_base_accounts)
) o
full join (
select floor((sum_of_weights-sky_base_accounts)/sky_base_accounts) as base_weights_diff,count(*) as num_segments 
from glasera.V154_weighting_working_table 
group by floor((sum_of_weights-sky_base_accounts)/sky_base_accounts)
) n
on o.base_weights_diff = n.base_weights_diff
order by 1


-- Look at the distribution of the ratio of vespa over base accounts
select coalesce(o.vespa_coverage,n.vespa_coverage) as vespa_coverage
        ,o.num_segments as old_num_segments
        ,n.num_segments as new_num_segments
from (
select round(vespa_accounts*1.0/sky_base_accounts,2) as vespa_coverage,count(*) as num_segments
from vespa_analysts.SC2_weightings
where scaling_day = '2013-07-14'
and vespa_accounts > 0
group by round(vespa_accounts*1.0/sky_base_accounts,2)
) o
full join (
select round(vespa_panel*1.0/sky_base_accounts,2) as vespa_coverage,count(*) as num_segments
from glasera.V154_weighting_working_table 
where vespa_panel > 0.1
group by round(vespa_panel*1.0/sky_base_accounts,2)
) n
on o.vespa_coverage = n.vespa_coverage
order by 1

select top 1000 *
from glasera.V154_weighting_working_table w
left join glasera.V154_segment_lookup_v1_1 s
on w.scaling_segment_id = s.updated_scaling_segment_id
where vespa_panel > 0.1
order by round(vespa_panel*1.0/sky_base_accounts,2), sky_base_accounts desc