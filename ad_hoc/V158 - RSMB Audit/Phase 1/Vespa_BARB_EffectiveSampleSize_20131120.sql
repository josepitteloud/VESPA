/*

Calculate Vespa and BARB effective sample size
in support of the RSMB audit review

Lead: Claudio Lima
Date: 21/11/2013

*/
select max(scaling_day)
from vespa_analysts.SC2_weightings

select max(adjusted_event_start_date_vespa)
from sk_prod.viq_viewing_data_scaling

-- Comparing effective sample size in vespa
-- VA vs VIQ weights
select viq.adjusted_event_start_date_vespa as scaling_day
        ,va.panel_size as va_panel_size
        ,va.effective_sample_size as va_effective_sample_size
        ,va.min_weight as va_min_weight
        ,va.avg_weight as va_avg_weight
        ,va.max_weight as va_max_weight
        ,viq.panel_size as viq_panel_size
        ,viq.effective_sample_size as viq_effective_sample_size
        ,viq.min_weight as viq_min_weight
        ,viq.avg_weight as viq_avg_weight
        ,viq.max_weight as viq_max_weight
        ,viq_panel_size*1.0/va_panel_size
from (
select scaling_day
        ,sum(vespa_accounts) as panel_size
        ,sum(weighting*vespa_accounts) as universe
        ,power(sum(weighting*vespa_accounts),2)/sum(power(weighting,2)*vespa_accounts) as effective_sample_size
        ,effective_sample_size/panel_size as relative_effective_sample_size 
        ,min(weighting) as min_weight
        ,avg(weighting) as avg_weight
        ,max(weighting) as max_weight
from vespa_analysts.SC2_weightings
where scaling_day >= '2013-03-01'
and vespa_accounts > 0
group by scaling_day
) va
right join
(
select adjusted_event_start_date_vespa
        ,count(*) as panel_size
        ,sum(calculated_scaling_weight) as universe
        ,power(sum(calculated_scaling_weight),2)/sum(power(calculated_scaling_weight,2)) as effective_sample_size
        ,effective_sample_size/panel_size as relative_effective_sample_size 
        ,min(calculated_scaling_weight) as min_weight
        ,avg(calculated_scaling_weight) as avg_weight
        ,max(calculated_scaling_weight) as max_weight
from sk_prod.viq_viewing_data_scaling
where adjusted_event_start_date_vespa >= '2013-03-01'
group by adjusted_event_start_date_vespa
) viq
on va.scaling_day = viq.adjusted_event_start_date_vespa
order by viq.adjusted_event_start_date_vespa
