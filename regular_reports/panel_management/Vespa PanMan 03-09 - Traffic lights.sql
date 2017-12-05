-- Project Vespa: Panel Management Report - traffic lights, showing balance of panel over each single variable
select
    variable_name
    ,sum(case when panel = 'DP' then imbalance_rating else 0 end) 	as DailyPanel_imbalance
    ,sum(case when panel = 'AP'  then imbalance_rating else 0 end)	as AlternatePanel_imbalance
from vespa_analysts.vespa_PanMan_09_traffic_lights
group by variable_name
order by min(sequencer)
;
