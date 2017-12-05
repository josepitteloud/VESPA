-- Project Vespa: Panel Management Report - graph of scaling weights, summarised into percentiles
select *
from vespa_PanMan_08_ordered_weightings
order by weighting_percentile
;
