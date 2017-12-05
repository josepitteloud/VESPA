-- Project Vespa: Panel Management Report - four weeks worth of recent performance metrics
select top 24 metric_date, sky_base_coverage, reliability_rating, households_reliably_reporting
from vespa_analysts.Vespa_PanMan_Historic_Panel_Metrics
order by metric_date desc
;
