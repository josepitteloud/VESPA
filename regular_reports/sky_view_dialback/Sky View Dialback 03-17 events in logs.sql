-- Sky View panel: Dialback Report Output 17: number of events in each log batch returned
select *
from vespa_analysts.sky_view_Dialback_17_events_per_log
order by box_rank, box_type, premiums, log_date, event_count_bracket
;

