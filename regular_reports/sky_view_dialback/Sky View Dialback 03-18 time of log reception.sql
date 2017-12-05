-- Sky View panel: Dialback Report Output 18: time when logs are received
select *
from vespa_analysts.sky_view_Dialback_18_time_logs_sent
order by box_rank, box_type, premiums, log_date, time_of_day
;
