-- Project Vespa: Dialback Report Output 12: enablement count for boxes enabled at start of 30 day period
select count(1)
from vespa_analysts.vespa_Dialback_box_listing
where enabled_30d = 1
;
