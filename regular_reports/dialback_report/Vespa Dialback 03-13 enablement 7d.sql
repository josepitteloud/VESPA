-- Project Vespa: Dialback Report Output 13: enablement count for boxes enabled at start of 7 day period
select count(1)
from vespa_analysts.vespa_Dialback_box_listing
where enabled_7d = 1
;
