-- Project Vespa: Dialback Report Output 12 for BB: enablement count for boxes enabled at start of 30 day period
select count(1)
from vespa_analysts.vespa_Dialback_box_listing_BB
where enabled_30d = 1
;

