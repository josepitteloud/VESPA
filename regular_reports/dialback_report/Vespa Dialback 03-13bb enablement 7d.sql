-- Project Vespa: Dialback Report Output 13 for BB: enablement count for boxes enabled at start of 7 day period
select count(1)
from vespa_analysts.vespa_Dialback_box_listing_BB
where enabled_7d = 1
;
