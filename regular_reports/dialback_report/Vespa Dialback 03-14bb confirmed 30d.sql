-- Project Vespa: Dialback Report Output 14 for BB: count of boxes confirmed activated at start of 30 day period
select count(1)
from vespa_analysts.vespa_Dialback_box_listing_BB
where confirmed_activation_30d = 1
;
