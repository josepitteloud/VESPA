-- Project Vespa: Dialback Report Output 15 for BB: count of boxes confirmed activated at start of 7 day period
select count(1)
from vespa_analysts.vespa_Dialback_box_listing_BB
where confirmed_activation_7d = 1
;
