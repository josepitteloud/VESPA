-- Sky View panel: Dialback Report Output 14: count of boxes confirmed activated at start of 30 day period
select count(1)
from vespa_analysts.sky_view_dialback_box_listing
-- where confirmed_activation_30d = 1
;
-- We've just got one statis source, everything is considered "confirmed"
