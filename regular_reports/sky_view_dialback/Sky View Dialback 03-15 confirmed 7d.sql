-- Sky View panel: Dialback Report Output 15: count of boxes confirmed activated at start of 7 day period
select count(1)
from vespa_analysts.sky_view_dialback_box_listing
-- where confirmed_activation_7d = 1
;
-- We've just got one statis source, everything is considered "confirmed". And there's also
-- no practical difference between the 30d and 7d enablement numbers for the same reason, but hey.
