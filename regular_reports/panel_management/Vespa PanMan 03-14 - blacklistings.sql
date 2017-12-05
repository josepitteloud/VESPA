-- Blacklisting boxes: these are the guys that haven't returned any data over
-- the whole household in the last 30 days, they may as well dissappear.
select account_number
from vespa_analysts.vespa_single_box_view
group by account_number
having max(coalesce(reporting_quality, 0.1)) = 0
;
-- NULL means the box is recent, we want to tolerate those.

-- Thing is, there are about 170k boxes here. Do we want that as a frigging huge
-- extract, or are we going to leave it in a table and then GRANT permissions to
-- someone? Might be easier that way around.
