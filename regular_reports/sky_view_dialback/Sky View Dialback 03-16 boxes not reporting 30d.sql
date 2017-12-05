-- Sky View panel: Dialback Report Output 16: profiling of boxes that didn;t dial back all month
select *
from sky_view_dialback_16_non_reporting_boxes
order by PS_flag, Box_Type, Box_has_anytime_plus, PVR
;
