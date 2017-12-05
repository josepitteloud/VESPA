-- Project Vespa: Dialback Report Output 16: profiling of boxes that didn;t dial back all month
select *
from vespa_Dialback_16_non_reporting_boxes
order by PS_flag, Box_Type, Box_has_anytime_plus, PVR
;
