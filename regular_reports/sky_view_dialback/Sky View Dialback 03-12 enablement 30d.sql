-- Sky View panel: Dialback Report Output 12: enablement count for boxes enabled at start of 30 day period
select count(1)
from vespa_analysts.vespa_single_box_view
where is_Sky_View_candidate = 1
;
-- We don't profile all the Sky View boxes, only the selected ones. So this list of
-- enabled boxes we pull straight from the Single Box View, then this total never gets
-- used any other time in the Sky View reports, everything happens with those that are
-- selected for data return.
