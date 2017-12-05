-- Project Vespa: Panel Management Report: Sample of poorly represented segments
select top 30
    scaling_segment_name || ' - ' || non_scaling_segment_name as segment_name
    ,Sky_Base_Households
    ,panel_households--,Vespa_Households
    ,Acceptably_reliable_households--,Reliable_Vespa_Live_Panel_Households
    ,0 as Somewhat_reliable_Vespa_Live_Panel_Households--,Somewhat_reliable_Vespa_Live_Panel_Households
    ,Unreliable_households--,Unreliable_Vespa_Live_Panel_Households
    ,Zero_reporting_households--,Zero_reporting_Vespa_Live_Panel_Households
    ,Recently_enabled_households--,Recently_activated_Vespa_Live_households
    ,Acceptably_reporting_index--,Good_HH_Vespa_Live_Index
from Vespa_PanMan_Scaling_Segment_Profiling
where Acceptably_reporting_index is not null--Good_HH_Vespa_Live_Index is not null
order by Acceptably_reporting_index, Sky_Base_Households desc--Good_HH_Vespa_Live_Index, Sky_Base_Households desc
;
