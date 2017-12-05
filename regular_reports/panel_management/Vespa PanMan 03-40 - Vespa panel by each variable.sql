-- Project Vespa: Panel Management Report Output - summary of all Vespa single variable stuff for variables used in scaling
select
    variable_value
    ,Sky_Base_Households
    ,Panel_Households
    ,Acceptable_Households
    ,Unreliable_Households
    ,Zero_reporting_Households
    ,Recently_enabled_households
    ,Good_Household_Index
from vespa_analysts.Vespa_PanMan_all_aggregated_results
where panel = 'DP' and scaling_or_not = 1
order by
-- explicit ordering for the variables so they fit into the existing template:
case aggregation_variable
    when 'UNIVERSE' then 1
    when 'REGION'   then 2
    when 'HHCOMP'   then 3
    when 'TENURE'   then 4
    when 'PACKAGE'  then 5
    when 'BOXTYPE'  then 6
END,
-- Explicit ordering for some variable types: nope, specifics were only value segments
variable_value
;
