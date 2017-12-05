-- Project Vespa: Panel Management Report Output - summary of all Alternate Panel 7 single variable stuff for variables not used in scaling
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
where panel = 'ALT7' and scaling_or_not = 0
order by
-- explicit ordering for the variables so they fit into the existing template:
case aggregation_variable
    when 'VALUESEG'         then 1
    when 'MOSAIC'           then 2
    when 'FINANCIALSTRAT'   then 3
    when 'ONNET'            then 4
    when 'SKYGO'            then 5
END,
-- Explicit ordering for some variable types:
case variable_value
    when 'Platinum'     then '1' -- Strings for comparable types to the variable value in the ELSE
    when 'Gold'         then '2'
    when 'Silver'       then '3'
    when 'Bronze'       then '4'
    when 'Copper'       then '5'
    when 'Unstable'     then '6'
    when 'Bedding In'   then '7'
    else variable_value
end
;
