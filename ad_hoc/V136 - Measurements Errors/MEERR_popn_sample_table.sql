/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script creates are MEERR_strata_popn_size and MEERR_strata_sample_size
** which contain the strata sample sizes and strata population sizes respectively.
**
******************************************************************************/
-- drop procedure MEERR_popn_sample
create procedure
        MEERR_popn_sample
        (
             @scaling_date      date,
             @profiling_date    date
        )
as begin

--Population sizes
--Create table which contains the population size for each strata
--Note profiling_date which is the last date before our date of interest (8th Dec)
--Need to check that sum(expected_boxes) will give us population size
if               object_id('MEERR_strata_popn_size') is not null drop table MEERR_strata_popn_size
SELECT           scaling_segment_id
                ,SUM(expected_boxes) AS population_size
        INTO     MEERR_strata_popn_size
        FROM     vespa_analysts.SC2_Sky_base_segment_snapshots
       WHERE     profiling_date = @profiling_date
    GROUP BY     scaling_segment_id
commit

--Create table which contains sample size for each strata, e.g. the number within each strata in the vespa panel
--Take account numbers from vespa_analysts.SC2_intervals. Now we need to find subscriber_id to get sample size.
--Within sub-query create table joing the alt_panel_date with single box view to link account numbers and subscriber ids
--Join these two table to get a count of subscriber_id's within each scling segment and hence sample size.
if            object_id('MEERR_strata_sample_size') is not null drop table MEERR_strata_sample_size
SELECT           scaling_segment_id
                ,COUNT(distinct subscriber_id) AS sample_size
        INTO    MEERR_strata_sample_size
        from    vespa_analysts.SC2_intervals intr
       where reporting_starts <= @scaling_date
         and reporting_ends   >= @scaling_date
    group by scaling_segment_id
commit

end
