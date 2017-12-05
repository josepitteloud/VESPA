/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** reach metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
** Minutes of show for which the reach metric is required.
**
** The reach metric calculates the proportion of viewers who have seen a series of events; 'reached'
** viewers are those that have seen at least one of the events.
**
** An event is a pre-defined period of three consecutive minutes; for this example we have picked
** the 15th, 45th and 75th minute of the show as the start of the event.
a particular channel at a specified time. To find the error assocaited with this
** value we need to find the total no. of people who are watching live TV at the same
** time as the show of interest is on air.
**
** The variance of the weights are assumed to be held in the table
** MEERR_jackknife_weight_vars
**
******************************************************************************/

begin

     declare @viewing_table     varchar(60)
     declare @progamme_name     varchar(40)
     declare @broadcast_start   datetime
     declare @broadcast_end     datetime
     declare @scaling_date      date
     declare @profiling_date    date
     declare @time_attributed   datetime
     declare @reach             double
     declare @SEreach           double
     declare @SEweights         double
     declare @population_size   int
     declare @begin_min1        int
     declare @begin_min2        int
     declare @begin_min3        int
     declare @end_min1          int
     declare @end_min2          int
     declare @end_min3          int

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201306'
     set     @progamme_name     = 'Britains Got Talent'
     set     @broadcast_start   = '2013-06-01 18:00:00.000000'
     set     @broadcast_end     = '2013-06-01 19:30:00.000000'
-- Check reach figures after 15, 45 and 75 minutes
     set     @begin_min1         = 15
     set     @begin_min2         = 45
     set     @begin_min3         = 75

--Variables calculated automatically from above values
     set     @scaling_date       = date(@broadcast_start)
     select  @profiling_date     = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date
     set     @end_min1           = @begin_min1 + 3
     set     @end_min2           = @begin_min2 + 3
     set     @end_min3           = @begin_min3 + 3

commit

-- Run procedure to create tables containing all viewing events when @programme_name is on
exec MEERR_viewing_table @viewing_table, @broadcast_start, @broadcast_end

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_popn_sample @scaling_date, @profiling_date

--Link all_viewing_table with their scaling segment ids and RIM weights
alter table   MEERR_all_viewing_table
        add   (scaling_segment_id       int)
update        MEERR_all_viewing_table a
        set   a.scaling_segment_id = b.scaling_segment_id
       from   vespa_analysts.SC2_Sky_base_segment_snapshots b
      where   a.account_number = b.account_number
        and   profiling_date = @profiling_date
commit

--Create table holding scaling segment id's, viewers of programme, minute_start, minute_end
alter table   MEERR_all_viewing_table
        add   (minute_start          int
              ,minute_end            int)
update        MEERR_all_viewing_table a
        set   a.minute_start = DATEDIFF(minute, @broadcast_start, instance_start_date_time_utc)
                             + (case when second(instance_start_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_all_viewing_table a
        set   a.minute_end = DATEDIFF(minute, @broadcast_start, instance_end_date_time_utc)
                           + (case when second(instance_end_date_time_utc) > 30 then 1 else 0 end)
commit

-- Create table which contains the number of people in each scaling segment id who watched at least
-- one of the particular broadcasts

--'MAX' in frequency case to ensure subscriber_id's are distinct and Frequency doesn't count
--two separate incidents. See scaling_segment_id = 16385, where viewer doesn't watch TV in one
--going but two separate times. Without distinct this would count twice.
if object_id('MEERR_strata_reach') is not null drop table MEERR_strata_reach
SELECT scaling_segment_id, COUNT(Frequency) AS viewers
        INTO MEERR_strata_reach
        FROM (
            SELECT subscriber_id, scaling_segment_id, MAX(
                   CASE WHEN    ((minute_start <= @begin_min1 AND minute_end >= @end_min1)  OR
                                 (minute_start <= @begin_min2 AND minute_end >= @end_min2)  OR
                                 (minute_start <= @begin_min3 AND minute_end >= @end_min3)) THEN 1 ELSE 0 END)
                         AS Frequency
                   FROM MEERR_all_viewing_table
                   GROUP BY subscriber_id, scaling_segment_id) AS sub1
            WHERE Frequency = 1
        GROUP BY scaling_segment_id
commit

-- Update MEERR_strata_reach to include population size, sample size and weights
alter table   MEERR_strata_reach
        add   (population_size       int
              ,sample_size           int
              ,weighting             double
              ,strata_prop           double
              ,strata_variance       double)
update        MEERR_strata_reach a
        set   a.population_size = b.population_size
       from   MEERR_strata_popn_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_reach a
        set   a.sample_size = b.sample_size
       from   MEERR_strata_sample_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_reach a
        set   a.weighting = b.weighting
       from   vespa_analysts.SC2_Weightings b
      where   a.scaling_segment_id = b.scaling_segment_id
        and   scaling_day = @scaling_date
--Again have to stop viewers > sample_size
update        MEERR_strata_reach a
        set   viewers =
                CASE WHEN viewers > sample_size THEN sample_size else viewers end
update        MEERR_strata_reach a
        SET   strata_prop = 1.0*viewers / sample_size
update        MEERR_strata_reach a
        SET   strata_variance = strata_prop * (1.0 - strata_prop)
commit

select        @population_size = (SELECT SUM(population_size) from MEERR_strata_popn_size)

SELECT  @reach = (
                SELECT   SUM (viewers*weighting)/@population_size
                        FROM MEERR_strata_reach
                  )

SELECT  @SEreach = (
            SELECT SQRT(SUM(1.0*(population_size - sample_size)/(population_size - 1)
                             *POWER(weighting/@population_size, 2)
                    *strata_variance))
                    FROM MEERR_strata_reach
                    WHERE population_size > 1
            )

SELECT @SEweights = (
        SELECT       SQRT(SUM(1.0*viewers*var_weights*viewers))/@population_size
                FROM MEERR_strata_reach calc
          INNER JOIN MEERR_jackknife_weight_vars vars
                  ON calc.scaling_segment_id = vars.scaling_segment_id
               WHERE population_size > 1 AND viewers > 0)

SELECT @reach, 1.96*(@SEreach+@SEweights)
commit
end

