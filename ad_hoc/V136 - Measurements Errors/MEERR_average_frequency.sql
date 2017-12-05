/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** average frequency metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
** Minutes of show for which the average frequency metric is required.
**
** The average frequency metric calculates the average number of times that 'reached' viewers
** have seen a series of events; 'reached' viewers are those that have seen at least one of the
** events. Due to this metric only being required for reached viewers we have to use a censored
** poisson distribution as the value zero is not allowed. The mean and standard deviation of
** this distribution form the basis of the SQL seen at the end of this file.
**
** An event is a pre-defined period of three consecutive minutes; for this example we have picked
** the 15th, 45th and 75th minute of the show as the start of the event.
**
** The variance of the weights are assumed to be held in the table
** MEERR_jackknife_weight_vars
**
******************************************************************************/

begin

     declare @viewing_table     varchar(60)
     declare @programme_name    varchar(40)
     declare @broadcast_start   datetime
     declare @broadcast_end     datetime
     declare @scaling_date      date
     declare @profiling_date    date
     declare @avfreq            double
     declare @SEavfreq          double
     declare @SEweights         double
     declare @population_size   int
     declare @begin_min1        int
     declare @begin_min2        int
     declare @begin_min3        int
     declare @end_min1          int
     declare @end_min2          int
     declare @end_min3          int
     declare @no_of_events      int
     declare @weightedviewers   double

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201306'
     set     @programme_name    = 'Britains Got Talent'
     set     @broadcast_start   = '2013-06-01 18:00:00.000000'
     set     @broadcast_end     = '2013-06-01 19:30:00.000000'
-- Check reach figures after 15, 45 and 75 minutes
     set     @begin_min1         = 15
     set     @begin_min2         = 45
     set     @begin_min3         = 75
     set     @no_of_events       = 3

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

select           @population_size = (SELECT SUM(population_size) from MEERR_strata_popn_size)
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

-- Create temp table containing subscriber_id, their scaling_segment_id and how many times they've watched
-- the times of interest
if object_id('#temp_freq_count') is not null drop table #temp_freq_count
SELECT subscriber_id, scaling_segment_id, MAX(
       CASE WHEN minute_start <= @begin_min1 AND minute_end >= @end_min1 THEN 1 ELSE 0 END +
       CASE WHEN minute_start <= @begin_min2 AND minute_end >= @end_min2 THEN 1 ELSE 0 END +
       CASE WHEN minute_start <= @begin_min3 AND minute_end >= @end_min3 THEN 1 ELSE 0 END) AS Frequency
    into #temp_freq_count
    FROM MEERR_all_viewing_table
    GROUP BY subscriber_id, scaling_segment_id

-- Remove those who haven't watched, or where frequency is greater than no. of occurences being looked at
delete #temp_freq_count
    WHERE Frequency = 1 and Frequency <= @no_of_events

-- Create table which contains the number of people in each scaling segment id who watched at least
-- one of the particular broadcasts

--'MAX' in frequency case to ensure subscriber_id's are distinct and Frequency doesn't count
--two separate incidents. Also set so that Frequency doesn't get counted more than three time, as can happen
-- as some events seme to overlap. If looking at four moments in a show's broadcast, need to increase this to four.
if object_id('MEERR_strata_freq') is not null drop table MEERR_strata_freq
SELECT scaling_segment_id, COUNT(Frequency) AS reachedviewers, SUM(Frequency) AS totalviews
        INTO MEERR_strata_freq
        FROM #temp_freq_count
        GROUP BY scaling_segment_id
commit

-- Update MEERR_strata_reach to include population size, sample size and weights
alter table   MEERR_strata_freq
        add   (population_size       int
              ,sample_size           int
              ,weighting             double
              ,strata_prop           double
              ,strata_variance       double)
update        MEERR_strata_freq a
        set   a.population_size = b.population_size
       from   MEERR_strata_popn_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_freq a
        set   a.sample_size = b.sample_size
       from   MEERR_strata_sample_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_freq a
        set   a.weighting = b.weighting
       from   vespa_analysts.SC2_Weightings b
      where   a.scaling_segment_id = b.scaling_segment_id
        and   scaling_day = @scaling_date
--Again have to stop (reached)viewers > sample_size
update        MEERR_strata_freq a
        set   totalviews =
                CASE WHEN reachedviewers > sample_size THEN sample_size else reachedviewers end
update        MEERR_strata_freq a
        set   totalviews =
                CASE WHEN totalviews > (@no_of_events*sample_size) THEN (@no_of_events*sample_size) else totalviews end
update        MEERR_strata_calculations a
        SET   strata_prop = 1.0*viewers / sample_size
update        MEERR_strata_calculations a
        SET   strata_variance = strata_prop * (1.0 - strata_prop)
commit

--For each scaling group find the no. of viewers who saw the event only once
if object_id('#temp_once_viewed_strata') is not null drop table #temp_once_viewed_strata
SELECT   scaling_segment_id
        ,COUNT(Frequency) AS onceviewed
        ,CAST(NULL AS double) AS lambda
        ,CAST(NULL AS double) AS strata_var
        INTO #temp_once_viewed_strata
        FROM #temp_freq_count
        WHERE Frequency = 1
        GROUP BY scaling_segment_id

--Do a left outer join on above two tables, and join onto a third table to get population size
if object_id('#temp_freq_strata') is not null drop table #temp_freq_strata
SELECT              f1.*, onceviewed, lambda, strata_var
        INTO        #temp_freq_strata
        FROM        MEERR_strata_freq          f1
 LEFT OUTER JOIN    #temp_once_viewed_strata   o1
         ON         f1.scaling_segment_id = o1.scaling_segment_id
commit

UPDATE #temp_freq_strata
        SET lambda = (1.0*totalviews)/(1.0*reachedviewers)*(1.0 - 1.0*COALESCE(onceviewed, 0.)/(1.0*totalviews))
--Set strata_var equal to zero for when lambda equals zero
UPDATE #temp_freq_strata SET strata_var =
(
    CASE WHEN lambda = 0 THEN 0
    ELSE 1.0*(lambda*POWER(1 - EXP(-1.0*lambda),-2))/reachedviewers
    END
)

SELECT          @avfreq = (
                        SELECT SUM(1.0*totalviews/reachedviewers*weighting)/SUM(weighting)
                                FROM MEERR_strata_freq)

SELECT          @weightedviewers = (
                SELECT SUM(weighting*reachedviewers)
                        FROM #temp_freq_strata)

SELECT          @SEavfreq = (
                    SELECT SQRT(SUM((1.0*(population_size - sample_size)/(population_size - 1))
                                *weighting*weighting*strata_var)
                                /(@weightedviewers*@weightedviewers)
                                )
                        FROM #temp_freq_strata
                        WHERE reachedviewers > 1)

SELECT @SEweights = (
                SELECT SQRT(SUM(totalviews*var_weights*totalviews))/(@weightedviewers)
                  FROM #temp_freq_strata           freq
            INNER JOIN MEERR_jackknife_weight_vars vars
                    ON freq.scaling_segment_id = vars.scaling_segment_id)

SELECT @avfreq, @SEavfreq, @SEweights
SELECT @avfreq, 1.96*(@SEavfreq+@SEweights)
commit
end
