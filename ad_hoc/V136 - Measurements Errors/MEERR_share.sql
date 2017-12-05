/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** share metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
** Minute of show for which the share metric is required.
**
** The share metric calculates the percentage of set top boxes switched to a particular
** channel at a specified time; it is written as a proportion of the entire Sky base.
** To find the error assocaited with this value we need to find the total no. of people
** who are watching live TV at the same time as the show of interest is on air.
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
     declare @time_attributed   datetime
     declare @share             double
     declare @SEshare           double
     declare @SEweights         double
     declare @population_weight double

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201306'
     set     @programme_name    = 'Britains Got Talent'
     set     @broadcast_start   = '2013-06-01 18:00:00.000000'
     set     @broadcast_end     = '2013-06-01 19:30:00.000000'
-- Check viewing figures after one hour
     set     @time_attributed   = '2013-06-01 19:00:00.000000'

--Variables calculated automatically from above values
     set     @scaling_date      = date(@broadcast_start)
     select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date
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

--Create table containing viewers by scaling_segment_id
if object_id('MEERR_all_scal_audience') is not null drop table MEERR_all_scal_audience
SELECT   scaling_segment_id
        ,COUNT(DISTINCT subscriber_id) as all_viewers
        INTO MEERR_all_scal_audience
        FROM MEERR_all_viewing_table
    group by scaling_segment_id
commit

--Create table containing viewers of programme of interest by scaling_segment_id
if object_id('MEERR_prog_scal_audience') is not null drop table MEERR_prog_scal_audience
SELECT   scaling_segment_id
        ,COUNT(DISTINCT subscriber_id) as viewers
        INTO MEERR_prog_scal_audience
        FROM MEERR_all_viewing_table
       WHERE programme_name = @programme_name
    group by scaling_segment_id
commit

--Create table holding scaling segment id's, viewers of TV, viewers of programme, population size, samplesize and weights
if object_id('MEERR_strata_calculations') is not null drop table MEERR_strata_calculations
SELECT        a.scaling_segment_id, all_viewers, COALESCE(viewers, 0) as viewers
        into  MEERR_strata_calculations
        from  MEERR_all_scal_audience a
   left join  MEERR_prog_scal_audience b
          on  a.scaling_segment_id = b.scaling_segment_id
alter table   MEERR_strata_calculations
        add   (population_size       int
              ,sample_size           int
              ,weighting             double
              ,strata_prop           double
              ,strata_variance       double)
update        MEERR_strata_calculations a
        set   a.population_size = b.population_size
       from   MEERR_strata_popn_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_calculations a
        set   a.sample_size = b.sample_size
--        from   MEERR_strata_sample_size b
--       where   a.scaling_segment_id = b.scaling_segment_id
-- update        MEERR_strata_calculations a
--         set   a.weighting = b.weighting
--        from   vespa_analysts.SC2_Weightings b
--       where   a.scaling_segment_id = b.scaling_segment_id
--         and   scaling_day = @scaling_date
-- --Check to ensure that viewers are not greater than sample_size
-- update        MEERR_strata_calculations a
--         set   viewers =
--                 CASE WHEN viewers > sample_size THEN sample_size else viewers end
-- update        MEERR_strata_calculations a
--         SET   strata_prop = 1.0*viewers / sample_size
-- update        MEERR_strata_calculations a
--         SET   strata_variance = strata_prop * (1.0 - strata_prop)
-- 
-- commit

/*#######################################
--Share
#########################################*/
select  @population_weight = (SELECT SUM(all_viewers*weighting) from MEERR_strata_calculations)
SELECT  @share =
                (SELECT SUM(1.0*viewers*weighting)
                        /SUM(1.0*all_viewers*weighting)
                        FROM MEERR_strata_calculations)

--Need to calculate the strata variance for total mins attributed for each strata
--Note that we are using the strata variance from the proportions, this is because
--the formula for the stratified variance of the total is N^2 times the stratified
--variance for the mean using the within-strata variance for the mean. The commented
--out text shows the within-strata variance for the total.
SELECT  @SEshare = (
            SELECT SQRT(SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *POWER(1.0*(all_viewers*weighting)/@population_weight, 2)
                    *strata_variance/sample_size))
                    FROM  MEERR_strata_calculations
                    WHERE population_size > 1)

SELECT @SEweights = (
          SELECT SQRT(SUM(1.0*viewers*var_weights*viewers))/@population_weight --@population_size
            FROM MEERR_strata_calculations calc
      INNER JOIN MEERR_jackknife_weight_vars vars
              ON calc.scaling_segment_id = vars.scaling_segment_id
           WHERE population_size > 1 AND viewers > 0)

SELECT  @share, 1.96*(@SEshare+@SEweights)
commit
end

