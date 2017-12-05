/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** average viewing time for a show metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
** Minute of show for which the impacts are required.
**
** The average viewing time for a show metric calculates the average amount of minutes watched of
** of specific showa specific channel(s) at specified time(s). To find the error associated with
** this value we need to find the sample size of each strata in order to obtain the within-strata variance.
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

     declare @avviewtime        real
     declare @avseviewtime      real
     declare @SEweights         real
     declare @population_weight real

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201304'
     set     @programme_name    = 'Game Of Thrones'
     set     @broadcast_start   = '2013-04-01 20:00:00'
     set     @broadcast_end     = '2013-04-01 21:15:00'

--Variables calculated automatically from above values
     set     @scaling_date      = date(@broadcast_start)
     select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date
commit

-- Run procedure to create tables containing all viewing events when @programme_name is on
exec MEERR_prog_table @viewing_table, @programme_name, @broadcast_start, @broadcast_end

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_popn_sample @scaling_date, @profiling_date

--Find the minute that viewers started watching and the minute they finished watching
--If the started or stopped watching more than thiry seconds into the minute add one
alter table   MEERR_prog_viewing_table
        add   (minute_start             int
              ,minute_end               int
              ,minutes_watched          int
              ,scaling_segment_id       int
              ,weighting                double)
update        MEERR_prog_viewing_table
        set   minute_start = DATEDIFF(minute, @broadcast_start, instance_start_date_time_utc)
                             + (case when second(instance_start_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_prog_viewing_table
        set   minute_end = DATEDIFF(minute, @broadcast_start, instance_end_date_time_utc)
                           + (case when second(instance_end_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_prog_viewing_table
        set   minutes_watched = minute_end - minute_start
update        MEERR_prog_viewing_table a
        set   a.scaling_segment_id = b.scaling_segment_id
       from   vespa_analysts.SC2_Sky_base_segment_snapshots b
      where   a.account_number = b.account_number
        and   profiling_date = @profiling_date
update        MEERR_prog_viewing_table a
        set   a.weighting = b.weighting
       from   vespa_analysts.SC2_Weightings b
      where   a.scaling_segment_id = b.scaling_segment_id
        and   scaling_day = @scaling_date
commit

--Remove rows where scaling_segment_id is null
delete MEERR_prog_viewing_table
        where scaling_segment_id is null
commit

/*
# Average viewing time
*/

IF object_id('MEERR_strata_viewing') IS NOT NULL DROP TABLE MEERR_strata_viewing
select scaling_segment_id, max(weighting) as weighting, count(distinct subscriber_id) as viewers, sum(minutes_watched) as total_viewing
        into MEERR_strata_viewing
        from MEERR_prog_viewing_table
       where minutes_watched is not null
    group by scaling_segment_id

-- Remove rows where total_viewing  = 0
delete MEERR_strata_viewing
       where total_viewing = 0
commit

ALTER TABLE     MEERR_strata_viewing
        ADD     (sample_size            int
                ,population_size        int
                ,strata_prop            double
                ,strata_variance        double)
UPDATE          MEERR_strata_viewing a
        SET     a.sample_size = b.sample_size
       FROM     MEERR_strata_sample_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_strata_viewing a
        SET     a.population_size = b.population_size
       FROM     MEERR_strata_popn_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_strata_viewing
        SET     strata_prop = 1.0*viewers / sample_size
UPDATE          MEERR_strata_viewing
        SET     strata_variance = sample_size * strata_prop * (1.0 - strata_prop)
commit

--Need to find weighted total viewing time and weighted audience size
set @population_weight = (SELECT SUM(viewers * weighting) FROM MEERR_strata_viewing)

set @avviewtime = (
                select    sum(weighting*total_viewing)/@population_weight
                     from MEERR_strata_viewing)

set @avseviewtime = (
                SELECT    SQRT(SUM((1.0*(population_size - sample_size)/(population_size - 1))
                           *POWER(weighting/@population_weight, 2)
                           *strata_variance/sample_size))
                     FROM MEERR_strata_viewing
                    WHERE population_size > 1 AND viewers > 0)

SET     @SEweights =
                (SELECT SQRT(SUM(total_viewing*var_weights*total_viewing))/@population_weight
                   FROM MEERR_strata_viewing calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1)

select @avviewtime, @avseviewtime, @SEweights
select @avviewtime, 1.96*(@avseviewtime + @SEweights)
commit
end
