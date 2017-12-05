/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** audience for a slot metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** Name of show
** datetime of start of broadcast
** datetime of end of broadcast
** Minute of slot for which the audience metric is required.
**
** The audience for a slot metric calculates the number of households viewing a particular
** channel(s) at a specified slot time. To find the error associated with this value we need
** to find the sample size of each strata in order to obtain the within-strata variance.
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
     declare @spot_attributed   int
     declare @SEspot_attributed double
     declare @SEweights         double
     declare @slot              int

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201304'
     set     @programme_name    = 'Game Of Thrones'
     set     @broadcast_start   = '2013-04-01 20:00:00'
     set     @broadcast_end     = '2013-04-01 21:15:00'
     SET     @slot              = 12

--Variables calculated automatically from above values
     set     @scaling_date      = date(@broadcast_start)
     select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date
     declare @programme_name2   varchar(40)
     set     @programme_name2   = UPPER(@programme_name) + '%'

commit

-- Run procedure to create tables containing all viewing events when @programme_name is on
exec MEERR_prog_table @viewing_table, @programme_name, @broadcast_start, @broadcast_end

-- Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_popn_sample @scaling_date, @profiling_date

/*
# Find scaling weights for the account in the MEERR_prog_viewing_table table
*/

--Find the minute that viewers started watching and the minute they finished watching
--If the started or stopped watching more than thiry seconds into the minute add one
alter table   MEERR_prog_viewing_table
        add   (minute_start             int
              ,minute_end               int
              ,total_mins               int
              ,scaling_segment_id       int)
update        MEERR_prog_viewing_table
        set   minute_start = DATEDIFF(minute, @broadcast_start, instance_start_date_time_utc)
                             + (case when second(instance_start_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_prog_viewing_table
        set   minute_end = DATEDIFF(minute, @broadcast_start, instance_end_date_time_utc)
                           + (case when second(instance_end_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_prog_viewing_table
        set   total_mins = minute_end - minute_start
update        MEERR_prog_viewing_table a
        set   a.scaling_segment_id = b.scaling_segment_id
       from   vespa_analysts.SC2_Sky_base_segment_snapshots b
      where   a.account_number = b.account_number
        and   profiling_date = @profiling_date

--Remove rows where scaling_segment_id is null
delete MEERR_prog_viewing_table
        where scaling_segment_id is null
commit

/*
# Audience for a slot
*/

/*
#Find how many people in each strata watched the first spot of the show @programme_name
#Join this to table with sample and population sizes
*/
IF object_id('MEERR_prog_strata_spot') IS NOT NULL DROP TABLE MEERR_prog_strata_spot
SELECT        scaling_segment_id, COUNT(DISTINCT(subscriber_id)) AS viewers
        INTO  MEERR_prog_strata_spot
        FROM  MEERR_prog_viewing_table
        WHERE minute_end > @slot
          AND minute_start <= @slot
        GROUP BY scaling_segment_id
commit

ALTER TABLE     MEERR_prog_strata_spot
        ADD     (sample_size            int
                ,population_size        int
                ,weighting              double
                ,strata_prop            double
                ,strata_variance        double)
UPDATE          MEERR_prog_strata_spot a
        SET     a.sample_size = b.sample_size
       FROM     MEERR_strata_sample_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_prog_strata_spot a
        SET     a.population_size = b.population_size
       FROM     MEERR_strata_popn_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_prog_strata_spot a
        SET     a.weighting = b.weighting
       FROM     vespa_analysts.SC2_Weightings b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
        and     scaling_day = @scaling_date
UPDATE          MEERR_prog_strata_spot
        SET     strata_prop = 1.0*viewers / sample_size
UPDATE          MEERR_prog_strata_spot
        SET     strata_variance = strata_prop * (1.0 - strata_prop)

SET     @spot_attributed =
                (SELECT SUM(1.0*viewers*weighting) FROM MEERR_prog_strata_spot)

SET     @SEspot_attributed =
                (SELECT SQRT(SUM(1.0*(population_size - sample_size)/(population_size - 1)
                    *(weighting*weighting*strata_variance/sample_size)))
                        FROM MEERR_prog_strata_spot
                    WHERE population_size > 1 AND viewers > 0)

SET     @SEweights =
                (SELECT SQRT(SUM(viewers*var_weights*viewers))
                   FROM MEERR_prog_strata_spot calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1)

SELECT  @spot_attributed, 1.96*(@SEspot_attributed+@SEweights)
COMMIT
end

