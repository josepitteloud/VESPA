/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** impacts for a break metric.
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
** The impacts for a break metric calculates the number of households viewing a particular
** viewing a particular slot on a specific channel(s) at specified time(s). The metric is additive,
** so that the total impacts is the impact for the first slot plus the impact for the second
** slot plus etc, etc. To find the error associated with this value we need to find the sample
** size of each strata in order to obtain the within-strata variance.
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

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201304'
     set     @programme_name    = 'Game Of Thrones'
     set     @broadcast_start   = '2013-04-01 20:00:00'
     set     @broadcast_end     = '2013-04-01 21:15:00'

-- #Break times were calculated as follows
-- 01/04/2013 21:12:34     - 01/04/2013 21:15:44
-- 01/04/2013 21:27:32     - 01/04/2013 21:30:42
-- 01/04/2013 21:45:08     - 01/04/2013 21:48:18
-- 01/04/2013 21:57:44     - 01/04/2013 22:00:14
-- 01/04/2013 22:09:53     - 01/04/2013 22:13:23
-- #Going to assume that the break that starts at 22:09:53 is after the show has finished, so will ignore.
-- These came from the following code
-- declare @programme_name2   varchar(40)
-- set     @programme_name2   = UPPER(@programme_name) + '%'
-- IF object_id('#temp_spot_data') IS NOT NULL DROP TABLE #temp_spot_data
-- select *
--         into #temp_spot_data
--         from neighbom.BARB_MASTER_SPOT_DATA
--         where BARB_date_of_transmission = date(@broadcast_start)
--            and UPPER(Preceding_Programme_Name) like @programme_name2
-- commit

     --Declare the minutes relating to the impacts of interest
     --In this example they are the first minute of each break
     DECLARE         @imp_min1 int
     DECLARE         @imp_min2 int
     DECLARE         @imp_min3 int
     DECLARE         @imp_min4 int
     SET             @imp_min1 = 12
     SET             @imp_min2 = 27
     SET             @imp_min3 = 45
     SET             @imp_min4 = 57

     --variable relating to the metric for each break
     DECLARE         @imp1 int
     DECLARE         @imp2 int
     DECLARE         @imp3 int
     DECLARE         @imp4 int
     DECLARE         @sdimp1 int
     DECLARE         @sdimp2 int
     DECLARE         @sdimp3 int
     DECLARE         @sdimp4 int
     DECLARE         @sew1 int
     DECLARE         @sew2 int
     DECLARE         @sew3 int
     DECLARE         @sew4 int

     declare         @impacts  int
     declare         @sdimpact real
     declare         @SEweights real

--Variables calculated automatically from above values
     set             @scaling_date      = date(@broadcast_start)
     select          @profiling_date    = max(profiling_date)
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
              ,scaling_segment_id       int
              ,weighting                double)
update        MEERR_prog_viewing_table
        set   minute_start = DATEDIFF(minute, @broadcast_start, instance_start_date_time_utc)
                             + (case when second(instance_start_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_prog_viewing_table
        set   minute_end = DATEDIFF(minute, @broadcast_start, instance_end_date_time_utc)
                           + (case when second(instance_end_date_time_utc) > 30 then 1 else 0 end)
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

--Remove rows where scaling_segment_id is null
delete MEERR_prog_viewing_table
        where scaling_segment_id is null
commit

/*
#Find how many people in each strata watched the first spot of each break of @programme_name
#Join this to table with sample and population sizes
*/
IF object_id('MEERR_prog_strata_impacts') IS NOT NULL DROP TABLE MEERR_prog_strata_impacts
CREATE TABLE MEERR_prog_strata_impacts (
      impact_no          int
     ,scaling_segment_id int
     ,viewers             int
)

--Need to do this separately as subscribers can be counted multiple times if they watch more than one impact
INSERT INTO MEERR_prog_strata_impacts (impact_no, scaling_segment_id, viewers)
        SELECT        1, scaling_segment_id, COUNT(DISTINCT(subscriber_id))
                FROM  MEERR_prog_viewing_table
                WHERE (minute_end > @imp_min1 AND minute_start <= @imp_min1)
                GROUP BY scaling_segment_id
INSERT INTO MEERR_prog_strata_impacts (impact_no, scaling_segment_id, viewers)
        SELECT        2, scaling_segment_id, COUNT(DISTINCT(subscriber_id))
                FROM  MEERR_prog_viewing_table
                WHERE (minute_end > @imp_min2 AND minute_start <= @imp_min2)
                GROUP BY scaling_segment_id
INSERT INTO MEERR_prog_strata_impacts (impact_no, scaling_segment_id, viewers)
        SELECT        3, scaling_segment_id, COUNT(DISTINCT(subscriber_id))
                FROM  MEERR_prog_viewing_table
                WHERE (minute_end > @imp_min3 AND minute_start <= @imp_min3)
                GROUP BY scaling_segment_id
INSERT INTO MEERR_prog_strata_impacts (impact_no, scaling_segment_id, viewers)
        SELECT        4, scaling_segment_id, COUNT(DISTINCT(subscriber_id))
                FROM  MEERR_prog_viewing_table
                WHERE (minute_end > @imp_min4 AND minute_start <= @imp_min4)
                GROUP BY scaling_segment_id
ALTER TABLE     MEERR_prog_strata_impacts
        ADD     (sample_size            int
                ,population_size        int
                ,weighting              double
                ,strata_prop            double
                ,strata_variance        double)
UPDATE          MEERR_prog_strata_impacts a
        SET     a.sample_size = b.sample_size
       FROM     MEERR_strata_sample_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_prog_strata_impacts a
        SET     a.population_size = b.population_size
       FROM     MEERR_strata_popn_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_prog_strata_impacts a
        set     a.weighting = b.weighting
       from     vespa_analysts.SC2_Weightings b
      where     a.scaling_segment_id = b.scaling_segment_id
        and     scaling_day = @scaling_date
UPDATE          MEERR_prog_strata_impacts
        SET     strata_prop = 1.0*viewers / sample_size
UPDATE          MEERR_prog_strata_impacts
        SET     strata_variance = strata_prop * (1.0 - strata_prop)
commit

/*
# Impacts
*/
SET             @imp1 =
                (SELECT SUM(1.0*viewers*weighting)
                   FROM MEERR_prog_strata_impacts
                  WHERE impact_no = 1)
SET             @imp2 =
                (SELECT SUM(1.0*viewers*weighting)
                   FROM MEERR_prog_strata_impacts
                  WHERE impact_no = 2)
SET             @imp3 =
                (SELECT SUM(1.0*viewers*weighting)
                   FROM MEERR_prog_strata_impacts
                  WHERE impact_no = 3)
SET             @imp4 =
                (SELECT SUM(1.0*viewers*weighting)
                   FROM MEERR_prog_strata_impacts
                  WHERE impact_no = 4)

SET     @sdimp1 =
                (SELECT SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size))
                   FROM MEERR_prog_strata_impacts
                  WHERE population_size > 1 AND viewers > 0 AND impact_no = 1)
SET     @sdimp2 =
                (SELECT SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size))
                   FROM MEERR_prog_strata_impacts
                    WHERE population_size > 1 AND viewers > 0 AND impact_no = 2)
SET     @sdimp3 =
                (SELECT SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size))
                   FROM MEERR_prog_strata_impacts
                    WHERE population_size > 1 AND viewers > 0 AND impact_no = 3)
SET     @sdimp4 =
                (SELECT SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size))
                   FROM MEERR_prog_strata_impacts
                    WHERE population_size > 1 AND viewers > 0 AND impact_no = 4)

SET     @sew1 =
                (SELECT SUM(viewers*var_weights*viewers)
                   FROM MEERR_prog_strata_impacts calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND viewers > 0 AND impact_no = 1)
SET     @sew2 =
                (SELECT SUM(viewers*var_weights*viewers)
                   FROM MEERR_prog_strata_impacts calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND viewers > 0 AND impact_no = 2)
SET     @sew3 =
                (SELECT SUM(viewers*var_weights*viewers)
                   FROM MEERR_prog_strata_impacts calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND viewers > 0 AND impact_no = 3)
SET     @sew4 =
                (SELECT SUM(viewers*var_weights*viewers)
                   FROM MEERR_prog_strata_impacts calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND viewers > 0 AND impact_no = 4)

set     @impacts  = @imp1 + @imp2 + @imp3 + @imp4
set     @sdimpact = sqrt(@sdimp1 + @sdimp2 + @sdimp3 + @sdimp4)
set     @SEweights = sqrt(@sew1 + @sew2 + @sew3 + @sew4)

select  @impacts, 1.96*(@sdimpact+@SEweights)

commit
end

