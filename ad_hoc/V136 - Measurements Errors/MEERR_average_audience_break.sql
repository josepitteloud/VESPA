/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** average audience for a break metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** Name of show
** datetime of start of broadcast
** datetime of end of broadcast
** datetime of the start of the break
** datetime of the end of the break
**
** The average audience for a break metric calculates the weighted average number of households viewing
** a particular channel(s) during a break; the weights come about due to the break not always occupying a
** whole minute. To find the error associated with this value we need to find the sample size of each
** strata in order to obtain the within-strata variance.
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
     declare @slot_start        datetime
     declare @slot_end          datetime

-- Variables which find the audience on a minute-by-minute basis
     declare @aud1              real
     declare @aud2              real
     declare @aud3              real
     declare @aud4              real
     declare @sd1               real
     declare @sd2               real
     declare @sd3               real
     declare @sd4               real
     declare @sew1              real
     declare @sew2              real
     declare @sew3              real
     declare @sew4              real
     declare @seconds_first_minute int
     declare @seconds_last_minute int
     declare @break_length      real
     DECLARE @avaudbreak        double
     DECLARE @SEavaudbreak      double
     declare @SEweights         double

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201304'
     set     @programme_name    = 'Game Of Thrones'
     set     @broadcast_start   = '2013-04-01 20:00:00'
     set     @broadcast_end     = '2013-04-01 21:15:00'

     set     @slot_start        = '2013-04-01 21:12:34'
     set     @slot_end          = '2013-04-01 21:15:44'

-- #Break times were calculated as follows
-- 01/04/2013 21:12:34     - 01/04/2013 21:15:44
-- 01/04/2013 21:27:32     - 01/04/2013 21:30:42
-- 01/04/2013 21:45:08     - 01/04/2013 21:48:18
-- 01/04/2013 21:57:44     - 01/04/2013 22:00:14
-- 01/04/2013 22:09:53     - 01/04/2013 22:13:23
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

--Variables calculated automatically from above values
     set     @scaling_date      = date(@broadcast_start)
     select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date
     set     @seconds_first_minute = 60 - second(@slot_start)
     set     @seconds_last_minute  = second(@slot_end)
     SET     @break_length         = (@seconds_first_minute + @seconds_last_minute)/60 + 2

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
              ,scaling_segment_id       int)
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

--Remove rows where scaling_segment_id is null
delete MEERR_prog_viewing_table
        where scaling_segment_id is null
commit

/*
#Average audience for a break
*/
-- Find minute-by-minute viewing figures for slot of programme of itnerest by scaling segment id
if object_id('MEERR_prog_viewing_minutes') is not null drop table MEERR_prog_viewing_minutes
CREATE TABLE    MEERR_prog_viewing_minutes (minute integer, scaling_segment_id integer, audience integer)
declare         @minute int
SET             @minute = minute(@slot_start)-1
WHILE           @minute < minute(@slot_end)
BEGIN
        SELECT  @minute = @minute + 1
        SELECT  @minute, scaling_segment_id, COUNT(*)
                INTO #scalcount
                FROM  MEERR_prog_viewing_table
                WHERE minute_end   >= @minute
                AND   minute_start <= (@minute - 1)
                GROUP BY scaling_segment_id
        INSERT INTO MEERR_prog_viewing_minutes SELECT * FROM #scalcount
        DROP TABLE #scalcount
END
commit
ALTER TABLE     MEERR_prog_viewing_minutes
        ADD     (sample_size            int
                ,population_size        int
                ,weighting              double
                ,strata_prop            double
                ,strata_variance        double)
UPDATE          MEERR_prog_viewing_minutes a
        SET     a.sample_size = b.sample_size
       FROM     MEERR_strata_sample_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_prog_viewing_minutes a
        SET     a.population_size = b.population_size
       FROM     MEERR_strata_popn_size b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
UPDATE          MEERR_prog_viewing_minutes a
        SET     a.weighting = b.weighting
       FROM     vespa_analysts.SC2_Weightings b
      WHERE     a.scaling_segment_id = b.scaling_segment_id
       and      scaling_day = @scaling_date
UPDATE          MEERR_prog_viewing_minutes
        SET     strata_prop = 1.0*audience/sample_size
UPDATE          MEERR_prog_viewing_minutes
        SET     strata_variance = strata_prop * (1.0 - strata_prop)
commit

SET     @aud1 =
                (SELECT @seconds_first_minute*SUM(1.0*audience*weighting)/60
                   FROM MEERR_prog_viewing_minutes
                  WHERE minute = minute(@slot_start))
SET     @aud2 =
                (SELECT 60*SUM(1.0*audience*weighting)/60
                   FROM MEERR_prog_viewing_minutes
                  WHERE minute = minute(@slot_start)+1)
SET     @aud3 =
                (SELECT 60*SUM(1.0*audience*weighting)/60
                   FROM MEERR_prog_viewing_minutes
                  WHERE minute = minute(@slot_start)+2)
SET     @aud4 =
                (SELECT @seconds_last_minute*SUM(1.0*audience*weighting)/60
                   FROM MEERR_prog_viewing_minutes
                  WHERE minute = minute(@slot_start)+3)

SET     @avaudbreak = (@aud1 + @aud2 + @aud3 + @aud4)/@break_length

SET     @sd1 =
                (SELECT 1.0*@seconds_first_minute/60 * (SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size)))
                   FROM MEERR_prog_viewing_minutes
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start))
SET     @sd2 =
                (SELECT (SUM((1.0*60/60*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size)))
                   FROM MEERR_prog_viewing_minutes
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start)+1)
SET     @sd3 =
                (SELECT (SUM((1.0*60/60*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size)))
                   FROM MEERR_prog_viewing_minutes
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start)+2)
SET     @sd4 =
                (SELECT 1.0*@seconds_last_minute/60 * (SUM((1.0*(population_size - sample_size)/(population_size - 1))
                    *(weighting*weighting*strata_variance/sample_size)))
                   FROM MEERR_prog_viewing_minutes
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start)+3)

SET     @sew1 =
                (SELECT POWER(1.0*@seconds_first_minute/60.0, 2) * SUM(audience*var_weights*audience)
                   FROM MEERR_prog_viewing_minutes calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start))
SET     @sew2 =
                (SELECT POWER(1.0*@seconds_first_minute/60.0, 2) * SUM(audience*var_weights*audience)
                   FROM MEERR_prog_viewing_minutes calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start)+1)
SET     @sew3 =
                (SELECT POWER(1.0*@seconds_first_minute/60.0, 2) * SUM(audience*var_weights*audience)
                   FROM MEERR_prog_viewing_minutes calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start)+2)
SET     @sew4 =
                (SELECT POWER(1.0*@seconds_first_minute/60.0, 2) * SUM(audience*var_weights*audience)
                   FROM MEERR_prog_viewing_minutes calc
             INNER JOIN MEERR_jackknife_weight_vars vars
                     ON calc.scaling_segment_id = vars.scaling_segment_id
                  WHERE population_size > 1 AND audience > 0 AND minute = minute(@slot_start)+3)

set @SEavaudbreak = sqrt(@sd1 + @sd2 + @sd3 + @sd4)

set @SEweights = sqrt(@sew1 + @sew2 + @sew3 + @sew4)

select @avaudbreak, 1.96 * (@SEavaudbreak+@SEweights)
commit
end


