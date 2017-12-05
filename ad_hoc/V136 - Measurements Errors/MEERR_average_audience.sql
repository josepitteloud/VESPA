/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** average audience for a programme metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
**
** The average audience metric sums the minute-by-minute viewing of the show of interest
** bfeore dividing by the show's length. To find the error associated with
** this value we need to find the sample size of each strata in order to obtain the
** within-strata variance.
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
     declare @avaudience        double
     declare @SEavaudience      double
     declare @SEweights         double
     declare @minute_length     int

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201306'
     set     @programme_name    = 'Britains Got Talent'
     set     @broadcast_start   = '2013-06-01 18:00:00.000000'
     set     @broadcast_end     = '2013-06-01 19:30:00.000000'

--Variables calculated automatically from above values
     set     @scaling_date      = date(@broadcast_start)
     select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date
     set     @minute_length     = DATEDIFF(minute, @broadcast_start, @broadcast_end)

commit

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_prog_table @viewing_table, @programme_name, @broadcast_start, @broadcast_end

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_popn_sample @scaling_date, @profiling_date

--Link all_viewing_table with their scaling segment ids and RIM weights
--Find the minute that viewers started watching and the minute they finished watching
--If the started or stopped watching more than thiry seconds into the minute add one
alter table   MEERR_prog_viewing_table
        add   (minute_start             int
              ,minute_end               int
              ,scaling_segment_id       int
              ,weighting                double)
update        MEERR_prog_viewing_table a
        set   a.minute_start = DATEDIFF(minute, @broadcast_start, instance_start_date_time_utc)
                             + (case when second(instance_start_date_time_utc) > 30 then 1 else 0 end)
update        MEERR_prog_viewing_table a
        set   a.minute_end = DATEDIFF(minute, @broadcast_start, instance_end_date_time_utc)
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
commit

--Remove rows where scaling_segment_id is null
delete MEERR_prog_viewing_table
        where scaling_segment_id is null

-- Add weighjts to population table
alter table   MEERR_strata_popn_size
        add   (weighting        double)
update        MEERR_strata_popn_size a
        set   a.weighting = b.weighting
       from   vespa_analysts.SC2_Weightings b
      WHERE   a.scaling_segment_id = b.scaling_segment_id
        and   scaling_day = @scaling_date

-- drop rows in MEERR_prog_viewing_table where subscriber_id is not in all_panel_data
delete MEERR_prog_viewing_table
        where subscriber_id not in (
                select       cast(subscriber_id as int)
                        from vespa_analysts.alt_panel_data
                       where panel = 12
                         and dt = @scaling_date)

/*#######################################
--Average audience
#########################################*/
--Find minute-by-minute viewing figures for programme of itnerest by scaling segment id
if object_id('MEERR_prog_viewing_minutes') is not null drop table MEERR_prog_viewing_minutes
CREATE TABLE    MEERR_prog_viewing_minutes (minute integer, scaling_segment_id integer, audience integer)
declare         @minute int
SET             @minute = 0
WHILE           @minute < @minute_length
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

ALTER TABLE  MEERR_prog_viewing_minutes
         ADD (pbinomial real)

UPDATE       MEERR_prog_viewing_minutes mins
         SET pbinomial = 1.0 * audience / sample_size
        FROM MEERR_strata_sample_size samp
       WHERE samp.scaling_segment_id = mins.scaling_segment_id
commit

ALTER TABLE  MEERR_prog_viewing_minutes
         ADD (weighting real)

UPDATE       MEERR_prog_viewing_minutes mins
         SET mins.weighting = a.weighting
        FROM MEERR_prog_viewing_table a
       WHERE mins.scaling_segment_id = a.scaling_segment_id
commit

--drop rows in MEERR_prog_viewing_minutes where pbinomial is null, due to no sample sizes being found
delete MEERR_prog_viewing_minutes
         where pbinomial is null

--update MEERR_prog_viewing_minutes by setting pbinomial equal to one when it is greater than one
UPDATE       MEERR_prog_viewing_minutes
         SET pbinomial = 1.0
       where pbinomial > 1.0

set    @avaudience = (
        SELECT     SUM(av_audience*weighting) FROM (
                   SELECT    scaling_segment_id
                            ,1.0*sum(audience)/@minute_length AS av_audience
                        FROM MEERR_prog_viewing_minutes
                    group by scaling_segment_id) AS sub1
        inner join vespa_analysts.SC2_Weightings b
                on sub1.scaling_segment_id = b.scaling_segment_id
             where scaling_day = @scaling_date)

--Have to put in a CASE statement for viewers, as at present we sometimes get viewers greater
--than sample size. One cause is that subscriber_id in viewing table not in alt_panel_table.
if object_id('#temp_strata_calc_av') is not null drop table #temp_strata_calc_av
select         sub1.*
              ,population_size
              ,sample_size
              ,case when audience > (@minute_length*sample_size)
                then 1.0
                else 1.0*audience/(@minute_length*sample_size)
               end as overall_pbinomial
              ,weighting
        into  #temp_strata_calc_av
        FROM  (
        SELECT           mins.scaling_segment_id
                        ,SUM(audience) as audience
                   FROM MEERR_prog_viewing_minutes mins
               group by mins.scaling_segment_id) as sub1
        inner join (select popn.scaling_segment_id, population_size, sample_size, weighting
                              from MEERR_strata_popn_size popn
                        inner join MEERR_strata_sample_size samp
                                on popn.scaling_segment_id = samp.scaling_segment_id) sub2
                on sub1.scaling_segment_id = sub2.scaling_segment_id

set @SEavaudience = (
        SELECT  SQRT(SUM(1.0*(population_size - sample_size)/(population_size - 1)
                *(weighting*weighting*overall_pbinomial*(1 - overall_pbinomial))/sample_size))
        FROM    #temp_strata_calc_av
           WHERE population_size > 1
    )

set @SEweights = (
        SELECT   SQRT(SUM(1.0*audience/@minute_length*var_weights*audience/@minute_length))
            FROM #temp_strata_calc_av smin
      INNER JOIN MEERR_jackknife_weight_vars vars
              ON smin.scaling_segment_id = vars.scaling_segment_id)

select @avaudience, 1.96*(@SEavaudience + @SEweights)
commit
end


