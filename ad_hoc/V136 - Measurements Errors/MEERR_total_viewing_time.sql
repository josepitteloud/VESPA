/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** total viewing time metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
**
** The total viewing time metric calculates the total amount of minutes watched of a
** particular show. To find the error assocaited with this
** value we need to find the total no. of people who are watching live TV at the same
** time as the show of interest is on air.
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
     declare @totalview         double
     declare @SEtotalview       double
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
     select @minute_length      = DATEDIFF(minute, @broadcast_start, @broadcast_end)

commit

-- Run procedure to create tables containing all viewing events when @programme_name is on
exec MEERR_viewing_table @viewing_table, @broadcast_start, @broadcast_end

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_popn_sample @scaling_date, @profiling_date

-- Create temp table which contains all_viewers by scaling_segment_id
if object_id('MEERR_prog_all_viewers') is not null drop table MEERR_prog_all_viewers
select  scaling_segment_id, count(distinct subscriber_id) as all_viewers
        into MEERR_prog_all_viewers
        from MEERR_all_viewing_table a
  inner join vespa_analysts.SC2_Sky_base_segment_snapshots b
          on a.account_number = b.account_number
       where profiling_date = @profiling_date
    group by scaling_segment_id
commit

-- Create table holding jsut the viewing info for @programme_name
if object_id('MEERR_prog_viewing_table') is not null drop table MEERR_prog_viewing_table
select *
        into MEERR_prog_viewing_table
        from MEERR_all_viewing_table
       where programme_name = @programme_name
commit

-- Drop original viewing table as no longer required
if object_id('MEERR_all_viewing_table') is not null drop table MEERR_all_viewing_table
commit

-- Link all_viewing_table with their scaling segment ids and RIM weights
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
--Don't know why this occurs, but it does
delete MEERR_prog_viewing_table
        where scaling_segment_id is null
commit

/*#######################################
-- Total viewing time
#########################################*/
-- Create table holding total viewing time by scaling segment id
if object_id('MEERR_strata_total_mins') is not null drop table MEERR_strata_total_mins
select scaling_segment_id, SUM(total_mins) as total_mins
        into MEERR_strata_total_mins
        from MEERR_prog_viewing_table
    group by scaling_segment_id
alter table   MEERR_strata_total_mins
        add   (sample_size              int
              ,population_size          int
              ,weighting                double
              ,pbinomial                double
              ,strata_var               double)
update        MEERR_strata_total_mins a
        set   a.sample_size = b.sample_size
       from   MEERR_strata_sample_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_total_mins a
        set   a.population_size = b.population_size
       from   MEERR_strata_popn_size b
      where   a.scaling_segment_id = b.scaling_segment_id
update        MEERR_strata_total_mins a
        set   a.weighting = b.weighting
       from   vespa_analysts.SC2_Weightings b
      where   a.scaling_segment_id = b.scaling_segment_id
        and   scaling_day = @scaling_date

-- Need to ensure that total_mins is not greater than (@minute_length*sample_size)
update        MEERR_strata_total_mins a
        set   total_mins =
                CASE WHEN total_mins > (@minute_length*sample_size) THEN (@minute_length*sample_size) else total_mins end

UPDATE        MEERR_strata_total_mins
        SET   pbinomial = (1.0 * total_mins) / (@minute_length*sample_size)

UPDATE        MEERR_strata_total_mins
        SET   pbinomial = (1.0 * total_mins) / (90*sample_size)


UPDATE        MEERR_strata_total_mins
         SET   strata_var = pbinomial * ( 1 - pbinomial)
commit

SELECT          @totalview = (
                        SELECT SUM(total_mins * weighting)
                                FROM MEERR_strata_total_mins
                        )

SELECT          @SEtotalview = (
            SELECT SQRT(SUM((1.0*(population_size - sample_size)/(population_size - 1))
                        *(weighting*weighting)
                        *(90*all_viewers)*strata_var/sample_size))
                        FROM MEERR_strata_total_mins   calc
                  INNER JOIN MEERR_prog_all_viewers   temp
                          ON calc.scaling_segment_id = temp.scaling_segment_id
                        WHERE population_size > 1
                )

SELECT @SEweights = (
                SELECT SQRT(SUM(1.0*total_mins*var_weights*total_mins))
                  FROM MEERR_strata_total_mins mins
            INNER JOIN MEERR_jackknife_weight_vars vars
                    ON mins.scaling_segment_id = vars.scaling_segment_id)

select @totalview, 1.96*(@SEtotalview+@SEweights)
commit
end

