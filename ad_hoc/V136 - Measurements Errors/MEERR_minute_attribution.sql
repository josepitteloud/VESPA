/******************************************************************************
**
** PROJECT VESPA: MEASUREMENT ERRORS
**
** Refer to
**      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=136&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2Factive.aspx
**
** This script calculates the value of, as well the error associated with, the
** minutes attribution metric.
**
** At present it is set up only for a single 'Live' showing of a TV programme.
**
** The user is required to enter the following information
** Database table containing the viewing data
** datetime of start of broadcast
** datetime of end of broadcast
** Name of show
** Minute of show for which the minute attribution metric is required.
**
** The minute attribution metric calculates the number of set top boxes switched to
** a particular channel at a specified time. To find the error associated with this
** value we need to find the sample size of each strata in order to obtain the
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
     declare @time_attributed   datetime
     declare @totalminattr      double
     declare @SEtotalminattr    double
     declare @SEweights         double

     set     @viewing_table     = 'sk_prod.VESPA_DP_PROG_VIEWED_201306'
     set     @programme_name    = 'Britains Got Talent'
     set     @broadcast_start   = '2013-06-01 18:00:00.000000'
     set     @broadcast_end     = '2013-06-01 19:30:00.000000'

-- Check minute attribution metric after one hour of show
     set     @time_attributed   = '2013-06-01 19:00:00.000000'

--Variables calculated automatically from above values
     set     @scaling_date      = date(@broadcast_start)
     select  @profiling_date    = max(profiling_date)
                                        from vespa_analysts.SC2_Sky_base_segment_snapshots
                                        where profiling_date <= @scaling_date

commit

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_prog_table @viewing_table, @programme_name, @broadcast_start, @broadcast_end

--Run procedure to create tables containing viewing events of interest, strata sample sizes and strata population sizes
exec MEERR_popn_sample @scaling_date, @profiling_date

--Link all_viewing_table with their scaling segment ids and RIM weights
alter table   MEERR_prog_viewing_table
        add   (scaling_segment_id       int
              ,weighting                double)
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

/*#######################################
--Minutes attributed
#########################################*/
declare @hour_attributed   int
declare @min_attributed    int
declare @time_attr_plus    date
set     @hour_attributed   = hour(@time_attributed)
set     @min_attributed    = minute(@time_attributed)
set     @time_attr_plus    = DATEADD(second, 30, @time_attributed)

if object_id('#temp_strata_min_attr') is not null drop table #temp_strata_min_attr
select  scaling_segment_id
       ,MAX(weighting) as weighting
       ,COUNT(distinct subscriber_id) as viewers
        into #temp_strata_min_attr
        FROM (
            select *
                    from MEERR_prog_viewing_table
                   where (event_start_date_time_utc <= @time_attributed and event_end_date_time_utc >= @time_attr_plus)
                      or (hour(event_start_date_time_utc) = @hour_attributed
                     and  minute(event_start_date_time_utc) = @min_attributed
                     and  DATEDIFF(second, event_start_date_time_utc, event_end_date_time_utc) > 30)
        ) AS sub1
        group by scaling_segment_id

SET  @totalminattr =
                (SELECT SUM(1.0*viewers*weighting)
                        FROM #temp_strata_min_attr)
commit

--Have to put in a CASE statement for viewers, as at present we sometimes get viewers greater
--than sample size. One cause is that subscriber_id in viewing table not in alt_panel_table.
if object_id('#temp_strata_calc_attr') is not null drop table #temp_strata_calc_attr
SELECT           attr.scaling_segment_id
                ,population_size
                ,sample_size
                ,weighting
                ,CASE WHEN viewers > sample_size then sample_size else viewers end as viewers
                ,1.0*viewers/sample_size as strata_prop
                ,strata_prop*(1.0-strata_prop) as strata_variance
           into #temp_strata_calc_attr
           FROM #temp_strata_min_attr attr
     inner join (select popn.scaling_segment_id, population_size, sample_size
                              from MEERR_strata_popn_size popn
                        inner join MEERR_strata_sample_size samp
                                on popn.scaling_segment_id = samp.scaling_segment_id) sub1
              on attr.scaling_segment_id = sub1.scaling_segment_id
           WHERE population_size > 1
             AND viewers > 0

SET  @SEtotalminattr =
                (SELECT SQRT(SUM(1.0*(population_size - sample_size)/(population_size - 1)
                    *(weighting*weighting*strata_variance/sample_size)))
                    FROM  #temp_strata_calc_attr)

SET  @SEweights =
                (SELECT SQRT(SUM(viewers*var_weights*viewers))
                        FROM #temp_strata_calc_attr calc
                  INNER JOIN MEERR_jackknife_weight_vars vars
                          ON calc.scaling_segment_id = vars.scaling_segment_id
                       WHERE population_size > 1)

select @totalminattr, 1.96*(@SEtotalminattr + @SEweights)

commit
end

