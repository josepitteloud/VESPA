
  -- ########################################################################
  -- #### Stage 1                                                        ####
  -- ########################################################################
select * from vespa_analysts.SC3_Metrics order by 1 desc;
select max(reporting_ends) from vespa_analysts.SC3_Intervals;
select cast(dt_min / 100 as varchar(10)) as dt_min, cast(dt_max / 100 as varchar(10)) as dt_max
  from (select min(dk_event_start_datehour_dim) as dt_min,
               max(dk_event_start_datehour_dim) as dt_max
          from sk_prod.vespa_dp_prog_viewed_current) a;


drop view if exists Scaling3_00_Input;
create view Scaling3_00_Input as
 -- select * from sk_prod.VESPA_DP_PROG_VIEWED_201309
 -- union all
  select * from sk_prod.VESPA_DP_PROG_VIEWED_201310;
commit;


begin

    execute SC3_recreate_env        -- CAREFULLY - THIS REMOVES ALL EXISTING SCALING DATA IN USER SCHEMA!!!

    declare @CP2_build_ID int
    declare @varStartDate date
    declare @varEndDate   date

    set @varStartDate = '2013-10-11'      -- A Friday
    set @varEndDate   = '2013-10-27'      -- The following Thursday

    execute logger_create_run 'Scaling 3.0 Custom', 'Weekly scaling run', @CP2_build_ID output
    commit

    execute SC3_v1_1__do_weekly_segmentation_CUSTOM @varStartDate, @CP2_build_ID, now()
    commit


    while @varStartDate <= @varEndDate
        begin
            execute SC3_v1_1__scale_Vespa_panel_CUSTOM @varStartDate, now(), @CP2_build_ID
            commit

            set @varStartDate = @varStartDate + 1
        end

    execute logger_get_latest_job_events 'Scaling 3.0 Custom', 4

end;


  -- ########################################################################
  -- #### Stage 2                                                        ####
  -- ########################################################################
 -- ### Push results to VA ###
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.SC3_Category_Subtotals            ;select min(scaling_date) as mn, max(scaling_date) as mx from SC3_Category_Subtotals        ;
select min(reporting_starts) as mn, max(reporting_ends) as mx from vespa_analysts.SC3_Intervals               ;select min(reporting_starts) as mn, max(reporting_ends) as mx from SC3_Intervals           ;
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.SC3_Metrics                       ;select min(scaling_date) as mn, max(scaling_date) as mx from SC3_Metrics                   ;
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.SC3_Non_Convergences              ;select min(scaling_date) as mn, max(scaling_date) as mx from SC3_Non_Convergences          ;
select min(profiling_date) as mn, max(profiling_date) as mx from vespa_analysts.SC3_Sky_base_segment_snapshots;select min(profiling_date) as mn, max(profiling_date) as mx from SC3_Sky_base_segment_snapshots;
select min(scaling_day) as mn, max(scaling_day) as mx from vespa_analysts.SC3_Weightings                      ;select min(scaling_day) as mn, max(scaling_day) as mx from SC3_Weightings                  ;
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.Vespa_Household_Weighting         ;select min(scaling_date) as mn, max(scaling_date) as mx from Vespa_Household_Weighting     ;

-- select  *
-- from    SC3_Metrics
-- where   scaling_date = (select max(scaling_date) from SC3_Metrics)

-- select top 100 * from SC3_Weightings


insert into vespa_analysts.SC3_Category_Subtotals         select * from SC3_Category_Subtotals        ; commit;
insert into vespa_analysts.SC3_Intervals                  select * from SC3_Intervals                 ; commit;
insert into vespa_analysts.SC3_Metrics
       (scaling_date, iterations, convergence, max_weight, av_weight, sum_of_weights, sky_base, vespa_panel, non_scalable_accounts, sum_of_convergence)
  select scaling_date, iterations, convergence, max_weight, av_weight, sum_of_weights, sky_base, vespa_panel, non_scalable_accounts, sum_of_convergence
    from SC3_Metrics                   ; commit;
insert into vespa_analysts.SC3_Non_Convergences           select * from SC3_Non_Convergences          ; commit;
insert into vespa_analysts.SC3_Sky_base_segment_snapshots select * from SC3_Sky_base_segment_snapshots; commit;
insert into vespa_analysts.SC3_Weightings                 select * from SC3_Weightings                ; commit;
insert into vespa_analysts.Vespa_Household_Weighting      select * from Vespa_Household_Weighting     ; commit;


update vespa_analysts.SC3_Metrics base
   set base.min_weight = det.min_weight
  from (select
              scaling_day,
              min(weighting) as min_weight
          from vespa_analysts.SC3_Weightings
         where weighting > 0.000001
         group by scaling_day) det
 where base.scaling_date = det.scaling_day
   and base.min_weight is null;


-- select * from vespa_analysts.SC3_Metrics                       ;
-- select min(scaling_date) as mn, max(scaling_date) as mx from SC3_Category_Subtotals        ;
-- select min(reporting_starts) as mn, max(reporting_ends) as mx from SC3_Intervals           ;
-- select min(scaling_date) as mn, max(scaling_date) as mx from SC3_Non_Convergences          ;
-- select min(profiling_date) as mn, max(profiling_date) as mx from SC3_Sky_base_segment_snapshots;
-- select min(scaling_day) as mn, max(scaling_day) as mx from SC3_Weightings                  ;
-- select min(scaling_date) as mn, max(scaling_date) as mx from Vespa_Household_Weighting     ;
-- 








