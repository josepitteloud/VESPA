
  -- ########################################################################
  -- #### Stage 1                                                        ####
  -- ########################################################################
                 -- Check current data in VA tables and ensure data in viewing table
  select * from vespa_analysts.SC2_Metrics order by 1 desc;
select max(reporting_ends) from vespa_analysts.SC2_Intervals;
select cast(dt_min / 100 as varchar(10)) as dt_min, cast(dt_max / 100 as varchar(10)) as dt_max
  from (select min(dk_event_start_datehour_dim) as dt_min,
               max(dk_event_start_datehour_dim) as dt_max
          from sk_prod.vespa_dp_prog_viewed_current) a;

                  -- insert months according to range of week being run
drop view if exists Scaling2_00_Input;
create view vespa_analysts.Scaling2_00_Input as
  --select * from /*sk_prod.*/VESPA_DP_PROG_VIEWED_201412
  --union all
  select * from VESPA_DP_PROG_VIEWED_current
commit;
select top 10 * from scaling2_00_input
call dba.sp_drop_table('vespa_analysts','Scaling2_00_Input')
call dba.sp_create_table('vespa_analysts','Scaling2_00_Input','account_number varchar(30)
                                                              ,service_instance_id varchar(30)
                                                              ,event_start_date_time_utc datetime
                                                              ,panel_id int
                                                               ')

insert into vespa_analysts.Scaling2_00_Input select account_number,service_instance_id,event_start_date_time_utc,panel_id
from VESPA_DP_PROG_VIEWED_current
group by account_number,service_instance_id,event_start_date_time_utc,panel_id
begin

    execute SC2_recreate_env        -- CAREFULLY - THIS REMOVES ALL EXISTING SCALING DATA IN USER SCHEMA!!!

    create variable @CP2_build_ID int
    create variable @varStartDate date
    create variable @varEndDate   date

                                                                        -- Change dates according to range being run

    set @varStartDate = '2016-01-25'      -- A Friday
    set @varEndDate   = '2016-01-31'      -- The following Thursday

    execute logger_create_run 'Scaling 2.1 Custom', 'Weekly scaling run', @CP2_build_ID output
    commit

    execute vespa_analysts.SC2_v2_1__do_weekly_segmentation_CUSTOM @varStartDate, @CP2_build_ID, now()
    commit

    while @varStartDate <= @varEndDate
        begin
            execute vespa_analysts.SC2_v2_1__scale_Vespa_panel_CUSTOM @varStartDate, now(), @CP2_build_ID
            commit

            set @varStartDate = @varStartDate + 1
        end

    execute logger_get_latest_job_events 'Scaling 2.1 Custom', 4

end;



  -- ########################################################################
  -- #### Stage 2                                                        ####
  -- ########################################################################
 -- ### Check dates are correct in own schema and continue from VA ###
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.SC2_Category_Subtotals            ;select min(scaling_date) as mn, max(scaling_date) as mx from SC2_Category_Subtotals        ;
select min(reporting_starts) as mn, max(reporting_ends) as mx from vespa_analysts.SC2_Intervals               ;select min(reporting_starts) as mn, max(reporting_ends) as mx from SC2_Intervals           ;
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.SC2_Metrics                       ;select min(scaling_date) as mn, max(scaling_date) as mx from SC2_Metrics                   ;
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.SC2_Non_Convergences              ;select min(scaling_date) as mn, max(scaling_date) as mx from SC2_Non_Convergences          ;
select min(profiling_date) as mn, max(profiling_date) as mx from vespa_analysts.SC2_Sky_base_segment_snapshots;select min(profiling_date) as mn, max(profiling_date) as mx from SC2_Sky_base_segment_snapshots;
select min(scaling_day) as mn, max(scaling_day) as mx from vespa_analysts.SC2_Weightings                      ;select min(scaling_day) as mn, max(scaling_day) as mx from SC2_Weightings                  ;
select min(scaling_date) as mn, max(scaling_date) as mx from vespa_analysts.Vespa_Household_Weighting         ;select min(scaling_date) as mn, max(scaling_date) as mx from Vespa_Household_Weighting     ;

select  *
from    SC2_Metrics
where   scaling_date = (select max(scaling_date) from SC2_Metrics)

select top 100 * from SC2_Weightings

  -- ########################################################################
  -- #### Stage 3                                                       ####
  -- ########################################################################
 -- ### Push results to VA ###
insert into vespa_analysts.SC2_Category_Subtotals         select * from SC2_Category_Subtotals        ; commit;
insert into vespa_analysts.SC2_Intervals                  select * from SC2_Intervals                 ; commit;
insert into vespa_analysts.SC2_Metrics
       (scaling_date, iterations, convergence, max_weight, av_weight, sum_of_weights, sky_base, vespa_panel, non_scalable_accounts, sum_of_convergence)
  select scaling_date, iterations, convergence, max_weight, av_weight, sum_of_weights, sky_base, vespa_panel, non_scalable_accounts, sum_of_convergence
    from SC2_Metrics                   ; commit;
insert into vespa_analysts.SC2_Non_Convergences           select * from SC2_Non_Convergences          ; commit;
insert into vespa_analysts.SC2_Sky_base_segment_snapshots select * from SC2_Sky_base_segment_snapshots; commit;
insert into vespa_analysts.SC2_Weightings                 select * from SC2_Weightings                ; commit;
insert into vespa_analysts.Vespa_Household_Weighting      select * from Vespa_Household_Weighting     ; commit;


update vespa_analysts.SC2_Metrics base
   set base.min_weight = det.min_weight
  from (select
              scaling_day,
              min(weighting) as min_weight
          from vespa_analysts.SC2_Weightings
         where weighting > 0.000001
         group by scaling_day) det
 where base.scaling_date = det.scaling_day
   and base.min_weight is null

                -- Check data in is okay in VA and dates in own schema
select * from vespa_analysts.SC2_Metrics                       ;
select min(scaling_date) as mn, max(scaling_date) as mx from SC2_Category_Subtotals        ;
select min(reporting_starts) as mn, max(reporting_ends) as mx from SC2_Intervals           ;
select min(scaling_date) as mn, max(scaling_date) as mx from SC2_Non_Convergences          ;
select min(profiling_date) as mn, max(profiling_date) as mx from SC2_Sky_base_segment_snapshots;
select min(scaling_day) as mn, max(scaling_day) as mx from SC2_Weightings                  ;
select min(scaling_date) as mn, max(scaling_date) as mx from Vespa_Household_Weighting     ;








select count() from SC2_Todays_panel_members
select count() from SC2_intervals
select count() from vespa_analysts.SC2_intervals
select top 10 * from vespa_analysts.SC2_intervals
select max(reporting_ends)  from vespa_analysts.SC2_intervals

select count() from vespa_analysts.scaling2_00_input
select count() from scaling2_00_input



account_number
service_instance_id


event_start_date_time_utc


