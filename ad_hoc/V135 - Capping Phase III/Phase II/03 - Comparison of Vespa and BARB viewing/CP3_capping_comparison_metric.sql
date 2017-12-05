-- Capping calibration - this procedure create AUG tables without any capping applied so they can
-- go through the automatic summary procees
-- Capping calibration - this procedure create AUG tables without any capping applied so they can
-- go through the automatic summary procees
if object_id('CP3_BARB_augs_table_creation') is not null drop procedure CP3_BARB_augs_table_creation
commit

go

create procedure CP3_BARB_augs_table_creation
     @CP3_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

        -- Place BARB data into a table for use with CP3_capping_calibration
        if object_id('temp_BARB_augs') is not null drop table temp_BARB_augs
        select *
                into temp_BARB_augs
                from vespa_analysts.BARB_Daily_Augs
               where event_date in (select capping_date from CA3_capping_days)

    commit

end; -- procedure CP3_BARB_augs_table_creation

commit;
go

if object_id('CP3_Vespa_augs_table_creation') is not null drop procedure CP3_Vespa_augs_table_creation
commit

go

create procedure CP3_Vespa_augs_table_creation
     @CP3_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

        -- Place Vespa data into a table for use with CP3_capping_calibration
        if object_id('temp_Vespa_augs') is not null drop table temp_Vespa_augs
        create table temp_Vespa_augs
        (
                 viewing_date           date
                ,start_time             time
                ,end_time               time
                ,event                  varchar(30)
                ,ntile_correction       int
                ,no_of_accounts         int
                ,weighted_accounts      real
                ,total_duration         int
                ,weighted_duration      real
                ,ave_hh_mins            real
                ,weighted_ave_hh_mins   real
        )

        -- Create table which contains viewing figures to be compared
        -- weighted_ave_hh_mins is the value from Vespa
        -- avg_viewing_time_per_HH is the value from BARB / Tech Edge
        if object_id('CA3_comparison_viewing') is not null drop table CA3_comparison_viewing
        create table CA3_comparison_viewing
        (
                 event                    varchar(30)
                ,viewing_date             date
                ,start_time               time
                ,ntile_correction         int
                ,weighted_ave_hh_mins     real
                ,avg_viewing_time_per_HH  real
        )

        if object_id('CA3_diff_viewing') is not null drop table CA3_diff_viewing
        create table CA3_diff_viewing
        (
                 start_time             time
                ,event                  varchar(30)
                ,Vespa_viewing          real
                ,BARB_viewing           real
                ,proportion_difference  real
        )

end; -- procedure CP3_Vespa_augs_table_creation

commit;
go

if object_id('CP3_Vespa_augs_table') is not null drop procedure CP3_Vespa_augs_table
commit

go

create procedure CP3_Vespa_augs_table
     @calc_date        date = NULL     -- Date of daily table caps to cache
    ,@ntile_correction  int = NULL      -- Value of ntile_correction used with capping data
    ,@CP3_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

        declare @var_sql varchar(15000)
        declare @augs_table varchar(100)

            set @augs_table = 'Vespa_Daily_Augs_'||dateformat(@calc_date, 'yyyymmdd')
-- execute('insert into test_table select *,' || @ntile_correction || ' from ' || @augs_table)
        declare @begin_time datetime
        declare @end_time datetime
            set @begin_time = @calc_date + cast('00:00:00.000000' as datetime)
            set @end_time   = dateadd(second, -1, dateadd(hour, 1, @begin_time))

    while       @begin_time < dateadd(day, 1, @calc_date) + cast('00:00:00.000000' as datetime)
    begin

    -- Currently merging VOSDAL, Playback and Showcase as Playback
    -- This is the current metric that we are using for our analysis, however we actually want the
    -- metric to be based on start hour.
    -- When the data from DB1 is available then we can delete the code within the next section and
    -- use the commented out code in the following part.
-- ================================================================================================
    -- Create a temp table of the average weighted HH viewing for each hour.
    set @var_sql = '
     insert into  temp_Vespa_augs
     select      ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                ,''' ||cast(@begin_time as time) || '''
                ,''' ||cast(@end_time as time) || '''
                ,event
                ,' || @ntile_correction || '            as ntile_correction
                ,count(distinct sub2.account_number)    as no_of_accounts
                ,sum(weights)                           as weighted_accounts
                ,sum(duration)                          as total_duration
                ,sum(weights*duration)                  as weighted_duration
                ,(1.0*total_duration/no_of_accounts)/60 as ave_hh_mins
                ,weighted_duration/weighted_accounts/60 as weighted_ave_hh_mins
           from (
                select   ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || ''' as begin_date
                        ,''' ||cast(@begin_time as time) || ''' as begin_time
                        ,''' ||cast(@end_time as time) || ''' as end_time
                        ,sub1.account_number
                        ,event
                        ,sum(duration) as duration
                         from (
                            select
                                     aug.account_number
                                    ,subscriber_id
                                    ,case
                                            when Timeshifting like ''LIVE_%''                                                                    then ''LIVE''
                                            when Timeshifting like ''VOSDAL%'' or Timeshifting like ''PLAYBACK%'' or Timeshifting = ''SHOWCASE'' then ''PLAYBACK''
                                            else ''Other''
                                     end as event
                                    ,case
                                            when viewing_starts <= cast(''' || @begin_time || ''' as datetime) then cast(''' || @begin_time || ''' as datetime) else viewing_starts
                                     end as hourly_starts
                                    ,case
                                            when viewing_stops >= cast(''' || @end_time || ''' as datetime) then cast(''' || @end_time || ''' as datetime) else viewing_stops
                                     end as hourly_stops
                                    ,datediff(second, hourly_starts, hourly_stops) as duration
                                from ' || @augs_table || ' aug
                               where viewing_starts   <= ''' || @end_time || '''
                                 and viewing_stops    >= ''' || @begin_time || '''
                                 and viewing_duration >  6
                                 and duration > 0
                        ) as sub1
                group by account_number, event
          ) as sub2
          inner join (
                        select intr.account_number, wei.weighting as weights
                          from vespa_analysts.SC2_Weightings wei
                    inner join vespa_analysts.SC2_Intervals  intr
                            on wei.scaling_segment_id = intr.scaling_segment_id
                         where scaling_day = ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                           and reporting_starts <= ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                           and reporting_ends >= ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                     ) as sub3
              on sub2.account_number = sub3.account_number
         group by event'
-- ================================================================================================


-- ================================================================================================
-- This metric should be sued once we have worked out how to get the start hour metric from DB1
    -- Create a temp table of the average weighted HH viewing for each start hour
    set @var_sql = '
     insert into  temp_Vespa_augs
     select      ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                ,''' ||cast(@begin_time as time) || '''
                ,''' ||cast(@end_time as time) || '''
                ,event
                ,' || @ntile_correction || '            as ntile_correction
                ,count(distinct sub2.account_number)    as no_of_accounts
                ,sum(weights)                           as weighted_accounts
                ,sum(duration)                          as total_duration
                ,sum(weights*duration)                  as weighted_duration
                ,(1.0*total_duration/no_of_accounts)/60 as ave_hh_mins
                ,weighted_duration/weighted_accounts/60 as weighted_ave_hh_mins
           from (
                select   ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || ''' as begin_date
                        ,''' ||cast(@begin_time as time) || ''' as begin_time
                        ,''' ||cast(@end_time as time) || ''' as end_time
                        ,sub1.account_number
                        ,event
                        ,sum(duration) as duration
                         from (
                            select
                                     aug.account_number
                                    ,subscriber_id
                                    ,case
                                            when Timeshifting like ''LIVE_%''                                                                    then ''LIVE''
                                            when Timeshifting like ''VOSDAL%'' or Timeshifting like ''PLAYBACK%'' or Timeshifting = ''SHOWCASE'' then ''PLAYBACK''
                                            else ''Other''
                                     end as event
                                   ,viewing_duration as duration
                                from ' || @augs_table || ' aug
                               where viewing_starts   >= ''' || @begin_time || '''
                                 and viewing_starts   <= ''' || @end_time || '''
                                 and viewing_duration >  6
                                 and viewing_date = @calc_date
                                 and ntile_correction = ' || @ntile_correction || '
                        ) as sub1
                group by account_number, event
          ) as sub2
          inner join (
                        select intr.account_number, wei.weighting as weights
                          from vespa_analysts.SC2_Weightings wei
                    inner join vespa_analysts.SC2_Intervals  intr
                            on wei.scaling_segment_id = intr.scaling_segment_id
                         where scaling_day = ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                           and reporting_starts <= ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                           and reporting_ends >= ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                     ) as sub3
              on sub2.account_number = sub3.account_number
         group by event'
-- ================================================================================================

        execute(@var_sql)

        set @begin_time = dateadd(hour, 1, @begin_time)
        set @end_time   = dateadd(second, -1, dateadd(hour, 1, @begin_time))
        end

--      Code used to collect all of the informtaion form the vespa_daily_augs tables created. Useful for debugging and
--      getting a general idea of how the data is looking.
--     set @var_sql = 'insert into testall_vespa_daily_augs select ' || @ntile_correction || ',''' || @calc_date || ''', * from ' || @augs_table
--     select @var_sql
--         execute(@var_sql)

    commit

end; -- procedure CP3_Vespa_augs_table

commit;
go




if object_id('CP3_capping_comparison_table') is not null drop procedure CP3_capping_comparison_table
commit

go

-- Procedure to keep a historical record of vespa augmented viewing habits during capping calibration.
create procedure CP3_capping_comparison_table
    @ntile_correction    int = null   -- Value of ntile_correction currently being used
   ,@CP3_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

        insert into CP3_capping_calibration_comparison
        (
            event_date
           ,Weekday_indicator
           ,start_hour
           ,type_of_event
           ,capping_correction
           ,Vespa_avg_viewing_time_per_HH
        )
        select
            viewing_date
           ,case
                when dow(viewing_date) between 2 and 6 then 1 else 0 end as Weekday_indicator
           ,start_time
           ,event
           ,@ntile_correction
           ,weighted_ave_hh_mins
        from temp_Vespa_augs
       where temp_Vespa_augs.ntile_correction = @ntile_correction

    commit

end; -- procedure CP3_capping_comparison_table

commit;
go



if object_id('CP3_capping_comparison_metric') is not null drop procedure CP3_capping_comparison_metric
commit

go

create procedure CP3_capping_comparison_metric
    @CP3_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

        -- Create table to use with metrics
        if object_id('CP3_comparison_metrics') is not null drop table CP3_comparison_metrics
        create table CP3_comparison_metrics
        (
                 event_date               date
                ,time_period              varchar(20)
                ,type_of_event            varchar(30)
                ,weekday_indicator        int
                ,capping_correction       int
                ,BARB_viewing_hh_metric   real
                ,vespa_viewing_hh_metric  real
                ,difference_metric        real
                ,rank                     int
        )

        insert into CP3_comparison_metrics
                select
                        event_date
                       ,'23-3'
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction
                       ,avg(BARB_avg_viewing_time_per_HH)  as BARB_viewing_hh_metric
                       ,avg(Vespa_avg_viewing_time_per_HH) as vespa_viewing_hh_metric
                       ,(vespa_viewing_hh_metric - BARB_viewing_hh_metric)/BARB_viewing_hh_metric as difference_metric
                       ,rank() over (partition by type_of_event, weekday_indicator order by abs(difference_metric)) as rank
                 from   CP3_capping_calibration_comparison
                where   (start_hour >= '23:00:00' or start_hour <= '03:00:00') and type_of_event = 'LIVE'
             group by   event_date
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction

        insert into CP3_comparison_metrics
                select
                        event_date
                       ,'4-14'
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction
                       ,avg(BARB_avg_viewing_time_per_HH)  as BARB_viewing_hh_metric
                       ,avg(Vespa_avg_viewing_time_per_HH) as vespa_viewing_hh_metric
                       ,(vespa_viewing_hh_metric - BARB_viewing_hh_metric)/BARB_viewing_hh_metric as difference_metric
                       ,rank() over (partition by type_of_event, weekday_indicator order by abs(difference_metric)) as rank
                 from   CP3_capping_calibration_comparison
                where   start_hour >= '04:00:00' and start_hour <= '14:00:00' and type_of_event = 'LIVE'
             group by   event_date
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction

        insert into CP3_comparison_metrics
                select
                        event_date
                       ,'15-19'
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction
                       ,avg(BARB_avg_viewing_time_per_HH)  as BARB_viewing_hh_metric
                       ,avg(Vespa_avg_viewing_time_per_HH) as vespa_viewing_hh_metric
                       ,(vespa_viewing_hh_metric - BARB_viewing_hh_metric)/BARB_viewing_hh_metric as difference_metric
                       ,rank() over (partition by type_of_event, weekday_indicator order by abs(difference_metric)) as rank
                 from   CP3_capping_calibration_comparison
                where   start_hour >= '15:00:00' and start_hour <= '19:00:00' and type_of_event = 'LIVE'
             group by   event_date
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction

        insert into CP3_comparison_metrics
                select
                        event_date
                       ,'20-22'
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction
                       ,avg(BARB_avg_viewing_time_per_HH)  as BARB_viewing_hh_metric
                       ,avg(Vespa_avg_viewing_time_per_HH) as vespa_viewing_hh_metric
                       ,(vespa_viewing_hh_metric - BARB_viewing_hh_metric)/BARB_viewing_hh_metric as difference_metric
                       ,rank() over (partition by type_of_event, weekday_indicator order by abs(difference_metric)) as rank
                 from   CP3_capping_calibration_comparison
                where   start_hour >= '20:00:00' and start_hour <= '22:00:00' and type_of_event = 'LIVE'
             group by   event_date
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction

        insert into CP3_comparison_metrics
                select
                        event_date
                       ,'All day'
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction
                       ,avg(BARB_avg_viewing_time_per_HH)  as BARB_viewing_hh_metric
                       ,avg(Vespa_avg_viewing_time_per_HH) as vespa_viewing_hh_metric
                       ,(vespa_viewing_hh_metric - BARB_viewing_hh_metric)/BARB_viewing_hh_metric as difference_metric
                       ,rank() over (partition by type_of_event, weekday_indicator order by abs(difference_metric)) as rank
                 from   CP3_capping_calibration_comparison
                where   type_of_event <> 'LIVE'
             group by   event_date
                       ,type_of_event
                       ,weekday_indicator
                       ,capping_correction
    commit

end; -- procedure CP3_capping_comparison_metric

commit;
go


