------ Analysis of the Comparisons between BARB, Daily Augs and CBI based on Average viewing per household per hour ------


-- Place Vespa data into a table for use with CP3_capping_comparisons
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

            set @augs_table = 'current_vespa_daily_augs_'||dateformat(@calc_date, 'yyyymmdd')
        declare @begin_time datetime
        declare @end_time datetime
            set @begin_time = @calc_date + cast('00:00:00.000000' as datetime)
            set @end_time   = dateadd(second, -1, dateadd(hour, 1, @begin_time))

    while       @begin_time < dateadd(day, 1, @calc_date) + cast('00:00:00.000000' as datetime)
    begin

    -- Create a temp table of the average weighted HH viewing for each hour for the Augs table.
    -- Currently merging VOSDAL, Playback and Showcase as Playback
--                                                                 when Timeshifting like ''PLAYBACK%'' or Timeshifting = ''SHOWCASE'' then ''PLAYBACK''
    set @var_sql = '
    insert into  temp_Vespa_augs
     select      ''' ||dateformat(@begin_time, 'yyyy-mm-dd') || '''
                ,''' ||cast(@begin_time as time) || '''
                ,''' ||cast(@end_time as time) || '''
                ,event
                ,' || @ntile_correction || ' as ntile_correction
                ,count(distinct sub2.account_number) as no_of_accounts
                ,sum(weights) as weighted_accounts
                ,sum(duration) as total_duration
                ,sum(weights*duration)as weighted_duration
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

        execute(@var_sql)

        set @begin_time = dateadd(hour, 1, @begin_time)
        set @end_time   = dateadd(second, -1, dateadd(hour, 1, @begin_time))
        end

    commit

end; -- procedure CP3_Vespa_augs_table

commit;
go

--Executing the stored procedure ---

declare @calc_date date
set     @calc_date = '2013-09-23'
while   @calc_date <= '2013-09-29'
begin
        exec CP3_Vespa_augs_table @calc_date, 0, 10
        set @calc_date = dateadd(day, 1, @calc_date)
end

--Comparison between Daily Augs and BARB Avg Viewing per HH in Minutes

        if object_id('CP3_capping_calibration_comparison') is not null drop table CP3_capping_calibration_comparison
        create table CP3_capping_calibration_comparison
        (
                event_date                           date
               ,Weekday_indicator                    int   -- A one indicates that the event_date is a weekday, a zero indicates a weekend
               ,start_hour                           time
               ,type_of_event                        varchar(20)
               ,capping_correction                   int
               ,BARB_avg_viewing_time_per_HH         double
               ,Vespa_avg_viewing_time_per_HH        double
        )

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
           ,0
           ,weighted_ave_hh_mins
        from temp_Vespa_augs


---Updating this to reflect both BARB and Vespa for easy comparisons

        update      CP3_capping_calibration_comparison ccom
                set BARB_avg_viewing_time_per_HH = avg_viewing_time_per_HH
               from glasera.BARB_Daily_augs      tbarb
              where ccom.event_date = tbarb.event_date
                and ccom.start_hour = tbarb.start_time
                and ccom.type_of_event = 'LIVE'
                and tbarb.event = 'Live only'

        update      CP3_capping_calibration_comparison ccom
                set BARB_avg_viewing_time_per_HH = avg_viewing_time_per_HH
               from glasera.BARB_Daily_augs      tbarb
              where ccom.event_date = tbarb.event_date
                and ccom.start_hour = tbarb.start_time
                and ccom.type_of_event  = 'PLAYBACK'
                and tbarb.event <> 'Live only'


--Calculating the Average viewing per household per hour for the Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events table from CBI

if object_id('Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_Comparison') is not null drop table Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_Comparison
create table Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_Comparison
(
         viewing_date                  date
        ,start_time                    time
        ,end_time                      time
        ,event                   varchar(30)
        ,no_of_accounts                 int
        ,weighted_accounts             real
        ,total_duration                 int
        ,weighted_duration             real
        ,vespa_ave_hh_mins             real
        ,vespa_weighted_ave_hh_mins    real
)

begin
        declare @begin_time datetime
        declare @end_time datetime
            set @begin_time = '2013-09-23 00:00:00.000000'
            set @end_time   = dateadd(second, -1, dateadd(hour, 1, @begin_time))

    while       @begin_time < '2013-09-30 00:00:00.000000'
    begin

     insert into  Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_Comparison
     select      date(@begin_time)
                ,cast(@begin_time as time)
                ,cast(@end_time as time)
                ,event
                ,count(distinct sub2.account_number) as no_of_accounts
                ,sum(weights) as weighted_accounts
                ,sum(duration) as total_duration
                ,sum(weights*duration)as weighted_duration
                ,1.0*total_duration/no_of_accounts/60 as vespa_ave_hh_mins
                ,weighted_duration/weighted_accounts/60 as vespa_weighted_ave_hh_mins
           from (
                select   date(@begin_time) as begin_date
                        ,cast(@begin_time as time) as begin_time
                        ,cast(@end_time as time) as end_time
                        ,sub1.account_number
                        ,event
                        ,1.0*sum(Duration) as Duration
                         from (
                            select
                                     Vesp.account_number
                                    ,subscriber_id
                        ,case
                             when Live_timeshifted_events = 0 then 'LIVE'
                             else 'PLAYBACK'
                        end as event
                                    ,case
                                            when instance_start_date_time_utc <= @begin_time then @begin_time else instance_start_date_time_utc
                                     end as hourly_starts
                                    ,case
                                            when capped_partial_flag = 1 and capping_end_date_time_utc >= @end_time then @end_time
                                            when capped_partial_flag = 1 and capping_end_date_time_utc >= @begin_time and capping_end_date_time_utc <= @end_time then capping_end_date_time_utc
                                            when instance_end_date_time_utc  >= @end_time then @end_time
                                            else instance_end_date_time_utc
                                     end as hourly_stops
                                   ,datediff(second, hourly_starts, hourly_stops) as Duration
                                from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_All Vesp
                               where instance_start_date_time_utc   <= @end_time
                                 and instance_end_date_time_utc    >= @begin_time
                                 and capped_full_flag = 0
                                 and Duration > 0
                        ) as sub1
                group by account_number, event
          ) as sub2
          inner join (
                        select intr.account_number, wei.weighting as weights
                          from vespa_analysts.SC2_Weightings wei
                    inner join vespa_analysts.SC2_Intervals  intr
                            on wei.scaling_segment_id = intr.scaling_segment_id
                         where scaling_day = date(@begin_time)
                           and reporting_starts <= date(@begin_time)
                           and reporting_ends >= date(@begin_time)
                     ) as sub3
              on sub2.account_number = sub3.account_number
        group by event

        set @begin_time = dateadd(hour, 1, @begin_time)
        set @end_time   = dateadd(second, -1, dateadd(hour, 1, @begin_time))
        end

end
;
alter table CP3_capping_calibration_comparison add CBI_Avg_Viewing_time_per_HH  real;


--Updating now with CBI data
update CP3_capping_calibration_comparison ccom
set CBI_Avg_Viewing_time_per_HH = vespa_weighted_ave_hh_mins
from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_Comparison VESP
     where ccom.event_date = VESP.viewing_date
     and ccom.start_hour = VESP.start_time
     and ccom.type_of_event = VESP.event
--Checks
select * from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_Comparison
select * from temp_Vespa_augs



select top 100* from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events_All
where subscriber_id = 23042508


