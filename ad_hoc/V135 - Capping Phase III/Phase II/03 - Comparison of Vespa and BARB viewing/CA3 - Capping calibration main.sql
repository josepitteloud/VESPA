
-- Capping calibration - this procedure incorporates all of the steps for capping calibration
-- The code is broken down into several steps
-----------------------------------------------------------------------------------------------------------
-- Part A: Table / parameter creation
-- A01: Take values of input parameters @start_date, @end_date & @days_of_week and create a table (CA3_capping_days)
--      with a sequence id to find the days of the week that are being looked at.
--      @start_date and @end_date are the dates between which are doing capping
--      @days_of_week is a string variable containing only (sorted) numbers relating to the days of the week that
--      we are looking at capping for. The numbers relate to which days within the @start_date and @end_date are being
--      looked at, e.g. 1 would refer to the first date (@start_date), whilst 6 would relate to the the sixth day
--      which is equal to dateadd(day, 6-1, @start_date). Since there cannot be more than a week between the
--      @start_date and @end_date no numbers greater than 7 should be entered into the string.
-- A02: Create blank table (CP3_capping_calibration_comparison) containing: event_date, start_hour, Weekday_indicator
--      type_of_event, capping_correction, vaerage viewing time from both Vespa and BARB, difference_metric (and any
--      other information that is required).
--      This will contain the information that we will use to find the ‘best’ capping threshold for each event.
-- A03: Create blank temp table (historical_capping_threshold_correction) containing details from current
--      values of capping threshold. Kept for historical purposes only
-- A04: Create table containing BARB \ Tech Edge data for dates of interest.
-- A05: Create (and initialise) parameters that will be used in the capping procedure.

-- -- : type_of_event, start_hour,
-- --      ntile_correction. Fill first two columns with a Cartesian product of every event type and start hour.
-- --      This table contains the rules and the value of the ntile correction that will be used in capping; the value
-- --      of ntile_correction will start at -5, and then be increased by 1 for each iteration of the loop, to a
-- --      maximum of 5 or (200 - current ntile value), whichever is the larger.
-----------------------------------------------------------------------------------------------------------
-- Part B:
-- B01: Update parameters: set ntile_correction in capping_threshold_correction table and @capping date
-- B02: Run capping for @capping_date, using value of ntile_correction from #capping_threshold_correction
-- B03: Create hourly AUGs tables containing Vespa data for dates of interest / ntile correction.
-- B04: Collate Vespa hourly AUGs tables
-- B05: Update hourly AUGs tables with BARB data
-- B06: Run the procedure CP3_capping_comparison_metric to calculate metrics of interest
-----------------------------------------------------------------------------------------------------------


if object_id('CP3_capping_calibration') is not null drop procedure CP3_capping_calibration
commit

go

create procedure CP3_capping_calibration
     @start_date        date = NULL        -- Date of daily table caps to cache
    ,@end_date          date = NULL        -- Date of daily table caps to cache
    ,@CP3_build_ID      bigint = NULL      -- Logger ID (so all builds end up in same queue
    ,@days_of_week      varchar(10) = null -- days of week that are used to find capping for.
as
begin

        -- Part A: Table / parameter creation
        -- Inital parameters, @i is used as a counter in several while loops
        declare @i              int


        -- A01: Create CA3_capping_days
        if object_id('CA3_capping_days') is not null drop table CA3_capping_days
        create table CA3_capping_days
        (
                seq_id            int identity primary key
               ,capping_date      date
        )

        declare @current_capping_date date
        select  @current_capping_date = @start_date
        if isnull(@days_of_week, '') = ''
        begin
                while   @current_capping_date <= @end_date
                begin
                        insert into CA3_capping_days(capping_date) values(@current_capping_date)
                        select @current_capping_date = dateadd(day, 1, @current_capping_date)
                        select @current_capping_date
                end
        end
        else
        begin
                select  @i = 1
                declare @string_len   int
                select  @string_len = len(@days_of_week)
                declare @string_i     int
                while   @i <= @string_len
                begin
                        select @string_i = substring(@days_of_week, @i, @i) - 1
                        insert into CA3_capping_days(capping_date) values(date(dateadd(day, @string_i, @start_date)))
                        select @i = @i + 1
                end
        end
        alter table CA3_capping_days add weekday_indicator tinyint
        update      CA3_capping_days set Weekday_indicator = (case when dow(capping_date) between 2 and 6 then 1 else 0 end)

        commit

        -- Final check of CA3_capping_days, probably unneccessary but should stop any dates outside input parameters being used.
        delete from CA3_capping_days where capping_date > @end_date

        execute logger_add_event @CP3_build_ID, 3, 'A01: CP3 capping days: Time = ' || now()

        -- A02: Createe CP3_capping_calibration_comparison
        if object_id('CP3_capping_calibration_comparison') is not null drop table CP3_capping_calibration_comparison
        create table CP3_capping_calibration_comparison
        (
                event_date                      date
               ,Weekday_indicator               int   -- A one indicates that the event_date is a weekday, a zero indicates a weekend
               ,start_hour                      time
               ,type_of_event                   varchar(20)
               ,capping_correction              int
               ,BARB_avg_viewing_time_per_HH    double
               ,Vespa_avg_viewing_time_per_HH   double
--                ,difference_metric               double
        )

        execute logger_add_event @CP3_build_ID, 3, 'A02: CP3 capping calibration comparison table made '

--         -- A03: Save current capping threshold values
--         if object_id('historical_capping_threshold_correction') is not null drop table historical_capping_threshold_correction
--         select *
--                 into historical_capping_threshold_correction
--                 from capping_threshold_corrections
--
--         execute logger_add_event @CP3_build_ID, 3, 'A03: Original capping threshold corrections saved with error = ' || @@error

        -- A04: Create tables containing viewing information from BARB and Vespa.
        -- Note that Vespa table will be blank until the capping stage.
        execute CP3_BARB_augs_table_creation @CP3_build_ID

        execute CP3_Vespa_augs_table_creation @CP3_build_ID

        execute logger_add_event @CP3_build_ID, 3, 'A04: BARB and Vespa viewing tavles created '

        -- A05: Create parameters that will be used in the capping procedure.
        declare @ntile_correction int
        set     @ntile_correction = -10
        declare @max_i            int
        set     @max_i            = (select max(seq_id) from CA3_capping_days)
        declare @capping_date     date

        execute logger_add_event @CP3_build_ID, 3, 'A05: CP3 parameters created '

        -- A06: Run profile box procedure
        -- Need to make the @profiling_thursday parameter more dynamic
        -- This is run to ensure that we have the full box profiles for all the accounts.
--         execute CP2_Profile_Boxes '2013-09-26', @CP3_build_ID
--         execute logger_add_event @CP3_build_ID, 3, 'A06: Profile box procedure run'

        -- Part B: Calculate cpping times for each new value of @ntile_correction

        -- B01: Update parameters: set ntile_correction in capping_threshold_correction table and @capping date
        while   @ntile_correction <= 10
        begin
                -- Reset counter
                select  @i = 1
                -- Set capping threshold to be @ntile_correction
                update          capping_threshold_corrections
                        set     ntile_correction = @ntile_correction

                execute logger_add_event @CP3_build_ID, 3, 'B01: Capping threshold corrections updated to ' || @ntile_correction, @@error

                -- B02: Run capping for @capping_date, using value of ntile_correction from #capping_threshold_correction
                while @i <= @max_i
                begin
                        select @capping_date = (select capping_date from CA3_capping_days where seq_id = @i)
                        execute logger_add_event @CP3_build_ID, 3, 'B02: Updated @capping_date to ' || @capping_date

                        -- Call capping procedure
                        execute CP2_build_days_caps @capping_date, @CP3_build_ID, 1
                        execute logger_add_event @CP3_build_ID, 3, 'B03: Capping script finished for ' || @capping_date || ' with error = ' || @@error

                        -- B03: Create hourly AUGs tables containing Vespa data for dates of interest / ntile correction.
                        -- Once capping is done, use following procedure to calculate average hourly viewing per Vespa HH
                        execute CP3_Vespa_augs_table @capping_date, @ntile_correction, @CP3_build_ID
                        execute logger_add_event @CP3_build_ID, 3, 'B03: Augmented data table created for ' || @capping_date || ' with error = ' || @@error

                        -- B04: Collate Vespa hourly AUGs tables
                        -- Once capping is done for all days of interest fill out table which contains comparisons between
                        -- BARB / Tech Edge data and the values from Vespa for this level of capping
                        execute CP3_capping_comparison_table @ntile_correction, @CP3_build_ID
                        execute logger_add_event @CP3_build_ID, 3, 'B04: Augmented data saved for @ntile_correction = ' || @ntile_correction || ' with error = ' || @@error

                        select @i = @i + 1
                end

                select @ntile_correction = @ntile_correction + 2

        end
        commit
        execute logger_add_event @CP3_build_ID, 3, 'Loop complete with error = ' || @@error

        -- B05: Update hourly AUGs tables with BARB data
        -- Once capping is done for all days of interest and all ntile values add on BARB data.
        -- Do in two parts due to names being different in each table
        -- Need to adjust this is we split Playblack and VOSDAL
        -- Could also look at making names in BARB table the same as Vespa. sometime when DB1 is being looked at
        update      CP3_capping_calibration_comparison ccom
                set BARB_avg_viewing_time_per_HH = avg_viewing_time_per_HH
               from temp_BARB_augs      tbarb
              where ccom.event_date = tbarb.event_date
                and ccom.start_hour = tbarb.start_time
                and ccom.type_of_event = 'LIVE'
                and tbarb.event = 'Live only'

        update      CP3_capping_calibration_comparison ccom
                set BARB_avg_viewing_time_per_HH = avg_viewing_time_per_HH
               from temp_BARB_augs      tbarb
              where ccom.event_date = tbarb.event_date
                and ccom.start_hour = tbarb.start_time
                and ccom.type_of_event  = 'PLAYBACK'
                and tbarb.event <> 'Live only'
        execute logger_add_event @CP3_build_ID, 3, 'B05: Updated augmented data table with BARB data with error = ' || @@error

        -- B06: Run the procedure CP3_capping_comparison_metric to calculate metrics of interest
        execute CP3_capping_comparison_metric @CP3_build_ID
        execute logger_add_event @CP3_build_ID, 3, 'B06: Metrics calculated with error = ' || @@error

--         -- B07: Revert capping threshold values back to original values
--         if object_id('capping_threshold_corrections') is not null drop table capping_threshold_corrections
--         select *
--                 into capping_threshold_corrections
--                 from historical_capping_threshold_correction

        -- B08: Update capping correction rules and rerun capping for all dates being looked at
        -- For some reason you have to set weekday indicators as part of the SQL statement
        declare @min_ind int
        declare @max_ind int
        set     @min_ind = (select min(weekday_indicator) from CP3_comparison_metrics)
        set     @max_ind = (select min(weekday_indicator) from CP3_comparison_metrics)

        if (@min_ind = 0)
        begin
            update capping_threshold_corrections ctc
               set ctc.ntile_correction = ccm.capping_correction
              from CP3_comparison_metrics ccm
             where ccm.type_of_event = 'LIVE'
               and ccm.rank = 1
               and ctc.live_timeshifted_events = 0
               and ctc.time_period = ccm.time_period
--                and ccm.event_date = @capping_date
               and ccm.weekday_indicator = 0
               and ctc.weekday_indicator = 0

            update capping_threshold_corrections ctc
               set ctc.ntile_correction = ccm.capping_correction
              from CP3_comparison_metrics ccm
             where ccm.type_of_event <> 'LIVE'
               and ccm.rank = 1
               and ctc.live_timeshifted_events > 0
--                and ccm.event_date = @capping_date
               and ccm.weekday_indicator = 0
               and ctc.weekday_indicator = 0
        end

        if (@max_ind = 1)
        begin
            update capping_threshold_corrections ctc
               set ctc.ntile_correction = ccm.capping_correction
              from CP3_comparison_metrics ccm
             where ccm.type_of_event = 'LIVE'
               and ccm.rank = 1
               and ctc.live_timeshifted_events = 0
               and ctc.time_period = ccm.time_period
--                and ccm.event_date = @capping_date
               and ccm.weekday_indicator = 1
               and ctc.weekday_indicator = 1

            update capping_threshold_corrections ctc
               set ctc.ntile_correction = ccm.capping_correction
              from CP3_comparison_metrics ccm
             where ccm.type_of_event <> 'LIVE'
               and ccm.rank = 1
               and ctc.live_timeshifted_events > 0
--                and ccm.event_date = @capping_date
               and ccm.weekday_indicator = 1
               and ctc.weekday_indicator = 1
        end

        set     @i  = 1
        while   @i <= @max_i
        begin
            select @capping_date = (select capping_date from CA3_capping_days where seq_id = @i)
            execute CP2_build_days_caps @capping_date, @CP3_build_ID
        end


end; -- CP3_capping_calibration
commit;

go

CP3_capping_calibration '2013-09-23', '2013-09-29', 1, '46'

