
-- Capping calibration - this procedure create AUG tables without any capping applied so they can
-- go through the automatic summary procees


if object_id('CP2_no_capping_simulation') is not null drop procedure CP2_no_capping_simulation;
commit;

go

create procedure CP2_no_capping_simulation
     @target_date       date = NULL     -- Date of daily table caps to cache
    ,@CP2_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

    --------------------------------------------------------------------------------
    -- A01) SET UP.
    --------------------------------------------------------------------------------

    -- create and populate variables
    declare @playback_days          tinyint         -- How many days back worth of playback viewing data to consider
    set     @playback_days = 28                     -- Want to be able to treat timeshifting of up to 28 days

    declare @BARB_day_cutoff_time   time            -- because we don't know if the day starts at 2AM or 4 or 6 or 9 and this means it's easily changed
    set     @BARB_day_cutoff_time = '02:00:00'      -- Treating the day as 2AM to 2AM. Currently not really using this though?

    declare @max_cap_bound_minutes  integer         -- The largest a capping bound can possibly be. This is important as we have to get viewing records out of the previous day's table to cap ea
    set     @max_cap_bound_minutes = 120

    declare @min_cap_bound_minutes  integer         -- The smallest a capping bound can possibly be.
    set     @min_cap_bound_minutes = 20

    declare @min_view_duration_sec  tinyint         -- The bound below which views are ignored, in seconds
    set     @min_view_duration_sec  = 6

    declare @uncapped_sample_pop_max integer        -- The maximum number of uncapped events to consider per bucket for selecting new end times for each capped event
    set     @uncapped_sample_pop_max = 10000        -- About 2.5 hours to process the matching loop for one day; good thing we're scheduling these overnight
                                                    -- Update: Now pulled back to 10k given we're also batching by initial channel now too

    declare @var_sql                varchar(15000)   -- For dynamic SQL over daily tables, though we're only capping one day so there's no looping
    declare @QA_catcher             integer

    -- Dev purposes only:
    --set @target_date = '2012-01-30';
    -- For dev build, we're going through to the end of Feb.

    execute logger_add_event @CP2_build_ID, 3, 'A01: CP2 caching caps for ' || convert(varchar(10),@target_date,123)
    commit


    --------------------------------------------------------------------------------
    -- B) - Get The viewing Data
    --------------------------------------------------------------------------------

    --------------------------------------------------------------------------------
    -- B02 - Get the viewing data
    --------------------------------------------------------------------------------
    if object_id('Capping2_01_Viewing_Records') is not null drop table Capping2_01_Viewing_Records
    create table Capping2_01_Viewing_Records (
        cb_row_ID                           bigint          not null primary key
        ,Account_Number                     varchar(20)     not null
        ,Subscriber_Id                      integer
        ,X_Type_Of_Viewing_Event            varchar(40)     not null
        ,Adjusted_Event_Start_Time          datetime
        ,X_Adjusted_Event_End_Time          datetime
        ,X_Viewing_Start_Time               datetime
        ,X_Viewing_End_Time                 datetime
        ,Tx_Start_Datetime_UTC              datetime
        ,X_Event_Duration                   decimal(10,0)
        ,X_Programme_Viewed_Duration        decimal(10,0)
        ,Programme_Trans_Sk                 bigint          not null
        ,daily_table_date                   date            not null
        ,live                               bit
        ,genre                              varchar(25)
        ,sub_genre                          varchar(25)
        ,epg_channel                        varchar(30)
        ,channel_name                       varchar(30)
        ,program_air_date                   date
        ,program_air_datetime               datetime
        ,event_start_day                    integer         default null
        ,event_start_hour                   integer         default null
        ,box_subscription                   varchar(1)      default 'U'
        ,initial_genre                      varchar(30)
        ,initial_channel_name               varchar(30)
        ,pack                               varchar(100)
        ,pack_grp                           varchar(30)
        ,bucket_id                          integer
    )

    create hg   index idx2 on Capping2_01_Viewing_Records (Subscriber_Id)
    create dttm index idx3 on Capping2_01_Viewing_Records (Adjusted_Event_Start_Time)
    create dttm index idx4 on Capping2_01_Viewing_Records (X_Viewing_Start_Time)
    create hg   index idx5 on Capping2_01_Viewing_Records (bucket_id)
    create lf   index idx6 on Capping2_01_Viewing_Records (event_start_hour)
    create lf   index idx7 on Capping2_01_Viewing_Records (event_start_day)



    declare @varBroadcastMinDate  int
    declare @varBroadcastMaxDate  int
    declare @varEventStartHour    int
    declare @varEventEndHour      int

    set @varBroadcastMinDate  = (dateformat(@target_date - @playback_days, 'yyyymmdd00'))
    set @varBroadcastMaxDate  = (dateformat(@target_date, 'yyyymmdd23'))          -- Broadcast to start no later than at 23:59 on the day
    set @varEventStartHour    = (dateformat(@target_date - 1, 'yyyymmdd23'))      -- Event to start no earlier than at 23:00 on the previous day
    set @varEventEndHour      = (dateformat(@target_date, 'yyyymmdd23'))          -- Event to start no later than at 23:59 on the next day

    insert into Capping2_01_Viewing_Records
    select
           pk_viewing_prog_instance_fact              as Cb_Row_Id
          ,Account_Number                             as Account_Number
          ,Subscriber_Id                              as Subscriber_Id
          ,Type_Of_Viewing_Event                      as X_Type_Of_Viewing_Event
          ,EVENT_START_DATE_TIME_UTC                  as Adjusted_Event_Start_Time
          ,EVENT_END_DATE_TIME_UTC                    as X_Adjusted_Event_End_Time
          ,INSTANCE_START_DATE_TIME_UTC               as X_Viewing_Start_Time
          ,INSTANCE_END_DATE_TIME_UTC                 as X_Viewing_End_Time
          ,BROADCAST_START_DATE_TIME_UTC              as Tx_Start_Datetime_UTC
          ,Duration                                   as X_Event_Duration
          ,datediff(second,INSTANCE_START_DATE_TIME_UTC, INSTANCE_END_DATE_TIME_UTC)
                                                      as X_Programme_Viewed_Duration
          ,dk_programme_instance_dim                  as Programme_Trans_Sk
          ,@target_date                               as Daily_Table_Date
          ,case when REPORTED_PLAYBACK_SPEED is null then 1           else 0                        end as live
          ,case when Genre_Description       is null then 'Unknown' else Genre_Description        end as genre
          ,case when Sub_Genre_Description   is null then 'Unknown' else Sub_Genre_Description    end as sub_genre
          ,null                                       as epg_channel               -- epg_channel
          ,channel_name                               as channel_name
          ,date(BROADCAST_START_DATE_TIME_UTC)        as program_air_date
          ,BROADCAST_START_DATE_TIME_UTC              as program_air_datetime
          ,datepart(day,
                    case
                        when (EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- prior Mar 12 - no change, consider UTC = local
                        when (EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        when (EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 13-Mar 14 => UTC = Local
                          else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
                      end)                            as event_start_day
          ,datepart(hour,
                    case
                        when (EVENT_START_DATE_TIME_UTC <  '2012-03-25 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- prior Mar 12 - no change, consider UTC = local
                        when (EVENT_START_DATE_TIME_UTC <  '2012-10-28 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 12-Oct 12 => DST, add 1 hour to UTC (http://www.timeanddate.com/worldclock/timezone.html?n=136)
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        when (EVENT_START_DATE_TIME_UTC <  '2014-03-30 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 13-Mar 14 => UTC = Local
                          else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
                      end)                            as event_start_hour
          ,'U'                                        as box_subscription          -- "Unknown" placeholder for box subscription status (P/S)
          ,null                                       as initial_genre             -- Initial genre
          ,null                                       as initial_channel_name      -- Initial channel (not that it changes)
          ,null                                       as pack                      -- Pack
          ,null                                       as pack_grp                  -- Pack group
          ,null                                       as bucket_id                 -- bucket_id
     from sk_prod.VESPA_EVENTS_ALL
    where (REPORTED_PLAYBACK_SPEED is null or REPORTED_PLAYBACK_SPEED = 2)
      and Duration > @min_view_duration_sec
      and Panel_id = 12
      and type_of_viewing_event <> 'Non viewing event'
      and DK_BROADCAST_START_DATEHOUR_DIM >= @varBroadcastMinDate
      and DK_BROADCAST_START_DATEHOUR_DIM <= @varBroadcastMaxDate
      and account_number is not null
      and DK_EVENT_START_DATEHOUR_DIM >= @varEventStartHour         -- Start with 2300 hours on the previous day to pick UTC records in DST time (DST = UTC + 1 between April & October)
      and DK_EVENT_START_DATEHOUR_DIM <= @varEventEndHour           -- End up with additional records for the next day, up to 04:00am
      and subscriber_id is not null                                 -- There shouldnt be any nulls, but there are
    commit

    -- Start off the control totals:
    if object_id('Capping2_tmp_Uncleansed_Viewing_Totals') is not null drop table Capping2_tmp_Uncleansed_Viewing_Totals

    select
          subscriber_id,
          round(sum(datediff(ss, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0, 0) as total_box_viewing
      into Capping2_tmp_Uncleansed_Viewing_Totals
      from Capping2_01_Viewing_Records
     where daily_table_date = @target_date
     group by subscriber_id
    commit

    delete from CP2_QA_daily_average_viewing
     where build_date = @target_date
    commit

    insert into CP2_QA_daily_average_viewing (build_date ,subscriber_count ,average_uncleansed_viewing)
      select
            @target_date,
            count(1),
            avg(total_box_viewing)
        from Capping2_tmp_Uncleansed_Viewing_Totals
    commit

    if object_id('Capping2_tmp_Uncleansed_Viewing_Totals') is not null drop table Capping2_tmp_Uncleansed_Viewing_Totals
    commit


    set @QA_catcher = -1

    select @QA_catcher = count(1)
      from Capping2_01_Viewing_Records

    execute logger_add_event @CP2_build_ID, 3, 'B01: Extract raw viewing completed', coalesce(@QA_catcher, -1)
    commit



    -------------------------------------------------------------------------------------------------
    -- SECTION C: CREATE THE CAPS
    -------------------------------------------------------------------------------------------------

    /* All capping removed */

    -------------------------------------------------------------------------------------------------
    -- D06) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE
    -------------------------------------------------------------------------------------------------

    delete from CP2_capped_data_holding_pen

    commit

    -- First we're only adding the capped data, have to remember to throw in
    -- the uncapped stuff too at a later stage when all the capping processing
    -- is done (but before the generic stuff like BARB minute and scaling
    -- weighting coeficients)
    insert into CP2_capped_data_holding_pen (
        cb_row_id
        ,subscriber_id
        ,account_number
        ,programme_trans_sk
        ,adjusted_event_start_time
        ,X_Adjusted_Event_End_Time
        ,x_viewing_start_time
        ,x_viewing_end_time
        ,capped_event_end_time
        ,timeshifting
        ,capped_flag
        -- Things we need for control totals:
        ,program_air_date
        ,live
        ,genre
    )
    select
        vr.cb_row_id
        ,vr.subscriber_id
        ,vr.account_number
        ,vr.programme_trans_sk
        ,vr.adjusted_event_start_time
        ,vr.X_Adjusted_Event_End_Time
        ,vr.x_viewing_start_time
        ,vr.x_viewing_end_time
        ,null                       -- this is "capped_event_end_time" - simulating "no capping"
        ,case
            when vr.live = 1 then 'LIVE'
            when vr.live = 0 then 'TIMESHIFT'   -- Will later be replaced with 'VOSDAL' or 'PLAYBACK7' or 'PLAYBACK28'
            else 'FAIL!'                        -- ## QA Check that there aren't any of these
          end
        ,0  --case
            --  when cewe.subscriber_id is not null then 11 -- 11 for things that need capping treatment
            --  else 0                                      -- 0 for no capping
            --end
        ,vr.program_air_date
        ,vr.live
        ,vr.genre
    from Capping2_01_Viewing_Records as vr
    --left join CP2_capped_events_with_endpoints as cewe
    --on  cewe.subscriber_id             = vr.subscriber_id
    --and cewe.adjusted_event_start_time = vr.adjusted_event_start_time

    commit


    execute logger_add_event @CP2_build_ID, 3, 'D06: Midway 1/3 (Populate holding pen)', coalesce(@QA_catcher, -1)
    commit

    -- More column renamiings: because we're not on the
    --  capped_x_viewing_start_time         => viewing_starts
    --  capped_x_viewing_end_time           => viewing_stops
    --  capped_x_programme_viewed_duration  => viewing_duration
    -- We're also discontinuing use of the Capping2_01_Viewing_Records table, because
    -- we've grabbed everything we want and are now just working in the holding
    -- pen. Actually, we could empty out the vireing records table (wait, have
    -- we imported all the uncapped data yet?)

    --update table to create revised start and end viewing times
/* ### THIS IS NOT NEEDED AS VIEWING TIMES ARE NOT MODIFIED ###
    update CP2_capped_data_holding_pen
    set viewing_starts = case
            -- if start of viewing_time is beyond capped end time then flag as null
            when capped_event_end_time <= x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
        end
       ,viewing_stops = case
            -- if start of viewing_time is beyond capped end time then flag as null
            when capped_event_end_time <= x_viewing_start_time then null
            -- if capped event end time is beyond end time then leave end time unchanged
            when capped_event_end_time > x_viewing_end_time then x_viewing_end_time
            -- if capped event end time is null then leave end time unchanged
            when capped_event_end_time is null then x_viewing_end_time
            -- otherwise set end time to capped event end time
            else capped_event_end_time
        end
    where capped_flag = 11  -- Only bother with the capped events...

    commit
*/

    -- And now the more basic case where there's no capping;
    update CP2_capped_data_holding_pen
    set viewing_starts = x_viewing_start_time
        ,viewing_stops = x_viewing_end_time
    where capped_flag = 0     -- always met

    commit

    --calculate revised programme viewed duration
    update CP2_capped_data_holding_pen
    set viewing_duration = datediff(second, viewing_starts, viewing_stops)

    commit

    --set capped_flag based on nature of capping
    --1 programme view not affected by capping
    --2 if programme view has been shortened by a long duration capping rule
    --3 if programme view has been excluded by a long duration capping rule

    --identify views which need to be capped
/* ### THIS IS NOT NEEDED AS VIEWING TIMES ARE NOT MODIFIED ###
    update CP2_capped_data_holding_pen
    set capped_flag = case
        when viewing_stops < x_viewing_end_time then 2
        when viewing_starts is null then 3
        else 1 end
    where capped_flag = 11

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where viewing_duration is not null

    execute logger_add_event @CP2_build_ID, 3, 'D06: Midway 2/3 (Calculate view bounds)', coalesce(@QA_catcher, -1)
    commit


    delete from  CP2_capped_data_holding_pen
    where capped_flag = 3

    commit
*/

    execute logger_add_event @CP2_build_ID, 3, 'D06: Complete! (Capping on viewing table)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D07) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPING
    -------------------------------------------------------------------------------------------------

    /* ##QA##EQ##: Pivot the results of this extraction query in Excel I guess:
    select * from CP2_QA_viewing_control_totals
    order by data_state, program_air_date, live, genre
    */

    /* What we expect:
        *. '1.) Collect' should match '2.) Pre-Cap'
        *. '4a.) Uncapped' + '4b.) Unafected' + '4d.) Truncated' should add up to '3.) Capped',
        *. '4a.) Uncapped' + '4b.) Unafected' + '4c.) Excluded' + '4d.) Truncated' + '4e.) T-Margin' should add up to '1.) Collect'
    They should match pretty much exactly, since we've rounded everything to 2dp in hours.
    */

    set @QA_catcher = -1

    execute logger_add_event @CP2_build_ID, 3, 'D07: NYIP! (Total viewing before / after capping)'
    commit



    --------------------------------------------------------------------------------
    -- E01  Add Playback and Vosdal flags to the viewing data
    --------------------------------------------------------------------------------

    /*
     vosdal (viewed on same day as live) and playback = within 7 days of air

     viewing_starts - adjusted for playback and capped events, otherwise it is the original viewing time

    */

    /* Don't need this, we're just playing with the timeshifting flag
    --Add the additional fields to the viewing table
    ALTER TABLE CP2_capped_data_holding_pen
            add (VOSDAL             as integer default 0
                ,Playback           as integer default 0
                ,playback_date      as date
                ,playback_post7     as integer default 0 );
    */

    -- Update the fields:
    Update CP2_capped_data_holding_pen
      set timeshifting = case
                          when live = 1                                                                 then 'LIVE'

                          when date(viewing_starts) = program_air_date                                  then 'VOSDAL'

                          when date(viewing_starts) > program_air_date and
                               viewing_starts <= dateadd(hour, 170, cast(program_air_date as datetime)) then 'PLAYBACK7'

                          when viewing_starts > dateadd(hour, 170, cast(program_air_date as datetime))  then 'PLAYBACK28'

                        end

    /* The old build:
     set  VOSDAL        = (case when viewing_starts <= dateadd(hh, 26,cast( cast( program_air_date as date) as datetime)) and live = 0
                                                                                                                                     then 1 else 0 end)

         ,Playback       =       (case when viewing_starts <= (dateadd(hour, 170, program_air_date))
                                       and  viewing_starts > (cast(dateadd(hour,26,program_air_date)  as datetime))and live = 0 then 1 else 0 end)

         ,playback_post7 =       (case when viewing_starts > (dateadd(day, 170, program_air_date))and live = 0
                                                                                                                            then 1 else 0 end); -- flag these so identifying mismatchs is easy later
    */
    --select top 10 * from CP2_capped_data_holding_pen where vosdal = 1
    --select top 10 * from CP2_capped_data_holding_pen where playback = 1
    --select top 10 * from CP2_capped_data_holding_pen where playback_post7 = 1

    -- So at this point all the capping and processing is done, and we can transfer these guys
    -- to the dynamic daily augmentation table.

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen
    where timeshifting is not null

    execute logger_add_event @CP2_build_ID, 3, 'E01: Complete! (Calculate view bounds)', coalesce(@QA_catcher, -1)
    commit



    --------------------------------------------------------------------------------
    -- PART G) - DYNAMIC DAILY CAPPED TABLE
    --------------------------------------------------------------------------------
      -- So we've done all this work, and now we need to dynamically put the results
      -- into a table named after the same day as the source daily table. Oh and we have
      -- to leave off the items that were in the previous daily table which we only
      -- included to get the right capping behaviour for early items.

    --------------------------------------------------------------------------------
    -- G01) - DYNAMIC DAILY AUGMENTATION TABLE: CREATION AND PERMISSIONS
    --------------------------------------------------------------------------------
    set @var_sql = '
                    if object_id(''V079_Daily_Events_Capped'') is not null drop table V079_Daily_Events_Capped

                    create table V079_Daily_Events_Capped (
                          Cb_Row_Id                   bigint              primary key,    -- Links to the viewing data daily table of the same day
                          Subscriber_Id               bigint              not null,
                          Account_Number              varchar(20)         not null,
                          Programme_Trans_Sk          bigint,                             -- to help out with the minute-by-minute stuff
                          Scaling_Segment_Id          bigint,                             -- To help with the MBM proc builds.... -- NYIP!
                          Scaling_Weighting           float,                              -- Also assisting with the MBM proc builds -- NYIP!
                          Viewing_Starts              datetime,                           -- Capped viewing start time (UTC time)
                          Viewing_Stops               datetime,
                          Viewing_Duration            bigint,                             -- Capped viewing in seconds
                          BARB_Minute_Start           datetime,                           -- Viewing with Capping treatment + BARB minute allocation -- NYIP!
                          BARB_Minute_End             datetime,                           -- NYIP!
                          Timeshifting                varchar(10),
                          Capped_Flag                 tinyint,                            -- 0-2 depending on capping treatment: 0 -> event not capped, 1 -> event capped but does not effect viewing, 2 -> event capped & shortens viewing, 3 -> event capped & excludes viewing (actually 3 will not turn up in the table, but that is what it means during processing)
                          Capped_Event_End_Time       datetime                            -- Only populated for capped events
                    )

                    create hg   index idx1 on V079_Daily_Events_Capped (Subscriber_Id)
                    create hg   index idx2 on V079_Daily_Events_Capped (Account_Number)
                    create hg   index idx3 on V079_Daily_Events_Capped (Programme_Trans_Sk)
                    create dttm index idx4 on V079_Daily_Events_Capped (Viewing_Starts)
                    create dttm index idx5 on V079_Daily_Events_Capped (Viewing_Stops)
                    '
    commit

    execute(@var_sql)
    commit


    --------------------------------------------------------------------------------
    -- G02) - DYNAMIC DAILY AUGMENTATION TABLE: POPULATION
    --------------------------------------------------------------------------------
      -- Specifically no filters here as we're running the QA actions over CP2_capped_data_holding_pen
      -- since that makes it a lot easier to get the totals and checks etc into logger than when
      -- doing everything dynamically of the daily augmented tables.
    set @var_sql = '
                    insert into V079_Daily_Events_Capped
                    select
                        cb_row_id
                        ,subscriber_id
                        ,account_number
                        ,programme_trans_sk
                        ,scaling_segment_id
                        ,scaling_weighting
                        ,viewing_starts
                        ,viewing_stops
                        ,viewing_duration
                        ,BARB_minute_start
                        ,BARB_minute_end
                        ,timeshifting
                        ,capped_flag
                        ,capped_event_end_time
                      from CP2_capped_data_holding_pen
                  '
    commit

    execute(@var_sql)
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
      from CP2_capped_data_holding_pen
     where date(viewing_starts) = @target_date

    execute logger_add_event @CP2_build_ID, 3, 'G02: Aug table completed', coalesce(@QA_catcher, -1)


end; -- procedure CP2_build_days_caps

commit;
go










