-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
--            This version uses the NEW data model (i.e VESPA_DP_PROG_VIEWED_CURRENT etc.)
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################

/******************************************************************************
**
** Project Vespa: Capping 2 - Daily update process
**
** This script has the big procedure which calculates the caps for one day and
** puts the results in it's own dynamically named table, in kind of the same
** naming convention as the daily tables with all the data.
**
** One complication in this build is the treatment of events that span the BARB
** day, and really, we have to make sure that the capping record is in the same
** daily table as the viewing record to which it applies. That said, we can throw
** those checks in as a unit test thing.
**
** Refer also to:
**
**  http://rtci/Vespa1/Capping.aspx
**
** Though, heh, currently that still refers to Capping 1, but that'll get an
** update soon, and will be much happier for it.
**
** So we're building caps with viewing timeshifted by up to 28 days. That is,
** if anything is timeshifted by any more than 28 days, it'll cease to exist
** for the Vespa universe. We're also building these things one day at a time
** with no more than one day of viewing data in the processing loop at any one
** time. Still don't know exactly how we're treating the caps that spill over
** the daily viewing boundry... the maximum cap limit is four hours, yes, so
** that's as far as we'd have to go back? Well, there's also the updates to
** manage; I think for each loop we'll consider the juristiction of each loop
** to be all those records which are just on the daily table of question. We
** are going back however many days to cap the timeshifted stuff, so broadcasts
** around the boundaries are not so much of an issue at all. Means we have to
** get a chunk of viewing from the previous daily table, starting at whatever
** the absolute maximum of cap bound is (which is a shame because that will
** bleed into the previous day's peak...)
**
**
** Code sections:
**
**  A) SET UP   A01 - Initialising required variables, notifying the Logger
**
**  B) GET THE VIEWING DATA
**
**              B01 - Get the viewing data from VESPA
**
**  C) CREATE THE CAPS
**
**              C01* - Collect viewing data
**              C02 - Condense viewing data into event listing
**              C03 - Add the customer DB metadata (some non-cust DB sources too)
**              C04 - Make all the Ntiles
**              C05 - These are tables of ntiles?
**              C06 - All the different tables of caps
**              C07 - Moving caps back onto central tables
**              C08 - Global capping bounds
**              C09 - Distribution of capping bounds (just for QA)
**
**  D) APPLY CAPS TO VIEWING DATA
**
**              D01* - Adding customer metadata to viewing
**              D02 - Determine capping application
**              D03 - Get bounds on duration-replacement lookup
**              D04 - Randomly choose replacement duration for capped events
**              D05 - Assign new end time
**              D06* - Put capping on original main viewing table
**              D07 - Investigation of total viewing during process
**              D08 - Profiling durations before and after capping
**
**  E) ADD ADDITIONAL FIELDS TO THE VIEWING DATA
**
**              E01 - Add Playback and Vosdal flags
**
**  G) TRANSFER CAPPED DATA TO DYNAMIC TABLE
**
**              G01 - Table creation - using same YYYYMMDD datestamp format
**              G02 - Dynamic table population
**
**  J) WEEKLY PROFILING BUILD (DIFFERENT PROC TO THE DAILY CAP BUILD)
**
**              J01 - Table cleansing
**              J02 - Primary / Secondary box flags
**              J03 - ...
**
**  R) RESET PROCEDURE - FOR ALL THE TEMPORARY / TRASNIENT THINGS, NOT THE HISTORIC DAILY AUGMENT DATA
**
** *: control totals on viewing data total duration taken after these sections
**
** Also: QA Extraction are marked by "##QA##EQ##" so do a search for that string to find the
** queries to run for control totals, etc. They'll be commented out as they're not part of
** the main build, but everything they need should be constructed.
**
** So: We've also got BARB minutes at the end of this script. Think we might build
** those into the Capping Cache as well, because it's useful. Let us do minute by
** minute really *really* easily.
**
** That said, this guy is still going to receieve massive rebuilding to process only
** a day at a time, actually cache things historically, log control totals, even get
** out some controlled builds for QA, generally all that good stuff.
**
** Things to do:
** 7/ Automated QA, including... total hours watched? split by what?
** 12/ Then we need to rebuild the thing again for the Phase 2 viewing data structure
** 17/ There's no use of "recorded_time_utc" in here at all? How are we managing the
**      timeshifting of stuff, by broadcast date or by consumption date? BARB minute
**      etc really wants to be done by consumption date. From memory "x_viewing_start_time"
**      isn't properly set for all records and you have to juggle "recorded_time_utc"
**      yourself, so, yeah, we may have to poke around with that. We've got a good query
**      from Jonathan to do it, so that'll make some things easier to manage. Then we'll
**      get a bit closer to some consistent treatment of stuff...
** 18/ Needs better sectioning around the BARB minute stuff. BARB NYIP at all thus far.
**      Update: This is also going to involve changing the lower bound treatment, since
**      it's done at the moment by excluding these things as they come out of the daily
**      table. See the note at ##18##.
** 19/ Still don't have the scaling segments on it. Are we adding the scaling segments?
**      Maybe in a future release, or at least, not in the thing we're urgently pushing
**      out for the BARB v Vespa for Chris. Scaling segments also NYIP.
** 20/ Get the timeshifting treatment right, currently it doesn't wory at all. Might be
**      as easy as making sure that programme air date is included in the initial pull
**      though. Update: Got a patch, have yet to check that it works entirely.
**
** Things done:
** 1/ Define all the major tables being used
** 2/ Introduce some normalisation for efficiency
** 3/ Pull back to 1 day processing at once
** 4/ Tidy up the redundancy and terrible IO wastage
** 13/ Might think about getting some other grouping flag for the buckets within which
**      caps get built, so that we only have to join on the big combination of stuff
**      once (to get the bucket IDs back) rather than having to join on 6 fields all
**      the time... going to have to do something major if it's taking hours and hours
**      to build a single day of caps.
** 15/ Going to have to fully rebuild the bit where capped events get their new end
**      times: flatted the population out into a distributon with a fixed number of
**      entries and the cumulative-intervals trick for the random thing selection.
** 5/ Put results into a cache for each day
** 16/ Figure out if we're going to include the BARB minute and timeshift split stuff
**      into this build too, because that'd allow us to build the minute by minute
**      graphs really easily from the stored procedures again, and that'd e brilliant,
**      as it would include scaling and capping too and give us BARB MBM over the full
**      Sky base in one little procedure that takes maybe a minute to run.
** 11/ Convert the BARB minute allocation back to a lookup on cb_row_id. Though: How
**      do we allign the BARB minute definitions to shows that don't line up on exact
**      minute ends in the EPG? Are there any? Let's check... Update: that BARB minute
**      issue around the programme endpoints is automatically resolved, as we're not
**      grouping by the programe key or cb_row_id at all, we're just extending the
**      first viewing item contributing to the dominant component in the BARB minute.
**      Doing the BARB minute by minute for the later show which has minutes in the
**      earlier programme tag could be annoying though. We'll just flag that known
**      issue and point out that it doesn't happen with programmes that start and end
**      exactly on minute bounds.
** 6/ Logger calls, ongoing QA totals...
** 8/ Form as a stored procedure
** 10/ Historic rebuild, of course. Will be huge. Going to have to find out how much we
**      need. Update: we've set it up so that it automatically rebuilds periods of high
**      demand and then chuggs away with everything else when it runs out of high priority
**      items.
** 9/ Scheduler integration
** 14/ Need a procedue which resets all the capping stuff from the vespa_analysts schema
**      because there's a lot of junk that ends up in there that should be reset once
**      we're happy with the QA. Which is kind of odd because we're only ever going to
**      have the last day's worth of caps in it when the build finishes. That said, we
**      will be able to isolate one day to build if we need to check one particular QA
**      issue or something like.
**
** How far back are we refreshng the thing? Scaling goes back what, three weeks, do
** we intent to build three weeks worth of capps each week? could take a while to
** compute, good thing we're doing it at night...
**
** Sybase 15 client note: Sybase has some annoying behavioural changes from the
** client for Sybase 12, and if you have the wrong client settings you might get
** syntax errors (really? WTF?). The setting you need are hidden under:
**  Tools->Options->SybaseIQ->Commands
** There are two check boxes, "Commit after every statement" and "Commit on exit
** or disconnect" and you want to make sure both are ticked.
**
******************************************************************************/

-- We've also got a few cases where *everything* ends up with the capped flag set;
-- how are we establishing the correct caps when we have no universe of uncapped
-- to choose from?

if object_id('CP2_build_days_caps') is not null then drop procedure CP2_build_days_caps end if;
commit;

go

create procedure CP2_build_days_caps
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
    -- Removing redundant tables which do not longer exist or has been renamed
    --------------------------------------------------------------------------------
    if object_id('CP2_Viewing_Records')                 is not null drop table CP2_Viewing_Records
    if object_id('CP2_01_Viewing_Records')              is not null drop table CP2_01_Viewing_Records
    if object_id('Uncleansed_Viewing_Totals')           is not null drop table Uncleansed_Viewing_Totals
    if object_id('CP2_4BARB_internal_viewing')          is not null drop table CP2_4BARB_internal_viewing
    if object_id('CP2_4BARB_view_endpoints')            is not null drop table CP2_4BARB_view_endpoints
    if object_id('cumulative_playback_corrections')     is not null drop table cumulative_playback_corrections
    if object_id('cumulative_playback_corrections_2')   is not null drop table cumulative_playback_corrections_2
    if object_id('CP2_viewing_control_cap_distrib')     is not null drop table CP2_viewing_control_cap_distrib
    if object_id('CP2_viewing_control_distribs')        is not null drop table CP2_viewing_control_distribs
    if object_id('CP2_viewing_control_totals')          is not null drop table CP2_viewing_control_totals


    --------------------------------------------------------------------------------
    -- B) - Get The viewing Data
    --------------------------------------------------------------------------------

    --------------------------------------------------------------------------------
    -- B02 - Get the viewing data
    --------------------------------------------------------------------------------
    if object_id('Capping2_01_Viewing_Records') is not null drop table Capping2_01_Viewing_Records
    create table Capping2_01_Viewing_Records (
         ID_Key                             bigint          primary key identity
        ,cb_row_ID                          bigint          not null
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
        ,Service_Key                        bigint
        ,daily_table_date                   date            not null
        ,genre                              varchar(25)
        ,sub_genre                          varchar(25)
        ,epg_channel                        varchar(30)
        ,channel_name                       varchar(30)
        ,program_air_date                   date
        ,program_air_datetime               datetime
        ,program_air_datetime_min           datetime
        ,live_timeshifted_events            Int
        ,followed_by_standbyin              tinyint
        ,event_start_day                    integer         default null
        ,event_start_hour                   integer         default null
        ,box_subscription                   varchar(1)      default 'U'
        ,initial_genre                      varchar(30)     default null
        ,initial_channel_name               varchar(30)     default null
        ,Initial_Service_Key                bigint          default null
        ,pack                               varchar(100)    default null
        ,pack_grp                           varchar(30)     default null
        ,bucket_id                          integer         default null
    )

    create hg   index idx2 on Capping2_01_Viewing_Records (Subscriber_Id)
    create dttm index idx3 on Capping2_01_Viewing_Records (Adjusted_Event_Start_Time)
    create dttm index idx4 on Capping2_01_Viewing_Records (X_Viewing_Start_Time)
    create hg   index idx5 on Capping2_01_Viewing_Records (bucket_id)
    create lf   index idx6 on Capping2_01_Viewing_Records (event_start_hour)
    create lf   index idx7 on Capping2_01_Viewing_Records (event_start_day)



    declare @varBroadcastMinDate  int
    declare @varEventStartHour    int
    declare @varEventEndHour      int

    set @varBroadcastMinDate  = (dateformat(@target_date - @playback_days, 'yyyymmdd00'))
    set @varEventStartHour    = (dateformat(@target_date - 1, 'yyyymmdd23'))      -- Event to start no earlier than at 23:00 on the previous day
    set @varEventEndHour      = (dateformat(@target_date, 'yyyymmdd23'))          -- Event to start no later than at 23:59 on the next day

    insert into Capping2_01_Viewing_Records
              (cb_row_ID,Account_Number,Subscriber_Id,X_Type_Of_Viewing_Event,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time,
               X_Viewing_Start_Time,X_Viewing_End_Time,Tx_Start_Datetime_UTC,X_Event_Duration,X_Programme_Viewed_Duration,Programme_Trans_Sk,
               Service_Key,daily_table_date,genre,sub_genre,epg_channel,channel_name,program_air_date,program_air_datetime,
               program_air_datetime_min,live_timeshifted_events,event_start_day,event_start_hour)
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
          ,Service_Key                                as Service_Key
          ,@target_date                               as Daily_Table_Date
          ,case when Genre_Description       is null then 'Unknown' else Genre_Description        end as genre
          ,case when Sub_Genre_Description   is null then 'Unknown' else Sub_Genre_Description    end as sub_genre
          ,null                                       as epg_channel               -- epg_channel
          ,channel_name                               as channel_name
          ,date(BROADCAST_START_DATE_TIME_UTC)        as program_air_date
          ,BROADCAST_START_DATE_TIME_UTC              as program_air_datetime
       ,min(BROADCAST_START_DATE_TIME_UTC) over (partition by Subscriber_Id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC) as BROADCAST_START_DATE_TIME_UTC_min
       ,case
            when live_recorded = 'RECORDED' and service_key in (4094,4095,4096,4097,4098) then 4
            when live_recorded = 'LIVE' then 0
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) = date(BROADCAST_START_DATE_TIME_UTC_min)
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_min,EVENT_START_DATE_TIME_UTC)/3600.0 as int) = 0
                    then 1
                    else 2 end
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) <> date(BROADCAST_START_DATE_TIME_UTC_min) then 3
        end as live_timeshifted_events
        ,null                                         as followed_by_standbyin
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

     from sk_prod.vespa_dp_prog_VIEWED_201309
    where live_recorded in ('LIVE','RECORDED')
      and Duration > @min_view_duration_sec                             -- Maintain minimum event duration
      and INSTANCE_START_DATE_TIME_UTC < INSTANCE_END_DATE_TIME_UTC     -- Remove 0sec instances
      and Panel_id = 12
      and DK_BROADCAST_START_DATEHOUR_DIM >= @varBroadcastMinDate
      and account_number is not null
      and BROADCAST_START_DATE_TIME_UTC is not null                     -- This was added by Patrick to eliminate the null BROADCAST_START_DATE_TIME_UTC for the 29th of Sept.
      and DK_EVENT_START_DATEHOUR_DIM >= @varEventStartHour             -- Start with 2300 hours on the previous day to pick UTC records in DST time (DST = UTC + 1 between April & October)
      and DK_EVENT_START_DATEHOUR_DIM <= @varEventEndHour               -- End up with additional records for the next day, up to 04:00am
      and subscriber_id is not null                                     -- There shouldnt be any nulls, but there are
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

    -------------------------------------------------------------------------------------------------
    -- C01) ASSEMBLE REQUIRED DAILY VIEWING DATA
    -------------------------------------------------------------------------------------------------
    if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1

    -- First off: Kick out the duplicates out that come in from the weird day wrapping stuff
    select subscriber_id, adjusted_event_start_time, X_Viewing_Start_Time, min(ID_Key) as Min_ID_Key
      into Capping2_tmp_View_dupe_Culling_1
      from Capping2_01_Viewing_Records
     group by subscriber_id, adjusted_event_start_time, X_Viewing_Start_Time
    commit

    create unique index idx1 on Capping2_tmp_View_dupe_Culling_1 (Min_ID_Key)
    commit

      -- Delete records with non-existing ID_Key in the deduped table
    delete from Capping2_01_Viewing_Records
      from Capping2_01_Viewing_Records a left join Capping2_tmp_View_dupe_Culling_1 b
        on a.ID_Key = b.Min_ID_Key
     where b.Min_ID_Key is null
   commit

    -- For logging and flagging and QA and stuff:

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from Capping2_tmp_View_dupe_Culling_1

    execute logger_add_event @CP2_build_ID, 3, 'C01: Midway 1/3 (Deduplication)', coalesce(@QA_catcher, -1)
    commit

    if object_id('Capping2_tmp_View_dupe_Culling_1') is not null drop table Capping2_tmp_View_dupe_Culling_1
    commit

    -- Deduplication complete!
    if object_id('Capping2_tmp_Cumulative_Playback_Corrections') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections
    if object_id('Capping2_tmp_Cumulative_Playback_Corrections_2') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections_2

    -- Need to fix the broken viewing times for the playback records: Still debugging this section ########
    select cb_row_id
        ,subscriber_id
        ,adjusted_event_start_time
        ,x_programme_viewed_duration
        ,rank() over (partition by subscriber_id, adjusted_event_start_time order by x_viewing_start_time, tx_start_datetime_utc desc, x_viewing_end_time,cb_row_id) as sequencer
        -- We've got one nasty duplicate thing in here; see cb_row_id 6463255125634728347 vs 6463255125650111897
        -- on the 11th of February: same subscriber ID and event start time, different end times, stuff like
        -- that shouldn't happen. Probably ordering by viewing end time is going to do terrible things to the
        -- viewing data (given it's for a correction in the playback sequence) but whatever, if it's a big deal
        -- the unit tests will catch it, and right now we just want something to run. If this doesn't work, we'll
        -- just clip everything from the conflicting events out of the data set, the loss won't be big at all.
    into Capping2_tmp_Cumulative_Playback_Corrections
    from Capping2_01_Viewing_Records
    where live_timeshifted_events in (1,2,3,4)

    -- Slight annoyance: Sybase won't let you order by anything other than numeric
    -- fields, so we still need this funny in-between table...
    commit
    create unique index sequencing_key on Capping2_tmp_Cumulative_Playback_Corrections (subscriber_id, adjusted_event_start_time, sequencer)
    commit

    select cb_row_ID
        ,cast(sum(x_programme_viewed_duration) over (
            partition by subscriber_id, adjusted_event_start_time
            order by sequencer
          ) as int) as x_cumul_programme_viewed_duration --Jon - this is the crazy thing where Sybase didn't like this field because it was an
                                                               --integer-expression, so we have to convert it to an integer
    into Capping2_tmp_Cumulative_Playback_Corrections_2
    from Capping2_tmp_Cumulative_Playback_Corrections

    commit

    create unique index fake_pk on Capping2_tmp_Cumulative_Playback_Corrections_2 (cb_row_ID)

    -- Push those back into the viewing table...
    update Capping2_01_Viewing_Records
    set x_viewing_end_time      = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
       ,x_viewing_start_time   = dateadd(second,-x_programme_viewed_duration,dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time))
    from Capping2_01_Viewing_Records
    inner join Capping2_tmp_Cumulative_Playback_Corrections_2 as cpc2
    on Capping2_01_Viewing_Records.cb_row_id = cpc2.cb_row_id

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from Capping2_tmp_Cumulative_Playback_Corrections_2


    IF object_id('Capping2_tmp_Cumulative_Playback_Corrections') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections
    IF object_id('Capping2_tmp_Cumulative_Playback_Corrections_2') is not null drop table Capping2_tmp_Cumulative_Playback_Corrections_2


    execute logger_add_event @CP2_build_ID, 3, 'C01: Midway 2/3 (Patch durations)', coalesce(@QA_catcher, -1)
    commit

    -- Finally, eliminate these "illegitimate" viewing records:

    --remove illegitimate playback views - these views are those that start on event end time and go beyond event end time
    delete from Capping2_01_Viewing_Records
    where X_Adjusted_Event_End_Time<x_viewing_end_time
    and x_viewing_start_time>=X_Adjusted_Event_End_Time
    -- Small, tiny, minisclue proportion of stuff...
    --2,596,912, 30m left
    --reset x_viewing_end_times for playback views
    update Capping2_01_Viewing_Records
    set x_viewing_end_time=X_Adjusted_Event_End_Time
    where X_Adjusted_Event_End_Time<x_viewing_end_time
    and x_viewing_start_time<X_Adjusted_Event_End_Time
    commit
    -- similarly tiny proportion.

    commit

    -- That table "Capping2_01_Viewing_Records" is the one that we take as our ball of viewing data.
    -- Okay, so we want some basic counts of total events and things like that, maybe even a
    -- profile of event duration distribution...
    if object_id('Capping2_tmp_Uncapped_Viewing_Totals') is not null drop table Capping2_tmp_Uncapped_Viewing_Totals

    select subscriber_id,
        round(sum(datediff(ss, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0, 0) as total_box_viewing
    into Capping2_tmp_Uncapped_Viewing_Totals
    from Capping2_01_Viewing_Records
    where daily_table_date = @target_date
    group by subscriber_id

    commit

    select @QA_catcher = avg(total_box_viewing)
    from Capping2_tmp_Uncapped_Viewing_Totals

    commit

    update CP2_QA_daily_average_viewing
    set average_uncapped_viewing = @QA_catcher
    where build_date = @target_date

    commit

    if object_id('Capping2_tmp_Uncapped_Viewing_Totals') is not null drop table Capping2_tmp_Uncapped_Viewing_Totals

    -- We've got a bunch of instances where we clear any current control totals out of the QA tables
    -- so that there's some safety against re-running the procedure on the sameday.
    delete from CP2_QA_viewing_control_totals
    where build_date = @target_date

    commit

    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '1.) Collect')
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1) as viewing_records
        ,round(sum(coalesce(datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time),0)) / 60.0 / 60 / 24.0, 2)
    from Capping2_01_Viewing_Records
    group by program_air_date, live_timeshifted_events, genre

    commit

    -- Distribution of event profiles will get done later...

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from Capping2_01_Viewing_Records

    execute logger_add_event @CP2_build_ID, 3, 'C01: Complete! (Data cleansing)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C02) CONDENSE VIEWING DATA INTO LISTING OF EVENTS
    -------------------------------------------------------------------------------------------------

    -- One Week is getting turned into CP2_event_listing...
    --IF object_id('one_week') is not null drop table one_week
    if object_id('CP2_event_listing') is not null drop table CP2_event_listing

    create table CP2_event_listing (
        Subscriber_Id                       integer         not null
        ,account_number                     varchar(20)     not null
        ,fake_cb_row_id                     bigint          not null    -- we just need it to break some ties it'll still be unique
        ,X_Type_Of_Viewing_Event            varchar(40)     not null
        ,Adjusted_Event_Start_Time          datetime        not null
        ,X_Adjusted_Event_End_Time          datetime
        ,event_start_hour                   tinyint
        ,event_start_day                    tinyint
        ,X_Event_Duration                   decimal(10,0)
        ,event_dur_mins                     integer
        ,live_timeshifted_events            Int
        ,initial_genre                      varchar(25)
        ,initial_sub_genre                  varchar(25)
        ,initial_channel_name               varchar(30)     -- This guy gets populated as uppercase and trimmed
        ,Initial_Service_Key                bigint
        ,program_air_date                   date
        ,program_air_datetime               datetime
        ,num_views                          int
        ,num_genre                          int
        ,num_sub_genre                      int
        ,viewed_duration                    int

        -- These guys are a channel categorisation lookup
        ,pack                               varchar(100)
        ,pack_grp                           varchar(30)
        -- We also use P/S box flags:
        ,box_subscription                   varchar(1)

        -- Columns used in applying caps:
        ,bucket_id                          integer         -- Composite lookup for: event_start_hour, event_start_day, initial_channel_name, live_timeshifted_events
        ,max_dur_mins                       int             default null
        ,capped_event                       bit             default 0

        -- Yeah, structure is always good:
        ,primary key (Subscriber_Id, Adjusted_Event_Start_Time) -- So we... *shouldn't* have any more than one event starting at the same time per box... might have to manage some deduplication...
    )
    -- We'll also need indices on this guy...

    create index    for_joins           on CP2_event_listing (account_number)
    create index    start_time_index    on CP2_event_listing (Adjusted_Event_Start_Time)
    create index    init_channel_index  on CP2_event_listing (initial_channel_name)
    create index for_the_joining_group  on CP2_event_listing (event_start_hour, event_start_day, initial_genre, box_subscription, pack_grp, live_timeshifted_events)
    create index by_bucket_index        on CP2_event_listing (bucket_id, pack_grp, box_subscription)

    commit

    --obtain event view
    insert into CP2_event_listing (
        Subscriber_Id
        ,account_number
        ,fake_cb_row_id
        ,Adjusted_Event_Start_Time
        ,X_Type_Of_Viewing_Event
        ,X_Adjusted_Event_End_Time
        ,X_Event_Duration
        ,event_start_hour
        ,event_start_day
        ,live_timeshifted_events
        ,num_views
        ,num_genre
        ,num_sub_genre
        ,viewed_duration
        ,event_dur_mins
        ,pack_grp
        ,box_subscription
        ,bucket_id
        ,initial_genre
        ,initial_sub_genre
        ,initial_channel_name
        ,Initial_Service_Key
    )
    select
         Subscriber_Id
        ,min(account_number)            -- should be unique given the subscriber_id
        ,min(cb_row_id)                 -- we just need something unique to break some ties
        ,Adjusted_Event_Start_Time
        ,min(X_Type_Of_Viewing_Event)   -- should also be determined give nsubscriber ID and Adjusted_Event_Start_Time
        ,min(X_Adjusted_Event_End_Time) --
        ,min(X_Event_Duration)          --
        ,min(event_start_hour)          --
        ,min(event_start_day)           --
        ,min(live_timeshifted_events)                      -- All these min(.) values should be determined by
        ,count(1)
        ,count(distinct genre)
        ,count(distinct sub_genre)
        ,sum(x_programme_viewed_duration)
        ,cast(min(x_event_duration) / 60 as int)
        -- Other columns we have to specifically mention because Sybase can't handle defaults going into compound indices
        ,null                           -- pack_grp needs it
        ,null                           -- same with box_subscription
        ,null                           -- and bucket_id
        -- So trying to update all the events with initial genre / channel goes badly, so
        -- we're going to hack in a guess here which will probably be wrong for every event
        -- with num_views>=2 but it does mean we don't have to update records with num_views=1
        -- and that might help us dodge the temp space errors we're getting. Maybe :-/
        ,min(genre)
        ,min(sub_genre)
        ,upper(trim(min(channel_name)))
        ,min(Service_Key)
    from Capping2_01_Viewing_Records
    group by Subscriber_Id, Adjusted_Event_Start_Time

    commit

    -- OK, event listing is now build.

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_event_listing

    execute logger_add_event @CP2_build_ID, 3, 'C02: Complete! (Event listing)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C03.1) ATTACH METADATA: GENRE AT EVENT START
    -------------------------------------------------------------------------------------------------
    IF object_id('CP2_First_Programmes_In_Event') IS NOT NULL DROP TABLE CP2_First_Programmes_In_Event

    create table CP2_First_Programmes_In_Event (
        subscriber_id                       integer         not null
        ,adjusted_event_start_time          datetime        not null
        -- For the genre assignement bits:
        ,genre                              varchar(25)
        ,sub_genre                          varchar(25)
        ,channel_name                       varchar(30)
        ,Service_Key                        bigint
        -- Things needed to assign caps to end of first program viewed (sectino C02.e)
        ,X_Adjusted_Event_End_Time          datetime
        ,x_viewing_end_time                 datetime
        ,sequencer                          integer         -- only needed for deduplication
        ,primary key (subscriber_id, adjusted_event_start_time, sequencer)
    )

    -- Build table for first viewing record in each event
    insert into CP2_First_Programmes_In_Event
    select
        -- OK, so we're clipping CP2_First_Programmes_In_Event down to things that actually get referenced:
        subscriber_id
        ,adjusted_event_start_time
        ,genre
        ,sub_genre
        ,channel_name
        ,Service_Key
        -- Things needed to assign caps to end of first program viewed (sectino C02.e)
        ,X_Adjusted_Event_End_Time
        ,x_viewing_end_time
        ,rank() over(partition by subscriber_id, adjusted_event_start_time order by x_viewing_start_time, cb_row_id desc)
    from Capping2_01_Viewing_Records
    commit
    --34723382
    -- delete all records which aren't necessary due to trank
    delete from CP2_First_Programmes_In_Event
    where sequencer <> 1

    commit

    /* Kept for reference while still in dev: we changed a few of the names to more sensible things
    --add channel, genre and sub genre
    alter table one_week                            -- => "CP2_event_listing"
    add genre_at_event_start_time varchar(30),      -- => "CP2_event_listing.initial_genre"
    add sub_genre_at_event_start_time varchar(30),  -- => "CP2_event_listing.initial_sub_genre"
    add channel_at_event_start_time varchar(30),    -- => "CP2_event_listing.initial_channel_name"
    add pack varchar(100) default null,             -- => "CP2_event_listing.pack"
    add pack_grp varchar(20) default null;          -- => "CP2_event_listing.pack_grp"
    */

    -- Okay, so doing the full update gives us errors, so we're again going to roll out the trick
    -- that we only need to worry about updating the records that aren't first in event.
    update CP2_event_listing
    set initial_genre          = fpie.genre
       ,initial_sub_genre      = fpie.sub_genre
       ,Initial_Service_Key    = fpie.Service_Key
    from CP2_event_listing
    inner join CP2_First_Programmes_In_Event as fpie
    on  CP2_event_listing.subscriber_id             = fpie.subscriber_id
    and CP2_event_listing.adjusted_event_start_time = fpie.adjusted_event_start_time
    where CP2_event_listing.num_views > 1
    -- Awesome, temp space issue averted. Going to have to keep an eye on that, and generally
    -- try to amange the "first item in event" things like this...

    commit

    -- After that, the table still gets used in a later section when we might
    -- opt to cap the event to end of first viewing record (ie end of first
    -- programme)

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_event_listing
    where Initial_Service_Key is not null

    execute logger_add_event @CP2_build_ID, 3, 'C03.1: Complete! (Genre at start)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C03.2) ATTACH METADATA: CHANNEL PACKS & PACK GROUPINGS
    -------------------------------------------------------------------------------------------------

    --add pack & network [Update: Network no longer in play]
    update CP2_event_listing base
       set base.pack = trim(cl.channel_pack)
      from Vespa_Analysts.Channel_Map_Dev_Service_Key_Attributes as cl
     where base.Initial_Service_Key = cl.Service_Key

    commit
    -- Would be much better to update the channel lookup, but hey...

    --add pack groups
    update CP2_event_listing
    set pack_grp = coalesce(pack, 'Other')
    from CP2_event_listing

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_event_listing
    where pack_grp <> 'Other'

    execute logger_add_event @CP2_build_ID, 3, 'C03.2: Complete! (Pack grouping)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C03.3) ATTACH METADATA: PRIMARY / SECONDARY BOX
    -------------------------------------------------------------------------------------------------

    -- Yeah, in the new build we just pull the reference in from the weekly profiling build...
    update CP2_event_listing
    set CP2_event_listing.box_subscription = bl.PS_flag
    from CP2_box_lookup as bl
    where CP2_event_listing.subscriber_id = bl.subscriber_id
    -- That's much easier than going back to the customer database for each day separately
    commit
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_event_listing
    where box_subscription in ('P', 'S')

    execute logger_add_event @CP2_build_ID, 3, 'C03.3: Complete! (Primary / secondary box)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C03.4) ATTACH METADATA: CAPPING BUCKET ID
    -------------------------------------------------------------------------------------------------

    -- This guy is a composite key that summarises event_start_hour, event_start_day, initial_genre
    -- and live_timeshifted_events into one integer that's easy to use (/index/join).
    delete from CP2_capping_buckets

    commit

    insert into CP2_capping_buckets (
        event_start_hour
        ,event_start_day
        ,initial_genre
        ,live_timeshifted_events
    )
    select distinct
        event_start_hour
        ,event_start_day
        ,initial_genre
        ,live_timeshifted_events
    from CP2_event_listing

    commit

    -- Push the bucket keys back onto the event listings:

    update CP2_event_listing
    set CP2_event_listing.bucket_id = cb.bucket_id
    from CP2_event_listing
    inner join CP2_capping_buckets as cb
    on  CP2_event_listing.event_start_hour        = cb.event_start_hour
    and CP2_event_listing.event_start_day         = cb.event_start_day
    and CP2_event_listing.initial_genre           = cb.initial_genre
    and CP2_event_listing.live_timeshifted_events = cb.live_timeshifted_events

    commit

    -- We'll put bucket_id on the viewing data in section D01 after we've put the
    -- metadata there too.

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_event_listing
    where bucket_id is not null

    execute logger_add_event @CP2_build_ID, 3, 'C03.4: Complete! (Bucket IDs)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C04) BUILDING N-TILES FOR CAPPING
    -------------------------------------------------------------------------------------------------

    -- OK so from here I might change the strategy and not mess around that much with how these
    -- structures actually work (besides plugging them into the appropriately normalised structures).
    -- I'm 600 lines through with twice as much to go, need to switch up the approach if we're to
    -- have any hope of actually getting this completed. Here's a list of tables we renamed so as to
    -- offer at leaast *some* clarity as to what's going on, or at least, which tables are owned and
    -- used by the Capping 2 process:
    --      ntiles_week     -> CP2_ntiles_week
    --      nt_4_19         -> CP2_nt_4_19
    --      nt_20_3         -> CP2_nt_20_3
    --      nt_lp           -> CP2_nt_lp
    --      week_caps       -> CP2_calculated_viewing_caps
    --      h23_3           -> CP2_h23_3
    --      h4_14           -> CP2_h4_14
    --      h15_19          -> CP2_h15_19
    --      h20_22          -> CP2_h20_22
    --      lp              -> CP2_lp
    --      2p              -> CP2_2p
    --      3p              -> CP2_3p
    --      4p              -> CP2_4p
    -- Other tables which are still floating around but will probably get killed when we normalise
    -- things out:
    --      CP2_QA_viewing_control_cap_distrib
    --      all_events              <- we already have CP2_event_listing, don't think we need this table. Update: it's GONE
    --      uncapped                => CP2_uncapped_events_lookup
    --      capped_events           => CP2_capped_events_lookup
    --      capped_events2          => CP2_capped_events_with_endpoints
    --      batching_lookup         -- depreciated - well, kind of replaced with CP2_capping_buckets, though buckets do different things
    --

    -- So yeah, this bit is super not-safe for accidental multiple runs. One might drop the tables
    -- the other is still using, and then it all goes way south.

    if object_id('CP2_ntiles_week') is not null drop table CP2_ntiles_week

    --calculate ntiles for caps
    select   live_timeshifted_events
            ,cast(adjusted_event_start_time as date) as event_date -- do we need this now we're processing one day at a time?
            ,event_start_day
            ,event_start_hour
            ,box_subscription
            ,pack_grp
            ,initial_genre
            ,event_dur_mins
            ,ntile(200) over (partition by live_timeshifted_events,event_start_day order by x_event_duration) as ntile_lp
            ,ntile(200) over (partition by live_timeshifted_events,event_start_day,event_start_hour,box_subscription,pack_grp,initial_genre order by x_event_duration) as ntile_1
            ,ntile(200) over (partition by live_timeshifted_events,event_start_day,event_start_hour,pack_grp,initial_genre order by x_event_duration) as ntile_2
            ,x_event_duration
            ,viewed_duration
            ,num_views
    into CP2_ntiles_week
    from CP2_event_listing
    where x_event_duration < 86400 -- 86400 seconds in a day or something

    -- Wait... One_week doesn't get used past this? does the CP2_event_listing get used past this?
    -- Not yet, heh, this is the last use of it... okey... looks like we'll be able to either trim
    -- some stuff out or renormalise some things or generally tidy up...

    commit

    --create indexes
    create hng index idx1 on CP2_ntiles_week(event_start_day)
    create hng index idx2 on CP2_ntiles_week(event_start_hour)
    --create hng index idx3 on CP2_ntiles_week(live_timeshifted_events); -- Not any more, not now it's a bit
    create hng index idx4 on CP2_ntiles_week(box_subscription)
    create hng index idx5 on CP2_ntiles_week(pack_grp)
    create hng index idx6 on CP2_ntiles_week(initial_genre)
    -- Which of these do we need? which ones are actually helping?

    commit

    --select distinct event_date,event_start_day from CP2_ntiles_week

    --check data
    --select count(*),sum(num_views) from CP2_ntiles_week
    --count(*)        sum(CP2_ntiles_week.num_views)
    --25928067        34274204

    --select count(*),sum(num_views) from one_week where band_dur_days = 0
    --25928067        34274204

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_ntiles_week

    execute logger_add_event @CP2_build_ID, 3, 'C04: Complete! (n-Tile generation)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C05) TABLES OF N-TILES (?)
    -------------------------------------------------------------------------------------------------

    -- All the different caps tables for the different regimes of capping

    --create capping limits for start hours 4-19
    if object_id('CP2_nt_4_19') is not null drop table CP2_nt_4_19

    SELECT live_timeshifted_events
    ,event_date
    ,event_start_day
    ,event_start_hour
    ,box_subscription
    ,pack_grp
    ,initial_genre
    ,ntile_1
    ,min(event_dur_mins) as min_dur_mins
    ,max(event_dur_mins) as max_dur_mins
    ,PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
    ,count(*) as num_events
    ,sum(num_views) as tot_views
    ,sum(x_event_duration) as event_duration
    ,sum(viewed_duration) as viewed_duration
    into CP2_nt_4_19
    FROM CP2_ntiles_week
    where event_start_hour>=4
    and event_start_hour<=19
    and live_timeshifted_events= 0
    group by live_timeshifted_events,event_date,event_start_day,event_start_hour,box_subscription,pack_grp,initial_genre,ntile_1

    commit

    --create indexes
    create hng index idx1 on CP2_nt_4_19(event_start_day)
    create hng index idx2 on CP2_nt_4_19(event_start_hour)
    create hng index idx4 on CP2_nt_4_19(box_subscription)
    create hng index idx5 on CP2_nt_4_19(pack_grp)
    create hng index idx6 on CP2_nt_4_19(initial_genre)

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_nt_4_19

    execute logger_add_event @CP2_build_ID, 3, 'C05: Midway 1/3 (_nt_4_19)', coalesce(@QA_catcher, -1)
    commit

    --create capping limits start hours 20-3
    if object_id('CP2_nt_20_3') is not null drop table CP2_nt_20_3

    SELECT live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,pack_grp
    ,initial_genre
    ,ntile_2
    ,min(event_dur_mins) as min_dur_mins
    ,max(event_dur_mins) as max_dur_mins
    ,PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
    ,count(*) as num_events
    ,sum(num_views) as tot_views
    ,sum(x_event_duration) as event_duration
    ,sum(viewed_duration) as viewed_duration
    into CP2_nt_20_3
    FROM CP2_ntiles_week
    where event_start_hour in (20,21,22,23,0,1,2,3)
    and live_timeshifted_events= 0
    group by live_timeshifted_events,event_start_day,event_start_hour,box_subscription,pack_grp,initial_genre,ntile_2

    commit

    --create indexes
    create hng index idx1 on CP2_nt_20_3(event_start_day)
    create hng index idx2 on CP2_nt_20_3(event_start_hour)
    create hng index idx4 on CP2_nt_20_3(pack_grp)
    create hng index idx5 on CP2_nt_20_3(initial_genre)

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_nt_20_3

    execute logger_add_event @CP2_build_ID, 3, 'C05: Midway 2/3 (_nt_20_3)', coalesce(@QA_catcher, -1)
    commit

    --create capping limits for timeshifted
    if object_id('CP2_nt_lp') is not null drop table CP2_nt_lp

    SELECT live_timeshifted_events
    ,event_start_day
    ,ntile_lp
    ,min(event_dur_mins) as min_dur_mins
    ,max(event_dur_mins) as max_dur_mins
    ,PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY event_dur_mins) as median_dur_mins
    ,count(*) as num_events
    ,sum(num_views) as tot_views
    ,sum(x_event_duration) as event_duration
    ,sum(viewed_duration) as viewed_duration
    into CP2_nt_lp
    FROM CP2_ntiles_week
    where live_timeshifted_events in (1,2,3,4)
    group by live_timeshifted_events,event_start_day,ntile_lp

    commit

    --create indexes
    create hng index idx1 on CP2_nt_lp(event_start_day)

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_nt_lp

    execute logger_add_event @CP2_build_ID, 3, 'C05: Complete! (_nt_lp)', coalesce(@QA_catcher, -1)
    commit
    -------------------------------------------------------------------------------------------------
    -- C06) ALL KINDS OF DIFFERENT CAPPING TABLES
    -------------------------------------------------------------------------------------------------
    --Bringing in the ntile correction factor in other to make the comparison with Barb more dynamic

    if object_id('capping_threshold_corrections') is not null drop table capping_threshold_corrections
    create table capping_threshold_corrections ( live_timeshifted_events integer
                             ,time_period varchar (7)
                             ,ntile_correction integer
                             )

    insert into capping_threshold_corrections values (0, '23-3', 0)
    insert into capping_threshold_corrections values (0, '4-14', 0)
    insert into capping_threshold_corrections values (0, '15-19',0)
    insert into capping_threshold_corrections values (0, '20-22',0)
    insert into capping_threshold_corrections values (1, 'null', 0)
    insert into capping_threshold_corrections values (2, 'null', 0)
    insert into capping_threshold_corrections values (3, 'null', 0)
    insert into capping_threshold_corrections values (4, 'null', 0)

    declare @ntile_correction integer

    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 0
       and time_period = '23-3'

     commit
    --obtain max cap limits for live_timeshifted_events

    if object_id('CP2_h23_3') is not null drop table CP2_h23_3

    --identify ntile threshold for event start hours 23-3
    select live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,pack_grp
    ,initial_genre
    ,max(ntile_2) as max_ntile
    ,max_ntile-10-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_h23_3
    from CP2_nt_20_3
    where event_start_hour in (23,0,1,2,3)
    and live_timeshifted_events = 0
    group by live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,pack_grp
    ,initial_genre

    commit

    update CP2_h23_3 t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_20_3 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre
    and t1.cap_ntile=t2.ntile_2

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_h23_3

    execute logger_add_event @CP2_build_ID, 3, 'C06: Midway 1/4 (_h23_3)', coalesce(@QA_catcher, -1)
    commit
-------------------------------------------------------------------------------------------------------
    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 0
       and time_period = '4-14'

    commit

    if object_id('CP2_h4_14') is not null drop table CP2_h4_14

    --identify ntile threshold for event start hours 4-14
    select live_timeshifted_events
    ,event_date
    ,event_start_day
    ,event_start_hour
    ,box_subscription
    ,pack_grp
    ,initial_genre
    ,max(ntile_1) as max_ntile
    ,case when event_start_hour in (4,5,10,11,12,13,14) then max_ntile-10-@ntile_correction
          when event_start_hour in (6,7,8,9) and datepart(weekday,event_date) in (1,7) then max_ntile-10-@ntile_correction
          when event_start_hour in (6,7,8,9) and datepart(weekday,event_date) in (2,3,4,5,6) then max_ntile-10-@ntile_correction
     end as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_h4_14
    from CP2_nt_4_19
    where event_start_hour in (4,5,6,7,8,9,10,11,12,13,14)
    and live_timeshifted_events = 0
    group by live_timeshifted_events
    ,event_date
    ,event_start_day
    ,event_start_hour
    ,box_subscription
    ,pack_grp
    ,initial_genre

    commit

    update CP2_h4_14 t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_4_19 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.box_subscription=t2.box_subscription
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre
    and t1.cap_ntile=t2.ntile_1
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_h4_14

    execute logger_add_event @CP2_build_ID, 3, 'C06: Midway 2/4 (_h4_14)', coalesce(@QA_catcher, -1)
    commit

--------------------------------------------------------------------------------------------------------
    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 0
       and time_period = '15-19'

     commit

    if object_id('CP2_h15_19') is not null drop table CP2_h15_19

    --identify ntile threshold for event start hours 15-19
    select live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,box_subscription
    ,pack_grp
    ,initial_genre
    ,max(ntile_1) as max_ntile
    ,max_ntile-10-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_h15_19
    from CP2_nt_4_19
    where event_start_hour in (15,16,17,18,19)
    and live_timeshifted_events = 0
    group by live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,box_subscription
    ,pack_grp
    ,initial_genre

    commit

    update CP2_h15_19 t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_4_19 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.box_subscription=t2.box_subscription
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre
    and t1.cap_ntile=t2.ntile_1
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_h4_14

    execute logger_add_event @CP2_build_ID, 3, 'C06: Midway 3/4 (_h15_19)', coalesce(@QA_catcher, -1)
    commit
------------------------------------------------------------------------------------------------------
    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 0
       and time_period = '20-22'

    commit

    if object_id('CP2_h20_22') is not null drop table CP2_h20_22

    --identify ntile threshold for event start hours 20-22
    select live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,pack_grp
    ,initial_genre
    ,max(ntile_2) as max_ntile
    ,max_ntile-10-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_h20_22
    from CP2_nt_20_3
    where event_start_hour in (20,21,22)
    and live_timeshifted_events = 0
    group by live_timeshifted_events
    ,event_start_day
    ,event_start_hour
    ,pack_grp
    ,initial_genre

    commit

    update CP2_h20_22 t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_20_3 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre
    and t1.cap_ntile=t2.ntile_2
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_h20_22

    execute logger_add_event @CP2_build_ID, 3, 'C06: Complete! (_h20_22)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C07) BUILDING CENTRAL LISTING OF DERIVED CAPS
    -------------------------------------------------------------------------------------------------

    delete from CP2_calculated_viewing_caps

    --identify caps for each variable dimension
    insert into CP2_calculated_viewing_caps (
         live_timeshifted_events
        ,event_start_day
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,initial_genre
        -- Again, managing the cannot-handle-defaults-into-multi-column-indices thing
        ,bucket_id
    )
    select distinct
         live_timeshifted_events
        ,event_start_day
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,initial_genre
        ,null
    from CP2_ntiles_week

    -- This is the last use of CP2_ntiles_week... if we reconstruct it's behaviour up to this point, we're good

    commit

    -- Throw on the bucket_id, it'll help us join a few things later:
    update CP2_calculated_viewing_caps
    set CP2_calculated_viewing_caps.bucket_id = cb.bucket_id
    from CP2_calculated_viewing_caps
    inner join CP2_capping_buckets as cb
    on  CP2_calculated_viewing_caps.event_start_day         = cb.event_start_day
    and CP2_calculated_viewing_caps.event_start_hour        = cb.event_start_hour
    and CP2_calculated_viewing_caps.initial_genre           = cb.initial_genre
    and CP2_calculated_viewing_caps.live_timeshifted_events = cb.live_timeshifted_events

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_calculated_viewing_caps
    where bucket_id is not null

    execute logger_add_event @CP2_build_ID, 3, 'C07: Midway 1/3 (Buckets)', coalesce(@QA_catcher, -1)
    commit

    -- doesn't split out across the pack group and the box subscription, but it still helps.

    -- If we do want to throw on an integer key for the buckets, we need to use:
    --      event_start_day, event_start_hour, pack_grp, initial_genre, live_timeshifted_events
    -- Sometimes the box subscription isn't used, that has to stay out as a different
    -- thing. Still doing the multi-column join then, but at least the integer key
    -- will be a *bit* better. Update: we now have the cap_bucket_id field on the
    -- CP2_calculated_viewing_caps table, able we'll come up with some other workaround for the
    -- caps that are applied uniformly across box subscription type.

    --update threshold table with cap limits
    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_h23_3 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre

    commit

    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_h4_14 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.box_subscription=t2.box_subscription
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre

    commit

    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_h15_19 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.box_subscription=t2.box_subscription
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre

    commit

    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_h20_22 t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.event_start_hour=t2.event_start_hour
    and t1.pack_grp=t2.pack_grp
    and t1.initial_genre=t2.initial_genre

    commit
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_calculated_viewing_caps
    where max_dur_mins is not null

    execute logger_add_event @CP2_build_ID, 3, 'C07: Midway 2/3 (Live only)', coalesce(@QA_catcher, -1)
    commit
-----------------------------------------------------------------
    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 1
       and time_period = 'null'

     commit

    if object_id('CP2_lp') is not null drop table CP2_lp

        --identify ntile threshold for 'VOSDAL <1hr' - timeshifted events
    select live_timeshifted_events
    ,event_start_day
    ,max(ntile_lp) as max_ntile
    ,max_ntile-10-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_lp
    FROM CP2_nt_lp
    where live_timeshifted_events = 1
    group by live_timeshifted_events,event_start_day

    update CP2_lp t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_lp t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.cap_ntile=t2.ntile_lp
    commit

    --update 'VOSDAL <1hr' limits in caps table
    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_lp t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.max_dur_mins is null

    commit
-------------------------------------------------------
    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 2
       and time_period = 'null'

    commit

    if object_id('CP2_2p') is not null drop table CP2_2p

        --identify ntile threshold for 'VOSDAL 1-24hr' - timeshifted events
    select live_timeshifted_events
    ,event_start_day
    ,max(ntile_lp) as max_ntile
    ,max_ntile-6-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_2p
    FROM CP2_nt_lp
    where live_timeshifted_events = 2
    group by live_timeshifted_events,event_start_day

    update CP2_2p t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_lp t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.cap_ntile=t2.ntile_lp
    commit

    --update 'VOSDAL 1-24hr' limits in caps table
    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_2p t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.max_dur_mins is null

    commit
-----------------------------------------------------------------------

    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 3
       and time_period = 'null'

    commit

    if object_id('CP2_3p') is not null drop table CP2_3p

        --identify ntile threshold for 'Playback (+1 day)' timeshifted events
    select live_timeshifted_events
    ,event_start_day
    ,max(ntile_lp) as max_ntile
    ,max_ntile-2-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_3p
    FROM CP2_nt_lp
    where live_timeshifted_events = 3
    group by live_timeshifted_events,event_start_day

    update CP2_3p t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_lp t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.cap_ntile=t2.ntile_lp
    commit

    --update 'Playback (+1 day)' limits in caps table
    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_3p t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.max_dur_mins is null

    commit

--------------------------------------------------------------------------
    select @ntile_correction = ntile_correction
      from capping_threshold_corrections
     where live_timeshifted_events = 4
       and time_period = 'null'
       
    commit

    if object_id('CP2_4p') is not null drop table CP2_4p

    --identify ntile threshold for 'Showcase' - timeshifted events
    select live_timeshifted_events
    ,event_start_day
    ,max(ntile_lp) as max_ntile
    ,max_ntile-1-@ntile_correction as cap_ntile
    ,cast(null as integer) as min_dur_mins
    into CP2_4p
    FROM CP2_nt_lp
    where live_timeshifted_events = 4
    group by live_timeshifted_events,event_start_day

    update CP2_4p t1
    set min_dur_mins=t2.min_dur_mins
    from CP2_nt_lp t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.cap_ntile=t2.ntile_lp
    commit

    --update 'Showcase' limits in caps table
    update CP2_calculated_viewing_caps t1
    set max_dur_mins=t2.min_dur_mins
    from CP2_4p t2
    where t1.live_timeshifted_events=t2.live_timeshifted_events
    and t1.event_start_day=t2.event_start_day
    and t1.max_dur_mins is null

    commit
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_calculated_viewing_caps
    where max_dur_mins is not null

    execute logger_add_event @CP2_build_ID, 3, 'C07: Complete! (Central cap listing)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C08) GLOBAL CAPPING BOUNDS
    -------------------------------------------------------------------------------------------------

    --reset capping limits that are less than the lower limit (see variables in the setup section)
    update CP2_calculated_viewing_caps
    set max_dur_mins = @min_cap_bound_minutes
    where (
        max_dur_mins < @min_cap_bound_minutes
        or max_dur_mins is null
    ) and live_timeshifted_events = 0

    --reset capping limits that are more than upper limit (see variables in the setup section)
    update CP2_calculated_viewing_caps
    set   max_dur_mins = @max_cap_bound_minutes
    where max_dur_mins > @max_cap_bound_minutes
    and live_timeshifted_events = 0

    commit
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_calculated_viewing_caps
    where max_dur_mins in (@min_cap_bound_minutes, @max_cap_bound_minutes)

    execute logger_add_event @CP2_build_ID, 3, 'C08: Complete! (Global cap bounds)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- C09) DISTRIBUTION OF CAPPING BOUNDS JUST FOR QA
    -------------------------------------------------------------------------------------------------

    -- Note that this isn't a profile over the use of caps, it's just on the caps that as get built;
    -- there's no extra weight here for caps that get used more often. Oh, also, all our caps should
    -- be between 20 and 120, so that's just a hundred entries that we can just go out and graph...

    delete from CP2_QA_viewing_control_cap_distrib
    where build_date = @target_date

    commit

    -- OK, here we're using the cumulative ranking duplication trick since we don't have any unique
    -- keys to force the rank to be unique over entries;
    insert into CP2_QA_viewing_control_cap_distrib (
        build_date
        ,max_dur_mins
        ,cap_instances
    )
    select
        @target_date
        ,max_dur_mins
        ,count(1)
    from CP2_calculated_viewing_caps
    group by max_dur_mins

    commit

    /* ##QA##EQ##: Extraction query: graph this guy in Excel I guess
    select * from CP2_QA_viewing_control_cap_distrib
    order by max_dur_mins;
    */

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_QA_viewing_control_cap_distrib
    where build_date = @target_date

    execute logger_add_event @CP2_build_ID, 3, 'C09: Complete! (Cap distributions)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D) APPLYING CAPS TO VIEWING DATA
    -------------------------------------------------------------------------------------------------

    -------------------------------------------------------------------------------------------------
    -- D01) ATTACH CUSTOMER METADATA TO VIEWING
    -------------------------------------------------------------------------------------------------

    /* Now on core table creation
    --add primary/secondary flag to views so thresholds can be applied
    alter table Capping2_01_Viewing_Records
    --add src_system_id varchar(50),                -- This is only ever populated, never used, we don't need it
    add box_subscription varchar(1) default 'U';
    */

    update Capping2_01_Viewing_Records
    set box_subscription = bl.PS_flag
    from Capping2_01_Viewing_Records as vr
    inner join CP2_box_lookup as bl
    on vr.subscriber_id=bl.subscriber_id

    commit

    --add genre, channel and pack to views so thresholds can be applied
    /* These guys have been added to the permanent table, and have also been given better names in some cases:
    alter table Capping2_01_Viewing_Records
    add initial_genre varchar(30),
    add channel_at_event_start_time varchar(30),            => initial_channel_name
    add pack varchar(100) default null,
    add pack_grp varchar(20) default null;
    */

    -- A lot of this processing is just a duplication of stuff on the viewing records
    -- table rather than the events table, but, whatever.

    -- Wait, why does this happen here? I thought we were capping on the events level table?
    -- Maybe we only use that to build the caps, and then the capping happens directly on viewing
    -- data? We'l check how that works.

    -- ahahah awesome, this guy is giving us Query Temp space errors on it. Thing is,
    -- we're joining both tables by subscriber_id and adjusted_event_start_time, while
    -- we have both pairs indexed in both tables (both have other things in the index
    -- afterwards, but the multicolumn indices still work over just the initial columns)
    /* NEEDING REFACTORING:
    update Capping2_01_Viewing_Records
    set Capping2_01_Viewing_Records.initial_genre        = t2.genre
       ,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
    from Capping2_01_Viewing_Records
    inner join CP2_First_Programmes_In_Event t2
    on  Capping2_01_Viewing_Records.subscriber_id             = t2.subscriber_id
    and Capping2_01_Viewing_Records.adjusted_event_start_time = t2.adjusted_event_start_time;
    */

    -- Okay, so we can't join in the first event stuff from the other table, Sybase is
    -- too small. But the initial genre is going to be exactly the genre for the first
    -- item in each event, which is going to permit simpler treatment for a huge portion
    -- of the data set:
    update Capping2_01_Viewing_Records
    set Capping2_01_Viewing_Records.initial_genre        = genre
       ,Capping2_01_Viewing_Records.initial_channel_name = channel_name
    where adjusted_event_start_time = x_viewing_start_time

    commit

    -- and now we can tack on the updates for subsequent viewing items:
    --    update Capping2_01_Viewing_Records
    --    set Capping2_01_Viewing_Records.initial_genre        = t2.genre
    --       ,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
    --    from Capping2_01_Viewing_Records
    --    inner join CP2_First_Programmes_In_Event t2
    --    on  Capping2_01_Viewing_Records.subscriber_id             = t2.subscriber_id
    --    and Capping2_01_Viewing_Records.adjusted_event_start_time = t2.adjusted_event_start_time
    --    where Capping2_01_Viewing_Records.initial_genre is null
    --    commit

    --replaced the above code with this as I ran into the temp tablespace issue.  This code needs
    --QA'd!!
    if object_id('temp_chan_genre') is not null drop table temp_chan_genre

    select rec.cb_row_id, event.genre, event.channel_name
    into temp_chan_genre
    from CP2_First_Programmes_In_Event event,
    Capping2_01_Viewing_Records rec
    where event.subscriber_id = rec.subscriber_id
    and event.adjusted_event_start_time = rec.adjusted_event_start_time
    and rec.initial_genre is null

    commit

    create index tmp_idx_genre on temp_chan_genre (cb_row_id)


    update Capping2_01_Viewing_Records
    set Capping2_01_Viewing_Records.initial_genre        = t2.genre
       ,Capping2_01_Viewing_Records.initial_channel_name = t2.channel_name
    from Capping2_01_Viewing_Records
    inner join temp_chan_genre t2
    on  Capping2_01_Viewing_Records.cb_row_id             = t2.cb_row_id
    commit

    if object_id('temp_chan_genre') is not null drop table temp_chan_genre


    -- query temp space issues averted! (though, it only reduced the size of the update
    -- my a factor of three, and the panel is probably growing more than that, so it
    -- might just turn up again following the ramp up and that'd be funny too.)

    -- OK, now we have the metadata on the viewing items, we can get the bucket IDs too:
    update Capping2_01_Viewing_Records
       set Capping2_01_Viewing_Records.bucket_id = cb.bucket_id
      from Capping2_01_Viewing_Records inner join CP2_capping_buckets as cb
        on Capping2_01_Viewing_Records.event_start_hour = cb.event_start_hour
       and Capping2_01_Viewing_Records.event_start_day  = cb.event_start_day
       and Capping2_01_Viewing_Records.initial_genre    = cb.initial_genre
       and Capping2_01_Viewing_Records.live_timeshifted_events = cb.live_timeshifted_events
    commit

    -- Wait, we already added this to the event listing, do we really need it on the
    -- viewing records too? whatever...
    --update Capping2_01_Viewing_Records base
    --   set base.pack = t2.channel_pack
    --  from Vespa_Analysts.Channel_Map_Cbi_Prod_Service_Key_Attributes as t2
    -- where base.Initial_Service_Key = t2.Service_Key
    --commit

    --add pack group so thresholds can be applied
    --update Capping2_01_Viewing_Records
    --set pack_grp = coalesce(pack, 'Other')
    --from Capping2_01_Viewing_Records
    --
    --commit

    -- Okay, so at this point, the control totals should till be identical...
    delete from CP2_QA_viewing_control_totals
    where data_state like '2%' or data_state like '3%' or data_state like '4%'
    and build_date = @target_date

    commit

    -- (clearing out all future control totals to so as to not cause any confusion)
    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '2.) Pre-Cap') -- aliases are handled in table construction
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round(sum(coalesce(datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time),0)) / 60.0 / 60 / 24.0, 2)
    from Capping2_01_Viewing_Records
    group by program_air_date, live_timeshifted_events, genre

    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from Capping2_01_Viewing_Records
    where pack is not null
    and bucket_id is not null
    and box_subscription in ('P', 'S')

    execute logger_add_event @CP2_build_ID, 3, 'D01: Complete! (Metadata on viewing)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D02) COMPARE VIEWING DATA TO CAPS; DETERMINE CAPPING APPLICATION
    -------------------------------------------------------------------------------------------------

    -- We're not ever using max_dur_mins on the viewing data table, it just gets applied to
    -- events, and even then it's only used to mark the ones that get capped so that we can
    -- assign endpoints of the uncapped guys to the capped items. So we're not even going to
    -- bother putting the duration on the viewing table (like it originally was), instead only
    -- put it on the table at the event aggregation level

    update CP2_event_listing
    set max_dur_mins = caps.max_dur_mins
    from CP2_event_listing as base
    inner join CP2_calculated_viewing_caps as caps
    on  base.bucket_id        = caps.bucket_id
    and base.pack_grp         = caps.pack_grp
    and base.box_subscription = caps.box_subscription
    -- (Do we actually need it while it's on the viewing data? do we only ever need it
    -- when it's on whole events? Actually, we never even use it on the events table either,
    -- because we use it to assign new event end times based on the distribution of other
    -- viewing events in the bucket; it only gets used to build the capped_event and then
    -- never gets seen again.)

    commit

    -- Follwing that, we'll make this decision about which of the events need to get capped:
    update CP2_event_listing
    set capped_event = case
        when dateadd(minute, max_dur_mins, adjusted_event_start_time) >= X_Adjusted_Event_End_Time then 0
        else 1
    end

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_event_listing
    where capped_event = 1

    execute logger_add_event @CP2_build_ID, 3, 'D02: Midway 1/3 (Find cappables)', coalesce(@QA_catcher, -1)
    commit

    -- Okay, update: trying to select the uncapped evens like this is just crazy, there are
    -- to many of them even before the ramp up. Instead, we're going to trim it down to just
    -- a sample of uncapped events, governed by the "@uncapped_sample_pop_max" at the top of the
    -- script, and use those to determine the capping bounds. This might give us a reasonable
    -- number of events all terminating at exactly the same second, and that'll only get worse
    -- when the boxes are scaled up to the Sky base, but whatever, it'll give us *some* kind
    -- of capping, as opposed to the temp space errors we're seeing at the moment. But, that
    -- was always going to be the case with this approach to capping anyway, because we're not
    -- fitting within a distribuion, we're just matching the endpoint of some other lucky
    -- viewing event.

    -- Because of how Sybase whines about how window functions work, we keep having to select
    -- these into different tables too, awesome.

    -- First stage: append a random number to do the random selection for us
    if object_id('CP2_uncapped_events_lookup_midway_1') is not null drop table CP2_uncapped_events_lookup_midway_1
    if object_id('CP2_uncapped_events_lookup_midway_2') is not null drop table CP2_uncapped_events_lookup_midway_2

    select
        bucket_id
        ,initial_channel_name
        ,X_Adjusted_Event_End_Time
        -- we're going to rank over this random variable to pick our sample:
        ,rand(number() * datepart(ms, now())) as sequencer
        -- We need these guys later to do the ordering that the endpoint selection thing uses;
        -- we can't build the event_id at this stage because the min_row & max_row trick wants
        -- the ranking to be dense, because a random element is selected between them.
        ,fake_cb_row_id
        ,adjusted_event_start_time
    into CP2_uncapped_events_lookup_midway_1
    from CP2_event_listing
    where capped_event=0

    commit

    -- CP2_uncapped_events_lookup also gets used to select the end times for capped events
    -- once the index lookup stuff is done, but other than that neither the uncapped nor the
    -- capped table gets used past the loop which populates that CP2_capped_events_with_endpoints table, which
    -- we may indeed keep as a seperate table and build that iteratively as we're going to
    -- need some way to drag the cb_row_id back in from the raw viewing table... maybe that
    -- happens at section D06.

    CREATE INDEX for_ranking on CP2_uncapped_events_lookup_midway_1 (bucket_id, initial_channel_name, sequencer)

    commit

    -- Second stage: rank by this random number
    select * -- yes this is a horrible form, but we've already clipped the source table down to only the things we need.
        ,rank() over (partition by bucket_id, initial_channel_name order by sequencer) as cull_ranking
    into CP2_uncapped_events_lookup_midway_2
    from CP2_uncapped_events_lookup_midway_1
    -- We'd have done it in the same query, but Sybase doesn't like trying to put
    -- things which determine on number() inside a rank function, oh well.

    -- Third part: cull all the overpopulated buckets
    delete from CP2_uncapped_events_lookup_midway_2
    where cull_ranking > @uncapped_sample_pop_max

    commit

    create index for_ordering on CP2_uncapped_events_lookup_midway_2 (bucket_id, initial_channel_name, adjusted_event_start_time, X_Adjusted_Event_End_Time, fake_cb_row_id)


    if object_id('CP2_uncapped_events_lookup') is not null drop table CP2_uncapped_events_lookup

    -- Third stage: build the uncapped table lookup using just the remaiing sample of stuff
    select bucket_id
        ,initial_channel_name
        ,rank() over (order by bucket_id, initial_channel_name, X_Adjusted_Event_End_Time, adjusted_event_start_time, fake_cb_row_id) as event_id
        ,X_Adjusted_Event_End_Time
    into CP2_uncapped_events_lookup
    from CP2_uncapped_events_lookup_midway_2

    commit

    -- Done! Clear out the intermediates.
    if object_id('CP2_uncapped_events_lookup_midway_1') is not null drop table CP2_uncapped_events_lookup_midway_1
    if object_id('CP2_uncapped_events_lookup_midway_2') is not null drop table CP2_uncapped_events_lookup_midway_2
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_uncapped_events_lookup

    execute logger_add_event @CP2_build_ID, 3, 'D02: Midway 2/3 (Uncapped lookup)', coalesce(@QA_catcher, -1)
    commit

    -- (## QA item: check how many buckets ended up with the maximum number of uncapped
    -- events in them? bearing in mind we now have channel + bucket...)

    -- identify capped universe
    if object_id('CP2_capped_events_lookup') is not null drop table CP2_capped_events_lookup

    select
        subscriber_id
        ,bucket_id
        ,initial_channel_name
        ,adjusted_event_start_time
        ,X_Adjusted_Event_End_Time
        ,max_dur_mins
    into CP2_capped_events_lookup
    from CP2_event_listing
    where capped_event=1

    commit

    -- create indexes to speed up processing
    create unique index fake_pk on CP2_uncapped_events_lookup (event_id)
    create        index idx1    on CP2_uncapped_events_lookup (bucket_id, initial_channel_name, X_Adjusted_Event_End_Time)
    create unique index fake_pk on   CP2_capped_events_lookup (bucket_id, initial_channel_name, adjusted_event_start_time, subscriber_id)
    -- bucket_id and cahnel name aren't needed there for completeness, but it really *really* helps on the query

    commit
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_events_lookup

    execute logger_add_event @CP2_build_ID, 3, 'D02: Complete! (Capped lookup)', coalesce(@QA_catcher, -1)
    commit

    -- Okay, so using these buckets for this match will reduce the IO, simplify index use, tidy
    -- up the batching process too, generally a good move all around.

    -- And hey, turns out we can grab this number as a pretty good indicator of batch progress:
    declare @number_of_capped_events float
    set @number_of_capped_events = @QA_catcher

    commit

    -------------------------------------------------------------------------------------------------
    -- D03) INDEX THE DURATION-REPLACEMENT LOOKUP
    -------------------------------------------------------------------------------------------------

    -- So we've given this section a pretty thorough overhaul and now it actually runs on a
    -- full day of data. Main points introduced:
    --  a. We're now batching by start hour, initial genre, and live/playback
    --  b. We've got a single numerical key which defines all combinations of the above in 1 integer
    --  c. Columns that never get used are no longer included on the capped and uncapped tables
    --  d. Indices are now directly tuned to support the joins in play
    --  e. We've bounded the uncapped population to a fixed number of items per bucket
    -- That said, this action is still the bottleneck for this entire capping build; it's taking
    -- hours to process even a single day's worth of caps, and this is pre-ramp up. Past ramp up
    -- with 3x as much data returned? We might end up capping one day of data in each overnight
    -- build. Good thing we have a scheduler that's able to check if the build has gone too far
    -- into the morning, leave things for the next day, and just pick up where it left off. And
    -- depending on how we made the prioritisation work... it can still build the regular reports
    -- before continuing with the capping tasks outstanding from the previous builds. Nice that
    -- we built a scheduler as solidly as we did then, eh?

    -- The buckets are simultaneously more agressive batching (smaller batches) and a lot cleaner
    -- to work with that the batches that just took dat & start hour. We don't even need the
    -- batching lookup table any more, we can just run it off the buckets table.
    declare @the_bucket     int
    declare @max_buckets    int
    declare @bucket_offset  int
    /*
    create variable @the_bucket     int;
    create variable @max_buckets    int;
    create variable @bucket_offset  int;
    */
    -- need somewhere to put the results:
    if object_id('CP2_capped_events_with_endpoints') is not null drop table CP2_capped_events_with_endpoints
    create table CP2_capped_events_with_endpoints ( -- currently not a temp thing because we want to be able to track how data gets into that table...
        subscriber_id                   integer
        ,Adjusted_Event_Start_Time      datetime
        ,X_Adjusted_Event_End_Time      datetime    -- Uncapped event time: needed for control total purposes
        ,max_dur_mins                   integer
        ,bucket_id                      integer
        ,initial_channel_name           varchar(30)
        ,firstrow                       integer
        ,lastrow                        integer
        -- Variables that get played with later as caps are set:
        ,rand_num                       float       default null
        ,uncap_row_num                  integer     default null
        ,capped_event_end_time          datetime    default null
    )
    -- If we need the start time, initial channel etc we can just go into the bucket lookup
    -- and get that stuff afterwards.

    -- We'll add indices after all the data goes in.

    commit

    -- Here is the start of the work loop:
    select
        -- Need this min/max thing because the buckets table has an IDENTITY key,
        -- and that doesn't get reset between builds...
        @the_bucket     = min(bucket_id)
        ,@bucket_offset = min(bucket_id)
        ,@max_buckets   = max(bucket_id)
    from CP2_capping_buckets

    commit

    -- Okay, now we can actually assemble the table:
    while @the_bucket <= @max_buckets
    begin

        insert into CP2_capped_events_with_endpoints (
            subscriber_id
            ,adjusted_event_start_time
            ,X_Adjusted_Event_End_Time
            ,max_dur_mins
            ,bucket_id
            ,initial_channel_name
            ,firstrow
            ,lastrow
        )
        select
            t1.subscriber_id
            ,t1.adjusted_event_start_time
            ,min(t1.X_Adjusted_Event_End_Time)  -- they're all the same anyways
            ,min(t1.max_dur_mins)
            ,@the_bucket
            ,min(t1.initial_channel_name)       -- also determined by adjusted_event_start_time
            -- identify first and last row id in uncapped events that have same profile as capped event
            ,min(t2.event_id)
            ,max(t2.event_id)
        from CP2_capped_events_lookup as t1
        left join CP2_uncapped_events_lookup as t2
        on  t1.bucket_id              = @the_bucket
        and t2.bucket_id              = @the_bucket
        and t1.initial_channel_name   = t2.initial_channel_name
        and t2.X_Adjusted_Event_End_Time >  dateadd(second, @min_view_duration_sec, t1.adjusted_event_start_time)     -- Capped event min length restriction (7 seconds)
        and t2.X_Adjusted_Event_End_Time <= dateadd(second, 180 * 60, t1.adjusted_event_start_time)                   -- Capped event max length restriction (180 minutes)
        and t2.X_Adjusted_Event_End_Time <= t1.X_Adjusted_Event_End_Time
        where t1.bucket_id            = @the_bucket
        -- and   t2.bucket_id            = @the_bucket     -- SBE: this condition is removed to retain capped event with no matching uncapped events
        group by t1.subscriber_id
            ,t1.adjusted_event_start_time
    --        ,t1.X_Adjusted_Event_End_Time -- isn't this entirely determined by the event start time? in fact yes, it is... worse than that, it's never used later in the build...

        commit

        -- Check the control totals every now and then for progress tracking purposes:
        if mod(@the_bucket - @bucket_offset + 1, 40) = 0     -- notify every 40 buckets; the first demo build had ~470 buckets to consider
        begin
            set @QA_catcher = -1

            -- How many items have we resolved thus far?
            select @QA_catcher = count(1)
            from CP2_capped_events_with_endpoints

            -- Note that the counter we're tracking should ultimately head towards the number of capped
            -- items. hey, let's put that in as a progress meter...

            execute logger_add_event @CP2_build_ID, 4, 'D03: Processed bucket ' || convert(varchar(10), @the_bucket - @bucket_offset + 1) || ' out of ' || convert(varchar(10), @max_buckets - @bucket_offset + 1) || ' (Events: ' || round(100 * @QA_catcher/@number_of_capped_events,1) || '%).', coalesce(@QA_catcher, -1)
            -- Bear in mind that the later buckets are going to be much smaller than the earlier ones,
            -- because buckets with not very many events are less likely to be discovered earlier in
            -- the DISTINCT pass; progress will probably jump with the first few cycles of buckets,
            -- and then spend a lot of buckets just tidying up little sparsely populated edge cases.

        end

        -- Move on to the next bucket
        set @the_bucket = @the_bucket + 1
        commit
    end
    -- This guy still takes ages, but it's now a managed/mitigated bottleneck rather
    -- than a showstopper.

    commit
    CREATE hg     INDEX idx1    ON CP2_capped_events_with_endpoints (uncap_row_num)
    create unique index fake_PK on CP2_capped_events_with_endpoints (subscriber_id, adjusted_event_start_time)
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_events_with_endpoints

    execute logger_add_event @CP2_build_ID, 3, 'D03: Complete! (Duration replacement)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D04) RANDOMLY CHOOSE REPLACEMENT DURATION FOR CAPPED EVENTS
    -------------------------------------------------------------------------------------------------

    --create a pretty random multiplier
    declare @multiplier bigint              --has to be a bigint if you are dealing with millions of records.
    --create variable @multiplier bigint
    SET @multiplier = DATEPART(millisecond,now())+1 -- pretty random number between 1 and 1000

    --generate random number for each capped event
    update CP2_capped_events_with_endpoints
    set rand_num = rand(number(*)*@multiplier)      --the number(*) function just gives a sequential number.
    commit

    --identify row id in uncapped universe to select
    update CP2_capped_events_with_endpoints
    set uncap_row_num=case when firstrow>0 then round(((lastrow - firstrow) * rand_num + firstrow),0) else null end

    commit
    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_events_with_endpoints
    where uncap_row_num is not null

    execute logger_add_event @CP2_build_ID, 3, 'D04: Complete! (Select replacements)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D05) ASSIGN NEW END TIMES
    -------------------------------------------------------------------------------------------------

    --assign new event end time to capped events
    update CP2_capped_events_with_endpoints t1
    set capped_event_end_time=t2.X_Adjusted_Event_End_Time
    from CP2_capped_events_with_endpoints as t1
    inner join CP2_uncapped_events_lookup as t2
    on t1.uncap_row_num=t2.event_id
    commit

    -- And that's the last use of the "uncapped" table
    if object_id('CP2_uncapped_events_lookup') is not null drop table CP2_uncapped_events_lookup
    if object_id('CP2_capped_events_lookup') is not null drop table CP2_capped_events_lookup
    commit


    --assign end time of first programme to capped events if no uncapped distribution is available
    update CP2_capped_events_with_endpoints t1
    set capped_event_end_time = case
                                    -- when capped time is still missing and first instance duration > max_dur_mins => max_dur_mins
                                  when (t1.capped_event_end_time is null) and
                                       (datediff(second, t2.adjusted_event_start_time, t2.x_viewing_end_time) > max_dur_mins * 60)
                                            then dateadd(minute, max_dur_mins, t2.adjusted_event_start_time)

                                  when (t1.capped_event_end_time is null) then t2.x_viewing_end_time
                                    else t1.capped_event_end_time
                                end
    from CP2_First_Programmes_In_Event t2
    where t1.subscriber_id=t2.subscriber_id
    and t1.adjusted_event_start_time=t2.adjusted_event_start_time
    and t1.firstrow is null
    commit
    --2501
    -- Joining to CP2_First_Programmes_In_Event again? Should the broadcast end time be already in the record
    -- we're treating, ie, can do with a single table update, no join? -Yes, but what if the event
    -- ends before the first show ends? using the first program table, we dodge that issue. Also: predcting
    -- temp space errors on this guy too, oh well.

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_events_with_endpoints
    where capped_event_end_time is not null

    execute logger_add_event @CP2_build_ID, 3, 'D05: Complete! (Assign end times)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D06) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE
    -------------------------------------------------------------------------------------------------

    -- Heh, no, we're going to throw all of this viewing into a new dynamically named table which
    -- matches the timestamp of the daily table. Or... well, we'll probably build the thing here
    -- and then do it as one single port, rather than have a whole bunch of dynamically named tables.
    -- But still... at this point we only need a tiny selection of what's on the viewing data tables,
    -- we're pretty close to the end of the process.

    -- Man, I hope I don't have to batch this thing as well.... nah, we seem okay.

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
        ,live_timeshifted_events
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
        ,cewe.capped_event_end_time
        ,case
            when vr.live_timeshifted_events = 0 then 'LIVE_Events'
            when vr.live_timeshifted_events in (1,2,3,4) then 'Timeshift'
            else 'FAIL!'                        -- ## QA Check that there aren't any of these
          end
        ,case
            when cewe.subscriber_id is not null then 11 -- 11 for things that need capping treatment
            else 0                                      -- 0 for no capping
          end
        ,vr.program_air_date
        ,vr.live_timeshifted_events
        ,vr.genre
    from Capping2_01_Viewing_Records as vr
    left join CP2_capped_events_with_endpoints as cewe
    on  cewe.subscriber_id             = vr.subscriber_id
    and cewe.adjusted_event_start_time = vr.adjusted_event_start_time
    -- WAIT! ## we need to get TIMESHIFTING flag in here too. Though we haven't
    -- checked VOSDAL / PLAYBACK7 / PLAYBACK28 yet, but we can flag LIVE stuff.

    commit

    /*
    --append fields to table to store additional metrics for capping
    alter table Capping2_01_Viewing_Records
    add (capped_event_end_time datetime
        ,capped_x_viewing_start_time datetime
        ,capped_x_viewing_end_time datetime
        ,capped_x_programme_viewed_duration integer
        ,capped_flag integer );

    --update daily view table with revised end times for capped events
    update Capping2_01_Viewing_Records t1
    set capped_event_end_time=t2.capped_event_end_time
    from CP2_capped_events_with_endpoints t2
    where t1.subscriber_id=t2.subscriber_id
    and t1.adjusted_event_start_time=t2.adjusted_event_start_time;

    commit;
    */

    -- How are we going to get the control totals out if we don't put the capping back onto
    -- the viewing records table? hmmm....

    -- Okay, before we get rid of the holding pen, let's grab the event distributions from it:
    delete from CP2_QA_event_control_distribs
    where build_date = @target_date

    -- okey, the way these GROUP BY statements go, this guy could be slow...

    commit

    insert into CP2_QA_event_control_distribs (
        build_date
        ,data_state
        ,duration_interval
        ,viewing_events
    )
    select
        @target_date
        ,convert(varchar(20), '1.) Uncapped')
        ,datediff(minute, Adjusted_Event_Start_Time, X_Adjusted_Event_End_Time) as grouping_minute -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
        ,count(1)
    from CP2_capped_events_with_endpoints
    group by grouping_minute

    commit

    insert into CP2_QA_event_control_distribs (
        build_date
        ,data_state
        ,duration_interval
        ,viewing_events
    )
    select
        @target_date
        ,convert(varchar(20), '2.) Capped')
        ,datediff(minute, Adjusted_Event_Start_Time, capped_event_end_time) as grouping_minute -- batched into 1m chunks, so 0 means viewing durations between 0s and 1 minute
        ,count(1)
    from CP2_capped_events_with_endpoints
    group by grouping_minute

    commit


    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen

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

    -- And now the more basic case where there's no capping;
    update CP2_capped_data_holding_pen
    set viewing_starts = x_viewing_start_time
        ,viewing_stops = x_viewing_end_time
    where capped_flag = 0

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


    -- Now the total viewing should be different... though there's no midpoint, it just *chunk* turns up all at once
    delete from CP2_QA_viewing_control_totals
    where data_state like '3%' or data_state like '4%'
    and build_date = @target_date

    commit

    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '3.) Capped') -- aliases are handled in table construction
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
    from CP2_capped_data_holding_pen
    group by program_air_date, live_timeshifted_events, genre

    -- OK, so that's the total of what's left, but we also want the breakdown by
    -- each capping action, so we can check that they all add up:

    -- First clear out the marks in case we're rerunning this section without starting
    -- from the top of the script:
    delete from CP2_QA_viewing_control_totals
    where data_state like '4%'
    and build_date = @target_date

    commit

    -- The total time in events that were not capped:
    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '4a.) Uncapped')
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
    from CP2_capped_data_holding_pen
    where capped_flag = 0
    group by program_air_date, live_timeshifted_events, genre

    -- Total time in events that were capped but the viewing for that record wasn't affected:
    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '4b.) Unaffected')
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
    from CP2_capped_data_holding_pen
    where capped_flag = 1
    group by program_air_date, live_timeshifted_events, genre

    -- Total time in events that were just dropped:
    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '4c.) Excluded')
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round(sum(coalesce(datediff(second, x_viewing_start_time, x_viewing_end_time),0)) / 60.0 / 60 / 24.0, 2)
    from CP2_capped_data_holding_pen
    where capped_flag = 3
    group by program_air_date, live_timeshifted_events, genre

    commit

    -- The total time left in events that were capped:
    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '4d.) Truncated')
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round(sum(coalesce(datediff(second, viewing_starts, viewing_stops),0)) / 60.0 / 60 / 24.0, 2)
    from CP2_capped_data_holding_pen
    where capped_flag = 2
    group by program_air_date, live_timeshifted_events, genre

    -- Total time removed from events that were capped
    insert into CP2_QA_viewing_control_totals
    select
        @target_date
        ,convert(varchar(20), '4e.) T-Margin')
        ,program_air_date
        ,live_timeshifted_events
        ,genre
        ,count(1)
        ,round((sum(coalesce(datediff(second, x_viewing_start_time, x_viewing_end_time),0))
            - sum(coalesce(datediff(second, viewing_starts, viewing_stops),0))) / 60.0 / 60 / 24.0, 2)
    from CP2_capped_data_holding_pen
    where capped_flag = 2
    group by program_air_date, live_timeshifted_events, genre

    commit

    set @QA_catcher = -1

    execute logger_add_event @CP2_build_ID, 3, 'D06: Midway 1/2 (Control totals)'
    commit

    -- Wait, so where's the bit where we delete all the records that were excluded by capping? Maybe we
    -- just don't migrate them into the dynamic table? Update: Nope, here it is:

    delete from  CP2_capped_data_holding_pen
    where capped_flag = 3

    commit

    -- At the same time, we're also reducing it to viewing records that are strictly contained
    -- within the viewing table we're processing.
    if object_id('capped_viewing_totals') is not null drop table capped_viewing_totals

    -- Oh, we should also grab the total average viewing (again)
    select subscriber_id,
        round(sum(viewing_duration) / 60.0, 0) as total_box_viewing
    into capped_viewing_totals
    from CP2_capped_data_holding_pen
    group by subscriber_id
    -- Don't need the WHERE filter, we've already removed things not on the daily table
    commit

    select @QA_catcher = avg(total_box_viewing)
    from capped_viewing_totals

    commit

    update CP2_QA_daily_average_viewing
    set average_capped_viewing = @QA_catcher
    where build_date = @target_date

    commit

    if object_id('capped_viewing_totals') is not null drop table capped_viewing_totals

    -- Section mostly complete!

    set @QA_catcher = -1

    commit

    select @QA_catcher = count(1)
    from CP2_capped_data_holding_pen

    execute logger_add_event @CP2_build_ID, 3, 'D06: Complete! (Capping on viewing table)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- D07) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPING
    -------------------------------------------------------------------------------------------------

    /* ##QA##EQ##: Pivot the results of this extraction query in Excel I guess:
    select * from CP2_QA_viewing_control_totals
    order by data_state, program_air_date, live_timeshifted_events, genre
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

    -------------------------------------------------------------------------------------------------
    -- D08) LOOKING AT VIEWING DURATION PROFILE BEFORE AND AFTER CAPPING
    -------------------------------------------------------------------------------------------------

    -- Okay, but we're going to batch it into 10s histogram thing, because other these tables will be
    -- huge, and we should still be able to get all the detail want from this view even:

    -- Wait, except this is going on viewing records and not on event lengths... neet to build the one
    -- that actually goes on event lengths... though do we have that anywhere? Er... the only place it
    -- currently lives at event level is on the temp table - CP2_capped_events_with_endpoints - maybe
    -- we make that guy permanent so we can interrogate it later? We'll also need the event start time
    -- and end time too so we can properly get all the durations we need directly from it. Okey.

    delete from CP2_QA_viewing_control_distribs
    where build_date = @target_date

    commit

    insert into CP2_QA_viewing_control_distribs (
        build_date
        ,data_state
        ,duration_interval
        ,viewing_events
    )
    select
        @target_date
        ,convert(varchar(20), '1.) Uncapped')
        ,floor(x_programme_viewed_duration / 10) * 10 as grouping_guy -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
        ,count(1)
    from Capping2_01_Viewing_Records
    where x_programme_viewed_duration > 0
    group by grouping_guy

    -- Is this the last time we need Capping2_01_Viewing_Records? From here, everything
    -- should be happening in the holding pen...

    commit

    insert into CP2_QA_viewing_control_distribs (
        build_date
        ,data_state
        ,duration_interval
        ,viewing_events
    )
    select
        @target_date
        ,convert(varchar(20), '2.) Capped')
        ,floor(viewing_duration / 10) * 10 as grouping_guy -- again giving the alias so as to make the grouping more transparent
        ,count(1)
    from CP2_capped_data_holding_pen
    where viewing_duration > 0
    group by grouping_guy

    commit

    /* ##QA##EQ##: Extraction query: make a graph in Excel or something
    select * from CP2_QA_viewing_control_distribs
    order by data_state, duration_interval
    */

    set @QA_catcher = -1

    execute logger_add_event @CP2_build_ID, 3, 'D08: Complete! (Viewing duration profile)'
    commit



    /*
    --------------------------------------------------------------------------------
    -- E - Add Additional Fields to the Viewing data
    --------------------------------------------------------------------------------

             E01 - Add Playback and Vosdal flags

    --------------------------------------------------------------------------------
    */


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
                          when live_timeshifted_events = 0  then 'LIVE_Events'

                          when live_timeshifted_events = 1  then 'VOSDAL <1hr'

                          when live_timeshifted_events = 2  then 'VOSDAL 1-24hr'

                          when live_timeshifted_events = 3  then 'PLAYBACK (+1day)'

                          when live_timeshifted_events = 4  then 'SHOWCASE'

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

--    set @QA_catcher = -1

--    select @QA_catcher = count(1)
--   from CP2_capped_data_holding_pen
--   where timeshifting is not null

--   execute logger_add_event @CP2_build_ID, 3, 'E01: Complete! (Calculate view bounds)', coalesce(@QA_catcher, -1)
--    commit



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
                    if object_id(''Vespa_Daily_Augs_##^^*^*##'') is not null drop table Vespa_Daily_Augs_##^^*^*##

                    create table Vespa_Daily_Augs_##^^*^*## (
                          Cb_Row_Id                   bigint              primary key,    -- Links to the viewing data daily table of the same day
                          Account_Number              varchar(20)         not null,
                          Subscriber_Id               bigint              not null,
                          Programme_Trans_Sk          bigint,                             -- to help out with the minute-by-minute stuff
                          Timeshifting                varchar(20),
                          Viewing_Starts              datetime,                           -- Capped viewing start time (UTC time)
                          Viewing_Stops               datetime,
                          Viewing_Duration            bigint,                             -- Capped viewing in seconds
                          Capped_Flag                 tinyint,                            -- 0-2 depending on capping treatment: 0 -> event not capped, 1 -> event capped but does not effect viewing, 2 -> event capped & shortens viewing, 3 -> event capped & excludes viewing (actually 3 will not turn up in the table, but that is what it means during processing)
                          Capped_Event_End_Time       datetime,                           -- Only populated for capped events
                          Scaling_Segment_Id          bigint,                             -- To help with the MBM proc builds.... -- NYIP!
                          Scaling_Weighting           float,                              -- Also assisting with the MBM proc builds -- NYIP!
                          BARB_Minute_Start           datetime,                           -- Viewing with Capping treatment + BARB minute allocation
                          BARB_Minute_End             datetime                            --
                    )

                    create hg   index idx1 on Vespa_Daily_Augs_##^^*^*## (Subscriber_Id)
                    create hg   index idx2 on Vespa_Daily_Augs_##^^*^*## (Account_Number)
                    create hg   index idx3 on Vespa_Daily_Augs_##^^*^*## (Programme_Trans_Sk)
                    create dttm index idx4 on Vespa_Daily_Augs_##^^*^*## (Viewing_Starts)
                    create dttm index idx5 on Vespa_Daily_Augs_##^^*^*## (Viewing_Stops)
                    '
    commit

    execute(replace(@var_sql,'##^^*^*##', dateformat(@target_date, 'yyyymmdd')))
    commit


    --------------------------------------------------------------------------------
    -- G02) - DYNAMIC DAILY AUGMENTATION TABLE: POPULATION
    --------------------------------------------------------------------------------
      -- Specifically no filters here as we're running the QA actions over CP2_capped_data_holding_pen
      -- since that makes it a lot easier to get the totals and checks etc into logger than when
      -- doing everything dynamically of the daily augmented tables.
    delete from CP2_capped_data_holding_pen
     where date(viewing_starts) <> @target_date
    commit

    set @var_sql = '
                    insert into Vespa_Daily_Augs_##^^*^*##
                           (Cb_Row_Id, Account_Number, Subscriber_Id, Programme_Trans_Sk, Timeshifting, Viewing_Starts,
                            Viewing_Stops, Viewing_Duration, Capped_Flag, Capped_Event_End_Time, Scaling_Segment_Id,
                            Scaling_Weighting, BARB_Minute_Start, BARB_Minute_End)
                      select
                          Cb_Row_Id,
                          Account_Number,
                          Subscriber_Id,
                          Programme_Trans_Sk,
                          Timeshifting,
                          Viewing_Starts,
                          Viewing_Stops,
                          Viewing_Duration,
                          Capped_Flag,
                          Capped_Event_End_Time,
                          Scaling_Segment_Id,
                          Scaling_Weighting,
                          BARB_Minute_Start,
                          BARB_Minute_End
                        from CP2_capped_data_holding_pen
                  '
    commit

    execute(replace(@var_sql,'##^^*^*##', dateformat(@target_date, 'yyyymmdd')))
    commit


    set @var_sql = '
                     grant select on vespa_daily_augs_##^^*^*## to vespa_group_low_security
                  '
    commit

    execute(replace(@var_sql,'##^^*^*##', dateformat(@target_date, 'yyyymmdd')))
    commit

    set @QA_catcher = -1

    select @QA_catcher = count(1)
      from CP2_capped_data_holding_pen
     where date(viewing_starts) = @target_date

    execute logger_add_event @CP2_build_ID, 3, 'G02: Aug table completed', coalesce(@QA_catcher, -1)


end; -- procedure CP2_build_days_caps

commit;
go

-------------------------------------------------------------------------------------------------
-- J - WEEKLY PROFILING BULD OF BOX METADATA
-------------------------------------------------------------------------------------------------

-- We don't need to profile each box on each different day, we're just going to profile
-- once a week (or something like that) at the beginning of the build week and use that
-- for the whole week. This way isn't going to be super robust against race conditions,
-- but the scheduler is fairly robust against two things running the same proc at the
-- same time. Still, we can also throw the build date onto the metadata table to ensure
-- things don't get desynchronised.

if object_id('CP2_Profile_Boxes') is not null then drop procedure CP2_Profile_Boxes end if;
commit;

go

create procedure CP2_Profile_Boxes
    @profiling_thursday     date = NULL
    ,@CP2_build_ID          bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin

    DECLARE @QA_catcher             integer

    -- Note that we've started the build:
    execute logger_add_event @CP2_build_ID, 3, 'New week: Profiling boxes as of ' || dateformat(@profiling_thursday, 'yyyy-mm-dd') ||'.'
    commit

    -------------------------------------------------------------------------------------------------
    -- J01) CLEARING OLD STUFF OUT OF THE TABLE, REPOPULATING
    -------------------------------------------------------------------------------------------------

    -- For the dev build we're using '2012-01-26' but now it's a proc we want to be able to fire
    -- in dates of our own choosing.
    --set @profiling_thursday = '2012-01-26'

    DELETE FROM CP2_box_lookup
    -- Yeah, the trick now is that no single loop will contain all the boxes we want to
    -- process, because we're only caching one day worth of caps at once. So we need to go
    -- over the daily tables again and pull out all the account numbers that we care about.

    -- We'd use temporary tables for these two guys, except that they get populated via
    -- some dynamic SQL, so temporary tables would fail (being inside a separate execution
    -- scope, sadface)
    DELETE FROM CP2_relevant_boxes

    declare @scanning_day               date
    set @scanning_day = dateadd(day, -1, @profiling_thursday)

    insert into CP2_relevant_boxes
      select distinct account_number
          ,subscriber_id
          ,service_instance_id
      from sk_prod.vespa_dp_prog_viewed_201309
     where event_start_date_time_utc >= @scanning_day
       and event_start_date_time_utc <= dateadd(day, 8, @scanning_day)
       and panel_id = 12
       and account_number is not null
       and subscriber_id is not null

    commit
--I took this out because we don't have access to this table --Patrick
--    insert into CP2_relevant_boxes
 --     select distinct account_number
--          ,subscriber_id
--          ,service_instance_id
--     from sk_prod.vespa_dp_prog_non_viewed_current
--     where event_start_date_time_utc >= @scanning_day
--       and event_start_date_time_utc <= dateadd(day, 8, @scanning_day)
--       and panel_id = 12
--       and account_number is not null
 --      and subscriber_id is not null

--commit

    execute logger_add_event @CP2_build_ID, 3, 'Days processed: ' || dateformat(@scanning_day, 'dd/mm/yyyy') || '-' || dateformat(dateadd(day, 8, @scanning_day), 'dd/mm/yyyy')
    commit


    -- We also need to populate the CP2_box_lookup table:
    insert into CP2_box_lookup (
        subscriber_id
        ,account_number
        ,service_instance_id
    )
    select
        subscriber_id
        ,min(account_number)
        ,min(service_instance_id)
    from CP2_relevant_boxes
    where subscriber_id is not null -- dunno if there are any, but we need to check
        and account_number is not null
    group by subscriber_id

    commit

    -- Maybe have some QA somewhere checking for duplication between account number / subscriber
    -- id / service instance id? the min(.) method is kind of ugly

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_box_lookup

    execute logger_add_event @CP2_build_ID, 3, 'J01: Complete! (Box lookup built)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- J02) PRIMARY & SECONDARY BOX FLAGS
    -------------------------------------------------------------------------------------------------
    if object_id('CP2_deduplicated_accounts') is not null drop table CP2_deduplicated_accounts

    -- For pulling stuff out of the customer database: we would join on service instance ID,
    -- except that it's not indexed in cust_subs_hist. So instead we pull out everything for
    -- these accounts, and then join back on service instance ID later.
    select distinct account_number, 1 as Dummy
    into CP2_deduplicated_accounts
    from CP2_relevant_boxes

    commit
    create unique index fake_pk on CP2_deduplicated_accounts (account_number)
    commit

    -- OK, now we can go get get P/S flgs:
    if object_id('all_PS_flags') is not null drop table all_PS_flags

    select distinct
        --da.account_number,        -- we're joining back in on service_instance_id, so we don't need account_number
        csh.service_instance_id,
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end as PS_flag
    into all_PS_flags
    from CP2_deduplicated_accounts as da
    inner join sk_prod.cust_subs_hist as csh
    on da.account_number = csh.account_number
    where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
    and csh.status_code in ('AC','AB','PC')
    and csh.effective_from_dt<=@profiling_thursday
    and csh.effective_to_dt>@profiling_thursday

    -- ^^ This guy, on the test build (300k distinct accounts) took 8 minutes. That's managable.

    commit

    -- OK, so building P/S off what's active on the Thursday could cause issues with
    -- recent activators not having subscriptions which give them flags, but I'm okay
    -- with there being a few 'U' entries for recent joiners to Sky for the first week
    -- they're on the Vespa panel. It's not about recently joining Vespa, it's about
    -- recently joining Sky, so it shouldn't be much of an issue at all.

    -- Index *should* be unique, but might not be if there are conflicts in Olive. So,
    -- more QA, check that these are actually unique.
    create index idx1 on all_PS_flags (service_instance_id)
    commit

    update CP2_box_lookup
    set CP2_box_lookup.PS_flag = apsf.PS_flag
    from CP2_box_lookup
    inner join all_PS_flags as apsf
    on CP2_box_lookup.service_instance_id = apsf.service_instance_id

    commit
    if object_id('CP2_deduplicated_accounts') is not null drop table CP2_deduplicated_accounts
    if object_id('all_PS_flags') is not null drop table all_PS_flags
    commit
    -- Need some QA on the these numbers, including warning about guys still flagged
    -- as 'U', but the process all seems okay.

    set @QA_catcher = -1

    select @QA_catcher = count(1)
    from CP2_box_lookup
    where PS_flag in ('P', 'S')

    execute logger_add_event @CP2_build_ID, 3, 'J02: Complete! (Derive P/S per box)', coalesce(@QA_catcher, -1)
    commit

    -------------------------------------------------------------------------------------------------
    -- J03) WHAT OTHER BOX / ACCOUNT DATA DO WE USE?
    -------------------------------------------------------------------------------------------------

    -- Nothing yet...

end; -- procedure CP2_Profile_Boxes

commit;
go

-- Takes a bit less than 10 minutes, that's cool.


-------------------------------------------------------------------------------------------------
-- PART L) WEEKLY CAP BUILD: THE OUTER LOOP
-------------------------------------------------------------------------------------------------

-- This procedure controls the big historic build and also the weekly refresh. Due to the volume
-- of computation involved, we're going to pursue a sophisticated strategy whereby we prioritise
-- some historical day, work those first, and then continue with the historical build, all through
-- the scheduler, so we can keep things going heavy at night but keep it clean in business hours.

if object_id('CP2_refresh_caps') is not null then drop procedure CP2_refresh_caps end if;
commit;
go


--------------------------------------------------------------------------------
-- R01) TRANSIENT TABLE RESET PROCEDURE
--------------------------------------------------------------------------------

-- The capping build generates a whole bunch of junk that clutters up a schema. This
-- guy will clear out all the transient objects, which might be cute as some of them
-- are pretty big - full daily viewing data dumps etc.

if object_id('CP2_clear_transient_tables') is not null then drop procedure CP2_clear_transient_tables end if;
commit;

go

create procedure CP2_clear_transient_tables
as
begin
    -- Tables that are reset and built in the middle of the script: I blanked some of them out for testing purposes)
     if object_id('CP2_capped_events_with_endpoints')    is not null drop table CP2_capped_events_with_endpoints
     if object_id('CP2_event_listing')                   is not null drop table CP2_event_listing
     if object_id('CP2_First_Programmes_In_Event')       is not null drop table CP2_First_Programmes_In_Event
     if object_id('CP2_h15_19')                          is not null drop table CP2_h15_19
     if object_id('CP2_h20_22')                          is not null drop table CP2_h20_22
     if object_id('CP2_h23_3')                           is not null drop table CP2_h23_3
     if object_id('CP2_h4_14')                           is not null drop table CP2_h4_14
     if object_id('CP2_lp')                              is not null drop table CP2_lp
     if object_id('CP2_2p')                              is not null drop table CP2_2p
     if object_id('CP2_3p')                              is not null drop table CP2_3p
     if object_id('CP2_4p')                              is not null drop table CP2_4p
     if object_id('CP2_nt_20_3')                         is not null drop table CP2_nt_20_3
     if object_id('CP2_nt_4_19')                         is not null drop table CP2_nt_4_19
     if object_id('CP2_nt_lp')                           is not null drop table CP2_nt_lp
     if object_id('CP2_ntiles_week')                     is not null drop table CP2_ntiles_week
     if object_id('Capping2_01_Viewing_Records')         is not null drop table Capping2_01_Viewing_Records


    -- Tables that exist eslewhere in the table creation script:
    truncate table CP2_box_lookup
    truncate table CP2_calculated_viewing_caps
    truncate table CP2_capped_data_holding_pen
    truncate table CP2_capping_buckets
    truncate table CP2_relevant_boxes

    commit

end;

commit;
go


