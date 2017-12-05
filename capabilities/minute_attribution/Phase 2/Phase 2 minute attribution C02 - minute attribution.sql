/*###############################################################################
# Created on:   29/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation for viewing events according to
#               BARB minute definition, modified & tweaked so it can be used
#               within Vespa capabilities
#               This is PROCEDURE based script for Phase 2 data structures
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 0.2 - creating input table for further processing
#               STEP 1.1 - applying minute attribution rules for playback events
#               STEP 2.1 - creating sub-universe with all live viewing records
#               STEP 2.2 - splitting viewing records so each minute as in "viewing
#                          start" & "viewing end" fields is represented by a single
#                          record
#               STEP 2.3 - calculating required totals & metrics for each minute
#                          and finally applying minute attribution
#               STEP 2.4 - calculating BARB minute
#               STEP 2.5 - creating Surfing events (separate table)
#               STEP 2.6 - creating constituents for Surfing events
#               STEP 3.1 - applying the results to the base table
#               STEP 3.2 - applying the results to the original/input table
#
# To do:
#               - review input parameters, if all are needed
#               - review fields in the iput table
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# Input table "VESPA_MinAttr_Phase2_01_Viewing_Delta" must exists
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 29/08/2012  SBE   v01 - initial version
# 10/09/2012  SBE   v02
#                   - Modification (workaround/proxy) to include (unrecognizable)
#                     evSurf events
#                   - Initial set of filters tightened to ensure only legitimate
#                     viewing events are considered in minute attribution
#                   - Input table field names aligned with the ones existing on
#                     sk_prod.VESPA_EVENTS_ALL to ensure consistency and a single
#                     version of the procedure can be used across all areas
# 20/09/2012  SBE   v03
#                   - optional input parameter added - "parBuildId". When present
#                     (not null), procedure will use the existing logger run
#                     rather than attempting to create a new one
# 05/11/2012  SBE   v04
#                   - all alter table statements removed
#                   - "Process_Date" required field added - it indicates the
#                     date for collection of instances/records that are processed
#                     - required for unique surfing events maintenance - at this
#                     stage this forces the requirement that only full/complete
#                     days can be processed by the script, i.e. no partial day
#                     processing is allowed (which would be against MA concept
#                     anyway)
#                   - Surfing & Constituent tables will have records removed for all
#                     days processed (based on all values of "Process_Date"
#                     so unique constrain violation is no longer an issue for
#                     multiple runs for the same day
#                   - algoritm amended to reflect definition change for live
#                     events, specifically:
#                       "...viewing time is attributed to the first longest event
#                        within a collection of events (for a given channel or all
#                        surfing events combined) that has the first longest
#                        combined viewing duration in that minute"
#                   - CITeam reference removed from logger procedures calls
#                   - minor bug fixed for "StartEndMinute_Relation" calculation
#                   - fixed issue in a scenario when there are no Constituent
#                     records and Sybase throws an error since it can't use
#                     rank() on a 0-record table
# 29/04/2013  SBE   v05
#                   - Added Event Start/End Time to properly identify surfing events
#                   - Removed non-essential input fields, data preparation phase
#                     is expected to happen outside of the procedure
#
#
###############################################################################*/


if object_id('Minute_Attribution_v05') is not null then drop procedure Minute_Attribution_v05 endif;
commit;


  -- ###############################################################################
  -- ##### Input table must include the following columns:                     #####
  -- #####  - Process_Date                                                     #####
  -- #####  -               - this indicates the date for collection of        #####
  -- #####  -                 instances/records that are processed - required  #####
  -- #####  -                 for unique surfing events maintenance - at this  #####
  -- #####  -                 stage, only full/complete days can be processed, #####
  -- #####  -                 i.e. no partial day processing is allowed        #####
  -- #####  - Instance_Id   - ID, must be unique                               #####
  -- #####  - Subscriber_Id                                                    #####
  -- #####  - Event_Start_Time                                                 #####
  -- #####  - Event_End_Time                                                   #####
  -- #####                  - this is EXCLUSIVE version, i.e. equal to         #####
  -- #####                    "Event_Start_Time" of the next event             #####
  -- #####  - Instance_Start_Time                                              #####
  -- #####  - Instance_End_Time                                                #####
  -- #####                  - this is EXCLUSIVE version, i.e. equal to         #####
  -- #####                    "Event_Start_Time" of the next event             #####
  -- #####  - Channel_Id                                                       #####
  -- #####  - Time_Since_Recording                                             #####
  -- #####  - Live_Flag                                                        #####
  -- #####  - BARB_Minute_Start                                                #####
  -- #####  - BARB_Minute_End                                                  #####
  -- #####                                                                     #####
  -- ###############################################################################

create procedure Minute_Attribution_v05
      @parInputTable            varchar(100) = null, -- See above for the list of required fields
      @parBuildDate             datetime = null,
      @parLogQADetails          bit = 0,             -- If "1" then each completed step triggers Logger event
      @parRefreshIdentifier     varchar(40) = '',    -- Logger - refresh identifier
      @parBuildId               bigint = null        -- Logger - add events to an existing logger process
as
begin
 -- #### (procedure start) #####


        -- ##############################################################################################################
        -- ##### STEP 0.1 - preparing environment                                                                   #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Define and set variables                                            #####
        -- ###############################################################################
      declare @varSurfingCutoff               tinyint
      declare @varMinMinuteViewingCutoff      tinyint
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @QA_result_1                    integer             -- QA result field
      declare @QA_result_2                    integer             -- QA result field
      declare @varSql                         varchar(10000)      -- SQL string for dynamic SQL execution

      set @varSurfingCutoff            = 15  -- Included in the lower range, i.e. "15" means "15 or less" -> viewing
                                             --   must be 16 seconds or more to be not classified as Surfing
      set @varMinMinuteViewingCutoff   = 30  -- Included in the lower range, i.e. "30" means "30 or less" -> viewing
                                             --   must be 31 seconds or more
      set @varProcessIdentifier        = 'MinuteAttr_v05'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Reset fields in the input table                                     #####
        -- ###############################################################################
      set @varSql = '
                    update ' || @parInputTable || ' base
                       set base.BARB_Minute_Start = null,
                           base.BARB_Minute_End   = null
                    '
      execute(@varSql)
      commit


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Minute Attribution - process started #######'

      if (@parLogQADetails = 0)
          execute logger_add_event @varBuildId, 3, '(note: quiet mode, reporting only warnings and errors)'
      else
          execute logger_add_event @varBuildId, 3, '(note: full reporting mode, reporting all details)'

      set @QA_result_1 = -1
      commit

      execute logger_add_event @varBuildId, 3, 'Step 0.1: Preparing environemnt', null


        -- ##############################################################################################################
        -- ##### STEP 0.2 - creating input table for further processing                                             #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Get all records from the original table                             #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase2_01_Viewing_Delta') is not null drop table VESPA_MinAttr_Phase2_01_Viewing_Delta
      set @varSql = '
                    select
                          Instance_Id                           as Instance_Id,
                          Subscriber_Id                         as Subscriber_Id,
                          Process_Date                          as Process_Date,
                          Event_Start_Time                      as Event_Starts,
                          Event_End_Time                        as Event_Stops,
                          Instance_Start_Time                   as Viewing_Starts,
                          Instance_End_Time                     as Viewing_Stops,
                          datediff(second, Event_Start_Time, Event_End_Time)
                                                                as Event_Duration,
                          datediff(second, Instance_Start_Time, Instance_End_Time)
                                                                as Viewing_Duration,
                          case
                            when (Live_Flag = 0) then dateadd(second, -Time_Since_Recording, Instance_Start_Time)
                              else null
                          end                                   as Recorded_Time,
                          Channel_Id                            as Channel_Identifier,
                          Live_Flag                             as Live_Flag,
                          BARB_Minute_Start                     as BARB_Minute_Start,
                          BARB_Minute_End                       as BARB_Minute_End,
                          cast(''Delta Table'' as varchar(15))  as Source
                      into VESPA_MinAttr_Phase2_01_Viewing_Delta
                      from ' || @parInputTable || '
                     where Instance_Start_Time < Instance_End_Time              -- Additional check - exclude events 0-second long and with missing time fields
                       and Instance_Start_Time is not null
                       and Instance_End_Time is not null
                    '
      execute(@varSql)
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase2_01_Viewing_Delta(Instance_Id)
      create hg index idx2 on VESPA_MinAttr_Phase2_01_Viewing_Delta(Subscriber_Id)
      create dttm index idx3 on VESPA_MinAttr_Phase2_01_Viewing_Delta(Viewing_Starts)
      create dttm index idx4 on VESPA_MinAttr_Phase2_01_Viewing_Delta(Viewing_Stops)

      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'Step 0.2: Input table created', null
        end



        -- ##############################################################################################################
        -- ##############################################################################################################
        -- ### SECTION 1 - ATTRIBUTE RELEVANT TX TIME FOR PLAYBACK EVENTS
        -- ### Rules:
        -- ###    - minute attribution is in respect to the original TX time
        -- ###    - minutes are inclusive, i.e. 12:20-12:30 indicates 11 minutes of viewing, including :20 & :30
        -- ###    - events must last at least 31 seconds in a minute, i.e. consider event starting at 9m40s and ending at 10m20s -
        -- ###            there is not more than 30 seconds in either minute (9 & 10) and therefore it won't be considered
        -- ###    - all playback events are considered independently
        -- ###    - START TIME: if viewing second is 00-29 -> START MINUTE is truncated (i.e. 12:00:25 -> 12:00)
        -- ###    - START TIME: if viewing second is 30-59 -> START MINUTE is truncated plus +1 minute added(i.e. 12:00:40 -> 12:01)
        -- ###    - END TIME: if viewing second is 00-29 -> END MINUTE is truncated plus -1 minute added (i.e. 12:00:25 -> 11:59)
        -- ###    - END TIME: if viewing second is 30-59 -> END MINUTE is truncated (i.e. 12:00:40 -> 12:00)
        -- ##############################################################################################################
        -- ##############################################################################################################


        -- ##############################################################################################################
        -- ##### STEP 1.1 - applying minute attribution rules for playback events                                   #####
        -- ##############################################################################################################
      update VESPA_MinAttr_Phase2_01_Viewing_Delta base
         set base.BARB_Minute_Start = case
                                          -- Events lasting 30s or less
                                        when (datediff(second, Recorded_Time, dateadd(second, viewing_duration - 1, Recorded_Time)) + 1 <= 30) then null

                                          -- Events starting and ending in two different/adjacent minutes but lasting 30s or less in both minutes
                                        when (datediff(second, Recorded_Time, dateadd(second, viewing_duration - 1, Recorded_Time)) <= 60) and
                                             (second(Recorded_Time) >= 30) and (second(dateadd(second, viewing_duration - 1, Recorded_Time)) <= 29) then null

                                          -- Qualifying events - rounding down
                                        when second(Recorded_Time) <= 29 then dateadd(second, -second(Recorded_Time), Recorded_Time)

                                          -- Qualifying events - rounding up
                                        when second(Recorded_Time) >= 30 then dateadd(second, 60 - second(Recorded_Time), Recorded_Time)

                                          else null

                                      end,

             base.BARB_Minute_End   = case
                                          -- Events lasting 29s or less
                                        when (datediff(second, Recorded_Time, dateadd(second, viewing_duration - 1, Recorded_Time)) + 1 <= 30) then null

                                          -- Events starting and ending in two different/adjacent minutes but lasting 29s or less in both minutes
                                        when (datediff(second, Recorded_Time, dateadd(second, viewing_duration - 1, Recorded_Time)) <= 60) and
                                             (second(Recorded_Time) >= 30) and (second(dateadd(second, viewing_duration - 1, Recorded_Time)) <= 29) then null

                                          -- Qualifying events - rounding down
                                        when second(dateadd(second, viewing_duration - 1, Recorded_Time)) <= 29
                                              then dateadd(second, -(60 + second(dateadd(second, viewing_duration - 1, Recorded_Time))), dateadd(second, viewing_duration - 1, Recorded_Time))

                                          -- Qualifying events - rounding up
                                        when second(dateadd(second, viewing_duration - 1, Recorded_Time)) >= 30
                                              then dateadd(second, 60 - (60 + second(dateadd(second, viewing_duration - 1, Recorded_Time))), dateadd(second, viewing_duration - 1, Recorded_Time))

                                          else null
                                      end
       where Live_Flag = 0
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of playback events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_01_Viewing_Delta
               where Live_Flag = 0

              execute logger_add_event @varBuildId, 3, 'Step 1.1 (Playback): Total number of events [expected: N/A]', @QA_result_1


                -- ##### Total number of playback events with minute attributed #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_01_Viewing_Delta
               where Live_Flag = 0
                 and BARB_Minute_Start is not null

              execute logger_add_event @varBuildId, 3, 'Step 1.1 (Playback): Number of attributed events [expected: < # of Playback Events]', @QA_result_1

        end

      set @QA_result_1 = -1
      commit



        -- ##############################################################################################################
        -- ##############################################################################################################
        -- ### SECTION 2 - ATTRIBUTE MINUTES FOR LIVE EVENTS
        -- ### Rules:
        -- ###    - minutes with total viewing equal or less than @varMinMinuteViewingCutoff to be left unattributed
        -- ###    - events lasting for @varSurfingCutoff or less to be classified as "Surfing"
        -- ###    - if total Surfing time in a minute is longer than any other viewing event in that minute, then:
        -- ###        - viewing for that minute is not attributed
        -- ###        - a record is created in a separate table which represents atribution to "Surfing" for that minute
        -- ###    - if surfing condition is not met, viewing time is attributed to the first, longest event in a minute
        -- ##############################################################################################################
        -- ##############################################################################################################


        -- ##############################################################################################################
        -- ##### STEP 2.1 - creating sub-universe with all live viewing records                                     #####
        -- ##############################################################################################################
      if object_id('VESPA_MinAttr_Phase2_02_All_Live_Viewing') is not null drop table VESPA_MinAttr_Phase2_02_All_Live_Viewing
      select
            Instance_Id,
            Subscriber_Id,
            Process_Date,
            Channel_Identifier,
            Event_Starts,
            dateadd(second, -1, Event_Stops) as Event_Stops,        -- Move to inclusive seconds/minutes
            Viewing_Starts,
            dateadd(second, -1, Viewing_Stops) as Viewing_Stops,    -- Move to inclusive seconds/minutes
            Event_Duration,
            Viewing_Duration as Instance_Duration,
            cast(null as datetime) as Viewing_Starts_Min,
            cast(null as datetime) as Viewing_Stops_Min,
            case
              when (Event_Duration <= @varSurfingCutoff) then 1
                else 0
            end as Event_Surfing_Flag,

            cast(0 as tinyint) as StartEndMinute_Relation,
            cast(0 as tinyint) as StartMinute_Result,
            cast(0 as tinyint) as EndMinute_Result,
            cast(null as datetime) as BARB_Minute_Start,
            cast(null as datetime) as BARB_Minute_End

        into VESPA_MinAttr_Phase2_02_All_Live_Viewing
        from VESPA_MinAttr_Phase2_01_Viewing_Delta
       where Live_Flag = 1
      commit

      update VESPA_MinAttr_Phase2_02_All_Live_Viewing
        set Viewing_Starts_Min  = dateadd(second, -second(Viewing_Starts), Viewing_Starts),
            Viewing_Stops_Min   = dateadd(second, -second(Viewing_Stops), Viewing_Stops)
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase2_02_All_Live_Viewing(Instance_Id)
      create hg index idx2 on VESPA_MinAttr_Phase2_02_All_Live_Viewing(Subscriber_Id)
      create dttm index idx3 on VESPA_MinAttr_Phase2_02_All_Live_Viewing(Viewing_Starts)
      create dttm index idx4 on VESPA_MinAttr_Phase2_02_All_Live_Viewing(Viewing_Stops)


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of live events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing

              execute logger_add_event @varBuildId, 3, 'Step 2.1 (Live): Total number of events [expected: N/A]', @QA_result_1

        end

      set @QA_result_1 = -1
      commit


        -- ##############################################################################################################
        -- ##### STEP 2.2 - splitting viewing records so each minute as in "viewing start" & "viewing end" fields   #####
        -- #####            is represented by a single record                                                       #####
        -- ##############################################################################################################
      if object_id('VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min') is not null drop table VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
      create table VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min (
          RowId                             unsigned bigint not null primary key default autoincrement,
          Instance_Id                       bigint    default null,
          Subscriber_Id                     bigint    default null,
          Process_Date                      date      default null,
          Channel_Identifier                bigint    default null,
          Channel_Identifier_Aggr_Surfing   bigint    default null,
          Record_Type                       smallint  default null,
          Event_Starts                      datetime  default null,
          Event_Stops                       datetime  default null,
          Viewing_Starts                    datetime  default null,
          Viewing_Stops                     datetime  default null,
          Min_Viewing_Starts                datetime  default null,
          Min_Viewing_Ends                  datetime  default null,
          Minute                            datetime  default null,
          Minute_Seq                        tinyint   default 0,
          Event_Surfing_Flag                smallint  default null,
          Event_Duration                    bigint    default null,
          Instance_Duration                 bigint    default null,
          Instance_Duration_In_Minute       bigint    default null,
          Total_Surfing_In_Minute           tinyint   default 0,
          Total_Channel_Duration_In_Minute  tinyint   default 0,
          Total_Duration_In_Minute          tinyint   default 0,
          First_Longest_Group_Flag          bit       default 0,
          First_Longest_Instance_Flag       bit       default 0,
          Minute_Attribution_Result         tinyint   default 0
      )

      create hg index idx1 on VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min(Instance_Id)
      create hg index idx2 on VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min(Subscriber_Id)
      create dttm index idx3 on VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min(Min_Viewing_Starts)
      create dttm index idx4 on VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min(Min_Viewing_Ends)
      create dttm index idx5 on VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min(Minute)
      create unique index idx6 on VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min(Instance_Id, Minute)


        -- ###############################################################################
        -- ##### Get records starting & ending in the same minute                    #####
        -- ###############################################################################
      insert into VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
                  (Instance_Id, Subscriber_Id, Process_Date, Channel_Identifier, Channel_Identifier_Aggr_Surfing,
                   Record_Type, Event_Starts, Event_Stops, Viewing_Starts, Viewing_Stops, Min_Viewing_Starts,
                   Min_Viewing_Ends, Minute, Event_Surfing_Flag, Event_Duration, Instance_Duration,
                   Instance_Duration_In_Minute)
      select
            Instance_Id,
            Subscriber_Id,
            Process_Date,
            Channel_Identifier,
            cast(null as integer) as Channel_Identifier_Aggr_Surfing,
            1 as Record_Type,
            Event_Starts,
            Event_Stops,
            Viewing_Starts,
            Viewing_Stops,
            Viewing_Starts as Min_Viewing_Starts,
            Viewing_Stops as Min_Viewing_Ends,
            dateadd(second, -second(Viewing_Starts), Viewing_Starts) as Minute,
            Event_Surfing_Flag,
            Event_Duration,
            Instance_Duration,
            Instance_Duration as Instance_Duration_In_Minute
        from VESPA_MinAttr_Phase2_02_All_Live_Viewing
       where datediff(second, Viewing_Starts, Viewing_Stops) <= 61
         and minute(Viewing_Starts) = minute(Viewing_Stops)
      commit


        -- ###############################################################################
        -- ##### Get start minute part events                                        #####
        -- ###############################################################################
      insert into VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
                  (Instance_Id, Subscriber_Id, Process_Date, Channel_Identifier, Channel_Identifier_Aggr_Surfing,
                   Record_Type, Event_Starts, Event_Stops, Viewing_Starts, Viewing_Stops, Min_Viewing_Starts,
                   Min_Viewing_Ends, Minute, Event_Surfing_Flag, Event_Duration, Instance_Duration,
                   Instance_Duration_In_Minute)
      select
            Instance_Id,
            Subscriber_Id,
            Process_Date,
            Channel_Identifier,
            cast(null as integer) as Channel_Identifier_Aggr_Surfing,
            2 as Record_Type,
            Event_Starts,
            Event_Stops,
            Viewing_Starts,
            Viewing_Stops,
            Viewing_Starts as Min_Viewing_Starts,
            dateadd(second, (60 - second(Viewing_Starts)) - 1, Viewing_Starts) as Min_Viewing_Ends,
            dateadd(second, -second(Viewing_Starts), Viewing_Starts) as Minute,
            Event_Surfing_Flag,
            Event_Duration,
            Instance_Duration,
            60 - second(Viewing_Starts) as Instance_Duration_In_Minute
        from VESPA_MinAttr_Phase2_02_All_Live_Viewing
       where datediff(second, Viewing_Starts, Viewing_Stops) > 61
          or minute(Viewing_Starts) <> minute(Viewing_Stops)
      commit


        -- ###############################################################################
        -- ##### Get end minute part events                                          #####
        -- ###############################################################################
      insert into VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
                  (Instance_Id, Subscriber_Id, Process_Date, Channel_Identifier, Channel_Identifier_Aggr_Surfing,
                   Record_Type, Event_Starts, Event_Stops, Viewing_Starts, Viewing_Stops, Min_Viewing_Starts,
                   Min_Viewing_Ends, Minute, Event_Surfing_Flag, Event_Duration, Instance_Duration,
                   Instance_Duration_In_Minute)
      select
            Instance_Id,
            Subscriber_Id,
            Process_Date,
            Channel_Identifier,
            cast(null as integer) as Channel_Identifier_Aggr_Surfing,
            3 as Record_Type,
            Event_Starts,
            Event_Stops,
            Viewing_Starts,
            Viewing_Stops,
            dateadd(second, -second(Viewing_Stops), Viewing_Stops) as Min_Viewing_Starts,
            Viewing_Stops as Min_Viewing_Ends,
            dateadd(second, -second(Viewing_Stops), Viewing_Stops) as Minute,
            Event_Surfing_Flag,
            Event_Duration,
            Instance_Duration,
            second(Viewing_Stops) + 1 as Instance_Duration_In_Minute
        from VESPA_MinAttr_Phase2_02_All_Live_Viewing
       where datediff(second, Viewing_Starts, Viewing_Stops) > 61
          or minute(Viewing_Starts) <> minute(Viewing_Stops)
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of split events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min

              execute logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Total number of split events [expected: > Total # of events]', @QA_result_1


                -- ##### Total number of "same minute" events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               where Record_Type = 1

              execute logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Number of Same Minute events [expected: N/A]', @QA_result_1


                -- ##### Total number of "start minute" events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               where Record_Type = 2

              execute logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Number of Start Minute events [expected: N/A]', @QA_result_1


                -- ##### Total number of "end minute" events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               where Record_Type = 3

              execute logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Number of End Minute events [expected: =Start Minute events]', @QA_result_1

        end

      set @QA_result_1 = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.3 - calculating required totals & metrics for each minute and finally applying minute       #####
        -- #####            attribution                                                                             #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Rank events in each minute (create a sequence)                      #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase2_tmp_Minute_Ranks') is not null drop table VESPA_MinAttr_Phase2_tmp_Minute_Ranks
      select
            RowId,
            rank() over (partition by Subscriber_Id, Minute order by Min_Viewing_Starts, Min_Viewing_Ends, RowId) as Minute_Seq
        into VESPA_MinAttr_Phase2_tmp_Minute_Ranks
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase2_tmp_Minute_Ranks(RowId)

      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base
         set base.Minute_Seq = det.Minute_Seq
        from VESPA_MinAttr_Phase2_tmp_Minute_Ranks det
       where base.RowId = det.RowId
      commit


        -- ###############################################################################
        -- ##### Create revised channel identifier which combines all surfing events #####
        -- ##### into a single ID                                                    #####
        -- ###############################################################################
      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base
         set base.Channel_Identifier_Aggr_Surfing = case
                                                      when base.Event_Surfing_Flag = 1 then -99999999
                                                        else base.Channel_Identifier
                                                    end
      commit


        -- ###############################################################################
        -- ##### Flag first longest event in each minute                             #####
        -- ###############################################################################
        -- #### Get total channel viewing duration in minute
      if object_id('VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration') is not null drop table VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration
      select
            Subscriber_Id,
            Minute,
            Channel_Identifier_Aggr_Surfing,
            sum(Instance_Duration_In_Minute) as Total_Channel_Duration_In_Minute
        into VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
       group by Subscriber_Id, Minute, Channel_Identifier_Aggr_Surfing
      commit

      create hg index idx1 on VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration(Subscriber_Id)
      create dttm index idx2 on VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration(Minute)
      create hg index idx3 on VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration(Channel_Identifier_Aggr_Surfing)

      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base
         set base.Total_Channel_Duration_In_Minute = det.Total_Channel_Duration_In_Minute
        from VESPA_MinAttr_Phase2_tmp_Total_Channel_Duration det
       where base.Subscriber_Id = det.Subscriber_Id
         and base.Minute = det.Minute
         and base.Channel_Identifier_Aggr_Surfing = det.Channel_Identifier_Aggr_Surfing
      commit


        -- ##### Flag the longest channel group in each minute #####
        -- Identify max duration value per minute
      if object_id('VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration') is not null drop table VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration
      select
            Subscriber_Id,
            Minute,
            max(Total_Channel_Duration_In_Minute) as Max_Total_Channel_Duration_In_Minute
        into VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
       group by Subscriber_Id, Minute
      commit

      create hg index idx1 on VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration(Subscriber_Id)
      create dttm index idx2 on VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration(Minute)

        -- Identify first instance id within minute, filtered by max duration value
      if object_id('VESPA_MinAttr_Phase2_tmp_Longest_Group') is not null drop table VESPA_MinAttr_Phase2_tmp_Longest_Group
      select
            base.Subscriber_Id,
            base.Minute,
            min(Minute_Seq) as Min_Minute_Seq,
            min(cast(null as int)) as Channel_Identifier_Aggr_Surfing
        into VESPA_MinAttr_Phase2_tmp_Longest_Group
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base,
             VESPA_MinAttr_Phase2_tmp_Longest_Group_Max_Duration mins
       where base.Subscriber_Id = mins.Subscriber_Id
         and base.Minute = mins.Minute
         and base.Total_Channel_Duration_In_Minute = mins.Max_Total_Channel_Duration_In_Minute
       group by base.Subscriber_Id, base.Minute
      commit

      create hg index idx1 on VESPA_MinAttr_Phase2_tmp_Longest_Group(Subscriber_Id)
      create dttm index idx2 on VESPA_MinAttr_Phase2_tmp_Longest_Group(Minute)
      create hg index idx3 on VESPA_MinAttr_Phase2_tmp_Longest_Group(Min_Minute_Seq)

      update VESPA_MinAttr_Phase2_tmp_Longest_Group base
         set base.Channel_Identifier_Aggr_Surfing = det.Channel_Identifier_Aggr_Surfing
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min det
       where base.Subscriber_id = det.Subscriber_id
         and base.Minute = det.Minute
         and base.Min_Minute_Seq = det.Minute_Seq
      commit

        -- Append to the live viewing table
      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base
         set base.First_Longest_Group_Flag = 1
        from VESPA_MinAttr_Phase2_tmp_Longest_Group det
       where base.Subscriber_id = det.Subscriber_id
         and base.Minute = det.Minute
         and base.Channel_Identifier_Aggr_Surfing = det.Channel_Identifier_Aggr_Surfing
      commit


        -- ##### Get the first longest instance for the first longest channel group in each minute #####
      if object_id('VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks') is not null drop table VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks
      select
            RowId,
                Subscriber_Id,
                Minute,
                Minute_Seq,
                Instance_Duration_In_Minute,
                Total_Channel_Duration_In_Minute,
            rank() over (partition by Subscriber_Id, Minute order by First_Longest_Group_Flag desc, Instance_Duration_In_Minute desc, Minute_Seq, RowId) as First_Longest_Rank
        into VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
      commit

      create hg index idx1 on VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks(RowId)

      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base
         set base.First_Longest_Instance_Flag  = 1
        from VESPA_MinAttr_Phase2_tmp_First_Longest_Ranks det
       where base.RowId = det.RowId
         and det.First_Longest_Rank = 1
      commit


        -- ###############################################################################
        -- ##### Calculate total surfing & total viewing time in each minute         #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase2_tmp_Minute_Summaries') is not null drop table VESPA_MinAttr_Phase2_tmp_Minute_Summaries
      select
            Subscriber_Id,
            Minute,
            sum(case
                  when (Event_Surfing_Flag = 1) then Instance_Duration_In_Minute
                    else 0
                end) as Total_Surfing_In_Minute,
            sum(Instance_Duration_In_Minute) as Total_Duration_In_Minute
       into VESPA_MinAttr_Phase2_tmp_Minute_Summaries
       from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
      group by Subscriber_Id, Minute

      create hg index idx1 on VESPA_MinAttr_Phase2_tmp_Minute_Summaries(Subscriber_Id)
      create dttm index idx2 on VESPA_MinAttr_Phase2_tmp_Minute_Summaries(Minute)


      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min base
         set base.Total_Surfing_In_Minute = det.Total_Surfing_In_Minute,
             base.Total_Duration_In_Minute = det.Total_Duration_In_Minute
        from VESPA_MinAttr_Phase2_tmp_Minute_Summaries det
       where base.Subscriber_Id = det.Subscriber_Id
         and base.Minute = det.Minute
      commit


        -- ##### QA #####
        -- ##### Number of instances without First Longest & without surfing events #####
      select @QA_result_1 = count(1)
        from (select Subscriber_Id, Minute, max(First_Longest_Instance_Flag) as Max_First_Longest_Instance_Flag, sum(Total_Surfing_In_Minute) as Sum_Total_Surfing_In_Minute
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               group by Subscriber_Id, Minute) det
       where Max_First_Longest_Instance_Flag = 0
         and Sum_Total_Surfing_In_Minute = 0

      if (@QA_result_1 > 0)
          execute logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Instance minutes without First Longest & surfing event [expected: 0]', @QA_result_1
      else
        if (@parLogQADetails = 1)
            execute logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Instance minutes without First Longest & surfing event [expected: 0]', @QA_result_1

        -- ##### Number of minutes with no instances of First_Longest_Instance_Flag = 1 (flag to be used for minute attribution) #####
      select @QA_result_1 = count(1)
        from (select Subscriber_Id, Minute, max(case when First_Longest_Instance_Flag = 1 then 1 else 0 end) as First_longest_Exists_Flag
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               group by Subscriber_Id, Minute
              having First_longest_Exists_Flag = 0) det

      if (@QA_result_1 > 0)
          execute logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Minutes with no instances of First_Longest_Instance_Flag=1 [expected: 0]', @QA_result_1
      else
        if (@parLogQADetails = 1)
            execute logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Minutes with no instances of First_Longest_Instance_Flag=1 [expected: 0]', @QA_result_1

      set @QA_result_1 = -1
      commit

        -- ##### Number of minutes with more than one First Longest #####
      select @QA_result_1 = count(1)
        from (select Subscriber_Id, Minute
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               where First_Longest_Instance_Flag = 1
               group by Subscriber_Id, Minute
              having count(*) > 1) det

      if (@QA_result_1 > 0)
          execute logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Minutes with more than one First Longest event [expected: 0]', @QA_result_1
      else
        if (@parLogQADetails = 1)
            execute logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Minutes with more than one First Longest event [expected: 0]', @QA_result_1

      set @QA_result_1 = -1
      commit


        -- ##### QA #####
        -- ##### Number of minutes with total > 60 #####
      select @QA_result_1 = count(1)
        from (select Subscriber_Id, Minute
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               group by Subscriber_Id, Minute
              having max(Total_Duration_In_Minute) > 60) det

      if (@QA_result_1 > 0)
          execute logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Minutes with Total Duration > 60 [expected: 0]', @QA_result_1
      else
        if (@parLogQADetails = 1)
            execute logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Minutes with Total Duration > 60 [expected: 0]', @QA_result_1

      set @QA_result_1 = -1
      commit


        -- ###############################################################################
        -- ##### Final step - based on rules, apply minute attribution               #####
        -- ###############################################################################
      update VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
         set Minute_Attribution_Result  = case

                                              -- Total viewing equal or less than the cutoff point (currently 30 seconds - viewing must be 31 seconds or more)
                                            when (Total_Duration_In_Minute <= @varMinMinuteViewingCutoff) then 1

                                              -- Flag all events except the first longest one
                                            when (First_Longest_Instance_Flag = 0) then 2

                                              -- Flag as "Surfing", when first longest instance within the first longest channel group is surfing
                                            when (First_Longest_Instance_Flag = 1) and (Event_Surfing_Flag = 1) then 3

                                              -- Else, flag as channel viewing
                                            when (First_Longest_Instance_Flag = 1) then 10

                                              else 0
                                          end
      commit


        -- ##### QA #####
        -- ##### Number of records with Minute_Attribution_Result=0 #####
      select @QA_result_1 = count(1)
        from (select Subscriber_Id, Minute
                from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
               group by Subscriber_Id, Minute
              having min(Minute_Attribution_Result) = 0) det

      if (@QA_result_1 > 0)
          execute logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Events with Minute_Attribution_Result=0 [expected: 0]', @QA_result_1
      else
        if (@parLogQADetails = 1)
            execute logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Events with Minute_Attribution_Result=0 [expected: 0]', @QA_result_1

      set @QA_result_1 = -1
      commit


        -- ###############################################################################
        -- ##### Apply results back to the original table for non-split records      #####
        -- ###############################################################################
      update VESPA_MinAttr_Phase2_02_All_Live_Viewing base
         set StartEndMinute_Relation  = case
                                            -- Start & End minutes are the same
                                          when (Viewing_Starts_Min = Viewing_Stops_Min) then 2
                                            -- End minute is right after the start one (spans over 2 minutes)
                                          when datediff(minute, Viewing_Starts_Min, Viewing_Stops_Min) = 1 then 1
                                            -- Event spans over at least 3 minutes
                                            else 0
                                        end
      commit

      update VESPA_MinAttr_Phase2_02_All_Live_Viewing base
         set base.StartMinute_Result = det.Minute_Attribution_Result
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min det
       where base.Instance_Id = det.Instance_Id
         and base.Viewing_Starts_Min = det.Minute
      commit

      update VESPA_MinAttr_Phase2_02_All_Live_Viewing base
         set base.EndMinute_Result = det.Minute_Attribution_Result
        from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min det
       where base.Instance_Id = det.Instance_Id
         and base.Viewing_Stops_Min = det.Minute
      commit


        -- ##### QA #####
        -- ##### Number of events with missing calculation result #####
      select @QA_result_1 = count(1)
        from VESPA_MinAttr_Phase2_02_All_Live_Viewing
       where StartMinute_Result is null
          or StartMinute_Result = 0
          or EndMinute_Result is null
          or EndMinute_Result = 0

      if (@QA_result_1 > 0)
          execute logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Events missing calculation result [expected: 0]', @QA_result_1
      else
        if (@parLogQADetails = 1)
            execute logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Events missing calculation result [expected: 0]', @QA_result_1

      set @QA_result_1 = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.4 - calculating BARB minute                                                                 #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Calculate BARB minute                                               #####
        -- ###############################################################################
      update VESPA_MinAttr_Phase2_02_All_Live_Viewing base
         set BARB_Minute_Start    = case
                                        -- Ignore Surfing events
                                      when (Event_Surfing_Flag = 1) then null

                                        -- Attribute current minute when it's the winning one
                                      when (StartMinute_Result = 10) then Viewing_Starts_Min

                                        -- Round up when Minute spans over at least 3 minutes AND is a non-winning one
                                      when (StartEndMinute_Relation = 0) and (StartMinute_Result < 10) then dateadd(minute, 1, Viewing_Starts_Min)

                                        -- Round up when Minute spans over 2 minutes AND is a non-winning one AND the next one is the winning one
                                      when (StartEndMinute_Relation = 1) and (StartMinute_Result < 10) and (EndMinute_Result = 10) then dateadd(minute, 1, Viewing_Starts_Min)

                                        -- "Remove" in other cases
                                      when ( (StartEndMinute_Relation = 2) and (StartMinute_Result < 10) ) or
                                           ( (StartEndMinute_Relation = 1) and (StartMinute_Result < 10) and (EndMinute_Result < 10) )
                                              then null

                                        else '1900-01-01 00:00:00'  -- (this value is not expected to occur - "default" value for QA purposes)
                                    end,

             BARB_Minute_End      = case
                                        -- Ignore Surfing events
                                      when (Event_Surfing_Flag = 1) then null

                                        -- Attribute current minute when it's the winning one
                                      when (EndMinute_Result = 10) then Viewing_Stops_Min

                                        -- Round down when Minute spans over at least 3 minutes AND is a non-winning one
                                      when (StartEndMinute_Relation = 0) and (EndMinute_Result < 10) then dateadd(minute, -1, Viewing_Stops_Min)

                                        -- Round up when Minute spans over 2 minutes AND is a non-winning one AND the previous one is the winning one
                                      when (StartEndMinute_Relation = 1) and (EndMinute_Result < 10) and (StartMinute_Result = 10) then dateadd(minute, -1, Viewing_Stops_Min)

                                        -- "Remove" in other cases
                                      when ( (StartEndMinute_Relation = 2) and (EndMinute_Result < 10) ) or
                                           ( (StartEndMinute_Relation = 1) and (EndMinute_Result < 10) and (StartMinute_Result < 10) )
                                              then null

                                        else '1900-01-01 00:00:00'  -- (this value is not expected to occur - "default" value for QA purposes)
                                    end
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Total number of live events #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing

              execute logger_add_event @varBuildId, 3, 'Step 2.4 (Live): Total number of events [expected: N/A]', @QA_result_1


                -- ##### Total number of live events with minute attributed #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_02_All_Live_Viewing
               where BARB_Minute_Start is not null

              execute logger_add_event @varBuildId, 3, 'Step 2.4 (Live): Number of attributed events [expected: < # of Events]', @QA_result_1

        end

      set @QA_result_1 = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.5 - creating Surfing events                                                                 #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Delete surfing events for days being processed                      #####
        -- ###############################################################################
        -- ### Select processed dates ###
      if object_id('VESPA_MinAttr_Phase2_tmp_Dates_Processed') is not null drop table VESPA_MinAttr_Phase2_tmp_Dates_Processed
      select distinct
            Process_Date,
            cast(1 as bit) as Dummy
        into VESPA_MinAttr_Phase2_tmp_Dates_Processed
        from VESPA_MinAttr_Phase2_01_Viewing_Delta
      commit

      create date index idx1 on VESPA_MinAttr_Phase2_tmp_Dates_Processed(Process_Date)

        -- ##### QA (1) #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Total number of constituent records #####
              -- select @QA_result_1 = count(1)
              --   from VESPA_SURF_CONSTITUENTS_PHASE2

              -- ##### Total number of surfing records #####
              select @QA_result_2 = count(1)
                from VESPA_SURF_MINUTES_PHASE2

        end

        -- ### Delete constituent records for days being processed ###

      -- delete from VESPA_SURF_CONSTITUENTS_PHASE2
      --  where Surf_Id in (select Surf_Id
      --                      from VESPA_SURF_MINUTES_PHASE2 a,
      --                           VESPA_MinAttr_Phase2_tmp_Dates_Processed b
      --                     where date(a.Surf_Minute_Start) = b.Process_Date)
      -- commit

        -- ### Delete surfing records for days being processed ###
      delete from VESPA_SURF_MINUTES_PHASE2
       where date(Surf_Minute_Start) in (select
                                               Process_Date
                                           from VESPA_MinAttr_Phase2_tmp_Dates_Processed)
      commit


        -- ##### QA (2) #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Deleted constituent records #####
              -- select @QA_result_1 = @QA_result_1 - count(1)
              --                                        from VESPA_SURF_CONSTITUENTS_PHASE2

              -- ##### Deleted constituent records #####
              select @QA_result_2 = @QA_result_2 - count(1)
                                                     from VESPA_SURF_MINUTES_PHASE2

              execute logger_add_event @varBuildId, 3, 'Step 2.5 (Live): Total number of Surfing records deleted [expected: N/A]', @QA_result_2
              -- execute logger_add_event @varBuildId, 3, 'Step 2.5 (Live): Total number of Constituent records deleted [expected: N/A]', @QA_result_1

        end

      set @QA_result_1 = -1
      set @QA_result_2 = -1
      commit


        -- ###############################################################################
        -- ##### Add one-minute events                                               #####
        -- ###############################################################################
      insert into VESPA_SURF_MINUTES_PHASE2 (Subscriber_Id, Surf_Minute_Start, Surf_Minute_End, build_date)
        select
              Subscriber_Id,
              Minute,
              Minute,
              min(@parBuildDate)
          from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
         where Minute_Attribution_Result = 3                        -- Minute => Surfing
           and First_Longest_Instance_Flag = 1                      -- Pick the first longest record
         group by Subscriber_Id, Minute, Minute                     -- Ensure uniqueness, although it shuold not be necessary if data was 100% correct
      commit


        -- ###############################################################################
        -- ##### Add complete minutes for evSurv events spanning over at least 3     #####
        -- ##### minutes                                                             #####
        -- ###############################################################################
      -- insert into VESPA_SURF_MINUTES_PHASE2 (Subscriber_Id, Surf_Minute_Start, Surf_Minute_End, build_date)
      --   select distinct
      --         Subscriber_Id,
      --         dateadd(second, (60 - second(Viewing_Starts)), Viewing_Starts),
      --         dateadd(second, -second(Viewing_Stops) - 60, Viewing_Stops),
      --         @parBuildDate
      --     from VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min
      --    where Event_Type = 'evSurf'
      --      and datediff(minute, Viewing_Starts, Viewing_Stops) >= 2      -- only events spanning over at least THREE different minutes
      -- commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Total number of surfing events #####
              select @QA_result_1 = count(1)
                from VESPA_SURF_MINUTES_PHASE2
               where build_date = @parBuildDate

              execute logger_add_event @varBuildId, 3, 'Step 2.5 (Live): Total number of Surfing events created [expected: N/A]', @QA_result_1

        end

      set @QA_result_1 = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.6 - creating constituents for Surfing events                                                #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Temporary table to calculate the front minute                       #####
        -- ###############################################################################
/*
      if object_id('VESPA_MinAttr_Phase2_tmp_Surf_Constituents') is not null drop table VESPA_MinAttr_Phase2_tmp_Surf_Constituents
      create table VESPA_MinAttr_Phase2_tmp_Surf_Constituents (
          Row_Id                      bigint      identity primary key,
          Subscriber_Id               bigint      not null,
          Surf_Id                     bigint      not null,
          Instance_Id                 bigint      not null,
          Minute_Seq                  tinyint     not null default 1,
          Surf_Minute_Start           datetime    null     default null,
          Surf_Minute_End             datetime    null     default null,
          Viewing_Starts              datetime    null     default null,
          Viewing_Stops               datetime    null     default null,
          Front_Minute                bit         null     default 0,
          Source                      varchar(10) null     default null,
      )

      create hg index idx1 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents(Surf_Id)
      create hg index idx2 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents(Instance_Id)
      create unique hg index idx3 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents(Surf_Id, Instance_Id)
      create dttm index idx4 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents(Viewing_Starts)
      create dttm index idx5 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents(Viewing_Stops)

      insert into VESPA_MinAttr_Phase2_tmp_Surf_Constituents
                    (Subscriber_Id, Surf_Id, Instance_Id, Minute_Seq, Surf_Minute_Start, Surf_Minute_End, Viewing_Starts, Viewing_Stops, Source)
        select
              base.Subscriber_Id,
              base.Surf_Id,
              det.Instance_Id,
              det.Minute_Seq,
              base.Surf_Minute_Start,
              base.Surf_Minute_End,
              det.Viewing_Starts,
              det.Viewing_Stops,
              'Minute'
          from VESPA_SURF_MINUTES_PHASE2 base,
               VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min det
         where base.Subscriber_Id = det.Subscriber_Id
           and base.Surf_Minute_Start = det.Minute
           and Event_Surfing_Flag = 1
        commit


      -- insert into VESPA_MinAttr_Phase2_tmp_Surf_Constituents
      --               (Subscriber_Id, Surf_Id, Instance_Id, Surf_Minute_Start, Surf_Minute_End, Viewing_Starts, Viewing_Stops, Source)
      --   select distinct
      --         base.Subscriber_Id,
      --         base.Surf_Id,
      --         det.Instance_Id,
      --         base.Surf_Minute_Start,
      --         base.Surf_Minute_End,
      --         det.Viewing_Starts,
      --         det.Viewing_Stops,
      --         'evSurf'
      --     from VESPA_SURF_MINUTES_PHASE2 base,
      --          VESPA_MinAttr_Phase2_03_All_Live_Viewing_By_Min det
      --    where Event_Type = 'evSurf'
      --      and datediff(minute, Viewing_Starts, Viewing_Stops) >= 2      -- only events spanning over at least THREE different minutes
      --      and base.Surf_Minute_Start = dateadd(second, (60 - second(det.Viewing_Starts)), det.Viewing_Starts)
      -- commit


        -- ###############################################################################
        -- ##### Get & apply ranks                                                   #####
        -- ###############################################################################
        -- ### Check "VESPA_MinAttr_Phase2_tmp_Surf_Constituents" is not empty since
        -- ### if it's the case, Sybase won't do rank() - next step
      select @QA_result_1 = count(1)
        from VESPA_MinAttr_Phase2_tmp_Surf_Constituents

      if (@QA_result_1 > 0)
        begin

            if object_id('VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min') is not null drop table VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min
            select
                  Row_Id,
                  rank() over (partition by Subscriber_Id, Instance_Id order by Surf_Minute_Start, Surf_Minute_End, Row_Id) as Surf_Seq
              into VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min
              from VESPA_MinAttr_Phase2_tmp_Surf_Constituents
            commit

            create unique hg index idx1 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min(Row_Id)
            create lf index idx2 on VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min(Surf_Seq)

              -- ##### Get first instances of each record #####
            update VESPA_MinAttr_Phase2_tmp_Surf_Constituents base
               set base.front_minute = 1
              from VESPA_MinAttr_Phase2_tmp_Surf_Constituents_Front_Min det
             where base.Row_Id = det.Row_Id
               and det.Surf_Seq = 1
            commit

              -- ##### Reset back to 0 for cases when only a single Instance_Id exists #####
            update VESPA_MinAttr_Phase2_tmp_Surf_Constituents base
               set base.front_minute = 0
              from (select
                          Instance_Id,
                          count(*) as Cnt
                      from VESPA_MinAttr_Phase2_tmp_Surf_Constituents
                     group by Instance_Id
                    having count(*) = 1) det
             where base.Instance_Id = det.Instance_Id
            commit

        end


        -- ###############################################################################
        -- ##### Fill the destination table                                          #####
        -- ###############################################################################
      insert into VESPA_SURF_CONSTITUENTS_PHASE2 (Surf_Id, Instance_Id, Minute_Seq, Front_Minute, Build_Date)
        select
              Surf_Id,
              Instance_Id,
              Minute_Seq,
              Front_Minute,
              @parBuildDate
          from VESPA_MinAttr_Phase2_tmp_Surf_Constituents
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Total number of Constituent records #####
              select @QA_result_1 = count(1)
                from VESPA_MinAttr_Phase2_tmp_Surf_Constituents

              execute logger_add_event @varBuildId, 3, 'Step 2.6 (Live): Total number of Constituent records created [expected: N/A]', @QA_result_1


              -- ##### Total number of Surf events in Constituents table #####
              select @QA_result_1 = count(distinct Surf_Id)
                from VESPA_MinAttr_Phase2_tmp_Surf_Constituents

              execute logger_add_event @varBuildId, 3, 'Step 2.6 (Live): Total number of Surfing events in Constituents table [same as in Step 2.5]', @QA_result_1


              -- ##### Total number of events considered as Front Minute #####
              select @QA_result_1 = sum(front_minute)
                from VESPA_MinAttr_Phase2_tmp_Surf_Constituents

              execute logger_add_event @varBuildId, 3, 'Step 2.6 (Live): Total number of events considered as Front Minute [expected: N/A]', @QA_result_1

        end

      set @QA_result_1 = -1
      commit
*/


        -- ##############################################################################################################
        -- ##### STEP 3.1 - applying the results to the base table                                                  #####
        -- ##############################################################################################################
      update VESPA_MinAttr_Phase2_01_Viewing_Delta base
         set base.BARB_Minute_Start = det.BARB_Minute_Start,
             base.BARB_Minute_End   = det.BARB_Minute_End
        from VESPA_MinAttr_Phase2_02_All_Live_Viewing det
       where base.Instance_Id = det.Instance_Id
      commit



        -- ##############################################################################################################
        -- ##### STEP 3.2 - applying the results to the original/input table                                        #####
        -- ##############################################################################################################
      set @varSql = '
                    update ' || @parInputTable || ' base
                       set base.BARB_Minute_Start = det.BARB_Minute_Start,
                           base.BARB_Minute_End   = det.BARB_Minute_End
                      from VESPA_MinAttr_Phase2_01_Viewing_Delta det
                     where base.Instance_Id = det.Instance_Id
                    '
      execute(@varSql)
      commit



      execute logger_add_event @varBuildId, 3, '####### Minute Attribution - process completed #######'


 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



