/*###############################################################################
# Created on:   06/08/2012
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Minute attribution calculation for viewing events according to
#               BARB minute definition, modified & tweaked so it can be used
#               within Vespa capabilities
#               This is PROCEDURE based script for Phase 1 data structures
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
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# Input table "VESPA_MinAttr_Phase1_01_Viewing_Delta" must exists
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2012  SBE   v01 - initial version
#                   First attempt to create a script for minute attribution
#                   calculation.
#                   To do:
#                     - consider intermediate table names change
#                     - apply @parBuildDate parameter to filtering criteria
#
# 20/08/2012  SBE   v02
#                   1) Algorithm adjustment for cases when total duration of two
#                      separate instances of viewing on the same channel is longer
#                      than duration on another channel in that minute, i.e.:
#                      Ch1 -> 19s, Ch2 -> 22s, Ch1 -> 19s
#                      => v01 would have attributed viewing to Ch2
#                      => v02 will attribute viewing to the first viewing instance
#                         on Ch1
#                      This has been clarified and agreed with Martin Neighbours
#                      on 20/08/2012
#                   2) Implemented dynamic table name feature so the procedure can
#                      attribute minutes to any input table meeting required criteria
#                   3) Code clean, table name changed, references to "Phase 1"
#                      explicitly added  etc.
#
# 23/08/2012  SBE   v03
#                   1) Modified step 0.2 to add missing evSurf & short viewing
#                      events originally filtered out while augmented tables were
#                      created
#                   2) Table name with Surfing events changed - suffix "PHASE1" added
#                   3) Surfing events - requirement for filling VESPA_SURF_CONSTITUENTS_PHASE1
#                      with relevant information added
#                   4) "parDailyTableDateStamp" paremeter added
#                   5) Surfing event defintion modified - it now includes events equal
#                      or shorter than "@varSurfingCutoff" OR event types = "evSurf"
#
###############################################################################*/


if object_id('Minute_Attribution_Phase1_v03') is not null then drop procedure Minute_Attribution_Phase1_v03 endif;
commit;


  -- ###############################################################################
  -- ##### Input table must include the following columns:                     #####
  -- #####  - cb_row_id                                                        #####
  -- #####  - subscriber_id                                                    #####
  -- #####  - viewing_starts                                                   #####
  -- #####  - viewing_stops      - this is EXCLUSIVE version, i.e. equal to    #####
  -- #####                         "viewing_starts" of the next event          #####
  -- #####  - viewing_duration                                                 #####
  -- #####  - live_flag          - 0/1 flag for timeshifted & live events      #####
  -- #####  - BARB_minute_start                                                #####
  -- #####  - BARB_minute_end                                                  #####
  -- ###############################################################################

create procedure Minute_Attribution_Phase1_v03
      @parInputTable            varchar(100) = null, -- See above for the list of required fields
      @parDailyTableDateStamp   varchar(8) = null,   -- "yyyymmdd" datestamp/suffix
      @parBuildDate             datetime = null,
      @parLogQADetails          bit = 0,             -- If "1" then each completed step triggers Logger event
      @parRefreshIdentifier     varchar(40) = ''     -- Logger - refresh identifier
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
      declare @QA_result                      integer             -- QA result field
      declare @varSql                         varchar(10000)      -- SQL string for dynamic SQL execution

      set @varSurfingCutoff            = 15  -- Included in the lower range, i.e. "15" means "15 or less" -> viewing
                                             --   must be 16 seconds or more to be not classified as Surfing
      set @varMinMinuteViewingCutoff   = 30  -- Included in the lower range, i.e. "30" means "30 or less" -> viewing
                                             --   must be 31 seconds or more
      set @varProcessIdentifier        = 'Vespa_MinAttr_P1_V03'


        -- ###############################################################################
        -- ##### Reset fields in the input table                                     #####
        -- ###############################################################################
      set @varSql = '
                    update ' || @parInputTable || ' base
                       set base.BARB_minute_start = null,
                           base.BARB_minute_end   = null
                    '
      execute(@varSql)
      commit


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      execute citeam.logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute citeam.logger_add_event @varBuildId, 3, '##### Minute Attribution - process started #####'

      if (@parLogQADetails = 0)
          execute citeam.logger_add_event @varBuildId, 3, '(note: quiet mode, reporting only warnings and errors)'
      else
          execute citeam.logger_add_event @varBuildId, 3, '(note: full reporting mode, reporting all details)'

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 0.2 - creating input table for further processing                                             #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Get all records from the original table                             #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase1_01_Viewing_Delta') is not null drop table VESPA_MinAttr_Phase1_01_Viewing_Delta
      set @varSql = '
                    select
                          cb_row_id,
                          subscriber_id,
                          cast(null as varchar(15)) as Event_Type,
                          viewing_starts,
                          viewing_stops,
                          viewing_duration,
                          cast(null as datetime) as recorded_time_UTC,
                          cast(null as unsigned bigint) as programme_trans_sk,
                          cast(null as int) as Channel_Identifier,    -- unique channel identifier - source: EPG
                          Live_Flag,
                          cast(null as datetime) as BARB_minute_start,
                          cast(null as datetime) as BARB_minute_end,
                          cast(''Augmented Table'' as varchar(15)) as Source
                      into VESPA_MinAttr_Phase1_01_Viewing_Delta
                      from ' || @parInputTable
      execute(@varSql)
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase1_01_Viewing_Delta(cb_row_id)
      create hg index idx2 on VESPA_MinAttr_Phase1_01_Viewing_Delta(subscriber_id)
      create dttm index idx3 on VESPA_MinAttr_Phase1_01_Viewing_Delta(viewing_starts)
      create dttm index idx4 on VESPA_MinAttr_Phase1_01_Viewing_Delta(viewing_stops)
      create lf index idx5 on VESPA_MinAttr_Phase1_01_Viewing_Delta(Live_Flag)



        -- ###############################################################################
        -- ##### Add formerly filtered out records                                   #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records') is not null drop table VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records
      set @varSql = '
                    select
                          cb_row_id,
                          subscriber_id,
                          Event_Type,

                          case
                            when Event_Type = ''evSurf'' then Adjusted_Event_Start_Time
                              else x_viewing_start_time
                          end as viewing_starts,

                          case
                            when Event_Type = ''evSurf'' then X_Adjusted_Event_End_Time
                              else x_viewing_end_time
                          end as viewing_stops,

                          case
                            when Event_Type = ''evSurf'' then datediff(second, Adjusted_Event_Start_Time, X_Adjusted_Event_End_Time)
                              else datediff(second, x_viewing_start_time, x_viewing_end_time)
                          end as viewing_duration,

                          cast(null as datetime) as recorded_time_UTC,
                          cast(null as unsigned bigint) as programme_trans_sk,
                          cast(null as int) as Channel_Identifier,    -- unique channel identifier - source: EPG
                          case
                            when (play_back_speed is null) then 1
                              else 0
                          end as Live_Flag,
                          cast(null as datetime) as BARB_minute_start,
                          cast(null as datetime) as BARB_minute_end,
                          cast(''Event Recovery'' as varchar(15)) as Source
                      into VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records
                      from sk_prod.VESPA_STB_PROG_EVENTS_' || @parDailyTableDateStamp || '
                     where (play_back_speed is null or play_back_speed = 2)
                       and x_programme_viewed_duration <= 10                                -- Only short-time events were excluded
                       and (x_programme_viewed_duration > 0 or Event_Type = ''evSurf'')     -- evSurf events have duration reset to 0
                       and Panel_id in (4,5,12)
                       and (x_type_of_viewing_event <> ''Non viewing event'' or Event_Type = ''evSurf'')
                      '
/*
and  Account_Number in (''200000847349'',''200000847620'',''200000850798'',''200000852109'',''200000853511'',''200000853990'',''200000854345'',
''200000855904'',''200000872321'',''200000881751'',''200000882767'',''200000884623'',''200000885927'',''200000890943'',
''200000895116'',''200000898987'',''200000908430'',''200000908455'',''200000913646'',''200000925350'',''200000935284'',
''200000940086'',''200000940441'',''200000940813'',''200000941225'',''200000941266'',''200000945952'',''200000946224'',
''200000946315'',''200000947222'',''200000949392'',''200000952255'',''200000956967'',''200000957445'',''200000963930'',
''200000972634'',''200000983185'',''200000983474'',''200000998282'',''200000999181'',''200000999686'',''200001001177'',
''200001010178'',''200001046321'',''200001046677'',''200001055223'',''200001059415'',''200001059423'',''200001070537'',
''200001073499'',''200001074620'',''200001090881'',''200001103940'',''200001115167'',''200001117858'',''200001123534'',
''200001129820'',''200001131875'',''200001136189'',''200001151899'',''200001153234'',''200001155262'',''200001156435'',
''200001158274'',''200001168034'',''200001173331'',''200001178017'',''200001179890'',''200001183074'',''200001187331'',
''200001191481'',''200001197884'',''200001198536'',''200001198676'',''200001200951'',''200001206339'',''200001210059'',
''200001211347'',''200001211677'',''200001214895'',''200001216114'',''200001216999'',''200001224258'',''200001231055'',
''200001234463'',''200001242409'',''200001243035'',''200001244223'',''200001245048'',''200001246228'',''200001251020'',
''200001254115'',''200001257977'',''200001258124'',''200001260518'',''200001261847'',''200001264932'',''200001267224'',
''200001269592'',''200001275284'',''621057230610'')
*/
      execute(@varSql)
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records(cb_row_id)

      delete from VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records
       where cb_row_id in (select cb_row_id
                             from VESPA_MinAttr_Phase1_01_Viewing_Delta)
      commit


      insert into VESPA_MinAttr_Phase1_01_Viewing_Delta
        select *
          from VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records
      commit


        -- ###############################################################################
        -- ##### Update missing information                                          #####
        -- ###############################################################################
      set @varSql = '
                    update VESPA_MinAttr_Phase1_01_Viewing_Delta base
                       set base.programme_trans_sk  = det.programme_trans_sk,
                           base.Channel_Identifier  = epg.service_id
                      from sk_prod.VESPA_STB_PROG_EVENTS_' || @parDailyTableDateStamp || ' det,
                           sk_prod.VESPA_EPG_DIM epg
                     where base.cb_row_id = det.cb_row_id
                       and det.programme_trans_sk = epg.programme_trans_sk
                    '
      execute(@varSql)
      commit

      set @varSql = '
                    update VESPA_MinAttr_Phase1_01_Viewing_Delta base
                       set base.recorded_time_UTC = det.recorded_time_UTC
                      from sk_prod.VESPA_STB_PROG_EVENTS_' || @parDailyTableDateStamp || ' det
                     where Live_Flag = 0
                       and base.cb_row_id = det.cb_row_id
                    '
      execute(@varSql)
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of live events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_tmp_Filtered_Out_Records

              execute citeam.logger_add_event @varBuildId, 3, 'Step 0.2: Number of missing events added [expected: N/A]', @QA_result


                -- ##### Total number of live events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_01_Viewing_Delta

              execute citeam.logger_add_event @varBuildId, 3, 'Step 0.2: Total number of events [expected: N/A]', @QA_result

        end

      set @QA_result = -1
      commit



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
      update VESPA_MinAttr_Phase1_01_Viewing_Delta base
         set base.BARB_minute_start = case
                                          -- Events lasting 30s or less
                                        when (datediff(second, recorded_time_UTC, dateadd(second, viewing_duration - 1, recorded_time_UTC)) + 1 <= 30) then null

                                          -- Events starting and ending in two different/adjacent minutes but lasting 30s or less in both minutes
                                        when (datediff(second, recorded_time_UTC, dateadd(second, viewing_duration - 1, recorded_time_UTC)) <= 60) and
                                             (second(recorded_time_UTC) >= 30) and (second(dateadd(second, viewing_duration - 1, recorded_time_UTC)) <= 29) then null

                                          -- Qualifying events - rounding down
                                        when second(recorded_time_UTC) <= 29 then dateadd(second, -second(recorded_time_UTC), recorded_time_UTC)

                                          -- Qualifying events - rounding up
                                        when second(recorded_time_UTC) >= 30 then dateadd(second, 60 - second(recorded_time_UTC), recorded_time_UTC)

                                          else null

                                      end,

             base.BARB_minute_end   = case
                                          -- Events lasting 29s or less
                                        when (datediff(second, recorded_time_UTC, dateadd(second, viewing_duration - 1, recorded_time_UTC)) + 1 <= 30) then null

                                          -- Events starting and ending in two different/adjacent minutes but lasting 29s or less in both minutes
                                        when (datediff(second, recorded_time_UTC, dateadd(second, viewing_duration - 1, recorded_time_UTC)) <= 60) and
                                             (second(recorded_time_UTC) >= 30) and (second(dateadd(second, viewing_duration - 1, recorded_time_UTC)) <= 29) then null

                                          -- Qualifying events - rounding down
                                        when second(dateadd(second, viewing_duration - 1, recorded_time_UTC)) <= 29
                                              then dateadd(second, -(60 + second(dateadd(second, viewing_duration - 1, recorded_time_UTC))), dateadd(second, viewing_duration - 1, recorded_time_UTC))

                                          -- Qualifying events - rounding up
                                        when second(dateadd(second, viewing_duration - 1, recorded_time_UTC)) >= 30
                                              then dateadd(second, 60 - (60 + second(dateadd(second, viewing_duration - 1, recorded_time_UTC))), dateadd(second, viewing_duration - 1, recorded_time_UTC))

                                          else null
                                      end
       where Live_Flag = 0
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of playback events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_01_Viewing_Delta
               where Live_Flag = 0

              execute citeam.logger_add_event @varBuildId, 3, 'Step 1.1 (Playback): Total number of events [expected: N/A]', @QA_result


                -- ##### Total number of playback events with minute attributed #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_01_Viewing_Delta
               where Live_Flag = 0
                 and BARB_minute_start is not null

              execute citeam.logger_add_event @varBuildId, 3, 'Step 1.1 (Playback): Number of attributed events [expected: < # of Playback Events]', @QA_result

        end

      set @QA_result = -1
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
      if object_id('VESPA_MinAttr_Phase1_02_All_Live_Viewing') is not null drop table VESPA_MinAttr_Phase1_02_All_Live_Viewing
      select
            cb_row_id,
            subscriber_id,
            Event_Type,
            Channel_Identifier,
            viewing_starts,
            dateadd(second, -1, viewing_stops) as viewing_stops,    -- Move to inclusive seconds/minutes
            viewing_duration as Event_Duration,
            cast(null as datetime) as Viewing_Starts_Min,
            cast(null as datetime) as Viewing_Stops_Min,
            case
              when (viewing_duration <= @varSurfingCutoff) or (Event_Type = 'evSurf') then 1
                else 0
            end as Event_Surfing_Flag

        into VESPA_MinAttr_Phase1_02_All_Live_Viewing
        from VESPA_MinAttr_Phase1_01_Viewing_Delta
       where Live_Flag = 1
      commit

      update VESPA_MinAttr_Phase1_02_All_Live_Viewing
        set Viewing_Starts_Min  = dateadd(second, -second(viewing_starts), viewing_starts),
            Viewing_Stops_Min   = dateadd(second, -second(viewing_stops), viewing_stops)
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase1_02_All_Live_Viewing(cb_row_id)
      create hg index idx2 on VESPA_MinAttr_Phase1_02_All_Live_Viewing(Subscriber_Id)
      create dttm index idx3 on VESPA_MinAttr_Phase1_02_All_Live_Viewing(viewing_starts)
      create dttm index idx4 on VESPA_MinAttr_Phase1_02_All_Live_Viewing(viewing_stops)


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of live events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_02_All_Live_Viewing

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.1 (Live): Total number of events [expected: N/A]', @QA_result

        end

      set @QA_result = -1
      commit


        -- ##############################################################################################################
        -- ##### STEP 2.2 - splitting viewing records so each minute as in "viewing start" & "viewing end" fields   #####
        -- #####            is represented by a single record                                                       #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Get records starting & ending in the same minute                    #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min') is not null drop table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
      select
            cb_row_id,
            subscriber_id,
            Event_Type,
            Channel_Identifier,
            1 as Record_Type,
            viewing_starts,
            viewing_stops,
            viewing_starts as Min_Viewing_Starts,
            viewing_stops as Min_Viewing_Ends,
            dateadd(second, -second(viewing_starts), viewing_starts) as Minute,
            Event_Surfing_Flag,
            Event_Duration,
            Event_Duration as Duration_In_Minute
        into VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        from VESPA_MinAttr_Phase1_02_All_Live_Viewing
       where datediff(second, viewing_starts, viewing_stops) <= 61
         and minute(viewing_starts) = minute(viewing_stops)
      commit


        -- ###############################################################################
        -- ##### Get start minute part events                                        #####
        -- ###############################################################################
      insert into VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
      select
            cb_row_id,
            subscriber_id,
            Event_Type,
            Channel_Identifier,
            2 as Record_Type,
            viewing_starts,
            viewing_stops,
            viewing_starts as Min_Viewing_Starts,
            dateadd(second, (60 - second(viewing_starts)) - 1, viewing_starts) as Min_Viewing_Ends,
            dateadd(second, -second(viewing_starts), viewing_starts) as Minute,
            Event_Surfing_Flag,
            Event_Duration,
            60 - second(viewing_starts) as Duration_In_Minute
        from VESPA_MinAttr_Phase1_02_All_Live_Viewing
       where datediff(second, viewing_starts, viewing_stops) > 61
          or minute(viewing_starts) <> minute(viewing_stops)
      commit


        -- ###############################################################################
        -- ##### Get end minute part events                                          #####
        -- ###############################################################################
      insert into VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
      select
            cb_row_id,
            subscriber_id,
            Event_Type,
            Channel_Identifier,
            3 as Record_Type,
            viewing_starts,
            viewing_stops,
            dateadd(second, -second(viewing_stops), viewing_stops) as Min_Viewing_Starts,
            viewing_stops as Min_Viewing_Ends,
            dateadd(second, -second(viewing_stops), viewing_stops) as Minute,
            Event_Surfing_Flag,
            Event_Duration,
            second(viewing_stops) + 1 as Duration_In_Minute
        from VESPA_MinAttr_Phase1_02_All_Live_Viewing
       where datediff(second, viewing_starts, viewing_stops) > 61
          or minute(viewing_starts) <> minute(viewing_stops)
      commit

      alter table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        add RowId unsigned bigint identity not null

      create hg index idx1 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(cb_row_id)
      create hg index idx2 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(Subscriber_Id)
      create dttm index idx3 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(Min_Viewing_Starts)
      create dttm index idx4 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(Min_Viewing_Ends)
      create dttm index idx5 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(Minute)
      create unique hg index idx6 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(RowId)
      create unique index idx7 on VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min(cb_row_id, Minute)


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of split events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Total number of split events [expected: > Total # of events]', @QA_result


                -- ##### Total number of "same minute" events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               where Record_Type = 1

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Number of Same Minute events [expected: N/A]', @QA_result


                -- ##### Total number of "start minute" events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               where Record_Type = 2

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Number of Start Minute events [expected: N/A]', @QA_result


                -- ##### Total number of "end minute" events #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               where Record_Type = 3

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.2 (Live): Number of End Minute events [expected: =Start Minute events]', @QA_result

        end

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.3 - calculating required totals & metrics for each minute and finally applying minute       #####
        -- #####            attribution                                                                             #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Rank events in each minute (create a sequence)                      #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase1_tmp_Minute_Ranks') is not null drop table VESPA_MinAttr_Phase1_tmp_Minute_Ranks
      select
            RowId,
            rank() over (partition by subscriber_id, Minute order by Min_Viewing_Starts, Min_Viewing_Ends, RowId) as Minute_Seq
        into VESPA_MinAttr_Phase1_tmp_Minute_Ranks
        from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase1_tmp_Minute_Ranks(RowId)


      alter table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        add (Minute_Seq tinyint default 0)

      update VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min base
         set base.Minute_Seq = det.Minute_Seq
        from VESPA_MinAttr_Phase1_tmp_Minute_Ranks det
       where base.RowId = det.RowId
      commit


        -- ###############################################################################
        -- ##### Flag first longest event in each minute                             #####
        -- ###############################################################################
      alter table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        add (Total_Channel_Duration tinyint default 0,
             First_Longest_Flag bit default 0)

        -- #### Get total channel viewing duration in minute
      if object_id('VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration') is not null drop table VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration
      select
            subscriber_id,
            Minute,
            Channel_Identifier,
            sum(Duration_In_Minute) as Total_Channel_Duration
        into VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration
        from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
       where Event_Surfing_Flag = 0
       group by subscriber_id, Minute, Channel_Identifier
      commit

      create hg index idx1 on VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration(Subscriber_Id)
      create dttm index idx2 on VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration(Minute)
      create hg index idx3 on VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration(Channel_Identifier)

      update VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min base
         set base.Total_Channel_Duration = det.Total_Channel_Duration
        from VESPA_MinAttr_Phase1_tmp_Total_Channel_Duration det
       where base.subscriber_id = det.subscriber_id
         and base.Minute = det.Minute
         and base.Channel_Identifier = det.Channel_Identifier
         and Event_Surfing_Flag = 0
      commit

        -- ##### Get first longet event in each minute #####
      if object_id('VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks') is not null drop table VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks
      select
            RowId,
                subscriber_id,
                Minute,
                Minute_Seq,
                Duration_In_Minute,
                total_Channel_duration,
            rank() over (partition by subscriber_id, Minute order by Total_Channel_Duration desc, Duration_In_Minute desc, Minute_Seq, RowId) as First_Longest_Rank
        into VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks
        from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
       where Event_Surfing_Flag = 0
      commit

      create hg index idx1 on VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks(RowId)

      update VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min base
         set base.First_Longest_Flag  = 1
        from VESPA_MinAttr_Phase1_tmp_First_Longest_Ranks det
       where base.RowId = det.RowId
         and det.First_Longest_Rank = 1
      commit


        -- ##### QA #####
        -- ##### Number of instances without First Longest & without surfing events #####
      select @QA_result = count(1)
        from (select subscriber_id, Minute, max(First_Longest_Flag) as Max_First_Longest_Flag, sum(Total_Surfing) as Sum_Total_Surfing
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               group by subscriber_id, Minute) det
       where Max_First_Longest_Flag = 0
         and Sum_Total_Surfing = 0

      if (@QA_result > 0)
          execute citeam.logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Instance minutes without First Longest & surfing event [expected: 0]', @QA_result
      else
        if (@parLogQADetails = 1)
            execute citeam.logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Instance minutes without First Longest & surfing event [expected: 0]', @QA_result

        -- ##### Number of minutes with more than one First Longest #####
      select @QA_result = count(1)
        from (select subscriber_id, Minute
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               where First_Longest_Flag = 1
               group by subscriber_id, Minute
              having count(*) > 1) det

      if (@QA_result > 0)
          execute citeam.logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Minutes with more than one First Longest event [expected: 0]', @QA_result
      else
        if (@parLogQADetails = 1)
            execute citeam.logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Minutes with more than one First Longest event [expected: 0]', @QA_result

      set @QA_result = -1
      commit


        -- ###############################################################################
        -- ##### Calculate total surfing time in each minute                         #####
        -- ###############################################################################
      alter table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        add (Total_Surfing tinyint default 0)

      update VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min base
         set base.Total_Surfing = det.Total_Surfing
        from (select
                    subscriber_id,
                    Minute,
                    sum(Duration_In_Minute) as Total_Surfing
               from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
              where Event_Surfing_Flag = 1
              group by subscriber_id, Minute) det
       where base.subscriber_id = det.subscriber_id
         and base.Minute = det.Minute
      commit


        -- ###############################################################################
        -- ##### Calculate total duration in each minute                             #####
        -- ###############################################################################
      alter table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        add (Total_Duration tinyint default 0)

      update VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min base
         set base.Total_Duration = det.Total_Duration
        from (select
                    subscriber_id,
                    Minute,
                    sum(Duration_In_Minute) as Total_Duration
               from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
              group by subscriber_id, Minute) det
       where base.subscriber_id = det.subscriber_id
         and base.Minute = det.Minute
      commit


        -- ##### QA #####
        -- ##### Number of minutes with total > 60 #####
      select @QA_result = count(1)
        from (select subscriber_id, Minute
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               group by subscriber_id, Minute
              having max(Total_Duration) > 60) det

      if (@QA_result > 0)
          execute citeam.logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Minutes with Total Duration > 60 [expected: 0]', @QA_result
      else
        if (@parLogQADetails = 1)
            execute citeam.logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Minutes with Total Duration > 60 [expected: 0]', @QA_result

      set @QA_result = -1
      commit


        -- ###############################################################################
        -- ##### Final step - based on rules, apply minute attribution               #####
        -- ###############################################################################
      alter table VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
        add (Minute_Attribution_Result tinyint default 0)

      update VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
         set Minute_Attribution_Result  = case

                                              -- Total viewing equal or less than the cutoff point (currently 30 seconds - viewing must be 31 seconds or more)
                                            when (Total_Duration <= @varMinMinuteViewingCutoff) then 1

                                              -- Flag all events except the first longest one
                                            when (First_Longest_Flag = 0) then 2

                                              -- Flag when first longest but less than total surfing (minute will be considered as "Surfing")
                                            when (First_Longest_Flag = 1) and (Total_Channel_Duration < Total_Surfing) then 3

                                              -- Flag when first longest and more than total surfing -> the winning one
                                            when (First_Longest_Flag = 1) and (Total_Channel_Duration >= Total_Surfing) then 10

                                              else 0
                                          end
      commit


        -- ##### QA #####
        -- ##### Number of records with Minute_Attribution_Result=0 #####
      select @QA_result = count(1)
        from (select subscriber_id, Minute
                from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
               group by subscriber_id, Minute
              having min(Minute_Attribution_Result) = 0) det

      if (@QA_result > 0)
          execute citeam.logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Events with Minute_Attribution_Result=0 [expected: 0]', @QA_result
      else
        if (@parLogQADetails = 1)
            execute citeam.logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Events with Minute_Attribution_Result=0 [expected: 0]', @QA_result

      set @QA_result = -1
      commit


        -- ###############################################################################
        -- ##### Apply results back to the original table for non-split records      #####
        -- ###############################################################################
      alter table VESPA_MinAttr_Phase1_02_All_Live_Viewing
        add (StartEndMinute_Relation tinyint default 0,
             StartMinute_Result tinyint default 0,
             EndMinute_Result tinyint default 0,
             BARB_Minute_Start datetime default null,
             BARB_Minute_End datetime default null)

      update VESPA_MinAttr_Phase1_02_All_Live_Viewing base
         set StartEndMinute_Relation  = case
                                            -- Start & End minutes are the same
                                          when (Viewing_Starts_Min = Viewing_Stops_Min) then 2
                                            -- End minute is right after the start one (spans over 2 minutes)
                                          when (minute(dateadd(minute, 1, viewing_starts)) = minute(viewing_stops)) then 1
                                            -- Event spans over at least 3 minutes
                                            else 0
                                        end
      commit

      update VESPA_MinAttr_Phase1_02_All_Live_Viewing base
         set base.StartMinute_Result = det.Minute_Attribution_Result
        from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min det
       where base.cb_row_id = det.cb_row_id
         and base.Viewing_Starts_Min = det.Minute
      commit

      update VESPA_MinAttr_Phase1_02_All_Live_Viewing base
         set base.EndMinute_Result = det.Minute_Attribution_Result
        from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min det
       where base.cb_row_id = det.cb_row_id
         and base.Viewing_Stops_Min = det.Minute
      commit


        -- ##### QA #####
        -- ##### Number of events with missing calculation result #####
      select @QA_result = count(1)
        from VESPA_MinAttr_Phase1_02_All_Live_Viewing
       where StartMinute_Result is null
          or StartMinute_Result = 0
          or EndMinute_Result is null
          or EndMinute_Result = 0

      if (@QA_result > 0)
          execute citeam.logger_add_event @varBuildId, 2, 'Step 2.3 (Live): Events missing calculation result [expected: 0]', @QA_result
      else
        if (@parLogQADetails = 1)
            execute citeam.logger_add_event @varBuildId, 3, 'Step 2.3 (Live): Events missing calculation result [expected: 0]', @QA_result

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.4 - calculating BARB minute                                                                 #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Calculate BARB minute                                               #####
        -- ###############################################################################
      update VESPA_MinAttr_Phase1_02_All_Live_Viewing base
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
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_02_All_Live_Viewing

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.4 (Live): Total number of events [expected: N/A]', @QA_result


                -- ##### Total number of live events with minute attributed #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_02_All_Live_Viewing
               where BARB_minute_start is not null

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.4 (Live): Number of attributed events [expected: < # of Events]', @QA_result

        end

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.5 - creating Surfing events                                                                 #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Add one-minute events                                               #####
        -- ###############################################################################
--truncate table VESPA_SURF_MINUTES_PHASE1

      insert into VESPA_SURF_MINUTES_PHASE1 (subscriber_id, surf_minute_start, surf_minute_end, build_date)
        select
              subscriber_id,
              Minute,
              Minute,
              min(@parBuildDate)
          from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
         where Minute_Attribution_Result in (2, 3, 10)
         group by subscriber_id, Minute, Minute
        having max(Total_Surfing) > max(Total_Channel_Duration)
      commit


        -- ###############################################################################
        -- ##### Add complete minutes for evSurv events spanning over at least 3     #####
        -- ##### minutes                                                             #####
        -- ###############################################################################
      insert into VESPA_SURF_MINUTES_PHASE1 (subscriber_id, surf_minute_start, surf_minute_end, build_date)
        select distinct
              subscriber_id,
              dateadd(second, (60 - second(viewing_starts)), viewing_starts),
              dateadd(second, -second(viewing_stops) - 60, viewing_stops),
              @parBuildDate
          from VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min
         where Event_Type = 'evSurf'
           and datediff(minute, viewing_starts, viewing_stops) >= 2      -- only events spanning over at least THREE different minutes
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Total number of surfing events #####
              select @QA_result = count(1)
                from VESPA_SURF_MINUTES_PHASE1
               where build_date = @parBuildDate

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.5 (Live): Total number of Surfing events [expected: N/A]', @QA_result

        end

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 2.6 - creating constituents for Surfing events                                                #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Temporary table to calculate get the front minute                   #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase1_tmp_Surf_Constituents') is not null drop table VESPA_MinAttr_Phase1_tmp_Surf_Constituents
      create table VESPA_MinAttr_Phase1_tmp_Surf_Constituents (
          Row_Id                      bigint      identity primary key,
          Subscriber_Id               bigint      not null,
          Surf_Id                     bigint      not null,
          Cb_row_id                   bigint      not null,
          Minute_Seq                  tinyint     not null default 1,
          Surf_Minute_Start           datetime    null     default null,
          Surf_Minute_End             datetime    null     default null,
          Viewing_Starts              datetime    null     default null,
          Viewing_Stops               datetime    null     default null,
          Front_Minute                bit         null     default 0,
          Source                      varchar(10) null     default null,
      )

      create hg index idx1 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents(surf_id)
      create hg index idx2 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents(cb_row_id)
      create unique hg index idx3 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents(surf_id, cb_row_id)
      create dttm index idx4 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents(viewing_starts)
      create dttm index idx5 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents(viewing_stops)

      insert into VESPA_MinAttr_Phase1_tmp_Surf_Constituents
                    (subscriber_id, surf_id, cb_row_id, Minute_Seq, surf_minute_start, surf_minute_end, viewing_starts, viewing_stops, Source)
        select
              base.subscriber_id,
              base.surf_id,
              det.cb_row_id,
              det.Minute_Seq,
              base.surf_minute_start,
              base.surf_minute_end,
              det.viewing_starts,
              det.viewing_stops,
              'Minute'
          from VESPA_SURF_MINUTES_PHASE1 base,
               VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min det
         where base.subscriber_id = det.subscriber_id
           and base.surf_minute_start = det.Minute
           and Event_Surfing_Flag = 1
        commit


      insert into VESPA_MinAttr_Phase1_tmp_Surf_Constituents
                    (subscriber_id, surf_id, cb_row_id, surf_minute_start, surf_minute_end, viewing_starts, viewing_stops, Source)
        select distinct
              base.subscriber_id,
              base.surf_id,
              det.cb_row_id,
              base.surf_minute_start,
              base.surf_minute_end,
              det.viewing_starts,
              det.viewing_stops,
              'evSurf'
          from VESPA_SURF_MINUTES_PHASE1 base,
               VESPA_MinAttr_Phase1_03_All_Live_Viewing_By_Min det
         where Event_Type = 'evSurf'
           and datediff(minute, viewing_starts, viewing_stops) >= 2      -- only events spanning over at least THREE different minutes
           and base.surf_minute_start = dateadd(second, (60 - second(det.viewing_starts)), det.viewing_starts)
      commit


        -- ###############################################################################
        -- ##### Get & apply ranks                                                   #####
        -- ###############################################################################
      if object_id('VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min') is not null drop table VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min
      select
            Row_Id,
            rank() over (partition by subscriber_id, cb_row_id order by surf_minute_start, surf_minute_end, Row_Id) as Surf_Seq
        into VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min
        from VESPA_MinAttr_Phase1_tmp_Surf_Constituents
      commit

      create unique hg index idx1 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min(Row_Id)
      create lf index idx2 on VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min(Surf_Seq)

        -- ##### Get first instances of each record #####
      update VESPA_MinAttr_Phase1_tmp_Surf_Constituents base
         set base.front_minute = 1
        from VESPA_MinAttr_Phase1_tmp_Surf_Constituents_Front_Min det
       where base.Row_Id = det.Row_Id
         and det.Surf_Seq = 1
      commit

        -- ##### Reset back to 0 for cases when only a single cb_row_id exists #####
      update VESPA_MinAttr_Phase1_tmp_Surf_Constituents base
         set base.front_minute = 0
        from (select
                    cb_row_id,
                    count(*) as Cnt
                from VESPA_MinAttr_Phase1_tmp_Surf_Constituents
               group by cb_row_id
              having count(*) = 1) det
       where base.cb_row_id = det.cb_row_id
      commit


        -- ###############################################################################
        -- ##### Fill the destination table                                          #####
        -- ###############################################################################
--truncate table VESPA_SURF_CONSTITUENTS_PHASE1
      insert into VESPA_SURF_CONSTITUENTS_PHASE1 (Surf_Id, Cb_row_Id, Minute_Seq, Front_Minute, Build_Date)
        select
              Surf_Id,
              Cb_row_Id,
              Minute_Seq,
              Front_Minute,
              @parBuildDate
          from VESPA_MinAttr_Phase1_tmp_Surf_Constituents
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

              -- ##### Total number of Constituent records #####
              select @QA_result = count(1)
                from VESPA_MinAttr_Phase1_tmp_Surf_Constituents

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.6 (Live): Total number of Constituent records [expected: N/A]', @QA_result


              -- ##### Total number of Surf events in Constituents table #####
              select @QA_result = count(distinct surf_id)
                from VESPA_MinAttr_Phase1_tmp_Surf_Constituents

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.6 (Live): Total number of Surfing events in Constituents table [same as in Step 2.5]', @QA_result


              -- ##### Total number of events considered as Front Minute #####
              select @QA_result = sum(front_minute)
                from VESPA_MinAttr_Phase1_tmp_Surf_Constituents

              execute citeam.logger_add_event @varBuildId, 3, 'Step 2.6 (Live): Total number of events considered as Front Minute [expected: N/A]', @QA_result

        end

      set @QA_result = -1
      commit



        -- ##############################################################################################################
        -- ##### STEP 3.1 - applying the results to the base table                                                  #####
        -- ##############################################################################################################
      update VESPA_MinAttr_Phase1_01_Viewing_Delta base
         set base.BARB_minute_start = det.BARB_Minute_Start,
             base.BARB_minute_end   = det.BARB_Minute_End
        from VESPA_MinAttr_Phase1_02_All_Live_Viewing det
       where base.cb_row_id = det.cb_row_id
      commit



        -- ##############################################################################################################
        -- ##### STEP 3.2 - applying the results to the original/input table                                        #####
        -- ##############################################################################################################
      set @varSql = '
                    update ' || @parInputTable || ' base
                       set base.BARB_minute_start = det.BARB_Minute_Start,
                           base.BARB_minute_end   = det.BARB_Minute_End
                      from VESPA_MinAttr_Phase1_01_Viewing_Delta det
                     where base.cb_row_id = det.cb_row_id
                    '
      execute(@varSql)
      commit



      execute citeam.logger_add_event @varBuildId, 3, '##### Minute Attribution - process completed #####'


 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



