/*###############################################################################
# Created on:   26/04/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Aug Enhancement process - main process procedure
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating base tables
#               STEP 2.0 - appending source data details
#               STEP 3.0 - applying the results to the original/input table
#
# To do:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# - input table "vespa_analysts.Vespa_Daily_Augs_YYYYMMDD" must exists
# - "VESPA_AugEnh_tmp_Source_Snapshot_Full" & "VESPA_AugEnh_tmp_Source_Snapshot_Aggr"
#   tables are required
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/04/2013  SBE   v01 - initial version
#
#
###############################################################################*/


if object_id('aug_AugEnhancement_S1_v01') is not null then drop procedure aug_AugEnhancement_S1_v01 endif;
commit;


create procedure aug_AugEnhancement_S1_v01
      @parAugDate               varchar(8) = null,
      @parLogQADetails          bit = 1,             -- If "1" then each completed step triggers Logger event
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
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @QA_result_1                    integer             -- QA result field
      declare @QA_result_2                    integer             -- QA result field
      declare @varSql                         varchar(10000)      -- SQL string for dynamic SQL execution

      set @varProcessIdentifier        = 'AugEnh_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


      set @QA_result_1 = -1
      set @QA_result_2 = -1
      commit


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### AUG Enhancement (stage 1) - process started #######'

      if (@parLogQADetails = 0)
          execute logger_add_event @varBuildId, 3, '(note: quiet mode, reporting only warnings and errors)'
      else
          execute logger_add_event @varBuildId, 3, '(note: full reporting mode, reporting all details)'

      set @QA_result_1 = -1
      commit


      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
            execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
            execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
            execute logger_add_event @varBuildId, 3, 'AUG table: Vespa_Daily_Augs_' || @parAugDate, null
        end



        -- ##############################################################################################################
        -- ##### STEP 1.0 - creating base tables                                                                   #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Get all records from the AUG table                                 #####
        -- ###############################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Creating input tables <<<<<', null


      if object_id('VESPA_AugEnh_tmp_Aug_Data') is not null drop table VESPA_AugEnh_tmp_Aug_Data
      set @varSql = '
                    select
                          Cb_Row_Id                             as Instance_Id,
                          cast(null as bigint)                  as Event_Id,
                          Subscriber_Id                         as Subscriber_Id,
                          cast(null as datetime)                as Broadcast_Viewing_Starts,
                          cast(null as datetime)                as Broadcast_Viewing_Stops,
                          cast(null as datetime)                as Event_Start_Time,
                          cast(null as datetime)                as Uncapped_Event_End_Time,
                          Viewing_Starts                        as Instance_Start_Time,
                          Viewing_Stops                         as Instance_End_Time,
                          cast(0 as int)                        as Time_Since_Recording,
                          case
                            when Timeshifting = ''LIVE'' then 1
                              else 0
                          end                                   as Live_Flag,
                          cast(9 as tinyint)                    as Match_Quality
                      into VESPA_AugEnh_tmp_Aug_Data
                      from Vespa_Daily_Augs_' || @parAugDate || '
                    '
      execute(@varSql)
      commit

      create unique hg index idx0 on VESPA_AugEnh_tmp_Aug_Data(Instance_Id)
      create        hg index idx1 on VESPA_AugEnh_tmp_Aug_Data(Event_Id)
      create        hg index idx2 on VESPA_AugEnh_tmp_Aug_Data(Subscriber_Id)
      create      dttm index idx3 on VESPA_AugEnh_tmp_Aug_Data(Instance_Start_Time)
      create        lf index idx4 on VESPA_AugEnh_tmp_Aug_Data(Match_Quality)


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of AUG records #####
              select @QA_result_1 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
              execute logger_add_event @varBuildId, 3, 'AUG table snapshot created', @QA_result_1

        end

      commit



        -- ##############################################################################################################
        -- ##### STEP 2.0 - appending source data details                                                           #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Using PKs                                                           #####
        -- ###############################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: appending source data details <<<<<', null


      update VESPA_AugEnh_tmp_Aug_Data base
         set base.Broadcast_Viewing_Starts  = case
                                                when base.Live_Flag = 1 then base.Instance_Start_Time
                                                  else dateadd(second, -1 * det.Time_In_Seconds_Since_Recording, base.Instance_Start_Time)
                                              end,
             base.Broadcast_Viewing_Stops   = case
                                                when base.Live_Flag = 1 then base.Instance_End_Time
                                                  else dateadd(second, -1 * det.Time_In_Seconds_Since_Recording, base.Instance_End_Time)
                                              end,
             base.Event_Start_Time          = det.Event_Start_Date_Time_Utc,
             base.Uncapped_Event_End_Time   = det.Event_End_Date_Time_Utc,
             base.Time_Since_Recording      = det.Time_In_Seconds_Since_Recording,
             base.Match_Quality             = 1
        from VESPA_AugEnh_tmp_Source_Snapshot_Full det
       where base.Instance_Id = det.Pk_Viewing_Prog_Instance_Fact
      commit


        -- ###############################################################################
        -- ##### Using Subscriber Id & Instance Start Time                           #####
        -- ###############################################################################
      update VESPA_AugEnh_tmp_Aug_Data base
         set base.Broadcast_Viewing_Starts  = case
                                                when base.Live_Flag = 1 then base.Instance_Start_Time
                                                  else dateadd(second, -1 * det.Time_Since_Recording, base.Instance_Start_Time)
                                              end,
             base.Broadcast_Viewing_Stops   = case
                                                when base.Live_Flag = 1 then base.Instance_End_Time
                                                  else dateadd(second, -1 * det.Time_Since_Recording, base.Instance_End_Time)
                                              end,
             base.Event_Start_Time          = det.Event_Start_Time,
             base.Uncapped_Event_End_Time   = det.Event_End_Time,
             base.Time_Since_Recording      = det.Time_Since_Recording,
             base.Match_Quality             = case
                                                when (det.Event_Start_Times_Cnt = 1) and (det.Event_End_Times_Cnt = 1) and                  -- Clean cases
                                                     (det.Instance_End_Times_Cnt = 1)                                            then 2

                                                when (det.Event_Start_Times_Cnt = 1) and (det.Event_End_Times_Cnt = 1)           then 3     -- Event data OK

                                                when (det.Event_Start_Times_Cnt = 1)                                             then 4     -- Event start time OK

                                                  else 5                                                                                    -- All other questionable scenarios
                                              end
        from VESPA_AugEnh_tmp_Source_Snapshot_Aggr det
       where base.Subscriber_Id = det.Subscriber_Id
         and base.Instance_Start_Time = det.Instance_Start_Time
         and base.Match_Quality = 9
      commit



        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total records matched on PKs #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality = 1
              execute logger_add_event @varBuildId, 3, 'Records matched using PKs: ' || cast(100.0 * @QA_result_2 / @QA_result_1 as decimal(10, 1)) || '%', @QA_result_2


                -- ##### Total records matched on SubId/Date - clean cases #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality = 2
              execute logger_add_event @varBuildId, 3, 'Sub ID & Instance Time matching (clean cases): ' || cast(100.0 * @QA_result_2 / @QA_result_1 as decimal(10, 1)) || '%', @QA_result_2


                -- ##### Total records matched on SubId/Date - event data OK #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality = 3
              execute logger_add_event @varBuildId, 3, 'Sub ID & Instance Time matching (event data OK): ' || cast(100.0 * @QA_result_2 / @QA_result_1 as decimal(10, 1)) || '%', @QA_result_2


                -- ##### Total records matched on SubId/Date - event start time OK #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality = 4
              execute logger_add_event @varBuildId, 3, 'Sub ID & Instance Time matching (event start time OK): ' || cast(100.0 * @QA_result_2 / @QA_result_1 as decimal(10, 1)) || '%', @QA_result_2


                -- ##### Total records matched on SubId/Date - questionable scenarios #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality = 5
              execute logger_add_event @varBuildId, 3, 'Sub ID & Instance Time matching (questionable scenarios): ' || cast(100.0 * @QA_result_2 / @QA_result_1 as decimal(10, 1)) || '%', @QA_result_2


                -- ##### Total unmatched records #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality = 9

              execute logger_add_event @varBuildId, 3, 'Unmatched records: ' || cast(100.0 * @QA_result_2 / @QA_result_1 as decimal(10, 1)) || '%', @QA_result_2

        end

      set @QA_result_2 = -1
      commit


        -- ###############################################################################
        -- ##### Creating Event Id                                                   #####
        -- ###############################################################################
      update VESPA_AugEnh_tmp_Aug_Data base
         set base.Event_Id  = det.Event_Id
        from (select
                    Subscriber_Id,
                    Event_Start_Time,
                    max(Instance_Id) as Event_Id
                from VESPA_AugEnh_tmp_Aug_Data
               where Match_Quality < 5
               group by Subscriber_Id, Event_Start_Time) det
       where base.Subscriber_Id = det.Subscriber_Id
         and base.Event_Start_Time = det.Event_Start_Time
      commit

      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'Event Ids created', null
        end



        -- ##############################################################################################################
        -- ##### STEP 3.0 - applying the results to the original/input table                                        #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: applying results to the original table <<<<<', null


        -- ###############################################################################
        -- ##### Adding new columns                                                  #####
        -- ###############################################################################
      set @varSql = '
                      alter table Vespa_Daily_Augs_' || @parAugDate || '
                        add (Event_Id                     bigint      default null,
                             Broadcast_Viewing_Starts     datetime    default null,
                             Broadcast_Viewing_Stops      datetime    default null,
                             Event_Start_Time             datetime    default null,
                             Uncapped_Event_End_Time      datetime    default null,
                             Time_Since_Recording         int         default 0,
                             Live_Flag                    bit         default 0,
                             Match_Quality                tinyint     default 9)

                      create hg   index  idx6 on Vespa_Daily_Augs_' || @parAugDate || ' (Event_Id)
                      create dttm index  idx7 on Vespa_Daily_Augs_' || @parAugDate || ' (Broadcast_Viewing_Starts)
                      create dttm index  idx8 on Vespa_Daily_Augs_' || @parAugDate || ' (Broadcast_Viewing_Stops)
                      create dttm index  idx9 on Vespa_Daily_Augs_' || @parAugDate || ' (Event_Start_Time)
                      create dttm index idx10 on Vespa_Daily_Augs_' || @parAugDate || ' (Uncapped_Event_End_Time)
                      create lf   index idx11 on Vespa_Daily_Augs_' || @parAugDate || ' (Match_Quality)

                      commit
                    '
      execute(@varSql)
      commit

      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'New columns added', null
        end


        -- ###############################################################################
        -- ##### Updating values                                                     #####
        -- ###############################################################################
      set @varSql = '
                      update Vespa_Daily_Augs_' || @parAugDate || ' base
                         set base.Event_Id                    = det.Event_Id,
                             base.Broadcast_Viewing_Starts    = det.Broadcast_Viewing_Starts,
                             base.Broadcast_Viewing_Stops     = det.Broadcast_Viewing_Stops,
                             base.Event_Start_Time            = det.Event_Start_Time,
                             base.Uncapped_Event_End_Time     = det.Uncapped_Event_End_Time,
                             base.Time_Since_Recording        = det.Time_Since_Recording,
                             base.Live_Flag                   = det.Live_Flag,
                             base.Match_Quality               = det.Match_Quality
                        from VESPA_AugEnh_tmp_Aug_Data det
                       where base.Cb_Row_Id = det.Instance_Id

                      commit
                    '
      execute(@varSql)
      commit

      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'Values updated', null
        end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### AUG Enhancement (stage 1) - process completed #######'
      execute logger_add_event @varBuildId, 3, ' '


 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



