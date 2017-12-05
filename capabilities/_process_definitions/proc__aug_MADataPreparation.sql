/*###############################################################################
# Created on:   24/04/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  MA Data Preparation - this module creates an table for MA processing
#               from a given vespa_analysts AUG table
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 0.2 - requirement checks
#               STEP 0.9 - creating source data view (external procedure)
#               STEP 1.0 - creating output table
#               STEP 2.0 - appending fields from the source data
#
# To do:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# - input table "vespa_analysts.Vespa_Daily_Augs_YYYYMMDD" must exists
# - it must include additional fields appended in "Stage1" process
# - "vespa_getSourceDataView_v01" must exist
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 24/04/2013  SBE   v01 - initial version
#
#
###############################################################################*/


if object_id('aug_MADataPreparation_v01') is not null then drop procedure aug_MADataPreparation_v01 endif;
commit;


create procedure aug_MADataPreparation_v01
      @parProcessDate           date = null,

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
      declare @varAugDate                     varchar(8)          -- AUG table suffix
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @QA_result_1                    integer             -- QA result field
      declare @QA_result_2                    integer             -- QA result field
      declare @varSql                         varchar(10000)      -- SQL string for dynamic SQL execution

      set @varAugDate                  = (dateformat(@parProcessDate, 'yyyymmdd'))

      set @varProcessIdentifier        = 'aug_MADataPrep_v01'

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

      execute logger_add_event @varBuildId, 3, '####### AUG MA data preparation - process started #######'

      if (@parLogQADetails = 0)
          execute logger_add_event @varBuildId, 3, '(note: quiet mode, reporting only warnings and errors)'
      else
          execute logger_add_event @varBuildId, 3, '(note: full reporting mode, reporting all details)'

      set @QA_result_1 = -1
      commit


      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null

      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'Process date: ' || @parProcessDate, null
            execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
            execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
            execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
            execute logger_add_event @varBuildId, 3, 'AUG tables: Vespa_Daily_Augs_' || @varAugDate || ' (+' || dateformat(@parProcessDate - 1, 'yyyymmdd') || ', +' || dateformat(@parProcessDate + 1, 'yyyymmdd') || ')', null
        end


        -- ##############################################################################################################
        -- ##### STEP 0.2 - requirement checks                                                                      #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.2: Requirement checks <<<<<', null

      set @varSql = '
                     if object_id(''vespa_analysts.Vespa_Daily_Augs_' || dateformat(@parProcessDate - 1, 'yyyymmdd') || ''') is null
                        begin
                            execute logger_add_event ' || @varBuildId || ', 1, ''[!!!] Error - required table does not exists (Vespa_Daily_Augs_' || dateformat(@parProcessDate - 1, 'yyyymmdd') || ')'', null
                            return
                        end

                     if object_id(''vespa_analysts.Vespa_Daily_Augs_' || dateformat(@parProcessDate, 'yyyymmdd') || ''') is null
                        begin
                            execute logger_add_event ' || @varBuildId || ', 1, ''[!!!] Error - required table does not exists (Vespa_Daily_Augs_' || dateformat(@parProcessDate, 'yyyymmdd') || ')'', null
                            return
                        end

                     if object_id(''vespa_analysts.Vespa_Daily_Augs_' || dateformat(@parProcessDate + 1, 'yyyymmdd') || ''') is null
                        begin
                            execute logger_add_event ' || @varBuildId || ', 1, ''[!!!] Error - required table does not exists (Vespa_Daily_Augs_' || dateformat(@parProcessDate + 1, 'yyyymmdd') || ')'', null
                            return
                        end

                    '
      execute(@varSql)
      commit


      set @QA_result_1 = (select min(event_level)
                            from z_logger_events
                           where run_id = @varBuildId)

      if (@QA_result_1 = 1)
          return


      if (@parLogQADetails = 1)
        begin
            execute logger_add_event @varBuildId, 3, 'All checks passed', null
        end


        -- ###############################################################################
        -- ##### STEP 0.9 - creating source data view (external procedure)           #####
        -- ###############################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.9: Creating source data view (external procedure) <<<<<', null
      execute vespa_getSourceDataView_v01 @parProcessDate, @parProcessDate, 'Vespa_MADataPrep_tmp_Source_Data', 1, null, @varBuildId



        -- ##############################################################################################################
        -- ##### STEP 1.0 - creating output table                                                                   #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Creating input tables <<<<<', null


        -- ###############################################################################
        -- ##### Get all records from the AUG table (for the selected date)          #####
        -- ###############################################################################
      set @varSql = '
                    if object_id(''Vespa_MADataPrep_tmp_Aug_' || @varAugDate || ''') is not null drop table Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '

                    select
                          cast(''' || @parProcessDate || ''' as date)     as Process_Date,
                          cast(''' || @parProcessDate || ''' as date)     as Aug_Date,
                          Cb_Row_Id                                       as Instance_Id,
                          Event_Id,
                          Subscriber_id                                   as Subscriber_Id,
                          Event_Start_Time                                as Event_Start_Time,
                          case
                            when Capped_Event_End_Time is null then Uncapped_Event_End_Time
                              else Capped_Event_End_Time
                          end                                             as Event_End_Time,
                          Viewing_Starts                                  as Instance_Start_Time,
                          Viewing_Stops                                   as Instance_End_Time,
                          Time_Since_Recording                            as Time_Since_Recording,
                          Programme_Trans_Sk                              as Programme_Instance_Id,
                          Live_Flag                                       as Live_Flag
                      into Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                      from vespa_analysts.Vespa_Daily_Augs_' || @varAugDate || '
                    commit

                    create unique hg index idx0 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Instance_Id)
                    create        hg index idx1 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Event_Id)
                    create        hg index idx2 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Subscriber_id)
                    create      dttm index idx3 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Event_Start_Time)
                    create      dttm index idx4 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Event_End_Time)
                    create      dttm index idx5 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Instance_Start_Time)
                    create      dttm index idx6 on Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '(Instance_End_Time)
                    '
      execute(@varSql)
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of AUG records #####
              set @varSql = '
                            declare @QA_result_1 integer

                            select @QA_result_1 = count(1)
                              from Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                             where Aug_Date = ''' || @parProcessDate || '''

                            execute logger_add_event ' || @varBuildId || ', 3, ''Current day events added (' || @parProcessDate || ')'', @QA_result_1

                            '
              execute(@varSql)
              commit

        end
      commit


        -- ###############################################################################
        -- ##### Get all records from the AUG table (for the previous day)           #####
        -- ###############################################################################
      set @varSql = '
                    insert into Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                    select
                          cast(''' || @parProcessDate || ''' as date)     as Process_Date,
                          cast(''' || @parProcessDate - 1 || ''' as date) as Aug_Date,
                          aug.Cb_Row_Id                                   as Instance_Id,
                          aug.Event_Id,
                          aug.Subscriber_id                               as Subscriber_Id,
                          aug.Event_Start_Time                            as Event_Start_Time,
                          case
                            when Capped_Event_End_Time is null then Uncapped_Event_End_Time
                              else Capped_Event_End_Time
                          end                                             as Event_End_Time,
                          Viewing_Starts                                  as Instance_Start_Time,
                          Viewing_Stops                                   as Instance_End_Time,
                          aug.Time_Since_Recording                        as Time_Since_Recording,
                          aug.Programme_Trans_Sk                          as Programme_Instance_Id,
                          aug.Live_Flag                                   as Live_Flag
                      from vespa_analysts.Vespa_Daily_Augs_' || dateformat(@parProcessDate - 1, 'yyyymmdd') || ' aug left join Vespa_MADataPrep_tmp_Aug_' || @varAugDate || ' base
                        on base.Subscriber_Id = aug.Subscriber_Id
                       and base.Event_Start_Time = aug.Event_Start_Time
                     where Viewing_Stops >= ''' || @parProcessDate || '''
                       and base.Event_Id is null
                    commit

                    '
      execute(@varSql)
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of AUG records #####
              set @varSql = '
                            declare @QA_result_1 integer

                            select @QA_result_1 = count(1)
                              from Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                             where Aug_Date = ''' || cast(@parProcessDate - 1 as date) || '''

                            execute logger_add_event ' || @varBuildId || ', 3, ''Previous day events added (' || cast(@parProcessDate - 1 as date) || ')'', @QA_result_1

                            '
              execute(@varSql)
              commit

        end

      commit


        -- ###############################################################################
        -- ##### Get all records from the AUG table (for the next day)               #####
        -- ###############################################################################
        -- Pick only very first events
      set @varSql = '
                    if object_id(''Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence'') is not null drop table Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence

                    select
                          Cb_Row_Id,
                          Event_Id,
                          Subscriber_id,
                          Event_Start_Time,
                          Capped_Event_End_Time,
                          Uncapped_Event_End_Time,
                          Viewing_Starts,
                          Viewing_Stops,
                          Time_Since_Recording,
                          Programme_Trans_Sk,
                          Live_Flag,
                          rank() over (partition by Subscriber_id order by Viewing_Starts, Viewing_Stops, Cb_Row_Id) as Instance_Sequence
                      into Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence
                      from vespa_analysts.Vespa_Daily_Augs_' || dateformat(@parProcessDate + 1, 'yyyymmdd') || '
                    commit

                    create        hg index idx1 on Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence(Event_Id)
                    create        hg index idx2 on Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence(Subscriber_id)
                    create      dttm index idx3 on Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence(Event_Start_Time)
                    create        lf index idx4 on Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence(Instance_Sequence)
                    '
      execute(@varSql)
      commit



        -- Get events
      set @varSql = '
                    insert into Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                    select
                          cast(''' || @parProcessDate || ''' as date)     as Process_Date,
                          cast(''' || @parProcessDate + 1 || ''' as date) as Aug_Date,
                          aug.Cb_Row_Id                                   as Instance_Id,
                          aug.Event_Id,
                          aug.Subscriber_id                               as Subscriber_Id,
                          aug.Event_Start_Time                            as Event_Start_Time,
                          case
                            when Capped_Event_End_Time is null then Uncapped_Event_End_Time
                              else Capped_Event_End_Time
                          end                                             as Event_End_Time,
                          aug.Viewing_Starts                              as Instance_Start_Time,
                          aug.Viewing_Stops                               as Instance_End_Time,
                          aug.Time_Since_Recording                        as Time_Since_Recording,
                          aug.Programme_Trans_Sk                          as Programme_Instance_Id,
                          aug.Live_Flag                                   as Live_Flag
                      from Vespa_MADataPrep_tmp_Aug_Next_Day_Sequence aug left join Vespa_MADataPrep_tmp_Aug_' || @varAugDate || ' base
                        on base.Subscriber_Id = aug.Subscriber_Id
                       and base.Event_Start_Time = aug.Event_Start_Time
                     where base.Event_Id is null
                       and aug.Instance_Sequence = 1
                    commit

                    '
      execute(@varSql)
      commit


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of AUG records #####
              set @varSql = '
                            declare @QA_result_1 integer

                            select @QA_result_1 = count(1)
                              from Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                             where Aug_Date = ''' || cast(@parProcessDate + 1 as date) || '''

                            execute logger_add_event ' || @varBuildId || ', 3, ''Next day events added (' || cast(@parProcessDate + 1 as date) || ')'', @QA_result_1

                            '
              execute(@varSql)
              commit

        end

      commit



        -- ##############################################################################################################
        -- ##### STEP 2.0 - appending fields from the source data                                                   #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Appending fields from the source data <<<<<', null


        -- ###############################################################################
        -- ##### Get additional information from source data                         #####
        -- ###############################################################################
        -- Adding fields
      set @varSql = '
                    alter table Vespa_MADataPrep_tmp_Aug_' || @varAugDate || '
                      add (Channel_Id           int     default -1)
                    '
      execute(@varSql)
      commit

      if (@parLogQADetails = 1)
        begin
              execute logger_add_event @varBuildId, 3, 'New fields added', null
        end


        -- Updating data
      set @varSql = '
                    update Vespa_MADataPrep_tmp_Aug_' || @varAugDate || ' base
                       set base.Channel_Id          = det.Dk_Channel_Dim
                      from Vespa_MADataPrep_tmp_Source_Data det
                     where base.Instance_Id = det.Pk_Viewing_Prog_Instance_Fact
                    commit
                    '
      execute(@varSql)
      commit

      if (@parLogQADetails = 1)
        begin
              execute logger_add_event @varBuildId, 3, 'New field values updated', null
        end



      execute logger_add_event @varBuildId, 3, '####### AUG MA data preparation - process completed #######'


 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################




