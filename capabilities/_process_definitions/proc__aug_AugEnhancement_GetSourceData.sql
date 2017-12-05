/*###############################################################################
# Created on:   26/04/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Aug Enhancement process - viewing data preparation procedure
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating source data view (external procedure)
#               STEP 2.0 - pulling all records from the source data
#               STEP 3.0 - creating an aggregated view
#
# To do:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# - procedure "vespa_analysts.vespa_getSourceDataView_v01" must exist
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/04/2013  SBE   v01 - initial version
#
#
###############################################################################*/


if object_id('aug_AugEnhancement_GetSourceData_v01') is not null then drop procedure aug_AugEnhancement_GetSourceData_v01 endif;
commit;


create procedure aug_AugEnhancement_GetSourceData_v01
      @parStartDate             date = null,         -- Event date start
      @parEndDate               date = null,         -- Event date end

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
      declare @varDateHourStart               int                 -- Event datehour start
      declare @varDateHourEnd                 int                 -- Event datehour end
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @QA_result_1                    integer             -- QA result field
      declare @QA_result_2                    integer             -- QA result field
      declare @varSql                         varchar(10000)      -- SQL string for dynamic SQL execution

      set @varProcessIdentifier        = 'AugEnh_Src_v01'

      set @varDateHourStart            = cast(dateformat(@parStartDate - 1, 'yyyymmdd00') as int)
      set @varDateHourEnd              = cast(dateformat(@parEndDate, 'yyyymmdd23') as int)


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

      execute logger_add_event @varBuildId, 3, '####### AUG Enhancement (source data) - process started #######'

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
            execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@parStartDate, 'dd/mm/yyyy') || ' - ' || dateformat(@parEndDate, 'dd/mm/yyyy'), null
        end


        -- ###############################################################################
        -- ##### STEP 1.0 - creating source data view (external procedure)           #####
        -- ###############################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Creating source data view (external procedure) <<<<<', null
      -- execute vespa_getSourceDataView_v01 @parStartDate, @parEndDate, 'Vespa_AugEnhancement_tmp_Source_Data', 1, null, @varBuildId



        -- ###############################################################################
        -- ##### STEP 2.0 - pulling all records from the source data                 #####
        -- ###############################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Pulling all records from the source data <<<<<', null

      if object_id('VESPA_AugEnh_tmp_Source_Snapshot_Full') is not null drop table VESPA_AugEnh_tmp_Source_Snapshot_Full
      set @varSql = '
                    select
                          Pk_Viewing_Prog_Instance_Fact,
                          Subscriber_Id,
                          Event_Start_Date_Time_Utc,
                          Event_End_Date_Time_Utc,
                          Instance_Start_Date_Time_Utc,
                          Instance_End_Date_Time_Utc,
                          Time_In_Seconds_Since_Recording
                      into VESPA_AugEnh_tmp_Source_Snapshot_Full
                      from Vespa_AugEnhancement_tmp_Source_Data
                     where dk_event_start_datehour_dim between ' || @varDateHourStart || ' and ' || @varDateHourEnd || '
                       and Panel_Id = 12
                       and (REPORTED_PLAYBACK_SPEED is null or REPORTED_PLAYBACK_SPEED = 2)
                       and INSTANCE_START_DATE_TIME_UTC < INSTANCE_END_DATE_TIME_UTC     -- Remove 0sec instances
                       and type_of_viewing_event <> ''Non viewing event''
                       and type_of_viewing_event is not null
                       and account_number is not null
                       and subscriber_id is not null                                     -- There shouldnt be any nulls, but there are
                    '
      execute(@varSql)
      commit

      create hg index idx0 on VESPA_AugEnh_tmp_Source_Snapshot_Full(Pk_Viewing_Prog_Instance_Fact)
      create hg index idx1 on VESPA_AugEnh_tmp_Source_Snapshot_Full(Subscriber_Id)
      create dttm index idx2 on VESPA_AugEnh_tmp_Source_Snapshot_Full(Instance_Start_Date_Time_Utc)

        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of records #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Source_Snapshot_Full
              execute logger_add_event @varBuildId, 3, 'Source viewing data pulled', @QA_result_2

        end

      set @QA_result_2 = -1
      commit


        -- ###############################################################################
        -- ##### STEP 3.0 - creating an aggregated view                              #####
        -- ###############################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 3.0: Creating an aggregated view <<<<<', null

      if object_id('VESPA_AugEnh_tmp_Source_Snapshot_Aggr') is not null drop table VESPA_AugEnh_tmp_Source_Snapshot_Aggr
      select
            min(Pk_Viewing_Prog_Instance_Fact)            as Instance_Id,
            count(distinct Pk_Viewing_Prog_Instance_Fact) as Instance_Ids_Cnt,
            Subscriber_Id                                 as Subscriber_Id,
            count(distinct Event_Start_Date_Time_Utc)     as Event_Start_Times_Cnt,
            count(distinct Event_End_Date_Time_Utc)       as Event_End_Times_Cnt,
            count(distinct Instance_End_Date_Time_Utc)    as Instance_End_Times_Cnt,
            min(Event_Start_Date_Time_Utc)                as Event_Start_Time,
            max(Event_End_Date_Time_Utc)                  as Event_End_Time,
            Instance_Start_Date_Time_Utc                  as Instance_Start_Time,
            max(Instance_End_Date_Time_Utc)               as Instance_End_Time,
            max(Time_In_Seconds_Since_Recording)          as Time_Since_Recording,
            count(*)                                      as Cnt
        into VESPA_AugEnh_tmp_Source_Snapshot_Aggr
        from VESPA_AugEnh_tmp_Source_Snapshot_Full
       group by Subscriber_Id, Instance_Start_Date_Time_Utc

      create unique hg index idx0 on VESPA_AugEnh_tmp_Source_Snapshot_Aggr(Instance_Id)
      create hg index idx1 on VESPA_AugEnh_tmp_Source_Snapshot_Aggr(Subscriber_Id)
      create dttm index idx2 on VESPA_AugEnh_tmp_Source_Snapshot_Aggr(Instance_Start_Time)


        -- ##### QA #####
      if (@parLogQADetails = 1)
        begin

                -- ##### Total number of records #####
              select @QA_result_2 = count(1)
                from VESPA_AugEnh_tmp_Source_Snapshot_Aggr
              execute logger_add_event @varBuildId, 3, 'Source data aggregated view created', @QA_result_2

        end

      set @QA_result_2 = -1
      commit


      execute logger_add_event @varBuildId, 3, '####### AUG Enhancement (source data) - process completed #######'
      execute logger_add_event @varBuildId, 3, ' '


 -- #### (procedure end) #####
end;

commit;
go


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##############################################################################################################



