/*###############################################################################
# Created on:   18/07/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Capping Metrics Report - Clean up script
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - removing temporary objects
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 12/07/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('CAP_Metrics_Rep_Clean_Up') is not null then drop procedure CAP_Metrics_Rep_Clean_Up end if;
create procedure CAP_Metrics_Rep_Clean_Up
      @parRefreshIdentifier     varchar(40) = '',    -- Logger - refresh identifier
      @parBuildId               bigint = null        -- Logger - add events to an existing logger process
as
begin

        -- ##############################################################################################################
        -- ##### STEP 0.1 - preparing environment                                                                   #####
        -- ##############################################################################################################

        -- ###############################################################################
        -- ##### Define and set variables                                            #####
        -- ###############################################################################

      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID
      declare @varSQL                         varchar(15000)

      set @varProcessIdentifier        = 'CAPMetRep_Cleanup_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Clean up] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'User context: ' || @varUsername, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - removing temporary objects                                                              #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Removing temporary objects <<<<<', null

      if object_id('CAP_Metrics_Rep_tmp_Consumer_View')              is not null drop table CAP_Metrics_Rep_tmp_Consumer_View
      if object_id('CAP_Metrics_Rep_tmp_OnOff_Net')                  is not null drop table CAP_Metrics_Rep_tmp_OnOff_Net
      if object_id('CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup')      is not null drop table CAP_Metrics_Rep_tmp_OnOff_Net_Exch_Lookup
      if object_id('CAP_Metrics_Rep_tmp_Universe')                   is not null drop table CAP_Metrics_Rep_tmp_Universe
      if object_id('CAP_Metrics_Rep_tmp_Value_Segment_Dates')        is not null drop table CAP_Metrics_Rep_tmp_Value_Segment_Dates
      if object_id('CAP_Metrics_Rep_tmp_Viewing_Records')            is not null drop table CAP_Metrics_Rep_tmp_Viewing_Records

      drop view if exists v_CAP_Metrics_Rep_tmp_Raw_Viewing


      execute logger_add_event @varBuildId, 3, 'Temporary objects deleted', null



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Clean up] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


