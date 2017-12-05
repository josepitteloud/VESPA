/*###############################################################################
# Created on:   21/02/2014
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - clean-up script
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - dropping temporary tables
#               STEP 2.0 - removing temporary data
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# N/A
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 21/02/2014  SBE   v01 - initial version
#
###############################################################################*/


if object_id('VAggr_8_Cleanup') is not null then drop procedure VAggr_8_Cleanup end if;
create procedure VAggr_8_Cleanup
      @parPeriodKey             bigint,
      @parTruncTempData         bit = 0,             -- Truncate data in temp tables, e.g. viewing, account attributes etc.
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
      declare @varSQL                         varchar(25000)

      set @varProcessIdentifier        = 'VAggr_8_Cleanup_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId

      if (@parTruncTempData is null or @parTruncTempData not in (0, 1) )
          set @parTruncTempData = 0



        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Cleaning up] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - Dropping temporary tables                                                               #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Dropping temporary tables <<<<<', null

        -- Account attributes
      if object_id('VAggr_tmp_Account_Weights') is not null drop table VAggr_tmp_Account_Weights
      if object_id('VAggr_tmp_Account_Portfolio_Snapshot') is not null drop table VAggr_tmp_Account_Portfolio_Snapshot
      if object_id('VAggr_tmp_Subscription_Base') is not null drop table VAggr_tmp_Subscription_Base
      if object_id('VAggr_tmp_Subscription_History') is not null drop table VAggr_tmp_Subscription_History
      if object_id('VAggr_tmp_Subscription_Summary') is not null drop table VAggr_tmp_Subscription_Summary
      if object_id('VAggr_tmp_Account_Subscription_Lookup') is not null drop table VAggr_tmp_Account_Subscription_Lookup
      if object_id('VAggr_tmp_PVR_Boxes') is not null drop table VAggr_tmp_PVR_Boxes

        -- Viewing data
      drop view if exists v_VAggr_02_Viewing_Events
      if object_id('VAggr_tmp_Prog_Instance_Summary') is not null drop table VAggr_tmp_Prog_Instance_Summary

        -- Grouping
      if object_id('VAggr_tmp_Scaling_Array') is not null drop table VAggr_tmp_Scaling_Array
      if object_id('VAggr_tmp_Grouping_Universe') is not null drop table VAggr_tmp_Grouping_Universe
      if object_id('VAggr_tmp_Grouping_Definitions') is not null drop table VAggr_tmp_Grouping_Definitions



        -- ##############################################################################################################
        -- ##### STEP 2.0 - Removing temporary data                                                                 #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Removing temporary data <<<<<', null
      if (@parTruncTempData = 1)
        begin
          truncate table VAggr_01_Account_Attributes
          truncate table VAggr_02_Viewing_Events
          truncate table VAggr_02_Viewing_Events_Sample
          truncate table VAggr_Meta_Run_Schedule_Thread_1
          truncate table VAggr_Meta_Run_Schedule_Thread_2
          truncate table VAggr_Meta_Run_Schedule_Thread_3
          truncate table VAggr_Meta_Run_Schedule_Thread_4
          truncate table VAggr_Fact_Thread_1
          truncate table VAggr_Fact_Thread_2
          truncate table VAggr_Fact_Thread_3
          truncate table VAggr_Fact_Thread_4
        end
      else
        begin
            execute logger_add_event @varBuildId, 3, '(skipped)', null
        end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Cleaning up] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;




