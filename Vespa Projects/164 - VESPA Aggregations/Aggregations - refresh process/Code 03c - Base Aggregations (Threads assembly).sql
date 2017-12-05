/*###############################################################################
# Created on:   28/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - base aggregations (assembling thread results)
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - updating schedule master table
#               STEP 2.0 - assembling thread run results
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VAggr_Fact_Thread_1
#     - VAggr_Fact_Thread_2
#     - VAggr_Fact_Thread_3
#     - VAggr_Fact_Thread_4
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/11/2013  SBE   v01 - initial version
# 06/12/2013  SBE   Master schedule table update from thread ones added
# 21/02/2014  SBE   - Removed reference to "bednaszs" schema
#                   - Changed thread fact table from Fact* to VFact*
# 16/06/2014  ABA   Added section to delete any existing aggregations from Fact table first (to facilitate updates)
#
###############################################################################*/


if object_id('VAggr_3_Base_Aggr_Assembly') is not null then drop procedure VAggr_3_Base_Aggr_Assembly end if;
create procedure VAggr_3_Base_Aggr_Assembly
      @parPeriodKey             bigint,
      @parSrcSchema             varchar(40) = '',    -- Thread fact tables location (schema name)
      @parDestSchema            varchar(40) = '',    -- Fact table location (schema name)
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


      set @varProcessIdentifier        = 'VAggr_3_Bs_Asmbl_v01'

      if (@parSrcSchema is null or @parSrcSchema = '')
          set @parSrcSchema = user_name()

      if (@parDestSchema is null or @parDestSchema = '')
          set @parDestSchema = user_name()

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations - assembly] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - updating schedule master table                                                          #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Updating schedule master table <<<<<', null

      set @varSql = '
                      update VAggr_Meta_Run_Schedule base
                         set base.Run_Processed_Flag = det.Run_Processed_Flag
                        from VAggr_Meta_Run_Schedule_Thread_##^0^## det
                       where base.Id = det.Id
                      commit

                      execute logger_add_event ' || @varBuildId || ', 3, ''Thread ###^0^## run progress updated to the master table'', @@rowcount
                    '
      execute( replace(@varSql, '##^0^##', '1') )
      execute( replace(@varSql, '##^0^##', '2') )
      execute( replace(@varSql, '##^0^##', '3') )
      execute( replace(@varSql, '##^0^##', '4') )



        -- ##############################################################################################################
        -- ##### STEP 2.0 - assembling thread run results                                                           #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 2.0: Assembling thread run results <<<<<', null

      execute logger_add_event @varBuildId, 3, 'Thread tables used: ' || @parSrcSchema || '.VAggr_Fact_Thread_{1..N}', null
      execute logger_add_event @varBuildId, 3, 'Fact table used: ' || @parDestSchema || '.Aggr_Fact', null


        --delete any aggregations for the same period if they exist first

      set @varSql = '
                      delete from ' || @parDestSchema || '.Aggr_Fact
                             from ' || @parDestSchema || '.Aggr_Fact fact,
                                  ' || @parSrcSchema || '.VAggr_Fact_Thread_##^0^## thread
                       where fact.Period_Key = thread.Period_Key
                         and fact.aggregation_key = thread.aggregation_key
                         and fact.account_number = thread.account_number
                         and fact.Period_Key = ' || @parPeriodKey || '

                      insert into ' || @parDestSchema || '.Aggr_Fact
                             (Period_Key,
                              Aggregation_Key,
                              Account_Number,
                              Panel_Id,
                              Metric_Value)
                        select
                              Period_Key,
                              Aggregation_Key,
                              Account_Number,
                              Panel_Id,
                              Metric_Value

                          from ' || @parSrcSchema || '.VAggr_Fact_Thread_##^0^##
                         where Period_Key = ' || @parPeriodKey || '
                      commit

                      execute logger_add_event ' || @varBuildId || ', 3, ''Thread ###^0^## records moved to "' || @parDestSchema || '.Aggr_Fact"'', @@rowcount
                    '
      execute( replace(@varSql, '##^0^##', '1') )
      execute( replace(@varSql, '##^0^##', '2') )
      execute( replace(@varSql, '##^0^##', '3') )
      execute( replace(@varSql, '##^0^##', '4') )


        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations - assembly] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;









