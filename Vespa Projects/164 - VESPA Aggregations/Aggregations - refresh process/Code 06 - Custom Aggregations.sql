/*###############################################################################
# Created on:   16/09/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - creating custom aggregations
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - calculating derived aggregations
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#   - VESPA_Shared.Aggr_Aggregation_Dim
#   - VAggr_Meta_Aggr_Definitions
#   - VAggr_Meta_Run_Schedule
#   - VESPA_Shared.Aggr_Fact
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 16/09/2013  SBE   v01 - initial version
# 19/11/2013  SBE   "Destination schema" input parameter added to enable tests
#                   without loading data into VESPA_Shared
# 20/11/2013  SBE   Rollback mechanism implemented to handle reprocessing scenarios
# 21/02/2014  SBE   Removed reference to "bednaszs" schema
#
###############################################################################*/


if object_id('VAggr_6_Custom_Aggr') is not null then drop procedure VAggr_6_Custom_Aggr end if;
create procedure VAggr_6_Custom_Aggr
      @parPeriodKey             bigint,
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
      declare @varLoopCurrId                  smallint
      declare @varLoopCurrSeq                 smallint
      declare @varLoopIterNum                 smallint

      declare @varDerivation                  varchar(25000)

      set @varProcessIdentifier        = 'VAggr_6_Custom_v01'

      if (@parDestSchema is null or @parDestSchema = '')
          set @parDestSchema = user_name()

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Custom aggregations] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - calculating derived aggregations                                                        #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Calculating derived aggregations <<<<<', null

      set @varLoopCurrId  = 0
      set @varLoopCurrSeq = 1
      set @varLoopIterNum = 0

        -- Get number of iterations
      set @varLoopIterNum = (select
                                   count(*)
                              from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                   VAggr_Meta_Aggr_Definitions def,
                                   VAggr_Meta_Run_Schedule lnk
                             where aggr.Aggregation_Key = def.Aggregation_Key
                               and aggr.Aggregation_Key = lnk.Aggregation_Key
                               and lnk.Period_Key = @parPeriodKey
                               and lnk.Run_Processed_Flag = 0
                               and aggr.Aggregation_Type = 'custom')

      execute logger_add_event @varBuildId, 3, 'Number of aggregations scheduled for processing: ' || @varLoopIterNum, null
      execute logger_add_event @varBuildId, 3, 'Fact table used: ' || @parDestSchema || '.Aggr_Fact', null


      while @varLoopCurrSeq <= @varLoopIterNum
        begin

              -- Get current aggregation Id
            set @varLoopCurrId = (select top 1
                                        Aggregation_Key
                                    from (select
                                                row_number() over (order by lnk.Run_Sequence, aggr.Aggregation_Key) as Seq_Order,
                                                aggr.Aggregation_Key
                                            from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                                 VAggr_Meta_Aggr_Definitions def,
                                                 VAggr_Meta_Run_Schedule lnk
                                           where aggr.Aggregation_Key = def.Aggregation_Key
                                             and aggr.Aggregation_Key = lnk.Aggregation_Key
                                             and lnk.Period_Key = @parPeriodKey
                                             and lnk.Run_Processed_Flag = 0
                                             and aggr.Aggregation_Type = 'custom') a
                                   order by Seq_Order)
                                   --where Seq_Order = @varLoopCurrSeq)

              -- Get current derivation rule
            select
                  @varDerivation  = Derivation
              from VAggr_Meta_Aggr_Definitions
             where Aggregation_Key = @varLoopCurrId

            execute logger_add_event @varBuildId, 3, '-- Processing aggregation #' || @varLoopCurrId || ' - "' || @varLoopCurrAggrName || '" (' || @varLoopCurrSeq || ' out of ' || @varLoopIterNum || ') --', null



              -- Reset values (for incomplete runs)
            set @varSql = '
                            delete from ' || @parDestSchema || '.Aggr_Metric_Group_Dim
                             where Metric_Group_Key in (select
                                                              Metric_Group_Key
                                                          from ' || @parDestSchema || '.Aggr_Fact
                                                         where Period_Key = ' || @parPeriodKey || '
                                                           and Aggregation_Key = ' || @varLoopCurrId || ')
                               and Metric_Group_Key > 3
                            commit
                            execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - existing metric groups deleted'', @@rowcount
                          '
            execute(@varSql)

            set @varSql = '
                            delete from ' || @parDestSchema || '.Aggr_Fact
                             where Period_Key = ' || @parPeriodKey || '
                               and Aggregation_Key = ' || @varLoopCurrId || '
                            commit
                            execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - fact records deleted'', @@rowcount
                          '
            execute(@varSql)

            set @varSql = '
                            delete from ' || @parDestSchema || '.Aggr_Low_Level_Group_Summaries
                             where Period_Key = ' || @parPeriodKey || '
                               and Aggregation_Key = ' || @varLoopCurrId || '
                            commit
                            execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - existing bin summaries deleted'', @@rowcount
                          '
            execute(@varSql)



              -- Calculate!
            set @varSql = '
                            insert into ' || @parDestSchema || '.Aggr_Fact
                                   (Period_Key,
                                    Aggregation_Key,
                                    Account_Number,
                                    Panel_Id,
                                    Metric_Value)
                              ' || @varDerivation || '
                            commit

                            execute logger_add_event ' || @varBuildId || ', 3, ''New records added'', @@rowcount


                            update VAggr_Meta_Run_Schedule
                               set Run_Processed_Flag = 1
                             where Period_Key = ' || @parPeriodKey || '
                               and Aggregation_Key = ' || @varLoopCurrId || '
                            commit

                            execute logger_add_event ' || @varBuildId || ', 3, ''Scheduled run flagged as completed in this run'', @@rowcount

                          '
            execute(
                    replace(
                      replace(@varSql, '##^PERIOD^##', @parPeriodKey) ,
                            '##^AGGR_KEY^##', @varLoopCurrId)
                   )

            set @varLoopCurrSeq = @varLoopCurrSeq + 1

        end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Custom aggregations] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;






