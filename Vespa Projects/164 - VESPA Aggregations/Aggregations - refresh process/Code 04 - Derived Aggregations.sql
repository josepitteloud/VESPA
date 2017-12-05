/*###############################################################################
# Created on:   28/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - derived aggregations (based on Base ones)
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - calculating derived aggregations
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VAggr_Meta_Aggr_Definitions
#     - VAggr_Meta_Run_Schedule
#     - VESPA_Shared.Aggr_Fact
#     - VESPA_Shared.Aggr_Account_Attributes
#     - VESPA_Shared.Aggr_Aggregation_Dim
#     - VESPA_Shared.Aggr_Period_Dim
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/08/2013  SBE   v01 - initial version
# 21/10/2013  SBE   "Destination schema" input parameter added to enable tests
#                   without loading data into VESPA_Shared
# 20/11/2013  SBE   Rollback mechanism implemented to handle reprocessing scenarios
# 21/02/2014  SBE   Removed reference to "bednaszs" schema
#
###############################################################################*/


if object_id('VAggr_4_Derived_Aggr') is not null then drop procedure VAggr_4_Derived_Aggr end if;
create procedure VAggr_4_Derived_Aggr
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
      declare @varLoopCurrAggrName            varchar(100)
      declare @varLoopCurrSeq                 smallint
      declare @varLoopIterNum                 smallint

      declare @varDerivation                  varchar(25000)
      declare @varMetric1                     smallint
      declare @varMetric2                     smallint
      declare @varMetric3                     smallint
      declare @varMetric4                     smallint
      declare @varMetric5                     smallint


      set @varProcessIdentifier        = 'VAggr_4_Derived_v01'

      if (@parDestSchema is null or @parDestSchema = '')
          set @parDestSchema = user_name()

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Derived aggregations] - process started #######', null
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

      set @varMetric1     = 0
      set @varMetric2     = 0
      set @varMetric3     = 0
      set @varMetric4     = 0
      set @varMetric5     = 0


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
                               and aggr.Aggregation_Type = 'derived')

      execute logger_add_event @varBuildId, 3, 'Number of aggregations scheduled for processing: ' || @varLoopIterNum, null
      execute logger_add_event @varBuildId, 3, 'Fact table used: ' || @parDestSchema || '.Aggr_Fact', null


      while @varLoopCurrSeq <= @varLoopIterNum
        begin

              -- Get current aggregation Id
            select top 1
                  @varLoopCurrId        = Aggregation_Key,
                  @varLoopCurrAggrName  = Aggregation_Name
              from (select
                          row_number() over (order by lnk.Run_Sequence, aggr.Aggregation_Key) as Seq_Order,
                          aggr.Aggregation_Key,
                          aggr.Aggregation_Name
                      from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                           VAggr_Meta_Aggr_Definitions def,
                           VAggr_Meta_Run_Schedule lnk
                     where aggr.Aggregation_Key = def.Aggregation_Key
                       and aggr.Aggregation_Key = lnk.Aggregation_Key
                       and lnk.Period_Key = @parPeriodKey
                       and lnk.Run_Processed_Flag = 0
                       and aggr.Aggregation_Type = 'derived') a
             order by Seq_Order
             --where Seq_Order = @varLoopCurrSeq)

              -- Get current derivation rule
            select
                  @varDerivation  = Derivation,
                  @varMetric1     = Metric_1,
                  @varMetric2     = Metric_2,
                  @varMetric3     = Metric_3,
                  @varMetric4     = Metric_4,
                  @varMetric5     = Metric_5
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
                              select
                                    ' || @parPeriodKey || ' as Period_Key,                      -- Period Key
                                    ' || @varLoopCurrId || ' as Aggregation_Key,                -- Aggregation Key
                                    acc.Account_Number,                                         -- Account Number
                                    acc.Panel_Id,
                                    ' || @varDerivation || '                                    -- Metric_Value

                                from VESPA_Shared.Aggr_Account_Attributes acc
                                                                   inner join VESPA_Shared.Aggr_Period_Dim prd
                                                                         on acc.Period_Key = prd.Period_Key
                                                                        and acc.Period_Key = ' || @parPeriodKey ||
                                      case
                                        when @varMetric1 > 0 then ' left join ' || @parDestSchema || '.Aggr_Fact met' || @varMetric1 ||
                                                                      '  on acc.Account_Number = met' || @varMetric1 || '.Account_Number ' ||
                                                                      ' and acc.Period_Key = met' || @varMetric1 || '.Period_Key ' ||
                                                                      ' and met' || @varMetric1 || '.Aggregation_Key = ' || @varMetric1
                                          else ''
                                      end ||

                                      case
                                        when @varMetric2 > 0 then ' left join ' || @parDestSchema || '.Aggr_Fact met' || @varMetric2 ||
                                                                      '  on acc.Account_Number = met' || @varMetric2 || '.Account_Number ' ||
                                                                      ' and acc.Period_Key = met' || @varMetric2 || '.Period_Key ' ||
                                                                      ' and met' || @varMetric2 || '.Aggregation_Key = ' || @varMetric2
                                          else ''
                                      end ||

                                      case
                                        when @varMetric3 > 0 then ' left join ' || @parDestSchema || '.Aggr_Fact met' || @varMetric3 ||
                                                                      '  on acc.Account_Number = met' || @varMetric3 || '.Account_Number ' ||
                                                                      ' and acc.Period_Key = met' || @varMetric3 || '.Period_Key ' ||
                                                                      ' and met' || @varMetric3 || '.Aggregation_Key = ' || @varMetric3
                                          else ''
                                      end ||

                                      case
                                        when @varMetric4 > 0 then ' left join ' || @parDestSchema || '.Aggr_Fact met' || @varMetric4 ||
                                                                      '  on acc.Account_Number = met' || @varMetric4 || '.Account_Number ' ||
                                                                      ' and acc.Period_Key = met' || @varMetric4 || '.Period_Key ' ||
                                                                      ' and met' || @varMetric4 || '.Aggregation_Key = ' || @varMetric4
                                          else ''
                                      end ||

                                      case
                                        when @varMetric5 > 0 then ' left join ' || @parDestSchema || '.Aggr_Fact met' || @varMetric5 ||
                                                                      '  on acc.Account_Number = met' || @varMetric5 || '.Account_Number ' ||
                                                                      ' and acc.Period_Key = met' || @varMetric5 || '.Period_Key ' ||
                                                                      ' and met' || @varMetric5 || '.Aggregation_Key = ' || @varMetric5
                                          else ''
                                      end || '

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
                      replace(
                        replace(
                          replace(
                            replace(@varSql, '#1#', 'met' || @varMetric1 || '.Metric_Value'),
                                  '#2#', 'met' || @varMetric2 || '.Metric_Value'),
                                '#3#', 'met' || @varMetric3 || '.Metric_Value'),
                              '#4#', 'met' || @varMetric4 || '.Metric_Value'),
                            '#5#', 'met' || @varMetric5 || '.Metric_Value')
                   )


            set @varLoopCurrSeq = @varLoopCurrSeq + 1

        end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;






