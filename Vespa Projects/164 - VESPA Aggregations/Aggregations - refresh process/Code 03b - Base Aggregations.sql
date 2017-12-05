/*###############################################################################
# Created on:   28/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - base aggregations (based on raw data)
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - calculating base aggregations
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VAggr_02_Viewing_Events
#     - VAggr_Meta_Aggr_Definitions
#     - VAggr_Meta_Run_Schedule
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
# 29/11/2013  SBE   Threading mechanism implemented
# 21/02/2014  SBE   - Removed reference to "bednaszs" schema
#                   - Changed thread fact table from Fact* to VFact*
#
###############################################################################*/


if object_id('VAggr_3_Base_Aggr') is not null then drop procedure VAggr_3_Base_Aggr end if;
create procedure VAggr_3_Base_Aggr
      @parPeriodKey             bigint,
      @parDestSchema            varchar(40) = '',    -- Fact table location (schema name)
      @parThreadId              tinyint = 0,         -- 0 => No threading, >0 => thread id
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
      declare @varFactTable                   varchar(30)
      declare @varSQL                         varchar(25000)
      declare @varLoopCurrId                  smallint
      declare @varLoopCurrAggrName            varchar(100)
      declare @varLoopCurrSeq                 smallint
      declare @varLoopIterNum                 smallint
      declare @varDerivation                  varchar(25000)


      set @varProcessIdentifier        = 'VAggr_3_Base_v01'

      if (@parDestSchema is null or @parDestSchema = '' or @parThreadId > 0)
          set @parDestSchema = user_name()

      if ( @parThreadId is null or @parThreadId not in (0, 1, 2, 3, 4) )
          set @parThreadId = 0

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId

      set @varFactTable = 'Aggr_Fact'
      if (@parThreadId > 0)
          set @varFactTable = 'V' || @varFactTable || '_Thread_' || @parThreadId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Mode: ' || case when @parThreadId = 0 then 'NORMAL' else 'THREADED (id: #' || @parThreadId || ')' end, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - calculating base aggregations                                                           #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Calculating base aggregations <<<<<', null
      set @varLoopCurrId  = 0
      set @varLoopCurrSeq = 1
      set @varLoopIterNum = 0

        -- ##### THREADED RUN DEPENDENCY ######
        -- Get number of iterations
      if (@parThreadId > 0)             -- ### WITH threading ####
        begin
            set @varLoopIterNum = (select
                                         count(*)
                                     from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                          VAggr_Meta_Aggr_Definitions def,
                                          (
                                            select a.* from VAggr_Meta_Run_Schedule_Thread_1 a union all
                                            select b.* from VAggr_Meta_Run_Schedule_Thread_2 b union all
                                            select c.* from VAggr_Meta_Run_Schedule_Thread_3 c union all
                                            select d.* from VAggr_Meta_Run_Schedule_Thread_4 d
                                           ) lnk
                                    where aggr.Aggregation_Key = def.Aggregation_Key
                                      and aggr.Aggregation_Key = lnk.Aggregation_Key
                                      and lnk.Period_Key = @parPeriodKey
                                      and lnk.Run_Processed_Flag = 0
                                      and lnk.Thread_Id = @parThreadId                                      -- When threading is ON - pick the exact thread id
                                      and aggr.Aggregation_Type = 'base')
        end

      else                              -- ### WITHOUT threading ####
        begin
            set @varLoopIterNum = (select
                                         count(*)
                                     from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                          VAggr_Meta_Aggr_Definitions def,
                                          VAggr_Meta_Run_Schedule lnk
                                    where aggr.Aggregation_Key = def.Aggregation_Key
                                      and aggr.Aggregation_Key = lnk.Aggregation_Key
                                      and lnk.Period_Key = @parPeriodKey
                                      and lnk.Run_Processed_Flag = 0
                                      and lnk.Thread_Id between 1 and 4                                     -- When threading is OFF - pick all ids
                                      and aggr.Aggregation_Type = 'base')
        end


      execute logger_add_event @varBuildId, 3, 'Number of aggregations scheduled for processing: ' || @varLoopIterNum, null
      execute logger_add_event @varBuildId, 3, 'Fact table used: ' || @parDestSchema || '.' || @varFactTable, null


      while @varLoopCurrSeq <= @varLoopIterNum
        begin

              -- ##### THREADED RUN DEPENDENCY ######
              -- Get current aggregation Id
            if (@parThreadId > 0)             -- ### WITH threading ####
              begin
                  select top 1
                        @varLoopCurrId        = Aggregation_Key,
                        @varLoopCurrAggrName  = Aggregation_Name
                    from (select
                                row_number() over (order by lnk.Run_Sequence, aggr.Aggregation_Key) as Seq_Order,
                                aggr.Aggregation_Key,
                                aggr.Aggregation_Name
                            from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                 VAggr_Meta_Aggr_Definitions def,
                                 (
                                   select a.* from VAggr_Meta_Run_Schedule_Thread_1 a union all
                                   select b.* from VAggr_Meta_Run_Schedule_Thread_2 b union all
                                   select c.* from VAggr_Meta_Run_Schedule_Thread_3 c union all
                                   select d.* from VAggr_Meta_Run_Schedule_Thread_4 d
                                  ) lnk
                           where aggr.Aggregation_Key = def.Aggregation_Key
                             and aggr.Aggregation_Key = lnk.Aggregation_Key
                             and lnk.Period_Key = @parPeriodKey
                             and lnk.Run_Processed_Flag = 0
                             and lnk.Thread_Id = @parThreadId                                               -- When threading is ON - pick the exact thread id
                             and aggr.Aggregation_Type = 'base') a
                   order by Seq_Order
              end

            else                              -- ### WITHOUT threading ####
              begin
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
                             and lnk.Thread_Id between 1 and 4                                              -- When threading is OFF - pick all ids
                             and aggr.Aggregation_Type = 'base') a
                   order by Seq_Order
              end


              -- Get current derivation rule
            set @varDerivation = (select
                                        Derivation
                                    from VAggr_Meta_Aggr_Definitions
                                   where Aggregation_Key = @varLoopCurrId)

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

              -- ##### THREADED RUN DEPENDENCY ######
            if (@parThreadId > 0)
              begin
                  set @varSql = '
                                  delete from ' || @parDestSchema || '.' || @varFactTable || '
                                   where Period_Key = ' || @parPeriodKey || '
                                     and Aggregation_Key = ' || @varLoopCurrId || '
                                  commit
                                  execute logger_add_event ' || @varBuildId || ', 3, ''Resetting values - thread fact records deleted'', @@rowcount
                                '
                  execute(@varSql)
              end

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
                            insert into ' || @parDestSchema || '.' || @varFactTable || '
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
                                    ##^0^##                                                     -- Metric_Value

                                from VESPA_Shared.Aggr_Account_Attributes acc
                                        inner join VESPA_Shared.Aggr_Period_Dim prd            on acc.Period_Key = prd.Period_Key
                                                                                              and acc.Period_Key = ' || @parPeriodKey || '
                                        left join VAggr_02_Viewing_Events vw                   on acc.Account_Number = vw.Account_Number
                               group by acc.Account_Number, acc.Panel_Id
                            commit

                            execute logger_add_event ' || @varBuildId || ', 3, ''New records added'', @@rowcount
                          '
            execute( replace(@varSql, '##^0^##', @varDerivation) )


            if (@parThreadId > 0)
              begin
                  set @varSql = '
                                  update VAggr_Meta_Run_Schedule_Thread_' || @parThreadId || '
                                     set Run_Processed_Flag = 1
                                   where Period_Key = ' || @parPeriodKey || '
                                     and Aggregation_Key = ' || @varLoopCurrId || '
                                  commit

                                  execute logger_add_event ' || @varBuildId || ', 3, ''Scheduled run flagged as completed in this run'', @@rowcount
                                '
                  execute( @varSql )
              end

            else
              begin
                  set @varSql = '
                                  update VAggr_Meta_Run_Schedule
                                     set Run_Processed_Flag = 1
                                   where Period_Key = ' || @parPeriodKey || '
                                     and Aggregation_Key = ' || @varLoopCurrId || '
                                  commit

                                  execute logger_add_event ' || @varBuildId || ', 3, ''Scheduled run flagged as completed in this run'', @@rowcount
                                '
                  execute( @varSql )
              end


            set @varLoopCurrSeq = @varLoopCurrSeq + 1

        end



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;







