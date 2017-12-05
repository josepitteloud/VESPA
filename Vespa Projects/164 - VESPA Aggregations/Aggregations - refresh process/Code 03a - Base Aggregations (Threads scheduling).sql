/*###############################################################################
# Created on:   28/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - base aggregations (scheduling for individual threads)
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - thread scheduling
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VAggr_Meta_Run_Schedule
#     - VAggr_Meta_Run_Schedule_Thread_1
#     - VAggr_Meta_Run_Schedule_Thread_2
#     - VAggr_Meta_Run_Schedule_Thread_3
#     - VAggr_Meta_Run_Schedule_Thread_4
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/11/2013  SBE   v01 - initial version
# 21/02/2014  SBE   Removed reference to "bednaszs" schema
# 06/06/2014  ABA   Fix to choose one aggregate from the schedule when there are duplicates - picks the most recent update
#
###############################################################################*/


if object_id('VAggr_3_Base_Aggr_Scheduling') is not null then drop procedure VAggr_3_Base_Aggr_Scheduling end if;
create procedure VAggr_3_Base_Aggr_Scheduling
      @parPeriodKey             bigint,
      @parSchema                varchar(40) = '',    -- Thread fact tables location (schema name)
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


      set @varProcessIdentifier        = 'VAggr_3_Bs_Schdl_v01'

      if (@parSchema is null or @parSchema = '')
          set @parSchema = user_name()

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations - thread scheduling] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - thread scheduling                                                                       #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Assembling thread run results <<<<<', null

      execute logger_add_event @varBuildId, 3, 'Thread tables used in schema: ' || @parSchema, null

      set @varSql = '
                      truncate table ' || @parSchema || '.VAggr_Meta_Run_Schedule_Thread_##^0^##
                      commit
                      execute logger_add_event ' || @varBuildId || ', 3, ''Thread ###^0^## schedule table emptied'', @@rowcount

                      insert into ' || @parSchema || '.VAggr_Meta_Run_Schedule_Thread_##^0^##
                             (Id,
                              Period_Key,
                              Aggregation_Key,
                              Thread_Id,
                              Run_Sequence,
                              Run_Processed_Flag)
                       select Id,
                              Period_Key,
                              Aggregation_Key,
                              Thread_Id,
                              Run_Sequence,
                              Run_Processed_Flag
                         from(
                              select
                                     sch.Id,
                                     sch.Period_Key,
                                     sch.Aggregation_Key,
                                     sch.Thread_Id,
                                     sch.Run_Sequence,
                                     sch.Run_Processed_Flag,
                                     sch.updated_on,
                                     dense_rank() over(partition by sch.period_key, sch.aggregation_key order by sch.updated_on desc) rank
                                from VESPA_Shared.Aggr_Aggregation_Dim aggr,
                                     ' || @parSchema || '.VAggr_Meta_Run_Schedule sch
                               where aggr.Aggregation_Key = sch.Aggregation_Key
                                 and aggr.Aggregation_Type = ''base''
                                 and sch.Period_Key = ' || @parPeriodKey || '
                                 and sch.Run_Processed_Flag = 0
                                 and sch.Thread_Id = ##^0^## ) d
                         where d.rank = 1
                         order by period_key, aggregation_key
                      commit

                      execute logger_add_event ' || @varBuildId || ', 3, ''Thread ###^0^## schedule created'', @@rowcount
                    '
      execute( replace(@varSql, '##^0^##', '1') )
      execute( replace(@varSql, '##^0^##', '2') )
      execute( replace(@varSql, '##^0^##', '3') )
      execute( replace(@varSql, '##^0^##', '4') )



        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Base aggregations - thread scheduling] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null
      commit

end;









