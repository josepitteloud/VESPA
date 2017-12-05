/*###############################################################################
# Created on:   18/09/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  VESPA Aggregations - metadata information creation:
#               VAggr_Meta_Run_Schedule
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - updating schedule
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - VESPA_Shared.VAggr_Meta_Run_Schedule
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/09/2013  SBE   Initial version
# 20/11/2013  SBE   Procedure created
# 26/11/2013  SBE   Threading mechanism implemented
# 14/05/2014  ABA   EE Re-branding change to comments - (Entertainment = Original/Variety/Family)
#
###############################################################################*/



if object_id('VAggr_0_Schedule') is not null then drop procedure VAggr_0_Schedule end if;
create procedure VAggr_0_Schedule
      @parPeriodKey             bigint,
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
      declare @varStartDate                   date
      declare @varEndDate                     date

      set @varProcessIdentifier        = 'VAggr_0_Schedule_v01'

      select
            @varStartDate = date(Period_Start),
            @varEndDate   = date(Period_End)
        from VESPA_Shared.Aggr_Period_Dim
       where Period_Key = @parPeriodKey

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### VESPA Aggregations [Account attributes] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varStartDate, 'dd/mm/yyyy')  || ' - ' || dateformat(@varEndDate, 'dd/mm/yyyy'), null



      -- ##############################################################################################################
      -- ##### STEP 1.0 - updating schedule                                                                       #####
      -- ##############################################################################################################
      -- Number of days data returned
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key) values (@parPeriodKey,  1)
    commit

      -- Phase 1 CIA aggregations (raw values)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  2, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  3, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  4, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  5, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  6, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  7, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  8, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  9, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 10, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 11, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 12, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 13, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 14, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 15, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 16, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 17, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 18, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 19, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 20, 3)
    commit

      -- Phase 1 CIA aggregations (derivations)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 21, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 22, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 23, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 24, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 25, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 26, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 27, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 28, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 29, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 30, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 31, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 32, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 33, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 34, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 35, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 36, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 37, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 38, 1)
    commit

      -- Offer seeker flag
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key) values (@parPeriodKey, 39)
    commit

      -- # of programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 40, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 41, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 42, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 43, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 44, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 45, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 46, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 47, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 48, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 49, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 50, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 51, 3)
    commit

      -- Average daily # of programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 52, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 53, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 54, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 55, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 56, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 57, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 58, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 59, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 60, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 61, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 62, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 63, 1)
    commit

      -- # of complete programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 64, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 65, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 66, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 67, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 68, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 69, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 70, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 71, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 72, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 73, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 74, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 75, 3)
    commit

      -- Average daily # of complete programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 76, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 77, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 78, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 79, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 80, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 81, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 82, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 83, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 84, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 85, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 86, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 87, 1)
    commit

      -- Original/Variety/Family packages combined into single aggregations
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 88, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 89, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 90, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 91, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 92, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 93, 1)
    commit

      -- Genre based viewing - total viewing duration
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  94, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  95, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  96, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  97, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  98, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey,  99, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 100, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 101, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 102, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 103, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 104, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 105, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 106, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 107, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 108, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 109, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 110, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 111, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 112, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 113, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 114, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 115, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 116, 1)
    commit

      -- Genre based viewing - number of programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 117, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 118, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 119, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 120, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 121, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 122, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 123, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 124, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 125, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 126, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 127, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 128, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 129, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 130, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 131, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 132, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 133, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 134, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 135, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 136, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 137, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 138, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 139, 4)
    commit

      -- Genre based viewing - number of complete programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 140, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 141, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 142, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 143, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 144, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 145, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 146, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 147, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 148, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 149, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 150, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 151, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 152, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 153, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 154, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 155, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 156, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 157, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 158, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 159, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 160, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 161, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 162, 3)
    commit

      -- Genre based viewing - SOV
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 163, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 164, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 165, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 166, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 167, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 168, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 169, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 170, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 171, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 172, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 173, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 174, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 175, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 176, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 177, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 178, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 179, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 180, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 181, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 182, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 183, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 184, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 185, 1)
    commit

      -- Genre based viewing - average daily number of programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 186, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 187, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 188, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 189, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 190, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 191, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 192, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 193, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 194, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 195, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 196, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 197, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 198, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 199, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 200, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 201, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 202, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 203, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 204, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 205, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 206, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 207, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 208, 1)
    commit

      -- Genre based viewing - average daily number of complete programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 209, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 210, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 211, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 212, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 213, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 214, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 215, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 216, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 217, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 218, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 219, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 220, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 221, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 222, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 223, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 224, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 225, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 226, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 227, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 228, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 229, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 230, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 231, 1)
    commit

      -- Channel viewing - total viewing duration
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 232, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 233, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 234, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 235, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 236, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 237, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 238, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 239, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 240, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 241, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 242, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 243, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 244, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 245, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 246, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 247, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 248, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 249, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 250, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 251, 3)
    commit

      -- Channel viewing - number of programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 252, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 253, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 254, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 255, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 256, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 257, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 258, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 259, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 260, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 261, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 262, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 263, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 264, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 265, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 266, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 267, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 268, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 269, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 270, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 271, 3)
    commit

      -- Channel viewing - number of complete programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 272, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 273, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 274, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 275, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 276, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 277, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 278, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 279, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 280, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 281, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 282, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 283, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 284, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 285, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 286, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 287, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 288, 4)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 289, 2)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 290, 3)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Thread_Id) values (@parPeriodKey, 291, 4)
    commit

      -- Channel viewing - SOV
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 292, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 293, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 294, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 295, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 296, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 297, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 298, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 299, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 300, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 301, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 302, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 303, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 304, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 305, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 306, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 307, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 308, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 309, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 310, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 311, 1)
    commit

      -- Channel viewing - average daily number of programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 312, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 313, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 314, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 315, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 316, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 317, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 318, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 319, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 320, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 321, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 322, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 323, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 324, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 325, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 326, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 327, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 328, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 329, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 330, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 331, 1)
    commit

      -- Channel viewing - average daily number of complete programmes
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 332, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 333, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 334, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 335, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 336, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 337, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 338, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 339, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 340, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 341, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 342, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 343, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 344, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 345, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 346, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 347, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 348, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 349, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 350, 1)
    insert into VAggr_Meta_Run_Schedule (Period_Key, Aggregation_Key, Grouping_Run) values (@parPeriodKey, 351, 1)
    commit


      -- ################################################################################

end;





