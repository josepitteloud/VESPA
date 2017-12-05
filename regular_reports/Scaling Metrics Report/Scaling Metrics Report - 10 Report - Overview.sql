/*###############################################################################
# Created on:   27/06/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Report output: Overview
#                 This procedure prepares output for the "Overview" page
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - final data preparation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - Vespa_Analysts.SC2_Metrics
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 27/06/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('SC_Metrics_Rep_Overview') is not null then drop procedure SC_Metrics_Rep_Overview end if;
create procedure SC_Metrics_Rep_Overview
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

      declare @varReportingStartDate          date
      declare @varReportingEndDate            date
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID


      set @varReportingEndDate = (select max(scaling_date)
                                    from vespa_analysts.SC2_Metrics)
      set @varReportingStartDate = dateadd(day, -364, @varReportingEndDate)

      if @varReportingStartDate < '2012-08-01'          -- Phase2 data only
          set @varReportingStartDate = '2012-08-01'

      set @varProcessIdentifier        = 'SCMetRep_Ovrvw_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Overview] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'User context: ' || @varUsername, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varReportingStartDate, 'dd/mm/yyyy') || ' - ' || dateformat(@varReportingEndDate, 'dd/mm/yyyy'), null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - final data preparation                                                                  #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Final data preparation <<<<<', null

      insert into vespa_analysts.SC_Metrics_Rep_02_Overview
             (Scaling_Date, Vespa_Panel, Sky_Base, Population_Coverage, Minimum_Weight,
              Maximum_Weight, Average_Weight, Sum_Of_Convergence, Iterations)
        select
              scaling_date,
              vespa_panel,
              sky_base,
              sum_of_weights,
              min_weight,
              max_weight,
              av_weight,
              sum_of_convergence,
              iterations
          from vespa_analysts.SC2_Metrics
         where scaling_date >= @varReportingStartDate
           and scaling_date not in (select
                                          scaling_date
                                      from vespa_analysts.SC_Metrics_Rep_02_Overview)
         order by scaling_date
      commit

      execute logger_add_event @varBuildId, 3, 'New records inserted', @@rowcount


        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Overview] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


