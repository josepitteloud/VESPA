/*###############################################################################
# Created on:   27/06/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Metadata
#                 This procedure prepares metadata tables required in other modules
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


if object_id('SC_Metrics_Rep_Metadata') is not null then drop procedure SC_Metrics_Rep_Metadata end if;
create procedure SC_Metrics_Rep_Metadata
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

      declare @varLastRunDate                 date
      declare @varLatestReportDate            date
      declare @varReportingStartDate          date
      declare @varReportingEndDate            date
      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varProcessIdentifier           varchar(20)         -- Logger - process ID

      set @varLastRunDate = (select max(Scaling_Date)
                                    from vespa_analysts.SC_Metrics_Rep_01_Period_Definitions)

      set @varReportingEndDate = (select max(scaling_date)
                                    from vespa_analysts.SC2_Metrics)
      set @varReportingStartDate = dateadd(day, -364, @varReportingEndDate)

      set @varProcessIdentifier        = 'SCMetRep_Mtd_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Metadata] - process started #######', null
      execute logger_add_event @varBuildId, 3, '>>>>> Step 0.1: Preparing environment <<<<<', null
      execute logger_add_event @varBuildId, 3, 'Process identifier: ' || @varProcessIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Refresh identifier: ' || @parRefreshIdentifier, null
      execute logger_add_event @varBuildId, 3, 'Build ID: ' || @varBuildId, null
      execute logger_add_event @varBuildId, 3, 'User context: ' || @varUsername, null
      execute logger_add_event @varBuildId, 3, 'Period: ' || dateformat(@varReportingStartDate, 'dd/mm/yyyy') || ' - ' || dateformat(@varReportingEndDate, 'dd/mm/yyyy'), null
      execute logger_add_event @varBuildId, 3, 'Last run date: ' || dateformat(@varLastRunDate, 'dd/mm/yyyy'), null



        -- ##############################################################################################################
        -- ##### STEP 1.0 - final data preparation                                                                  #####
        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '>>>>> Step 1.0: Final data preparation <<<<<', null

      truncate table vespa_analysts.SC_Metrics_Rep_01_Period_Definitions


      while @varReportingStartDate <= @varReportingEndDate
        begin

            insert into Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions
                   (Scaling_Date, WeekCommencing_Date, Daily, Weekly, Monthly)
                 values (
                          @varReportingStartDate,
                          case
                            when datepart(weekday, @varReportingStartDate) = 6 then @varReportingStartDate
                            when datepart(weekday, @varReportingStartDate) = 7 then @varReportingStartDate - 1
                              else @varReportingStartDate - (datepart(weekday, @varReportingStartDate) + 1)
                          end,
                          1,
                          case when datepart(weekday, @varReportingStartDate) = 6 then 1 else 0 end,    -- Flag Fridays
                          case when datepart(day, @varReportingStartDate) = 1 then 1 else 0 end         -- Flag first days of each month
                        )
            commit

            set @varReportingStartDate = @varReportingStartDate + 1

        end

      update Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions base
         set base.Sky_Week = det.Subs_Week_Of_Year
        from sk_prod.Sky_Calendar det
       where base.WeekCommencing_Date = det.Calendar_Date
      commit


      execute logger_add_event @varBuildId, 3, 'Period definitions table created', null


        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Scaling Metrics Report [Metadata] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


