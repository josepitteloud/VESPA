/*###############################################################################
# Created on:   06/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Capping Metrics Report - Metadata
#                 This procedure prepares metadata tables required in other modules
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - final data preparation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 06/08/2013  SBE   v01 - initial version
#
###############################################################################*/


if object_id('CAP_Metrics_Rep_Metadata') is not null then drop procedure CAP_Metrics_Rep_Metadata end if;
create procedure CAP_Metrics_Rep_Metadata
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

      set @varLastRunDate = (select cast(Max_Date as date)
                               from (select max(substr(table_name, 18, 8)) as Max_Date
                                       from systab
                                      where lower(table_name) like 'vespa_daily_augs_%') a)

      set @varReportingEndDate = @varLastRunDate
      set @varReportingStartDate = dateadd(day, -364, @varReportingEndDate)

      set @varProcessIdentifier        = 'CAPMetRep_Mtd_v01'

      if (@parBuildId is not null)
          set @varBuildId = @parBuildId


        -- ###############################################################################
        -- ##### Create logger event                                                 #####
        -- ###############################################################################
      if (@parBuildId is null)
          execute logger_create_run @varProcessIdentifier, @parRefreshIdentifier, @varBuildId output

      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Metadata] - process started #######', null
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

      truncate table vespa_analysts.CAP_Metrics_Rep_01_Period_Definitions


      while @varReportingStartDate <= @varReportingEndDate
        begin

            insert into Vespa_Analysts.CAP_Metrics_Rep_01_Period_Definitions
                   (Event_Date, WeekCommencing_Date, Daily, Weekly, Monthly)
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

      update Vespa_Analysts.CAP_Metrics_Rep_01_Period_Definitions base
         set base.Sky_Week = det.Subs_Week_Of_Year
        from sk_prod.Sky_Calendar det
       where base.WeekCommencing_Date = det.Calendar_Date
      commit


      execute logger_add_event @varBuildId, 3, 'Period definitions table created', null


        -- ##############################################################################################################
      execute logger_add_event @varBuildId, 3, '####### Capping Metrics Report [Metadata] - process completed #######', null
      execute logger_add_event @varBuildId, 3, ' ', null


end;


