/*###############################################################################
# Created on:   12/08/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Capping Metrics Report - Wrapper for complete report run
#
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# All report procedures must exist
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 12/08/2013  SBE   v01 - initial version
#
###############################################################################*/


select max(Event_Date) from Vespa_Analysts.CAP_Metrics_Rep_01_Period_Definitions;     -- Q1
select cast(Max_Date as date)                                                           -- Q2
  from (select max(substr(table_name, 18, 8)) as Max_Date
          from systab
         where lower(table_name) like 'vespa_daily_augs_%') a;


begin

    declare @varBuildId int
    declare @varStartDate date
    declare @varEndDate   date

    set @varStartDate = '2013-07-26'          -- Typically, value should be the Q1 result (+1 day) above
    set @varEndDate   = '2013-08-01'          -- Typically, value should be no greater then the Q2 result above

    execute logger_create_run 'Capping Metrics Rep', 'Regular report run', @varBuildId output
    commit

      -- Data preparation
    execute CAP_Metrics_Rep_Metadata 'Regular run', @varBuildId
    execute CAP_Metrics_Rep_Universe_Preparation @varStartDate, @varEndDate, 'Regular run', @varBuildId

      -- Report summaries
    execute CAP_Metrics_Rep_Overview '', @varBuildId

      -- Clean up environment
    execute CAP_Metrics_Rep_Clean_Up 'Regular run', @varBuildId


    execute logger_get_latest_job_events 'Capping Metrics Rep', 4

end;












