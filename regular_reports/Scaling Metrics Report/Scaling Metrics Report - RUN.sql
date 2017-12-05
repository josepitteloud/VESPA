/*###############################################################################
# Created on:   16/07/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Scaling Metrics Report - Wrapper for complete report run
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
# 16/07/2013  SBE   v01 - initial version
#
###############################################################################*/



select max(Scaling_Date) from Vespa_Analysts.SC_Metrics_Rep_01_Period_Definitions;      -- Q1
select max(Scaling_Date) from Vespa_Analysts.SC2_Metrics;                               -- Q2


begin

    declare @varBuildId int
    declare @varStartDate date
    declare @varEndDate   date

    set @varStartDate = '2013-07-26'          -- Typically, value should be the Q1 result (+1 day) above
    set @varEndDate   = '2013-08-01'          -- Typically, value should be no greater then the Q2 result above

    execute logger_create_run 'Scaling Metrics Rep', 'Regular report run', @varBuildId output
    commit

      -- Data preparation
    execute SC_Metrics_Rep_Metadata 'Regular run', @varBuildId
    execute SC_Metrics_Rep_Universe_Preparation @varStartDate, @varEndDate, 'Regular run', @varBuildId

      -- Report summaries
    execute SC_Metrics_Rep_Overview 'Regular run', @varBuildId
    execute SC_Metrics_Rep_Variables 'Regular run', @varBuildId
    execute SC_Metrics_Rep_Coverage_By_Cat @varStartDate, @varEndDate, 'Regular run', @varBuildId
    execute SC_Metrics_Rep_Traffic_Lights @varStartDate, @varEndDate, 'Regular run', @varBuildId

      -- Clean up environment
    execute SC_Metrics_Rep_Clean_Up 'Regular run', @varBuildId


    execute logger_get_latest_job_events 'Scaling Metrics Rep', 4

end;












