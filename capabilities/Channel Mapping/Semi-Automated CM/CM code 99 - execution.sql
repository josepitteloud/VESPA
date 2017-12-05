/*###############################################################################
# Created on:   18/10/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - process execution
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/10/2016  SBE   Initial version
# 20/04/2017  JG
#
###############################################################################*/


   begin
/*
create variable @varLastCMRunDate    date;
create variable @varCurrentCMRunDate date     = today();
create variable @varGreenLightToRun  smallint = 1;
*/
           declare @varLastCMRunDate    date     --=
           declare @varCurrentCMRunDate date     = today()
           declare @varGreenLightToRun  smallint = 1

            select @varLastCMRunDate = max(AMEND_DATE) + 1 from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
          truncate table CM_22_Change_Details

           execute CM_01_Metadata_Prep             @varGreenLightToRun, @varLastCMRunDate, @varCurrentCMRunDate, @varGreenLightToRun output
           execute CM_02_Dependency_Maintenance    @varGreenLightToRun, @varGreenLightToRun output

             while @varLastCMRunDate < @varCurrentCMRunDate begin
                         set @varLastCMRunDate = @varLastCMRunDate + 1 -- Increment by 1 at the start since last run date is the same as
                                                                       -- current date for the previous run hence the overlap of 1 day exists

                     execute CM_03_Feed_Load                 @varGreenLightToRun, @varLastCMRunDate, @varCurrentCMRunDate, @varGreenLightToRun output
                     execute CM_04_CM_Assembly               @varGreenLightToRun, @varGreenLightToRun output
                     execute CM_05_CM_Derivations            @varGreenLightToRun, @varGreenLightToRun output
                     execute CM_06_CM_Amendment_Preparation  @varGreenLightToRun, @varLastCMRunDate, @varGreenLightToRun output
                     execute CM_07_CM_Change_Application     @varGreenLightToRun, @varGreenLightToRun output

                          if (@varGreenLightToRun <> 1) return 0                -- Exit the loop if process is already halted

                          -- Update "last run date" parameter to hold last run date value
                      update CM_00_Process_Metadata
                         set CM_Param_Value__Date = @varLastCMRunDate
                       where CM_Parameter_Name = 'Previous run date'
                      commit

               end
     end;
      go


select Run_Date, Service_key, Channel_Name, Action, Field, Current_Value, New_Value, Effective_From_Date, Effective_To_Date
  from CM_22_Change_Details
 order by Action, Field, Service_key, Run_Date, Field_Position;

















