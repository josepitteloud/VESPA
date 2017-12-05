/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - dependency maintenance
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 25/07/2016  SBE   Initial version
# 18/04/2017  JG    Added logging table
###############################################################################*/

select * from CM_24_Run_Log
      -- ##############################################################################################################
      -- ##### Dependency maintenance procedure                                                                   #####
      -- ##############################################################################################################
    drop procedure CM_02_Dependency_Maintenance;
  create procedure CM_02_Dependency_Maintenance
         @parGreenLightToRun         smallint = 0
        ,@result                     smallint output
      as begin

                   declare @varProcCurrentVersion      smallint
                       set @varProcCurrentVersion          = 1                         -- Increment on any change to this procedure

                   declare @varProcExpectedVersion     smallint
                       set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_02_Dependency_Maintenance')

                        if (@parGreenLightToRun <> 1) begin
                                     message '[!!!!!] CM_02_Dependency_Maintenance: PROCESS IS ALREADY HALTED' type status to client
                                      select @result = 0
                                      return 0
                                      insert into CM_24_Run_Log
                                            (run_date
                                            ,msg
                                            )
                                      select today()
                                            ,'[!!!!!] CM_02_Dependency_Maintenance: PROCESS IS ALREADY HALTED'
                       end

                        if (@varProcExpectedVersion is null) begin
                                     message '[!!!!!] CM_02_Dependency_Maintenance: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
                                      select @result = 0
                                      return 0
                                      insert into CM_24_Run_Log
                                            (run_date
                                            ,msg
                                            )
                                      select today()
                                            ,'[!!!!!] CM_02_Dependency_Maintenance: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
                       end

                        if (@varProcCurrentVersion <> @varProcExpectedVersion) begin
                                     message '[!!!!!] CM_02_Dependency_Maintenance: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
                                      insert into CM_24_Run_Log
                                            (run_date
                                            ,msg
                                            )
                                      select today()
                                            ,'[!!!!!] CM_02_Dependency_Maintenance: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED'
                                     message '[!!!!!] CM_02_Dependency_Maintenance: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
                                      select @result = 0
                                      return 0
                                      insert into CM_24_Run_Log
                                            (run_date
                                            ,msg
                                            )
                                      select today()
                                            ,'[!!!!!] CM_02_Dependency_Maintenance: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
                       end

                   message '[' || now() || '] #############################################################' type status to client
                   message '[' || now() || '] ####### Step 2 start: dependency maintenance' type status to client



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### BARB - VESPA lookup maintenance                                                                    #####
    -- ##############################################################################################################
    -- ##############################################################################################################



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### LANDMARK - VESPA lookup maintenance                                                                #####
    -- ##############################################################################################################
    -- ##############################################################################################################



    -- ##############################################################################################################
                   message '[' || now() || '] ####### Step 2 completed' type status to client
                    insert into CM_24_Run_Log
                          (run_date
                          ,msg
                          )
                    select today()
                          ,'[' || now() || '] ####### Step 2 completed'
                   message '[' || now() || '] #############################################################' type status to client
                   message ' ' type status to client

     end;
      go



  -- ##############################################################################################################
  -- ##############################################################################################################





























