/*###############################################################################
# Created on:   18/10/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - process metadata preparation and maintenance
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
# 20/04/2017  JG    Version with logging table
###############################################################################*/


      -- ##############################################################################################################
      -- ##### Metadata preparation procedure                                                                     #####
      -- ##############################################################################################################
    drop procedure CM_01_Metadata_Prep;
  create procedure CM_01_Metadata_Prep
         @parGreenLightToRun         smallint = 0,
         @parLastCMRunDate           date = null,                        -- Last date CM was run - calculated automatically, this can be used to override automatic value
         @parCurrentCMRunDate        date = null,                        -- Current CM run date - calculated automatically but can be overriden
         @result                     smallint output
      as begin

           declare @varStructCurrentVersion    smallint
           declare @varProcCurrentVersion      smallint
               set @varStructCurrentVersion        = 2                         -- Increment on any change to DATA STRUCTURES
               set @varProcCurrentVersion          = 1                         -- Increment on any change to this procedure

                -- Verify structures version
           declare @varStructExpectedVersion   smallint
               set @varStructExpectedVersion       = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - data structures')

                if (@parGreenLightToRun <> 1) begin
                     message '[!!!!!] Data structures: PROCESS IS ALREADY HALTED' type status to client
                      select @result = 0
                      return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] Data structures: PROCESS IS ALREADY HALTED'
               end

                if (@varStructExpectedVersion is null) begin
                     message '[!!!!!] Data structures: REQUIRED STRUCTURES VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
                      select @result = 0
                      return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] Data structures: REQUIRED STRUCTURES VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
               end

                if (@varStructCurrentVersion <> @varStructExpectedVersion) begin
                     message '[!!!!!] Data structures: TABLES ARE OUTDATED - PROCESS EXECUTION HALTED' type status to client
                     message '[!!!!!] Data structures: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
                      select @result = 0
                      return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] Data structures: TABLES ARE OUTDATED - PROCESS EXECUTION HALTED'
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] Data structures: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
               end

                -- Verify procedure version
           declare @varProcExpectedVersion     smallint
               set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_01_Metadata_Prep')

                if (@parGreenLightToRun <> 1) begin
                     message '[!!!!!] CM_01_Metadata_Prep: PROCESS IS ALREADY HALTED' type status to client
                      select @result = 0
                      return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] CM_01_Metadata_Prep: PROCESS IS ALREADY HALTED'
               end

                if (@varProcExpectedVersion is null) begin
                     message '[!!!!!] CM_01_Metadata_Prep: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
                     select @result = 0
                     return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] CM_01_Metadata_Prep: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
               end

                if (@varProcCurrentVersion <> @varProcExpectedVersion) begin
                     message '[!!!!!] CM_01_Metadata_Prep: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
                     message '[!!!!!] CM_01_Metadata_Prep: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
                     select @result = 0
                     return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] CM_01_Metadata_Prep: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED'
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] CM_01_Metadata_Prep: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
               end

           declare @varLastCMRunDate           date
           declare @varCurrentCMRunDate        date
           declare @varSQL                     text
           declare @varParameterExists         tinyint
           message '[' || now() || '] #############################################################' type status to client
           message '[' || now() || '] ####### Step 1 start: metadata preparation' type status to client
           message '[' || now() || '] #############################################################' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 1 start: metadata preparation'

               set @varLastCMRunDate = case when @parLastCMRunDate is not null and @parLastCMRunDate between today() - 90 and today() then @parLastCMRunDate
                                            else (select max(AMEND_DATE) from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES)
                                        end
               set @varCurrentCMRunDate = case when @parCurrentCMRunDate is not null and @parCurrentCMRunDate between today() - 90 and today() then @parCurrentCMRunDate
                                               else today()
                                           end

              -- ##############################################################################################################
              -- ##############################################################################################################
              -- ##### Set previous run date                                                                              #####
              -- ##############################################################################################################
              -- ##############################################################################################################
           message '[' || now() || '] ~~ Setting PREVIOUS RUN DATE to "' || @varLastCMRunDate || '"' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Setting PREVIOUS RUN DATE to "' || @varLastCMRunDate || '"'

               set @varParameterExists   = (select count(*) from CM_00_Process_Metadata where CM_Parameter_Name = 'Previous run date')
                if @varParameterExists = 0 begin
                      insert into CM_00_Process_Metadata
                            (CM_Parameter_Name
                            ,CM_Param_Value__Date)
                      values('Previous run date'
                            ,@varLastCMRunDate
                            )
                    commit
               end
              else begin
                      update CM_00_Process_Metadata
                         set CM_Param_Value__Date = @varLastCMRunDate
                       where CM_Parameter_Name = 'Previous run date'
                      commit
               end

                -- ##############################################################################################################
                -- ##############################################################################################################
                -- ##### Set current run date                                                                               #####
                -- ##############################################################################################################
                -- ##############################################################################################################
           message '[' || now() || '] ~~ Setting CURRENT RUN DATE to "' || @varCurrentCMRunDate || '"' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Setting CURRENT RUN DATE to "' || @varCurrentCMRunDate || '"'
                     
               set @varParameterExists   = (select count(*) from CM_00_Process_Metadata where CM_Parameter_Name = 'Current run date')
                if @varParameterExists = 0 begin
                      insert into CM_00_Process_Metadata
                            (CM_Parameter_Name
                            ,CM_Param_Value__Date
                            )
                      values('Current run date'
                            ,@varCurrentCMRunDate
                            )
                      commit
               end
              else begin
                      update CM_00_Process_Metadata
                         set CM_Param_Value__Date = @varCurrentCMRunDate
                       where CM_Parameter_Name = 'Current run date'
                      commit
               end
           message ' ' type status to client

                -- ##############################################################################################################
                -- ##############################################################################################################
                -- ##### Create a list of non-maintained channels                                                           #####
                -- ##############################################################################################################
                -- ##############################################################################################################
           message '[' || now() || '] ~~ Creating a list of non-maintained channels' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Creating a list of non-maintained channels'
                  
            delete from CM_00_Process_Metadata
             where CM_Parameter_Name = 'Non-maintained channel'
            commit

               set @varSQL = '
                           load table CM_00_Process_Metadata (
                               CM_Parameter_Name,
                               CM_Param_Value__Num,
                                  Notes''\n''
                              )
                              FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/_Non-maintained_SKs.csv''
                              SKIP 1
                              QUOTES ON
                              ESCAPES OFF
                              DELIMITED BY '',''
                            '
           execute ( @varSQL )
            commit
           message '[' || now() || '] Done (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Done (' || @@rowcount || ' rows)'
           message ' ' type status to client

                -- ##############################################################################################################
                -- ##############################################################################################################
                -- ##### Create a snapshot of the current CM data                                                           #####
                -- ##############################################################################################################
                -- ##############################################################################################################
           message '[' || now() || '] ~~ Taking a snapshot of the current PROD table' type status to client
          truncate table CM_23_Prod_Channel_Map_History

            insert into CM_23_Prod_Channel_Map_History
                  (SERVICE_KEY, FULL_NAME, EPG_NUMBER, EPG_NAME, VESPA_NAME, CHANNEL_NAME, TECHEDGE_NAME, INFOSYS_NAME, BARB_REPORTED,
                         ACTIVEX, CHANNEL_OWNER, OLD_PACKAGING, NEW_PACKAGING, PAY_FREE_INDICATOR, CHANNEL_GENRE, CHANNEL_TYPE, FORMAT, PARENT_SERVICE_KEY,
                         TIMESHIFT_STATUS, TIMESHIFT_MINUTES, RETAIL, CHANNEL_REACH, HD_SWAP_EPG_NUMBER, SENSITIVE_CHANNEL, SPOT_SOURCE, PROMO_SOURCE, NOTES,
                         EFFECTIVE_FROM, EFFECTIVE_TO, TYPE_ID, UI_DESCR, EPG_CHANNEL, AMEND_DATE, CHANNEL_PACK, SERVICE_ATTRIBUTE_VERSION, PRIMARY_SALES_HOUSE,
                         CHANNEL_GROUP, PROVIDER_ID, PAY_SKY_SPORTS_FLAG, PAY_SPORTS_FLAG, PAY_TV_FLAG, KEY_PAY_ENTERTAINMENT_FLAG, SKY_SPORTS_NEWS_FLAG,
                         SKY_MOVIES_FLAG, BT_SPORT_FLAG)
            select
          /*
                      SERVICE_KEY,
                      case when FULL_NAME                   is null then 'Unknown' else FULL_NAME end,                                  -- [!!!]
                      case when EPG_NUMBER                  is null then -1        else EPG_NUMBER end,
                      case when EPG_NAME                    is null then 'Unknown' else EPG_NAME end,
                      case when VESPA_NAME                  is null then 'Unknown' else VESPA_NAME end,
                      case when CHANNEL_NAME                is null then 'Unknown' else CHANNEL_NAME end,
                      case when TECHEDGE_NAME               is null then 'Unknown' else TECHEDGE_NAME end,
                      case when INFOSYS_NAME                is null then ''        else INFOSYS_NAME end,
                      case when BARB_REPORTED               is null then 'NO'      else BARB_REPORTED end,
                      ACTIVEX,
                      case when CHANNEL_OWNER               is null then ''        else CHANNEL_OWNER end,
                      case when OLD_PACKAGING               is null then ''        else OLD_PACKAGING end,
                      case when NEW_PACKAGING               is null then ''        else NEW_PACKAGING end,
                      case when PAY_FREE_INDICATOR          is null then ''        else PAY_FREE_INDICATOR end,
                      case when CHANNEL_GENRE               is null then ''        else CHANNEL_GENRE end,
                      case when CHANNEL_TYPE                is null then ''        else CHANNEL_TYPE end,
                      case when FORMAT                      is null then ''        else FORMAT end,
                      case when PARENT_SERVICE_KEY          is null then -1        else PARENT_SERVICE_KEY end,
                      case when TIMESHIFT_STATUS            is null then ''        else TIMESHIFT_STATUS end,
                      case when TIMESHIFT_MINUTES           is null then 0         else TIMESHIFT_MINUTES end,
                      case when RETAIL                      is null then ''        else RETAIL end,
                      case when CHANNEL_REACH               is null then ''        else CHANNEL_REACH end,
                      case when HD_SWAP_EPG_NUMBER          is null then -1        else HD_SWAP_EPG_NUMBER end,
                      case when SENSITIVE_CHANNEL           is null then 0         else SENSITIVE_CHANNEL end,
                      case when SPOT_SOURCE                 is null then 'None'    else SPOT_SOURCE end,
                      case when PROMO_SOURCE                is null then 'None'    else PROMO_SOURCE end,
                      case when NOTES                       is null then ''        else NOTES end,
                      EFFECTIVE_FROM,
                      EFFECTIVE_TO,
                      case when TYPE_ID                     is null then -1        else TYPE_ID end,
                      case when UI_DESCR                    is null then ''        else UI_DESCR end,
                      case when EPG_CHANNEL                 is null then ''        else EPG_CHANNEL end,
                      Case when AMEND_DATE                  is null then date(EFFECTIVE_FROM) else AMEND_DATE end,
                      case when CHANNEL_PACK                is null then ''        else CHANNEL_PACK end,
                      SERVICE_ATTRIBUTE_VERSION,
                      case when PRIMARY_SALES_HOUSE         is null then ''        else PRIMARY_SALES_HOUSE end,
                      case when CHANNEL_GROUP               is null then ''        else CHANNEL_GROUP end,
                      case when PROVIDER_ID                 is null then ''        else PROVIDER_ID end,
                      case when PAY_SKY_SPORTS_FLAG         is null then 'No'      else PAY_SKY_SPORTS_FLAG end,
                      case when PAY_SPORTS_FLAG             is null then 'No'      else PAY_SPORTS_FLAG end,
                      case when PAY_TV_FLAG                 is null then 'No'      else PAY_TV_FLAG end,
                      case when KEY_PAY_ENTERTAINMENT_FLAG  is null then 'No'      else KEY_PAY_ENTERTAINMENT_FLAG end,
                      case when SKY_SPORTS_NEWS_FLAG        is null then 'No'      else SKY_SPORTS_NEWS_FLAG end,
                      case when SKY_MOVIES_FLAG             is null then 'No'      else SKY_MOVIES_FLAG end,
                      case when BT_SPORT_FLAG               is null then 'No'      else BT_SPORT_FLAG end
          */

                      SERVICE_KEY,
                      FULL_NAME,
                      EPG_NUMBER,
                      EPG_NAME,
                      VESPA_NAME,
                      CHANNEL_NAME,
                      TECHEDGE_NAME,
                      INFOSYS_NAME,
                      BARB_REPORTED,
                      ACTIVEX,
                      CHANNEL_OWNER,
                      OLD_PACKAGING,
                      NEW_PACKAGING,
                      PAY_FREE_INDICATOR,
                      CHANNEL_GENRE,
                      CHANNEL_TYPE,
                      FORMAT,
                      PARENT_SERVICE_KEY,
                      TIMESHIFT_STATUS,
                      TIMESHIFT_MINUTES,
                      RETAIL,
                      CHANNEL_REACH,
                      HD_SWAP_EPG_NUMBER,
                      SENSITIVE_CHANNEL,
                      SPOT_SOURCE,
                      PROMO_SOURCE,
                      NOTES,
                      EFFECTIVE_FROM,
                      EFFECTIVE_TO,
                      TYPE_ID,
                      UI_DESCR,
                      EPG_CHANNEL,
                      AMEND_DATE,
                      CHANNEL_PACK,
                      SERVICE_ATTRIBUTE_VERSION,
                      PRIMARY_SALES_HOUSE,
                      CHANNEL_GROUP,
                      PROVIDER_ID,
                      PAY_SKY_SPORTS_FLAG,
                      PAY_SPORTS_FLAG,
                      PAY_TV_FLAG,
                      KEY_PAY_ENTERTAINMENT_FLAG,
                      SKY_SPORTS_NEWS_FLAG,
                      SKY_MOVIES_FLAG,
                      BT_SPORT_FLAG
              from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
                  -- from pme06.Channel_map_prod_service_key_attributes_new
            commit

           message '[' || now() || '] Done (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Done (' || @@rowcount || ' rows)'
           message ' ' type status to client


              -- ##############################################################################################################
           message '[' || now() || '] ####### Step 1 completed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 1 completed'
           message ' ' type status to client

            return 1

     end;
      go

-- select * from CM_00_Process_Metadata;


  -- ##############################################################################################################
  -- ##############################################################################################################


























