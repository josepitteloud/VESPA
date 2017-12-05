/*###############################################################################
# Created on:   20/10/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - publishing results
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 20/10/2016  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Publishing results                                                                                 #####
  -- ##############################################################################################################
drop procedure CM_08_CM_Publish_Results;
create procedure CM_08_CM_Publish_Results
    @parGreenLightToRun         smallint = 0
as
begin

    declare @varProcCurrentVersion      smallint
    set @varProcCurrentVersion          = 3                         -- Increment on any change to this procedure

    declare @varProcExpectedVersion     smallint
    set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_08_CM_Publish_Results')

    if (@parGreenLightToRun <> 1)
      begin
          message '[!!!!!] CM_08_CM_Publish_Results: CAN''T SEE THE GREEN LIGHT TO PROCEED - EXECUTION HALTED' type status to client
          return 0
      end

    if (@varProcExpectedVersion is null)
      begin
          message '[!!!!!] CM_08_CM_Publish_Results: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - EXECUTION HALTED' type status to client
          return 0
      end

    if (@varProcCurrentVersion <> @varProcExpectedVersion)
      begin
          message '[!!!!!] CM_08_CM_Publish_Results: PROCEDURE IS OUTDATED - EXECUTION HALTED' type status to client
          message '[!!!!!] CM_08_CM_Publish_Results: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
          return 0
      end


    message '[' || now() || '] #############################################################' type status to client
    message '[' || now() || '] ####### Step 8 start: CM publishing results' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 8 start: CM publishing results'



    -- ##############################################################################################################
    -- ##### Backing up current version                                                                         #####
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Backing up current PROD version' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Backing up current PROD version'

    truncate table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES__AUTO_BACKUP

    insert into vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES__AUTO_BACKUP
              (SERVICE_KEY, FULL_NAME, EPG_NUMBER, EPG_NAME, VESPA_NAME, CHANNEL_NAME, TECHEDGE_NAME, INFOSYS_NAME, BARB_REPORTED,
               ACTIVEX, CHANNEL_OWNER, OLD_PACKAGING, NEW_PACKAGING, PAY_FREE_INDICATOR, CHANNEL_GENRE, CHANNEL_TYPE, FORMAT, PARENT_SERVICE_KEY,
               TIMESHIFT_STATUS, TIMESHIFT_MINUTES, RETAIL, CHANNEL_REACH, HD_SWAP_EPG_NUMBER, SENSITIVE_CHANNEL, SPOT_SOURCE, PROMO_SOURCE, NOTES,
               EFFECTIVE_FROM, EFFECTIVE_TO, TYPE_ID, UI_DESCR, EPG_CHANNEL, AMEND_DATE, CHANNEL_PACK, SERVICE_ATTRIBUTE_VERSION, PRIMARY_SALES_HOUSE,
               CHANNEL_GROUP, PROVIDER_ID, PAY_SKY_SPORTS_FLAG, PAY_SPORTS_FLAG, PAY_TV_FLAG, KEY_PAY_ENTERTAINMENT_FLAG, SKY_SPORTS_NEWS_FLAG,
               SKY_MOVIES_FLAG, BT_SPORT_FLAG)
      select
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
       order by SERVICE_KEY, date(EFFECTIVE_FROM), date(EFFECTIVE_TO)
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
    -- ##### Publish to VA schema                                                                               #####
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Publishing to VESPA_ANALYSTS' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Publishing to VESPA_ANALYSTS'

    truncate table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES

    insert into vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
              (SERVICE_KEY, FULL_NAME, EPG_NUMBER, EPG_NAME, VESPA_NAME, CHANNEL_NAME, TECHEDGE_NAME, INFOSYS_NAME, BARB_REPORTED,
               ACTIVEX, CHANNEL_OWNER, OLD_PACKAGING, NEW_PACKAGING, PAY_FREE_INDICATOR, CHANNEL_GENRE, CHANNEL_TYPE, FORMAT, PARENT_SERVICE_KEY,
               TIMESHIFT_STATUS, TIMESHIFT_MINUTES, RETAIL, CHANNEL_REACH, HD_SWAP_EPG_NUMBER, SENSITIVE_CHANNEL, SPOT_SOURCE, PROMO_SOURCE, NOTES,
               EFFECTIVE_FROM, EFFECTIVE_TO, TYPE_ID, UI_DESCR, EPG_CHANNEL, AMEND_DATE, CHANNEL_PACK, SERVICE_ATTRIBUTE_VERSION, PRIMARY_SALES_HOUSE,
               CHANNEL_GROUP, PROVIDER_ID, PAY_SKY_SPORTS_FLAG, PAY_SPORTS_FLAG, PAY_TV_FLAG, KEY_PAY_ENTERTAINMENT_FLAG, SKY_SPORTS_NEWS_FLAG,
               SKY_MOVIES_FLAG, BT_SPORT_FLAG)
      select
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
        from CM_23_Prod_Channel_Map_History
       order by SERVICE_KEY, date(EFFECTIVE_FROM), date(EFFECTIVE_TO)
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
    message '[' || now() || '] ####### Step 6 completed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 6 completed'
    message '[' || now() || '] #############################################################' type status to client
    message ' ' type status to client


end;
go



  -- ##############################################################################################################
  -- ##############################################################################################################


















