/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - change application
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
#
###############################################################################*/




  -- ##############################################################################################################
  -- ##### Derived and calculated fields                                                                      #####
  -- ##############################################################################################################
drop procedure CM_07_CM_Change_Application;
create procedure CM_07_CM_Change_Application
    @parGreenLightToRun         smallint = 0,
    @result                     smallint output
as
begin

    declare @varProcCurrentVersion      smallint
    set @varProcCurrentVersion          = 2                         -- Increment on any change to this procedure

    declare @varProcExpectedVersion     smallint
    set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_07_CM_Change_Application')

    if (@parGreenLightToRun <> 1)
      begin
          message '[!!!!!] CM_07_CM_Change_Application: PROCESS IS ALREADY HALTED' type status to client
          select @result = 0
          return 0
      end

    if (@varProcExpectedVersion is null)
      begin
          message '[!!!!!] CM_07_CM_Change_Application: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
          select @result = 0
          return 0
      end

    if (@varProcCurrentVersion <> @varProcExpectedVersion)
      begin
          message '[!!!!!] CM_07_CM_Change_Application: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
          message '[!!!!!] CM_07_CM_Change_Application: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
          select @result = 0
          return 0
      end


    message '[' || now() || '] #############################################################' type status to client
    message '[' || now() || '] ####### Step 7 start: CM change application' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 7 start: CM change application'



      -- ##############################################################################################################
      -- ##### Close active records for amended and terminated channels                                           #####
      -- ##############################################################################################################
    message '[' || now() || '] ~~ Closing active records for amended and terminated channels' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Closing active records for amended and terminated channels'

    update CM_23_Prod_Channel_Map_History base
       set base.ActiveX       = 'N',
           base.Effective_To  = case
                                  when stat.Action = 'Terminated' then stat.New_Effective_To            -- Use calculated effective to
                                  when stat.Action = 'Amended' then stat.New_Effective_From             -- Calculated is '2999' for the new record so end the old record when the new starts
                                end,
           base.Amend_Date    = case
                                  when stat.Action = 'Terminated' then stat.Run_Date
                                    else base.Amend_Date
                                end
      from CM_21_Channel_Statuses stat
     where base.Service_Key = stat.Service_Key
       and base.ActiveX = 'Y'
       and stat.Action in ('Terminated', 'Amended')
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
      -- ##### Add new records for amended and new channels                                                       #####
      -- ##############################################################################################################
    message '[' || now() || '] ~~ Adding new records' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Adding new records'

    insert into CM_23_Prod_Channel_Map_History
          (SERVICE_KEY, FULL_NAME, EPG_NUMBER, EPG_NAME, VESPA_NAME, CHANNEL_NAME, TECHEDGE_NAME, INFOSYS_NAME, BARB_REPORTED, ACTIVEX, CHANNEL_OWNER,
           OLD_PACKAGING, NEW_PACKAGING, PAY_FREE_INDICATOR, CHANNEL_GENRE, CHANNEL_TYPE, FORMAT, PARENT_SERVICE_KEY, TIMESHIFT_STATUS, TIMESHIFT_MINUTES,
           RETAIL, CHANNEL_REACH, HD_SWAP_EPG_NUMBER, SENSITIVE_CHANNEL, SPOT_SOURCE, PROMO_SOURCE, NOTES, EFFECTIVE_FROM, EFFECTIVE_TO, TYPE_ID, UI_DESCR,
           EPG_CHANNEL, AMEND_DATE, CHANNEL_PACK, SERVICE_ATTRIBUTE_VERSION, PRIMARY_SALES_HOUSE, CHANNEL_GROUP, PROVIDER_ID, PAY_SKY_SPORTS_FLAG,
           PAY_SPORTS_FLAG, PAY_TV_FLAG, KEY_PAY_ENTERTAINMENT_FLAG, SKY_SPORTS_NEWS_FLAG, SKY_MOVIES_FLAG, BT_SPORT_FLAG)
      select
            base.SERVICE_KEY,
            base.FULL_NAME,
            base.EPG_NUMBER,
            base.EPG_NAME,
            base.VESPA_NAME,
            base.CHANNEL_NAME,
            base.TECHEDGE_NAME,
            base.INFOSYS_NAME,
            base.BARB_REPORTED,
            base.ACTIVEX,
            base.CHANNEL_OWNER,
            base.OLD_PACKAGING,
            base.NEW_PACKAGING,
            base.PAY_FREE_INDICATOR,
            base.CHANNEL_GENRE,
            base.CHANNEL_TYPE,
            base.FORMAT,
            base.PARENT_SERVICE_KEY,
            base.TIMESHIFT_STATUS,
            base.TIMESHIFT_MINUTES,
            base.RETAIL,
            base.CHANNEL_REACH,
            base.HD_SWAP_EPG_NUMBER,
            base.SENSITIVE_CHANNEL,
            base.SPOT_SOURCE,
            base.PROMO_SOURCE,
            base.NOTES,
            stat.New_Effective_From,
            stat.New_Effective_To,
            base.TYPE_ID,
            base.UI_DESCR,
            base.EPG_CHANNEL,
            stat.Run_Date,
            base.CHANNEL_PACK,
            base.SERVICE_ATTRIBUTE_VERSION,
            base.PRIMARY_SALES_HOUSE,
            base.CHANNEL_GROUP,
            base.PROVIDER_ID,
            base.PAY_SKY_SPORTS_FLAG,
            base.PAY_SPORTS_FLAG,
            base.PAY_TV_FLAG,
            base.KEY_PAY_ENTERTAINMENT_FLAG,
            base.SKY_SPORTS_NEWS_FLAG,
            base.SKY_MOVIES_FLAG,
            base.BT_SPORT_FLAG
        from CM_21_Channel_Statuses stat,
             CM_20_Final_Channel_Mapping_Data base
       where base.Service_Key = stat.Service_Key
         and stat.Action in ('Amended', 'New')
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
    -- ##### Updating version and times for effective to & from dates                                           #####
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Updating CM version and seconds element for effective from/to dates' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Updating CM version and seconds element for effective from/to dates'

      -- Update CM version
    update CM_23_Prod_Channel_Map_History base
      set base.SERVICE_ATTRIBUTE_VERSION = 1 + (select max(SERVICE_ATTRIBUTE_VERSION) as SERVICE_ATTRIBUTE_VERSION from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES)
    commit


      -- Update (reset time) Effective From/To dates
    update CM_23_Prod_Channel_Map_History base
       set base.Effective_From  = dateadd(second, base.SERVICE_ATTRIBUTE_VERSION, cast( date(base.Effective_From) || ' 06:00:00' as datetime)),
           base.Effective_To    = case
                                    when date(base.Effective_To) = '2999-12-31' then cast('2999-12-31 00:00:00' as datetime)
                                      else dateadd(second, base.SERVICE_ATTRIBUTE_VERSION - 1, cast( date(base.Effective_To) || ' 06:00:00' as datetime))
                                  end
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
    message '[' || now() || '] ####### Step 7 completed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 7 completed'
    message '[' || now() || '] #############################################################' type status to client
    message ' ' type status to client


end;
go


  -- ##############################################################################################################
  -- ##############################################################################################################


















