/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - assembly
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
  -- ##### CM assembly procedure                                                                              #####
  -- ##############################################################################################################
    drop procedure CM_04_CM_Assembly;
  create procedure CM_04_CM_Assembly
    @parGreenLightToRun         smallint = 0,
    @result                     smallint output
      as begin

           declare @varProcCurrentVersion      smallint
               set @varProcCurrentVersion          = 1                         -- Increment on any change to this procedure
           declare @varProcExpectedVersion     smallint
               set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_04_CM_Assembly')

                if (@parGreenLightToRun <> 1) begin
                     message '[!!!!!] CM_04_CM_Assembly: PROCESS IS ALREADY HALTED' type status to client
                      select @result = 0
                      return 0
                      insert into CM_24_Run_Log
                            (run_date
                            ,msg
                            )
                      select today()
                            ,'[!!!!!] CM_04_CM_Assembly: PROCESS IS ALREADY HALTED'
               end

                if (@varProcExpectedVersion is null) begin
                     message '[!!!!!] CM_04_CM_Assembly: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
                     select @result = 0
                     return 0
                     insert into CM_24_Run_Log
                           (run_date
                           ,msg
                           )
                     select today()
                           ,'[!!!!!] CM_04_CM_Assembly: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
               end

                if (@varProcCurrentVersion <> @varProcExpectedVersion) begin
                     message '[!!!!!] CM_04_CM_Assembly: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
                     message '[!!!!!] CM_04_CM_Assembly: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
                     select @result = 0
                     return 0
                     insert into CM_24_Run_Log
                           (run_date
                           ,msg
                           )
                     select today()
                           ,'[!!!!!] CM_04_CM_Assembly: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED'
                     insert into CM_24_Run_Log
                           (run_date
                           ,msg
                           )
                     select today()
                           ,'[!!!!!] CM_04_CM_Assembly: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
              end

          message '[' || now() || '] #############################################################' type status to client
          message '[' || now() || '] ####### Step 4 start: CM assembly' type status to client
           insert into CM_24_Run_Log
                 (run_date
                 ,msg
                 )
           select today()
                 ,'[' || now() || '] ####### Step 4 start: CM assembly'

                -- ##############################################################################################################
                -- ##############################################################################################################
                -- ##### SERVICE INTEGRATION                                                                                #####
                -- ##############################################################################################################
                -- ##############################################################################################################
           message '[' || now() || '] ~~ Assembling existing CM data & SERVICE INTEGRATION feed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Assembling existing CM data & SERVICE INTEGRATION feed'
          truncate table CM_20_Final_Channel_Mapping_Data


                -- Get existing data for non-maintained channels (i.e. VOD)
            insert into CM_20_Final_Channel_Mapping_Data
                  (SERVICE_KEY, FULL_NAME, EPG_NUMBER, EPG_NAME, VESPA_NAME, CHANNEL_NAME, TECHEDGE_NAME, INFOSYS_NAME, BARB_REPORTED, ACTIVEX, CHANNEL_OWNER,
                   OLD_PACKAGING, NEW_PACKAGING, PAY_FREE_INDICATOR, CHANNEL_GENRE, CHANNEL_TYPE, FORMAT, PARENT_SERVICE_KEY, TIMESHIFT_STATUS, TIMESHIFT_MINUTES,
                   RETAIL, CHANNEL_REACH, HD_SWAP_EPG_NUMBER, SENSITIVE_CHANNEL, SPOT_SOURCE, PROMO_SOURCE, NOTES, EFFECTIVE_FROM, EFFECTIVE_TO, TYPE_ID, UI_DESCR,
                   EPG_CHANNEL, AMEND_DATE, CHANNEL_PACK, SERVICE_ATTRIBUTE_VERSION, PRIMARY_SALES_HOUSE, CHANNEL_GROUP, PROVIDER_ID, PAY_SKY_SPORTS_FLAG,
                   PAY_SPORTS_FLAG, PAY_TV_FLAG, KEY_PAY_ENTERTAINMENT_FLAG, SKY_SPORTS_NEWS_FLAG, SKY_MOVIES_FLAG, BT_SPORT_FLAG)
            select base.SERVICE_KEY,
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
                   base.EFFECTIVE_FROM,
                   base.EFFECTIVE_TO,
                   base.TYPE_ID,
                   base.UI_DESCR,
                   base.EPG_CHANNEL,
                   base.AMEND_DATE,
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
              from CM_23_Prod_Channel_Map_History base
             where (
                    base.SERVICE_KEY < 1000 or                                                                            -- Pull VOD (large range of numbers)
                    base.SERVICE_KEY in (select CM_Param_Value__Num from CM_00_Process_Metadata where CM_Parameter_Name = 'Non-maintained channel')
                   )
               and base.ActiveX = 'Y'
          order by base.SERVICE_KEY
            commit
           message '[' || now() || '] Done - non-maintained CM records (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Done - non-maintained CM records (' || @@rowcount || ' rows)'

               -- Get base data from Service Integration (SI)
           insert into CM_20_Final_Channel_Mapping_Data
                 (SERVICE_KEY, FULL_NAME, EPG_NUMBER, EPG_NAME, VESPA_NAME, CHANNEL_NAME, TECHEDGE_NAME, INFOSYS_NAME, BARB_REPORTED, ACTIVEX, CHANNEL_OWNER,
                  OLD_PACKAGING, NEW_PACKAGING, PAY_FREE_INDICATOR, CHANNEL_GENRE, CHANNEL_TYPE, FORMAT, PARENT_SERVICE_KEY, TIMESHIFT_STATUS, TIMESHIFT_MINUTES,
                  RETAIL, CHANNEL_REACH, HD_SWAP_EPG_NUMBER, SENSITIVE_CHANNEL, SPOT_SOURCE, PROMO_SOURCE, NOTES, EFFECTIVE_FROM, EFFECTIVE_TO, TYPE_ID, UI_DESCR,
                  EPG_CHANNEL, AMEND_DATE, CHANNEL_PACK, SERVICE_ATTRIBUTE_VERSION, PRIMARY_SALES_HOUSE, CHANNEL_GROUP, PROVIDER_ID, PAY_SKY_SPORTS_FLAG,
                  PAY_SPORTS_FLAG, PAY_TV_FLAG, KEY_PAY_ENTERTAINMENT_FLAG, SKY_SPORTS_NEWS_FLAG, SKY_MOVIES_FLAG, BT_SPORT_FLAG,
                  xCHANNEL_GROUP_ID, xCountry, xChannel_Flags, xSI_Match_Flag, xSK_Automated_Flag)
           select base.SI_SERVICE_KEY,
                  max(base.UI_DESCR),
                  max(case
                        when base.SI_SERVICE_KEY in (2005, 2017, 2076) then base.xMin_EPG_Number                          -- exception for some BBC channels
                        when base.SI_SERVICE_KEY in (2075) then base.xMax_EPG_Number                                      -- exception for BBC 2 HD channel
                        when base.xSK_Record_Count = 1 then base.SELECT_NUM                                               -- single-record SKs - select EPG number directly
                        when base.xSK_Record_Count = 2 and base.xFormat = 'SD' then base.xMin_EPG_Number                  -- pair for SK - pick lower for SD version
                        when base.xSK_Record_Count = 2 and base.xFormat = 'HD' then base.xMax_EPG_Number                  -- pair for SK - pick higher for HD version
                          else -1
                      end),
                  max(base.NAME),
                  max(base.NAME),
                  max(base.UI_DESCR),
                  'Unknown',
                  '',
                  'NO',
                  '?',
                  '',
                  '',
                  '',
                  '',
                  max(base.GENRE),
                  '',
                  max(base.xFORMAT),
                  -1,
                  '',
                  max(base.xTimeshift_Minutes),
                  '',
                  '',
                  -1,
                  0,
                  'None',
                  'None',
                  '',
                  '1980-01-01',
                  '1980-01-01',
                  -1,
                  '',
                  '',
                  '1980-01-01',
                  '',
                  0,
                  '',
                  '',
                  '',
                  'No',
                  'No',
                  'No',
                  'No',
                  'No',
                  'No',
                  'No',
                  max(base.CHANNEL_GROUP_ID),
                  max(base.xCountry),
                  max(base.xChannel_Flags),
                  1,
                  1
              from CM_01_Service_Integration_Feed base left join CM_20_Final_Channel_Mapping_Data nm on base.SI_SERVICE_KEY = nm.SERVICE_KEY
             where nm.SERVICE_KEY is null                                                                                 -- Only records not already loaded
          group by SI_SERVICE_KEY
            commit
           message '[' || now() || '] Done - SERVICE INTEGRATION feed (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Done - SERVICE INTEGRATION feed (' || @@rowcount || ' rows)'
           message ' ' type status to client



                -- ##############################################################################################################
                -- ##############################################################################################################
                -- ##### CONDITIONAL ACCESS                                                                                 #####
                -- ##############################################################################################################
                -- ##############################################################################################################
           message '[' || now() || '] ~~ Appending CONDITIONAL ACCESS data' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Appending CONDITIONAL ACCESS data'
            update CM_20_Final_Channel_Mapping_Data base
               set base.CHANNEL_TYPE            = case
                                                    -- when base.SERVICE_KEY = 1850 then ''
                                                    when base.FORMAT = 'Radio' then 'NR - FTA - Radio'
                                                      else ca.CA
                                                  end,
                   base.xCA_Match_Flag          = 1
              from CM_02_Conditional_Access_Feed ca
             where base.Service_Key = ca.SI_SERVICE_KEY
               and base.xSK_Automated_Flag = 1
            commit

                -- Radio channels are not in CONDITIONAL ACCESS but channel type is based on the FORMAT field (derived from SI)
            update CM_20_Final_Channel_Mapping_Data base
               set base.CHANNEL_TYPE            = case
                                                    when base.FORMAT = 'Radio' then 'NR - FTA - Radio'
                                                      else base.CHANNEL_TYPE
                                                  end
             where base.xSK_Automated_Flag = 1
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
    -- ##### VESPA_PROGRAMME_SCHEDULE                                                                           #####
    -- ##############################################################################################################
    -- ##############################################################################################################
           message '[' || now() || '] ~~ Appending PROGRAMME SCHEDULE data' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Appending PROGRAMME SCHEDULE data'
            update CM_20_Final_Channel_Mapping_Data base
               set base.xBSS_Code            = ps.BSS_Code
              from CM_03_VESPA_Programme_Schedule_Feed ps
             where base.Service_Key = ps.Service_Key
               and ps.BSS_Code_Sequence = 1
               and base.xSK_Automated_Flag = 1 -- Value for the last known record used
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
                -- ##### BARB                                                                                               #####
                -- ##############################################################################################################
                -- ##############################################################################################################
           message '[' || now() || '] ~~ Appending BARB data' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Appending BARB data'
            update CM_20_Final_Channel_Mapping_Data base
               set base.xBARB_Match_Flag        = 1,
                   base.BARB_REPORTED           = 'YES',                                                                    -- Match found, link active, channel deemed as "reported"
                   base.CHANNEL_OWNER           = case
                                                    when barb.Broadcast_Group_Name is null then ''
                                                      else barb.Broadcast_Group_Name
                                                  end,
                   base.xSales_House_1          = barb.Sales_House_1,
                   base.xSales_House_2          = barb.Sales_House_2,
                   base.xSales_House_3          = barb.Sales_House_3,
                   base.xSales_House_4          = barb.Sales_House_4,
                   base.xSales_House_5          = barb.Sales_House_5,
                   base.xSales_House_6          = barb.Sales_House_6,
                   base.xBroadcast_Group_Id     = barb.Broadcast_Group_Id,
                   base.xBroadcast_Group_Name   = barb.Broadcast_Group_Name
              from (select a.Service_Key,
                           max(b.Sales_House_1)        as Sales_House_1,
                           max(b.Sales_House_2)        as Sales_House_2,
                           max(b.Sales_House_3)        as Sales_House_3,
                           max(b.Sales_House_4)        as Sales_House_4,
                           max(b.Sales_House_5)        as Sales_House_5,
                           max(b.Sales_House_6)        as Sales_House_6,
                           max(b.Broadcast_Group_Id)   as Broadcast_Group_Id,
                           max(b.Broadcast_Group_Name) as Broadcast_Group_Name,
                           max(b.Sales_House_Name)     as Sales_House_Name
                      from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB a,
                           CM_05_BARB_Feed b
                     where a.Log_Station_Code = b.Log_Station_Code
                       and a.Effective_From <= now()
                       and a.Effective_To >= now()
                       and a.Service_Key is not null
                  group by a.Service_Key) barb
             where base.Service_Key = barb.Service_Key
               and base.xSK_Automated_Flag = 1
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
                 -- ##### Landmark                                                                                           #####
                 -- ##############################################################################################################
                 -- ##############################################################################################################
            message '[' || now() || '] ~~ Appending LANDMARK data' type status to client
             insert into CM_24_Run_Log
                   (run_date
                   ,msg
                   )
             select today()
                   ,'[' || now() || '] ~~ Appending LANDMARK data'
             update CM_20_Final_Channel_Mapping_Data base
                set base.xLandmark_Match_Flag    = case
                                                     when land.Service_Key is not null then 1
                                                       else 0
                                                   end
               from (select
                           lk.Service_Key
                       from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK lk,
                            CM_06_Landmark_Feed fd
                      where lk.SARE_NO = fd.MEDIA_SALES_AREA_NUMBER
                        and lk.Effective_From <= now()
                        and lk.Effective_To >= now()
                        and lk.Service_Key is not null
                        and fd.Effective_From <= now()
                        and fd.Effective_To >= now()) land
              where base.Service_Key = land.Service_Key
                and base.xSK_Automated_Flag = 1
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
    message '[' || now() || '] ####### Step 4 completed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 4 completed'
    message '[' || now() || '] #############################################################' type status to client
    message ' ' type status to client


end;
go


  -- ##############################################################################################################
  -- ##############################################################################################################















