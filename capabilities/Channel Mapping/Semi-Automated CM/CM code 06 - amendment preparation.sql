/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - amendment preparation
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
    drop procedure CM_06_CM_Amendment_Preparation;
  create procedure CM_06_CM_Amendment_Preparation
    @parGreenLightToRun         smallint = 0,
    @parRunDate                 date,
    @result                     smallint output
      as begin

           declare @varProcCurrentVersion      smallint
           set @varProcCurrentVersion          = 1                         -- Increment on any change to this procedure

           declare @varProcExpectedVersion     smallint
           set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_06_CM_Amendment_Preparation')

           if (@parGreenLightToRun <> 1)
             begin
                 message '[!!!!!] CM_06_CM_Amendment_Preparation: PROCESS IS ALREADY HALTED' type status to client
                 select @result = 0
                 return 0
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[!!!!!] CM_06_CM_Amendment_Preparation: PROCESS IS ALREADY HALTED'
             end

           if (@varProcExpectedVersion is null)
             begin
                 message '[!!!!!] CM_06_CM_Amendment_Preparation: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
                 select @result = 0
                 return 0
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[!!!!!] CM_06_CM_Amendment_Preparation: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
             end

           if (@varProcCurrentVersion <> @varProcExpectedVersion)
             begin
                 message '[!!!!!] CM_06_CM_Amendment_Preparation: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[!!!!!] CM_06_CM_Amendment_Preparation: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED'
                 message '[!!!!!] CM_06_CM_Amendment_Preparation: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
                 select @result = 0
                 return 0
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[!!!!!] CM_06_CM_Amendment_Preparation: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
             end

           declare @varSQL   text

           message '[' || now() || '] #############################################################' type status to client
           message '[' || now() || '] ####### Step 6 start: CM amendment preparation' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ####### Step 6 start: CM amendment preparation'


           -- ##############################################################################################################
           -- ##### Create list of all known service keys (CM & SI)                                                    #####
           -- ##############################################################################################################
           message '[' || now() || '] ~~ Get list of all known service keys' type status to client

                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ~~ Get list of all known service keys'

           truncate table CM_21_Channel_Statuses

           insert into CM_21_Channel_Statuses
                 (Run_Date
                 ,Service_Key
                 )
           select distinct @parRunDate
                 ,Service_Key
             from CM_20_Final_Channel_Mapping_Data
            union
           select @parRunDate
                 ,Service_Key
             from CM_23_Prod_Channel_Map_History
            where ActiveX = 'Y'
         order by 1
           commit

           update CM_21_Channel_Statuses stat
              set stat.SK_Automated_Flag = base.xSK_Automated_Flag                                         -- Initially (previous step) all channels are flagged with "1" - this update applies 0s to records which are not maintained
             from CM_20_Final_Channel_Mapping_Data base
            where stat.Service_Key = base.Service_Key
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
           -- ##### Get current CM information                                                                         #####
           -- ##############################################################################################################
           message '[' || now() || '] ~~ Append current CM information' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ~~ Append current CM information'

             -- Active channels
           update CM_21_Channel_Statuses base
              set base.Current_CM_Flag         = 1,
                  base.Current_Effective_From  = sk.Effective_From,
                  base.Current_Effective_To    = sk.Effective_To
             from CM_23_Prod_Channel_Map_History sk
            where base.Service_Key = sk.Service_key
              and sk.ActiveX = 'Y'
           commit

             -- Effective_To for previously terminated channels
           update CM_21_Channel_Statuses base
              set base.Current_Effective_To    = sk.Effective_To
             from (select
                         Service_Key,
                         max(Effective_To) as Effective_To
                     from CM_23_Prod_Channel_Map_History
                    where ActiveX <> 'Y'
                    group by Service_Key) sk
            where base.Service_Key = sk.Service_key
              and base.Current_CM_Flag = 0
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
           -- ##### Get new CM information                                                                             #####
           -- ##############################################################################################################
           message '[' || now() || '] ~~ ' type status to client

           update CM_21_Channel_Statuses base
              set base.New_CM_Flag             = 1
             from CM_20_Final_Channel_Mapping_Data sk
            where base.Service_Key = sk.Service_key
              and sk.xSK_Automated_Flag = 1
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
           -- ##### Work out channel status                                                                            #####
           -- ##############################################################################################################
           message '[' || now() || '] ~~ Calculate channel status' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ~~ Calculate channel status'

             -- NEW AND TERMINATED CHANNELS
           update CM_21_Channel_Statuses base
              set base.Action              = case
                                               when base.Current_CM_Flag = 0 and base.New_CM_Flag = 1 then 'New'
                                               when base.Current_CM_Flag = 1 and base.New_CM_Flag = 0 then 'Terminated'
                                                 else base.Action
                                             end
            where base.SK_Automated_Flag = 1
           commit
           message '[' || now() || '] Done (' || @@rowcount || ' rows)' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] Done (' || @@rowcount || ' rows)'
           message ' ' type status to client


             -- FLAG CHANNELS WITH CHANGES
             -- Comment out fields NOT MAINTAINED automatically
           update CM_21_Channel_Statuses base
              set base.Action  = case
                                   when chg.Num_Changes > 0 then 'Amended'
                                     else 'No change'
                                  end
             from (select
                         newcm.Service_Key,
                         sum(
                             case when newcm.FULL_NAME                    = curr.FULL_NAME                    then 0 else 1 end +
                             case when newcm.EPG_NUMBER                   = curr.EPG_NUMBER                   then 0 else 1 end +
                             case when newcm.EPG_NAME                     = curr.EPG_NAME                     then 0 else 1 end +
                             case when newcm.VESPA_NAME                   = curr.VESPA_NAME                   then 0 else 1 end +
                             case when newcm.CHANNEL_NAME                 = curr.CHANNEL_NAME                 then 0 else 1 end +
                             case when newcm.TECHEDGE_NAME                = curr.TECHEDGE_NAME                then 0 else 1 end +
                             case when newcm.INFOSYS_NAME                 = curr.INFOSYS_NAME                 then 0 else 1 end +
                             case when newcm.BARB_REPORTED                = curr.BARB_REPORTED                then 0 else 1 end +
                             -- case when newcm.ACTIVEX                      = curr.ACTIVEX                      then 0 else 1 end +                             -- CM METADATA
                             case when newcm.CHANNEL_OWNER                = curr.CHANNEL_OWNER                then 0 else 1 end +
                             case when newcm.OLD_PACKAGING                = curr.OLD_PACKAGING                then 0 else 1 end +
                             case when newcm.NEW_PACKAGING                = curr.NEW_PACKAGING                then 0 else 1 end +
                             case when newcm.PAY_FREE_INDICATOR           = curr.PAY_FREE_INDICATOR           then 0 else 1 end +
                             case when newcm.CHANNEL_GENRE                = curr.CHANNEL_GENRE                then 0 else 1 end +
                             case when newcm.CHANNEL_TYPE                 = curr.CHANNEL_TYPE                 then 0 else 1 end +
                             case when newcm.FORMAT                       = curr.FORMAT                       then 0 else 1 end +
                             case when newcm.PARENT_SERVICE_KEY           = curr.PARENT_SERVICE_KEY           then 0 else 1 end +
                             case when newcm.TIMESHIFT_STATUS             = curr.TIMESHIFT_STATUS             then 0 else 1 end +
                             case when newcm.TIMESHIFT_MINUTES            = curr.TIMESHIFT_MINUTES            then 0 else 1 end +
                             case when newcm.RETAIL                       = curr.RETAIL                       then 0 else 1 end +
                             case when newcm.CHANNEL_REACH                = curr.CHANNEL_REACH                then 0 else 1 end +
                             case when newcm.HD_SWAP_EPG_NUMBER           = curr.HD_SWAP_EPG_NUMBER           then 0 else 1 end +
                             case when newcm.SENSITIVE_CHANNEL            = curr.SENSITIVE_CHANNEL            then 0 else 1 end +
                             case when newcm.SPOT_SOURCE                  = curr.SPOT_SOURCE                  then 0 else 1 end +
                             case when newcm.PROMO_SOURCE                 = curr.PROMO_SOURCE                 then 0 else 1 end +
                             case when newcm.NOTES                        = curr.NOTES                        then 0 else 1 end +
                             -- case when newcm.EFFECTIVE_FROM               = curr.EFFECTIVE_FROM               then 0 else 1 end +                             -- CM METADATA
                             -- case when newcm.EFFECTIVE_TO                 = curr.EFFECTIVE_TO                 then 0 else 1 end +                             -- CM METADATA
                             case when newcm.TYPE_ID                      = curr.TYPE_ID                      then 0 else 1 end +
                             case when newcm.UI_DESCR                     = curr.UI_DESCR                     then 0 else 1 end +
                             case when newcm.EPG_CHANNEL                  = curr.EPG_CHANNEL                  then 0 else 1 end +
                             -- case when newcm.AMEND_DATE                   = curr.AMEND_DATE                   then 0 else 1 end +                             -- CM METADATA
                             case when newcm.CHANNEL_PACK                 = curr.CHANNEL_PACK                 then 0 else 1 end +
                             -- case when newcm.SERVICE_ATTRIBUTE_VERSION    = curr.SERVICE_ATTRIBUTE_VERSION    then 0 else 1 end +                             -- CM METADATA
                             case when newcm.PRIMARY_SALES_HOUSE          = curr.PRIMARY_SALES_HOUSE          then 0 else 1 end +
                             case when newcm.CHANNEL_GROUP                = curr.CHANNEL_GROUP                then 0 else 1 end +
                             case when newcm.PROVIDER_ID                  = curr.PROVIDER_ID                  then 0 else 1 end +
                             case when newcm.PAY_SKY_SPORTS_FLAG          = curr.PAY_SKY_SPORTS_FLAG          then 0 else 1 end +
                             case when newcm.PAY_SPORTS_FLAG              = curr.PAY_SPORTS_FLAG              then 0 else 1 end +
                             case when newcm.PAY_TV_FLAG                  = curr.PAY_TV_FLAG                  then 0 else 1 end +
                             case when newcm.KEY_PAY_ENTERTAINMENT_FLAG   = curr.KEY_PAY_ENTERTAINMENT_FLAG   then 0 else 1 end +
                             case when newcm.SKY_SPORTS_NEWS_FLAG         = curr.SKY_SPORTS_NEWS_FLAG         then 0 else 1 end +
                             case when newcm.SKY_MOVIES_FLAG              = curr.SKY_MOVIES_FLAG              then 0 else 1 end +
                             case when newcm.BT_SPORT_FLAG                = curr.BT_SPORT_FLAG                then 0 else 1 end
                            ) as Num_Changes
                     from CM_20_Final_Channel_Mapping_Data newcm,
                          CM_23_Prod_Channel_Map_History curr
                    where newcm.Service_Key = curr.Service_Key
                      and curr.ActiveX = 'Y'
                      and newcm.xSK_Automated_Flag = 1
                    group by newcm.Service_Key) chg
            where base.Service_Key = chg.Service_Key
              and base.Action not in ('New', 'Terminated')
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
           -- ##### Calculate correct Effective From / To dates for New and Terminated channels                        #####
           -- ##############################################################################################################
           message '[' || now() || '] ~~ Calculating effective from/to dates' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ~~ Calculating effective from/to dates'

           update CM_21_Channel_Statuses base
              set base.New_Effective_From    = case
                                                 when base.Action = 'Not maintained' then null
                                                 when base.Action = 'No change' then null
                                                 when base.Action = 'Terminated' then null
                                                 when base.Action = 'New' then (@parRunDate - 1)                            -- Channels are launched "from yesterday" AM to allow them to be active the full day
                                                 when base.Action = 'Amended' then (@parRunDate - 1)                        -- Changes are effective from "yesterday AM" to allow them to have changes eefective for the entire day
                                                   else null
                                               end,
                  base.New_Effective_To      = case
                                                 when base.Action = 'Not maintained' then null
                                                 when base.Action = 'No change' then null
                                                 when base.Action = 'Terminated' then @parRunDate                           -- Channels are teminated "this AM" so they are left active the full day yesterday
                                                 when base.Action = 'New' then '2999-12-31'
                                                 when base.Action = 'Amended' then '2999-12-31'
                                                   else null
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
           -- ##### Generate change summary                                                                            #####
           -- ##############################################################################################################
           message '[' || now() || '] ~~ Generating change summary' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ~~ Generating change summary'

           message '[' || now() || '] ####### Listing terminated channels #######' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] ####### Listing terminated channels #######'
           insert into CM_22_Change_Details
                 (Run_Date, Service_key, Channel_Name, Action, Field, Current_Value, New_Value, Effective_To_Date)
             select
                   @parRunDate,
                   stat.Service_Key,
                   curr.Channel_Name,
                   stat.Action,
                   '',
                   '',
                   '',
                   stat.New_Effective_To
               from CM_21_Channel_Statuses stat,
                    CM_23_Prod_Channel_Map_History curr
              where stat.Service_Key = curr.Service_Key
                and curr.ActiveX = 'Y'
                and stat.Action = 'Terminated'
           commit
           message '[' || now() || '] Completed - ' || @@rowcount || ' channels listed' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,'[' || now() || '] Completed - ' || @@rowcount || ' channels listed'


           set @varSQL = '
                           message ''['' || now() || ''] ####### Summarising changes for ##^1^## #######'' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,''['' || now() || ''] ####### Summarising changes for ##^1^## #######''

                           message ''['' || now() || ''] ~~ New channels'' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,''['' || now() || ''] ~~ New channels''
                           insert into CM_22_Change_Details
                                 (Run_Date, Service_key, Channel_Name, Action, Field, Current_Value, New_Value, Effective_From_Date, Effective_To_Date, Field_Position)
                             select
                            ''' || @parRunDate || ''',
                                   stat.Service_Key,
                                   chg.Channel_Name,
                                   stat.Action,
                                   replace(''##^1^##'', ''_'', '' ''),
                                   chg.Curr_Value,
                                   chg.New_Value,
                                   stat.New_Effective_From,
                                   stat.New_Effective_To,
                                   ##^2^##
                               from CM_21_Channel_Statuses stat,
                                    (select
                                           newcm.Service_Key,
                                           newcm.Full_Name as Channel_Name,
                                           ''(n/a)'' as Curr_Value,
                                           cast(newcm.##^1^## as varchar(255)) as New_Value
                                       from CM_20_Final_Channel_Mapping_Data newcm
                                      where newcm.xSK_Automated_Flag = 1) chg
                              where stat.Service_Key = chg.Service_Key
                                and stat.Action in (''New'')
                           commit
                           message ''['' || now() || ''] Done ('' || @@rowcount || '' channels affected)'' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,''['' || now() || ''] Done ('' || @@rowcount || '' channels affected)''

                           message ''['' || now() || ''] ~~ Amended channels'' type status to client
                   insert into CM_24_Run_Log
                         (run_date
                         ,msg
                         )
                   select today()
                         ,''['' || now() || ''] ~~ Amended channels'' 
                           insert into CM_22_Change_Details
                                 (Run_Date, Service_key, Channel_Name, Action, Field, Current_Value, New_Value, Effective_From_Date, Effective_To_Date, Field_Position)
                             select
                            ''' || @parRunDate || ''',
                                   stat.Service_Key,
                                   chg.Channel_Name,
                                   stat.Action,
                                   replace(''##^1^##'', ''_'', '' ''),
                                   chg.Curr_Value,
                                   chg.New_Value,
                                   stat.New_Effective_From,
                                   stat.New_Effective_To,
                                   ##^2^##
                               from CM_21_Channel_Statuses stat,
                                    (select
                                           coalesce(curr.Service_Key, newcm.Service_Key) as Service_Key,
                                           coalesce(curr.Full_Name, newcm.Full_Name) as Channel_Name,
                                           coalesce(cast(curr.##^1^## as varchar(255)), '''') as Curr_Value,
                                           coalesce(cast(newcm.##^1^## as varchar(255)), '''') as New_Value
                                       from (select * from CM_23_Prod_Channel_Map_History where ActiveX = ''Y'') curr full join CM_20_Final_Channel_Mapping_Data newcm
                                         on curr.Service_Key = newcm.Service_Key
                                      where (curr.Service_Key is null or curr.ActiveX = ''Y'')
                                        and (newcm.Service_Key is null or newcm.xSK_Automated_Flag = 1)) chg
                              where stat.Service_Key = chg.Service_Key
                                and chg.Curr_Value <> chg.New_Value
                                and stat.Action in (''Amended'')
                           commit
                           message ''['' || now() || ''] Done ('' || @@rowcount || '' channels affected)'' type status to client

                         '

           -- Comment out fields NOT MAINTAINED automatically
           execute( replace( replace(@varSQL, '##^2^##', '10'), '##^1^##', 'FULL_NAME'))
           execute( replace( replace(@varSQL, '##^2^##', '20'), '##^1^##', 'EPG_NUMBER'))
           execute( replace( replace(@varSQL, '##^2^##', '30'), '##^1^##', 'EPG_NAME'))
           execute( replace( replace(@varSQL, '##^2^##', '40'), '##^1^##', 'VESPA_NAME'))
           execute( replace( replace(@varSQL, '##^2^##', '50'), '##^1^##', 'CHANNEL_NAME'))
           execute( replace( replace(@varSQL, '##^2^##', '60'), '##^1^##', 'TECHEDGE_NAME'))
           execute( replace( replace(@varSQL, '##^2^##', '70'), '##^1^##', 'INFOSYS_NAME'))
           execute( replace( replace(@varSQL, '##^2^##', '80'), '##^1^##', 'BARB_REPORTED'))
           -- execute( replace( replace(@varSQL, '##^2^##', '80'), '##^1^##', 'ACTIVEX'))                                                                    -- CM METADATA
           execute( replace( replace(@varSQL, '##^2^##', '90'), '##^1^##', 'CHANNEL_OWNER'))
           execute( replace( replace(@varSQL, '##^2^##', '100'), '##^1^##', 'OLD_PACKAGING'))
           execute( replace( replace(@varSQL, '##^2^##', '110'), '##^1^##', 'NEW_PACKAGING'))
           execute( replace( replace(@varSQL, '##^2^##', '120'), '##^1^##', 'PAY_FREE_INDICATOR'))
           execute( replace( replace(@varSQL, '##^2^##', '130'), '##^1^##', 'CHANNEL_GENRE'))
           execute( replace( replace(@varSQL, '##^2^##', '140'), '##^1^##', 'CHANNEL_TYPE'))
           execute( replace( replace(@varSQL, '##^2^##', '150'), '##^1^##', 'FORMAT'))
           execute( replace( replace(@varSQL, '##^2^##', '160'), '##^1^##', 'PARENT_SERVICE_KEY'))
           execute( replace( replace(@varSQL, '##^2^##', '170'), '##^1^##', 'TIMESHIFT_STATUS'))
           execute( replace( replace(@varSQL, '##^2^##', '180'), '##^1^##', 'TIMESHIFT_MINUTES'))
           execute( replace( replace(@varSQL, '##^2^##', '190'), '##^1^##', 'RETAIL'))
           execute( replace( replace(@varSQL, '##^2^##', '200'), '##^1^##', 'CHANNEL_REACH'))
           execute( replace( replace(@varSQL, '##^2^##', '210'), '##^1^##', 'HD_SWAP_EPG_NUMBER'))
           execute( replace( replace(@varSQL, '##^2^##', '220'), '##^1^##', 'SENSITIVE_CHANNEL'))
           execute( replace( replace(@varSQL, '##^2^##', '230'), '##^1^##', 'SPOT_SOURCE'))
           execute( replace( replace(@varSQL, '##^2^##', '240'), '##^1^##', 'PROMO_SOURCE'))
           execute( replace( replace(@varSQL, '##^2^##', '250'), '##^1^##', 'NOTES'))
           -- execute( replace( replace(@varSQL, '##^2^##', '250'), '##^1^##', 'EFFECTIVE_FROM'))                                                            -- CM METADATA
           -- execute( replace( replace(@varSQL, '##^2^##', '250'), '##^1^##', 'EFFECTIVE_TO'))                                                              -- CM METADATA
           execute( replace( replace(@varSQL, '##^2^##', '260'), '##^1^##', 'TYPE_ID'))
           execute( replace( replace(@varSQL, '##^2^##', '270'), '##^1^##', 'UI_DESCR'))
           execute( replace( replace(@varSQL, '##^2^##', '280'), '##^1^##', 'EPG_CHANNEL'))
           -- execute( replace( replace(@varSQL, '##^2^##', '290'), '##^1^##', 'AMEND_DATE'))
           execute( replace( replace(@varSQL, '##^2^##', '290'), '##^1^##', 'CHANNEL_PACK'))
           -- execute( replace( replace(@varSQL, '##^2^##', '290'), '##^1^##', 'SERVICE_ATTRIBUTE_VERSION'))                                                 -- CM METADATA
           execute( replace( replace(@varSQL, '##^2^##', '300'), '##^1^##', 'PRIMARY_SALES_HOUSE'))
           execute( replace( replace(@varSQL, '##^2^##', '310'), '##^1^##', 'CHANNEL_GROUP'))
           execute( replace( replace(@varSQL, '##^2^##', '320'), '##^1^##', 'PROVIDER_ID'))
           execute( replace( replace(@varSQL, '##^2^##', '330'), '##^1^##', 'PAY_SKY_SPORTS_FLAG'))
           execute( replace( replace(@varSQL, '##^2^##', '340'), '##^1^##', 'PAY_SPORTS_FLAG'))
           execute( replace( replace(@varSQL, '##^2^##', '350'), '##^1^##', 'PAY_TV_FLAG'))
           execute( replace( replace(@varSQL, '##^2^##', '360'), '##^1^##', 'KEY_PAY_ENTERTAINMENT_FLAG'))
           execute( replace( replace(@varSQL, '##^2^##', '370'), '##^1^##', 'SKY_SPORTS_NEWS_FLAG'))
           execute( replace( replace(@varSQL, '##^2^##', '380'), '##^1^##', 'SKY_MOVIES_FLAG'))
           execute( replace( replace(@varSQL, '##^2^##', '390'), '##^1^##', 'BT_SPORT_FLAG') )

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


















