/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - feed loading
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
  -- ##### Feed load procedure                                                                                #####
  -- ##############################################################################################################
    drop procedure CM_03_Feed_Load;
create procedure CM_03_Feed_Load
    @parGreenLightToRun         smallint = 0,
    @parSIFeedDate              date = null,                        -- Reference date for SI feeds
    @parAnyFeedDate             date = null,                        -- Reference date for all other feeds
    @result                     smallint output
as
begin

    declare @varProcCurrentVersion      smallint
    set @varProcCurrentVersion          = 3                         -- Increment on any change to this procedure

    declare @varProcExpectedVersion     smallint
    set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_03_Feed_Load')

    if (@parGreenLightToRun <> 1)
      begin
          message '[!!!!!] CM_03_Feed_Load: PROCESS IS ALREADY HALTED' type status to client
          select @result = 0
          return 0
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_03_Feed_Load: PROCESS IS ALREADY HALTED'
      end

    if (@varProcExpectedVersion is null)
      begin
          message '[!!!!!] CM_03_Feed_Load: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
          select @result = 0
          return 0
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_03_Feed_Load: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
      end

    if (@varProcCurrentVersion <> @varProcExpectedVersion)
      begin
          message '[!!!!!] CM_03_Feed_Load: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_03_Feed_Load: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED'
          message '[!!!!!] CM_03_Feed_Load: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
          select @result = 0
          return 0
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_03_Feed_Load: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
      end


    declare @varFeedDate          date
    set @varFeedDate              = '2016-10-17'

    declare @varSQL               text

    message '[' || now() || '] #############################################################' type status to client
    message '[' || now() || '] ####### Step 3 start: feed load' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 3 start: feed load'



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### SERVICE INTEGRATION                                                                                #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Processing SERVICE INTEGRATION feed: "CM_feed_' || @parSIFeedDate || '_SI.csv"' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Processing SERVICE INTEGRATION feed: "CM_feed_' || @parSIFeedDate || '_SI.csv"'
    truncate table CM_01_Service_Integration_Feed

    set @varSQL = '
                    load table CM_01_Service_Integration_Feed (
                        SELECT_NUM,
                        NAME,
                        UI_DESCR,
                        SI_SERVICE_KEY,
                        SI_SERVICE_ID,
                        CHANNEL_ID,
                        CHANNEL_GROUP_ID,
                        CHANNEL_UI_DESCR,
                        SI_TYPE,
                        GENRE,
                        TRANSPORT_ID,
                        NETWORK_ID,
                        NOTES''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_SI.csv''
                    SKIP 1
                    QUOTES ON
                    ESCAPES OFF
                    DELIMITED BY '',''
                  '
    execute( replace(@varSQL, '##^1^##', @parSIFeedDate) )
    commit
    message '[' || now() || '] Raw data loaded (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Raw data loaded (' || @@rowcount || ' rows)'


      -- Calculate custom fields
    update CM_01_Service_Integration_Feed base
       set base.NAME                = trim(base.NAME),                                                                                  -- Remove leading and trailing blanks from names for all SKs
           base.UI_DESCR            = trim(base.UI_DESCR),                                                                              -- Remove leading and trailing blanks from names for all SKs

           base.xFormat             = case
                                        when lower(trim(base.SI_TYPE)) = 'hd time shifted' then 'HD'
                                        when trim(base.SI_TYPE) = 'HD TV' then 'HD'
                                        when trim(base.SI_TYPE) = 'TV' then 'SD'
                                          else base.SI_TYPE
                                      end,
           base.xCountry            = case
                                        when lower(trim(base.UI_DESCR)) like 'roi %' or lower(trim(base.UI_DESCR)) like '% roi' or
                                             lower(trim(base.UI_DESCR)) like '% roi %' or lower(trim(base.UI_DESCR)) like '%(%for%roi)' then 'ROI'
                                        when lower(trim(base.UI_DESCR)) = 'comedycentralxroi' then 'ROI'                                -- Exception
                                        when lower(trim(base.UI_DESCR)) = 'e! ireland' then 'ROI'                                       -- Exception
                                          else 'UK'
                                      end,
           base.xTimeshift_Minutes  = case
                                        when lower(trim(replace(base.NAME, ' ', ''))) like 'davejavu' then 60                           -- Exception
                                        when lower(trim(base.UI_DESCR)) like 'tiny pop +1' then 60                                      -- Exception
                                        when lower(trim(base.NAME)) like '%+24%' or lower(trim(base.NAME)) like '%+24' then 1440
                                        when lower(trim(base.NAME)) like '%+2%' or lower(trim(base.NAME)) like '%+2' then 120
                                        when lower(trim(base.NAME)) like '%+1%' or lower(trim(base.NAME)) like '%+1' or lower(trim(base.NAME)) like '%+' then 60
                                          else 0
                                      end,
           base.xChannel_Flags      = case                                                                                              -- Commercial channel
                                        when lower(base.UI_DESCR) like '% comm%' then 'X'
                                          else '.'
                                      end
                                      ||
                                      case                                                                                              -- Pub channel
                                        when lower(base.UI_DESCR) like '% pub%' then 'X'
                                          else '.'
                                      end
                                      ||

                                      '--------'                                                                                        -- Unused
    commit


      -- Calculate record count and minimum/maximum EPG number for each service key
    update CM_01_Service_Integration_Feed base
       set base.xSK_Record_Count  = der.xSK_Record_Count,
           base.xMin_EPG_Number   = der.xMin_EPG_Number,
           base.xMax_EPG_Number   = der.xMax_EPG_Number
      from (select
                  SI_SERVICE_KEY,
                  count(*) as xSK_Record_Count,
                  min(SELECT_NUM) as xMin_EPG_Number,
                  max(SELECT_NUM) as xMax_EPG_Number
              from CM_01_Service_Integration_Feed
             group by SI_SERVICE_KEY) der
     where base.SI_SERVICE_KEY = der.SI_SERVICE_KEY
    commit
    message '[' || now() || '] Custom fields updated (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Custom fields updated (' || @@rowcount || ' rows)'


      -- Delete channels flagged as non-maintained if they appear in the SI feed
    delete from CM_00_Process_Metadata
     where CM_Parameter_Name = 'Non-maintained channel'
       and CM_Param_Value__Num in (select SI_SERVICE_KEY from CM_01_Service_Integration_Feed)
    commit
    message '[' || now() || '] Non-maintained channels flagged as automatic (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Non-maintained channels flagged as automatic (' || @@rowcount || ' rows)'
    message ' ' type status to client



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### CONDITIONAL ACCESS                                                                                 #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Processing CONDITIONAL ACCESS feed: "CM_feed_' || @parAnyFeedDate || '_CA.csv"' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Processing CONDITIONAL ACCESS feed: "CM_feed_' || @parAnyFeedDate || '_CA.csv"'
    truncate table CM_02_Conditional_Access_Feed


    if object_id('CM_02_Conditional_Access_Feed_load') is not null
    begin 
        drop table CM_02_Conditional_Access_Feed_load 
    end 
    
    
    CREATE TABLE CM_02_Conditional_Access_Feed_load (	
        uniqid bigint identity,
	EPG smallint, 
	Name1 varchar(255), 
	UI_DESCR varchar(255), 
	SI_SERVICE_KEY bigint, 
	Source_Channel bigint, 
	SI_SERVICE_ID bigint, 
	TRANSPORT_ID bigint, 
	Type1 varchar(255), 
	GENRE varchar(255), 
	Retail varchar(255), 
	CA varchar(255), 
	Legacy_CA varchar(255), 
	Regional varchar(255), 
	Regional_Blackout varchar(255), 
	Template varchar(255), 
	Param1 varchar(255), 
	Param2 varchar(255), 
	Param3 varchar(255), 
	Param4 varchar(255), 
	Param5 varchar(255), 
	Param6 varchar(255), 
	Param7 varchar(255), 
	Param8 varchar(255), 
	Param9 varchar(255), 
	Param10 varchar(255), 
	Param11 varchar(255), 
	Param12 varchar(255), 
	Pairing varchar(255), 
	Taping varchar(255), 
	Status varchar(255), 
	CCI_Value bigint, 
	Launch_Date varchar(255), 
	Comments varchar(2048), 
	Commercial_Pack varchar(255), 
	Item_Type varchar(255), 
	Path varchar(2048),
	score bigint NULL,
	CA_inferred varchar(255), 
	rank bigint NULL
    )
    commit
 
    --declare  @varSQL  text
    --declare  @parAnyFeedDate date
    --    set @parAnyFeedDate = today()-2
  
    set @varSQL = '
                    load table CM_02_Conditional_Access_Feed_load (
                        EPG,
                        Name1,
                        --UI_DESCR,
                        SI_SERVICE_KEY,
                        Source_Channel,
                        SI_SERVICE_ID,
                        TRANSPORT_ID,
                        Type1,
                        GENRE,
                        Retail,
                        CA,
                        Legacy_CA,
                        Regional,
                        Regional_Blackout,
                        Template,
                        Param1,
                        Param2,
                        Param3,
                        Param4,
                        Param5,
                        Param6,
                        Param7,
                        Param8,
                        Param9,
                        Param10,
                        Param11,
                        Param12,
                        Pairing,
                        Taping,
                        Status,
                        CCI_Value,
                        Launch_Date,
                        Comments,
                        Commercial_Pack,
                        Item_Type,
                        Path''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_CA.csv''
                    SKIP 1
                    QUOTES OFF
                    ESCAPES OFF
                    DELIMITED BY '',''
                  '
    execute( replace(@varSQL, '##^1^##', @parAnyFeedDate) )
    commit
                    

    UPDATE CM_02_Conditional_Access_Feed_load
       SET score = 
            --case when lower(coalesce(EPG, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Name1, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(UI_DESCR, 'tbc')) = 'tbc' then 1 else 0 end +
	    --case when lower(coalesce(SI_SERVICE_KEY, 'tbc')) = 'tbc' then 1 else 0 end +
	    --case when lower(coalesce(Source_Channel, 'tbc')) = 'tbc' then 1 else 0 end +
	    --case when lower(coalesce(SI_SERVICE_ID, 'tbc')) = 'tbc' then 1 else 0 end +
	    --case when lower(coalesce(TRANSPORT_ID, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Type1, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(GENRE, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Retail, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(CA, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Legacy_CA, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Regional, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Regional_Blackout, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Template, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param1, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param2, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param3, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param4, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param5, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param6, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param7, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param8, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param9, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param10, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param11, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Param12, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Pairing, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Taping, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Status, 'tbc')) = 'tbc' then 1 else 0 end +
	    --case when lower(coalesce(CCI_Value, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Launch_Date, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Comments, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Commercial_Pack, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Item_Type, 'tbc')) = 'tbc' then 1 else 0 end +
	    case when lower(coalesce(Path, 'tbc')) = 'tbc' then 1 else 0 end
      from CM_02_Conditional_Access_Feed_load s
     where si_service_key IS NOT NULL
    commit
   
    if object_id('CM_02_Conditional_Access_Feed_load_tmp') is not null 
    begin
        drop table CM_02_Conditional_Access_Feed_load_tmp 
    end 
    
    select uniqid,
           dense_rank() over(partition by a.si_service_key order by a.score asc, a.uniqid asc) as rank, 
           max(case when lower(CA) = 'tbc' then NULL else CA end) over(partition by a.si_service_key) as CA_inferred
      into CM_02_Conditional_Access_Feed_load_tmp     
      from CM_02_Conditional_Access_Feed_load a
    commit
             
    UPDATE CM_02_Conditional_Access_Feed_load x   
       SET x.rank        =  s.rank,
           x.CA_inferred =  s.CA_inferred
      from CM_02_Conditional_Access_Feed_load_tmp s
     where x.uniqid = s.uniqid
       and x.si_service_key IS NOT NULL   
    commit
   
    if object_id('CM_02_Conditional_Access_Feed_load_tmp') is not null 
    begin
        drop table CM_02_Conditional_Access_Feed_load_tmp 
    end


    -- create stats for logging if duplicates exist   
    declare @removing_records integer
   
    select @removing_records = count(1)
      from CM_02_Conditional_Access_Feed_load
     where rank <> 1
   
    if @removing_records > 0
    begin          
        insert into CM_24_Run_Log(run_date, msg)
        values(today(), '[' || now() || '] Removing('||@removing_records|| ') duplicates from CA feed')  
    end 



    truncate table CM_02_Conditional_Access_Feed
    INSERT into CM_02_Conditional_Access_Feed(EPG,
           Name1,
           UI_DESCR,
           SI_SERVICE_KEY,
           Source_Channel,
           SI_SERVICE_ID,
           TRANSPORT_ID,
           Type1,
           GENRE,
           Retail,
           CA,
           Legacy_CA,
           Regional,
           Regional_Blackout,
           Template,
           Param1,
           Param2,
           Param3,
           Param4,
           Param5,
           Param6,
           Param7,
           Param8,
           Param9,
           Param10,
           Param11,
           Param12,
           Pairing,
           Taping,
           Status,
           CCI_Value,
           Launch_Date,
           Comments,
           Commercial_Pack,
           Item_Type,
           Path)---put params in
    select EPG,
           Name1,
           UI_DESCR,
           SI_SERVICE_KEY,
           Source_Channel,
           SI_SERVICE_ID,
           TRANSPORT_ID,
           Type1,
           GENRE,
           Retail,
           coalesce(CA_inferred, ''), --CA
           Legacy_CA,
           Regional,
           Regional_Blackout,
           Template,
           Param1,
           Param2,
           Param3,
           Param4,
           Param5,
           Param6,
           Param7,
           Param8,
           Param9,
           Param10,
           Param11,
           Param12,
           Pairing,
           Taping,
           Status,
           CCI_Value,
           Launch_Date,
           Comments,
           Commercial_Pack,
           Item_Type,
           Path
      from CM_02_Conditional_Access_Feed_load
     where rank = 1 
       and si_service_key IS NOT NULL
     order by si_service_key
    commit

    
declare @missing_ca_records integer
 select @missing_ca_records = count(1)
   from CM_02_Conditional_Access_Feed
  where CA = ''


    message '[' || now() || '] Raw data loaded (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Raw data loaded (' || @@rowcount || ' rows) with('||@missing_ca_records||') CA field errors'
    message ' ' type status to client



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### VESPA_PROGRAMME_SCHEDULE                                                                           #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Processing PROGRAMME SCHEDULE feed (Olive tables)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Processing PROGRAMME SCHEDULE feed (Olive tables)'
    message '[' || now() || '] Data range: ' || cast(@parSIFeedDate - 28 as date) || ' - ' || @parSIFeedDate type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Data range: ' || cast(@parSIFeedDate - 28 as date) || ' - ' || @parSIFeedDate
    truncate table CM_03_VESPA_Programme_Schedule_Feed


      -- Get list of all available Service Keys for the last X days
    insert into CM_03_VESPA_Programme_Schedule_Feed
          (Service_Key, BSS_Code, BSS_Code_Datetime, BSS_Code_Sequence)
      select ps.Service_Key,
             ps.BSS_Code,
             ps.Broadcast_Start_Date_Time_Utc,
             rank () over (partition by ps.Service_Key order by ps.Broadcast_Start_Date_Time_Utc desc, ps.Pk_Programme_Instance_Dim) as BSS_Code_Sequence
        from (select
                    Service_Key,
                    BSS_Code,
                    Broadcast_Start_Date_Time_Utc,
                    Pk_Programme_Instance_Dim
                from sk_prod.VESPA_PROGRAMME_SCHEDULE ps
               where date(Broadcast_Start_Date_Time_Utc) <= @parSIFeedDate
                 and date(Broadcast_Start_Date_Time_Utc) >= @parSIFeedDate - 28                                                         -- Pick last month or so - more than enough
                 and Service_Key >= 1000                                                                                                -- Linear channels only
                 and Service_Key < 65535
               group by Service_Key, BSS_Code, Broadcast_Start_Date_Time_Utc, Pk_Programme_Instance_Dim) ps
    commit
    message '[' || now() || '] Data processed (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Data processed (' || @@rowcount || ' rows)'
    message ' ' type status to client



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### Manual changes                                                                                     #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Processing MANUAL CHANGES feed: "CM_feed_' || @parAnyFeedDate || '_Manual_changes.csv"' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Processing MANUAL CHANGES feed: "CM_feed_' || @parAnyFeedDate || '_Manual_changes.csv"'
    truncate table CM_04_Manual_Changes_Feed

    set @varSQL = '
                    load table CM_04_Manual_Changes_Feed (
                        User_Notes,
                        SERVICE_KEY                  null(''.''),
                        FULL_NAME                    null(''.''),
                        EPG_NUMBER                   null(''.''),
                        EPG_NAME                     null(''.''),
                        VESPA_NAME                   null(''.''),
                        CHANNEL_NAME                 null(''.''),
                        TECHEDGE_NAME                null(''.''),
                        INFOSYS_NAME                 null(''.''),
                        BARB_REPORTED                null(''.''),
                        ACTIVEX                      null(''.''),
                        CHANNEL_OWNER                null(''.''),
                        OLD_PACKAGING                null(''.''),
                        NEW_PACKAGING                null(''.''),
                        PAY_FREE_INDICATOR           null(''.''),
                        CHANNEL_GENRE                null(''.''),
                        CHANNEL_TYPE                 null(''.''),
                        FORMAT                       null(''.''),
                        PARENT_SERVICE_KEY           null(''.''),
                        TIMESHIFT_STATUS             null(''.''),
                        TIMESHIFT_MINUTES            null(''.''),
                        RETAIL                       null(''.''),
                        CHANNEL_REACH                null(''.''),
                        HD_SWAP_EPG_NUMBER           null(''.''),
                        SENSITIVE_CHANNEL            null(''.''),
                        SPOT_SOURCE                  null(''.''),
                        PROMO_SOURCE                 null(''.''),
                        NOTES                        null(''.''),
                        EFFECTIVE_FROM               null(''.''),
                        EFFECTIVE_TO                 null(''.''),
                        TYPE_ID                      null(''.''),
                        UI_DESCR                     null(''.''),
                        EPG_CHANNEL                  null(''.''),
                        AMEND_DATE                   null(''.''),
                        CHANNEL_PACK                 null(''.''),
                        SERVICE_ATTRIBUTE_VERSION    null(''.''),
                        PRIMARY_SALES_HOUSE          null(''.''),
                        CHANNEL_GROUP                null(''.''),
                        PROVIDER_ID                  null(''.''),
                        PAY_SKY_SPORTS_FLAG          null(''.''),
                        PAY_SPORTS_FLAG              null(''.''),
                        PAY_TV_FLAG                  null(''.''),
                        KEY_PAY_ENTERTAINMENT_FLAG   null(''.''),
                        SKY_SPORTS_NEWS_FLAG         null(''.''),
                        SKY_MOVIES_FLAG              null(''.''),
                        BT_SPORT_FLAG                null(''.''),
                        Dummy''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_Manual_changes.csv''
                    SKIP 3
                    QUOTES ON
                    ESCAPES OFF
                    DELIMITED BY '',''
                  '
    execute( replace(@varSQL, '##^1^##', @parAnyFeedDate) )
    commit
    message '[' || now() || '] Data processed (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Data processed (' || @@rowcount || ' rows)'
    message ' ' type status to client



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### BARB                                                                                               #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Processing BARB feed (Olive tables)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Processing BARB feed (Olive tables)'
    message '[' || now() || '] Snapshot "as of" date: ' || @parSIFeedDate type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Snapshot "as of" date: ' || @parSIFeedDate
    truncate table CM_05_BARB_Feed

    --[!!!] This does not work as expected: non-unique SERVICE KEY<->LOG STATION NAME is produced. Requires further rule tightening
    insert into CM_05_BARB_Feed
          (Service_Key, Station_Sequence, Log_Station_Code, DB2_Station_Code, Log_Station_Name, Log_Station_Short_Name,
           Log_Station_15_Char_Name, Reporting_Start_Date, Reporting_End_Date, Sales_House_1, Sales_House_2, Sales_House_3,
           Sales_House_4, Sales_House_5, Sales_House_6, Broadcast_Group_Id, Broadcast_Group_Name, Sales_House_Name)
      select
            lkp.Service_Key,
            rank () over (partition by barb.Log_Station_Name order by barb.Reporting_Start_Date desc, barb.Reporting_End_Date, barb.Area_Geography, barb.Area_Flags) as Station_Sequence,
            barb.Log_Station_Code,
            barb.DB2_Station_Code,
            barb.Log_Station_Name,
            barb.Log_Station_Short_Name,
            barb.Log_Station_15Char_Name,
            barb.Reporting_Start_Date,
            barb.Reporting_End_Date,
            barb.Sales_House_1,
            barb.Sales_House_2,
            barb.Sales_House_3,
            barb.Sales_House_4,
            barb.Sales_House_5,
            barb.Sales_House_6,
            barb.Broadcast_Group_Id,
            barb.Broadcast_Group_Name,
            barb.Sales_House_Name
        from (select
                    max(File_Creation_Date) over( partition by Log_Station_Code) as Max_File_Creation_Date,                                        -- For deduping
                    File_Creation_Date,
                    a.Log_Station_Code,
                    a.DB2_Station_Code,
                    a.Log_Station_Name,
                    a.Log_Station_Short_Name,
                    a.Log_Station_15Char_Name,
                    case
                      when a.Reporting_Start_Date is null then cast('1980-01-01' as date)
                        else a.Reporting_Start_Date
                    end as Reporting_Start_Date,
                    case
                      when a.Reporting_End_Date is null then cast('2999-12-31' as date)
                        else a.Reporting_End_Date
                    end as Reporting_End_Date,
                    a.Sales_House_1,
                    a.Sales_House_2,
                    a.Sales_House_3,
                    a.Sales_House_4,
                    a.Sales_House_5,
                    a.Sales_House_6,
                    a.Broadcast_Group_Id,
                    b.Broadcast_Group_Name,
                    c.Sales_House_Name,
                    a.Area_Geography,
                    a.Area_Flags
                from sk_prod_vespa_restricted.BARB_LOG_STATIONS_REP a                                                                              -- [!!!] to be moved to VA schema
                      left join (select
                                       Broadcast_Group_Id,
                                       Broadcast_Group_Name,
                                       Reporting_Start_Date,
                                       Reporting_End_Date,
                                       rank () over (partition by Broadcast_Group_Id order by Reporting_Start_Date desc, Reporting_End_Date desc, Broadcast_Group_Name) as Group_Sequence
                                   from BARB_MASTER_FILE_BROADCAST_GROUP_RECORD) b on a.Broadcast_Group_Id = b.Broadcast_Group_Id                  -- [!!!] to be moved to VA schema
                                                                                  and b.Group_Sequence = 1
                      left join (select
                                       Sales_House_Identifier,
                                       Sales_House_Name,
                                       Reporting_Start_Date,
                                       Reporting_End_Date,
                                       rank () over (partition by Sales_House_Identifier order by Reporting_Start_Date desc, Reporting_End_Date desc, Sales_House_Identifier) as Sales_House_Sequence
                                   from BARB_MASTER_FILE_SALES_HOUSE_RECORD) c on a.Sales_House_1 = c.Sales_House_Identifier                       -- [!!!] to be moved to VA schema
                                                                              and c.Sales_House_Sequence = 1
               where a.Reporting_Start_Date <= @parSIFeedDate
                 and (
                      a.Reporting_End_Date > @parSIFeedDate
                      or
                      a.Reporting_End_Date is null
                     )
             ) barb,
            vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB lkp,
            (select
                   Service_Key,
                   Log_Station_Code,
                   max(STI_Code) as STI_Code
               from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_BARB
              group by Service_Key, Log_Station_Code) ded

       where barb.Max_File_Creation_Date = barb.File_Creation_Date
         and barb.Log_Station_Code = lkp.Log_Station_Code
         and lkp.Effective_From <= @parSIFeedDate
         and lkp.Effective_To >= @parSIFeedDate
         and lkp.Service_Key is not null
         and lkp.Service_Key = ded.Service_Key
         and lkp.Log_Station_Code = ded.Log_Station_Code
         and lkp.STI_Code = ded.STI_Code
       order by lkp.Service_Key desc, barb.Reporting_Start_Date, barb.Reporting_End_Date, barb.Area_Geography, barb.Area_Flags, barb.Log_Station_Name
    commit


    update CM_05_BARB_Feed base
       set base.Station_Identifier = trim(lower(replace(base.Log_Station_Name, ' ', '')))
    commit

    message '[' || now() || '] Data processed (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Data processed (' || @@rowcount || ' rows)'
    message ' ' type status to client



    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### Landmark                                                                                           #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Processing LANDMARK feed: "CM_feed_' || @parAnyFeedDate || '_Landmark.csv"' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Processing LANDMARK feed: "CM_feed_' || @parAnyFeedDate || '_Landmark.csv"'
    truncate table CM_06_Landmark_Feed

    set @varSQL = '
                    load table CM_06_Landmark_Feed (
                        MEDIA_SALES_AREA_PK,
                        MEDIA_SALES_AREA_NUMBER,
                        CURRENCY_CODE,
                        DEFAULT_MINUTAGE,
                        BASE_DEMO_NO,
                        SECONDARY_BASE_DEMO_NO,
                        RATING_SUPPLIER_CODE,
                        BASE_LENGTH,
                        STATION_PRICE,
                        MEDIA_SALES_AREA_CODE,
                        MEDIA_SALES_AREA_NAME,
                        MEDIA_SALES_AREA_SHORT_NAME,
                        MEDIA_SALES_AREA_DESCRIPTION,
                        TX_START_DATE,
                        TX_END_DATE,
                        IA_START_DATE,
                        IA_END_DATE,
                        MEDIA_TARGET_SALES_AREA_CODE,
                        MEDIA_TARGET_SALES_AREA_NAME,
                        MEDIA_TARGET_SALES_AREA_SHORT_NAME,
                        MEDIA_TARGET_SALES_AREA_SHORT_CODE,
                        EFFECTIVE_FROM,
                        EFFECTIVE_TO,
                        CURRENT_DIM,
                        CREATE_DATE,
                        UPDATE_DATE,
                        LOAD_ID,
                        Active,
                        Sky,
                        SK''\n''
                    )
                    FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_Landmark.csv''
                    SKIP 1
                    QUOTES ON
                    ESCAPES OFF
                    NOTIFY 1000
                    DELIMITED BY '',''
                  '
    execute( replace(@varSQL, '##^1^##', @parAnyFeedDate) )
    commit

    message '[' || now() || '] Data processed (' || @@rowcount || ' rows)' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] Data processed (' || @@rowcount || ' rows)'
    message ' ' type status to client


    -- ##############################################################################################################
    message '[' || now() || '] ####### Step 3 completed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 3 completed'
    message '[' || now() || '] #############################################################' type status to client
    message ' ' type status to client


end;
go


-- select * from CM_01_Service_Integration_Feed;
-- select * from CM_02_Conditional_Access_Feed;
-- select * from CM_03_VESPA_Programme_Schedule_Feed;
-- select * from CM_04_Manual_Changes_Feed;
-- select * from CM_06_Landmark_Feed;

-- select * from CM_05_BARB_Feed;
-- select count(*) from CM_05_BARB_Feed;
-- select count(*) from BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD;



  -- ##############################################################################################################
  -- ##############################################################################################################





