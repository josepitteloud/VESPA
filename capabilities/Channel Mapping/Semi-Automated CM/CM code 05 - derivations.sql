/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - derivations
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
drop procedure CM_05_CM_Derivations;
create procedure CM_05_CM_Derivations
    @parGreenLightToRun         smallint = 0,
    @result                     smallint output
as
begin

    declare @varProcCurrentVersion      smallint
    set @varProcCurrentVersion          = 4                         -- Increment on any change to this procedure

    declare @varProcExpectedVersion     smallint
    set @varProcExpectedVersion         = (select CM_Param_Value__Num from vespa_analysts.CM_00_Process_Global_Parameters where CM_Parameter_Name = 'PROC VERSION - CM_05_CM_Derivations')

    if (@parGreenLightToRun <> 1)
      begin
          message '[!!!!!] CM_05_CM_Derivations: PROCESS IS ALREADY HALTED' type status to client
          select @result = 0
          return 0
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_05_CM_Derivations: PROCESS IS ALREADY HALTED'
      end

    if (@varProcExpectedVersion is null)
      begin
          message '[!!!!!] CM_05_CM_Derivations: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED' type status to client
          select @result = 0
          return 0
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_05_CM_Derivations: REQUIRED PROCEDURE VERSION COULD NOT BE DETERMINED - PROCESS EXECUTION HALTED'
      end

    if (@varProcCurrentVersion <> @varProcExpectedVersion)
      begin
          message '[!!!!!] CM_05_CM_Derivations: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_05_CM_Derivations: PROCEDURE IS OUTDATED - PROCESS EXECUTION HALTED'
          message '[!!!!!] CM_05_CM_Derivations: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required' type status to client
          select @result = 0
          return 0
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[!!!!!] CM_05_CM_Derivations: v' || @varProcCurrentVersion || ' is being used but v' || @varProcExpectedVersion || ' is expected - update required'
      end


    message '[' || now() || '] #############################################################' type status to client
    message '[' || now() || '] ####### Step 5 start: CM derivations' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 5 start: CM derivations'


    -- ##############################################################################################################
    -- ##############################################################################################################
    -- ##### Calculate PARENT_SERVICE_KEY                                                                       #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Calculating parent service key' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
            ,'[' || now() || '] ~~ Calculating parent service key'

    -- Non-timeshifted & HD channels
    update CM_20_Final_Channel_Mapping_Data base
       set base.PARENT_SERVICE_KEY = par.SERVICE_KEY
      from (select
                  SERVICE_KEY,
                  xCountry,
                  xCHANNEL_GROUP_ID,
                  xChannel_Flags
              from CM_20_Final_Channel_Mapping_Data
             where Timeshift_Minutes = 0
               and Format = 'SD') par
     where base.xCHANNEL_GROUP_ID = par.xCHANNEL_GROUP_ID
       and base.xCountry = par.xCountry
       and base.xChannel_Flags = par.xChannel_Flags
       and base.Timeshift_Minutes = 0
       and base.Format = 'HD'
       and base.xSK_Automated_Flag = 1
    commit


      -- Timeshifted channels
    update CM_20_Final_Channel_Mapping_Data base
       set base.PARENT_SERVICE_KEY = par.SERVICE_KEY
      from (select
                  SERVICE_KEY,
                  xCountry,
                  xCHANNEL_GROUP_ID,
                  xChannel_Flags
              from CM_20_Final_Channel_Mapping_Data
             where Timeshift_Minutes = 0
               and Format = 'SD') par
     where base.xCHANNEL_GROUP_ID = par.xCHANNEL_GROUP_ID
       and base.xCountry = par.xCountry
       and base.xChannel_Flags = par.xChannel_Flags
       and base.Timeshift_Minutes > 0
       and base.xSK_Automated_Flag = 1
    commit


      -- Non-timeshifted channels & non-HD (set to their own SK)
    update CM_20_Final_Channel_Mapping_Data base
       set base.PARENT_SERVICE_KEY = base.SERVICE_KEY
     where base.Timeshift_Minutes = 0
       and base.Format <> 'HD'
       and base.xSK_Automated_Flag = 1
    commit


      -- Apply manual mappings (override)
    update CM_20_Final_Channel_Mapping_Data base
       set base.PARENT_SERVICE_KEY  = case
                                          -- BBC 1
                                        when base.SERVICE_KEY = 2076 then 2002        -- HD BBC One               -> BBC 1 London
                                        when base.SERVICE_KEY = 2081 then 2005        -- BBC 1 HD NI              -> BBC 1 N. Ireland
                                        when base.SERVICE_KEY = 2082 then 2004        -- BBC 1 Scotland HD        -> BBC 1 Scotland
                                        when base.SERVICE_KEY = 2083 then 2003        -- BBC 1 HD Wales           -> BBC 1 Wales
                                        when base.SERVICE_KEY = 1037 then 1094        -- BBC One HD (for ROI)     ->  BBC1 NI (for ROI)

                                          -- BBC 2
                                        when base.SERVICE_KEY = 2075 then 2006        -- HD BBC Two (UK)          -> BBC 2 England
                                        when base.SERVICE_KEY = 1091 then 1057        -- BBC Two HD (for ROI)     ->  BBC 2 NI (for ROI)

                                          -- ITV
                                        when base.SERVICE_KEY = 6504 then 6504        -- ITV1 HD London
                                        when base.SERVICE_KEY = 6503 then 6503        -- ITV1 HD Mid West
                                        when base.SERVICE_KEY = 1045 then 1045        -- ITV 1 Anglia HD
                                        when base.SERVICE_KEY = 6505 then 6505        -- ITV1 HD North
                                        when base.SERVICE_KEY = 1043 then 1043        -- ITV Tyne Tees HD
                                        when base.SERVICE_KEY = 6502 then 6502        -- ITV1 HD South East
                                        when base.SERVICE_KEY = 6501 then 6501        -- ITV1 HD Wales
                                        when base.SERVICE_KEY = 4055 then 4055        -- STV HD
                                        when base.SERVICE_KEY = 6510 then 6510        -- UTV HD
                                        when base.SERVICE_KEY = 1044 then 1044        -- ITV 1 YTV HD
                                        when base.SERVICE_KEY = 1061 then 1061        -- ITV1 Border HD
                                        when base.SERVICE_KEY = 1062 then 1062        -- ITV1 W Country HD
                                        when base.SERVICE_KEY = 1063 then 1063        -- ITV1 West HD


                                          -- ITV + 1
                                        when base.SERVICE_KEY = 6128 then 6089        -- ITV - ITV1+1 Anglia      -> ITV - ITV1 Anglia East     (Anglia East and West currently have same regional programming so either okay)
                                        when base.SERVICE_KEY = 6126 then 6390        -- ITV - ITV1+1 Tyne Tees   -> ITV - ITV1 Tyne Tees
                                        when base.SERVICE_KEY = 6365 then 6142        -- ITV - ITV1+1 Meridian    -> ITV - ITV1 Meridian South East
                                        when base.SERVICE_KEY = 6125 then 6040        -- ITV - ITV1+1 W Country   -> ITV - ITV1 - Westcountry
                                        when base.SERVICE_KEY = 6145 then 6011        -- ITV - ITV1+1 Midlands    -> ITV - ITV1 Central East
                                        when base.SERVICE_KEY = 6355 then 6130        -- ITV - ITV1+1 Granada     -> ITV - ITV1 Granada
                                        when base.SERVICE_KEY = 6127 then 6030        -- ITV - ITV1+1 West        -> ITV - ITV1 West
                                        when base.SERVICE_KEY = 6012 then 6020        -- ITV - ITV1+1 Wales       -> ITV - ITV1 Wales
                                        when base.SERVICE_KEY = 6065 then 6160        -- ITV - ITV1+1 Yorkshire   -> ITV - ITV1 Yorkshire West
                                        when base.SERVICE_KEY = 6155 then 6000        -- ITV - ITV1+1 London      -> ITV - ITV1 London

                                          -- Channel 4
                                        when base.SERVICE_KEY = 4075 then 1621        -- HD Channel 4 HD          -> Ch4 London
                                        when base.SERVICE_KEY = 1670 then 1621        -- C4 +1 London             -> Ch4 London
                                        when base.SERVICE_KEY = 1675 then 1626        -- C4 +1 Scotland           -> Ch4 Scotland
                                        when base.SERVICE_KEY = 1674 then 1625        -- C4 +1 Ulster             -> Ch4 Ulster
                                        when base.SERVICE_KEY = 1673 then 1624        -- C4 +1 North              -> Ch4 North
                                        when base.SERVICE_KEY = 1672 then 1623        -- C4 +1 Midlands           -> Ch4 Midlands
                                        when base.SERVICE_KEY = 1671 then 1622        -- C4 +1 South              -> Ch4 South

                                          -- Channel 5
                                        when base.SERVICE_KEY = 4058 then 1800        -- HD Channel 5             -> Channel 5 Main/UK
                                        when base.SERVICE_KEY = 1839 then 1800        -- Channel 5 +1             -> Channel 5 Main/UK

                                          -- Issues with channel group ID / other fields
                                        when base.SERVICE_KEY = 3665 then 3665        -- BT Sport 1 HD Pub
                                        when base.SERVICE_KEY = 3666 then 3666        -- BT Sport 2 HD Pub
                                        when base.SERVICE_KEY = 1047 then 1047        -- Insight HD
                                        when base.SERVICE_KEY = 3147 then 3147        -- NHK World HD
                                        when base.SERVICE_KEY = 4101 then 4101        -- HD Sky Insider
                                        when base.SERVICE_KEY = 5541 then 5541        -- HD Daystar
                                        when base.SERVICE_KEY = 1066 then 1066        -- Arirang TV HD (Arirang HD)
                                        when base.SERVICE_KEY = 3414 then 3414        -- TV Record HD (HD Record TV)
                                        when base.SERVICE_KEY = 3751 then 3751        -- TrueChrstms+1 (True Movies 2)
                                        when base.SERVICE_KEY = 5338 then 5338        -- True Ent +1 (True Drama)

                                        when base.SERVICE_KEY = 9008 then 9008        -- H1Test
                                        when base.SERVICE_KEY = 1022 then 1022        -- HD Sky Intro

                                        when base.SERVICE_KEY = 1501 then 1501        -- Sky Box Office (SBO 1)
                                        when base.SERVICE_KEY = 1502 then 1502        -- Sky Box Office (SBO 02)
                                        when base.SERVICE_KEY = 1503 then 1503        -- Sky Box Office (SBO 3)
                                        when base.SERVICE_KEY = 1504 then 1504        -- Sky Box Office (SBO 04)
                                        when base.SERVICE_KEY = 1505 then 1505        -- Sky Box Office (SBO 5)
                                        when base.SERVICE_KEY = 1506 then 1506        -- Sky Box Office (SBO 6)
                                        when base.SERVICE_KEY = 1507 then 1507        -- Sky Box Office (SBO 7)
                                        when base.SERVICE_KEY = 1508 then 1508        -- Sky Box Office (SBO 8)
                                        when base.SERVICE_KEY = 1509 then 1509        -- Sky Box Office (SBO 9)
                                        when base.SERVICE_KEY = 1510 then 1510        -- Sky Box Office (SBO 10)
                                        when base.SERVICE_KEY = 1511 then 1511        -- Sky Box Office (SBO 11)
                                        when base.SERVICE_KEY = 1512 then 1512        -- Sky Box Office (SBO 12)
                                        when base.SERVICE_KEY = 1513 then 1513        -- Sky Box Office (SBO 13)
                                        when base.SERVICE_KEY = 1514 then 1514        -- Sky Box Office (SBO 14)
                                        when base.SERVICE_KEY = 1515 then 1515        -- Sky Box Office (SBO 15)
                                        when base.SERVICE_KEY = 1539 then 1539        -- Sky Box Office (SBO 44)
                                        when base.SERVICE_KEY = 1690 then 1690        -- Sky Box Office (SBO 43)
                                        when base.SERVICE_KEY = 4032 then 4032        -- SBO HD (HD SBO)
                                        when base.SERVICE_KEY = 1699 then 1699        -- Sky Box Office Preview Chnl

                                          else base.PARENT_SERVICE_KEY
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
    -- ##### Calculate other derived fields                                                                     #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Calculating other derived fields' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Calculating other derived fields'

    update CM_20_Final_Channel_Mapping_Data base
       set base.TYPE_ID                 = case
                                            when base.Service_Key between 4094 and 4098               then 194                                  -- Push VOD (currently not maintained)

                                            when base.Service_Key < 1000 or base.Service_Key  = 65535 or                                        -- Pull VOD (currently not maintained)
                                                 lower(base.FULL_NAME) = 'video on demand'            then 4

                                            when base.FORMAT = 'Radio'                                then 2                                    -- Radio

                                            when (lower(base.EPG_NAME) like 'sky box office%' or lower(base.EPG_NAME) like 'sbo%') and
                                                 base.FORMAT = 'SD'                                   then 5                                    -- SBO SD

                                            when (lower(base.EPG_NAME) like 'sky box office%' or lower(base.EPG_NAME) like 'sbo%') and
                                                 base.FORMAT = 'HD'                                   then 141                                  -- SBO HD

                                            when lower(base.FULL_NAME) = 'interactive applications' or                                          -- Interactive
                                                 base.FORMAT = 'interactive'                          then 130

                                            when lower(base.FULL_NAME) like 'sky sports interactive%' then 147                                  -- Sky Sports Interactive

                                            when base.FORMAT = 'SD'                                   then 1                                    -- Linear SD

                                            when base.FORMAT = 'HD'                                   then 25                                   -- Linear HD

                                              else -1
                                          end
     where xSK_Automated_Flag = 1
    commit


    update CM_20_Final_Channel_Mapping_Data base
       set base.ACTIVEX                 = 'Y',
           base.PAY_FREE_INDICATOR      = case
                                            when (lower(base.CHANNEL_TYPE) like '%retail%' 
                                                  AND lower(base.CHANNEL_TYPE) not like '%non%') or
                                                 lower(base.CHANNEL_TYPE) = 'ppv' or
                                                 lower(base.CHANNEL_TYPE) = 'nr - pay-per-view' or
                                                 lower(base.CHANNEL_TYPE) = 'nr - conditional access' then 'PAY'

                                            when lower(base.CHANNEL_TYPE) like 'nr%' or
                                                 lower(base.CHANNEL_TYPE) like 'fta%' or
                                                 lower(base.CHANNEL_TYPE) like 'ftv%' then 'FTA'

                                            when lower(base.CHANNEL_TYPE) like '%staff%' then 'STAFF'
                                              else ''                                                                                              -- Fall-back to capture other cases (i.e. nulls & other values)
                                          end,
           base.TIMESHIFT_STATUS        = case
                                            when base.TIMESHIFT_MINUTES > 0 then 'Timeshift'
                                            when base.TYPE_ID = 5 then 'NVOD'
                                            when base.TYPE_ID = 141 then 'NVOD'
                                              else 'Principal'
                                          end,
           base.SENSITIVE_CHANNEL       = case
                                            when lower(base.BARB_REPORTED) = 'no' and lower(base.CHANNEL_GENRE) in ('adult', 'religious') then 1
                                              else 0
                                          end,

           base.SPOT_SOURCE             = case
                                            when base.xLandmark_Match_Flag = 1 then 'Landmark'
                                            when lower(base.BARB_REPORTED) = 'yes' and base.xSales_House_1 is not null then 'BARB'
                                              else 'None'
                                          end,

           base.PROMO_SOURCE            = case
                                            when base.xBSS_CODE is not null and trim(lower(base.xBSS_CODE)) <> 'unknown' and trim(lower(base.xBSS_CODE)) <> '(unknown)' then 'BSS'
                                            when BARB_REPORTED = 'YES' then 'BARB'
                                              else 'None'
                                          end,
           base.EFFECTIVE_TO            = '2999-12-31 00:00:00',

           base.PRIMARY_SALES_HOUSE     = case
                                            when base.xSales_House_Name is not null and
                                                 lower(base.BARB_REPORTED) = 'yes' and
                                                 base.xSales_House_1 is not null and base.xSales_House_1 <> 0 and
                                                 (base.xSales_House_2 is null or base.xSales_House_2 = 0) and
                                                 (base.xSales_House_3 is null or base.xSales_House_3 = 0) and
                                                 (base.xSales_House_4 is null or base.xSales_House_4 = 0) and
                                                 (base.xSales_House_5 is null or base.xSales_House_5 = 0) and
                                                 (base.xSales_House_6 is null or base.xSales_House_6 = 0) then base.xSales_House_Name
                                              else ''
                                          end,

           base.PAY_SKY_SPORTS_FLAG     = case
                                            when lower(trim(base.CHANNEL_TYPE)) in ('retail - sports', 'retail - sports + hd pack') then 'Yes'            -- All Sky Sports
                                              else 'No'
                                          end,
           base.PAY_SPORTS_FLAG         = case
                                            when lower(trim(base.CHANNEL_TYPE)) in ('retail - sports', 'retail - sports + hd pack') then 'Yes'            -- All Sky Sports
                                            when lower(trim(base.CHANNEL_TYPE)) in ('nr - conditional access') and base.CHANNEL_GENRE = 'Sports' and      -- All BT Sport
                                                lower(base.CHANNEL_NAME) like '%bt%' then 'Yes'
                                              else 'No'
                                          end,
           base.PAY_TV_FLAG             = case
                                            when lower(base.CHANNEL_TYPE) like 'retail%' or
                                                 lower(base.CHANNEL_TYPE) = 'nr - pay-per-view' or
                                                 lower(base.CHANNEL_TYPE) = 'nr - conditional access' then 'Yes'
                                              else 'No'
                                          end,
           base.KEY_PAY_ENTERTAINMENT_FLAG
                                        = case
                                            when base.xCHANNEL_GROUP_ID in (1412, 1752, 2201, 1402, 1833, 1305, 1842, 2510, 1813,
                                                                            1726, 1841, 2401, 2406, 2407, 2409, 4548, 2505) then 'Yes'
                                              else 'No'
                                          end,
           base.SKY_SPORTS_NEWS_FLAG    = case
                                            when base.xCHANNEL_GROUP_ID = 1314 then 'Yes'                                                                 -- Sky Sports News
                                              else 'No'
                                          end,
           base.SKY_MOVIES_FLAG         = case
                                            when lower(trim(base.CHANNEL_TYPE)) in ('retail - movies','retail - movies + hd pack') then 'Yes'
                                              else 'No'
                                          end,
           base.BT_SPORT_FLAG           = case
                                            when lower(trim(base.CHANNEL_TYPE)) in ('nr - conditional access') and base.CHANNEL_GENRE = 'Sports' and      -- All BT Sport
                                                lower(base.CHANNEL_NAME) like '%bt%' then 'Yes'
                                              else 'No'
                                          end
     where xSK_Automated_Flag = 1
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
    -- ##### Update non-maintained fields with current values from CM                                           #####
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Using existing values for non-maintained fields' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Using existing values for non-maintained fields'

    -- (!!!!) Comment out fields MAINTAINED automatically
    -- (!!!!) Leave non-maintained fields uncommented/active
    update CM_20_Final_Channel_Mapping_Data base
       set
           -- base.FULL_NAME                   = cm.FULL_NAME,                                                -- 3/10
           -- base.EPG_NUMBER                  = cm.EPG_NUMBER,                                               -- 3/10
           -- base.EPG_NAME                    = cm.EPG_NAME,                                                 -- 3/10
           -- base.VESPA_NAME                  = cm.VESPA_NAME,                                               -- 3/10
           -- base.CHANNEL_NAME                = cm.CHANNEL_NAME,                                             -- 3/10
           base.TECHEDGE_NAME               = cm.TECHEDGE_NAME,                                                                                           -- No rules defined - currently not maintained
           base.INFOSYS_NAME                = cm.INFOSYS_NAME,                                                                                            -- No rules defined - currently not maintained
           -- base.BARB_REPORTED               = cm.BARB_REPORTED,                                                        -- 1/11
           -- base.ACTIVEX                     = cm.ACTIVEX,                                                  -- CM metadata - calculated automatically within the process
           base.CHANNEL_OWNER               = cm.CHANNEL_OWNER,                                                                                           -- BARB feed is not created correcly - duplicates exists, no rules provided
           base.OLD_PACKAGING               = cm.OLD_PACKAGING,                                                                                           -- No rules defined - currently not maintained
           base.NEW_PACKAGING               = cm.NEW_PACKAGING,                                                                                           -- No rules defined - currently not maintained
           -- base.PAY_FREE_INDICATOR          = cm.PAY_FREE_INDICATOR,                                                   -- 1/11
           -- base.CHANNEL_GENRE               = cm.CHANNEL_GENRE,                                            -- 3/10
           -- base.CHANNEL_TYPE                = cm.CHANNEL_TYPE,                                             -- 3/10     -- 1/11
           -- base.FORMAT                      = cm.FORMAT,                                                               -- 1/11
           -- base.PARENT_SERVICE_KEY          = cm.PARENT_SERVICE_KEY,                                                   -- 1/11
           -- base.TIMESHIFT_STATUS            = cm.TIMESHIFT_STATUS,                                         -- 3/10     -- 1/11
           -- base.TIMESHIFT_MINUTES           = cm.TIMESHIFT_MINUTES,                                        -- 3/10
           base.RETAIL                      = cm.RETAIL,                                                                                                  -- No rules defined - currently not maintained
           base.CHANNEL_REACH               = cm.CHANNEL_REACH,                                                                                           -- No rules defined - currently not maintained
           base.HD_SWAP_EPG_NUMBER          = cm.HD_SWAP_EPG_NUMBER,                                                                                      -- No rules defined - currently not maintained
           base.SENSITIVE_CHANNEL           = case when cm.SENSITIVE_CHANNEL = 1 then 1 else base.SENSITIVE_CHANNEL end,  -- 1/11 ("If previously set as sensitive - carry over, otherwise use the rule")
           base.SPOT_SOURCE                 = cm.SPOT_SOURCE,                                                                                             -- BARB feed is not created correcly - duplicates exists, no rules provided
           -- base.PROMO_SOURCE                = cm.PROMO_SOURCE,                                                         -- 1/11
           base.NOTES                       = cm.NOTES,                                                                                                   -- No rules defined - currently not maintained
           -- base.EFFECTIVE_FROM              = cm.EFFECTIVE_FROM,                                           -- CM metadata - calculated automatically within the process
           -- base.EFFECTIVE_TO                = cm.EFFECTIVE_TO,                                             -- CM metadata - calculated automatically within the process
           -- base.TYPE_ID                     = cm.TYPE_ID,                                                              -- 1/11
           base.UI_DESCR                    = cm.UI_DESCR,                                                                                                -- No rules defined - currently not maintained
           base.EPG_CHANNEL                 = cm.EPG_CHANNEL,                                                                                             -- No rules defined - currently not maintained
           base.AMEND_DATE                  = cm.AMEND_DATE,                                                  -- CM metadata - UPDATE EACH TIME TO RETAIN PREVIOUS VALUE FOR EACH RECORD
           base.CHANNEL_PACK                = cm.CHANNEL_PACK,                                                                                            -- No rules defined - currently not maintained
           -- base.SERVICE_ATTRIBUTE_VERSION   = cm.SERVICE_ATTRIBUTE_VERSION,                                -- CM metadata - calculated automatically within the process
           base.PRIMARY_SALES_HOUSE         = cm.PRIMARY_SALES_HOUSE,                                                                                     -- BARB feed is not created correcly - duplicates exists, no rules provided
           base.CHANNEL_GROUP               = cm.CHANNEL_GROUP,                                                                                           -- No rules defined - currently not maintained
           base.PROVIDER_ID                 = cm.PROVIDER_ID                                                                                              -- No rules defined - currently not maintained
           -- base.PAY_SKY_SPORTS_FLAG         = cm.PAY_SKY_SPORTS_FLAG,                                                  -- 1/11
           -- base.PAY_SPORTS_FLAG             = cm.PAY_SPORTS_FLAG,                                                      -- 1/11
           -- base.PAY_TV_FLAG                 = cm.PAY_TV_FLAG,                                                          -- 1/11
           -- base.KEY_PAY_ENTERTAINMENT_FLAG  = cm.KEY_PAY_ENTERTAINMENT_FLAG,                                           -- 1/11
           -- base.SKY_SPORTS_NEWS_FLAG        = cm.SKY_SPORTS_NEWS_FLAG,                                                 -- 1/11
           -- base.SKY_MOVIES_FLAG             = cm.SKY_MOVIES_FLAG,                                                      -- 1/11
           -- base.BT_SPORT_FLAG               = cm.BT_SPORT_FLAG                                                         -- 1/11
      from (select
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
             where ActiveX = 'Y'
               and date(Effective_To) = '2999-12-31') cm
     where base.SERVICE_KEY = cm.SERVICE_KEY
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
    -- ##### Manual changes                                                                                     #####
    -- ##############################################################################################################
    -- ##############################################################################################################
    message '[' || now() || '] ~~ Applying manual changes' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ~~ Applying manual changes'

    update CM_20_Final_Channel_Mapping_Data base
       set base.FULL_NAME                   = case when man.FULL_NAME <> '.' then man.FULL_NAME else base.FULL_NAME end,
           base.EPG_NUMBER                  = case when man.EPG_NUMBER is not null then man.EPG_NUMBER else base.EPG_NUMBER end,
           base.EPG_NAME                    = case when man.EPG_NAME is not null then man.EPG_NAME else base.EPG_NAME end,
           base.VESPA_NAME                  = case when man.VESPA_NAME is not null then man.VESPA_NAME else base.VESPA_NAME end,
           base.CHANNEL_NAME                = case when man.CHANNEL_NAME is not null then man.CHANNEL_NAME else base.CHANNEL_NAME end,
           base.TECHEDGE_NAME               = case when man.TECHEDGE_NAME is not null then man.TECHEDGE_NAME else base.TECHEDGE_NAME end,
           base.INFOSYS_NAME                = case when man.INFOSYS_NAME is not null then man.INFOSYS_NAME else base.INFOSYS_NAME end,
           base.BARB_REPORTED               = case when man.BARB_REPORTED is not null then man.BARB_REPORTED else base.BARB_REPORTED end,
           base.ACTIVEX                     = case when man.ACTIVEX is not null then man.ACTIVEX else base.ACTIVEX end,
           base.CHANNEL_OWNER               = case when man.CHANNEL_OWNER is not null then man.CHANNEL_OWNER else base.CHANNEL_OWNER end,
           base.OLD_PACKAGING               = case when man.OLD_PACKAGING is not null then man.OLD_PACKAGING else base.OLD_PACKAGING end,
           base.NEW_PACKAGING               = case when man.NEW_PACKAGING is not null then man.NEW_PACKAGING else base.NEW_PACKAGING end,
           base.PAY_FREE_INDICATOR          = case when man.PAY_FREE_INDICATOR is not null then man.PAY_FREE_INDICATOR else base.PAY_FREE_INDICATOR end,
           base.CHANNEL_GENRE               = case when man.CHANNEL_GENRE is not null then man.CHANNEL_GENRE else base.CHANNEL_GENRE end,
           base.CHANNEL_TYPE                = case when man.CHANNEL_TYPE is not null then man.CHANNEL_TYPE else base.CHANNEL_TYPE end,
           base.FORMAT                      = case when man.FORMAT is not null then man.FORMAT else base.FORMAT end,
           base.PARENT_SERVICE_KEY          = case when man.PARENT_SERVICE_KEY is not null then man.PARENT_SERVICE_KEY else base.PARENT_SERVICE_KEY end,
           base.TIMESHIFT_STATUS            = case when man.TIMESHIFT_STATUS is not null then man.TIMESHIFT_STATUS else base.TIMESHIFT_STATUS end,
           base.TIMESHIFT_MINUTES           = case when man.TIMESHIFT_MINUTES is not null then man.TIMESHIFT_MINUTES else base.TIMESHIFT_MINUTES end,
           base.RETAIL                      = case when man.RETAIL is not null then man.RETAIL else base.RETAIL end,
           base.CHANNEL_REACH               = case when man.CHANNEL_REACH is not null then man.CHANNEL_REACH else base.CHANNEL_REACH end,
           base.HD_SWAP_EPG_NUMBER          = case when man.HD_SWAP_EPG_NUMBER is not null then man.HD_SWAP_EPG_NUMBER else base.HD_SWAP_EPG_NUMBER end,
           base.SENSITIVE_CHANNEL           = case when man.SENSITIVE_CHANNEL is not null then man.SENSITIVE_CHANNEL else base.SENSITIVE_CHANNEL end,
           base.SPOT_SOURCE                 = case when man.SPOT_SOURCE is not null then man.SPOT_SOURCE else base.SPOT_SOURCE end,
           base.PROMO_SOURCE                = case when man.PROMO_SOURCE is not null then man.PROMO_SOURCE else base.PROMO_SOURCE end,
           base.NOTES                       = case when man.NOTES is not null then man.NOTES else base.NOTES end,
           base.EFFECTIVE_FROM              = case when man.EFFECTIVE_FROM is not null then man.EFFECTIVE_FROM else base.EFFECTIVE_FROM end,
           base.EFFECTIVE_TO                = case when man.EFFECTIVE_TO is not null then man.EFFECTIVE_TO else base.EFFECTIVE_TO end,
           base.TYPE_ID                     = case when man.TYPE_ID is not null then man.TYPE_ID else base.TYPE_ID end,
           base.UI_DESCR                    = case when man.UI_DESCR is not null then man.UI_DESCR else base.UI_DESCR end,
           base.EPG_CHANNEL                 = case when man.EPG_CHANNEL is not null then man.EPG_CHANNEL else base.EPG_CHANNEL end,
           base.AMEND_DATE                  = case when man.AMEND_DATE is not null then man.AMEND_DATE else base.AMEND_DATE end,
           base.CHANNEL_PACK                = case when man.CHANNEL_PACK is not null then man.CHANNEL_PACK else base.CHANNEL_PACK end,
           base.SERVICE_ATTRIBUTE_VERSION   = case when man.SERVICE_ATTRIBUTE_VERSION is not null then man.SERVICE_ATTRIBUTE_VERSION else base.SERVICE_ATTRIBUTE_VERSION end,
           base.PRIMARY_SALES_HOUSE         = case when man.PRIMARY_SALES_HOUSE is not null then man.PRIMARY_SALES_HOUSE else base.PRIMARY_SALES_HOUSE end,
           base.CHANNEL_GROUP               = case when man.CHANNEL_GROUP is not null then man.CHANNEL_GROUP else base.CHANNEL_GROUP end,
           base.PROVIDER_ID                 = case when man.PROVIDER_ID is not null then man.PROVIDER_ID else base.PROVIDER_ID end,
           base.PAY_SKY_SPORTS_FLAG         = case when man.PAY_SKY_SPORTS_FLAG is not null then man.PAY_SKY_SPORTS_FLAG else base.PAY_SKY_SPORTS_FLAG end,
           base.PAY_SPORTS_FLAG             = case when man.PAY_SPORTS_FLAG is not null then man.PAY_SPORTS_FLAG else base.PAY_SPORTS_FLAG end,
           base.PAY_TV_FLAG                 = case when man.PAY_TV_FLAG is not null then man.PAY_TV_FLAG else base.PAY_TV_FLAG end,
           base.KEY_PAY_ENTERTAINMENT_FLAG  = case when man.KEY_PAY_ENTERTAINMENT_FLAG is not null then man.KEY_PAY_ENTERTAINMENT_FLAG else base.KEY_PAY_ENTERTAINMENT_FLAG end,
           base.SKY_SPORTS_NEWS_FLAG        = case when man.SKY_SPORTS_NEWS_FLAG is not null then man.SKY_SPORTS_NEWS_FLAG else base.SKY_SPORTS_NEWS_FLAG end,
           base.SKY_MOVIES_FLAG             = case when man.SKY_MOVIES_FLAG is not null then man.SKY_MOVIES_FLAG else base.SKY_MOVIES_FLAG end,
           base.BT_SPORT_FLAG               = case when man.BT_SPORT_FLAG is not null then man.BT_SPORT_FLAG else base.BT_SPORT_FLAG end
      from CM_04_Manual_Changes_Feed man
     where base.Service_Key = man.Service_Key
       and man.Service_Key is not null
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
    message '[' || now() || '] ####### Step 5 completed' type status to client
            insert into CM_24_Run_Log
                  (run_date
                  ,msg
                  )
            select today()
                  ,'[' || now() || '] ####### Step 5 completed'
    message '[' || now() || '] #############################################################' type status to client
    message ' ' type status to client


end;
go


  -- ##############################################################################################################
  -- ##############################################################################################################









