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
  -- ##############################################################################################################
  -- ##### SERVICE INTEGRATION                                                                                #####
  -- ##############################################################################################################
  -- ##############################################################################################################
truncate table CM_01_Service_Integration_Feed;
/*
insert into CM_01_Service_Integration_Feed
        (NAME,ui_descr,si_service_key,channel_id,channel_group_id,channel_ui_descr,transport_id,type,genre,epg_num,cm_date)
  select NAME,ui_descr,si_service_key,channel_id,channel_group_id,channel_ui_descr,transport_id,type,genre,epg_num,cm_date
    from pme06.export_chanmap_cur
*/


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
    NOTES'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JasonT/export_chanmap_2016-09-26.csv'
SKIP 1
QUOTES ON
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;



  -- Calculate custom fields
update CM_01_Service_Integration_Feed base
   set base.xFormat             = case
                                    when lower(trim(base.SI_TYPE)) = 'hd time shifted' then 'HD'                                    -- [!!!]
                                    when trim(base.SI_TYPE) = 'HD TV' then 'HD'
                                    when trim(base.SI_TYPE) = 'TV' then 'SD'
                                      else base.SI_TYPE
                                  end,
       base.xCountry            = case
                                    when lower(trim(base.UI_DESCR)) like 'roi %' or lower(trim(base.UI_DESCR)) like '% roi' or
                                          lower(trim(base.UI_DESCR)) like '% roi %' then 'ROI'
                                    when lower(trim(base.UI_DESCR)) = 'comedycentralxroi' then 'ROI'                                -- Exception
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
                                  ;
commit;



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
 where base.SI_SERVICE_KEY = der.SI_SERVICE_KEY;
commit;


select * from CM_01_Service_Integration_Feed;




  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##### CONDITIONAL ACCESS                                                                                 #####
  -- ##############################################################################################################
  -- ##############################################################################################################
truncate table CM_02_Conditional_Access_Feed;
load table CM_02_Conditional_Access_Feed (
    EPG,
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
    Path'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JasonT/CM feed 2016-09-26 - CA.csv'
SKIP 1
QUOTES ON
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;


-- [!!!] #1 WORKAROUND UNTIL CLARIFIED
delete from CM_02_Conditional_Access_Feed
 where SI_SERVICE_KEY = 3111
   and Name1 = 'DAYSTAR'
   and EPG = 583;
commit;


select * from CM_02_Conditional_Access_Feed;




  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##### VESPA_PROGRAMME_SCHEDULE                                                                           #####
  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Get list of all available Service Keys for the last X days
truncate table CM_03_VESPA_Programme_Schedule_Feed;
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
           where date(Broadcast_Start_Date_Time_Utc) <= today()
             and date(Broadcast_Start_Date_Time_Utc) >= today() - 28                                                                -- [!!!] #4 Parameter to be defined
             and Service_Key >= 1000                                                                                                -- [!!!] #4 Linear channels only
             and Service_Key < 65535
           group by Service_Key, BSS_Code, Broadcast_Start_Date_Time_Utc, Pk_Programme_Instance_Dim) ps;
  -- where BSS_Code_Sequence = 1;
commit;


select * from CM_03_VESPA_Programme_Schedule_Feed;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##### Manual changes                                                                                     #####
  -- ##############################################################################################################
  -- ##############################################################################################################
truncate table CM_04_Manual_Changes_Feed;
load table CM_04_Manual_Changes_Feed (
    User_Notes,
    SERVICE_KEY                  null('.'),
    FULL_NAME                    null('.'),
    EPG_NUMBER                   null('.'),
    EPG_NAME                     null('.'),
    VESPA_NAME                   null('.'),
    CHANNEL_NAME                 null('.'),
    TECHEDGE_NAME                null('.'),
    INFOSYS_NAME                 null('.'),
    BARB_REPORTED                null('.'),
    ACTIVEX                      null('.'),
    CHANNEL_OWNER                null('.'),
    OLD_PACKAGING                null('.'),
    NEW_PACKAGING                null('.'),
    PAY_FREE_INDICATOR           null('.'),
    CHANNEL_GENRE                null('.'),
    CHANNEL_TYPE                 null('.'),
    FORMAT                       null('.'),
    PARENT_SERVICE_KEY           null('.'),
    TIMESHIFT_STATUS             null('.'),
    TIMESHIFT_MINUTES            null('.'),
    RETAIL                       null('.'),
    CHANNEL_REACH                null('.'),
    HD_SWAP_EPG_NUMBER           null('.'),
    SENSITIVE_CHANNEL            null('.'),
    SPOT_SOURCE                  null('.'),
    PROMO_SOURCE                 null('.'),
    NOTES                        null('.'),
    EFFECTIVE_FROM               null('.'),
    EFFECTIVE_TO                 null('.'),
    TYPE_ID                      null('.'),
    UI_DESCR                     null('.'),
    EPG_CHANNEL                  null('.'),
    AMEND_DATE                   null('.'),
    CHANNEL_PACK                 null('.'),
    SERVICE_ATTRIBUTE_VERSION    null('.'),
    PRIMARY_SALES_HOUSE          null('.'),
    CHANNEL_GROUP                null('.'),
    PROVIDER_ID                  null('.'),
    PAY_SKY_SPORTS_FLAG          null('.'),
    PAY_SPORTS_FLAG              null('.'),
    PAY_TV_FLAG                  null('.'),
    KEY_PAY_ENTERTAINMENT_FLAG   null('.'),
    SKY_SPORTS_NEWS_FLAG         null('.'),
    SKY_MOVIES_FLAG              null('.'),
    BT_SPORT_FLAG                null('.'),
    Dummy'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JasonT/CM feed 2016-09-26 - Manual changes.csv'
SKIP 3
QUOTES ON
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;


select * from  CM_04_Manual_Changes_Feed;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##### BARB                                                                                               #####
  -- ##############################################################################################################
  -- ##############################################################################################################
truncate table CM_05_BARB_Feed;
insert into CM_05_BARB_Feed
      (Station_Sequence, Log_Station_Code, DB2_Station_Code, Log_Station_Name, Log_Station_Short_Name, Log_Station_15_Char_Name,
       Reporting_Start_Date, Reporting_End_Date, Sales_House_1, Sales_House_2, Sales_House_3, Sales_House_4, Sales_House_5,
       Sales_House_6, Broadcast_Group_Id, Broadcast_Group_Name, Sales_House_Name)
  select
        rank () over (partition by barb.Log_Station_Name order by barb.Reporting_Start_Date desc, barb.Reporting_End_Date, barb.Area_Geography, barb.Area_Flags) as Station_Sequence,
        barb.Log_Station_Code,
        barb.DB2_Station_Code,
        barb.Log_Station_Name,
        barb.Log_Station_Short_Name,
        barb.Log_Station_15_Char_Name,
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
                a.Log_Station_Code,
                a.DB2_Station_Code,
                a.Log_Station_Name,
                a.Log_Station_Short_Name,
                a.Log_Station_15_Char_Name,
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
            from BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD a
                  left join (select
                                   Broadcast_Group_Id,
                                   Broadcast_Group_Name,
                                   Reporting_Start_Date,
                                   Reporting_End_Date,
                                   rank () over (partition by Broadcast_Group_Id order by Reporting_Start_Date desc, Reporting_End_Date desc, Broadcast_Group_Name) as Group_Sequence
                               from BARB_MASTER_FILE_BROADCAST_GROUP_RECORD) b on a.Broadcast_Group_Id = b.Broadcast_Group_Id
                                                                              and b.Group_Sequence = 1
                  left join (select
                                   Sales_House_Identifier,
                                   Sales_House_Name,
                                   Reporting_Start_Date,
                                   Reporting_End_Date,
                                   rank () over (partition by Sales_House_Identifier order by Reporting_Start_Date desc, Reporting_End_Date desc, Sales_House_Identifier) as Sales_House_Sequence
                               from BARB_MASTER_FILE_SALES_HOUSE_RECORD) c on a.Sales_House_1 = c.Sales_House_Identifier
                                                                          and c.Sales_House_Sequence = 1
         ) barb;
commit;


update CM_05_BARB_Feed base
   set base.Station_Identifier = trim(lower(replace(base.Log_Station_Name, ' ', '')));
commit;


select * from CM_05_BARB_Feed;
select count(*) from CM_05_BARB_Feed;                                                                                               -- [!!!] Diagnostic to be added to check whether there are no duplictes introduced here as there is no PK
select count(*) from BARB_MASTER_FILE_LOG_STATIONS_REPORTING_RECORD;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ##### Landmark                                                                                           #####
  -- ##############################################################################################################
  -- ##############################################################################################################
truncate table CM_06_Landmark_Feed;
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
    SK'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Channel mapping processing/CM feed 2016-05-23 - Landmark.csv'
SKIP 1
QUOTES ON
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;


select * from CM_06_Landmark_Feed;



  -- ##############################################################################################################
  -- ##############################################################################################################





