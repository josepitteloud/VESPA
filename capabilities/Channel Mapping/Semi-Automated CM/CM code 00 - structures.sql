/*###############################################################################
# Created on:   25/07/2016
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  Channel Mapping process - structure creation
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
  -- ##### Global parameters                                                                                  #####
  -- ##############################################################################################################
  -- NOT TO BE USED BUT LEFT COMMENTED OUT DEFINITION TO BE RETAINED FOR REFERENCE
/*
if object_id('CM_00_Process_Global_Parameters') is not null then drop table CM_00_Process_Global_Parameters end if;
create table CM_00_Process_Global_Parameters (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    CM_Parameter_Name                       varchar(255)      not null  default '???',
    CM_Param_Value                          varchar(255)      null      default '???',
    CM_Param_Value__Num                     bigint            null      default 0,
    CM_Param_Value__Decimal                 decimal(15, 6)    null      default 0,
    CM_Param_Value__Date                    date              null      default '1980-01-01',
    CM_Param_Value__Datetime                datetime          null      default '1980-01-01 00:00:00',
    Notes                                   varchar(255)      null      default null
);
create        hg   index idx01 on CM_00_Process_Global_Parameters(CM_Parameter_Name);
grant select on CM_00_Process_Global_Parameters to public;
*/


  -- ##############################################################################################################
  -- ##### Process metadata                                                                                   #####
  -- ##############################################################################################################
if object_id('CM_00_Process_Metadata') is not null then drop table CM_00_Process_Metadata end if;
create table CM_00_Process_Metadata (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    CM_Parameter_Name                       varchar(255)      not null  default '???',
    CM_Param_Value__Char                    varchar(255)      null      default '???',
    CM_Param_Value__Num                     bigint            null      default 0,
    CM_Param_Value__Decimal                 decimal(15, 6)    null      default 0,
    CM_Param_Value__Date                    date              null      default '1980-01-01',
    CM_Param_Value__Datetime                datetime          null      default '1980-01-01 00:00:00',
    Notes                                   varchar(255)      null      default null
);
create        hg   index idx01 on CM_00_Process_Metadata(CM_Parameter_Name);
grant select on CM_00_Process_Metadata to public;



  -- ##############################################################################################################
  -- ##### Tables required to store structured input data                                                     #####
  -- ##############################################################################################################
if object_id('CM_01_Service_Integration_Feed') is not null then drop table CM_01_Service_Integration_Feed end if;
create table CM_01_Service_Integration_Feed (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    SELECT_NUM                              smallint          not null  default 0,
    NAME                                    varchar(255)      not null  default '???',
    UI_DESCR                                varchar(255)      not null  default 0,
    SI_SERVICE_KEY                          bigint            not null  default 0,
    SI_SERVICE_ID                           bigint            not null  default 0,
    CHANNEL_ID                              bigint            null      default null,
    CHANNEL_GROUP_ID                        bigint            null      default null,
    CHANNEL_UI_DESCR                        varchar(255)      null      default null,
    SI_TYPE                                 varchar(255)      not null  default '???',
    GENRE                                   varchar(255)      null      default 'Unknown',
    TRANSPORT_ID                            bigint            not null  default 0,
    NETWORK_ID                              bigint            not null  default 0,
    NOTES                                   varchar(255)      not null  default '???',
    xFormat                                 varchar(25)       not null  default '???',
    xSK_Record_Count                        tinyint           not null  default 0,
    xMin_EPG_Number                         smallint          not null  default 0,
    xMax_EPG_Number                         smallint          not null  default 0,
    xCountry                                varchar(25)       not null  default '???',
    xTimeshift_Minutes                      smallint          not null  default 0,
    xChannel_Flags                          varchar(10)       not null  default '----------'
);
create unique hg   index idx01 on CM_01_Service_Integration_Feed(SELECT_NUM, SI_SERVICE_KEY);
grant select on CM_01_Service_Integration_Feed to public;


if object_id('CM_02_Conditional_Access_Feed') is not null then drop table CM_02_Conditional_Access_Feed end if;
create table CM_02_Conditional_Access_Feed (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    EPG                                     smallint          null      default 0,
    Name1                                   varchar(255)      null      default '???',
    UI_DESCR                                varchar(255)      null      default 0,
    SI_SERVICE_KEY                          bigint            null      default 0,
    Source_Channel                          bigint            null      default 0,
    SI_SERVICE_ID                           bigint            null      default 0,
    TRANSPORT_ID                            bigint            null      default 0,
    Type1                                   varchar(255)      null      default 0,
    GENRE                                   varchar(255)      null      default 0,
    Retail                                  varchar(255)      null      default 0,
    CA                                      varchar(255)      null      default 0,
    Legacy_CA                               varchar(255)      null      default 0,
    Regional                                varchar(255)      null      default 0,
    Regional_Blackout                       varchar(255)      null      default 0,
    Template                                varchar(255)      null      default 0,
    Param1                                  varchar(255)      null      default 0,
    Param2                                  varchar(255)      null      default 0,
    Param3                                  varchar(255)      null      default 0,
    Param4                                  varchar(255)      null      default 0,
    Param5                                  varchar(255)      null      default 0,
    Param6                                  varchar(255)      null      default 0,
    Param7                                  varchar(255)      null      default 0,
    Param8                                  varchar(255)      null      default 0,
    Param9                                  varchar(255)      null      default 0,
    Param10                                 varchar(255)      null      default 0,
    Param11                                 varchar(255)      null      default 0,
    Param12                                 varchar(255)      null      default 0,
    Pairing                                 varchar(255)      null      default 0,
    Taping                                  varchar(255)      null      default 0,
    Status                                  varchar(255)      null      default 0,
    CCI_Value                               bigint            null      default 0,
    Launch_Date                             varchar(255)      null      default 0,
    Comments                                varchar(2048)     null      default 0,
    Commercial_Pack                         varchar(255)      null      default 0,
    Item_Type                               varchar(255)      null      default 0,
    Path                                    varchar(2048)     null      default 0
);
create unique hg   index idx01 on CM_02_Conditional_Access_Feed(SI_SERVICE_KEY);
grant select on CM_02_Conditional_Access_Feed to public;


if object_id('CM_03_VESPA_Programme_Schedule_Feed') is not null then drop table CM_03_VESPA_Programme_Schedule_Feed end if;
create table CM_03_VESPA_Programme_Schedule_Feed (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    Service_Key                             bigint            not null  default 0,
    BSS_Code                                varchar(255)      null      default 'Unknown',
    BSS_Code_Datetime                       datetime          null      default null,
    BSS_Code_Sequence                       smallint          null      default 0
);
create        hg   index idx01 on CM_03_VESPA_Programme_Schedule_Feed(Service_Key);
grant select on CM_03_VESPA_Programme_Schedule_Feed to public;


if object_id('CM_04_Manual_Changes_Feed') is not null then drop table CM_04_Manual_Changes_Feed end if;
create table CM_04_Manual_Changes_Feed (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    User_Notes                              varchar(2048)     null      default null,
    SERVICE_KEY                             int               null      default null,
    FULL_NAME                               varchar(200)      null      default null,
    EPG_NUMBER                              int               null      default null,
    EPG_NAME                                varchar(200)      null      default null,
    VESPA_NAME                              varchar(200)      null      default null,
    CHANNEL_NAME                            varchar(200)      null      default null,
    TECHEDGE_NAME                           varchar(200)      null      default null,
    INFOSYS_NAME                            varchar(200)      null      default null,
    BARB_REPORTED                           varchar(200)      null      default null,
    ACTIVEX                                 varchar(200)      null      default null,
    CHANNEL_OWNER                           varchar(200)      null      default null,
    OLD_PACKAGING                           varchar(200)      null      default null,
    NEW_PACKAGING                           varchar(200)      null      default null,
    PAY_FREE_INDICATOR                      varchar(200)      null      default null,
    CHANNEL_GENRE                           varchar(200)      null      default null,
    CHANNEL_TYPE                            varchar(200)      null      default null,
    FORMAT                                  varchar(200)      null      default null,
    PARENT_SERVICE_KEY                      int               null      default null,
    TIMESHIFT_STATUS                        varchar(200)      null      default null,
    TIMESHIFT_MINUTES                       int               null      default null,
    RETAIL                                  varchar(200)      null      default null,
    CHANNEL_REACH                           varchar(200)      null      default null,
    HD_SWAP_EPG_NUMBER                      int               null      default null,
    SENSITIVE_CHANNEL                       bit               null      default null,
    SPOT_SOURCE                             varchar(200)      null      default null,
    PROMO_SOURCE                            varchar(200)      null      default null,
    NOTES                                   varchar(200)      null      default null,
    EFFECTIVE_FROM                          timestamp         null      default null,
    EFFECTIVE_TO                            timestamp         null      default null,
    TYPE_ID                                 int               null      default null,
    UI_DESCR                                varchar(200)      null      default null,
    EPG_CHANNEL                             varchar(200)      null      default null,
    AMEND_DATE                              date              null      default null,
    CHANNEL_PACK                            varchar(200)      null      default null,
    SERVICE_ATTRIBUTE_VERSION               int               null      default null,
    PRIMARY_SALES_HOUSE                     varchar(200)      null      default null,
    CHANNEL_GROUP                           varchar(200)      null      default null,
    PROVIDER_ID                             varchar(25)       null      default null,
    PAY_SKY_SPORTS_FLAG                     varchar(3)        null      default null,
    PAY_SPORTS_FLAG                         varchar(3)        null      default null,
    PAY_TV_FLAG                             varchar(3)        null      default null,
    KEY_PAY_ENTERTAINMENT_FLAG              varchar(3)        null      default null,
    SKY_SPORTS_NEWS_FLAG                    varchar(3)        null      default null,
    SKY_MOVIES_FLAG                         varchar(3)        null      default null,
    BT_SPORT_FLAG                           varchar(3)        null      default null,
    Dummy                                   varchar(1)        null      default null
);
create unique lf   index idx01 on CM_04_Manual_Changes_Feed(SERVICE_KEY);
grant select on CM_04_Manual_Changes_Feed to public;


if object_id('CM_05_BARB_Feed') is not null then drop table CM_05_BARB_Feed end if;
create table CM_05_BARB_Feed (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    Service_Key                             int               not null,
    Station_Sequence                        smallint          null      default null,
    Station_Identifier                      varchar(255)      null      default '',
    Log_Station_Code                        smallint          null      default null,
    DB2_Station_Code                        smallint          null      default null,
    Log_Station_Name                        varchar(255)      null      default null,
    Log_Station_Short_Name                  varchar(255)      null      default null,
    Log_Station_15_Char_Name                varchar(255)      null      default null,
    Reporting_Start_Date                    date              null      default null,
    Reporting_End_Date                      date              null      default null,
    Sales_House_1                           smallint          null      default null,
    Sales_House_2                           smallint          null      default null,
    Sales_House_3                           smallint          null      default null,
    Sales_House_4                           smallint          null      default null,
    Sales_House_5                           smallint          null      default null,
    Sales_House_6                           smallint          null      default null,
    Broadcast_Group_Id                      smallint          null      default null,
    Broadcast_Group_Name                    varchar(255)      null      default null,
    Sales_House_Name                        varchar(255)      null      default null

);
create        lf   index idx01 on CM_05_BARB_Feed(Service_Key);
create        lf   index idx02 on CM_05_BARB_Feed(Log_Station_Code);
grant select on CM_05_BARB_Feed to public;


if object_id('CM_06_Landmark_Feed') is not null then drop table CM_06_Landmark_Feed end if;
create table CM_06_Landmark_Feed (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    MEDIA_SALES_AREA_PK                     bigint            null      default null,
    MEDIA_SALES_AREA_NUMBER                 bigint            null      default null,
    CURRENCY_CODE                           varchar(10)       null      default null,
    DEFAULT_MINUTAGE                        smallint          null      default null,
    BASE_DEMO_NO                            smallint          null      default null,
    SECONDARY_BASE_DEMO_NO                  smallint          null      default null,
    RATING_SUPPLIER_CODE                    varchar(10)       null      default null,
    BASE_LENGTH                             smallint          null      default null,
    STATION_PRICE                           varchar(20)       null      default null,
    MEDIA_SALES_AREA_CODE                   varchar(10)       null      default null,
    MEDIA_SALES_AREA_NAME                   varchar(255)      null      default null,
    MEDIA_SALES_AREA_SHORT_NAME             varchar(255)      null      default null,
    MEDIA_SALES_AREA_DESCRIPTION            varchar(255)      null      default null,
    TX_START_DATE                           bigint            null      default null,
    TX_END_DATE                             bigint            null      default null,
    IA_START_DATE                           bigint            null      default null,
    IA_END_DATE                             bigint            null      default null,
    MEDIA_TARGET_SALES_AREA_CODE            bigint            null      default null,
    MEDIA_TARGET_SALES_AREA_NAME            varchar(255)      null      default null,
    MEDIA_TARGET_SALES_AREA_SHORT_NAME      varchar(255)      null      default null,
    MEDIA_TARGET_SALES_AREA_SHORT_CODE      varchar(10)       null      default null,
    EFFECTIVE_FROM                          datetime          null      default null,
    EFFECTIVE_TO                            datetime          null      default null,
    CURRENT_DIM                             smallint          null      default null,
    CREATE_DATE                             datetime          null      default null,
    UPDATE_DATE                             datetime          null      default null,
    LOAD_ID                                 varchar(10)       null      default null,
    Active                                  varchar(10)       null      default null,
    Sky                                     varchar(10)       null      default null,
    SK                                      varchar(10)       null      default null
);
grant select on CM_06_Landmark_Feed to public;



  -- ##############################################################################################################
  -- ##### Result tables                                                                                      #####
  -- ##############################################################################################################
if object_id('CM_20_Final_Channel_Mapping_Data') is not null then drop table CM_20_Final_Channel_Mapping_Data end if;
create table CM_20_Final_Channel_Mapping_Data (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    SERVICE_KEY                             int               not null,                         -- SERVICE INTEGRATION
    FULL_NAME                               varchar(200)      not null  default 'Unknown',      -- SERVICE INTEGRATION
    EPG_NUMBER                              int               not null  default -1,             -- SERVICE INTEGRATION
    EPG_NAME                                varchar(200)      not null  default 'Unknown',      -- SERVICE INTEGRATION
    VESPA_NAME                              varchar(200)      not null  default 'Unknown',      -- SERVICE INTEGRATION
    CHANNEL_NAME                            varchar(200)      not null  default 'Unknown',      -- SERVICE INTEGRATION
    TECHEDGE_NAME                           varchar(200)      not null  default 'Unknown',      -- (not maintained)
    INFOSYS_NAME                            varchar(200)      not null  default '',             -- (not maintained)
    BARB_REPORTED                           varchar(200)      not null  default 'NO',           -- BARB
    ACTIVEX                                 varchar(200)      not null,                         -- (calculated)
    CHANNEL_OWNER                           varchar(200)      not null  default '',             -- BARB
    OLD_PACKAGING                           varchar(200)      not null  default '',             -- (not maintained)
    NEW_PACKAGING                           varchar(200)      not null  default '',             -- (not maintained)
    PAY_FREE_INDICATOR                      varchar(200)      not null  default '',             -- (derived)
    CHANNEL_GENRE                           varchar(200)      not null  default '',             -- SERVICE INTEGRATION
    CHANNEL_TYPE                            varchar(200)      not null  default '',             -- CONDITIONAL ACCESS
    FORMAT                                  varchar(200)      not null  default '',             -- SERVICE INTEGRATION
    PARENT_SERVICE_KEY                      int               not null  default -1,             -- (derived)
    TIMESHIFT_STATUS                        varchar(200)      not null  default '',             -- (derived)
    TIMESHIFT_MINUTES                       int               not null  default 0,              -- SERVICE INTEGRATION
    RETAIL                                  varchar(200)      not null  default '',             -- (not maintained)
    CHANNEL_REACH                           varchar(200)      not null  default '',             -- (derived)
    HD_SWAP_EPG_NUMBER                      int               not null  default -1,             -- (derived)
    SENSITIVE_CHANNEL                       bit               not null  default 0,              -- (derived)
    SPOT_SOURCE                             varchar(200)      not null  default 'None',         -- BARB
    PROMO_SOURCE                            varchar(200)      not null  default 'None',         -- ???
    NOTES                                   varchar(200)      not null  default '',             -- (not maintained)
    EFFECTIVE_FROM                          timestamp         not null,                         -- (calculated)
    EFFECTIVE_TO                            timestamp         not null,                         -- (calculated)
    TYPE_ID                                 int               not null  default -1,             -- (derived)
    UI_DESCR                                varchar(200)      not null  default '',             -- (not maintained)
    EPG_CHANNEL                             varchar(200)      not null  default '',             -- (not maintained)
    AMEND_DATE                              date              not null,                         -- (calculated)
    CHANNEL_PACK                            varchar(200)      not null  default '',             -- (not maintained)
    SERVICE_ATTRIBUTE_VERSION               int               not null,                         -- (calculated)
    PRIMARY_SALES_HOUSE                     varchar(200)      not null  default '',             -- BARB
    CHANNEL_GROUP                           varchar(200)      not null  default '',             -- (not maintained)
    PROVIDER_ID                             varchar(25)       not null  default '',             -- (not maintained)
    PAY_SKY_SPORTS_FLAG                     varchar(3)        not null  default 'No',           -- (derived)
    PAY_SPORTS_FLAG                         varchar(3)        not null  default 'No',           -- (derived)
    PAY_TV_FLAG                             varchar(3)        not null  default 'No',           -- (derived)
    KEY_PAY_ENTERTAINMENT_FLAG              varchar(3)        not null  default 'No',           -- (derived)
    SKY_SPORTS_NEWS_FLAG                    varchar(3)        not null  default 'No',           -- (derived)
    SKY_MOVIES_FLAG                         varchar(3)        not null  default 'No',           -- (derived)
    BT_SPORT_FLAG                           varchar(3)        not null  default 'No',           -- (derived)

    xSI_Match_Flag                          bit               not null  default 0,              --
    xCA_Match_Flag                          bit               not null  default 0,              --
    xBARB_Match_Flag                        bit               not null  default 0,              --
    xLandmark_Match_Flag                    bit               not null  default 0,              --
    xSK_Automated_Flag                      bit               not null  default 0,              --

      -- ### Extra fields used in derivations ###
      -- SI
    xCHANNEL_GROUP_ID                       bigint            null      default null,           --
    xCountry                                varchar(25)       not null  default '???',          --
    xChannel_Flags                          varchar(10)       not null  default '----------',   --

      -- CM_03_VESPA_Programme_Schedule_Feed
    xBSS_Code                               varchar(255)      null      default 'Unknown',

      -- BARB
    xSales_House_1                          smallint          null      default null,
    xSales_House_2                          smallint          null      default null,
    xSales_House_3                          smallint          null      default null,
    xSales_House_4                          smallint          null      default null,
    xSales_House_5                          smallint          null      default null,
    xSales_House_6                          smallint          null      default null,
    xBroadcast_Group_Id                     smallint          null      default null,
    xBroadcast_Group_Name                   varchar(255)      null      default '',
    xSales_House_Name                       varchar(255)      null      default '',


);
create unique lf   index idx01 on CM_20_Final_Channel_Mapping_Data(SERVICE_KEY);
grant select on CM_20_Final_Channel_Mapping_Data to public;



if object_id('CM_21_Channel_Statuses') is not null then drop table CM_21_Channel_Statuses end if;
create table CM_21_Channel_Statuses (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    Run_Date                                date              null      default null,
    Service_key                             int               null      default null,
    SK_Automated_Flag                       smallint          not null  default 1,                --  -1 -> unknown, 0 -> No, 1 -> Yes
    Current_CM_Flag                         bit               null      default 0,
    New_CM_Flag                             bit               null      default 0,
    Action                                  varchar(50)       null      default 'Not maintained',

    Current_Effective_From                  datetime          null      default null,
    Current_Effective_To                    datetime          null      default null,
    New_Effective_From                      datetime          null      default null,
    New_Effective_To                        datetime          null      default null
);
create unique lf   index idx01 on CM_21_Channel_Statuses(SERVICE_KEY);
grant select on CM_21_Channel_Statuses to public;



if object_id('CM_22_Change_Details') is not null then drop table CM_22_Change_Details end if;
create table CM_22_Change_Details (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Data
    Run_Date                                date              null      default null,
    Service_key                             int               null      default null,
    Channel_Name                            varchar(50)       null      default '???',
    Action                                  varchar(50)       null      default '???',
    Field                                   varchar(50)       null      default '???',
    Current_Value                           varchar(255)      null      default '???',
    New_Value                               varchar(255)      null      default '???',
    Effective_From_Date                     date              null      default null,
    Effective_To_Date                       date              null      default null,
    Field_Position                          smallint          null      default 0
);
create        hg   index idx01 on CM_22_Change_Details(Service_key, Field);
grant select on CM_22_Change_Details to public;



if object_id('CM_23_Prod_Channel_Map_History') is not null then drop table CM_23_Prod_Channel_Map_History end if;
create table CM_23_Prod_Channel_Map_History (
    SERVICE_KEY                             int               not null,
    FULL_NAME                               varchar(200)      not null  default 'Unknown',
    EPG_NUMBER                              int               not null  default -1,
    EPG_NAME                                varchar(200)      not null  default 'Unknown',
    VESPA_NAME                              varchar(200)      not null  default 'Unknown',
    CHANNEL_NAME                            varchar(200)      not null  default 'Unknown',
    TECHEDGE_NAME                           varchar(200)      not null  default 'Unknown',
    INFOSYS_NAME                            varchar(200)      not null  default '',
    BARB_REPORTED                           varchar(200)      not null  default 'NO',
    ACTIVEX                                 varchar(200)      not null,
    CHANNEL_OWNER                           varchar(200)      not null  default '',
    OLD_PACKAGING                           varchar(200)      not null  default '',
    NEW_PACKAGING                           varchar(200)      not null  default '',
    PAY_FREE_INDICATOR                      varchar(200)      not null  default '',
    CHANNEL_GENRE                           varchar(200)      not null  default '',
    CHANNEL_TYPE                            varchar(200)      not null  default '',
    FORMAT                                  varchar(200)      not null  default '',
    PARENT_SERVICE_KEY                      int               not null  default -1,
    TIMESHIFT_STATUS                        varchar(200)      not null  default '',
    TIMESHIFT_MINUTES                       int               not null  default 0,
    RETAIL                                  varchar(200)      not null  default '',
    CHANNEL_REACH                           varchar(200)      not null  default '',
    HD_SWAP_EPG_NUMBER                      int               not null  default -1,
    SENSITIVE_CHANNEL                       bit               not null  default 0,
    SPOT_SOURCE                             varchar(200)      not null  default 'None',
    PROMO_SOURCE                            varchar(200)      not null  default 'None',
    NOTES                                   varchar(200)      not null  default '',
    EFFECTIVE_FROM                          timestamp         not null,
    EFFECTIVE_TO                            timestamp         not null,
    TYPE_ID                                 int               not null  default -1,
    UI_DESCR                                varchar(200)      not null  default '',
    EPG_CHANNEL                             varchar(200)      not null  default '',
    AMEND_DATE                              date              not null,
    CHANNEL_PACK                            varchar(200)      not null  default '',
    SERVICE_ATTRIBUTE_VERSION               int               not null,
    PRIMARY_SALES_HOUSE                     varchar(200)      not null  default '',
    CHANNEL_GROUP                           varchar(200)      not null  default '',
    PROVIDER_ID                             varchar(25)       not null  default '',
    PAY_SKY_SPORTS_FLAG                     varchar(3)        not null  default 'No',
    PAY_SPORTS_FLAG                         varchar(3)        not null  default 'No',
    PAY_TV_FLAG                             varchar(3)        not null  default 'No',
    KEY_PAY_ENTERTAINMENT_FLAG              varchar(3)        not null  default 'No',
    SKY_SPORTS_NEWS_FLAG                    varchar(3)        not null  default 'No',
    SKY_MOVIES_FLAG                         varchar(3)        not null  default 'No',
    BT_SPORT_FLAG                           varchar(3)        not null  default 'No'
    );
create        lf   index idx01 on CM_23_Prod_Channel_Map_History(SERVICE_KEY);
create        dttm index idx02 on CM_23_Prod_Channel_Map_History(EFFECTIVE_FROM);
create        dttm index idx03 on CM_23_Prod_Channel_Map_History(EFFECTIVE_TO);
grant select on CM_23_Prod_Channel_Map_History to public;

      if object_id('CM_24_Run_Log') is not null then drop table CM_24_Run_Log end if;
  create table CM_24_Run_Log (
         run_date date
        ,msg      varchar(200)
        );
   grant select on CM_24_Run_Log to public;


  -- ##############################################################################################################
  -- ##############################################################################################################


























