/*###############################################################################
# Created on:   24/05/2017
# Created by:   Alan Barber (AB)
# Description:  Channel Mapping process - process VOD Provider IDs and maintenance
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 24/05/2017  AB   Initial version
# xx/xx/xxxx  xx   xxxxxxxxxxxxx
###############################################################################*/

-- exec CM_07_VOD_Feed_Load 1, today(), 0

-- ##############################################################################################################
-- #####   CREATE Load Table                                                                                #####
-- ##############################################################################################################

drop procedure CM_07_VOD_Feed_Load;

create procedure CM_07_VOD_Feed_Load
    @parGreenLightToRun         smallint = 0,  
    @parAnyFeedDate             date = null,                        -- Reference date for all other feeds
    @result                     smallint output
as
begin

    if object_id('CM_07_Vod_Feed') is not null
    begin
        DROP TABLE CM_07_Vod_Feed commit 
    end

    CREATE TABLE "CM_07_Vod_Feed" (
            "SERVICE_KEY"                   int             NOT NULL DEFAULT -1,
            "FULL_NAME"                     varchar(200)    NOT NULL DEFAULT 'Unknown',
            "EPG_NUMBER"                    int             NOT NULL DEFAULT -1,
            "EPG_NAME"                      varchar(200)    NOT NULL DEFAULT 'Unknown',
            "VESPA_NAME"                    varchar(200)    NOT NULL DEFAULT 'Unknown',
            "CHANNEL_NAME"                  varchar(200)    NOT NULL DEFAULT 'Unknown',
            "TECHEDGE_NAME"                 varchar(200)    NOT NULL DEFAULT 'Unknown',
            "INFOSYS_NAME"                  varchar(200)    NOT NULL DEFAULT '',
            "BARB_REPORTED"                 varchar(200)    NOT NULL DEFAULT 'NO',
            "ACTIVEX"                       varchar(200)    NOT NULL DEFAULT 'N',
            "CHANNEL_OWNER"                 varchar(200)    NOT NULL DEFAULT '',
            "OLD_PACKAGING"                 varchar(200)    NOT NULL DEFAULT '',
            "NEW_PACKAGING"                 varchar(200)    NOT NULL DEFAULT '',
            "PAY_FREE_INDICATOR"            varchar(200)    NOT NULL DEFAULT '',
            "CHANNEL_GENRE"                 varchar(200)    NOT NULL DEFAULT '',
            "CHANNEL_TYPE"                  varchar(200)    NOT NULL DEFAULT '',
            "FORMAT"                        varchar(200)    NOT NULL DEFAULT '',
            "PARENT_SERVICE_KEY"            int             NOT NULL DEFAULT -1,
            "TIMESHIFT_STATUS"              varchar(200)    NOT NULL DEFAULT '',
            "TIMESHIFT_MINUTES"             int             NOT NULL DEFAULT 0,
            "RETAIL"                        varchar(200)    NOT NULL DEFAULT '',
            "CHANNEL_REACH"                 varchar(200)    NOT NULL DEFAULT '',
            "HD_SWAP_EPG_NUMBER"            int             NOT NULL DEFAULT -1,
            "SENSITIVE_CHANNEL"             bit             NOT NULL DEFAULT 0,
            "SPOT_SOURCE"                   varchar(200)    NOT NULL DEFAULT 'None',
            "PROMO_SOURCE"                  varchar(200)    NOT NULL DEFAULT 'None',
            "NOTES"                         varchar(200)    NOT NULL DEFAULT '',
            "EFFECTIVE_FROM"                timestamp       NOT NULL DEFAULT '2999-12-31',
            "EFFECTIVE_TO"                  timestamp       NOT NULL DEFAULT '2999-12-31',
            "TYPE_ID"                       int             NOT NULL DEFAULT -1,
            "UI_DESCR"                      varchar(200)    NOT NULL DEFAULT '',
            "EPG_CHANNEL"                   varchar(200)    NOT NULL DEFAULT '',
            "AMEND_DATE"                    date            NOT NULL DEFAULT '2999-12-31',
            "CHANNEL_PACK"                  varchar(200)    NOT NULL DEFAULT '',
            "SERVICE_ATTRIBUTE_VERSION"     int             NOT NULL DEFAULT 9999,
            "PRIMARY_SALES_HOUSE"           varchar(200)    NOT NULL DEFAULT '',
            "CHANNEL_GROUP"                 varchar(200)    NOT NULL DEFAULT '',
            "PROVIDER_ID"                   varchar(25)     NOT NULL DEFAULT '',
            "PAY_SKY_SPORTS_FLAG"           varchar(3)      NOT NULL DEFAULT 'No',
            "PAY_SPORTS_FLAG"               varchar(3)      NOT NULL DEFAULT 'No',
            "PAY_TV_FLAG"                   varchar(3)      NOT NULL DEFAULT 'No',
            "KEY_PAY_ENTERTAINMENT_FLAG"    varchar(3)      NOT NULL DEFAULT 'No',
            "SKY_SPORTS_NEWS_FLAG"          varchar(3)      NOT NULL DEFAULT 'No',
            "SKY_MOVIES_FLAG"               varchar(3)      NOT NULL DEFAULT 'No',
            "BT_SPORT_FLAG"                 varchar(3)      NOT NULL DEFAULT 'No'
    )
    grant select on CM_07_Vod_Feed to public
    commit
  
    declare @varSQL text

    SET @varSQL = 
    'load table CM_07_Vod_Feed (
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
        BT_SPORT_FLAG''\n''
    )
        FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Channel_Mapping_Process/_Process_inputs_/CM_feed_##^1^##_VOD.csv''
        SKIP 1
        QUOTES ON
        ESCAPES OFF
        DELIMITED BY '',''
    '
        

    execute( replace(@varSQL, '##^1^##', @parAnyFeedDate) )
    commit

   
    --check what attribute version should be used here
    declare @service_attribute_version integer

    select @service_attribute_version = max(service_attribute_version)+1 -- <-- +1 for this build
      from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES


    --update ss of effective_from represents the CM release
    UPDATE CM_07_Vod_Feed
       SET effective_from = dateadd(ss, @service_attribute_version, dateformat(Effective_From, 'YYYY-MM-DD 06:00:00')) ,
             effective_to = dateformat(Effective_From, '2999-12-31 00:00:00')
    commit
    
    IF (select max(service_attribute_version) from CM_23_Prod_Channel_Map_History) != @service_attribute_version
    BEGIN
        insert into CM_24_Run_Log(run_date, msg)
        values(today(), 
              '['||now()||'] CM_031_VOD_Feed_Load: Attribute version in table: CM_23_Prod_Channel_Map_History is not aligned. Should be v'||@service_attribute_version)
        commit 
    END

    --delete VOD service keys that are not already in prod CM, this is just incase we run this proc a 2nd time as a standalone proc (eliminates potential insertion of dups)
    DELETE CM_23_Prod_Channel_Map_History x  
     where x.service_key > (select max(service_key)
                              from vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
                             where service_key < 1000)
       and x.service_key <1000
    commit   
  

    /***** Insert into Channel Mapping proper.. to be picked up by the usual publishing process
    ******/
    INSERT into CM_23_Prod_Channel_Map_History
    select *
      from CM_07_Vod_Feed
    commit

    insert into CM_24_Run_Log(run_date, msg)
       values(today(), 
              '['||now()||'] CM_031_VOD_Feed_Load: Inserted('||@@rowcount||') new provider IDs')
    commit          






    /******************

    NOW we need to add in the change details section
    ******************/
      if object_id('CM_22_VOD_Change_Details') is not null
        begin
            DROP TABLE CM_22_VOD_Change_Details commit 
        end
    
      select top 1 *
        into --barbera.CM_22_Change_Details_vod_additions
             CM_22_VOD_Change_Details
        from CM_22_Change_Details
      commit

      truncate table CM_22_VOD_Change_Details commit

      INSERT INTO CM_22_VOD_Change_Details(
      Updated_On, Updated_By, Run_Date, Service_key, Channel_Name, "Action", Field, Current_Value, New_Value, Effective_From_Date, Effective_To_Date, Field_Position)
      select now(), user_name(), today(), service_key , channel_name, 'New', 'SERVICE KEY', '(n/a)', cast(service_key as varchar(4)), Effective_From, Effective_To, 0
        from --CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_vod_additions
             CM_07_VOD_Feed
      commit

--select *
--  from CM_22_VOD_Change_Details


    return 1

end;


/* ## TESTS -----------------------------------------------------------

select *
from CM_07_Vod_Feed

select *
from CM_23_Prod_Channel_Map_History

*/
