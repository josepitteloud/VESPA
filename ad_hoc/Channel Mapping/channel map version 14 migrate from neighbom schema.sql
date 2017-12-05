-------------------------
-- SERVICE_KEY_ATTRIBUTES
-------------------------

-- production

-- backingup...
select *
into    "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES_BKP"
from    "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES";
commit;


if object_id('VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES"
(
        "Service_key"        int DEFAULT NULL,
        "FULL_NAME"          varchar(255) DEFAULT NULL,
        "EPG_NUMBER"         int DEFAULT NULL,
        "EPG_NAME"           varchar(255) DEFAULT NULL,
        "VESPA_NAME"         varchar(255) DEFAULT NULL,
        "CHANNEL_NAME"       varchar(255) DEFAULT NULL,
        "TECHEDGE_NAME"      varchar(255) DEFAULT NULL,
        "INFOSYS_NAME"       varchar(255) DEFAULT NULL,
        "BARB_REPORTED"      varchar(255) DEFAULT NULL,
        "ACTIVE"             varchar(255) DEFAULT NULL,
        "CHANNEL_OWNER"      varchar(255) DEFAULT NULL,
        "OLD_PACKAGING"      varchar(255) DEFAULT NULL,
        "NEW_PACKAGING"      varchar(255) DEFAULT NULL,
        "PAY_FREE_INDICATOR" varchar(255) DEFAULT NULL,
        "CHANNEL_GENRE"      varchar(255) DEFAULT NULL,
        "CHANNEL_TYPE"       varchar(255) DEFAULT NULL,
        "FORMAT"             varchar(255) DEFAULT NULL,
        "parent_service_key" int DEFAULT NULL,
        "TIMESHIFT_STATUS"   varchar(255) DEFAULT NULL,
        "TIMESHIFT_MINUTES"  int DEFAULT NULL,
        "RETAIL"             varchar(255) DEFAULT NULL,
        "CHANNEL_REACH"      varchar(255) DEFAULT NULL,
        "HD_SWAP_EPG_NUMBER" int DEFAULT NULL,
        "SENSITIVE_CHANNEL"  bit NOT NULL DEFAULT NULL,
        "SPOT_SOURCE"        varchar(255) DEFAULT NULL,
        "PROMO_SOURCE"       varchar(255) DEFAULT NULL,
        "NOTES"              varchar(255) DEFAULT NULL,
        "EFFECTIVE_FROM"     date DEFAULT NULL,
        "EFFECTIVE_TO"       date DEFAULT NULL,
        "type_id"            int DEFAULT NULL,
        "UI_DESCR"           varchar(255) DEFAULT NULL,
        "EPG_CHANNEL"        varchar(255) DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "channel_pack"       varchar(30) DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "primary_sales_house" varchar(255) DEFAULT NULL,
        "channel_group"      varchar(255) DEFAULT NULL
)
;

-- dev

-- backing up
select  *
into    "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES_BKP"
from    "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES";
commit;

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES" (
        "Service_key"        int DEFAULT NULL,
        "FULL_NAME"          varchar(255) DEFAULT NULL,
        "EPG_NUMBER"         int DEFAULT NULL,
        "EPG_NAME"           varchar(255) DEFAULT NULL,
        "VESPA_NAME"         varchar(255) DEFAULT NULL,
        "CHANNEL_NAME"       varchar(255) DEFAULT NULL,
        "TECHEDGE_NAME"      varchar(255) DEFAULT NULL,
        "INFOSYS_NAME"       varchar(255) DEFAULT NULL,
        "BARB_REPORTED"      varchar(255) DEFAULT NULL,
        "ACTIVE"             varchar(255) DEFAULT NULL,
        "CHANNEL_OWNER"      varchar(255) DEFAULT NULL,
        "OLD_PACKAGING"      varchar(255) DEFAULT NULL,
        "NEW_PACKAGING"      varchar(255) DEFAULT NULL,
        "PAY_FREE_INDICATOR" varchar(255) DEFAULT NULL,
        "CHANNEL_GENRE"      varchar(255) DEFAULT NULL,
        "CHANNEL_TYPE"       varchar(255) DEFAULT NULL,
        "FORMAT"             varchar(255) DEFAULT NULL,
        "parent_service_key" int DEFAULT NULL,
        "TIMESHIFT_STATUS"   varchar(255) DEFAULT NULL,
        "TIMESHIFT_MINUTES"  int DEFAULT NULL,
        "RETAIL"             varchar(255) DEFAULT NULL,
        "CHANNEL_REACH"      varchar(255) DEFAULT NULL,
        "HD_SWAP_EPG_NUMBER" int DEFAULT NULL,
        "SENSITIVE_CHANNEL"  bit NOT NULL DEFAULT NULL,
        "SPOT_SOURCE"        varchar(255) DEFAULT NULL,
        "PROMO_SOURCE"       varchar(255) DEFAULT NULL,
        "NOTES"              varchar(255) DEFAULT NULL,
        "EFFECTIVE_FROM"     date DEFAULT NULL,
        "EFFECTIVE_TO"       date DEFAULT NULL,
        "type_id"            int DEFAULT NULL,
        "UI_DESCR"           varchar(255) DEFAULT NULL,
        "EPG_CHANNEL"        varchar(255) DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "channel_pack"       varchar(30) DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "primary_sales_house" varchar(255) DEFAULT NULL,
        "channel_group"      varchar(255) DEFAULT NULL
)
;

-- populate dev version

INSERT INTO VESPA_ANALYSTS.channel_map_dev_service_key_attributes (
        Service_key,
        FULL_NAME,
        EPG_NUMBER,
        EPG_NAME,
        VESPA_NAME,
        CHANNEL_NAME,
        TECHEDGE_NAME,
        INFOSYS_NAME,
        BARB_REPORTED,
        ACTIVE,CHANNEL_OWNER,
        OLD_PACKAGING,
        NEW_PACKAGING,
        PAY_FREE_INDICATOR,
        CHANNEL_GENRE,
        CHANNEL_TYPE,
        FORMAT,
        parent_service_key,
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
        type_id,
        UI_DESCR,
        EPG_CHANNEL,
        amend_date,
        channel_pack,
        version,
        primary_sales_house,
        channel_group)
SELECT  Service_key,
        FULL_NAME,
        EPG_NUMBER,
        EPG_NAME,
        VESPA_NAME,
        CHANNEL_NAME,
        TECHEDGE_NAME,
        INFOSYS_NAME,
        BARB_REPORTED,
        ACTIVE,CHANNEL_OWNER,
        OLD_PACKAGING,
        NEW_PACKAGING,
        PAY_FREE_INDICATOR,
        CHANNEL_GENRE,
        CHANNEL_TYPE,
        FORMAT,
        parent_service_key,
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
        type_id,
        UI_DESCR,
        EPG_CHANNEL,
        amend_date,
        channel_pack,
        version,
        primary_sales_house,
        channel_group
FROM neighbom.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES;
commit;
-- archive

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES') is not null
drop table VESPA_ANALYSTS.CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES;

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES" (
        "Service_key"        int DEFAULT NULL,
        "FULL_NAME"          varchar(255) DEFAULT NULL,
        "EPG_NUMBER"         int DEFAULT NULL,
        "EPG_NAME"           varchar(255) DEFAULT NULL,
        "VESPA_NAME"         varchar(255) DEFAULT NULL,
        "CHANNEL_NAME"       varchar(255) DEFAULT NULL,
        "TECHEDGE_NAME"      varchar(255) DEFAULT NULL,
        "INFOSYS_NAME"       varchar(255) DEFAULT NULL,
        "BARB_REPORTED"      varchar(255) DEFAULT NULL,
        "ACTIVE"             varchar(255) DEFAULT NULL,
        "CHANNEL_OWNER"      varchar(255) DEFAULT NULL,
        "OLD_PACKAGING"      varchar(255) DEFAULT NULL,
        "NEW_PACKAGING"      varchar(255) DEFAULT NULL,
        "PAY_FREE_INDICATOR" varchar(255) DEFAULT NULL,
        "CHANNEL_GENRE"      varchar(255) DEFAULT NULL,
        "CHANNEL_TYPE"       varchar(255) DEFAULT NULL,
        "FORMAT"             varchar(255) DEFAULT NULL,
        "parent_service_key" int DEFAULT NULL,
        "TIMESHIFT_STATUS"   varchar(255) DEFAULT NULL,
        "TIMESHIFT_MINUTES"  int DEFAULT NULL,
        "RETAIL"             varchar(255) DEFAULT NULL,
        "CHANNEL_REACH"      varchar(255) DEFAULT NULL,
        "HD_SWAP_EPG_NUMBER" int DEFAULT NULL,
        "SENSITIVE_CHANNEL"  bit NOT NULL DEFAULT NULL,
        "SPOT_SOURCE"        varchar(255) DEFAULT NULL,
        "PROMO_SOURCE"       varchar(255) DEFAULT NULL,
        "NOTES"              varchar(255) DEFAULT NULL,
        "EFFECTIVE_FROM"     date DEFAULT NULL,
        "EFFECTIVE_TO"       date DEFAULT NULL,
        "type_id"            int DEFAULT NULL,
        "UI_DESCR"           varchar(255) DEFAULT NULL,
        "EPG_CHANNEL"        varchar(255) DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "channel_pack"       varchar(30) DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "primary_sales_house" varchar(255) DEFAULT NULL,
        "channel_group"      varchar(255) DEFAULT NULL
)
;

-- populate archive version

INSERT INTO VESPA_ANALYSTS.channel_map_arc_service_key_attributes (
        Service_key,
        FULL_NAME,
        EPG_NUMBER,
        EPG_NAME,
        VESPA_NAME,
        CHANNEL_NAME,
        TECHEDGE_NAME,
        INFOSYS_NAME,
        BARB_REPORTED,
        ACTIVE,CHANNEL_OWNER,
        OLD_PACKAGING,
        NEW_PACKAGING,
        PAY_FREE_INDICATOR,
        CHANNEL_GENRE,
        CHANNEL_TYPE,
        FORMAT,
        parent_service_key,
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
        type_id,
        UI_DESCR,
        EPG_CHANNEL,
        amend_date,
        channel_pack,
        version,
        primary_sales_house,
        channel_group)
SELECT  Service_key,
        FULL_NAME,
        EPG_NUMBER,
        EPG_NAME,
        VESPA_NAME,
        CHANNEL_NAME,
        TECHEDGE_NAME,
        INFOSYS_NAME,
        BARB_REPORTED,
        ACTIVE,CHANNEL_OWNER,
        OLD_PACKAGING,
        NEW_PACKAGING,
        PAY_FREE_INDICATOR,
        CHANNEL_GENRE,
        CHANNEL_TYPE,
        FORMAT,
        parent_service_key,
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
        type_id,
        UI_DESCR,
        EPG_CHANNEL,
        amend_date,
        channel_pack,
        version,
        primary_sales_house,
        channel_group
FROM neighbom.CHANNEL_MAP_arc_SERVICE_KEY_ATTRIBUTES;
commit;
-- service_key_barb

-- Production

--backing up
select  *
into    VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB_BKP
from    VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB;
COMMIT;

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB" (
        "service_key"        int DEFAULT NULL,
        "log_station_code"   int DEFAULT NULL,
        "STI_code"           int DEFAULT NULL,
        "panel_code"         int DEFAULT NULL,
        "promo_panel_code"   int DEFAULT NULL,
        "effective_from"     date DEFAULT NULL,
        "effective_to"       date DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "AP04_version"       varchar(10) DEFAULT NULL,
        "AP04_date"          date DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "dummy_barb_code"    varchar(3) DEFAULT NULL
)
;

-- backing up
select  *
into    "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_BARB_BKP"
from    "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_BARB";
commit;

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_BARB') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_BARB";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_BARB" (
        "service_key"        int DEFAULT NULL,
        "log_station_code"   int DEFAULT NULL,
        "STI_code"           int DEFAULT NULL,
        "panel_code"         int DEFAULT NULL,
        "promo_panel_code"   int DEFAULT NULL,
        "effective_from"     date DEFAULT NULL,
        "effective_to"       date DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "AP04_version"       varchar(10) DEFAULT NULL,
        "AP04_date"          date DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "dummy_barb_code"    varchar(3) DEFAULT NULL
)
;

-- populate dev

INSERT INTO VESPA_ANALYSTS.channel_map_dev_service_key_barb (
        service_key,
        log_station_code,
        STI_code,
        panel_code,
        promo_panel_code,
        effective_from,
        effective_to,
        amend_date,
        AP04_version,
        version,
        dummy_barb_code)
SELECT  service_key,
        log_station_code,
        STI_code,
        panel_code,
        promo_panel_code,
        effective_from,
        effective_to,
        amend_date,
        AP04_version,
        version,
        dummy_barb_code
FROM neighbom.CHANNEL_MAP_DEV_SERVICE_KEY_BARB;
commit;
-- archive

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_ARC_SERVICE_KEY_BARB') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_ARC_SERVICE_KEY_BARB";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_ARC_SERVICE_KEY_BARB" (
        "service_key"        int DEFAULT NULL,
        "log_station_code"   int DEFAULT NULL,
        "STI_code"           int DEFAULT NULL,
        "panel_code"         int DEFAULT NULL,
        "promo_panel_code"   int DEFAULT NULL,
        "effective_from"     date DEFAULT NULL,
        "effective_to"       date DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "AP04_version"       varchar(10) DEFAULT NULL,
        "AP04_date"          date DEFAULT NULL,
        "version"            int DEFAULT NULL,
        "dummy_barb_code"    varchar(3) DEFAULT NULL
)
;


INSERT INTO VESPA_ANALYSTS.channel_map_arc_service_key_barb (
        service_key,
        log_station_code,
        STI_code,
        panel_code,
        promo_panel_code,
        effective_from,
        effective_to,
        amend_date,
        AP04_version,
        version,
        dummy_barb_code)
SELECT  service_key,
        log_station_code,
        STI_code,
        panel_code,
        promo_panel_code,
        effective_from,
        effective_to,
        amend_date,
        AP04_version,
        version,
        dummy_barb_code
FROM neighbom.CHANNEL_MAP_arc_SERVICE_KEY_BARB;
commit;
-----------
-- Landmark
-----------

-- Production

--backing up
select  *
into    VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK_BKP
from    VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK;
commit;

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK" (
        "SERVICE_KEY"        int DEFAULT NULL,
        "SARE_NO"            int DEFAULT NULL,
        "EFFECTIVE_FROM"     date DEFAULT NULL,
        "EFFECTIVE_TO"       date DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "version"            int DEFAULT NULL
)
;

-- dev
-- backing up
select  *
into    VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK_BKP
from    VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK;
commit;

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK" (
        "SERVICE_KEY"        int DEFAULT NULL,
        "SARE_NO"            int DEFAULT NULL,
        "EFFECTIVE_FROM"     date DEFAULT NULL,
        "EFFECTIVE_TO"       date DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "version"            int DEFAULT NULL
)
;


-- populate dev

INSERT INTO VESPA_ANALYSTS.channel_map_dev_service_key_landmark (
        SERVICE_KEY,
        SARE_NO,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        amend_date,
        version)
SELECT  SERVICE_KEY,
        SARE_NO,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        amend_date,
        version
FROM neighbom.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK;
commit;

-- archive

if object_id('VESPA_ANALYSTS.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK') is not null
DROP TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK";

CREATE TABLE "VESPA_ANALYSTS"."CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK" (
        "SERVICE_KEY"        int DEFAULT NULL,
        "SARE_NO"            int DEFAULT NULL,
        "EFFECTIVE_FROM"     date DEFAULT NULL,
        "EFFECTIVE_TO"       date DEFAULT NULL,
        "amend_date"         date DEFAULT NULL,
        "version"            int DEFAULT NULL
)
;

-- populate archive

INSERT INTO VESPA_ANALYSTS.channel_map_arc_service_key_landmark (
        SERVICE_KEY,
        SARE_NO,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        amend_date,
        version)
SELECT  SERVICE_KEY,
        SARE_NO,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        amend_date,
        version
FROM neighbom.CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK;
commit;



------------- PRIVILEGES...

grant select on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security
grant select on CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security
grant select on CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security
grant select on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB to vespa_group_low_security
grant select on CHANNEL_MAP_DEV_SERVICE_KEY_BARB to vespa_group_low_security
grant select on CHANNEL_MAP_ARC_SERVICE_KEY_BARB to vespa_group_low_security
grant select on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK to vespa_group_low_security
grant select on CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK to vespa_group_low_security
grant select on CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK to vespa_group_low_security

grant all privileges on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES to neighbom
grant all privileges on CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES to neighbom
grant all privileges on CHANNEL_MAP_ARC_SERVICE_KEY_ATTRIBUTES to neighbom
grant all privileges on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB to neighbom
grant all privileges on CHANNEL_MAP_DEV_SERVICE_KEY_BARB to neighbom
grant all privileges on CHANNEL_MAP_ARC_SERVICE_KEY_BARB to neighbom
grant all privileges on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK to neighbom
grant all privileges on CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK to neighbom
grant all privileges on CHANNEL_MAP_ARC_SERVICE_KEY_LANDMARK to neighbom

commit 