-- 9.1 Production Tables

truncate table VESPA_ANALYSTS.channel_map_prod_service_key_attributes
truncate table VESPA_ANALYSTS.channel_map_prod_service_key_barb
truncate table VESPA_ANALYSTS.channel_map_prod_service_key_landmark

INSERT INTO VESPA_ANALYSTS.channel_map_prod_service_key_attributes
        (
        Service_key,
        FULL_NAME,
        EPG_NUMBER,
        EPG_NAME,
        VESPA_NAME,
        CHANNEL_NAME,
        TECHEDGE_NAME,
        INFOSYS_NAME,
        BARB_REPORTED,
        ACTIVE,
        CHANNEL_OWNER,
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
        ACTIVE,
        CHANNEL_OWNER,
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
FROM VESPA_ANALYSTS.channel_map_dev_service_key_attributes

INSERT INTO VESPA_ANALYSTS.channel_map_prod_service_key_barb
        (
        service_key,
        log_station_code,
        STI_code,
        panel_code,
        promo_panel_code,
        effective_from,
        effective_to,
        amend_date,
        version,
        dummy_barb_code
)
SELECT  service_key,
        log_station_code,
        STI_code,
        panel_code,
        promo_panel_code,
        effective_from,
        effective_to,
        amend_date,
        version,
        dummy_barb_code
FROM VESPA_ANALYSTS.channel_map_dev_service_key_barb

INSERT INTO VESPA_ANALYSTS.channel_map_prod_service_key_landmark
        (
        SERVICE_KEY,
        SARE_NO,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        amend_date,
        version
        )
SELECT  SERVICE_KEY,
        SARE_NO,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        amend_date,
        version
FROM VESPA_ANALYSTS.channel_map_dev_service_key_landmark
