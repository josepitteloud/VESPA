/* 7.3 Service Key Attributes
Copy all DELETE statements generated by Excel.
Make sure the table name is correct and execute them.
Example:
DELETE FROM VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES WHERE Service_key = 1302 AND EFFECTIVE_FROM = '2013-06-30 06:00:26'; -- Sky Sports Ashes Channel renamed
Copy all SELECT statements generated by Excel.
Paste them after the following INSERT statement, remove UNION from the last record and execute.
Example:
INSERT INTO VESPA_ANALYSTS.channel_map_dev_service_key_attributes
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
        ACTIVEX,
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
        service_attribute_version,
        primary_sales_house,
        channel_group,
       provider_ID)
        SELECT 5521,'News 18 India',573,'News 18 India','News 18 India','News 18 India','','','NO','','','','','FTA','News','','',5521,'Principal',0,'Retail','All',573,0,'None', 'None','','2013-07-01 06:00:26','2013-08-05 06:00:25',1,'','','2013-09-12','Other',26,'',''  -- News 18 India BARB reported
*/
		
		
		
		
		