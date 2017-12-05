
-------------------------------------------------------------------------------------------
-------------- ARCHIVE THE CURRENT VERSIONS!!!!!!!
-------------------------------------------------------------------------------------------

INSERT 	INTO VESPA_ANALYSTS.channel_map_arc_service_key_attributes
select 	SERVICE_KEY
		,FULL_NAME
		,EPG_NUMBER
		,EPG_NAME
		,VESPA_NAME
		,CHANNEL_NAME
		,TECHEDGE_NAME
		,INFOSYS_NAME
		,BARB_REPORTED
		,ACTIVEX
		,CHANNEL_OWNER
		,OLD_PACKAGING
		,NEW_PACKAGING
		,PAY_FREE_INDICATOR
		,CHANNEL_GENRE
		,CHANNEL_TYPE
		,FORMAT
		,PARENT_SERVICE_KEY
		,TIMESHIFT_STATUS
		,TIMESHIFT_MINUTES
		,RETAIL
		,CHANNEL_REACH
		,HD_SWAP_EPG_NUMBER
		,SENSITIVE_CHANNEL
		,SPOT_SOURCE
		,PROMO_SOURCE
		,NOTES
		,EFFECTIVE_FROM
		,EFFECTIVE_TO
		,TYPE_ID
		,UI_DESCR
		,EPG_CHANNEL
		,AMEND_DATE
		,CHANNEL_PACK
		,SERVICE_ATTRIBUTE_VERSION
		,PRIMARY_SALES_HOUSE
		,CHANNEL_GROUP
		,PROVIDER_ID
from   	VESPA_ANALYSTS.channel_map_prod_service_key_attributes
commit

INSERT 	INTO VESPA_ANALYSTS.channel_map_arc_service_key_barb
SELECT  *
FROM 	VESPA_ANALYSTS.channel_map_prod_service_key_barb
commit

INSERT 	INTO VESPA_ANALYSTS.channel_map_arc_service_key_landmark
SELECT	*
FROM 	VESPA_ANALYSTS.channel_map_prod_service_key_landmark
commit

--------------------------------------------------------------------------------
----------------- deploy the production table!
--------------------------------------------------------------------------------

truncate table VESPA_ANALYSTS.channel_map_dev_service_key_attributes
truncate table VESPA_ANALYSTS.channel_map_dev_service_key_barb
truncate table VESPA_ANALYSTS.channel_map_dev_service_key_landmark

INSERT INTO VESPA_ANALYSTS.channel_map_dev_service_key_attributes
SELECT	*
FROM 	<YOUR SCHEMA>.channel_map_dev_service_key_attributes
commit

INSERT 	INTO VESPA_ANALYSTS.channel_map_dev_service_key_barb
SELECT  *
FROM 	<YOUR SCHEMA>.channel_map_dev_service_key_barb
commit

INSERT 	INTO VESPA_ANALYSTS.channel_map_dev_service_key_landmark
SELECT	*
FROM 	<YOUR SCHEMA>.channel_map_dev_service_key_landmark
commit

truncate table VESPA_ANALYSTS.channel_map_prod_service_key_attributes
truncate table VESPA_ANALYSTS.channel_map_prod_service_key_barb
truncate table VESPA_ANALYSTS.channel_map_prod_service_key_landmark
commit

insert  into VESPA_ANALYSTS.channel_map_prod_service_key_attributes
select  *
from    VESPA_ANALYSTS.channel_map_dev_service_key_attributes
commit

insert  into VESPA_ANALYSTS.channel_map_prod_service_key_barb
select  *
from    VESPA_ANALYSTS.channel_map_dev_service_key_barb
commit

insert  into VESPA_ANALYSTS.channel_map_prod_service_key_landmark
select  *
from    VESPA_ANALYSTS.channel_map_dev_service_key_landmark
commit


execute vespa_analysts.ska_description_fill
commit