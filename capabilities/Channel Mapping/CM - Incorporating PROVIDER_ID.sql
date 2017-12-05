


if object_id('vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z') is not null
	drop table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z
	
commit

create table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z(

	SERVICE_KEY 			integer 		default null
	,FULL_NAME 				varchar(200) 	default null
	,EPG_NUMBER 			integer 		default null
	,EPG_NAME 				varchar(200) 	default null
	,VESPA_NAME 			varchar(200) 	default null
	,CHANNEL_NAME 			varchar(200) 	default null
	,TECHEDGE_NAME 			varchar(200) 	default null
	,INFOSYS_NAME 			varchar(200) 	default null
	,BARB_REPORTED 			varchar(200) 	default null
	,ACTIVE 				varchar(200) 	default null
	,CHANNEL_OWNER 			varchar(200) 	default null
	,OLD_PACKAGING 			varchar(200) 	default null
	,NEW_PACKAGING 			varchar(200) 	default null
	,PAY_FREE_INDICATOR 	varchar(200) 	default null
	,CHANNEL_GENRE 			varchar(200) 	default null
	,CHANNEL_TYPE 			varchar(200) 	default null
	,FORMAT 				varchar(200) 	default null
	,PARENT_SERVICE_KEY 	integer 		default null
	,TIMESHIFT_STATUS 		varchar(200) 	default null
	,TIMESHIFT_MINUTES 		integer 		default null
	,RETAIL 				varchar(200) 	default null
	,CHANNEL_REACH 			varchar(200) 	default null
	,HD_SWAP_EPG_NUMBER 	integer 		default null
	,SENSITIVE_CHANNEL 		bit 
	,SPOT_SOURCE 			varchar(200) 	default null
	,PROMO_SOURCE 			varchar(200) 	default null
	,NOTES 					varchar(200) 	default null
	,EFFECTIVE_FROM 		timestamp 		default null
	,EFFECTIVE_TO 			timestamp 		default null
	,TYPE_ID 				integer 		default null
	,UI_DESCR 				varchar(200) 	default null
	,EPG_CHANNEL 			varchar(200) 	default null
	,AMEND_DATE 			date 			default null
	,CHANNEL_PACK 			varchar(200) 	default null
	,VERSION 				integer 		default null
	,PRIMARY_SALES_HOUSE 	varchar(200) 	default null
	,CHANNEL_GROUP 			varchar(200) 	default null
	,PROVIDER_ID			integer			default null
	
)
commit

create hg index hg1 on vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z(service_key)
commit

grant select on vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z to vespa_group_low_security
commit



insert  into vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z
select  *
        ,null
from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES



commit

if object_id('vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES') is not null
	drop table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
	
commit

create table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES(

	SERVICE_KEY 			integer 		default null
	,FULL_NAME 				varchar(200) 	default null
	,EPG_NUMBER 			integer 		default null
	,EPG_NAME 				varchar(200) 	default null
	,VESPA_NAME 			varchar(200) 	default null
	,CHANNEL_NAME 			varchar(200) 	default null
	,TECHEDGE_NAME 			varchar(200) 	default null
	,INFOSYS_NAME 			varchar(200) 	default null
	,BARB_REPORTED 			varchar(200) 	default null
	,ACTIVE 				varchar(200) 	default null
	,CHANNEL_OWNER 			varchar(200) 	default null
	,OLD_PACKAGING 			varchar(200) 	default null
	,NEW_PACKAGING 			varchar(200) 	default null
	,PAY_FREE_INDICATOR 	varchar(200) 	default null
	,CHANNEL_GENRE 			varchar(200) 	default null
	,CHANNEL_TYPE 			varchar(200) 	default null
	,FORMAT 				varchar(200) 	default null
	,PARENT_SERVICE_KEY 	integer 		default null
	,TIMESHIFT_STATUS 		varchar(200) 	default null
	,TIMESHIFT_MINUTES 		integer 		default null
	,RETAIL 				varchar(200) 	default null
	,CHANNEL_REACH 			varchar(200) 	default null
	,HD_SWAP_EPG_NUMBER 	integer 		default null
	,SENSITIVE_CHANNEL 		bit 
	,SPOT_SOURCE 			varchar(200) 	default null
	,PROMO_SOURCE 			varchar(200) 	default null
	,NOTES 					varchar(200) 	default null
	,EFFECTIVE_FROM 		timestamp 		default null
	,EFFECTIVE_TO 			timestamp 		default null
	,TYPE_ID 				integer 		default null
	,UI_DESCR 				varchar(200) 	default null
	,EPG_CHANNEL 			varchar(200) 	default null
	,AMEND_DATE 			date 			default null
	,CHANNEL_PACK 			varchar(200) 	default null
	,VERSION 				integer 		default null
	,PRIMARY_SALES_HOUSE 	varchar(200) 	default null
	,CHANNEL_GROUP 			varchar(200) 	default null
	,PROVIDER_ID			integer			default null
	
)
commit

create hg index hg1 on vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES(service_key)
commit

grant select on vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security
commit



insert  into vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
select  *
from    vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z



commit

drop table vespa_analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_Z

commit

