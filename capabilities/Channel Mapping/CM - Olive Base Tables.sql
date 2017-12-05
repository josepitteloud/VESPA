 /*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:                         CHANNEL MAPPING ETL
**Analysts:                             Angel Donnarumma
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             02/04/2014
**Project Code (Insight Collation):
**Sharepoint Folder:                    
                                                                        
**Business Brief:

        This script sets the basic tables for the CM Export...

**Tables:
	
	S1 - CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
	S2 - CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION
	S3 - CHANNEL_MAP_PROD_SERVICE_KEY_BARB
	S4 - CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK

**Stats:
	
	1 Minutes run... End-to-End...
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------------------
-- S1 - CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
-----------------------------------------------

if object_id('CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES') is not null
	drop table CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES;
	
commit;

create table CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES(

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
	,PROVIDER_ID			varchar(25)		default null
	/* ,PAY_SKY_SPORTS_FLAG	varchar(3)		default null
	,PAY_SPORTS_FLAG		varchar(3)		default null
	,PAY_TV_FLAG			varchar(3)		default null
	,KEY_PAY_ENTERTAINMENT_FLAG	varchar(3)		default null
	,SKY_SPORTS_NEWS_FLAG		varchar(3)		default null
	,SKY_MOVIES_FLAG			varchar(3)		default null
	,BT_SPORT_FLAG				varchar(3)		default null */ --> this flags ('YES'/'NO') are here because of a requirement from IDS Techs, please mind that:
																--> + they are not maintained
																--> + they should not be here (they don't really add any value at all)
																--> + a flag of 'YES'/'NO' is not properly done, it should always be a bit for 1/0
	
);
commit;

create hg index hg1 on CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES(service_key);
commit;

grant select on CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security;
commit;




-----------------------------------------------------------
-- S2 - CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION
-----------------------------------------------------------

if object_id('CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION') is not null
	drop table CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION;
	
commit;

create table CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION(

	ATTRIBUTE_FIELD				varchar(50)
	,ATTRIBUTE_SYSTEM_NAME		varchar(100)
	,ATTRIBUTE_FRIENDLY_NAME	varchar(100)
	,EFFECTIVE_FROM				date
	,EFFECTIVE_TO				date
	,SERVICE_ATTRIBUTE_VERSION	integer
	,STATIC_VARIABLE			varchar(1)

);
commit;

create date index d1 on CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION(effective_from);
create date index d2 on CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION(effective_to);
commit;

grant select on CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES_DESCRIPTION to vespa_group_low_security;
commit;


-----------------------------------------
-- S3 - CHANNEL_MAP_PROD_SERVICE_KEY_BARB
-----------------------------------------

create table CHANNEL_MAP_PROD_SERVICE_KEY_BARB(

	SERVICE_KEY 		integer 	default null
	,LOG_STATION_CODE	integer 	default null
	,STI_CODE 			integer 	default null
	,PANEL_CODE 		integer 	default null
	,PROMO_PANEL_CODE 	integer 	default null
	,EFFECTIVE_FROM 	timestamp	default null
	,EFFECTIVE_TO 		timestamp 	default null
	,AMEND_DATE 		date 		default null
	,VERSION 			integer 	default null
	,DUMMY_BARB_CODE 	varchar(3) 	default null
);
commit;

create hg index hg1 on CHANNEL_MAP_PROD_SERVICE_KEY_BARB(service_key);
commit;

grant select on CHANNEL_MAP_PROD_SERVICE_KEY_BARB to vespa_group_low_security;
commit;


---------------------------------------------
-- S4 - CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK
---------------------------------------------

create table CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK(

	SERVICE_KEY		integer 	default null
	,SARE_NO 		integer 	default null
	,EFFECTIVE_FROM timestamp 	default null
	,EFFECTIVE_TO 	timestamp	default null
	,AMEND_DATE 	date 		default null
	,VERSION 		integer 	default null
	
);
commit;

create hg index hg1 on CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK(service_key);
commit;

grant select on CHANNEL_MAP_PROD_SERVICE_KEY_LANDMARK to vespa_group_low_security;
commit;