----SCRIPT TO CREATE LANDMARK AND BARB TABLES AND ADD THESE INTO THE
----VESPA_ANALYSTS SCHEMA

create table CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES (
Service_key integer default null,
FULL_NAME varchar(255) default null,
EPG_NUMBER integer default null,
EPG_NAME varchar(255) default null,
VESPA_NAME varchar(255) default null,
CHANNEL_NAME varchar(255) default null,
TECHEDGE_NAME varchar(255) default null,
INFOSYS_NAME varchar(255) default null,
BARB_REPORTED varchar(255) default null,
ACTIVE varchar(255) default null,
CHANNEL_OWNER varchar(255) default null,
OLD_PACKAGING varchar(255) default null,
NEW_PACKAGING varchar(255) default null,
PAY_FREE_INDICATOR varchar(255) default null,
CHANNEL_GENRE varchar(255) default null,
CHANNEL_TYPE varchar(255) default null,
FORMAT varchar(255) default null,
parent_service_key integer default null,
TIMESHIFT_STATUS varchar(255) default null,
TIMESHIFT_MINUTES integer default null,
RETAIL varchar(255) default null,
CHANNEL_REACH varchar(255) default null,
HD_SWAP_EPG_NUMBER integer default null,
SENSITIVE_CHANNEL bit not null,
SPOT_SOURCE varchar(255) default null,
PROMO_SOURCE varchar(255) default null,
NOTES varchar(255) default null,
EFFECTIVE_FROM date default null,
EFFECTIVE_TO date default null,
type_id integer default null,
UI_DESCR varchar(255) default null,
EPG_CHANNEL varchar(255) default null,
amend_date date default null,
channel_pack varchar(30) default null
);

--A02: Creating CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB

create table CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB(
service_key integer default null,
log_station_code integer default null,
STI_code integer default null,
panel_code integer default null,
promo_panel_code integer default null,
effective_from date default null,
effective_to date default null,
amend_date date default null,
AP04_version varchar(10) default null,
AP04_date date default null
);

--A03: Creating CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK

create table CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK(
SERVICE_KEY integer default null,
SARE_NO integer default null,
EFFECTIVE_FROM date default null,
EFFECTIVE_TO date default null,
amend_date date default null
);

create unique index unq_ch_map_landmark on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK
(service_key, effective_from, effective_to);

--A04 Creating CHANNEL_MAP_DEV_IANS_SENSITIVE_CHANNELS

create table CHANNEL_MAP_DEV_IANS_SENSITIVE_CHANNELS
(channel_name	varchar	(255),
ssp_network_id	integer	,
transport_id	integer	,
service_id	integer	,
sensitive_channel	integer	,
barb_channel	integer	,
pay_free_indicator	varchar	(100),
grouping_indicator	varchar	(255),
network_indicator	varchar	(255),
vespa_genre	varchar	(255),
series_inclusion	integer);

--A05 Creating CHANNEL_MAP_DEV_LOG_STATION_PANEL

create table CHANNEL_MAP_DEV_LOG_STATION_PANEL
(
log_station_code	integer,
STI_code	integer,
Description	varchar	(255),
Commercial	varchar,
Programme	varchar,
Commercial_panel_code	integer,
DB2_station_code	integer,
Date_added_to_file	date,
Reporting_started_date	date,
Reporting_ceased_date	date,
TE_channel	varchar	(255),
TE_log_station_code	varchar	(255),
TE_STI	varchar	(255)	);


--A06 Creating CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES

create table CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
(Service_key	integer,
FULL_NAME	varchar(255),
EPG_NUMBER	integer,
EPG_NAME	varchar(255),
VESPA_NAME	varchar(255),
CHANNEL_NAME	varchar(255),
TECHEDGE_NAME	varchar(255),
INFOSYS_NAME	varchar(255),
BARB_REPORTED	varchar(255),
ACTIVE	varchar	(255),
CHANNEL_OWNER	varchar(255),
OLD_PACKAGING	varchar(255),
NEW_PACKAGING	varchar(255),
PAY_FREE_INDICATOR	varchar(255),
CHANNEL_GENRE	varchar(255),
CHANNEL_TYPE	varchar(255),
FORMAT	varchar(255),
parent_service_key	integer	,
TIMESHIFT_STATUS	varchar	(255),
TIMESHIFT_MINUTES	integer,
RETAIL	varchar(255),
CHANNEL_REACH	varchar(255),
HD_SWAP_EPG_NUMBER	integer	,
SENSITIVE_CHANNEL	bit	,
SPOT_SOURCE	varchar(255),
PROMO_SOURCE	varchar(255),
NOTES	varchar(255),
EFFECTIVE_FROM	date,
EFFECTIVE_TO	date,
type_id	integer	,
UI_DESCR	varchar(255),
EPG_CHANNEL	varchar(255),
amend_date	date,
channel_pack	varchar	(30));


--A07 Creating CHANNEL_MAP_DEV_SERVICE_KEY_BARB	

create table CHANNEL_MAP_DEV_SERVICE_KEY_BARB	
(service_key	integer,
log_station_code	integer	,
STI_code	integer,
panel_code	integer	,
promo_panel_code	integer	,
effective_from	date	,
effective_to	date	,
amend_date	date	,
AP04_version	varchar	(10),
AP04_date	date	);

--A08 Creating CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK

CREATE TABLE CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK
(
SERVICE_KEY	integer	,
SARE_NO	integer	,
EFFECTIVE_FROM	date,
EFFECTIVE_TO	date,
amend_date	date);


--A09 grant select etc rights on these tables

grant select, insert, update, delete on CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK to vespa_group_low_security,patelj;
grant select, insert, update, delete on CHANNEL_MAP_DEV_SERVICE_KEY_BARB to vespa_group_low_security,patelj;
grant select, insert, update, delete on CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security,patelj;
grant select, insert, update, delete on CHANNEL_MAP_DEV_LOG_STATION_PANEL to vespa_group_low_security,patelj;
grant select, insert, update, delete on CHANNEL_MAP_DEV_IANS_SENSITIVE_CHANNELS to vespa_group_low_security,patelj;

grant select on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES to vespa_group_low_security,patelj;
grant select on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_BARB to vespa_group_low_security,patelj;
grant select on CHANNEL_MAP_CBI_PROD_SERVICE_KEY_LANDMARK to vespa_group_low_security,patelj;