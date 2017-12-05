/******************************************************************************
** Capping calibration exercise - ONE-OFF script
**
******************************************************************************/

/* Channels selected for calibration:
  BBC 1
  BBC 2
  CH4
  Channel 5
  Comedy Central
  Dave
  Discovery
  ITV Breakfast
  ITV Breakfast HD
  ITV1
  ITV1 HD
  ITV2
  Sky 1
  Sky Atlantic
  Sky Movies Premiere
  Sky News
  Sky Sports 1
  Sky Sports 2
  Sky Sports News
*/
if object_id('V079_Techedge_Channel_Lookup') is not null then drop table V079_Techedge_Channel_Lookup end if;
create table V079_Techedge_Channel_Lookup (
    full_name             varchar(50) default null,
    techedge_name         varchar(50) default null,
    primary_sales_house   varchar(50) default null,
    channel_category      varchar(50) default null,
    service_id            int default null,
    service_key           int default null
);

create hg index idx1 on V079_Techedge_Channel_Lookup(service_id);
create hg index idx2 on V079_Techedge_Channel_Lookup(service_key);


insert into V079_Techedge_Channel_Lookup values ('BBC One CI', 'BBC 1', 'BBC', 'BBC', 6361, 2074);
insert into V079_Techedge_Channel_Lookup values ('BBC One East (E)', 'BBC 1', 'BBC', 'BBC', 10306, 2106);
insert into V079_Techedge_Channel_Lookup values ('BBC One East (W)', 'BBC 1', 'BBC', 'BBC', 6351, 2073);
insert into V079_Techedge_Channel_Lookup values ('BBC One East Midlands', 'BBC 1', 'BBC', 'BBC', 10305, 2105);
insert into V079_Techedge_Channel_Lookup values ('BBC One HD', 'BBC 1', 'BBC', 'BBC', 6941, 2076);
insert into V079_Techedge_Channel_Lookup values ('BBC One London', 'BBC 1', 'BBC', 'BBC', 6301, 2002);
insert into V079_Techedge_Channel_Lookup values ('BBC One North East & Cumbria', 'BBC 1', 'BBC', 'BBC', 10355, 2155);
insert into V079_Techedge_Channel_Lookup values ('BBC One North West', 'BBC 1', 'BBC', 'BBC', 6441, 2102);
insert into V079_Techedge_Channel_Lookup values ('BBC One Northern Ireland', 'BBC 1', 'BBC', 'BBC', 10361, 2005);
insert into V079_Techedge_Channel_Lookup values ('BBC One Oxford', 'BBC 1', 'BBC', 'BBC', 10356, 2156);
insert into V079_Techedge_Channel_Lookup values ('BBC One Scotland', 'BBC 1', 'BBC', 'BBC', 6421, 2004);
insert into V079_Techedge_Channel_Lookup values ('BBC One South', 'BBC 1', 'BBC', 'BBC', 10353, 2153);
insert into V079_Techedge_Channel_Lookup values ('BBC One South East', 'BBC 1', 'BBC', 'BBC', 6461, 2152);
insert into V079_Techedge_Channel_Lookup values ('BBC One South West', 'BBC 1', 'BBC', 'BBC', 10354, 2154);
insert into V079_Techedge_Channel_Lookup values ('BBC One Wales', 'BBC 1', 'BBC', 'BBC', 10311, 2003);
insert into V079_Techedge_Channel_Lookup values ('BBC One West', 'BBC 1', 'BBC', 'BBC', 6341, 2151);
insert into V079_Techedge_Channel_Lookup values ('BBC One West Midlands', 'BBC 1', 'BBC', 'BBC', 10301, 2101);
insert into V079_Techedge_Channel_Lookup values ('BBC One Yorkshire', 'BBC 1', 'BBC', 'BBC', 6451, 2104);
insert into V079_Techedge_Channel_Lookup values ('BBC One Yorkshire & Lincolnshire', 'BBC 1', 'BBC', 'BBC', 10303, 2103);
insert into V079_Techedge_Channel_Lookup values ('BBC Two England', 'BBC 2', 'BBC', 'BBC', 6302, 2006);
insert into V079_Techedge_Channel_Lookup values ('BBC Two Northern Ireland', 'BBC 2', 'BBC', 'BBC', 10362, 2017);
insert into V079_Techedge_Channel_Lookup values ('BBC Two Scotland', 'BBC 2', 'BBC', 'BBC', 6422, 2016);
insert into V079_Techedge_Channel_Lookup values ('BBC Two Wales', 'BBC 2', 'BBC', 'BBC', 10312, 2015);
insert into V079_Techedge_Channel_Lookup values ('Channel 4 London', 'CH4', 'C4', 'C4', 9211, 1621);
insert into V079_Techedge_Channel_Lookup values ('Channel 4 Midlands', 'CH4', 'C4', 'C4', 9213, 1623);
insert into V079_Techedge_Channel_Lookup values ('Channel 4 North', 'CH4', 'C4', 'C4', 9214, 1624);
insert into V079_Techedge_Channel_Lookup values ('Channel 4 Scotland', 'CH4', 'C4', 'C4', 9216, 1626);
insert into V079_Techedge_Channel_Lookup values ('Channel 4 South', 'CH4', 'C4', 'C4', 9212, 1622);
insert into V079_Techedge_Channel_Lookup values ('Channel 4 Ulster', 'CH4', 'C4', 'C4', 9215, 1625);
insert into V079_Techedge_Channel_Lookup values ('Channel 4HD', 'CH4', 'C4', 'C4', 21200, 4075);
insert into V079_Techedge_Channel_Lookup values ('Channel 5', 'Channel 5', 'FIVE', 'FIVE', 7701, 1800);
insert into V079_Techedge_Channel_Lookup values ('Channel 5  Northern Ireland', 'Channel 5', 'FIVE', 'FIVE', 7704, 1828);
insert into V079_Techedge_Channel_Lookup values ('Channel 5 HD', 'Channel 5', 'FIVE', 'FIVE', 3858, 4058);
insert into V079_Techedge_Channel_Lookup values ('Channel 5 London', 'Channel 5', 'FIVE', 'FIVE', 7700, 1801);
insert into V079_Techedge_Channel_Lookup values ('Channel 5 North', 'Channel 5', 'FIVE', 'FIVE', 7702, 1829);
insert into V079_Techedge_Channel_Lookup values ('Channel 5 Scotland', 'Channel 5', 'FIVE', 'FIVE', 7703, 1830);
insert into V079_Techedge_Channel_Lookup values ('Comedy Central', 'Comedy Central', 'Sky', 'ENTERTAINMENT', 6040, 2510);
insert into V079_Techedge_Channel_Lookup values ('Comedy Central HD', 'Comedy Central', 'Sky', 'ENTERTAINMENT', 3856, 4056);
insert into V079_Techedge_Channel_Lookup values ('Dave', 'Dave', 'C4', 'UKTV', 6506, 2306);
insert into V079_Techedge_Channel_Lookup values ('Dave HD', 'Dave', 'C4', 'UKTV', 3809, 3809);
insert into V079_Techedge_Channel_Lookup values ('Discovery', 'Discovery', 'Sky', 'DOCUMENTARIES', 6201, 2401);
insert into V079_Techedge_Channel_Lookup values ('ITV 2', 'ITV2', 'ITV', 'ITV Digital', 10070, 6240);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Anglia E', 'ITV1', 'ITV', 'ITV', 10090, 6089);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Anglia South', 'ITV1', 'ITV', 'ITV', null, 6180);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Anglia W', 'ITV1', 'ITV', 'ITV', 12110, 6381);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Border', 'ITV1', 'ITV', 'ITV', 10120, 6110);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Central E', 'ITV1', 'ITV', 'ITV', 20701, 6011);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Central S', 'ITV1', 'ITV', 'ITV', 20700, 6010);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Central SW', 'ITV1', 'ITV', 'ITV', 12140, 6015);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Central W', 'ITV1', 'ITV', 'ITV', 10100, 6300);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Channel Is', 'ITV1', 'ITV', 'ITV', 10200, 6200);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Granada', 'ITV1', 'ITV', 'ITV', 10080, 6130);
insert into V079_Techedge_Channel_Lookup values ('ITV1 HD London', 'ITV1', 'ITV', 'ITV', 10000, 6504);
insert into V079_Techedge_Channel_Lookup values ('ITV1 HD Mid West', 'ITV1 HD', 'ITV', 'ITV', 3852, 6503);
insert into V079_Techedge_Channel_Lookup values ('ITV1 HD North', 'ITV1 HD', 'ITV', 'ITV', 3851, 6505);
insert into V079_Techedge_Channel_Lookup values ('ITV1 HD S East', 'ITV1 HD', 'ITV', 'ITV', 6942, 6502);
insert into V079_Techedge_Channel_Lookup values ('ITV1 London', 'ITV1', 'ITV', 'ITV', 10060, 6000);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Meridian E', 'ITV1', 'ITV', 'ITV', null, 6141);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Meridian N', 'ITV1', 'ITV', 'ITV', 12101, 6143);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Meridian S', 'ITV1', 'ITV', 'ITV', 10140, 6140);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Meridian SE', 'ITV1', 'ITV', 'ITV', 10150, 6142);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Scottish E', 'ITV1', 'ITV', 'ITV', 10221, 6371);
insert into V079_Techedge_Channel_Lookup values ('ITV1 STV Grampian', 'ITV1', 'ITV', 'ITV', 10210, 6210);
insert into V079_Techedge_Channel_Lookup values ('ITV1 STV Scottish', 'ITV1', 'ITV', 'ITV', 10220, 6220);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Tyne Tees', 'ITV1', 'ITV', 'ITV', 10130, 6390);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Tyne Tees South', 'ITV1', 'ITV', 'ITV', null, 6391);
insert into V079_Techedge_Channel_Lookup values ('ITV1 UTV', 'ITV1', 'ITV', 'ITV', 10230, 6230);
insert into V079_Techedge_Channel_Lookup values ('ITV1 W Country', 'ITV1', 'ITV', 'ITV', 10040, 6040);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Wales', 'ITV1', 'ITV', 'ITV', 10020, 6020);
insert into V079_Techedge_Channel_Lookup values ('ITV1 West', 'ITV1', 'ITV', 'ITV', 10030, 6030);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Yorkshire', 'ITV1', 'ITV', 'ITV', 10160, 6160);
insert into V079_Techedge_Channel_Lookup values ('ITV1 Yorkshire East', 'ITV1', 'ITV', 'ITV', 12120, 6161);
insert into V079_Techedge_Channel_Lookup values ('Sky Atlantic', 'Sky Atlantic', 'Sky', 'SKY ENTERTAINMENT', 4712, 1412);
insert into V079_Techedge_Channel_Lookup values ('Sky Atlantic HD', 'Sky Atlantic', 'Sky', 'SKY ENTERTAINMENT', 3853, 4053);
insert into V079_Techedge_Channel_Lookup values ('Sky News', 'Sky News', 'Sky', 'NEWS', 4704, 1404);
insert into V079_Techedge_Channel_Lookup values ('Sky News HD', 'Sky News', 'Sky', 'NEWS', 3850, 4050);
insert into V079_Techedge_Channel_Lookup values ('Sky Premiere', 'Sky Movies Premiere', 'Sky', 'MOVIES', 4404, 1409);
insert into V079_Techedge_Channel_Lookup values ('Sky Premiere HD', 'Sky Movies Premiere', 'Sky', 'MOVIES', 3821, 4021);
insert into V079_Techedge_Channel_Lookup values ('Sky Sports 1', 'Sky Sports 1', 'Sky', 'SPORTS', 4214, 1301);
insert into V079_Techedge_Channel_Lookup values ('Sky Sports 1 HD', 'Sky Sports 1', 'Sky', 'SPORTS', 3802, 4002);
insert into V079_Techedge_Channel_Lookup values ('Sky Sports 2', 'Sky Sports 2', 'Sky', 'SPORTS', 5505, 1302);
insert into V079_Techedge_Channel_Lookup values ('Sky Sports 2 HD', 'Sky Sports 2', 'Sky', 'SPORTS', 3881, 4081);
insert into V079_Techedge_Channel_Lookup values ('Sky Sports News', 'Sky Sports News', 'Sky', 'SPORTS', 4907, 1314);
insert into V079_Techedge_Channel_Lookup values ('Sky Sports News HD', 'Sky Sports News', 'Sky', 'SPORTS', 3849, 4049);
insert into V079_Techedge_Channel_Lookup values ('Sky1', 'Sky 1', 'Sky', 'SKY ENTERTAINMENT', 4703, 1402);
insert into V079_Techedge_Channel_Lookup values ('Sky1 HD', 'Sky 1', 'Sky', 'SKY ENTERTAINMENT', 3861, 4061);
commit;
