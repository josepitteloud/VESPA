--For the load table statements to work, the files must be in csv format, and be called something.csv, so I have transferred the .dat_dis fields over to prod2x2 and renamed them as .csv

--No Landmark
--No attribution audit




drop table barb_spot_data;
drop table barb_promo_data;
drop table landmark_data;
drop table bss_data;
drop table attribution_data;
drop table barb_data_amends;
drop table attribution_audit_data;

/*
Tables also loaded:
slotsqa_service_key;
slotsqa_spot_reporting
slotsqa_service_key_attributes
slotsqa_service_key_landmark
*/

--BARB Spot (CET) file
create table barb_spot_data
(record_type varchar(500)
,insert_delete_amend_code varchar(500)
,date_of_transmission date
,reporting_panel_code varchar(500)
,log_station_code_for_break varchar(500)
,break_split_transmission_indicator varchar(500)
,break_platform_indicator varchar(500)
,break_start_time varchar(500)
,spot_break_total_duration varchar(500)
,break_type varchar(500)
,spot_type varchar(500)
,broadcaster_spot_number varchar(500)
,station_code varchar(500)
,log_station_code_for_spot int
,split_transmission_indicator int
,spot_platform_indicator varchar(500)
,hd_simulcast_spot_platform_indicator varchar(500)
,spot_start_time varchar(500)
,spot_duration varchar(500)
,clearcast_commercial_number varchar(500)
,sales_house_brand_description varchar(500)
,preceding_programme_name varchar(500)
,succeding_programme_name varchar(500)
,sales_house_identifier varchar(500)
,campaign_approval_id varchar(500)
,campaign_approval_id_version_number varchar(500)
,interactive_spot_platform_indicator varchar(500)
,blank_for_padding varchar(500)
);

--Load BARB CET files
create variable @dt date;
set @dt='20121024';
while @dt <= '20121024'
begin
     execute('     LOAD TABLE  barb_spot_data
     (record_type''¿'',
     insert_delete_amend_code''¿'',
     date_of_transmission''¿'',
     reporting_panel_code''¿'',
     log_station_code_for_break''¿'',
     break_split_transmission_indicator''¿'',
     break_platform_indicator''¿'',
     break_start_time''¿'',
     spot_break_total_duration''¿'',
     break_type''¿'',
     spot_type''¿'',
     broadcaster_spot_number''¿'',
     station_code''¿'',
     log_station_code_for_spot''¿'',
     split_transmission_indicator''¿'',
     spot_platform_indicator''¿'',
     hd_simulcast_spot_platform_indicator''¿'',
     spot_start_time''¿'',
     spot_duration''¿'',
     clearcast_commercial_number''¿'',
     sales_house_brand_description''¿'',
     preceding_programme_name''¿'',
     succeding_programme_name''¿'',
     sales_house_identifier''¿'',
     campaign_approval_id''¿'',
     campaign_approval_id_version_number''¿'',
     interactive_spot_platform_indicator''¿'',
     blank_for_padding''\n''
     )
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B' || replace(@dt,'-','') || '.CET.dat_dis.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''¿''')
     commit
     set @dt = @dt + 1
end;

--BARB Promo & Sponsorship (PSP) file
create table barb_promo_data(
     record_type varchar(500)
    ,insert_delete_amend_code varchar(500)
    ,date_of_transmission date
    ,reporting_panel_code varchar(500)
    ,station_code varchar(500)
    ,log_station_code varchar(500)
    ,split_transmission_indicator varchar(500)
    ,platform_indicator varchar(500)
    ,hd_simulcast_platform_indicator varchar(500)
    ,interval_start_time varchar(500)
    ,interval_end_time varchar(500)
    ,broadcaster_transmission_code varchar(500)
    ,interval_name varchar(500)
    ,area_flags varchar(500)
    ,content_id varchar(500)
    ,isan_number varchar(500)
    ,audience_category1_live_viewing varchar(500)
    ,audience_category1_consolidated_viewing varchar(500)
    ,audience_category2_live_viewing varchar(500)
    ,audience_category2_consolidated_viewing varchar(500)
    ,audience_category3_live_viewing varchar(500)
    ,audience_category3_consolidated_viewing varchar(500)
    ,audience_category4_live_viewing varchar(500)
    ,audience_category4_consolidated_viewing varchar(500)
    ,audience_category5_live_viewing varchar(500)
    ,audience_category5_consolidated_viewing varchar(500)
    ,audience_category6_live_viewing varchar(500)
    ,audience_category6_consolidated_viewing varchar(500)
    ,audience_category7_live_viewing varchar(500)
    ,audience_category7_consolidated_viewing varchar(500)
    ,audience_category8_live_viewing varchar(500)
    ,audience_category8_consolidated_viewing varchar(500)
    ,audience_category9_live_viewing varchar(500)
    ,audience_category9_consolidated_viewing varchar(500)
    ,audience_category10_live_viewing varchar(500)
    ,audience_category10_consolidated_viewing varchar(500)
    ,audience_category11_live_viewing varchar(500)
    ,audience_category11_consolidated_viewing varchar(500)
    ,audience_category12_live_viewing varchar(500)
    ,audience_category12_consolidated_viewing varchar(500)
    ,audience_category13_live_viewing varchar(500)
    ,audience_category13_consolidated_viewing varchar(500)
    ,audience_category14_live_viewing varchar(500)
    ,audience_category14_consolidated_viewing varchar(500)
    ,audience_category15_live_viewing varchar(500)
    ,audience_category15_consolidated_viewing varchar(500)
    ,audience_category16_live_viewing varchar(500)
    ,audience_category16_consolidated_viewing varchar(500)
    ,audience_category17_live_viewing varchar(500)
    ,audience_category17_consolidated_viewing varchar(500)
    ,audience_category18_live_viewing varchar(500)
    ,audience_category18_consolidated_viewing varchar(500)
    ,audience_category19_live_viewing varchar(500)
    ,audience_category19_consolidated_viewing varchar(500)
    ,audience_category20_live_viewing varchar(500)
    ,audience_category20_consolidated_viewing varchar(500)
    ,audience_category21_live_viewing varchar(500)
    ,audience_category21_consolidated_viewing varchar(500)
    ,audience_category22_live_viewing varchar(500)
    ,audience_category22_consolidated_viewing varchar(500)
    ,audience_category23_live_viewing varchar(500)
    ,audience_category23_consolidated_viewing varchar(500)
    ,audience_category24_live_viewing varchar(500)
    ,audience_category24_consolidated_viewing varchar(500)
    ,audience_category25_live_viewing varchar(500)
    ,audience_category25_consolidated_viewing varchar(500)
    ,audience_category26_live_viewing varchar(500)
    ,audience_category26_consolidated_viewing varchar(500)
    ,audience_category27_live_viewing varchar(500)
    ,audience_category27_consolidated_viewing varchar(500)
    ,audience_category28_live_viewing varchar(500)
    ,audience_category28_consolidated_viewing varchar(500)
    ,audience_category29_live_viewing varchar(500)
    ,audience_category29_consolidated_viewing varchar(500)
    ,audience_category30_live_viewing varchar(500)
    ,audience_category30_consolidated_viewing varchar(500)
    ,audience_category31_live_viewing varchar(500)
    ,audience_category31_consolidated_viewing varchar(500)
    ,audience_category32_live_viewing varchar(500)
    ,audience_category32_consolidated_viewing varchar(500)
    ,audience_category33_live_viewing varchar(500)
    ,audience_category33_consolidated_viewing varchar(500)
    ,audience_category34_live_viewing varchar(500)
    ,audience_category34_consolidated_viewing varchar(500)
    ,audience_category35_live_viewing varchar(500)
    ,audience_category35_consolidated_viewing varchar(500)
    ,audience_category36_live_viewing varchar(500)
    ,audience_category36_consolidated_viewing varchar(500)
    ,audience_category37_live_viewing varchar(500)
    ,audience_category37_consolidated_viewing varchar(500)
    ,audience_category38_live_viewing varchar(500)
    ,audience_category38_consolidated_viewing varchar(500)
    ,audience_category39_live_viewing varchar(500)
    ,audience_category39_consolidated_viewing varchar(500)
    ,audience_category40_live_viewing varchar(500)
    ,audience_category40_consolidated_viewing varchar(500)
    ,audience_category41_live_viewing varchar(500)
    ,audience_category41_consolidated_viewing varchar(500)
    ,audience_category42_live_viewing varchar(500)
    ,audience_category42_consolidated_viewing varchar(500)
    ,audience_category43_live_viewing varchar(500)
    ,audience_category43_consolidated_viewing varchar(500)
    ,audience_category44_live_viewing varchar(500)
    ,audience_category44_consolidated_viewing varchar(500)
    ,audience_category45_live_viewing varchar(500)
    ,audience_category45_consolidated_viewing varchar(500)
    ,audience_category46_live_viewing varchar(500)
    ,audience_category46_consolidated_viewing varchar(500)
    ,audience_category47_live_viewing varchar(500)
    ,audience_category47_consolidated_viewing varchar(500)
    ,audience_category48_live_viewing varchar(500)
    ,audience_category48_consolidated_viewing varchar(500)
    ,audience_category49_live_viewing varchar(500)
    ,audience_category49_consolidated_viewing varchar(500)
    ,audience_category50_live_viewing varchar(500)
    ,audience_category50_consolidated_viewing varchar(500)
    ,audience_category51_live_viewing varchar(500)
    ,audience_category51_consolidated_viewing varchar(500)
    ,audience_category52_live_viewing varchar(500)
    ,audience_category52_consolidated_viewing varchar(500)
    ,audience_category53_live_viewing varchar(500)
    ,audience_category53_consolidated_viewing varchar(500)
    ,audience_category54_live_viewing varchar(500)
    ,audience_category54_consolidated_viewing varchar(500)
    ,audience_category55_live_viewing varchar(500)
    ,audience_category55_consolidated_viewing varchar(500)
    ,audience_category56_live_viewing varchar(500)
    ,audience_category56_consolidated_viewing varchar(500)
    ,audience_category57_live_viewing varchar(500)
    ,audience_category57_consolidated_viewing varchar(500)
    ,audience_category58_live_viewing varchar(500)
    ,audience_category58_consolidated_viewing varchar(500)
    ,audience_category59_live_viewing varchar(500)
    ,audience_category59_consolidated_viewing varchar(500)
    ,audience_category60_live_viewing varchar(500)
    ,audience_category60_consolidated_viewing varchar(500)
    ,audience_category61_live_viewing varchar(500)
    ,audience_category61_consolidated_viewing varchar(500)
    ,audience_category62_live_viewing varchar(500)
    ,audience_category62_consolidated_viewing varchar(500)
    ,audience_category63_live_viewing varchar(500)
    ,audience_category63_consolidated_viewing varchar(500)
    ,audience_category64_live_viewing varchar(500)
    ,audience_category64_consolidated_viewing varchar(500)
    ,audience_category65_live_viewing varchar(500)
    ,audience_category65_consolidated_viewing varchar(500)
    ,audience_category66_live_viewing varchar(500)
    ,audience_category66_consolidated_viewing varchar(500)
    ,audience_category67_live_viewing varchar(500)
    ,audience_category67_consolidated_viewing varchar(500)
    ,audience_category68_live_viewing varchar(500)
    ,audience_category68_consolidated_viewing varchar(500)
    ,audience_category69_live_viewing varchar(500)
    ,audience_category69_consolidated_viewing varchar(500)
    ,audience_category70_live_viewing varchar(500)
    ,audience_category70_consolidated_viewing varchar(500)
    ,audience_category71_live_viewing varchar(500)
    ,audience_category71_consolidated_viewing varchar(500)
    ,blank_for_padding varchar(500)
);

--Load BARB Promo & Sponsorship file
set @dt='20121024';
while @dt <= '20121024'
begin
     execute('
     LOAD TABLE  barb_promo_data(
          record_type''¿'',
          insert_delete_amend_code''¿'',
          date_of_transmission''¿'',
          reporting_panel_code''¿'',
          station_code''¿'',
          log_station_code''¿'',
          split_transmission_indicator''¿'',
          platform_indicator''¿'',
          hd_simulcast_platform_indicator''¿'',
          interval_start_time''¿'',
          interval_end_time''¿'',
          broadcaster_transmission_code''¿'',
          interval_name''¿'',
          area_flags''¿'',
          content_id''¿'',
          isan_number''¿'',
          audience_category1_live_viewing''¿'',
          audience_category1_consolidated_viewing''¿'',
          audience_category2_live_viewing''¿'',
          audience_category2_consolidated_viewing''¿'',
          audience_category3_live_viewing''¿'',
          audience_category3_consolidated_viewing''¿'',
          audience_category4_live_viewing''¿'',
          audience_category4_consolidated_viewing''¿'',
          audience_category5_live_viewing''¿'',
          audience_category5_consolidated_viewing''¿'',
          audience_category6_live_viewing''¿'',
          audience_category6_consolidated_viewing''¿'',
          audience_category7_live_viewing''¿'',
          audience_category7_consolidated_viewing''¿'',
          audience_category8_live_viewing''¿'',
          audience_category8_consolidated_viewing''¿'',
          audience_category9_live_viewing''¿'',
          audience_category9_consolidated_viewing''¿'',
          audience_category10_live_viewing''¿'',
          audience_category10_consolidated_viewing''¿'',
          audience_category11_live_viewing''¿'',
          audience_category11_consolidated_viewing''¿'',
          audience_category12_live_viewing''¿'',
          audience_category12_consolidated_viewing''¿'',
          audience_category13_live_viewing''¿'',
          audience_category13_consolidated_viewing''¿'',
          audience_category14_live_viewing''¿'',
          audience_category14_consolidated_viewing''¿'',
          audience_category15_live_viewing''¿'',
          audience_category15_consolidated_viewing''¿'',
          audience_category16_live_viewing''¿'',
          audience_category16_consolidated_viewing''¿'',
          audience_category17_live_viewing''¿'',
          audience_category17_consolidated_viewing''¿'',
          audience_category18_live_viewing''¿'',
          audience_category18_consolidated_viewing''¿'',
          audience_category19_live_viewing''¿'',
          audience_category19_consolidated_viewing''¿'',
          audience_category20_live_viewing''¿'',
          audience_category20_consolidated_viewing''¿'',
          audience_category21_live_viewing''¿'',
          audience_category21_consolidated_viewing''¿'',
          audience_category22_live_viewing''¿'',
          audience_category22_consolidated_viewing''¿'',
          audience_category23_live_viewing''¿'',
          audience_category23_consolidated_viewing''¿'',
          audience_category24_live_viewing''¿'',
          audience_category24_consolidated_viewing''¿'',
          audience_category25_live_viewing''¿'',
          audience_category25_consolidated_viewing''¿'',
          audience_category26_live_viewing''¿'',
          audience_category26_consolidated_viewing''¿'',
          audience_category27_live_viewing''¿'',
          audience_category27_consolidated_viewing''¿'',
          audience_category28_live_viewing''¿'',
          audience_category28_consolidated_viewing''¿'',
          audience_category29_live_viewing''¿'',
          audience_category29_consolidated_viewing''¿'',
          audience_category30_live_viewing''¿'',
          audience_category30_consolidated_viewing''¿'',
          audience_category31_live_viewing''¿'',
          audience_category31_consolidated_viewing''¿'',
          audience_category32_live_viewing''¿'',
          audience_category32_consolidated_viewing''¿'',
          audience_category33_live_viewing''¿'',
          audience_category33_consolidated_viewing''¿'',
          audience_category34_live_viewing''¿'',
          audience_category34_consolidated_viewing''¿'',
          audience_category35_live_viewing''¿'',
          audience_category35_consolidated_viewing''¿'',
          audience_category36_live_viewing''¿'',
          audience_category36_consolidated_viewing''¿'',
          audience_category37_live_viewing''¿'',
          audience_category37_consolidated_viewing''¿'',
          audience_category38_live_viewing''¿'',
          audience_category38_consolidated_viewing''¿'',
          audience_category39_live_viewing''¿'',
          audience_category39_consolidated_viewing''¿'',
          audience_category40_live_viewing''¿'',
          audience_category40_consolidated_viewing''¿'',
          audience_category41_live_viewing''¿'',
          audience_category41_consolidated_viewing''¿'',
          audience_category42_live_viewing''¿'',
          audience_category42_consolidated_viewing''¿'',
          audience_category43_live_viewing''¿'',
          audience_category43_consolidated_viewing''¿'',
          audience_category44_live_viewing''¿'',
          audience_category44_consolidated_viewing''¿'',
          audience_category45_live_viewing''¿'',
          audience_category45_consolidated_viewing''¿'',
          audience_category46_live_viewing''¿'',
          audience_category46_consolidated_viewing''¿'',
          audience_category47_live_viewing''¿'',
          audience_category47_consolidated_viewing''¿'',
          audience_category48_live_viewing''¿'',
          audience_category48_consolidated_viewing''¿'',
          audience_category49_live_viewing''¿'',
          audience_category49_consolidated_viewing''¿'',
          audience_category50_live_viewing''¿'',
          audience_category50_consolidated_viewing''¿'',
          audience_category51_live_viewing''¿'',
          audience_category51_consolidated_viewing''¿'',
          audience_category52_live_viewing''¿'',
          audience_category52_consolidated_viewing''¿'',
          audience_category53_live_viewing''¿'',
          audience_category53_consolidated_viewing''¿'',
          audience_category54_live_viewing''¿'',
          audience_category54_consolidated_viewing''¿'',
          audience_category55_live_viewing''¿'',
          audience_category55_consolidated_viewing''¿'',
          audience_category56_live_viewing''¿'',
          audience_category56_consolidated_viewing''¿'',
          audience_category57_live_viewing''¿'',
          audience_category57_consolidated_viewing''¿'',
          audience_category58_live_viewing''¿'',
          audience_category58_consolidated_viewing''¿'',
          audience_category59_live_viewing''¿'',
          audience_category59_consolidated_viewing''¿'',
          audience_category60_live_viewing''¿'',
          audience_category60_consolidated_viewing''¿'',
          audience_category61_live_viewing''¿'',
          audience_category61_consolidated_viewing''¿'',
          audience_category62_live_viewing''¿'',
          audience_category62_consolidated_viewing''¿'',
          audience_category63_live_viewing''¿'',
          audience_category63_consolidated_viewing''¿'',
          audience_category64_live_viewing''¿'',
          audience_category64_consolidated_viewing''¿'',
          audience_category65_live_viewing''¿'',
          audience_category65_consolidated_viewing''¿'',
          audience_category66_live_viewing''¿'',
          audience_category66_consolidated_viewing''¿'',
          audience_category67_live_viewing''¿'',
          audience_category67_consolidated_viewing''¿'',
          audience_category68_live_viewing''¿'',
          audience_category68_consolidated_viewing''¿'',
          audience_category69_live_viewing''¿'',
          audience_category69_consolidated_viewing''¿'',
          audience_category70_live_viewing''¿'',
          audience_category70_consolidated_viewing''¿'',
          audience_category71_live_viewing''¿'',
          audience_category71_consolidated_viewing''¿'',
          blank_for_padding''\n''
     )
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B' || replace(@dt,'-','') || '.PSP.dat_dis.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''¿''')
     set @dt = @dt + 1
     commit
end;

--BARB amendment data
create table barb_data_amends(
      Record_type varchar(500)
     ,Commercial_number varchar(500)
     ,Match_Group varchar(500)
     ,Date_of_Transmission varchar(500)
     ,Buying_Agency_Code varchar(500)
     ,Buying_Agency_Name varchar(500)
     ,Advertiser_Code varchar(500)
     ,Advertiser_Name varchar(500)
     ,Holding_company_code varchar(500)
     ,Holding_company_name varchar(500)
     ,Product_Code varchar(500)
     ,Product_Name varchar(500)
     ,NMR_Category_Code varchar(500)
     ,Clearcast_Telephone_Number varchar(500)
     ,Clearcast_Commercial_Title varchar(500)
     ,Spot_Length varchar(500)
     ,Clearcast_Web_Address varchar(500)
);

create table data_raw(dta varchar(1000));
LOAD TABLE data_raw(dta'\n')
     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121018.CE1.dat_dis.csv'
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000;

commit;

  insert into barb_data_amends(
      Record_type
     ,Commercial_number
     ,Match_Group
     ,Date_of_Transmission
     ,Buying_Agency_Code
     ,Buying_Agency_Name
     ,Advertiser_Code
     ,Advertiser_Name
     ,Holding_company_code
     ,Holding_company_name
     ,Product_Code
     ,Product_Name
     ,NMR_Category_Code
     ,Clearcast_Telephone_Number
     ,Clearcast_Commercial_Title
     ,Spot_Length
     ,Clearcast_Web_Address
)
  select substr(dta,  1,  2)
        ,substr(dta,  3, 15)
        ,substr(dta, 18,  3)
        ,substr(dta, 21,  8)
        ,substr(dta, 29,  7)
        ,substr(dta, 36, 20)
        ,substr(dta, 56,  7)
        ,substr(dta, 63, 20)
        ,substr(dta, 83,  7)
        ,substr(dta, 90, 20)
        ,substr(dta,110,  7)
        ,substr(dta,117, 35)
        ,substr(dta,152,  6)
        ,substr(dta,158, 30)
        ,substr(dta,188, 50)
        ,substr(dta,238,  3)
        ,substr(dta,241, 50)
from data_raw
;

select record_type,count(1) from barb_data_amends group by record_type;


--BSS data
create table bss_data(
     Record_Type    varchar(500)
    ,TX_Date        varchar(500)
    ,TX_Time        varchar(500)
    ,SSP_Network_ID varchar(500)
    ,Transport_ID   varchar(500)
    ,Service_ID     varchar(500)
    ,SI_Service_Key varchar(500)
    ,TX_ID          varchar(500)
    ,Duration       varchar(500)
    ,Media_Code     varchar(500)
    ,Media_Code_Name           varchar(500)
    ,Slot_Type                 varchar(500)
    ,Product_Description       varchar(500)
    ,Product_Code              varchar(500)
    ,Preceding_Programme_Name  varchar(500)
    ,Succeeding_Programme_Name varchar(500)
    ,SI_Event_Chain_Key        varchar(500)
);

--set @dt='20120528';
--while @dt <= '20120604'
begin
     execute('
     LOAD TABLE  bss_data(
         Record_Type''¿'',
         TX_Date''¿'',
         TX_Time''¿'',
         SSP_Network_ID''¿'',
         Transport_ID''¿'',
         Service_ID''¿'',
         SI_Service_Key''¿'',
         TX_ID''¿'',
         Duration''¿'',
         Media_Code''¿'',
         Media_Code_Name''¿'',
         Slot_Type''¿'',
         Product_Description''¿'',
         Product_Code''¿'',
         Preceding_Programme_Name''¿'',
         Succeeding_Programme_Name''¿'',
         SI_Event_Chain_Key''\n''
     )
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/BSS-PROMO-SPONS_20121101021502_247.dat_dis.csv'' --I deleted the time part of the file name
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''¿'''
     )
     set @dt = @dt +1
     commit
end;


--Landmark data
create table landmark_data(
       SARE_NO INTEGER,
       SLOT_START_BROADCAST_DATE DATE,
       SLOT_START_BROADCAST_TIME_HOURS INTEGER,
       SLOT_START_TIME_MINUTES INTEGER,
       SLOT_START_TIME_SECONDS INTEGER,
       BROADCAST_ID VARCHAR(255),
       SOURCE_SLOT_INSTANCE_ID VARCHAR(20),
       ADSMART_FLAG tinyint,
       ADSMART_PRIORITY tinyint,
       ADSMART_TOTAL_PRIORITY tinyint,
       CAMPAIGN_APPROVAL_ID VARCHAR(200),
       CAMPAIGN_APPROVAL_VERSION INTEGER,
       SOURCE_PRECEDING_PROGRAMME_NAME VARCHAR(40),
       SOURCE_SUCCEEDING_PROGRAMME_NAME VARCHAR(40),
       MEDIA_CODE VARCHAR(15),
       SLOT_TYPE VARCHAR(4),
       MEDIA_SPOT_TYPE VARCHAR(50),
       SLOT_NAME VARCHAR(50),
       SLOT_DURATION SMALLINT,
       CLEARCAST_COMMERCIAL_NO VARCHAR(15),
       PRODUCT_CODE INTEGER,
       PRODUCT_NAME VARCHAR(50),
       BARB_SALES_HOUSE_ID INTEGER,
       BUYER_CODE VARCHAR(6),
       ADVERTISER_CODE VARCHAR(6),
       STATUS CHAR(1),
       BREAK_POSITION VARCHAR(1),
       POSITION_IN_BREAK_NAME VARCHAR(1)
);

--select SLOT_START_BROADCAST_DATE,count(1) from landmark_data group by SLOT_START_BROADCAST_DATE

drop table landmark_data_raw;
create table landmark_data_raw(dta varchar(500));
LOAD TABLE landmark_data_raw(dta'\n')
  FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/20121101_ssp_channel_map.dat_dis.csv'
  QUOTES OFF
  ESCAPES OFF;

alter table landmark_data_raw
add (
    delim1  int
   ,delim2  int
   ,delim3  int
   ,delim4  int
   ,delim5  int
   ,delim6  int
   ,delim7  int
   ,delim8  int
   ,delim9  int
   ,delim10 int
   ,delim11 int
   ,delim12 int
   ,delim13 int
   ,delim14 int
   ,delim15 int
   ,delim16 int
   ,delim17 int
   ,delim18 int
   ,delim19 int
   ,delim20 int
   ,delim21 int
   ,delim22 int
   ,delim23 int
   ,delim24 int
   ,delim25 int
   ,delim26 int
   ,delim27 int
   ,delim28 int
);

update landmark_data_raw
   set delim1 = charindex('¿', dta)
;

create variable @counter int;
   set @counter=2;
while @counter <= 28
begin
       execute(
         'update landmark_data_raw
             set delim' || @counter || ' = charindex(''¿'', substr(dta, delim' || @counter - 1 || ' + 1, 500)) + delim' || @counter - 1
     )
     set @counter = @counter + 1
     commit
end;

insert into landmark_data(
     SARE_NO ,
     SLOT_START_BROADCAST_DATE ,
     SLOT_START_BROADCAST_TIME_HOURS ,
     SLOT_START_TIME_MINUTES ,
     SLOT_START_TIME_SECONDS ,
     BROADCAST_ID ,
     SOURCE_SLOT_INSTANCE_ID ,
     ADSMART_FLAG ,
     ADSMART_PRIORITY ,
     ADSMART_TOTAL_PRIORITY ,
     CAMPAIGN_APPROVAL_ID ,
     CAMPAIGN_APPROVAL_VERSION ,
     SOURCE_PRECEDING_PROGRAMME_NAME ,
     SOURCE_SUCCEEDING_PROGRAMME_NAME ,
     MEDIA_CODE ,
     SLOT_TYPE ,
     MEDIA_SPOT_TYPE ,
     SLOT_NAME ,
     SLOT_DURATION ,
     CLEARCAST_COMMERCIAL_NO ,
     PRODUCT_CODE ,
     PRODUCT_NAME ,
     BARB_SALES_HOUSE_ID ,
     BUYER_CODE ,
     ADVERTISER_CODE ,
     STATUS ,
     BREAK_POSITION ,
     POSITION_IN_BREAK_NAME
)
  select substr(dta, 1          , delim1  -           1)
        ,right(substr(dta, delim1  + 1, delim2  - delim1  - 1), 4) || '-' || substr(substr(dta, delim1  + 1, delim2  - delim1  - 1),4,2) || '-' || left(substr(dta, delim1  + 1, delim2  - delim1  - 1),2)
        ,substr(dta, delim2  + 1, delim3  - delim2  - 1)
        ,substr(dta, delim3  + 1, delim4  - delim3  - 1)
        ,substr(dta, delim4  + 1, delim5  - delim4  - 1)
        ,substr(dta, delim5  + 1, delim6  - delim5  - 1)
        ,substr(dta, delim6  + 1, delim7  - delim6  - 1)
        ,substr(dta, delim7  + 1, delim8  - delim7  - 1)
        ,substr(dta, delim8  + 1, delim9  - delim8  - 1)
        ,substr(dta, delim9  + 1, delim10 - delim9  - 1)
        ,substr(dta, delim10 + 1, delim11 - delim10 - 1)
        ,substr(dta, delim11 + 1, delim12 - delim11 - 1)
        ,substr(dta, delim12 + 1, delim13 - delim12 - 1)
        ,substr(dta, delim13 + 1, delim14 - delim13 - 1)
        ,substr(dta, delim14 + 1, delim15 - delim14 - 1)
        ,substr(dta, delim15 + 1, delim16 - delim15 - 1)
        ,substr(dta, delim16 + 1, delim17 - delim16 - 1)
        ,substr(dta, delim17 + 1, delim18 - delim17 - 1)
        ,substr(dta, delim18 + 1, delim19 - delim18 - 1)
        ,substr(dta, delim19 + 1, delim20 - delim19 - 1)
        ,substr(dta, delim20 + 1, delim21 - delim20 - 1)
        ,substr(dta, delim21 + 1, delim22 - delim21 - 1)
        ,substr(dta, delim22 + 1, delim23 - delim22 - 1)
        ,substr(dta, delim23 + 1, delim24 - delim23 - 1)
        ,substr(dta, delim24 + 1, delim25 - delim24 - 1)
        ,substr(dta, delim25 + 1, delim26 - delim25 - 1)
        ,substr(dta, delim26 + 1, delim27 - delim26 - 1)
        ,substr(dta, delim27 + 1, delim28 - delim27 - 1)
from landmark_data_raw;
select * from landmark_data

-- --original Attribution data
-- create table attribution_data(
--       Record_type varchar(500)
--      ,Commercial_Number varchar(500)
--      ,Match_Group varchar(500)
--      ,Date_of_Transmission varchar(500)
--      ,Buying_Agency_Code varchar(500)
--      ,Buying_Agency_Name varchar(500)
--      ,Advertiser_Code varchar(500)
--      ,Advertiser_Name varchar(500)
--      ,Holding_company_Code varchar(500)
--      ,Holding_company_name varchar(500)
--      ,Product_Code varchar(500)
--      ,Product_Name varchar(500)
--      ,NMR_Category_Code varchar(500)
--      ,Clearcast_Telephone_Number varchar(500)
--      ,Clearcast_Commercial_Title varchar(500)
--      ,Spot_Length varchar(500)
--      ,Clearcast_Web_Address varchar(500)
-- );
--
-- set @dt='20120528';
-- while @dt <= '20120611'
-- begin
--      execute('
--      LOAD TABLE  attribution_data(
--           Record_type''¿'',
--           Commercial_Number''¿'',
--           Match_Group''¿'',
--           Date_of_Transmission''¿'',
--           Buying_Agency_Code''¿'',
--           Buying_Agency_Name''¿'',
--           Advertiser_Code''¿'',
--           Advertiser_Name''¿'',
--           Holding_company_Code''¿'',
--           Holding_company_name''¿'',
--           Product_Code''¿'',
--           Product_Name''¿'',
--           NMR_Category_Code''¿'',
--           Clearcast_Telephone_Number''¿'',
--           Clearcast_Commercial_Title''¿'',
--           Spot_Length''¿'',
--           Clearcast_Web_Address''\n''
--      )
--      FROM ''/SKP2x2f1/prod/sky/olive/data/share/clarityq/export/Jon/slots/aw' || replace(@dt,'-','') || '.sky.dat_dis.csv'' --these appear to contain all data for week commencing
--      QUOTES OFF
--      ESCAPES OFF
--      NOTIFY 1000
--      delimited by ''¿''
--      ')
--      set @dt = @dt + 7
--      commit
-- end;

--Attribution data
create table attribution_data(
      Record_type varchar(500)
     ,Commercial_number varchar(500)
     ,Match_Group varchar(500)
     ,Date_of_Transmission varchar(500)
     ,Buying_Agency_Code varchar(500)
     ,Buying_Agency_Name varchar(500)
     ,Advertiser_Code varchar(500)
     ,Advertiser_Name varchar(500)
     ,Holding_company_code varchar(500)
     ,Holding_company_name varchar(500)
     ,Product_Code varchar(500)
     ,Product_Name varchar(500)
     ,NMR_Category_Code varchar(500)
     ,Clearcast_Telephone_Number varchar(500)
     ,Clearcast_Commercial_Title varchar(500)
     ,Spot_Length varchar(500)
     ,Clearcast_Web_Address varchar(500)
);

truncate table data_raw;
set @dt='20121126';
while @dt <= '20121126'
begin
     execute('
     LOAD TABLE  data_raw(
          dta''\n''
     )
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/aw' || replace(@dt,'-','') || '.sky.dat_dis.csv'' --these are the weekly files containing all data for week commencing
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     ')
     set @dt = @dt + 7
     commit
end;

--this is the backfill file
LOAD TABLE  data_raw(     dta'\n')
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/ma20120226.sky.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
  insert into attribution_data(
      Record_type
     ,Commercial_number
     ,Match_Group
     ,Date_of_Transmission
     ,Buying_Agency_Code
     ,Buying_Agency_Name
     ,Advertiser_Code
     ,Advertiser_Name
     ,Holding_company_code
     ,Holding_company_name
     ,Product_Code
     ,Product_Name
     ,NMR_Category_Code
     ,Clearcast_Telephone_Number
     ,Clearcast_Commercial_Title
     ,Spot_Length
     ,Clearcast_Web_Address
)
  select substr(dta,  1,  2)
        ,substr(dta,  3, 15)
        ,substr(dta, 18,  3)
        ,substr(dta, 21,  8)
        ,substr(dta, 29,  7)
        ,substr(dta, 36, 20)
        ,substr(dta, 56,  7)
        ,substr(dta, 63, 20)
        ,substr(dta, 83,  7)
        ,substr(dta, 90, 20)
        ,substr(dta,110,  7)
        ,substr(dta,117, 35)
        ,substr(dta,152,  6)
        ,substr(dta,158, 30)
        ,substr(dta,188, 50)
        ,substr(dta,238,  3)
        ,substr(dta,241, 50)
from data_raw
;

--Attribution Audit data
create table attribution_audit_data(
      Record_type varchar(500)
     ,Audit_Action_Date varchar(500)
     ,Audit_Action_Time varchar(500)
     ,Audit_Action varchar(500)
     ,Commercial_Number varchar(500)
     ,Match_Group varchar(500)
     ,Name_Type varchar(500)
     ,Code_From varchar(500)
     ,Name_From varchar(500)
     ,Code_To varchar(500)
     ,Name_To varchar(500)
);

truncate table data_raw;
set @dt='20120227';
while @dt <= '20120702'
begin
     execute('
     LOAD TABLE data_raw(
          dta''\n''
     )
     FROM ''/SKP2x2f1/prod/sky/olive/data/share/clarityq/export/Jon/slots/aw_audit_' || replace(@dt,'-','') || '.sky.csv'' --these appear to contain all data for week commencing
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     ')
     set @dt = @dt + 7
     commit
end;

  insert into attribution_audit_data(
      Record_type
     ,Audit_Action_Date
     ,Audit_Action_Time
     ,Audit_Action
     ,Commercial_Number
     ,Match_Group
     ,Name_Type
     ,Code_From
     ,Name_From
     ,Code_To
     ,Name_To
)
  select substr(dta,  1,  2)
        ,substr(dta,  3,  8)
        ,substr(dta, 11,  6)
        ,substr(dta, 17,  1)
        ,substr(dta, 18, 15)
        ,substr(dta, 33,  3)
        ,substr(dta, 36,  1)
        ,substr(dta, 37,  7)
        ,substr(dta, 44, 50)
        ,substr(dta, 94,  7)
        ,substr(dta,101, 50)
from data_raw
;


--process the audit file
select count(1) from attribution_data
select count(1) from attribution_audit_data
select top 100 * from attribution_audit_data
where audit_action<>'1';
--there shouldn't be any, but there are 19, all blank though.

select *
    from attribution_audit_data as aud
        inner join attribution_data as att on att.commercial_number = aud.commercial_number
                                          and att.match_group       = aud.match_group
                                          and name_type = 'P'
                                          and att.product_code <> aud.code_from -- there are 37 product changes where the code_from doesn't match the existing code
                                          and att.product_code <> aud.code_to   -- there are 2 product changes where the code_to doesn't matchg either.
;

--looking at one example:
select * from attribution_data
where commercial_number like 'TRAWTSM201030%'
;

select * from attribution_audit_data
where commercial_number like 'TRAWTSM201030%'
;

--The code has changed more than once. There are also some that are changed back to the original code, so we will have to keep loop by audit date, to get them in the right order

create variable @dt date;
set @dt = '2012-02-28'; --this is the date range we currently have
while @dt <= '2012-07-06'
begin
     --Product
       update attribution_data       as att
          set att.product_code      = aud.code_to
             ,att.product_name      = aud.name_to
         from attribution_audit_data as aud
        where att.commercial_number = aud.commercial_number
          and att.match_group       = aud.match_group
          and name_type             = 'P'
          and att.product_code      = aud.code_from
          and audit_action_date     = replace(cast(@dt as varchar), '-', '')

     --Advertiser
       update attribution_data       as att
          set att.advertiser_code   = aud.code_to
             ,att.advertiser_name   = aud.name_to
         from attribution_audit_data as aud
        where att.commercial_number = aud.commercial_number
          and att.match_group       = aud.match_group
          and name_type             = 'A'
          and att.advertiser_code   = aud.code_from
          and audit_action_date     = replace(cast(@dt as varchar), '-', '')

     --Holding company
       update attribution_data       as att
          set att.holding_company_code = aud.code_to
             ,att.holding_company_name = aud.name_to
         from attribution_audit_data as aud
        where att.commercial_number    = aud.commercial_number
          and att.match_group          = aud.match_group
          and name_type                = 'H'
          and att.holding_company_code = aud.code_from
          and audit_action_date        = replace(cast(@dt as varchar), '-', '')

     --Buying agency
       update attribution_data       as att
          set att.buying_agency_code = aud.code_to
             ,att.buying_agency_name = aud.name_to
         from attribution_audit_data as aud
        where att.commercial_number  = aud.commercial_number
          and att.match_group        = aud.match_group
          and name_type              = 'B'
          and att.buying_agency_code = aud.code_from
          and audit_action_date      = replace(cast(@dt as varchar), '-', '')

     --NMR
       update attribution_data       as att
          set att.nmr_category_code = aud.code_to
         from attribution_audit_data as aud
        where att.commercial_number = aud.commercial_number
          and att.match_group       = aud.match_group
          and name_type             = 'M'
          and att.nmr_category_code = aud.code_from
          and audit_action_date     = replace(cast(@dt as varchar), '-', '')

     set @dt = @dt +1
     commit
end; --15m



-- --Service Key the table is there, but it doesn't want to load - I have loaded it and the other pages manually
-- CREATE TABLE "slotsqa_service_key" (
--  "service_key"        int,
--  "log_station_code"   int,
--  "sti_code"           int,
--  "effective_from"     varchar(254) DEFAULT NULL,
--  "effective_to"       varchar(254) DEFAULT NULL,
--  "log_sti"            int,
--  "barb"               varchar(254) DEFAULT NULL,
--  "epg_channel"        varchar(254) DEFAULT NULL,
--  "epg_group_name"     varchar(254) DEFAULT NULL,
--  "channel_name"       varchar(254) DEFAULT NULL,
--  "user_interface_description" varchar(254) DEFAULT NULL,
--  "amend_date"         varchar(254) DEFAULT NULL,
--  "comment"            varchar(254) DEFAULT NULL
-- )
-- ;
--
-- LOAD TABLE  slotsqa_service_key(
-- service_key',',
-- log_station_code',',
-- sti_code',',
-- effective_from',',
-- effective_to',',
-- log_sti',',
-- barb',',
-- epg_channel',',
-- epg_group_name',',
-- channel_name',',
-- user_interface_description',',
-- amend_date',',
-- comment'\n'
-- )
-- FROM '/SKP2x2f1/prod/sky/olive/data/share/clarityq/export/Jon/slots/service_key.csv'
-- QUOTES OFF
-- ESCAPES OFF
-- NOTIFY 1000
-- delimited by ','
-- ;
--


grant all on barb_spot_data to public;
grant all on barb_promo_data to public;
grant all on barb_data_amends to public;
grant all on landmark_data to public;
grant all on bss_data to public;
grant all on attribution_data to public;
grant all on attribution_audit_data to public;
grant all on slotsqa_spot_reporting to public;
grant all on slotsqa_service_key to public;
grant all on slotsqa_service_key_attributes to public;
grant all on slotsqa_service_key_landmark to public;

------------------------------------
--BARB Spot Data import checks
------------------------------------

--Test 1.2 Perform a count of the number of records by date, panel, log station code and STI code.
  select count(1) as counts
        ,date_of_transmission
        ,reporting_panel_code
        ,log_station_code_for_spot
        ,split_transmission_indicator
    from barb_spot_data
group by date_of_transmission
        ,reporting_panel_code
        ,log_station_code_for_spot
        ,split_transmission_indicator
;

--Test 1.3 Match the data to the data on the [1.3 spot reporting] tab, by panel, log station code, STI code,  where spot = 'S'.
--The underlying data provides memo spots for a number of the panels which enables the results to be analysed e.g. for ITV regions.  We are only interested in sold spots.
  select date_of_transmission
        ,reporting_panel_code
        ,log_station_code_for_spot
        ,split_transmission_indicator
        ,count(1) as counts
--    into slotsqa_barbtest2
    from slotsqa_spot_reporting as slt
         right join barb_spot_data as bar on bar.reporting_panel_code         = slt.panel_code
                                         and bar.log_station_code_for_spot    = cast(slt.log_station_code as int)
                                         and bar.split_transmission_indicator = cast(slt.sti as int)
   where spot = 'S'
group by date_of_transmission
        ,reporting_panel_code
        ,log_station_code_for_spot
        ,split_transmission_indicator
;

--Test 1.5 Ensure that inserts and deletes are processed correctly
  select count(1)
        ,insert_delete_amend_code
    from barb_spot_data
group by insert_delete_amend_code
;

--Test 1.9 Check accuracy of input file against an independent source.
  select date_of_transmission
        ,break_start_time
        ,right('00000' || log_station_code_for_spot, 5) || split_transmission_indicator as code
    into #codecount
    from barb_spot_data
group by date_of_transmission
        ,break_start_time
        ,code
;

--Test 1.15 Perform a count of break start times by day by log station code & STI code to find missing data
  select code
        ,sum(case when date_of_transmission = '2012-05-28' then 1 else 0 end) as d20120528
        ,sum(case when date_of_transmission = '2012-05-29' then 1 else 0 end) as d20120529
        ,sum(case when date_of_transmission = '2012-05-30' then 1 else 0 end) as d20120530
        ,sum(case when date_of_transmission = '2012-05-31' then 1 else 0 end) as d20120531
        ,sum(case when date_of_transmission = '2012-06-01' then 1 else 0 end) as d20120601
        ,sum(case when date_of_transmission = '2012-06-02' then 1 else 0 end) as d20120602
        ,sum(case when date_of_transmission = '2012-06-03' then 1 else 0 end) as d20120603
        ,sum(case when date_of_transmission = '2012-06-04' then 1 else 0 end) as d20120604
    from #codecount
group by code
order by code
;

  select distinct(service_key)
    into #keys4
    from slotsqa_service_key_attributes
;

  select kys.service_key
        ,right('00000' || ser.log_station_code, 5) || ser.sti_code as code
    into #codes4
    from #keys4 as kys
         left join slotsqa_service_key as ser on cast(kys.service_key as int) = cast(ser.service_key as int)
   where code <> '00000'
group by kys.service_key
        ,code
; --

  select code
        ,min(co4.service_key) as service_key
        ,min(barb) as barb
    into #lookup
    from #codes4 as co4
         inner join slotsqa_service_key as slo on cast(co4.service_key as int) = cast(slo.service_key as int)
group by code
;

  select cod.code
        ,min(lkp.service_key)
        ,min(lkp.barb)
        ,count(1)
    from #codecount as cod
         inner join #lookup as lkp on cod.code = lkp.code
group by cod.code
;


--Test 1.10 Create a table of spots which do not have bits 3 or 4 set (binary) and are not 00 - these are not carried on the Sky platform.
--Note to do this, the source field is imported as an alpha.  Exclude all instances of "00".  Convert the field to binary then back to alpha,
--anything that does not have a 1 in 3rd or 4th character from the right shold be included in the exceptions list
  select spot_platform_indicator               as spi
        ,case when right(spot_platform_indicator,1) in ('4','5','6','7','C','D','E','F') then 1 else 0 end bit3_set
        ,case when right(spot_platform_indicator,1) in ('8','9','A','B','C','D','E','F') then 1 else 0 end bit4_set
        ,count(1) as counts
    from barb_spot_data
group by spi
        ,bit3_set
        ,bit4_set
;

  select *
--    into slotsqa_barbtest4
    from barb_spot_data
   where right(spot_platform_indicator,1) not in ('0','1','2','3')
;

--Test 1.11 Perform a count by date, log station code, STI code (excluding non-digital stellite).
  select count(1) as counts
        ,date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
    from barb_spot_data                   as bar
         left join slotsqa_spot_reporting as sre on bar.reporting_panel_code         = sre.panel_code
                                                and bar.log_station_code_for_spot    = cast(sre.log_station_code as int)
                                                and bar.split_transmission_indicator = cast(sre.sti as int)
   where right(spot_platform_indicator,1) in ('0','1','2','3')
     and spot = 'S'
group by date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
;

--Test 1.11a This is temporary - all sold spots should be included in the final solution, but we know that CBI have only used panel 50.  Include count of the number of sold spots that have been excluded.
  select count(1) as counts
    from barb_spot_data                   as bar
         left join slotsqa_spot_reporting as sre on bar.reporting_panel_code         = sre.panel_code
                                                and bar.log_station_code_for_spot    = cast(sre.log_station_code as int)
                                                and bar.split_transmission_indicator = cast(sre.sti as int)
   where right(spot_platform_indicator,1) in ('0','1','2','3')
     and spot = 'S'
     and reporting_panel_code <> '50'
;

--Test 1.11b Perform a count by date, log station code, STI code. (excluding non-digital stellite and panel 50)
  select count(1) as counts
        ,date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
    from barb_spot_data                   as bar
         left join slotsqa_spot_reporting as sre on bar.reporting_panel_code         = sre.panel_code
                                                and bar.log_station_code_for_spot    = cast(sre.log_station_code as int)
                                                and bar.split_transmission_indicator = cast(sre.sti as int)
   where right(spot_platform_indicator,1) in ('0','1','2','3')
     and spot = 'S'
     and reporting_panel_code <> '50'
group by date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
;

--Test 1.12 Expand the data using the channel map to service key by joining on log station code and STI code [1.4 SERVICE_KEY_BARB]
  select top 100 *
    from slotsqa_service_key as ser
         left join slotsqa_spot_reporting as rep on cast(ser.log_sti as varchar) = right('00000' || rep.log_station_code, 5) || rep.sti
where rep.sti is null
;

--Test 1.13 Check that all service_keys have an entry of BARB in the spot_source in [3.1 service_key_attributes tab]
  select right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator as code
        ,count(1) as counts
    into #codes
    from barb_spot_data as bas
group by code
; --347

  select service_key
        ,sum(counts) as counters
    into #keys
    from #codes as cod
         left join slotsqa_service_key as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by service_key
; --316

drop table slotsqa_keys;
  select kys.service_key,spot_source
        ,sum(counters) as counts
    into slotsqa_keys
    from #keys as kys
         left join slotsqa_service_key_attributes as att on cast(kys.service_key as int) = cast(att.service_key as int)
group by kys.service_key,spot_source
; --

select sum(counts),spot_source from slotsqa_keys group by spot_source;
select * from slotsqa_keys where spot_source <>'BARB';

--Test 1.14 Check any entries in [3.1 service_key_attributes tab] that do not have BARB data.
--Note in some instance this will be possible if a spot log has not been returned.
  select distinct(service_key)
    into #keys2
    from slotsqa_service_key_attributes
   where promo_source = 'BARB'
; --323

  select kys.service_key
        ,right('00000' || ser.log_station_code, 5) || ser.sti_code as code
    into #codes2
    from #keys2 as kys
         left join slotsqa_service_key as ser on cast(kys.service_key as int)= cast(ser.service_key as int)
group by kys.service_key
        ,code
; --651

  select service_key
         ,code
    from #codes2 as cod
         left join barb_spot_data as bas on cod.code = right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator
   where bas.log_station_code_for_spot is null
     and code = '00000'
group by service_key
        ,code
; --all have a code

select log_sti as code
,barb
into #codes
from slotsqa_service_key
group by code,barb

  select ser.log_sti
        ,barb
    from slotsqa_service_key as ser
         left join barb_spot_data as bas on cast(ser.log_sti as varchar) = right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator
   where bas.log_station_code_for_spot is null
group by ser.log_sti
        ,barb
; --101 service keys have no data


------------------------------------
--Landmark Data import checks
------------------------------------
--Test 2.2
select * from landmark_data
where status <> 'S'
;

--Test 2.3 Check that all service keys in the file have an entry of Landmark in the spot source in [3.1 service key attributes tab]
  select skl.service_key
        ,count(1) as cow
    into #keys23
    from landmark_data                          as lan
         inner join slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
group by skl.service_key
; --223

  select kys.service_key
         ,spot_source
    from #keys23                                   as kys
         inner join slotsqa_service_key_attributes as ska on kys.service_key = ska.service_key
   where spot_source <> 'Landmark'
;

--Test 2.8 Find any service keys in service key attributes that do not have a matching sare number
  select distinct(ska.service_key)
    from slotsqa_service_key_attributes         as ska
         left join slotsqa_service_key_landmark as skl on ska.service_key = skl.service_key
where skl.sare_no is null
;

--Test 2.4 Check any entries in [3.1 service key attributes tab] for which we don't have spot data
  select distinct(service_key)
    into #keys24
    from slotsqa_service_key_attributes
   where spot_source = 'Landmark'
; --185

  select cast(sare_no as int) as sare_no
    into #sares
    from #keys24                                 as kys
         inner join slotsqa_service_key_landmark as skl on kys.service_key = skl.service_key
; --184

  select distinct(sare_no)
    into #landmark_sares
    from landmark_data
; --185

  select sar.sare_no
    from #sares                    as sar
         left join #landmark_sares as lan on sar.sare_no = lan.sare_no
   where lan.sare_no is null
;

--Test 2.6
  select lan.sare_no
        ,count(1) as cow
    from landmark_data                          as lan
         left join slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
   where skl.sare_no is null
group by lan.sare_no
;

--Test 2.7
  select lan.sare_no
        ,count(1) as cow
    from landmark_data                          as lan
         left join slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
   where skl.service_key is null
group by lan.sare_no
;

------------------------------------
--BARB v Landmark cross check
------------------------------------
--Test 3.1 Summarise by landmark and BARB data by service key and match for each day
  select distinct(service_key)
        ,cast (0 as bit) as barb
        ,cast (0 as bit) as landmark
    into #keys
    from slotsqa_service_key_attributes
   where barb_mapped = 'Yes'
;

  select right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator as code
        ,date_of_transmission
        ,count(1) as counts
    into #codes
    from barb_spot_data as bas
group by code
        ,date_of_transmission
; --2571

  select service_key
        ,date_of_transmission
    into #keys31
    from #codes as cod
         left join slotsqa_service_key as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by service_key
        ,date_of_transmission
;--2380

  select skl.service_key
        ,SLOT_START_BROADCAST_DATE
        ,count(1) as cow
    into #keys312
    from landmark_data                          as lan
         inner join slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
group by skl.service_key
        ,SLOT_START_BROADCAST_DATE
;--3693

  update #keys as kys
     set barb = 1
    from #keys31 as bar
   where cast(kys.service_key as int)= bar.service_key
;--311

  update #keys as kys
     set landmark = 1
    from #keys312 as lan
   where kys.service_key = lan.service_key
;--130

  select kys.service_key
        ,max(case when date_of_transmission      = '2012-05-28' then 1 else 0 end) as d2011d05d28_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-05-28' then 1 else 0 end) as d2011d05d28_landmark
        ,max(case when date_of_transmission      = '2012-05-29' then 1 else 0 end) as d2011d05d29_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-05-29' then 1 else 0 end) as d2011d05d29_landmark
        ,max(case when date_of_transmission      = '2012-05-30' then 1 else 0 end) as d2011d05d30_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-05-30' then 1 else 0 end) as d2011d05d30_landmark
        ,max(case when date_of_transmission      = '2012-05-31' then 1 else 0 end) as d2011d05d31_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-05-31' then 1 else 0 end) as d2011d05d31_landmark
        ,max(case when date_of_transmission      = '2012-06-01' then 1 else 0 end) as d2011d06d01_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-06-01' then 1 else 0 end) as d2011d06d01_landmark
        ,max(case when date_of_transmission      = '2012-06-02' then 1 else 0 end) as d2011d06d02_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-06-02' then 1 else 0 end) as d2011d06d02_landmark
        ,max(case when date_of_transmission      = '2012-06-03' then 1 else 0 end) as d2011d06d03_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-06-03' then 1 else 0 end) as d2011d06d03_landmark
        ,max(case when date_of_transmission      = '2012-06-04' then 1 else 0 end) as d2011d06d04_barb
        ,max(case when SLOT_START_BROADCAST_DATE = '2012-06-04' then 1 else 0 end) as d2011d06d04_landmark
    from #keys              as kys
         left join #keys31  as k31 on cast(kys.service_key as int) = cast(k31.service_key as int)
         left join #keys312 as k32 on cast(kys.service_key as int) = cast(k32.service_key as int)
group by kys.service_key
;

--Test 3.2
drop table #keys;
drop table #codes;

  select cast(service_key as int)
    into #keys
    from slotsqa_service_key_attributes
   where barb_mapped = 'Yes'
     and spot_source = 'Landmark'
group by service_key
;--108

  select ser.log_station_code
        ,ser.sti_code
    into #codes
    from #keys as kys
         inner join slotsqa_service_key as ser on kys.service_key = ser.service_key
group by ser.log_station_code
        ,ser.sti_code
;--100

create hg index idx1 on #codes(log_station_code,sti_code);
create hg index idx1 on barb_spot_data(log_station_code_for_spot,split_transmission_indicator);
create hg index idx2 on landmark_data(SLOT_START_BROADCAST_TIME_HOURS);
create hg index idx3 on landmark_data(SLOT_START_TIME_MINUTES);
create hg index idx4 on landmark_data(SLOT_START_TIME_SECONDS);

select count(1) from (
  select date_of_transmission,spot_start_time
    from barb_spot_data          as bar
         inner join #codes       as cod on bar.log_station_code_for_spot = cod.log_station_code
                                       and bar.split_transmission_indicator = cod.sti_code
         left join landmark_data as lan on bar.date_of_transmission = lan.SLOT_START_BROADCAST_DATE
                                       and bar.break_start_time     = lan.SLOT_START_BROADCAST_TIME_HOURS || right('0' || SLOT_START_TIME_MINUTES, 2) || right('0' || SLOT_START_TIME_SECONDS, 2)
   where lan.SLOT_START_BROADCAST_DATE is null
group by date_of_transmission,spot_start_time
) as sub
;--5485 missing

select count(1) from (
  select date_of_transmission,spot_start_time
    from barb_spot_data          as bar
group by date_of_transmission,spot_start_time
) as sub
;--out of 391,541

select 5485.0/391541.0

--Test 3.3
drop table #spots33a;
drop table #spots33b;
drop table #sares;

  select sales_house_identifier, count(1) as cow
    into #spots33a
    from barb_spot_data    as bar
         inner join #codes as cod on bar.log_station_code_for_spot    = cod.log_station_code
                                 and bar.split_transmission_indicator = cod.sti_code
group by sales_house_identifier
;--11

  select sare_no
    into #sares
    from slotsqa_service_key_landmark as skl
         inner join #keys             as kys on cast(skl.service_key as int) = kys.service_key
;

  select BARB_SALES_HOUSE_ID, count(1) as cow
    into #spots33b
    from landmark_data     as lan
         inner join #sares as sar on lan.sare_no = cast(sar.sare_no as int)
group by BARB_SALES_HOUSE_ID
;--12

select * from #spots33a
select * from #spots33b

--Test 3.4
  select count(1) as cow,spot_duration
    from barb_spot_data    as bar
         inner join #codes as cod on bar.log_station_code_for_spot    = cod.log_station_code
                                 and bar.split_transmission_indicator = cod.sti_code
group by spot_duration
;--11

  select count(1) as cow,SLOT_DURATION
    from landmark_data     as lan
         inner join #sares as sar on lan.sare_no = cast(sar.sare_no as int)
group by SLOT_DURATION
;--11

--Test 3.5
select count(1) from (
  select count(1) as cow
    from barb_spot_data    as bar
         inner join #codes as cod on bar.log_station_code_for_spot    = cod.log_station_code
                                 and bar.split_transmission_indicator = cod.sti_code
group by clearcast_commercial_number
) as sub
;--1643

select count(1) from (
  select CLEARCAST_COMMERCIAL_NO, count(1) as cow
    from landmark_data     as lan
         inner join #sares as sar on lan.sare_no = cast(sar.sare_no as int)
group by CLEARCAST_COMMERCIAL_NO
) as sub
;--1

--Test 3.6
  select spot_type,count(1) as cow
    from barb_spot_data    as bar
         inner join #codes as cod on bar.log_station_code_for_spot    = cod.log_station_code
                                 and bar.split_transmission_indicator = cod.sti_code
group by spot_type
;--

  select MEDIA_SPOT_TYPE, count(1) as cow
    from landmark_data     as lan
         inner join #sares as sar on lan.sare_no = cast(sar.sare_no as int)
group by MEDIA_SPOT_TYPE
;--

--Test 3.7
select count(1) from (
  select count(1) as cow,Campaign_Approval_ID
    from barb_spot_data    as bar
         inner join #codes as cod on bar.log_station_code_for_spot    = cod.log_station_code
                                 and bar.split_transmission_indicator = cod.sti_code
group by Campaign_Approval_ID
) as sub
;--1
select min(CAMPAIGN_APPROVAL_ID), max(CAMPAIGN_APPROVAL_ID) from barb_spot_data;

select count(1) from (
  select count(1) as cow
    from landmark_data     as lan
         inner join #sares as sar on lan.sare_no = cast(sar.sare_no as int)
group by CAMPAIGN_APPROVAL_ID
) as sub
;--1760

--SJ Test 3 Tests
-- Correct duplication in the spots reporting table
-- Film 24 4319 has been replaced with Sony TV 4319
delete
from sj_slotsqa_spot_reporting
where db2_station = '4319'
and description = 'Film 24 (was Bonanza (was Action! 241 (wasSoundtrack)'

/****** Barb versus Landmark Comparison ******/

select top 100* from barb_spot_data
select top 100* from landmark_data


--Test 3.0

select slot_start_broadcast_date, count(*)
from landmark_data
group by slot_start_broadcast_date
order by slot_start_broadcast_date

select date_of_transmission, count(*)
from barb_spot_data
group by date_of_transmission
order by date_of_transmission

/*
slot_start_broadcast_date
2012-02-01
2012-02-02
2012-02-03
2012-02-04
2012-02-05
2012-05-24
2012-05-25
2012-05-26
2012-05-27
2012-05-28
2012-05-29
2012-05-30
2012-05-31
2012-06-01
2012-06-02
2012-06-03
2012-06-04
2012-06-05

date_of_transmission
2012-05-28
2012-05-29
2012-05-30
2012-05-31
2012-06-01
2012-06-02
2012-06-03
2012-06-04
*/

--Test 3.0

select   slot_start_broadcast_date
        ,count(distinct service_key)
from landmark_data as lan
        inner join sj_slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
--where slot_start_broadcast_date in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by slot_start_broadcast_date
order by slot_start_broadcast_date


select date_of_transmission
        ,count(distinct service_key)
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
where date_of_transmission in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by date_of_transmission
order by date_of_transmission

-- Test 3.1a

--LANDMARK

select   slot_start_broadcast_date
        ,lan.SLOT_START_BROADCAST_TIME_HOURS || right('0' || lan.SLOT_START_TIME_MINUTES, 2) || right('0' || lan.SLOT_START_TIME_SECONDS, 2) as spot_start_time
        ,skl.service_key
        ,spot_source
        ,BARB_SALES_HOUSE_ID
        ,SLOT_DURATION
into #landmark -- drop table #landmark
from landmark_data as lan
        left join sj_slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
        left join sj_slotsqa_service_key_attributes as att on att.service_key = skl.service_key
--where slot_start_broadcast_date in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by slot_start_broadcast_date, spot_start_time, skl.service_key, spot_source,BARB_SALES_HOUSE_ID,SLOT_DURATION
order by slot_start_broadcast_date, spot_start_time, skl.service_key, spot_source,BARB_SALES_HOUSE_ID,SLOT_DURATION

--1178021 Row(s) affected

-- BARB

select date_of_transmission
        ,spot_start_time
        ,ser.service_key
        ,spot_source
        ,sales_house_identifier
        ,SPOT_DURATION as SLOT_DURATION
into #barb -- drop table #barb
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
       left join sj_slotsqa_service_key_attributes as att on att.service_key = ser.service_key
where date_of_transmission in ('20120528','20120529','20120530','20120531','20120601','20120602','20120603','20120604')
group by date_of_transmission, spot_start_time, ser.service_key, spot_source,sales_house_identifier,SLOT_DURATION
order by date_of_transmission, spot_start_time, ser.service_key, spot_source,sales_house_identifier,SLOT_DURATION

--1030552 Row(s) affected


 select distinct(service_key)
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
        ,spot_source
    into #keys -- drop table #keys
    from sj_slotsqa_service_key_attributes
;

select service_key
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb
group by service_key;

select service_key
       ,count(case when slot_start_broadcast_date = '20120528' then spot_start_time else null end) as landmark_28
       ,count(case when slot_start_broadcast_date = '20120529' then spot_start_time else null end) as landmark_29
       ,count(case when slot_start_broadcast_date = '20120530' then spot_start_time else null end) as landmark_30
       ,count(case when slot_start_broadcast_date = '20120531' then spot_start_time else null end) as landmark_31
       ,count(case when slot_start_broadcast_date = '20120601' then spot_start_time else null end) as landmark_01
       ,count(case when slot_start_broadcast_date = '20120602' then spot_start_time else null end) as landmark_02
       ,count(case when slot_start_broadcast_date = '20120603' then spot_start_time else null end) as landmark_03
       ,count(case when slot_start_broadcast_date = '20120604' then spot_start_time else null end) as landmark_04
into #landmark_keys -- drop table #landmark_keys
from #landmark
group by service_key
order by service_key;

Update #keys as base
set     base.barb_28 = ba.barb_28
        ,base.barb_29 = ba.barb_29
        ,base.barb_30 = ba.barb_30
        ,base.barb_31 = ba.barb_31
        ,base.barb_01 = ba.barb_01
        ,base.barb_02 = ba.barb_02
        ,base.barb_03 = ba.barb_03
        ,base.barb_04 = ba.barb_04
from #keys as base
        inner join #barb_keys as ba on base.service_key = ba.service_key;


Update #keys as base
set     base.landmark_28 = ba.landmark_28
        ,base.landmark_29 = ba.landmark_29
        ,base.landmark_30 = ba.landmark_30
        ,base.landmark_31 = ba.landmark_31
        ,base.landmark_01 = ba.landmark_01
        ,base.landmark_02 = ba.landmark_02
        ,base.landmark_03 = ba.landmark_03
        ,base.landmark_04 = ba.landmark_04
from #keys as base
        inner join #landmark_keys as ba on base.service_key = ba.service_key;

select * from #keys
order by
(case when spot_source = 'Landmark' then 1
              when spot_source = 'BARB' then 2
              else 3 end)

--Test 3.3


select bas.date_of_transmission
       ,bas.spot_start_time
       ,ser.service_key
       ,bas.spot_duration
       ,bas.clearcast_commercial_number
       ,bas.spot_type
       ,bas.Campaign_Approval_ID
       ,bas.Campaign_Approval_ID_Version_number
       ,cast(sales_house_identifier as int) as sales_house_identifier
       ,cast(0 as int) as landmark
       ,cast(0 as int) as landmark_slot_duration
       ,cast(0 as int) as landmark_CLEARCAST_COMMERCIAL_NO
       ,cast(0 as varchar(10)) as landmark_MEDIA_SPOT_TYPE
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_ID
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_VERSION
       ,cast(0 as int) as landmark_BARB_SALES_HOUSE_ID
into #barb_spots -- drop table #barb_spots
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
       left join sj_slotsqa_service_key_attributes as att on att.service_key = ser.service_key
where spot_source = 'Landmark'
group by bas.date_of_transmission, bas.spot_start_time, ser.service_key,bas.spot_duration,bas.clearcast_commercial_number,bas.spot_type
, bas.Campaign_Approval_ID,bas.Campaign_Approval_ID_Version_number,sales_house_identifier;

select lan.slot_start_broadcast_date
       ,lan.SLOT_START_BROADCAST_TIME_HOURS || right('0' || lan.SLOT_START_TIME_MINUTES, 2) || right('0' || lan.SLOT_START_TIME_SECONDS, 2) as spot_start_time
       ,skl.service_key
       ,slot_duration
       ,cast(lan.CLEARCAST_COMMERCIAL_NO as int)
       ,MEDIA_SPOT_TYPE
       ,CAMPAIGN_APPROVAL_ID
       ,CAMPAIGN_APPROVAL_VERSION
       ,cast(BARB_SALES_HOUSE_ID as int) as BARB_SALES_HOUSE_ID
into #landmark_spots -- drop table #landmark_spots
from landmark_data as lan
        left join sj_slotsqa_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
        left join sj_slotsqa_service_key_attributes as att on att.service_key = skl.service_key
where spot_source = 'Landmark'
group by lan.slot_start_broadcast_date, spot_start_time, skl.service_key
, slot_duration,CLEARCAST_COMMERCIAL_NO,MEDIA_SPOT_TYPE,CAMPAIGN_APPROVAL_ID,CAMPAIGN_APPROVAL_VERSION,BARB_SALES_HOUSE_ID;


Update #barb_spots
set landmark = 1
    ,bs.landmark_slot_duration = ls.Slot_duration
    --,bs.landmark_CLEARCAST_COMMERCIAL_NO = ls.CLEARCAST_COMMERCIAL_NO
    ,bs.landmark_MEDIA_SPOT_TYPE = ls.MEDIA_SPOT_TYPE
    ,bs.landmark_CAMPAIGN_APPROVAL_ID = ls.CAMPAIGN_APPROVAL_ID
    ,bs.landmark_CAMPAIGN_APPROVAL_VERSION = ls.CAMPAIGN_APPROVAL_VERSION
    ,bs.landmark_BARB_SALES_HOUSE_ID = BARB_SALES_HOUSE_ID
from #barb_spots as bs
        inner join #landmark_spots as ls on bs.date_of_transmission = ls.slot_start_broadcast_date
                                        and bs.spot_start_time = ls.spot_start_time
                                        and bs.service_key = ls.service_key

-- Identifier

drop table #keys
drop table  #barb_keys
drop table #landmark_keys

 select distinct(identifier)
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
    into #keys -- drop table #keys
    from (    select cast(BARB_SALES_HOUSE_ID as  int) as identifier from #landmark_spots
        union select cast(sales_house_identifier as int) as identifier from #barb_spots) as h
;

select cast(sales_house_identifier as int) as identifier
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb_spots
where landmark = 1
group by identifier;

select cast(landmark_BARB_SALES_HOUSE_ID as int) as identifier
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as landmark_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as landmark_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as landmark_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as landmark_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as landmark_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as landmark_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as landmark_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as landmark_04
into #landmark_keys -- drop table #landmark_keys
from #barb_spots
where landmark = 1
group by identifier
order by identifier;

Update #keys as base
set     base.barb_28 = ba.barb_28
        ,base.barb_29 = ba.barb_29
        ,base.barb_30 = ba.barb_30
        ,base.barb_31 = ba.barb_31
        ,base.barb_01 = ba.barb_01
        ,base.barb_02 = ba.barb_02
        ,base.barb_03 = ba.barb_03
        ,base.barb_04 = ba.barb_04
from #keys as base
        inner join #barb_keys as ba on base.identifier = ba.identifier;


Update #keys as base
set     base.landmark_28 = ba.landmark_28
        ,base.landmark_29 = ba.landmark_29
        ,base.landmark_30 = ba.landmark_30
        ,base.landmark_31 = ba.landmark_31
        ,base.landmark_01 = ba.landmark_01
        ,base.landmark_02 = ba.landmark_02
        ,base.landmark_03 = ba.landmark_03
        ,base.landmark_04 = ba.landmark_04
from #keys as base
        inner join #landmark_keys as ba on base.identifier = ba.identifier;

select * from #keys
order by identifier


--Test 3.4

drop table #keys
drop table  #barb_keys
drop table #landmark_keys

 select distinct(SLOT_DURATION) as  SLOT_DURATION
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
    into #keys -- drop table #keys
    from (    select cast(SLOT_DURATION as  int) as SLOT_DURATION from #landmark_spots
        union select cast(SPOT_DURATION as int) as SLOT_DURATION from #barb_spots) as h
;

select cast(SPOT_DURATION as int) as SLOT_DURATION
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb_spots
where  landmark=1
group by SLOT_DURATION;

select cast(landmark_SLOT_DURATION as int) as SLOT_DURATION
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as landmark_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as landmark_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as landmark_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as landmark_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as landmark_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as landmark_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as landmark_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as landmark_04
into #landmark_keys -- drop table #landmark_keys
from #barb_spots
where landmark=1
group by SLOT_DURATION
order by SLOT_DURATION;

Update #keys as base
set     base.barb_28 = ba.barb_28
        ,base.barb_29 = ba.barb_29
        ,base.barb_30 = ba.barb_30
        ,base.barb_31 = ba.barb_31
        ,base.barb_01 = ba.barb_01
        ,base.barb_02 = ba.barb_02
        ,base.barb_03 = ba.barb_03
        ,base.barb_04 = ba.barb_04
from #keys as base
        inner join #barb_keys as ba on base.SLOT_DURATION = ba.SLOT_DURATION;


Update #keys as base
set     base.landmark_28 = ba.landmark_28
        ,base.landmark_29 = ba.landmark_29
        ,base.landmark_30 = ba.landmark_30
        ,base.landmark_31 = ba.landmark_31
        ,base.landmark_01 = ba.landmark_01
        ,base.landmark_02 = ba.landmark_02
        ,base.landmark_03 = ba.landmark_03
        ,base.landmark_04 = ba.landmark_04
from #keys as base
        inner join #landmark_keys as ba on base.SLOT_DURATION = ba.SLOT_DURATION;

select spot_duration, landmark_slot_duration, count(*) from #barb_spots
where landmark = 1
group by spot_duration, landmark_slot_duration
order by spot_duration, landmark_slot_duration


-- test 3.3

select landmark_BARB_SALES_HOUSE_ID, sales_house_identifier, count(*)
from #barb_spots
where landmark = 1
group by landmark_BARB_SALES_HOUSE_ID, sales_house_identifier
order by landmark_BARB_SALES_HOUSE_ID, sales_house_identifier

--test 3.4

select landmark_slot_duration, spot_duration, count(*)
from #barb_spots
--where landmark = 1
group by landmark_slot_duration, spot_duration
order by landmark_slot_duration, spot_duration

--test 3.5

select landmark_media_spot_type, spot_type, count(*)
from #barb_spots
where landmark = 1
group by landmark_media_spot_type, spot_type
order by landmark_media_spot_type, spot_type

--test 3.5

select landmark_Campaign_Approval_ID, Campaign_Approval_ID, count(*)
from #barb_spots
where landmark = 1
group by landmark_Campaign_Approval_ID, Campaign_Approval_ID
order by landmark_Campaign_Approval_ID, Campaign_Approval_ID

--test 3.6

select landmark_CAMPAIGN_APPROVAL_VERSION, Campaign_Approval_ID_Version_number, count(*)
from #barb_spots
where landmark = 1
group by landmark_CAMPAIGN_APPROVAL_VERSION, Campaign_Approval_ID_Version_number
order by landmark_CAMPAIGN_APPROVAL_VERSION, Campaign_Approval_ID_Version_number

CAMPAIGN_APPROVAL_VERSION = Campaign_Approval_ID_Version Number


-- test 3.12

drop table #keys
drop table #barb
drop table #landmark

select distinct service_key
into #keys
from sj_slotsqa_service_key_attributes

select distinct service_key, cast(0 as bit) as attributes_table
into #barb
from sj_slotsqa_service_key

Update #barb
set attributes_table = 1
from #barb as ba
        inner join #keys as k on ba.service_key = k.service_key

select count(*) from #barb where attributes_table = 1
select * from #barb where attributes_table <> 1
-- 377
-- 370


select distinct service_key, cast(0 as bit) as attributes_table
into #landmark
from sj_slotsqa_service_key_landmark

Update #landmark
set attributes_table = 1
from #landmark as ba
        inner join #keys as k on ba.service_key = k.service_key

  select * from #landmark where attributes_table <> 1
-- 233
-- 232

select count(*) from #landmark where attributes_table = 1










------------------------------------
--Attribution Data import checks
------------------------------------
  select count(1)
        ,count(distinct commercial_number)
    from attribution_data;

select 1221-966;

  select count(1)
        ,count(distinct commercial_number || '@' || match_group)
    from attribution_data;

--Test 4.3
  select distinct(commercial_no)
    into #comms
    from (select distinct(clearcast_commercial_number) as commercial_no
            from barb_spot_data
           union
          select distinct(CLEARCAST_COMMERCIAL_NO)
            from landmark_data
        ) as sub
; --3791

  select commercial_no
    from #comms                     as com
         left join attribution_data as att on com.commercial_no = att.Commercial_Number
   where att.Commercial_Number is null
; --11


------------------------------------
--Match to CBI checks
------------------------------------
--- test 5.1 (see 3.1)


-- Sales house

drop table #keys
drop table  #barb_keys
drop table #landmark_keys

select bas.date_of_transmission
       ,bas.spot_start_time
       ,ser.service_key
       ,bas.spot_duration
       ,bas.clearcast_commercial_number
       ,bas.spot_type
       ,bas.Campaign_Approval_ID
       ,bas.Campaign_Approval_ID_Version_number
       ,cast(sales_house_identifier as int) as sales_house_identifier
       ,cast(0 as int) as landmark
       ,cast(0 as int) as landmark_slot_duration
       ,cast(0 as int) as landmark_CLEARCAST_COMMERCIAL_NO
       ,cast(0 as varchar(10)) as landmark_MEDIA_SPOT_TYPE
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_ID
       ,cast(0 as int) as landmark_CAMPAIGN_APPROVAL_VERSION
       ,cast(0 as int) as landmark_BARB_SALES_HOUSE_ID
into #barb_spots -- drop table #barb_spots
from barb_spot_data as bas
       left join sj_slotsqa_service_key as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
       left join sj_slotsqa_service_key_attributes as att on att.service_key = ser.service_key
where spot_source = 'BARB'
group by bas.date_of_transmission, bas.spot_start_time, ser.service_key,bas.spot_duration,bas.clearcast_commercial_number,bas.spot_type
, bas.Campaign_Approval_ID,bas.Campaign_Approval_ID_Version_number,sales_house_identifier;


 select distinct(identifier) as identifier
        ,cast (0 as int) as barb_28
        ,cast (0 as int) as barb_29
        ,cast (0 as int) as barb_30
        ,cast (0 as int) as barb_31
        ,cast (0 as int) as barb_01
        ,cast (0 as int) as barb_02
        ,cast (0 as int) as barb_03
        ,cast (0 as int) as barb_04
        ,cast (0 as int) as landmark_28
        ,cast (0 as int) as landmark_29
        ,cast (0 as int) as landmark_30
        ,cast (0 as int) as landmark_31
        ,cast (0 as int) as landmark_01
        ,cast (0 as int) as landmark_02
        ,cast (0 as int) as landmark_03
        ,cast (0 as int) as landmark_04
    into #keys -- drop table #keys
    from (select cast(sales_house_identifier as int) as identifier from #barb_spots) as h
;

select cast(sales_house_identifier as int) as identifier
       ,count(case when date_of_transmission = '20120528' then spot_start_time else null end) as barb_28
       ,count(case when date_of_transmission = '20120529' then spot_start_time else null end) as barb_29
       ,count(case when date_of_transmission = '20120530' then spot_start_time else null end) as barb_30
       ,count(case when date_of_transmission = '20120531' then spot_start_time else null end) as barb_31
       ,count(case when date_of_transmission = '20120601' then spot_start_time else null end) as barb_01
       ,count(case when date_of_transmission = '20120602' then spot_start_time else null end) as barb_02
       ,count(case when date_of_transmission = '20120603' then spot_start_time else null end) as barb_03
       ,count(case when date_of_transmission = '20120604' then spot_start_time else null end) as barb_04
into #barb_keys -- drop table  #barb_keys
from #barb_spots
group by identifier;

select * from #keys
order by identifier

--Test 5.4
--Sybase query
  select distinct (clearcast_commercial_number)
    from barb_spot_data
order by clearcast_commercial_no
;

--Netezza query
  select distinct (clearcast_commercial_no)
    from SMI_ACCESS.SMI_ETL.V_SLOT_DIM
order by clearcast_commercial_no



------------------------------------
--BSS Data import checks
------------------------------------
--Test 6.2 Check that all entries in the file are identified correctly in the [3.1 service key attributes table] with BSS in the promo_source column.  Use service_key as the match key
select count(1) from bss_data
--27004
  select count(1)
        ,case when ska.service_key is null then 0 else 1 end as exist
    from bss_data                         as bss
         left join slotsqa_service_key_attributes as ska on bss.si_service_key = ska.service_key
group by exist
;

--Test 6.3 Check for any entries in [3.1 service key atributes tab] that have BSS in the promo_source column, but for which we do not have data.  Use service key as the match key
  select distinct(service_key)
    into #keys3
    from slotsqa_service_key_attributes
   where promo_source='BSS'
; --106

  select service_key
    from #keys3 as kys
         left join bss_data as bss on cast(kys.service_key as int) = cast(bss.si_service_key as int)
   where bss.si_service_key is null
group by service_key
;--16

--The promo source info might be wrong, so let's just see what we've got. for Barb firstly
  select distinct(service_key)
    into #keys4
    from slotsqa_service_key_attributes
; --

  select kys.service_key
        ,right('00000' || ser.log_station_code, 5) || ser.sti_code as code
    into #codes4
    from #keys4 as kys
         left join slotsqa_service_key as ser on cast(kys.service_key as inT) = cast(ser.service_key as int)
   where code <> '00000'
group by kys.service_key
        ,code
; --

  select service_key
        ,code
    from #codes4 as cod
         inner join barb_spot_data as bas on cod.code = right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator
group by service_key
        ,code
;

--and for BSS
  select service_key
    from #keys4 as kys
         inner join bss_data as bas on cast(kys.service_key as int) = cast(bas.si_service_key as int)
group by service_key
;

--Test 6.4 Confirm whether the source file has the +1 channel data in e..g. service_key 1448 has BSS in the promo_source column, but 3620 the +1 channel is marked up as BARB.
--This is not strictly correct and there is not anything in this table that identifies the parent channel.
--We may need any additional column in the table to identify the service key of the parent channel so that the BSS data can be expanded and timeshifted correctly.
  select barb,count(1)
    from bss_data as bss
         inner join slotsqa_service_key as ser on cast(bss.si_service_key as int) = cast(ser.service_key as int)
   where ser.barb like '%+1%' or ser.barb like '%+ 1%'
group by barb
;


------------------------------------
--BARB Promo Data import checks
------------------------------------
--Test 7.2 Perform a count of the number of records by date, panel, log station code and STI code.
  select split_transmission_indicator,count(1)
    from barb_promo_data
group by split_transmission_indicator
;-- all sti are 0

  select count(1) as cow
        ,date_of_transmission
        ,reporting_panel_code
        ,log_station_code
    from barb_promo_data
group by date_of_transmission
        ,reporting_panel_code
        ,log_station_code
;

--Test 7.3 Perform a count by insert/delete code - if any records do not have values blank (original), I (insertion), D (deletion) then highlight these
  select count(1) as cow
        ,insert_delete_amend_code
    from barb_promo_data
group by insert_delete_amend_code
;

--Test 7.7 Create a table of spots which do not have bits 3 or 4 set (binary) and are not 00 - these are not carried on the Sky platform.
--Note to do this, the source field is imported as an alpha.  Exclude all instances of "00".
--Convert the field to binary then back to alpha, anything that does not have a 1 in 3rd or 4th character from the right shold be included in the exceptions list
  select count(1)
        ,platform_indicator
    from barb_promo_data
group by platform_indicator
;

--Test 7.8 Perform a count by date, log station code, STI code.

  select count(1) as cow
        ,date_of_transmission
        ,log_station_code
    from barb_promo_data
group by date_of_transmission
        ,log_station_code
order by date_of_transmission
        ,log_station_code
;

--Test 7.9 Expand the data using the channel map to service key by joining on log station code and STI code [1.4 SERVICE_KEY_BARB]
  select top 10 *
    from barb_promo_data               as bar
         left join slotsqa_service_key as ser on cast(ser.log_sti as varchar) = right('00000' || bar.log_station_code, 5) || bar.split_transmission_indicator
;

drop table #keys;
drop table #codes;

--Test 7.10 Check that all service_keys that have data have a Promo_source of BARB in [3.1 service key attributes tab]
  select right('00000' || bas.log_station_code, 5) || bas.split_transmission_indicator as code
        ,count(1) as counts
    into #codes
    from barb_promo_data as bas
group by code
; --61

  select code
        ,service_key
        ,sum(counts) as counters
    into #keys
    from #codes as cod
         left join slotsqa_service_key as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by code
        ,service_key
; --151

  select code,kys.service_key,promo_source
        ,sum(counters) as counts
    from #keys as kys
         left join slotsqa_service_key_attributes as att on cast(kys.service_key as int) = cast(att.service_key as int)
   where promo_source <> 'BARB' or promo_source is null
group by code,kys.service_key,promo_source
; --

drop table #keys;
drop table #codes;

--Test 7.11 Produce a list of any entries that have BARB in [3.1. service key atrributes tab], but for which we do not have any promo or sponsorship data.
  select service_key
    into #keys
    from slotsqa_service_key_attributes
   where promo_source = 'BARB'
group by service_key
; --323

  select right('00000' || ser.log_station_code, 5) || ser.sti_code as code
    into #codes
    from #keys as kys
         left join slotsqa_service_key as ser on cast(kys.service_key as int) = cast(ser.service_key as int)
group by code
;--360

drop table #barb;

  select right('00000' || log_station_code, 5) || split_transmission_indicator as code
    into #barb
    from barb_promo_data
group by code
;--61

  select cod.code
    from #codes as cod
         left join #barb as bar on cod.code = bar.code
   where bar.code is null
group by cod.code
;--300



drop table #codes;
drop table keys;

--Test 8.1
  select right('00000' || bas.log_station_code, 5) || bas.split_transmission_indicator as code
        ,count(1) as counts
    into #codes
    from barb_promo_data as bas
group by code
; --61

  select code
        ,service_key
        ,sum(counts) as counters
    into #keys
    from #codes as cod
         left join slotsqa_service_key as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by code
        ,service_key
; --162

  select service_key
        ,sum(case when date_of_transmission = '2012-05-28' then 1 else 0 end) as d2012d05d28_barb
        ,sum(case when date_of_transmission = '2012-05-29' then 1 else 0 end) as d2012d05d29_barb
        ,sum(case when date_of_transmission = '2012-05-30' then 1 else 0 end) as d2012d05d30_barb
        ,sum(case when date_of_transmission = '2012-05-31' then 1 else 0 end) as d2012d05d31_barb
        ,sum(case when date_of_transmission = '2012-06-01' then 1 else 0 end) as d2012d06d01_barb
        ,sum(case when date_of_transmission = '2012-06-02' then 1 else 0 end) as d2012d06d02_barb
        ,sum(case when date_of_transmission = '2012-06-03' then 1 else 0 end) as d2012d06d03_barb
        ,sum(case when date_of_transmission = '2012-06-04' then 1 else 0 end) as d2012d06d04_barb
    from barb_promo_data as bas
         left join #keys as kys on right('00000' || bas.log_station_code, 5) || bas.split_transmission_indicator = kys.code
group by service_key
;

  select SI_Service_Key
        ,sum(case when tx_date = '2012-05-28' then 1 else 0 end) as d2012d05d28_bss
        ,sum(case when tx_date = '2012-05-29' then 1 else 0 end) as d2012d05d29_bss
        ,sum(case when tx_date = '2012-05-30' then 1 else 0 end) as d2012d05d30_bss
        ,sum(case when tx_date = '2012-05-31' then 1 else 0 end) as d2012d05d31_bss
        ,sum(case when tx_date = '2012-06-01' then 1 else 0 end) as d2012d06d01_bss
        ,sum(case when tx_date = '2012-06-02' then 1 else 0 end) as d2012d06d02_bss
        ,sum(case when tx_date = '2012-06-03' then 1 else 0 end) as d2012d06d03_bss
        ,sum(case when tx_date = '2012-06-04' then 1 else 0 end) as d2012d06d04_bss
    from bss_data
group by SI_Service_Key
;


  select si_service_key,count(1) from bss_data group by si_service_key

  select distinct(service_key)
    into #keys4





--Test 8.1
alter table barb_promo_data add derived_service_key int;

  update barb_promo_data as bar
     set bar.derived_service_key = ser.service_key
    from slotsqa_service_key as ser
   where cast(bar.log_station_code as int)             = ser.log_station_code
     and cast(bar.split_transmission_indicator as int) = ser.sti_code
;

--Sybase query
  select cast(si_service_key as int) as service_key
        ,count(1)
    from bss_data
group by service_key
union
  select derived_service_key as service_key
        ,count(1)
    from barb_promo_data
group by service_key
;

--Netezza query
  select count(1), service_key
    from SMI_ACCESS.SMI_ETL.V_VIEWING_SLOT_INSTANCE_FACT as ins
         inner join SMI_ACCESS.SMI_ETL.V_CHANNEL_DIM     as chn on ins.DK_CHANNEL_DIM = chn.PK_CHANNEL_DIM
group by service_key
order by service_key

select * from bss_data where si_service_key='1475'
select tx_date, count(1) from bss_data group by tx_date
2012-05-27 22092
2012-05-28 27004
2012-05-29 28044
2012-05-30 26722
2012-05-31 26315
2012-06-01 26775
2012-06-02 22299
2012-06-04 23467








