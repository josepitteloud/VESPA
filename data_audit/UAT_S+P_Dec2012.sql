-- --For the load table statements to work, the files must be in csv format, and be called something.csv, so I have transferred the .dat_dis fields over to prod2x2 and renamed them as .csv




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

in Netezza as:
DIS_REFERENCE..SERVICE_KEY_CODE_MAPPING
DIS_REFERENCE..SERVICE_KEY_ATTRIBUTES


select * from
--truncate table
channel_map_dev_service_key_attributes
select * from
truncate table
channel_map_dev_service_key_landmark
select * from
truncate table
channel_map_dev_service_key_barb
select top 10 * from landmark_data

select log_station_code,sti_code,bar.service_key,full_name from Vespa_analysts.channel_map_dev_service_key_attributes as att inner join Vespa_analysts.channel_map_dev_service_key_barb as bar on att.service_key=bar.service_key
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
select top 10 * from barb_spot_data;
select top 10 * from barb_data_amends;
truncate table barb_spot_data;

--Load BARB CET files
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
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121207_CET_dat_dis.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''¿''')
;

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
--     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B' || replace(@dt,'-','') || '.PSP.dat_dis.csv''
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121207_PSP_dat_dis.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''¿''')
;

--BARB Spot data amendment files
drop table barb_data_amends;
create table barb_data_amends
(record_type varchar(500)
,insert_delete_amend_code varchar(500)
,date_of_transmission varchar(500)
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
,log_station_code_for_spot varchar(500)
,split_transmission_indicator varchar(500)
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

truncate table barb_data_amends;
drop table data_raw;
create table data_raw(dta varchar(1000));
truncate table data_raw;

LOAD TABLE barb_data_amends(
  record_type
,insert_delete_amend_code
,date_of_transmission
,reporting_panel_code
,log_station_code_for_break
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,spot_break_total_duration
,break_type
,spot_type
,broadcaster_spot_number
,station_code
,log_station_code_for_spot
,split_transmission_indicator
,spot_platform_indicator
,hd_simulcast_spot_platform_indicator
,spot_start_time
,spot_duration
,clearcast_commercial_number
,sales_house_brand_description
,preceding_programme_name
,succeding_programme_name
,sales_house_identifier
,campaign_approval_id
,campaign_approval_id_version_number
,interactive_spot_platform_indicator
,blank_for_padding'\n'
)
--     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121215.CE1.csv'
--     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121207.CE3.dat_dis.csv'
--     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121224.CE1.dat_dis.csv'
     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121224.CE3.dat_dis.csv'
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by '¿';

commit;
  insert into barb_data_amends(
  record_type
,insert_delete_amend_code
,date_of_transmission
,reporting_panel_code
,log_station_code_for_break
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,spot_break_total_duration
,break_type
,spot_type
,broadcaster_spot_number
,station_code
,log_station_code_for_spot
,split_transmission_indicator
,spot_platform_indicator
,hd_simulcast_spot_platform_indicator
,spot_start_time
,spot_duration
,clearcast_commercial_number
,sales_house_brand_description
,preceding_programme_name
,succeding_programme_name
,sales_house_identifier
,campaign_approval_id
,campaign_approval_id_version_number
,interactive_spot_platform_indicator
,blank_for_padding
)
  select substr(dta,  1,  1)
        ,substr(dta,  2,  1)
        ,substr(dta,  3,  8)
        ,substr(dta, 11,  2)
        ,substr(dta, 13,  3)
        ,substr(dta, 16,  1)
        ,substr(dta, 17,  2)
        ,substr(dta, 19,  6)
        ,substr(dta, 25,  3)
        ,substr(dta, 28,  2)

        ,substr(dta, 30,  2)
        ,substr(dta, 32,  8)
        ,substr(dta, 40,  2)
        ,substr(dta, 42,  5)
        ,substr(dta, 47,  1)
        ,substr(dta, 48,  2)
        ,substr(dta, 50,  1)
        ,substr(dta, 51,  6)
        ,substr(dta, 57,  2)
        ,substr(dta, 59, 13)

        ,substr(dta, 72, 40)
        ,substr(dta,112, 40)
        ,substr(dta,152,  5)
        ,substr(dta,157, 10)
        ,substr(dta,167,  5)
        ,substr(dta,172,  2)
        ,substr(dta,174,  2)
        ,substr(dta,176,500)
from data_raw
;

select top 100 * from barb_spot_data;
select top 1000 * from barb_data_amends;

--BARB Promo data amendment files
create table barb_promo_amends(record_type varchar(2)
                              ,insert_delete_amend_code varchar(1)
                              ,date_of_transmission varchar(8)
                              ,reporting_panel_code varchar(5)
                              ,station_code varchar(5)
                              ,log_station_code varchar(5)
                              ,break_split_transmission_indicator varchar(2)
                              ,break_platform_indicator varchar(2)
                              ,hd_simulcast_platform_indicator varchar(2)
                              ,break_start_time varchar(6)
                              ,break_end_time varchar(6)
                              ,broadcaster_transmission_code varchar(20)
                              ,interval_name varchar(100)
                              ,sponsorship_code varchar(15)
                              ,break_type varchar(2)
                              ,area_flags varchar(4)
                              ,content_id varchar(24)
                              ,isan_number varchar(34)
                              ,blank_for_padding varchar(500)
);


drop table data_raw;
truncate table data_raw
create table data_raw(dta varchar(1000));
LOAD TABLE data_raw(dta'\n')
--     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121207_PS2.csv'
--     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121215_PS2.csv'
     FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/B20121224.PS1.dat_dis.csv'
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000;
commit;

  insert into barb_promo_amends(record_type
                              ,insert_delete_amend_code
                              ,date_of_transmission
                              ,reporting_panel_code
                              ,station_code
                              ,log_station_code
                              ,break_split_transmission_indicator
                              ,break_platform_indicator
                              ,hd_simulcast_platform_indicator
                              ,break_start_time
                              ,break_end_time
                              ,broadcaster_transmission_code
                              ,interval_name
                              ,sponsorship_code
                              ,break_type
                              ,area_flags
                              ,content_id
                              ,isan_number
                              ,blank_for_padding
)
  select substr(dta,  1,  2)
        ,substr(dta,  3,  1)
        ,substr(dta,  4,  8)
        ,substr(dta, 12,  5)
        ,substr(dta, 17,  5)
        ,substr(dta, 22,  5)
        ,substr(dta, 27,  2)
        ,substr(dta, 29,  2)
        ,substr(dta, 31,  2)
        ,substr(dta, 33,  6)
        ,substr(dta, 39,  6)
        ,substr(dta, 45, 20)
        ,substr(dta, 65,100)
        ,substr(dta,165, 15)
        ,substr(dta,180,  2)
        ,substr(dta,182,  4)
        ,substr(dta,186, 24)
        ,substr(dta,210, 24)
        ,substr(dta,234,500)
from data_raw
;





select --top 100
date_of_transmission,spot_start_time,* from barb_data_amends
where insert_delete_amend_code<>'I'

select record_type,insert_delete_amend_code,count(1) from barb_data_amends group by record_type,insert_delete_amend_code;
select insert_delete_amend_code,count(1) from barb_data_amends group by insert_delete_amend_code;
select min(spot_start_time),max(spot_start_time) from barb_data_amends where spot_start_time is not null and spot_start_time<>''
select min(spot_start_time),max(spot_start_time) from barb_spot_data where spot_start_time is not null and spot_start_time<>''
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

truncate table bss_data
--set @dt='20120528';
--while @dt <= '20120604'
--begin
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
--     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/BSS-PROMO-SPONS_20121215021504_291_dat_dis.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''¿'''
     )
select slot_type,count(1) from bss_data group by slot_type
drop table landmark_data
--Landmark data
create table landmark_data(
       SARE_NO INTEGER,
       SLOT_START_BROADCAST_DATE varchar(50),
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
,EXTRA_DATE date
);

--select SLOT_START_BROADCAST_DATE,count(1) from landmark_data group by SLOT_START_BROADCAST_DATE
truncate table landmark_data;
load table landmark_data(
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
,EXTRA_DATE'\n'
)
  FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/landmark_dump_20121207_part4.csv'
  QUOTES OFF
  ESCAPES OFF
DELIMITED BY ','
  ;

select top 100 * from landmark_data
select sare_no,count(1) from landmark_data group by sare_no




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
drop table attribution_data;
create table attribution_data(
      Record_type varchar(500)
     ,Commercial_Number varchar(500)
     ,Match_Group varchar(500)
     ,Date_of_Transmission varchar(500)
     ,Buying_Agency_Code varchar(500)
     ,Buying_Agency_Name varchar(500)
     ,Advertiser_Code varchar(500)
     ,Advertiser_Name varchar(500)
     ,Holding_company_Code varchar(500)
     ,Holding_company_name varchar(500)
     ,Product_Code varchar(500)
     ,Product_Name varchar(500)
     ,NMR_Category_Code varchar(500)
     ,Clearcast_Telephone_Number varchar(500)
     ,Clearcast_Commercial_Title varchar(500)
     ,Spot_Length varchar(500)
     ,Clearcast_Web_Address varchar(500)
);

     execute('
     LOAD TABLE  attribution_data(
          Record_type''¿'',
          Commercial_Number''¿'',
          Match_Group''¿'',
          Date_of_Transmission''¿'',
          Buying_Agency_Code''¿'',
          Buying_Agency_Name''¿'',
          Advertiser_Code''¿'',
          Advertiser_Name''¿'',
          Holding_company_Code''¿'',
          Holding_company_name''¿'',
          Product_Code''¿'',
          Product_Name''¿'',
          NMR_Category_Code''¿'',
          Clearcast_Telephone_Number''¿'',
          Clearcast_Commercial_Title''¿'',
          Spot_Length''¿'',
          Clearcast_Web_Address''\n''
     )
--     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/aw20121126_sky_dat_dis.csv''
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/aw20121203_sky_dat_dis.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     delimited by ''''
     ')


--this is the backfill file
truncate table data_raw;
LOAD TABLE  data_raw(     dta'\n')
truncate table attribution_data

load table attribution_data(
      Record_type',',
      Commercial_number',',
      Match_Group',',
      Date_of_Transmission',',
      Buying_Agency_Code',',
      Buying_Agency_Name',',
      Advertiser_Code',',
      Advertiser_Name',',
      Holding_company_code',',
      Holding_company_name',',
      Product_Code',',
      Product_Name',',
      NMR_Category_Code',',
      Clearcast_Telephone_Number',',
      Clearcast_Commercial_Title',',
      Spot_Length',',
      Clearcast_Web_Address'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/slots/ma20120226.sky.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
delimited by ''
; --253,461

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

     execute('
     LOAD TABLE data_raw(
          dta''\n''
     )
--     FROM ''/SKP2x2f1/prod/sky/olive/data/share/clarityq/export/Jon/slots/aw_audit_' || replace(@dt,'-','') || '.sky.csv'' --these appear to contain all data for week commencing
     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/aw_audit_20121210.sky.csv''
--     FROM ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/UAT_S+P_Dec2012/aw_audit_20121217.sky.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     ')
;
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
select top 100 * from attribution_audit_data where name_type='P'
select top 100 * from attribution_data
where audit_action<>'1';
select name_type,count(1) from attribution_audit_data group by name_type
--there shouldn't be any, but there are 2, all blank though.

select top 100 *
    from attribution_audit_data as aud
        inner join attribution_data as att on att.commercial_number = aud.commercial_number
                                          and att.match_group       = aud.match_group
                                          and name_type = 'P'
                                          and att.product_code <> aud.code_from
                                          and att.product_code <> aud.code_to
;

--looking at one example:
select * from attribution_data
where commercial_number like 'TRAWTSM201030%'
;

select * from attribution_audit_data
where commercial_number like 'TRAWTSM201030%'
;

--The code has changed more than once. There are also some that are changed back to the original code, so we will have to keep loop by audit date, to get them in the right order
select max(audit_action_date) from attribution_audit_data
0000010
20121221

select audit_action_date,count(1) from attribution_audit_data group by audit_action_date
order by audit_action_date
20121213


select cast(match_group as int) as m,count(1) from attribution_data group by m
select cast(substr(match_group,2,1) as int) as m,count(1) from attribution_data group by m
select top 10 * from attribution_audit_data
select top 10 * from attribution_data
select product_code,count(1) from attribution_data group by product_code

select record_type,count(1) from attribution_audit_data group by record_type
select audit_action,count(1) from attribution_audit_data group by audit_action
select audit_action_date,count(1) from attribution_audit_data group by audit_action_date
--4 records have date in the wrong format
select match_group, name_type,count(1) from attribution_audit_data group by match_group, name_type


     --Product
       update attribution_data       as att
          set att.product_code      = aud.code_to
             ,att.product_name      = aud.name_to
select code_from,code_to,name_from,name_to,*
         from attribution_audit_data as aud
inner join attribution_data as att on att.commercial_number = aud.commercial_number
--        where att.commercial_number = aud.commercial_number
          and cast(att.match_group as int) = cast(aud.match_group as int)
          and name_type             = 'P'
          and att.product_code      = aud.code_from

     --Advertiser
       update attribution_data       as att
          set att.advertiser_code   = aud.code_to
             ,att.advertiser_name   = aud.name_to
select count(1)
         from attribution_audit_data as aud
inner join attribution_data as att on att.commercial_number = aud.commercial_number
--        where att.commercial_number = aud.commercial_number
          and cast(att.match_group as int) = cast(aud.match_group as int)
          and name_type             = 'A'
          and att.advertiser_code   = aud.code_from

     --Holding company
       update attribution_data       as att
          set att.holding_company_code = aud.code_to
             ,att.holding_company_name = aud.name_to
select count(1)
         from attribution_audit_data as aud
inner join attribution_data as att on att.commercial_number = aud.commercial_number
--        where att.commercial_number    = aud.commercial_number
          and cast(att.match_group as int) = cast(aud.match_group as int)
          and name_type                = 'H'
          and att.holding_company_code = aud.code_from

     --Buying agency
       update attribution_data       as att
          set att.buying_agency_code = aud.code_to
             ,att.buying_agency_name = aud.name_to
         from attribution_audit_data as aud
        where att.commercial_number  = aud.commercial_number
          and cast(att.match_group as int) = cast(aud.match_group as int)
          and name_type              = 'B'
          and att.buying_agency_code = aud.code_from
          and audit_action_date      = replace(cast(@dt as varchar), '-', '')

     --NMR
       update attribution_data       as att
          set att.nmr_category_code = aud.code_to
         from attribution_audit_data as aud
        where att.commercial_number = aud.commercial_number
          and cast(att.match_group as int) = cast(aud.match_group as int)
          and name_type             = 'M'
          and att.nmr_category_code = aud.code_from
          and audit_action_date     = replace(cast(@dt as varchar), '-', '')



/*
select top 10 * from attribution_audit_data where code_from ='P151800'


processing attribution audit file:
there is a change from P151800 to P163920

SELECT *
--  FROM SMI_ACCESS.SMI_ETL.V_SLOT_DIM
  FROM SMI_DW.SMI_ETL.SLOT_DIM
 where product_code like 'P151800%'
order by product_code

gives the following slot dims:

SELECT *
  FROM SMI_ACCESS.SMI_ETL.V_VIEWING_SLOT_INSTANCE_FACT
 where dk_slot_dim in (400121
,5000202
,8600183
,1900293
,3700154
,6000121)
 LIMIT 100;

there are none for P163920

*/




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
order by date_of_transmission
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
--select *
    from channel_map_dev_service_key_barb as slt
         right join barb_spot_data as bar on cast(bar.reporting_panel_code as int) = slt.panel_code
                                         and bar.log_station_code_for_spot    = cast(slt.log_station_code as int)
                                         and bar.split_transmission_indicator = cast(slt.sti_code as int)
   where spot_type = 'CS'
group by date_of_transmission
        ,reporting_panel_code
        ,log_station_code_for_spot
        ,split_transmission_indicator
order by date_of_transmission
        ,reporting_panel_code
        ,log_station_code_for_spot
        ,split_transmission_indicator
;

select spot_type,count(1) from barb_spot_data group by spot_type

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
        ,count(1)
--    into #codecount
    from barb_spot_data
group by date_of_transmission
        ,break_start_time
        ,code
order by date_of_transmission
        ,break_start_time
        ,code
;
select top 10 * from barb_spot_data
select date_of_transmission,count(1) from barb_spot_data group by date_of_transmission
--20130128 retest
--living+1 = service key 2205 = log_sti 46860 is in the data
--Syfy+1 = 2513 = 42290 is in the data
--SBO e.g. 1501 = 49871 NOT in data
--Bliss, Starz, not in Techedge?

20130129
only Syfy+1 missing from data



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
select distinct (spot_platform_indicator) from barb_spot_data
  select *
--    into slotsqa_barbtest4
    from barb_spot_data
   where right(spot_platform_indicator,1) not in ('0','1','2','3')
;

select spot_type,count(1) from barb_spot_data group by spot_type
--all spot types this time are TS or CS. What does this mean? commercial/terrestrial spot?


--Test 1.11 Perform a count by date, log station code, STI code (excluding non-digital stellite).
  select count(1) as counts
        ,date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
    from barb_spot_data                   as bar
         left join channel_map_dev_service_key_barb as sre on cast(bar.reporting_panel_code as int)        = sre.panel_code
                                                                         and bar.log_station_code_for_spot    = cast(sre.log_station_code as int)
                                                                         and bar.split_transmission_indicator = cast(sre.sti_code as int)
--    where right(spot_platform_indicator,1) in ('0','1','2','3')
--      and spot_type = 'S'
group by date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
order by date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
;
select count(1),spot_type from barb_spot_data group by spot_type

--Test 1.11a This is temporary - all sold spots should be included in the final solution, but we know that CBI have only used panel 50.  Include count of the number of sold spots that have been excluded.
  select count(1) as counts
    from barb_spot_data                   as bar
         left join channel_map_dev_service_key_barb as sre on cast(bar.reporting_panel_code as int)        = sre.panel_code
                                                                         and bar.log_station_code_for_spot    = cast(sre.log_station_code as int)
                                                                         and bar.split_transmission_indicator = cast(sre.sti_code as int)
    where right(spot_platform_indicator,1) in ('0','1','2','3')
      and spot_type = 'CS'
      and reporting_panel_code = '50'
;

--Test 1.11b Perform a count by date, log station code, STI code. (excluding non-digital stellite and panel 50)
  select count(1) as counts
        ,date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
    from barb_spot_data                   as bar
         left join channel_map_dev_service_key_barb as sre on cast(bar.reporting_panel_code as int)        = sre.panel_code
                                                                         and bar.log_station_code_for_spot    = cast(sre.log_station_code as int)
                                                                         and bar.split_transmission_indicator = cast(sre.sti_code as int)
   where right(spot_platform_indicator,1) in ('0','1','2','3')
     and spot_type = 'CS'
     and reporting_panel_code <> '50'
group by date_of_transmission
        ,log_station_code_for_spot
        ,split_transmission_indicator
order by date_of_transmission
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
drop table #codes;

  select right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator as code
        ,count(1) as counts
    into #codes
    from barb_spot_data as bas
group by code
; --363

  select service_key
        ,sum(counts) as counters
    into #keys
    from #codes as cod
         left join channel_map_dev_service_key_barb as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by service_key
; --345

  select kys.service_key,spot_source
        ,sum(counters) as counts
    from #keys as kys
         left join channel_map_dev_service_key_attributes as att on cast(kys.service_key as int) = cast(att.service_key as int)
group by kys.service_key,spot_source
order by kys.service_key
; --

select sum(counts),spot_source from slotsqa_keys group by spot_source;
select * from slotsqa_keys where spot_source <>'BARB';

select * from Vespa_analysts.channel_map_dev_service_key_attributes

--Test 1.14 Check any entries in [3.1 service_key_attributes tab] that do not have BARB data.
--Note in some instance this will be possible if a spot log has not been returned.
select top 10 * from barb_spot_data
  select distinct(service_key)
    into #keys2
    from Vespa_analysts.channel_map_dev_service_key_attributes
   where promo_source = 'BARB'
; --353

  select kys.service_key
        ,right('00000' || ser.log_station_code, 5) || ser.sti_code as code
    into #codes2
    from #keys2 as kys
         left join Vespa_analysts.channel_map_dev_service_key_barb as ser on cast(kys.service_key as int)= cast(ser.service_key as int)
group by kys.service_key
        ,code
; --753

  select service_key
        ,sum(case when bas.log_station_code_for_spot is null then 0 else 1 end)
    from #codes2 as cod
         left join barb_spot_data as bas on cod.code = right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator
group by service_key
;


  select code
    from #codes2
         left join barb_spot_data as bas on code = right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator
   where bas.log_station_code_for_spot is null
group by code
order by code
; --117 service keys have no data

--Test 1.15 Perform a count of break start times by day by log station code & STI code to find missing data
select date_of_transmission,count(1) from #codecount
group by date_of_transmission
;

  select right('000000' || log_station_code || sti_code,6) as code
    into #codes
    from channel_map_dev_service_key_barb
group by code
; --831

  select date_of_transmission
        ,break_start_time
        ,count(1)
    from #codes as cod
         inner join barb_spot_data as bar on cod.code = right('00000' || bar.log_station_code_for_break,5) || break_split_transmission_indicator
group by date_of_transmission
        ,break_start_time
order by date_of_transmission
        ,break_start_time
;



select top 10 * from barb_spot_data


------------------------------------
--Landmark Data import checks
------------------------------------
select status,count(1) from landmark_data group by status
select * from landmark_data_raw
select * from Vespa_analysts.channel_map_dev_service_key_landmark
select distinct sare_no from landmark_data
select count(1) from landmark_data

--Test 2.2
select * from landmark_data
where status <> 'S'
;

select status,count(1)  from landmark_data group by status

--Test 2.3 Check that all service keys in the file have an entry of Landmark in the spot source in [3.1 service key attributes tab]
  select skl.service_key
        ,count(1) as cow
    into #keys23
    from landmark_data                          as lan
         inner join channel_map_dev_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
group by skl.service_key
; --242

  select kys.service_key
         ,spot_source
    from #keys23                                   as kys
         inner join channel_map_dev_service_key_attributes as ska on kys.service_key = ska.service_key
   where spot_source <> 'Landmark'
     and effective_from < '2012-12-07'
     and effective_to   > '2012-12-07'
;--0

--Test 2.4 Check any entries in [3.1 service key attributes tab] for which we don't have spot data
  select service_key,epg_name
    into #keys24
    from channel_map_dev_service_key_attributes
   where spot_source = 'Landmark'
     and effective_from < '2012-12-24'
     and effective_to   > '2012-12-24'
group by service_key,epg_name
; --290

  select cast(sare_no as int) as sare_no,epg_name,kys.service_key
    into #sares
    from #keys24                                 as kys
         inner join channel_map_dev_service_key_landmark as skl on kys.service_key = skl.service_key
; --291

  select sare_no
    into #landmark_sares
    from landmark_data
group by sare_no
; --157

  select distinct(service_key)
    from #sares                    as sar
         left join #landmark_sares as lan on sar.sare_no = lan.sare_no
   where lan.sare_no is null
; --47


--Test 2.6
  select lan.sare_no
        ,count(1) as cow
    from landmark_data                          as lan
         left join channel_map_dev_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
   where skl.sare_no is null
group by lan.sare_no
;

--Test 2.7
  select lan.sare_no
        ,count(1) as cow
    from landmark_data                          as lan
         left join channel_map_dev_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
   where skl.service_key is null
group by lan.sare_no
;

--Test 2.8 Find any service keys in service key attributes that do not have a matching sare number
  select distinct(ska.service_key)
    from channel_map_dev_service_key_attributes         as ska
         left join channel_map_dev_service_key_landmark as skl on ska.service_key = skl.service_key
where skl.sare_no is null
and spot_source='Landmark'
or promo_source='Landmark'
;


------------------------------------
--BARB v Landmark cross check
------------------------------------
--Test 3.1 Summarise by landmark and BARB data by service key and match for each day - Using 2012-12-07
  select distinct(service_key)
        ,cast (0 as bit) as barb
        ,cast (0 as bit) as landmark
    into #keys
    from channel_map_dev_service_key_attributes
   where barb_reported = 'YES'
;--457

  select right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator as code
        ,date_of_transmission
        ,count(1) as counts
    into #codes
    from barb_spot_data as bas
group by code
        ,date_of_transmission
;--358

  select service_key
        ,date_of_transmission
    into #keys31
    from #codes as cod
         left join channel_map_dev_service_key_barb as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by service_key
        ,date_of_transmission
;--339

  select skl.service_key
        ,SLOT_START_BROADCAST_DATE
        ,count(1) as cow
    into #keys312
    from landmark_data                          as lan
         inner join channel_map_dev_service_key_landmark as skl on lan.sare_no = cast(skl.sare_no as int)
group by skl.service_key
        ,SLOT_START_BROADCAST_DATE
;--257

  update #keys as kys
     set barb = 1
    from #keys31 as bar
   where cast(kys.service_key as int)= bar.service_key
;--338

  update #keys as kys
     set landmark = 1
    from #keys312 as lan
   where kys.service_key = lan.service_key
;--173

select distinct(date_of_transmission) from #keys31--barb
2012-12-07

select distinct(SLOT_START_BROADCAST_DATE) from #keys312--LM
2012-12-07

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
        ,max(case when date_of_transmission      = '2012-12-07' then 1 else 0 end) as d2011d06d04_barb
        ,max(case when SLOT_START_BROADCAST_DATE like '2012-12-07%' then 1 else 0 end) as d2011d06d04_landmark
    from #keys              as kys
         left join #keys31  as k31 on cast(kys.service_key as int) = cast(k31.service_key as int)
         left join #keys312 as k32 on cast(kys.service_key as int) = cast(k32.service_key as int)
group by kys.service_key
;
select min(date_of_transmission),max(date_of_transmission),min(SLOT_START_BROADCAST_DATE),max(SLOT_START_BROADCAST_DATE)
    from #keys              as kys
         left join #keys31  as k31 on cast(kys.service_key as int) = cast(k31.service_key as int)
         left join #keys312 as k32 on cast(kys.service_key as int) = cast(k32.service_key as int)
--Test 3.2
drop table #keys;
drop table #codes;

  select cast(service_key as int)
    into #keys
    from channel_map_dev_service_key_attributes
--   where barb_reported = 'YES'
--     and spot_source = 'Landmark'
--     and effective_from < '2012-12-07'
--     and effective_to   > '2012-12-07'
group by service_key
;--186

  select ser.log_station_code
        ,ser.sti_code
    into #codes
    from #keys as kys
         inner join channel_map_dev_service_key_barb as ser on kys.service_key = ser.service_key
group by ser.log_station_code
        ,ser.sti_code
;--143

create hg index idx1 on #codes(log_station_code,sti_code);
create hg index idx1 on channel_map_dev_service_key_barb(log_station_code_for_spot,split_transmission_indicator);
create hg index idx2 on landmark_data(SLOT_START_BROADCAST_TIME_HOURS);
create hg index idx3 on landmark_data(SLOT_START_TIME_MINUTES);
create hg index idx4 on landmark_data(SLOT_START_TIME_SECONDS);
select top 10 * from landmark_data

select count(1) from (
  select date_of_transmission,spot_start_time
    from barb_spot_data          as bar
         inner join #codes       as cod on bar.log_station_code_for_spot = cod.log_station_code
                                       and bar.split_transmission_indicator = cod.sti_code
         left join landmark_data as lan on bar.date_of_transmission = cast(left(lan.SLOT_START_BROADCAST_DATE,10) as date)
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
;--12

  select sare_no
    into #sares
    from channel_map_dev_service_key_landmark as skl
         inner join #keys             as kys on cast(skl.service_key as int) = kys.service_key
;--191

  select BARB_SALES_HOUSE_ID, count(1) as cow
    into #spots33b
    from landmark_data     as lan
         inner join #sares as sar on lan.sare_no = cast(sar.sare_no as int)
group by BARB_SALES_HOUSE_ID
;--9

select * from #spots33a;
select * from #spots33b;

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
create hg index idx1 on channel_map_dev_service_key_landmark(sare_no);
create hg index idx1 on channel_map_dev_service_key_barb(service_key);
create hg index idx2 on channel_map_dev_service_key_barb(log_station_code,sti_code);
create hg index idx1 on landmark_data(sare_no);
create hg index idx1 on barb_spot_data(log_station_code_for_spot,split_transmission_indicator);


  select lan.sare_no
        ,kys.service_key
        ,lgs.log_station_code
        ,lgs.sti_code
    into #keys
    from landmark_data as lan
         inner join channel_map_dev_service_key_landmark as kys on lan.sare_no          = kys.sare_no
         inner join channel_map_dev_service_key_barb     as lgs on kys.service_key      = lgs.service_key
         inner join barb_spot_data                       as bar on lgs.log_station_code = bar.log_station_code_for_spot
                                                               and lgs.sti_code         = bar.split_transmission_indicator
group by lan.sare_no
        ,kys.service_key
        ,lgs.log_station_code
        ,lgs.sti_code
--171

  select clearcast_commercial_number,count(1) as cow
    from barb_spot_data   as bar
         inner join #keys as kys on bar.log_station_code_for_spot    = kys.log_station_code
                                and bar.split_transmission_indicator = kys.sti_code
group by clearcast_commercial_number
; --1252

  select CLEARCAST_COMMERCIAL_NO, count(1) as cow
    from landmark_data    as lan
         inner join #keys as kys on lan.sare_no = cast(kys.sare_no as int)
group by CLEARCAST_COMMERCIAL_NO
;--1246

select * from barb_spot_data
where CLEARCAST_COMMERCIAL_Number in (
'GRFRUMC003030'
,'GRFMCRU003030'
)
and log_station_code_for_spot = 4814

select * from #keys

--the only differences are between 6am and 7am - due to utc time being applied


--Test 3.6
  select spot_type,count(1) as cow
    from barb_spot_data   as bar
         inner join #keys as kys on bar.log_station_code_for_spot    = kys.log_station_code
                                and bar.split_transmission_indicator = kys.sti_code
group by spot_type
; --2

  select media_spot_type, count(1) as cow
    from landmark_data    as lan
         inner join #keys as kys on lan.sare_no = cast(kys.sare_no as int)
group by media_spot_type
;--2


--Test 3.7
  select CAMPAIGN_APPROVAL_ID,count(1) as cow
    from barb_spot_data   as bar
         inner join #keys as kys on bar.log_station_code_for_spot    = kys.log_station_code
                                and bar.split_transmission_indicator = kys.sti_code
group by CAMPAIGN_APPROVAL_ID
; --

  select CAMPAIGN_APPROVAL_ID, count(1) as cow
    from landmark_data    as lan
         inner join #keys as kys on lan.sare_no = cast(kys.sare_no as int)
group by CAMPAIGN_APPROVAL_ID
;--

select cAMPAIGN_APPROVAL_ID,count(1) from barb_spot_data
group by cAMPAIGN_APPROVAL_ID

--test 3.8
  select campaign_approval_id_version_number,count(1) as cow
    from barb_spot_data   as bar
         inner join #keys as kys on bar.log_station_code_for_spot    = kys.log_station_code
                                and bar.split_transmission_indicator = kys.sti_code
group by campaign_approval_id_version_number
; --

  select CAMPAIGN_APPROVAL_version, count(1) as cow
    from landmark_data    as lan
         inner join #keys as kys on lan.sare_no = cast(kys.sare_no as int)
group by CAMPAIGN_APPROVAL_version
;--




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
--these match

--Test 4.3
  select distinct(commercial_no)
    into #comms
    from (select distinct(clearcast_commercial_number) as commercial_no
            from barb_spot_data
           union
          select distinct(CLEARCAST_COMMERCIAL_NO)
            from landmark_data
        ) as sub
; --5502

  select commercial_no
    from #comms                     as com
         left join attribution_data as att on com.commercial_no = att.Commercial_Number
   where att.Commercial_Number is null
; --1204

select top 10 * from barb_spot_data order by clearcast_commercial_number

------------------------------------
--Match to CBI checks
------------------------------------
--- test 5.1 (see 3.1)

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
         left join channel_map_dev_service_key_barb as ser on right('00000' || bas.log_station_code_for_spot, 5) || bas.split_transmission_indicator = right('00000' || ser.log_station_code, 5) || ser.sti_code
         left join channel_map_dev_service_key_attributes as att on att.service_key = ser.service_key
   where spot_source = 'BARB'
      and att.effective_from < '2012-12-07'
      and att.effective_to > '2012-12-07'
group by bas.date_of_transmission, bas.spot_start_time, ser.service_key,bas.spot_duration,bas.clearcast_commercial_number,bas.spot_type
, bas.Campaign_Approval_ID,bas.Campaign_Approval_ID_Version_number,sales_house_identifier
;--132,998

select service_key,count(1)
  from #barb_spots
group by service_key
order by service_key
; --165
select date_of_transmission,count(1) from #barb_spots group by date_of_transmission

--Netezza query
  select service_key
        ,count(1)
        ,max(dk_broadcast_start_datehour_dim)
    from smi_access.smi_etl.V_CHANNEL_DIM                   as chd
         inner join smi_access.smi_etl.V_SLOT_INSTANCE_ASOC as sia on chd.pk_channel_dim = sia.dk_channel_dim
         inner join smi_access.smi_etl.v_slot_Dim           as slt on slt.pk_slot_dim    = sia.dk_slot_dim
   where dk_broadcast_start_datehour_dim like '20121207%'
     and slot_type='SPOTS'
group by service_key
;

select spot_start_time,count(1)
  from #barb_spots
where sales_house_identifier=49
group by spot_start_time
order by spot_start_time
;




--Test 5.2
--Sybase
select sales_house_identifier
,count(1)
from barb_spot_data
group by sales_house_identifier
order by sales_house_identifier
;

--Netezza
select barb_sales_house_id
      ,count(1)
  from ADMIN.V_MEDIA_SALES_HOUSE_DIM as msh
       inner join ADMIN.V_SLOT_INSTANCE_ASOC as sia on msh.pk_media_sales_house_dim = sia.dk_media_sales_house_dim
-- where dk_broadcast_start_datehour_dim like '20121024%'
group by barb_sales_house_id
order by barb_sales_house_id




--Test 5.3
select sum(case when slot_duration <> adjusted_slot_duration then 1 else 0 end),count(1)
,service_key
  from ADMIN.V_SLOT_INSTANCE_DIM             as sid
       inner join ADMIN.V_SLOT_DIM           as slt on sid.pk_slot_instance_dim = slt.pk_slot_dim
       inner join ADMIN.V_SLOT_INSTANCE_ASOC as sia on slt.pk_slot_dim = sia.dk_slot_dim
       inner join ADMIN.V_CHANNEL_DIM        as chn on dk_channel_dim = chn.pk_channel_dim
group by service_key

--Test 5.4
--Sybase query
select service_key,count(1) from (
  select ser.service_key,clearcast_commercial_number
    from barb_spot_data as bar
         inner join channel_map_dev_service_key_barb as ser on bar.log_station_code_for_spot=ser.log_station_code
                                                                          and bar.split_transmission_indicator = ser.sti_code
         inner join channel_map_dev_service_key_attributes as map on ser.service_key = map.service_key
where spot_source='BARB'
group by ser.service_key,clearcast_commercial_number
) as sub group by service_key order by service_key
;--


--Netezza query
select service_key,count(1) from (
  select service_key
        ,clearcast_commercial_no
 from smi_access..v_slot_dim as sld
      inner join smi_access..V_SLOT_INSTANCE_ASOC as sia on sld.pk_slot_dim    = sia.DK_SLOT_DIM
   inner join smi_access..V_CHANNEL_DIM        as chn on sia.DK_CHANNEL_DIM = chn.PK_CHANNEL_DIM
   where sia.DK_BROADCAST_START_DATEHOUR_DIM like '20121207%'
and dk_slot_instance_source_type_dim = 800001 --BARB
group by service_key
        ,clearcast_commercial_no
--order by service_key
        --,clearcast_commercial_no
) as sub group by service_key
order by service_key
;--

  select clearcast_commercial_number
    from barb_spot_data as bar
         inner join channel_map_dev_service_key_barb as ser on bar.log_station_code_for_spot=ser.log_station_code
                                                                          and bar.split_transmission_indicator = ser.sti_code
where service_key=2502
group by service_key,clearcast_commercial_number
order by clearcast_commercial_number






select date_of_transmission,count(1) from barb_spot_data group by date_of_transmission
--2012-12-07

--Test 5.5
  select distinct clearcast_commercial_number from (
  select distinct clearcast_commercial_number
    from barb_spot_data
union
  select distinct clearcast_commercial_no
    from landmark_data
) as sub
order by clearcast_commercial_number


  SELECT distinct clearcast_commercial_no
    FROM SMI_ACCESS..V_SLOT_DIM as sld
         left join smi_access..v_SLOT_instance_asoc as sia on sld.pk_slot_dim = sia.DK_SLOT_DIM
   where dk_broadcast_start_datehour_dim like '20121207%'
order by clearcast_commercial_no
--a small number of diffs, due to time diff

------------------------------------
--BSS Data import checks
------------------------------------
--Test 6.2 Check that all entries in the file are identified correctly in the [3.1 service key attributes table] with BSS in the promo_source column.  Use service_key as the match key
select count(1) from bss_data
--57812

  select count(1)
        ,case when ska.service_key is null then 0 else 1 end as exist
    from bss_data                                         as bss
         left join channel_map_dev_service_key_attributes as ska on cast(bss.si_service_key as int) = ska.service_key
group by exist
;--3248 not found

  select top 10 bss.si_service_key
    from bss_data                                                        as bss
         left join Vespa_analysts.channel_map_dev_service_key_attributes as ska on cast(bss.si_service_key as int) = ska.service_key
where ska.service_key is null
group by bss.si_service_key
--just 5106


--Test 6.3 Check for any entries in [3.1 service key atributes tab] that have BSS in the promo_source column, but for which we do not have data.  Use service key as the match key
  select distinct(service_key)
    into #keys3
    from Vespa_analysts.channel_map_dev_service_key_attributes
   where promo_source='BSS'
; --140

--but for which we do not have data.  Use service key as the match key
  select service_key
    from #keys3 as kys
         left join bss_data as bss on cast(kys.service_key as int) = cast(bss.si_service_key as int)
   where bss.si_service_key is null
group by service_key
order by service_key
;--24

--Test 6.4 Confirm whether the source file has the +1 channel data in e..g. service_key 1448 has BSS in the promo_source column, but 3620 the +1 channel is marked up as BARB.
--This is not strictly correct and there is not anything in this table that identifies the parent channel.
--We may need any additional column in the table to identify the service key of the parent channel so that the BSS data can be expanded and timeshifted correctly.
  select barb,count(1)
    from bss_data as bss
         inner join Vespa_analysts.channel_map_dev_service_key_attributes as ser on cast(bss.si_service_key as int) = cast(ser.service_key as int)
   where ser.barb like '%+1%' or ser.barb like '%+ 1%'
group by barb
;

select * from Vespa_analysts.channel_map_dev_service_key_attributes

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
         left join Vespa_analysts.channel_map_dev_service_key_barb as ser on cast(ser.sti_code as varchar) = right('00000' || bar.log_station_code, 5) || bar.split_transmission_indicator
;

drop table #keys;
drop table #codes;

--Test 7.10 Check that all service_keys that have data have a Promo_source of BARB in [3.1 service key attributes tab]
  select right('00000' || bas.log_station_code, 5) || bas.split_transmission_indicator as code
        ,count(1) as counts
    into #codes
    from barb_promo_data as bas
group by code
; --86

  select code
        ,service_key
        ,sum(counts) as counters
    into #keys
    from #codes as cod
         left join channel_map_dev_service_key_barb as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by code
        ,service_key
; --192

  select code,kys.service_key,promo_source
        ,sum(counters) as counts
    from #keys as kys
         left join channel_map_dev_service_key_attributes as att on cast(kys.service_key as int) = cast(att.service_key as int)
   where (promo_source <> 'BARB' or promo_source is null)
     and effective_from < '2012-12-07'
     and effective_to   > '2012-12-07'
group by code,kys.service_key,promo_source
; --
select * from channel_map_dev_service_key_attributes where service_key=3150
     and effective_from < '2012-12-07'
     and effective_to   > '2012-12-07'

drop table #keys;
drop table #codes;

--Test 7.11 Produce a list of any entries that have BARB in [3.1. service key atrributes tab], but for which we do not have any promo or sponsorship data.
  select service_key
    into #keys
    from channel_map_dev_service_key_attributes
   where promo_source = 'BARB'
     and effective_from < '2012-12-07'
     and effective_to   > '2012-12-07'
group by service_key
; --304

  select right('00000' || ser.log_station_code, 5) || ser.sti_code as code
    into #codes
    from #keys as kys
         left join channel_map_dev_service_key_barb as ser on cast(kys.service_key as int) = cast(ser.service_key as int)
group by code
;--344

drop table #barb;

  select right('00000' || log_station_code, 5) || split_transmission_indicator as code
    into #barb
    from barb_promo_data
group by code
;--86

  select cod.code
    from #codes as cod
         left join #barb as bar on cod.code = bar.code
   where bar.code is null
group by cod.code
;--258

drop table #codes;
drop table #keys;


------------------------------------
--Check Promos to CBI
------------------------------------

--Test 8.1
  select right('00000' || bas.log_station_code, 5) || bas.split_transmission_indicator as code
        ,count(1) as counts
    into #codes
    from barb_promo_data as bas
group by code
; --86

  select code
        ,service_key
        ,sum(counts) as counters
    into #keys
    from #codes as cod
         left join channel_map_dev_service_key_barb as ser on cod.code = right('00000' || ser.log_station_code, 5) || ser.sti_code
group by code
        ,service_key
; --192

select date_of_transmission,count(1) from barb_promo_data group by date_of_transmission

  select service_key
        ,count(1)
    from barb_promo_data as bas
         left join #keys as kys on right('00000' || bas.log_station_code, 5) || bas.split_transmission_indicator = kys.code
group by service_key
;

select tx_date,count(1) from bss_data group by tx_date

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






--Test 8.1
alter table barb_promo_data add derived_service_key int;

  update barb_promo_data as bar
     set bar.derived_service_key = ser.service_key
    from Vespa_analysts.channel_map_dev_service_key_barb as ser
   where cast(bar.log_station_code as int)             = ser.log_station_code
     and cast(bar.split_transmission_indicator as int) = ser.sti_code
;--86,105

select derived_service_key,count(1)
from barb_promo_data
where date_of_transmission = '2012-12-07'
group by derived_service_key

--BSS part
--Sybase query
  select cast(si_service_key as int) as service_key
        ,count(1)
    from bss_data as bss
    inner join channel_map_dev_service_key_attributes as map on cast(bss.si_service_key as inT)= map.service_key
where map.promo_source='BSS'
and effective_from < '2012-12-07'
and effective_to > '2012-12-07'
group by service_key
order by service_key
;

--Netezza query
  select count(1), service_key,substr(dk_broadcast_start_datehour_dim,1,8) as dt
    from SMI_dw..VIEWING_SLOT_INSTANCE_FACT_static  as ins
         left join SMI_dw.SMI_ETL.CHANNEL_DIM     as chn on ins.DK_CHANNEL_DIM = chn.PK_CHANNEL_DIM
where dt='20121207'
group by service_key,dt
order by dt,service_key
;

  select top 10 *
    from bss_data as bss
where si_service_key='1001'

    inner join channel_map_dev_service_key_attributes as map on cast(bss.si_service_key as inT)= map.service_key
where map.promo_source='BSS'
and effective_from < '2012-12-07'
and effective_to > '2012-12-07'
group by service_key
order by service_key




select substr(dk_broadcast_start_datehour_dim,1,8) as dt,count(1)
    from admin.VIEWING_SLOT_INSTANCE_FACT_static  as ins
group by dt
order by dt

select max(DK_ADSMART_MEDIA_CAMPAIGN_DIM),dk_broadcast_start_datehour_dim from SMI_ACCESS.VIEWING_SLOT_INSTANCE_FACT_static
group by dk_broadcast_start_datehour_dim
limit 100


select dk_broadcast_start_datehour_dim,count(1),sum(case when chn.pK_CHANNEL_DIM is null then 0 else 1 end)
  from admin.VIEWING_SLOT_INSTANCE_FACT_static as ins
       left join admin.CHANNEL_DIM           as chn on ins.DK_CHANNEL_DIM = chn.PK_CHANNEL_DIM
group by dk_broadcast_start_datehour_dim
order by dk_broadcast_start_datehour_dim




select dk_channel_dim,* from
admin.VIEWING_SLOT_INSTANCE_FACT_static
where DK_BROADCAST_START_DATEHOUR_DIM
limit 100

select slot_status,count(1) from
admin.VIEWING_SLOT_INSTANCE_FACT_static
group by slot_status

select slot_status,count(1) from
admin.VIEWING_SLOT_INSTANCE_FACT_volatile
group by slot_status












--Barb part
  select derived_service_key as service_key
        ,count(1)
    from barb_promo_data
group by service_key
;

--Test 8.2
select sum(case when derived_service_key is null or derived_service_key=0 then 0 else 1 end),count(1) from barb_promo_data
;

--Test 8.3
  select log_station_code
        ,split_transmission_indicator
        ,count(1)
    from barb_promo_data
group by log_station_code
        ,split_transmission_indicator

--Test 8.4
select sum(case when slot_duration <> adjusted_slot_duration then 1 else 0 end),count(1)
      ,service_key
  from ADMIN.V_SLOT_INSTANCE_DIM             as sid
       inner join ADMIN.V_SLOT_DIM           as slt on sid.pk_slot_instance_dim = slt.pk_slot_dim
       inner join ADMIN.V_SLOT_INSTANCE_ASOC as sia on slt.pk_slot_dim = sia.dk_slot_dim
       inner join ADMIN.V_CHANNEL_DIM        as chn on dk_channel_dim = chn.pk_channel_dim
group by service_key

--Test 8.5i
--BARB
select derived_service_key,count(1)
from barb_promo_data
where date_of_transmission='2012-12-07'
group by derived_service_key
order by derived_service_key
--62

--Netezza (tested in production, so that we have matching dates)
  select service_key,count(1)
    from smi_dw.smi_etl.SLOT_INSTANCE_ASOC     as sia
         inner join smi_access.smi_etl.V_CHANNEL_DIM as chn on chn.pk_channel_dim = sia.dk_channel_dim
         inner join SMI_DW.SMI_ETL.SLOT_DIM          as sld on sia.dk_slot_dim = sld.PK_SLOT_DIM
         inner join SMI_DW.SMI_ETL.SLOT_INSTANCE_DIM as sid on sid.PK_SLOT_INSTANCE_DIM = sia.DK_SLOT_INSTANCE_DIM
   where slot_type='PROMOS'
     and (   (substring(DK_BROADCAST_START_DATEHOUR_DIM,1,8) ='20121207' and source_system='BARB')
          or (substring(DK_BROADCAST_START_DATEHOUR_DIM,1,8) ='20121214' and source_system='BSS' )    )
group by service_key
order by service_key



;

--BSS dates don't match
select si_service_key
from bss_data
--where tx_date='2012-12-14'
group by si_service_key



---------------------
--Test 9 - Sequencing
---------------------

--Test 9.1
--can't check by interval time so: simpler version:
  select interval_start_time,count(1)
    from barb_promo_data
where date_of_transmission='2012-12-07'
group by interval_start_time
;

--Netezza version:
select sid.date_from,count(1)
from smi_dw.smi_etl.slot_dim                 as sld
inner join smi_dw.smi_etl.slot_instance_asoc as sia on sld.pk_slot_dim = sia.dk_slot_dim
inner join smi_dw.smi_etl.slot_instance_dim  as sid on sia.nk_slot_instance_dim = sid.nk_slot_instance_dim
where slot_type='PROMOS'
group by sid.date_from


select count(1)
  from ADMIN.V_SLOT_INSTANCE_DIM                  as sin
;

select top 10 * from barb_promo_data

---
barb_promo_data - 2012-10-24
bss_data        - 2012-10-31










select event_start_datetime_utc,count(1)
from sk_prod.vespa_events_all
group by event_start_datetime_utc




---




select top 1000 * from barb_data_amends
select top 100 * from data_raw

--Test 10.3
--only one:4508-0
--
--Sky Sports Interactive Lo 1
--Sky Sports Interactive Hi 2

--test 10.4
2012-12-07
204420
205203
215244



select distinct(break_start_time) from barb_spot_data


 select distinct dk_broadcast_start_time_dim
   FROM SMI_DW.SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
where dk_broadcast_start_datehour_dim like '20121207%'
order by dk_broadcast_start_time_dim desc
limit 100



--test 10.5
select distinct(clearcast_commercial_number)
from barb_data_amends


SELECT count(distinct clearcast_commercial_no)
  FROM SMI_DW.SMI_ETL.SLOT_DIM
where clearcast_commercial_no in (
'EURPGYB559030'
,'TAGINGB104030'
,'ARNJDBT001030'
,'MUENOGL004030'
,'VCCOOPG397030'
,'OHESCVB059030'
,'CNZLEGO410030'
,'DDBTMAU039030'
,'CCEGCAM203030'
,'CHICWSG276030'
,'HOGPGDG067030'
,'TAGKLGB005030'
,'APONEXS007030'
,'WKUTEUK030030'
,'TCPLFTC115010'
,'GRYPGFL561030'
,'WPPCARA082030'
,'BMBTHOM199060'
,'EFRNWFA034030'
,'MUMBOWW026010'
,'WCRSKYD551030'
,'TTBBBSF026010'
,'NSAHYUN195030'
,'MIFPGOB294030'
,'BURTDAZ174030'
,'DLKHLJS069010'
,'ABCWONG033030'
,'CHIARTX069020'
,'PPCFXPG019020'
,'SSCPGIM064030'
,'BMBFCTW111060'
,'EDNUNLM002030'
,'NSADSJB445030'
,'SCCRAGC101020'
,'BMBTHOM192030'
,'FMCSKRP117030'
,'FMCSKRP118010'
,'GRYPGPP590020'
,'ABCWONG029030'
,'MCEBQCH219030'
,'PPCFXPG006010'
,'BBHAUAV014030'
,'LIPWTCH006020'
,'KRODFWS373020'
,'CBJMKEM381010'
,'AMVCTEM076030'
,'VCCBTVC013030'
,'CBJMKBR380030'
,'VCCABAP020010'
,'BMBDAIR002020'
,'REPDRMV822030'
,'CHIARTX071020'
,'BURTMDN001020'
,'HOGMSWS662030'
,'PPCFXLP038020'
,'AMVMCMB749030'
,'BURTCOP023020'
,'MPGPUIG030020'
,'MPGPUIG027020'
,'BSCSKPR014030'
,'INKSDAC025010'
,'BBHBABR759020'
,'DLKHLGF066030'
,'RKYHOSC033030'
,'MCMALSP374020'
,'ICPCPFR290010'
,'FOWSKYV083030'
,'SAACLCW037020'
,'WCRSKYD538040'
,'WCRSKYD539030'
,'BBHKFPD622030'
,'BURTCOP020010'
,'MPGPUIG029020'
,'BRIHSPF006010'
,'BBHKFBR026030'
,'TGSGMBR020010'
,'DRSPSFF250030'
,'MCEBQCH216020'
,'RKYBEBO007020'
,'MIAAPPL511030'
,'JWTJJBE030020'
,'SSCASBR192040'
,'SSCASBR189040'
,'TTBJPGC027010'
,'KRODFWS375020'
,'GEDLORE326020'
,'MPGKGJC004010'
,'BDAAUDR003030'
,'FLLPUFT003030'
,'BETGUMT021010'
,'GEDLORE213020'
,'KRODFWS376020'
,'JMLBBOD105030'
,'MUMMAEP085010'
,'GEDLORE339020'
,'JMLSWER001030'
,'EDNUNPP008010'
,'HOGPGLN001030'
,'GEDLORE314020'
,'DARGOCP041030'
,'WPNSAIM032030'
,'GEDLORE337020'
,'JMLSWER105030'
,'GRAMECC024020'
,'LIPWTCH005020'
,'HOGNITS100030'
,'JMLBBOD101030'
,'WPNSAHM028030'
,'PFLJVHP101030'
,'GRAMECC025020'
,'VCCABRP021030'
,'AMVSBDR857020'
,'WPPOAKF136030'
,'HOGNIFA050030'
,'CSTNAHL013030'
,'BETGUMT020010'
,'WPNSAPF029030'
,'SSCPGPA522030'
,'MCEBQCH217020'
,'SSCPGAR714030'
,'MIFPGOB300030'
,'GRYPGFZ203030'
,'WONLGPK104010'
,'BBHGOCH132030'
,'WPPOAKF122020'
,'SAARIMM703020'
,'TRAPURE002030'
,'TVDMCSH775030'
,'WPNSAHP031030'
,'BBHAUAV013060'
,'WCRSKYD542040'
,'WCRSKYD543030'
,'BURTMAC967020'
,'SSCPGAR713030'
,'DRSPSBC254030'
,'JWTDEBA406010'
,'BBHKFPD623010'
,'ABDLFWW021010'
,'KRODFWS374020'
,'VCCABRP022010'
,'SCCNUIN002020'
,'VCCABRN019030'
,'LIPAGEC029010'
,'BURTLDT018020'
,'INKTBMT036010'
,'HADHARV201030'
,'PPCPPJR020010'
,'KRODFWS509020'
,'GRYPGFZ195020'
,'DHGOUTD520020'
,'HOGMSWS633020'
,'TTBNEWS011010'
,'INKTBMT052010'
,'WDFWKTV024030'
,'RUDNVDL001060'
,'SSCPGOL634030'
,'PPCFXPG008010'
,'INKTBMT051010'
,'WPNSAOP030030'
,'EURNUAP005030'
,'BOWSIMM011030'
,'LIPWTCH011010'
,'WDFWKTV023030'
,'BURTLDT020020'
,'PPLPPLF002030'
,'BETGUMT019010'
,'LOGINPF002030'
,'PPCFXPG005020'
,'BURTMAC965060'
,'JWTJJCP051020'
,'KRODFWS371020'
,'INKTBMT037010'
,'KRODFWS370040'
,'TTBBBAW031010'
,'TGFENSP479010'
,'TTBBBTT027010'
,'AMVBTHB543040'
,'COIDOHJ060020'
,'HOGLGTV003030'
,'RKYLTCA279030'
,'RKYLTCA283010'
,'TAGNXGB029030'
,'TGFENSP478030'
,'WPPOAKF121030'
,'PPCFXPG007010'
,'MIFPGOB302030'
,'BURTALW346020'
,'TRAPURE007010'
,'AMVBTHB544030'
,'WPPFURV051030'
,'MCELOEO509030'
,'TAGNXGB025020'
,'IGCENTE002030'
,'WPPFURV053010'
,'MWOWIKG321030'
,'TAGNXGB046020'
,'AMVBTBL538030'
,'BMBDAIR001020'
,'NSADSJB460030'
,'WPPFURV054010'
,'CBJMKBR379030'
,'MWOWIKS323010'
,'MUMBOWW027010'
,'NSADSJB448030'
,'TAGNXGB007030'
,'WPPFURV052010'
,'PPCFXLP037020'
,'NSADSJB447030'
,'TAGNXGB004030'
,'MIFCCBR050030'
,'HMLCPML001030'
,'FOWSKYV080030'
,'CHIARTX074020'
,'WPPFURV050030'
,'FOWSKYV079030'
,'TAGTEBD538020'
,'FOWSKYV082030'
,'KRODFWS372020'
,'NSADSJB451030'
,'TAGNXGB032020'
,'GNWDYDP001030'
,'TAGNXGB017020'
,'TAGNXGB015020'
,'DDBJLCL002020'
,'REPDRMV761030'
,'TAGNXGB028020'
,'PPCFXPG009010'
,'TVTODAY'
,'SSCASEV221020'
,'AMVBTSF537040'
,'RISHTRCU000411'
,'RISHTRCU000424'
,'RISHTRCU000224'
,'DTVSCAM042090'
,'RISHTRCU000355'
,'RISHTRCU000014'
,'RISHTRCU000425'
,'RISHTRCU000318'
,'DTVSCOC022090'
,'DTVSCJT028090'
,'RISHTRCU000225'
,'RISHTRCU000310'
,'RISHTRCU000015'
,'RISHTRCU000063'
,'DARGOCP040040'
,'RISHTRCU000315'
,'RISHTRCU000258'
,'RISHTRCU000422'
,'RISHTRCU000179'
,'RISHTRCU000450'
,'RISHTRCU000104'
,'RISHTRCU000356'
,'RISHTRCU000010'
,'TKYEOMC001030'
,'LBEDISL010050'
,'RISHTRCU000470'
,'RISHTRCU000504'
,'RISHTRCU000003'
,'RISHTRCU000471'
,'AMVPCBR300020'
,'RISHTRCU000423'
,'RISHTRCU000320'
,'RISHTRCU000268'
,'BBHKFPD619010'
,'DTVSCAM048090'
);

--test 10.6
--only one - 37


--defect 29
select audit_action_date,count(1) from attribution_audit_data group by audit_action_date

--defect 33
select * from barb_data_amends






---
drop table #barb_spot;
drop table #barb_spot2;

select log_station_code_for_spot || split_transmission_indicator as lscsti, count(1)
,log_station_code_for_spot , split_transmission_indicator
into #barb_spot
from barb_spot_data
group by lscsti
,log_station_code_for_spot , split_transmission_indicator
--363

select service_key,lscsti
into #barb_spot2
from channel_map_dev_service_key_barb as lkp
inner join #barb_spot as bar on lkp.log_station_code || sti_code = bar.lscsti
--669

select log_station_code || split_transmission_indicator as lscsti, count(1)
into #barb_promo
from barb_promo_data
group by lscsti
--86

select service_key
,lscsti
--into #barb_promo2
from channel_map_dev_service_key_barb as lkp
inner join #barb_promo as bar on lkp.log_station_code || sti_code = bar.lscsti
group by service_key
,lscsti
order by service_key
--150

select sare_no,count(1)
into #landmark
from landmark_data
group by sare_no
--157

select distinct(service_key)
into #landmark2
from channel_map_dev_service_key_landmark as lkp
inner join #landmark as lan on lkp.sare_no = lan.sare_no
--242

select * from #barb_spot2;
select * from #barb_promo2;
select * from #landmark2;



select distinct(si_service_key) from bss_data


--defect 007
select source_system,source_channel_code_1,source_channel_code_2,count(1)
from smi_access.smi_etl.v_slot_instance_dim
group by source_system,source_channel_code_1,source_channel_code_2
order by source_channel_code_1

--channel mapping
select * from dis_reference.dis_etl.v_service_key_code_mapping

--new query
select map.service_key,source_system,source_channel_code_1,source_channel_code_2,count(1)
from smi_access.smi_etl.v_slot_instance_dim as slt
     right join dis_reference.dis_etl.v_service_key_code_mapping as map on slt.source_channel_code_1 = map.spot_source_code_1
where spot_source='Landmark'
group by map.service_key,source_system,source_channel_code_1,source_channel_code_2
order by map.service_key








select date_of_transmission,count(1) from barb_promo_data group by date_of_transmission
select * from barb_data_amends
select count(1) from attribution_audit_data;
select top 10 * from attribution_audit_data where audit_action_date='20121211'
select name_type,count(1) from attribution_audit_data group by name_type
select audit_action_date,count(1) from attribution_audit_data group by audit_action_date


barb_spots for Dec7
151557
21 amends

barb_promos for Dec7
98,310
no amends

landmark
197,971 for Jan9
9086 amends for Dec13

bss Dec14
19227 promos
5136 sponsorship




11002
select * from attribution_audit_data
where name_type in ('2','','B')

select top 10 * from attribution_audit_data
select audit_action_date,count(1)
from attribution_audit_data
group by audit_action_date
order by audit_action_date

select commercial_number,max(audit_action_date)
from attribution_audit_data
group by commercial_number
order by commercial_number




name_type count(1)
  2
2 2
B 16
H 20
A 29
P 33
M 8984





SELECT count(1)
FROM neighbom.BARB_CET_LOG
WHERE RIGHT (filename,3) <> 'CET' and audit_action = 'Records Inserted to master file'
select user



select top 10 * from attribution_data
select top 10 * from landmark_data

select count(distinct clearcast_commercial_no) from landmark_data
--1334

select count(distinct commercial_number) from attribution_data
--207141

select count(distinct commercial_number) from landmark_data as lan inner join attribution_data as att on lan.clearcast_commercial_no=att.commercial_number
--526



select * from barb_data_amends

select
--*
distinct dat.clearcast_commercial_number
from barb_spot_data as dat
inner join barb_data_amends as ame on dat.clearcast_commercial_number = ame.clearcast_commercial_number


select * from
attribution_data where commercial_number in (
'TAGKLGB005030'
,'EURPGYB559030'
,'CHICWSG276030'
,'VCCOOPG397030'
,'APONEXS007030'
,'MUENOGL004030'
,'TAGINGB104030'
,'ARNJDBT001030'
,'HOGPGDG067030'
,'CCEGCAM203030'
,'DDBTMAU039030'
,'CNZLEGO410030'
,'OHESCVB059030'
)





select count(1) from barb_spot_data where


select count(1) from barb_spot_data
where



select * from #barb_spot2



select *
from bss_data
where cast(si_service_key as int) in(
4085
,4059
,4027
,4024
,4005
,1405
,1364
,1363
,1362
,1361
,1355
,1346
,1344
,1343
,1341
,1320
,1309
,1308
,4054
,1704
,1703
,1430
,1345
,1313
,1311
,1310
,4335
,4101
,4001
,3836
,1851
,1831
,1307
)


select top 10 * from landmark_data
where sare_no=3002


select top 100 * from attribution_data
where commercial_number in ('GEDLORE326020','GRFBDAS001010')

select top 100 * from attribution_audit_data
where commercial_number in ('CHICWSG276030')

--defect 029 test:


select  --dk_broadcast_start_datehour_dim,*
dk_broadcast_start_datehour_dim,dk_broadcast_start_time_dim,product_code,product_name,clearcast_commercial_no
from smi_dw.smi_etl.SLOT_DIM as sld
inner join smi_dw.SMI_ETL.SLOT_INSTANCE_ASOC as sia on sld.pk_slot_dim = sia.DK_SLOT_DIM
where clearcast_commercial_no in (
--'DLKMOVA217030' --NMR change
--,'BURTCOP002030'--NMR change
--,'MCMALCG351020'--NMR change
--,'MCMALCG346020'--NMR change
--,'GEDLORE326020' --ok
--'GRFBDAS001010' --ok
'CHICWSG276030' --x already changed at 20121204
--,'GRFBDAS002020' --ok
--,'DLKMOBD215060'--NMR change
--,'TRAPURE006010'--NMR change
)
group by dk_broadcast_start_datehour_dim,dk_broadcast_start_time_dim,product_code,product_name,clearcast_commercial_no
order by clearcast_commercial_no,dk_broadcast_start_datehour_dim


select count(1) from attribution_audit_data


--these are the only 10 that have data before and after their audit date
select * from attribution_audit_data
where commercial_number in (
'DLKMOVA217030'
,'BURTCOP002030'
,'MCMALCG351020'
,'MCMALCG346020'
,'GEDLORE326020'
,'GRFBDAS001010'
,'CHICWSG276030'
,'GRFBDAS002020'
,'DLKMOBD215060'
,'TRAPURE006010'
)



select count(1)  from attribution_audit_data

select count(1) from (
select audit_action_date, commercial_number
from attribution_audit_data
group by audit_action_date, commercial_number
) as sub
--7505

select count(distinct commercial_number) from attribution_audit_data
--7504




select top 10 *
from channel_map_dev_service_key_attributes as t1
inner join channel_map_dev_service_key_attributes as t2 on t1.parent_service_key = t2.service_key
where t1.timeshift_minutes>0
and t1.effective_from < '2012-12-07'
and t1.effective_to   > '2012-12-07'
and t2.effective_from < '2012-12-07'
and t2.effective_to   > '2012-12-07'
and (t1.promo_source <> t2.promo_source
or t1.spot_source <> t2.spot_source)










select len(nmr_category_code) as l,count(1) from attribution_data group by l


--finding landmark data
select count(1),min(slot_start_broadcast_date),max(slot_start_broadcast_date) from
ADMIN.STT_LANDMARK_SPOTS_INITIAL_MAPPING

dis_prepare.ADMIN.SLOT_TIMETABLE_LANDMARK_SPOTS_d
COUNT MIN MAX
2634639 2012-04-24 2012-12-23

dis_prepare.ADMIN.SLOT_TIMETABLE_LANDMARK_SPOTS_d_151212
2477203 2012-11-01 2012-12-13

dis_prepare.ADMIN.SLOT_TIMETABLE_LANDMARK_SPOTS_d_backlog
321663110 2010-06-20 2013-01-10

dis_prepare.ADMIN.STT_LANDMARK_SPOTS_INITIAL_MAPPING
4400588 2012-04-24 2012-12-23


select slot_start_broadcast_date,count(1) from dis_prepare.ADMIN.SLOT_TIMETABLE_LANDMARK_SPOTS_d_backlog
group by slot_start_broadcast_date
order by slot_start_broadcast_date

select * from dis_prepare.ADMIN.SLOT_TIMETABLE_LANDMARK_SPOTS_d_backlog
limit 10



select count(1) from landmark_data
select top 10 * from landmark_data
select min(slot_start_broadcast_date) from landmark_data

select top 10 * from barb_spot_data
select max(date_of_transmission) from barb_spot_data

select insert_delete_amend_code,count(1) from barb_data_amends
where date_of_transmission='2012-12-24'
group by insert_delete_amend_code

select top 100 spot_start_time,* from barb_data_amends
where clearcast_commercial_number='ABCWONG029030'
order by spot_start_time


select top 10 * from neighbom.barb_master_spot_data
where barb_date_of_transmission = '2012-12-24'
and clearcast_commercial_no='ABCWONG029030'
and barb_spot_start_time='095858'

--test 10.10
select top 100 * from barb_promo_amends
where insert_delete_amend_code ='D'
and log_station_code <>'11111'
and broadcaster_transmission_code <>''
order by break_start_time


select top 1000 * from barb_promo_amends
where broadcaster_transmission_code ='00000000000075288049'
order by break_start_time

select
insert_delete_amend_code
,date_of_transmission
,reporting_panel_Code
,station_code
,log_station_code
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,break_end_time
,broadcaster_transmission_code
,interval_name
,sponsorship_code
,break_type
,area_flags
,content_id
,isan_number
into #promo_amends
from barb_promo_amends
where broadcaster_transmission_code <>''
group by insert_delete_amend_code
,date_of_transmission
,reporting_panel_Code
,station_code
,log_station_code
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,break_end_time
,broadcaster_transmission_code
,interval_name
,sponsorship_code
,break_type
,area_flags
,content_id
,isan_number
;

select count(1) from barb_promo_amends
2820
select count(1) from #promo_amends
544
select * from #promo_amends

select
date_of_transmission
,reporting_panel_Code
,station_code
,log_station_code
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,break_end_time
,broadcaster_transmission_code
,interval_name
,sponsorship_code
,break_type
,area_flags
,content_id
,isan_number
,count(1) as cow
into #promo_amends2
from #promo_amends
group by date_of_transmission
,reporting_panel_Code
,station_code
,log_station_code
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,break_end_time
,broadcaster_transmission_code
,interval_name
,sponsorship_code
,break_type
,area_flags
,content_id
,isan_number
;

--these are just the real changes i.e. not just changes to the numbers at the end
select bas.*
,count(1) as cow
into #promo_amends3
from #promo_amends2 as sub
inner join #promo_amends as bas on bas.broadcaster_transmission_code = sub.broadcaster_transmission_code
                               and bas.date_of_transmission = sub.date_of_transmission
                               and bas.reporting_panel_Code = sub.reporting_panel_Code
                               and bas.station_code = sub.station_code
                               and bas.log_station_code = sub.log_station_code
                               and bas.break_split_transmission_indicator = sub.break_split_transmission_indicator
                               and bas.break_platform_indicator = sub.break_platform_indicator
                               and bas.break_start_time = sub.break_start_time
                               and bas.break_end_time = sub.break_end_time
                               and bas.broadcaster_transmission_code = sub.broadcaster_transmission_code
                               and bas.interval_name = sub.interval_name
                               and bas.sponsorship_code = sub.sponsorship_code
                               and bas.break_type = sub.break_type
                               and bas.area_flags = sub.area_flags
                               and bas.content_id = sub.content_id
                               and bas.isan_number = sub.isan_number
group by date_of_transmission
,reporting_panel_Code
,station_code
,log_station_code
,break_split_transmission_indicator
,break_platform_indicator
,break_start_time
,break_end_time
,broadcaster_transmission_code
,interval_name
,sponsorship_code
,break_type
,area_flags
,content_id
,isan_number
;

select log_station_code,reporting_panel_code,break_start_time,insert_delete_amend_code,*
from #promo_amends
order by log_station_code,reporting_panel_code,break_start_time,insert_delete_amend_code

select log_station_code,reporting_panel_code,break_start_time,*
from #promo_amends2
order by log_station_code,reporting_panel_code,break_start_time

select log_station_code_for_break from barb_data_amends group by log_station_code_for_break

select * from barb_data_amends
where log_station_code_for_break='5055'


select slot_start_broadcast_date from landmark_data group by slot_start_broadcast_date
select top 10 * from barb_spot_data

--test 3.2 again
select log_station_code_for_spot,split_transmission_indicator,count(1)
into #barb
from barb_spot_data
group by log_station_code_for_spot,split_transmission_indicator
;

select service_key
into #barb2
from #barb as bar
inner join channel_map_dev_service_key_barb as map on bar.log_station_code_for_spot = map.log_station_code
and bar.split_transmission_indicator = map.sti_code
group by service_key
;

select service_key
into #landmark
from landmark_data
group by service_key
;

select service_key
into #landmark2
from landmark_data as lan
inner join channel_map_dev_service_key_landmark as map on lan.sare_no=map.sare_no
group by service_key
;

select lan.service_key
into #both
from #barb2 as bar
inner join #landmark2 as lan on bar.service_key = lan.service_key
group by lan.service_key

  select broadcaster_spot_number
    from barb_spot_data                              as bas
         inner join channel_map_dev_service_key_barb as map on bas.log_station_code_for_spot = map.log_station_code
                                                           and bas.split_transmission_indicator = map.sti_code
         inner join #both                            as bth on bth.service_key = map.service_key
group by broadcaster_spot_number
order by broadcaster_spot_number
;--48,118

  select source_slot_instance_id
    from landmark_data as bas
         inner join channel_map_dev_service_key_landmark as map on bas.sare_no = map.sare_no
         inner join #both                                as bth on bth.service_key = map.service_key
group by source_slot_instance_id
order by source_slot_instance_id
;--47,983

select top 10 * from landmark_data



broadcaster_spot_number
417884443
not in landmark
select count(1) from landmark_data
where source_slot_instance_id='41784443'

select top 10 * from barb_spot_data
where break_start_time > 70000

--test 3.11
  select bar.service_key,map.*
    from #barb2 as bar
         left join #landmark2 as lan on bar.service_key=lan.service_key
         inner join channel_map_dev_service_key_attributes as map on bar.service_key = map.service_key
   where lan.service_key is null
and spot_source<>'BARB'
and effective_from < '2012-12-07'
and effective_to > '2012-12-07'
--4033 (1814), 4088 (1448)


--test 3.10
select map.*
  from #landmark2 as lan
         inner join channel_map_dev_service_key_attributes as map on lan.service_key = map.service_key
where spot_source<>'Landmark'
and effective_from < '2012-12-07'
and effective_to > '2012-12-07'

select top 10 * from attribution_data
where commercial_number='TAGDVGB155020'

--test 4.3
select top 10 bar.* from barb_spot_data as bar
left join attribution_data as att on bar.clearcast_commercial_number = att.commercial_number
where att.commercial_number is null

select count(1)
from barb_spot_data as bar
left join attribution_data as att on bar.clearcast_commercial_number = att.commercial_number
where att.commercial_number is null

select bar.clearcast_commercial_number
from barb_spot_data as bar
left join attribution_data as att on bar.clearcast_commercial_number = att.commercial_number
where att.commercial_number is null
group by bar.clearcast_commercial_number

select date_of_transmission,count(1) from attribution_data
group by date_of_transmission

select * from barb_spot_data
where clearcast_commercial_number in ('BBMFDCB003030'
,'BRYSSXM001020'
,'BRYSSXM002020'
,'BRYSSXM004020'
,'TVTODAY'
,'RISHTRCU000319'
,'RISHTRCU000353'
,'RISHTRCU000371'
)



select * from #keys
select * from channel_map_dev_service_key_attributes


select top 10 * from landmark_data

where sare_no = 8002


select top 10 * from bss_data
where si_service_key='1814'






select * from barb_spot_data
where log_station_code_for_spot=5046




select distinct(date_of_transmission) from barb_spot_data






check campaign ids


--defect 046 retest
  select substr(sia.DK_BROADCAST_START_DATEHOUR_DIM,1,8) as dt
        ,service_key
        ,count(1)
    from smi_access..v_slot_dim as sld
         inner join smi_dw..viewing_slot_instance_fact_volatile as sia on sld.pk_slot_dim    = sia.DK_SLOT_DIM
         inner join smi_access..V_CHANNEL_DIM        as chn on sia.DK_CHANNEL_DIM = chn.PK_CHANNEL_DIM
--  where dk_slot_instance_source_type_dim = 800001 --BARB
  where dk_slot_instance_source_type_dim = 800002 --LM
--where dk_slot_instance_source_type_dim = 800003 --BSS
and service_key in (4033,4088)
and slot_type='SLOT'
group by service_key,dt
order by service_key,dt




--tests to analyse campaign_approval_ids
select campaign_approval_id, count(1)
from barb_spot_data
group by  campaign_approval_id


select campaign_approval_id, count(1)
--select top 10 *
from neighbom.barb_master_spot_data
--where barb_date_of_transmission = '2012-12-07'
where filename='B20121207.CET'
group by campaign_approval_id


select log_station_code_for_spot,split_transmission_indicator,count(1) from (
  select right('0' || break_start_time,6) as tm
        ,log_station_code_for_spot,split_transmission_indicator
    from barb_spot_data
group by tm,log_station_code_for_spot,split_transmission_indicator
) as sub group by log_station_code_for_spot,split_transmission_indicator;

select log_station_code,sti_code,count(1) from (
  select local_break_start_date_time as tm
        ,log_station_code,sti_code
    from neighbom.barb_master_spot_data
   where filename='B20121207.CET'
group by tm, log_station_code,sti_code
)as sub group by log_station_code,sti_code



select * from barb_spot_data
where log_station_code_for_spot=10262
and split_transmission_indicator=0
order by break_start_time
;
select *
 from neighbom.barb_master_spot_data
where local_break_start_date_time='2012-12-07 11:27:10.000000'

where log_station_code = 10262
and sti_code=0
and filename='B20121207.CET'
order by local_break_start_date_time

select * from greenj.barb_spot_data
where log_station_code_for_spot || 'x' || split_transmission_indicator in (
'1015x6'
,'1016x7'
,'10211x0'
,'10294x0'
,'11202x1'
,'11202x2'
,'1204x2'
,'1208x1'
,'1213x0'
,'1215x6'
,'1216x4'
,'14002x0'
,'14002x1'
,'14067x0'
,'1407x0'
,'1408x0'
,'1408x1'
,'1416x1'
,'1416x4'
,'4738x0'
,'5057x0'
,'5058x0'
,'10262x0'
,'10263x0'
)

grant select on barb_spot_data to public
commit







