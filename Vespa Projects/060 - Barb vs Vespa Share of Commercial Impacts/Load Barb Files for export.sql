--Create Files for export--


create table vespa_analysts.project060_raw_spot_file_20120428_for_export
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120428_for_export
(  full_column_detail '\n')
FROM '/staging2/B20120428.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120428 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120428_for_export add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120428 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120428 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120428_for_export where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120428;
--drop table vespa_analysts.project060_spot_file_20120428;
select substr(full_column_detail,1,2) as record_type
,substr(full_column_detail,3,1) as insert_delete_amend_code
,substr(full_column_detail,4,8) as date_of_transmission
,substr(full_column_detail,12,5)  as reporting_panel_code
,substr(full_column_detail,17,5)  as log_station_code_for_break
,substr(full_column_detail,22,2)  as break_split_transmission_indicator
,substr(full_column_detail,24,2)  as break_platform_indicator
,substr(full_column_detail,26,6)  as break_start_time
,substr(full_column_detail,32,5)  as spot_break_total_duration
,substr(full_column_detail,37,2)  as break_type
,substr(full_column_detail,39,2)  as spot_type
,substr(full_column_detail,41,12)  as broadcaster_spot_number
,substr(full_column_detail,53,5)  as station_code
,substr(full_column_detail,58,5)  as log_station_code_for_spot
,substr(full_column_detail,63,2)  as split_transmission_indicator
,substr(full_column_detail,65,2)  as spot_platform_indicator
,substr(full_column_detail,67,2)  as hd_simulcast_spot_platform_indicator
,substr(full_column_detail,69,6)  as spot_start_time
,substr(full_column_detail,75,5)  as spot_duration
,substr(full_column_detail,80,15)  as clearcast_commercial_number
,substr(full_column_detail,95,35)  as sales_house_brand_description
,substr(full_column_detail,130,40)  as preceding_programme_name
,substr(full_column_detail,170,40)  as succeding_programme_name
,substr(full_column_detail,210,5)  as sales_house_identifier
,substr(full_column_detail,215,10)  as campaign_approval_id
,substr(full_column_detail,225,5)  as campaign_approval_id_version_number
,substr(full_column_detail,230,2)  as interactive_spot_platform_indicator
,substr(full_column_detail,232,17)  as blank_for_padding
into vespa_analysts.project060_spot_file_20120428_for_export
from vespa_analysts.project060_raw_spot_file_20120428_for_export
;



create table vespa_analysts.project060_raw_spot_file_20120429_for_export
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120429_for_export
(  full_column_detail '\n')
FROM '/staging2/B20120429.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120429 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120429_for_export add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120429 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120429 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120429_for_export where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120429;
--drop table vespa_analysts.project060_spot_file_20120429;
select substr(full_column_detail,1,2) as record_type
,substr(full_column_detail,3,1) as insert_delete_amend_code
,substr(full_column_detail,4,8) as date_of_transmission
,substr(full_column_detail,12,5)  as reporting_panel_code
,substr(full_column_detail,17,5)  as log_station_code_for_break
,substr(full_column_detail,22,2)  as break_split_transmission_indicator
,substr(full_column_detail,24,2)  as break_platform_indicator
,substr(full_column_detail,26,6)  as break_start_time
,substr(full_column_detail,32,5)  as spot_break_total_duration
,substr(full_column_detail,37,2)  as break_type
,substr(full_column_detail,39,2)  as spot_type
,substr(full_column_detail,41,12)  as broadcaster_spot_number
,substr(full_column_detail,53,5)  as station_code
,substr(full_column_detail,58,5)  as log_station_code_for_spot
,substr(full_column_detail,63,2)  as split_transmission_indicator
,substr(full_column_detail,65,2)  as spot_platform_indicator
,substr(full_column_detail,67,2)  as hd_simulcast_spot_platform_indicator
,substr(full_column_detail,69,6)  as spot_start_time
,substr(full_column_detail,75,5)  as spot_duration
,substr(full_column_detail,80,15)  as clearcast_commercial_number
,substr(full_column_detail,95,35)  as sales_house_brand_description
,substr(full_column_detail,130,40)  as preceding_programme_name
,substr(full_column_detail,170,40)  as succeding_programme_name
,substr(full_column_detail,210,5)  as sales_house_identifier
,substr(full_column_detail,215,10)  as campaign_approval_id
,substr(full_column_detail,225,5)  as campaign_approval_id_version_number
,substr(full_column_detail,230,2)  as interactive_spot_platform_indicator
,substr(full_column_detail,232,17)  as blank_for_padding
into vespa_analysts.project060_spot_file_20120429_for_export
from vespa_analysts.project060_raw_spot_file_20120429_for_export
;



create table vespa_analysts.project060_raw_spot_file_20120430_for_export
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120430_for_export
(  full_column_detail '\n')
FROM '/staging2/B20120430.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120430 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120430_for_export add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120430 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120430 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120430_for_export where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120430;
--drop table vespa_analysts.project060_spot_file_20120430;
select substr(full_column_detail,1,2) as record_type
,substr(full_column_detail,3,1) as insert_delete_amend_code
,substr(full_column_detail,4,8) as date_of_transmission
,substr(full_column_detail,12,5)  as reporting_panel_code
,substr(full_column_detail,17,5)  as log_station_code_for_break
,substr(full_column_detail,22,2)  as break_split_transmission_indicator
,substr(full_column_detail,24,2)  as break_platform_indicator
,substr(full_column_detail,26,6)  as break_start_time
,substr(full_column_detail,32,5)  as spot_break_total_duration
,substr(full_column_detail,37,2)  as break_type
,substr(full_column_detail,39,2)  as spot_type
,substr(full_column_detail,41,12)  as broadcaster_spot_number
,substr(full_column_detail,53,5)  as station_code
,substr(full_column_detail,58,5)  as log_station_code_for_spot
,substr(full_column_detail,63,2)  as split_transmission_indicator
,substr(full_column_detail,65,2)  as spot_platform_indicator
,substr(full_column_detail,67,2)  as hd_simulcast_spot_platform_indicator
,substr(full_column_detail,69,6)  as spot_start_time
,substr(full_column_detail,75,5)  as spot_duration
,substr(full_column_detail,80,15)  as clearcast_commercial_number
,substr(full_column_detail,95,35)  as sales_house_brand_description
,substr(full_column_detail,130,40)  as preceding_programme_name
,substr(full_column_detail,170,40)  as succeding_programme_name
,substr(full_column_detail,210,5)  as sales_house_identifier
,substr(full_column_detail,215,10)  as campaign_approval_id
,substr(full_column_detail,225,5)  as campaign_approval_id_version_number
,substr(full_column_detail,230,2)  as interactive_spot_platform_indicator
,substr(full_column_detail,232,17)  as blank_for_padding
into vespa_analysts.project060_spot_file_20120430_for_export
from vespa_analysts.project060_raw_spot_file_20120430_for_export
;

select * from vespa_analysts.project060_spot_file_20120428_for_export; output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB20120428.csv' format ascii;
select * from vespa_analysts.project060_spot_file_20120429_for_export; output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB20120429.csv' format ascii;
select * from vespa_analysts.project060_spot_file_20120430_for_export; output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB20120430.csv' format ascii;

select top 10 * from vespa_analysts.project060_spot_file_20120430_for_export ;  output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\file format.xls' format excel;
