----Import Spot and Promotion Files from Staging to for Project 060---

--


create table vespa_analysts.project060_raw_spot_file_20120429
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120429
(  full_column_detail '\n')
FROM '/staging2/B20120429.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120429 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120429 add row_number integer identity;

select * from vespa_analysts.project060_raw_spot_file_20120429;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120429 where row_number =1;


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
into vespa_analysts.project060_spot_file_20120429
from vespa_analysts.project060_raw_spot_file_20120429
;

select * from vespa_analysts.project060_spot_file_20120429 order by spot_start_time ;

---Load in Sales House Names and Channel Mapping---

create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;

commit;

alter table vespa_analysts.project060_spot_file_20120429 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120429
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120429 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on a.sales_house_identifier=b.sales_house_identifier
;

commit;

---Create Service Key to Triplet key and Channel Lookup----














---Ad Hoc Analysis


select log_station_code_for_spot,split_transmission_indicator, count(*) as spots
 from vespa_analysts.project060_spot_file_20120429 
where log_station_code_for_spot='01008'
group by log_station_code_for_spot,split_transmission_indicator
 order by log_station_code_for_spot,split_transmission_indicator
;





select sales_house ,sales_house_identifier, count(*) as records from vespa_analysts.project060_spot_file_20120429
where reporting_panel_code = '00050'
 group by sales_house,sales_house_identifier order by records desc






commit;

select station_code ,log_station_code_for_spot, count(*) as spots
 from vespa_analysts.project060_spot_file_20120429 
group by  station_code ,log_station_code_for_spot order by spots desc
;


select break_start_time , log_station_code_for_break ,split_transmission_indicator, count(*) as spots
 from vespa_analysts.project060_spot_file_20120429 
where reporting_panel_code = '00050'
group by   log_station_code_for_break ,split_transmission_indicator ,break_start_time order by  log_station_code_for_break ,split_transmission_indicator ,break_start_time
;

select log_station_code_for_spot , count(*)
 from vespa_analysts.project060_spot_file_20120429 
where reporting_panel_code = '00050'
group by log_station_code_for_spot
order by log_station_code_for_spot
-- and log_station_code_for_break = '11411'
;
commit;

commit;

select spot_platform_indicator, count(*) as spots
 from vespa_analysts.project060_spot_file_20120429 
group by  spot_platform_indicator order by spots desc
;



select top 100 * from sk_prod.vespa_epg_dim where tx_date = '20120429' ;

select service_key 
, ssp_network_id
,transport_id
 ,service_id 
, min(channel_name) as channel 
, max(channel_name) as max_channel 
into #service_key_triplet_lookup
from sk_prod.vespa_epg_dim where tx_date = '20120429' 
group by service_key 
, ssp_network_id
,transport_id
 ,service_id  ;

select * from #service_key_triplet_lookup where min_channel<>max_channel;
select * from #service_key_triplet_lookup order by channel ;

select count(*) , count(distinct service_key) from #service_key_triplet_lookup;
--dedupe triplet 
select barb_code , channel_name , count(*)  as records from sk_prod.vespa_epg_dim where tx_date = '20120429' group by barb_code , channel_name order by records desc 

commit;

