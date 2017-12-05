----Import Spot and Promotion Files from Staging to for Project 060---

--

commit;
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

--select * from vespa_analysts.project060_raw_spot_file_20120429 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120429 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120429 where row_number =1;

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
into vespa_analysts.project060_spot_file_20120429
from vespa_analysts.project060_raw_spot_file_20120429
;

--select * from vespa_analysts.project060_spot_file_20120429 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120429 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120429 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120429 where spot_platform_indicator not in ( '00','0A','28');

commit;
--select count(*) from vespa_analysts.project060_spot_file_20120429;
---Create Service Key to Triplet key and Channel Lookup----
--Only 1 channel per service_key/triplet but min (channel_name) used to be sure

select service_key 
, ssp_network_id
,transport_id
 ,service_id 
, min(channel_name) as channel  
into vespa_analysts.project060_service_key_triplet_lookup
from sk_prod.vespa_epg_dim where tx_date = '20120429' 
group by service_key 
, ssp_network_id
,transport_id
 ,service_id  ;

commit;

--select * from vespa_analysts.project060_service_key_triplet_lookup order by upper(channel) ;


--Load in Log Station Code/STI to Service Key Lookup

create table vespa_analysts.log_station_sti_to_service_key_lookup
(SERVICE_KEY integer
,LOG_STATION_CODE integer
,STI_CODE integer
);

input into vespa_analysts.log_station_sti_to_service_key_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\Log Station And STI to Service Key Lookup.csv' format ascii;

commit;
--select * from vespa_analysts.log_station_sti_to_service_key_lookup order by log_station_code;
---Load in list of Spot/Platform/Channels thare sold spots not just memorandum spots---
--drop table vespa_analysts.sold_spots_by_panel;
create table vespa_analysts.sold_spots_by_panel
(Panel_description varchar(128)
,Panel_code integer
,description_pt2 varchar(64)
,db2_station    integer
,log_station_code integer
,sti    integer
,ibt    varchar(1)
,prog   varchar(1)
,spot   varchar(1)
,break_y_n  varchar(1)
);
commit;

input into vespa_analysts.sold_spots_by_panel
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\sold spots by panelv3.csv' format ascii;

commit;

--select * from vespa_analysts.sold_spots_by_panel  where log_station_code is not null order by log_station_code;
--Create table with only sold spots
--drop table vespa_analysts.sold_spots_by_panel_sold_only;
select panel_code
,log_station_code
,sti
into vespa_analysts.sold_spots_by_panel_sold_only
from vespa_analysts.sold_spots_by_panel
where spot = 'S'
;
commit;

--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120429_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120429_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120429_expanded
from vespa_analysts.project060_spot_file_20120429 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120429_expanded;

--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120429_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120429_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120429_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select channel_name , count(*) as records from vespa_analysts.project060_spot_file_20120429_expanded group by channel_name order by records desc , channel_name;
--
/*
select service_key , log_station_code_for_spot, split_transmission_indicator 
, count(*) as records from vespa_analysts.project060_spot_file_20120429_expanded 
where channel_name = 'Unknown'
group by service_key , log_station_code_for_spot, split_transmission_indicator order by records desc ;

select  split_transmission_indicator 
, count(*) as records from vespa_analysts.project060_spot_file_20120429_expanded 

group by  split_transmission_indicator order by records desc ;


*/


--select count(*) from vespa_analysts.project060_spot_file_20120429 ;
--select count(*) from vespa_analysts.project060_spot_file_20120429_expanded where service_key is null;
--select channel_name,service_key, reporting_panel_code, count(*) as records from vespa_analysts.project060_spot_file_20120429_expanded group by  channel_name,service_key, reporting_panel_code order by records desc ;
--select * from vespa_analysts.project060_spot_file_20120429_expanded where reporting_panel_code in ('00021','00037') order by break_start_time;
--


--Spots with No channel details
/*
select log_station_code_for_spot , split_transmission_indicator ,service_key, count(*) as records from vespa_analysts.project060_spot_file_20120429_expanded 
where channel_name = 'Unknown' group by log_station_code_for_spot , split_transmission_indicator,service_key
order by records desc;


select * from vespa_analysts.log_station_sti_to_service_key_lookup where log_station_code in (4688)

*/

---Ad Hoc Analysis

select * from vespa_analysts.project060_spot_file_20120429_expanded where service_key in (6390,6391) order by break_start_time;


select log_station_code_for_spot,split_transmission_indicator, count(*) as spots
 from vespa_analysts.project060_spot_file_20120429 
where log_station_code_for_spot='01008'
group by log_station_code_for_spot,split_transmission_indicator
 order by log_station_code_for_spot,split_transmission_indicator
;

select service_key , channel_name from  sk_prod.vespa_epg_dim where service_key in (6274) 
--and tx_date = '20120428'
 group by service_key , channel_name;

commit;
select tx_date , count(*) from  sk_prod.vespa_epg_dim where service_key in (6274) 
--and tx_date = '20120428'
 group by tx_date
order by tx_date;

select * from  sk_prod.vespa_epg_dim where service_key in (3218) order by tx_date, tx_date_time_utc

select * from  sk_prod.vespa_epg_dim where channel_name = 'PBS' and tx_date = '20120429' order by tx_date, tx_date_time_utc



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

---Counts by Sales house before any deletions----

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
into vespa_analysts.project060_spot_file_20120429_test
from vespa_analysts.project060_raw_spot_file_20120429
;

--select * from vespa_analysts.project060_spot_file_20120429 order by spot_start_time ;

---Load in Sales House Names and Channel Mapping---

alter table vespa_analysts.project060_spot_file_20120429_test add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120429_test
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120429_test as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on a.sales_house_identifier=b.sales_house_identifier
;

commit;

delete from vespa_analysts.project060_spot_file_20120429_test where spot_platform_indicator not in ( '00','0A','28');

commit;

--drop table vespa_analysts.project060_spot_file_20120429_sold_spots_test;
select a.log_station_code_for_spot
,a.split_transmission_indicator
,min(b.service_key) as service_key_min
into #service_key
from vespa_analysts.project060_spot_file_20120429_test as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE
group by a.log_station_code_for_spot
,a.split_transmission_indicator
;



select a.*
,case when c.panel_code is not null then 1 else 0 end as sold_spot_match
,d.service_key_min
,channel
into vespa_analysts.project060_spot_file_20120429_sold_spots_test_sky_only
from vespa_analysts.project060_spot_file_20120429_test as a

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
left outer join #service_key as d
on cast(a.log_station_code_for_spot as integer)= cast(d.log_station_code_for_spot as integer) and cast(a.split_transmission_indicator as integer)=cast(d.split_transmission_indicator as integer)
left outer join vespa_analysts.project060_service_key_triplet_lookup as e
on d.service_key_min=e.service_key
;





/*
select sales_house
,count(*) as spots
,sum(sold_spot_match) as sold_spots
from vespa_analysts.project060_spot_file_20120429_sold_spots_test
group by sales_house
order by spots desc
;
*/

select log_station_code_for_spot
,split_transmission_indicator
,sales_house
,sales_house_identifier
,channel
,count(*) as spots
,sum(sold_spot_match) as sold_spots
from vespa_analysts.project060_spot_file_20120429_sold_spots_test_sky_only
where sales_house = 'SKY SALES'
group by log_station_code_for_spot
,split_transmission_indicator
,sales_house
,sales_house_identifier
,channel
order by spots desc
;

commit;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4381;

select log_station_code_for_spot
,split_transmission_indicator
,b.service_key
,sales_house
,count(*) as spots
,sum(sold_spot_match) as sold_spots
from vespa_analysts.project060_spot_file_20120429_sold_spots_test as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE
where sales_house = 'Unknown'
group by log_station_code_for_spot
,split_transmission_indicator
,b.service_key
,sales_house
order by sold_spots
;
commit;
select *  from vespa_analysts.project060_spot_file_20120429_sold_spots_test where log_station_code_for_spot='04381'

--select *  from vespa_analysts.project060_spot_file_20120429_test order by reporting_panel_code desc;


----Version With No Deletions

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
into vespa_analysts.project060_spot_file_20120429_insert_delete
from vespa_analysts.project060_raw_spot_file_20120429
;


select insert_delete_amend_code , count(*) as records
from  vespa_analysts.project060_spot_file_20120429_insert_delete
group by insert_delete_amend_code
order by insert_delete_amend_code













