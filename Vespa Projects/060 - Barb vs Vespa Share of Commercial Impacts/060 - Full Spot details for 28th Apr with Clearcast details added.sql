
----Get full spot by spot viewing figures for every day---


commit;
alter table vespa_analysts.project060_spot_file_20120428_expanded add row_number integer identity;
create hg index idx3 on vespa_analysts.project060_spot_file_20120428_expanded(row_number);
commit;

--drop table vespa_analysts.project060_spot_full_details_20120428;
---Get Views by Spot
select a.row_number
,sum(case when a.corrected_spot_transmission_start_minute between b.first_broadcast_minute and b.last_broadcast_minute then 1 else 0 end) as unweighted_views
,sum(case when a.corrected_spot_transmission_start_minute between b.first_broadcast_minute and b.last_broadcast_minute then weighting_value else 0 end) as weighted_views
into vespa_analysts.project060_spot_full_details_20120428
from vespa_analysts.project060_spot_file_20120428_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120428 as b
on a.service_key=b.service_key
where a.service_key is not null
group by  a.row_number
;
commit;
--Match details back to get spot info---
--select top 100 * from vespa_analysts.project060_spot_file_20120428_expanded;
--drop table vespa_analysts.spot_20120428_summary;
select b.service_key
,b.channel_name
,b.sales_house
,b.sales_house_identifier
,b.log_station_code_for_spot as log_station_code
,b.split_transmission_indicator
,b.spot_duration_integer
,b.date_of_transmission
,b.spot_start_time
,b.corrected_spot_transmission_start_datetime
,b.clearcast_commercial_number
, a.unweighted_views , a.weighted_views  
into vespa_analysts.spot_20120428_summary
from vespa_analysts.project060_spot_full_details_20120428 as a
left outer join vespa_analysts.project060_spot_file_20120428_expanded as b
on a.row_number=b.row_number
order by service_key , log_station_code , corrected_spot_transmission_start_datetime;

--select * from vespa_analysts.spot_20120429_summary where channel_name = 'PBS' order by corrected_spot_transmission_start_datetime;
--select * from vespa_analysts.project060_spot_file_20120428_expanded where channel_name= '4Music' order by corrected_spot_transmission_start_datetime
--select * from vespa_analysts.project060_spot_file_20120428_expanded where channel_name= 'PBS' order by corrected_spot_transmission_start_datetime
--select unweighted_views , count(*) as spots from vespa_analysts.spot_20120428_summary group by unweighted_views order by unweighted_views;

select * from vespa_analysts.spot_20120428_summary order by service_key , log_station_code , corrected_spot_transmission_start_datetime;


output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\full_spot_details_20120428.csv' format ascii;

commit;

select service_key
,log_station_code
,split_transmission_indicator
--,spot_duration_integer
,count(*) as spots
,sum(case when unweighted_views =0 then 1 else 0 end) as zero_rated_spots
from vespa_analysts.spot_20120428_summary
group by service_key
,log_station_code
,split_transmission_indicator
--,spot_duration_integer
order by service_key
,log_station_code
,split_transmission_indicator
--,spot_duration_integer
;

commit;

--select * from #spot_20120428_summary order by unweighted_views ;

