--select distinct channel_name from dbarnett.v250_sports_rights_programme_lookup order by channel_name;

----Extra Rights to Add to Table----
--drop table dbarnett.v250_sports_rights_programme_lookup_part_2;
create table dbarnett.v250_sports_rights_programme_lookup_part_2
(channel_name varchar(50)
,analysis_right varchar(200)
,master_deal varchar(200)
,deal       varchar(200)
,title      varchar(200)
,broadcast_datetime_text varchar(200)
--,broadcast_datetime datetime
)
;
commit;
input into dbarnett.v250_sports_rights_programme_lookup_part_2
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\All Rights CSV for import (part2) v1.csv' format ascii;

commit;


alter table dbarnett.v250_sports_rights_programme_lookup_part_2 add broadcast_datetime datetime;

update dbarnett.v250_sports_rights_programme_lookup_part_2
set broadcast_datetime=cast(broadcast_datetime_text as datetime)
from dbarnett.v250_sports_rights_programme_lookup_part_2
;
commit;


drop table dbarnett.v250_sports_rights_with_service_key_part_2;
select a.*
,b.service_key
into dbarnett.v250_sports_rights_with_service_key_part_2
from dbarnett.v250_sports_rights_programme_lookup_part_2 as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.channel_name = b.channel_name
;
commit;
--select * from dbarnett.v250_sports_rights_with_service_key_part_2;
---Add on EPG Data--
--Add Record ID on match to EPG data--
alter table dbarnett.v250_sports_rights_with_service_key_part_2 add programme_id integer identity;
commit;

alter table dbarnett.v250_sports_rights_with_service_key_part_2 add broadcast_date date;
commit;

update dbarnett.v250_sports_rights_with_service_key_part_2
set broadcast_date=cast (broadcast_datetime as date)
from dbarnett.v250_sports_rights_with_service_key_part_2
;
commit;


select distinct service_key
into #sports_service_keys_part_2
from dbarnett.v250_sports_rights_with_service_key_part_2;
commit;

CREATE HG INDEX idx1 ON #sports_service_keys_part_2 (service_key);
--drop table  #epg_list;
select a.service_key
,programme_name
,genre_description
,sub_genre_description
,dk_programme_instance_dim
,dk_programme_dim
,programme_instance_duration
,broadcast_start_date_time_local
,synopsis
,cast (broadcast_start_date_time_local as date) as broadcast_date
into #epg_list_part_2
from sk_prod.Vespa_programme_schedule as a
left outer join #sports_service_keys_part_2 as b
on a.service_key=b.service_key
where b.service_key is not null and broadcast_date between '2012-11-01' and '2013-10-31'
;
commit;
--select * from  sk_prod.Vespa_programme_schedule where programme_name is like '%UFC%'

CREATE HG INDEX idx1 ON #epg_list_part_2 (service_key);
CREATE HG INDEX idx2 ON #epg_list_part_2 (broadcast_date);
CREATE HG INDEX idx3 ON #epg_list_part_2 (dk_programme_instance_dim);
commit;
--select distinct genre_description , sub_genre_description from sk_prod.Vespa_programme_schedule order by  genre_description , sub_genre_description where programme_name like '%americas

--select distinct programme_name  from sk_prod.Vespa_programme_schedule  where sub_genre_description='Watersports' order by programme_name
---Join EPG Data to Rights data by day and serice_key
drop table dbarnett.v250_sports_rights_with_possible_matches_part_2;
select a.*
,b.programme_name
,b.genre_description
,b.sub_genre_description
,b.dk_programme_instance_dim
,b.dk_programme_dim
,b.programme_instance_duration
,b.broadcast_start_date_time_local
,b.synopsis
into dbarnett.v250_sports_rights_with_possible_matches_part_2
from dbarnett.v250_sports_rights_with_service_key_part_2  as a
left outer join #epg_list_part_2 as b
on a.service_key=b.service_key 
where a.broadcast_date between b.broadcast_date-1 and  b.broadcast_date+1
;
commit;

alter table dbarnett.v250_sports_rights_with_possible_matches_part_2 add time_from_broadcast integer;

update dbarnett.v250_sports_rights_with_possible_matches_part_2
set time_from_broadcast=datediff(second,broadcast_start_date_time_local,broadcast_datetime) 
from dbarnett.v250_sports_rights_with_possible_matches_part_2
;

alter table dbarnett.v250_sports_rights_with_possible_matches_part_2 add time_from_broadcast_absolute integer;

update dbarnett.v250_sports_rights_with_possible_matches_part_2
set time_from_broadcast_absolute=case when time_from_broadcast<0 then time_from_broadcast*-1 else time_from_broadcast end
from dbarnett.v250_sports_rights_with_possible_matches_part_2
;
commit;
alter table dbarnett.v250_sports_rights_with_possible_matches_part_2 add record_id integer identity;

select a.*
,RANK() OVER ( PARTITION BY service_key 

                ,broadcast_datetime
                
                ORDER BY time_from_broadcast_absolute ASC , record_id ) AS Rank
into #rank_part_2
from dbarnett.v250_sports_rights_with_possible_matches_part_2 as a
;

commit;
drop table dbarnett.v250_sports_rights_epg_detail_part_2 ;
select * 
into dbarnett.v250_sports_rights_epg_detail_part_2 
from #rank_part_2
where rank =1 and service_key <10000 --Get Rid of Anytime/On demand service keys where broadcast time matching not possible
;
commit;

alter table dbarnett.v250_sports_rights_epg_detail_part_2 add row_number integer identity;
select  * from dbarnett.v250_sports_rights_epg_detail_part_2;
output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Full EPG Data For matching\Full EPG Data Part 2 v1.csv' format ascii;
commit;

---Add Index
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_detail_part_2 (dk_programme_instance_dim);
commit;

---Add On Live Non Live Splits 
---v2 also has updated analysis right info (e.g., Day of Week for Champions League etc.,)

--drop table  dbarnett.v250_epg_live_non_live_lookup_part_2;
create table dbarnett.v250_epg_live_non_live_lookup_part_2
(row_number integer
,live integer
,analysis_right varchar(255)
)
;
commit;
input into dbarnett.v250_epg_live_non_live_lookup_part_2
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\Live and Non Live Rights Part 2 v1.csv' format ascii;

commit;

--select * from dbarnett.v250_sports_rights_epg_detail_part_2 where title = 'Swansea v St Gallen'
--select 


--drop table dbarnett.v250_sports_rights_epg_data_for_analysis_part_2;
select a.*
,b.live
,b.analysis_right as analysis_right_new
into dbarnett.v250_sports_rights_epg_data_for_analysis_part_2
from dbarnett.v250_sports_rights_epg_detail_part_2 as a
left outer join dbarnett.v250_epg_live_non_live_lookup_part_2 as b
on a.row_number = b.row_number 
where b.live is not null
;
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis_part_2;

CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_data_for_analysis_part_2 (dk_programme_instance_dim);
commit;

update dbarnett.v250_sports_rights_epg_data_for_analysis_part_2
set row_number =row_number+100000
from dbarnett.v250_sports_rights_epg_data_for_analysis_part_2
;

alter table dbarnett.v250_sports_rights_epg_data_for_analysis_part_2 add first_dk_record integer default 1;
----Add Back to main table---


insert into  dbarnett.v250_sports_rights_epg_data_for_analysis
(select * from dbarnett.v250_sports_rights_epg_data_for_analysis_part_2)
; commit;



