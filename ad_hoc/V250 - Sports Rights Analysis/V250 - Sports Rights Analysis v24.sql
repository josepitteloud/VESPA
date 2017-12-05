select  service_key
,vespa_name as channel_name

into v250_channel_to_service_key_lookup
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES 
--where effective_to >= '2999-12-31'
group by service_key
,channel_name
;
commit;
--select * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES  where service_key in (1301,1701,4002)
--select * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES  where service_key in (2076)
--select * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES  where epg_name = 'ITV1 Tyne Tees'

select service_key
,min(channel_name) as min_name
,max(channel_name) as max_name
,count(distinct channel_name) as different_channel_names
into #service_key_details
from v250_channel_to_service_key_lookup
group by service_key
;
commit;
--select * from #service_key_details  order by different_channel_names desc, min_name;

select service_key
,case when min_name='Other TV' then max_name else min_name end as channel_name
into v250_channel_to_service_key_lookup_deduped
from #service_key_details
group by service_key
,channel_name
;

commit;

--Correct BBC2 to BBC HD---

select * from  v250_channel_to_service_key_lookup_deduped order by channel_name;


update v250_channel_to_service_key_lookup_deduped
set channel_name='BBC HD'
from v250_channel_to_service_key_lookup_deduped
where service_key=2075
;
commit;

update v250_channel_to_service_key_lookup_deduped
set channel_name='More 4'
from v250_channel_to_service_key_lookup_deduped
where service_key=4043
;
commit;

--select * from v250_channel_to_service_key_lookup_deduped order by service_key desc
--select * from v250_channel_to_service_key_lookup_deduped order by channel_name

---Import in Rights Data----
--select * into dbarnett.v250_sports_rights_programme_lookup_old from dbarnett.v250_sports_rights_programme_lookup;commit;
--drop table dbarnett.v250_sports_rights_programme_lookup;
create table dbarnett.v250_sports_rights_programme_lookup
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
input into dbarnett.v250_sports_rights_programme_lookup
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\All Rights CSV for importv9.csv' format ascii;

commit;


alter table dbarnett.v250_sports_rights_programme_lookup add broadcast_datetime datetime;

update dbarnett.v250_sports_rights_programme_lookup
set broadcast_datetime=cast(broadcast_datetime_text as datetime)
from dbarnett.v250_sports_rights_programme_lookup
;
commit;
/*
---Correct 2 events where broadcast_datetime text incorrectly in title
update dbarnett.v250_sports_rights_programme_lookup
set broadcast_datetime=cast(title as datetime)
from dbarnett.v250_sports_rights_programme_lookup
where title = '2013-03-19 06:00:00'
;

select * from dbarnett.v250_sports_rights_programme_lookup where title = '2013-03-19 06:00:00'
select * from dbarnett.v250_sports_rights_programme_lookup where channel_name = 'ESPN'

commit;
*/
--select count(*) from dbarnett.v250_sports_rights_programme_lookup;
--select master_deal , deal from dbarnett.v250_sports_rights_programme_lookup group by master_deal , deal order by master_deal , deal

--select channel_name,master_deal , deal from dbarnett.v250_sports_rights_programme_lookup group by channel_name,master_deal , deal order by channel_name,master_deal , deal
--select * from  dbarnett.v250_sports_rights_programme_lookup where channel_name = 'Channel 5'

--select * from dbarnett.v250_sports_rights_programme_lookup where title = 'Wimbledon'


---Create Matched table of Service Key/Channel and Sports Rights---
drop table dbarnett.v250_sports_rights_with_service_key;
select a.*
,b.service_key
into dbarnett.v250_sports_rights_with_service_key
from dbarnett.v250_sports_rights_programme_lookup as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.channel_name = b.channel_name
;
commit;
--select * from dbarnett.v250_sports_rights_with_service_key;
---Add on EPG Data--
--Add Record ID on match to EPG data--
alter table dbarnett.v250_sports_rights_with_service_key add programme_id integer identity;
commit;

alter table dbarnett.v250_sports_rights_with_service_key add broadcast_date date;
commit;

update dbarnett.v250_sports_rights_with_service_key
set broadcast_date=cast (broadcast_datetime as date)
from dbarnett.v250_sports_rights_with_service_key
;
commit;

--select channel_name , count(*) from dbarnett.v250_sports_rights_with_programme_details  where dk_programme_instance_dim is not null group by channel_name
--select top 150 * from dbarnett.v250_sports_rights_with_programme_details
---Return all EPG data into a temp table to enable matching by day and service key--
--drop table #sports_service_keys;
select distinct service_key
into #sports_service_keys
from dbarnett.v250_sports_rights_with_service_key;
commit;

CREATE HG INDEX idx1 ON #sports_service_keys (service_key);
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
into #epg_list
from sk_prod.Vespa_programme_schedule as a
left outer join #sports_service_keys as b
on a.service_key=b.service_key
where b.service_key is not null and broadcast_date between '2012-11-01' and '2013-10-31'
;
commit;
--select * from  sk_prod.Vespa_programme_schedule where programme_name is like '%UFC%'

CREATE HG INDEX idx1 ON #epg_list (service_key);
CREATE HG INDEX idx2 ON #epg_list (broadcast_date);
CREATE HG INDEX idx3 ON #epg_list (dk_programme_instance_dim);
commit;
--select distinct genre_description , sub_genre_description from sk_prod.Vespa_programme_schedule order by  genre_description , sub_genre_description where programme_name like '%americas

--select distinct programme_name  from sk_prod.Vespa_programme_schedule  where sub_genre_description='Watersports' order by programme_name
---Join EPG Data to Rights data by day and serice_key
drop table dbarnett.v250_sports_rights_with_possible_matches;
select a.*
,b.programme_name
,b.genre_description
,b.sub_genre_description
,b.dk_programme_instance_dim
,b.dk_programme_dim
,b.programme_instance_duration
,b.broadcast_start_date_time_local
,b.synopsis
into dbarnett.v250_sports_rights_with_possible_matches
from dbarnett.v250_sports_rights_with_service_key  as a
left outer join #epg_list as b
on a.service_key=b.service_key 
where a.broadcast_date between b.broadcast_date-1 and  b.broadcast_date+1
;
commit;

alter table dbarnett.v250_sports_rights_with_possible_matches add time_from_broadcast integer;

update dbarnett.v250_sports_rights_with_possible_matches
set time_from_broadcast=datediff(second,broadcast_start_date_time_local,broadcast_datetime) 
from dbarnett.v250_sports_rights_with_possible_matches
;

alter table dbarnett.v250_sports_rights_with_possible_matches add time_from_broadcast_absolute integer;

update dbarnett.v250_sports_rights_with_possible_matches
set time_from_broadcast_absolute=case when time_from_broadcast<0 then time_from_broadcast*-1 else time_from_broadcast end
from dbarnett.v250_sports_rights_with_possible_matches
;
commit;
--select top 500 * from dbarnett.v250_sports_rights_with_possible_matches;
---Create Rank to find nearest record per service key, sports right and programme_dim---

alter table dbarnett.v250_sports_rights_with_possible_matches add record_id integer identity;


--alter table dbarnett.v250_sports_rights_with_possible_matches add programme_rank integer;

--drop table  #rank;
select a.*
,RANK() OVER ( PARTITION BY service_key 

                ,broadcast_datetime
                
                ORDER BY time_from_broadcast_absolute ASC , record_id ) AS Rank
into #rank
from dbarnett.v250_sports_rights_with_possible_matches as a
;

commit;
/*
select top 1000 * from #rank order by service_key 

                ,broadcast_datetime
                ,programme_id , rank



select top 500 datediff(second,broadcast_start_date_time_local,broadcast_datetime) as time_from_bcast,* from  dbarnett.v250_sports_rights_with_possible_matches order by programme_id,time_from_bcast
*/

----Only include records where rank = 1;
drop table dbarnett.v250_sports_rights_epg_detail ;
select * 
into dbarnett.v250_sports_rights_epg_detail 
from #rank
where rank =1 and service_key <10000 --Get Rid of Anytime/On demand service keys where broadcast time matching not possible
;
commit;
--select top 100 * from dbarnett.v250_sports_rights_epg_detail where channel_name = 'Eurosport' ;
---Add on Eurosport Details---
--drop table dbarnett.v250_sports_rights_epg_detail_test;



---Get all Eurosport Programme Titles---
insert into dbarnett.v250_sports_rights_epg_detail
(select  'Eurosport' as channel_name

,case when programme_instance_name in (
'Live US Open Tennis'	
,'Tennis US Open 2012'	
,'US Open 2012 Murray vs Djokovic'	
,'US Open Final Murray vs Djokovic'	
,'US Open Tennis')	then 'US Open Tennis - Eurosport'
when programme_instance_name in (
'Live Cycling Tour de France'	
,'Live Tour de France Presentation'	
,'Tour de France 2014 Presentation') then	'Tour de France - Eurosport'
when programme_instance_name in ('Athletics World Championships','Live Athletics World Championships') 
then 	'IAAF World Athletics Championship - Eurosport'

when programme_instance_name in ('French Open Tennis'	
,'Live French Open Elena Baltacha..'	
,'Live French Open Laura Robson...'	
,'Live French Open Tennis' ) then 	'French Open Tennis - Eurosport'

when programme_instance_name in ('Australian Open Womens Final'	
,'Live Australian Open Doubles Final'	
,'Live Australian Open Womens Final'	
,'Live Tennis Australian Open'	
,'Live Tennis Djokovic vs Ferrer'	
,'Live Tennis Murray vs Berankis'	
,'Live Tennis Murray vs Chardy'	
,'Live Tennis Murray vs Djokovic'	
,'Live Tennis Murray vs Federer'	
,'Live Tennis Murray vs Simon'	
,'Live Tennis Murray vs Sousa'	
,'Live Tennis Robson vs Kvitova'	
,'Live Tennis Robson vs Stephens'	
,'Live Tennis Watson vs Pervak'	
,'Live Tennis Watson vs Radwanska'	
,'Tennis Australian Open'	
,'Tennis Australian Open 2012'	
,'Tennis Australian Open 2013'	
,'Tennis Australian Open Preview'	
,'Tennis Murray vs Djokovic')
then 'Australian Open Tennis - Eurosport'

when programme_instance_name in (
'Africa Cup of Nations'	
,'Africa Cup of Nations 2013'	
,'Live Africa Cup of Nations'	
,'Live Africa Cup of Nations 2013')	then 'Africa Cup of Nations - Eurosport'
when programme_instance_name in ('Cycling Tour of Britain') then 'Cycling Tour of Britain - Eurosport'
else 'Other' end as analysis_right

,case when programme_instance_name in (
'Live US Open Tennis'	
,'Tennis US Open 2012'	
,'US Open 2012 Murray vs Djokovic'	
,'US Open Final Murray vs Djokovic'	
,'US Open Tennis')	then 'US Open Tennis - Eurosport'
when programme_instance_name in (
'Live Cycling Tour de France'	
,'Live Tour de France Presentation'	
,'Tour de France 2014 Presentation') then	'Tour de France - Eurosport'
when programme_instance_name in ('Athletics World Championships','Live Athletics World Championships') 
then 	'IAAF World Athletics Championship - Eurosport'

when programme_instance_name in ('French Open Tennis'	
,'Live French Open Elena Baltacha..'	
,'Live French Open Laura Robson...'	
,'Live French Open Tennis' ) then 	'French Open Tennis - Eurosport'

when programme_instance_name in ('Australian Open Womens Final'	
,'Live Australian Open Doubles Final'	
,'Live Australian Open Womens Final'	
,'Live Tennis Australian Open'	
,'Live Tennis Djokovic vs Ferrer'	
,'Live Tennis Murray vs Berankis'	
,'Live Tennis Murray vs Chardy'	
,'Live Tennis Murray vs Djokovic'	
,'Live Tennis Murray vs Federer'	
,'Live Tennis Murray vs Simon'	
,'Live Tennis Murray vs Sousa'	
,'Live Tennis Robson vs Kvitova'	
,'Live Tennis Robson vs Stephens'	
,'Live Tennis Watson vs Pervak'	
,'Live Tennis Watson vs Radwanska'	
,'Tennis Australian Open'	
,'Tennis Australian Open 2012'	
,'Tennis Australian Open 2013'	
,'Tennis Australian Open Preview'	
,'Tennis Murray vs Djokovic')
then 'Australian Open Tennis - Eurosport'

when programme_instance_name in (
'Africa Cup of Nations'	
,'Africa Cup of Nations 2013'	
,'Live Africa Cup of Nations'	
,'Live Africa Cup of Nations 2013')	then 'Africa Cup of Nations - Eurosport'

when programme_instance_name in ('Cycling Tour of Britain') then 'Cycling Tour of Britain - Eurosport'
else 'Other' end as master_deal
,null as deal

,programme_instance_name as title

,null as broadcast_datetime_text
,broadcast_start_date_time_local as broadcast_datetime
,service_key
,null as programme_id
,cast (broadcast_start_date_time_local as date) as broadcast_date
,programme_instance_name as programme_name
,genre_description
,sub_genre_description
,dk_programme_instance_dim
,dk_programme_dim
,programme_instance_duration
,broadcast_start_date_time_local
,synopsis
,0 as time_from_broadcast
,0 as time_from_broadcast_absolute
,null as record_id
,1 as rank
--into #eurosport_progs
from sk_prod.Vespa_programme_schedule 
where service_key in (4004,1726,4009,1841) and cast (broadcast_start_date_time_local as date)  between '2012-11-01' and '2013-10-31' 
);
commit;

delete from dbarnett.v250_sports_rights_epg_detail where channel_name='Eurosport' and analysis_right='Other';
commit;
--select * from dbarnett.v250_sports_rights_epg_detail where channel_name='Eurosport';
--select * from dbarnett.v250_sports_rights_epg_detail_test
--select * from sk_prod.vespa_dp_prog_viewed
alter table dbarnett.v250_sports_rights_epg_detail add row_number integer identity;
select  * from dbarnett.v250_sports_rights_epg_detail;
output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Full EPG Data For matching\Full EPG Data v2.csv' format ascii;
commit;

---Add Index
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_detail (dk_programme_instance_dim);
commit;

---Add On Live Non Live Splits 
---v2 also has updated analysis right info (e.g., Day of Week for Champions League etc.,)

--drop table  dbarnett.v250_epg_live_non_live_lookup;
create table dbarnett.v250_epg_live_non_live_lookup
(row_number integer
,live integer
,analysis_right varchar(255)
)
;
commit;
input into dbarnett.v250_epg_live_non_live_lookup
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\Live and Non Live Rightsv2.csv' format ascii;

commit;

--select * from dbarnett.v250_epg_live_non_live_lookup where analysis_right = 'Australian Open Tennis - Eurosport'

---Create Analysis Table of EPG Data ---
drop table dbarnett.v250_sports_rights_epg_data_for_analysis;
select a.*
,b.live
,b.analysis_right as analysis_right_new
into dbarnett.v250_sports_rights_epg_data_for_analysis
from dbarnett.v250_sports_rights_epg_detail as a
left outer join dbarnett.v250_epg_live_non_live_lookup as b
on a.row_number = b.row_number 
where b.live is not null
;
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis where channel_name='Eurosport';

CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_data_for_analysis (dk_programme_instance_dim);
commit;

update dbarnett.v250_sports_rights_epg_data_for_analysis
set broadcast_datetime_text= cast(broadcast_start_date_time_local as varchar)
from  dbarnett.v250_sports_rights_epg_data_for_analysis
where broadcast_datetime_text is null
;
commit;



----Dedup by dk_programme_instance_dim---

select dk_programme_instance_dim
,min(row_number) as first_record_per_dk
into #first_rec_per_dk
from dbarnett.v250_sports_rights_epg_data_for_analysis
group by dk_programme_instance_dim
;
--select count(*) from dbarnett.v250_sports_rights_epg_data_for_analysis
--select * from #first_rec_per_dk;
commit;
CREATE HG INDEX idx1 ON #first_rec_per_dk(first_record_per_dk);
commit;
--alter table dbarnett.v250_sports_rights_epg_data_for_analysis delete first_dk_record;
alter table dbarnett.v250_sports_rights_epg_data_for_analysis add first_dk_record bigint;

update dbarnett.v250_sports_rights_epg_data_for_analysis
set first_dk_record=case when b.first_record_per_dk is not null then 1 else 0 end
from  dbarnett.v250_sports_rights_epg_data_for_analysis as a
left outer join #first_rec_per_dk as b
on a.row_number=b.first_record_per_dk
;
commit;
--select sum(first_dk_record) , count(*) from dbarnett.v250_sports_rights_epg_data_for_analysis

delete from dbarnett.v250_sports_rights_epg_data_for_analysis where first_dk_record=0; commit;

----Add on Eurosport Tour of Britain missed out in previous code----
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis;
insert into dbarnett.v250_sports_rights_epg_data_for_analysis
(
select  'Eurosport' as channel_name

,case when programme_instance_name in ('Cycling Tour of Britain') then 'Cycling Tour of Britain - Eurosport'
else 'Other' end as analysis_right

,'Cycling Tour of Britain - Eurosport'  as master_deal
,'Cycling Tour of Britain - Eurosport' as deal
,programme_instance_name as title
,null as broadcast_datetime_text
,broadcast_start_date_time_local as broadcast_datetime
,service_key
,null as programme_id
,cast (broadcast_start_date_time_local as date) as broadcast_date
,programme_instance_name as programme_name
,genre_description
,sub_genre_description
,dk_programme_instance_dim
,dk_programme_dim
,programme_instance_duration
,broadcast_start_date_time_local
,synopsis
,0 as time_from_broadcast
,0 as time_from_broadcast_absolute
,null as record_id
,dk_programme_instance_dim as rank
,dk_programme_instance_dim as row_number
,0 as live
,'Cycling Tour of Britain - Eurosport' as analysis_right_new
,1 as first_dk_record
--into #eurosport_tour_of_britain
from sk_prod.Vespa_programme_schedule 
where service_key in (4004,1726,4009,1841) and cast (broadcast_start_date_time_local as date)  between '2012-11-01' and '2013-10-31' 
and analysis_right= 'Cycling Tour of Britain - Eurosport'
)
;
commit;
--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis
--select distinct analysis_right_new from dbarnett.v250_sports_rights_epg_data_for_analysis order by analysis_right_new;

----Correct Sat Eve for PL---

update dbarnett.v250_sports_rights_epg_data_for_analysis
set analysis_right_new='Premier League Football - Sky Sports (Sat Night Live)'
where analysis_right_new='Premier League Football - Sky Sports (Sat Eve)'
;
commit;


-----Part 2 of EPG Data - to add on items missed during intial run---

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

----ENd of Part 2 of adding to EPG----

--alter table dbarnett.v250_sports_rights_epg_data_for_analysis delete analysis_right_grouped ;
alter table dbarnett.v250_sports_rights_epg_data_for_analysis add analysis_right_grouped varchar(80);

update dbarnett.v250_sports_rights_epg_data_for_analysis
set analysis_right_grouped = 
case when analysis_right_new in ('ECB Test Cricket Sky Sports'
,'ECB non-Test Cricket Sky Sports')
then 'ECB Cricket Sky Sports'
when analysis_right_new in (
'F1 (Practice Live)- BBC'
,'F1 (Qualifying Live)- BBC'
,'F1 (Race Live)- BBC'
,'F1 (non-Live)- BBC')
then 'F1 - BBC'
when analysis_right_new in 
('Formula One 2012-2018 - (Practice Live) Sky Sports'
,'Formula One 2012-2018 - (Qualifying Live) Sky Sports'
,'Formula One 2012-2018 - (Race Live) Sky Sports'
,'Formula One 2012-2018 - (non-Live) Sky Sports')
then 'F1 - Sky Sports'

when analysis_right_new in (
'Premier League Football - Sky Sports (MNF)'
,'Premier League Football - Sky Sports (Match Choice)'
,'Premier League Football - Sky Sports (Sat Lunchtime)'
,'Premier League Football - Sky Sports (Sat Night Live)'
,'Premier League Football - Sky Sports (Sun 4pm)'
,'Premier League Football - Sky Sports (Sun Lunchtime)'
,'Premier League Football - Sky Sports (non Live)'
,'Premier League Football - Sky Sports (other Live)')
then 'Premier League Football - Sky Sports'
when 
analysis_right_new
in ('England Friendlies (Football) - ITV'
,'England World Cup Qualifying (Away) - ITV'
,'England World Cup Qualifying (Home) - ITV')
then 'England Football Internationals - ITV'

WHEN analysis_right_new
in ('UEFA Champions League -  Sky Sports (Tue)'
,'UEFA Champions League -  Sky Sports (Wed)'
,'UEFA Champions League -  Sky Sports (non Live)'
,'UEFA Champions League -  Sky Sports (other Live)')
then 'UEFA Champions League -  Sky Sports'
else analysis_right_new end
from dbarnett.v250_sports_rights_epg_data_for_analysis
;
commit;



-----Part 3 of EPG Data - to add on items missed during intial run---

----Extra Rights to Add to Table----
--drop table dbarnett.v250_sports_rights_programme_lookup_part_3;
create table dbarnett.v250_sports_rights_programme_lookup_part_3
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
input into dbarnett.v250_sports_rights_programme_lookup_part_3
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\All Rights CSV for import (part3) v1.csv' format ascii;

commit;


alter table dbarnett.v250_sports_rights_programme_lookup_part_3 add broadcast_datetime datetime;

update dbarnett.v250_sports_rights_programme_lookup_part_3
set broadcast_datetime=cast(broadcast_datetime_text as datetime)
from dbarnett.v250_sports_rights_programme_lookup_part_3
;
commit;


drop table dbarnett.v250_sports_rights_with_service_key_part_3;
select a.*
,b.service_key
into dbarnett.v250_sports_rights_with_service_key_part_3
from dbarnett.v250_sports_rights_programme_lookup_part_3 as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.channel_name = b.channel_name
;
commit;
--select * from dbarnett.v250_sports_rights_with_service_key_part_3;
---Add on EPG Data--
--Add Record ID on match to EPG data--
alter table dbarnett.v250_sports_rights_with_service_key_part_3 add programme_id integer identity;
commit;

alter table dbarnett.v250_sports_rights_with_service_key_part_3 add broadcast_date date;
commit;

update dbarnett.v250_sports_rights_with_service_key_part_3
set broadcast_date=cast (broadcast_datetime as date)
from dbarnett.v250_sports_rights_with_service_key_part_3
;
commit;


select distinct service_key
into #sports_service_keys_part_3
from dbarnett.v250_sports_rights_with_service_key_part_3;
commit;

CREATE HG INDEX idx1 ON #sports_service_keys_part_3 (service_key);
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
into #epg_list_part_3
from sk_prod.Vespa_programme_schedule as a
left outer join #sports_service_keys_part_3 as b
on a.service_key=b.service_key
where b.service_key is not null and broadcast_date between '2012-11-01' and '2013-10-31'
;
commit;
--select * from  sk_prod.Vespa_programme_schedule where programme_name is like '%UFC%'

CREATE HG INDEX idx1 ON #epg_list_part_3 (service_key);
CREATE HG INDEX idx2 ON #epg_list_part_3 (broadcast_date);
CREATE HG INDEX idx3 ON #epg_list_part_3 (dk_programme_instance_dim);
commit;
--select distinct genre_description , sub_genre_description from sk_prod.Vespa_programme_schedule order by  genre_description , sub_genre_description where programme_name like '%americas

--select distinct programme_name  from sk_prod.Vespa_programme_schedule  where sub_genre_description='Watersports' order by programme_name
---Join EPG Data to Rights data by day and serice_key
drop table dbarnett.v250_sports_rights_with_possible_matches_part_3;
select a.*
,b.programme_name
,b.genre_description
,b.sub_genre_description
,b.dk_programme_instance_dim
,b.dk_programme_dim
,b.programme_instance_duration
,b.broadcast_start_date_time_local
,b.synopsis
into dbarnett.v250_sports_rights_with_possible_matches_part_3
from dbarnett.v250_sports_rights_with_service_key_part_3  as a
left outer join #epg_list_part_3 as b
on a.service_key=b.service_key 
where a.broadcast_date between b.broadcast_date-1 and  b.broadcast_date+1
;
commit;

alter table dbarnett.v250_sports_rights_with_possible_matches_part_3 add time_from_broadcast integer;

update dbarnett.v250_sports_rights_with_possible_matches_part_3
set time_from_broadcast=datediff(second,broadcast_start_date_time_local,broadcast_datetime) 
from dbarnett.v250_sports_rights_with_possible_matches_part_3
;

alter table dbarnett.v250_sports_rights_with_possible_matches_part_3 add time_from_broadcast_absolute integer;

update dbarnett.v250_sports_rights_with_possible_matches_part_3
set time_from_broadcast_absolute=case when time_from_broadcast<0 then time_from_broadcast*-1 else time_from_broadcast end
from dbarnett.v250_sports_rights_with_possible_matches_part_3
;
commit;
alter table dbarnett.v250_sports_rights_with_possible_matches_part_3 add record_id integer identity;

select a.*
,RANK() OVER ( PARTITION BY service_key 

                ,broadcast_datetime
                
                ORDER BY time_from_broadcast_absolute ASC , record_id ) AS Rank
into #rank_part_3
from dbarnett.v250_sports_rights_with_possible_matches_part_3 as a
;

commit;
drop table dbarnett.v250_sports_rights_epg_detail_part_3 ;
select * 
into dbarnett.v250_sports_rights_epg_detail_part_3 
from #rank_part_3
where rank =1 and service_key <10000 --Get Rid of Anytime/On demand service keys where broadcast time matching not possible
;
commit;

alter table dbarnett.v250_sports_rights_epg_detail_part_3 add row_number integer identity;
select  * from dbarnett.v250_sports_rights_epg_detail_part_3;
output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Full EPG Data For matching\Full EPG Data Part 3 v1.csv' format ascii;
commit;

---Add Index
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_detail_part_3 (dk_programme_instance_dim);
commit;

---Add On Live Non Live Splits 
---v2 also has updated analysis right info (e.g., Day of Week for Champions League etc.,)

--drop table  dbarnett.v250_epg_live_non_live_lookup_part_3;
create table dbarnett.v250_epg_live_non_live_lookup_part_3
(row_number integer
,live integer
,analysis_right varchar(255)
)
;
commit;
input into dbarnett.v250_epg_live_non_live_lookup_part_3
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\Live and Non Live Rights Part 3 v1.csv' format ascii;

commit;

--select * from dbarnett.v250_sports_rights_epg_detail_part_3 where title = 'Swansea v St Gallen'
--select 


--drop table dbarnett.v250_sports_rights_epg_data_for_analysis_part_3;
select a.*
,b.live
,b.analysis_right as analysis_right_new
into dbarnett.v250_sports_rights_epg_data_for_analysis_part_3
from dbarnett.v250_sports_rights_epg_detail_part_3 as a
left outer join dbarnett.v250_epg_live_non_live_lookup_part_3 as b
on a.row_number = b.row_number 
where b.live is not null
;
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis_part_3;

CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_data_for_analysis_part_3 (dk_programme_instance_dim);
commit;

update dbarnett.v250_sports_rights_epg_data_for_analysis_part_3
set row_number =row_number+200000
from dbarnett.v250_sports_rights_epg_data_for_analysis_part_3
;

alter table dbarnett.v250_sports_rights_epg_data_for_analysis_part_3 add first_dk_record integer default 1;
alter table dbarnett.v250_sports_rights_epg_data_for_analysis_part_3 add analysis_right_grouped varchar(80) default 'World Athletics Championship - More 4';
----Add Back to main table---


insert into  dbarnett.v250_sports_rights_epg_data_for_analysis
(select * from dbarnett.v250_sports_rights_epg_data_for_analysis_part_3)
; commit;

---End of Part 3 of Adding to EPG

---Create details of 1 record per programme (i.e., remove dupes at Service Key level - i.e., if prog on SD and HD only consider it once
--when looking at Share of Content metrics---

drop table dbarnett.v250_epg_list;
select a.service_key
,a.genre_description
,a.sub_genre_description
,a.dk_programme_instance_dim
,a.programme_instance_duration
,cast (a.broadcast_start_date_time_local as date) as broadcast_date
,a.broadcast_start_date_time_local
,b.channel_name
,case when b.channel_name in ('BBC 1','BBC 2','BBC HD','BBC Three') then 'BBC'
 when b.channel_name in ('BT Sport 1','BT Sport 2') then 'BT Sport'
when b.channel_name in ('Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 'Eurosport'
 when b.channel_name in ('Challenge','Channel 4','Channel 5','ESPN','ITV1','ITV4') then b.channel_name
 when b.channel_name in ('ESPN Classic','ESPN America') then 'ESPN'
when b.channel_name in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports F1') then 'Sky Sports'
when b.channel_name in ('Sky1','Sky2') then 'Sky 1 and Sky 2'
 else 'Other' end as channel_name_grouped
,case when c.live=1 then 1 else 0 end as live_status
,case when analysis_right_new is null then channel_name_grouped +' '+ a.sub_genre_description else analysis_right_new end as analysis_right_full

into dbarnett.v250_epg_list
from sk_prod.Vespa_programme_schedule as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as c
on a.dk_programme_instance_dim=c.dk_programme_instance_dim
where  ( a.service_key in (6000,4523
,3615
,1758
,3150
,3022
,3027
,3023
,3028
,3515
,4402
,3255
,3411
,5416
,3209
,4401
,3406
,5087
,3638
,3107
,5353
,3825
,2330
,2303
,4151
,3813
,2402
,3210
,5071
,5068
,3580
,4753
,3806
,1354
,1355
,6753
,5703
,5708
,3645
,2156
,2006
,2012
,2018
,2011
,2072
,3544
,2061
,3208
,4100
,3719
,5905
,1873
,2611
,4205
,3260
,5530
,5609
,3417
,3653
,3625
,3627
,3752
,2552
,4077
,1371
,2020
,2019
,4610
,3617
,3352
,3211
,4547
,2202
,1626
,1830
,3403
,4102
,3505
,3407
,1757
,4420
,4541
,3815
,1448
,4130
,6273
,4505
,5610
,3611
,5337
,5602
,3714
,2510
,1813
,1872
,3410
,5741
,4215
,1825
,4034
,6233
,6231
,6232
,4115
,2306
,3012
,3830
,3632
,2401
,2407
,2408
,2403
,2406
,4548
,2405
,2409
,4071
,1881
,1887
,2522
,1884
,1843
,3777
,3609
,3618
,1370
,2612
,1360
,1628
,2302
,1151
,3141
,3639
,3221
,2142
,2121
,4604
,4004
,4009
,3101
,1627
,5165
,3781
,3590
,1357
,4560
,1305
,3010
,4407
,4110
,2304
,2308
,1874
,5706
,1875
,2619
,2301
,1894
,3001
,2413
,5740
,5900
,3916
,3641
,6240
,6260
,6272
,6391
,6532
,6533
,6534
,5707
,3354
,3359
,5761
,3357
,1853
,3386
,3656
,1858
,4262
,4216
,5070
,2609
,3732
,4089
,1877
,3682
,2603
,3541
,4007
,1879
,3340
,3735
,3708
,3516
,3831
,2501
,2508
,2509
,2507
,2516
,2506
,2521
,3508
,2512
,2503
,5715
,5285
,1834
,3731
,1847
,1806
,1821
,5521
,3616
,3147
,3356
,1857
,4340
,1846
,1849
,3510
,3914
,3409
,3800
,5500
,3258
,1832
,3750
,4263
,5311
,5952
,4350
,4201
,3415
,3646
,3000
,6761
,5907
,4105
,5915
,3636
,2325
,3412
,3915
,4001
,5608
,3353
,1251
,1252
,1256
,3213
,5701
,3631
,3630
,4409
,4210
,3601
,4551
,6758
,4933
,4015
,1001
,1752
,1753
,1412
,1812
,1002
,1818
,1816
,1808
,1815
,1811
,2201
,2203
,2207
,1838
,1404
,1409
,1807
,1701
,1301
,1302
,1333
,1322
,1306
,1314
,1402
,1833
,1430
,3215
,1350
,3709
,3358
,2601
,3613
,3612
,1772
,3608
,5300
,1771
,3603
,2711
,5712
,3408
,3206
,2505
,5605
,1372
,3811
,1253
,3805
,3547
,1802
,3525
,3935
,3780
,3812
,4644
,5607
,4266
,3643
,3751
,1255
,3351
,1805
,5882
,4410
,3251
,3104
,1842
,4360
,3720
,2502
,3531
,2511
,3108
,3810
,2617
,4550
,2305
,2608
,2607
,2606
) or c.record_id = 2297633)
   and (analysis_right_new is not null or a.genre_description='Sports')
and  broadcast_date between '2012-11-01' and '2013-10-31'
;
commit;

----Add an Analysis Right Grouped for Certain Sports e.g., Premier League/F1---
--select distinct analysis_right_full from dbarnett.v250_epg_list order by analysis_right_full;
--alter table 
--alter table dbarnett.v250_epg_list delete analysis_right_grouped
alter table dbarnett.v250_epg_list add analysis_right_grouped varchar(80);

update dbarnett.v250_epg_list
set analysis_right_grouped = 
case when analysis_right_full in ('ECB Test Cricket Sky Sports'
,'ECB non-Test Cricket Sky Sports')
then 'ECB Cricket Sky Sports'
when analysis_right_full in (
'F1 (Practice Live)- BBC'
,'F1 (Qualifying Live)- BBC'
,'F1 (Race Live)- BBC'
,'F1 (non-Live)- BBC')
then 'F1 - BBC'
when analysis_right_full in 
('Formula One 2012-2018 - (Practice Live) Sky Sports'
,'Formula One 2012-2018 - (Qualifying Live) Sky Sports'
,'Formula One 2012-2018 - (Race Live) Sky Sports'
,'Formula One 2012-2018 - (non-Live) Sky Sports')
then 'F1 - Sky Sports'

when analysis_right_full in (
'Premier League Football - Sky Sports (MNF)'
,'Premier League Football - Sky Sports (Match Choice)'
,'Premier League Football - Sky Sports (Sat Lunchtime)'
,'Premier League Football - Sky Sports (Sat Night Live)'
,'Premier League Football - Sky Sports (Sun 4pm)'
,'Premier League Football - Sky Sports (Sun Lunchtime)'
,'Premier League Football - Sky Sports (non Live)'
,'Premier League Football - Sky Sports (other Live)')
then 'Premier League Football - Sky Sports'
when 
analysis_right_full
in ('England Friendlies (Football) - ITV'
,'England World Cup Qualifying (Away) - ITV'
,'England World Cup Qualifying (Home) - ITV')
then 'England Football Internationals - ITV'

WHEN analysis_right_full
in ('UEFA Champions League -  Sky Sports (Tue)'
,'UEFA Champions League -  Sky Sports (Wed)'
,'UEFA Champions League -  Sky Sports (non Live)'
,'UEFA Champions League -  Sky Sports (other Live)')
then 'UEFA Champions League -  Sky Sports'

else analysis_right_full end
from dbarnett.v250_epg_list
;
commit;

---Append Grouped EPG metrics back on to table--
drop table dbarnett.v250_grouped_rights;
select * into dbarnett.v250_grouped_rights from dbarnett.v250_epg_list
where analysis_right_full<>analysis_right_grouped
;
commit;

--select * from dbarnett.v250_grouped_rights;

update dbarnett.v250_grouped_rights
set analysis_right_full=analysis_right_grouped
from dbarnett.v250_grouped_rights
;

commit;

insert into dbarnett.v250_epg_list
(select * from dbarnett.v250_grouped_rights)
;

commit;


CREATE HG INDEX idx1 ON dbarnett.v250_epg_list (dk_programme_instance_dim);


commit;
--select analysis_right_full, analysis_right_grouped from dbarnett.v250_epg_list group by analysis_right_full, analysis_right_grouped order by analysis_right_full, analysis_right_grouped

--select distinct analysis_right_full from dbarnett.v250_epg_list order by analysis_right_full

--select * from dbarnett.v250_sports_rights_epg_data_for_analysis where channel_name = 'Eurosport';

--select * from dbarnett.v250_sports_rights_epg_data_for_analysis where analysis_right_new = 'Australian Open Tennis - Eurosport';


--select * from dbarnett.v250_rights_deduped_by_channel where channel_name = 'Eurosport'
--select * from dbarnett.v250_rights_deduped_by_channel where analysis_right_new='NFL - BBC' order by broadcast_datetime_text;
--select * from dbarnett.v250_rights_broadcast_by_live_status where analysis_right='NFL - BBC' order by analysis_right, live;
drop table dbarnett.v250_rights_broadcast_by_live_status;
select analysis_right_full as analysis_right
,live_status as live
,broadcast_date
,sum(programme_instance_duration) as total_broadcast
,count(*) as programmes_broadcast
into dbarnett.v250_rights_broadcast_by_live_status
from dbarnett.v250_epg_list
group by  analysis_right
,live
,broadcast_date
;

drop table dbarnett.v250_rights_broadcast_overall;
select analysis_right_full as analysis_right
,broadcast_date
,sum(programme_instance_duration) as total_broadcast
,count(*) as programmes_broadcast
into dbarnett.v250_rights_broadcast_overall
from dbarnett.v250_epg_list
group by  analysis_right
,broadcast_date
;

commit;
--select count(*)  from dbarnett.v250_epg_list
--select * from dbarnett.v250_rights_broadcast_overall where analysis_right = 'ITV1 Darts'

--select distinct analysis_right from dbarnett.v250_rights_broadcast_overall order by analysis_right;
--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis
--select * from dbarnett.v250_rights_broadcast_by_live_status order by analysis_right , live;
--select top 100 * from dbarnett.v250_all_sports_programmes_viewed;

----Create Deduped version of Viewing Data (Has some dupes where same prog watched multiple days)

select account_number
,dk_programme_instance_dim
,sum(viewing_duration) as viewing_duration_total
,sum(viewing_events) as viewing_events_total
into dbarnett.v250_all_sports_programmes_viewed_deduped
from dbarnett.v250_all_sports_programmes_viewed
group by account_number
,dk_programme_instance_dim
;
commit;

alter table dbarnett.v250_all_sports_programmes_viewed_deduped add service_key bigint;
alter table dbarnett.v250_all_sports_programmes_viewed_deduped add sub_genre_description varchar(40);
alter table dbarnett.v250_all_sports_programmes_viewed_deduped add broadcast_date date;
--select top 100 * from sk_prod.Vespa_programme_schedule
update dbarnett.v250_all_sports_programmes_viewed_deduped
set service_key=b.service_key
,sub_genre_description=b.sub_genre_description
,broadcast_date=cast(b.broadcast_start_date_time_local as date)
from dbarnett.v250_all_sports_programmes_viewed_deduped as a
left outer join  sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;



--select max(len( sub_genre_description))  from  sk_prod.Vespa_programme_schedule
--Add Channel Name--
--alter table dbarnett.v250_all_sports_programmes_viewed_deduped add channel_name varchar(40);

update dbarnett.v250_all_sports_programmes_viewed_deduped
set channel_name=b.channel_name
from dbarnett.v250_all_sports_programmes_viewed_deduped as a
left outer join  v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
;
commit;
--select * from dbarnett.v250_all_sports_programmes_viewed_deduped where channel_name is null
---Group Channels Together e.g., Sky Sports---

update dbarnett.v250_all_sports_programmes_viewed_deduped
set channel_name= case when channel_name in ('BBC 1','BBC 2','BBC HD','BBC Three') then 'BBC'
 when channel_name in ('BT Sport 1','BT Sport 2') then 'BT Sport'
when channel_name in ('Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 'Eurosport'
 when channel_name in ('Challenge','Channel 4','Channel 5','ESPN','ITV1','ITV4') then channel_name
 when channel_name in ('ESPN Classic','ESPN America') then 'ESPN'
when channel_name in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports F1') then 'Sky Sports'
when channel_name in ('Sky1','Sky2') then 'Sky 1 and Sky 2'

 else 'Other' end
from dbarnett.v250_all_sports_programmes_viewed_deduped 
commit;


--select distinct channel_name from dbarnett.v250_all_sports_programmes_viewed_deduped  order by channel_name;
--select distinct channel_name ,sum(viewing_duration_total from dbarnett.v250_all_sports_programmes_viewed_deduped  order by channel_name;
--select account_number+' '+channel_name  from dbarnett.v250_all_sports_programmes_viewed_deduped  ;
--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis
--select distinct analysis_right_grouped  from dbarnett.v250_sports_rights_epg_data_for_analysis order by analysis_right_grouped
-----Uses Viewing Data (dbarnett.v250_all_sports_programmes_viewed) Taken From v250 - Genre Level Sports Analysis v01.sql---
--select top 100 * from dbarnett.v250_all_sports_programmes_viewed;
--select account_number , dk_programme_instance_dim , count(*) as records from dbarnett.v250_all_sports_programmes_viewed group by account_number , dk_programme_instance_dim  order by records desc
--select distinct channel_name from dbarnett.v250_all_sports_programmes_viewed as a left  order by channel_name;
--drop table #rights_viewing_by_account;
--
select account_number
,case when analysis_right_new is null then a.channel_name +' '+ a.sub_genre_description else analysis_right_new end as analysis_right_full
,live
,a.broadcast_date
,sum(viewing_duration_total) as total_seconds_viewed
,count(distinct a.dk_programme_instance_dim) as programmes_viewed
,sum(viewing_events_total) as total_viewing_events

,sum(case when a.sub_genre_description in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=600 then 1 
when a.sub_genre_description not in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=900 then 1  
else 0 end) as programmes_viewed_over_threshold_value

into #rights_viewing_by_account
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
--where account_number is not null
group by account_number
,analysis_right_full
,live
,a.broadcast_date
;
commit;

---Summarise by Account by Right by Live/non Live---
drop table dbarnett.v250_sports_rights_viewed_by_right_and_live_status;
select account_number
,analysis_right_full as analysis_right
,live
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_and_live_status
from #rights_viewing_by_account
group by account_number
,analysis_right
,live
;
commit;


---Summarise by Account Overall---
drop table dbarnett.v250_sports_rights_viewed_by_right_overall
select account_number
,analysis_right_full as analysis_right
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_overall
from #rights_viewing_by_account
group by account_number
,analysis_right
;
commit;


----repeat for Grouped areas of analysis----
--drop table #rights_viewing_by_account_grouped;
select account_number
,case when analysis_right_grouped is null then a.channel_name +' '+ a.sub_genre_description else analysis_right_grouped end as analysis_right_full
,live
,a.broadcast_date
,sum(viewing_duration_total) as total_seconds_viewed
,count(distinct a.dk_programme_instance_dim) as programmes_viewed
,sum(viewing_events_total) as total_viewing_events

,sum(case when a.sub_genre_description in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=600 then 1 
when a.sub_genre_description not in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=900 then 1  
else 0 end) as programmes_viewed_over_threshold_value

into #rights_viewing_by_account_grouped
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where analysis_right_grouped in ('ECB Cricket Sky Sports'
,'England Football Internationals - ITV'
,'F1 - BBC'
,'F1 - Sky Sports'
,'Premier League Football - Sky Sports'
,'UEFA Champions League -  Sky Sports')
group by account_number
,analysis_right_full
,live
,a.broadcast_date
;
commit;

---Summarise by Account by Right by Live/non Live---
drop table dbarnett.v250_sports_rights_viewed_by_right_and_live_status_grouped;
select account_number
,analysis_right_full as analysis_right
,live
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_and_live_status_grouped
from #rights_viewing_by_account_grouped
group by account_number
,analysis_right
,live
;
commit;


---Summarise by Account Overall---
drop table dbarnett.v250_sports_rights_viewed_by_right_overall_grouped
select account_number
,analysis_right_full as analysis_right
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_overall_grouped
from #rights_viewing_by_account_grouped
group by account_number
,analysis_right
;
commit;

---Add Regular and Grouped Viewing Activity into a single Table---


insert into  dbarnett.v250_sports_rights_viewed_by_right_overall
(select * from dbarnett.v250_sports_rights_viewed_by_right_overall_grouped)
; commit;


insert into  dbarnett.v250_sports_rights_viewed_by_right_and_live_status
(select * from dbarnett.v250_sports_rights_viewed_by_right_and_live_status_grouped)
; commit;



--SELECT TOP 500 * FROM dbarnett.v250_sports_rights_viewed_by_right_overall_grouped
--SELECT analysis_right , sum(total_duration_viewed_seconds) FROM dbarnett.v250_sports_rights_viewed_by_right_overall group by analysis_right order by analysis_right;



----Create Master Account Table based on Total Days Viewing and Total Sports/Overall Viewing---
--dbarnett.v250_days_viewed_by_account
--dbarnett.v250_daily_viewing_duration

-- from V250 - Genre Level Sports Analysis (Total Viewing).sql
--select top 500 * from dbarnett.v250_unannualised_right_activity_by_live_non_live;
--select top 500 * from dbarnett.v250_daily_viewing_duration;

select account_number
,sum(b.viewing_duration) as total_viewing_duration
into #total_viewing_duration
from dbarnett.v250_daily_viewing_duration as b
group by account_number
;
commit;

CREATE HG INDEX idx1 ON #total_viewing_duration (account_number);
commit;

select account_number
,sum(c.viewing_duration) as total_viewing_duration
into #total_viewing_duration_sports
from dbarnett.v250_all_sports_programmes_viewed as c
group by account_number
;
commit;

CREATE HG INDEX idx1 ON #total_viewing_duration_sports (account_number);
commit;
--select count(*) from dbarnett.v250_master_account_list;
--drop table dbarnett.v250_master_account_list;
select a.account_number
,a.total_days_with_viewing
,case when b.total_viewing_duration is null then 0 else b.total_viewing_duration end as total_viewing_duration_all
,case when c.total_viewing_duration is null then 0 else c.total_viewing_duration end as total_viewing_duration_sports
into dbarnett.v250_master_account_list
from dbarnett.v250_days_viewed_by_account as a
left outer join #total_viewing_duration as b
on a.account_number = b.account_number
left outer join #total_viewing_duration_sports as c
on a.account_number = c.account_number
;
commit;


------





--select analysis_right,sum(total_duration_viewed_seconds) as tot_dur from dbarnett.v250_sports_rights_viewed_by_right_overall group by analysis_right order by analysis_right;

--select top 100 * from dbarnett.v250_sports_rights_viewed_by_right_overall;
--select distinct analysis_right from dbarnett.v250_sports_rights_viewed_by_right_overall order by analysis_right;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_viewed_by_right_overall (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_sports_rights_viewed_by_right_overall (analysis_right);
commit;
----Convert Activity to one record per account--
--dbarnett.v250_sports_rights_viewed_by_right_overall
--Part 1 - Overall
drop table dbarnett.v250_unannualised_right_activity;
select account_number

,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then broadcast_days_viewed else 0 end) 
as AFCEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then broadcast_days_viewed else 0 end) 
as AFCITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Americas Cup - BBC' then broadcast_days_viewed else 0 end) 
as AMCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then broadcast_days_viewed else 0 end) 
as AHCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Football - BT Sport' then broadcast_days_viewed else 0 end) 
as AUFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Open Tennis - BBC' then broadcast_days_viewed else 0 end) 
as AOTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as AOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Aviva Premiership - ESPN' then broadcast_days_viewed else 0 end) 
as AVPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC American Football' then broadcast_days_viewed else 0 end) 
as AFBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Athletics' then broadcast_days_viewed else 0 end) 
as ATHBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Boxing' then broadcast_days_viewed else 0 end) 
as BOXBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Darts' then broadcast_days_viewed else 0 end) 
as DRTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Football' then broadcast_days_viewed else 0 end) 
as FOOTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Golf' then broadcast_days_viewed else 0 end) 
as GOLFBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Rugby' then broadcast_days_viewed else 0 end) 
as RUGBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Tennis' then broadcast_days_viewed else 0 end) 
as TENBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Unknown' then broadcast_days_viewed else 0 end) 
as UNKBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Watersports' then broadcast_days_viewed else 0 end) 
as WATBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Wintersports' then broadcast_days_viewed else 0 end) 
as WINBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Boxing  - Channel 5' then broadcast_days_viewed else 0 end) 
as BOXCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as BOXMSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Brazil Football - BT Sport' then broadcast_days_viewed else 0 end) 
as BFTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as BILSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='British Open Golf - BBC' then broadcast_days_viewed else 0 end) 
as BOGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport American Football' then broadcast_days_viewed else 0 end) 
as AFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Athletics' then broadcast_days_viewed else 0 end) 
as ATHBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Baseball' then broadcast_days_viewed else 0 end) 
as BASEBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Basketball' then broadcast_days_viewed else 0 end) 
as BASKBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Boxing' then broadcast_days_viewed else 0 end) 
as BOXBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Cricket' then broadcast_days_viewed else 0 end) 
as CRIBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Extreme' then broadcast_days_viewed else 0 end) 
as EXTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Football' then broadcast_days_viewed else 0 end) 
as FOOTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Rugby' then broadcast_days_viewed else 0 end) 
as RUGBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Tennis' then broadcast_days_viewed else 0 end) 
as TENBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Unknown' then broadcast_days_viewed else 0 end) 
as UNKBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Wintersports' then broadcast_days_viewed else 0 end) 
as WINBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Bundesliga - BT Sport' then broadcast_days_viewed else 0 end) 
as BUNBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Bundesliga- ESPN' then broadcast_days_viewed else 0 end) 
as BUNESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Darts' then broadcast_days_viewed else 0 end) 
as DRTCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Extreme' then broadcast_days_viewed else 0 end) 
as EXTCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Unknown' then broadcast_days_viewed else 0 end) 
as UNKCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Wrestling' then broadcast_days_viewed else 0 end) 
as WRECHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Champions League - ITV' then broadcast_days_viewed else 0 end) 
as CHLITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as ICCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 American Football' then broadcast_days_viewed else 0 end) 
as AMCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Equestrian' then broadcast_days_viewed else 0 end) 
as EQUCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Football' then broadcast_days_viewed else 0 end) 
as FOOTCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Racing' then broadcast_days_viewed else 0 end) 
as RACCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Watersports' then broadcast_days_viewed else 0 end) 
as WATCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Wintersports' then broadcast_days_viewed else 0 end) 
as WINCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Boxing' then broadcast_days_viewed else 0 end) 
as BOXOCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Cricket' then broadcast_days_viewed else 0 end) 
as CRICH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Wrestling' then broadcast_days_viewed else 0 end) 
as WRECH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then broadcast_days_viewed else 0 end) 
as CHELCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Community Shield - ITV' then broadcast_days_viewed else 0 end) 
as CMSITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Confederations Cup - BBC' then broadcast_days_viewed else 0 end) 
as CONCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Conference - BT Sport' then broadcast_days_viewed else 0 end) 
as CONFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then broadcast_days_viewed else 0 end) 
as CLVITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then broadcast_days_viewed else 0 end) 
as CUCISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then broadcast_days_viewed else 0 end) 
as CTBEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then broadcast_days_viewed else 0 end) 
as CTCITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Derby - Channel 4' then broadcast_days_viewed else 0 end) 
as DERCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then broadcast_days_viewed else 0 end) 
as ECBHCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as GECRSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Football Internationals - ITV' then broadcast_days_viewed else 0 end) 
as GENGITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then broadcast_days_viewed else 0 end) 
as EFRITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ENRSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then broadcast_days_viewed else 0 end) 
as EWQAITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then broadcast_days_viewed else 0 end) 
as EWQHITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN American Football' then broadcast_days_viewed else 0 end) 
as AMESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Athletics' then broadcast_days_viewed else 0 end) 
as ATHESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Baseball' then broadcast_days_viewed else 0 end) 
as BASEESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Basketball' then broadcast_days_viewed else 0 end) 
as BASKESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Boxing' then broadcast_days_viewed else 0 end) 
as BOXESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Cricket' then broadcast_days_viewed else 0 end) 
as CRIESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Darts' then broadcast_days_viewed else 0 end) 
as DARTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Equestrian' then broadcast_days_viewed else 0 end) 
as EQUESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Extreme' then broadcast_days_viewed else 0 end) 
as EXTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Football' then broadcast_days_viewed else 0 end) 
as FOOTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Golf' then broadcast_days_viewed else 0 end) 
as GOLFESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Racing' then broadcast_days_viewed else 0 end) 
as RACESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Rugby' then broadcast_days_viewed else 0 end) 
as RUGESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Tennis' then broadcast_days_viewed else 0 end) 
as TENESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Unknown' then broadcast_days_viewed else 0 end) 
as UNKESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Watersports' then broadcast_days_viewed else 0 end) 
as WATESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Wintersports' then broadcast_days_viewed else 0 end) 
as WINESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Wrestling' then broadcast_days_viewed else 0 end) 
as WREESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - BT Sport' then broadcast_days_viewed else 0 end) 
as ELBTSP_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - ESPN' then broadcast_days_viewed else 0 end) 
as ELESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - ITV' then broadcast_days_viewed else 0 end) 
as ELITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as ETGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport American Football' then broadcast_days_viewed else 0 end) 
as AMEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Athletics' then broadcast_days_viewed else 0 end) 
as ATHEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Baseball' then broadcast_days_viewed else 0 end) 
as BASEEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Basketball' then broadcast_days_viewed else 0 end) 
as BASKEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Boxing' then broadcast_days_viewed else 0 end) 
as BOXEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Cricket' then broadcast_days_viewed else 0 end) 
as CRIEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Darts' then broadcast_days_viewed else 0 end) 
as DARTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Extreme' then broadcast_days_viewed else 0 end) 
as EXTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Football' then broadcast_days_viewed else 0 end) 
as FOOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Golf' then broadcast_days_viewed else 0 end) 
as GOLFEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Racing' then broadcast_days_viewed else 0 end) 
as RACEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Tennis' then broadcast_days_viewed else 0 end) 
as TENEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Unknown' then broadcast_days_viewed else 0 end) 
as UNKEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Watersports' then broadcast_days_viewed else 0 end) 
as WATEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Wintersports' then broadcast_days_viewed else 0 end) 
as WINEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 - BBC' then broadcast_days_viewed else 0 end) 
as GF1BBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 - Sky Sports' then broadcast_days_viewed else 0 end) 
as GF1SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (non-Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1NBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1PBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1QBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Race Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1RBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='FA Cup - ESPN' then broadcast_days_viewed else 0 end) 
as FACESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='FA Cup - ITV' then broadcast_days_viewed else 0 end) 
as FACITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then broadcast_days_viewed else 0 end) 
as FLCCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then broadcast_days_viewed else 0 end) 
as FLOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1NSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1PSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1QSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1RSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='French Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as FOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='French Open Tennis - ITV' then broadcast_days_viewed else 0 end) 
as FOTITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Grand National - Channel 4' then broadcast_days_viewed else 0 end) 
as GDNCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then broadcast_days_viewed else 0 end) 
as HECSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as IRBSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then broadcast_days_viewed else 0 end) 
as WACEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then broadcast_days_viewed else 0 end) 
as IHCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='India Premier League - ITV' then broadcast_days_viewed else 0 end) 
as IPLITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='International Freindlies - ESPN' then broadcast_days_viewed else 0 end) 
as IFESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='International Friendlies - BT Sport' then broadcast_days_viewed else 0 end) 
as IFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Motor Sport' then broadcast_days_viewed else 0 end) 
as MOTSITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Cricket' then broadcast_days_viewed else 0 end) 
as CRIITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Darts' then broadcast_days_viewed else 0 end) 
as DARTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Tennis' then broadcast_days_viewed else 0 end) 
as TENITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Ligue 1 - BT Sport' then broadcast_days_viewed else 0 end) 
as L1BTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Ligue 1 - ESPN' then broadcast_days_viewed else 0 end) 
as L1ESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Match of the day - BBC' then broadcast_days_viewed else 0 end) 
as MOTDBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MROSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRSSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Moto GP BBC' then broadcast_days_viewed else 0 end) 
as MGPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='NBA - Sky Sports' then broadcast_days_viewed else 0 end) 
as NBASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL - BBC' then broadcast_days_viewed else 0 end) 
as NFLBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL - Channel 4' then broadcast_days_viewed else 0 end) 
as NFLCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NFLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NIFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Oaks - Channel 4' then broadcast_days_viewed else 0 end) 
as OAKCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other American Football' then broadcast_days_viewed else 0 end) 
as AMOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Athletics' then broadcast_days_viewed else 0 end) 
as ATHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Baseball' then broadcast_days_viewed else 0 end) 
as BASEOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Basketball' then broadcast_days_viewed else 0 end) 
as BASKOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Boxing' then broadcast_days_viewed else 0 end) 
as BOXOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Cricket' then broadcast_days_viewed else 0 end) 
as CRIOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Darts' then broadcast_days_viewed else 0 end) 
as DARTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Equestrian' then broadcast_days_viewed else 0 end) 
as EQUOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Extreme' then broadcast_days_viewed else 0 end) 
as EXTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Fishing' then broadcast_days_viewed else 0 end) 
as FSHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Football' then broadcast_days_viewed else 0 end) 
as FOOTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Golf' then broadcast_days_viewed else 0 end) 
as GOLFOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Racing' then broadcast_days_viewed else 0 end) 
as RACOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Rugby' then broadcast_days_viewed else 0 end) 
as RUGOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then broadcast_days_viewed else 0 end) 
as ORUGESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Snooker/Pool' then broadcast_days_viewed else 0 end) 
as OTHSNP_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Tennis' then broadcast_days_viewed else 0 end) 
as OTHTEN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Unknown' then broadcast_days_viewed else 0 end) 
as OTHUNK_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Watersports' then broadcast_days_viewed else 0 end) 
as OTHWAT_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Wintersports' then broadcast_days_viewed else 0 end) 
as OTHWIN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Wrestling' then broadcast_days_viewed else 0 end) 
as OTHWRE_Broadcast_Days_Viewed
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PGASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League - BT Sport' then broadcast_days_viewed else 0 end) 
as PLBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League - ESPN' then broadcast_days_viewed else 0 end) 
as PLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PLDSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports' then broadcast_days_viewed else 0 end) 
as GPLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then broadcast_days_viewed else 0 end) 
as PLMCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then broadcast_days_viewed else 0 end) 
as PLMNFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as PLNLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as PLOLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then broadcast_days_viewed else 0 end) 
as PLSNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then broadcast_days_viewed else 0 end) 
as PLS4SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSULSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then broadcast_days_viewed else 0 end) 
as PRUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ROISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Royal Ascot - Channel 4' then broadcast_days_viewed else 0 end) 
as RASCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then broadcast_days_viewed else 0 end) 
as RIEBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then broadcast_days_viewed else 0 end) 
as RIIBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then broadcast_days_viewed else 0 end) 
as RISBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then broadcast_days_viewed else 0 end) 
as RIWBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLCCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League - Sky Sports' then broadcast_days_viewed else 0 end) 
as RLGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLWCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SARUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SFASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Serie A - BT Sport' then broadcast_days_viewed else 0 end) 
as SABTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Serie A - ESPN' then broadcast_days_viewed else 0 end) 
as SAESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='SFL - ESPN' then broadcast_days_viewed else 0 end) 
as SFLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Six Nations - BBC' then broadcast_days_viewed else 0 end) 
as SNRBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then broadcast_days_viewed else 0 end) 
as BOXS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then broadcast_days_viewed else 0 end) 
as FOOTS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then broadcast_days_viewed else 0 end) 
as UNKS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then broadcast_days_viewed else 0 end) 
as WRES12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports American Football' then broadcast_days_viewed else 0 end) 
as AMSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Athletics' then broadcast_days_viewed else 0 end) 
as ATHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Baseball' then broadcast_days_viewed else 0 end) 
as BASESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Basketball' then broadcast_days_viewed else 0 end) 
as BASKSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Boxing' then broadcast_days_viewed else 0 end) 
as BOXSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Cricket' then broadcast_days_viewed else 0 end) 
as CRISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Darts' then broadcast_days_viewed else 0 end) 
as DARTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Equestrian' then broadcast_days_viewed else 0 end) 
as EQUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Extreme' then broadcast_days_viewed else 0 end) 
as EXTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Fishing' then broadcast_days_viewed else 0 end) 
as FISHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Football' then broadcast_days_viewed else 0 end) 
as FOOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Golf' then broadcast_days_viewed else 0 end) 
as GOLFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Racing' then broadcast_days_viewed else 0 end) 
as RACSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Rugby' then broadcast_days_viewed else 0 end) 
as RUGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Tennis' then broadcast_days_viewed else 0 end) 
as TENSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Unknown' then broadcast_days_viewed else 0 end) 
as UNKSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Watersports' then broadcast_days_viewed else 0 end) 
as WATSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Wintersports' then broadcast_days_viewed else 0 end) 
as WINSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Wrestling' then broadcast_days_viewed else 0 end) 
as WRESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as SOLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as SACSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPFL - BT Sport' then broadcast_days_viewed else 0 end) 
as SPFLBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPL - ESPN' then broadcast_days_viewed else 0 end) 
as SPLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPL - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then broadcast_days_viewed else 0 end) 
as SP5SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='The boat race - BBC' then broadcast_days_viewed else 0 end) 
as BTRBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='The football league show - BBC' then broadcast_days_viewed else 0 end) 
as FLSBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='The Masters Golf - BBC' then broadcast_days_viewed else 0 end) 
as MGBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='TNA Wrestling Challenge' then broadcast_days_viewed else 0 end) 
as TNACHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Tour de France - Eurosport' then broadcast_days_viewed else 0 end) 
as TDFEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Tour de France - ITV' then broadcast_days_viewed else 0 end) 
as TDFITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as USMGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as USOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then broadcast_days_viewed else 0 end) 
as USOGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then broadcast_days_viewed else 0 end) 
as CLASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as CLNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as CLOSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then broadcast_days_viewed else 0 end) 
as CLTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then broadcast_days_viewed else 0 end) 
as CLWSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='US Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as USOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='USA Football - BT Sport' then broadcast_days_viewed else 0 end) 
as USFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then broadcast_days_viewed else 0 end) 
as USPGASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='WCQ - ESPN' then broadcast_days_viewed else 0 end) 
as WCQESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as WIFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Wimbledon - BBC' then broadcast_days_viewed else 0 end) 
as WIMBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Athletics Championship - More 4' then broadcast_days_viewed else 0 end) 
as WACMR4_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Club Championship - BBC' then broadcast_days_viewed else 0 end) 
as WCLBBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then broadcast_days_viewed else 0 end) 
as WCQBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then broadcast_days_viewed else 0 end) 
as WDCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World snooker championship - BBC' then broadcast_days_viewed else 0 end) 
as WSCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='WWE Sky 1 and 2' then broadcast_days_viewed else 0 end) 
as WWES12_Broadcast_Days_Viewed
,sum(case when analysis_right ='WWE Sky Sports' then broadcast_days_viewed else 0 end) 
as WWESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AFCEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then total_duration_viewed_seconds else 0 end) 
as AFCITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Americas Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as AMCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as AHCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as AUFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Open Tennis - BBC' then total_duration_viewed_seconds else 0 end) 
as AOTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Aviva Premiership - ESPN' then total_duration_viewed_seconds else 0 end) 
as AVPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC American Football' then total_duration_viewed_seconds else 0 end) 
as AFBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Darts' then total_duration_viewed_seconds else 0 end) 
as DRTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Watersports' then total_duration_viewed_seconds else 0 end) 
as WATBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Boxing  - Channel 5' then total_duration_viewed_seconds else 0 end) 
as BOXCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BOXMSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Brazil Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BFTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BILSS_Total_Seconds_Viewed
,sum(case when analysis_right ='British Open Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as BOGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport American Football' then total_duration_viewed_seconds else 0 end) 
as AFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Bundesliga - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BUNBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Bundesliga- ESPN' then total_duration_viewed_seconds else 0 end) 
as BUNESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Darts' then total_duration_viewed_seconds else 0 end) 
as DRTCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Champions League - ITV' then total_duration_viewed_seconds else 0 end) 
as CHLITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ICCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 American Football' then total_duration_viewed_seconds else 0 end) 
as AMCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Racing' then total_duration_viewed_seconds else 0 end) 
as RACCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Watersports' then total_duration_viewed_seconds else 0 end) 
as WATCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRICH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then total_duration_viewed_seconds else 0 end) 
as CHELCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Community Shield - ITV' then total_duration_viewed_seconds else 0 end) 
as CMSITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Confederations Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as CONCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Conference - BT Sport' then total_duration_viewed_seconds else 0 end) 
as CONFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then total_duration_viewed_seconds else 0 end) 
as CLVITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CUCISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then total_duration_viewed_seconds else 0 end) 
as CTBEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then total_duration_viewed_seconds else 0 end) 
as CTCITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Derby - Channel 4' then total_duration_viewed_seconds else 0 end) 
as DERCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then total_duration_viewed_seconds else 0 end) 
as ECBHCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GECRSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='England Football Internationals - ITV' then total_duration_viewed_seconds else 0 end) 
as GENGITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then total_duration_viewed_seconds else 0 end) 
as EFRITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ENRSS_Total_Seconds_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQAITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQHITV_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN American Football' then total_duration_viewed_seconds else 0 end) 
as AMESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Darts' then total_duration_viewed_seconds else 0 end) 
as DARTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Football' then total_duration_viewed_seconds else 0 end) 
as FOOTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Racing' then total_duration_viewed_seconds else 0 end) 
as RACESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Tennis' then total_duration_viewed_seconds else 0 end) 
as TENESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Watersports' then total_duration_viewed_seconds else 0 end) 
as WATESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Wrestling' then total_duration_viewed_seconds else 0 end) 
as WREESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as ELBTSP_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - ESPN' then total_duration_viewed_seconds else 0 end) 
as ELESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - ITV' then total_duration_viewed_seconds else 0 end) 
as ELITV_Total_Seconds_Viewed
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ETGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport American Football' then total_duration_viewed_seconds else 0 end) 
as AMEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Darts' then total_duration_viewed_seconds else 0 end) 
as DARTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Racing' then total_duration_viewed_seconds else 0 end) 
as RACEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Watersports' then total_duration_viewed_seconds else 0 end) 
as WATEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 - BBC' then total_duration_viewed_seconds else 0 end) 
as GF1BBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GF1SS_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (non-Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1NBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1PBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1QBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Race Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1RBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='FA Cup - ESPN' then total_duration_viewed_seconds else 0 end) 
as FACESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='FA Cup - ITV' then total_duration_viewed_seconds else 0 end) 
as FACITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_duration_viewed_seconds else 0 end) 
as FLCCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_duration_viewed_seconds else 0 end) 
as FLOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1NSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1PSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1QSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1RSS_Total_Seconds_Viewed
,sum(case when analysis_right ='French Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as FOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='French Open Tennis - ITV' then total_duration_viewed_seconds else 0 end) 
as FOTITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Grand National - Channel 4' then total_duration_viewed_seconds else 0 end) 
as GDNCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as HECSS_Total_Seconds_Viewed
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IRBSS_Total_Seconds_Viewed
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then total_duration_viewed_seconds else 0 end) 
as WACEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IHCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='India Premier League - ITV' then total_duration_viewed_seconds else 0 end) 
as IPLITV_Total_Seconds_Viewed
,sum(case when analysis_right ='International Freindlies - ESPN' then total_duration_viewed_seconds else 0 end) 
as IFESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='International Friendlies - BT Sport' then total_duration_viewed_seconds else 0 end) 
as IFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MOTSITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Darts' then total_duration_viewed_seconds else 0 end) 
as DARTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Tennis' then total_duration_viewed_seconds else 0 end) 
as TENITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='Ligue 1 - BT Sport' then total_duration_viewed_seconds else 0 end) 
as L1BTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Ligue 1 - ESPN' then total_duration_viewed_seconds else 0 end) 
as L1ESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Match of the day - BBC' then total_duration_viewed_seconds else 0 end) 
as MOTDBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MROSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRSSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Moto GP BBC' then total_duration_viewed_seconds else 0 end) 
as MGPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='NBA - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NBASS_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL - BBC' then total_duration_viewed_seconds else 0 end) 
as NFLBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL - Channel 4' then total_duration_viewed_seconds else 0 end) 
as NFLCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NFLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NIFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Oaks - Channel 4' then total_duration_viewed_seconds else 0 end) 
as OAKCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Other American Football' then total_duration_viewed_seconds else 0 end) 
as AMOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Darts' then total_duration_viewed_seconds else 0 end) 
as DARTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Fishing' then total_duration_viewed_seconds else 0 end) 
as FSHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Football' then total_duration_viewed_seconds else 0 end) 
as FOOTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Racing' then total_duration_viewed_seconds else 0 end) 
as RACOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then total_duration_viewed_seconds else 0 end) 
as ORUGESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as OTHSNP_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Tennis' then total_duration_viewed_seconds else 0 end) 
as OTHTEN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Unknown' then total_duration_viewed_seconds else 0 end) 
as OTHUNK_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Watersports' then total_duration_viewed_seconds else 0 end) 
as OTHWAT_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Wintersports' then total_duration_viewed_seconds else 0 end) 
as OTHWIN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Wrestling' then total_duration_viewed_seconds else 0 end) 
as OTHWRE_Total_Seconds_Viewed
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PGASS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as PLBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League - ESPN' then total_duration_viewed_seconds else 0 end) 
as PLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PLDSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GPLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_duration_viewed_seconds else 0 end) 
as PLMCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then total_duration_viewed_seconds else 0 end) 
as PLMNFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as PLNLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as PLOLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_duration_viewed_seconds else 0 end) 
as PLSNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_duration_viewed_seconds else 0 end) 
as PLS4SS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSULSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PRUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ROISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Royal Ascot - Channel 4' then total_duration_viewed_seconds else 0 end) 
as RASCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIEBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIIBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RISBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIWBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLCCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as RLGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLWCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SARUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SFASS_Total_Seconds_Viewed
,sum(case when analysis_right ='Serie A - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SABTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Serie A - ESPN' then total_duration_viewed_seconds else 0 end) 
as SAESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='SFL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SFLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Six Nations - BBC' then total_duration_viewed_seconds else 0 end) 
as SNRBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRES12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports American Football' then total_duration_viewed_seconds else 0 end) 
as AMSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Baseball' then total_duration_viewed_seconds else 0 end) 
as BASESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Cricket' then total_duration_viewed_seconds else 0 end) 
as CRISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Darts' then total_duration_viewed_seconds else 0 end) 
as DARTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Fishing' then total_duration_viewed_seconds else 0 end) 
as FISHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Football' then total_duration_viewed_seconds else 0 end) 
as FOOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Racing' then total_duration_viewed_seconds else 0 end) 
as RACSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Tennis' then total_duration_viewed_seconds else 0 end) 
as TENSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Watersports' then total_duration_viewed_seconds else 0 end) 
as WATSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SOLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SACSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='SPFL - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SPFLBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='SPL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SPLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='SPL - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SP5SS_Total_Seconds_Viewed
,sum(case when analysis_right ='The boat race - BBC' then total_duration_viewed_seconds else 0 end) 
as BTRBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='The football league show - BBC' then total_duration_viewed_seconds else 0 end) 
as FLSBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='The Masters Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as MGBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='TNA Wrestling Challenge' then total_duration_viewed_seconds else 0 end) 
as TNACHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Tour de France - Eurosport' then total_duration_viewed_seconds else 0 end) 
as TDFEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Tour de France - ITV' then total_duration_viewed_seconds else 0 end) 
as TDFITV_Total_Seconds_Viewed
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USMGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CLASS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as CLNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as CLOSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_duration_viewed_seconds else 0 end) 
as CLTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_duration_viewed_seconds else 0 end) 
as CLWSS_Total_Seconds_Viewed
,sum(case when analysis_right ='US Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as USOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='USA Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as USFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USPGASS_Total_Seconds_Viewed
,sum(case when analysis_right ='WCQ - ESPN' then total_duration_viewed_seconds else 0 end) 
as WCQESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WIFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Wimbledon - BBC' then total_duration_viewed_seconds else 0 end) 
as WIMBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='World Athletics Championship - More 4' then total_duration_viewed_seconds else 0 end) 
as WACMR4_Total_Seconds_Viewed
,sum(case when analysis_right ='World Club Championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WCLBBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then total_duration_viewed_seconds else 0 end) 
as WCQBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WDCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='World snooker championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WSCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='WWE Sky 1 and 2' then total_duration_viewed_seconds else 0 end) 
as WWES12_Total_Seconds_Viewed
,sum(case when analysis_right ='WWE Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WWESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AFCEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as AFCITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Americas Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AMCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as AHCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as AUFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Open Tennis - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AOTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Aviva Premiership - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as AVPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Boxing  - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BOXMSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Brazil Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BFTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BILSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='British Open Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BOGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Bundesliga - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BUNBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Bundesliga- ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as BUNESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Champions League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CHLITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ICCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRICH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as CHELCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Community Shield - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CMSITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Confederations Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as CONCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Conference - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as CONFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CLVITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CUCISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as CTBEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then total_programmes_viewed_over_threshold else 0 end) 
as CTCITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Derby - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as DERCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as ECBHCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GECRSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Football Internationals - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as GENGITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EFRITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ENRSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQAITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQHITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WREESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as ELBTSP_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ELESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as ELITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ETGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as GF1BBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GF1SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (non-Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1NBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1PBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1QBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Race Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1RBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='FA Cup - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as FACESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='FA Cup - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FACITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_programmes_viewed_over_threshold else 0 end) 
as FLCCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_programmes_viewed_over_threshold else 0 end) 
as FLOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1NSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1PSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1QSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1RSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='French Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as FOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='French Open Tennis - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FOTITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Grand National - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as GDNCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as HECSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IRBSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as WACEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IHCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='India Premier League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as IPLITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='International Freindlies - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as IFESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='International Friendlies - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as IFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MOTSITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Ligue 1 - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as L1BTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Ligue 1 - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as L1ESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Match of the day - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MOTDBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MROSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRSSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Moto GP BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NBA - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NBASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as NFLBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as NFLCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NFLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NIFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Oaks - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as OAKCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FSHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ORUGESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as OTHSNP_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as OTHTEN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as OTHUNK_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWAT_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWIN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWRE_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PGASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as PLBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as PLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PLDSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GPLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMNFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLNLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLOLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_programmes_viewed_over_threshold else 0 end) 
as PLS4SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSULSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PRUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ROISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Royal Ascot - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as RASCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIEBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIIBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RISBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIWBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLCCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as RLGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLWCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SARUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SFASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Serie A - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SABTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Serie A - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SAESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SFL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SFLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Six Nations - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as SNRBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRES12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FISHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SOLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SACSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPFL - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SPFLBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SPLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPL - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SP5SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The boat race - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BTRBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The football league show - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as FLSBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The Masters Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='TNA Wrestling Challenge' then total_programmes_viewed_over_threshold else 0 end) 
as TNACHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Tour de France - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as TDFEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Tour de France - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as TDFITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USMGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CLASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLOSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_programmes_viewed_over_threshold else 0 end) 
as CLTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_programmes_viewed_over_threshold else 0 end) 
as CLWSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='US Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as USOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='USA Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as USFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USPGASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WCQ - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as WCQESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WIFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Wimbledon - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WIMBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Athletics Championship - More 4' then total_programmes_viewed_over_threshold else 0 end) 
as WACMR4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Club Championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WCLBBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as WCQBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WDCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World snooker championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WSCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WWE Sky 1 and 2' then total_programmes_viewed_over_threshold else 0 end) 
as WWES12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WWE Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WWESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then number_of_events_viewed else 0 end) 
as AFCEUR_Total_Viewing_Events
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then number_of_events_viewed else 0 end) 
as AFCITV_Total_Viewing_Events
,sum(case when analysis_right ='Americas Cup - BBC' then number_of_events_viewed else 0 end) 
as AMCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATGSS_Total_Viewing_Events
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATPSS_Total_Viewing_Events
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then number_of_events_viewed else 0 end) 
as AHCSS_Total_Viewing_Events
,sum(case when analysis_right ='Australian Football - BT Sport' then number_of_events_viewed else 0 end) 
as AUFBTS_Total_Viewing_Events
,sum(case when analysis_right ='Australian Open Tennis - BBC' then number_of_events_viewed else 0 end) 
as AOTBBC_Total_Viewing_Events
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as AOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Aviva Premiership - ESPN' then number_of_events_viewed else 0 end) 
as AVPSS_Total_Viewing_Events
,sum(case when analysis_right ='BBC American Football' then number_of_events_viewed else 0 end) 
as AFBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Athletics' then number_of_events_viewed else 0 end) 
as ATHBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Boxing' then number_of_events_viewed else 0 end) 
as BOXBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Darts' then number_of_events_viewed else 0 end) 
as DRTBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Equestrian' then number_of_events_viewed else 0 end) 
as EQUBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Football' then number_of_events_viewed else 0 end) 
as FOOTBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Golf' then number_of_events_viewed else 0 end) 
as GOLFBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Rugby' then number_of_events_viewed else 0 end) 
as RUGBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Tennis' then number_of_events_viewed else 0 end) 
as TENBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Unknown' then number_of_events_viewed else 0 end) 
as UNKBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Watersports' then number_of_events_viewed else 0 end) 
as WATBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Wintersports' then number_of_events_viewed else 0 end) 
as WINBBC_Total_Viewing_Events
,sum(case when analysis_right ='Boxing  - Channel 5' then number_of_events_viewed else 0 end) 
as BOXCH5_Total_Viewing_Events
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as BOXMSS_Total_Viewing_Events
,sum(case when analysis_right ='Brazil Football - BT Sport' then number_of_events_viewed else 0 end) 
as BFTBTS_Total_Viewing_Events
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as BILSS_Total_Viewing_Events
,sum(case when analysis_right ='British Open Golf - BBC' then number_of_events_viewed else 0 end) 
as BOGSS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport American Football' then number_of_events_viewed else 0 end) 
as AFBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Athletics' then number_of_events_viewed else 0 end) 
as ATHBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Baseball' then number_of_events_viewed else 0 end) 
as BASEBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Basketball' then number_of_events_viewed else 0 end) 
as BASKBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Boxing' then number_of_events_viewed else 0 end) 
as BOXBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Cricket' then number_of_events_viewed else 0 end) 
as CRIBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Equestrian' then number_of_events_viewed else 0 end) 
as EQUBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Extreme' then number_of_events_viewed else 0 end) 
as EXTBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Football' then number_of_events_viewed else 0 end) 
as FOOTBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Rugby' then number_of_events_viewed else 0 end) 
as RUGBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Tennis' then number_of_events_viewed else 0 end) 
as TENBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Unknown' then number_of_events_viewed else 0 end) 
as UNKBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Wintersports' then number_of_events_viewed else 0 end) 
as WINBTS_Total_Viewing_Events
,sum(case when analysis_right ='Bundesliga - BT Sport' then number_of_events_viewed else 0 end) 
as BUNBTS_Total_Viewing_Events
,sum(case when analysis_right ='Bundesliga- ESPN' then number_of_events_viewed else 0 end) 
as BUNESPN_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Darts' then number_of_events_viewed else 0 end) 
as DRTCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Extreme' then number_of_events_viewed else 0 end) 
as EXTCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Unknown' then number_of_events_viewed else 0 end) 
as UNKCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Wrestling' then number_of_events_viewed else 0 end) 
as WRECHA_Total_Viewing_Events
,sum(case when analysis_right ='Champions League - ITV' then number_of_events_viewed else 0 end) 
as CHLITV_Total_Viewing_Events
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as ICCSS_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 American Football' then number_of_events_viewed else 0 end) 
as AMCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Boxing' then number_of_events_viewed else 0 end) 
as BOXCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Equestrian' then number_of_events_viewed else 0 end) 
as EQUCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Extreme' then number_of_events_viewed else 0 end) 
as EXTCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Football' then number_of_events_viewed else 0 end) 
as FOOTCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Racing' then number_of_events_viewed else 0 end) 
as RACCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Watersports' then number_of_events_viewed else 0 end) 
as WATCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Wintersports' then number_of_events_viewed else 0 end) 
as WINCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Boxing' then number_of_events_viewed else 0 end) 
as BOXOCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Cricket' then number_of_events_viewed else 0 end) 
as CRICH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Wrestling' then number_of_events_viewed else 0 end) 
as WRECH5_Total_Viewing_Events
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then number_of_events_viewed else 0 end) 
as CHELCH4_Total_Viewing_Events
,sum(case when analysis_right ='Community Shield - ITV' then number_of_events_viewed else 0 end) 
as CMSITV_Total_Viewing_Events
,sum(case when analysis_right ='Confederations Cup - BBC' then number_of_events_viewed else 0 end) 
as CONCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Conference - BT Sport' then number_of_events_viewed else 0 end) 
as CONFBTS_Total_Viewing_Events
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then number_of_events_viewed else 0 end) 
as CLVITV_Total_Viewing_Events
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then number_of_events_viewed else 0 end) 
as CUCISS_Total_Viewing_Events
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then number_of_events_viewed else 0 end) 
as CTBEUR_Total_Viewing_Events
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then number_of_events_viewed else 0 end) 
as CTCITV_Total_Viewing_Events
,sum(case when analysis_right ='Derby - Channel 4' then number_of_events_viewed else 0 end) 
as DERCH4_Total_Viewing_Events
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then number_of_events_viewed else 0 end) 
as ECBHCH5_Total_Viewing_Events
,sum(case when analysis_right ='ECB Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as GECRSS_Total_Viewing_Events
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBNSS_Total_Viewing_Events
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBTSS_Total_Viewing_Events
,sum(case when analysis_right ='England Football Internationals - ITV' then number_of_events_viewed else 0 end) 
as GENGITV_Total_Viewing_Events
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then number_of_events_viewed else 0 end) 
as EFRITV_Total_Viewing_Events
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as ENRSS_Total_Viewing_Events
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then number_of_events_viewed else 0 end) 
as EWQAITV_Total_Viewing_Events
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then number_of_events_viewed else 0 end) 
as EWQHITV_Total_Viewing_Events
,sum(case when analysis_right ='ESPN American Football' then number_of_events_viewed else 0 end) 
as AMESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Athletics' then number_of_events_viewed else 0 end) 
as ATHESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Baseball' then number_of_events_viewed else 0 end) 
as BASEESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Basketball' then number_of_events_viewed else 0 end) 
as BASKESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Boxing' then number_of_events_viewed else 0 end) 
as BOXESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Cricket' then number_of_events_viewed else 0 end) 
as CRIESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Darts' then number_of_events_viewed else 0 end) 
as DARTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Equestrian' then number_of_events_viewed else 0 end) 
as EQUESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Extreme' then number_of_events_viewed else 0 end) 
as EXTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Football' then number_of_events_viewed else 0 end) 
as FOOTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Golf' then number_of_events_viewed else 0 end) 
as GOLFESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Ice Hockey' then number_of_events_viewed else 0 end) 
as IHESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Motor Sport' then number_of_events_viewed else 0 end) 
as MSPESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Racing' then number_of_events_viewed else 0 end) 
as RACESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Rugby' then number_of_events_viewed else 0 end) 
as RUGESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Tennis' then number_of_events_viewed else 0 end) 
as TENESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Unknown' then number_of_events_viewed else 0 end) 
as UNKESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Watersports' then number_of_events_viewed else 0 end) 
as WATESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Wintersports' then number_of_events_viewed else 0 end) 
as WINESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Wrestling' then number_of_events_viewed else 0 end) 
as WREESPN_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - BT Sport' then number_of_events_viewed else 0 end) 
as ELBTSP_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - ESPN' then number_of_events_viewed else 0 end) 
as ELESPN_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - ITV' then number_of_events_viewed else 0 end) 
as ELITV_Total_Viewing_Events
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as ETGSS_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport American Football' then number_of_events_viewed else 0 end) 
as AMEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Athletics' then number_of_events_viewed else 0 end) 
as ATHEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Baseball' then number_of_events_viewed else 0 end) 
as BASEEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Basketball' then number_of_events_viewed else 0 end) 
as BASKEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Boxing' then number_of_events_viewed else 0 end) 
as BOXEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Cricket' then number_of_events_viewed else 0 end) 
as CRIEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Darts' then number_of_events_viewed else 0 end) 
as DARTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Equestrian' then number_of_events_viewed else 0 end) 
as EQUEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Extreme' then number_of_events_viewed else 0 end) 
as EXTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Football' then number_of_events_viewed else 0 end) 
as FOOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Golf' then number_of_events_viewed else 0 end) 
as GOLFEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Ice Hockey' then number_of_events_viewed else 0 end) 
as IHEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Racing' then number_of_events_viewed else 0 end) 
as RACEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Tennis' then number_of_events_viewed else 0 end) 
as TENEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Unknown' then number_of_events_viewed else 0 end) 
as UNKEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Watersports' then number_of_events_viewed else 0 end) 
as WATEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Wintersports' then number_of_events_viewed else 0 end) 
as WINEUR_Total_Viewing_Events
,sum(case when analysis_right ='F1 - BBC' then number_of_events_viewed else 0 end) 
as GF1BBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 - Sky Sports' then number_of_events_viewed else 0 end) 
as GF1SS_Total_Viewing_Events
,sum(case when analysis_right ='F1 (non-Live)- BBC' then number_of_events_viewed else 0 end) 
as F1NBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then number_of_events_viewed else 0 end) 
as F1PBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then number_of_events_viewed else 0 end) 
as F1QBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Race Live)- BBC' then number_of_events_viewed else 0 end) 
as F1RBBC_Total_Viewing_Events
,sum(case when analysis_right ='FA Cup - ESPN' then number_of_events_viewed else 0 end) 
as FACESPN_Total_Viewing_Events
,sum(case when analysis_right ='FA Cup - ITV' then number_of_events_viewed else 0 end) 
as FACITV_Total_Viewing_Events
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then number_of_events_viewed else 0 end) 
as FLCCSS_Total_Viewing_Events
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then number_of_events_viewed else 0 end) 
as FLOTSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1NSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1PSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1QSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1RSS_Total_Viewing_Events
,sum(case when analysis_right ='French Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as FOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='French Open Tennis - ITV' then number_of_events_viewed else 0 end) 
as FOTITV_Total_Viewing_Events
,sum(case when analysis_right ='Grand National - Channel 4' then number_of_events_viewed else 0 end) 
as GDNCH4_Total_Viewing_Events
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then number_of_events_viewed else 0 end) 
as HECSS_Total_Viewing_Events
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as IRBSS_Total_Viewing_Events
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then number_of_events_viewed else 0 end) 
as WACEUR_Total_Viewing_Events
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then number_of_events_viewed else 0 end) 
as IHCSS_Total_Viewing_Events
,sum(case when analysis_right ='India Premier League - ITV' then number_of_events_viewed else 0 end) 
as IPLITV_Total_Viewing_Events
,sum(case when analysis_right ='International Freindlies - ESPN' then number_of_events_viewed else 0 end) 
as IFESPN_Total_Viewing_Events
,sum(case when analysis_right ='International Friendlies - BT Sport' then number_of_events_viewed else 0 end) 
as IFBTS_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Football' then number_of_events_viewed else 0 end) 
as FOOTITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Motor Sport' then number_of_events_viewed else 0 end) 
as MOTSITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Cricket' then number_of_events_viewed else 0 end) 
as CRIITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Darts' then number_of_events_viewed else 0 end) 
as DARTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Extreme' then number_of_events_viewed else 0 end) 
as EXTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Football' then number_of_events_viewed else 0 end) 
as FOOTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Tennis' then number_of_events_viewed else 0 end) 
as TENITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV4_Total_Viewing_Events
,sum(case when analysis_right ='Ligue 1 - BT Sport' then number_of_events_viewed else 0 end) 
as L1BTS_Total_Viewing_Events
,sum(case when analysis_right ='Ligue 1 - ESPN' then number_of_events_viewed else 0 end) 
as L1ESPN_Total_Viewing_Events
,sum(case when analysis_right ='Match of the day - BBC' then number_of_events_viewed else 0 end) 
as MOTDBBC_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then number_of_events_viewed else 0 end) 
as MROSS_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRPSS_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRSSS_Total_Viewing_Events
,sum(case when analysis_right ='Moto GP BBC' then number_of_events_viewed else 0 end) 
as MGPBBC_Total_Viewing_Events
,sum(case when analysis_right ='NBA - Sky Sports' then number_of_events_viewed else 0 end) 
as NBASS_Total_Viewing_Events
,sum(case when analysis_right ='NFL - BBC' then number_of_events_viewed else 0 end) 
as NFLBBC_Total_Viewing_Events
,sum(case when analysis_right ='NFL - Channel 4' then number_of_events_viewed else 0 end) 
as NFLCH4_Total_Viewing_Events
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as NFLSS_Total_Viewing_Events
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then number_of_events_viewed else 0 end) 
as NIFSS_Total_Viewing_Events
,sum(case when analysis_right ='Oaks - Channel 4' then number_of_events_viewed else 0 end) 
as OAKCH4_Total_Viewing_Events
,sum(case when analysis_right ='Other American Football' then number_of_events_viewed else 0 end) 
as AMOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Athletics' then number_of_events_viewed else 0 end) 
as ATHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Baseball' then number_of_events_viewed else 0 end) 
as BASEOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Basketball' then number_of_events_viewed else 0 end) 
as BASKOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Boxing' then number_of_events_viewed else 0 end) 
as BOXOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Cricket' then number_of_events_viewed else 0 end) 
as CRIOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Darts' then number_of_events_viewed else 0 end) 
as DARTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Equestrian' then number_of_events_viewed else 0 end) 
as EQUOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Extreme' then number_of_events_viewed else 0 end) 
as EXTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Fishing' then number_of_events_viewed else 0 end) 
as FSHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Football' then number_of_events_viewed else 0 end) 
as FOOTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Golf' then number_of_events_viewed else 0 end) 
as GOLFOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Ice Hockey' then number_of_events_viewed else 0 end) 
as IHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Motor Sport' then number_of_events_viewed else 0 end) 
as MSPOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Racing' then number_of_events_viewed else 0 end) 
as RACOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Rugby' then number_of_events_viewed else 0 end) 
as RUGOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then number_of_events_viewed else 0 end) 
as ORUGESPN_Total_Viewing_Events
,sum(case when analysis_right ='Other Snooker/Pool' then number_of_events_viewed else 0 end) 
as OTHSNP_Total_Viewing_Events
,sum(case when analysis_right ='Other Tennis' then number_of_events_viewed else 0 end) 
as OTHTEN_Total_Viewing_Events
,sum(case when analysis_right ='Other Unknown' then number_of_events_viewed else 0 end) 
as OTHUNK_Total_Viewing_Events
,sum(case when analysis_right ='Other Watersports' then number_of_events_viewed else 0 end) 
as OTHWAT_Total_Viewing_Events
,sum(case when analysis_right ='Other Wintersports' then number_of_events_viewed else 0 end) 
as OTHWIN_Total_Viewing_Events
,sum(case when analysis_right ='Other Wrestling' then number_of_events_viewed else 0 end) 
as OTHWRE_Total_Viewing_Events
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then number_of_events_viewed else 0 end) 
as PGASS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League - BT Sport' then number_of_events_viewed else 0 end) 
as PLBTS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League - ESPN' then number_of_events_viewed else 0 end) 
as PLESPN_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as PLDSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports' then number_of_events_viewed else 0 end) 
as GPLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then number_of_events_viewed else 0 end) 
as PLMCSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then number_of_events_viewed else 0 end) 
as PLMNFSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as PLNLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as PLOLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then number_of_events_viewed else 0 end) 
as PLSNSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then number_of_events_viewed else 0 end) 
as PLS4SS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSULSS_Total_Viewing_Events
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then number_of_events_viewed else 0 end) 
as PRUSS_Total_Viewing_Events
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as ROISS_Total_Viewing_Events
,sum(case when analysis_right ='Royal Ascot - Channel 4' then number_of_events_viewed else 0 end) 
as RASCH4_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then number_of_events_viewed else 0 end) 
as RIEBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then number_of_events_viewed else 0 end) 
as RIIBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then number_of_events_viewed else 0 end) 
as RISBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then number_of_events_viewed else 0 end) 
as RIWBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then number_of_events_viewed else 0 end) 
as RLCCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League - Sky Sports' then number_of_events_viewed else 0 end) 
as RLGSS_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then number_of_events_viewed else 0 end) 
as RLWCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as SARUSS_Total_Viewing_Events
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as SFASS_Total_Viewing_Events
,sum(case when analysis_right ='Serie A - BT Sport' then number_of_events_viewed else 0 end) 
as SABTS_Total_Viewing_Events
,sum(case when analysis_right ='Serie A - ESPN' then number_of_events_viewed else 0 end) 
as SAESPN_Total_Viewing_Events
,sum(case when analysis_right ='SFL - ESPN' then number_of_events_viewed else 0 end) 
as SFLESPN_Total_Viewing_Events
,sum(case when analysis_right ='Six Nations - BBC' then number_of_events_viewed else 0 end) 
as SNRBBC_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then number_of_events_viewed else 0 end) 
as BOXS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then number_of_events_viewed else 0 end) 
as FOOTS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then number_of_events_viewed else 0 end) 
as UNKS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then number_of_events_viewed else 0 end) 
as WRES12_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports American Football' then number_of_events_viewed else 0 end) 
as AMSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Athletics' then number_of_events_viewed else 0 end) 
as ATHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Baseball' then number_of_events_viewed else 0 end) 
as BASESS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Basketball' then number_of_events_viewed else 0 end) 
as BASKSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Boxing' then number_of_events_viewed else 0 end) 
as BOXSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Cricket' then number_of_events_viewed else 0 end) 
as CRISS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Darts' then number_of_events_viewed else 0 end) 
as DARTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Equestrian' then number_of_events_viewed else 0 end) 
as EQUSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Extreme' then number_of_events_viewed else 0 end) 
as EXTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Fishing' then number_of_events_viewed else 0 end) 
as FISHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Football' then number_of_events_viewed else 0 end) 
as FOOTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Golf' then number_of_events_viewed else 0 end) 
as GOLFSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Ice Hockey' then number_of_events_viewed else 0 end) 
as IHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Motor Sport' then number_of_events_viewed else 0 end) 
as MSPSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Racing' then number_of_events_viewed else 0 end) 
as RACSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Rugby' then number_of_events_viewed else 0 end) 
as RUGSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Tennis' then number_of_events_viewed else 0 end) 
as TENSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Unknown' then number_of_events_viewed else 0 end) 
as UNKSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Watersports' then number_of_events_viewed else 0 end) 
as WATSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Wintersports' then number_of_events_viewed else 0 end) 
as WINSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Wrestling' then number_of_events_viewed else 0 end) 
as WRESS_Total_Viewing_Events
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as SOLSS_Total_Viewing_Events
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as SACSS_Total_Viewing_Events
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as SPFSS_Total_Viewing_Events
,sum(case when analysis_right ='SPFL - BT Sport' then number_of_events_viewed else 0 end) 
as SPFLBTS_Total_Viewing_Events
,sum(case when analysis_right ='SPL - ESPN' then number_of_events_viewed else 0 end) 
as SPLESPN_Total_Viewing_Events
,sum(case when analysis_right ='SPL - Sky Sports' then number_of_events_viewed else 0 end) 
as SPLSS_Total_Viewing_Events
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then number_of_events_viewed else 0 end) 
as SP5SS_Total_Viewing_Events
,sum(case when analysis_right ='The boat race - BBC' then number_of_events_viewed else 0 end) 
as BTRBBC_Total_Viewing_Events
,sum(case when analysis_right ='The football league show - BBC' then number_of_events_viewed else 0 end) 
as FLSBBC_Total_Viewing_Events
,sum(case when analysis_right ='The Masters Golf - BBC' then number_of_events_viewed else 0 end) 
as MGBBC_Total_Viewing_Events
,sum(case when analysis_right ='TNA Wrestling Challenge' then number_of_events_viewed else 0 end) 
as TNACHA_Total_Viewing_Events
,sum(case when analysis_right ='Tour de France - Eurosport' then number_of_events_viewed else 0 end) 
as TDFEUR_Total_Viewing_Events
,sum(case when analysis_right ='Tour de France - ITV' then number_of_events_viewed else 0 end) 
as TDFITV_Total_Viewing_Events
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as USMGSS_Total_Viewing_Events
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as USOTSS_Total_Viewing_Events
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then number_of_events_viewed else 0 end) 
as USOGSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then number_of_events_viewed else 0 end) 
as CLASS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as CLNSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as CLOSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then number_of_events_viewed else 0 end) 
as CLTSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then number_of_events_viewed else 0 end) 
as CLWSS_Total_Viewing_Events
,sum(case when analysis_right ='US Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as USOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='USA Football - BT Sport' then number_of_events_viewed else 0 end) 
as USFBTS_Total_Viewing_Events
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then number_of_events_viewed else 0 end) 
as USPGASS_Total_Viewing_Events
,sum(case when analysis_right ='WCQ - ESPN' then number_of_events_viewed else 0 end) 
as WCQESPN_Total_Viewing_Events
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as WIFSS_Total_Viewing_Events
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as WICSS_Total_Viewing_Events
,sum(case when analysis_right ='Wimbledon - BBC' then number_of_events_viewed else 0 end) 
as WIMBBC_Total_Viewing_Events
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as WICCSS_Total_Viewing_Events
,sum(case when analysis_right ='World Athletics Championship - More 4' then number_of_events_viewed else 0 end) 
as WACMR4_Total_Viewing_Events
,sum(case when analysis_right ='World Club Championship - BBC' then number_of_events_viewed else 0 end) 
as WCLBBBC_Total_Viewing_Events
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then number_of_events_viewed else 0 end) 
as WCQBTS_Total_Viewing_Events
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then number_of_events_viewed else 0 end) 
as WDCSS_Total_Viewing_Events
,sum(case when analysis_right ='World snooker championship - BBC' then number_of_events_viewed else 0 end) 
as WSCBBC_Total_Viewing_Events
,sum(case when analysis_right ='WWE Sky 1 and 2' then number_of_events_viewed else 0 end) 
as WWES12_Total_Viewing_Events
,sum(case when analysis_right ='WWE Sky Sports' then number_of_events_viewed else 0 end) 
as WWESS_Total_Viewing_Events

into dbarnett.v250_unannualised_right_activity
from dbarnett.v250_sports_rights_viewed_by_right_overall
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_unannualised_right_activity;

---repeat for Live/Non Live Splits---
---pt2
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_viewed_by_right_and_live_status (account_number);
CREATE HG INDEX idx2 ON dbarnett.v250_sports_rights_viewed_by_right_and_live_status(analysis_right);
commit;
drop table dbarnett.v250_unannualised_right_activity_by_live_non_live;

select account_number

,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then broadcast_days_viewed else 0 end) 
as AFCEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then broadcast_days_viewed else 0 end) 
as AFCITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Americas Cup - BBC' then broadcast_days_viewed else 0 end) 
as AMCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then broadcast_days_viewed else 0 end) 
as ATPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then broadcast_days_viewed else 0 end) 
as AHCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Football - BT Sport' then broadcast_days_viewed else 0 end) 
as AUFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Open Tennis - BBC' then broadcast_days_viewed else 0 end) 
as AOTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as AOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Aviva Premiership - ESPN' then broadcast_days_viewed else 0 end) 
as AVPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC American Football' then broadcast_days_viewed else 0 end) 
as AFBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Athletics' then broadcast_days_viewed else 0 end) 
as ATHBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Boxing' then broadcast_days_viewed else 0 end) 
as BOXBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Darts' then broadcast_days_viewed else 0 end) 
as DRTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Football' then broadcast_days_viewed else 0 end) 
as FOOTBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Golf' then broadcast_days_viewed else 0 end) 
as GOLFBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Rugby' then broadcast_days_viewed else 0 end) 
as RUGBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Tennis' then broadcast_days_viewed else 0 end) 
as TENBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Unknown' then broadcast_days_viewed else 0 end) 
as UNKBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Watersports' then broadcast_days_viewed else 0 end) 
as WATBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='BBC Wintersports' then broadcast_days_viewed else 0 end) 
as WINBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Boxing  - Channel 5' then broadcast_days_viewed else 0 end) 
as BOXCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as BOXMSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Brazil Football - BT Sport' then broadcast_days_viewed else 0 end) 
as BFTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as BILSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='British Open Golf - BBC' then broadcast_days_viewed else 0 end) 
as BOGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport American Football' then broadcast_days_viewed else 0 end) 
as AFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Athletics' then broadcast_days_viewed else 0 end) 
as ATHBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Baseball' then broadcast_days_viewed else 0 end) 
as BASEBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Basketball' then broadcast_days_viewed else 0 end) 
as BASKBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Boxing' then broadcast_days_viewed else 0 end) 
as BOXBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Cricket' then broadcast_days_viewed else 0 end) 
as CRIBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Extreme' then broadcast_days_viewed else 0 end) 
as EXTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Football' then broadcast_days_viewed else 0 end) 
as FOOTBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Rugby' then broadcast_days_viewed else 0 end) 
as RUGBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Tennis' then broadcast_days_viewed else 0 end) 
as TENBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Unknown' then broadcast_days_viewed else 0 end) 
as UNKBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='BT Sport Wintersports' then broadcast_days_viewed else 0 end) 
as WINBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Bundesliga - BT Sport' then broadcast_days_viewed else 0 end) 
as BUNBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Bundesliga- ESPN' then broadcast_days_viewed else 0 end) 
as BUNESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Darts' then broadcast_days_viewed else 0 end) 
as DRTCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Extreme' then broadcast_days_viewed else 0 end) 
as EXTCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Unknown' then broadcast_days_viewed else 0 end) 
as UNKCHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Challenge Wrestling' then broadcast_days_viewed else 0 end) 
as WRECHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Champions League - ITV' then broadcast_days_viewed else 0 end) 
as CHLITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as ICCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 American Football' then broadcast_days_viewed else 0 end) 
as AMCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Equestrian' then broadcast_days_viewed else 0 end) 
as EQUCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Football' then broadcast_days_viewed else 0 end) 
as FOOTCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Racing' then broadcast_days_viewed else 0 end) 
as RACCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Watersports' then broadcast_days_viewed else 0 end) 
as WATCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 4 Wintersports' then broadcast_days_viewed else 0 end) 
as WINCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Athletics' then broadcast_days_viewed else 0 end) 
as ATHCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Boxing' then broadcast_days_viewed else 0 end) 
as BOXOCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Cricket' then broadcast_days_viewed else 0 end) 
as CRICH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Unknown' then broadcast_days_viewed else 0 end) 
as UNKCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Channel 5 Wrestling' then broadcast_days_viewed else 0 end) 
as WRECH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then broadcast_days_viewed else 0 end) 
as CHELCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Community Shield - ITV' then broadcast_days_viewed else 0 end) 
as CMSITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Confederations Cup - BBC' then broadcast_days_viewed else 0 end) 
as CONCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Conference - BT Sport' then broadcast_days_viewed else 0 end) 
as CONFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then broadcast_days_viewed else 0 end) 
as CLVITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then broadcast_days_viewed else 0 end) 
as CUCISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then broadcast_days_viewed else 0 end) 
as CTBEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then broadcast_days_viewed else 0 end) 
as CTCITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Derby - Channel 4' then broadcast_days_viewed else 0 end) 
as DERCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then broadcast_days_viewed else 0 end) 
as ECBHCH5_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as GECRSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then broadcast_days_viewed else 0 end) 
as ECBTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Football Internationals - ITV' then broadcast_days_viewed else 0 end) 
as GENGITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then broadcast_days_viewed else 0 end) 
as EFRITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ENRSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then broadcast_days_viewed else 0 end) 
as EWQAITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then broadcast_days_viewed else 0 end) 
as EWQHITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN American Football' then broadcast_days_viewed else 0 end) 
as AMESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Athletics' then broadcast_days_viewed else 0 end) 
as ATHESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Baseball' then broadcast_days_viewed else 0 end) 
as BASEESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Basketball' then broadcast_days_viewed else 0 end) 
as BASKESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Boxing' then broadcast_days_viewed else 0 end) 
as BOXESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Cricket' then broadcast_days_viewed else 0 end) 
as CRIESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Darts' then broadcast_days_viewed else 0 end) 
as DARTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Equestrian' then broadcast_days_viewed else 0 end) 
as EQUESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Extreme' then broadcast_days_viewed else 0 end) 
as EXTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Football' then broadcast_days_viewed else 0 end) 
as FOOTESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Golf' then broadcast_days_viewed else 0 end) 
as GOLFESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Racing' then broadcast_days_viewed else 0 end) 
as RACESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Rugby' then broadcast_days_viewed else 0 end) 
as RUGESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Tennis' then broadcast_days_viewed else 0 end) 
as TENESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Unknown' then broadcast_days_viewed else 0 end) 
as UNKESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Watersports' then broadcast_days_viewed else 0 end) 
as WATESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Wintersports' then broadcast_days_viewed else 0 end) 
as WINESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='ESPN Wrestling' then broadcast_days_viewed else 0 end) 
as WREESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - BT Sport' then broadcast_days_viewed else 0 end) 
as ELBTSP_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - ESPN' then broadcast_days_viewed else 0 end) 
as ELESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Europa League - ITV' then broadcast_days_viewed else 0 end) 
as ELITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as ETGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport American Football' then broadcast_days_viewed else 0 end) 
as AMEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Athletics' then broadcast_days_viewed else 0 end) 
as ATHEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Baseball' then broadcast_days_viewed else 0 end) 
as BASEEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Basketball' then broadcast_days_viewed else 0 end) 
as BASKEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Boxing' then broadcast_days_viewed else 0 end) 
as BOXEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Cricket' then broadcast_days_viewed else 0 end) 
as CRIEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Darts' then broadcast_days_viewed else 0 end) 
as DARTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Equestrian' then broadcast_days_viewed else 0 end) 
as EQUEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Extreme' then broadcast_days_viewed else 0 end) 
as EXTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Football' then broadcast_days_viewed else 0 end) 
as FOOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Golf' then broadcast_days_viewed else 0 end) 
as GOLFEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Racing' then broadcast_days_viewed else 0 end) 
as RACEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Tennis' then broadcast_days_viewed else 0 end) 
as TENEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Unknown' then broadcast_days_viewed else 0 end) 
as UNKEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Watersports' then broadcast_days_viewed else 0 end) 
as WATEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Eurosport Wintersports' then broadcast_days_viewed else 0 end) 
as WINEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 - BBC' then broadcast_days_viewed else 0 end) 
as GF1BBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 - Sky Sports' then broadcast_days_viewed else 0 end) 
as GF1SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (non-Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1NBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1PBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1QBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='F1 (Race Live)- BBC' then broadcast_days_viewed else 0 end) 
as F1RBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='FA Cup - ESPN' then broadcast_days_viewed else 0 end) 
as FACESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='FA Cup - ITV' then broadcast_days_viewed else 0 end) 
as FACITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then broadcast_days_viewed else 0 end) 
as FLCCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then broadcast_days_viewed else 0 end) 
as FLOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1NSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1PSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1QSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then broadcast_days_viewed else 0 end) 
as F1RSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='French Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as FOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='French Open Tennis - ITV' then broadcast_days_viewed else 0 end) 
as FOTITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='Grand National - Channel 4' then broadcast_days_viewed else 0 end) 
as GDNCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then broadcast_days_viewed else 0 end) 
as HECSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as IRBSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then broadcast_days_viewed else 0 end) 
as WACEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then broadcast_days_viewed else 0 end) 
as IHCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='India Premier League - ITV' then broadcast_days_viewed else 0 end) 
as IPLITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='International Freindlies - ESPN' then broadcast_days_viewed else 0 end) 
as IFESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='International Friendlies - BT Sport' then broadcast_days_viewed else 0 end) 
as IFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Motor Sport' then broadcast_days_viewed else 0 end) 
as MOTSITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV1 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV1_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Boxing' then broadcast_days_viewed else 0 end) 
as BOXITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Cricket' then broadcast_days_viewed else 0 end) 
as CRIITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Darts' then broadcast_days_viewed else 0 end) 
as DARTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Extreme' then broadcast_days_viewed else 0 end) 
as EXTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Football' then broadcast_days_viewed else 0 end) 
as FOOTITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Rugby' then broadcast_days_viewed else 0 end) 
as RUGITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Tennis' then broadcast_days_viewed else 0 end) 
as TENITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='ITV4 Unknown' then broadcast_days_viewed else 0 end) 
as UNKITV4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Ligue 1 - BT Sport' then broadcast_days_viewed else 0 end) 
as L1BTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Ligue 1 - ESPN' then broadcast_days_viewed else 0 end) 
as L1ESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Match of the day - BBC' then broadcast_days_viewed else 0 end) 
as MOTDBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MROSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then broadcast_days_viewed else 0 end) 
as MRSSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Moto GP BBC' then broadcast_days_viewed else 0 end) 
as MGPBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='NBA - Sky Sports' then broadcast_days_viewed else 0 end) 
as NBASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL - BBC' then broadcast_days_viewed else 0 end) 
as NFLBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL - Channel 4' then broadcast_days_viewed else 0 end) 
as NFLCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NFLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then broadcast_days_viewed else 0 end) 
as NIFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Oaks - Channel 4' then broadcast_days_viewed else 0 end) 
as OAKCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other American Football' then broadcast_days_viewed else 0 end) 
as AMOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Athletics' then broadcast_days_viewed else 0 end) 
as ATHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Baseball' then broadcast_days_viewed else 0 end) 
as BASEOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Basketball' then broadcast_days_viewed else 0 end) 
as BASKOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Boxing' then broadcast_days_viewed else 0 end) 
as BOXOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Cricket' then broadcast_days_viewed else 0 end) 
as CRIOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Darts' then broadcast_days_viewed else 0 end) 
as DARTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Equestrian' then broadcast_days_viewed else 0 end) 
as EQUOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Extreme' then broadcast_days_viewed else 0 end) 
as EXTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Fishing' then broadcast_days_viewed else 0 end) 
as FSHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Football' then broadcast_days_viewed else 0 end) 
as FOOTOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Golf' then broadcast_days_viewed else 0 end) 
as GOLFOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Racing' then broadcast_days_viewed else 0 end) 
as RACOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Rugby' then broadcast_days_viewed else 0 end) 
as RUGOTH_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then broadcast_days_viewed else 0 end) 
as ORUGESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Snooker/Pool' then broadcast_days_viewed else 0 end) 
as OTHSNP_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Tennis' then broadcast_days_viewed else 0 end) 
as OTHTEN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Unknown' then broadcast_days_viewed else 0 end) 
as OTHUNK_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Watersports' then broadcast_days_viewed else 0 end) 
as OTHWAT_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Wintersports' then broadcast_days_viewed else 0 end) 
as OTHWIN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Other Wrestling' then broadcast_days_viewed else 0 end) 
as OTHWRE_Broadcast_Days_Viewed
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PGASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League - BT Sport' then broadcast_days_viewed else 0 end) 
as PLBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League - ESPN' then broadcast_days_viewed else 0 end) 
as PLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then broadcast_days_viewed else 0 end) 
as PLDSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports' then broadcast_days_viewed else 0 end) 
as GPLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then broadcast_days_viewed else 0 end) 
as PLMCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then broadcast_days_viewed else 0 end) 
as PLMNFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as PLNLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as PLOLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then broadcast_days_viewed else 0 end) 
as PLSNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then broadcast_days_viewed else 0 end) 
as PLS4SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then broadcast_days_viewed else 0 end) 
as PLSULSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then broadcast_days_viewed else 0 end) 
as PRUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as ROISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Royal Ascot - Channel 4' then broadcast_days_viewed else 0 end) 
as RASCH4_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then broadcast_days_viewed else 0 end) 
as RIEBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then broadcast_days_viewed else 0 end) 
as RIIBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then broadcast_days_viewed else 0 end) 
as RISBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then broadcast_days_viewed else 0 end) 
as RIWBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLCCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League - Sky Sports' then broadcast_days_viewed else 0 end) 
as RLGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then broadcast_days_viewed else 0 end) 
as RLWCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SARUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SFASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Serie A - BT Sport' then broadcast_days_viewed else 0 end) 
as SABTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Serie A - ESPN' then broadcast_days_viewed else 0 end) 
as SAESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='SFL - ESPN' then broadcast_days_viewed else 0 end) 
as SFLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Six Nations - BBC' then broadcast_days_viewed else 0 end) 
as SNRBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then broadcast_days_viewed else 0 end) 
as BOXS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then broadcast_days_viewed else 0 end) 
as FOOTS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then broadcast_days_viewed else 0 end) 
as UNKS12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then broadcast_days_viewed else 0 end) 
as WRES12_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports American Football' then broadcast_days_viewed else 0 end) 
as AMSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Athletics' then broadcast_days_viewed else 0 end) 
as ATHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Baseball' then broadcast_days_viewed else 0 end) 
as BASESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Basketball' then broadcast_days_viewed else 0 end) 
as BASKSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Boxing' then broadcast_days_viewed else 0 end) 
as BOXSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Cricket' then broadcast_days_viewed else 0 end) 
as CRISS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Darts' then broadcast_days_viewed else 0 end) 
as DARTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Equestrian' then broadcast_days_viewed else 0 end) 
as EQUSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Extreme' then broadcast_days_viewed else 0 end) 
as EXTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Fishing' then broadcast_days_viewed else 0 end) 
as FISHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Football' then broadcast_days_viewed else 0 end) 
as FOOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Golf' then broadcast_days_viewed else 0 end) 
as GOLFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Ice Hockey' then broadcast_days_viewed else 0 end) 
as IHSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Motor Sport' then broadcast_days_viewed else 0 end) 
as MSPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Racing' then broadcast_days_viewed else 0 end) 
as RACSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Rugby' then broadcast_days_viewed else 0 end) 
as RUGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then broadcast_days_viewed else 0 end) 
as SNPSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Tennis' then broadcast_days_viewed else 0 end) 
as TENSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Unknown' then broadcast_days_viewed else 0 end) 
as UNKSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Watersports' then broadcast_days_viewed else 0 end) 
as WATSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Wintersports' then broadcast_days_viewed else 0 end) 
as WINSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sky Sports Wrestling' then broadcast_days_viewed else 0 end) 
as WRESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then broadcast_days_viewed else 0 end) 
as SOLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as SACSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPFL - BT Sport' then broadcast_days_viewed else 0 end) 
as SPFLBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPL - ESPN' then broadcast_days_viewed else 0 end) 
as SPLESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='SPL - Sky Sports' then broadcast_days_viewed else 0 end) 
as SPLSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then broadcast_days_viewed else 0 end) 
as SP5SS_Broadcast_Days_Viewed
,sum(case when analysis_right ='The boat race - BBC' then broadcast_days_viewed else 0 end) 
as BTRBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='The football league show - BBC' then broadcast_days_viewed else 0 end) 
as FLSBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='The Masters Golf - BBC' then broadcast_days_viewed else 0 end) 
as MGBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='TNA Wrestling Challenge' then broadcast_days_viewed else 0 end) 
as TNACHA_Broadcast_Days_Viewed
,sum(case when analysis_right ='Tour de France - Eurosport' then broadcast_days_viewed else 0 end) 
as TDFEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='Tour de France - ITV' then broadcast_days_viewed else 0 end) 
as TDFITV_Broadcast_Days_Viewed
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as USMGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as USOTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then broadcast_days_viewed else 0 end) 
as USOGSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then broadcast_days_viewed else 0 end) 
as CLASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then broadcast_days_viewed else 0 end) 
as CLNSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then broadcast_days_viewed else 0 end) 
as CLOSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then broadcast_days_viewed else 0 end) 
as CLTSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then broadcast_days_viewed else 0 end) 
as CLWSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='US Open Tennis - Eurosport' then broadcast_days_viewed else 0 end) 
as USOTEUR_Broadcast_Days_Viewed
,sum(case when analysis_right ='USA Football - BT Sport' then broadcast_days_viewed else 0 end) 
as USFBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then broadcast_days_viewed else 0 end) 
as USPGASS_Broadcast_Days_Viewed
,sum(case when analysis_right ='WCQ - ESPN' then broadcast_days_viewed else 0 end) 
as WCQESPN_Broadcast_Days_Viewed
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then broadcast_days_viewed else 0 end) 
as WIFSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Wimbledon - BBC' then broadcast_days_viewed else 0 end) 
as WIMBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then broadcast_days_viewed else 0 end) 
as WICCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Athletics Championship - More 4' then broadcast_days_viewed else 0 end) 
as WACMR4_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Club Championship - BBC' then broadcast_days_viewed else 0 end) 
as WCLBBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then broadcast_days_viewed else 0 end) 
as WCQBTS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then broadcast_days_viewed else 0 end) 
as WDCSS_Broadcast_Days_Viewed
,sum(case when analysis_right ='World snooker championship - BBC' then broadcast_days_viewed else 0 end) 
as WSCBBC_Broadcast_Days_Viewed
,sum(case when analysis_right ='WWE Sky 1 and 2' then broadcast_days_viewed else 0 end) 
as WWES12_Broadcast_Days_Viewed
,sum(case when analysis_right ='WWE Sky Sports' then broadcast_days_viewed else 0 end) 
as WWESS_Broadcast_Days_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AFCEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then total_duration_viewed_seconds else 0 end) 
as AFCITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Americas Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as AMCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ATPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as AHCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as AUFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Open Tennis - BBC' then total_duration_viewed_seconds else 0 end) 
as AOTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as AOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Aviva Premiership - ESPN' then total_duration_viewed_seconds else 0 end) 
as AVPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC American Football' then total_duration_viewed_seconds else 0 end) 
as AFBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Darts' then total_duration_viewed_seconds else 0 end) 
as DRTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Watersports' then total_duration_viewed_seconds else 0 end) 
as WATBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='BBC Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Boxing  - Channel 5' then total_duration_viewed_seconds else 0 end) 
as BOXCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BOXMSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Brazil Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BFTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as BILSS_Total_Seconds_Viewed
,sum(case when analysis_right ='British Open Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as BOGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport American Football' then total_duration_viewed_seconds else 0 end) 
as AFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='BT Sport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Bundesliga - BT Sport' then total_duration_viewed_seconds else 0 end) 
as BUNBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Bundesliga- ESPN' then total_duration_viewed_seconds else 0 end) 
as BUNESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Darts' then total_duration_viewed_seconds else 0 end) 
as DRTCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Challenge Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Champions League - ITV' then total_duration_viewed_seconds else 0 end) 
as CHLITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ICCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 American Football' then total_duration_viewed_seconds else 0 end) 
as AMCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Racing' then total_duration_viewed_seconds else 0 end) 
as RACCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Watersports' then total_duration_viewed_seconds else 0 end) 
as WATCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 4 Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRICH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Channel 5 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRECH5_Total_Seconds_Viewed
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then total_duration_viewed_seconds else 0 end) 
as CHELCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Community Shield - ITV' then total_duration_viewed_seconds else 0 end) 
as CMSITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Confederations Cup - BBC' then total_duration_viewed_seconds else 0 end) 
as CONCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Conference - BT Sport' then total_duration_viewed_seconds else 0 end) 
as CONFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then total_duration_viewed_seconds else 0 end) 
as CLVITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CUCISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then total_duration_viewed_seconds else 0 end) 
as CTBEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then total_duration_viewed_seconds else 0 end) 
as CTCITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Derby - Channel 4' then total_duration_viewed_seconds else 0 end) 
as DERCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then total_duration_viewed_seconds else 0 end) 
as ECBHCH5_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GECRSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ECBTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='England Football Internationals - ITV' then total_duration_viewed_seconds else 0 end) 
as GENGITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then total_duration_viewed_seconds else 0 end) 
as EFRITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ENRSS_Total_Seconds_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQAITV_Total_Seconds_Viewed
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then total_duration_viewed_seconds else 0 end) 
as EWQHITV_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN American Football' then total_duration_viewed_seconds else 0 end) 
as AMESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Darts' then total_duration_viewed_seconds else 0 end) 
as DARTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Football' then total_duration_viewed_seconds else 0 end) 
as FOOTESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Racing' then total_duration_viewed_seconds else 0 end) 
as RACESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Tennis' then total_duration_viewed_seconds else 0 end) 
as TENESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Watersports' then total_duration_viewed_seconds else 0 end) 
as WATESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='ESPN Wrestling' then total_duration_viewed_seconds else 0 end) 
as WREESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as ELBTSP_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - ESPN' then total_duration_viewed_seconds else 0 end) 
as ELESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Europa League - ITV' then total_duration_viewed_seconds else 0 end) 
as ELITV_Total_Seconds_Viewed
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ETGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport American Football' then total_duration_viewed_seconds else 0 end) 
as AMEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Darts' then total_duration_viewed_seconds else 0 end) 
as DARTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Football' then total_duration_viewed_seconds else 0 end) 
as FOOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Racing' then total_duration_viewed_seconds else 0 end) 
as RACEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Tennis' then total_duration_viewed_seconds else 0 end) 
as TENEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Watersports' then total_duration_viewed_seconds else 0 end) 
as WATEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Eurosport Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 - BBC' then total_duration_viewed_seconds else 0 end) 
as GF1BBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GF1SS_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (non-Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1NBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1PBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1QBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='F1 (Race Live)- BBC' then total_duration_viewed_seconds else 0 end) 
as F1RBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='FA Cup - ESPN' then total_duration_viewed_seconds else 0 end) 
as FACESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='FA Cup - ITV' then total_duration_viewed_seconds else 0 end) 
as FACITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_duration_viewed_seconds else 0 end) 
as FLCCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_duration_viewed_seconds else 0 end) 
as FLOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1NSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1PSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1QSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as F1RSS_Total_Seconds_Viewed
,sum(case when analysis_right ='French Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as FOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='French Open Tennis - ITV' then total_duration_viewed_seconds else 0 end) 
as FOTITV_Total_Seconds_Viewed
,sum(case when analysis_right ='Grand National - Channel 4' then total_duration_viewed_seconds else 0 end) 
as GDNCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as HECSS_Total_Seconds_Viewed
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IRBSS_Total_Seconds_Viewed
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then total_duration_viewed_seconds else 0 end) 
as WACEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as IHCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='India Premier League - ITV' then total_duration_viewed_seconds else 0 end) 
as IPLITV_Total_Seconds_Viewed
,sum(case when analysis_right ='International Freindlies - ESPN' then total_duration_viewed_seconds else 0 end) 
as IFESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='International Friendlies - BT Sport' then total_duration_viewed_seconds else 0 end) 
as IFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MOTSITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV1 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV1_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Darts' then total_duration_viewed_seconds else 0 end) 
as DARTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Tennis' then total_duration_viewed_seconds else 0 end) 
as TENITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='ITV4 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKITV4_Total_Seconds_Viewed
,sum(case when analysis_right ='Ligue 1 - BT Sport' then total_duration_viewed_seconds else 0 end) 
as L1BTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Ligue 1 - ESPN' then total_duration_viewed_seconds else 0 end) 
as L1ESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Match of the day - BBC' then total_duration_viewed_seconds else 0 end) 
as MOTDBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MROSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_duration_viewed_seconds else 0 end) 
as MRSSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Moto GP BBC' then total_duration_viewed_seconds else 0 end) 
as MGPBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='NBA - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NBASS_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL - BBC' then total_duration_viewed_seconds else 0 end) 
as NFLBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL - Channel 4' then total_duration_viewed_seconds else 0 end) 
as NFLCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NFLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as NIFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Oaks - Channel 4' then total_duration_viewed_seconds else 0 end) 
as OAKCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Other American Football' then total_duration_viewed_seconds else 0 end) 
as AMOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Baseball' then total_duration_viewed_seconds else 0 end) 
as BASEOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Cricket' then total_duration_viewed_seconds else 0 end) 
as CRIOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Darts' then total_duration_viewed_seconds else 0 end) 
as DARTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Fishing' then total_duration_viewed_seconds else 0 end) 
as FSHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Football' then total_duration_viewed_seconds else 0 end) 
as FOOTOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Racing' then total_duration_viewed_seconds else 0 end) 
as RACOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGOTH_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then total_duration_viewed_seconds else 0 end) 
as ORUGESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as OTHSNP_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Tennis' then total_duration_viewed_seconds else 0 end) 
as OTHTEN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Unknown' then total_duration_viewed_seconds else 0 end) 
as OTHUNK_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Watersports' then total_duration_viewed_seconds else 0 end) 
as OTHWAT_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Wintersports' then total_duration_viewed_seconds else 0 end) 
as OTHWIN_Total_Seconds_Viewed
,sum(case when analysis_right ='Other Wrestling' then total_duration_viewed_seconds else 0 end) 
as OTHWRE_Total_Seconds_Viewed
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PGASS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League - BT Sport' then total_duration_viewed_seconds else 0 end) 
as PLBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League - ESPN' then total_duration_viewed_seconds else 0 end) 
as PLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PLDSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as GPLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_duration_viewed_seconds else 0 end) 
as PLMCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then total_duration_viewed_seconds else 0 end) 
as PLMNFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as PLNLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as PLOLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_duration_viewed_seconds else 0 end) 
as PLSNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_duration_viewed_seconds else 0 end) 
as PLS4SS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_duration_viewed_seconds else 0 end) 
as PLSULSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as PRUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as ROISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Royal Ascot - Channel 4' then total_duration_viewed_seconds else 0 end) 
as RASCH4_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIEBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIIBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then total_duration_viewed_seconds else 0 end) 
as RISBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then total_duration_viewed_seconds else 0 end) 
as RIWBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLCCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as RLGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then total_duration_viewed_seconds else 0 end) 
as RLWCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SARUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SFASS_Total_Seconds_Viewed
,sum(case when analysis_right ='Serie A - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SABTS_Total_Seconds_Viewed
,sum(case when analysis_right ='Serie A - ESPN' then total_duration_viewed_seconds else 0 end) 
as SAESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='SFL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SFLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Six Nations - BBC' then total_duration_viewed_seconds else 0 end) 
as SNRBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then total_duration_viewed_seconds else 0 end) 
as FOOTS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKS12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRES12_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports American Football' then total_duration_viewed_seconds else 0 end) 
as AMSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Athletics' then total_duration_viewed_seconds else 0 end) 
as ATHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Baseball' then total_duration_viewed_seconds else 0 end) 
as BASESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Basketball' then total_duration_viewed_seconds else 0 end) 
as BASKSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Boxing' then total_duration_viewed_seconds else 0 end) 
as BOXSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Cricket' then total_duration_viewed_seconds else 0 end) 
as CRISS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Darts' then total_duration_viewed_seconds else 0 end) 
as DARTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Equestrian' then total_duration_viewed_seconds else 0 end) 
as EQUSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Extreme' then total_duration_viewed_seconds else 0 end) 
as EXTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Fishing' then total_duration_viewed_seconds else 0 end) 
as FISHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Football' then total_duration_viewed_seconds else 0 end) 
as FOOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Golf' then total_duration_viewed_seconds else 0 end) 
as GOLFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Ice Hockey' then total_duration_viewed_seconds else 0 end) 
as IHSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Motor Sport' then total_duration_viewed_seconds else 0 end) 
as MSPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Racing' then total_duration_viewed_seconds else 0 end) 
as RACSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Rugby' then total_duration_viewed_seconds else 0 end) 
as RUGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then total_duration_viewed_seconds else 0 end) 
as SNPSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Tennis' then total_duration_viewed_seconds else 0 end) 
as TENSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Unknown' then total_duration_viewed_seconds else 0 end) 
as UNKSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Watersports' then total_duration_viewed_seconds else 0 end) 
as WATSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Wintersports' then total_duration_viewed_seconds else 0 end) 
as WINSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sky Sports Wrestling' then total_duration_viewed_seconds else 0 end) 
as WRESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SOLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SACSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='SPFL - BT Sport' then total_duration_viewed_seconds else 0 end) 
as SPFLBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='SPL - ESPN' then total_duration_viewed_seconds else 0 end) 
as SPLESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='SPL - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SPLSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_duration_viewed_seconds else 0 end) 
as SP5SS_Total_Seconds_Viewed
,sum(case when analysis_right ='The boat race - BBC' then total_duration_viewed_seconds else 0 end) 
as BTRBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='The football league show - BBC' then total_duration_viewed_seconds else 0 end) 
as FLSBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='The Masters Golf - BBC' then total_duration_viewed_seconds else 0 end) 
as MGBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='TNA Wrestling Challenge' then total_duration_viewed_seconds else 0 end) 
as TNACHA_Total_Seconds_Viewed
,sum(case when analysis_right ='Tour de France - Eurosport' then total_duration_viewed_seconds else 0 end) 
as TDFEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='Tour de France - ITV' then total_duration_viewed_seconds else 0 end) 
as TDFITV_Total_Seconds_Viewed
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USMGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USOGSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then total_duration_viewed_seconds else 0 end) 
as CLASS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_duration_viewed_seconds else 0 end) 
as CLNSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_duration_viewed_seconds else 0 end) 
as CLOSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_duration_viewed_seconds else 0 end) 
as CLTSS_Total_Seconds_Viewed
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_duration_viewed_seconds else 0 end) 
as CLWSS_Total_Seconds_Viewed
,sum(case when analysis_right ='US Open Tennis - Eurosport' then total_duration_viewed_seconds else 0 end) 
as USOTEUR_Total_Seconds_Viewed
,sum(case when analysis_right ='USA Football - BT Sport' then total_duration_viewed_seconds else 0 end) 
as USFBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_duration_viewed_seconds else 0 end) 
as USPGASS_Total_Seconds_Viewed
,sum(case when analysis_right ='WCQ - ESPN' then total_duration_viewed_seconds else 0 end) 
as WCQESPN_Total_Seconds_Viewed
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WIFSS_Total_Seconds_Viewed
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICSS_Total_Seconds_Viewed
,sum(case when analysis_right ='Wimbledon - BBC' then total_duration_viewed_seconds else 0 end) 
as WIMBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WICCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='World Athletics Championship - More 4' then total_duration_viewed_seconds else 0 end) 
as WACMR4_Total_Seconds_Viewed
,sum(case when analysis_right ='World Club Championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WCLBBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then total_duration_viewed_seconds else 0 end) 
as WCQBTS_Total_Seconds_Viewed
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WDCSS_Total_Seconds_Viewed
,sum(case when analysis_right ='World snooker championship - BBC' then total_duration_viewed_seconds else 0 end) 
as WSCBBC_Total_Seconds_Viewed
,sum(case when analysis_right ='WWE Sky 1 and 2' then total_duration_viewed_seconds else 0 end) 
as WWES12_Total_Seconds_Viewed
,sum(case when analysis_right ='WWE Sky Sports' then total_duration_viewed_seconds else 0 end) 
as WWESS_Total_Seconds_Viewed
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AFCEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as AFCITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Americas Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AMCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ATPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as AHCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as AUFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Open Tennis - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as AOTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as AOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Aviva Premiership - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as AVPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BBC Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Boxing  - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BOXMSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Brazil Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BFTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as BILSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='British Open Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BOGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='BT Sport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Bundesliga - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as BUNBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Bundesliga- ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as BUNESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DRTCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Challenge Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Champions League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CHLITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ICCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 4 Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRICH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Channel 5 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRECH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as CHELCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Community Shield - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CMSITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Confederations Cup - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as CONCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Conference - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as CONFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then total_programmes_viewed_over_threshold else 0 end) 
as CLVITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CUCISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as CTBEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then total_programmes_viewed_over_threshold else 0 end) 
as CTCITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Derby - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as DERCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then total_programmes_viewed_over_threshold else 0 end) 
as ECBHCH5_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GECRSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ECBTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Football Internationals - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as GENGITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EFRITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ENRSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQAITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as EWQHITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ESPN Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WREESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as ELBTSP_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ELESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Europa League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as ELITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ETGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Eurosport Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as GF1BBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GF1SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (non-Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1NBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1PBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1QBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='F1 (Race Live)- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as F1RBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='FA Cup - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as FACESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='FA Cup - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FACITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then total_programmes_viewed_over_threshold else 0 end) 
as FLCCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then total_programmes_viewed_over_threshold else 0 end) 
as FLOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1NSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1PSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1QSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as F1RSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='French Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as FOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='French Open Tennis - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as FOTITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Grand National - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as GDNCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as HECSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IRBSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as WACEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as IHCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='India Premier League - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as IPLITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='International Freindlies - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as IFESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='International Friendlies - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as IFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MOTSITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV1 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV1_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='ITV4 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKITV4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Ligue 1 - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as L1BTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Ligue 1 - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as L1ESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Match of the day - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MOTDBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MROSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as MRSSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Moto GP BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGPBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NBA - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NBASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as NFLBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as NFLCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NFLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as NIFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Oaks - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as OAKCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASEOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRIOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FSHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGOTH_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as ORUGESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as OTHSNP_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as OTHTEN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as OTHUNK_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWAT_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWIN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Other Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as OTHWRE_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PGASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as PLBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as PLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PLDSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as GPLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then total_programmes_viewed_over_threshold else 0 end) 
as PLMNFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLNLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLOLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then total_programmes_viewed_over_threshold else 0 end) 
as PLS4SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then total_programmes_viewed_over_threshold else 0 end) 
as PLSULSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as PRUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as ROISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Royal Ascot - Channel 4' then total_programmes_viewed_over_threshold else 0 end) 
as RASCH4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIEBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIIBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RISBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RIWBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLCCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as RLGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then total_programmes_viewed_over_threshold else 0 end) 
as RLWCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SARUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SFASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Serie A - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SABTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Serie A - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SAESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SFL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SFLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Six Nations - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as SNRBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKS12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRES12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports American Football' then total_programmes_viewed_over_threshold else 0 end) 
as AMSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Athletics' then total_programmes_viewed_over_threshold else 0 end) 
as ATHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Baseball' then total_programmes_viewed_over_threshold else 0 end) 
as BASESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Basketball' then total_programmes_viewed_over_threshold else 0 end) 
as BASKSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Boxing' then total_programmes_viewed_over_threshold else 0 end) 
as BOXSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Cricket' then total_programmes_viewed_over_threshold else 0 end) 
as CRISS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Darts' then total_programmes_viewed_over_threshold else 0 end) 
as DARTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Equestrian' then total_programmes_viewed_over_threshold else 0 end) 
as EQUSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Extreme' then total_programmes_viewed_over_threshold else 0 end) 
as EXTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Fishing' then total_programmes_viewed_over_threshold else 0 end) 
as FISHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Football' then total_programmes_viewed_over_threshold else 0 end) 
as FOOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Golf' then total_programmes_viewed_over_threshold else 0 end) 
as GOLFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Ice Hockey' then total_programmes_viewed_over_threshold else 0 end) 
as IHSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Motor Sport' then total_programmes_viewed_over_threshold else 0 end) 
as MSPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Racing' then total_programmes_viewed_over_threshold else 0 end) 
as RACSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Rugby' then total_programmes_viewed_over_threshold else 0 end) 
as RUGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then total_programmes_viewed_over_threshold else 0 end) 
as SNPSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Tennis' then total_programmes_viewed_over_threshold else 0 end) 
as TENSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Unknown' then total_programmes_viewed_over_threshold else 0 end) 
as UNKSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Watersports' then total_programmes_viewed_over_threshold else 0 end) 
as WATSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Wintersports' then total_programmes_viewed_over_threshold else 0 end) 
as WINSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sky Sports Wrestling' then total_programmes_viewed_over_threshold else 0 end) 
as WRESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SOLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SACSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPFL - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as SPFLBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPL - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as SPLESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='SPL - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SPLSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as SP5SS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The boat race - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as BTRBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The football league show - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as FLSBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='The Masters Golf - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as MGBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='TNA Wrestling Challenge' then total_programmes_viewed_over_threshold else 0 end) 
as TNACHA_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Tour de France - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as TDFEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Tour de France - ITV' then total_programmes_viewed_over_threshold else 0 end) 
as TDFITV_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USMGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USOGSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as CLASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLNSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then total_programmes_viewed_over_threshold else 0 end) 
as CLOSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then total_programmes_viewed_over_threshold else 0 end) 
as CLTSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then total_programmes_viewed_over_threshold else 0 end) 
as CLWSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='US Open Tennis - Eurosport' then total_programmes_viewed_over_threshold else 0 end) 
as USOTEUR_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='USA Football - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as USFBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as USPGASS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WCQ - ESPN' then total_programmes_viewed_over_threshold else 0 end) 
as WCQESPN_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WIFSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Wimbledon - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WIMBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WICCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Athletics Championship - More 4' then total_programmes_viewed_over_threshold else 0 end) 
as WACMR4_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Club Championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WCLBBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then total_programmes_viewed_over_threshold else 0 end) 
as WCQBTS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WDCSS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='World snooker championship - BBC' then total_programmes_viewed_over_threshold else 0 end) 
as WSCBBC_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WWE Sky 1 and 2' then total_programmes_viewed_over_threshold else 0 end) 
as WWES12_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='WWE Sky Sports' then total_programmes_viewed_over_threshold else 0 end) 
as WWESS_Programmes_Viewed_over_threshold
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then number_of_events_viewed else 0 end) 
as AFCEUR_Total_Viewing_Events
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then number_of_events_viewed else 0 end) 
as AFCITV_Total_Viewing_Events
,sum(case when analysis_right ='Americas Cup - BBC' then number_of_events_viewed else 0 end) 
as AMCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATGSS_Total_Viewing_Events
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then number_of_events_viewed else 0 end) 
as ATPSS_Total_Viewing_Events
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then number_of_events_viewed else 0 end) 
as AHCSS_Total_Viewing_Events
,sum(case when analysis_right ='Australian Football - BT Sport' then number_of_events_viewed else 0 end) 
as AUFBTS_Total_Viewing_Events
,sum(case when analysis_right ='Australian Open Tennis - BBC' then number_of_events_viewed else 0 end) 
as AOTBBC_Total_Viewing_Events
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as AOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Aviva Premiership - ESPN' then number_of_events_viewed else 0 end) 
as AVPSS_Total_Viewing_Events
,sum(case when analysis_right ='BBC American Football' then number_of_events_viewed else 0 end) 
as AFBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Athletics' then number_of_events_viewed else 0 end) 
as ATHBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Boxing' then number_of_events_viewed else 0 end) 
as BOXBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Darts' then number_of_events_viewed else 0 end) 
as DRTBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Equestrian' then number_of_events_viewed else 0 end) 
as EQUBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Football' then number_of_events_viewed else 0 end) 
as FOOTBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Golf' then number_of_events_viewed else 0 end) 
as GOLFBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Rugby' then number_of_events_viewed else 0 end) 
as RUGBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Tennis' then number_of_events_viewed else 0 end) 
as TENBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Unknown' then number_of_events_viewed else 0 end) 
as UNKBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Watersports' then number_of_events_viewed else 0 end) 
as WATBBC_Total_Viewing_Events
,sum(case when analysis_right ='BBC Wintersports' then number_of_events_viewed else 0 end) 
as WINBBC_Total_Viewing_Events
,sum(case when analysis_right ='Boxing  - Channel 5' then number_of_events_viewed else 0 end) 
as BOXCH5_Total_Viewing_Events
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as BOXMSS_Total_Viewing_Events
,sum(case when analysis_right ='Brazil Football - BT Sport' then number_of_events_viewed else 0 end) 
as BFTBTS_Total_Viewing_Events
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as BILSS_Total_Viewing_Events
,sum(case when analysis_right ='British Open Golf - BBC' then number_of_events_viewed else 0 end) 
as BOGSS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport American Football' then number_of_events_viewed else 0 end) 
as AFBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Athletics' then number_of_events_viewed else 0 end) 
as ATHBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Baseball' then number_of_events_viewed else 0 end) 
as BASEBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Basketball' then number_of_events_viewed else 0 end) 
as BASKBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Boxing' then number_of_events_viewed else 0 end) 
as BOXBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Cricket' then number_of_events_viewed else 0 end) 
as CRIBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Equestrian' then number_of_events_viewed else 0 end) 
as EQUBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Extreme' then number_of_events_viewed else 0 end) 
as EXTBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Football' then number_of_events_viewed else 0 end) 
as FOOTBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Rugby' then number_of_events_viewed else 0 end) 
as RUGBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Tennis' then number_of_events_viewed else 0 end) 
as TENBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Unknown' then number_of_events_viewed else 0 end) 
as UNKBTS_Total_Viewing_Events
,sum(case when analysis_right ='BT Sport Wintersports' then number_of_events_viewed else 0 end) 
as WINBTS_Total_Viewing_Events
,sum(case when analysis_right ='Bundesliga - BT Sport' then number_of_events_viewed else 0 end) 
as BUNBTS_Total_Viewing_Events
,sum(case when analysis_right ='Bundesliga- ESPN' then number_of_events_viewed else 0 end) 
as BUNESPN_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Darts' then number_of_events_viewed else 0 end) 
as DRTCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Extreme' then number_of_events_viewed else 0 end) 
as EXTCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Unknown' then number_of_events_viewed else 0 end) 
as UNKCHA_Total_Viewing_Events
,sum(case when analysis_right ='Challenge Wrestling' then number_of_events_viewed else 0 end) 
as WRECHA_Total_Viewing_Events
,sum(case when analysis_right ='Champions League - ITV' then number_of_events_viewed else 0 end) 
as CHLITV_Total_Viewing_Events
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as ICCSS_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 American Football' then number_of_events_viewed else 0 end) 
as AMCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Boxing' then number_of_events_viewed else 0 end) 
as BOXCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Equestrian' then number_of_events_viewed else 0 end) 
as EQUCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Extreme' then number_of_events_viewed else 0 end) 
as EXTCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Football' then number_of_events_viewed else 0 end) 
as FOOTCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Racing' then number_of_events_viewed else 0 end) 
as RACCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Watersports' then number_of_events_viewed else 0 end) 
as WATCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 4 Wintersports' then number_of_events_viewed else 0 end) 
as WINCH4_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Athletics' then number_of_events_viewed else 0 end) 
as ATHCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Boxing' then number_of_events_viewed else 0 end) 
as BOXOCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Cricket' then number_of_events_viewed else 0 end) 
as CRICH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Unknown' then number_of_events_viewed else 0 end) 
as UNKCH5_Total_Viewing_Events
,sum(case when analysis_right ='Channel 5 Wrestling' then number_of_events_viewed else 0 end) 
as WRECH5_Total_Viewing_Events
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then number_of_events_viewed else 0 end) 
as CHELCH4_Total_Viewing_Events
,sum(case when analysis_right ='Community Shield - ITV' then number_of_events_viewed else 0 end) 
as CMSITV_Total_Viewing_Events
,sum(case when analysis_right ='Confederations Cup - BBC' then number_of_events_viewed else 0 end) 
as CONCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Conference - BT Sport' then number_of_events_viewed else 0 end) 
as CONFBTS_Total_Viewing_Events
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then number_of_events_viewed else 0 end) 
as CLVITV_Total_Viewing_Events
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then number_of_events_viewed else 0 end) 
as CUCISS_Total_Viewing_Events
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then number_of_events_viewed else 0 end) 
as CTBEUR_Total_Viewing_Events
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then number_of_events_viewed else 0 end) 
as CTCITV_Total_Viewing_Events
,sum(case when analysis_right ='Derby - Channel 4' then number_of_events_viewed else 0 end) 
as DERCH4_Total_Viewing_Events
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then number_of_events_viewed else 0 end) 
as ECBHCH5_Total_Viewing_Events
,sum(case when analysis_right ='ECB Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as GECRSS_Total_Viewing_Events
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBNSS_Total_Viewing_Events
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then number_of_events_viewed else 0 end) 
as ECBTSS_Total_Viewing_Events
,sum(case when analysis_right ='England Football Internationals - ITV' then number_of_events_viewed else 0 end) 
as GENGITV_Total_Viewing_Events
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then number_of_events_viewed else 0 end) 
as EFRITV_Total_Viewing_Events
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as ENRSS_Total_Viewing_Events
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then number_of_events_viewed else 0 end) 
as EWQAITV_Total_Viewing_Events
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then number_of_events_viewed else 0 end) 
as EWQHITV_Total_Viewing_Events
,sum(case when analysis_right ='ESPN American Football' then number_of_events_viewed else 0 end) 
as AMESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Athletics' then number_of_events_viewed else 0 end) 
as ATHESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Baseball' then number_of_events_viewed else 0 end) 
as BASEESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Basketball' then number_of_events_viewed else 0 end) 
as BASKESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Boxing' then number_of_events_viewed else 0 end) 
as BOXESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Cricket' then number_of_events_viewed else 0 end) 
as CRIESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Darts' then number_of_events_viewed else 0 end) 
as DARTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Equestrian' then number_of_events_viewed else 0 end) 
as EQUESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Extreme' then number_of_events_viewed else 0 end) 
as EXTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Football' then number_of_events_viewed else 0 end) 
as FOOTESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Golf' then number_of_events_viewed else 0 end) 
as GOLFESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Ice Hockey' then number_of_events_viewed else 0 end) 
as IHESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Motor Sport' then number_of_events_viewed else 0 end) 
as MSPESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Racing' then number_of_events_viewed else 0 end) 
as RACESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Rugby' then number_of_events_viewed else 0 end) 
as RUGESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Tennis' then number_of_events_viewed else 0 end) 
as TENESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Unknown' then number_of_events_viewed else 0 end) 
as UNKESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Watersports' then number_of_events_viewed else 0 end) 
as WATESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Wintersports' then number_of_events_viewed else 0 end) 
as WINESPN_Total_Viewing_Events
,sum(case when analysis_right ='ESPN Wrestling' then number_of_events_viewed else 0 end) 
as WREESPN_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - BT Sport' then number_of_events_viewed else 0 end) 
as ELBTSP_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - ESPN' then number_of_events_viewed else 0 end) 
as ELESPN_Total_Viewing_Events
,sum(case when analysis_right ='Europa League - ITV' then number_of_events_viewed else 0 end) 
as ELITV_Total_Viewing_Events
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as ETGSS_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport American Football' then number_of_events_viewed else 0 end) 
as AMEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Athletics' then number_of_events_viewed else 0 end) 
as ATHEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Baseball' then number_of_events_viewed else 0 end) 
as BASEEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Basketball' then number_of_events_viewed else 0 end) 
as BASKEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Boxing' then number_of_events_viewed else 0 end) 
as BOXEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Cricket' then number_of_events_viewed else 0 end) 
as CRIEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Darts' then number_of_events_viewed else 0 end) 
as DARTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Equestrian' then number_of_events_viewed else 0 end) 
as EQUEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Extreme' then number_of_events_viewed else 0 end) 
as EXTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Football' then number_of_events_viewed else 0 end) 
as FOOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Golf' then number_of_events_viewed else 0 end) 
as GOLFEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Ice Hockey' then number_of_events_viewed else 0 end) 
as IHEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Motor Sport' then number_of_events_viewed else 0 end) 
as MSPEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Racing' then number_of_events_viewed else 0 end) 
as RACEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Tennis' then number_of_events_viewed else 0 end) 
as TENEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Unknown' then number_of_events_viewed else 0 end) 
as UNKEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Watersports' then number_of_events_viewed else 0 end) 
as WATEUR_Total_Viewing_Events
,sum(case when analysis_right ='Eurosport Wintersports' then number_of_events_viewed else 0 end) 
as WINEUR_Total_Viewing_Events
,sum(case when analysis_right ='F1 - BBC' then number_of_events_viewed else 0 end) 
as GF1BBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 - Sky Sports' then number_of_events_viewed else 0 end) 
as GF1SS_Total_Viewing_Events
,sum(case when analysis_right ='F1 (non-Live)- BBC' then number_of_events_viewed else 0 end) 
as F1NBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then number_of_events_viewed else 0 end) 
as F1PBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then number_of_events_viewed else 0 end) 
as F1QBBC_Total_Viewing_Events
,sum(case when analysis_right ='F1 (Race Live)- BBC' then number_of_events_viewed else 0 end) 
as F1RBBC_Total_Viewing_Events
,sum(case when analysis_right ='FA Cup - ESPN' then number_of_events_viewed else 0 end) 
as FACESPN_Total_Viewing_Events
,sum(case when analysis_right ='FA Cup - ITV' then number_of_events_viewed else 0 end) 
as FACITV_Total_Viewing_Events
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then number_of_events_viewed else 0 end) 
as FLCCSS_Total_Viewing_Events
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then number_of_events_viewed else 0 end) 
as FLOTSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1NSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1PSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1QSS_Total_Viewing_Events
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then number_of_events_viewed else 0 end) 
as F1RSS_Total_Viewing_Events
,sum(case when analysis_right ='French Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as FOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='French Open Tennis - ITV' then number_of_events_viewed else 0 end) 
as FOTITV_Total_Viewing_Events
,sum(case when analysis_right ='Grand National - Channel 4' then number_of_events_viewed else 0 end) 
as GDNCH4_Total_Viewing_Events
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then number_of_events_viewed else 0 end) 
as HECSS_Total_Viewing_Events
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as IRBSS_Total_Viewing_Events
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then number_of_events_viewed else 0 end) 
as WACEUR_Total_Viewing_Events
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then number_of_events_viewed else 0 end) 
as IHCSS_Total_Viewing_Events
,sum(case when analysis_right ='India Premier League - ITV' then number_of_events_viewed else 0 end) 
as IPLITV_Total_Viewing_Events
,sum(case when analysis_right ='International Freindlies - ESPN' then number_of_events_viewed else 0 end) 
as IFESPN_Total_Viewing_Events
,sum(case when analysis_right ='International Friendlies - BT Sport' then number_of_events_viewed else 0 end) 
as IFBTS_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Football' then number_of_events_viewed else 0 end) 
as FOOTITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Motor Sport' then number_of_events_viewed else 0 end) 
as MOTSITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV1 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV1_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Boxing' then number_of_events_viewed else 0 end) 
as BOXITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Cricket' then number_of_events_viewed else 0 end) 
as CRIITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Darts' then number_of_events_viewed else 0 end) 
as DARTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Extreme' then number_of_events_viewed else 0 end) 
as EXTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Football' then number_of_events_viewed else 0 end) 
as FOOTITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Rugby' then number_of_events_viewed else 0 end) 
as RUGITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Tennis' then number_of_events_viewed else 0 end) 
as TENITV4_Total_Viewing_Events
,sum(case when analysis_right ='ITV4 Unknown' then number_of_events_viewed else 0 end) 
as UNKITV4_Total_Viewing_Events
,sum(case when analysis_right ='Ligue 1 - BT Sport' then number_of_events_viewed else 0 end) 
as L1BTS_Total_Viewing_Events
,sum(case when analysis_right ='Ligue 1 - ESPN' then number_of_events_viewed else 0 end) 
as L1ESPN_Total_Viewing_Events
,sum(case when analysis_right ='Match of the day - BBC' then number_of_events_viewed else 0 end) 
as MOTDBBC_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then number_of_events_viewed else 0 end) 
as MROSS_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRPSS_Total_Viewing_Events
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then number_of_events_viewed else 0 end) 
as MRSSS_Total_Viewing_Events
,sum(case when analysis_right ='Moto GP BBC' then number_of_events_viewed else 0 end) 
as MGPBBC_Total_Viewing_Events
,sum(case when analysis_right ='NBA - Sky Sports' then number_of_events_viewed else 0 end) 
as NBASS_Total_Viewing_Events
,sum(case when analysis_right ='NFL - BBC' then number_of_events_viewed else 0 end) 
as NFLBBC_Total_Viewing_Events
,sum(case when analysis_right ='NFL - Channel 4' then number_of_events_viewed else 0 end) 
as NFLCH4_Total_Viewing_Events
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as NFLSS_Total_Viewing_Events
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then number_of_events_viewed else 0 end) 
as NIFSS_Total_Viewing_Events
,sum(case when analysis_right ='Oaks - Channel 4' then number_of_events_viewed else 0 end) 
as OAKCH4_Total_Viewing_Events
,sum(case when analysis_right ='Other American Football' then number_of_events_viewed else 0 end) 
as AMOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Athletics' then number_of_events_viewed else 0 end) 
as ATHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Baseball' then number_of_events_viewed else 0 end) 
as BASEOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Basketball' then number_of_events_viewed else 0 end) 
as BASKOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Boxing' then number_of_events_viewed else 0 end) 
as BOXOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Cricket' then number_of_events_viewed else 0 end) 
as CRIOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Darts' then number_of_events_viewed else 0 end) 
as DARTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Equestrian' then number_of_events_viewed else 0 end) 
as EQUOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Extreme' then number_of_events_viewed else 0 end) 
as EXTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Fishing' then number_of_events_viewed else 0 end) 
as FSHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Football' then number_of_events_viewed else 0 end) 
as FOOTOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Golf' then number_of_events_viewed else 0 end) 
as GOLFOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Ice Hockey' then number_of_events_viewed else 0 end) 
as IHOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Motor Sport' then number_of_events_viewed else 0 end) 
as MSPOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Racing' then number_of_events_viewed else 0 end) 
as RACOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Rugby' then number_of_events_viewed else 0 end) 
as RUGOTH_Total_Viewing_Events
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then number_of_events_viewed else 0 end) 
as ORUGESPN_Total_Viewing_Events
,sum(case when analysis_right ='Other Snooker/Pool' then number_of_events_viewed else 0 end) 
as OTHSNP_Total_Viewing_Events
,sum(case when analysis_right ='Other Tennis' then number_of_events_viewed else 0 end) 
as OTHTEN_Total_Viewing_Events
,sum(case when analysis_right ='Other Unknown' then number_of_events_viewed else 0 end) 
as OTHUNK_Total_Viewing_Events
,sum(case when analysis_right ='Other Watersports' then number_of_events_viewed else 0 end) 
as OTHWAT_Total_Viewing_Events
,sum(case when analysis_right ='Other Wintersports' then number_of_events_viewed else 0 end) 
as OTHWIN_Total_Viewing_Events
,sum(case when analysis_right ='Other Wrestling' then number_of_events_viewed else 0 end) 
as OTHWRE_Total_Viewing_Events
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then number_of_events_viewed else 0 end) 
as PGASS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League - BT Sport' then number_of_events_viewed else 0 end) 
as PLBTS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League - ESPN' then number_of_events_viewed else 0 end) 
as PLESPN_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then number_of_events_viewed else 0 end) 
as PLDSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports' then number_of_events_viewed else 0 end) 
as GPLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then number_of_events_viewed else 0 end) 
as PLMCSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then number_of_events_viewed else 0 end) 
as PLMNFSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as PLNLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as PLOLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSLSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then number_of_events_viewed else 0 end) 
as PLSNSS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then number_of_events_viewed else 0 end) 
as PLS4SS_Total_Viewing_Events
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then number_of_events_viewed else 0 end) 
as PLSULSS_Total_Viewing_Events
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then number_of_events_viewed else 0 end) 
as PRUSS_Total_Viewing_Events
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as ROISS_Total_Viewing_Events
,sum(case when analysis_right ='Royal Ascot - Channel 4' then number_of_events_viewed else 0 end) 
as RASCH4_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then number_of_events_viewed else 0 end) 
as RIEBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then number_of_events_viewed else 0 end) 
as RIIBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then number_of_events_viewed else 0 end) 
as RISBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then number_of_events_viewed else 0 end) 
as RIWBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then number_of_events_viewed else 0 end) 
as RLCCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League - Sky Sports' then number_of_events_viewed else 0 end) 
as RLGSS_Total_Viewing_Events
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then number_of_events_viewed else 0 end) 
as RLWCBBC_Total_Viewing_Events
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then number_of_events_viewed else 0 end) 
as SARUSS_Total_Viewing_Events
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then number_of_events_viewed else 0 end) 
as SFASS_Total_Viewing_Events
,sum(case when analysis_right ='Serie A - BT Sport' then number_of_events_viewed else 0 end) 
as SABTS_Total_Viewing_Events
,sum(case when analysis_right ='Serie A - ESPN' then number_of_events_viewed else 0 end) 
as SAESPN_Total_Viewing_Events
,sum(case when analysis_right ='SFL - ESPN' then number_of_events_viewed else 0 end) 
as SFLESPN_Total_Viewing_Events
,sum(case when analysis_right ='Six Nations - BBC' then number_of_events_viewed else 0 end) 
as SNRBBC_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then number_of_events_viewed else 0 end) 
as BOXS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then number_of_events_viewed else 0 end) 
as FOOTS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then number_of_events_viewed else 0 end) 
as MSPS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then number_of_events_viewed else 0 end) 
as UNKS12_Total_Viewing_Events
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then number_of_events_viewed else 0 end) 
as WRES12_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports American Football' then number_of_events_viewed else 0 end) 
as AMSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Athletics' then number_of_events_viewed else 0 end) 
as ATHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Baseball' then number_of_events_viewed else 0 end) 
as BASESS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Basketball' then number_of_events_viewed else 0 end) 
as BASKSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Boxing' then number_of_events_viewed else 0 end) 
as BOXSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Cricket' then number_of_events_viewed else 0 end) 
as CRISS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Darts' then number_of_events_viewed else 0 end) 
as DARTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Equestrian' then number_of_events_viewed else 0 end) 
as EQUSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Extreme' then number_of_events_viewed else 0 end) 
as EXTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Fishing' then number_of_events_viewed else 0 end) 
as FISHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Football' then number_of_events_viewed else 0 end) 
as FOOTSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Golf' then number_of_events_viewed else 0 end) 
as GOLFSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Ice Hockey' then number_of_events_viewed else 0 end) 
as IHSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Motor Sport' then number_of_events_viewed else 0 end) 
as MSPSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Racing' then number_of_events_viewed else 0 end) 
as RACSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Rugby' then number_of_events_viewed else 0 end) 
as RUGSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then number_of_events_viewed else 0 end) 
as SNPSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Tennis' then number_of_events_viewed else 0 end) 
as TENSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Unknown' then number_of_events_viewed else 0 end) 
as UNKSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Watersports' then number_of_events_viewed else 0 end) 
as WATSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Wintersports' then number_of_events_viewed else 0 end) 
as WINSS_Total_Viewing_Events
,sum(case when analysis_right ='Sky Sports Wrestling' then number_of_events_viewed else 0 end) 
as WRESS_Total_Viewing_Events
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then number_of_events_viewed else 0 end) 
as SOLSS_Total_Viewing_Events
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as SACSS_Total_Viewing_Events
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then number_of_events_viewed else 0 end) 
as SPFSS_Total_Viewing_Events
,sum(case when analysis_right ='SPFL - BT Sport' then number_of_events_viewed else 0 end) 
as SPFLBTS_Total_Viewing_Events
,sum(case when analysis_right ='SPL - ESPN' then number_of_events_viewed else 0 end) 
as SPLESPN_Total_Viewing_Events
,sum(case when analysis_right ='SPL - Sky Sports' then number_of_events_viewed else 0 end) 
as SPLSS_Total_Viewing_Events
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then number_of_events_viewed else 0 end) 
as SP5SS_Total_Viewing_Events
,sum(case when analysis_right ='The boat race - BBC' then number_of_events_viewed else 0 end) 
as BTRBBC_Total_Viewing_Events
,sum(case when analysis_right ='The football league show - BBC' then number_of_events_viewed else 0 end) 
as FLSBBC_Total_Viewing_Events
,sum(case when analysis_right ='The Masters Golf - BBC' then number_of_events_viewed else 0 end) 
as MGBBC_Total_Viewing_Events
,sum(case when analysis_right ='TNA Wrestling Challenge' then number_of_events_viewed else 0 end) 
as TNACHA_Total_Viewing_Events
,sum(case when analysis_right ='Tour de France - Eurosport' then number_of_events_viewed else 0 end) 
as TDFEUR_Total_Viewing_Events
,sum(case when analysis_right ='Tour de France - ITV' then number_of_events_viewed else 0 end) 
as TDFITV_Total_Viewing_Events
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as USMGSS_Total_Viewing_Events
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as USOTSS_Total_Viewing_Events
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then number_of_events_viewed else 0 end) 
as USOGSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then number_of_events_viewed else 0 end) 
as CLASS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then number_of_events_viewed else 0 end) 
as CLNSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then number_of_events_viewed else 0 end) 
as CLOSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then number_of_events_viewed else 0 end) 
as CLTSS_Total_Viewing_Events
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then number_of_events_viewed else 0 end) 
as CLWSS_Total_Viewing_Events
,sum(case when analysis_right ='US Open Tennis - Eurosport' then number_of_events_viewed else 0 end) 
as USOTEUR_Total_Viewing_Events
,sum(case when analysis_right ='USA Football - BT Sport' then number_of_events_viewed else 0 end) 
as USFBTS_Total_Viewing_Events
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then number_of_events_viewed else 0 end) 
as USPGASS_Total_Viewing_Events
,sum(case when analysis_right ='WCQ - ESPN' then number_of_events_viewed else 0 end) 
as WCQESPN_Total_Viewing_Events
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then number_of_events_viewed else 0 end) 
as WIFSS_Total_Viewing_Events
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then number_of_events_viewed else 0 end) 
as WICSS_Total_Viewing_Events
,sum(case when analysis_right ='Wimbledon - BBC' then number_of_events_viewed else 0 end) 
as WIMBBC_Total_Viewing_Events
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then number_of_events_viewed else 0 end) 
as WICCSS_Total_Viewing_Events
,sum(case when analysis_right ='World Athletics Championship - More 4' then number_of_events_viewed else 0 end) 
as WACMR4_Total_Viewing_Events
,sum(case when analysis_right ='World Club Championship - BBC' then number_of_events_viewed else 0 end) 
as WCLBBBC_Total_Viewing_Events
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then number_of_events_viewed else 0 end) 
as WCQBTS_Total_Viewing_Events
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then number_of_events_viewed else 0 end) 
as WDCSS_Total_Viewing_Events
,sum(case when analysis_right ='World snooker championship - BBC' then number_of_events_viewed else 0 end) 
as WSCBBC_Total_Viewing_Events
,sum(case when analysis_right ='WWE Sky 1 and 2' then number_of_events_viewed else 0 end) 
as WWES12_Total_Viewing_Events
,sum(case when analysis_right ='WWE Sky Sports' then number_of_events_viewed else 0 end) 
as WWESS_Total_Viewing_Events

into dbarnett.v250_unannualised_right_activity_by_live_non_live
from dbarnett.v250_sports_rights_viewed_by_right_and_live_status
group by account_number
;
commit;


--select top 500 * from dbarnett.v250_rights_broadcast_overall
--select count(*) from dbarnett.v250_master_account_list

---Create Annualised totals for each right--
--select distinct analysis_right from dbarnett.v250_rights_broadcast_overall order by analysis_right;
--Calculate Number of Days each right broadcast--
--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis
--select top 100 * from dbarnett.v250_rights_broadcast_overall;
--select distinct analysis_right from dbarnett.v250_rights_broadcast_overall order by analysis_right;
--

drop table dbarnett.v250_days_right_viewable_by_account;
select a.account_number
,b.analysis_right
,count(*) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account
from dbarnett.v250_days_viewing_by_account as a
left outer join dbarnett.v250_rights_broadcast_overall as b
on a.viewing_date=b.broadcast_date
group by  a.account_number
,b.analysis_right
;

commit;
--select top 100 * from dbarnett.v250_days_right_viewable_by_account;

---Calculate Number of Days each right (and Live non/Live split broadcast)---
drop table dbarnett.v250_days_right_viewable_by_account_by_live_status;
select a.account_number
,b.analysis_right
,b.live
,count(*) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status
from dbarnett.v250_days_viewing_by_account as a
left outer join dbarnett.v250_rights_broadcast_by_live_status as b
on a.viewing_date=b.broadcast_date
group by  a.account_number
,b.analysis_right
,b.live
;

commit;

--select distinct analysis_right from dbarnett.v250_days_right_viewable_by_account_by_live_status order by analysis_right
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis  where analysis_right='NFL - BBC' order by live and live=1 
--select top 100 * from dbarnett.v250_days_right_viewable_by_account_by_live_status;

---Match Days Viewable to Days broadcast to get % of Content Accounting returning data for each right/account

--select top 100 * from dbarnett.v250_rights_broadcast_overall;
--drop table #summary_by_analysis_right;
select analysis_right
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,sum(programmes_broadcast) as total_programmes_broadcast
into #summary_by_analysis_right
from dbarnett.v250_rights_broadcast_overall
group by analysis_right
;

commit;
--select * from #summary_by_analysis_right;
CREATE HG INDEX idx1 ON #summary_by_analysis_right (analysis_right);
commit;
--drop table #summary_by_analysis_right_by_live_status;
select analysis_right
,live
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,sum(programmes_broadcast) as total_programmes_broadcast
into #summary_by_analysis_right_by_live_status
from dbarnett.v250_rights_broadcast_by_live_status
group by analysis_right
,live
;
commit;
--select * from  dbarnett.v250_rights_broadcast_by_live_status where analysis_right='NFL - BBC'
--select * from #summary_by_analysis_right_by_live_status;
--select top 100 * from dbarnett.v250_days_right_viewable_by_account
commit;

alter table dbarnett.v250_days_right_viewable_by_account add days_right_broadcast integer;
alter table dbarnett.v250_days_right_viewable_by_account add right_broadcast_duration integer;
alter table dbarnett.v250_days_right_viewable_by_account add right_broadcast_programmes integer;

update dbarnett.v250_days_right_viewable_by_account
set days_right_broadcast=b.days_broadcast
,right_broadcast_duration=b.right_broadcast_duration
,right_broadcast_programmes=b.total_programmes_broadcast
from dbarnett.v250_days_right_viewable_by_account as a
left outer join #summary_by_analysis_right as b
on a.analysis_right=b.analysis_right;

commit;
--select * from dbarnett.v250_days_right_viewable_by_account where analysis_right = 'World Club Championship - BBC';

alter table dbarnett.v250_days_right_viewable_by_account_by_live_status add days_right_broadcast integer;
alter table dbarnett.v250_days_right_viewable_by_account_by_live_status add right_broadcast_duration integer;
alter table dbarnett.v250_days_right_viewable_by_account_by_live_status add right_broadcast_programmes integer;

update dbarnett.v250_days_right_viewable_by_account_by_live_status
set days_right_broadcast=b.days_broadcast
,right_broadcast_duration=b.right_broadcast_duration
,right_broadcast_programmes=b.total_programmes_broadcast
from dbarnett.v250_days_right_viewable_by_account_by_live_status as a
left outer join #summary_by_analysis_right_by_live_status as b
on a.analysis_right=b.analysis_right and a.live=b.live;

commit;


--select top 100 * from  dbarnett.v250_master_account_list as a
--select top 100 * from  dbarnett.v250_unannualised_right_activity as a
--select top 100 * from  dbarnett.v250_days_right_viewable_by_account  as a
--,cast(total_viewing_duration_all as real) * 365 / cast(total_days_with_viewing as real) as annualised_total_viewing_duration_seconds

----Convert dbarnett.v250_days_right_viewable_by_account  to one record per account----
drop table dbarnett.v250_right_viewable_account_summary;
select account_number
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_viewable else 0 end) 
as BILION_days_right_viewable
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_viewable else 0 end) 
as ENGRUG_days_right_viewable
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_viewable else 0 end) 
as CAPCUP_days_right_viewable
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_viewable else 0 end) 
as NFLSS_days_right_viewable
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_viewable else 0 end) 
as PGATR_days_right_viewable
,sum(case when analysis_right ='Rugby League - Sky Sports' then days_right_viewable else 0 end) 
as SUPLG_days_right_viewable
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_viewable else 0 end) 
as LALIGA_days_right_viewable
,sum(case when analysis_right ='Tour de France - Eurosport' then days_right_viewable else 0 end) 
as TDFEUR_days_right_viewable
,sum(case when analysis_right ='Tour de France - ITV' then days_right_viewable else 0 end) 
as TDFITV_days_right_viewable
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_viewable else 0 end) 
as USMGOLF_days_right_viewable
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_viewable else 0 end) 
as USTENSS_days_right_viewable
,sum(case when analysis_right ='US Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as USTENEUR_days_right_viewable
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_broadcast else 0 end) 
as BILION_days_right_broadcast
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as ENGRUG_days_right_broadcast
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_broadcast else 0 end) 
as CAPCUP_days_right_broadcast
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as NFLSS_days_right_broadcast
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_broadcast else 0 end) 
as PGATR_days_right_broadcast
,sum(case when analysis_right ='Rugby League - Sky Sports' then days_right_broadcast else 0 end) 
as SUPLG_days_right_broadcast
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_broadcast else 0 end) 
as LALIGA_days_right_broadcast
,sum(case when analysis_right ='Tour de France - Eurosport' then days_right_broadcast else 0 end) 
as TDFEUR_days_right_broadcast
,sum(case when analysis_right ='Tour de France - ITV' then days_right_broadcast else 0 end) 
as TDFITV_days_right_broadcast
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_broadcast else 0 end) 
as USMGOLF_days_right_broadcast
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_broadcast else 0 end) 
as USTENSS_days_right_broadcast
,sum(case when analysis_right ='US Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as USTENEUR_days_right_broadcast


,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as BILION_right_broadcast_duration
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as ENGRUG_right_broadcast_duration
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_duration else 0 end) 
as CAPCUP_right_broadcast_duration
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as NFLSS_right_broadcast_duration
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_duration else 0 end) 
as PGATR_right_broadcast_duration
,sum(case when analysis_right ='Rugby League - Sky Sports' then right_broadcast_duration else 0 end) 
as SUPLG_right_broadcast_duration
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as LALIGA_right_broadcast_duration
,sum(case when analysis_right ='Tour de France - Eurosport' then right_broadcast_duration else 0 end) 
as TDFEUR_right_broadcast_duration
,sum(case when analysis_right ='Tour de France - ITV' then right_broadcast_duration else 0 end) 
as TDFITV_right_broadcast_duration
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as USMGOLF_right_broadcast_duration
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as USTENSS_right_broadcast_duration
,sum(case when analysis_right ='US Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as USTENEUR_right_broadcast_duration



,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as BILION_right_broadcast_programmes
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ENGRUG_right_broadcast_programmes
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_programmes else 0 end) 
as CAPCUP_right_broadcast_programmes
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NFLSS_right_broadcast_programmes
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PGATR_right_broadcast_programmes
,sum(case when analysis_right ='Rugby League - Sky Sports' then right_broadcast_programmes else 0 end) 
as SUPLG_right_broadcast_programmes
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as LALIGA_right_broadcast_programmes
,sum(case when analysis_right ='Tour de France - Eurosport' then right_broadcast_programmes else 0 end) 
as TDFEUR_right_broadcast_programmes
,sum(case when analysis_right ='Tour de France - ITV' then right_broadcast_programmes else 0 end) 
as TDFITV_right_broadcast_programmes
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as USMGOLF_right_broadcast_programmes
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as USTENSS_right_broadcast_programmes
,sum(case when analysis_right ='US Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as USTENEUR_right_broadcast_programmes

into  dbarnett.v250_right_viewable_account_summary
from  dbarnett.v250_days_right_viewable_by_account 
group by account_number
;
commit;
--select count(*) from  dbarnett.v250_right_viewable_account_summary;
---Aggregate for live non live---
drop table dbarnett.v250_right_viewable_account_summary_by_live_status;
select account_number
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_viewable else 0 end) 
as BILION_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_viewable else 0 end) 
as ENGRUG_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_viewable else 0 end) 
as CAPCUP_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_viewable else 0 end) 
as NFLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_viewable else 0 end) 
as PGATR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then days_right_viewable else 0 end) 
as SUPLG_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_viewable else 0 end) 
as LALIGA_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then days_right_viewable else 0 end) 
as TDFEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then days_right_viewable else 0 end) 
as TDFITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_viewable else 0 end) 
as USMGOLF_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_viewable else 0 end) 
as USTENSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as USTENEUR_days_right_viewable_LIVE

,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_viewable else 0 end) 
as BILION_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_viewable else 0 end) 
as ENGRUG_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_viewable else 0 end) 
as CAPCUP_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_viewable else 0 end) 
as NFLSS_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_viewable else 0 end) 
as PGATR_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then days_right_viewable else 0 end) 
as SUPLG_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_viewable else 0 end) 
as LALIGA_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then days_right_viewable else 0 end) 
as TDFEUR_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then days_right_viewable else 0 end) 
as TDFITV_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_viewable else 0 end) 
as USMGOLF_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_viewable else 0 end) 
as USTENSS_days_right_viewable_NON_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as USTENEUR_days_right_viewable_NON_LIVE

,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_broadcast else 0 end) 
as BILION_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as ENGRUG_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_broadcast else 0 end) 
as CAPCUP_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as NFLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_broadcast else 0 end) 
as PGATR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then days_right_broadcast else 0 end) 
as SUPLG_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_broadcast else 0 end) 
as LALIGA_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then days_right_broadcast else 0 end) 
as TDFEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then days_right_broadcast else 0 end) 
as TDFITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_broadcast else 0 end) 
as USMGOLF_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_broadcast else 0 end) 
as USTENSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as USTENEUR_days_right_broadcast_LIVE

,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_broadcast else 0 end) 
as BILION_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as ENGRUG_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_broadcast else 0 end) 
as CAPCUP_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as NFLSS_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_broadcast else 0 end) 
as PGATR_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then days_right_broadcast else 0 end) 
as SUPLG_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_broadcast else 0 end) 
as LALIGA_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then days_right_broadcast else 0 end) 
as TDFEUR_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then days_right_broadcast else 0 end) 
as TDFITV_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_broadcast else 0 end) 
as USMGOLF_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_broadcast else 0 end) 
as USTENSS_days_right_broadcast_NON_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as USTENEUR_days_right_broadcast_NON_LIVE



,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as BILION_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as ENGRUG_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_duration else 0 end) 
as CAPCUP_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as NFLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_duration else 0 end) 
as PGATR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_duration else 0 end) 
as SUPLG_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as LALIGA_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then right_broadcast_duration else 0 end) 
as TDFEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then right_broadcast_duration else 0 end) 
as TDFITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as USMGOLF_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as USTENSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as USTENEUR_right_broadcast_duration_LIVE

,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as BILION_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as ENGRUG_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_duration else 0 end) 
as CAPCUP_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as NFLSS_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_duration else 0 end) 
as PGATR_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_duration else 0 end) 
as SUPLG_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as LALIGA_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then right_broadcast_duration else 0 end) 
as TDFEUR_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then right_broadcast_duration else 0 end) 
as TDFITV_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as USMGOLF_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as USTENSS_right_broadcast_duration_NON_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as USTENEUR_right_broadcast_duration_NON_LIVE




,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as BILION_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ENGRUG_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_programmes else 0 end) 
as CAPCUP_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NFLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PGATR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_programmes else 0 end) 
as SUPLG_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as LALIGA_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then right_broadcast_programmes else 0 end) 
as TDFEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then right_broadcast_programmes else 0 end) 
as TDFITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as USMGOLF_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as USTENSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as USTENEUR_right_broadcast_programmes_LIVE

,sum(case when live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as BILION_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ENGRUG_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_programmes else 0 end) 
as CAPCUP_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NFLSS_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PGATR_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_programmes else 0 end) 
as SUPLG_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as LALIGA_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - Eurosport' then right_broadcast_programmes else 0 end) 
as TDFEUR_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='Tour de France - ITV' then right_broadcast_programmes else 0 end) 
as TDFITV_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as USMGOLF_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as USTENSS_right_broadcast_programmes_NON_LIVE
,sum(case when live=0 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as USTENEUR_right_broadcast_programmes_NON_LIVE


into dbarnett.v250_right_viewable_account_summary_by_live_status
from  dbarnett.v250_days_right_viewable_by_account_by_live_status
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_days_right_viewable_by_account_by_live_status;
commit;

CREATE HG INDEX idx1 ON dbarnett.v250_right_viewable_account_summary (account_number);
CREATE HG INDEX idx1 ON dbarnett.v250_right_viewable_account_summary_by_live_status (account_number);
commit;

--select top 500 * from  dbarnett.v250_master_account_list as a;
--select top 500 * from dbarnett.v250_right_viewable_account_summary_by_live_status;
--select top 500 * from dbarnett.v250_unannualised_right_activity as b

-----
--drop table dbarnett.v250_annualised_activity_table_for_workshop_v2;
select a.account_number
,total_days_with_viewing
, cast(total_viewing_duration_all as real)/60 * 365/total_days_with_viewing as annualised_total_viewing_duration_all
, cast(total_viewing_duration_sports as real)/60 * 365/total_days_with_viewing as annualised_total_viewing_duration_sports
,30 as account_weight

,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_seconds_viewed_NON_LIVE / cast(e.BILION_days_right_viewable_NON_LIVE as real)/60* cast(e.BILION_days_right_broadcast_NON_LIVE as real) end as Totm_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_seconds_viewed_LIVE / cast(e.BILION_days_right_viewable_LIVE as real)/60* cast(e.BILION_days_right_broadcast_LIVE as real) end as Totm_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_seconds_viewed / cast(c.BILION_days_right_viewable as real)/60* cast(c.BILION_days_right_broadcast as real) end as Totm_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_NON_LIVE / cast(e.ENGRUG_days_right_viewable_NON_LIVE as real)/60* cast(e.ENGRUG_days_right_broadcast_NON_LIVE as real) end as Totm_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_LIVE / cast(e.ENGRUG_days_right_viewable_LIVE as real)/60* cast(e.ENGRUG_days_right_broadcast_LIVE as real) end as Totm_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_seconds_viewed / cast(c.ENGRUG_days_right_viewable as real)/60* cast(c.ENGRUG_days_right_broadcast as real) end as Totm_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_NON_LIVE / cast(e.CAPCUP_days_right_viewable_NON_LIVE as real)/60* cast(e.CAPCUP_days_right_broadcast_NON_LIVE as real) end as Totm_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_LIVE / cast(e.CAPCUP_days_right_viewable_LIVE as real)/60* cast(e.CAPCUP_days_right_broadcast_LIVE as real) end as Totm_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_seconds_viewed / cast(c.CAPCUP_days_right_viewable as real)/60* cast(c.CAPCUP_days_right_broadcast as real) end as Totm_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_NON_LIVE / cast(e.NFLSS_days_right_viewable_NON_LIVE as real)/60* cast(e.NFLSS_days_right_broadcast_NON_LIVE as real) end as Totm_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_LIVE / cast(e.NFLSS_days_right_viewable_LIVE as real)/60* cast(e.NFLSS_days_right_broadcast_LIVE as real) end as Totm_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_seconds_viewed / cast(c.NFLSS_days_right_viewable as real)/60* cast(c.NFLSS_days_right_broadcast as real) end as Totm_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_NON_LIVE / cast(e.PGATR_days_right_viewable_NON_LIVE as real)/60* cast(e.PGATR_days_right_broadcast_NON_LIVE as real) end as Totm_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_LIVE / cast(e.PGATR_days_right_viewable_LIVE as real)/60* cast(e.PGATR_days_right_broadcast_LIVE as real) end as Totm_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_seconds_viewed / cast(c.PGATR_days_right_viewable as real)/60* cast(c.PGATR_days_right_broadcast as real) end as Totm_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_NON_LIVE / cast(e.SUPLG_days_right_viewable_NON_LIVE as real)/60* cast(e.SUPLG_days_right_broadcast_NON_LIVE as real) end as Totm_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_LIVE / cast(e.SUPLG_days_right_viewable_LIVE as real)/60* cast(e.SUPLG_days_right_broadcast_LIVE as real) end as Totm_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_seconds_viewed / cast(c.SUPLG_days_right_viewable as real)/60* cast(c.SUPLG_days_right_broadcast as real) end as Totm_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_NON_LIVE / cast(e.LALIGA_days_right_viewable_NON_LIVE as real)/60* cast(e.LALIGA_days_right_broadcast_NON_LIVE as real) end as Totm_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_LIVE / cast(e.LALIGA_days_right_viewable_LIVE as real)/60* cast(e.LALIGA_days_right_broadcast_LIVE as real) end as Totm_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_seconds_viewed / cast(c.LALIGA_days_right_viewable as real)/60* cast(c.LALIGA_days_right_broadcast as real) end as Totm_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_NON_LIVE / cast(e.TDFEUR_days_right_viewable_NON_LIVE as real)/60* cast(e.TDFEUR_days_right_broadcast_NON_LIVE as real) end as Totm_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_LIVE / cast(e.TDFEUR_days_right_viewable_LIVE as real)/60* cast(e.TDFEUR_days_right_broadcast_LIVE as real) end as Totm_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_seconds_viewed / cast(c.TDFEUR_days_right_viewable as real)/60* cast(c.TDFEUR_days_right_broadcast as real) end as Totm_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_NON_LIVE / cast(e.TDFITV_days_right_viewable_NON_LIVE as real)/60* cast(e.TDFITV_days_right_broadcast_NON_LIVE as real) end as Totm_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_LIVE / cast(e.TDFITV_days_right_viewable_LIVE as real)/60* cast(e.TDFITV_days_right_broadcast_LIVE as real) end as Totm_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_seconds_viewed / cast(c.TDFITV_days_right_viewable as real)/60* cast(c.TDFITV_days_right_broadcast as real) end as Totm_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_NON_LIVE / cast(e.USMGOLF_days_right_viewable_NON_LIVE as real)/60* cast(e.USMGOLF_days_right_broadcast_NON_LIVE as real) end as Totm_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_LIVE / cast(e.USMGOLF_days_right_viewable_LIVE as real)/60* cast(e.USMGOLF_days_right_broadcast_LIVE as real) end as Totm_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_seconds_viewed / cast(c.USMGOLF_days_right_viewable as real)/60* cast(c.USMGOLF_days_right_broadcast as real) end as Totm_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_NON_LIVE / cast(e.USTENSS_days_right_viewable_NON_LIVE as real)/60* cast(e.USTENSS_days_right_broadcast_NON_LIVE as real) end as Totm_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_LIVE / cast(e.USTENSS_days_right_viewable_LIVE as real)/60* cast(e.USTENSS_days_right_broadcast_LIVE as real) end as Totm_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_seconds_viewed / cast(c.USTENSS_days_right_viewable as real)/60* cast(c.USTENSS_days_right_broadcast as real) end as Totm_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_NON_LIVE / cast(e.USTENEUR_days_right_viewable_NON_LIVE as real)/60* cast(e.USTENEUR_days_right_broadcast_NON_LIVE as real) end as Totm_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_LIVE / cast(e.USTENEUR_days_right_viewable_LIVE as real)/60* cast(e.USTENEUR_days_right_broadcast_LIVE as real) end as Totm_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_seconds_viewed / cast(c.USTENEUR_days_right_viewable as real)/60* cast(c.USTENEUR_days_right_broadcast as real) end as Totm_A_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_programmes_viewed_over_threshold_NON_LIVE / cast(e.BILION_days_right_viewable_NON_LIVE as real)* cast(e.BILION_days_right_broadcast_NON_LIVE as real) end as PV_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_programmes_viewed_over_threshold_LIVE / cast(e.BILION_days_right_viewable_LIVE as real)* cast(e.BILION_days_right_broadcast_LIVE as real) end as PV_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_programmes_viewed_over_threshold / cast(c.BILION_days_right_viewable as real)* cast(c.BILION_days_right_broadcast as real) end as PV_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_programmes_viewed_over_threshold_NON_LIVE / cast(e.ENGRUG_days_right_viewable_NON_LIVE as real)* cast(e.ENGRUG_days_right_broadcast_NON_LIVE as real) end as PV_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_programmes_viewed_over_threshold_LIVE / cast(e.ENGRUG_days_right_viewable_LIVE as real)* cast(e.ENGRUG_days_right_broadcast_LIVE as real) end as PV_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_programmes_viewed_over_threshold / cast(c.ENGRUG_days_right_viewable as real)* cast(c.ENGRUG_days_right_broadcast as real) end as PV_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_programmes_viewed_over_threshold_NON_LIVE / cast(e.CAPCUP_days_right_viewable_NON_LIVE as real)* cast(e.CAPCUP_days_right_broadcast_NON_LIVE as real) end as PV_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_programmes_viewed_over_threshold_LIVE / cast(e.CAPCUP_days_right_viewable_LIVE as real)* cast(e.CAPCUP_days_right_broadcast_LIVE as real) end as PV_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_programmes_viewed_over_threshold / cast(c.CAPCUP_days_right_viewable as real)* cast(c.CAPCUP_days_right_broadcast as real) end as PV_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_programmes_viewed_over_threshold_NON_LIVE / cast(e.NFLSS_days_right_viewable_NON_LIVE as real)* cast(e.NFLSS_days_right_broadcast_NON_LIVE as real) end as PV_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_programmes_viewed_over_threshold_LIVE / cast(e.NFLSS_days_right_viewable_LIVE as real)* cast(e.NFLSS_days_right_broadcast_LIVE as real) end as PV_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_programmes_viewed_over_threshold / cast(c.NFLSS_days_right_viewable as real)* cast(c.NFLSS_days_right_broadcast as real) end as PV_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_programmes_viewed_over_threshold_NON_LIVE / cast(e.PGATR_days_right_viewable_NON_LIVE as real)* cast(e.PGATR_days_right_broadcast_NON_LIVE as real) end as PV_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_programmes_viewed_over_threshold_LIVE / cast(e.PGATR_days_right_viewable_LIVE as real)* cast(e.PGATR_days_right_broadcast_LIVE as real) end as PV_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_programmes_viewed_over_threshold / cast(c.PGATR_days_right_viewable as real)* cast(c.PGATR_days_right_broadcast as real) end as PV_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_programmes_viewed_over_threshold_NON_LIVE / cast(e.SUPLG_days_right_viewable_NON_LIVE as real)* cast(e.SUPLG_days_right_broadcast_NON_LIVE as real) end as PV_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_programmes_viewed_over_threshold_LIVE / cast(e.SUPLG_days_right_viewable_LIVE as real)* cast(e.SUPLG_days_right_broadcast_LIVE as real) end as PV_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_programmes_viewed_over_threshold / cast(c.SUPLG_days_right_viewable as real)* cast(c.SUPLG_days_right_broadcast as real) end as PV_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_programmes_viewed_over_threshold_NON_LIVE / cast(e.LALIGA_days_right_viewable_NON_LIVE as real)* cast(e.LALIGA_days_right_broadcast_NON_LIVE as real) end as PV_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_programmes_viewed_over_threshold_LIVE / cast(e.LALIGA_days_right_viewable_LIVE as real)* cast(e.LALIGA_days_right_broadcast_LIVE as real) end as PV_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_programmes_viewed_over_threshold / cast(c.LALIGA_days_right_viewable as real)* cast(c.LALIGA_days_right_broadcast as real) end as PV_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_programmes_viewed_over_threshold_NON_LIVE / cast(e.TDFEUR_days_right_viewable_NON_LIVE as real)* cast(e.TDFEUR_days_right_broadcast_NON_LIVE as real) end as PV_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_programmes_viewed_over_threshold_LIVE / cast(e.TDFEUR_days_right_viewable_LIVE as real)* cast(e.TDFEUR_days_right_broadcast_LIVE as real) end as PV_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_programmes_viewed_over_threshold / cast(c.TDFEUR_days_right_viewable as real)* cast(c.TDFEUR_days_right_broadcast as real) end as PV_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_programmes_viewed_over_threshold_NON_LIVE / cast(e.TDFITV_days_right_viewable_NON_LIVE as real)* cast(e.TDFITV_days_right_broadcast_NON_LIVE as real) end as PV_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_programmes_viewed_over_threshold_LIVE / cast(e.TDFITV_days_right_viewable_LIVE as real)* cast(e.TDFITV_days_right_broadcast_LIVE as real) end as PV_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_programmes_viewed_over_threshold / cast(c.TDFITV_days_right_viewable as real)* cast(c.TDFITV_days_right_broadcast as real) end as PV_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_programmes_viewed_over_threshold_NON_LIVE / cast(e.USMGOLF_days_right_viewable_NON_LIVE as real)* cast(e.USMGOLF_days_right_broadcast_NON_LIVE as real) end as PV_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_programmes_viewed_over_threshold_LIVE / cast(e.USMGOLF_days_right_viewable_LIVE as real)* cast(e.USMGOLF_days_right_broadcast_LIVE as real) end as PV_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_programmes_viewed_over_threshold / cast(c.USMGOLF_days_right_viewable as real)* cast(c.USMGOLF_days_right_broadcast as real) end as PV_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_programmes_viewed_over_threshold_NON_LIVE / cast(e.USTENSS_days_right_viewable_NON_LIVE as real)* cast(e.USTENSS_days_right_broadcast_NON_LIVE as real) end as PV_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_programmes_viewed_over_threshold_LIVE / cast(e.USTENSS_days_right_viewable_LIVE as real)* cast(e.USTENSS_days_right_broadcast_LIVE as real) end as PV_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_programmes_viewed_over_threshold / cast(c.USTENSS_days_right_viewable as real)* cast(c.USTENSS_days_right_broadcast as real) end as PV_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_programmes_viewed_over_threshold_NON_LIVE / cast(e.USTENEUR_days_right_viewable_NON_LIVE as real)* cast(e.USTENEUR_days_right_broadcast_NON_LIVE as real) end as PV_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_programmes_viewed_over_threshold_LIVE / cast(e.USTENEUR_days_right_viewable_LIVE as real)* cast(e.USTENEUR_days_right_broadcast_LIVE as real) end as PV_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_programmes_viewed_over_threshold / cast(c.USTENEUR_days_right_viewable as real)* cast(c.USTENEUR_days_right_broadcast as real) end as PV_A_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_viewing_events_NON_LIVE / cast(e.BILION_days_right_viewable_NON_LIVE as real)* cast(e.BILION_days_right_broadcast_NON_LIVE as real) end as VE_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_viewing_events_LIVE / cast(e.BILION_days_right_viewable_LIVE as real)* cast(e.BILION_days_right_broadcast_LIVE as real) end as VE_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_viewing_events / cast(c.BILION_days_right_viewable as real)* cast(c.BILION_days_right_broadcast as real) end as VE_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_viewing_events_NON_LIVE / cast(e.ENGRUG_days_right_viewable_NON_LIVE as real)* cast(e.ENGRUG_days_right_broadcast_NON_LIVE as real) end as VE_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_viewing_events_LIVE / cast(e.ENGRUG_days_right_viewable_LIVE as real)* cast(e.ENGRUG_days_right_broadcast_LIVE as real) end as VE_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_viewing_events / cast(c.ENGRUG_days_right_viewable as real)* cast(c.ENGRUG_days_right_broadcast as real) end as VE_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_viewing_events_NON_LIVE / cast(e.CAPCUP_days_right_viewable_NON_LIVE as real)* cast(e.CAPCUP_days_right_broadcast_NON_LIVE as real) end as VE_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_viewing_events_LIVE / cast(e.CAPCUP_days_right_viewable_LIVE as real)* cast(e.CAPCUP_days_right_broadcast_LIVE as real) end as VE_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_viewing_events / cast(c.CAPCUP_days_right_viewable as real)* cast(c.CAPCUP_days_right_broadcast as real) end as VE_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_viewing_events_NON_LIVE / cast(e.NFLSS_days_right_viewable_NON_LIVE as real)* cast(e.NFLSS_days_right_broadcast_NON_LIVE as real) end as VE_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_viewing_events_LIVE / cast(e.NFLSS_days_right_viewable_LIVE as real)* cast(e.NFLSS_days_right_broadcast_LIVE as real) end as VE_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_viewing_events / cast(c.NFLSS_days_right_viewable as real)* cast(c.NFLSS_days_right_broadcast as real) end as VE_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_viewing_events_NON_LIVE / cast(e.PGATR_days_right_viewable_NON_LIVE as real)* cast(e.PGATR_days_right_broadcast_NON_LIVE as real) end as VE_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_viewing_events_LIVE / cast(e.PGATR_days_right_viewable_LIVE as real)* cast(e.PGATR_days_right_broadcast_LIVE as real) end as VE_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_viewing_events / cast(c.PGATR_days_right_viewable as real)* cast(c.PGATR_days_right_broadcast as real) end as VE_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_viewing_events_NON_LIVE / cast(e.SUPLG_days_right_viewable_NON_LIVE as real)* cast(e.SUPLG_days_right_broadcast_NON_LIVE as real) end as VE_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_viewing_events_LIVE / cast(e.SUPLG_days_right_viewable_LIVE as real)* cast(e.SUPLG_days_right_broadcast_LIVE as real) end as VE_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_viewing_events / cast(c.SUPLG_days_right_viewable as real)* cast(c.SUPLG_days_right_broadcast as real) end as VE_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_viewing_events_NON_LIVE / cast(e.LALIGA_days_right_viewable_NON_LIVE as real)* cast(e.LALIGA_days_right_broadcast_NON_LIVE as real) end as VE_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_viewing_events_LIVE / cast(e.LALIGA_days_right_viewable_LIVE as real)* cast(e.LALIGA_days_right_broadcast_LIVE as real) end as VE_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_viewing_events / cast(c.LALIGA_days_right_viewable as real)* cast(c.LALIGA_days_right_broadcast as real) end as VE_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_viewing_events_NON_LIVE / cast(e.TDFEUR_days_right_viewable_NON_LIVE as real)* cast(e.TDFEUR_days_right_broadcast_NON_LIVE as real) end as VE_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_viewing_events_LIVE / cast(e.TDFEUR_days_right_viewable_LIVE as real)* cast(e.TDFEUR_days_right_broadcast_LIVE as real) end as VE_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_viewing_events / cast(c.TDFEUR_days_right_viewable as real)* cast(c.TDFEUR_days_right_broadcast as real) end as VE_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_viewing_events_NON_LIVE / cast(e.TDFITV_days_right_viewable_NON_LIVE as real)* cast(e.TDFITV_days_right_broadcast_NON_LIVE as real) end as VE_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_viewing_events_LIVE / cast(e.TDFITV_days_right_viewable_LIVE as real)* cast(e.TDFITV_days_right_broadcast_LIVE as real) end as VE_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_viewing_events / cast(c.TDFITV_days_right_viewable as real)* cast(c.TDFITV_days_right_broadcast as real) end as VE_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_viewing_events_NON_LIVE / cast(e.USMGOLF_days_right_viewable_NON_LIVE as real)* cast(e.USMGOLF_days_right_broadcast_NON_LIVE as real) end as VE_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_viewing_events_LIVE / cast(e.USMGOLF_days_right_viewable_LIVE as real)* cast(e.USMGOLF_days_right_broadcast_LIVE as real) end as VE_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_viewing_events / cast(c.USMGOLF_days_right_viewable as real)* cast(c.USMGOLF_days_right_broadcast as real) end as VE_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_viewing_events_NON_LIVE / cast(e.USTENSS_days_right_viewable_NON_LIVE as real)* cast(e.USTENSS_days_right_broadcast_NON_LIVE as real) end as VE_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_viewing_events_LIVE / cast(e.USTENSS_days_right_viewable_LIVE as real)* cast(e.USTENSS_days_right_broadcast_LIVE as real) end as VE_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_viewing_events / cast(c.USTENSS_days_right_viewable as real)* cast(c.USTENSS_days_right_broadcast as real) end as VE_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_viewing_events_NON_LIVE / cast(e.USTENEUR_days_right_viewable_NON_LIVE as real)* cast(e.USTENEUR_days_right_broadcast_NON_LIVE as real) end as VE_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_viewing_events_LIVE / cast(e.USTENEUR_days_right_viewable_LIVE as real)* cast(e.USTENEUR_days_right_broadcast_LIVE as real) end as VE_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_viewing_events / cast(c.USTENEUR_days_right_viewable as real)* cast(c.USTENEUR_days_right_broadcast as real) end as VE_A_USTENEUR


,case when d.BILION_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.BILION_total_viewing_events_NON_LIVE=0 then 0  when d.BILION_total_viewing_events_NON_LIVE is null then 0  when d.BILION_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_viewing_events_NON_LIVE / cast(d.BILION_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_BILION
,case when d.BILION_programmes_viewed_over_threshold_LIVE=0 then 0  when d.BILION_programmes_viewed_over_threshold_LIVE=0 then 0  when d.BILION_programmes_viewed_over_threshold_LIVE is null then 0  when d.BILION_programmes_viewed_over_threshold_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_viewing_events_LIVE / cast(d.BILION_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_BILION
,case when b.BILION_programmes_viewed_over_threshold=0 then 0  when b.BILION_programmes_viewed_over_threshold=0 then 0  when b.BILION_programmes_viewed_over_threshold is null then 0  when b.BILION_programmes_viewed_over_threshold is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_viewing_events / cast(b.BILION_programmes_viewed_over_threshold as real) end as AVE_A_BILION
,case when d.ENGRUG_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.ENGRUG_total_viewing_events_NON_LIVE=0 then 0  when d.ENGRUG_total_viewing_events_NON_LIVE is null then 0  when d.ENGRUG_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_viewing_events_NON_LIVE / cast(d.ENGRUG_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_ENGRUG
,case when d.ENGRUG_programmes_viewed_over_threshold_LIVE=0 then 0  when d.ENGRUG_programmes_viewed_over_threshold_LIVE=0 then 0  when d.ENGRUG_programmes_viewed_over_threshold_LIVE is null then 0  when d.ENGRUG_programmes_viewed_over_threshold_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_viewing_events_LIVE / cast(d.ENGRUG_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_ENGRUG
,case when b.ENGRUG_programmes_viewed_over_threshold=0 then 0  when b.ENGRUG_programmes_viewed_over_threshold=0 then 0  when b.ENGRUG_programmes_viewed_over_threshold is null then 0  when b.ENGRUG_programmes_viewed_over_threshold is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_viewing_events / cast(b.ENGRUG_programmes_viewed_over_threshold as real) end as AVE_A_ENGRUG
,case when d.CAPCUP_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.CAPCUP_total_viewing_events_NON_LIVE=0 then 0  when d.CAPCUP_total_viewing_events_NON_LIVE is null then 0  when d.CAPCUP_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_viewing_events_NON_LIVE / cast(d.CAPCUP_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_CAPCUP
,case when d.CAPCUP_programmes_viewed_over_threshold_LIVE=0 then 0  when d.CAPCUP_programmes_viewed_over_threshold_LIVE=0 then 0  when d.CAPCUP_programmes_viewed_over_threshold_LIVE is null then 0  when d.CAPCUP_programmes_viewed_over_threshold_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_viewing_events_LIVE / cast(d.CAPCUP_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_CAPCUP
,case when b.CAPCUP_programmes_viewed_over_threshold=0 then 0  when b.CAPCUP_programmes_viewed_over_threshold=0 then 0  when b.CAPCUP_programmes_viewed_over_threshold is null then 0  when b.CAPCUP_programmes_viewed_over_threshold is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_viewing_events / cast(b.CAPCUP_programmes_viewed_over_threshold as real) end as AVE_A_CAPCUP
,case when d.NFLSS_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.NFLSS_total_viewing_events_NON_LIVE=0 then 0  when d.NFLSS_total_viewing_events_NON_LIVE is null then 0  when d.NFLSS_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_viewing_events_NON_LIVE / cast(d.NFLSS_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_NFLSS
,case when d.NFLSS_programmes_viewed_over_threshold_LIVE=0 then 0  when d.NFLSS_programmes_viewed_over_threshold_LIVE=0 then 0  when d.NFLSS_programmes_viewed_over_threshold_LIVE is null then 0  when d.NFLSS_programmes_viewed_over_threshold_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_viewing_events_LIVE / cast(d.NFLSS_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_NFLSS
,case when b.NFLSS_programmes_viewed_over_threshold=0 then 0  when b.NFLSS_programmes_viewed_over_threshold=0 then 0  when b.NFLSS_programmes_viewed_over_threshold is null then 0  when b.NFLSS_programmes_viewed_over_threshold is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_viewing_events / cast(b.NFLSS_programmes_viewed_over_threshold as real) end as AVE_A_NFLSS
,case when d.PGATR_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.PGATR_total_viewing_events_NON_LIVE=0 then 0  when d.PGATR_total_viewing_events_NON_LIVE is null then 0  when d.PGATR_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_viewing_events_NON_LIVE / cast(d.PGATR_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_PGATR
,case when d.PGATR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.PGATR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.PGATR_programmes_viewed_over_threshold_LIVE is null then 0  when d.PGATR_programmes_viewed_over_threshold_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_viewing_events_LIVE / cast(d.PGATR_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_PGATR
,case when b.PGATR_programmes_viewed_over_threshold=0 then 0  when b.PGATR_programmes_viewed_over_threshold=0 then 0  when b.PGATR_programmes_viewed_over_threshold is null then 0  when b.PGATR_programmes_viewed_over_threshold is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_viewing_events / cast(b.PGATR_programmes_viewed_over_threshold as real) end as AVE_A_PGATR
,case when d.SUPLG_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.SUPLG_total_viewing_events_NON_LIVE=0 then 0  when d.SUPLG_total_viewing_events_NON_LIVE is null then 0  when d.SUPLG_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_viewing_events_NON_LIVE / cast(d.SUPLG_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_SUPLG
,case when d.SUPLG_programmes_viewed_over_threshold_LIVE=0 then 0  when d.SUPLG_programmes_viewed_over_threshold_LIVE=0 then 0  when d.SUPLG_programmes_viewed_over_threshold_LIVE is null then 0  when d.SUPLG_programmes_viewed_over_threshold_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_viewing_events_LIVE / cast(d.SUPLG_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_SUPLG
,case when b.SUPLG_programmes_viewed_over_threshold=0 then 0  when b.SUPLG_programmes_viewed_over_threshold=0 then 0  when b.SUPLG_programmes_viewed_over_threshold is null then 0  when b.SUPLG_programmes_viewed_over_threshold is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_viewing_events / cast(b.SUPLG_programmes_viewed_over_threshold as real) end as AVE_A_SUPLG
,case when d.LALIGA_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.LALIGA_total_viewing_events_NON_LIVE=0 then 0  when d.LALIGA_total_viewing_events_NON_LIVE is null then 0  when d.LALIGA_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_viewing_events_NON_LIVE / cast(d.LALIGA_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_LALIGA
,case when d.LALIGA_programmes_viewed_over_threshold_LIVE=0 then 0  when d.LALIGA_programmes_viewed_over_threshold_LIVE=0 then 0  when d.LALIGA_programmes_viewed_over_threshold_LIVE is null then 0  when d.LALIGA_programmes_viewed_over_threshold_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_viewing_events_LIVE / cast(d.LALIGA_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_LALIGA
,case when b.LALIGA_programmes_viewed_over_threshold=0 then 0  when b.LALIGA_programmes_viewed_over_threshold=0 then 0  when b.LALIGA_programmes_viewed_over_threshold is null then 0  when b.LALIGA_programmes_viewed_over_threshold is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_viewing_events / cast(b.LALIGA_programmes_viewed_over_threshold as real) end as AVE_A_LALIGA
,case when d.TDFEUR_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.TDFEUR_total_viewing_events_NON_LIVE=0 then 0  when d.TDFEUR_total_viewing_events_NON_LIVE is null then 0  when d.TDFEUR_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_viewing_events_NON_LIVE / cast(d.TDFEUR_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_TDFEUR
,case when d.TDFEUR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.TDFEUR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.TDFEUR_programmes_viewed_over_threshold_LIVE is null then 0  when d.TDFEUR_programmes_viewed_over_threshold_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_viewing_events_LIVE / cast(d.TDFEUR_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_TDFEUR
,case when b.TDFEUR_programmes_viewed_over_threshold=0 then 0  when b.TDFEUR_programmes_viewed_over_threshold=0 then 0  when b.TDFEUR_programmes_viewed_over_threshold is null then 0  when b.TDFEUR_programmes_viewed_over_threshold is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_viewing_events / cast(b.TDFEUR_programmes_viewed_over_threshold as real) end as AVE_A_TDFEUR
,case when d.TDFITV_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.TDFITV_total_viewing_events_NON_LIVE=0 then 0  when d.TDFITV_total_viewing_events_NON_LIVE is null then 0  when d.TDFITV_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_viewing_events_NON_LIVE / cast(d.TDFITV_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_TDFITV
,case when d.TDFITV_programmes_viewed_over_threshold_LIVE=0 then 0  when d.TDFITV_programmes_viewed_over_threshold_LIVE=0 then 0  when d.TDFITV_programmes_viewed_over_threshold_LIVE is null then 0  when d.TDFITV_programmes_viewed_over_threshold_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_viewing_events_LIVE / cast(d.TDFITV_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_TDFITV
,case when b.TDFITV_programmes_viewed_over_threshold=0 then 0  when b.TDFITV_programmes_viewed_over_threshold=0 then 0  when b.TDFITV_programmes_viewed_over_threshold is null then 0  when b.TDFITV_programmes_viewed_over_threshold is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_viewing_events / cast(b.TDFITV_programmes_viewed_over_threshold as real) end as AVE_A_TDFITV
,case when d.USMGOLF_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.USMGOLF_total_viewing_events_NON_LIVE=0 then 0  when d.USMGOLF_total_viewing_events_NON_LIVE is null then 0  when d.USMGOLF_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_viewing_events_NON_LIVE / cast(d.USMGOLF_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_USMGOLF
,case when d.USMGOLF_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USMGOLF_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USMGOLF_programmes_viewed_over_threshold_LIVE is null then 0  when d.USMGOLF_programmes_viewed_over_threshold_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_viewing_events_LIVE / cast(d.USMGOLF_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_USMGOLF
,case when b.USMGOLF_programmes_viewed_over_threshold=0 then 0  when b.USMGOLF_programmes_viewed_over_threshold=0 then 0  when b.USMGOLF_programmes_viewed_over_threshold is null then 0  when b.USMGOLF_programmes_viewed_over_threshold is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_viewing_events / cast(b.USMGOLF_programmes_viewed_over_threshold as real) end as AVE_A_USMGOLF
,case when d.USTENSS_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.USTENSS_total_viewing_events_NON_LIVE=0 then 0  when d.USTENSS_total_viewing_events_NON_LIVE is null then 0  when d.USTENSS_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_viewing_events_NON_LIVE / cast(d.USTENSS_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_USTENSS
,case when d.USTENSS_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USTENSS_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USTENSS_programmes_viewed_over_threshold_LIVE is null then 0  when d.USTENSS_programmes_viewed_over_threshold_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_viewing_events_LIVE / cast(d.USTENSS_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_USTENSS
,case when b.USTENSS_programmes_viewed_over_threshold=0 then 0  when b.USTENSS_programmes_viewed_over_threshold=0 then 0  when b.USTENSS_programmes_viewed_over_threshold is null then 0  when b.USTENSS_programmes_viewed_over_threshold is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_viewing_events / cast(b.USTENSS_programmes_viewed_over_threshold as real) end as AVE_A_USTENSS
,case when d.USTENEUR_programmes_viewed_over_threshold_NON_LIVE=0 then 0  when d.USTENEUR_total_viewing_events_NON_LIVE=0 then 0  when d.USTENEUR_total_viewing_events_NON_LIVE is null then 0  when d.USTENEUR_programmes_viewed_over_threshold_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_viewing_events_NON_LIVE / cast(d.USTENEUR_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_USTENEUR
,case when d.USTENEUR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USTENEUR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USTENEUR_programmes_viewed_over_threshold_LIVE is null then 0  when d.USTENEUR_programmes_viewed_over_threshold_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_viewing_events_LIVE / cast(d.USTENEUR_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_USTENEUR
,case when b.USTENEUR_programmes_viewed_over_threshold=0 then 0  when b.USTENEUR_programmes_viewed_over_threshold=0 then 0  when b.USTENEUR_programmes_viewed_over_threshold is null then 0  when b.USTENEUR_programmes_viewed_over_threshold is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_viewing_events / cast(b.USTENEUR_programmes_viewed_over_threshold as real) end as AVE_A_USTENEUR


,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_broadcast_days_viewed_NON_LIVE / cast(e.BILION_days_right_viewable_NON_LIVE as real)* cast(e.BILION_days_right_broadcast_NON_LIVE as real) end as Days_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_broadcast_days_viewed_LIVE / cast(e.BILION_days_right_viewable_LIVE as real)* cast(e.BILION_days_right_broadcast_LIVE as real) end as Days_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_broadcast_days_viewed / cast(c.BILION_days_right_viewable as real)* cast(c.BILION_days_right_broadcast as real) end as Days_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_broadcast_days_viewed_NON_LIVE / cast(e.ENGRUG_days_right_viewable_NON_LIVE as real)* cast(e.ENGRUG_days_right_broadcast_NON_LIVE as real) end as Days_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_broadcast_days_viewed_LIVE / cast(e.ENGRUG_days_right_viewable_LIVE as real)* cast(e.ENGRUG_days_right_broadcast_LIVE as real) end as Days_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_broadcast_days_viewed / cast(c.ENGRUG_days_right_viewable as real)* cast(c.ENGRUG_days_right_broadcast as real) end as Days_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_broadcast_days_viewed_NON_LIVE / cast(e.CAPCUP_days_right_viewable_NON_LIVE as real)* cast(e.CAPCUP_days_right_broadcast_NON_LIVE as real) end as Days_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_broadcast_days_viewed_LIVE / cast(e.CAPCUP_days_right_viewable_LIVE as real)* cast(e.CAPCUP_days_right_broadcast_LIVE as real) end as Days_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_broadcast_days_viewed / cast(c.CAPCUP_days_right_viewable as real)* cast(c.CAPCUP_days_right_broadcast as real) end as Days_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_broadcast_days_viewed_NON_LIVE / cast(e.NFLSS_days_right_viewable_NON_LIVE as real)* cast(e.NFLSS_days_right_broadcast_NON_LIVE as real) end as Days_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_broadcast_days_viewed_LIVE / cast(e.NFLSS_days_right_viewable_LIVE as real)* cast(e.NFLSS_days_right_broadcast_LIVE as real) end as Days_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_broadcast_days_viewed / cast(c.NFLSS_days_right_viewable as real)* cast(c.NFLSS_days_right_broadcast as real) end as Days_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_broadcast_days_viewed_NON_LIVE / cast(e.PGATR_days_right_viewable_NON_LIVE as real)* cast(e.PGATR_days_right_broadcast_NON_LIVE as real) end as Days_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_broadcast_days_viewed_LIVE / cast(e.PGATR_days_right_viewable_LIVE as real)* cast(e.PGATR_days_right_broadcast_LIVE as real) end as Days_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_broadcast_days_viewed / cast(c.PGATR_days_right_viewable as real)* cast(c.PGATR_days_right_broadcast as real) end as Days_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_broadcast_days_viewed_NON_LIVE / cast(e.SUPLG_days_right_viewable_NON_LIVE as real)* cast(e.SUPLG_days_right_broadcast_NON_LIVE as real) end as Days_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_broadcast_days_viewed_LIVE / cast(e.SUPLG_days_right_viewable_LIVE as real)* cast(e.SUPLG_days_right_broadcast_LIVE as real) end as Days_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_broadcast_days_viewed / cast(c.SUPLG_days_right_viewable as real)* cast(c.SUPLG_days_right_broadcast as real) end as Days_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_broadcast_days_viewed_NON_LIVE / cast(e.LALIGA_days_right_viewable_NON_LIVE as real)* cast(e.LALIGA_days_right_broadcast_NON_LIVE as real) end as Days_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_broadcast_days_viewed_LIVE / cast(e.LALIGA_days_right_viewable_LIVE as real)* cast(e.LALIGA_days_right_broadcast_LIVE as real) end as Days_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_broadcast_days_viewed / cast(c.LALIGA_days_right_viewable as real)* cast(c.LALIGA_days_right_broadcast as real) end as Days_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_broadcast_days_viewed_NON_LIVE / cast(e.TDFEUR_days_right_viewable_NON_LIVE as real)* cast(e.TDFEUR_days_right_broadcast_NON_LIVE as real) end as Days_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_broadcast_days_viewed_LIVE / cast(e.TDFEUR_days_right_viewable_LIVE as real)* cast(e.TDFEUR_days_right_broadcast_LIVE as real) end as Days_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_broadcast_days_viewed / cast(c.TDFEUR_days_right_viewable as real)* cast(c.TDFEUR_days_right_broadcast as real) end as Days_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_broadcast_days_viewed_NON_LIVE / cast(e.TDFITV_days_right_viewable_NON_LIVE as real)* cast(e.TDFITV_days_right_broadcast_NON_LIVE as real) end as Days_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_broadcast_days_viewed_LIVE / cast(e.TDFITV_days_right_viewable_LIVE as real)* cast(e.TDFITV_days_right_broadcast_LIVE as real) end as Days_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_broadcast_days_viewed / cast(c.TDFITV_days_right_viewable as real)* cast(c.TDFITV_days_right_broadcast as real) end as Days_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_broadcast_days_viewed_NON_LIVE / cast(e.USMGOLF_days_right_viewable_NON_LIVE as real)* cast(e.USMGOLF_days_right_broadcast_NON_LIVE as real) end as Days_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_broadcast_days_viewed_LIVE / cast(e.USMGOLF_days_right_viewable_LIVE as real)* cast(e.USMGOLF_days_right_broadcast_LIVE as real) end as Days_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_broadcast_days_viewed / cast(c.USMGOLF_days_right_viewable as real)* cast(c.USMGOLF_days_right_broadcast as real) end as Days_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_broadcast_days_viewed_NON_LIVE / cast(e.USTENSS_days_right_viewable_NON_LIVE as real)* cast(e.USTENSS_days_right_broadcast_NON_LIVE as real) end as Days_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_broadcast_days_viewed_LIVE / cast(e.USTENSS_days_right_viewable_LIVE as real)* cast(e.USTENSS_days_right_broadcast_LIVE as real) end as Days_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_broadcast_days_viewed / cast(c.USTENSS_days_right_viewable as real)* cast(c.USTENSS_days_right_broadcast as real) end as Days_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_broadcast_days_viewed_NON_LIVE / cast(e.USTENEUR_days_right_viewable_NON_LIVE as real)* cast(e.USTENEUR_days_right_broadcast_NON_LIVE as real) end as Days_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_broadcast_days_viewed_LIVE / cast(e.USTENEUR_days_right_viewable_LIVE as real)* cast(e.USTENEUR_days_right_broadcast_LIVE as real) end as Days_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_broadcast_days_viewed / cast(c.USTENEUR_days_right_viewable as real)* cast(c.USTENEUR_days_right_broadcast as real) end as Days_A_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_broadcast_days_viewed_NON_LIVE / cast(e.BILION_days_right_viewable_NON_LIVE as real) end as SOCVD_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_broadcast_days_viewed_LIVE / cast(e.BILION_days_right_viewable_LIVE as real) end as SOCVD_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_broadcast_days_viewed / cast(c.BILION_days_right_viewable as real) end as SOCVD_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_broadcast_days_viewed_NON_LIVE / cast(e.ENGRUG_days_right_viewable_NON_LIVE as real) end as SOCVD_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_broadcast_days_viewed_LIVE / cast(e.ENGRUG_days_right_viewable_LIVE as real) end as SOCVD_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_broadcast_days_viewed / cast(c.ENGRUG_days_right_viewable as real) end as SOCVD_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_broadcast_days_viewed_NON_LIVE / cast(e.CAPCUP_days_right_viewable_NON_LIVE as real) end as SOCVD_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_broadcast_days_viewed_LIVE / cast(e.CAPCUP_days_right_viewable_LIVE as real) end as SOCVD_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_broadcast_days_viewed / cast(c.CAPCUP_days_right_viewable as real) end as SOCVD_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_broadcast_days_viewed_NON_LIVE / cast(e.NFLSS_days_right_viewable_NON_LIVE as real) end as SOCVD_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_broadcast_days_viewed_LIVE / cast(e.NFLSS_days_right_viewable_LIVE as real) end as SOCVD_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_broadcast_days_viewed / cast(c.NFLSS_days_right_viewable as real) end as SOCVD_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_broadcast_days_viewed_NON_LIVE / cast(e.PGATR_days_right_viewable_NON_LIVE as real) end as SOCVD_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_broadcast_days_viewed_LIVE / cast(e.PGATR_days_right_viewable_LIVE as real) end as SOCVD_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_broadcast_days_viewed / cast(c.PGATR_days_right_viewable as real) end as SOCVD_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_broadcast_days_viewed_NON_LIVE / cast(e.SUPLG_days_right_viewable_NON_LIVE as real) end as SOCVD_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_broadcast_days_viewed_LIVE / cast(e.SUPLG_days_right_viewable_LIVE as real) end as SOCVD_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_broadcast_days_viewed / cast(c.SUPLG_days_right_viewable as real) end as SOCVD_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_broadcast_days_viewed_NON_LIVE / cast(e.LALIGA_days_right_viewable_NON_LIVE as real) end as SOCVD_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_broadcast_days_viewed_LIVE / cast(e.LALIGA_days_right_viewable_LIVE as real) end as SOCVD_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_broadcast_days_viewed / cast(c.LALIGA_days_right_viewable as real) end as SOCVD_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_broadcast_days_viewed_NON_LIVE / cast(e.TDFEUR_days_right_viewable_NON_LIVE as real) end as SOCVD_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_broadcast_days_viewed_LIVE / cast(e.TDFEUR_days_right_viewable_LIVE as real) end as SOCVD_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_broadcast_days_viewed / cast(c.TDFEUR_days_right_viewable as real) end as SOCVD_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_broadcast_days_viewed_NON_LIVE / cast(e.TDFITV_days_right_viewable_NON_LIVE as real) end as SOCVD_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_broadcast_days_viewed_LIVE / cast(e.TDFITV_days_right_viewable_LIVE as real) end as SOCVD_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_broadcast_days_viewed / cast(c.TDFITV_days_right_viewable as real) end as SOCVD_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_broadcast_days_viewed_NON_LIVE / cast(e.USMGOLF_days_right_viewable_NON_LIVE as real) end as SOCVD_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_broadcast_days_viewed_LIVE / cast(e.USMGOLF_days_right_viewable_LIVE as real) end as SOCVD_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_broadcast_days_viewed / cast(c.USMGOLF_days_right_viewable as real) end as SOCVD_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_broadcast_days_viewed_NON_LIVE / cast(e.USTENSS_days_right_viewable_NON_LIVE as real) end as SOCVD_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_broadcast_days_viewed_LIVE / cast(e.USTENSS_days_right_viewable_LIVE as real) end as SOCVD_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_broadcast_days_viewed / cast(c.USTENSS_days_right_viewable as real) end as SOCVD_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_broadcast_days_viewed_NON_LIVE / cast(e.USTENEUR_days_right_viewable_NON_LIVE as real) end as SOCVD_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_broadcast_days_viewed_LIVE / cast(e.USTENEUR_days_right_viewable_LIVE as real) end as SOCVD_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_broadcast_days_viewed / cast(c.USTENEUR_days_right_viewable as real) end as SOCVD_A_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_programmes_viewed_over_threshold_NON_LIVE / cast(e.BILION_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_programmes_viewed_over_threshold_LIVE / cast(e.BILION_right_broadcast_programmes_LIVE as real) end as SOCP_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_programmes_viewed_over_threshold / cast(c.BILION_right_broadcast_programmes as real) end as SOCP_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_programmes_viewed_over_threshold_NON_LIVE / cast(e.ENGRUG_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_programmes_viewed_over_threshold_LIVE / cast(e.ENGRUG_right_broadcast_programmes_LIVE as real) end as SOCP_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_programmes_viewed_over_threshold / cast(c.ENGRUG_right_broadcast_programmes as real) end as SOCP_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_programmes_viewed_over_threshold_NON_LIVE / cast(e.CAPCUP_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_programmes_viewed_over_threshold_LIVE / cast(e.CAPCUP_right_broadcast_programmes_LIVE as real) end as SOCP_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_programmes_viewed_over_threshold / cast(c.CAPCUP_right_broadcast_programmes as real) end as SOCP_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_programmes_viewed_over_threshold_NON_LIVE / cast(e.NFLSS_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_programmes_viewed_over_threshold_LIVE / cast(e.NFLSS_right_broadcast_programmes_LIVE as real) end as SOCP_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_programmes_viewed_over_threshold / cast(c.NFLSS_right_broadcast_programmes as real) end as SOCP_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_programmes_viewed_over_threshold_NON_LIVE / cast(e.PGATR_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_programmes_viewed_over_threshold_LIVE / cast(e.PGATR_right_broadcast_programmes_LIVE as real) end as SOCP_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_programmes_viewed_over_threshold / cast(c.PGATR_right_broadcast_programmes as real) end as SOCP_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_programmes_viewed_over_threshold_NON_LIVE / cast(e.SUPLG_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_programmes_viewed_over_threshold_LIVE / cast(e.SUPLG_right_broadcast_programmes_LIVE as real) end as SOCP_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_programmes_viewed_over_threshold / cast(c.SUPLG_right_broadcast_programmes as real) end as SOCP_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_programmes_viewed_over_threshold_NON_LIVE / cast(e.LALIGA_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_programmes_viewed_over_threshold_LIVE / cast(e.LALIGA_right_broadcast_programmes_LIVE as real) end as SOCP_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_programmes_viewed_over_threshold / cast(c.LALIGA_right_broadcast_programmes as real) end as SOCP_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_programmes_viewed_over_threshold_NON_LIVE / cast(e.TDFEUR_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_programmes_viewed_over_threshold_LIVE / cast(e.TDFEUR_right_broadcast_programmes_LIVE as real) end as SOCP_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_programmes_viewed_over_threshold / cast(c.TDFEUR_right_broadcast_programmes as real) end as SOCP_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_programmes_viewed_over_threshold_NON_LIVE / cast(e.TDFITV_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_programmes_viewed_over_threshold_LIVE / cast(e.TDFITV_right_broadcast_programmes_LIVE as real) end as SOCP_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_programmes_viewed_over_threshold / cast(c.TDFITV_right_broadcast_programmes as real) end as SOCP_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_programmes_viewed_over_threshold_NON_LIVE / cast(e.USMGOLF_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_programmes_viewed_over_threshold_LIVE / cast(e.USMGOLF_right_broadcast_programmes_LIVE as real) end as SOCP_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_programmes_viewed_over_threshold / cast(c.USMGOLF_right_broadcast_programmes as real) end as SOCP_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_programmes_viewed_over_threshold_NON_LIVE / cast(e.USTENSS_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_programmes_viewed_over_threshold_LIVE / cast(e.USTENSS_right_broadcast_programmes_LIVE as real) end as SOCP_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_programmes_viewed_over_threshold / cast(c.USTENSS_right_broadcast_programmes as real) end as SOCP_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_programmes_viewed_over_threshold_NON_LIVE / cast(e.USTENEUR_right_broadcast_programmes_NON_LIVE as real) end as SOCP_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_programmes_viewed_over_threshold_LIVE / cast(e.USTENEUR_right_broadcast_programmes_LIVE as real) end as SOCP_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_programmes_viewed_over_threshold / cast(c.USTENEUR_right_broadcast_programmes as real) end as SOCP_A_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_seconds_viewed_NON_LIVE / cast(e.BILION_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_seconds_viewed_LIVE / cast(e.BILION_right_broadcast_duration_LIVE as real) end as SOCDurL_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_seconds_viewed / cast(c.BILION_right_broadcast_duration as real) end as SOCDurA_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_NON_LIVE / cast(e.ENGRUG_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_LIVE / cast(e.ENGRUG_right_broadcast_duration_LIVE as real) end as SOCDurL_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_seconds_viewed / cast(c.ENGRUG_right_broadcast_duration as real) end as SOCDurA_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_NON_LIVE / cast(e.CAPCUP_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_LIVE / cast(e.CAPCUP_right_broadcast_duration_LIVE as real) end as SOCDurL_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_seconds_viewed / cast(c.CAPCUP_right_broadcast_duration as real) end as SOCDurA_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_NON_LIVE / cast(e.NFLSS_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_LIVE / cast(e.NFLSS_right_broadcast_duration_LIVE as real) end as SOCDurL_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_seconds_viewed / cast(c.NFLSS_right_broadcast_duration as real) end as SOCDurA_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_NON_LIVE / cast(e.PGATR_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_LIVE / cast(e.PGATR_right_broadcast_duration_LIVE as real) end as SOCDurL_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_seconds_viewed / cast(c.PGATR_right_broadcast_duration as real) end as SOCDurA_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_NON_LIVE / cast(e.SUPLG_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_LIVE / cast(e.SUPLG_right_broadcast_duration_LIVE as real) end as SOCDurL_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_seconds_viewed / cast(c.SUPLG_right_broadcast_duration as real) end as SOCDurA_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_NON_LIVE / cast(e.LALIGA_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_LIVE / cast(e.LALIGA_right_broadcast_duration_LIVE as real) end as SOCDurL_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_seconds_viewed / cast(c.LALIGA_right_broadcast_duration as real) end as SOCDurA_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_NON_LIVE / cast(e.TDFEUR_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_LIVE / cast(e.TDFEUR_right_broadcast_duration_LIVE as real) end as SOCDurL_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_seconds_viewed / cast(c.TDFEUR_right_broadcast_duration as real) end as SOCDurA_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_NON_LIVE / cast(e.TDFITV_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_LIVE / cast(e.TDFITV_right_broadcast_duration_LIVE as real) end as SOCDurL_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_seconds_viewed / cast(c.TDFITV_right_broadcast_duration as real) end as SOCDurA_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_NON_LIVE / cast(e.USMGOLF_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_LIVE / cast(e.USMGOLF_right_broadcast_duration_LIVE as real) end as SOCDurL_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_seconds_viewed / cast(c.USMGOLF_right_broadcast_duration as real) end as SOCDurA_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_NON_LIVE / cast(e.USTENSS_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_LIVE / cast(e.USTENSS_right_broadcast_duration_LIVE as real) end as SOCDurL_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_seconds_viewed / cast(c.USTENSS_right_broadcast_duration as real) end as SOCDurA_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_NON_LIVE / cast(e.USTENEUR_right_broadcast_duration_NON_LIVE as real) end as SOCDurH_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_LIVE / cast(e.USTENEUR_right_broadcast_duration_LIVE as real) end as SOCDurL_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_seconds_viewed / cast(c.USTENEUR_right_broadcast_duration as real) end as SOCDurA_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_sports as real )end as SOVS_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_seconds_viewed / cast(a.total_viewing_duration_sports as real )end as SOVS_A_USTENEUR
,case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_BILION
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_NON_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_seconds_viewed_LIVE / cast(a.total_viewing_duration_all as real )end as SOVAll_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_seconds_viewed / cast(a.total_viewing_duration_all as real )end as SOVAll_A_USTENEUR



into dbarnett.v250_annualised_activity_table_final_v1
---Master Table
from dbarnett.v250_master_account_list as a

--Overall Actual Viewing
left outer join dbarnett.v250_unannualised_right_activity as b
on a.account_number = b.account_number

--Overall Days Broadcast Watchable--
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number

--Live/Non Live Broadcast Actual Viewing
left outer join dbarnett.v250_unannualised_right_activity_by_live_non_live as d
on a.account_number = d.account_number

--Live/Non Live Days Broadcast and Watchable
left outer join dbarnett.v250_right_viewable_account_summary_by_live_status as e
on a.account_number = e.account_number

where total_days_with_viewing>=280 and total_viewing_duration_sports >0

;
commit;

grant all on dbarnett.v250_annualised_activity_table_for_workshop_v2 to public;
commit;

----Update Original file

update dbarnett.v250_annualised_activity_table_for_workshop
set AVE_A_BILION=b.AVE_A_BILION
,AVE_A_ENGRUG=b.AVE_A_ENGRUG
,AVE_A_CAPCUP=b.AVE_A_CAPCUP
,AVE_A_NFLSS=b.AVE_A_NFLSS
,AVE_A_PGATR=b.AVE_A_PGATR
,AVE_A_SUPLG=b.AVE_A_SUPLG
,AVE_A_LALIGA=b.AVE_A_LALIGA
,AVE_A_TDFEUR=b.AVE_A_TDFEUR
,AVE_A_TDFITV=b.AVE_A_TDFITV
,AVE_A_USMGOLF=b.AVE_A_USMGOLF
,AVE_A_USTENSS=b.AVE_A_USTENSS
,AVE_A_USTENEUR=b.AVE_A_USTENEUR
,AVE_L_BILION=b.AVE_L_BILION
,AVE_L_ENGRUG=b.AVE_L_ENGRUG
,AVE_L_CAPCUP=b.AVE_L_CAPCUP
,AVE_L_NFLSS=b.AVE_L_NFLSS
,AVE_L_PGATR=b.AVE_L_PGATR
,AVE_L_SUPLG=b.AVE_L_SUPLG
,AVE_L_LALIGA=b.AVE_L_LALIGA
,AVE_L_TDFEUR=b.AVE_L_TDFEUR
,AVE_L_TDFITV=b.AVE_L_TDFITV
,AVE_L_USMGOLF=b.AVE_L_USMGOLF
,AVE_L_USTENSS=b.AVE_L_USTENSS
,AVE_L_USTENEUR=b.AVE_L_USTENEUR
,AVE_H_BILION=b.AVE_H_BILION
,AVE_H_ENGRUG=b.AVE_H_ENGRUG
,AVE_H_CAPCUP=b.AVE_H_CAPCUP
,AVE_H_NFLSS=b.AVE_H_NFLSS
,AVE_H_PGATR=b.AVE_H_PGATR
,AVE_H_SUPLG=b.AVE_H_SUPLG
,AVE_H_LALIGA=b.AVE_H_LALIGA
,AVE_H_TDFEUR=b.AVE_H_TDFEUR
,AVE_H_TDFITV=b.AVE_H_TDFITV
,AVE_H_USMGOLF=b.AVE_H_USMGOLF
,AVE_H_USTENSS=b.AVE_H_USTENSS
,AVE_H_USTENEUR=b.AVE_H_USTENEUR


from dbarnett.v250_annualised_activity_table_for_workshop as a
left outer join dbarnett.v250_annualised_activity_table_for_workshop_v2 as b
on a.account_number =b.account_number
;
commit;
---Add On Account Status--

select a.account_number
into dbarnett.v250_accounts_for_survey
from dbarnett.v250_annualised_activity_table_for_workshop as a
left outer join v250_single_profiling_view as b
on a.account_number=b.account_number
where b.current_status_code in ('AC','AB','PC')
;
commit;

select * from dbarnett.v250_accounts_for_survey;
output to 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\analysis accounts for survey.csv' format ascii;
commit;

grant all on dbarnett.v250_accounts_for_survey to public; commit;


select number_of_sports_premiums
,count(*)
from dbarnett.v250_annualised_activity_table_for_workshop as a
left outer join v250_single_profiling_view as b
on a.account_number=b.account_number
where b.current_status_code in ('AC','AB','PC')
group by number_of_sports_premiums
;

commit;

--select top 100 * from dbarnett.v250_annualised_activity_table_for_workshop ;
--select d.BILION_programmes_viewed_over_threshold_LIVE from dbarnett.v250_unannualised_right_activity_by_live_non_live as d
--select b.BILION_programmes_viewed_over_threshold from dbarnett.v250_unannualised_right_activity as b
--select * from dbarnett.v250_unannualised_right_activity_by_live_non_live;
--select count(*) from  dbarnett.v250_master_account_list as a where total_days_with_viewing>=280 and total_viewing_duration_all =0


/*

select


case when d.BILION_broadcast_days_viewed_NON_LIVE=0 then 0  when d.BILION_total_viewing_events_NON_LIVE=0 then 0  when d.BILION_total_viewing_events_NON_LIVE is null then 0  when d.BILION_broadcast_days_viewed_NON_LIVE is null then 0  when e.BILION_days_right_viewable_NON_LIVE =0 then 0 when e.BILION_days_right_broadcast_NON_LIVE=0 then 0  else d.BILION_total_viewing_events_NON_LIVE / cast(d.BILION_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_BILION
,case when d.BILION_broadcast_days_viewed_LIVE=0 then 0  when d.BILION_programmes_viewed_over_threshold_LIVE=0 then 0  when d.BILION_programmes_viewed_over_threshold_LIVE is null then 0  when d.BILION_broadcast_days_viewed_LIVE is null then 0  when e.BILION_days_right_viewable_LIVE =0 then 0 when e.BILION_days_right_broadcast_LIVE=0 then 0  else d.BILION_total_viewing_events_LIVE / cast(d.BILION_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_BILION
,case when b.BILION_broadcast_days_viewed=0 then 0  when b.BILION_programmes_viewed_over_threshold=0 then 0  when b.BILION_programmes_viewed_over_threshold is null then 0  when b.BILION_broadcast_days_viewed is null then 0  when c.BILION_days_right_viewable =0 then 0 when c.BILION_days_right_broadcast=0 then 0  else b.BILION_total_viewing_events / cast(b.BILION_programmes_viewed_over_threshold as real) end as AVE_A_BILION
/*
,case when d.ENGRUG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.ENGRUG_total_viewing_events_NON_LIVE=0 then 0  when d.ENGRUG_total_viewing_events_NON_LIVE is null then 0  when d.ENGRUG_broadcast_days_viewed_NON_LIVE is null then 0  when e.ENGRUG_days_right_viewable_NON_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_NON_LIVE=0 then 0  else d.ENGRUG_total_viewing_events_NON_LIVE / cast(d.ENGRUG_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_ENGRUG
,case when d.ENGRUG_broadcast_days_viewed_LIVE=0 then 0  when d.ENGRUG_programmes_viewed_over_threshold_LIVE=0 then 0  when d.ENGRUG_programmes_viewed_over_threshold_LIVE is null then 0  when d.ENGRUG_broadcast_days_viewed_LIVE is null then 0  when e.ENGRUG_days_right_viewable_LIVE =0 then 0 when e.ENGRUG_days_right_broadcast_LIVE=0 then 0  else d.ENGRUG_total_viewing_events_LIVE / cast(d.ENGRUG_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_ENGRUG
,case when b.ENGRUG_broadcast_days_viewed=0 then 0  when b.ENGRUG_programmes_viewed_over_threshold=0 then 0  when b.ENGRUG_programmes_viewed_over_threshold is null then 0  when b.ENGRUG_broadcast_days_viewed is null then 0  when c.ENGRUG_days_right_viewable =0 then 0 when c.ENGRUG_days_right_broadcast=0 then 0  else b.ENGRUG_total_viewing_events / cast(b.ENGRUG_programmes_viewed_over_threshold as real) end as AVE_A_ENGRUG
,case when d.CAPCUP_broadcast_days_viewed_NON_LIVE=0 then 0  when d.CAPCUP_total_viewing_events_NON_LIVE=0 then 0  when d.CAPCUP_total_viewing_events_NON_LIVE is null then 0  when d.CAPCUP_broadcast_days_viewed_NON_LIVE is null then 0  when e.CAPCUP_days_right_viewable_NON_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_NON_LIVE=0 then 0  else d.CAPCUP_total_viewing_events_NON_LIVE / cast(d.CAPCUP_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_CAPCUP
,case when d.CAPCUP_broadcast_days_viewed_LIVE=0 then 0  when d.CAPCUP_programmes_viewed_over_threshold_LIVE=0 then 0  when d.CAPCUP_programmes_viewed_over_threshold_LIVE is null then 0  when d.CAPCUP_broadcast_days_viewed_LIVE is null then 0  when e.CAPCUP_days_right_viewable_LIVE =0 then 0 when e.CAPCUP_days_right_broadcast_LIVE=0 then 0  else d.CAPCUP_total_viewing_events_LIVE / cast(d.CAPCUP_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_CAPCUP
,case when b.CAPCUP_broadcast_days_viewed=0 then 0  when b.CAPCUP_programmes_viewed_over_threshold=0 then 0  when b.CAPCUP_programmes_viewed_over_threshold is null then 0  when b.CAPCUP_broadcast_days_viewed is null then 0  when c.CAPCUP_days_right_viewable =0 then 0 when c.CAPCUP_days_right_broadcast=0 then 0  else b.CAPCUP_total_viewing_events / cast(b.CAPCUP_programmes_viewed_over_threshold as real) end as AVE_A_CAPCUP
,case when d.NFLSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.NFLSS_total_viewing_events_NON_LIVE=0 then 0  when d.NFLSS_total_viewing_events_NON_LIVE is null then 0  when d.NFLSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.NFLSS_days_right_viewable_NON_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_NON_LIVE=0 then 0  else d.NFLSS_total_viewing_events_NON_LIVE / cast(d.NFLSS_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_NFLSS
,case when d.NFLSS_broadcast_days_viewed_LIVE=0 then 0  when d.NFLSS_programmes_viewed_over_threshold_LIVE=0 then 0  when d.NFLSS_programmes_viewed_over_threshold_LIVE is null then 0  when d.NFLSS_broadcast_days_viewed_LIVE is null then 0  when e.NFLSS_days_right_viewable_LIVE =0 then 0 when e.NFLSS_days_right_broadcast_LIVE=0 then 0  else d.NFLSS_total_viewing_events_LIVE / cast(d.NFLSS_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_NFLSS
,case when b.NFLSS_broadcast_days_viewed=0 then 0  when b.NFLSS_programmes_viewed_over_threshold=0 then 0  when b.NFLSS_programmes_viewed_over_threshold is null then 0  when b.NFLSS_broadcast_days_viewed is null then 0  when c.NFLSS_days_right_viewable =0 then 0 when c.NFLSS_days_right_broadcast=0 then 0  else b.NFLSS_total_viewing_events / cast(b.NFLSS_programmes_viewed_over_threshold as real) end as AVE_A_NFLSS
,case when d.PGATR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.PGATR_total_viewing_events_NON_LIVE=0 then 0  when d.PGATR_total_viewing_events_NON_LIVE is null then 0  when d.PGATR_broadcast_days_viewed_NON_LIVE is null then 0  when e.PGATR_days_right_viewable_NON_LIVE =0 then 0 when e.PGATR_days_right_broadcast_NON_LIVE=0 then 0  else d.PGATR_total_viewing_events_NON_LIVE / cast(d.PGATR_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_PGATR
,case when d.PGATR_broadcast_days_viewed_LIVE=0 then 0  when d.PGATR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.PGATR_programmes_viewed_over_threshold_LIVE is null then 0  when d.PGATR_broadcast_days_viewed_LIVE is null then 0  when e.PGATR_days_right_viewable_LIVE =0 then 0 when e.PGATR_days_right_broadcast_LIVE=0 then 0  else d.PGATR_total_viewing_events_LIVE / cast(d.PGATR_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_PGATR
,case when b.PGATR_broadcast_days_viewed=0 then 0  when b.PGATR_programmes_viewed_over_threshold=0 then 0  when b.PGATR_programmes_viewed_over_threshold is null then 0  when b.PGATR_broadcast_days_viewed is null then 0  when c.PGATR_days_right_viewable =0 then 0 when c.PGATR_days_right_broadcast=0 then 0  else b.PGATR_total_viewing_events / cast(b.PGATR_programmes_viewed_over_threshold as real) end as AVE_A_PGATR
,case when d.SUPLG_broadcast_days_viewed_NON_LIVE=0 then 0  when d.SUPLG_total_viewing_events_NON_LIVE=0 then 0  when d.SUPLG_total_viewing_events_NON_LIVE is null then 0  when d.SUPLG_broadcast_days_viewed_NON_LIVE is null then 0  when e.SUPLG_days_right_viewable_NON_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_NON_LIVE=0 then 0  else d.SUPLG_total_viewing_events_NON_LIVE / cast(d.SUPLG_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_SUPLG
,case when d.SUPLG_broadcast_days_viewed_LIVE=0 then 0  when d.SUPLG_programmes_viewed_over_threshold_LIVE=0 then 0  when d.SUPLG_programmes_viewed_over_threshold_LIVE is null then 0  when d.SUPLG_broadcast_days_viewed_LIVE is null then 0  when e.SUPLG_days_right_viewable_LIVE =0 then 0 when e.SUPLG_days_right_broadcast_LIVE=0 then 0  else d.SUPLG_total_viewing_events_LIVE / cast(d.SUPLG_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_SUPLG
,case when b.SUPLG_broadcast_days_viewed=0 then 0  when b.SUPLG_programmes_viewed_over_threshold=0 then 0  when b.SUPLG_programmes_viewed_over_threshold is null then 0  when b.SUPLG_broadcast_days_viewed is null then 0  when c.SUPLG_days_right_viewable =0 then 0 when c.SUPLG_days_right_broadcast=0 then 0  else b.SUPLG_total_viewing_events / cast(b.SUPLG_programmes_viewed_over_threshold as real) end as AVE_A_SUPLG
,case when d.LALIGA_broadcast_days_viewed_NON_LIVE=0 then 0  when d.LALIGA_total_viewing_events_NON_LIVE=0 then 0  when d.LALIGA_total_viewing_events_NON_LIVE is null then 0  when d.LALIGA_broadcast_days_viewed_NON_LIVE is null then 0  when e.LALIGA_days_right_viewable_NON_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_NON_LIVE=0 then 0  else d.LALIGA_total_viewing_events_NON_LIVE / cast(d.LALIGA_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_LALIGA
,case when d.LALIGA_broadcast_days_viewed_LIVE=0 then 0  when d.LALIGA_programmes_viewed_over_threshold_LIVE=0 then 0  when d.LALIGA_programmes_viewed_over_threshold_LIVE is null then 0  when d.LALIGA_broadcast_days_viewed_LIVE is null then 0  when e.LALIGA_days_right_viewable_LIVE =0 then 0 when e.LALIGA_days_right_broadcast_LIVE=0 then 0  else d.LALIGA_total_viewing_events_LIVE / cast(d.LALIGA_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_LALIGA
,case when b.LALIGA_broadcast_days_viewed=0 then 0  when b.LALIGA_programmes_viewed_over_threshold=0 then 0  when b.LALIGA_programmes_viewed_over_threshold is null then 0  when b.LALIGA_broadcast_days_viewed is null then 0  when c.LALIGA_days_right_viewable =0 then 0 when c.LALIGA_days_right_broadcast=0 then 0  else b.LALIGA_total_viewing_events / cast(b.LALIGA_programmes_viewed_over_threshold as real) end as AVE_A_LALIGA
,case when d.TDFEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFEUR_total_viewing_events_NON_LIVE=0 then 0  when d.TDFEUR_total_viewing_events_NON_LIVE is null then 0  when d.TDFEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFEUR_days_right_viewable_NON_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFEUR_total_viewing_events_NON_LIVE / cast(d.TDFEUR_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_TDFEUR
,case when d.TDFEUR_broadcast_days_viewed_LIVE=0 then 0  when d.TDFEUR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.TDFEUR_programmes_viewed_over_threshold_LIVE is null then 0  when d.TDFEUR_broadcast_days_viewed_LIVE is null then 0  when e.TDFEUR_days_right_viewable_LIVE =0 then 0 when e.TDFEUR_days_right_broadcast_LIVE=0 then 0  else d.TDFEUR_total_viewing_events_LIVE / cast(d.TDFEUR_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_TDFEUR
,case when b.TDFEUR_broadcast_days_viewed=0 then 0  when b.TDFEUR_programmes_viewed_over_threshold=0 then 0  when b.TDFEUR_programmes_viewed_over_threshold is null then 0  when b.TDFEUR_broadcast_days_viewed is null then 0  when c.TDFEUR_days_right_viewable =0 then 0 when c.TDFEUR_days_right_broadcast=0 then 0  else b.TDFEUR_total_viewing_events / cast(b.TDFEUR_programmes_viewed_over_threshold as real) end as AVE_A_TDFEUR
,case when d.TDFITV_broadcast_days_viewed_NON_LIVE=0 then 0  when d.TDFITV_total_viewing_events_NON_LIVE=0 then 0  when d.TDFITV_total_viewing_events_NON_LIVE is null then 0  when d.TDFITV_broadcast_days_viewed_NON_LIVE is null then 0  when e.TDFITV_days_right_viewable_NON_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_NON_LIVE=0 then 0  else d.TDFITV_total_viewing_events_NON_LIVE / cast(d.TDFITV_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_TDFITV
,case when d.TDFITV_broadcast_days_viewed_LIVE=0 then 0  when d.TDFITV_programmes_viewed_over_threshold_LIVE=0 then 0  when d.TDFITV_programmes_viewed_over_threshold_LIVE is null then 0  when d.TDFITV_broadcast_days_viewed_LIVE is null then 0  when e.TDFITV_days_right_viewable_LIVE =0 then 0 when e.TDFITV_days_right_broadcast_LIVE=0 then 0  else d.TDFITV_total_viewing_events_LIVE / cast(d.TDFITV_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_TDFITV
,case when b.TDFITV_broadcast_days_viewed=0 then 0  when b.TDFITV_programmes_viewed_over_threshold=0 then 0  when b.TDFITV_programmes_viewed_over_threshold is null then 0  when b.TDFITV_broadcast_days_viewed is null then 0  when c.TDFITV_days_right_viewable =0 then 0 when c.TDFITV_days_right_broadcast=0 then 0  else b.TDFITV_total_viewing_events / cast(b.TDFITV_programmes_viewed_over_threshold as real) end as AVE_A_TDFITV
,case when d.USMGOLF_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USMGOLF_total_viewing_events_NON_LIVE=0 then 0  when d.USMGOLF_total_viewing_events_NON_LIVE is null then 0  when d.USMGOLF_broadcast_days_viewed_NON_LIVE is null then 0  when e.USMGOLF_days_right_viewable_NON_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_NON_LIVE=0 then 0  else d.USMGOLF_total_viewing_events_NON_LIVE / cast(d.USMGOLF_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_USMGOLF
,case when d.USMGOLF_broadcast_days_viewed_LIVE=0 then 0  when d.USMGOLF_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USMGOLF_programmes_viewed_over_threshold_LIVE is null then 0  when d.USMGOLF_broadcast_days_viewed_LIVE is null then 0  when e.USMGOLF_days_right_viewable_LIVE =0 then 0 when e.USMGOLF_days_right_broadcast_LIVE=0 then 0  else d.USMGOLF_total_viewing_events_LIVE / cast(d.USMGOLF_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_USMGOLF
,case when b.USMGOLF_broadcast_days_viewed=0 then 0  when b.USMGOLF_programmes_viewed_over_threshold=0 then 0  when b.USMGOLF_programmes_viewed_over_threshold is null then 0  when b.USMGOLF_broadcast_days_viewed is null then 0  when c.USMGOLF_days_right_viewable =0 then 0 when c.USMGOLF_days_right_broadcast=0 then 0  else b.USMGOLF_total_viewing_events / cast(b.USMGOLF_programmes_viewed_over_threshold as real) end as AVE_A_USMGOLF
,case when d.USTENSS_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENSS_total_viewing_events_NON_LIVE=0 then 0  when d.USTENSS_total_viewing_events_NON_LIVE is null then 0  when d.USTENSS_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENSS_days_right_viewable_NON_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENSS_total_viewing_events_NON_LIVE / cast(d.USTENSS_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_USTENSS
,case when d.USTENSS_broadcast_days_viewed_LIVE=0 then 0  when d.USTENSS_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USTENSS_programmes_viewed_over_threshold_LIVE is null then 0  when d.USTENSS_broadcast_days_viewed_LIVE is null then 0  when e.USTENSS_days_right_viewable_LIVE =0 then 0 when e.USTENSS_days_right_broadcast_LIVE=0 then 0  else d.USTENSS_total_viewing_events_LIVE / cast(d.USTENSS_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_USTENSS
,case when b.USTENSS_broadcast_days_viewed=0 then 0  when b.USTENSS_programmes_viewed_over_threshold=0 then 0  when b.USTENSS_programmes_viewed_over_threshold is null then 0  when b.USTENSS_broadcast_days_viewed is null then 0  when c.USTENSS_days_right_viewable =0 then 0 when c.USTENSS_days_right_broadcast=0 then 0  else b.USTENSS_total_viewing_events / cast(b.USTENSS_programmes_viewed_over_threshold as real) end as AVE_A_USTENSS
,case when d.USTENEUR_broadcast_days_viewed_NON_LIVE=0 then 0  when d.USTENEUR_total_viewing_events_NON_LIVE=0 then 0  when d.USTENEUR_total_viewing_events_NON_LIVE is null then 0  when d.USTENEUR_broadcast_days_viewed_NON_LIVE is null then 0  when e.USTENEUR_days_right_viewable_NON_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_NON_LIVE=0 then 0  else d.USTENEUR_total_viewing_events_NON_LIVE / cast(d.USTENEUR_programmes_viewed_over_threshold_NON_LIVE as real) end as AVE_H_USTENEUR
,case when d.USTENEUR_broadcast_days_viewed_LIVE=0 then 0  when d.USTENEUR_programmes_viewed_over_threshold_LIVE=0 then 0  when d.USTENEUR_programmes_viewed_over_threshold_LIVE is null then 0  when d.USTENEUR_broadcast_days_viewed_LIVE is null then 0  when e.USTENEUR_days_right_viewable_LIVE =0 then 0 when e.USTENEUR_days_right_broadcast_LIVE=0 then 0  else d.USTENEUR_total_viewing_events_LIVE / cast(d.USTENEUR_programmes_viewed_over_threshold_LIVE as real) end as AVE_L_USTENEUR
,case when b.USTENEUR_broadcast_days_viewed=0 then 0  when b.USTENEUR_programmes_viewed_over_threshold=0 then 0  when b.USTENEUR_programmes_viewed_over_threshold is null then 0  when b.USTENEUR_broadcast_days_viewed is null then 0  when c.USTENEUR_days_right_viewable =0 then 0 when c.USTENEUR_days_right_broadcast=0 then 0  else b.USTENEUR_total_viewing_events / cast(b.USTENEUR_programmes_viewed_over_threshold as real) end as AVE_A_USTENEUR

*/
from dbarnett.v250_master_account_list as a
left outer join dbarnett.v250_unannualised_right_activity as b
on a.account_number = b.account_number

--Overall Days Broadcast Watchable--
left outer join dbarnett.v250_right_viewable_account_summary as c
on a.account_number = c.account_number

--Live/Non Live Broadcast Actual Viewing
left outer join dbarnett.v250_unannualised_right_activity_by_live_non_live as d
on a.account_number = d.account_number

--Live/Non Live Days Broadcast and Watchable
left outer join dbarnett.v250_right_viewable_account_summary_by_live_status as e
on a.account_number = e.account_number

where total_days_with_viewing>=280 and total_viewing_duration_sports >0 
--order by d.BILION_programmes_viewed_over_threshold_LIVE
--and b.USTENEUR_total_viewing_events is null
--and b.USTENEUR_programmes_viewed_over_threshold is not null
group by 
b.USTENEUR_total_viewing_events 
,b.USTENEUR_programmes_viewed_over_threshold 

*/

/*
select analysis_right
,live
,count(*)
from dbarnett.v250_sports_rights_epg_data_for_analysis 
group by analysis_right
,live
order by  analysis_right
,live
*/

---select * from dbarnett.v250_right_viewable_account_summary;


/*
--select * from dbarnett.v250_workshop_dataset_test;
create table dbarnett.v250_workshop_dataset_test
(
Account_Number varchar(40),
total_sports_duration real,
total_viewing_duration real,
viewing_days integer,
account_weight real,
Totm_H_BILION real,
PV_H_BILION real,
VE_H_BILION real,
AVE_H_BILION real,
Days_H_BILION real,
SOCVD_H_BILION real,
SOCP_H_BILION real,
SOCDurH_BILION real,
SOVS_H_BILION real,
SOVAll_H_BILION real,
Totm_L_BILION real,
PV_L_BILION real,
VE_L_BILION real,
AVE_L_BILION real,
Days_L_BILION real,
SOCVD_L_BILION real,
SOCP_L_BILION real,
SOCDurL_BILION real,
SOVS_L_BILION real,
SOVAll_L_BILION real,
Totm_A_BILION real,
PV_A_BILION real,
VE_A_BILION real,
AVE_A_BILION real,
Days_A_BILION real,
SOCVD_A_BILION real,
SOCP_A_BILION real,
SOCDurA_BILION real,
SOVS_A_BILION real,
SOVAll_A_BILION real,
Totm_H_ENGRUG real,
PV_H_ENGRUG real,
VE_H_ENGRUG real,
AVE_H_ENGRUG real,
Days_H_ENGRUG real,
SOCVD_H_ENGRUG real,
SOCP_H_ENGRUG real,
SOCDurH_ENGRUG real,
SOVS_H_ENGRUG real,
SOVAll_H_ENGRUG real,
Totm_L_ENGRUG real,
PV_L_ENGRUG real,
VE_L_ENGRUG real,
AVE_L_ENGRUG real,
Days_L_ENGRUG real,
SOCVD_L_ENGRUG real,
SOCP_L_ENGRUG real,
SOCDurL_ENGRUG real,
SOVS_L_ENGRUG real,
SOVAll_L_ENGRUG real,
Totm_A_ENGRUG real,
PV_A_ENGRUG real,
VE_A_ENGRUG real,
AVE_A_ENGRUG real,
Days_A_ENGRUG real,
SOCVD_A_ENGRUG real,
SOCP_A_ENGRUG real,
SOCDurA_ENGRUG real,
SOVS_A_ENGRUG real,
SOVAll_A_ENGRUG real,
Totm_H_CAPCUP real,
PV_H_CAPCUP real,
VE_H_CAPCUP real,
AVE_H_CAPCUP real,
Days_H_CAPCUP real,
SOCVD_H_CAPCUP real,
SOCP_H_CAPCUP real,
SOCDurH_CAPCUP real,
SOVS_H_CAPCUP real,
SOVAll_H_CAPCUP real,
Totm_L_CAPCUP real,
PV_L_CAPCUP real,
VE_L_CAPCUP real,
AVE_L_CAPCUP real,
Days_L_CAPCUP real,
SOCVD_L_CAPCUP real,
SOCP_L_CAPCUP real,
SOCDurL_CAPCUP real,
SOVS_L_CAPCUP real,
SOVAll_L_CAPCUP real,
Totm_A_CAPCUP real,
PV_A_CAPCUP real,
VE_A_CAPCUP real,
AVE_A_CAPCUP real,
Days_A_CAPCUP real,
SOCVD_A_CAPCUP real,
SOCP_A_CAPCUP real,
SOCDurA_CAPCUP real,
SOVS_A_CAPCUP real,
SOVAll_A_CAPCUP real,
Totm_H_NFLSS real,
PV_H_NFLSS real,
VE_H_NFLSS real,
AVE_H_NFLSS real,
Days_H_NFLSS real,
SOCVD_H_NFLSS real,
SOCP_H_NFLSS real,
SOCDurH_NFLSS real,
SOVS_H_NFLSS real,
SOVAll_H_NFLSS real,
Totm_L_NFLSS real,
PV_L_NFLSS real,
VE_L_NFLSS real,
AVE_L_NFLSS real,
Days_L_NFLSS real,
SOCVD_L_NFLSS real,
SOCP_L_NFLSS real,
SOCDurL_NFLSS real,
SOVS_L_NFLSS real,
SOVAll_L_NFLSS real,
Totm_A_NFLSS real,
PV_A_NFLSS real,
VE_A_NFLSS real,
AVE_A_NFLSS real,
Days_A_NFLSS real,
SOCVD_A_NFLSS real,
SOCP_A_NFLSS real,
SOCDurA_NFLSS real,
SOVS_A_NFLSS real,
SOVAll_A_NFLSS real,
Totm_H_PGATR real,
PV_H_PGATR real,
VE_H_PGATR real,
AVE_H_PGATR real,
Days_H_PGATR real,
SOCVD_H_PGATR real,
SOCP_H_PGATR real,
SOCDurH_PGATR real,
SOVS_H_PGATR real,
SOVAll_H_PGATR real,
Totm_L_PGATR real,
PV_L_PGATR real,
VE_L_PGATR real,
AVE_L_PGATR real,
Days_L_PGATR real,
SOCVD_L_PGATR real,
SOCP_L_PGATR real,
SOCDurL_PGATR real,
SOVS_L_PGATR real,
SOVAll_L_PGATR real,
Totm_A_PGATR real,
PV_A_PGATR real,
VE_A_PGATR real,
AVE_A_PGATR real,
Days_A_PGATR real,
SOCVD_A_PGATR real,
SOCP_A_PGATR real,
SOCDurA_PGATR real,
SOVS_A_PGATR real,
SOVAll_A_PGATR real,
Totm_H_SUPLG real,
PV_H_SUPLG real,
VE_H_SUPLG real,
AVE_H_SUPLG real,
Days_H_SUPLG real,
SOCVD_H_SUPLG real,
SOCP_H_SUPLG real,
SOCDurH_SUPLG real,
SOVS_H_SUPLG real,
SOVAll_H_SUPLG real,
Totm_L_SUPLG real,
PV_L_SUPLG real,
VE_L_SUPLG real,
AVE_L_SUPLG real,
Days_L_SUPLG real,
SOCVD_L_SUPLG real,
SOCP_L_SUPLG real,
SOCDurL_SUPLG real,
SOVS_L_SUPLG real,
SOVAll_L_SUPLG real,
Totm_A_SUPLG real,
PV_A_SUPLG real,
VE_A_SUPLG real,
AVE_A_SUPLG real,
Days_A_SUPLG real,
SOCVD_A_SUPLG real,
SOCP_A_SUPLG real,
SOCDurA_SUPLG real,
SOVS_A_SUPLG real,
SOVAll_A_SUPLG real,
Totm_H_LALIGA real,
PV_H_LALIGA real,
VE_H_LALIGA real,
AVE_H_LALIGA real,
Days_H_LALIGA real,
SOCVD_H_LALIGA real,
SOCP_H_LALIGA real,
SOCDurH_LALIGA real,
SOVS_H_LALIGA real,
SOVAll_H_LALIGA real,
Totm_L_LALIGA real,
PV_L_LALIGA real,
VE_L_LALIGA real,
AVE_L_LALIGA real,
Days_L_LALIGA real,
SOCVD_L_LALIGA real,
SOCP_L_LALIGA real,
SOCDurL_LALIGA real,
SOVS_L_LALIGA real,
SOVAll_L_LALIGA real,
Totm_A_LALIGA real,
PV_A_LALIGA real,
VE_A_LALIGA real,
AVE_A_LALIGA real,
Days_A_LALIGA real,
SOCVD_A_LALIGA real,
SOCP_A_LALIGA real,
SOCDurA_LALIGA real,
SOVS_A_LALIGA real,
SOVAll_A_LALIGA real,
Totm_H_TDFEUR real,
PV_H_TDFEUR real,
VE_H_TDFEUR real,
AVE_H_TDFEUR real,
Days_H_TDFEUR real,
SOCVD_H_TDFEUR real,
SOCP_H_TDFEUR real,
SOCDurH_TDFEUR real,
SOVS_H_TDFEUR real,
SOVAll_H_TDFEUR real,
Totm_L_TDFEUR real,
PV_L_TDFEUR real,
VE_L_TDFEUR real,
AVE_L_TDFEUR real,
Days_L_TDFEUR real,
SOCVD_L_TDFEUR real,
SOCP_L_TDFEUR real,
SOCDurL_TDFEUR real,
SOVS_L_TDFEUR real,
SOVAll_L_TDFEUR real,
Totm_A_TDFEUR real,
PV_A_TDFEUR real,
VE_A_TDFEUR real,
AVE_A_TDFEUR real,
Days_A_TDFEUR real,
SOCVD_A_TDFEUR real,
SOCP_A_TDFEUR real,
SOCDurA_TDFEUR real,
SOVS_A_TDFEUR real,
SOVAll_A_TDFEUR real,
Totm_H_TDFITV real,
PV_H_TDFITV real,
VE_H_TDFITV real,
AVE_H_TDFITV real,
Days_H_TDFITV real,
SOCVD_H_TDFITV real,
SOCP_H_TDFITV real,
SOCDurH_TDFITV real,
SOVS_H_TDFITV real,
SOVAll_H_TDFITV real,
Totm_L_TDFITV real,
PV_L_TDFITV real,
VE_L_TDFITV real,
AVE_L_TDFITV real,
Days_L_TDFITV real,
SOCVD_L_TDFITV real,
SOCP_L_TDFITV real,
SOCDurL_TDFITV real,
SOVS_L_TDFITV real,
SOVAll_L_TDFITV real,
Totm_A_TDFITV real,
PV_A_TDFITV real,
VE_A_TDFITV real,
AVE_A_TDFITV real,
Days_A_TDFITV real,
SOCVD_A_TDFITV real,
SOCP_A_TDFITV real,
SOCDurA_TDFITV real,
SOVS_A_TDFITV real,
SOVAll_A_TDFITV real,
Totm_H_USMGOLF real,
PV_H_USMGOLF real,
VE_H_USMGOLF real,
AVE_H_USMGOLF real,
Days_H_USMGOLF real,
SOCVD_H_USMGOLF real,
SOCP_H_USMGOLF real,
SOCDurH_USMGOLF real,
SOVS_H_USMGOLF real,
SOVAll_H_USMGOLF real,
Totm_L_USMGOLF real,
PV_L_USMGOLF real,
VE_L_USMGOLF real,
AVE_L_USMGOLF real,
Days_L_USMGOLF real,
SOCVD_L_USMGOLF real,
SOCP_L_USMGOLF real,
SOCDurL_USMGOLF real,
SOVS_L_USMGOLF real,
SOVAll_L_USMGOLF real,
Totm_A_USMGOLF real,
PV_A_USMGOLF real,
VE_A_USMGOLF real,
AVE_A_USMGOLF real,
Days_A_USMGOLF real,
SOCVD_A_USMGOLF real,
SOCP_A_USMGOLF real,
SOCDurA_USMGOLF real,
SOVS_A_USMGOLF real,
SOVAll_A_USMGOLF real,
Totm_H_USTENSS real,
PV_H_USTENSS real,
VE_H_USTENSS real,
AVE_H_USTENSS real,
Days_H_USTENSS real,
SOCVD_H_USTENSS real,
SOCP_H_USTENSS real,
SOCDurH_USTENSS real,
SOVS_H_USTENSS real,
SOVAll_H_USTENSS real,
Totm_L_USTENSS real,
PV_L_USTENSS real,
VE_L_USTENSS real,
AVE_L_USTENSS real,
Days_L_USTENSS real,
SOCVD_L_USTENSS real,
SOCP_L_USTENSS real,
SOCDurL_USTENSS real,
SOVS_L_USTENSS real,
SOVAll_L_USTENSS real,
Totm_A_USTENSS real,
PV_A_USTENSS real,
VE_A_USTENSS real,
AVE_A_USTENSS real,
Days_A_USTENSS real,
SOCVD_A_USTENSS real,
SOCP_A_USTENSS real,
SOCDurA_USTENSS real,
SOVS_A_USTENSS real,
SOVAll_A_USTENSS real,
Totm_H_USTENEUR real,
PV_H_USTENEUR real,
VE_H_USTENEUR real,
AVE_H_USTENEUR real,
Days_H_USTENEUR real,
SOCVD_H_USTENEUR real,
SOCP_H_USTENEUR real,
SOCDurH_USTENEUR real,
SOVS_H_USTENEUR real,
SOVAll_H_USTENEUR real,
Totm_L_USTENEUR real,
PV_L_USTENEUR real,
VE_L_USTENEUR real,
AVE_L_USTENEUR real,
Days_L_USTENEUR real,
SOCVD_L_USTENEUR real,
SOCP_L_USTENEUR real,
SOCDurL_USTENEUR real,
SOVS_L_USTENEUR real,
SOVAll_L_USTENEUR real,
Totm_A_USTENEUR real,
PV_A_USTENEUR real,
VE_A_USTENEUR real,
AVE_A_USTENEUR real,
Days_A_USTENEUR real,
SOCVD_A_USTENEUR real,
SOCP_A_USTENEUR real,
SOCDurA_USTENEUR real,
SOVS_A_USTENEUR real,
SOVAll_A_USTENEUR real);

commit;

grant all on dbarnett.v250_workshop_dataset_test to public;

*/


commit;

alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_BILION as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_ENGRUG as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_CAPCUP as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_NFLSS as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_PGATR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_SUPLG as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_LALIGA as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_TDFEUR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_TDFITV as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_USMGOLF as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_USTENSS as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_A_USTENEUR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_BILION as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_ENGRUG as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_CAPCUP as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_NFLSS as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_PGATR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_SUPLG as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_LALIGA as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_TDFEUR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_TDFITV as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_USMGOLF as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_USTENSS as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_L_USTENEUR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_BILION as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_ENGRUG as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_CAPCUP as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_NFLSS as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_PGATR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_SUPLG as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_LALIGA as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_TDFEUR as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_TDFITV as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_USMGOLF as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_USTENSS as real;
alter table dbarnett.v250_annualised_activity_table_for_workshop add AVE_H_USTENEUR as real;
commit;

--select AVE_H_USTENEUR from dbarnett.v250_annualised_activity_table_for_workshop

select SOCP_A_SUPLG
,count(*)
from dbarnett.v250_annualised_activity_table_for_workshop
group by SOCP_A_SUPLG
order by SOCP_A_SUPLG desc


select * from dbarnett.v250_annualised_activity_table_for_workshop where  SOCP_A_SUPLG >1

commit;



/*

update dbarnett.v250_epg_list
set analysis_right_grouped = 'UEFA Champions League -  Sky Sports'

from dbarnett.v250_epg_list
where analysis_right_grouped = 'UEFA Champions League -  Sky Sports (other Live)'
;
commit;

--select * from  dbarnett.v250_rights_broadcast_by_live_status where analysis_right='NFL - BBC'
--select * from  dbarnett.v250_sports_rights_epg_data_for_analysis where analysis_right='NFL - BBC' order by broadcast_datetime

select service_key , sum(viewing_duration_total) from dbarnett.v250_all_sports_programmes_viewed_deduped where channel_name='ITV1' group by service_key order by service_key