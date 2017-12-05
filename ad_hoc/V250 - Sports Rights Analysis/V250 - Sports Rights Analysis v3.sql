select  service_key
,vespa_name as channel_name

into v250_channel_to_service_key_lookup
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES 
--where effective_to >= '2999-12-31'
group by service_key
,channel_name
;
--select * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES  where service_key=6391
--select * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES  where epg_name = 'ITV1 Tyne Tees'

select service_key
,min(channel_name) as min_name
,max(channel_name) as max_name
,count(distinct channel_name) as different_channel_names
into #service_key_details
from v250_channel_to_service_key_lookup
group by service_key
;

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

select * from v250_channel_to_service_key_lookup_deduped order by service_key desc

---Import in Rights Data----

--drop table dbarnett.v250_sports_rights_programme_lookup;
create table dbarnett.v250_sports_rights_programme_lookup
(channel_name varchar(50)
,master_deal varchar(200)
,deal       varchar(200)
,title      varchar(200)
,broadcast_datetime_text varchar(200)
--,broadcast_datetime datetime
)
;
commit;
input into dbarnett.v250_sports_rights_programme_lookup
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\All Rights CSV for import.csv' format ascii;

commit;


alter table dbarnett.v250_sports_rights_programme_lookup add broadcast_datetime datetime;

update dbarnett.v250_sports_rights_programme_lookup
set broadcast_datetime=cast(broadcast_datetime_text as datetime)
from dbarnett.v250_sports_rights_programme_lookup
;
commit;
--select count(*) from dbarnett.v250_sports_rights_programme_lookup;

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
,cast (broadcast_start_date_time_local as date) as broadcast_date
into #epg_list
from sk_prod.Vespa_programme_schedule as a
left outer join #sports_service_keys as b
on a.service_key=b.service_key
where b.service_key is not null and broadcast_date between '2012-11-01' and '2013-10-31'
;
commit;


CREATE HG INDEX idx1 ON #epg_list (service_key);
CREATE HG INDEX idx2 ON #epg_list (broadcast_date);
commit;

---Join EPG Data to Rights data by day and serice_key

select a.*
,b.programme_name
,b.genre_description
,b.sub_genre_description
,b.dk_programme_instance_dim
,b.dk_programme_dim
,b.programme_instance_duration
,b.broadcast_start_date_time_local

into dbarnett.v250_sports_rights_with_possible_matches
from dbarnett.v250_sports_rights_with_service_key  as a
left outer join #epg_list as b
on a.service_key=b.service_key and a.broadcast_date=b.broadcast_date
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

--select top 500 * from dbarnett.v250_sports_rights_epg_detail 

--Run Check of Titles selected--


select service_key
,channel_name 
, title
, programme_name
,broadcast_datetime
,broadcast_start_date_time_local
,time_from_broadcast_absolute
,rank
,case when time_from_broadcast_absolute=0 then '01: Same Time' 
 when time_from_broadcast_absolute<=300 then '02: Up to 5 Minutes either side' 
 when time_from_broadcast_absolute<=900 then '03: Up to 15 Minutes either side' 
when time_from_broadcast_absolute<=1800 then '04: Up to 30 Minutes either side' else '05: Over 30 minutes difference' 
 end as time_difference
from dbarnett.v250_sports_rights_epg_detail
order by channel_name
,broadcast_datetime
,broadcast_start_date_time_local;

output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\programme match.csv' format ascii;

commit;



select channel_name , case when time_from_broadcast_absolute=0 then '01: Same Time' 
 when time_from_broadcast_absolute<=300 then '02: Up to 5 Minutes either side' 
 when time_from_broadcast_absolute<=900 then '03: Up to 15 Minutes either side' 
when time_from_broadcast_absolute<=1800 then '04: Up to 30 Minutes either side' else '05: Over 30 minutes difference' 
 end as time_difference
, count(*) as records 
from dbarnett.v250_sports_rights_epg_detail 
group by channel_name , time_difference
order by channel_name , time_difference;

commit;
--select count(*) from dbarnett.v250_sports_rights_epg_detail 
---Get All Viewing Details from Sep 2013--


select 
a.account_number
,dk_programme_instance_dim
,service_key
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #sep_2013_viewing
from  sk_prod.vespa_dp_prog_viewed_201309 as a
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and right(account_number,3)='086'
group by a.account_number
,dk_programme_instance_dim
,service_key
;

commit;


CREATE HG INDEX idx1 ON #sep_2013_viewing (dk_programme_instance_dim);
commit;


select a.account_number
,case when b.master_deal is null then genre_description||' '||Channel_name as right_type
,b.deal 
,sum(viewing_duration) as total_duration_viewed
,sum(case when viewing_duration>=180 then 1 else 0 end) as programmes_viewed_03min_plus
,sum(case when viewing_duration>=900 then 1 else 0 end) as programmes_viewed_15min_plus
into #viewing_with_rights_details
from #sep_2013_viewing as a

left outer join dbarnett.v250_sports_rights_epg_detail as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where b.dk_programme_instance_dim is not null
group by  account_number
,right_type
,deal 


---Get all Eurosport Programme Titles---


select  programme_instance_name
,min(synopsis) as programme_detail
from sk_prod.Vespa_programme_schedule 
where service_key in (4004,1726,4009,1841) and cast (broadcast_start_date_time_local as date)  between '2012-11-01' and '2013-10-31'
group by programme_instance_name
order by programme_instance_name
;
commit;





/*



select *  from dbarnett.v250_sports_rights_epg_detail  where channel_name= 'Sky Sports 1' and time_from_broadcast_absolute>1800
select * from sk_prod.Vespa_programme_schedule where service_key = 4002 and cast(broadcast_start_date_time_local as date) = '2013-02-06' order by broadcast_start_date_time_local 


select * from sk_prod.Vespa_programme_schedule where service_key = 6391 and cast(broadcast_start_date_time_local as date) = '2012-11-04' order by broadcast_start_date_time_local 

select distinct service_key from dbarnett.v250_sports_rights_epg_detail

select dk_programme_instance_dim , count(*) as recs, count(distinct dk_programme_instance_dim) as dist_rec from dbarnett.v250_sports_rights_epg_detail group by dk_programme_instance_dim
having recs>1

select * from dbarnett.v250_sports_rights_epg_detail where dk_programme_instance_dim=4800104

select * from sk_prod.Vespa_programme_schedule where service_key = 1322
 and cast(broadcast_start_date_time_local as date) = '2013-02-13' order by broadcast_start_date_time_local 
;


select service_key, channel_name, count(*) from dbarnett.v250_sports_rights_epg_detail group by service_key, channel_name order by service_key desc

select top 5000 * from dbarnett.v250_sports_rights_epg_detail 
where channel_name = 'BT Sport 1'
; 

select count(*) from dbarnett.v250_sports_rights_epg_detail 
where channel_name = 'BT Sport 1'
; 


select count(*) from dbarnett.v250_sports_rights_programme_lookup
where channel_name = 'BT Sport 1'
; 
commit;

select count(*) , count(distinct dk_programme_instance_dim) from dbarnett.v250_sports_rights_epg_detail where service_key <100000

select dk_programme_instance_dim,count(*) as recs , count(distinct dk_programme_instance_dim) 
from dbarnett.v250_sports_rights_epg_detail
where service_key <100000
group by dk_programme_instance_dim
 order by recs desc
;

select * from  dbarnett.v250_sports_rights_epg_detail where dk_programme_instance_dim=767202570

select * from  dbarnett.v250_sports_rights_with_possible_matches where dk_programme_instance_dim=767202570
select * from  sk_prod.Vespa_programme_schedule where dk_programme_instance_dim=811749744
select * from  sk_prod.Vespa_programme_schedule where channel_name = 'Sky Sports F1' and cast(broadcast_start_date_time_local as date) = '2013-06-21'
commit;






/*
select channel_name , broadcast_datetime
 , count(*) as records 
from dbarnett.v250_sports_rights_programme_lookup
group by channel_name , broadcast_datetime
order by records desc;


select service_key , broadcast_datetime
 , count(*) as records 
from dbarnett.v250_sports_rights_with_programme_details 
group by service_key , broadcast_datetime
order by records desc;
*/

--select * from dbarnett.v250_sports_rights_with_service_key;

--select * from sk_prod.Vespa_programme_schedule where service_key = 4002 and broadcast_start_date_time_local = '2013-06-07 14:30:00'

--select * from sk_prod.Vespa_programme_schedule where service_key = 2082 and cast(broadcast_start_date_time_local as date) = '2012-11-25' order by broadcast_start_date_time_local



/*

select *
,cast(broadcast_datetime_text as datetime) as bcast_dtime
--,cast(broadcast_datetime as date) as bcast_dtime2
--, convert(varchar(200),broadcast_datetime,120) 
from dbarnett.v250_sports_rights_programme_lookup;

select distinct channel_name from v250_channel_to_service_key_lookup_deduped order by channel_name;


select distinct vespa_name
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES 
order by vespa_name
select * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES where service_key = 2075

select   * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES order by epg_name

select distinct service_key from sk_prod.Vespa_programme_schedule where channel_name = 'BBC HD'


select * from sk_prod.Vespa_programme_schedule where channel_name = 'BBC HD'



---Check Channel Name spelling same on Service Key lookup as rights data---

*/












