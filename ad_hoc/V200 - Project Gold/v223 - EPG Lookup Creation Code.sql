

---EPG Type--

---Create Lookup of all sports programmes from EPG data---
--drop table dbarnett.v223_sports_epg_lookup;
select distinct dk_programme_instance_dim
into dbarnett.v223_sports_epg_lookup
from dbarnett.v223_all_sports_programmes_viewed
;
commit;

---Add on Other details from th EPG Table---
alter table dbarnett.v223_sports_epg_lookup add (
channel_name varchar(40)
,service_key bigint
,genre_description varchar(20)
,sub_genre_description varchar(20)
,programme_instance_duration int
,programme_instance_name varchar(40)
,synopsis varchar(350)
,broadcast_start_date_time_local datetime
,broadcast_start_date_time_utc datetime
,bss_name varchar(70)
);

commit;

update dbarnett.v223_sports_epg_lookup
set channel_name =b.channel_name
,service_key  =b.service_key
,genre_description  =b.genre_description
,sub_genre_description  =b.sub_genre_description
,programme_instance_duration  =b.programme_instance_duration
,programme_instance_name =b.programme_instance_name
,synopsis  =b.synopsis
,broadcast_start_date_time_local  =b.broadcast_start_date_time_local
,broadcast_start_date_time_utc  =b.broadcast_start_date_time_utc
,bss_name  =b.bss_name
from dbarnett.v223_sports_epg_lookup as a
left outer join sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;

---Add on grouped channel info---
alter table dbarnett.v223_sports_epg_lookup add channel_name_inc_hd_staggercast_channel_families varchar(100);
commit;
update dbarnett.v223_sports_epg_lookup
set channel_name_inc_hd_staggercast_channel_families =b.channel_name_inc_hd_staggercast_channel_families

from dbarnett.v223_sports_epg_lookup as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name=b.channel_name
;
commit;


CREATE HG INDEX idx1 ON dbarnett.v223_sports_epg_lookup (dk_programme_instance_dim);

CREATE HG INDEX idx2 ON dbarnett.v223_sports_epg_lookup (channel_name_inc_hd_staggercast_channel_families);

---Set Other TV to BT Sport--
update dbarnett.v223_sports_epg_lookup
set channel_name_inc_hd_staggercast_channel_families ='BT Sport'
from dbarnett.v223_sports_epg_lookup as a 
where channel_name='Other TV'
;
commit;

---Add on Duration Details---
--grant all on dbarnett.v223_sports_epg_lookup to public;
alter table dbarnett.v223_sports_epg_lookup add total_duration_viewed bigint;
commit;


select a.dk_programme_instance_dim  ,sum(viewing_duration) as total_duration
into #duration_by_programme
from dbarnett.v223_sports_epg_lookup  as a
left outer join dbarnett.v223_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.dk_programme_instance_dim
;
commit;

CREATE HG INDEX idx1 ON #duration_by_programme (dk_programme_instance_dim);
commit;
update dbarnett.v223_sports_epg_lookup
set total_duration_viewed =b.total_duration
from dbarnett.v223_sports_epg_lookup as a
left outer join  #duration_by_programme as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim

;
commit;

---Add on Detailed Split Info for football type--
--select * from shaha.F_Fixtures_EPG;
--select * from F_Fixtures_EPG;
alter table dbarnett.v223_sports_epg_lookup add programme_sub_genre_type varchar(50);
commit;

update dbarnett.v223_sports_epg_lookup
set programme_sub_genre_type =b.fixtures_league
from dbarnett.v223_sports_epg_lookup as a
left outer join  shaha.F_Fixtures_EPG as b
on a.dk_programme_instance_dim=b.pk_programme_instance_dim

;
commit;
--select count(*) from #epg_programme_duration_summary; 
--drop table #epg_programme_duration_summary;
select programme_instance_name
, broadcast_start_date_time_utc 
,sub_genre_description
,max(synopsis) as synopsis_desc
,max(channel_name_inc_hd_staggercast_channel_families) as channel_name_max
,max(programme_sub_genre_type) as sub_genre_type
, sum(total_duration_viewed) as tot_dur 
,RANK() OVER ( ORDER BY tot_dur desc,programme_instance_name
, broadcast_start_date_time_utc 
,sub_genre_description) AS Rank
into #epg_programme_duration_summary
from  dbarnett.v223_sports_epg_lookup 
group by programme_instance_name, broadcast_start_date_time_utc,sub_genre_description
order by tot_dur desc
;
--select distinct sub_genre_description from #epg_programme_duration_summary 
---Get All Programmes which were watched for 3500 hours plus by the Vespa Base--
commit;
--select * from #epg_programme_duration_summary where round(tot_dur/3600,0) >=3500 order by tot_dur desc
--select * from shaha.F_Daily
---Add on Sub genre type details---
--drop table v223_sub_genre_type_lookup;
create table v223_sub_genre_type_lookup
(rank integer
,sub_genre_type  varchar(100)
)
;

input into v223_sub_genre_type_lookup
from 'G:\RTCI\Lookup Tables\Project 223 - EPG Sub genre type lookup.csv' format ascii;

commit;
--select * from v200_channel_lookup_with_channel_family order by channel_name;
grant all on v223_sub_genre_type_lookup to public;
commit;

CREATE HG INDEX idx1 ON v223_sub_genre_type_lookup (rank);
commit;
--drop table #add_sub_genre_subtype;
select a.*
,case when b.sub_genre_type is null then sub_genre_description else b.sub_genre_type end as sub_genre_subtype 
into #add_sub_genre_subtype
from #epg_programme_duration_summary as a
left outer join v223_sub_genre_type_lookup as b
on a.rank=b.rank
;
commit;


---Add Back on to main EPG Table--
commit;

update dbarnett.v223_sports_epg_lookup
set programme_sub_genre_type =b.sub_genre_subtype
from dbarnett.v223_sports_epg_lookup as a
left outer join  #add_sub_genre_subtype as b
on a.programme_instance_name=b.programme_instance_name
and a.broadcast_start_date_time_utc =b.broadcast_start_date_time_utc
and a.sub_genre_description=b.sub_genre_description
;
commit;

CREATE HG INDEX idx3 ON dbarnett.v223_sports_epg_lookup (programme_sub_genre_type);
commit;


---Correct where BT Sport 1 and BT Sport 2 Included

---
update dbarnett.v223_sports_epg_lookup
set channel_name_inc_hd_staggercast_channel_families='BT Sport'
from dbarnett.v223_sports_epg_lookup
where left(channel_name_inc_hd_staggercast_channel_families,8)='BT Sport'
;
commit;

update dbarnett.v223_sports_epg_lookup
set channel_name_inc_hd_staggercast_channel_families='SBO'
from dbarnett.v223_sports_epg_lookup
where channel_name_inc_hd_staggercast_channel_families in (
'SBO PPV'
,'Sky Box Office'
,'SBO1'
,'SBO')
;
commit;

--select programme_sub_genre_type , count(*) from dbarnett.v223_sports_epg_lookup group by programme_sub_genre_type order by programme_sub_genre_type
--select * from dbarnett.v223_sports_epg_lookup where upper(synopsis)  like '%HEINEKEN CUP%'





---Create Summary by Account---
--select top 100 * from dbarnett.v223_all_sports_programmes_viewed;
--select top 100 * from dbarnett.v223_sports_epg_lookup;
--drop table v223_unbundling_viewing_summary_by_account;


update dbarnett.v223_sports_epg_lookup 
set programme_sub_genre_type= 'FA Cup'
where programme_sub_genre_type='FA CUP'
;

update dbarnett.v223_sports_epg_lookup 
set programme_sub_genre_type= 'WWE'
where programme_instance_name like 'WWE%' or programme_instance_name like '% WWE%'
;
commit;