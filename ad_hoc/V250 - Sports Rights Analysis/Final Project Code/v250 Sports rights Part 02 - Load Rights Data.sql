/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 02 (Load Rights Data)

        Analyst: Dan Barnett
        SK Prod: 5

        Loads in collated list of rights and then matches this to EPG data to get 'best' match on most likely programme
        (i.e, closest match between time supplied in rights data and EPG broadcast time)
        rights data a mix of Scheduled Broadcast time/Actual i.e., BARB broadcast time and fixture kick off

*/------------------------------------------------------------------------------------------------------------------

---Import in Rights Data----
--select * into v250_sports_rights_programme_lookup_old from v250_sports_rights_programme_lookup;commit;
--drop table v250_sports_rights_programme_lookup;
create table v250_sports_rights_programme_lookup
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
input into v250_sports_rights_programme_lookup
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\All Rights CSV for importv9.csv' format ascii;

commit;


alter table v250_sports_rights_programme_lookup add broadcast_datetime datetime;

update v250_sports_rights_programme_lookup
set broadcast_datetime=cast(broadcast_datetime_text as datetime)
from v250_sports_rights_programme_lookup
;
commit;
/*
---Correct 2 events where broadcast_datetime text incorrectly in title
update v250_sports_rights_programme_lookup
set broadcast_datetime=cast(title as datetime)
from v250_sports_rights_programme_lookup
where title = '2013-03-19 06:00:00'
;

select * from v250_sports_rights_programme_lookup where title = '2013-03-19 06:00:00'
select * from v250_sports_rights_programme_lookup where channel_name = 'ESPN'

commit;
*/
---Create Matched table of Service Key/Channel and Sports Rights---
drop table v250_sports_rights_with_service_key;
select a.*
,b.service_key
into v250_sports_rights_with_service_key
from v250_sports_rights_programme_lookup as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.channel_name = b.channel_name
;
commit;
--select * from v250_sports_rights_with_service_key;
---Add on EPG Data--
--Add Record ID on match to EPG data--
alter table v250_sports_rights_with_service_key add programme_id integer identity;
commit;

alter table v250_sports_rights_with_service_key add broadcast_date date;
commit;

update v250_sports_rights_with_service_key
set broadcast_date=cast (broadcast_datetime as date)
from v250_sports_rights_with_service_key
;
commit;

--select channel_name , count(*) from v250_sports_rights_with_programme_details  where dk_programme_instance_dim is not null group by channel_name
--select top 150 * from v250_sports_rights_with_programme_details
---Return all EPG data into a temp table to enable matching by day and service key--
--drop table #sports_service_keys;
select distinct service_key
into #sports_service_keys
from v250_sports_rights_with_service_key;
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
drop table v250_sports_rights_with_possible_matches;
select a.*
,b.programme_name
,b.genre_description
,b.sub_genre_description
,b.dk_programme_instance_dim
,b.dk_programme_dim
,b.programme_instance_duration
,b.broadcast_start_date_time_local
,b.synopsis
into v250_sports_rights_with_possible_matches
from v250_sports_rights_with_service_key  as a
left outer join #epg_list as b
on a.service_key=b.service_key
where a.broadcast_date between b.broadcast_date-1 and  b.broadcast_date+1
;
commit;

alter table v250_sports_rights_with_possible_matches add time_from_broadcast integer;

update v250_sports_rights_with_possible_matches
set time_from_broadcast=datediff(second,broadcast_start_date_time_local,broadcast_datetime)
from v250_sports_rights_with_possible_matches
;

alter table v250_sports_rights_with_possible_matches add time_from_broadcast_absolute integer;

update v250_sports_rights_with_possible_matches
set time_from_broadcast_absolute=case when time_from_broadcast<0 then time_from_broadcast*-1 else time_from_broadcast end
from v250_sports_rights_with_possible_matches
;
commit;
--select top 500 * from v250_sports_rights_with_possible_matches;
---Create Rank to find nearest record per service key, sports right and programme_dim---

alter table v250_sports_rights_with_possible_matches add record_id integer identity;


--alter table v250_sports_rights_with_possible_matches add programme_rank integer;

--drop table  #rank;
select a.*
,RANK() OVER ( PARTITION BY service_key

                ,broadcast_datetime

                ORDER BY time_from_broadcast_absolute ASC , record_id ) AS Rank
into #rank
from v250_sports_rights_with_possible_matches as a
;

commit;

----Only include records where rank = 1;
drop table v250_sports_rights_epg_detail ;
select *
into v250_sports_rights_epg_detail
from #rank
where rank =1 and service_key <10000 --Get Rid of Anytime/On demand service keys where broadcast time matching not possible
;
commit;
---Add on Eurosport Details---
--drop table v250_sports_rights_epg_detail_test;

---Get all Eurosport Programme Titles---
insert into v250_sports_rights_epg_detail
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

delete from v250_sports_rights_epg_detail where channel_name='Eurosport' and analysis_right='Other';
commit;
--select * from v250_sports_rights_epg_detail where channel_name='Eurosport';
--select * from v250_sports_rights_epg_detail_test
--select * from sk_prod.vespa_dp_prog_viewed

--Create Output table To be able to include/exclude best matches - i.e., in some cases closest match is not correct e..g,
--due to regional differences in programme broadcast--

alter table v250_sports_rights_epg_detail add row_number integer identity;
select  * from v250_sports_rights_epg_detail;
output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Full EPG Data For matching\Full EPG Data v2.csv' format ascii;
commit;

---Add Index
CREATE HG INDEX idx1 ON v250_sports_rights_epg_detail (dk_programme_instance_dim);
commit;
