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
--select top 100 * from dbarnett.v250_sports_rights_epg_detail ;
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
--drop table  dbarnett.v250_epg_live_non_live_lookup;
create table dbarnett.v250_epg_live_non_live_lookup
(row_number integer
,live integer

)
;
commit;
input into dbarnett.v250_epg_live_non_live_lookup
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\Live and Non Live Rights.csv' format ascii;

commit;

--select * from dbarnett.v250_epg_live_non_live_lookup

---Create Analysis Table of EPG Data ---

select a.*
,b.live

into dbarnett.v250_sports_rights_epg_data_for_analysis
from dbarnett.v250_sports_rights_epg_detail as a
left outer join dbarnett.v250_epg_live_non_live_lookup as b
on a.row_number = b.row_number 
where b.live is not null
;


CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_data_for_analysis (dk_programme_instance_dim);
commit;


select analysis_right
,live
,count(*)
from dbarnett.v250_sports_rights_epg_data_for_analysis 
group by analysis_right
,live
order by  analysis_right
,live



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











