
--select top 100 * from sk_prod.vespa_dp_prog_viewed_201308;
commit;
--drop table v223_unbundling_viewing_201308;

create table dbarnett.viewing_month_order_lookup
(month varchar(6)
,month_order integer
)
;

input into dbarnett.viewing_month_order_lookup
from 'G:\RTCI\Lookup Tables\Month Order Lookup.csv' format ascii;

commit;

--Create Lookup Table of All Daily Augs Tables that are populated---

CREATE TABLE Augs_Tables_Dates_Available (Date_ DATE, Rank INT); --  drop table F_Dates_Augs -- select * from F_Dates_Augs

INSERT INTO Augs_Tables_Dates_Available (Date_ , Rank)
(
SELECT DATEFORMAT(CAST(SUBSTRING(table_name, 18, 8) AS DATE), 'yyyy-mm-dd') AS Date_
        ,RANK() OVER ( PARTITION BY NULL ORDER BY Date_ ASC) AS Rank
        FROM   SP_TABLES()
        WHERE  table_owner = 'vespa_analysts'
        AND LOWER(table_name) LIKE 'vespa_daily_augs_%'
        AND LOWER(table_name) NOT LIKE '%invalid%'
--        GROUP  BY DATEFORMAT(CAST(SUBSTRING(table_name, 18, 8) AS DATE), 'yyyy-mm-dd') 
       
GROUP  BY Date_
ORDER  BY Date_ ASC
);

---select * from  Augs_Tables_Dates_Available;


---Create Empty table to insert all sports programmes viewed--
--drop table dbarnett.v223_all_sports_programmes_viewed;
create table dbarnett.v223_all_sports_programmes_viewed
(account_number varchar(20)
,dk_programme_instance_dim bigint
,viewing_duration int
)
;
commit;



--select * from dbarnett.viewing_month_order_lookup;

CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @month           varchar(6);
CREATE VARIABLE @var_sql                varchar(3000);
SET @var_cntr = 44;    --44= 201308





SET @var_sql = '
insert into dbarnett.v223_all_sports_programmes_viewed
(account_number
,dk_programme_instance_dim 
,viewing_duration 
)

select 
a.account_number
,dk_programme_instance_dim
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and genre_description=''Sports''
group by a.account_number
,dk_programme_instance_dim
;
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

--delete from Project_161_viewing_table;
commit;
while @var_cntr <= 45       --45=Sep 2013
begin
set @month=(select month from dbarnett.viewing_month_order_lookup where month_order=@var_cntr)
    EXECUTE(replace(@var_sql,'##^^*^*##',@month))
--    commit

    set @var_cntr = @var_cntr+1
end;
commit;
--delete from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim=-1; commit;
---Repeat for Daily Activity between 1st Aug and End Sep (28th when 1st Run)---

--select * from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim is null
--alter table dbarnett.v223_all_sports_programmes_viewed delete table_date;
--alter table dbarnett.v223_all_sports_programmes_viewed add table_date varchar(10);
--drop  VARIABLE @day       ;


CREATE VARIABLE @day           varchar(8);
SET @var_cntr = 3;    --2= 1st Aug 2012

SET @var_sql = '
insert into dbarnett.v223_all_sports_programmes_viewed
(account_number
,dk_programme_instance_dim 
,viewing_duration 
--,table_date
) 


select 
a.account_number
,pk_programme_instance_dim
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
from  vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where 
--panel_id = 12 and 
genre_description=''Sports''
group by a.account_number
,pk_programme_instance_dim

';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;
--select * into dbarnett.v223_all_sports_programmes_viewed_backup from dbarnett.v223_all_sports_programmes_viewed; commit;
--delete from Project_161_viewing_table;
commit;
while @var_cntr <= 363 --363 = 31st July 2013
begin
set @day=(select replace(cast(Date_ as varchar),'-','') from  Augs_Tables_Dates_Available where rank=@var_cntr)
    EXECUTE(replace(@var_sql,'##^^*^*##',@day))
--    commit

    set @var_cntr = @var_cntr+1
end;
commit;

---Add Index---


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed (account_number);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed (dk_programme_instance_dim);

commit;

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
---Run on sample first



--drop table dbarnett.v223_sports_epg_lookup_aug_12_jul_13;
--Create version of EPG table that's just Aug'12-Jul'13
--select distinct programme_sub_genre_type  from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 order by programme_sub_genre_type desc;
select * into dbarnett.v223_sports_epg_lookup_aug_12_jul_13 from dbarnett.v223_sports_epg_lookup 
where cast(broadcast_start_date_time_utc as date) between '2012-08-01' and '2013-07-31';
commit;

CREATE HG INDEX idx1 ON dbarnett.v223_sports_epg_lookup_aug_12_jul_13 (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_sports_epg_lookup_aug_12_jul_13 (channel_name_inc_hd_staggercast_channel_families);
CREATE HG INDEX idx3 ON dbarnett.v223_sports_epg_lookup_aug_12_jul_13 (programme_sub_genre_type);
commit;



---Add in Number of analysis days in the period that that type of Activity has viewing days for--
--drop table v223_broadcast_days_per_sub_type;
select case when channel_name_inc_hd_staggercast_channel_families = 'Sky Sports Channels' then 'Sky Sports Channels'
when channel_name_inc_hd_staggercast_channel_families in ( 'ESPN','BT Sport') then 'ESPN/BT Sport'
when channel_name_inc_hd_staggercast_channel_families in ( 'Sky Sports News') then 'Sky Sports News'
else 'Other Channel' end as channel_type_grouped

,case when programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then 'Football League'
when programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League') then 'Scottish Football'
else programme_sub_genre_type end as programme_sub_genre_type_grouped
, cast(broadcast_start_date_time_utc as date) as broadcast_date
into v223_broadcast_days_per_sub_type
from dbarnett.v223_sports_epg_lookup_aug_12_jul_13
group by channel_type_grouped
,programme_sub_genre_type_grouped,broadcast_date
;
commit;

select  channel_type_grouped
,programme_sub_genre_type_grouped
--,broadcast_date
,count(*) as broadcast_days
into #total_days_per_channel_programme_type
from v223_broadcast_days_per_sub_type
group by channel_type_grouped
,programme_sub_genre_type_grouped
--,broadcast_date
order by programme_sub_genre_type_grouped desc
;
--select * from #total_days_per_channel_programme_type;

--select count(*) from #total_days_per_channel_programme_type;
commit;
--CREATE HG INDEX idx1 ON #total_days_per_channel_programme_type (account_number);
CREATE HG INDEX idx2 ON  v223_broadcast_days_per_sub_type (channel_type_grouped);
CREATE HG INDEX idx3 ON  v223_broadcast_days_per_sub_type (programme_sub_genre_type_grouped);
CREATE HG INDEX idx4 ON  v223_broadcast_days_per_sub_type (broadcast_date);
commit;
--drop table v223_broadcast_days_per_sub_type_watched_by_account;drop table v223_broadcast_days_per_sub_type_cut_down;
select * into v223_broadcast_days_per_sub_type_cut_down from v223_broadcast_days_per_sub_type where programme_sub_genre_type_grouped in 
('Football League', 'Scottish Football','Cricket - Ashes','Formula 1','Premier League','Champions League','FA Cup'
,'World Cup Qualifiers','International Friendlies','Capital One Cup','La Liga','Europa League')
;

commit;
CREATE HG INDEX idx2 ON  v223_broadcast_days_per_sub_type_cut_down (channel_type_grouped);
CREATE HG INDEX idx3 ON  v223_broadcast_days_per_sub_type_cut_down (programme_sub_genre_type_grouped);
CREATE HG INDEX idx4 ON  v223_broadcast_days_per_sub_type_cut_down (broadcast_date);
commit;

--select distinct programme_sub_genre_type_grouped from v223_broadcast_days_per_sub_type order by programme_sub_genre_type_grouped;

--drop table v223_broadcast_days_per_sub_type_watched_by_account;
select account_number
,channel_type_grouped
,programme_sub_genre_type_grouped
,count(*) as days_viewing
into v223_broadcast_days_per_sub_type_watched_by_account
from dbarnett.v223_daily_viewing_duration as a
left outer join v223_broadcast_days_per_sub_type_cut_down as b
on a.viewing_date=b.broadcast_date
where b.broadcast_date is not null 
--and right(account_number,2)='55'
group by account_number
,channel_type_grouped
,programme_sub_genre_type_grouped
;

--select distinct programme_sub_genre_type_grouped from v223_broadcast_days_per_sub_type_watched_by_account order by programme_sub_genre_type_grouped;

commit;
CREATE HG INDEX idx2 ON  v223_broadcast_days_per_sub_type_watched_by_account (account_number);
/*
select account_number,count(*) as records from v223_broadcast_days_per_sub_type_watched_by_account group by account_number order by records desc;
select count(*),count(distinct account_number) from v223_broadcast_days_per_sub_type_watched_by_account;
select * from v223_broadcast_days_per_sub_type_watched_by_account where programme_sub_genre_type_grouped like '%Europa%' order by account_number , days_viewing;
*/
--drop table v223_days_viewing_genre_types_by_account;
--Rework to one record per account--
select account_number
,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Capital One Cup') then days_viewing else 0 end) as sky_capital_one_cup_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Champions League') then days_viewing else 0 end) as sky_champions_league_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Champions League') then days_viewing else 0 end) as other_champions_league_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Cricket - Ashes') then days_viewing else 0 end) as sky_ashes_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Cricket - Ashes') then days_viewing else 0 end) as other_ashes_days

,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Europa League') then days_viewing else 0 end) as other_Europa_League_days
,max(case when channel_type_grouped in ('ESPN/BT Sport') and programme_sub_genre_type_grouped in ('Europa League') then days_viewing else 0 end) as espn_bt_Europa_League_days

,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('FA Cup') then days_viewing else 0 end) as other_fa_cup_days
,max(case when channel_type_grouped in ('ESPN/BT Sport') and programme_sub_genre_type_grouped in ('FA Cup') then days_viewing else 0 end) as espn_bt_fa_cup_days


,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Football League') then days_viewing else 0 end) as sky_football_league_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Formula 1') then days_viewing else 0 end) as sky_F1_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Formula 1') then days_viewing else 0 end) as other_F1_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('International Friendlies') then days_viewing else 0 end) as sky_International_friendlies_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('International Friendlies') then days_viewing else 0 end) as other_International_friendlies_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('La Liga') then days_viewing else 0 end) as sky_la_liga_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Premier League') then days_viewing else 0 end) as sky_premier_league_days
,max(case when channel_type_grouped in ('ESPN/BT Sport') and programme_sub_genre_type_grouped in ('Premier League') then days_viewing else 0 end) as espn_bt_premier_league_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Scottish Football') then days_viewing else 0 end) as sky_scottish_football_days
,max(case when channel_type_grouped in ('ESPN/BT Sport') and programme_sub_genre_type_grouped in ('Scottish Football') then days_viewing else 0 end) as espn_bt_scottish_football_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('World Cup Qualifiers') then days_viewing else 0 end) as sky_World_Cup_Qualifiers_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('World Cup Qualifiers') then days_viewing else 0 end) as other_World_Cup_Qualifiers_days

,11 as total_sky_capital_one_cup_days
,23 as total_sky_champions_league_days
,14 as total_other_champions_league_days
,9 as total_sky_ashes_days
,9 as total_other_ashes_days

,7 as total_espn_bt_fa_cup_days
,12 as total_other_fa_cup_days

,9 as total_espn_bt_Europa_League_days
,14 as total_other_Europa_League_days

,66 as total_sky_football_league_days

,48 as total_sky_F1_days
,36 as total_other_F1_days

,6 as total_sky_international_friendlies_days
,15 as total_other_international_friendlies_days

,8 as total_sky_world_cup_qualifiers_days
,8 as total_other_world_cup_qualifiers_days

,54 as total_sky_la_liga_days

,72 as total_sky_premier_league_days
,23 as total_espn_premier_league_days

,28 as total_sky_scottish_football_days
,13 as total_espn_bt_scottish_football_days

into v223_days_viewing_genre_types_by_account
from v223_broadcast_days_per_sub_type_watched_by_account
group by account_number
;
commit;

--select sum(espn_bt_europa_league_days) from v223_days_viewing_genre_types_by_account

CREATE HG INDEX idx1 ON  v223_days_viewing_genre_types_by_account (account_number);
commit;








--drop table v223_unbundling_viewing_summary_by_account_sample;

--select top 100 *  from dbarnett.v223_all_sports_programmes_viewed;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit
--drop table v223_unbundling_viewing_summary_by_account;
select account_number
,sum(case when viewing_duration_post_5am >0 then 1 else 0 end) as days_with_viewing
into #count_by_day_v1
from dbarnett.v223_daily_viewing_duration
where viewing_date between '2012-08-01' and '2013-07-31'
group by account_number
;
commit;
CREATE HG INDEX idx1 ON #count_by_day_v1 (account_number);


drop table dbarnett.v223_all_sports_programmes_viewed_sample;
/*
select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed as a
where account_number is null and account_number is not null
;
*/
--insert into dbarnett.v223_all_sports_programmes_viewed_sample

select a.*
,c.channel_name_inc_hd_staggercast_channel_families
,c.sub_genre_description
,c.programme_sub_genre_type
,c.programme_instance_duration
into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed as a
left outer join #count_by_day_v1 as b
on a.account_number = b.account_number
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as c
on a.dk_programme_instance_dim=c.dk_programme_instance_dim
--where a.account_number = '621056141909'
where b.days_with_viewing>=280
;
commit;


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
CREATE HG INDEX idx3 ON dbarnett.v223_all_sports_programmes_viewed_sample (channel_name_inc_hd_staggercast_channel_families);
CREATE HG INDEX idx4 ON dbarnett.v223_all_sports_programmes_viewed_sample (sub_genre_description);
CREATE HG INDEX idx5 ON dbarnett.v223_all_sports_programmes_viewed_sample (programme_sub_genre_type);
CREATE HG INDEX idx6 ON dbarnett.v223_all_sports_programmes_viewed_sample (programme_instance_duration);
commit;
--drop table v223_unbundling_viewing_summary_by_account;
--select count(*) from v223_unbundling_viewing_summary_by_account;
--select top 10 * from v223_unbundling_viewing_summary_by_account;
--select * from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 order by channel_name_inc_hd_staggercast_channel_families
select account_number

,sum(viewing_duration) as total_viewing_duration_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
 then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_total

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_total

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' then  viewing_duration else 0 end) as viewing_duration_ESPN_total

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D') and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D') and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_non_sky_sports_football


,sum(case when sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_overall_football

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
 then  viewing_duration else 0 end) as viewing_duration_Terrestrial_total

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_Terrestrial_football


---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

--Champions League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Champions League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_champions_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='Champions League' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_champions_league

--FA Cup--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='FA Cup' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='FA Cup' then  viewing_duration else 0 end) as viewing_duration_ESPN_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports News') 
and programme_sub_genre_type='FA Cup' then  viewing_duration else 0 end) as viewing_duration_other_FA_Cup

--Europa League--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Europa League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Europa League' then  viewing_duration else 0 end) as viewing_duration_ESPN_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports Channels' ,'Sky 3D','Sky Sports News') 
and programme_sub_genre_type='Europa League' then  viewing_duration else 0 end) as viewing_duration_other_europa_league


--World Cup Qualifiers
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='World Cup Qualifiers' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_world_cup_qualifiers

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='World Cup Qualifiers' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_world_cup_qualifiers

--select distinct genre_description,programme_sub_genre_type from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 order by genre_description ,programme_sub_genre_type;

--International Friendlies
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='International Friendlies' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_International_Friendlies

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='International Friendlies' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_International_Friendlies

--Scottish Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Scottish_Football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League')  then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_Scottish_Football

--Capital One Cup
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('Capital One Cup') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Capital_One_Cup

--La Liga
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('La Liga') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_La_Liga

--Football League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') 
then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


---SSN All
,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky Sports News' 
then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_News

----Cricket
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - non Ashes' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_cricket_exc_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - Ashes' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_cricket_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - non Ashes' then  viewing_duration else 0 end) as viewing_duration_cricket_exc_ashes_non_Sky_Sports_or_SSN

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - Ashes' then  viewing_duration else 0 end) as viewing_duration_cricket_ashes_non_Sky_Sports_or_SSN

---Golf
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Ryder Cup' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Major' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_golf_major

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Ryder Cup' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Major' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_golf_major


---Tennis---
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Tennis' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_tennis

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Tennis' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_tennis_exc_wimbledon

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Wimbledon' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_wimbledon

---Motor Sport exc. F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Motor Sport' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_motor_sport_exc_f1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Motor Sport' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_motor_sport_exc_f1

---F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Formula 1' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_motor_sport_Formula_1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Formula 1' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_Formula_1

---Racing--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Racing' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_horse_racing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Racing' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_horse_racing

---Snooker/Pool--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Snooker/Pool' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_snooker_pool

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Snooker/Pool' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SSN_snooker_pool

---Wrestling--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Wrestling' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_inc_SBO_wrestling

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Wrestling' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SBO_wrestling


---WWE--
,sum(case when programme_sub_genre_type='WWE' then viewing_duration else 0 end) as viewing_duration_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','Sky 3D')
and programme_sub_genre_type='WWE' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('SBO')
and programme_sub_genre_type='WWE' then  viewing_duration else 0 end) as viewing_duration_SBO_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky 1 or 2') 
and programme_sub_genre_type='WWE' then  viewing_duration else 0 end) as viewing_duration_Sky_1_or_2_WWE


---Rugby----
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
 and sub_genre_description = 'Rugby' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
 and sub_genre_description = 'Rugby'  then  viewing_duration else 0 end) as viewing_duration_BT_Sport_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
 and sub_genre_description = 'Rugby'  then  viewing_duration else 0 end) as viewing_duration_ESPN_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','BT Sport','ESPN','Sky 3D') 
 and sub_genre_description = 'Rugby'  then  viewing_duration else 0 end) as viewing_duration_rugby_other_channels

---Darts
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Darts' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Darts

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Darts' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_Darts

--Boxing
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Boxing' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_inc_SBO_boxing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Boxing' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_or_SBO_boxing

-----Niche Sports (exludes Football/Rugby/Golf/Tennis/F1/Cricket)
--Duration
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description not in ('Cricket','Football','Rugby','Tennis','Golf') and programme_sub_genre_type not in ('Formula 1')  then  viewing_duration else 0 end) as viewing_duration_niche_sports_sky_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description not in ('Cricket','Football','Rugby','Tennis','Golf') and programme_sub_genre_type not in ('Formula 1')  then  viewing_duration else 0 end) as viewing_duration_niche_sports_non_sky_sports





---Repeat for Number of Programmes viewed>=180 sec
,sum(case when viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_total

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_BT_Sport_total

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_ESPN_total


,sum(case when sub_genre_description='Football' and viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_overall_football

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_sky_sports_football



,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Terrestrial_total

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Terrestrial_football


---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Premier League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Pick_TV_premier_league

--Champions League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Champions League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_champions_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='Champions League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_champions_league

--FA Cup--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='FA Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_BT_Sport_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='FA Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_ESPN_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports News') 
and programme_sub_genre_type='FA Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_other_FA_Cup

--Europa League--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Europa League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_BT_Sport_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Europa League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_ESPN_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports Channels' ,'Sky 3D','Sky Sports News') 
and programme_sub_genre_type='Europa League' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_other_europa_league


--World Cup Qualifiers
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='World Cup Qualifiers' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_world_cup_qualifiers

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='World Cup Qualifiers' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_world_cup_qualifiers

--select distinct genre_description,programme_sub_genre_type from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 order by genre_description ,programme_sub_genre_type;

--International Friendlies
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='International Friendlies' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_International_Friendlies

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='International Friendlies' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_International_Friendlies

--Scottish Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League') and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Scottish_Football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League')  and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_Scottish_Football

--Capital One Cup
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('Capital One Cup') and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Capital_One_Cup

--La Liga
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('La Liga') and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_La_Liga

--Football League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') 
and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Football_League


---SSN All
,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky Sports News' 
and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_News

----Cricket
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - non Ashes' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_cricket_exc_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - Ashes' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_cricket_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - non Ashes' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_cricket_exc_ashes_non_Sky_Sports_or_SSN

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - Ashes' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_cricket_ashes_non_Sky_Sports_or_SSN

---Golf
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Ryder Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Major' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_golf_major

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Ryder Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Major' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_golf_major


---Tennis---
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Tennis' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_tennis

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Tennis' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_tennis_exc_wimbledon

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Wimbledon' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_wimbledon

---Motor Sport exc. F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Motor Sport' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_motor_sport_exc_f1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Motor Sport' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_motor_sport_exc_f1

---F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Formula 1' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_motor_sport_Formula_1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Formula 1' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_Formula_1

---Racing--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Racing' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_horse_racing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Racing' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_horse_racing

---Snooker/Pool--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Snooker/Pool' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_snooker_pool

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Snooker/Pool' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SSN_snooker_pool

---Wrestling--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Wrestling' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_inc_SBO_wrestling

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Wrestling' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SBO_wrestling


---WWE--
,sum(case when programme_sub_genre_type='WWE'  and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','Sky 3D')
and programme_sub_genre_type='WWE' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('SBO')
and programme_sub_genre_type='WWE' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_SBO_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families  in ('Sky 1 or 2') 
and programme_sub_genre_type='WWE' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_1_or_2_WWE


---Rugby----
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
 and sub_genre_description = 'Rugby' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
 and sub_genre_description = 'Rugby'  and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_BT_Sport_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
 and sub_genre_description = 'Rugby'  and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_ESPN_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','BT Sport','ESPN','Sky 3D') 
 and sub_genre_description = 'Rugby'  and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_rugby_other_channels

---Darts
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Darts' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Darts

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Darts' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_Darts

--Boxing
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Boxing' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_inc_SBO_boxing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Boxing' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_or_SBO_boxing

-----Niche Sports (exludes Football/Rugby/Golf/Tennis/F1/Cricket)
--3min+
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description not in ('Cricket','Football','Rugby','Tennis','Golf') and programme_sub_genre_type not in ('Formula 1')  and viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_niche_sports_sky_sports
,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description not in ('Cricket','Football','Rugby','Tennis','Golf') and programme_sub_genre_type not in ('Formula 1')  and viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_niche_sports_non_sky_sports



---Repeat for Number where 60% or 1hr Viewed---
,sum(case when viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1 else 0 end) as programmes_engaged_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
 and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_total

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_total

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_total


,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
andsub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) 
as programmes_engaged_sky_sports_football


,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
andsub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) 
as programmes_engaged_non_sky_sports_football



,sum(case when sub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_overall_football

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
 and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Terrestrial_total

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
and sub_genre_description='Football'
 and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Terrestrial_football


---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Premier League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Pick_TV_premier_league

--Champions League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Champions League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_champions_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='Champions League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_champions_league

--FA Cup--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='FA Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='FA Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports News') 
and programme_sub_genre_type='FA Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_other_FA_Cup

--Europa League--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Europa League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Europa League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports Channels' ,'Sky 3D','Sky Sports News') 
and programme_sub_genre_type='Europa League' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_other_europa_league


--World Cup Qualifiers
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='World Cup Qualifiers' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_world_cup_qualifiers

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='World Cup Qualifiers' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_world_cup_qualifiers

--select distinct genre_description,programme_sub_genre_type from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 order by genre_description ,programme_sub_genre_type;

--International Friendlies
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='International Friendlies' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_International_Friendlies

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='International Friendlies' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_International_Friendlies

--Scottish Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League') and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Scottish_Football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League')  and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_Scottish_Football

--Capital One Cup
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('Capital One Cup') and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Capital_One_Cup

--La Liga
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('La Liga') and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_La_Liga

--Football League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') 
and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Football_League


---SSN All
,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky Sports News' 
and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_News

----Cricket
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - non Ashes' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_cricket_exc_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - Ashes' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_cricket_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - non Ashes' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_cricket_exc_ashes_non_Sky_Sports_or_SSN

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - Ashes' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_cricket_ashes_non_Sky_Sports_or_SSN

---Golf
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Ryder Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Major' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_golf_major

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Ryder Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Major' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_golf_major


---Tennis---
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Tennis' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_tennis

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Tennis' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_tennis_exc_wimbledon

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Wimbledon' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_wimbledon

---Motor Sport exc. F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Motor Sport' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_motor_sport_exc_f1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Motor Sport' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_motor_sport_exc_f1

---F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Formula 1' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_motor_sport_Formula_1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Formula 1' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_Formula_1

---Racing--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Racing' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_horse_racing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Racing' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_horse_racing

---Snooker/Pool--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Snooker/Pool' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_snooker_pool

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Snooker/Pool' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_snooker_pool

---Wrestling--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Wrestling' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_inc_SBO_wrestling

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Wrestling' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SBO_wrestling


---WWE--
,sum(case when programme_sub_genre_type='WWE' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','Sky 3D')
and programme_sub_genre_type='WWE' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('SBO')
and programme_sub_genre_type='WWE' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_SBO_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky 1 or 2') 
and programme_sub_genre_type='WWE' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_1_or_2_WWE


---Rugby----
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
 and sub_genre_description = 'Rugby' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
 and sub_genre_description = 'Rugby'  and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
 and sub_genre_description = 'Rugby'  and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','BT Sport','ESPN','Sky 3D') 
 and sub_genre_description = 'Rugby'  and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_rugby_other_channels

---Darts
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Darts' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Darts

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Darts' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_Darts

--Boxing
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Boxing' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_inc_SBO_boxing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Boxing' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SBO_boxing

-----Niche Sports (exludes Football/Rugby/Golf/Tennis/F1/Cricket)
--Engaged
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description not in ('Cricket','Football','Rugby','Tennis','Golf') and programme_sub_genre_type not in ('Formula 1')  and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_niche_sports_sky_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description not in ('Cricket','Football','Rugby','Tennis','Golf') and programme_sub_genre_type not in ('Formula 1')  and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_niche_sports_non_sky_sports




into v223_unbundling_viewing_summary_by_account
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
--left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
--on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;
--select top 100 * from dbarnett.v223_sports_epg_lookup_aug_12_jul_13;
--select * from v223_unbundling_viewing_summary_by_account;


-------End of Account Summary---

--select count(*) from v223_unbundling_viewing_summary_by_account;

---Add on Number of Viewing Days over the Aug 2012-July 2013 Period---



select account_number
,sum(case when viewing_duration_post_5am >0 then 1 else 0 end) as days_with_viewing
into #count_by_day
from dbarnett.v223_daily_viewing_duration
where viewing_date between '2012-08-01' and '2013-07-31'
group by account_number
;
commit;
CREATE HG INDEX idx1 ON #count_by_day (account_number);
commit;
alter table v223_unbundling_viewing_summary_by_account add days_with_viewing integer;

update v223_unbundling_viewing_summary_by_account
set days_with_viewing= case when b.days_with_viewing is null then 0 else b.days_with_viewing end
from v223_unbundling_viewing_summary_by_account as a
left outer join #count_by_day as b
on a.account_number=b.account_number
;
commit;

--select sky_f1_days , count(*) from  v223_days_viewing_genre_types_by_account group by sky_f1_days order by sky_f1_days

/*
select channel_type_grouped
,programme_sub_genre_type_grouped
,count(*) as records
from v223_broadcast_days_per_sub_type_cut_down
group by channel_type_grouped
,programme_sub_genre_type_grouped
order by 
programme_sub_genre_type_grouped
,channel_type_grouped
;
*/


----Add Ranks and Average Weekly Duration per area---

alter table v223_unbundling_viewing_summary_by_account add 
(
minutes_sport real
,minutes_sport_sky_sports real
,minutes_sport_sky_sports_news real

,minutes_sport_terrestrial real
,minutes_sport_football_terrestrial real

,minutes_sport_ESPN real
--,minutes_sport_BT real

,minutes_football_sky_sports real
,minutes_football_ESPN_BT real

,minutes_football_premier_league_sky_sports real
,minutes_football_premier_league_ESPN_BT real

,minutes_football_champions_league_sky_sports real
,minutes_football_champions_league_non_sky_sports real


,minutes_football_europa_league_ESPN_BT real
,minutes_football_europa_league_Other_Channels real

,minutes_football_fa_cup_ESPN_BT real
,minutes_football_fa_cup_Other_Channels real

,minutes_football_world_cup_qualifier_sky_sports real
,minutes_football_world_cup_qualifier_non_sky_sports real

,minutes_football_international_friendly_sky_sports real
,minutes_football_international_friendly_non_sky_sports real


,minutes_football_scottish_football_sky_sports real
,minutes_football_scottish_football_non_sky_sports real

,minutes_football_Capital_One_Cup_sky_sports real

,minutes_football_La_Liga_sky_sports real

,minutes_football_football_league_sky_sports real

,minutes_cricket_ashes_sky_sports real
,minutes_cricket_ashes_non_sky_sports real

,minutes_cricket_non_ashes_sky_sports real
,minutes_cricket_non_ashes_non_sky_sports real


,minutes_golf_sky_sports real
,minutes_golf_non_sky_sports real

,minutes_tennis_sky_sports real
,minutes_tennis_non_sky_sports real

,minutes_motor_sport_sky_sports real
,minutes_motor_sport_non_sky_sports real

,minutes_F1_sky_sports real
,minutes_F1_non_sky_sports real

,minutes_horse_racing_sky_sports real
,minutes_horse_racing_non_sky_sports real

,minutes_snooker_pool_sky_sports real
,minutes_snooker_pool_non_sky_sports real

,minutes_rugby_sky_sports real
,minutes_rugby_non_sky_sports real

,minutes_wrestling_sky_sports real
,minutes_wrestling_non_sky_sports real

,minutes_wwe real
,minutes_wwe_sky_sports real
,minutes_wwe_sbo real
,minutes_wwe_sky_1_or_2 real

,minutes_darts_sky_sports real
,minutes_darts_non_sky_sports real

,minutes_boxing_sky_sports real
,minutes_boxing_non_sky_sports real


---3min Plus Prog--
,annualised_programmes_3min_plus_sport real
,annualised_programmes_3min_plus_sport_sky_sports real
,annualised_programmes_3min_plus_sport_sky_sports_news real

,annualised_programmes_3min_plus_sport_terrestrial real
,annualised_programmes_3min_plus_sport_football_terrestrial real

,annualised_programmes_3min_plus_sport_ESPN real
--,annualised_programmes_3min_plus_sport_BT real

,annualised_programmes_3min_plus_football_sky_sports real
,annualised_programmes_3min_plus_football_ESPN_BT real

,annualised_programmes_3min_plus_football_premier_league_sky_sports real
,annualised_programmes_3min_plus_football_premier_league_ESPN_BT real

,annualised_programmes_3min_plus_football_champions_league_sky_sports real
,annualised_programmes_3min_plus_football_champions_league_non_sky_sports real


,annualised_programmes_3min_plus_football_europa_league_ESPN_BT real
,annualised_programmes_3min_plus_football_europa_league_Other_Channels real

,annualised_programmes_3min_plus_football_fa_cup_ESPN_BT real
,annualised_programmes_3min_plus_football_fa_cup_Other_Channels real

,annualised_programmes_3min_plus_football_world_cup_qualifier_sky_sports real
,annualised_programmes_3min_plus_football_world_cup_qualifier_non_sky_sports real

,annualised_programmes_3min_plus_football_international_friendly_sky_sports real
,annualised_programmes_3min_plus_football_international_friendly_non_sky_sports real


,annualised_programmes_3min_plus_football_scottish_football_sky_sports real
,annualised_programmes_3min_plus_football_scottish_football_non_sky_sports real

,annualised_programmes_3min_plus_football_Capital_One_Cup_sky_sports real

,annualised_programmes_3min_plus_football_La_Liga_sky_sports real

,annualised_programmes_3min_plus_football_football_league_sky_sports real

,annualised_programmes_3min_plus_cricket_ashes_sky_sports real
,annualised_programmes_3min_plus_cricket_ashes_non_sky_sports real

,annualised_programmes_3min_plus_cricket_non_ashes_sky_sports real
,annualised_programmes_3min_plus_cricket_non_ashes_non_sky_sports real


,annualised_programmes_3min_plus_golf_sky_sports real
,annualised_programmes_3min_plus_golf_non_sky_sports real

,annualised_programmes_3min_plus_tennis_sky_sports real
,annualised_programmes_3min_plus_tennis_non_sky_sports real

,annualised_programmes_3min_plus_motor_sport_sky_sports real
,annualised_programmes_3min_plus_motor_sport_non_sky_sports real

,annualised_programmes_3min_plus_F1_sky_sports real
,annualised_programmes_3min_plus_F1_non_sky_sports real

,annualised_programmes_3min_plus_horse_racing_sky_sports real
,annualised_programmes_3min_plus_horse_racing_non_sky_sports real

,annualised_programmes_3min_plus_snooker_pool_sky_sports real
,annualised_programmes_3min_plus_snooker_pool_non_sky_sports real

,annualised_programmes_3min_plus_rugby_sky_sports real
,annualised_programmes_3min_plus_rugby_non_sky_sports real

,annualised_programmes_3min_plus_wrestling_sky_sports real
,annualised_programmes_3min_plus_wrestling_non_sky_sports real

,annualised_programmes_3min_plus_wwe real
,annualised_programmes_3min_plus_wwe_sky_sports real
,annualised_programmes_3min_plus_wwe_sbo real
,annualised_programmes_3min_plus_wwe_sky_1_or_2 real

,annualised_programmes_3min_plus_darts_sky_sports real
,annualised_programmes_3min_plus_darts_non_sky_sports real

,annualised_programmes_3min_plus_boxing_sky_sports real
,annualised_programmes_3min_plus_boxing_non_sky_sports real


--Engaged Programmes--
,annualised_programmes_engaged_sport real
,annualised_programmes_engaged_sport_sky_sports real
,annualised_programmes_engaged_sport_sky_sports_news real

,annualised_programmes_engaged_sport_terrestrial real
,annualised_programmes_engaged_sport_football_terrestrial real

,annualised_programmes_engaged_sport_ESPN real
--,annualised_programmes_engaged_sport_BT real

,annualised_programmes_engaged_football_sky_sports real
,annualised_programmes_engaged_football_ESPN_BT real

,annualised_programmes_engaged_football_premier_league_sky_sports real
,annualised_programmes_engaged_football_premier_league_ESPN_BT real

,annualised_programmes_engaged_football_champions_league_sky_sports real
,annualised_programmes_engaged_football_champions_league_non_sky_sports real


,annualised_programmes_engaged_football_europa_league_ESPN_BT real
,annualised_programmes_engaged_football_europa_league_Other_Channels real

,annualised_programmes_engaged_football_fa_cup_ESPN_BT real
,annualised_programmes_engaged_football_fa_cup_Other_Channels real

,annualised_programmes_engaged_football_world_cup_qualifier_sky_sports real
,annualised_programmes_engaged_football_world_cup_qualifier_non_sky_sports real

,annualised_programmes_engaged_football_international_friendly_sky_sports real
,annualised_programmes_engaged_football_international_friendly_non_sky_sports real


,annualised_programmes_engaged_football_scottish_football_sky_sports real
,annualised_programmes_engaged_football_scottish_football_non_sky_sports real

,annualised_programmes_engaged_football_Capital_One_Cup_sky_sports real

,annualised_programmes_engaged_football_La_Liga_sky_sports real

,annualised_programmes_engaged_football_football_league_sky_sports real

,annualised_programmes_engaged_cricket_ashes_sky_sports real
,annualised_programmes_engaged_cricket_ashes_non_sky_sports real

,annualised_programmes_engaged_cricket_non_ashes_sky_sports real
,annualised_programmes_engaged_cricket_non_ashes_non_sky_sports real


,annualised_programmes_engaged_golf_sky_sports real
,annualised_programmes_engaged_golf_non_sky_sports real

,annualised_programmes_engaged_tennis_sky_sports real
,annualised_programmes_engaged_tennis_non_sky_sports real

,annualised_programmes_engaged_motor_sport_sky_sports real
,annualised_programmes_engaged_motor_sport_non_sky_sports real

,annualised_programmes_engaged_F1_sky_sports real
,annualised_programmes_engaged_F1_non_sky_sports real

,annualised_programmes_engaged_horse_racing_sky_sports real
,annualised_programmes_engaged_horse_racing_non_sky_sports real

,annualised_programmes_engaged_snooker_pool_sky_sports real
,annualised_programmes_engaged_snooker_pool_non_sky_sports real

,annualised_programmes_engaged_rugby_sky_sports real
,annualised_programmes_engaged_rugby_non_sky_sports real

,annualised_programmes_engaged_wrestling_sky_sports real
,annualised_programmes_engaged_wrestling_non_sky_sports real

,annualised_programmes_engaged_wwe real
,annualised_programmes_engaged_wwe_sky_sports real
,annualised_programmes_engaged_wwe_sbo real
,annualised_programmes_engaged_wwe_sky_1_or_2 real

,annualised_programmes_engaged_darts_sky_sports real
,annualised_programmes_engaged_darts_non_sky_sports real

,annualised_programmes_engaged_boxing_sky_sports real
,annualised_programmes_engaged_boxing_non_sky_sports real


---Rank Each section---

,rank_minutes_sport integer
,rank_minutes_sport_sky_sports integer
,rank_minutes_sport_sky_sports_news integer

,rank_minutes_sport_terrestrial integer
,rank_minutes_sport_football_terrestrial integer

,rank_minutes_sport_ESPN integer
--,rank_minutes_sport_BT integer


,rank_minutes_football_sky_sports integer
,rank_minutes_football_sky_sports_news integer
--,rank_minutes_football_ESPN_BT integer
--,rank_minutes_football_Other_Channels integer

,rank_minutes_football_premier_league_sky_sports integer
,rank_minutes_football_premier_league_ESPN_BT integer

,rank_minutes_football_champions_league_sky_sports integer
,rank_minutes_football_champions_league_non_sky_sports integer


,rank_minutes_football_europa_league_ESPN_BT integer
,rank_minutes_football_europa_league_Other_Channels integer

,rank_minutes_football_fa_cup_ESPN_BT integer
,rank_minutes_football_fa_cup_Other_Channels integer

,rank_minutes_football_world_cup_qualifier_sky_sports integer
,rank_minutes_football_world_cup_qualifier_non_sky_sports integer

,rank_minutes_football_international_friendly_sky_sports integer
,rank_minutes_football_international_friendly_non_sky_sports integer

,rank_minutes_football_Capital_One_Cup_sky_sports integer

,rank_minutes_football_La_Liga_sky_sports integer

,rank_minutes_football_football_league_sky_sports integer

,rank_minutes_cricket_ashes_sky_sports integer
,rank_minutes_cricket_ashes_non_sky_sports integer

,rank_minutes_cricket_non_ashes_sky_sports integer
,rank_minutes_cricket_non_ashes_non_sky_sports integer



,rank_minutes_golf_sky_sports integer
,rank_minutes_golf_non_sky_sports integer

,rank_minutes_tennis_sky_sports integer
,rank_minutes_tennis_non_sky_sports integer

,rank_minutes_motor_sport_sky_sports integer
,rank_minutes_motor_sport_non_sky_sports integer

,rank_minutes_F1_sky_sports integer
,rank_minutes_F1_non_sky_sports integer

,rank_minutes_horse_racing_sky_sports integer
,rank_minutes_horse_racing_non_sky_sports integer

,rank_minutes_snooker_pool_sky_sports integer
,rank_minutes_snooker_pool_non_sky_sports integer

,rank_minutes_rugby_sky_sports integer
,rank_minutes_rugby_non_sky_sports integer

,rank_minutes_wrestling_sky_sports integer
,rank_minutes_wrestling_non_sky_sports integer

,rank_minutes_wwe integer
,rank_minutes_wwe_sky_sports integer
,rank_minutes_wwe_sbo integer
,rank_minutes_wwe_sky_1_or_2 integer


,rank_minutes_darts_sky_sports integer
,rank_minutes_darts_non_sky_sports integer

,rank_minutes_boxing_sky_sports integer
,rank_minutes_boxing_non_sky_sports integer

---Rank by #3min+ progs

,rank_prog_3min_plus_sport integer
,rank_prog_3min_plus_sport_sky_sports integer
,rank_prog_3min_plus_sport_sky_sports_news integer

,rank_prog_3min_plus_sport_terrestrial integer
,rank_prog_3min_plus_sport_football_terrestrial integer

,rank_prog_3min_plus_sport_ESPN integer
--,rank_prog_3min_plus_sport_BT integer


,rank_prog_3min_plus_football_sky_sports integer
,rank_prog_3min_plus_football_sky_sports_news integer
--,rank_prog_3min_plus_football_ESPN_BT integer
--,rank_prog_3min_plus_football_Other_Channels integer

,rank_prog_3min_plus_football_premier_league_sky_sports integer
,rank_prog_3min_plus_football_premier_league_ESPN_BT integer

,rank_prog_3min_plus_football_champions_league_sky_sports integer
,rank_prog_3min_plus_football_champions_league_non_sky_sports integer


,rank_prog_3min_plus_football_europa_league_ESPN_BT integer
,rank_prog_3min_plus_football_europa_league_Other_Channels integer

,rank_prog_3min_plus_football_fa_cup_ESPN_BT integer
,rank_prog_3min_plus_football_fa_cup_Other_Channels integer

,rank_prog_3min_plus_football_world_cup_qualifier_sky_sports integer
,rank_prog_3min_plus_football_world_cup_qualifier_non_sky_sports integer

,rank_prog_3min_plus_football_international_friendly_sky_sports integer
,rank_prog_3min_plus_football_international_friendly_non_sky_sports integer

,rank_prog_3min_plus_football_Capital_One_Cup_sky_sports integer

,rank_prog_3min_plus_football_La_Liga_sky_sports integer

,rank_prog_3min_plus_football_football_league_sky_sports integer

,rank_prog_3min_plus_cricket_ashes_sky_sports integer
,rank_prog_3min_plus_cricket_ashes_non_sky_sports integer

,rank_prog_3min_plus_cricket_non_ashes_sky_sports integer
,rank_prog_3min_plus_cricket_non_ashes_non_sky_sports integer



,rank_prog_3min_plus_golf_sky_sports integer
,rank_prog_3min_plus_golf_non_sky_sports integer

,rank_prog_3min_plus_tennis_sky_sports integer
,rank_prog_3min_plus_tennis_non_sky_sports integer

,rank_prog_3min_plus_motor_sport_sky_sports integer
,rank_prog_3min_plus_motor_sport_non_sky_sports integer

,rank_prog_3min_plus_F1_sky_sports integer
,rank_prog_3min_plus_F1_non_sky_sports integer

,rank_prog_3min_plus_horse_racing_sky_sports integer
,rank_prog_3min_plus_horse_racing_non_sky_sports integer

,rank_prog_3min_plus_snooker_pool_sky_sports integer
,rank_prog_3min_plus_snooker_pool_non_sky_sports integer

,rank_prog_3min_plus_rugby_sky_sports integer
,rank_prog_3min_plus_rugby_non_sky_sports integer

,rank_prog_3min_plus_wrestling_sky_sports integer
,rank_prog_3min_plus_wrestling_non_sky_sports integer

,rank_prog_3min_plus_wwe integer
,rank_prog_3min_plus_wwe_sky_sports integer
,rank_prog_3min_plus_wwe_sbo integer
,rank_prog_3min_plus_wwe_sky_1_or_2 integer


,rank_prog_3min_plus_darts_sky_sports integer
,rank_prog_3min_plus_darts_non_sky_sports integer

,rank_prog_3min_plus_boxing_sky_sports integer
,rank_prog_3min_plus_boxing_non_sky_sports integer


---Rank by engaged progs

,rank_prog_engaged_sport integer
,rank_prog_engaged_sport_sky_sports integer
,rank_prog_engaged_sport_sky_sports_news integer

,rank_prog_engaged_sport_terrestrial integer
,rank_prog_engaged_sport_football_terrestrial integer

,rank_prog_engaged_sport_ESPN integer
--,rank_prog_engaged_sport_BT integer


,rank_prog_engaged_football_sky_sports integer
,rank_prog_engaged_football_sky_sports_news integer
--,rank_prog_engaged_football_ESPN_BT integer
--,rank_prog_engaged_football_Other_Channels integer

,rank_prog_engaged_football_premier_league_sky_sports integer
,rank_prog_engaged_football_premier_league_ESPN_BT integer

,rank_prog_engaged_football_champions_league_sky_sports integer
,rank_prog_engaged_football_champions_league_non_sky_sports integer

,rank_prog_engaged_football_europa_league_ESPN_BT integer
,rank_prog_engaged_football_europa_league_Other_Channels integer

,rank_prog_engaged_football_fa_cup_ESPN_BT integer
,rank_prog_engaged_football_fa_cup_Other_Channels integer

,rank_prog_engaged_football_world_cup_qualifier_sky_sports integer
,rank_prog_engaged_football_world_cup_qualifier_non_sky_sports integer

,rank_prog_engaged_football_international_friendly_sky_sports integer
,rank_prog_engaged_football_international_friendly_non_sky_sports integer

,rank_prog_engaged_football_Capital_One_Cup_sky_sports integer

,rank_prog_engaged_football_La_Liga_sky_sports integer

,rank_prog_engaged_football_football_league_sky_sports integer

,rank_prog_engaged_cricket_ashes_sky_sports integer
,rank_prog_engaged_cricket_ashes_non_sky_sports integer

,rank_prog_engaged_cricket_non_ashes_sky_sports integer
,rank_prog_engaged_cricket_non_ashes_non_sky_sports integer



,rank_prog_engaged_golf_sky_sports integer
,rank_prog_engaged_golf_non_sky_sports integer

,rank_prog_engaged_tennis_sky_sports integer
,rank_prog_engaged_tennis_non_sky_sports integer

,rank_prog_engaged_motor_sport_sky_sports integer
,rank_prog_engaged_motor_sport_non_sky_sports integer

,rank_prog_engaged_F1_sky_sports integer
,rank_prog_engaged_F1_non_sky_sports integer

,rank_prog_engaged_horse_racing_sky_sports integer
,rank_prog_engaged_horse_racing_non_sky_sports integer

,rank_prog_engaged_snooker_pool_sky_sports integer
,rank_prog_engaged_snooker_pool_non_sky_sports integer

,rank_prog_engaged_rugby_sky_sports integer
,rank_prog_engaged_rugby_non_sky_sports integer

,rank_prog_engaged_wrestling_sky_sports integer
,rank_prog_engaged_wrestling_non_sky_sports integer

,rank_prog_engaged_wwe integer
,rank_prog_engaged_wwe_sky_sports integer
,rank_prog_engaged_wwe_sbo integer
,rank_prog_engaged_wwe_sky_1_or_2 integer


,rank_prog_engaged_darts_sky_sports integer
,rank_prog_engaged_darts_non_sky_sports integer

,rank_prog_engaged_boxing_sky_sports integer
,rank_prog_engaged_boxing_non_sky_sports integer

---Variables added in second phase of analysis (Footabll/Cricket/Niche Sports)-----
,viewing_duration_overall_football real
,viewing_duration_Sky_Sports_cricket_overall real
,viewing_duration_non_Sky_Sports_cricket_overall real
,viewing_duration_niche_sports_sky_sports real
,viewing_duration_niche_sports_non_sky_sports real

,annualised_programmes_3min_plus_overall_football real
,annualised_programmes_3min_plus_Sky_Sports_cricket_overall real
,annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall real
,annualised_programmes_3min_plus_niche_sports_sky_sports real
,annualised_programmes_3min_plus_niche_sports_non_sky_sports real

,annualised_programmes_engaged_overall_football real
,annualised_programmes_engaged_Sky_Sports_cricket_overall real
,annualised_programmes_engaged_non_Sky_Sports_cricket_overall real
,annualised_programmes_engaged_niche_sports_sky_sports real
,annualised_programmes_engaged_niche_sports_non_sky_sports real


,rank_minutes_overall_football integer
,rank_minutes_Sky_Sports_cricket_overall integer
,rank_minutes_non_Sky_Sports_cricket_overall integer
,rank_minutes_niche_sports_sky_sports integer
,rank_minutes_niche_sports_non_sky_sports integer

,rank_prog_3min_plus_overall_football integer
,rank_prog_3min_plus_Sky_Sports_cricket_overall integer
,rank_prog_3min_plus_non_Sky_Sports_cricket_overall integer
,rank_prog_3min_plus_niche_sports_sky_sports integer
,rank_prog_3min_plus_niche_sports_non_sky_sports integer

,rank_prog_engaged_overall_football integer
,rank_prog_engaged_Sky_Sports_cricket_overall integer
,rank_prog_engaged_non_Sky_Sports_cricket_overall integer
,rank_prog_engaged_niche_sports_sky_sports integer
,rank_prog_engaged_niche_sports_non_sky_sports integer

---Phase 2 extra variables---
,minutes_sky_sports_football real
,minutes_non_sky_sports_football real
,annualised_programmes_3min_plus_sky_sports_football real
,annualised_programmes_3min_plus_non_sky_sports_football real
,annualised_programmes_engaged_sky_sports_football real
,annualised_programmes_engaged_non_sky_sports_football real

,minutes_sky_sports_exc_wwe real
,annualised_programmes_3min_plus_sky_sports_exc_wwe real
,annualised_programmes_engaged_sky_sports_exc_wwe real



,rank_minutes_sky_sports_football integer
,rank_minutes_non_sky_sports_football integer
,rank_prog_3min_plus_sky_sports_football integer
,rank_prog_3min_plus_non_sky_sports_football integer
,rank_prog_engaged_sky_sports_football integer
,rank_prog_engaged_non_sky_sports_football integer

,rank_minutes_sky_sports_exc_wwe integer
,rank_prog_3min_plus_sky_sports_exc_wwe integer
,rank_prog_engaged_sky_sports_exc_wwe integer


)
;


update v223_unbundling_viewing_summary_by_account
set 

minutes_sport =(total_viewing_duration_sports
)/60*(365/cast(days_with_viewing as real))

,minutes_sport_sky_sports =
(viewing_duration_Sky_Sports_total
)/60*(365/cast(days_with_viewing as real))

,minutes_football_sky_sports =
(viewing_duration_sky_sports_football
)/60*(365/cast(days_with_viewing as real))

,minutes_football_non_sky_sports =
(viewing_duration_sky_sports_football
)/60*(365/cast(days_with_viewing as real))




,minutes_sport_espn =viewing_duration_espn_total/60*(365/cast(days_with_viewing as real))
,minutes_sport_terrestrial =viewing_duration_Terrestrial_total/60*(365/cast(days_with_viewing as real))
,minutes_sport_football_terrestrial =viewing_duration_Terrestrial_football/60*(365/cast(days_with_viewing as real))

,minutes_sport_sky_sports_news =viewing_duration_Sky_Sports_News/60*(365/cast(days_with_viewing as real))

,minutes_football_premier_league_sky_sports =case when sky_premier_league_days=0 then -1 
when cast(sky_premier_league_days as real)/total_sky_premier_league_days<0.5 then -1 
else viewing_duration_Sky_Sports_premier_league/60*(total_sky_premier_league_days/cast(sky_premier_league_days as real)) end


,minutes_football_premier_league_ESPN_BT =case when espn_bt_premier_league_days=0 then -1 
when cast(espn_bt_premier_league_days as real)/total_espn_premier_league_days
<0.5 then -1 
else (viewing_duration_BT_Sport_premier_league+
viewing_duration_ESPN_premier_league)/60*(total_espn_premier_league_days
/cast(espn_bt_premier_league_days as real)) end

,minutes_football_champions_league_sky_sports =case when sky_champions_league_days=0 then -1 
when cast(sky_champions_league_days as real)/total_sky_champions_league_days<0.5 then -1 
else viewing_duration_Sky_Sports_champions_league/60*(total_sky_champions_league_days/cast(sky_champions_league_days as real)) end

,minutes_football_champions_league_non_sky_sports =case when other_champions_league_days =0 then -1 
when cast(other_champions_league_days as real)/total_other_champions_league_days <0.5 then -1 
else viewing_duration_non_sky_Sports_champions_league/60*(total_other_champions_league_days 
/cast(other_champions_league_days as real)) end


,minutes_football_europa_league_ESPN_BT =case when espn_bt_europa_league_days=0 then -1 
when cast(espn_bt_europa_league_days as real)/total_espn_bt_europa_league_days<0.5 then -1 
else viewing_duration_ESPN_europa_league/60*(total_espn_bt_europa_league_days
/cast(espn_bt_europa_league_days as real)) end

,minutes_football_europa_league_Other_Channels =case when other_Europa_League_days=0 then -1 
when cast(other_Europa_League_days as real)/total_other_Europa_League_days<0.5 then -1 
else viewing_duration_other_europa_league/60*(total_other_Europa_League_days
/cast(other_Europa_League_days as real)) end

,minutes_football_fa_cup_ESPN_BT =case when espn_bt_fa_cup_days=0 then -1 
when cast(espn_bt_fa_cup_days as real)/total_espn_bt_fa_cup_days<0.5 then -1 
else (viewing_duration_BT_Sport_FA_Cup+viewing_duration_ESPN_FA_Cup)/60*(total_espn_bt_fa_cup_days
/cast(espn_bt_fa_cup_days as real)) end

,minutes_football_fa_cup_Other_Channels =case when other_fa_cup_days=0 then -1 
when cast(other_fa_cup_days as real)/total_other_fa_cup_days<0.5 then -1 
else viewing_duration_other_FA_Cup/60*(total_other_fa_cup_days
/cast(other_fa_cup_days as real)) end

,minutes_football_world_cup_qualifier_sky_sports =case when sky_World_Cup_Qualifiers_days=0 then -1 
when cast(sky_World_Cup_Qualifiers_days as real)/total_sky_World_Cup_Qualifiers_days<0.5 then -1 
else viewing_duration_Sky_Sports_World_Cup_Qualifiers/60*(total_sky_World_Cup_Qualifiers_days/cast(sky_World_Cup_Qualifiers_days as real)) end

,minutes_football_world_cup_qualifier_non_sky_sports =case when other_World_Cup_Qualifiers_days=0 then -1 
when cast(other_World_Cup_Qualifiers_days as real)/total_other_World_Cup_Qualifiers_days<0.5 then -1 
else viewing_duration_non_sky_Sports_World_Cup_Qualifiers/60*(total_other_World_Cup_Qualifiers_days/cast(other_World_Cup_Qualifiers_days as real)) end


,minutes_football_international_friendly_sky_sports =case when sky_international_friendlies_days=0 then -1 
when cast(sky_international_friendlies_days as real)/total_sky_international_friendlies_days<0.5 then -1 
else viewing_duration_Sky_Sports_international_friendlies/60*(total_sky_international_friendlies_days/cast(sky_international_friendlies_days as real)) end

,minutes_football_international_friendly_non_sky_sports =case when other_international_friendlies_days=0 then -1 
when cast(other_international_friendlies_days as real)/total_other_international_friendlies_days<0.5 then -1 
else viewing_duration_non_sky_Sports_international_friendlies/60*(total_other_international_friendlies_days/cast(other_international_friendlies_days as real)) end

,minutes_football_scottish_football_sky_sports =case when sky_scottish_football_days=0 then -1 
when cast(sky_scottish_football_days as real)/total_sky_scottish_football_days<0.5 then -1 
else viewing_duration_Sky_Sports_scottish_football/60*(total_sky_scottish_football_days/cast(sky_scottish_football_days as real)) end

,minutes_football_scottish_football_non_sky_sports =case when espn_bt_scottish_football_days=0 then -1 
when cast(espn_bt_scottish_football_days as real)/total_espn_bt_scottish_football_days<0.5 then -1 
else viewing_duration_non_sky_sports_scottish_football/60*(total_espn_bt_scottish_football_days/cast(espn_bt_scottish_football_days as real)) end


,minutes_football_Capital_One_Cup_sky_sports =case when sky_capital_one_cup_days=0 then -1 
when cast(sky_capital_one_cup_days as real)/total_sky_capital_one_cup_days<0.5 then -1 
else viewing_duration_Sky_Sports_capital_one_cup/60*(total_sky_capital_one_cup_days/cast(sky_capital_one_cup_days as real)) end


,minutes_football_La_Liga_sky_sports =case when sky_la_liga_days=0 then -1 
when cast(sky_la_liga_days as real)/total_sky_la_liga_days<0.5 then -1 
else viewing_duration_Sky_Sports_la_liga/60*(total_sky_la_liga_days/cast(sky_la_liga_days as real)) end

,minutes_football_football_league_sky_sports =case when sky_football_league_days=0 then -1 
when cast(sky_football_league_days as real)/total_sky_football_league_days<0.5 then -1 
else viewing_duration_Sky_Sports_football_league/60*(total_sky_football_league_days/cast(sky_football_league_days as real)) end

,minutes_cricket_ashes_sky_sports =case when sky_ashes_days=0 then -1 
when cast(sky_ashes_days as real)/total_sky_ashes_days<0.5 then -1 
else viewing_duration_Sky_Sports_cricket_ashes/60*(total_sky_ashes_days/cast(sky_ashes_days as real)) end

,minutes_cricket_ashes_non_sky_sports =case when other_ashes_days=0 then -1 
when cast(other_ashes_days as real)/total_other_ashes_days<0.5 then -1 
else viewing_duration_cricket_ashes_non_Sky_Sports_or_SSN
/60*(total_other_ashes_days/cast(other_ashes_days as real)) end

,minutes_cricket_non_ashes_sky_sports =viewing_duration_Sky_Sports_cricket_exc_ashes/60*(365/cast(days_with_viewing as real))
,minutes_cricket_non_ashes_non_sky_sports =
viewing_duration_cricket_exc_ashes_non_Sky_Sports_or_SSN/60*(365/cast(days_with_viewing as real))

,minutes_golf_sky_sports =
(viewing_duration_Sky_Sports_golf_other+
viewing_duration_Sky_Sports_golf_ryder_cup+
viewing_duration_Sky_Sports_golf_major)/60*(365/cast(days_with_viewing as real))

,minutes_golf_non_sky_sports =(viewing_duration_non_Sky_Sports_or_SSN_golf_other+
viewing_duration_non_Sky_Sports_or_SSN_golf_ryder_cup+
viewing_duration_non_Sky_Sports_or_SSN_golf_major

)/60*(365/cast(days_with_viewing as real))

,minutes_tennis_sky_sports =viewing_duration_Sky_Sports_tennis/60*(365/cast(days_with_viewing as real))

,minutes_tennis_non_sky_sports =
(viewing_duration_non_Sky_Sports_or_SSN_tennis_exc_wimbledon+
viewing_duration_non_Sky_Sports_or_SSN_wimbledon)/60*(365/cast(days_with_viewing as real))

,minutes_motor_sport_sky_sports =viewing_duration_Sky_Sports_motor_sport_exc_f1/60*(365/cast(days_with_viewing as real))
,minutes_motor_sport_non_sky_sports =viewing_duration_non_Sky_Sports_or_SSN_motor_sport_exc_f1/60*(365/cast(days_with_viewing as real))


,minutes_F1_sky_sports =case when sky_f1_days=0 then -1 
when cast(sky_f1_days as real)/total_sky_f1_days<0.5 then -1 
else viewing_duration_Sky_Sports_motor_sport_Formula_1/60*(total_sky_f1_days/cast(sky_f1_days as real)) end

,minutes_F1_non_sky_sports =case when other_f1_days=0 then -1 
when cast(other_f1_days as real)/total_other_f1_days<0.5 then -1 
else viewing_duration_non_Sky_Sports_or_SSN_Formula_1/60*(total_other_f1_days/cast(other_f1_days as real)) end


,minutes_horse_racing_sky_sports =viewing_duration_Sky_Sports_horse_racing/60*(365/cast(days_with_viewing as real))
,minutes_horse_racing_non_sky_sports =viewing_duration_non_Sky_Sports_or_SSN_horse_racing/60*(365/cast(days_with_viewing as real))

,minutes_snooker_pool_sky_sports =viewing_duration_Sky_Sports_snooker_pool/60*(365/cast(days_with_viewing as real))
,minutes_snooker_pool_non_sky_sports =viewing_duration_non_Sky_Sports_or_SSN_snooker_pool/60*(365/cast(days_with_viewing as real))

,minutes_rugby_sky_sports =viewing_duration_Sky_Sports_Rugby/60*(365/cast(days_with_viewing as real))
,minutes_rugby_non_sky_sports =(viewing_duration_BT_Sport_rugby+
viewing_duration_ESPN_rugby+
viewing_duration_rugby_other_channels)
/60*(365/cast(days_with_viewing as real))

,minutes_wrestling_sky_sports =viewing_duration_Sky_Sports_inc_SBO_wrestling/60*(365/cast(days_with_viewing as real))
,minutes_wrestling_non_sky_sports =viewing_duration_non_Sky_Sports_or_SBO_wrestling/60*(365/cast(days_with_viewing as real))

---WWE--

,minutes_wwe =viewing_duration_WWE/60*(365/cast(days_with_viewing as real))
,minutes_wwe_sky_sports =viewing_duration_Sky_Sports_WWE/60*(365/cast(days_with_viewing as real))
,minutes_wwe_sbo =viewing_duration_SBO_WWE/60*(365/cast(days_with_viewing as real))
,minutes_wwe_sky_1_or_2 =viewing_duration_Sky_1_or_2_WWE/60*(365/cast(days_with_viewing as real))

,minutes_darts_sky_sports =viewing_duration_Sky_Sports_Darts/60*(365/cast(days_with_viewing as real))
,minutes_darts_non_sky_sports =viewing_duration_non_Sky_Sports_Darts/60*(365/cast(days_with_viewing as real))

,minutes_boxing_sky_sports =viewing_duration_Sky_Sports_inc_SBO_boxing
/60*(365/cast(days_with_viewing as real))
,minutes_boxing_non_sky_sports =viewing_duration_non_Sky_Sports_or_SBO_boxing
/60*(365/cast(days_with_viewing as real))

---repeat for #3min+ programmes--


,annualised_programmes_3min_plus_sport =(programmes_3min_plus_sports
)*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_sport_sky_sports =
(programmes_3min_plus_Sky_Sports_total
)*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_football_sky_sports =
(programmes_3min_plus_Sky_Sports_premier_league+
programmes_3min_plus_Sky_Sports_champions_league+
programmes_3min_plus_Sky_Sports_world_cup_qualifiers+
programmes_3min_plus_Sky_Sports_International_Friendlies+
programmes_3min_plus_Sky_Sports_Scottish_Football+
programmes_3min_plus_Sky_Sports_Capital_One_Cup+
programmes_3min_plus_Sky_Sports_La_Liga+
programmes_3min_plus_Sky_Sports_Football_League
)*(365/cast(days_with_viewing as real))


,annualised_programmes_3min_plus_sport_espn =programmes_3min_plus_espn_total*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_sport_terrestrial =programmes_3min_plus_Terrestrial_total*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_sport_football_terrestrial =programmes_3min_plus_Terrestrial_football*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_sport_sky_sports_news =programmes_3min_plus_Sky_Sports_News*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_football_premier_league_sky_sports =case when sky_premier_league_days=0 then -1 
when cast(sky_premier_league_days as real)/total_sky_premier_league_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_premier_league*(total_sky_premier_league_days/cast(sky_premier_league_days as real)) end


,annualised_programmes_3min_plus_football_premier_league_ESPN_BT =case when espn_bt_premier_league_days=0 then -1 
when cast(espn_bt_premier_league_days as real)/total_espn_premier_league_days
<0.5 then -1 
else (programmes_3min_plus_BT_Sport_premier_league+
programmes_3min_plus_ESPN_premier_league)*(total_espn_premier_league_days
/cast(espn_bt_premier_league_days as real)) end

,annualised_programmes_3min_plus_football_champions_league_sky_sports =case when sky_champions_league_days=0 then -1 
when cast(sky_champions_league_days as real)/total_sky_champions_league_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_champions_league*(total_sky_champions_league_days/cast(sky_champions_league_days as real)) end

,annualised_programmes_3min_plus_football_champions_league_non_sky_sports =case when other_champions_league_days =0 then -1 
when cast(other_champions_league_days as real)/total_other_champions_league_days <0.5 then -1 
else programmes_3min_plus_non_sky_Sports_champions_league*(total_other_champions_league_days 
/cast(other_champions_league_days as real)) end


,annualised_programmes_3min_plus_football_europa_league_ESPN_BT =case when espn_bt_europa_league_days=0 then -1 
when cast(espn_bt_europa_league_days as real)/total_espn_bt_europa_league_days<0.5 then -1 
else programmes_3min_plus_ESPN_europa_league*(total_espn_bt_europa_league_days
/cast(espn_bt_europa_league_days as real)) end


,annualised_programmes_3min_plus_football_europa_league_Other_Channels =case when other_europa_league_days=0 then -1 
when cast(other_europa_league_days as real)/total_other_europa_league_days<0.5 then -1 
else programmes_3min_plus_other_europa_league*(total_other_europa_league_days
/cast(other_europa_league_days as real)) end


,annualised_programmes_3min_plus_football_fa_cup_ESPN_BT =case when espn_bt_fa_cup_days=0 then -1 
when cast(espn_bt_fa_cup_days as real)/total_espn_bt_fa_cup_days<0.5 then -1 
else (programmes_3min_plus_BT_Sport_FA_Cup+programmes_3min_plus_ESPN_FA_Cup)*(total_espn_bt_fa_cup_days
/cast(espn_bt_fa_cup_days as real)) end

,annualised_programmes_3min_plus_football_fa_cup_Other_Channels =case when other_fa_cup_days=0 then -1 
when cast(other_fa_cup_days as real)/total_other_fa_cup_days<0.5 then -1 
else programmes_3min_plus_other_FA_Cup*(total_other_fa_cup_days
/cast(other_fa_cup_days as real)) end

,annualised_programmes_3min_plus_football_world_cup_qualifier_sky_sports =case when sky_World_Cup_Qualifiers_days=0 then -1 
when cast(sky_World_Cup_Qualifiers_days as real)/total_sky_World_Cup_Qualifiers_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_World_Cup_Qualifiers*(total_sky_World_Cup_Qualifiers_days/cast(sky_World_Cup_Qualifiers_days as real)) end

,annualised_programmes_3min_plus_football_world_cup_qualifier_non_sky_sports =case when other_World_Cup_Qualifiers_days=0 then -1 
when cast(other_World_Cup_Qualifiers_days as real)/total_other_World_Cup_Qualifiers_days<0.5 then -1 
else programmes_3min_plus_non_sky_Sports_World_Cup_Qualifiers*(total_other_World_Cup_Qualifiers_days/cast(other_World_Cup_Qualifiers_days as real)) end


,annualised_programmes_3min_plus_football_international_friendly_sky_sports =case when sky_international_friendlies_days=0 then -1 
when cast(sky_international_friendlies_days as real)/total_sky_international_friendlies_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_international_friendlies*(total_sky_international_friendlies_days/cast(sky_international_friendlies_days as real)) end

,annualised_programmes_3min_plus_football_international_friendly_non_sky_sports =case when other_international_friendlies_days=0 then -1 
when cast(other_international_friendlies_days as real)/total_other_international_friendlies_days<0.5 then -1 
else programmes_3min_plus_non_sky_Sports_international_friendlies*(total_other_international_friendlies_days/cast(other_international_friendlies_days as real)) end

,annualised_programmes_3min_plus_football_scottish_football_sky_sports =case when sky_scottish_football_days=0 then -1 
when cast(sky_scottish_football_days as real)/total_sky_scottish_football_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_scottish_football*(total_sky_scottish_football_days/cast(sky_scottish_football_days as real)) end

,annualised_programmes_3min_plus_football_scottish_football_non_sky_sports =case when espn_bt_scottish_football_days=0 then -1 
when cast(espn_bt_scottish_football_days as real)/total_espn_bt_scottish_football_days<0.5 then -1 
else programmes_3min_plus_non_sky_sports_scottish_football*(total_espn_bt_scottish_football_days/cast(espn_bt_scottish_football_days as real)) end


,annualised_programmes_3min_plus_football_Capital_One_Cup_sky_sports =case when sky_capital_one_cup_days=0 then -1 
when cast(sky_capital_one_cup_days as real)/total_sky_capital_one_cup_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_capital_one_cup*(total_sky_capital_one_cup_days/cast(sky_capital_one_cup_days as real)) end


,annualised_programmes_3min_plus_football_La_Liga_sky_sports =case when sky_la_liga_days=0 then -1 
when cast(sky_la_liga_days as real)/total_sky_la_liga_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_la_liga*(total_sky_la_liga_days/cast(sky_la_liga_days as real)) end

,annualised_programmes_3min_plus_football_football_league_sky_sports =case when sky_football_league_days=0 then -1 
when cast(sky_football_league_days as real)/total_sky_football_league_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_football_league*(total_sky_football_league_days/cast(sky_football_league_days as real)) end

,annualised_programmes_3min_plus_cricket_ashes_sky_sports =case when sky_ashes_days=0 then -1 
when cast(sky_ashes_days as real)/total_sky_ashes_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_cricket_ashes*(total_sky_ashes_days/cast(sky_ashes_days as real)) end

,annualised_programmes_3min_plus_cricket_ashes_non_sky_sports =case when other_ashes_days=0 then -1 
when cast(other_ashes_days as real)/total_other_ashes_days<0.5 then -1 
else programmes_3min_plus_cricket_ashes_non_Sky_Sports_or_SSN
*(total_other_ashes_days/cast(other_ashes_days as real)) end

,annualised_programmes_3min_plus_cricket_non_ashes_sky_sports =programmes_3min_plus_Sky_Sports_cricket_exc_ashes*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_cricket_non_ashes_non_sky_sports =
programmes_3min_plus_cricket_exc_ashes_non_Sky_Sports_or_SSN*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_golf_sky_sports =
(programmes_3min_plus_Sky_Sports_golf_other+
programmes_3min_plus_Sky_Sports_golf_ryder_cup+
programmes_3min_plus_Sky_Sports_golf_major)*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_golf_non_sky_sports =(programmes_3min_plus_non_Sky_Sports_or_SSN_golf_other+
programmes_3min_plus_non_Sky_Sports_or_SSN_golf_ryder_cup+
programmes_3min_plus_non_Sky_Sports_or_SSN_golf_major

)*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_tennis_sky_sports =programmes_3min_plus_Sky_Sports_tennis*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_tennis_non_sky_sports =
(programmes_3min_plus_non_Sky_Sports_or_SSN_tennis_exc_wimbledon+
programmes_3min_plus_non_Sky_Sports_or_SSN_wimbledon)*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_motor_sport_sky_sports =programmes_3min_plus_Sky_Sports_motor_sport_exc_f1*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_motor_sport_non_sky_sports =programmes_3min_plus_non_Sky_Sports_or_SSN_motor_sport_exc_f1*(365/cast(days_with_viewing as real))


,annualised_programmes_3min_plus_F1_sky_sports =case when sky_f1_days=0 then -1 
when cast(sky_f1_days as real)/total_sky_f1_days<0.5 then -1 
else programmes_3min_plus_Sky_Sports_motor_sport_Formula_1*(total_sky_f1_days/cast(sky_f1_days as real)) end

,annualised_programmes_3min_plus_F1_non_sky_sports =case when other_f1_days=0 then -1 
when cast(other_f1_days as real)/total_other_f1_days<0.5 then -1 
else programmes_3min_plus_non_Sky_Sports_or_SSN_Formula_1*(total_other_f1_days/cast(other_f1_days as real)) end


,annualised_programmes_3min_plus_horse_racing_sky_sports =programmes_3min_plus_Sky_Sports_horse_racing*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_horse_racing_non_sky_sports =programmes_3min_plus_non_Sky_Sports_or_SSN_horse_racing*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_snooker_pool_sky_sports =programmes_3min_plus_Sky_Sports_snooker_pool*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_snooker_pool_non_sky_sports =programmes_3min_plus_non_Sky_Sports_or_SSN_snooker_pool*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_rugby_sky_sports =programmes_3min_plus_Sky_Sports_Rugby*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_rugby_non_sky_sports =(programmes_3min_plus_BT_Sport_rugby+
programmes_3min_plus_ESPN_rugby+
programmes_3min_plus_rugby_other_channels)
*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_wrestling_sky_sports =programmes_3min_plus_Sky_Sports_inc_SBO_wrestling*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_wrestling_non_sky_sports =programmes_3min_plus_non_Sky_Sports_or_SBO_wrestling*(365/cast(days_with_viewing as real))

---WWE--

,annualised_programmes_3min_plus_wwe =programmes_3min_plus_WWE*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_wwe_sky_sports =programmes_3min_plus_Sky_Sports_WWE*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_wwe_sbo =programmes_3min_plus_SBO_WWE*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_wwe_sky_1_or_2 =programmes_3min_plus_Sky_1_or_2_WWE*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_darts_sky_sports =programmes_3min_plus_Sky_Sports_Darts*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_darts_non_sky_sports =programmes_3min_plus_non_Sky_Sports_Darts*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_boxing_sky_sports =programmes_3min_plus_Sky_Sports_inc_SBO_boxing
*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_boxing_non_sky_sports =programmes_3min_plus_non_Sky_Sports_or_SBO_boxing
*(365/cast(days_with_viewing as real))

---Repeat for engaged programmes--

--Engaged Programmes--

,annualised_programmes_engaged_sport =(programmes_engaged_sports
)*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_sport_sky_sports =
(programmes_engaged_Sky_Sports_total
)*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_football_sky_sports =
(programmes_engaged_Sky_Sports_premier_league+
programmes_engaged_Sky_Sports_champions_league+
programmes_engaged_Sky_Sports_world_cup_qualifiers+
programmes_engaged_Sky_Sports_International_Friendlies+
programmes_engaged_Sky_Sports_Scottish_Football+
programmes_engaged_Sky_Sports_Capital_One_Cup+
programmes_engaged_Sky_Sports_La_Liga+
programmes_engaged_Sky_Sports_Football_League
)*(365/cast(days_with_viewing as real))


,annualised_programmes_engaged_sport_espn =programmes_engaged_espn_total*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_sport_terrestrial =programmes_engaged_Terrestrial_total*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_sport_football_terrestrial =programmes_engaged_Terrestrial_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_sport_sky_sports_news =programmes_engaged_Sky_Sports_News*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_football_premier_league_sky_sports =case when sky_premier_league_days=0 then -1 
when cast(sky_premier_league_days as real)/total_sky_premier_league_days<0.5 then -1 
else programmes_engaged_Sky_Sports_premier_league*(total_sky_premier_league_days/cast(sky_premier_league_days as real)) end


,annualised_programmes_engaged_football_premier_league_ESPN_BT =case when espn_bt_premier_league_days=0 then -1 
when cast(espn_bt_premier_league_days as real)/total_espn_premier_league_days
<0.5 then -1 
else (programmes_engaged_BT_Sport_premier_league+
programmes_engaged_ESPN_premier_league)*(total_espn_premier_league_days
/cast(espn_bt_premier_league_days as real)) end

,annualised_programmes_engaged_football_champions_league_sky_sports =case when sky_champions_league_days=0 then -1 
when cast(sky_champions_league_days as real)/total_sky_champions_league_days<0.5 then -1 
else programmes_engaged_Sky_Sports_champions_league*(total_sky_champions_league_days/cast(sky_champions_league_days as real)) end

,annualised_programmes_engaged_football_champions_league_non_sky_sports =case when other_champions_league_days =0 then -1 
when cast(other_champions_league_days as real)/total_other_champions_league_days <0.5 then -1 
else programmes_engaged_non_sky_Sports_champions_league*(total_other_champions_league_days 
/cast(other_champions_league_days as real)) end


,annualised_programmes_engaged_football_europa_league_ESPN_BT =case when espn_bt_europa_league_days=0 then -1 
when cast(espn_bt_europa_league_days as real)/total_espn_bt_europa_league_days<0.5 then -1 
else programmes_engaged_espn_europa_league*(total_espn_bt_europa_league_days
/cast(espn_bt_europa_league_days as real)) end


,annualised_programmes_engaged_football_europa_league_Other_Channels =case when other_europa_league_days=0 then -1 
when cast(other_europa_league_days as real)/total_other_europa_league_days<0.5 then -1 
else programmes_engaged_other_europa_league*(total_other_europa_league_days
/cast(other_europa_league_days as real)) end


,annualised_programmes_engaged_football_fa_cup_ESPN_BT =case when espn_bt_fa_cup_days=0 then -1 
when cast(espn_bt_fa_cup_days as real)/total_espn_bt_fa_cup_days<0.5 then -1 
else (programmes_engaged_BT_Sport_FA_Cup+programmes_engaged_ESPN_FA_Cup)*(total_espn_bt_fa_cup_days
/cast(espn_bt_fa_cup_days as real)) end

,annualised_programmes_engaged_football_fa_cup_Other_Channels =case when other_fa_cup_days=0 then -1 
when cast(other_fa_cup_days as real)/total_other_fa_cup_days<0.5 then -1 
else programmes_engaged_other_FA_Cup*(total_other_fa_cup_days
/cast(other_fa_cup_days as real)) end

,annualised_programmes_engaged_football_world_cup_qualifier_sky_sports =case when sky_World_Cup_Qualifiers_days=0 then -1 
when cast(sky_World_Cup_Qualifiers_days as real)/total_sky_World_Cup_Qualifiers_days<0.5 then -1 
else programmes_engaged_Sky_Sports_World_Cup_Qualifiers*(total_sky_World_Cup_Qualifiers_days/cast(sky_World_Cup_Qualifiers_days as real)) end

,annualised_programmes_engaged_football_world_cup_qualifier_non_sky_sports =case when other_World_Cup_Qualifiers_days=0 then -1 
when cast(other_World_Cup_Qualifiers_days as real)/total_other_World_Cup_Qualifiers_days<0.5 then -1 
else programmes_engaged_non_sky_Sports_World_Cup_Qualifiers*(total_other_World_Cup_Qualifiers_days/cast(other_World_Cup_Qualifiers_days as real)) end


,annualised_programmes_engaged_football_international_friendly_sky_sports =case when sky_international_friendlies_days=0 then -1 
when cast(sky_international_friendlies_days as real)/total_sky_international_friendlies_days<0.5 then -1 
else programmes_engaged_Sky_Sports_international_friendlies*(total_sky_international_friendlies_days/cast(sky_international_friendlies_days as real)) end

,annualised_programmes_engaged_football_international_friendly_non_sky_sports =case when other_international_friendlies_days=0 then -1 
when cast(other_international_friendlies_days as real)/total_other_international_friendlies_days<0.5 then -1 
else programmes_engaged_non_sky_Sports_international_friendlies*(total_other_international_friendlies_days/cast(other_international_friendlies_days as real)) end

,annualised_programmes_engaged_football_scottish_football_sky_sports =case when sky_scottish_football_days=0 then -1 
when cast(sky_scottish_football_days as real)/total_sky_scottish_football_days<0.5 then -1 
else programmes_engaged_Sky_Sports_scottish_football*(total_sky_scottish_football_days/cast(sky_scottish_football_days as real)) end

,annualised_programmes_engaged_football_scottish_football_non_sky_sports =case when espn_bt_scottish_football_days=0 then -1 
when cast(espn_bt_scottish_football_days as real)/total_espn_bt_scottish_football_days<0.5 then -1 
else programmes_engaged_non_sky_sports_scottish_football*(total_espn_bt_scottish_football_days/cast(espn_bt_scottish_football_days as real)) end


,annualised_programmes_engaged_football_Capital_One_Cup_sky_sports =case when sky_capital_one_cup_days=0 then -1 
when cast(sky_capital_one_cup_days as real)/total_sky_capital_one_cup_days<0.5 then -1 
else programmes_engaged_Sky_Sports_capital_one_cup*(total_sky_capital_one_cup_days/cast(sky_capital_one_cup_days as real)) end


,annualised_programmes_engaged_football_La_Liga_sky_sports =case when sky_la_liga_days=0 then -1 
when cast(sky_la_liga_days as real)/total_sky_la_liga_days<0.5 then -1 
else programmes_engaged_Sky_Sports_la_liga*(total_sky_la_liga_days/cast(sky_la_liga_days as real)) end

,annualised_programmes_engaged_football_football_league_sky_sports =case when sky_football_league_days=0 then -1 
when cast(sky_football_league_days as real)/total_sky_football_league_days<0.5 then -1 
else programmes_engaged_Sky_Sports_football_league*(total_sky_football_league_days/cast(sky_football_league_days as real)) end

,annualised_programmes_engaged_cricket_ashes_sky_sports =case when sky_ashes_days=0 then -1 
when cast(sky_ashes_days as real)/total_sky_ashes_days<0.5 then -1 
else programmes_engaged_Sky_Sports_cricket_ashes*(total_sky_ashes_days/cast(sky_ashes_days as real)) end

,annualised_programmes_engaged_cricket_ashes_non_sky_sports =case when other_ashes_days=0 then -1 
when cast(other_ashes_days as real)/total_other_ashes_days<0.5 then -1 
else programmes_engaged_cricket_ashes_non_Sky_Sports_or_SSN
*(total_other_ashes_days/cast(other_ashes_days as real)) end

,annualised_programmes_engaged_cricket_non_ashes_sky_sports =programmes_engaged_Sky_Sports_cricket_exc_ashes*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_cricket_non_ashes_non_sky_sports =
programmes_engaged_cricket_exc_ashes_non_Sky_Sports_or_SSN*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_golf_sky_sports =
(programmes_engaged_Sky_Sports_golf_other+
programmes_engaged_Sky_Sports_golf_ryder_cup+
programmes_engaged_Sky_Sports_golf_major)*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_golf_non_sky_sports =(programmes_engaged_non_Sky_Sports_or_SSN_golf_other+
programmes_engaged_non_Sky_Sports_or_SSN_golf_ryder_cup+
programmes_engaged_non_Sky_Sports_or_SSN_golf_major

)*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_tennis_sky_sports =programmes_engaged_Sky_Sports_tennis*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_tennis_non_sky_sports =
(programmes_engaged_non_Sky_Sports_or_SSN_tennis_exc_wimbledon+
programmes_engaged_non_Sky_Sports_or_SSN_wimbledon)*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_motor_sport_sky_sports =programmes_engaged_Sky_Sports_motor_sport_exc_f1*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_motor_sport_non_sky_sports =programmes_engaged_non_Sky_Sports_or_SSN_motor_sport_exc_f1*(365/cast(days_with_viewing as real))


,annualised_programmes_engaged_F1_sky_sports =case when sky_f1_days=0 then -1 
when cast(sky_f1_days as real)/total_sky_f1_days<0.5 then -1 
else programmes_engaged_Sky_Sports_motor_sport_Formula_1*(total_sky_f1_days/cast(sky_f1_days as real)) end

,annualised_programmes_engaged_F1_non_sky_sports =case when other_f1_days=0 then -1 
when cast(other_f1_days as real)/total_other_f1_days<0.5 then -1 
else programmes_engaged_non_Sky_Sports_or_SSN_Formula_1*(total_other_f1_days/cast(other_f1_days as real)) end


,annualised_programmes_engaged_horse_racing_sky_sports =programmes_engaged_Sky_Sports_horse_racing*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_horse_racing_non_sky_sports =programmes_engaged_non_Sky_Sports_or_SSN_horse_racing*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_snooker_pool_sky_sports =programmes_engaged_Sky_Sports_snooker_pool*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_snooker_pool_non_sky_sports =programmes_engaged_non_Sky_Sports_or_SSN_snooker_pool*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_rugby_sky_sports =programmes_engaged_Sky_Sports_Rugby*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_rugby_non_sky_sports =(programmes_engaged_BT_Sport_rugby+
programmes_engaged_ESPN_rugby+
programmes_engaged_rugby_other_channels)
*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_wrestling_sky_sports =programmes_engaged_Sky_Sports_inc_SBO_wrestling*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_wrestling_non_sky_sports =programmes_engaged_non_Sky_Sports_or_SBO_wrestling*(365/cast(days_with_viewing as real))

---WWE--

,annualised_programmes_engaged_wwe =programmes_engaged_WWE*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_wwe_sky_sports =programmes_engaged_Sky_Sports_WWE*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_wwe_sbo =programmes_engaged_SBO_WWE*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_wwe_sky_1_or_2 =programmes_engaged_Sky_1_or_2_WWE*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_darts_sky_sports =programmes_engaged_Sky_Sports_Darts*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_darts_non_sky_sports =programmes_engaged_non_Sky_Sports_Darts*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_boxing_sky_sports =programmes_engaged_Sky_Sports_inc_SBO_boxing
*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_boxing_non_sky_sports =programmes_engaged_non_Sky_Sports_or_SBO_boxing
*(365/cast(days_with_viewing as real))

----Second set of analysis variables

,minutes_overall_football =viewing_duration_overall_football/60*(365/cast(days_with_viewing as real))
,minutes_Sky_Sports_cricket_overall =viewing_duration_Sky_Sports_cricket_overall/60*(365/cast(days_with_viewing as real))
,minutes_non_Sky_Sports_cricket_overall =viewing_duration_non_Sky_Sports_cricket_overall/60*(365/cast(days_with_viewing as real))
,minutes_niche_sports_sky_sports =viewing_duration_niche_sports_sky_sports/60*(365/cast(days_with_viewing as real))
,minutes_niche_sports_non_sky_sports =viewing_duration_niche_sports_non_sky_sports/60*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_overall_football =programmes_3min_plus_overall_football *(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_Sky_Sports_cricket_overall = programmes_3min_plus_Sky_Sports_cricket_overall*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall = programmes_3min_plus_non_Sky_Sports_cricket_overall*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_niche_sports_sky_sports = programmes_3min_plus_niche_sports_sky_sports*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_niche_sports_non_sky_sports = programmes_3min_plus_niche_sports_non_sky_sports*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_overall_football = programmes_engaged_overall_football*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_Sky_Sports_cricket_overall = programmes_engaged_Sky_Sports_cricket_overall*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_non_Sky_Sports_cricket_overall = programmes_engaged_non_Sky_Sports_cricket_overall*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_niche_sports_sky_sports = programmes_engaged_niche_sports_sky_sports*(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_niche_sports_non_sky_sports = programmes_engaged_niche_sports_non_sky_sports*(365/cast(days_with_viewing as real))

---Phase 2 Variables---
,minutes_sky_sports_football =(viewing_duration_sky_sports_football
)/60*(365/cast(days_with_viewing as real))
,minutes_non_sky_sports_football =(viewing_duration_non_sky_sports_football
)/60*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_sky_sports_football 
=programmes_3min_plus_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_non_sky_sports_football 
=programmes_3min_plus_non_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_sky_sports_football 
=programmes_engaged_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_non_sky_sports_football
=programmes_engaged_non_sky_sports_football*(365/cast(days_with_viewing as real))

,minutes_sky_sports_exc_wwe =(viewing_duration_sky_sports_exc_wwe
)/60*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_sky_sports_exc_wwe 
=programmes_3min_plus_sky_sports_exc_wwe *(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_sky_sports_exc_wwe 
=programmes_engaged_sky_sports_exc_wwe *(365/cast(days_with_viewing as real))


from v223_unbundling_viewing_summary_by_account as a
left outer join v223_days_viewing_genre_types_by_account as b
on a.account_number =b.account_number
where days_with_viewing>=280
;
commit;

--select top 100 * from v223_unbundling_viewing_summary_by_account
--select top 100 * from v223_days_viewing_genre_types_by_account
---Update Ranks
--drop table #rank_minutes_details;
select account_number

,rank() over (  ORDER BY  minutes_sport desc) as rank_minutes_sport
,rank() over (  ORDER BY  minutes_sport_sky_sports desc) as rank_minutes_sport_sky_sports
,rank() over (  ORDER BY  minutes_sport_sky_sports_news desc) as rank_minutes_sport_sky_sports_news

,rank() over (  ORDER BY  minutes_sport_espn desc) as rank_minutes_sport_espn
,rank() over (  ORDER BY  minutes_sport_terrestrial desc) as rank_minutes_sport_terrestrial
,rank() over (  ORDER BY  minutes_sport_football_terrestrial desc) as rank_minutes_sport_football_terrestrial

,rank() over (  ORDER BY  minutes_football_sky_sports desc) as rank_minutes_football_sky_sports
,rank() over (  ORDER BY  minutes_sky_sports_football desc) as rank_minutes_sky_sports_football
,rank() over (  ORDER BY  minutes_non_sky_sports_football desc) as rank_minutes_non_sky_sports_football
,rank() over (  ORDER BY  minutes_sky_sports_exc_wwe desc) as rank_minutes_sky_sports_exc_wwe

--,rank() over (  ORDER BY  minutes_football_ESPN_BT desc) as rank_minutes_football_ESPN_BT

,rank() over (  ORDER BY  minutes_football_premier_league_sky_sports desc) as rank_minutes_football_premier_league_sky_sports
,rank() over (  ORDER BY  minutes_football_premier_league_ESPN_BT desc) as rank_minutes_football_premier_league_ESPN_BT

,rank() over (  ORDER BY  minutes_football_champions_league_sky_sports desc) as rank_minutes_football_champions_league_sky_sports
,rank() over (  ORDER BY  minutes_football_champions_league_non_sky_sports desc) as rank_minutes_football_champions_league_non_sky_sports

,rank() over (  ORDER BY  minutes_football_europa_league_ESPN_BT desc) as rank_minutes_football_europa_league_ESPN_BT
,rank() over (  ORDER BY  minutes_football_europa_league_Other_Channels desc) as rank_minutes_football_europa_league_Other_Channels

,rank() over (  ORDER BY  minutes_football_fa_cup_ESPN_BT desc) as rank_minutes_football_fa_cup_ESPN_BT
,rank() over (  ORDER BY  minutes_football_fa_cup_Other_Channels desc) as rank_minutes_football_fa_cup_Other_Channels

,rank() over (  ORDER BY  minutes_football_world_cup_qualifier_sky_sports desc) as rank_minutes_football_world_cup_qualifier_sky_sports
,rank() over (  ORDER BY  minutes_football_world_cup_qualifier_non_sky_sports desc) as rank_minutes_football_world_cup_qualifier_non_sky_sports

,rank() over (  ORDER BY  minutes_football_international_friendly_sky_sports desc) as rank_minutes_football_international_friendly_sky_sports
,rank() over (  ORDER BY  minutes_football_international_friendly_non_sky_sports desc) as rank_minutes_football_international_friendly_non_sky_sports


,rank() over (  ORDER BY  minutes_football_scottish_football_sky_sports desc) as rank_minutes_football_scottish_football_sky_sports
,rank() over (  ORDER BY  minutes_football_scottish_football_non_sky_sports desc) as rank_minutes_football_scottish_football_non_sky_sports

,rank() over (  ORDER BY  minutes_football_Capital_One_Cup_sky_sports desc) as rank_minutes_football_Capital_One_Cup_sky_sports

,rank() over (  ORDER BY  minutes_football_La_Liga_sky_sports desc) as rank_minutes_football_La_Liga_sky_sports

,rank() over (  ORDER BY  minutes_football_football_league_sky_sports desc) as rank_minutes_football_football_league_sky_sports

,rank() over (  ORDER BY  minutes_cricket_ashes_sky_sports desc) as rank_minutes_cricket_ashes_sky_sports
,rank() over (  ORDER BY  minutes_cricket_ashes_non_sky_sports desc) as rank_minutes_cricket_ashes_non_sky_sports

,rank() over (  ORDER BY  minutes_cricket_non_ashes_sky_sports desc) as rank_minutes_cricket_non_ashes_sky_sports
,rank() over (  ORDER BY  minutes_cricket_non_ashes_non_sky_sports desc) as rank_minutes_cricket_non_ashes_non_sky_sports


,rank() over (  ORDER BY  minutes_golf_sky_sports desc) as rank_minutes_golf_sky_sports
,rank() over (  ORDER BY  minutes_golf_non_sky_sports desc) as rank_minutes_golf_non_sky_sports

,rank() over (  ORDER BY  minutes_tennis_sky_sports desc) as rank_minutes_tennis_sky_sports
,rank() over (  ORDER BY  minutes_tennis_non_sky_sports desc) as rank_minutes_tennis_non_sky_sports

,rank() over (  ORDER BY  minutes_motor_sport_sky_sports desc) as rank_minutes_motor_sport_sky_sports
,rank() over (  ORDER BY  minutes_motor_sport_non_sky_sports desc) as rank_minutes_motor_sport_non_sky_sports

,rank() over (  ORDER BY  minutes_F1_sky_sports desc) as rank_minutes_F1_sky_sports
,rank() over (  ORDER BY  minutes_F1_non_sky_sports desc) as rank_minutes_F1_non_sky_sports

,rank() over (  ORDER BY  minutes_horse_racing_sky_sports desc) as rank_minutes_horse_racing_sky_sports
,rank() over (  ORDER BY  minutes_horse_racing_non_sky_sports desc) as rank_minutes_horse_racing_non_sky_sports

,rank() over (  ORDER BY  minutes_snooker_pool_sky_sports desc) as rank_minutes_snooker_pool_sky_sports
,rank() over (  ORDER BY  minutes_snooker_pool_non_sky_sports desc) as rank_minutes_snooker_pool_non_sky_sports

,rank() over (  ORDER BY  minutes_rugby_sky_sports desc) as rank_minutes_rugby_sky_sports
,rank() over (  ORDER BY  minutes_rugby_non_sky_sports desc) as rank_minutes_rugby_non_sky_sports

,rank() over (  ORDER BY  minutes_wrestling_sky_sports desc) as rank_minutes_wrestling_sky_sports
,rank() over (  ORDER BY  minutes_wrestling_non_sky_sports desc) as rank_minutes_wrestling_non_sky_sports

,rank() over (  ORDER BY  minutes_wwe desc) as rank_minutes_wwe
,rank() over (  ORDER BY  minutes_wwe_sky_sports desc) as rank_minutes_wwe_sky_sports
,rank() over (  ORDER BY  minutes_wwe_sbo desc) as rank_minutes_wwe_sbo
,rank() over (  ORDER BY  minutes_wwe_sky_1_or_2 desc) as rank_minutes_wwe_sky_1_or_2

,rank() over (  ORDER BY  minutes_darts_sky_sports desc) as rank_minutes_darts_sky_sports
,rank() over (  ORDER BY  minutes_darts_non_sky_sports desc) as rank_minutes_darts_non_sky_sports

,rank() over (  ORDER BY  minutes_boxing_sky_sports desc) as rank_minutes_boxing_sky_sports
,rank() over (  ORDER BY  minutes_boxing_non_sky_sports desc) as rank_minutes_boxing_non_sky_sports


,rank() over (  ORDER BY  minutes_overall_football desc) as rank_minutes_overall_football
,rank() over (  ORDER BY  minutes_Sky_Sports_cricket_overall desc) as rank_minutes_Sky_Sports_cricket_overall
,rank() over (  ORDER BY  minutes_non_Sky_Sports_cricket_overall desc) as rank_minutes_non_Sky_Sports_cricket_overall
,rank() over (  ORDER BY  minutes_niche_sports_sky_sports desc) as rank_minutes_niche_sports_sky_sports
,rank() over (  ORDER BY  minutes_niche_sports_non_sky_sports desc) as rank_minutes_niche_sports_non_sky_sports


---Repeat Ranks by Number of 3+min progs (then by minutes)
,rank() over (  ORDER BY annualised_programmes_3min_plus_sport  desc, minutes_sport desc) as rank_prog_3min_plus_sport
,rank() over (  ORDER BY annualised_programmes_3min_plus_sport_sky_sports  desc ,  minutes_sport_sky_sports desc) as rank_prog_3min_plus_sport_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_sport_sky_sports_news desc ,  minutes_sport_sky_sports_news desc) as rank_prog_3min_plus_sport_sky_sports_news

,rank() over (  ORDER BY annualised_programmes_3min_plus_sport_espn  desc ,  minutes_sport_espn desc) as rank_prog_3min_plus_sport_espn
,rank() over (  ORDER BY annualised_programmes_3min_plus_sport_terrestrial desc ,  minutes_sport_terrestrial desc) as rank_prog_3min_plus_sport_terrestrial
,rank() over (  ORDER BY annualised_programmes_3min_plus_sport_football_terrestrial  desc ,  minutes_sport_football_terrestrial desc) as rank_prog_3min_plus_sport_football_terrestrial

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_sky_sports desc ,  minutes_football_sky_sports desc) as rank_prog_3min_plus_football_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_premier_league_sky_sports  desc ,  minutes_football_premier_league_sky_sports desc) as rank_prog_3min_plus_football_premier_league_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_premier_league_ESPN_BT  desc ,  minutes_football_premier_league_ESPN_BT desc) as rank_prog_3min_plus_football_premier_league_ESPN_BT

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_champions_league_sky_sports  desc ,  minutes_football_champions_league_sky_sports desc) as rank_prog_3min_plus_football_champions_league_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_champions_league_non_sky_sports desc ,  minutes_football_champions_league_non_sky_sports desc) as rank_prog_3min_plus_football_champions_league_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_europa_league_ESPN_BT desc ,  minutes_football_europa_league_ESPN_BT desc) as rank_prog_3min_plus_football_europa_league_ESPN_BT
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_europa_league_Other_Channels desc ,  minutes_football_europa_league_Other_Channels desc) as rank_prog_3min_plus_football_europa_league_Other_Channels

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_fa_cup_ESPN_BT  desc ,  minutes_football_fa_cup_ESPN_BT desc) as rank_prog_3min_plus_football_fa_cup_ESPN_BT
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_fa_cup_Other_Channels  desc ,  minutes_football_fa_cup_Other_Channels desc) as rank_prog_3min_plus_football_fa_cup_Other_Channels

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_world_cup_qualifier_sky_sports  desc ,  minutes_football_world_cup_qualifier_sky_sports desc) as rank_prog_3min_plus_football_world_cup_qualifier_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_world_cup_qualifier_non_sky_sports  desc ,  minutes_football_world_cup_qualifier_non_sky_sports desc) as rank_prog_3min_plus_football_world_cup_qualifier_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_international_friendly_sky_sports  desc ,  minutes_football_international_friendly_sky_sports desc) as rank_prog_3min_plus_football_international_friendly_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_international_friendly_non_sky_sports  desc ,  minutes_football_international_friendly_non_sky_sports desc) as rank_prog_3min_plus_football_international_friendly_non_sky_sports


,rank() over (  ORDER BY annualised_programmes_3min_plus_football_scottish_football_sky_sports  desc ,  minutes_football_scottish_football_sky_sports desc) as rank_prog_3min_plus_football_scottish_football_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_football_scottish_football_non_sky_sports  desc ,  minutes_football_scottish_football_non_sky_sports desc) as rank_prog_3min_plus_football_scottish_football_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_Capital_One_Cup_sky_sports  desc ,  minutes_football_Capital_One_Cup_sky_sports desc) as rank_prog_3min_plus_football_Capital_One_Cup_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_La_Liga_sky_sports  desc ,  minutes_football_La_Liga_sky_sports desc) as rank_prog_3min_plus_football_La_Liga_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_football_football_league_sky_sports  desc ,  minutes_football_football_league_sky_sports desc) as rank_prog_3min_plus_football_football_league_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_cricket_ashes_sky_sports  desc ,  minutes_cricket_ashes_sky_sports desc) as rank_prog_3min_plus_cricket_ashes_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_cricket_ashes_non_sky_sports  desc ,  minutes_cricket_ashes_non_sky_sports desc) as rank_prog_3min_plus_cricket_ashes_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_cricket_non_ashes_sky_sports  desc ,  minutes_cricket_non_ashes_sky_sports desc) as rank_prog_3min_plus_cricket_non_ashes_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_cricket_non_ashes_non_sky_sports  desc ,  minutes_cricket_non_ashes_non_sky_sports desc) as rank_prog_3min_plus_cricket_non_ashes_non_sky_sports


,rank() over (  ORDER BY annualised_programmes_3min_plus_golf_sky_sports desc ,  minutes_golf_sky_sports desc) as rank_prog_3min_plus_golf_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_golf_sky_sports desc ,  minutes_golf_sky_sports desc) as rank_prog_3min_plus_golf_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_tennis_sky_sports  desc ,  minutes_tennis_sky_sports desc) as rank_prog_3min_plus_tennis_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_tennis_non_sky_sports  desc ,  minutes_tennis_non_sky_sports desc) as rank_prog_3min_plus_tennis_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_motor_sport_sky_sports  desc ,  minutes_motor_sport_sky_sports desc) as rank_prog_3min_plus_motor_sport_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_motor_sport_non_sky_sports  desc ,  minutes_motor_sport_non_sky_sports desc) as rank_prog_3min_plus_motor_sport_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_F1_sky_sports  desc ,  minutes_F1_sky_sports desc) as rank_prog_3min_plus_F1_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_F1_non_sky_sports  desc ,  minutes_F1_non_sky_sports desc) as rank_prog_3min_plus_F1_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_horse_racing_sky_sports  desc ,  minutes_horse_racing_sky_sports desc) as rank_prog_3min_plus_horse_racing_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_horse_racing_non_sky_sports  desc ,  minutes_horse_racing_non_sky_sports desc) as rank_prog_3min_plus_horse_racing_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_snooker_pool_sky_sports  desc ,  minutes_snooker_pool_sky_sports desc) as rank_prog_3min_plus_snooker_pool_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_snooker_pool_non_sky_sports  desc ,  minutes_snooker_pool_non_sky_sports desc) as rank_prog_3min_plus_snooker_pool_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_rugby_sky_sports  desc ,  minutes_rugby_sky_sports desc) as rank_prog_3min_plus_rugby_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_rugby_non_sky_sports  desc ,  minutes_rugby_non_sky_sports desc) as rank_prog_3min_plus_rugby_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_wrestling_sky_sports  desc ,  minutes_wrestling_sky_sports desc) as rank_prog_3min_plus_wrestling_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_wrestling_non_sky_sports   desc ,  minutes_wrestling_non_sky_sports desc) as rank_prog_3min_plus_wrestling_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_wwe  desc ,  minutes_wwe desc) as rank_prog_3min_plus_wwe
,rank() over (  ORDER BY annualised_programmes_3min_plus_wwe_sky_sports  desc ,  minutes_wwe_sky_sports desc) as rank_prog_3min_plus_wwe_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_wwe_sbo  desc ,  minutes_wwe_sbo desc) as rank_prog_3min_plus_wwe_sbo
,rank() over (  ORDER BY annualised_programmes_3min_plus_wwe_sky_1_or_2  desc ,  minutes_wwe_sky_1_or_2 desc) as rank_prog_3min_plus_wwe_sky_1_or_2

,rank() over (  ORDER BY annualised_programmes_3min_plus_darts_sky_sports  desc ,  minutes_darts_sky_sports desc) as rank_prog_3min_plus_darts_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_darts_non_sky_sports  desc ,  minutes_darts_non_sky_sports desc) as rank_prog_3min_plus_darts_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_boxing_sky_sports  desc ,  minutes_boxing_sky_sports desc) as rank_prog_3min_plus_boxing_sky_sports
,rank() over (  ORDER BY annualised_programmes_3min_plus_boxing_non_sky_sports  desc ,  minutes_boxing_non_sky_sports desc) as rank_prog_3min_plus_boxing_non_sky_sports


,rank() over (  ORDER BY annualised_programmes_3min_plus_overall_football   desc ,  minutes_overall_football desc) as rank_prog_3min_plus_overall_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_Sky_Sports_cricket_overall   desc ,  minutes_Sky_Sports_cricket_overall  desc) as rank_prog_3min_plus_Sky_Sports_cricket_overall 
,rank() over (  ORDER BY annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall   desc ,  minutes_non_Sky_Sports_cricket_overall  desc) as rank_prog_3min_plus_non_Sky_Sports_cricket_overall 
,rank() over (  ORDER BY annualised_programmes_3min_plus_niche_sports_sky_sports  desc ,  minutes_niche_sports_sky_sports  desc) as rank_prog_3min_plus_niche_sports_sky_sports 
,rank() over (  ORDER BY annualised_programmes_3min_plus_niche_sports_non_sky_sports  desc ,  minutes_niche_sports_non_sky_sports desc) as rank_prog_3min_plus_niche_sports_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_3min_plus_sky_sports_football   desc ,  minutes_sky_sports_football desc) as rank_prog_3min_plus_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_non_sky_sports_football   desc ,  minutes_non_sky_sports_football desc) as rank_prog_3min_plus_non_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_sky_sports_exc_wwe   desc ,  minutes_sky_sports_exc_wwe desc) as rank_prog_3min_plus_sky_sports_exc_wwe
into #rank_minutes_details
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;


exec sp_create_tmp_table_idx '#rank_minutes_details', 'account_number';
commit;

update v223_unbundling_viewing_summary_by_account
set 
rank_minutes_sport  =b.rank_minutes_sport 
,rank_minutes_sport_sky_sports  =b.rank_minutes_sport_sky_sports 
,rank_minutes_sport_sky_sports_news  =b.rank_minutes_sport_sky_sports_news 

,rank_minutes_sport_espn  =b.rank_minutes_sport_espn
,rank_minutes_sport_terrestrial  =b.rank_minutes_sport_terrestrial
,rank_minutes_sport_football_terrestrial  =b.rank_minutes_sport_football_terrestrial

,rank_minutes_football_sky_sports  =b.rank_minutes_football_sky_sports 

,rank_minutes_football_premier_league_sky_sports  =b.rank_minutes_football_premier_league_sky_sports 
,rank_minutes_football_premier_league_ESPN_BT  =b.rank_minutes_football_premier_league_ESPN_BT 

,rank_minutes_football_champions_league_sky_sports  =b.rank_minutes_football_champions_league_sky_sports 
,rank_minutes_football_champions_league_non_sky_sports  =b.rank_minutes_football_champions_league_non_sky_sports 

,rank_minutes_football_fa_cup_ESPN_BT  =b.rank_minutes_football_fa_cup_ESPN_BT 
,rank_minutes_football_fa_cup_Other_Channels  =b.rank_minutes_football_fa_cup_Other_Channels 

,rank_minutes_football_europa_league_ESPN_BT  =b.rank_minutes_football_europa_league_ESPN_BT 


,rank_minutes_football_europa_league_Other_Channels  =b.rank_minutes_football_europa_league_Other_Channels 


,rank_minutes_football_world_cup_qualifier_sky_sports  =b.rank_minutes_football_world_cup_qualifier_sky_sports 
,rank_minutes_football_world_cup_qualifier_non_sky_sports  =b.rank_minutes_football_world_cup_qualifier_non_sky_sports 

,rank_minutes_football_international_friendly_sky_sports  =b.rank_minutes_football_international_friendly_sky_sports 
,rank_minutes_football_international_friendly_non_sky_sports  =b.rank_minutes_football_international_friendly_non_sky_sports 

,rank_minutes_football_Capital_One_Cup_sky_sports  =b.rank_minutes_football_Capital_One_Cup_sky_sports 

,rank_minutes_football_La_Liga_sky_sports  =b.rank_minutes_football_La_Liga_sky_sports 

,rank_minutes_football_football_league_sky_sports  =b.rank_minutes_football_football_league_sky_sports 

,rank_minutes_cricket_ashes_sky_sports  =b.rank_minutes_cricket_ashes_sky_sports 
,rank_minutes_cricket_ashes_non_sky_sports  =b.rank_minutes_cricket_ashes_non_sky_sports 

,rank_minutes_cricket_non_ashes_sky_sports  =b.rank_minutes_cricket_non_ashes_sky_sports 
,rank_minutes_cricket_non_ashes_non_sky_sports  =b.rank_minutes_cricket_non_ashes_non_sky_sports 

,rank_minutes_golf_sky_sports  =b.rank_minutes_golf_sky_sports 
,rank_minutes_golf_non_sky_sports  =b.rank_minutes_golf_non_sky_sports 

,rank_minutes_tennis_sky_sports  =b.rank_minutes_tennis_sky_sports 
,rank_minutes_tennis_non_sky_sports  =b.rank_minutes_tennis_non_sky_sports 

,rank_minutes_motor_sport_sky_sports  =b.rank_minutes_motor_sport_sky_sports 
,rank_minutes_motor_sport_non_sky_sports  =b.rank_minutes_motor_sport_non_sky_sports 

,rank_minutes_F1_sky_sports  =b.rank_minutes_F1_sky_sports 
,rank_minutes_F1_non_sky_sports  =b.rank_minutes_F1_non_sky_sports 

,rank_minutes_horse_racing_sky_sports  =b.rank_minutes_horse_racing_sky_sports 
,rank_minutes_horse_racing_non_sky_sports  =b.rank_minutes_horse_racing_non_sky_sports 

,rank_minutes_snooker_pool_sky_sports  =b.rank_minutes_snooker_pool_sky_sports 
,rank_minutes_snooker_pool_non_sky_sports  =b.rank_minutes_snooker_pool_non_sky_sports 

,rank_minutes_rugby_sky_sports  =b.rank_minutes_rugby_sky_sports 
,rank_minutes_rugby_non_sky_sports  =b.rank_minutes_rugby_non_sky_sports 

,rank_minutes_wrestling_sky_sports  =b.rank_minutes_wrestling_sky_sports 
,rank_minutes_wrestling_non_sky_sports  =b.rank_minutes_wrestling_non_sky_sports 

,rank_minutes_wwe  =b.rank_minutes_wwe 
,rank_minutes_wwe_sky_sports  =b.rank_minutes_wwe_sky_sports 
,rank_minutes_wwe_sbo  =b.rank_minutes_wwe_sbo 
,rank_minutes_wwe_sky_1_or_2  =b.rank_minutes_wwe_sky_1_or_2

,rank_minutes_darts_sky_sports  =b.rank_minutes_darts_sky_sports 
,rank_minutes_darts_non_sky_sports  =b.rank_minutes_darts_non_sky_sports 

,rank_minutes_boxing_sky_sports  =b.rank_minutes_boxing_sky_sports 
,rank_minutes_boxing_non_sky_sports  =b.rank_minutes_boxing_non_sky_sports 

,rank_minutes_sky_sports_football=b.rank_minutes_sky_sports_football
,rank_minutes_non_sky_sports_football=b.rank_minutes_non_sky_sports_football
,rank_minutes_sky_sports_exc_wwe=b.rank_minutes_sky_sports_exc_wwe
---Update 3min ranks
,rank_prog_3min_plus_sport  =b.rank_prog_3min_plus_sport 
,rank_prog_3min_plus_sport_sky_sports  =b.rank_prog_3min_plus_sport_sky_sports 
,rank_prog_3min_plus_sport_sky_sports_news  =b.rank_prog_3min_plus_sport_sky_sports_news 

,rank_prog_3min_plus_sport_espn  =b.rank_prog_3min_plus_sport_espn
,rank_prog_3min_plus_sport_terrestrial  =b.rank_prog_3min_plus_sport_terrestrial
,rank_prog_3min_plus_sport_football_terrestrial  =b.rank_prog_3min_plus_sport_football_terrestrial

,rank_prog_3min_plus_football_sky_sports  =b.rank_prog_3min_plus_football_sky_sports 
--,rank_prog_3min_plus_football_ESPN_BT  =b.rank_prog_3min_plus_football_ESPN_BT 

,rank_prog_3min_plus_football_premier_league_sky_sports  =b.rank_prog_3min_plus_football_premier_league_sky_sports 
,rank_prog_3min_plus_football_premier_league_ESPN_BT  =b.rank_prog_3min_plus_football_premier_league_ESPN_BT 

,rank_prog_3min_plus_football_champions_league_sky_sports  =b.rank_prog_3min_plus_football_champions_league_sky_sports 
,rank_prog_3min_plus_football_champions_league_non_sky_sports  =b.rank_prog_3min_plus_football_champions_league_non_sky_sports 

,rank_prog_3min_plus_football_fa_cup_ESPN_BT  =b.rank_prog_3min_plus_football_fa_cup_ESPN_BT 
,rank_prog_3min_plus_football_fa_cup_Other_Channels  =b.rank_prog_3min_plus_football_fa_cup_Other_Channels 

,rank_prog_3min_plus_football_europa_league_ESPN_BT  =b.rank_prog_3min_plus_football_europa_league_ESPN_BT
,rank_prog_3min_plus_football_europa_league_Other_Channels  =b.rank_prog_3min_plus_football_europa_league_Other_Channels 


,rank_prog_3min_plus_football_world_cup_qualifier_sky_sports  =b.rank_prog_3min_plus_football_world_cup_qualifier_sky_sports 
,rank_prog_3min_plus_football_world_cup_qualifier_non_sky_sports  =b.rank_prog_3min_plus_football_world_cup_qualifier_non_sky_sports 

,rank_prog_3min_plus_football_international_friendly_sky_sports  =b.rank_prog_3min_plus_football_international_friendly_sky_sports 
,rank_prog_3min_plus_football_international_friendly_non_sky_sports  =b.rank_prog_3min_plus_football_international_friendly_non_sky_sports 

,rank_prog_3min_plus_football_Capital_One_Cup_sky_sports  =b.rank_prog_3min_plus_football_Capital_One_Cup_sky_sports 

,rank_prog_3min_plus_football_La_Liga_sky_sports  =b.rank_prog_3min_plus_football_La_Liga_sky_sports 

,rank_prog_3min_plus_football_football_league_sky_sports  =b.rank_prog_3min_plus_football_football_league_sky_sports 

,rank_prog_3min_plus_cricket_ashes_sky_sports  =b.rank_prog_3min_plus_cricket_ashes_sky_sports 
,rank_prog_3min_plus_cricket_ashes_non_sky_sports  =b.rank_prog_3min_plus_cricket_ashes_non_sky_sports 

,rank_prog_3min_plus_cricket_non_ashes_sky_sports  =b.rank_prog_3min_plus_cricket_non_ashes_sky_sports 
,rank_prog_3min_plus_cricket_non_ashes_non_sky_sports  =b.rank_prog_3min_plus_cricket_non_ashes_non_sky_sports 

,rank_prog_3min_plus_golf_sky_sports  =b.rank_prog_3min_plus_golf_sky_sports 
,rank_prog_3min_plus_golf_non_sky_sports  =b.rank_prog_3min_plus_golf_non_sky_sports 

,rank_prog_3min_plus_tennis_sky_sports  =b.rank_prog_3min_plus_tennis_sky_sports 
,rank_prog_3min_plus_tennis_non_sky_sports  =b.rank_prog_3min_plus_tennis_non_sky_sports 

,rank_prog_3min_plus_motor_sport_sky_sports  =b.rank_prog_3min_plus_motor_sport_sky_sports 
,rank_prog_3min_plus_motor_sport_non_sky_sports  =b.rank_prog_3min_plus_motor_sport_non_sky_sports 

,rank_prog_3min_plus_F1_sky_sports  =b.rank_prog_3min_plus_F1_sky_sports 
,rank_prog_3min_plus_F1_non_sky_sports  =b.rank_prog_3min_plus_F1_non_sky_sports 

,rank_prog_3min_plus_horse_racing_sky_sports  =b.rank_prog_3min_plus_horse_racing_sky_sports 
,rank_prog_3min_plus_horse_racing_non_sky_sports  =b.rank_prog_3min_plus_horse_racing_non_sky_sports 

,rank_prog_3min_plus_snooker_pool_sky_sports  =b.rank_prog_3min_plus_snooker_pool_sky_sports 
,rank_prog_3min_plus_snooker_pool_non_sky_sports  =b.rank_prog_3min_plus_snooker_pool_non_sky_sports 

,rank_prog_3min_plus_rugby_sky_sports  =b.rank_prog_3min_plus_rugby_sky_sports 
,rank_prog_3min_plus_rugby_non_sky_sports  =b.rank_prog_3min_plus_rugby_non_sky_sports 

,rank_prog_3min_plus_wrestling_sky_sports  =b.rank_prog_3min_plus_wrestling_sky_sports 
,rank_prog_3min_plus_wrestling_non_sky_sports  =b.rank_prog_3min_plus_wrestling_non_sky_sports 

,rank_prog_3min_plus_wwe  =b.rank_prog_3min_plus_wwe 
,rank_prog_3min_plus_wwe_sky_sports  =b.rank_prog_3min_plus_wwe_sky_sports 
,rank_prog_3min_plus_wwe_sbo  =b.rank_prog_3min_plus_wwe_sbo 
,rank_prog_3min_plus_wwe_sky_1_or_2  =b.rank_prog_3min_plus_wwe_sky_1_or_2

,rank_prog_3min_plus_darts_sky_sports  =b.rank_prog_3min_plus_darts_sky_sports 
,rank_prog_3min_plus_darts_non_sky_sports  =b.rank_prog_3min_plus_darts_non_sky_sports 

,rank_prog_3min_plus_boxing_sky_sports  =b.rank_prog_3min_plus_boxing_sky_sports 
,rank_prog_3min_plus_boxing_non_sky_sports  =b.rank_prog_3min_plus_boxing_non_sky_sports 

,rank_prog_3min_plus_sky_sports_football=b.rank_prog_3min_plus_sky_sports_football
,rank_prog_3min_plus_non_sky_sports_football=b.rank_prog_3min_plus_non_sky_sports_football
,rank_prog_3min_plus_sky_sports_exc_wwe=b.rank_prog_3min_plus_sky_sports_exc_wwe

---V2
,rank_minutes_overall_football=b.rank_minutes_overall_football
,rank_minutes_Sky_Sports_cricket_overall=b.rank_minutes_Sky_Sports_cricket_overall
,rank_minutes_non_Sky_Sports_cricket_overall=b.rank_minutes_non_Sky_Sports_cricket_overall
,rank_minutes_niche_sports_sky_sports=b.rank_minutes_niche_sports_sky_sports
,rank_minutes_niche_sports_non_sky_sports=b.rank_minutes_niche_sports_non_sky_sports


,rank_prog_3min_plus_overall_football=b.rank_prog_3min_plus_overall_football
,rank_prog_3min_plus_Sky_Sports_cricket_overall=b.rank_prog_3min_plus_Sky_Sports_cricket_overall
,rank_prog_3min_plus_non_Sky_Sports_cricket_overall=b.rank_prog_3min_plus_non_Sky_Sports_cricket_overall 
,rank_prog_3min_plus_niche_sports_sky_sports =b.rank_prog_3min_plus_niche_sports_sky_sports
,rank_prog_3min_plus_niche_sports_non_sky_sports=b.ank_prog_3min_plus_niche_sports_non_sky_sports


from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details as b
on a.account_number = b.account_number
where days_with_viewing>=280
;
commit;

---Update second set of ranks---
select account_number

---Repeat Ranks by Number of engaged programmes (then by minutes)
,rank() over (  ORDER BY annualised_programmes_engaged_sport  desc ,  minutes_sport desc) as rank_prog_engaged_sport
,rank() over (  ORDER BY annualised_programmes_engaged_sport_sky_sports  desc ,  minutes_sport_sky_sports desc) as rank_prog_engaged_sport_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_sport_sky_sports_news  desc ,  minutes_sport_sky_sports_news desc) as rank_prog_engaged_sport_sky_sports_news

,rank() over (  ORDER BY annualised_programmes_engaged_sport_espn  desc ,  minutes_sport_espn desc) as rank_prog_engaged_sport_espn
,rank() over (  ORDER BY annualised_programmes_engaged_sport_terrestrial  desc ,  minutes_sport_terrestrial desc) as rank_prog_engaged_sport_terrestrial
,rank() over (  ORDER BY annualised_programmes_engaged_sport_football_terrestrial  desc ,  minutes_sport_football_terrestrial desc) as rank_prog_engaged_sport_football_terrestrial

,rank() over (  ORDER BY annualised_programmes_engaged_football_sky_sports  desc ,  minutes_football_sky_sports desc) as rank_prog_engaged_football_sky_sports
--,rank() over (  ORDER BY   desc ,  minutes_football_ESPN_BT desc) as rank_prog_engaged_football_ESPN_BT

,rank() over (  ORDER BY annualised_programmes_engaged_football_premier_league_sky_sports  desc ,  minutes_football_premier_league_sky_sports desc) as rank_prog_engaged_football_premier_league_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_football_premier_league_ESPN_BT  desc ,  minutes_football_premier_league_ESPN_BT desc) as rank_prog_engaged_football_premier_league_ESPN_BT

,rank() over (  ORDER BY annualised_programmes_engaged_football_champions_league_sky_sports  desc ,  minutes_football_champions_league_sky_sports desc) as rank_prog_engaged_football_champions_league_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_football_champions_league_non_sky_sports  desc ,  minutes_football_champions_league_non_sky_sports desc) as rank_prog_engaged_football_champions_league_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_football_europa_league_ESPN_BT  desc ,  minutes_football_europa_league_ESPN_BT desc) as rank_prog_engaged_football_europa_league_ESPN_BT
,rank() over (  ORDER BY annualised_programmes_engaged_football_europa_league_Other_Channels  desc ,  minutes_football_europa_league_Other_Channels desc) as rank_prog_engaged_football_europa_league_Other_Channels

,rank() over (  ORDER BY annualised_programmes_engaged_football_fa_cup_ESPN_BT  desc ,  minutes_football_fa_cup_ESPN_BT desc) as rank_prog_engaged_football_fa_cup_ESPN_BT
,rank() over (  ORDER BY annualised_programmes_engaged_football_fa_cup_Other_Channels  desc ,  minutes_football_fa_cup_Other_Channels desc) as rank_prog_engaged_football_fa_cup_Other_Channels

,rank() over (  ORDER BY annualised_programmes_engaged_football_world_cup_qualifier_sky_sports  desc ,  minutes_football_world_cup_qualifier_sky_sports desc) as rank_prog_engaged_football_world_cup_qualifier_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_football_world_cup_qualifier_non_sky_sports desc ,  minutes_football_world_cup_qualifier_non_sky_sports desc) as rank_prog_engaged_football_world_cup_qualifier_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_football_international_friendly_sky_sports  desc ,  minutes_football_international_friendly_sky_sports desc) as rank_prog_engaged_football_international_friendly_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_football_international_friendly_non_sky_sports  desc ,  minutes_football_international_friendly_non_sky_sports desc) as rank_prog_engaged_football_international_friendly_non_sky_sports


,rank() over (  ORDER BY annualised_programmes_engaged_football_scottish_football_sky_sports  desc ,  minutes_football_scottish_football_sky_sports desc) as rank_prog_engaged_football_scottish_football_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_football_scottish_football_non_sky_sports  desc ,  minutes_football_scottish_football_non_sky_sports desc) as rank_prog_engaged_football_scottish_football_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_football_Capital_One_Cup_sky_sports  desc ,  minutes_football_Capital_One_Cup_sky_sports desc) as rank_prog_engaged_football_Capital_One_Cup_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_football_La_Liga_sky_sports  desc ,  minutes_football_La_Liga_sky_sports desc) as rank_prog_engaged_football_La_Liga_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_football_football_league_sky_sports  desc ,  minutes_football_football_league_sky_sports desc) as rank_prog_engaged_football_football_league_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_cricket_ashes_sky_sports  desc ,  minutes_cricket_ashes_sky_sports desc) as rank_prog_engaged_cricket_ashes_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_cricket_ashes_non_sky_sports  desc ,  minutes_cricket_ashes_non_sky_sports desc) as rank_prog_engaged_cricket_ashes_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_cricket_non_ashes_sky_sports  desc ,  minutes_cricket_non_ashes_sky_sports desc) as rank_prog_engaged_cricket_non_ashes_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_cricket_non_ashes_non_sky_sports  desc ,  minutes_cricket_non_ashes_non_sky_sports desc) as rank_prog_engaged_cricket_non_ashes_non_sky_sports


,rank() over (  ORDER BY annualised_programmes_engaged_golf_sky_sports  desc ,  minutes_golf_sky_sports desc) as rank_prog_engaged_golf_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_golf_non_sky_sports  desc ,  minutes_golf_non_sky_sports desc) as rank_prog_engaged_golf_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_tennis_sky_sports  desc ,  minutes_tennis_sky_sports desc) as rank_prog_engaged_tennis_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_tennis_non_sky_sports  desc ,  minutes_tennis_non_sky_sports desc) as rank_prog_engaged_tennis_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_motor_sport_sky_sports  desc ,  minutes_motor_sport_sky_sports desc) as rank_prog_engaged_motor_sport_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_motor_sport_non_sky_sports  desc ,  minutes_motor_sport_non_sky_sports desc) as rank_prog_engaged_motor_sport_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_F1_sky_sports  desc ,  minutes_F1_sky_sports desc) as rank_prog_engaged_F1_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_F1_non_sky_sports  desc ,  minutes_F1_non_sky_sports desc) as rank_prog_engaged_F1_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_horse_racing_sky_sports  desc ,  minutes_horse_racing_sky_sports desc) as rank_prog_engaged_horse_racing_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_horse_racing_non_sky_sports  desc ,  minutes_horse_racing_non_sky_sports desc) as rank_prog_engaged_horse_racing_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_snooker_pool_sky_sports  desc ,  minutes_snooker_pool_sky_sports desc) as rank_prog_engaged_snooker_pool_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_snooker_pool_non_sky_sports  desc ,  minutes_snooker_pool_non_sky_sports desc) as rank_prog_engaged_snooker_pool_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_rugby_sky_sports  desc ,  minutes_rugby_sky_sports desc) as rank_prog_engaged_rugby_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_rugby_non_sky_sports  desc ,  minutes_rugby_non_sky_sports desc) as rank_prog_engaged_rugby_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_wrestling_sky_sports  desc ,  minutes_wrestling_sky_sports desc) as rank_prog_engaged_wrestling_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_wrestling_non_sky_sports  desc ,  minutes_wrestling_non_sky_sports desc) as rank_prog_engaged_wrestling_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_wwe  desc ,  minutes_wwe desc) as rank_prog_engaged_wwe
,rank() over (  ORDER BY annualised_programmes_engaged_wwe_sky_sports  desc ,  minutes_wwe_sky_sports desc) as rank_prog_engaged_wwe_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_wwe_sbo  desc ,  minutes_wwe_sbo desc) as rank_prog_engaged_wwe_sbo
,rank() over (  ORDER BY annualised_programmes_engaged_wwe_sky_1_or_2  desc ,  minutes_wwe_sky_1_or_2 desc) as rank_prog_engaged_wwe_sky_1_or_2

,rank() over (  ORDER BY annualised_programmes_engaged_darts_sky_sports  desc ,  minutes_darts_sky_sports desc) as rank_prog_engaged_darts_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_darts_non_sky_sports  desc ,  minutes_darts_non_sky_sports desc) as rank_prog_engaged_darts_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_boxing_sky_sports  desc ,  minutes_boxing_sky_sports desc) as rank_prog_engaged_boxing_sky_sports
,rank() over (  ORDER BY annualised_programmes_engaged_boxing_non_sky_sports  desc ,  minutes_boxing_non_sky_sports desc) as rank_prog_engaged_boxing_non_sky_sports


--V2--
,rank() over (  ORDER BY annualised_programmes_engaged_overall_football   desc ,  minutes_overall_football desc) as rank_prog_engaged_overall_football
,rank() over (  ORDER BY annualised_programmes_engaged_Sky_Sports_cricket_overall   desc ,  minutes_Sky_Sports_cricket_overall  desc) as rank_prog_engaged_Sky_Sports_cricket_overall 
,rank() over (  ORDER BY annualised_programmes_engaged_non_Sky_Sports_cricket_overall   desc ,  minutes_non_Sky_Sports_cricket_overall  desc) as rank_prog_engaged_non_Sky_Sports_cricket_overall 
,rank() over (  ORDER BY annualised_programmes_engaged_niche_sports_sky_sports  desc ,  minutes_niche_sports_sky_sports  desc) as rank_prog_engaged_niche_sports_sky_sports 
,rank() over (  ORDER BY annualised_programmes_engaged_niche_sports_non_sky_sports  desc ,  minutes_niche_sports_non_sky_sports desc) as rank_prog_engaged_niche_sports_non_sky_sports

,rank() over (  ORDER BY annualised_programmes_engaged_sky_sports_football  desc ,  minutes_sky_sports_football desc) as rank_prog_engaged_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_engaged_non_sky_sports_football  desc ,  minutes_non_sky_sports_football desc) as rank_prog_engaged_non_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_engaged_sky_sports_exc_wwe  desc ,  minutes_sky_sports_exc_wwe desc) as rank_prog_engaged_sky_sports_exc_wwe


into #rank_minutes_details_engaged
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;


exec sp_create_tmp_table_idx '#rank_minutes_details_engaged', 'account_number';
commit;

update v223_unbundling_viewing_summary_by_account

---Update Engaged progs
 set rank_prog_engaged_sport  =b.rank_prog_engaged_sport 
,rank_prog_engaged_sport_sky_sports  =b.rank_prog_engaged_sport_sky_sports 
,rank_prog_engaged_sport_sky_sports_news  =b.rank_prog_engaged_sport_sky_sports_news 

,rank_prog_engaged_sport_espn  =b.rank_prog_engaged_sport_espn
,rank_prog_engaged_sport_terrestrial  =b.rank_prog_engaged_sport_terrestrial
,rank_prog_engaged_sport_football_terrestrial  =b.rank_prog_engaged_sport_football_terrestrial

,rank_prog_engaged_football_sky_sports  =b.rank_prog_engaged_football_sky_sports 
--,rank_prog_engaged_football_ESPN_BT  =b.rank_prog_engaged_football_ESPN_BT 

,rank_prog_engaged_football_premier_league_sky_sports  =b.rank_prog_engaged_football_premier_league_sky_sports 
,rank_prog_engaged_football_premier_league_ESPN_BT  =b.rank_prog_engaged_football_premier_league_ESPN_BT 

,rank_prog_engaged_football_champions_league_sky_sports  =b.rank_prog_engaged_football_champions_league_sky_sports 
,rank_prog_engaged_football_champions_league_non_sky_sports  =b.rank_prog_engaged_football_champions_league_non_sky_sports 

,rank_prog_engaged_football_fa_cup_ESPN_BT  =b.rank_prog_engaged_football_fa_cup_ESPN_BT 
,rank_prog_engaged_football_fa_cup_Other_Channels  =b.rank_prog_engaged_football_fa_cup_Other_Channels 

,rank_prog_engaged_football_europa_league_ESPN_BT  =b.rank_prog_engaged_football_europa_league_ESPN_BT 
,rank_prog_engaged_football_europa_league_Other_Channels  =b.rank_prog_engaged_football_europa_league_Other_Channels 


,rank_prog_engaged_football_world_cup_qualifier_sky_sports  =b.rank_prog_engaged_football_world_cup_qualifier_sky_sports 
,rank_prog_engaged_football_world_cup_qualifier_non_sky_sports  =b.rank_prog_engaged_football_world_cup_qualifier_non_sky_sports 

,rank_prog_engaged_football_international_friendly_sky_sports  =b.rank_prog_engaged_football_international_friendly_sky_sports 
,rank_prog_engaged_football_international_friendly_non_sky_sports  =b.rank_prog_engaged_football_international_friendly_non_sky_sports 

,rank_prog_engaged_football_Capital_One_Cup_sky_sports  =b.rank_prog_engaged_football_Capital_One_Cup_sky_sports 

,rank_prog_engaged_football_La_Liga_sky_sports  =b.rank_prog_engaged_football_La_Liga_sky_sports 

,rank_prog_engaged_football_football_league_sky_sports  =b.rank_prog_engaged_football_football_league_sky_sports 

,rank_prog_engaged_cricket_ashes_sky_sports  =b.rank_prog_engaged_cricket_ashes_sky_sports 
,rank_prog_engaged_cricket_ashes_non_sky_sports  =b.rank_prog_engaged_cricket_ashes_non_sky_sports 

,rank_prog_engaged_cricket_non_ashes_sky_sports  =b.rank_prog_engaged_cricket_non_ashes_sky_sports 
,rank_prog_engaged_cricket_non_ashes_non_sky_sports  =b.rank_prog_engaged_cricket_non_ashes_non_sky_sports 

,rank_prog_engaged_golf_sky_sports  =b.rank_prog_engaged_golf_sky_sports 
,rank_prog_engaged_golf_non_sky_sports  =b.rank_prog_engaged_golf_non_sky_sports 

,rank_prog_engaged_tennis_sky_sports  =b.rank_prog_engaged_tennis_sky_sports 
,rank_prog_engaged_tennis_non_sky_sports  =b.rank_prog_engaged_tennis_non_sky_sports 

,rank_prog_engaged_motor_sport_sky_sports  =b.rank_prog_engaged_motor_sport_sky_sports 
,rank_prog_engaged_motor_sport_non_sky_sports  =b.rank_prog_engaged_motor_sport_non_sky_sports 

,rank_prog_engaged_F1_sky_sports  =b.rank_prog_engaged_F1_sky_sports 
,rank_prog_engaged_F1_non_sky_sports  =b.rank_prog_engaged_F1_non_sky_sports 

,rank_prog_engaged_horse_racing_sky_sports  =b.rank_prog_engaged_horse_racing_sky_sports 
,rank_prog_engaged_horse_racing_non_sky_sports  =b.rank_prog_engaged_horse_racing_non_sky_sports 

,rank_prog_engaged_snooker_pool_sky_sports  =b.rank_prog_engaged_snooker_pool_sky_sports 
,rank_prog_engaged_snooker_pool_non_sky_sports  =b.rank_prog_engaged_snooker_pool_non_sky_sports 

,rank_prog_engaged_rugby_sky_sports  =b.rank_prog_engaged_rugby_sky_sports 
,rank_prog_engaged_rugby_non_sky_sports  =b.rank_prog_engaged_rugby_non_sky_sports 

,rank_prog_engaged_wrestling_sky_sports  =b.rank_prog_engaged_wrestling_sky_sports 
,rank_prog_engaged_wrestling_non_sky_sports  =b.rank_prog_engaged_wrestling_non_sky_sports 

,rank_prog_engaged_wwe  =b.rank_prog_engaged_wwe 
,rank_prog_engaged_wwe_sky_sports  =b.rank_prog_engaged_wwe_sky_sports 
,rank_prog_engaged_wwe_sbo  =b.rank_prog_engaged_wwe_sbo 
,rank_prog_engaged_wwe_sky_1_or_2  =b.rank_prog_engaged_wwe_sky_1_or_2

,rank_prog_engaged_darts_sky_sports  =b.rank_prog_engaged_darts_sky_sports 
,rank_prog_engaged_darts_non_sky_sports  =b.rank_prog_engaged_darts_non_sky_sports 

,rank_prog_engaged_boxing_sky_sports  =b.rank_prog_engaged_boxing_sky_sports 
,rank_prog_engaged_boxing_non_sky_sports  =b.rank_prog_engaged_boxing_non_sky_sports 

--V2--

,rank_prog_engaged_overall_football=b.rank_prog_engaged_overall_football
,rank_prog_engaged_Sky_Sports_cricket_overall =b.rank_prog_engaged_Sky_Sports_cricket_overall
,rank_prog_engaged_non_Sky_Sports_cricket_overall=b.rank_prog_engaged_non_Sky_Sports_cricket_overall 
,rank_prog_engaged_niche_sports_sky_sports =b.rank_prog_engaged_niche_sports_sky_sports
,rank_prog_engaged_niche_sports_non_sky_sports=b.rank_prog_engaged_niche_sports_non_sky_sports


,rank_prog_engaged_sky_sports_football=b.rank_prog_engaged_sky_sports_football
,rank_prog_engaged_non_sky_sports_football=b.rank_prog_engaged_non_sky_sports_football
,rank_prog_engaged_sky_sports_exc_wwe=b.rank_prog_engaged_sky_sports_exc_wwe

from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details_engaged as b
on a.account_number = b.account_number
where days_with_viewing>=280
;
commit;






--select top 500 * from  v223_unbundling_viewing_summary_by_account where days_with_viewing>=280 
--select rank_minutes_football_capital_one_cup_sky_sports , count(*) as records from  v223_unbundling_viewing_summary_by_account where days_with_viewing>=280 group by rank_minutes_football_capital_one_cup_sky_sports order by records desc

--Get Rank Value where duration is 0 (none watched) and -1 (Insufficient data)

---Create Percentiles---

alter table v223_unbundling_viewing_summary_by_account add
(percentile_minutes_sport integer
,percentile_minutes_sport_sky_sports integer
,percentile_minutes_sport_sky_sports_news integer

,percentile_minutes_sport_terrestrial integer
,percentile_minutes_sport_football_terrestrial integer

,percentile_minutes_sport_espn integer
,percentile_minutes_football_sky_sports integer

,percentile_minutes_football_premier_league_sky_sports integer
,percentile_minutes_football_premier_league_ESPN_BT integer

,percentile_minutes_football_champions_league_sky_sports integer
,percentile_minutes_football_champions_league_non_sky_sports integer

,percentile_minutes_football_fa_cup_sky_sports integer
,percentile_minutes_football_fa_cup_ESPN_BT integer
,percentile_minutes_football_fa_cup_Other_Channels integer

,percentile_minutes_football_europa_league_ESPN_BT integer
,percentile_minutes_football_europa_league_Other_Channels integer

,percentile_minutes_football_world_cup_qualifier_sky_sports integer
,percentile_minutes_football_world_cup_qualifier_non_sky_sports integer

,percentile_minutes_football_international_friendly_sky_sports integer
,percentile_minutes_football_international_friendly_non_sky_sports integer

,percentile_minutes_football_Capital_One_Cup_sky_sports integer

,percentile_minutes_football_La_Liga_sky_sports integer

,percentile_minutes_football_football_league_sky_sports integer

,percentile_minutes_cricket_ashes_sky_sports integer
,percentile_minutes_cricket_ashes_non_sky_sports integer

,percentile_minutes_cricket_non_ashes_sky_sports integer
,percentile_minutes_cricket_non_ashes_non_sky_sports integer



,percentile_minutes_golf_sky_sports integer
,percentile_minutes_golf_non_sky_sports integer

,percentile_minutes_tennis_sky_sports integer
,percentile_minutes_tennis_non_sky_sports integer

,percentile_minutes_motor_sport_sky_sports integer
,percentile_minutes_motor_sport_non_sky_sports integer

,percentile_minutes_F1_sky_sports integer
,percentile_minutes_F1_non_sky_sports integer

,percentile_minutes_horse_racing_sky_sports integer
,percentile_minutes_horse_racing_non_sky_sports integer

,percentile_minutes_snooker_pool_sky_sports integer
,percentile_minutes_snooker_pool_non_sky_sports integer

,percentile_minutes_rugby_sky_sports integer
,percentile_minutes_rugby_non_sky_sports integer

,percentile_minutes_wrestling_sky_sports integer
,percentile_minutes_wrestling_non_sky_sports integer


,percentile_minutes_wwe  integer
,percentile_minutes_wwe_sky_sports  integer
,percentile_minutes_wwe_sbo integer
,percentile_minutes_sky_1_or_2 integer

,percentile_minutes_darts_sky_sports integer
,percentile_minutes_darts_non_sky_sports integer

,percentile_minutes_boxing_sky_sports integer
,percentile_minutes_boxing_non_sky_sports integer


---V2
,percentile_minutes_overall_football integer
,percentile_minutes_Sky_Sports_cricket_overall integer
,percentile_minutes_non_Sky_Sports_cricket_overall integer
,percentile_minutes_niche_sports_sky_sports integer
,percentile_minutes_niche_sports_non_sky_sports integer

--

)
;


update v223_unbundling_viewing_summary_by_account 
set  
percentile_minutes_sport =case when minutes_sport =-1 then 999 when minutes_sport =0 then 100 else abs(rank_minutes_sport /3000)+1 end
,percentile_minutes_sport_sky_sports =case when minutes_sport_sky_sports =-1 then 999 when minutes_sport_sky_sports =0 then 100 else abs(rank_minutes_sport_sky_sports /3000)+1 end
,percentile_minutes_sport_sky_sports_news =case when minutes_sport_sky_sports_news =-1 then 999 when minutes_sport_sky_sports_news =0 then 100 else abs(rank_minutes_sport_sky_sports_news /3000)+1 end

,percentile_minutes_sport_terrestrial =case when minutes_sport_terrestrial =-1 then 999 when minutes_sport_terrestrial =0 then 100 else abs(rank_minutes_sport_terrestrial /3000)+1 end
,percentile_minutes_sport_football_terrestrial =case when minutes_sport_football_terrestrial =-1 then 999 when minutes_sport_football_terrestrial =0 then 100 else abs(rank_minutes_sport_football_terrestrial /3000)+1 end


,percentile_minutes_sport_espn =case when minutes_sport_espn =-1 then 999 when minutes_sport_espn =0 then 100 else abs(rank_minutes_sport_espn /3000)+1 end

,percentile_minutes_football_sky_sports =case when minutes_football_sky_sports =-1 then 999 when minutes_football_sky_sports =0 then 100 else abs(rank_minutes_football_sky_sports /3000)+1 end

,percentile_minutes_football_premier_league_sky_sports =case when minutes_football_premier_league_sky_sports =-1 then 999 when minutes_football_premier_league_sky_sports =0 then 100 else abs(rank_minutes_football_premier_league_sky_sports /3000)+1 end
,percentile_minutes_football_premier_league_ESPN_BT =case when minutes_football_premier_league_ESPN_BT =-1 then 999 when minutes_football_premier_league_ESPN_BT =0 then 100 else abs(rank_minutes_football_premier_league_ESPN_BT /3000)+1 end

,percentile_minutes_football_champions_league_sky_sports =case when minutes_football_champions_league_sky_sports =-1 then 999 when minutes_football_champions_league_sky_sports =0 then 100 else abs(rank_minutes_football_champions_league_sky_sports /3000)+1 end
,percentile_minutes_football_champions_league_non_sky_sports =case when minutes_football_champions_league_non_sky_sports =-1 then 999 when minutes_football_champions_league_non_sky_sports =0 then 100 else abs(rank_minutes_football_champions_league_non_sky_sports /3000)+1 end

--,percentile_minutes_football_fa_cup_sky_sports =case when minutes_football_fa_cup_sky_sports =-1 then 999 when minutes_football_fa_cup_sky_sports =0 then 100 else abs(rank_minutes_football_fa_cup_sky_sports /3000)+1 end
,percentile_minutes_football_europa_league_ESPN_BT =case when minutes_football_europa_league_ESPN_BT =-1 then 999 when minutes_football_europa_league_ESPN_BT =0 then 100 else abs(rank_minutes_football_europa_league_ESPN_BT /3000)+1 end
,percentile_minutes_football_europa_league_Other_Channels =case when minutes_football_europa_league_Other_Channels =-1 then 999 when minutes_football_europa_league_Other_Channels =0 then 100 else abs(rank_minutes_football_europa_league_Other_Channels /3000)+1 end

,percentile_minutes_football_fa_cup_ESPN_BT =case when minutes_football_fa_cup_ESPN_BT =-1 then 999 when minutes_football_fa_cup_ESPN_BT =0 then 100 else abs(rank_minutes_football_fa_cup_ESPN_BT /3000)+1 end
,percentile_minutes_football_fa_cup_Other_Channels =case when minutes_football_fa_cup_Other_Channels =-1 then 999 when minutes_football_fa_cup_Other_Channels =0 then 100 else abs(rank_minutes_football_fa_cup_Other_Channels /3000)+1 end


,percentile_minutes_football_world_cup_qualifier_sky_sports =case when minutes_football_world_cup_qualifier_sky_sports =-1 then 999 when minutes_football_world_cup_qualifier_sky_sports =0 then 100 else abs(rank_minutes_football_world_cup_qualifier_sky_sports /3000)+1 end
,percentile_minutes_football_world_cup_qualifier_non_sky_sports =case when minutes_football_world_cup_qualifier_non_sky_sports =-1 then 999 when minutes_football_world_cup_qualifier_non_sky_sports =0 then 100 else abs(rank_minutes_football_world_cup_qualifier_non_sky_sports /3000)+1 end

,percentile_minutes_football_international_friendly_sky_sports =case when minutes_football_international_friendly_sky_sports =-1 then 999 when minutes_football_international_friendly_sky_sports =0 then 100 else abs(rank_minutes_football_international_friendly_sky_sports /3000)+1 end
,percentile_minutes_football_international_friendly_non_sky_sports =case when minutes_football_international_friendly_non_sky_sports =-1 then 999 when minutes_football_international_friendly_non_sky_sports =0 then 100 else abs(rank_minutes_football_international_friendly_non_sky_sports /3000)+1 end

,percentile_minutes_football_Capital_One_Cup_sky_sports =case when minutes_football_Capital_One_Cup_sky_sports =-1 then 999 when minutes_football_Capital_One_Cup_sky_sports =0 then 100 else abs(rank_minutes_football_Capital_One_Cup_sky_sports /3000)+1 end

,percentile_minutes_football_La_Liga_sky_sports =case when minutes_football_La_Liga_sky_sports =-1 then 999 when minutes_football_La_Liga_sky_sports =0 then 100 else abs(rank_minutes_football_La_Liga_sky_sports /3000)+1 end

,percentile_minutes_football_football_league_sky_sports =case when minutes_football_football_league_sky_sports =-1 then 999 when minutes_football_football_league_sky_sports =0 then 100 else abs(rank_minutes_football_football_league_sky_sports /3000)+1 end

,percentile_minutes_cricket_ashes_sky_sports =case when minutes_cricket_ashes_sky_sports =-1 then 999 when minutes_cricket_ashes_sky_sports =0 then 100 else abs(rank_minutes_cricket_ashes_sky_sports /3000)+1 end
,percentile_minutes_cricket_ashes_non_sky_sports =case when minutes_cricket_ashes_non_sky_sports =-1 then 999 when minutes_cricket_ashes_non_sky_sports =0 then 100 else abs(rank_minutes_cricket_ashes_non_sky_sports /3000)+1 end

,percentile_minutes_cricket_non_ashes_sky_sports =case when minutes_cricket_non_ashes_sky_sports =-1 then 999 when minutes_cricket_non_ashes_sky_sports =0 then 100 else abs(rank_minutes_cricket_non_ashes_sky_sports /3000)+1 end
,percentile_minutes_cricket_non_ashes_non_sky_sports =case when minutes_cricket_non_ashes_non_sky_sports =-1 then 999 when minutes_cricket_non_ashes_non_sky_sports =0 then 100 else abs(rank_minutes_cricket_non_ashes_non_sky_sports /3000)+1 end



,percentile_minutes_golf_sky_sports =case when minutes_golf_sky_sports =-1 then 999 when minutes_golf_sky_sports =0 then 100 else abs(rank_minutes_golf_sky_sports /3000)+1 end
,percentile_minutes_golf_non_sky_sports =case when minutes_golf_non_sky_sports =-1 then 999 when minutes_golf_non_sky_sports =0 then 100 else abs(rank_minutes_golf_non_sky_sports /3000)+1 end

,percentile_minutes_tennis_sky_sports =case when minutes_tennis_sky_sports =-1 then 999 when minutes_tennis_sky_sports =0 then 100 else abs(rank_minutes_tennis_sky_sports /3000)+1 end
,percentile_minutes_tennis_non_sky_sports =case when minutes_tennis_non_sky_sports =-1 then 999 when minutes_tennis_non_sky_sports =0 then 100 else abs(rank_minutes_tennis_non_sky_sports /3000)+1 end

,percentile_minutes_motor_sport_sky_sports =case when minutes_motor_sport_sky_sports =-1 then 999 when minutes_motor_sport_sky_sports =0 then 100 else abs(rank_minutes_motor_sport_sky_sports /3000)+1 end
,percentile_minutes_motor_sport_non_sky_sports =case when minutes_motor_sport_non_sky_sports =-1 then 999 when minutes_motor_sport_non_sky_sports =0 then 100 else abs(rank_minutes_motor_sport_non_sky_sports /3000)+1 end

,percentile_minutes_F1_sky_sports =case when minutes_F1_sky_sports =-1 then 999 when minutes_F1_sky_sports =0 then 100 else abs(rank_minutes_F1_sky_sports /3000)+1 end
,percentile_minutes_F1_non_sky_sports =case when minutes_F1_non_sky_sports =-1 then 999 when minutes_F1_non_sky_sports =0 then 100 else abs(rank_minutes_F1_non_sky_sports /3000)+1 end

,percentile_minutes_horse_racing_sky_sports =case when minutes_horse_racing_sky_sports =-1 then 999 when minutes_horse_racing_sky_sports =0 then 100 else abs(rank_minutes_horse_racing_sky_sports /3000)+1 end
,percentile_minutes_horse_racing_non_sky_sports =case when minutes_horse_racing_non_sky_sports =-1 then 999 when minutes_horse_racing_non_sky_sports =0 then 100 else abs(rank_minutes_horse_racing_non_sky_sports /3000)+1 end

,percentile_minutes_snooker_pool_sky_sports =case when minutes_snooker_pool_sky_sports =-1 then 999 when minutes_snooker_pool_sky_sports =0 then 100 else abs(rank_minutes_snooker_pool_sky_sports /3000)+1 end
,percentile_minutes_snooker_pool_non_sky_sports =case when minutes_snooker_pool_non_sky_sports =-1 then 999 when minutes_snooker_pool_non_sky_sports =0 then 100 else abs(rank_minutes_snooker_pool_non_sky_sports /3000)+1 end

,percentile_minutes_rugby_sky_sports =case when minutes_rugby_sky_sports =-1 then 999 when minutes_rugby_sky_sports =0 then 100 else abs(rank_minutes_rugby_sky_sports /3000)+1 end
,percentile_minutes_rugby_non_sky_sports =case when minutes_rugby_non_sky_sports =-1 then 999 when minutes_rugby_non_sky_sports =0 then 100 else abs(rank_minutes_rugby_non_sky_sports /3000)+1 end

,percentile_minutes_wrestling_sky_sports =case when minutes_wrestling_sky_sports =-1 then 999 when minutes_wrestling_sky_sports =0 then 100 else abs(rank_minutes_wrestling_sky_sports /3000)+1 end
,percentile_minutes_wrestling_non_sky_sports =case when minutes_wrestling_non_sky_sports =-1 then 999 when minutes_wrestling_non_sky_sports =0 then 100 else abs(rank_minutes_wrestling_non_sky_sports /3000)+1 end

,percentile_minutes_wwe =case when minutes_wwe =-1 then 999 when minutes_wwe =0 then 100 else abs(rank_minutes_wwe /3000)+1 end
,percentile_minutes_wwe_sky_sports =case when minutes_wwe_sky_sports =-1 then 999 when minutes_wwe_sky_sports =0 then 100 else abs(rank_minutes_wwe_sky_sports /3000)+1 end
,percentile_minutes_wwe_sbo =case when minutes_wwe_sbo =-1 then 999 when minutes_wwe_sbo =0 then 100 else abs(rank_minutes_wwe_sbo /3000)+1 end
,percentile_minutes_sky_1_or_2 =case when minutes_wwe_sky_1_or_2 =-1 then 999 when minutes_wwe_sky_1_or_2 =0 then 100 else abs(rank_minutes_wwe_sky_1_or_2 /3000)+1 end

,percentile_minutes_darts_sky_sports =case when minutes_darts_sky_sports =-1 then 999 when minutes_darts_sky_sports =0 then 100 else abs(rank_minutes_darts_sky_sports /3000)+1 end
,percentile_minutes_darts_non_sky_sports =case when minutes_darts_non_sky_sports =-1 then 999 when minutes_darts_non_sky_sports =0 then 100 else abs(rank_minutes_darts_non_sky_sports /3000)+1 end

,percentile_minutes_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 999 when minutes_boxing_sky_sports =0 then 100 else abs(rank_minutes_boxing_sky_sports /3000)+1 end
,percentile_minutes_boxing_non_sky_sports =case when minutes_boxing_non_sky_sports =-1 then 999 when minutes_boxing_non_sky_sports =0 then 100 else abs(rank_minutes_boxing_non_sky_sports /3000)+1 end


,percentile_minutes_overall_football =case when minutes_overall_football =-1 then 999 when minutes_overall_football =0 then 100 else abs(rank_minutes_overall_football /3000)+1 end
,percentile_minutes_Sky_Sports_cricket_overall =case when minutes_Sky_Sports_cricket_overall =-1 then 999 when minutes_Sky_Sports_cricket_overall =0 then 100 else abs(rank_minutes_Sky_Sports_cricket_overall /3000)+1 end
,percentile_minutes_non_Sky_Sports_cricket_overall =case when minutes_non_Sky_Sports_cricket_overall =-1 then 999 when minutes_non_Sky_Sports_cricket_overall =0 then 100 else abs(rank_minutes_non_Sky_Sports_cricket_overall /3000)+1 end

,percentile_minutes_niche_sports_sky_sports =case when minutes_niche_sports_sky_sports =-1 then 999 when minutes_niche_sports_sky_sports =0 then 100 else abs(rank_minutes_niche_sports_sky_sports /3000)+1 end
,percentile_minutes_niche_sports_non_Sky_Sports =case when minutes_niche_sports_non_Sky_Sports =-1 then 999 when minutes_niche_sports_non_Sky_Sports =0 then 100 else abs(rank_minutes_niche_sports_non_Sky_Sports /3000)+1 end



from v223_unbundling_viewing_summary_by_account 
;
commit;


---Repeat with percentile splits for 3min+ progs and Engaged Progs----

alter table v223_unbundling_viewing_summary_by_account add
(percentile_prog_3min_plus_sport integer
,percentile_prog_3min_plus_sport_sky_sports integer
,percentile_prog_3min_plus_sport_sky_sports_news integer

,percentile_prog_3min_plus_sport_terrestrial integer
,percentile_prog_3min_plus_sport_football_terrestrial integer

,percentile_prog_3min_plus_sport_espn integer
,percentile_prog_3min_plus_football_sky_sports integer

,percentile_prog_3min_plus_football_premier_league_sky_sports integer
,percentile_prog_3min_plus_football_premier_league_ESPN_BT integer

,percentile_prog_3min_plus_football_champions_league_sky_sports integer
,percentile_prog_3min_plus_football_champions_league_non_sky_sports integer

,percentile_prog_3min_plus_football_fa_cup_sky_sports integer
,percentile_prog_3min_plus_football_fa_cup_ESPN_BT integer
,percentile_prog_3min_plus_football_fa_cup_Other_Channels integer

,percentile_prog_3min_plus_football_europa_league_ESPN_BT integer
,percentile_prog_3min_plus_football_europa_league_Other_Channels integer

,percentile_prog_3min_plus_football_world_cup_qualifier_sky_sports integer
,percentile_prog_3min_plus_football_world_cup_qualifier_non_sky_sports integer

,percentile_prog_3min_plus_football_international_friendly_sky_sports integer
,percentile_prog_3min_plus_football_international_friendly_non_sky_sports integer

,percentile_prog_3min_plus_football_Capital_One_Cup_sky_sports integer

,percentile_prog_3min_plus_football_La_Liga_sky_sports integer

,percentile_prog_3min_plus_football_football_league_sky_sports integer

,percentile_prog_3min_plus_cricket_ashes_sky_sports integer
,percentile_prog_3min_plus_cricket_ashes_non_sky_sports integer

,percentile_prog_3min_plus_cricket_non_ashes_sky_sports integer
,percentile_prog_3min_plus_cricket_non_ashes_non_sky_sports integer



,percentile_prog_3min_plus_golf_sky_sports integer
,percentile_prog_3min_plus_golf_non_sky_sports integer

,percentile_prog_3min_plus_tennis_sky_sports integer
,percentile_prog_3min_plus_tennis_non_sky_sports integer

,percentile_prog_3min_plus_motor_sport_sky_sports integer
,percentile_prog_3min_plus_motor_sport_non_sky_sports integer

,percentile_prog_3min_plus_F1_sky_sports integer
,percentile_prog_3min_plus_F1_non_sky_sports integer

,percentile_prog_3min_plus_horse_racing_sky_sports integer
,percentile_prog_3min_plus_horse_racing_non_sky_sports integer

,percentile_prog_3min_plus_snooker_pool_sky_sports integer
,percentile_prog_3min_plus_snooker_pool_non_sky_sports integer

,percentile_prog_3min_plus_rugby_sky_sports integer
,percentile_prog_3min_plus_rugby_non_sky_sports integer

,percentile_prog_3min_plus_wrestling_sky_sports integer
,percentile_prog_3min_plus_wrestling_non_sky_sports integer


,percentile_prog_3min_plus_wwe  integer
,percentile_prog_3min_plus_wwe_sky_sports  integer
,percentile_prog_3min_plus_wwe_sbo integer
,percentile_prog_3min_plus_sky_1_or_2 integer

,percentile_prog_3min_plus_darts_sky_sports integer
,percentile_prog_3min_plus_darts_non_sky_sports integer

,percentile_prog_3min_plus_boxing_sky_sports integer
,percentile_prog_3min_plus_boxing_non_sky_sports integer

,percentile_prog_3min_plus_overall_football integer
,percentile_prog_3min_plus_Sky_Sports_cricket_overall integer
,percentile_prog_3min_plus_non_Sky_Sports_cricket_overall integer
,percentile_prog_3min_plus_niche_sports_sky_sports integer
,percentile_prog_3min_plus_niche_sports_non_sky_sports integer
)
;


update v223_unbundling_viewing_summary_by_account 
set  
percentile_prog_3min_plus_sport =case when minutes_sport =-1 then 999 when minutes_sport =0 then 100 else abs(rank_prog_3min_plus_sport /3000)+1 end
,percentile_prog_3min_plus_sport_sky_sports =case when minutes_sport_sky_sports =-1 then 999 when minutes_sport_sky_sports =0 then 100 else abs(rank_prog_3min_plus_sport_sky_sports /3000)+1 end
,percentile_prog_3min_plus_sport_sky_sports_news =case when minutes_sport_sky_sports_news =-1 then 999 when minutes_sport_sky_sports_news =0 then 100 else abs(rank_prog_3min_plus_sport_sky_sports_news /3000)+1 end

,percentile_prog_3min_plus_sport_terrestrial =case when minutes_sport_terrestrial =-1 then 999 when minutes_sport_terrestrial =0 then 100 else abs(rank_prog_3min_plus_sport_terrestrial /3000)+1 end
,percentile_prog_3min_plus_sport_football_terrestrial =case when minutes_sport_football_terrestrial =-1 then 999 when minutes_sport_football_terrestrial =0 then 100 else abs(rank_prog_3min_plus_sport_football_terrestrial /3000)+1 end


,percentile_prog_3min_plus_sport_espn =case when minutes_sport_espn =-1 then 999 when minutes_sport_espn =0 then 100 else abs(rank_prog_3min_plus_sport_espn /3000)+1 end

,percentile_prog_3min_plus_football_sky_sports =case when minutes_football_sky_sports =-1 then 999 when minutes_football_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_sky_sports /3000)+1 end

,percentile_prog_3min_plus_football_premier_league_sky_sports =case when minutes_football_premier_league_sky_sports =-1 then 999 when minutes_football_premier_league_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_premier_league_sky_sports /3000)+1 end
,percentile_prog_3min_plus_football_premier_league_ESPN_BT =case when minutes_football_premier_league_ESPN_BT =-1 then 999 when minutes_football_premier_league_ESPN_BT =0 then 100 else abs(rank_prog_3min_plus_football_premier_league_ESPN_BT /3000)+1 end

,percentile_prog_3min_plus_football_champions_league_sky_sports =case when minutes_football_champions_league_sky_sports =-1 then 999 when minutes_football_champions_league_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_champions_league_sky_sports /3000)+1 end
,percentile_prog_3min_plus_football_champions_league_non_sky_sports =case when minutes_football_champions_league_non_sky_sports =-1 then 999 when minutes_football_champions_league_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_champions_league_non_sky_sports /3000)+1 end

--,percentile_prog_3min_plus_football_fa_cup_sky_sports =case when minutes_football_fa_cup_sky_sports =-1 then 999 when minutes_football_fa_cup_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_fa_cup_sky_sports /3000)+1 end
,percentile_prog_3min_plus_football_europa_league_ESPN_BT =case when minutes_football_europa_league_ESPN_BT =-1 then 999 when minutes_football_europa_league_ESPN_BT =0 then 100 else abs(rank_prog_3min_plus_football_europa_league_ESPN_BT /3000)+1 end
,percentile_prog_3min_plus_football_europa_league_Other_Channels =case when minutes_football_europa_league_Other_Channels =-1 then 999 when minutes_football_europa_league_Other_Channels =0 then 100 else abs(rank_prog_3min_plus_football_europa_league_Other_Channels /3000)+1 end

,percentile_prog_3min_plus_football_fa_cup_ESPN_BT =case when minutes_football_fa_cup_ESPN_BT =-1 then 999 when minutes_football_fa_cup_ESPN_BT =0 then 100 else abs(rank_prog_3min_plus_football_fa_cup_ESPN_BT /3000)+1 end
,percentile_prog_3min_plus_football_fa_cup_Other_Channels =case when minutes_football_fa_cup_Other_Channels =-1 then 999 when minutes_football_fa_cup_Other_Channels =0 then 100 else abs(rank_prog_3min_plus_football_fa_cup_Other_Channels /3000)+1 end


,percentile_prog_3min_plus_football_world_cup_qualifier_sky_sports =case when minutes_football_world_cup_qualifier_sky_sports =-1 then 999 when minutes_football_world_cup_qualifier_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_world_cup_qualifier_sky_sports /3000)+1 end
,percentile_prog_3min_plus_football_world_cup_qualifier_non_sky_sports =case when minutes_football_world_cup_qualifier_non_sky_sports =-1 then 999 when minutes_football_world_cup_qualifier_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_world_cup_qualifier_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_football_international_friendly_sky_sports =case when minutes_football_international_friendly_sky_sports =-1 then 999 when minutes_football_international_friendly_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_international_friendly_sky_sports /3000)+1 end
,percentile_prog_3min_plus_football_international_friendly_non_sky_sports =case when minutes_football_international_friendly_non_sky_sports =-1 then 999 when minutes_football_international_friendly_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_international_friendly_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_football_Capital_One_Cup_sky_sports =case when minutes_football_Capital_One_Cup_sky_sports =-1 then 999 when minutes_football_Capital_One_Cup_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_Capital_One_Cup_sky_sports /3000)+1 end

,percentile_prog_3min_plus_football_La_Liga_sky_sports =case when minutes_football_La_Liga_sky_sports =-1 then 999 when minutes_football_La_Liga_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_La_Liga_sky_sports /3000)+1 end

,percentile_prog_3min_plus_football_football_league_sky_sports =case when minutes_football_football_league_sky_sports =-1 then 999 when minutes_football_football_league_sky_sports =0 then 100 else abs(rank_prog_3min_plus_football_football_league_sky_sports /3000)+1 end

,percentile_prog_3min_plus_cricket_ashes_sky_sports =case when minutes_cricket_ashes_sky_sports =-1 then 999 when minutes_cricket_ashes_sky_sports =0 then 100 else abs(rank_prog_3min_plus_cricket_ashes_sky_sports /3000)+1 end
,percentile_prog_3min_plus_cricket_ashes_non_sky_sports =case when minutes_cricket_ashes_non_sky_sports =-1 then 999 when minutes_cricket_ashes_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_cricket_ashes_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_cricket_non_ashes_sky_sports =case when minutes_cricket_non_ashes_sky_sports =-1 then 999 when minutes_cricket_non_ashes_sky_sports =0 then 100 else abs(rank_prog_3min_plus_cricket_non_ashes_sky_sports /3000)+1 end
,percentile_prog_3min_plus_cricket_non_ashes_non_sky_sports =case when minutes_cricket_non_ashes_non_sky_sports =-1 then 999 when minutes_cricket_non_ashes_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_cricket_non_ashes_non_sky_sports /3000)+1 end



,percentile_prog_3min_plus_golf_sky_sports =case when minutes_golf_sky_sports =-1 then 999 when minutes_golf_sky_sports =0 then 100 else abs(rank_prog_3min_plus_golf_sky_sports /3000)+1 end
,percentile_prog_3min_plus_golf_non_sky_sports =case when minutes_golf_non_sky_sports =-1 then 999 when minutes_golf_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_golf_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_tennis_sky_sports =case when minutes_tennis_sky_sports =-1 then 999 when minutes_tennis_sky_sports =0 then 100 else abs(rank_prog_3min_plus_tennis_sky_sports /3000)+1 end
,percentile_prog_3min_plus_tennis_non_sky_sports =case when minutes_tennis_non_sky_sports =-1 then 999 when minutes_tennis_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_tennis_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_motor_sport_sky_sports =case when minutes_motor_sport_sky_sports =-1 then 999 when minutes_motor_sport_sky_sports =0 then 100 else abs(rank_prog_3min_plus_motor_sport_sky_sports /3000)+1 end
,percentile_prog_3min_plus_motor_sport_non_sky_sports =case when minutes_motor_sport_non_sky_sports =-1 then 999 when minutes_motor_sport_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_motor_sport_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_F1_sky_sports =case when minutes_F1_sky_sports =-1 then 999 when minutes_F1_sky_sports =0 then 100 else abs(rank_prog_3min_plus_F1_sky_sports /3000)+1 end
,percentile_prog_3min_plus_F1_non_sky_sports =case when minutes_F1_non_sky_sports =-1 then 999 when minutes_F1_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_F1_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_horse_racing_sky_sports =case when minutes_horse_racing_sky_sports =-1 then 999 when minutes_horse_racing_sky_sports =0 then 100 else abs(rank_prog_3min_plus_horse_racing_sky_sports /3000)+1 end
,percentile_prog_3min_plus_horse_racing_non_sky_sports =case when minutes_horse_racing_non_sky_sports =-1 then 999 when minutes_horse_racing_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_horse_racing_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_snooker_pool_sky_sports =case when minutes_snooker_pool_sky_sports =-1 then 999 when minutes_snooker_pool_sky_sports =0 then 100 else abs(rank_prog_3min_plus_snooker_pool_sky_sports /3000)+1 end
,percentile_prog_3min_plus_snooker_pool_non_sky_sports =case when minutes_snooker_pool_non_sky_sports =-1 then 999 when minutes_snooker_pool_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_snooker_pool_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_rugby_sky_sports =case when minutes_rugby_sky_sports =-1 then 999 when minutes_rugby_sky_sports =0 then 100 else abs(rank_prog_3min_plus_rugby_sky_sports /3000)+1 end
,percentile_prog_3min_plus_rugby_non_sky_sports =case when minutes_rugby_non_sky_sports =-1 then 999 when minutes_rugby_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_rugby_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_wrestling_sky_sports =case when minutes_wrestling_sky_sports =-1 then 999 when minutes_wrestling_sky_sports =0 then 100 else abs(rank_prog_3min_plus_wrestling_sky_sports /3000)+1 end
,percentile_prog_3min_plus_wrestling_non_sky_sports =case when minutes_wrestling_non_sky_sports =-1 then 999 when minutes_wrestling_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_wrestling_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_wwe =case when minutes_wwe =-1 then 999 when minutes_wwe =0 then 100 else abs(rank_prog_3min_plus_wwe /3000)+1 end
,percentile_prog_3min_plus_wwe_sky_sports =case when minutes_wwe_sky_sports =-1 then 999 when minutes_wwe_sky_sports =0 then 100 else abs(rank_prog_3min_plus_wwe_sky_sports /3000)+1 end
,percentile_prog_3min_plus_wwe_sbo =case when minutes_wwe_sbo =-1 then 999 when minutes_wwe_sbo =0 then 100 else abs(rank_prog_3min_plus_wwe_sbo /3000)+1 end
,percentile_prog_3min_plus_sky_1_or_2 =case when minutes_wwe_sky_1_or_2 =-1 then 999 when minutes_wwe_sky_1_or_2 =0 then 100 else abs(rank_prog_3min_plus_wwe_sky_1_or_2 /3000)+1 end

,percentile_prog_3min_plus_darts_sky_sports =case when minutes_darts_sky_sports =-1 then 999 when minutes_darts_sky_sports =0 then 100 else abs(rank_prog_3min_plus_darts_sky_sports /3000)+1 end
,percentile_prog_3min_plus_darts_non_sky_sports =case when minutes_darts_non_sky_sports =-1 then 999 when minutes_darts_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_darts_non_sky_sports /3000)+1 end

,percentile_prog_3min_plus_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 999 when minutes_boxing_sky_sports =0 then 100 else abs(rank_prog_3min_plus_boxing_sky_sports /3000)+1 end
,percentile_prog_3min_plus_boxing_non_sky_sports =case when minutes_boxing_non_sky_sports =-1 then 999 when minutes_boxing_non_sky_sports =0 then 100 else abs(rank_prog_3min_plus_boxing_non_sky_sports /3000)+1 end



,percentile_prog_3min_plus_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 999 when minutes_boxing_sky_sports =0 then 100 else abs(rank_prog_3min_plus_boxing_sky_sports /3000)+1 end


,percentile_prog_3min_plus_overall_football =case when minutes_overall_football =-1 then 999 when minutes_overall_football =0 then 100 else abs(rank_prog_3min_plus_overall_football /3000)+1 end
,percentile_prog_3min_plus_Sky_Sports_cricket_overall =case when minutes_Sky_Sports_cricket_overall =-1 then 999 when minutes_Sky_Sports_cricket_overall =0 then 100 else abs(rank_prog_3min_plus_Sky_Sports_cricket_overall /3000)+1 end
,percentile_prog_3min_plus_non_Sky_Sports_cricket_overall =case when minutes_non_Sky_Sports_cricket_overall =-1 then 999 when minutes_non_Sky_Sports_cricket_overall =0 then 100 else abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall /3000)+1 end

,percentile_prog_3min_plus_niche_sports_sky_sports =case when minutes_niche_sports_sky_sports =-1 then 999 when minutes_niche_sports_sky_sports =0 then 100 else abs(rank_prog_3min_plus_niche_sports_sky_sports /3000)+1 end
,percentile_prog_3min_plus_niche_sports_non_Sky_Sports =case when minutes_niche_sports_non_Sky_Sports =-1 then 999 when minutes_niche_sports_non_Sky_Sports =0 then 100 else abs(rank_prog_3min_plus_niche_sports_non_Sky_Sports /3000)+1 end

from v223_unbundling_viewing_summary_by_account 
;
commit;



----Repeat for Engaged Programmes-----


alter table v223_unbundling_viewing_summary_by_account add
(percentile_prog_engaged_sport integer
,percentile_prog_engaged_sport_sky_sports integer
,percentile_prog_engaged_sport_sky_sports_news integer

,percentile_prog_engaged_sport_terrestrial integer
,percentile_prog_engaged_sport_football_terrestrial integer

,percentile_prog_engaged_sport_espn integer
,percentile_prog_engaged_football_sky_sports integer

,percentile_prog_engaged_football_premier_league_sky_sports integer
,percentile_prog_engaged_football_premier_league_ESPN_BT integer

,percentile_prog_engaged_football_champions_league_sky_sports integer
,percentile_prog_engaged_football_champions_league_non_sky_sports integer

,percentile_prog_engaged_football_fa_cup_sky_sports integer
,percentile_prog_engaged_football_fa_cup_ESPN_BT integer
,percentile_prog_engaged_football_fa_cup_Other_Channels integer

,percentile_prog_engaged_football_europa_league_ESPN_BT integer
,percentile_prog_engaged_football_europa_league_Other_Channels integer

,percentile_prog_engaged_football_world_cup_qualifier_sky_sports integer
,percentile_prog_engaged_football_world_cup_qualifier_non_sky_sports integer

,percentile_prog_engaged_football_international_friendly_sky_sports integer
,percentile_prog_engaged_football_international_friendly_non_sky_sports integer

,percentile_prog_engaged_football_Capital_One_Cup_sky_sports integer

,percentile_prog_engaged_football_La_Liga_sky_sports integer

,percentile_prog_engaged_football_football_league_sky_sports integer

,percentile_prog_engaged_cricket_ashes_sky_sports integer
,percentile_prog_engaged_cricket_ashes_non_sky_sports integer

,percentile_prog_engaged_cricket_non_ashes_sky_sports integer
,percentile_prog_engaged_cricket_non_ashes_non_sky_sports integer



,percentile_prog_engaged_golf_sky_sports integer
,percentile_prog_engaged_golf_non_sky_sports integer

,percentile_prog_engaged_tennis_sky_sports integer
,percentile_prog_engaged_tennis_non_sky_sports integer

,percentile_prog_engaged_motor_sport_sky_sports integer
,percentile_prog_engaged_motor_sport_non_sky_sports integer

,percentile_prog_engaged_F1_sky_sports integer
,percentile_prog_engaged_F1_non_sky_sports integer

,percentile_prog_engaged_horse_racing_sky_sports integer
,percentile_prog_engaged_horse_racing_non_sky_sports integer

,percentile_prog_engaged_snooker_pool_sky_sports integer
,percentile_prog_engaged_snooker_pool_non_sky_sports integer

,percentile_prog_engaged_rugby_sky_sports integer
,percentile_prog_engaged_rugby_non_sky_sports integer

,percentile_prog_engaged_wrestling_sky_sports integer
,percentile_prog_engaged_wrestling_non_sky_sports integer


,percentile_prog_engaged_wwe  integer
,percentile_prog_engaged_wwe_sky_sports  integer
,percentile_prog_engaged_wwe_sbo integer
,percentile_prog_engaged_sky_1_or_2 integer

,percentile_prog_engaged_darts_sky_sports integer
,percentile_prog_engaged_darts_non_sky_sports integer

,percentile_prog_engaged_boxing_sky_sports integer
,percentile_prog_engaged_boxing_non_sky_sports integer


,percentile_prog_engaged_overall_football integer
,percentile_prog_engaged_Sky_Sports_cricket_overall integer
,percentile_prog_engaged_non_Sky_Sports_cricket_overall integer
,percentile_prog_engaged_niche_sports_sky_sports integer
,percentile_prog_engaged_niche_sports_non_sky_sports integer
)
;


update v223_unbundling_viewing_summary_by_account 
set  
percentile_prog_engaged_sport =case when minutes_sport =-1 then 999 when minutes_sport =0 then 100 else abs(rank_prog_engaged_sport /3000)+1 end
,percentile_prog_engaged_sport_sky_sports =case when minutes_sport_sky_sports =-1 then 999 when minutes_sport_sky_sports =0 then 100 else abs(rank_prog_engaged_sport_sky_sports /3000)+1 end
,percentile_prog_engaged_sport_sky_sports_news =case when minutes_sport_sky_sports_news =-1 then 999 when minutes_sport_sky_sports_news =0 then 100 else abs(rank_prog_engaged_sport_sky_sports_news /3000)+1 end

,percentile_prog_engaged_sport_terrestrial =case when minutes_sport_terrestrial =-1 then 999 when minutes_sport_terrestrial =0 then 100 else abs(rank_prog_engaged_sport_terrestrial /3000)+1 end
,percentile_prog_engaged_sport_football_terrestrial =case when minutes_sport_football_terrestrial =-1 then 999 when minutes_sport_football_terrestrial =0 then 100 else abs(rank_prog_engaged_sport_football_terrestrial /3000)+1 end


,percentile_prog_engaged_sport_espn =case when minutes_sport_espn =-1 then 999 when minutes_sport_espn =0 then 100 else abs(rank_prog_engaged_sport_espn /3000)+1 end

,percentile_prog_engaged_football_sky_sports =case when minutes_football_sky_sports =-1 then 999 when minutes_football_sky_sports =0 then 100 else abs(rank_prog_engaged_football_sky_sports /3000)+1 end

,percentile_prog_engaged_football_premier_league_sky_sports =case when minutes_football_premier_league_sky_sports =-1 then 999 when minutes_football_premier_league_sky_sports =0 then 100 else abs(rank_prog_engaged_football_premier_league_sky_sports /3000)+1 end
,percentile_prog_engaged_football_premier_league_ESPN_BT =case when minutes_football_premier_league_ESPN_BT =-1 then 999 when minutes_football_premier_league_ESPN_BT =0 then 100 else abs(rank_prog_engaged_football_premier_league_ESPN_BT /3000)+1 end

,percentile_prog_engaged_football_champions_league_sky_sports =case when minutes_football_champions_league_sky_sports =-1 then 999 when minutes_football_champions_league_sky_sports =0 then 100 else abs(rank_prog_engaged_football_champions_league_sky_sports /3000)+1 end
,percentile_prog_engaged_football_champions_league_non_sky_sports =case when minutes_football_champions_league_non_sky_sports =-1 then 999 when minutes_football_champions_league_non_sky_sports =0 then 100 else abs(rank_prog_engaged_football_champions_league_non_sky_sports /3000)+1 end

--,percentile_prog_engaged_football_fa_cup_sky_sports =case when minutes_football_fa_cup_sky_sports =-1 then 999 when minutes_football_fa_cup_sky_sports =0 then 100 else abs(rank_prog_engaged_football_fa_cup_sky_sports /3000)+1 end
,percentile_prog_engaged_football_europa_league_ESPN_BT =case when minutes_football_europa_league_ESPN_BT =-1 then 999 when minutes_football_europa_league_ESPN_BT =0 then 100 else abs(rank_prog_engaged_football_europa_league_ESPN_BT /3000)+1 end
,percentile_prog_engaged_football_europa_league_Other_Channels =case when minutes_football_europa_league_Other_Channels =-1 then 999 when minutes_football_europa_league_Other_Channels =0 then 100 else abs(rank_prog_engaged_football_europa_league_Other_Channels /3000)+1 end

,percentile_prog_engaged_football_fa_cup_ESPN_BT =case when minutes_football_fa_cup_ESPN_BT =-1 then 999 when minutes_football_fa_cup_ESPN_BT =0 then 100 else abs(rank_prog_engaged_football_fa_cup_ESPN_BT /3000)+1 end
,percentile_prog_engaged_football_fa_cup_Other_Channels =case when minutes_football_fa_cup_Other_Channels =-1 then 999 when minutes_football_fa_cup_Other_Channels =0 then 100 else abs(rank_prog_engaged_football_fa_cup_Other_Channels /3000)+1 end


,percentile_prog_engaged_football_world_cup_qualifier_sky_sports =case when minutes_football_world_cup_qualifier_sky_sports =-1 then 999 when minutes_football_world_cup_qualifier_sky_sports =0 then 100 else abs(rank_prog_engaged_football_world_cup_qualifier_sky_sports /3000)+1 end
,percentile_prog_engaged_football_world_cup_qualifier_non_sky_sports =case when minutes_football_world_cup_qualifier_non_sky_sports =-1 then 999 when minutes_football_world_cup_qualifier_non_sky_sports =0 then 100 else abs(rank_prog_engaged_football_world_cup_qualifier_non_sky_sports /3000)+1 end

,percentile_prog_engaged_football_international_friendly_sky_sports =case when minutes_football_international_friendly_sky_sports =-1 then 999 when minutes_football_international_friendly_sky_sports =0 then 100 else abs(rank_prog_engaged_football_international_friendly_sky_sports /3000)+1 end
,percentile_prog_engaged_football_international_friendly_non_sky_sports =case when minutes_football_international_friendly_non_sky_sports =-1 then 999 when minutes_football_international_friendly_non_sky_sports =0 then 100 else abs(rank_prog_engaged_football_international_friendly_non_sky_sports /3000)+1 end

,percentile_prog_engaged_football_Capital_One_Cup_sky_sports =case when minutes_football_Capital_One_Cup_sky_sports =-1 then 999 when minutes_football_Capital_One_Cup_sky_sports =0 then 100 else abs(rank_prog_engaged_football_Capital_One_Cup_sky_sports /3000)+1 end

,percentile_prog_engaged_football_La_Liga_sky_sports =case when minutes_football_La_Liga_sky_sports =-1 then 999 when minutes_football_La_Liga_sky_sports =0 then 100 else abs(rank_prog_engaged_football_La_Liga_sky_sports /3000)+1 end

,percentile_prog_engaged_football_football_league_sky_sports =case when minutes_football_football_league_sky_sports =-1 then 999 when minutes_football_football_league_sky_sports =0 then 100 else abs(rank_prog_engaged_football_football_league_sky_sports /3000)+1 end

,percentile_prog_engaged_cricket_ashes_sky_sports =case when minutes_cricket_ashes_sky_sports =-1 then 999 when minutes_cricket_ashes_sky_sports =0 then 100 else abs(rank_prog_engaged_cricket_ashes_sky_sports /3000)+1 end
,percentile_prog_engaged_cricket_ashes_non_sky_sports =case when minutes_cricket_ashes_non_sky_sports =-1 then 999 when minutes_cricket_ashes_non_sky_sports =0 then 100 else abs(rank_prog_engaged_cricket_ashes_non_sky_sports /3000)+1 end

,percentile_prog_engaged_cricket_non_ashes_sky_sports =case when minutes_cricket_non_ashes_sky_sports =-1 then 999 when minutes_cricket_non_ashes_sky_sports =0 then 100 else abs(rank_prog_engaged_cricket_non_ashes_sky_sports /3000)+1 end
,percentile_prog_engaged_cricket_non_ashes_non_sky_sports =case when minutes_cricket_non_ashes_non_sky_sports =-1 then 999 when minutes_cricket_non_ashes_non_sky_sports =0 then 100 else abs(rank_prog_engaged_cricket_non_ashes_non_sky_sports /3000)+1 end



,percentile_prog_engaged_golf_sky_sports =case when minutes_golf_sky_sports =-1 then 999 when minutes_golf_sky_sports =0 then 100 else abs(rank_prog_engaged_golf_sky_sports /3000)+1 end
,percentile_prog_engaged_golf_non_sky_sports =case when minutes_golf_non_sky_sports =-1 then 999 when minutes_golf_non_sky_sports =0 then 100 else abs(rank_prog_engaged_golf_non_sky_sports /3000)+1 end

,percentile_prog_engaged_tennis_sky_sports =case when minutes_tennis_sky_sports =-1 then 999 when minutes_tennis_sky_sports =0 then 100 else abs(rank_prog_engaged_tennis_sky_sports /3000)+1 end
,percentile_prog_engaged_tennis_non_sky_sports =case when minutes_tennis_non_sky_sports =-1 then 999 when minutes_tennis_non_sky_sports =0 then 100 else abs(rank_prog_engaged_tennis_non_sky_sports /3000)+1 end

,percentile_prog_engaged_motor_sport_sky_sports =case when minutes_motor_sport_sky_sports =-1 then 999 when minutes_motor_sport_sky_sports =0 then 100 else abs(rank_prog_engaged_motor_sport_sky_sports /3000)+1 end
,percentile_prog_engaged_motor_sport_non_sky_sports =case when minutes_motor_sport_non_sky_sports =-1 then 999 when minutes_motor_sport_non_sky_sports =0 then 100 else abs(rank_prog_engaged_motor_sport_non_sky_sports /3000)+1 end

,percentile_prog_engaged_F1_sky_sports =case when minutes_F1_sky_sports =-1 then 999 when minutes_F1_sky_sports =0 then 100 else abs(rank_prog_engaged_F1_sky_sports /3000)+1 end
,percentile_prog_engaged_F1_non_sky_sports =case when minutes_F1_non_sky_sports =-1 then 999 when minutes_F1_non_sky_sports =0 then 100 else abs(rank_prog_engaged_F1_non_sky_sports /3000)+1 end

,percentile_prog_engaged_horse_racing_sky_sports =case when minutes_horse_racing_sky_sports =-1 then 999 when minutes_horse_racing_sky_sports =0 then 100 else abs(rank_prog_engaged_horse_racing_sky_sports /3000)+1 end
,percentile_prog_engaged_horse_racing_non_sky_sports =case when minutes_horse_racing_non_sky_sports =-1 then 999 when minutes_horse_racing_non_sky_sports =0 then 100 else abs(rank_prog_engaged_horse_racing_non_sky_sports /3000)+1 end

,percentile_prog_engaged_snooker_pool_sky_sports =case when minutes_snooker_pool_sky_sports =-1 then 999 when minutes_snooker_pool_sky_sports =0 then 100 else abs(rank_prog_engaged_snooker_pool_sky_sports /3000)+1 end
,percentile_prog_engaged_snooker_pool_non_sky_sports =case when minutes_snooker_pool_non_sky_sports =-1 then 999 when minutes_snooker_pool_non_sky_sports =0 then 100 else abs(rank_prog_engaged_snooker_pool_non_sky_sports /3000)+1 end

,percentile_prog_engaged_rugby_sky_sports =case when minutes_rugby_sky_sports =-1 then 999 when minutes_rugby_sky_sports =0 then 100 else abs(rank_prog_engaged_rugby_sky_sports /3000)+1 end
,percentile_prog_engaged_rugby_non_sky_sports =case when minutes_rugby_non_sky_sports =-1 then 999 when minutes_rugby_non_sky_sports =0 then 100 else abs(rank_prog_engaged_rugby_non_sky_sports /3000)+1 end

,percentile_prog_engaged_wrestling_sky_sports =case when minutes_wrestling_sky_sports =-1 then 999 when minutes_wrestling_sky_sports =0 then 100 else abs(rank_prog_engaged_wrestling_sky_sports /3000)+1 end
,percentile_prog_engaged_wrestling_non_sky_sports =case when minutes_wrestling_non_sky_sports =-1 then 999 when minutes_wrestling_non_sky_sports =0 then 100 else abs(rank_prog_engaged_wrestling_non_sky_sports /3000)+1 end

,percentile_prog_engaged_wwe =case when minutes_wwe =-1 then 999 when minutes_wwe =0 then 100 else abs(rank_prog_engaged_wwe /3000)+1 end
,percentile_prog_engaged_wwe_sky_sports =case when minutes_wwe_sky_sports =-1 then 999 when minutes_wwe_sky_sports =0 then 100 else abs(rank_prog_engaged_wwe_sky_sports /3000)+1 end
,percentile_prog_engaged_wwe_sbo =case when minutes_wwe_sbo =-1 then 999 when minutes_wwe_sbo =0 then 100 else abs(rank_prog_engaged_wwe_sbo /3000)+1 end
,percentile_prog_engaged_sky_1_or_2 =case when minutes_wwe_sky_1_or_2 =-1 then 999 when minutes_wwe_sky_1_or_2 =0 then 100 else abs(rank_prog_engaged_wwe_sky_1_or_2 /3000)+1 end

,percentile_prog_engaged_darts_sky_sports =case when minutes_darts_sky_sports =-1 then 999 when minutes_darts_sky_sports =0 then 100 else abs(rank_prog_engaged_darts_sky_sports /3000)+1 end
,percentile_prog_engaged_darts_non_sky_sports =case when minutes_darts_non_sky_sports =-1 then 999 when minutes_darts_non_sky_sports =0 then 100 else abs(rank_prog_engaged_darts_non_sky_sports /3000)+1 end

,percentile_prog_engaged_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 999 when minutes_boxing_sky_sports =0 then 100 else abs(rank_prog_engaged_boxing_sky_sports /3000)+1 end
,percentile_prog_engaged_boxing_non_sky_sports =case when minutes_boxing_non_sky_sports =-1 then 999 when minutes_boxing_non_sky_sports =0 then 100 else abs(rank_prog_engaged_boxing_non_sky_sports /3000)+1 end

,percentile_prog_engaged_overall_football =case when minutes_overall_football =-1 then 999 when minutes_overall_football =0 then 100 else abs(rank_prog_engaged_overall_football /3000)+1 end
,percentile_prog_engaged_Sky_Sports_cricket_overall =case when minutes_Sky_Sports_cricket_overall =-1 then 999 when minutes_Sky_Sports_cricket_overall =0 then 100 else abs(rank_prog_engaged_Sky_Sports_cricket_overall /3000)+1 end
,percentile_prog_engaged_non_Sky_Sports_cricket_overall =case when minutes_non_Sky_Sports_cricket_overall =-1 then 999 when minutes_non_Sky_Sports_cricket_overall =0 then 100 else abs(rank_prog_engaged_non_Sky_Sports_cricket_overall /3000)+1 end

,percentile_prog_engaged_niche_sports_sky_sports =case when minutes_niche_sports_sky_sports =-1 then 999 when minutes_niche_sports_sky_sports =0 then 100 else abs(rank_prog_engaged_niche_sports_sky_sports /3000)+1 end
,percentile_prog_engaged_niche_sports_non_Sky_Sports =case when minutes_niche_sports_non_Sky_Sports =-1 then 999 when minutes_niche_sports_non_Sky_Sports =0 then 100 else abs(rank_prog_engaged_niche_sports_non_Sky_Sports /3000)+1 end


from v223_unbundling_viewing_summary_by_account 
;
commit;


----Repeat for Deciles---


alter table v223_unbundling_viewing_summary_by_account add
(decile_minutes_sport integer
,decile_minutes_sport_sky_sports integer
,decile_minutes_sport_sky_sports_news integer

,decile_minutes_sport_terrestrial integer
,decile_minutes_sport_football_terrestrial integer

,decile_minutes_sport_espn integer
,decile_minutes_football_sky_sports integer

,decile_minutes_football_premier_league_sky_sports integer
,decile_minutes_football_premier_league_ESPN_BT integer

,decile_minutes_football_champions_league_sky_sports integer
,decile_minutes_football_champions_league_non_sky_sports integer

,decile_minutes_football_fa_cup_sky_sports integer
,decile_minutes_football_fa_cup_ESPN_BT integer
,decile_minutes_football_fa_cup_Other_Channels integer

,decile_minutes_football_europa_league_ESPN_BT integer
,decile_minutes_football_europa_league_Other_Channels integer

,decile_minutes_football_world_cup_qualifier_sky_sports integer
,decile_minutes_football_world_cup_qualifier_non_sky_sports integer

,decile_minutes_football_international_friendly_sky_sports integer
,decile_minutes_football_international_friendly_non_sky_sports integer

,decile_minutes_football_Capital_One_Cup_sky_sports integer

,decile_minutes_football_La_Liga_sky_sports integer

,decile_minutes_football_football_league_sky_sports integer

,decile_minutes_cricket_ashes_sky_sports integer
,decile_minutes_cricket_ashes_non_sky_sports integer

,decile_minutes_cricket_non_ashes_sky_sports integer
,decile_minutes_cricket_non_ashes_non_sky_sports integer



,decile_minutes_golf_sky_sports integer
,decile_minutes_golf_non_sky_sports integer

,decile_minutes_tennis_sky_sports integer
,decile_minutes_tennis_non_sky_sports integer

,decile_minutes_motor_sport_sky_sports integer
,decile_minutes_motor_sport_non_sky_sports integer

,decile_minutes_F1_sky_sports integer
,decile_minutes_F1_non_sky_sports integer

,decile_minutes_horse_racing_sky_sports integer
,decile_minutes_horse_racing_non_sky_sports integer

,decile_minutes_snooker_pool_sky_sports integer
,decile_minutes_snooker_pool_non_sky_sports integer

,decile_minutes_rugby_sky_sports integer
,decile_minutes_rugby_non_sky_sports integer

,decile_minutes_wrestling_sky_sports integer
,decile_minutes_wrestling_non_sky_sports integer


,decile_minutes_wwe  integer
,decile_minutes_wwe_sky_sports  integer
,decile_minutes_wwe_sbo integer
,decile_minutes_sky_1_or_2 integer

,decile_minutes_darts_sky_sports integer
,decile_minutes_darts_non_sky_sports integer

,decile_minutes_boxing_sky_sports integer
,decile_minutes_boxing_non_sky_sports integer


,decile_minutes_overall_football integer
,decile_minutes_Sky_Sports_cricket_overall integer
,decile_minutes_non_Sky_Sports_cricket_overall integer
,decile_minutes_niche_sports_sky_sports integer
,decile_minutes_niche_sports_non_sky_sports integer
)
;


update v223_unbundling_viewing_summary_by_account 
set  
decile_minutes_sport =case when minutes_sport =-1 then 99 when minutes_sport =0 then 10 else abs(rank_minutes_sport /30000)+1 end
,decile_minutes_sport_sky_sports =case when minutes_sport_sky_sports =-1 then 99 when minutes_sport_sky_sports =0 then 10 else abs(rank_minutes_sport_sky_sports /30000)+1 end
,decile_minutes_sport_sky_sports_news =case when minutes_sport_sky_sports_news =-1 then 99 when minutes_sport_sky_sports_news =0 then 10 else abs(rank_minutes_sport_sky_sports_news /30000)+1 end

,decile_minutes_sport_terrestrial =case when minutes_sport_terrestrial =-1 then 99 when minutes_sport_terrestrial =0 then 10 else abs(rank_minutes_sport_terrestrial /30000)+1 end
,decile_minutes_sport_football_terrestrial =case when minutes_sport_football_terrestrial =-1 then 99 when minutes_sport_football_terrestrial =0 then 10 else abs(rank_minutes_sport_football_terrestrial /30000)+1 end


,decile_minutes_sport_espn =case when minutes_sport_espn =-1 then 99 when minutes_sport_espn =0 then 10 else abs(rank_minutes_sport_espn /30000)+1 end

,decile_minutes_football_sky_sports =case when minutes_football_sky_sports =-1 then 99 when minutes_football_sky_sports =0 then 10 else abs(rank_minutes_football_sky_sports /30000)+1 end

,decile_minutes_football_premier_league_sky_sports =case when minutes_football_premier_league_sky_sports =-1 then 99 when minutes_football_premier_league_sky_sports =0 then 10 else abs(rank_minutes_football_premier_league_sky_sports /30000)+1 end
,decile_minutes_football_premier_league_ESPN_BT =case when minutes_football_premier_league_ESPN_BT =-1 then 99 when minutes_football_premier_league_ESPN_BT =0 then 10 else abs(rank_minutes_football_premier_league_ESPN_BT /30000)+1 end

,decile_minutes_football_champions_league_sky_sports =case when minutes_football_champions_league_sky_sports =-1 then 99 when minutes_football_champions_league_sky_sports =0 then 10 else abs(rank_minutes_football_champions_league_sky_sports /30000)+1 end
,decile_minutes_football_champions_league_non_sky_sports =case when minutes_football_champions_league_non_sky_sports =-1 then 99 when minutes_football_champions_league_non_sky_sports =0 then 10 else abs(rank_minutes_football_champions_league_non_sky_sports /30000)+1 end

--,decile_minutes_football_fa_cup_sky_sports =case when minutes_football_fa_cup_sky_sports =-1 then 99 when minutes_football_fa_cup_sky_sports =0 then 10 else abs(rank_minutes_football_fa_cup_sky_sports /30000)+1 end
,decile_minutes_football_europa_league_ESPN_BT =case when minutes_football_europa_league_ESPN_BT =-1 then 99 when minutes_football_europa_league_ESPN_BT =0 then 10 else abs(rank_minutes_football_europa_league_ESPN_BT /30000)+1 end
,decile_minutes_football_europa_league_Other_Channels =case when minutes_football_europa_league_Other_Channels =-1 then 99 when minutes_football_europa_league_Other_Channels =0 then 10 else abs(rank_minutes_football_europa_league_Other_Channels /30000)+1 end

,decile_minutes_football_fa_cup_ESPN_BT =case when minutes_football_fa_cup_ESPN_BT =-1 then 99 when minutes_football_fa_cup_ESPN_BT =0 then 10 else abs(rank_minutes_football_fa_cup_ESPN_BT /30000)+1 end
,decile_minutes_football_fa_cup_Other_Channels =case when minutes_football_fa_cup_Other_Channels =-1 then 99 when minutes_football_fa_cup_Other_Channels =0 then 10 else abs(rank_minutes_football_fa_cup_Other_Channels /30000)+1 end


,decile_minutes_football_world_cup_qualifier_sky_sports =case when minutes_football_world_cup_qualifier_sky_sports =-1 then 99 when minutes_football_world_cup_qualifier_sky_sports =0 then 10 else abs(rank_minutes_football_world_cup_qualifier_sky_sports /30000)+1 end
,decile_minutes_football_world_cup_qualifier_non_sky_sports =case when minutes_football_world_cup_qualifier_non_sky_sports =-1 then 99 when minutes_football_world_cup_qualifier_non_sky_sports =0 then 10 else abs(rank_minutes_football_world_cup_qualifier_non_sky_sports /30000)+1 end

,decile_minutes_football_international_friendly_sky_sports =case when minutes_football_international_friendly_sky_sports =-1 then 99 when minutes_football_international_friendly_sky_sports =0 then 10 else abs(rank_minutes_football_international_friendly_sky_sports /30000)+1 end
,decile_minutes_football_international_friendly_non_sky_sports =case when minutes_football_international_friendly_non_sky_sports =-1 then 99 when minutes_football_international_friendly_non_sky_sports =0 then 10 else abs(rank_minutes_football_international_friendly_non_sky_sports /30000)+1 end

,decile_minutes_football_Capital_One_Cup_sky_sports =case when minutes_football_Capital_One_Cup_sky_sports =-1 then 99 when minutes_football_Capital_One_Cup_sky_sports =0 then 10 else abs(rank_minutes_football_Capital_One_Cup_sky_sports /30000)+1 end

,decile_minutes_football_La_Liga_sky_sports =case when minutes_football_La_Liga_sky_sports =-1 then 99 when minutes_football_La_Liga_sky_sports =0 then 10 else abs(rank_minutes_football_La_Liga_sky_sports /30000)+1 end

,decile_minutes_football_football_league_sky_sports =case when minutes_football_football_league_sky_sports =-1 then 99 when minutes_football_football_league_sky_sports =0 then 10 else abs(rank_minutes_football_football_league_sky_sports /30000)+1 end

,decile_minutes_cricket_ashes_sky_sports =case when minutes_cricket_ashes_sky_sports =-1 then 99 when minutes_cricket_ashes_sky_sports =0 then 10 else abs(rank_minutes_cricket_ashes_sky_sports /30000)+1 end
,decile_minutes_cricket_ashes_non_sky_sports =case when minutes_cricket_ashes_non_sky_sports =-1 then 99 when minutes_cricket_ashes_non_sky_sports =0 then 10 else abs(rank_minutes_cricket_ashes_non_sky_sports /30000)+1 end

,decile_minutes_cricket_non_ashes_sky_sports =case when minutes_cricket_non_ashes_sky_sports =-1 then 99 when minutes_cricket_non_ashes_sky_sports =0 then 10 else abs(rank_minutes_cricket_non_ashes_sky_sports /30000)+1 end
,decile_minutes_cricket_non_ashes_non_sky_sports =case when minutes_cricket_non_ashes_non_sky_sports =-1 then 99 when minutes_cricket_non_ashes_non_sky_sports =0 then 10 else abs(rank_minutes_cricket_non_ashes_non_sky_sports /30000)+1 end



,decile_minutes_golf_sky_sports =case when minutes_golf_sky_sports =-1 then 99 when minutes_golf_sky_sports =0 then 10 else abs(rank_minutes_golf_sky_sports /30000)+1 end
,decile_minutes_golf_non_sky_sports =case when minutes_golf_non_sky_sports =-1 then 99 when minutes_golf_non_sky_sports =0 then 10 else abs(rank_minutes_golf_non_sky_sports /30000)+1 end

,decile_minutes_tennis_sky_sports =case when minutes_tennis_sky_sports =-1 then 99 when minutes_tennis_sky_sports =0 then 10 else abs(rank_minutes_tennis_sky_sports /30000)+1 end
,decile_minutes_tennis_non_sky_sports =case when minutes_tennis_non_sky_sports =-1 then 99 when minutes_tennis_non_sky_sports =0 then 10 else abs(rank_minutes_tennis_non_sky_sports /30000)+1 end

,decile_minutes_motor_sport_sky_sports =case when minutes_motor_sport_sky_sports =-1 then 99 when minutes_motor_sport_sky_sports =0 then 10 else abs(rank_minutes_motor_sport_sky_sports /30000)+1 end
,decile_minutes_motor_sport_non_sky_sports =case when minutes_motor_sport_non_sky_sports =-1 then 99 when minutes_motor_sport_non_sky_sports =0 then 10 else abs(rank_minutes_motor_sport_non_sky_sports /30000)+1 end

,decile_minutes_F1_sky_sports =case when minutes_F1_sky_sports =-1 then 99 when minutes_F1_sky_sports =0 then 10 else abs(rank_minutes_F1_sky_sports /30000)+1 end
,decile_minutes_F1_non_sky_sports =case when minutes_F1_non_sky_sports =-1 then 99 when minutes_F1_non_sky_sports =0 then 10 else abs(rank_minutes_F1_non_sky_sports /30000)+1 end

,decile_minutes_horse_racing_sky_sports =case when minutes_horse_racing_sky_sports =-1 then 99 when minutes_horse_racing_sky_sports =0 then 10 else abs(rank_minutes_horse_racing_sky_sports /30000)+1 end
,decile_minutes_horse_racing_non_sky_sports =case when minutes_horse_racing_non_sky_sports =-1 then 99 when minutes_horse_racing_non_sky_sports =0 then 10 else abs(rank_minutes_horse_racing_non_sky_sports /30000)+1 end

,decile_minutes_snooker_pool_sky_sports =case when minutes_snooker_pool_sky_sports =-1 then 99 when minutes_snooker_pool_sky_sports =0 then 10 else abs(rank_minutes_snooker_pool_sky_sports /30000)+1 end
,decile_minutes_snooker_pool_non_sky_sports =case when minutes_snooker_pool_non_sky_sports =-1 then 99 when minutes_snooker_pool_non_sky_sports =0 then 10 else abs(rank_minutes_snooker_pool_non_sky_sports /30000)+1 end

,decile_minutes_rugby_sky_sports =case when minutes_rugby_sky_sports =-1 then 99 when minutes_rugby_sky_sports =0 then 10 else abs(rank_minutes_rugby_sky_sports /30000)+1 end
,decile_minutes_rugby_non_sky_sports =case when minutes_rugby_non_sky_sports =-1 then 99 when minutes_rugby_non_sky_sports =0 then 10 else abs(rank_minutes_rugby_non_sky_sports /30000)+1 end

,decile_minutes_wrestling_sky_sports =case when minutes_wrestling_sky_sports =-1 then 99 when minutes_wrestling_sky_sports =0 then 10 else abs(rank_minutes_wrestling_sky_sports /30000)+1 end
,decile_minutes_wrestling_non_sky_sports =case when minutes_wrestling_non_sky_sports =-1 then 99 when minutes_wrestling_non_sky_sports =0 then 10 else abs(rank_minutes_wrestling_non_sky_sports /30000)+1 end

,decile_minutes_wwe =case when minutes_wwe =-1 then 99 when minutes_wwe =0 then 10 else abs(rank_minutes_wwe /30000)+1 end
,decile_minutes_wwe_sky_sports =case when minutes_wwe_sky_sports =-1 then 99 when minutes_wwe_sky_sports =0 then 10 else abs(rank_minutes_wwe_sky_sports /30000)+1 end
,decile_minutes_wwe_sbo =case when minutes_wwe_sbo =-1 then 99 when minutes_wwe_sbo =0 then 10 else abs(rank_minutes_wwe_sbo /30000)+1 end
,decile_minutes_sky_1_or_2 =case when minutes_wwe_sky_1_or_2 =-1 then 99 when minutes_wwe_sky_1_or_2 =0 then 10 else abs(rank_minutes_wwe_sky_1_or_2 /30000)+1 end

,decile_minutes_darts_sky_sports =case when minutes_darts_sky_sports =-1 then 99 when minutes_darts_sky_sports =0 then 10 else abs(rank_minutes_darts_sky_sports /30000)+1 end
,decile_minutes_darts_non_sky_sports =case when minutes_darts_non_sky_sports =-1 then 99 when minutes_darts_non_sky_sports =0 then 10 else abs(rank_minutes_darts_non_sky_sports /30000)+1 end

,decile_minutes_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 99 when minutes_boxing_sky_sports =0 then 10 else abs(rank_minutes_boxing_sky_sports /30000)+1 end
,decile_minutes_boxing_non_sky_sports =case when minutes_boxing_non_sky_sports =-1 then 99 when minutes_boxing_non_sky_sports =0 then 10 else abs(rank_minutes_boxing_non_sky_sports /30000)+1 end

,decile_minutes_overall_football =case when minutes_overall_football =-1 then 99 when minutes_overall_football =0 then 10 else abs(rank_minutes_overall_football /30000)+1 end
,decile_minutes_Sky_Sports_cricket_overall =case when minutes_Sky_Sports_cricket_overall =-1 then 99 when minutes_Sky_Sports_cricket_overall =0 then 10 else abs(rank_minutes_Sky_Sports_cricket_overall /30000)+1 end
,decile_minutes_non_Sky_Sports_cricket_overall =case when minutes_non_Sky_Sports_cricket_overall =-1 then 99 when minutes_non_Sky_Sports_cricket_overall =0 then 10 else abs(rank_minutes_non_Sky_Sports_cricket_overall /30000)+1 end

,decile_minutes_niche_sports_sky_sports =case when minutes_niche_sports_sky_sports =-1 then 99 when minutes_niche_sports_sky_sports =0 then 10 else abs(rank_minutes_niche_sports_sky_sports /30000)+1 end
,decile_minutes_niche_sports_non_Sky_Sports =case when minutes_niche_sports_non_Sky_Sports =-1 then 99 when minutes_niche_sports_non_Sky_Sports =0 then 10 else abs(rank_minutes_niche_sports_non_Sky_Sports /30000)+1 end






from v223_unbundling_viewing_summary_by_account 
;
commit;


---Repeat with decile splits for 3min+ progs and Engaged Progs----

alter table v223_unbundling_viewing_summary_by_account add
(decile_prog_3min_plus_sport integer
,decile_prog_3min_plus_sport_sky_sports integer
,decile_prog_3min_plus_sport_sky_sports_news integer

,decile_prog_3min_plus_sport_terrestrial integer
,decile_prog_3min_plus_sport_football_terrestrial integer

,decile_prog_3min_plus_sport_espn integer
,decile_prog_3min_plus_football_sky_sports integer

,decile_prog_3min_plus_football_premier_league_sky_sports integer
,decile_prog_3min_plus_football_premier_league_ESPN_BT integer

,decile_prog_3min_plus_football_champions_league_sky_sports integer
,decile_prog_3min_plus_football_champions_league_non_sky_sports integer

,decile_prog_3min_plus_football_fa_cup_sky_sports integer
,decile_prog_3min_plus_football_fa_cup_ESPN_BT integer
,decile_prog_3min_plus_football_fa_cup_Other_Channels integer

,decile_prog_3min_plus_football_europa_league_ESPN_BT integer
,decile_prog_3min_plus_football_europa_league_Other_Channels integer

,decile_prog_3min_plus_football_world_cup_qualifier_sky_sports integer
,decile_prog_3min_plus_football_world_cup_qualifier_non_sky_sports integer

,decile_prog_3min_plus_football_international_friendly_sky_sports integer
,decile_prog_3min_plus_football_international_friendly_non_sky_sports integer

,decile_prog_3min_plus_football_Capital_One_Cup_sky_sports integer

,decile_prog_3min_plus_football_La_Liga_sky_sports integer

,decile_prog_3min_plus_football_football_league_sky_sports integer

,decile_prog_3min_plus_cricket_ashes_sky_sports integer
,decile_prog_3min_plus_cricket_ashes_non_sky_sports integer

,decile_prog_3min_plus_cricket_non_ashes_sky_sports integer
,decile_prog_3min_plus_cricket_non_ashes_non_sky_sports integer



,decile_prog_3min_plus_golf_sky_sports integer
,decile_prog_3min_plus_golf_non_sky_sports integer

,decile_prog_3min_plus_tennis_sky_sports integer
,decile_prog_3min_plus_tennis_non_sky_sports integer

,decile_prog_3min_plus_motor_sport_sky_sports integer
,decile_prog_3min_plus_motor_sport_non_sky_sports integer

,decile_prog_3min_plus_F1_sky_sports integer
,decile_prog_3min_plus_F1_non_sky_sports integer

,decile_prog_3min_plus_horse_racing_sky_sports integer
,decile_prog_3min_plus_horse_racing_non_sky_sports integer

,decile_prog_3min_plus_snooker_pool_sky_sports integer
,decile_prog_3min_plus_snooker_pool_non_sky_sports integer

,decile_prog_3min_plus_rugby_sky_sports integer
,decile_prog_3min_plus_rugby_non_sky_sports integer

,decile_prog_3min_plus_wrestling_sky_sports integer
,decile_prog_3min_plus_wrestling_non_sky_sports integer


,decile_prog_3min_plus_wwe  integer
,decile_prog_3min_plus_wwe_sky_sports  integer
,decile_prog_3min_plus_wwe_sbo integer
,decile_prog_3min_plus_sky_1_or_2 integer

,decile_prog_3min_plus_darts_sky_sports integer
,decile_prog_3min_plus_darts_non_sky_sports integer

,decile_prog_3min_plus_boxing_sky_sports integer
,decile_prog_3min_plus_boxing_non_sky_sports integer

,decile_prog_3min_plus_overall_football integer
,decile_prog_3min_plus_Sky_Sports_cricket_overall integer
,decile_prog_3min_plus_non_Sky_Sports_cricket_overall integer
,decile_prog_3min_plus_niche_sports_sky_sports integer
,decile_prog_3min_plus_niche_sports_non_sky_sports integer

)
;


update v223_unbundling_viewing_summary_by_account 
set  
decile_prog_3min_plus_sport =case when minutes_sport =-1 then 99 when minutes_sport =0 then 10 else abs(rank_prog_3min_plus_sport /30000)+1 end
,decile_prog_3min_plus_sport_sky_sports =case when minutes_sport_sky_sports =-1 then 99 when minutes_sport_sky_sports =0 then 10 else abs(rank_prog_3min_plus_sport_sky_sports /30000)+1 end
,decile_prog_3min_plus_sport_sky_sports_news =case when minutes_sport_sky_sports_news =-1 then 99 when minutes_sport_sky_sports_news =0 then 10 else abs(rank_prog_3min_plus_sport_sky_sports_news /30000)+1 end

,decile_prog_3min_plus_sport_terrestrial =case when minutes_sport_terrestrial =-1 then 99 when minutes_sport_terrestrial =0 then 10 else abs(rank_prog_3min_plus_sport_terrestrial /30000)+1 end
,decile_prog_3min_plus_sport_football_terrestrial =case when minutes_sport_football_terrestrial =-1 then 99 when minutes_sport_football_terrestrial =0 then 10 else abs(rank_prog_3min_plus_sport_football_terrestrial /30000)+1 end


,decile_prog_3min_plus_sport_espn =case when minutes_sport_espn =-1 then 99 when minutes_sport_espn =0 then 10 else abs(rank_prog_3min_plus_sport_espn /30000)+1 end

,decile_prog_3min_plus_football_sky_sports =case when minutes_football_sky_sports =-1 then 99 when minutes_football_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_sky_sports /30000)+1 end

,decile_prog_3min_plus_football_premier_league_sky_sports =case when minutes_football_premier_league_sky_sports =-1 then 99 when minutes_football_premier_league_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_premier_league_sky_sports /30000)+1 end
,decile_prog_3min_plus_football_premier_league_ESPN_BT =case when minutes_football_premier_league_ESPN_BT =-1 then 99 when minutes_football_premier_league_ESPN_BT =0 then 10 else abs(rank_prog_3min_plus_football_premier_league_ESPN_BT /30000)+1 end

,decile_prog_3min_plus_football_champions_league_sky_sports =case when minutes_football_champions_league_sky_sports =-1 then 99 when minutes_football_champions_league_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_champions_league_sky_sports /30000)+1 end
,decile_prog_3min_plus_football_champions_league_non_sky_sports =case when minutes_football_champions_league_non_sky_sports =-1 then 99 when minutes_football_champions_league_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_champions_league_non_sky_sports /30000)+1 end

--,decile_prog_3min_plus_football_fa_cup_sky_sports =case when minutes_football_fa_cup_sky_sports =-1 then 99 when minutes_football_fa_cup_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_fa_cup_sky_sports /30000)+1 end
,decile_prog_3min_plus_football_europa_league_ESPN_BT =case when minutes_football_europa_league_ESPN_BT =-1 then 99 when minutes_football_europa_league_ESPN_BT =0 then 10 else abs(rank_prog_3min_plus_football_europa_league_ESPN_BT /30000)+1 end
,decile_prog_3min_plus_football_europa_league_Other_Channels =case when minutes_football_europa_league_Other_Channels =-1 then 99 when minutes_football_europa_league_Other_Channels =0 then 10 else abs(rank_prog_3min_plus_football_europa_league_Other_Channels /30000)+1 end

,decile_prog_3min_plus_football_fa_cup_ESPN_BT =case when minutes_football_fa_cup_ESPN_BT =-1 then 99 when minutes_football_fa_cup_ESPN_BT =0 then 10 else abs(rank_prog_3min_plus_football_fa_cup_ESPN_BT /30000)+1 end
,decile_prog_3min_plus_football_fa_cup_Other_Channels =case when minutes_football_fa_cup_Other_Channels =-1 then 99 when minutes_football_fa_cup_Other_Channels =0 then 10 else abs(rank_prog_3min_plus_football_fa_cup_Other_Channels /30000)+1 end


,decile_prog_3min_plus_football_world_cup_qualifier_sky_sports =case when minutes_football_world_cup_qualifier_sky_sports =-1 then 99 when minutes_football_world_cup_qualifier_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_world_cup_qualifier_sky_sports /30000)+1 end
,decile_prog_3min_plus_football_world_cup_qualifier_non_sky_sports =case when minutes_football_world_cup_qualifier_non_sky_sports =-1 then 99 when minutes_football_world_cup_qualifier_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_world_cup_qualifier_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_football_international_friendly_sky_sports =case when minutes_football_international_friendly_sky_sports =-1 then 99 when minutes_football_international_friendly_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_international_friendly_sky_sports /30000)+1 end
,decile_prog_3min_plus_football_international_friendly_non_sky_sports =case when minutes_football_international_friendly_non_sky_sports =-1 then 99 when minutes_football_international_friendly_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_international_friendly_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_football_Capital_One_Cup_sky_sports =case when minutes_football_Capital_One_Cup_sky_sports =-1 then 99 when minutes_football_Capital_One_Cup_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_Capital_One_Cup_sky_sports /30000)+1 end

,decile_prog_3min_plus_football_La_Liga_sky_sports =case when minutes_football_La_Liga_sky_sports =-1 then 99 when minutes_football_La_Liga_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_La_Liga_sky_sports /30000)+1 end

,decile_prog_3min_plus_football_football_league_sky_sports =case when minutes_football_football_league_sky_sports =-1 then 99 when minutes_football_football_league_sky_sports =0 then 10 else abs(rank_prog_3min_plus_football_football_league_sky_sports /30000)+1 end

,decile_prog_3min_plus_cricket_ashes_sky_sports =case when minutes_cricket_ashes_sky_sports =-1 then 99 when minutes_cricket_ashes_sky_sports =0 then 10 else abs(rank_prog_3min_plus_cricket_ashes_sky_sports /30000)+1 end
,decile_prog_3min_plus_cricket_ashes_non_sky_sports =case when minutes_cricket_ashes_non_sky_sports =-1 then 99 when minutes_cricket_ashes_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_cricket_ashes_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_cricket_non_ashes_sky_sports =case when minutes_cricket_non_ashes_sky_sports =-1 then 99 when minutes_cricket_non_ashes_sky_sports =0 then 10 else abs(rank_prog_3min_plus_cricket_non_ashes_sky_sports /30000)+1 end
,decile_prog_3min_plus_cricket_non_ashes_non_sky_sports =case when minutes_cricket_non_ashes_non_sky_sports =-1 then 99 when minutes_cricket_non_ashes_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_cricket_non_ashes_non_sky_sports /30000)+1 end



,decile_prog_3min_plus_golf_sky_sports =case when minutes_golf_sky_sports =-1 then 99 when minutes_golf_sky_sports =0 then 10 else abs(rank_prog_3min_plus_golf_sky_sports /30000)+1 end
,decile_prog_3min_plus_golf_non_sky_sports =case when minutes_golf_non_sky_sports =-1 then 99 when minutes_golf_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_golf_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_tennis_sky_sports =case when minutes_tennis_sky_sports =-1 then 99 when minutes_tennis_sky_sports =0 then 10 else abs(rank_prog_3min_plus_tennis_sky_sports /30000)+1 end
,decile_prog_3min_plus_tennis_non_sky_sports =case when minutes_tennis_non_sky_sports =-1 then 99 when minutes_tennis_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_tennis_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_motor_sport_sky_sports =case when minutes_motor_sport_sky_sports =-1 then 99 when minutes_motor_sport_sky_sports =0 then 10 else abs(rank_prog_3min_plus_motor_sport_sky_sports /30000)+1 end
,decile_prog_3min_plus_motor_sport_non_sky_sports =case when minutes_motor_sport_non_sky_sports =-1 then 99 when minutes_motor_sport_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_motor_sport_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_F1_sky_sports =case when minutes_F1_sky_sports =-1 then 99 when minutes_F1_sky_sports =0 then 10 else abs(rank_prog_3min_plus_F1_sky_sports /30000)+1 end
,decile_prog_3min_plus_F1_non_sky_sports =case when minutes_F1_non_sky_sports =-1 then 99 when minutes_F1_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_F1_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_horse_racing_sky_sports =case when minutes_horse_racing_sky_sports =-1 then 99 when minutes_horse_racing_sky_sports =0 then 10 else abs(rank_prog_3min_plus_horse_racing_sky_sports /30000)+1 end
,decile_prog_3min_plus_horse_racing_non_sky_sports =case when minutes_horse_racing_non_sky_sports =-1 then 99 when minutes_horse_racing_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_horse_racing_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_snooker_pool_sky_sports =case when minutes_snooker_pool_sky_sports =-1 then 99 when minutes_snooker_pool_sky_sports =0 then 10 else abs(rank_prog_3min_plus_snooker_pool_sky_sports /30000)+1 end
,decile_prog_3min_plus_snooker_pool_non_sky_sports =case when minutes_snooker_pool_non_sky_sports =-1 then 99 when minutes_snooker_pool_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_snooker_pool_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_rugby_sky_sports =case when minutes_rugby_sky_sports =-1 then 99 when minutes_rugby_sky_sports =0 then 10 else abs(rank_prog_3min_plus_rugby_sky_sports /30000)+1 end
,decile_prog_3min_plus_rugby_non_sky_sports =case when minutes_rugby_non_sky_sports =-1 then 99 when minutes_rugby_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_rugby_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_wrestling_sky_sports =case when minutes_wrestling_sky_sports =-1 then 99 when minutes_wrestling_sky_sports =0 then 10 else abs(rank_prog_3min_plus_wrestling_sky_sports /30000)+1 end
,decile_prog_3min_plus_wrestling_non_sky_sports =case when minutes_wrestling_non_sky_sports =-1 then 99 when minutes_wrestling_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_wrestling_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_wwe =case when minutes_wwe =-1 then 99 when minutes_wwe =0 then 10 else abs(rank_prog_3min_plus_wwe /30000)+1 end
,decile_prog_3min_plus_wwe_sky_sports =case when minutes_wwe_sky_sports =-1 then 99 when minutes_wwe_sky_sports =0 then 10 else abs(rank_prog_3min_plus_wwe_sky_sports /30000)+1 end
,decile_prog_3min_plus_wwe_sbo =case when minutes_wwe_sbo =-1 then 99 when minutes_wwe_sbo =0 then 10 else abs(rank_prog_3min_plus_wwe_sbo /30000)+1 end
,decile_prog_3min_plus_sky_1_or_2 =case when minutes_wwe_sky_1_or_2 =-1 then 99 when minutes_wwe_sky_1_or_2 =0 then 10 else abs(rank_prog_3min_plus_wwe_sky_1_or_2 /30000)+1 end

,decile_prog_3min_plus_darts_sky_sports =case when minutes_darts_sky_sports =-1 then 99 when minutes_darts_sky_sports =0 then 10 else abs(rank_prog_3min_plus_darts_sky_sports /30000)+1 end
,decile_prog_3min_plus_darts_non_sky_sports =case when minutes_darts_non_sky_sports =-1 then 99 when minutes_darts_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_darts_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 99 when minutes_boxing_sky_sports =0 then 10 else abs(rank_prog_3min_plus_boxing_sky_sports /30000)+1 end
,decile_prog_3min_plus_boxing_non_sky_sports =case when minutes_boxing_non_sky_sports =-1 then 99 when minutes_boxing_non_sky_sports =0 then 10 else abs(rank_prog_3min_plus_boxing_non_sky_sports /30000)+1 end

,decile_prog_3min_plus_overall_football =case when minutes_overall_football =-1 then 99 when minutes_overall_football =0 then 10 else abs(rank_prog_3min_plus_overall_football /30000)+1 end
,decile_prog_3min_plus_Sky_Sports_cricket_overall =case when minutes_Sky_Sports_cricket_overall =-1 then 99 when minutes_Sky_Sports_cricket_overall =0 then 10 else abs(rank_prog_3min_plus_Sky_Sports_cricket_overall /30000)+1 end
,decile_prog_3min_plus_non_Sky_Sports_cricket_overall =case when minutes_non_Sky_Sports_cricket_overall =-1 then 99 when minutes_non_Sky_Sports_cricket_overall =0 then 10 else abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall /30000)+1 end

,decile_prog_3min_plus_niche_sports_sky_sports =case when minutes_niche_sports_sky_sports =-1 then 99 when minutes_niche_sports_sky_sports =0 then 10 else abs(rank_prog_3min_plus_niche_sports_sky_sports /30000)+1 end
,decile_prog_3min_plus_niche_sports_non_Sky_Sports =case when minutes_niche_sports_non_Sky_Sports =-1 then 99 when minutes_niche_sports_non_Sky_Sports =0 then 10 else abs(rank_prog_3min_plus_niche_sports_non_Sky_Sports /30000)+1 end


from v223_unbundling_viewing_summary_by_account 
;
commit;



----Repeat for Engaged Programmes-----


alter table v223_unbundling_viewing_summary_by_account add
(decile_prog_engaged_sport integer
,decile_prog_engaged_sport_sky_sports integer
,decile_prog_engaged_sport_sky_sports_news integer

,decile_prog_engaged_sport_terrestrial integer
,decile_prog_engaged_sport_football_terrestrial integer

,decile_prog_engaged_sport_espn integer
,decile_prog_engaged_football_sky_sports integer

,decile_prog_engaged_football_premier_league_sky_sports integer
,decile_prog_engaged_football_premier_league_ESPN_BT integer

,decile_prog_engaged_football_champions_league_sky_sports integer
,decile_prog_engaged_football_champions_league_non_sky_sports integer

,decile_prog_engaged_football_fa_cup_sky_sports integer
,decile_prog_engaged_football_fa_cup_ESPN_BT integer
,decile_prog_engaged_football_fa_cup_Other_Channels integer

,decile_prog_engaged_football_europa_league_ESPN_BT integer
,decile_prog_engaged_football_europa_league_Other_Channels integer

,decile_prog_engaged_football_world_cup_qualifier_sky_sports integer
,decile_prog_engaged_football_world_cup_qualifier_non_sky_sports integer

,decile_prog_engaged_football_international_friendly_sky_sports integer
,decile_prog_engaged_football_international_friendly_non_sky_sports integer

,decile_prog_engaged_football_Capital_One_Cup_sky_sports integer

,decile_prog_engaged_football_La_Liga_sky_sports integer

,decile_prog_engaged_football_football_league_sky_sports integer

,decile_prog_engaged_cricket_ashes_sky_sports integer
,decile_prog_engaged_cricket_ashes_non_sky_sports integer

,decile_prog_engaged_cricket_non_ashes_sky_sports integer
,decile_prog_engaged_cricket_non_ashes_non_sky_sports integer



,decile_prog_engaged_golf_sky_sports integer
,decile_prog_engaged_golf_non_sky_sports integer

,decile_prog_engaged_tennis_sky_sports integer
,decile_prog_engaged_tennis_non_sky_sports integer

,decile_prog_engaged_motor_sport_sky_sports integer
,decile_prog_engaged_motor_sport_non_sky_sports integer

,decile_prog_engaged_F1_sky_sports integer
,decile_prog_engaged_F1_non_sky_sports integer

,decile_prog_engaged_horse_racing_sky_sports integer
,decile_prog_engaged_horse_racing_non_sky_sports integer

,decile_prog_engaged_snooker_pool_sky_sports integer
,decile_prog_engaged_snooker_pool_non_sky_sports integer

,decile_prog_engaged_rugby_sky_sports integer
,decile_prog_engaged_rugby_non_sky_sports integer

,decile_prog_engaged_wrestling_sky_sports integer
,decile_prog_engaged_wrestling_non_sky_sports integer


,decile_prog_engaged_wwe  integer
,decile_prog_engaged_wwe_sky_sports  integer
,decile_prog_engaged_wwe_sbo integer
,decile_prog_engaged_sky_1_or_2 integer

,decile_prog_engaged_darts_sky_sports integer
,decile_prog_engaged_darts_non_sky_sports integer

,decile_prog_engaged_boxing_sky_sports integer
,decile_prog_engaged_boxing_non_sky_sports integer


,decile_prog_engaged_overall_football integer
,decile_prog_engaged_Sky_Sports_cricket_overall integer
,decile_prog_engaged_non_Sky_Sports_cricket_overall integer
,decile_prog_engaged_niche_sports_sky_sports integer
,decile_prog_engaged_niche_sports_non_sky_sports integer

)
;


update v223_unbundling_viewing_summary_by_account 
set  
decile_prog_engaged_sport =case when minutes_sport =-1 then 99 when minutes_sport =0 then 10 else abs(rank_prog_engaged_sport /30000)+1 end
,decile_prog_engaged_sport_sky_sports =case when minutes_sport_sky_sports =-1 then 99 when minutes_sport_sky_sports =0 then 10 else abs(rank_prog_engaged_sport_sky_sports /30000)+1 end
,decile_prog_engaged_sport_sky_sports_news =case when minutes_sport_sky_sports_news =-1 then 99 when minutes_sport_sky_sports_news =0 then 10 else abs(rank_prog_engaged_sport_sky_sports_news /30000)+1 end

,decile_prog_engaged_sport_terrestrial =case when minutes_sport_terrestrial =-1 then 99 when minutes_sport_terrestrial =0 then 10 else abs(rank_prog_engaged_sport_terrestrial /30000)+1 end
,decile_prog_engaged_sport_football_terrestrial =case when minutes_sport_football_terrestrial =-1 then 99 when minutes_sport_football_terrestrial =0 then 10 else abs(rank_prog_engaged_sport_football_terrestrial /30000)+1 end


,decile_prog_engaged_sport_espn =case when minutes_sport_espn =-1 then 99 when minutes_sport_espn =0 then 10 else abs(rank_prog_engaged_sport_espn /30000)+1 end

,decile_prog_engaged_football_sky_sports =case when minutes_football_sky_sports =-1 then 99 when minutes_football_sky_sports =0 then 10 else abs(rank_prog_engaged_football_sky_sports /30000)+1 end

,decile_prog_engaged_football_premier_league_sky_sports =case when minutes_football_premier_league_sky_sports =-1 then 99 when minutes_football_premier_league_sky_sports =0 then 10 else abs(rank_prog_engaged_football_premier_league_sky_sports /30000)+1 end
,decile_prog_engaged_football_premier_league_ESPN_BT =case when minutes_football_premier_league_ESPN_BT =-1 then 99 when minutes_football_premier_league_ESPN_BT =0 then 10 else abs(rank_prog_engaged_football_premier_league_ESPN_BT /30000)+1 end

,decile_prog_engaged_football_champions_league_sky_sports =case when minutes_football_champions_league_sky_sports =-1 then 99 when minutes_football_champions_league_sky_sports =0 then 10 else abs(rank_prog_engaged_football_champions_league_sky_sports /30000)+1 end
,decile_prog_engaged_football_champions_league_non_sky_sports =case when minutes_football_champions_league_non_sky_sports =-1 then 99 when minutes_football_champions_league_non_sky_sports =0 then 10 else abs(rank_prog_engaged_football_champions_league_non_sky_sports /30000)+1 end

--,decile_prog_engaged_football_fa_cup_sky_sports =case when minutes_football_fa_cup_sky_sports =-1 then 99 when minutes_football_fa_cup_sky_sports =0 then 10 else abs(rank_prog_engaged_football_fa_cup_sky_sports /30000)+1 end
,decile_prog_engaged_football_europa_league_ESPN_BT =case when minutes_football_europa_league_ESPN_BT =-1 then 99 when minutes_football_europa_league_ESPN_BT =0 then 10 else abs(rank_prog_engaged_football_europa_league_ESPN_BT /30000)+1 end
,decile_prog_engaged_football_europa_league_Other_Channels =case when minutes_football_europa_league_Other_Channels =-1 then 99 when minutes_football_europa_league_Other_Channels =0 then 10 else abs(rank_prog_engaged_football_europa_league_Other_Channels /30000)+1 end

,decile_prog_engaged_football_fa_cup_ESPN_BT =case when minutes_football_fa_cup_ESPN_BT =-1 then 99 when minutes_football_fa_cup_ESPN_BT =0 then 10 else abs(rank_prog_engaged_football_fa_cup_ESPN_BT /30000)+1 end
,decile_prog_engaged_football_fa_cup_Other_Channels =case when minutes_football_fa_cup_Other_Channels =-1 then 99 when minutes_football_fa_cup_Other_Channels =0 then 10 else abs(rank_prog_engaged_football_fa_cup_Other_Channels /30000)+1 end


,decile_prog_engaged_football_world_cup_qualifier_sky_sports =case when minutes_football_world_cup_qualifier_sky_sports =-1 then 99 when minutes_football_world_cup_qualifier_sky_sports =0 then 10 else abs(rank_prog_engaged_football_world_cup_qualifier_sky_sports /30000)+1 end
,decile_prog_engaged_football_world_cup_qualifier_non_sky_sports =case when minutes_football_world_cup_qualifier_non_sky_sports =-1 then 99 when minutes_football_world_cup_qualifier_non_sky_sports =0 then 10 else abs(rank_prog_engaged_football_world_cup_qualifier_non_sky_sports /30000)+1 end

,decile_prog_engaged_football_international_friendly_sky_sports =case when minutes_football_international_friendly_sky_sports =-1 then 99 when minutes_football_international_friendly_sky_sports =0 then 10 else abs(rank_prog_engaged_football_international_friendly_sky_sports /30000)+1 end
,decile_prog_engaged_football_international_friendly_non_sky_sports =case when minutes_football_international_friendly_non_sky_sports =-1 then 99 when minutes_football_international_friendly_non_sky_sports =0 then 10 else abs(rank_prog_engaged_football_international_friendly_non_sky_sports /30000)+1 end

,decile_prog_engaged_football_Capital_One_Cup_sky_sports =case when minutes_football_Capital_One_Cup_sky_sports =-1 then 99 when minutes_football_Capital_One_Cup_sky_sports =0 then 10 else abs(rank_prog_engaged_football_Capital_One_Cup_sky_sports /30000)+1 end

,decile_prog_engaged_football_La_Liga_sky_sports =case when minutes_football_La_Liga_sky_sports =-1 then 99 when minutes_football_La_Liga_sky_sports =0 then 10 else abs(rank_prog_engaged_football_La_Liga_sky_sports /30000)+1 end

,decile_prog_engaged_football_football_league_sky_sports =case when minutes_football_football_league_sky_sports =-1 then 99 when minutes_football_football_league_sky_sports =0 then 10 else abs(rank_prog_engaged_football_football_league_sky_sports /30000)+1 end

,decile_prog_engaged_cricket_ashes_sky_sports =case when minutes_cricket_ashes_sky_sports =-1 then 99 when minutes_cricket_ashes_sky_sports =0 then 10 else abs(rank_prog_engaged_cricket_ashes_sky_sports /30000)+1 end
,decile_prog_engaged_cricket_ashes_non_sky_sports =case when minutes_cricket_ashes_non_sky_sports =-1 then 99 when minutes_cricket_ashes_non_sky_sports =0 then 10 else abs(rank_prog_engaged_cricket_ashes_non_sky_sports /30000)+1 end

,decile_prog_engaged_cricket_non_ashes_sky_sports =case when minutes_cricket_non_ashes_sky_sports =-1 then 99 when minutes_cricket_non_ashes_sky_sports =0 then 10 else abs(rank_prog_engaged_cricket_non_ashes_sky_sports /30000)+1 end
,decile_prog_engaged_cricket_non_ashes_non_sky_sports =case when minutes_cricket_non_ashes_non_sky_sports =-1 then 99 when minutes_cricket_non_ashes_non_sky_sports =0 then 10 else abs(rank_prog_engaged_cricket_non_ashes_non_sky_sports /30000)+1 end



,decile_prog_engaged_golf_sky_sports =case when minutes_golf_sky_sports =-1 then 99 when minutes_golf_sky_sports =0 then 10 else abs(rank_prog_engaged_golf_sky_sports /30000)+1 end
,decile_prog_engaged_golf_non_sky_sports =case when minutes_golf_non_sky_sports =-1 then 99 when minutes_golf_non_sky_sports =0 then 10 else abs(rank_prog_engaged_golf_non_sky_sports /30000)+1 end

,decile_prog_engaged_tennis_sky_sports =case when minutes_tennis_sky_sports =-1 then 99 when minutes_tennis_sky_sports =0 then 10 else abs(rank_prog_engaged_tennis_sky_sports /30000)+1 end
,decile_prog_engaged_tennis_non_sky_sports =case when minutes_tennis_non_sky_sports =-1 then 99 when minutes_tennis_non_sky_sports =0 then 10 else abs(rank_prog_engaged_tennis_non_sky_sports /30000)+1 end

,decile_prog_engaged_motor_sport_sky_sports =case when minutes_motor_sport_sky_sports =-1 then 99 when minutes_motor_sport_sky_sports =0 then 10 else abs(rank_prog_engaged_motor_sport_sky_sports /30000)+1 end
,decile_prog_engaged_motor_sport_non_sky_sports =case when minutes_motor_sport_non_sky_sports =-1 then 99 when minutes_motor_sport_non_sky_sports =0 then 10 else abs(rank_prog_engaged_motor_sport_non_sky_sports /30000)+1 end

,decile_prog_engaged_F1_sky_sports =case when minutes_F1_sky_sports =-1 then 99 when minutes_F1_sky_sports =0 then 10 else abs(rank_prog_engaged_F1_sky_sports /30000)+1 end
,decile_prog_engaged_F1_non_sky_sports =case when minutes_F1_non_sky_sports =-1 then 99 when minutes_F1_non_sky_sports =0 then 10 else abs(rank_prog_engaged_F1_non_sky_sports /30000)+1 end

,decile_prog_engaged_horse_racing_sky_sports =case when minutes_horse_racing_sky_sports =-1 then 99 when minutes_horse_racing_sky_sports =0 then 10 else abs(rank_prog_engaged_horse_racing_sky_sports /30000)+1 end
,decile_prog_engaged_horse_racing_non_sky_sports =case when minutes_horse_racing_non_sky_sports =-1 then 99 when minutes_horse_racing_non_sky_sports =0 then 10 else abs(rank_prog_engaged_horse_racing_non_sky_sports /30000)+1 end

,decile_prog_engaged_snooker_pool_sky_sports =case when minutes_snooker_pool_sky_sports =-1 then 99 when minutes_snooker_pool_sky_sports =0 then 10 else abs(rank_prog_engaged_snooker_pool_sky_sports /30000)+1 end
,decile_prog_engaged_snooker_pool_non_sky_sports =case when minutes_snooker_pool_non_sky_sports =-1 then 99 when minutes_snooker_pool_non_sky_sports =0 then 10 else abs(rank_prog_engaged_snooker_pool_non_sky_sports /30000)+1 end

,decile_prog_engaged_rugby_sky_sports =case when minutes_rugby_sky_sports =-1 then 99 when minutes_rugby_sky_sports =0 then 10 else abs(rank_prog_engaged_rugby_sky_sports /30000)+1 end
,decile_prog_engaged_rugby_non_sky_sports =case when minutes_rugby_non_sky_sports =-1 then 99 when minutes_rugby_non_sky_sports =0 then 10 else abs(rank_prog_engaged_rugby_non_sky_sports /30000)+1 end

,decile_prog_engaged_wrestling_sky_sports =case when minutes_wrestling_sky_sports =-1 then 99 when minutes_wrestling_sky_sports =0 then 10 else abs(rank_prog_engaged_wrestling_sky_sports /30000)+1 end
,decile_prog_engaged_wrestling_non_sky_sports =case when minutes_wrestling_non_sky_sports =-1 then 99 when minutes_wrestling_non_sky_sports =0 then 10 else abs(rank_prog_engaged_wrestling_non_sky_sports /30000)+1 end

,decile_prog_engaged_wwe =case when minutes_wwe =-1 then 99 when minutes_wwe =0 then 10 else abs(rank_prog_engaged_wwe /30000)+1 end
,decile_prog_engaged_wwe_sky_sports =case when minutes_wwe_sky_sports =-1 then 99 when minutes_wwe_sky_sports =0 then 10 else abs(rank_prog_engaged_wwe_sky_sports /30000)+1 end
,decile_prog_engaged_wwe_sbo =case when minutes_wwe_sbo =-1 then 99 when minutes_wwe_sbo =0 then 10 else abs(rank_prog_engaged_wwe_sbo /30000)+1 end
,decile_prog_engaged_sky_1_or_2 =case when minutes_wwe_sky_1_or_2 =-1 then 99 when minutes_wwe_sky_1_or_2 =0 then 10 else abs(rank_prog_engaged_wwe_sky_1_or_2 /30000)+1 end

,decile_prog_engaged_darts_sky_sports =case when minutes_darts_sky_sports =-1 then 99 when minutes_darts_sky_sports =0 then 10 else abs(rank_prog_engaged_darts_sky_sports /30000)+1 end
,decile_prog_engaged_darts_non_sky_sports =case when minutes_darts_non_sky_sports =-1 then 99 when minutes_darts_non_sky_sports =0 then 10 else abs(rank_prog_engaged_darts_non_sky_sports /30000)+1 end

,decile_prog_engaged_boxing_sky_sports =case when minutes_boxing_sky_sports =-1 then 99 when minutes_boxing_sky_sports =0 then 10 else abs(rank_prog_engaged_boxing_sky_sports /30000)+1 end
,decile_prog_engaged_boxing_non_sky_sports =case when minutes_boxing_non_sky_sports =-1 then 99 when minutes_boxing_non_sky_sports =0 then 10 else abs(rank_prog_engaged_boxing_non_sky_sports /30000)+1 end
,decile_prog_engaged_overall_football =case when minutes_overall_football =-1 then 99 when minutes_overall_football =0 then 10 else abs(rank_prog_engaged_overall_football /30000)+1 end
,decile_prog_engaged_Sky_Sports_cricket_overall =case when minutes_Sky_Sports_cricket_overall =-1 then 99 when minutes_Sky_Sports_cricket_overall =0 then 10 else abs(rank_prog_engaged_Sky_Sports_cricket_overall /30000)+1 end
,decile_prog_engaged_non_Sky_Sports_cricket_overall =case when minutes_non_Sky_Sports_cricket_overall =-1 then 99 when minutes_non_Sky_Sports_cricket_overall =0 then 10 else abs(rank_prog_engaged_non_Sky_Sports_cricket_overall /30000)+1 end

,decile_prog_engaged_niche_sports_sky_sports =case when minutes_niche_sports_sky_sports =-1 then 99 when minutes_niche_sports_sky_sports =0 then 10 else abs(rank_prog_engaged_niche_sports_sky_sports /30000)+1 end
,decile_prog_engaged_niche_sports_non_Sky_Sports =case when minutes_niche_sports_non_Sky_Sports =-1 then 99 when minutes_niche_sports_non_Sky_Sports =0 then 10 else abs(rank_prog_engaged_niche_sports_non_Sky_Sports /30000)+1 end

from v223_unbundling_viewing_summary_by_account 
;
commit;






--select top 10 * from v223_unbundling_viewing_summary_by_account

--select count(*) from v223_unbundling_viewing_summary_by_account

--select percentile_prog_engaged_wwe , count(*)  from v223_unbundling_viewing_summary_by_account group by percentile_prog_engaged_wwe order by percentile_prog_engaged_wwe
---Add on Weights for accounts----

--drop table #account_details_for_weighting;
select b.account_number
,case when bb_type  in ('NA','6) NA') and talk_product is null then 'a) TV Only' else 'b) TV with BB and or Talk' end as product_holding_group

,case when b.value_segment = 'UNSTABLE' then 'UNSTABLE' else 'STABLE' end as value_segment_group
,case when b.affluence_septile in ('4','5','6') then '01) High Affluence' else '02) Non-High Affluence' end as affluence_group
,case when b.tenure in ('A) 0-12 Months','B) 1-2 Years','C) 2-3 Years') then 'A-C) 0-36 Months' else tenure end as tenure_group 

,case          when b.box_type in ('FDB & FDB','HD & FDB','HD & HD','HD & Skyplus','HDx & FDB','HDx & HDx','HDx & Skyplus','Skyplus & FDB','Skyplus & Skyplus') then 'a) Multiple Boxes'  
                        when b.box_type in ('HD & No_secondary_box','HDx & No_secondary_box','Skyplus & No_secondary_box') then 'b) Single Non-FDB Box' 
                        when b.box_type in ('FDB & No_secondary_box') then 'c) FDB Box' else 'b) Single Non-FDB Box'  end as box_type_group ---Set non-Matches to Most popular group 

,case when number_of_sports_premiums=2 and number_of_movies_premiums=2 then 'a) Top Tier' 
                        when number_of_sports_premiums=2 and number_of_movies_premiums=0 then 'b) Dual Sports' 
                        when number_of_sports_premiums=0 and number_of_movies_premiums=2 then 'c) Dual Movies' 
                        when number_of_sports_premiums+ number_of_movies_premiums>0 then 'd) Other Premiums' else 'e) Other' end as tv_package_group

,case when c.days_with_viewing>=280 then 1 else 0 end as vespa_viewing_account

into #account_details_for_weighting
from v220_zero_mix_active_uk_accounts as b
left outer join v223_unbundling_viewing_summary_by_account as c
on b.account_number=c.account_number
;

select product_holding_group
,value_segment_group
,affluence_group
,tenure_group 

,box_type_group
,tv_package_group
,count(*) as accounts
,sum(vespa_viewing_account) as vespa_account
into #weighting_values
from #account_details_for_weighting
group by product_holding_group
,value_segment_group
,affluence_group
,tenure_group 

,box_type_group
,tv_package_group
;

commit;

select account_number
,accounts/cast(vespa_account as real) as weight
into #account_weight
from #account_details_for_weighting as a
left outer join #weighting_values as b
on  a.product_holding_group=b.product_holding_group
and a.tv_package_group=b.tv_package_group
and a.box_type_group=b.box_type_group
and a.value_segment_group=b.value_segment_group
and a.tenure_group=b.tenure_group
and a.affluence_group=b.affluence_group 
where vespa_viewing_account=1 
;
commit;

alter table v223_unbundling_viewing_summary_by_account add account_weight real;

update v223_unbundling_viewing_summary_by_account 
set account_weight= b.weight
from v223_unbundling_viewing_summary_by_account as a
left outer join #account_weight as b
on a.account_number = b.account_number
where days_with_viewing>=280
;
--select sum(account_weight) from v223_unbundling_viewing_summary_by_account 

--select count(*) from v220_zero_mix_active_uk_accounts;
commit;











--drop table dbarnett.v223_Unbundling_pivot_activity_data;
select a.account_number
,account_weight

,percentile_minutes_sport 
--,percentile_minutes_sport_sky_sports 
,percentile_minutes_sport_sky_sports_news 

,percentile_minutes_sport_terrestrial 
,percentile_minutes_sport_football_terrestrial 

,percentile_minutes_sport_espn 
,percentile_minutes_football_sky_sports 

,percentile_minutes_football_premier_league_sky_sports 
,percentile_minutes_football_premier_league_ESPN_BT 

,percentile_minutes_football_champions_league_sky_sports 
,percentile_minutes_football_champions_league_non_sky_sports 

--,percentile_minutes_football_fa_cup_sky_sports 
,percentile_minutes_football_fa_cup_ESPN_BT 
,percentile_minutes_football_fa_cup_Other_Channels 

,percentile_minutes_football_europa_league_ESPN_BT 
,percentile_minutes_football_europa_league_Other_Channels 

,percentile_minutes_football_world_cup_qualifier_sky_sports 
,percentile_minutes_football_world_cup_qualifier_non_sky_sports 

,percentile_minutes_football_international_friendly_sky_sports 
,percentile_minutes_football_international_friendly_non_sky_sports 

,percentile_minutes_football_Capital_One_Cup_sky_sports 

,percentile_minutes_football_La_Liga_sky_sports 

,percentile_minutes_football_football_league_sky_sports 

,percentile_minutes_cricket_ashes_sky_sports 
,percentile_minutes_cricket_ashes_non_sky_sports 

,percentile_minutes_cricket_non_ashes_sky_sports 
,percentile_minutes_cricket_non_ashes_non_sky_sports 



,percentile_minutes_golf_sky_sports 
,percentile_minutes_golf_non_sky_sports 

,percentile_minutes_tennis_sky_sports 
,percentile_minutes_tennis_non_sky_sports 

,percentile_minutes_motor_sport_sky_sports 
,percentile_minutes_motor_sport_non_sky_sports 

,percentile_minutes_F1_sky_sports 
,percentile_minutes_F1_non_sky_sports 

,percentile_minutes_horse_racing_sky_sports 
,percentile_minutes_horse_racing_non_sky_sports 

,percentile_minutes_snooker_pool_sky_sports 
,percentile_minutes_snooker_pool_non_sky_sports 

,percentile_minutes_rugby_sky_sports 
,percentile_minutes_rugby_non_sky_sports 

,percentile_minutes_wrestling_sky_sports 
,percentile_minutes_wrestling_non_sky_sports 


,percentile_minutes_wwe  
,percentile_minutes_wwe_sky_sports  
,percentile_minutes_wwe_sbo 
,percentile_minutes_sky_1_or_2 

,percentile_minutes_darts_sky_sports 
,percentile_minutes_darts_non_sky_sports 

,percentile_minutes_boxing_sky_sports 
,percentile_minutes_boxing_non_sky_sports 

,percentile_minutes_overall_football 
,percentile_minutes_Sky_Sports_cricket_overall 
,percentile_minutes_non_Sky_Sports_cricket_overall 
,percentile_minutes_niche_sports_sky_sports 
,percentile_minutes_niche_sports_non_sky_sports 

---3min+
,percentile_prog_3min_plus_sport 
,percentile_prog_3min_plus_sport_sky_sports 
,percentile_prog_3min_plus_sport_sky_sports_news 

,percentile_prog_3min_plus_sport_terrestrial 
,percentile_prog_3min_plus_sport_football_terrestrial 

,percentile_prog_3min_plus_sport_espn 
--,percentile_prog_3min_plus_football_sky_sports 

,percentile_prog_3min_plus_football_premier_league_sky_sports 
,percentile_prog_3min_plus_football_premier_league_ESPN_BT 

,percentile_prog_3min_plus_football_champions_league_sky_sports 
,percentile_prog_3min_plus_football_champions_league_non_sky_sports 

--,percentile_prog_3min_plus_football_fa_cup_sky_sports 
,percentile_prog_3min_plus_football_fa_cup_ESPN_BT 
,percentile_prog_3min_plus_football_fa_cup_Other_Channels 

,percentile_prog_3min_plus_football_europa_league_ESPN_BT 
,percentile_prog_3min_plus_football_europa_league_Other_Channels 

,percentile_prog_3min_plus_football_world_cup_qualifier_sky_sports 
,percentile_prog_3min_plus_football_world_cup_qualifier_non_sky_sports 

,percentile_prog_3min_plus_football_international_friendly_sky_sports 
,percentile_prog_3min_plus_football_international_friendly_non_sky_sports 

,percentile_prog_3min_plus_football_Capital_One_Cup_sky_sports 

,percentile_prog_3min_plus_football_La_Liga_sky_sports 

,percentile_prog_3min_plus_football_football_league_sky_sports 

,percentile_prog_3min_plus_cricket_ashes_sky_sports 
,percentile_prog_3min_plus_cricket_ashes_non_sky_sports 

,percentile_prog_3min_plus_cricket_non_ashes_sky_sports 
,percentile_prog_3min_plus_cricket_non_ashes_non_sky_sports 



,percentile_prog_3min_plus_golf_sky_sports 
,percentile_prog_3min_plus_golf_non_sky_sports 

,percentile_prog_3min_plus_tennis_sky_sports 
,percentile_prog_3min_plus_tennis_non_sky_sports 

,percentile_prog_3min_plus_motor_sport_sky_sports 
,percentile_prog_3min_plus_motor_sport_non_sky_sports 

,percentile_prog_3min_plus_F1_sky_sports 
,percentile_prog_3min_plus_F1_non_sky_sports 

,percentile_prog_3min_plus_horse_racing_sky_sports 
,percentile_prog_3min_plus_horse_racing_non_sky_sports 

,percentile_prog_3min_plus_snooker_pool_sky_sports 
,percentile_prog_3min_plus_snooker_pool_non_sky_sports 

,percentile_prog_3min_plus_rugby_sky_sports 
,percentile_prog_3min_plus_rugby_non_sky_sports 

,percentile_prog_3min_plus_wrestling_sky_sports 
,percentile_prog_3min_plus_wrestling_non_sky_sports 


,percentile_prog_3min_plus_wwe  
,percentile_prog_3min_plus_wwe_sky_sports  
,percentile_prog_3min_plus_wwe_sbo 
,percentile_prog_3min_plus_sky_1_or_2 

,percentile_prog_3min_plus_darts_sky_sports 
,percentile_prog_3min_plus_darts_non_sky_sports 

,percentile_prog_3min_plus_boxing_sky_sports 
,percentile_prog_3min_plus_boxing_non_sky_sports 


,percentile_prog_3min_plus_overall_football 
,percentile_prog_3min_plus_Sky_Sports_cricket_overall 
,percentile_prog_3min_plus_non_Sky_Sports_cricket_overall 
,percentile_prog_3min_plus_niche_sports_sky_sports 
,percentile_prog_3min_plus_niche_sports_non_sky_sports 

---Engaged

,percentile_prog_engaged_sport 
,percentile_prog_engaged_sport_sky_sports 
,percentile_prog_engaged_sport_sky_sports_news 

,percentile_prog_engaged_sport_terrestrial 
,percentile_prog_engaged_sport_football_terrestrial 

,percentile_prog_engaged_sport_espn 
--,percentile_prog_engaged_football_sky_sports 

,percentile_prog_engaged_football_premier_league_sky_sports 
,percentile_prog_engaged_football_premier_league_ESPN_BT 

,percentile_prog_engaged_football_champions_league_sky_sports 
,percentile_prog_engaged_football_champions_league_non_sky_sports 

--,percentile_prog_engaged_football_fa_cup_sky_sports 
,percentile_prog_engaged_football_fa_cup_ESPN_BT 
,percentile_prog_engaged_football_fa_cup_Other_Channels 

,percentile_prog_engaged_football_europa_league_ESPN_BT 
,percentile_prog_engaged_football_europa_league_Other_Channels 

,percentile_prog_engaged_football_world_cup_qualifier_sky_sports 
,percentile_prog_engaged_football_world_cup_qualifier_non_sky_sports 

,percentile_prog_engaged_football_international_friendly_sky_sports 
,percentile_prog_engaged_football_international_friendly_non_sky_sports 

,percentile_prog_engaged_football_Capital_One_Cup_sky_sports 

,percentile_prog_engaged_football_La_Liga_sky_sports 

,percentile_prog_engaged_football_football_league_sky_sports 

,percentile_prog_engaged_cricket_ashes_sky_sports 
,percentile_prog_engaged_cricket_ashes_non_sky_sports 

,percentile_prog_engaged_cricket_non_ashes_sky_sports 
,percentile_prog_engaged_cricket_non_ashes_non_sky_sports 



,percentile_prog_engaged_golf_sky_sports 
,percentile_prog_engaged_golf_non_sky_sports 

,percentile_prog_engaged_tennis_sky_sports 
,percentile_prog_engaged_tennis_non_sky_sports 

,percentile_prog_engaged_motor_sport_sky_sports 
,percentile_prog_engaged_motor_sport_non_sky_sports 

,percentile_prog_engaged_F1_sky_sports 
,percentile_prog_engaged_F1_non_sky_sports 

,percentile_prog_engaged_horse_racing_sky_sports 
,percentile_prog_engaged_horse_racing_non_sky_sports 

,percentile_prog_engaged_snooker_pool_sky_sports 
,percentile_prog_engaged_snooker_pool_non_sky_sports 

,percentile_prog_engaged_rugby_sky_sports 
,percentile_prog_engaged_rugby_non_sky_sports 

,percentile_prog_engaged_wrestling_sky_sports 
,percentile_prog_engaged_wrestling_non_sky_sports 


,percentile_prog_engaged_wwe  
,percentile_prog_engaged_wwe_sky_sports  
,percentile_prog_engaged_wwe_sbo 
,percentile_prog_engaged_sky_1_or_2 

,percentile_prog_engaged_darts_sky_sports 
,percentile_prog_engaged_darts_non_sky_sports 

,percentile_prog_engaged_boxing_sky_sports 
,percentile_prog_engaged_boxing_non_sky_sports 

,percentile_prog_engaged_overall_football 
,percentile_prog_engaged_Sky_Sports_cricket_overall 
,percentile_prog_engaged_non_Sky_Sports_cricket_overall 
,percentile_prog_engaged_niche_sports_sky_sports 
,percentile_prog_engaged_niche_sports_non_sky_sports 

---Repeat for Decile
,decile_minutes_sport 
,decile_minutes_sport_sky_sports 
,decile_minutes_sport_sky_sports_news 

,decile_minutes_sport_terrestrial 
,decile_minutes_sport_football_terrestrial 

,decile_minutes_sport_espn 
--,decile_minutes_football_sky_sports 

,decile_minutes_football_premier_league_sky_sports 
,decile_minutes_football_premier_league_ESPN_BT 

,decile_minutes_football_champions_league_sky_sports 
,decile_minutes_football_champions_league_non_sky_sports 

--,decile_minutes_football_fa_cup_sky_sports 
,decile_minutes_football_fa_cup_ESPN_BT 
,decile_minutes_football_fa_cup_Other_Channels 

,decile_minutes_football_europa_league_ESPN_BT 
,decile_minutes_football_europa_league_Other_Channels 

,decile_minutes_football_world_cup_qualifier_sky_sports 
,decile_minutes_football_world_cup_qualifier_non_sky_sports 

,decile_minutes_football_international_friendly_sky_sports 
,decile_minutes_football_international_friendly_non_sky_sports 

,decile_minutes_football_Capital_One_Cup_sky_sports 

,decile_minutes_football_La_Liga_sky_sports 

,decile_minutes_football_football_league_sky_sports 

,decile_minutes_cricket_ashes_sky_sports 
,decile_minutes_cricket_ashes_non_sky_sports 

,decile_minutes_cricket_non_ashes_sky_sports 
,decile_minutes_cricket_non_ashes_non_sky_sports 



,decile_minutes_golf_sky_sports 
,decile_minutes_golf_non_sky_sports 

,decile_minutes_tennis_sky_sports 
,decile_minutes_tennis_non_sky_sports 

,decile_minutes_motor_sport_sky_sports 
,decile_minutes_motor_sport_non_sky_sports 

,decile_minutes_F1_sky_sports 
,decile_minutes_F1_non_sky_sports 

,decile_minutes_horse_racing_sky_sports 
,decile_minutes_horse_racing_non_sky_sports 

,decile_minutes_snooker_pool_sky_sports 
,decile_minutes_snooker_pool_non_sky_sports 

,decile_minutes_rugby_sky_sports 
,decile_minutes_rugby_non_sky_sports 

,decile_minutes_wrestling_sky_sports 
,decile_minutes_wrestling_non_sky_sports 


,decile_minutes_wwe  
,decile_minutes_wwe_sky_sports  
,decile_minutes_wwe_sbo 
,decile_minutes_sky_1_or_2 

,decile_minutes_darts_sky_sports 
,decile_minutes_darts_non_sky_sports 

,decile_minutes_boxing_sky_sports 
,decile_minutes_boxing_non_sky_sports 


,decile_minutes_overall_football 
,decile_minutes_Sky_Sports_cricket_overall 
,decile_minutes_non_Sky_Sports_cricket_overall 
,decile_minutes_niche_sports_sky_sports 
,decile_minutes_niche_sports_non_sky_sports 

---3min+
,decile_prog_3min_plus_sport 
,decile_prog_3min_plus_sport_sky_sports 
,decile_prog_3min_plus_sport_sky_sports_news 

,decile_prog_3min_plus_sport_terrestrial 
,decile_prog_3min_plus_sport_football_terrestrial 

,decile_prog_3min_plus_sport_espn 
,decile_prog_3min_plus_football_sky_sports 

,decile_prog_3min_plus_football_premier_league_sky_sports 
,decile_prog_3min_plus_football_premier_league_ESPN_BT 

,decile_prog_3min_plus_football_champions_league_sky_sports 
,decile_prog_3min_plus_football_champions_league_non_sky_sports 

--,decile_prog_3min_plus_football_fa_cup_sky_sports 
,decile_prog_3min_plus_football_fa_cup_ESPN_BT 
,decile_prog_3min_plus_football_fa_cup_Other_Channels 

,decile_prog_3min_plus_football_europa_league_ESPN_BT 
,decile_prog_3min_plus_football_europa_league_Other_Channels 

,decile_prog_3min_plus_football_world_cup_qualifier_sky_sports 
,decile_prog_3min_plus_football_world_cup_qualifier_non_sky_sports 

,decile_prog_3min_plus_football_international_friendly_sky_sports 
,decile_prog_3min_plus_football_international_friendly_non_sky_sports 

,decile_prog_3min_plus_football_Capital_One_Cup_sky_sports 

,decile_prog_3min_plus_football_La_Liga_sky_sports 

,decile_prog_3min_plus_football_football_league_sky_sports 

,decile_prog_3min_plus_cricket_ashes_sky_sports 
,decile_prog_3min_plus_cricket_ashes_non_sky_sports 

,decile_prog_3min_plus_cricket_non_ashes_sky_sports 
,decile_prog_3min_plus_cricket_non_ashes_non_sky_sports 



,decile_prog_3min_plus_golf_sky_sports 
,decile_prog_3min_plus_golf_non_sky_sports 

,decile_prog_3min_plus_tennis_sky_sports 
,decile_prog_3min_plus_tennis_non_sky_sports 

,decile_prog_3min_plus_motor_sport_sky_sports 
,decile_prog_3min_plus_motor_sport_non_sky_sports 

,decile_prog_3min_plus_F1_sky_sports 
,decile_prog_3min_plus_F1_non_sky_sports 

,decile_prog_3min_plus_horse_racing_sky_sports 
,decile_prog_3min_plus_horse_racing_non_sky_sports 

,decile_prog_3min_plus_snooker_pool_sky_sports 
,decile_prog_3min_plus_snooker_pool_non_sky_sports 

,decile_prog_3min_plus_rugby_sky_sports 
,decile_prog_3min_plus_rugby_non_sky_sports 

,decile_prog_3min_plus_wrestling_sky_sports 
,decile_prog_3min_plus_wrestling_non_sky_sports 


,decile_prog_3min_plus_wwe  
,decile_prog_3min_plus_wwe_sky_sports  
,decile_prog_3min_plus_wwe_sbo 
,decile_prog_3min_plus_sky_1_or_2 

,decile_prog_3min_plus_darts_sky_sports 
,decile_prog_3min_plus_darts_non_sky_sports 

,decile_prog_3min_plus_boxing_sky_sports 
,decile_prog_3min_plus_boxing_non_sky_sports 


,decile_prog_3min_plus_overall_football 
,decile_prog_3min_plus_Sky_Sports_cricket_overall 
,decile_prog_3min_plus_non_Sky_Sports_cricket_overall 
,decile_prog_3min_plus_niche_sports_sky_sports 
,decile_prog_3min_plus_niche_sports_non_sky_sports 


---Engaged

,decile_prog_engaged_sport 
,decile_prog_engaged_sport_sky_sports 
,decile_prog_engaged_sport_sky_sports_news 

,decile_prog_engaged_sport_terrestrial 
,decile_prog_engaged_sport_football_terrestrial 

,decile_prog_engaged_sport_espn 
,decile_prog_engaged_football_sky_sports 

,decile_prog_engaged_football_premier_league_sky_sports 
,decile_prog_engaged_football_premier_league_ESPN_BT 

,decile_prog_engaged_football_champions_league_sky_sports 
,decile_prog_engaged_football_champions_league_non_sky_sports 

--,decile_prog_engaged_football_fa_cup_sky_sports 
,decile_prog_engaged_football_fa_cup_ESPN_BT 
,decile_prog_engaged_football_fa_cup_Other_Channels 

,decile_prog_engaged_football_europa_league_ESPN_BT 
,decile_prog_engaged_football_europa_league_Other_Channels 

,decile_prog_engaged_football_world_cup_qualifier_sky_sports 
,decile_prog_engaged_football_world_cup_qualifier_non_sky_sports 

,decile_prog_engaged_football_international_friendly_sky_sports 
,decile_prog_engaged_football_international_friendly_non_sky_sports 

,decile_prog_engaged_football_Capital_One_Cup_sky_sports 

,decile_prog_engaged_football_La_Liga_sky_sports 

,decile_prog_engaged_football_football_league_sky_sports 

,decile_prog_engaged_cricket_ashes_sky_sports 
,decile_prog_engaged_cricket_ashes_non_sky_sports 

,decile_prog_engaged_cricket_non_ashes_sky_sports 
,decile_prog_engaged_cricket_non_ashes_non_sky_sports 



,decile_prog_engaged_golf_sky_sports 
,decile_prog_engaged_golf_non_sky_sports 

,decile_prog_engaged_tennis_sky_sports 
,decile_prog_engaged_tennis_non_sky_sports 

,decile_prog_engaged_motor_sport_sky_sports 
,decile_prog_engaged_motor_sport_non_sky_sports 

,decile_prog_engaged_F1_sky_sports 
,decile_prog_engaged_F1_non_sky_sports 

,decile_prog_engaged_horse_racing_sky_sports 
,decile_prog_engaged_horse_racing_non_sky_sports 

,decile_prog_engaged_snooker_pool_sky_sports 
,decile_prog_engaged_snooker_pool_non_sky_sports 

,decile_prog_engaged_rugby_sky_sports 
,decile_prog_engaged_rugby_non_sky_sports 

,decile_prog_engaged_wrestling_sky_sports 
,decile_prog_engaged_wrestling_non_sky_sports 


,decile_prog_engaged_wwe  
,decile_prog_engaged_wwe_sky_sports  
,decile_prog_engaged_wwe_sbo 
,decile_prog_engaged_sky_1_or_2 

,decile_prog_engaged_darts_sky_sports 
,decile_prog_engaged_darts_non_sky_sports 

,decile_prog_engaged_boxing_sky_sports 
,decile_prog_engaged_boxing_non_sky_sports 

,decile_prog_engaged_overall_football 
,decile_prog_engaged_Sky_Sports_cricket_overall 
,decile_prog_engaged_non_Sky_Sports_cricket_overall 
,decile_prog_engaged_niche_sports_sky_sports 
,decile_prog_engaged_niche_sports_non_sky_sports 


----Phase 2 Splits---
,phase_2_percentile_engaged_sport 
,phase_2_decile_engaged_sport 

,phase_2_percentile_engaged_sport_sky_sports 
,phase_2_decile_engaged_sport_sky_sports 
,phase_2_percentile_engaged_football_sky_sports 
,phase_2_decile_engaged_football_sky_sports 
,phase_2_percentile_engaged_football_non_sky_sports 
,phase_2_decile_engaged_football_non_sky_sports 
,phase_2_percentile_engaged_rugby_sky_sports 
,phase_2_decile_engaged_rugby_sky_sports 
,phase_2_percentile_engaged_rugby_non_sky_sports 
,phase_2_decile_engaged_rugby_non_sky_sports 
,phase_2_percentile_engaged_cricket_sky_sports 
,phase_2_decile_engaged_cricket_sky_sports 
,phase_2_percentile_engaged_cricket_non_sky_sports 
,phase_2_decile_engaged_cricket_non_sky_sports 
,phase_2_percentile_engaged_F1_sky_sports 
,phase_2_decile_engaged_F1_sky_sports 
,phase_2_percentile_engaged_F1_non_sky_sports 
,phase_2_decile_engaged_F1_non_sky_sports 
,phase_2_percentile_engaged_Golf_sky_sports 
,phase_2_decile_engaged_Golf_sky_sports 
,phase_2_percentile_engaged_Golf_non_sky_sports 
,phase_2_decile_engaged_Golf_non_sky_sports 
,phase_2_percentile_engaged_Tennis_sky_sports 
,phase_2_decile_engaged_Tennis_sky_sports 
,phase_2_percentile_engaged_Tennis_non_sky_sports 
,phase_2_decile_engaged_Tennis_non_sky_sports 

---Repeat from 3 min versions---
,phase_2_percentile_3min_plus_sport 
,phase_2_decile_3min_plus_sport 
,phase_2_percentile_3min_plus_sport_sky_sports 
,phase_2_decile_3min_plus_sport_sky_sports 
,phase_2_percentile_3min_plus_football_sky_sports 
,phase_2_decile_3min_plus_football_sky_sports 
,phase_2_percentile_3min_plus_football_non_sky_sports 
,phase_2_decile_3min_plus_football_non_sky_sports 
,phase_2_percentile_3min_plus_rugby_sky_sports 
,phase_2_decile_3min_plus_rugby_sky_sports 
,phase_2_percentile_3min_plus_rugby_non_sky_sports 
,phase_2_decile_3min_plus_rugby_non_sky_sports 
,phase_2_percentile_3min_plus_cricket_sky_sports 
,phase_2_decile_3min_plus_cricket_sky_sports 
,phase_2_percentile_3min_plus_cricket_non_sky_sports 
,phase_2_decile_3min_plus_cricket_non_sky_sports 
,phase_2_percentile_3min_plus_F1_sky_sports 
,phase_2_decile_3min_plus_F1_sky_sports 
,phase_2_percentile_3min_plus_F1_non_sky_sports 
,phase_2_decile_3min_plus_F1_non_sky_sports 
,phase_2_percentile_3min_plus_Golf_sky_sports 
,phase_2_decile_3min_plus_Golf_sky_sports 
,phase_2_percentile_3min_plus_Golf_non_sky_sports 
,phase_2_decile_3min_plus_Golf_non_sky_sports 
,phase_2_percentile_3min_plus_Tennis_sky_sports 
,phase_2_decile_3min_plus_Tennis_sky_sports 
,phase_2_percentile_3min_plus_Tennis_non_sky_sports 
,phase_2_decile_3min_plus_Tennis_non_sky_sports 


--CL and PL--
,phase_2_percentile_engaged_football_champions_league_sky_sports
,phase_2_percentile_engaged_football_champions_league_non_sky_sports
,phase_2_percentile_engaged_football_premier_league_sky_sports
,phase_2_percentile_engaged_football_premier_league_non_sky_sports

,phase_2_decile_engaged_football_champions_league_sky_sports
,phase_2_decile_engaged_football_champions_league_non_sky_sports
,phase_2_decile_engaged_football_premier_league_sky_sports
,phase_2_decile_engaged_football_premier_league_non_sky_sports

,phase_2_percentile_3min_plus_football_champions_league_sky_sports
,phase_2_percentile_3min_plus_football_champions_league_non_sky_sports
,phase_2_percentile_3min_plus_football_premier_league_sky_sports
,phase_2_percentile_3min_plus_football_premier_league_non_sky_sports

,phase_2_decile_3min_plus_football_champions_league_sky_sports
,phase_2_decile_3min_plus_football_champions_league_non_sky_sports
,phase_2_decile_3min_plus_football_premier_league_sky_sports
,phase_2_decile_3min_plus_football_premier_league_non_sky_sports





---Profiling Vars--
,tv_package_group
,b.tenure
,b.isba_tv_region
,CASE b.hh_composition      when   '00' then 	'a) Family'
when '01'	then 'a) Family'
when '02'	then 'a) Family'
when '03'	then 'a) Family'
when '04'	then 'b) Single'
when '05'	then 'b) Single'
when '06'	then 'c) Homesharer'
when '07'	then 'c) Homesharer'
when '08'	then 'c) Homesharer'
when '09'	then 'a) Family'
when '10'	then 'a) Family'
when '11'	then 'c) Homesharer'
when 'U' 	then 'd) Unclassified'
else 'd) Unclassified' end as household_composition
,case when date_of_last_downgrade>='2013-03-12' then 1 else 0 end as downgrade_in_last_06M      
,case when all_downgrades>=5 then 'a) 5+ downgrades ever'
      when all_downgrades>=2 then 'b) 2-4 downgrades ever'
      when all_downgrades>0 then 'c) 1 downgrade ever' else 'd) Never Downgraded' end as downgrade_ever

,case when sports_downgrades>=5 then 'a) 5+ sports downgrades ever'
      when sports_downgrades>=2 then 'b) 2-4 sports downgrades ever'
      when sports_downgrades>0 then 'c) 1 sports downgrades ever' else 'd) Never Downgraded Sports' end as downgrade_ever_sports_channels

,case when all_upgrades>=5 then 'a) 5+ upgrades ever'
      when all_upgrades>=2 then 'b) 2-4 upgrades ever'
      when all_upgrades>0 then 'c) 1 upgrade ever' else 'd) Never upgraded' end as upgrade_ever

,case when sports_upgrades>=5 then 'a) 5+ sports upgrades ever'
      when sports_upgrades>=2 then 'b) 2-4 sports upgrades ever'
      when sports_upgrades>0 then 'c) 1 sports upgrade ever' else 'd) Never upgraded Sports' end as upgrade_ever_sports_channels
,case when b.cable_area='Y' then 1 else 0 end as cable_area_hh
,b.value_segment
,case when b.affluence_septile is null then 'U' 
        when b.affluence_septile = '0' then '0: Lowest Affluence' 
        when b.affluence_septile = '6' then '6: Highest Affluence' else b.affluence_septile end as affluence_septile_type
,b.box_type_group

,case when c.bb_type in ('1) Fibre','2) Unlimited','3) Everyday','4) Everyday Lite','5) Connect') then 1 else 0 end as has_bb
,case when c.bb_type in ('1) Fibre') then 1 else 0 end as has_bb_fibre
,case when talk_product is not null then 1 else 0 end as has_talk
,case when has_bb=1 and has_talk =1 then 'a) TV, BB and Talk'
      when has_bb=1 and has_talk =0 then 'b) TV and BB'
      when has_bb=0 and has_talk =1 then 'c) TV and Talk' else 'd) TV Only' end as tv_bb_talk
,case   when last_12m_bill_paid<200 then 'a) Under £200'
        when last_12m_bill_paid<300 then 'b) £200-£299'
        when last_12m_bill_paid<400 then 'c) £300-£399'
        when last_12m_bill_paid<500 then 'd) £400-£499'
        when last_12m_bill_paid<600 then 'e) £500-£599'
        when last_12m_bill_paid<700 then 'f) £600-£699'
        when last_12m_bill_paid<800 then 'g) £700-£799' else 'h) £800+' end as last_12mths_bill_amt

,last_12m_bill_paid*account_weight as last_12m_bill_paid_weighted
--Add in Extra Profile Variables--
,CQM 
,case when adsmartable_hh =1 then 1 else 0 end as adsmartable_household
,social_grade
,case when social_grade in ('A','B','C1') then 1 else 0 end as social_grade_ABC1
,Mirror_Men
,Mirror_Women
,Mirror_has_children as Mirror_Children

,
case mosaic_group
when 'A' then 	'Alpha Territory'
when 'B' then 	'Professional Rewards'
when 'C' then 	'Rural Solitude'
when 'D' then 	'Small Town Diversity'
when 'E' then 	'Active Retirement'
when 'F' then 	'Suburban Mindsets'
when 'G' then 	'Careers and Kids'
when 'H' then 	'New Homemakers'
when 'I' then 	'Ex-Council Community'
when 'J' then 	'Claimant Cultures'
when 'K' then 	'Upper Floor Living'
when 'L' then 	'Elderly Needs'
when 'M' then 	'Industrial Heritage'
when 'N' then 	'Terraced Melting Pot'
when 'O' then 	'Liberal Opinions'
when 'U' then 	'Unclassified'
else null end as h_mosaic_uk_group

,case True_Touch_Type 
when 1 then 'A: Experienced Netizens'
when 2 then 'A: Experienced Netizens'
when 3 then 'A: Experienced Netizens'
when 4 then 'A: Experienced Netizens'
when 5 then 'B: Cyber Tourists'
when 6 then 'B: Cyber Tourists'
when 7 then 'B: Cyber Tourists'
when 8 then 'B: Cyber Tourists'
when 9 then 'C: Digital Culture'
when 10 then 'C: Digital Culture'
when 11 then 'C: Digital Culture'
when 12 then 'D: Modern Media Margins'
when 13 then 'D: Modern Media Margins'
when 14 then 'D: Modern Media Margins'
when 15 then 'D: Modern Media Margins'
when 16 then 'E: Traditional Approach'
when 17 then 'E: Traditional Approach'
when 18 then 'E: Traditional Approach'
when 19 then 'E: Traditional Approach'
when 20 then 'E: Traditional Approach'
when 21 then 	'F: New tech Novices'
when 22 then 'F: New tech Novices'
when 99 then 	'G: Unclassified'
else 'G: Unclassified' end as True_Touch_Group
               
                ,child_hh_00_to_04
                ,child_hh_05_to_11
                ,child_hh_12_to_17
,case financial_stress 
when '0' then '0: Very low'
when '1' then '1: Low'
when '2' then '2: Medium'
when '3' then '3: High'
when '4' then '4: Very high'
when 'U' then '5: Unclassified'
else '5: Unclassified' end as financial_stress_hh


,case when (minutes_sport *account_weight)<0 then 0 else (minutes_sport *account_weight) end  as minutes_sport_total
,case when (minutes_sport_sky_sports *account_weight)<0 then 0 else (minutes_sport_sky_sports *account_weight) end  as minutes_sport_sky_sports_total
--,case when (minutes_football_sky_sports *account_weight)<0 then 0 else (minutes_football_sky_sports *account_weight) end  as minutes_football_sky_sports_total
,case when (minutes_sport_espn *account_weight)<0 then 0 else (minutes_sport_espn *account_weight) end  as minutes_sport_espn_total
,case when (minutes_sport_terrestrial *account_weight)<0 then 0 else (minutes_sport_terrestrial *account_weight) end  as minutes_sport_terrestrial_total
,case when (minutes_sport_football_terrestrial *account_weight)<0 then 0 else (minutes_sport_football_terrestrial *account_weight) end  as minutes_sport_football_terrestrial_total
,case when (minutes_sport_sky_sports_news *account_weight)<0 then 0 else (minutes_sport_sky_sports_news *account_weight) end  as minutes_sport_sky_sports_news_total
,case when (minutes_football_premier_league_sky_sports *account_weight)<0 then 0 else (minutes_football_premier_league_sky_sports *account_weight) end  as minutes_football_premier_league_sky_sports_total
,case when (minutes_football_premier_league_ESPN_BT *account_weight)<0 then 0 else (minutes_football_premier_league_ESPN_BT *account_weight) end  as minutes_football_premier_league_ESPN_BT_total
,case when (minutes_football_champions_league_sky_sports *account_weight)<0 then 0 else (minutes_football_champions_league_sky_sports *account_weight) end  as minutes_football_champions_league_sky_sports_total
,case when (minutes_football_champions_league_non_sky_sports *account_weight)<0 then 0 else (minutes_football_champions_league_non_sky_sports *account_weight) end  as minutes_football_champions_league_non_sky_sports_total
,case when (minutes_football_europa_league_ESPN_BT *account_weight)<0 then 0 else (minutes_football_europa_league_ESPN_BT *account_weight) end  as minutes_football_europa_league_ESPN_BT_total
,case when (minutes_football_europa_league_Other_Channels *account_weight)<0 then 0 else (minutes_football_europa_league_Other_Channels *account_weight) end  as minutes_football_europa_league_Other_Channels_total
,case when (minutes_football_fa_cup_ESPN_BT *account_weight)<0 then 0 else (minutes_football_fa_cup_ESPN_BT *account_weight) end  as minutes_football_fa_cup_ESPN_BT_total
,case when (minutes_football_fa_cup_Other_Channels *account_weight)<0 then 0 else (minutes_football_fa_cup_Other_Channels *account_weight) end  as minutes_football_fa_cup_Other_Channels_total
,case when (minutes_football_world_cup_qualifier_sky_sports *account_weight)<0 then 0 else (minutes_football_world_cup_qualifier_sky_sports *account_weight) end  as minutes_football_world_cup_qualifier_sky_sports_total
,case when (minutes_football_world_cup_qualifier_non_sky_sports *account_weight)<0 then 0 else (minutes_football_world_cup_qualifier_non_sky_sports *account_weight) end  as minutes_football_world_cup_qualifier_non_sky_sports_total
,case when (minutes_football_international_friendly_sky_sports *account_weight)<0 then 0 else (minutes_football_international_friendly_sky_sports *account_weight) end  as minutes_football_international_friendly_sky_sports_total
,case when (minutes_football_international_friendly_non_sky_sports *account_weight)<0 then 0 else (minutes_football_international_friendly_non_sky_sports *account_weight) end  as minutes_football_international_friendly_non_sky_sports_total
,case when (minutes_football_scottish_football_sky_sports *account_weight)<0 then 0 else (minutes_football_scottish_football_sky_sports *account_weight) end  as minutes_football_scottish_football_sky_sports_total
,case when (minutes_football_scottish_football_non_sky_sports *account_weight)<0 then 0 else (minutes_football_scottish_football_non_sky_sports *account_weight) end  as minutes_football_scottish_football_non_sky_sports_total
,case when (minutes_football_Capital_One_Cup_sky_sports *account_weight)<0 then 0 else (minutes_football_Capital_One_Cup_sky_sports *account_weight) end  as minutes_football_Capital_One_Cup_sky_sports_total
,case when (minutes_football_La_Liga_sky_sports *account_weight)<0 then 0 else (minutes_football_La_Liga_sky_sports *account_weight) end  as minutes_football_La_Liga_sky_sports_total
,case when (minutes_football_football_league_sky_sports *account_weight)<0 then 0 else (minutes_football_football_league_sky_sports *account_weight) end  as minutes_football_football_league_sky_sports_total
,case when (minutes_cricket_ashes_sky_sports *account_weight)<0 then 0 else (minutes_cricket_ashes_sky_sports *account_weight) end  as minutes_cricket_ashes_sky_sports_total
,case when (minutes_cricket_ashes_non_sky_sports *account_weight)<0 then 0 else (minutes_cricket_ashes_non_sky_sports *account_weight) end  as minutes_cricket_ashes_non_sky_sports_total
,case when (minutes_cricket_non_ashes_sky_sports *account_weight)<0 then 0 else (minutes_cricket_non_ashes_sky_sports *account_weight) end  as minutes_cricket_non_ashes_sky_sports_total
,case when (minutes_cricket_non_ashes_non_sky_sports *account_weight)<0 then 0 else (minutes_cricket_non_ashes_non_sky_sports *account_weight) end  as minutes_cricket_non_ashes_non_sky_sports_total
,case when (minutes_golf_sky_sports *account_weight)<0 then 0 else (minutes_golf_sky_sports *account_weight) end  as minutes_golf_sky_sports_total
,case when (minutes_golf_non_sky_sports *account_weight)<0 then 0 else (minutes_golf_non_sky_sports *account_weight) end  as minutes_golf_non_sky_sports_total
,case when (minutes_tennis_sky_sports *account_weight)<0 then 0 else (minutes_tennis_sky_sports *account_weight) end  as minutes_tennis_sky_sports_total
,case when (minutes_tennis_non_sky_sports *account_weight)<0 then 0 else (minutes_tennis_non_sky_sports *account_weight) end  as minutes_tennis_non_sky_sports_total
,case when (minutes_motor_sport_sky_sports *account_weight)<0 then 0 else (minutes_motor_sport_sky_sports *account_weight) end  as minutes_motor_sport_sky_sports_total
,case when (minutes_motor_sport_non_sky_sports *account_weight)<0 then 0 else (minutes_motor_sport_non_sky_sports *account_weight) end  as minutes_motor_sport_non_sky_sports_total
,case when (minutes_F1_sky_sports *account_weight)<0 then 0 else (minutes_F1_sky_sports *account_weight) end  as minutes_F1_sky_sports_total
,case when (minutes_F1_non_sky_sports *account_weight)<0 then 0 else (minutes_F1_non_sky_sports *account_weight) end  as minutes_F1_non_sky_sports_total
,case when (minutes_horse_racing_sky_sports *account_weight)<0 then 0 else (minutes_horse_racing_sky_sports *account_weight) end  as minutes_horse_racing_sky_sports_total
,case when (minutes_horse_racing_non_sky_sports *account_weight)<0 then 0 else (minutes_horse_racing_non_sky_sports *account_weight) end  as minutes_horse_racing_non_sky_sports_total
,case when (minutes_snooker_pool_sky_sports *account_weight)<0 then 0 else (minutes_snooker_pool_sky_sports *account_weight) end  as minutes_snooker_pool_sky_sports_total
,case when (minutes_snooker_pool_non_sky_sports *account_weight)<0 then 0 else (minutes_snooker_pool_non_sky_sports *account_weight) end  as minutes_snooker_pool_non_sky_sports_total
,case when (minutes_rugby_sky_sports *account_weight)<0 then 0 else (minutes_rugby_sky_sports *account_weight) end  as minutes_rugby_sky_sports_total
,case when (minutes_rugby_non_sky_sports *account_weight)<0 then 0 else (minutes_rugby_non_sky_sports *account_weight) end  as minutes_rugby_non_sky_sports_total
,case when (minutes_wrestling_sky_sports *account_weight)<0 then 0 else (minutes_wrestling_sky_sports *account_weight) end  as minutes_wrestling_sky_sports_total
,case when (minutes_wrestling_non_sky_sports *account_weight)<0 then 0 else (minutes_wrestling_non_sky_sports *account_weight) end  as minutes_wrestling_non_sky_sports_total
,case when (minutes_wwe *account_weight)<0 then 0 else (minutes_wwe *account_weight) end  as minutes_wwe_total
,case when (minutes_wwe_sky_sports *account_weight)<0 then 0 else (minutes_wwe_sky_sports *account_weight) end  as minutes_wwe_sky_sports_total
,case when (minutes_wwe_sbo *account_weight)<0 then 0 else (minutes_wwe_sbo *account_weight) end  as minutes_wwe_sbo_total
,case when (minutes_wwe_sky_1_or_2 *account_weight)<0 then 0 else (minutes_wwe_sky_1_or_2 *account_weight) end  as minutes_wwe_sky_1_or_2_total
,case when (minutes_darts_sky_sports *account_weight)<0 then 0 else (minutes_darts_sky_sports *account_weight) end  as minutes_darts_sky_sports_total
,case when (minutes_darts_non_sky_sports *account_weight)<0 then 0 else (minutes_darts_non_sky_sports *account_weight) end  as minutes_darts_non_sky_sports_total
,case when (minutes_boxing_sky_sports *account_weight)<0 then 0 else (minutes_boxing_sky_sports *account_weight) end  as minutes_boxing_sky_sports_total
,case when (minutes_boxing_non_sky_sports *account_weight)<0 then 0 else (minutes_boxing_non_sky_sports *account_weight) end  as minutes_boxing_non_sky_sports_total

,case when (minutes_overall_football  *account_weight)<0 then 0 else (minutes_overall_football  *account_weight) end  as minutes_overall_football_total 
,case when (minutes_Sky_Sports_cricket_overall  *account_weight)<0 then 0 else (minutes_Sky_Sports_cricket_overall  *account_weight) end  as minutes_Sky_Sports_cricket_overall_total
,case when (minutes_non_Sky_Sports_cricket_overall  *account_weight)<0 then 0 else (minutes_non_Sky_Sports_cricket_overall  *account_weight) end  as minutes_non_Sky_Sports_cricket_overall_total

,case when (minutes_niche_sports_sky_sports *account_weight)<0 then 0 else (minutes_niche_sports_sky_sports *account_weight) end  as minutes_niche_sports_sky_sports_total
,case when (minutes_niche_sports_non_sky_sports *account_weight)<0 then 0 else (minutes_niche_sports_non_sky_sports *account_weight) end  as minutes_niche_sports_non_sky_sports_total

,case when (minutes_sky_sports_football *account_weight)<0 then 0 else (minutes_sky_sports_football *account_weight) end  as minutes_sky_sports_football_total
,case when (minutes_non_sky_sports_football *account_weight)<0 then 0 else (minutes_non_sky_sports_football *account_weight) end  as minutes_non_sky_sports_football_total

,case when (minutes_sky_sports_exc_wwe *account_weight)<0 then 0 else (minutes_sky_sports_exc_wwe *account_weight) end  as minutes_sky_sports_exc_wwe_total

,case when (annualised_programmes_3min_plus_sport *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sport *account_weight) end  as annualised_programmes_3min_plus_sport_total
,case when (annualised_programmes_3min_plus_sport_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sport_sky_sports *account_weight) end  as annualised_programmes_3min_plus_sport_sky_sports_total
--,case when (annualised_programmes_3min_plus_football_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_sky_sports_total
,case when (annualised_programmes_3min_plus_sport_espn *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sport_espn *account_weight) end  as annualised_programmes_3min_plus_sport_espn_total
,case when (annualised_programmes_3min_plus_sport_terrestrial *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sport_terrestrial *account_weight) end  as annualised_programmes_3min_plus_sport_terrestrial_total
,case when (annualised_programmes_3min_plus_sport_football_terrestrial *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sport_football_terrestrial *account_weight) end  as annualised_programmes_3min_plus_sport_football_terrestrial_total
,case when (annualised_programmes_3min_plus_sport_sky_sports_news *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sport_sky_sports_news *account_weight) end  as annualised_programmes_3min_plus_sport_sky_sports_news_total
,case when (annualised_programmes_3min_plus_football_premier_league_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_premier_league_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_premier_league_sky_sports_total
,case when (annualised_programmes_3min_plus_football_premier_league_ESPN_BT *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_premier_league_ESPN_BT *account_weight) end  as annualised_programmes_3min_plus_football_premier_league_ESPN_BT_total
,case when (annualised_programmes_3min_plus_football_champions_league_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_champions_league_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_champions_league_sky_sports_total
,case when (annualised_programmes_3min_plus_football_champions_league_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_champions_league_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_champions_league_non_sky_sports_total
,case when (annualised_programmes_3min_plus_football_europa_league_ESPN_BT *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_europa_league_ESPN_BT *account_weight) end  as annualised_programmes_3min_plus_football_europa_league_ESPN_BT_total
,case when (annualised_programmes_3min_plus_football_europa_league_Other_Channels *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_europa_league_Other_Channels *account_weight) end  as annualised_programmes_3min_plus_football_europa_league_Other_Channels_total
,case when (annualised_programmes_3min_plus_football_fa_cup_ESPN_BT *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_fa_cup_ESPN_BT *account_weight) end  as annualised_programmes_3min_plus_football_fa_cup_ESPN_BT_total
,case when (annualised_programmes_3min_plus_football_fa_cup_Other_Channels *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_fa_cup_Other_Channels *account_weight) end  as annualised_programmes_3min_plus_football_fa_cup_Other_Channels_total
,case when (annualised_programmes_3min_plus_football_world_cup_qualifier_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_world_cup_qualifier_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_world_cup_qualifier_sky_sports_total
,case when (annualised_programmes_3min_plus_football_world_cup_qualifier_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_world_cup_qualifier_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_world_cup_qualifier_non_sky_sports_total
,case when (annualised_programmes_3min_plus_football_international_friendly_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_international_friendly_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_international_friendly_sky_sports_total
,case when (annualised_programmes_3min_plus_football_international_friendly_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_international_friendly_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_international_friendly_non_sky_sports_total
,case when (annualised_programmes_3min_plus_football_scottish_football_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_scottish_football_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_scottish_football_sky_sports_total
,case when (annualised_programmes_3min_plus_football_scottish_football_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_scottish_football_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_scottish_football_non_sky_sports_total
,case when (annualised_programmes_3min_plus_football_Capital_One_Cup_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_Capital_One_Cup_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_Capital_One_Cup_sky_sports_total
,case when (annualised_programmes_3min_plus_football_La_Liga_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_La_Liga_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_La_Liga_sky_sports_total
,case when (annualised_programmes_3min_plus_football_football_league_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_football_football_league_sky_sports *account_weight) end  as annualised_programmes_3min_plus_football_football_league_sky_sports_total
,case when (annualised_programmes_3min_plus_cricket_ashes_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_cricket_ashes_sky_sports *account_weight) end  as annualised_programmes_3min_plus_cricket_ashes_sky_sports_total
,case when (annualised_programmes_3min_plus_cricket_ashes_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_cricket_ashes_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_cricket_ashes_non_sky_sports_total
,case when (annualised_programmes_3min_plus_cricket_non_ashes_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_cricket_non_ashes_sky_sports *account_weight) end  as annualised_programmes_3min_plus_cricket_non_ashes_sky_sports_total
,case when (annualised_programmes_3min_plus_cricket_non_ashes_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_cricket_non_ashes_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_cricket_non_ashes_non_sky_sports_total
,case when (annualised_programmes_3min_plus_golf_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_golf_sky_sports *account_weight) end  as annualised_programmes_3min_plus_golf_sky_sports_total
,case when (annualised_programmes_3min_plus_golf_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_golf_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_golf_non_sky_sports_total
,case when (annualised_programmes_3min_plus_tennis_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_tennis_sky_sports *account_weight) end  as annualised_programmes_3min_plus_tennis_sky_sports_total
,case when (annualised_programmes_3min_plus_tennis_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_tennis_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_tennis_non_sky_sports_total
,case when (annualised_programmes_3min_plus_motor_sport_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_motor_sport_sky_sports *account_weight) end  as annualised_programmes_3min_plus_motor_sport_sky_sports_total
,case when (annualised_programmes_3min_plus_motor_sport_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_motor_sport_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_motor_sport_non_sky_sports_total
,case when (annualised_programmes_3min_plus_F1_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_F1_sky_sports *account_weight) end  as annualised_programmes_3min_plus_F1_sky_sports_total
,case when (annualised_programmes_3min_plus_F1_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_F1_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_F1_non_sky_sports_total
,case when (annualised_programmes_3min_plus_horse_racing_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_horse_racing_sky_sports *account_weight) end  as annualised_programmes_3min_plus_horse_racing_sky_sports_total
,case when (annualised_programmes_3min_plus_horse_racing_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_horse_racing_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_horse_racing_non_sky_sports_total
,case when (annualised_programmes_3min_plus_snooker_pool_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_snooker_pool_sky_sports *account_weight) end  as annualised_programmes_3min_plus_snooker_pool_sky_sports_total
,case when (annualised_programmes_3min_plus_snooker_pool_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_snooker_pool_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_snooker_pool_non_sky_sports_total
,case when (annualised_programmes_3min_plus_rugby_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_rugby_sky_sports *account_weight) end  as annualised_programmes_3min_plus_rugby_sky_sports_total
,case when (annualised_programmes_3min_plus_rugby_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_rugby_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_rugby_non_sky_sports_total
,case when (annualised_programmes_3min_plus_wrestling_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wrestling_sky_sports *account_weight) end  as annualised_programmes_3min_plus_wrestling_sky_sports_total
,case when (annualised_programmes_3min_plus_wrestling_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wrestling_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_wrestling_non_sky_sports_total
,case when (annualised_programmes_3min_plus_wwe *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe *account_weight) end  as annualised_programmes_3min_plus_wwe_total
,case when (annualised_programmes_3min_plus_wwe_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe_sky_sports *account_weight) end  as annualised_programmes_3min_plus_wwe_sky_sports_total
,case when (annualised_programmes_3min_plus_wwe_sbo *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe_sbo *account_weight) end  as annualised_programmes_3min_plus_wwe_sbo_total
,case when (annualised_programmes_3min_plus_wwe_sky_1_or_2 *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe_sky_1_or_2 *account_weight) end  as annualised_programmes_3min_plus_wwe_sky_1_or_2_total
,case when (annualised_programmes_3min_plus_darts_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_darts_sky_sports *account_weight) end  as annualised_programmes_3min_plus_darts_sky_sports_total
,case when (annualised_programmes_3min_plus_darts_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_darts_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_darts_non_sky_sports_total
,case when (annualised_programmes_3min_plus_boxing_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_boxing_sky_sports *account_weight) end  as annualised_programmes_3min_plus_boxing_sky_sports_total
,case when (annualised_programmes_3min_plus_boxing_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_boxing_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_boxing_non_sky_sports_total

,case when (annualised_programmes_3min_plus_overall_football  *account_weight)<0 then 0 else (annualised_programmes_3min_plus_overall_football  *account_weight) end  as annualised_programmes_3min_plus_overall_football_total 
,case when (annualised_programmes_3min_plus_Sky_Sports_cricket_overall  *account_weight)<0 then 0 else (annualised_programmes_3min_plus_Sky_Sports_cricket_overall  *account_weight) end  as annualised_programmes_3min_plus_Sky_Sports_cricket_overall_total
,case when (annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall  *account_weight)<0 then 0 else (annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall  *account_weight) end  as annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall_total
,case when (annualised_programmes_3min_plus_niche_sports_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_niche_sports_sky_sports *account_weight) end  as annualised_programmes_3min_plus_niche_sports_sky_sports_total
,case when (annualised_programmes_3min_plus_niche_sports_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_niche_sports_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_niche_sports_non_sky_sports_total

,case when (annualised_programmes_3min_plus_sky_sports_football   *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sky_sports_football   *account_weight) end  as annualised_programmes_3min_plus_sky_sports_football_total 
,case when (annualised_programmes_3min_plus_non_sky_sports_football   *account_weight)<0 then 0 else (annualised_programmes_3min_plus_non_sky_sports_football   *account_weight) end  as annualised_programmes_3min_plus_non_sky_sports_football_total 
,case when (annualised_programmes_3min_plus_sky_sports_exc_wwe   *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sky_sports_exc_wwe   *account_weight) end  as annualised_programmes_3min_plus_sky_sports_exc_wwe_total 

 


,case when (annualised_programmes_engaged_sport *account_weight)<0 then 0 else (annualised_programmes_engaged_sport *account_weight) end  as annualised_programmes_engaged_sport_total
,case when (annualised_programmes_engaged_sport_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_sport_sky_sports *account_weight) end  as annualised_programmes_engaged_sport_sky_sports_total
--,case when (annualised_programmes_engaged_football_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_sky_sports *account_weight) end  as annualised_programmes_engaged_football_sky_sports_total
,case when (annualised_programmes_engaged_sport_espn *account_weight)<0 then 0 else (annualised_programmes_engaged_sport_espn *account_weight) end  as annualised_programmes_engaged_sport_espn_total
,case when (annualised_programmes_engaged_sport_terrestrial *account_weight)<0 then 0 else (annualised_programmes_engaged_sport_terrestrial *account_weight) end  as annualised_programmes_engaged_sport_terrestrial_total
,case when (annualised_programmes_engaged_sport_football_terrestrial *account_weight)<0 then 0 else (annualised_programmes_engaged_sport_football_terrestrial *account_weight) end  as annualised_programmes_engaged_sport_football_terrestrial_total
,case when (annualised_programmes_engaged_sport_sky_sports_news *account_weight)<0 then 0 else (annualised_programmes_engaged_sport_sky_sports_news *account_weight) end  as annualised_programmes_engaged_sport_sky_sports_news_total
,case when (annualised_programmes_engaged_football_premier_league_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_premier_league_sky_sports *account_weight) end  as annualised_programmes_engaged_football_premier_league_sky_sports_total
,case when (annualised_programmes_engaged_football_premier_league_ESPN_BT *account_weight)<0 then 0 else (annualised_programmes_engaged_football_premier_league_ESPN_BT *account_weight) end  as annualised_programmes_engaged_football_premier_league_ESPN_BT_total
,case when (annualised_programmes_engaged_football_champions_league_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_champions_league_sky_sports *account_weight) end  as annualised_programmes_engaged_football_champions_league_sky_sports_total
,case when (annualised_programmes_engaged_football_champions_league_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_champions_league_non_sky_sports *account_weight) end  as annualised_programmes_engaged_football_champions_league_non_sky_sports_total
,case when (annualised_programmes_engaged_football_europa_league_ESPN_BT *account_weight)<0 then 0 else (annualised_programmes_engaged_football_europa_league_ESPN_BT *account_weight) end  as annualised_programmes_engaged_football_europa_league_ESPN_BT_total
,case when (annualised_programmes_engaged_football_europa_league_Other_Channels *account_weight)<0 then 0 else (annualised_programmes_engaged_football_europa_league_Other_Channels *account_weight) end  as annualised_programmes_engaged_football_europa_league_Other_Channels_total
,case when (annualised_programmes_engaged_football_fa_cup_ESPN_BT *account_weight)<0 then 0 else (annualised_programmes_engaged_football_fa_cup_ESPN_BT *account_weight) end  as annualised_programmes_engaged_football_fa_cup_ESPN_BT_total
,case when (annualised_programmes_engaged_football_fa_cup_Other_Channels *account_weight)<0 then 0 else (annualised_programmes_engaged_football_fa_cup_Other_Channels *account_weight) end  as annualised_programmes_engaged_football_fa_cup_Other_Channels_total
,case when (annualised_programmes_engaged_football_world_cup_qualifier_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_world_cup_qualifier_sky_sports *account_weight) end  as annualised_programmes_engaged_football_world_cup_qualifier_sky_sports_total
,case when (annualised_programmes_engaged_football_world_cup_qualifier_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_world_cup_qualifier_non_sky_sports *account_weight) end  as annualised_programmes_engaged_football_world_cup_qualifier_non_sky_sports_total
,case when (annualised_programmes_engaged_football_international_friendly_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_international_friendly_sky_sports *account_weight) end  as annualised_programmes_engaged_football_international_friendly_sky_sports_total
,case when (annualised_programmes_engaged_football_international_friendly_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_international_friendly_non_sky_sports *account_weight) end  as annualised_programmes_engaged_football_international_friendly_non_sky_sports_total
,case when (annualised_programmes_engaged_football_scottish_football_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_scottish_football_sky_sports *account_weight) end  as annualised_programmes_engaged_football_scottish_football_sky_sports_total
,case when (annualised_programmes_engaged_football_scottish_football_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_scottish_football_non_sky_sports *account_weight) end  as annualised_programmes_engaged_football_scottish_football_non_sky_sports_total
,case when (annualised_programmes_engaged_football_Capital_One_Cup_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_Capital_One_Cup_sky_sports *account_weight) end  as annualised_programmes_engaged_football_Capital_One_Cup_sky_sports_total
,case when (annualised_programmes_engaged_football_La_Liga_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_La_Liga_sky_sports *account_weight) end  as annualised_programmes_engaged_football_La_Liga_sky_sports_total
,case when (annualised_programmes_engaged_football_football_league_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_football_football_league_sky_sports *account_weight) end  as annualised_programmes_engaged_football_football_league_sky_sports_total
,case when (annualised_programmes_engaged_cricket_ashes_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_cricket_ashes_sky_sports *account_weight) end  as annualised_programmes_engaged_cricket_ashes_sky_sports_total
,case when (annualised_programmes_engaged_cricket_ashes_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_cricket_ashes_non_sky_sports *account_weight) end  as annualised_programmes_engaged_cricket_ashes_non_sky_sports_total
,case when (annualised_programmes_engaged_cricket_non_ashes_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_cricket_non_ashes_sky_sports *account_weight) end  as annualised_programmes_engaged_cricket_non_ashes_sky_sports_total
,case when (annualised_programmes_engaged_cricket_non_ashes_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_cricket_non_ashes_non_sky_sports *account_weight) end  as annualised_programmes_engaged_cricket_non_ashes_non_sky_sports_total
,case when (annualised_programmes_engaged_golf_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_golf_sky_sports *account_weight) end  as annualised_programmes_engaged_golf_sky_sports_total
,case when (annualised_programmes_engaged_golf_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_golf_non_sky_sports *account_weight) end  as annualised_programmes_engaged_golf_non_sky_sports_total
,case when (annualised_programmes_engaged_tennis_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_tennis_sky_sports *account_weight) end  as annualised_programmes_engaged_tennis_sky_sports_total
,case when (annualised_programmes_engaged_tennis_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_tennis_non_sky_sports *account_weight) end  as annualised_programmes_engaged_tennis_non_sky_sports_total
,case when (annualised_programmes_engaged_motor_sport_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_motor_sport_sky_sports *account_weight) end  as annualised_programmes_engaged_motor_sport_sky_sports_total
,case when (annualised_programmes_engaged_motor_sport_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_motor_sport_non_sky_sports *account_weight) end  as annualised_programmes_engaged_motor_sport_non_sky_sports_total
,case when (annualised_programmes_engaged_F1_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_F1_sky_sports *account_weight) end  as annualised_programmes_engaged_F1_sky_sports_total
,case when (annualised_programmes_engaged_F1_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_F1_non_sky_sports *account_weight) end  as annualised_programmes_engaged_F1_non_sky_sports_total
,case when (annualised_programmes_engaged_horse_racing_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_horse_racing_sky_sports *account_weight) end  as annualised_programmes_engaged_horse_racing_sky_sports_total
,case when (annualised_programmes_engaged_horse_racing_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_horse_racing_non_sky_sports *account_weight) end  as annualised_programmes_engaged_horse_racing_non_sky_sports_total
,case when (annualised_programmes_engaged_snooker_pool_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_snooker_pool_sky_sports *account_weight) end  as annualised_programmes_engaged_snooker_pool_sky_sports_total
,case when (annualised_programmes_engaged_snooker_pool_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_snooker_pool_non_sky_sports *account_weight) end  as annualised_programmes_engaged_snooker_pool_non_sky_sports_total
,case when (annualised_programmes_engaged_rugby_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_rugby_sky_sports *account_weight) end  as annualised_programmes_engaged_rugby_sky_sports_total
,case when (annualised_programmes_engaged_rugby_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_rugby_non_sky_sports *account_weight) end  as annualised_programmes_engaged_rugby_non_sky_sports_total
,case when (annualised_programmes_engaged_wrestling_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_wrestling_sky_sports *account_weight) end  as annualised_programmes_engaged_wrestling_sky_sports_total
,case when (annualised_programmes_engaged_wrestling_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_wrestling_non_sky_sports *account_weight) end  as annualised_programmes_engaged_wrestling_non_sky_sports_total
,case when (annualised_programmes_engaged_wwe *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe *account_weight) end  as annualised_programmes_engaged_wwe_total
,case when (annualised_programmes_engaged_wwe_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe_sky_sports *account_weight) end  as annualised_programmes_engaged_wwe_sky_sports_total
,case when (annualised_programmes_engaged_wwe_sbo *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe_sbo *account_weight) end  as annualised_programmes_engaged_wwe_sbo_total
,case when (annualised_programmes_engaged_wwe_sky_1_or_2 *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe_sky_1_or_2 *account_weight) end  as annualised_programmes_engaged_wwe_sky_1_or_2_total
,case when (annualised_programmes_engaged_darts_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_darts_sky_sports *account_weight) end  as annualised_programmes_engaged_darts_sky_sports_total
,case when (annualised_programmes_engaged_darts_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_darts_non_sky_sports *account_weight) end  as annualised_programmes_engaged_darts_non_sky_sports_total
,case when (annualised_programmes_engaged_boxing_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_boxing_sky_sports *account_weight) end  as annualised_programmes_engaged_boxing_sky_sports_total
,case when (annualised_programmes_engaged_boxing_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_boxing_non_sky_sports *account_weight) end  as annualised_programmes_engaged_boxing_non_sky_sports_total


,case when (annualised_programmes_engaged_overall_football  *account_weight)<0 then 0 else (annualised_programmes_engaged_overall_football  *account_weight) end  as annualised_programmes_engaged_overall_football_total 
,case when (annualised_programmes_engaged_Sky_Sports_cricket_overall  *account_weight)<0 then 0 else (annualised_programmes_engaged_Sky_Sports_cricket_overall  *account_weight) end  as annualised_programmes_engaged_Sky_Sports_cricket_overall_total
,case when (annualised_programmes_engaged_non_Sky_Sports_cricket_overall  *account_weight)<0 then 0 else (annualised_programmes_engaged_non_Sky_Sports_cricket_overall  *account_weight) end  as annualised_programmes_engaged_non_Sky_Sports_cricket_overall_total
,case when (annualised_programmes_engaged_niche_sports_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_niche_sports_sky_sports *account_weight) end  as annualised_programmes_engaged_niche_sports_sky_sports_total
,case when (annualised_programmes_engaged_niche_sports_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_niche_sports_non_sky_sports *account_weight) end  as annualised_programmes_engaged_niche_sports_non_sky_sports_total

,case when (annualised_programmes_engaged_sky_sports_football   *account_weight)<0 then 0 else (annualised_programmes_engaged_sky_sports_football   *account_weight) end  as annualised_programmes_engaged_sky_sports_football_total 
,case when (annualised_programmes_engaged_non_sky_sports_football   *account_weight)<0 then 0 else (annualised_programmes_engaged_non_sky_sports_football   *account_weight) end  as annualised_programmes_engaged_non_sky_sports_football_total 
,case when (annualised_programmes_engaged_sky_sports_exc_wwe   *account_weight)<0 then 0 else (annualised_programmes_engaged_sky_sports_exc_wwe   *account_weight) end  as annualised_programmes_engaged_sky_sports_exc_wwe_total 


,cb_address_postcode      
,cb_address_postcode_area      
,cb_address_postcode_district  

,sky_go_reg_distinct_days_used_L06M  
,sky_go_reg_distinct_days_used_L12M  

,c.hdtv                                
,c.multiroom                           
,c.skyplus                          

into dbarnett.v223_Unbundling_pivot_activity_data
from v223_unbundling_viewing_summary_by_account  as a
left outer join  v220_zero_mix_active_uk_accounts as b
on a.account_number = b.account_number
left outer join  v223_single_profiling_view as c
on a.account_number = c.account_number

where account_weight>0

;

commit;

---Add On Sky Go Flag to add to # distinct days already on--

alter table dbarnett.v223_Unbundling_pivot_activity_data add sky_go_used_L12M_flag tinyint;

update dbarnett.v223_Unbundling_pivot_activity_data
set sky_go_used_L12M_flag=case when sky_go_reg_distinct_days_used_L12M>0 then 1 else 0 end
;
commit;
--select sum(sky_go_used_L12M_flag),count(*),sum(account_weight),sum(account_weight*sky_go_used_L12M_flag) from dbarnett.v223_Unbundling_pivot_activity_data

--select top 100 * from v223_single_profiling_view;
--select round(annualised_programmes_3min_plus_golf_non_sky_sports,0) as golf_non_ss ,count(*) as records from v223_unbundling_viewing_summary_by_account  group by golf_non_ss order by golf_non_ss
grant all on dbarnett.v223_Unbundling_pivot_activity_data to public;

commit;
--select phase_2_percentile_engaged_football_champions_league_sky_sports from dbarnett.v223_Unbundling_pivot_activity_data
/*
grant all on dbarnett.v223_unbundling_viewing_summary_by_account  to public;

commit;
*/






---Produce Cut Down version for Powerpivot---

--drop table dbarnett.v223_Unbundling_pivot_activity_data_cut_down_for_powerpivot;
select account_weight

,percentile_minutes_sport 
,percentile_minutes_sport_sky_sports 
,percentile_minutes_wrestling_sky_sports 
,percentile_minutes_wrestling_non_sky_sports 
,percentile_minutes_wwe  
,percentile_minutes_wwe_sky_sports  
,percentile_minutes_wwe_sbo 
,percentile_minutes_sky_1_or_2 
,percentile_minutes_sky_sports_exc_wwe
---3min+
,percentile_prog_3min_plus_sport 
,percentile_prog_3min_plus_sport_sky_sports 
,percentile_prog_3min_plus_wrestling_sky_sports 
,percentile_prog_3min_plus_wrestling_non_sky_sports 
,percentile_prog_3min_plus_wwe  
,percentile_prog_3min_plus_wwe_sky_sports  
,percentile_prog_3min_plus_wwe_sbo 
,percentile_prog_3min_plus_sky_1_or_2 
,percentile_prog_3min_plus_sky_sports_exc_wwe 
---Engaged

,percentile_prog_engaged_sport 
,percentile_prog_engaged_sport_sky_sports 
,percentile_prog_engaged_wrestling_sky_sports 
,percentile_prog_engaged_wrestling_non_sky_sports 
,percentile_prog_engaged_wwe  
,percentile_prog_engaged_wwe_sky_sports  
,percentile_prog_engaged_wwe_sbo 
,percentile_prog_engaged_sky_1_or_2 
,percentile_prog_engaged_sky_sports_exc_wwe
---Repeat for Decile
,decile_minutes_sport 
,decile_minutes_sport_sky_sports 
,decile_minutes_wrestling_sky_sports 
,decile_minutes_wrestling_non_sky_sports 
,decile_minutes_wwe  
,decile_minutes_wwe_sky_sports  
,decile_minutes_wwe_sbo 
,decile_minutes_sky_1_or_2 
,decile_minutes_sky_sports_exc_wwe 
---3min+
,decile_prog_3min_plus_sport 
,decile_prog_3min_plus_sport_sky_sports 
,decile_prog_3min_plus_wrestling_sky_sports 
,decile_prog_3min_plus_wrestling_non_sky_sports 
,decile_prog_3min_plus_wwe  
,decile_prog_3min_plus_wwe_sky_sports  
,decile_prog_3min_plus_wwe_sbo 
,decile_prog_3min_plus_sky_1_or_2 
,decile_prog_3min_plus_sky_sports_exc_wwe

---Engaged
,decile_prog_engaged_sport 
,decile_prog_engaged_sport_sky_sports 
,decile_prog_engaged_wrestling_sky_sports 
,decile_prog_engaged_wrestling_non_sky_sports 
,decile_prog_engaged_wwe  
,decile_prog_engaged_wwe_sky_sports  
,decile_prog_engaged_wwe_sbo 
,decile_prog_engaged_sky_1_or_2 
,decile_prog_engaged_sky_sports_exc_wwe



---Profiling Vars--
,tv_package_group
,b.tenure
,b.isba_tv_region
,CASE b.hh_composition      when   '00' then 	'a) Family'
when '01'	then 'a) Family'
when '02'	then 'a) Family'
when '03'	then 'a) Family'
when '04'	then 'b) Single'
when '05'	then 'b) Single'
when '06'	then 'c) Homesharer'
when '07'	then 'c) Homesharer'
when '08'	then 'c) Homesharer'
when '09'	then 'a) Family'
when '10'	then 'a) Family'
when '11'	then 'c) Homesharer'
when 'U' 	then 'd) Unclassified'
else 'd) Unclassified' end as household_composition
,case when b.cable_area='Y' then 1 else 0 end as cable_area_hh
,b.value_segment
,case when b.affluence_septile is null then 'U' 
        when b.affluence_septile = '0' then '0: Lowest Affluence' 
        when b.affluence_septile = '6' then '6: Highest Affluence' else b.affluence_septile end as affluence_septile_type
,b.box_type_group

,case when c.bb_type in ('1) Fibre','2) Unlimited','3) Everyday','4) Everyday Lite','5) Connect') then 1 else 0 end as has_bb
,case when c.bb_type in ('1) Fibre') then 1 else 0 end as has_bb_fibre
,case when talk_product is not null then 1 else 0 end as has_talk
,case when has_bb=1 and has_talk =1 then 'a) TV, BB and Talk'
      when has_bb=1 and has_talk =0 then 'b) TV and BB'
      when has_bb=0 and has_talk =1 then 'c) TV and Talk' else 'd) TV Only' end as tv_bb_talk
,case   when last_12m_bill_paid<200 then 'a) Under £200'
        when last_12m_bill_paid<300 then 'b) £200-£299'
        when last_12m_bill_paid<400 then 'c) £300-£399'
        when last_12m_bill_paid<500 then 'd) £400-£499'
        when last_12m_bill_paid<600 then 'e) £500-£599'
        when last_12m_bill_paid<700 then 'f) £600-£699'
        when last_12m_bill_paid<800 then 'g) £700-£799' else 'h) £800+' end as last_12mths_bill_amt


--Add in Extra Profile Variables--
,CQM 
,case when adsmartable_hh =1 then 1 else 0 end as adsmartable_household
,social_grade
,case when social_grade in ('A','B','C1') then 1 else 0 end as social_grade_ABC1
,Mirror_Men
,Mirror_Women
,Mirror_has_children as Mirror_Children

,
case mosaic_group
when 'A' then 	'Alpha Territory'
when 'B' then 	'Professional Rewards'
when 'C' then 	'Rural Solitude'
when 'D' then 	'Small Town Diversity'
when 'E' then 	'Active Retirement'
when 'F' then 	'Suburban Mindsets'
when 'G' then 	'Careers and Kids'
when 'H' then 	'New Homemakers'
when 'I' then 	'Ex-Council Community'
when 'J' then 	'Claimant Cultures'
when 'K' then 	'Upper Floor Living'
when 'L' then 	'Elderly Needs'
when 'M' then 	'Industrial Heritage'
when 'N' then 	'Terraced Melting Pot'
when 'O' then 	'Liberal Opinions'
when 'U' then 	'Unclassified'
else null end as h_mosaic_uk_group

,case True_Touch_Type 
when 1 then 'A: Experienced Netizens'
when 2 then 'A: Experienced Netizens'
when 3 then 'A: Experienced Netizens'
when 4 then 'A: Experienced Netizens'
when 5 then 'B: Cyber Tourists'
when 6 then 'B: Cyber Tourists'
when 7 then 'B: Cyber Tourists'
when 8 then 'B: Cyber Tourists'
when 9 then 'C: Digital Culture'
when 10 then 'C: Digital Culture'
when 11 then 'C: Digital Culture'
when 12 then 'D: Modern Media Margins'
when 13 then 'D: Modern Media Margins'
when 14 then 'D: Modern Media Margins'
when 15 then 'D: Modern Media Margins'
when 16 then 'E: Traditional Approach'
when 17 then 'E: Traditional Approach'
when 18 then 'E: Traditional Approach'
when 19 then 'E: Traditional Approach'
when 20 then 'E: Traditional Approach'
when 21 then 	'F: New tech Novices'
when 22 then 'F: New tech Novices'
when 99 then 	'G: Unclassified'
else 'G: Unclassified' end as True_Touch_Group
               
                ,child_hh_00_to_04
                ,child_hh_05_to_11
                ,child_hh_12_to_17
,case financial_stress 
when '0' then '0: Very low'
when '1' then '1: Low'
when '2' then '2: Medium'
when '3' then '3: High'
when '4' then '4: Very high'
when 'U' then '5: Unclassified'
else '5: Unclassified' end as financial_stress_hh


,case when (minutes_sport_sky_sports *account_weight)<0 then 0 else (minutes_sport_sky_sports *account_weight) end  as minutes_sport_sky_sports_total
,case when (minutes_sky_sports_exc_wwe *account_weight)<0 then 0 else (minutes_sky_sports_exc_wwe *account_weight) end  as minutes_sky_sports_exc_wwe_total
,case when (minutes_wwe *account_weight)<0 then 0 else (minutes_wwe *account_weight) end  as minutes_wwe_total
,case when (minutes_wwe_sky_sports *account_weight)<0 then 0 else (minutes_wwe_sky_sports *account_weight) end  as minutes_wwe_sky_sports_total
,case when (minutes_wwe_sbo *account_weight)<0 then 0 else (minutes_wwe_sbo *account_weight) end  as minutes_wwe_sbo_total
,case when (minutes_wwe_sky_1_or_2 *account_weight)<0 then 0 else (minutes_wwe_sky_1_or_2 *account_weight) end  as minutes_wwe_sky_1_or_2_total


,case when (annualised_programmes_3min_plus_wrestling_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wrestling_sky_sports *account_weight) end  as annualised_programmes_3min_plus_wrestling_sky_sports_total
,case when (annualised_programmes_3min_plus_sky_sports_exc_wwe *account_weight)<0 then 0 else (annualised_programmes_3min_plus_sky_sports_exc_wwe *account_weight) end  as annualised_programmes_3min_plus_sky_sports_exc_wwe_total
,case when (annualised_programmes_3min_plus_wrestling_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wrestling_non_sky_sports *account_weight) end  as annualised_programmes_3min_plus_wrestling_non_sky_sports_total
,case when (annualised_programmes_3min_plus_wwe *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe *account_weight) end  as annualised_programmes_3min_plus_wwe_total
,case when (annualised_programmes_3min_plus_wwe_sky_sports *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe_sky_sports *account_weight) end  as annualised_programmes_3min_plus_wwe_sky_sports_total
,case when (annualised_programmes_3min_plus_wwe_sbo *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe_sbo *account_weight) end  as annualised_programmes_3min_plus_wwe_sbo_total
,case when (annualised_programmes_3min_plus_wwe_sky_1_or_2 *account_weight)<0 then 0 else (annualised_programmes_3min_plus_wwe_sky_1_or_2 *account_weight) end  as annualised_programmes_3min_plus_wwe_sky_1_or_2_total


,case when (annualised_programmes_engaged_wrestling_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_wrestling_sky_sports *account_weight) end  as annualised_programmes_engaged_wrestling_sky_sports_total
,case when (annualised_programmes_engaged_sky_sports_exc_wwe *account_weight)<0 then 0 else (annualised_programmes_engaged_sky_sports_exc_wwe *account_weight) end  as annualised_programmes_engaged_sky_sports_exc_wwe_total

,case when (annualised_programmes_engaged_wrestling_non_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_wrestling_non_sky_sports *account_weight) end  as annualised_programmes_engaged_wrestling_non_sky_sports_total
,case when (annualised_programmes_engaged_wwe *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe *account_weight) end  as annualised_programmes_engaged_wwe_total
,case when (annualised_programmes_engaged_wwe_sky_sports *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe_sky_sports *account_weight) end  as annualised_programmes_engaged_wwe_sky_sports_total
,case when (annualised_programmes_engaged_wwe_sbo *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe_sbo *account_weight) end  as annualised_programmes_engaged_wwe_sbo_total
,case when (annualised_programmes_engaged_wwe_sky_1_or_2 *account_weight)<0 then 0 else (annualised_programmes_engaged_wwe_sky_1_or_2 *account_weight) end  as annualised_programmes_engaged_wwe_sky_1_or_2_total

,sky_go_reg_distinct_days_used_L06M  
,sky_go_reg_distinct_days_used_L12M  

,c.hdtv                                
,c.multiroom                           
,c.skyplus                          

into dbarnett.v223_Unbundling_pivot_activity_data_cut_down_for_powerpivot
from v223_unbundling_viewing_summary_by_account  as a
left outer join  v220_zero_mix_active_uk_accounts as b
on a.account_number = b.account_number
left outer join  v223_single_profiling_view as c
on a.account_number = c.account_number

where account_weight>0

;

commit;



grant all on dbarnett.v223_Unbundling_pivot_activity_data_cut_down_for_powerpivot to public;

commit;

-----Extra Code for Phase II Version of Variables---


select account_number
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D') and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D') and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_non_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_sky_sports_football


,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) 
as programmes_engaged_sky_sports_football


,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) 
as programmes_engaged_non_sky_sports_football


into v223_unbundling_viewing_summary_by_account_phase_2
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
--left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
--on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

---Add these variables on to main table---

alter table v223_unbundling_viewing_summary_by_account add 
(viewing_duration_sky_sports_football integer
,viewing_duration_non_sky_sports_football integer
,programmes_3min_plus_sky_sports_football integer
,programmes_3min_plus_non_sky_sports_football integer
,programmes_engaged_sky_sports_football integer
,programmes_engaged_non_sky_sports_football integer

,viewing_duration_sky_sports_exc_wwe integer
,programmes_3min_plus_sky_sports_exc_wwe integer
,programmes_engaged_sky_sports_exc_wwe integer
)
;
commit;

--alter table v223_unbundling_viewing_summary_by_account delete  programmes_engaged_sky_sports__exc_wwe; commit;
--alter table v223_unbundling_viewing_summary_by_account add  programmes_engaged_sky_sports_exc_wwe integer; commit;


commit;
CREATE HG INDEX idx1 ON v223_unbundling_viewing_summary_by_account_phase_2 (account_number);

update v223_unbundling_viewing_summary_by_account
set viewing_duration_sky_sports_football =b.viewing_duration_sky_sports_football
,viewing_duration_non_sky_sports_football =b.viewing_duration_non_sky_sports_football
,programmes_3min_plus_sky_sports_football =b.programmes_3min_plus_sky_sports_football
,programmes_3min_plus_non_sky_sports_football =b.programmes_3min_plus_non_sky_sports_football
,programmes_engaged_sky_sports_football =b.programmes_engaged_sky_sports_football
,programmes_engaged_non_sky_sports_football =b.programmes_engaged_non_sky_sports_football
from v223_unbundling_viewing_summary_by_account as a
left outer join v223_unbundling_viewing_summary_by_account_phase_2 as b
on a.account_number = b.account_number
;


update v223_unbundling_viewing_summary_by_account
 set viewing_duration_sky_sports_exc_wwe =viewing_duration_Sky_Sports_total-viewing_duration_Sky_Sports_WWE
,programmes_3min_plus_sky_sports_exc_wwe =programmes_3min_plus_Sky_Sports_total-programmes_3min_plus_Sky_Sports_WWE
,programmes_engaged_sky_sports_exc_wwe =programmes_engaged_Sky_Sports_total-programmes_engaged_Sky_Sports_WWE
from v223_unbundling_viewing_summary_by_account
;
commit;

--select top 100 viewing_duration_sky_sports_exc_wwe from v223_unbundling_viewing_summary_by_account

----Update Annualised version of figures---

alter table v223_unbundling_viewing_summary_by_account add 
(minutes_sky_sports_football real
,minutes_non_sky_sports_football real
,minutes_sky_sports_exc_wwe real

,annualised_programmes_3min_plus_sky_sports_football real
,annualised_programmes_3min_plus_non_sky_sports_football real
,annualised_programmes_3min_plus_sky_sports_exc_wwe real

,annualised_programmes_engaged_sky_sports_football real
,annualised_programmes_engaged_non_sky_sports_football real
,annualised_programmes_engaged_sky_sports_exc_wwe real
)
;
commit;

update v223_unbundling_viewing_summary_by_account

set minutes_sky_sports_football =(viewing_duration_sky_sports_football
)/60*(365/cast(days_with_viewing as real))
,minutes_non_sky_sports_football =(viewing_duration_non_sky_sports_football
)/60*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_sky_sports_football 
=programmes_3min_plus_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_non_sky_sports_football 
=programmes_3min_plus_non_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_sky_sports_football 
=programmes_engaged_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_non_sky_sports_football
=programmes_engaged_non_sky_sports_football*(365/cast(days_with_viewing as real))

,minutes_sky_sports_exc_wwe =(viewing_duration_sky_sports_exc_wwe
)/60*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_sky_sports_exc_wwe 
=programmes_3min_plus_sky_sports_exc_wwe *(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_sky_sports_exc_wwe 
=programmes_engaged_sky_sports_exc_wwe *(365/cast(days_with_viewing as real))
from v223_unbundling_viewing_summary_by_account
;
commit;

--select top 100 rank_minutes_sky_sports_football from v223_unbundling_viewing_summary_by_account


alter table v223_unbundling_viewing_summary_by_account add 
(
rank_minutes_sky_sports_football integer
,rank_minutes_non_sky_sports_football integer
,rank_prog_3min_plus_sky_sports_football integer
,rank_prog_3min_plus_non_sky_sports_football integer
,rank_prog_engaged_sky_sports_football integer
,rank_prog_engaged_non_sky_sports_football integer

,rank_minutes_sky_sports_exc_wwe integer
,rank_prog_3min_plus_sky_sports_exc_wwe integer
,rank_prog_engaged_sky_sports_exc_wwe integer
)
;
commit;
select account_number

,rank() over (  ORDER BY  minutes_sky_sports_football desc) as rank_minutes_sky_sports_football
,rank() over (  ORDER BY  minutes_non_sky_sports_football desc) as rank_minutes_non_sky_sports_football
,rank() over (  ORDER BY  minutes_sky_sports_exc_wwe desc) as rank_minutes_sky_sports_exc_wwe

,rank() over (  ORDER BY annualised_programmes_3min_plus_sky_sports_football   desc ,  minutes_sky_sports_football desc) as rank_prog_3min_plus_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_non_sky_sports_football   desc ,  minutes_non_sky_sports_football desc) as rank_prog_3min_plus_non_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_sky_sports_exc_wwe   desc ,  minutes_sky_sports_exc_wwe desc) as rank_prog_3min_plus_sky_sports_exc_wwe

,rank() over (  ORDER BY annualised_programmes_engaged_sky_sports_football  desc ,  minutes_sky_sports_football desc) as rank_prog_engaged_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_engaged_non_sky_sports_football  desc ,  minutes_non_sky_sports_football desc) as rank_prog_engaged_non_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_engaged_sky_sports_exc_wwe  desc ,  minutes_sky_sports_exc_wwe desc) as rank_prog_engaged_sky_sports_exc_wwe

into #rank_minutes_details
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;

--select top 500 * from #rank_minutes_details;
exec sp_create_tmp_table_idx '#rank_minutes_details', 'account_number';
commit;


update v223_unbundling_viewing_summary_by_account
set 
rank_minutes_sky_sports_football=b.rank_minutes_sky_sports_football
,rank_minutes_non_sky_sports_football=b.rank_minutes_non_sky_sports_football
,rank_minutes_sky_sports_exc_wwe=b.rank_minutes_sky_sports_exc_wwe

,rank_prog_3min_plus_sky_sports_football=b.rank_prog_3min_plus_sky_sports_football
,rank_prog_3min_plus_non_sky_sports_football=b.rank_prog_3min_plus_non_sky_sports_football
,rank_prog_3min_plus_sky_sports_exc_wwe=b.rank_prog_3min_plus_sky_sports_exc_wwe

,rank_prog_engaged_sky_sports_football=b.rank_prog_engaged_sky_sports_football
,rank_prog_engaged_non_sky_sports_football=b.rank_prog_engaged_non_sky_sports_football
,rank_prog_engaged_sky_sports_exc_wwe=b.rank_prog_engaged_sky_sports_exc_wwe

from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details as b
on a.account_number = b.account_number
;

commit;


---Add on Percentiles and Deciles--

alter table v223_unbundling_viewing_summary_by_account add
(percentile_minutes_sky_sports_football integer
,percentile_minutes_non_sky_sports_football  integer
,percentile_minutes_sky_sports_exc_wwe  integer

,decile_minutes_sky_sports_football integer
,decile_minutes_non_sky_sports_football  integer
,decile_minutes_sky_sports_exc_wwe  integer


,percentile_prog_3min_plus_sky_sports_football integer
,percentile_prog_3min_plus_non_sky_sports_football  integer
,percentile_prog_3min_plus_sky_sports_exc_wwe  integer

,decile_prog_3min_plus_sky_sports_football integer
,decile_prog_3min_plus_non_sky_sports_football  integer
,decile_prog_3min_plus_sky_sports_exc_wwe  integer


,percentile_prog_engaged_sky_sports_football integer
,percentile_prog_engaged_non_sky_sports_football  integer
,percentile_prog_engaged_sky_sports_exc_wwe  integer

,decile_prog_engaged_sky_sports_football integer
,decile_prog_engaged_non_sky_sports_football  integer
,decile_prog_engaged_sky_sports_exc_wwe  integer

)
;





update v223_unbundling_viewing_summary_by_account 
set  
percentile_minutes_sky_sports_football =case when minutes_sky_sports_football =-1 then 999 when minutes_sky_sports_football =0 then 100 else abs(rank_minutes_sky_sports_football /3000)+1 end
,percentile_minutes_non_sky_sports_football =case when minutes_non_sky_sports_football =-1 then 999 when minutes_non_sky_sports_football =0 then 100 else abs(rank_minutes_non_sky_sports_football /3000)+1 end
,percentile_minutes_sky_sports_exc_wwe =case when minutes_sky_sports_exc_wwe =-1 then 999 when minutes_sky_sports_exc_wwe =0 then 100 else abs(rank_minutes_sky_sports_exc_wwe /3000)+1 end

,decile_minutes_sky_sports_football =case when minutes_sky_sports_football =-1 then 99 when minutes_sky_sports_football =0 then 10 else abs(rank_minutes_sky_sports_football /30000)+1 end
,decile_minutes_non_sky_sports_football =case when minutes_non_sky_sports_football =-1 then 99 when minutes_non_sky_sports_football =0 then 10 else abs(rank_minutes_non_sky_sports_football /30000)+1 end
,decile_minutes_sky_sports_exc_wwe =case when minutes_sky_sports_exc_wwe =-1 then 99 when minutes_sky_sports_exc_wwe =0 then 10 else abs(rank_minutes_sky_sports_exc_wwe /30000)+1 end


,percentile_prog_3min_plus_sky_sports_football =case when annualised_programmes_3min_plus_sky_sports_football =-1 then 999 when annualised_programmes_3min_plus_sky_sports_football =0 then 100 else abs(rank_prog_3min_plus_sky_sports_football /3000)+1 end
,percentile_prog_3min_plus_non_sky_sports_football =case when annualised_programmes_3min_plus_non_sky_sports_football =-1 then 999 when annualised_programmes_3min_plus_non_sky_sports_football =0 then 100 else abs(rank_prog_3min_plus_non_sky_sports_football /3000)+1 end
,percentile_prog_3min_plus_sky_sports_exc_wwe =case when annualised_programmes_3min_plus_sky_sports_exc_wwe =-1 then 999 when annualised_programmes_3min_plus_sky_sports_exc_wwe =0 then 100 else abs(rank_prog_3min_plus_sky_sports_exc_wwe /3000)+1 end

,decile_prog_3min_plus_sky_sports_football =case when annualised_programmes_3min_plus_sky_sports_football =-1 then 99 when annualised_programmes_3min_plus_sky_sports_football =0 then 10 else abs(rank_prog_3min_plus_sky_sports_football /30000)+1 end
,decile_prog_3min_plus_non_sky_sports_football =case when annualised_programmes_3min_plus_non_sky_sports_football =-1 then 99 when annualised_programmes_3min_plus_non_sky_sports_football =0 then 10 else abs(rank_prog_3min_plus_non_sky_sports_football /30000)+1 end
,decile_prog_3min_plus_sky_sports_exc_wwe =case when annualised_programmes_3min_plus_sky_sports_exc_wwe =-1 then 99 when annualised_programmes_3min_plus_sky_sports_exc_wwe =0 then 10 else abs(rank_prog_3min_plus_sky_sports_exc_wwe /30000)+1 end

,percentile_prog_engaged_sky_sports_football =case when annualised_programmes_engaged_sky_sports_football =-1 then 999 when annualised_programmes_engaged_sky_sports_football =0 then 100 else abs(rank_prog_engaged_sky_sports_football /3000)+1 end
,percentile_prog_engaged_non_sky_sports_football =case when annualised_programmes_engaged_non_sky_sports_football =-1 then 999 when annualised_programmes_engaged_non_sky_sports_football =0 then 100 else abs(rank_prog_engaged_non_sky_sports_football /3000)+1 end
,percentile_prog_engaged_sky_sports_exc_wwe =case when annualised_programmes_engaged_sky_sports_exc_wwe =-1 then 999 when annualised_programmes_engaged_sky_sports_exc_wwe =0 then 100 else abs(rank_prog_engaged_sky_sports_exc_wwe /3000)+1 end

,decile_prog_engaged_sky_sports_football =case when annualised_programmes_engaged_sky_sports_football =-1 then 99 when annualised_programmes_engaged_sky_sports_football =0 then 10 else abs(rank_prog_engaged_sky_sports_football /30000)+1 end
,decile_prog_engaged_non_sky_sports_football =case when annualised_programmes_engaged_non_sky_sports_football =-1 then 99 when annualised_programmes_engaged_non_sky_sports_football =0 then 10 else abs(rank_prog_engaged_non_sky_sports_football /30000)+1 end
,decile_prog_engaged_sky_sports_exc_wwe =case when annualised_programmes_engaged_sky_sports_exc_wwe =-1 then 99 when annualised_programmes_engaged_sky_sports_exc_wwe =0 then 10 else abs(rank_prog_engaged_sky_sports_exc_wwe /30000)+1 end

from v223_unbundling_viewing_summary_by_account 
;

commit;


----Create New Percentile/Decile calculations----


--Create Macro variables for number of records that have #3min progs >0

create variable @total_programmes_3min_plus_sport integer;
set @total_programmes_3min_plus_sport
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_sport>0);

create variable @total_programmes_3min_plus_sky_sports_football integer;
set @total_programmes_3min_plus_sky_sports_football
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_sky_sports_football>0);

create variable @total_programmes_3min_plus_non_sky_sports_football integer;
set @total_programmes_3min_plus_non_sky_sports_football
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_non_sky_sports_football>0);


create variable @total_programmes_3min_plus_golf_sky_sports integer;
set @total_programmes_3min_plus_golf_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_golf_sky_sports>0);

create variable @total_programmes_3min_plus_golf_non_sky_sports integer;
set @total_programmes_3min_plus_golf_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_golf_non_sky_sports>0);


create variable @total_programmes_3min_plus_rugby_sky_sports integer;
set @total_programmes_3min_plus_rugby_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_rugby_sky_sports>0);

create variable @total_programmes_3min_plus_rugby_non_sky_sports integer;
set @total_programmes_3min_plus_rugby_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_rugby_non_sky_sports>0);


create variable @total_programmes_3min_plus_cricket_sky_sports integer;
set @total_programmes_3min_plus_cricket_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_Sky_Sports_cricket_overall>0);

create variable @total_programmes_3min_plus_cricket_non_sky_sports integer;
set @total_programmes_3min_plus_cricket_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall>0);


create variable @total_programmes_3min_plus_tennis_sky_sports integer;
set @total_programmes_3min_plus_tennis_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_tennis_sky_sports>0);

create variable @total_programmes_3min_plus_tennis_non_sky_sports integer;
set @total_programmes_3min_plus_tennis_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_tennis_non_sky_sports>0);


create variable @total_programmes_3min_plus_f1_sky_sports integer;
set @total_programmes_3min_plus_f1_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_f1_sky_sports>0);

create variable @total_programmes_3min_plus_f1_non_sky_sports integer;
set @total_programmes_3min_plus_f1_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_f1_non_sky_sports>0);


commit;
create variable @total_programmes_3min_plus_sport_sky_sports integer;
set @total_programmes_3min_plus_sport_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_sport_sky_sports>0);



alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Tennis_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Tennis_non_sky_sports integer;



alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_sport_sky_sports integer;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;



update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_sport = case when annualised_programmes_3min_plus_sport=0 then 999 
when abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/100))+1 >100 then 100 
else abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/100))+1 end
,phase_2_decile_engaged_sport = case when annualised_programmes_3min_plus_sport=0 then 99 
when abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/10))+1 end
;
commit;

--@total_programmes_3min_plus_sky_sports_football
update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 999 
when abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 >100 then 100 
else 
abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 end
,phase_2_decile_engaged_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 99 
when abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 999 
when abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 >100 then 100 else 
abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 end
,phase_2_decile_engaged_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 99 
when abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 >10 then 10 else 
abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 end
;
commit;



--rugby

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 999 
when abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 end
,phase_2_decile_engaged_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 99 
when abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 >10 then 10 else  
abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 end
,phase_2_decile_engaged_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 end
;
commit;

---cricket



update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 end
,phase_2_decile_engaged_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 end
,phase_2_decile_engaged_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 end
;
commit;

--F1

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 999 
when abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 end
,phase_2_decile_engaged_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 99 
when abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 end
,phase_2_decile_engaged_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 99  
when abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 end
;
commit;


--Golf

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 999 
when abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 end
,phase_2_decile_engaged_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 99  
when abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 end
,phase_2_decile_engaged_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 end
;
commit;


---Tennis

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 999 
when abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 end
,phase_2_decile_engaged_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 99 
when abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 end
,phase_2_decile_engaged_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 end
;
commit;


---repeat Phase 2 Deciles/Percentiles but for 3min+ Viewing---

commit;


alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Tennis_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Tennis_non_sky_sports integer;


alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_sport_sky_sports integer;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_sport = case when annualised_programmes_3min_plus_sport=0 then 999 
when abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/100))+1 end
,phase_2_decile_3min_plus_sport = case when annualised_programmes_3min_plus_sport=0 then 99 
when abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/10))+1 end
;
commit;

--@total_programmes_3min_plus_sky_sports_football
update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 999 
when abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 >100 then 100 
else 
abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 end
,phase_2_decile_3min_plus_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 99 
when abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 999 
when abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 end
,phase_2_decile_3min_plus_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 99 
when abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 >10 then 10 else 
abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 end
;
commit;



--rugby

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 end
,phase_2_decile_3min_plus_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 >10 then 10 else  
abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 end
;
commit;

---cricket



update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 end
,phase_2_decile_3min_plus_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 end
;
commit;

--F1

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 end
,phase_2_decile_3min_plus_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 99  
when abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 end
;
commit;


--Golf

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 99  
when abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 end
;
commit;


---Tennis

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 end
;
commit;


--select top 100 * from v223_unbundling_viewing_summary_by_account;




alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_sport_sky_sports integer;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;

alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_sport_sky_sports integer;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;




--select *   from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 where programme_instance_name like '%Valencia%' and  programme_instance_name like '%PSG%' 

--select *   from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 where channel_name_inc_hd_staggercast_channel_families= 'ITV1' and cast(broadcast_start_date_time_local as date) ='2013-02-12'
--select *   from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 where programme_instance_name='Real Madrid v Man UtdLive'



--select synopsis  from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 where programme_sub_genre_type='Champions League'

--drop table v223_champions_league_fixtures;
select broadcast_start_date_time_local
,cast(broadcast_start_date_time_local as date) as broadcast_date
,channel_name_inc_hd_staggercast_channel_families
,programme_instance_name
,min(synopsis) as synopsis_type
into v223_champions_league_fixtures
  from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 
where (programme_sub_genre_type='Champions League'
or programme_instance_name in ('Real Madrid v Man UtdLive',
'UCL Celtic v Juventus',
'UEFA Champions League Live...')) and channel_name_inc_hd_staggercast_channel_families in ('ITV1','Sky Sports Channels')
and (programme_instance_name like '%Live%' or  programme_instance_name like 'UCL%') and broadcast_start_date_time_local<'2013-06-01'
group by broadcast_start_date_time_local
,broadcast_date
,channel_name_inc_hd_staggercast_channel_families
,programme_instance_name
order by 
channel_name_inc_hd_staggercast_channel_families
,broadcast_start_date_time_local
,broadcast_date
;

commit;
--select *   from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 where programme_instance_name like '%Madrid%' and  programme_instance_name like '%United%' 
--select * from v223_champions_league_fixtures;
drop table v223_champions_league_fixtures_teams;
create table v223_champions_league_fixtures_teams
            (programme_instance_name varchar(100)
             ,home varchar(40)
             , away varchar(40))

INSERT INTO v223_champions_league_fixtures_teams
            (programme_instance_name
             ,home
             , away)

select  'UCL Helsingborgs v Celtic','Helsingborgs','Celtic'
 union select 'UCL Real Madrid v Manchester City','Real Madrid','Manchester City'
 union select 'UCL Cluj v Manchester United','Cluj','Manchester United'
 union select 'UCL Manchester United v FC Braga','Manchester United','FC Braga'
 union select 'UCL Manchester City v Ajax','Manchester City','Ajax'
 union select 'UCL Juventus v Chelsea','Juventus','Chelsea'
 union select 'UCL Olympiakos v Arsenal','Olympiakos','Arsenal'
 union select 'UCL Arsenal v Bayern Munich','Arsenal','Bayern Munich'
 union select 'UCL Paris St Germain v Barcelona','PSG','Barcelona'
 union select 'UCL Galatasaray SK v Real Madrid','Galatasaray','Real Madrid'
 union select 'UCL Bayern Munich v Barcelona','Bayern Munich','Barcelona'
 union select 'UCL Real Madrid v Dortmund','Real Madrid','Borussia Dortmund'
 union select 'UCL B Dortmund v Bayern Munich','Dortmund','Bayern Munich'
 union select 'Malaga v PanathinaikosLive','Malaga','Panathinaikos'
 union select 'Celtic v HelsingborgsLive','Celtic','Helsingborgs'
 union select 'Montpellier v Arsenal  Live','Montpellier','Arsenal  '
 union select 'Chelsea v Juventus  Live','Chelsea','Juventus  '
 union select 'Manchester Utd v GalatasarayLive','Manchester United','Galatasaray'
 union select 'Celtic v Benfica  Live','Celtic','Benfica  '
 union select 'Spartak v Celtic Live','Spartak Moscow','Celtic '
 union select 'Nordsjaelland v ChelseaLive','Nordsjaelland','Chelsea'
 union select 'Benfica v Barcelona  Live','Benfica','Barcelona  '
 union select 'Zenit v AC MilanLive','Zenit','AC Milan'
 union select 'Manchester City v DortmundLive','Manchester City','Borussia Dortmund'
 union select 'Arsenal v Olympiacos  Live','Arsenal','Olympiacos  '
 union select 'Spartak v BenficaLive','Spartak Moscow','Benfica'
 union select 'Barcelona v CelticLive','Barcelona','Celtic'
 union select 'Shakhtar Donetsk v ChelseaLive','Shakhtar Donetsk','Chelsea'
 union select 'Zenit v AnderlechtLive','Zenit','Anderlecht'
 union select 'Ajax v Manchester CityLive','Ajax','Manchester City'
 union select 'Arsenal v SchalkeLive','Arsenal','Schalke'
 union select 'Schalke v ArsenalLive','Schalke','Arsenal'
 union select 'Real Madrid v DortmundLive','Real Madrid','Borussia Dortmund'
 union select 'Celtic v BarcelonaLive','Celtic','Barcelona'
 union select 'Braga v Man UtdLive','FC Braga','Manchester United'
 union select 'Chelsea v ShakhtarLive','Chelsea','Shakhtar'
 union select 'Spartak Moscow v BarcelonaLive','Spartak Moscow','Barcelona'
 union select 'Benfica v CelticLive','Benfica','Celtic'
 union select 'Galatasaray v Man UtdLive','Galatasaray','Manchester United'
 union select 'Zenit v MalagaLive','Zenit','Malaga'
 union select 'Man City v Real MadridLive','Manchester City','Real Madrid'
 union select 'Arsenal v MontpellierLive','Arsenal','Montpellier'
 union select 'Borussia Dortmund v Man CityLive','Borussia Dortmund','Manchester City'
 union select 'Real Madrid v AjaxLive','Real Madrid','Ajax'
 union select 'Chelsea v NordsjaellandLive','Chelsea','Nordsjaelland'
 union select 'Man Utd v ClujLive','Manchester United','Cluj'
 union select 'Celtic v Spartak MoscowLive','Celtic','Spartak Moscow'
 union select 'AC Milan v BarcelonaLive','AC Milan','Barcelona'
 union select 'Bayern Munich v JuventusLive','Bayern Munich','Juventus'
 union select 'Real Madrid v GalatasarayLive','Real Madrid','Galatasaray'
 union select 'Malaga v Borussia DortmundLive','Malaga','Borussia Dortmund'
 union select 'Borussia Dortmund v MalagaLive','Borussia Dortmund','Malaga'
 union select 'Barcelona v PSGLive','Barcelona','PSG'
 union select 'Juventus v Bayern MunichLive','Juventus','Bayern Munich'
 union select 'B. Dortmund v Real MadridLive','Borussia Dortmund','Real Madrid'
 union select 'Barcelona v Bayern MunichLive','Barcelona','Bayern Munich'
 union select 'Live Champions League Final','Borussia Dortmund','Bayern Munich'

union select 'Real Madrid v Man UtdLive','Real Madrid','Manchester United'
union select 'UCL Celtic v Juventus','Celtic','Juventus'
union select 'UEFA Champions League Live...','Celtic','Juventus'

;

commit;

--select * from v223_champions_league_fixtures_teams;

---Add details back to epg data

alter table v223_champions_league_fixtures add (home varchar(40)
                                                , away varchar(40))
;

commit;

--select * from v223_champions_league_fixtures;

update v223_champions_league_fixtures
set home = b.home
,away=b.away
from v223_champions_league_fixtures as a
left outer join v223_champions_league_fixtures_teams as b
on a.programme_instance_name=b.programme_instance_name
;
commit;

---Get all Champions League Viewing---

---Get all DIM values for these fixtures from EPG table--
drop table v223_all_champions_league_DIM;
select a.*
,b.dk_programme_instance_dim
,b.programme_instance_duration
into v223_all_champions_league_DIM
from v223_champions_league_fixtures as a
left outer join  dbarnett.v223_sports_epg_lookup_aug_12_jul_13  as b
on a.channel_name_inc_hd_staggercast_channel_families=b.channel_name_inc_hd_staggercast_channel_families
 and a.broadcast_start_date_time_local=b.broadcast_start_date_time_local
;
commit;

--select * from v223_all_champions_league_DIM;
--select top 100 * from dbarnett.v223_all_sports_programmes_viewed;
--select top 100 * from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 ;

CREATE HG INDEX idx1 ON v223_all_champions_league_DIM (dk_programme_instance_dim);
drop table v223_all_champions_league_viewing;
select a.account_number
,a.viewing_duration
,b.*
,case when viewing_duration>=180 then 1 else 0 end as viewing_3min_plus
,case when viewing_duration>=900 then 1 else 0 end as viewing_15min_plus
,case when viewing_duration>=1800 then 1 else 0 end as viewing_30min_plus
,case when viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1 else 0 end as programmes_viewed_engaged
,case when home='Arsenal' then 1 when away='Arsenal' then 1 else 0 end as Arsenal
,case when home='Chelsea' then 1 when away='Chelsea' then 1 else 0 end as Chelsea
,case when home='Manchester United' then 1 when away='Manchester United' then 1 else 0 end as Manchester_United
,case when home='Manchester City' then 1 when away='Manchester City' then 1 else 0 end as Manchester_City
into v223_all_champions_league_viewing
from dbarnett.v223_all_sports_programmes_viewed as a
left outer join v223_all_champions_league_DIM as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim 
where b.dk_programme_instance_dim is not null
;

commit;



--select top 100 * from v223_all_champions_league_viewing;

--select top 100 * from dbarnett.v223_daily_viewing_duration;
--select top 100 * from dbarnett.v223_champions_league_fixtures;
--drop table v223_all_accounts_possible_and_actual_CL_days;

select channel_name_inc_hd_staggercast_channel_families
,broadcast_date
,programme_instance_name
,1 as match_key
into #distinct_channel_days
from dbarnett.v223_champions_league_fixtures
group by channel_name_inc_hd_staggercast_channel_families
,broadcast_date
,programme_instance_name
;

--Calculate number of possible viewing days per account for those with a weight---
select account_number
,1 as match_key
into #accounts_for_viewing
from v223_unbundling_viewing_summary_by_account 
where account_weight>0
;

---Create cartesian product of all accounts and all possible fixtures--
drop table v223_all_accounts_possible_and_actual_CL_days;
select b.account_number
,a.channel_name_inc_hd_staggercast_channel_families
,a.broadcast_date
,a.programme_instance_name
into v223_all_accounts_possible_and_actual_CL_days
from #distinct_channel_days as a
left outer join #accounts_for_viewing as b
on a.match_key=b.match_key
;
commit;


--Add flag on for dates that account returned data---

alter table v223_all_accounts_possible_and_actual_CL_days add viewing_data_day tinyint;


CREATE HG INDEX idx1 ON v223_all_accounts_possible_and_actual_CL_days (account_number);

update v223_all_accounts_possible_and_actual_CL_days
set viewing_data_day=case when b.viewing_duration_post_5am>0 then 1 else 0 end
from v223_all_accounts_possible_and_actual_CL_days as a
left outer join dbarnett.v223_daily_viewing_duration as b
on a.account_number = b.account_number and a.broadcast_date=b.viewing_date 
;
commit;

--select top 100 * from v223_all_accounts_possible_and_actual_CL_days

--select sum(viewing_data_day) , count(*) from v223_all_accounts_possible_and_actual_CL_days
---aggregate to deal with where someone may watch same programme on different dk's e.g., HD/Non HD--

select a.account_number
,a.channel_name_inc_hd_staggercast_channel_families
,a.broadcast_date
,a.programme_instance_name
,max(viewing_3min_plus) as watched_3min_plus
,max(viewing_15min_plus) as watched_15min_plus
,max(viewing_30min_plus) as watched_30min_plus
,max(programmes_viewed_engaged) as watched_engaged
,max(Arsenal) as arsenal_fixture
,max(Chelsea) as chelsea_fixture
,max(Manchester_United) as manchester_united_fixture
,max(Manchester_City) as man_city_fixture
,sum(viewing_duration) as total_seconds_viewed
into #v223_all_champions_league_viewing_deduped
from v223_all_champions_league_viewing as a
group by a.account_number
,a.channel_name_inc_hd_staggercast_channel_families
,a.broadcast_date
,a.programme_instance_name
;

---Add on actual Viewing

alter table v223_all_accounts_possible_and_actual_CL_days add watched_3min_plus tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add watched_15min_plus tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add watched_30min_plus tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add watched_engaged tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add arsenal_fixture tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add chelsea_fixture tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add manchester_united_fixture tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add man_city_fixture tinyint;
alter table v223_all_accounts_possible_and_actual_CL_days add total_seconds_viewed integer;

commit;

update v223_all_accounts_possible_and_actual_CL_days
set watched_3min_plus=b.watched_3min_plus
,watched_15min_plus=b.watched_15min_plus
,watched_30min_plus=b.watched_30min_plus
,watched_engaged=b.watched_engaged
,arsenal_fixture=b.arsenal_fixture
,chelsea_fixture=b.chelsea_fixture
,manchester_united_fixture=b.manchester_united_fixture
,man_city_fixture=b.man_city_fixture
,total_seconds_viewed=b.total_seconds_viewed
from v223_all_accounts_possible_and_actual_CL_days as a
left outer join #v223_all_champions_league_viewing_deduped as b
on a.account_number=b.account_number
and a.channel_name_inc_hd_staggercast_channel_families=b.channel_name_inc_hd_staggercast_channel_families
and a.broadcast_date=b.broadcast_date
and a.programme_instance_name=b.programme_instance_name
;
commit;

--select top 100 * from v223_all_champions_league_DIM;

select max(broadcast_start_date_time_local) as bcast_time
,programme_instance_name
into #programme_braodcast_time
from v223_all_champions_league_DIM
group by programme_instance_name
;
commit;



alter table v223_all_accounts_possible_and_actual_CL_days add broadcast_start_date_time_local datetime;

update v223_all_accounts_possible_and_actual_CL_days
set broadcast_start_date_time_local = b.bcast_time
from v223_all_accounts_possible_and_actual_CL_days as a
left outer join #programme_braodcast_time as b
on a.programme_instance_name=b.programme_instance_name
commit;


--select programme_instance_name ,count(*) as accounts, sum(viewing_data_day) from v223_all_accounts_possible_and_actual_CL_days group by programme_instance_name order by programme_instance_name

--select top 100 * from v223_all_accounts_possible_and_actual_CL_days;
/*
select * from v223_champions_league_fixtures order by broadcast_start_date_time_local
,broadcast_start_date_time_local
,channel_name_inc_hd_staggercast_channel_families
*/

---Create Definition of Loyalty by Replicating EPL work--
select account_number
,channel_name_inc_hd_staggercast_channel_families
,broadcast_start_date_time_local
,max(viewing_data_day) as could_have_watched
,max(case when watched_15min_plus=1 then 1 else 0 end) as did_watch
into #summary_by_rights_broadcast_time
from v223_all_accounts_possible_and_actual_CL_days
group by account_number
,channel_name_inc_hd_staggercast_channel_families
,broadcast_start_date_time_local
;

--select * from #summary_by_rights_broadcast_time;
--select sum(could_have_watched) , sum(did_watch) from #summary_by_rights_broadcast_time;

select account_number
,sum(case when channel_name_inc_hd_staggercast_channel_families = 'Sky Sports Channels' then could_have_watched else 0 end) as sky_could_have_watched
,sum(case when channel_name_inc_hd_staggercast_channel_families = 'Sky Sports Channels' then did_watch else 0 end) as sky_did_watch

,sum(case when channel_name_inc_hd_staggercast_channel_families = 'ITV1' then could_have_watched else 0 end) as ITV_could_have_watched
,sum(case when channel_name_inc_hd_staggercast_channel_families = 'ITV1' then did_watch else 0 end) as ITV_did_watch
into #summary_by_account
from #summary_by_rights_broadcast_time
group by account_number
;



------Add on BB Activity and BT Sport Activity---


--BB_TYPE;
--drop table #all_bb_ever;
Select           account_number   
,max(case when      effective_to_dt='9999-09-09' then 1 else 0 end) as active_now   
INTO            #all_bb_ever
FROM            sk_prod.cust_subs_hist
WHERE           subscription_sub_type = 'Broadband DSL Line'
and             effective_from_dt != effective_to_dt
and             (status_code IN ('AC','AB') 
                OR (status_code='PC' and prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
                OR (status_code='CF' AND prev_status_code='PC')
                OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY        account_number              
;
commit;
--select count(*) , sum(active_now) from #all_bb_ever;

---BT Sport Viewer--
select account_number
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #aug_bt_sport
from sk_prod.vespa_dp_prog_viewed_201308
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and service_key in (3625,3627,3661,3663)
group by account_number
;

select account_number
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #sep_bt_sport
from sk_prod.vespa_dp_prog_viewed_201309
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and service_key in (3625,3627,3661,3663)
group by account_number
;

select account_number
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
into #oct_bt_sport
from sk_prod.vespa_dp_prog_viewed_201310
where  capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12 and service_key in (3625,3627,3661,3663)
group by account_number
;

---Add together--

select * into #all_bt_sport from #aug_bt_sport;

insert into #all_bt_sport
(select * from #sep_bt_sport)
;


insert into #all_bt_sport
(select * from #oct_bt_sport)
;
commit;

select account_number
,sum(viewing_duration) as total_duration
into #bt_sport_viewing
from #all_bt_sport
group by account_number
;
commit;

---Create Sky Sports and ITV CL Loyalty Segments--
select account_number
, case when cast(sky_did_watch AS real) / cast(sky_could_have_watched AS real) >=0.5 then '01: High'
       when cast(sky_did_watch AS real) / cast(sky_could_have_watched AS real) >=0.2 then '02: Medium'
       when cast(sky_did_watch AS real)>0 then '03: Low' else '04: None' end as sky_cl_loyalty
, case when cast(itv_did_watch AS real) / cast(itv_could_have_watched AS real) >=0.5 then '01: High'
       when cast(itv_did_watch AS real) / cast(itv_could_have_watched AS real) >=0.2 then '02: Medium'
       when cast(itv_did_watch AS real)>0 then '03: Low' else '04: None' end itv_cl_loyalty
into #loyalty_summary
from #summary_by_account
--group by account_number
;

--select sky_cl_loyalty , count(*) from #loyalty_summary group by sky_cl_loyalty


--select * from #all_bt_sport;


alter table dbarnett.v223_Unbundling_pivot_activity_data add sky_cl_loyalty varchar(20);
alter table dbarnett.v223_Unbundling_pivot_activity_data add itv_cl_loyalty varchar(20);
alter table dbarnett.v223_Unbundling_pivot_activity_data add bb_status_ever varchar(10);
alter table dbarnett.v223_Unbundling_pivot_activity_data add bt_sport_viewer tinyint;

update dbarnett.v223_Unbundling_pivot_activity_data
set sky_cl_loyalty= case when b.account_number is null then '04: None' else b.sky_cl_loyalty end
,itv_cl_loyalty= case when b.account_number is null then '04: None' else b.itv_cl_loyalty end

from dbarnett.v223_Unbundling_pivot_activity_data as a
left outer join #loyalty_summary as b
on a.account_number = b.account_number
;
commit;

update dbarnett.v223_Unbundling_pivot_activity_data
set bb_status_ever= case when active_now=1 then '01: Has BB' when active_now=0 then '02: Had BB' else '03: Never Had'  end
from dbarnett.v223_Unbundling_pivot_activity_data as a
left outer join #all_bb_ever as b
on a.account_number = b.account_number
;
commit;

update dbarnett.v223_Unbundling_pivot_activity_data
set bt_sport_viewer= case when total_duration>180 then 1 else 0  end
from dbarnett.v223_Unbundling_pivot_activity_data as a
left outer join #bt_sport_viewing as b
on a.account_number = b.account_number
;
commit;

/*
select sky_cl_loyalty , sum(account_weight),sum(bt_sport_viewer) from dbarnett.v223_Unbundling_pivot_activity_data group by sky_cl_loyalty order by sky_cl_loyalty
select itv_cl_loyalty , sum(account_weight) from dbarnett.v223_Unbundling_pivot_activity_data group by itv_cl_loyalty order by itv_cl_loyalty
select bb_status_ever , sum(account_weight),sum(bt_sport_viewer) from dbarnett.v223_Unbundling_pivot_activity_data group by bb_status_ever order by bb_status_ever
*/
grant all on dbarnett.v223_Unbundling_pivot_activity_data to public;
commit;


--Add on Churn Activity----

select          account_number
                ,effective_from_dt as status_change_date
                ,case 
                when status_code ='AB' then 'Active Blocked'
                when status_code ='PC' then 'Pend Cancel'
                when status_code ='PO' then 'Cuscan'
                when status_code ='SC' then 'Syscan'
                else 'Not churn' end as status_201311
                ,rank() over (partition by  csh.account_number 
                order by  csh.effective_from_dt,csh.cb_row_id) as churn_rank--Rank to get the first event
into            #V223_status_change_201311
from            sk_prod.cust_subs_hist as csh
where           subscription_sub_type ='DTV Primary Viewing'     
and             status_code in ('PO','SC')                       
and             status_code_changed = 'Y' 
and             effective_from_dt >= '2013-08-01'
and             effective_from_dt <= '2013-11-30'                 
and             effective_from_dt != effective_to_dt
order by        account_number
;
delete from #V223_status_change_201311       
where churn_rank>1;

---Add on Phase II Deciles/Percentiles for CL and EPL (and Other football areas)-----
--select top 100 * from #V223_status_change_201311;
commit;
CREATE HG INDEX idx1 ON #V223_status_change_201311  (account_number);
alter table dbarnett.v223_Unbundling_pivot_activity_data add churn_since_aug_2013 tinyint;

update dbarnett.v223_Unbundling_pivot_activity_data
set churn_since_aug_2013= case when b.account_number is not null then 1 else 0 end
from dbarnett.v223_Unbundling_pivot_activity_data as a
left outer join #V223_status_change_201311   as b
on a.account_number = b.account_number
;
commit;

--select sum(churn_since_aug_2013) , count(*) from dbarnett.v223_Unbundling_pivot_activity_data











--select top 10 * from v223_unbundling_viewing_summary_by_account;
--select @total_programmes_3min_plus_Tennis_non_sky_sports
/*
alter table v223_unbundling_viewing_summary_by_account  delete phase_2_percentile_engaged_Tennis_non_sky_sports;
alter table v223_unbundling_viewing_summary_by_account  delete phase_2_decile_engaged_Tennis_non_sky_sports;
commit;
*/



--select @total_programmes_3min_plus_sport
--select decile_prog_engaged_sky_sports_exc_wwe , count(*) from v223_unbundling_viewing_summary_by_account group by decile_prog_engaged_sky_sports_exc_wwe order by decile_prog_engaged_sky_sports_exc_wwe
--select phase_2_percentile_engaged_Football_sky_sports , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_engaged_Football_sky_sports order by phase_2_percentile_engaged_Football_sky_sports
--select phase_2_percentile_engaged_Tennis_non_sky_sports , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_engaged_Tennis_non_sky_sports order by phase_2_percentile_engaged_Tennis_non_sky_sports

--select phase_2_decile_engaged_Tennis_non_sky_sports , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_decile_engaged_Tennis_non_sky_sports order by phase_2_decile_engaged_Tennis_non_sky_sports

commit;
--select phase_2_percentile_engaged_sport , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_engaged_sport order by phase_2_percentile_engaged_sport
--select phase_2_percentile_3min_plus_sport , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_3min_plus_sport order by phase_2_percentile_3min_plus_sport



--select * from v223_unbundling_viewing_summary_by_account where phase_2_percentile_engaged_Football_sky_sports=100

--select @total_programmes_3min_plus_sky_sports_football;
/*
select abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/10))+1 as val2
,rank_prog_engaged_sport
,@total_programmes_3min_plus_sport
from v223_unbundling_viewing_summary_by_account
;
*/

/* select top 500 account_number
,minutes_sky_sports_football_total
,minutes_non_sky_sports_football_total
,annualised_programmes_3min_plus_sky_sports_football_total 
,annualised_programmes_3min_plus_non_sky_sports_football_total
,annualised_programmes_engaged_sky_sports_football_total 
,annualised_programmes_engaged_non_sky_sports_football_total


,phase_2_percentile_engaged_sport 
,phase_2_decile_engaged_sport 
,phase_2_percentile_engaged_football_sky_sports 
,phase_2_decile_engaged_football_sky_sports 
,phase_2_percentile_engaged_football_non_sky_sports 
,phase_2_decile_engaged_football_non_sky_sports 
,phase_2_percentile_engaged_rugby_sky_sports 
,phase_2_decile_engaged_rugby_sky_sports 
,phase_2_percentile_engaged_rugby_non_sky_sports 
,phase_2_decile_engaged_rugby_non_sky_sports 
,phase_2_percentile_engaged_cricket_sky_sports 
,phase_2_decile_engaged_cricket_sky_sports 
,phase_2_percentile_engaged_cricket_non_sky_sports 
,phase_2_decile_engaged_cricket_non_sky_sports 
,phase_2_percentile_engaged_F1_sky_sports 
,phase_2_decile_engaged_F1_sky_sports 
,phase_2_percentile_engaged_F1_non_sky_sports 
,phase_2_decile_engaged_F1_non_sky_sports 
,phase_2_percentile_engaged_Golf_sky_sports 
,phase_2_decile_engaged_Golf_sky_sports 
,phase_2_percentile_engaged_Golf_non_sky_sports 
,phase_2_decile_engaged_Golf_non_sky_sports 
,phase_2_percentile_engaged_Tennis_sky_sports 
,phase_2_decile_engaged_Tennis_sky_sports 
,phase_2_percentile_engaged_Tennis_non_sky_sports 
,phase_2_decile_engaged_Tennis_non_sky_sports
from dbarnett.v223_Unbundling_pivot_activity_data
*/

commit;

---Rugby Analysis---

select channel_name_inc_hd_staggercast_channel_families , programme_sub_genre_type , programme_instance_name , synopsis , sum(total_duration_viewed) as tot_view 
from dbarnett.v223_sports_epg_lookup_aug_12_jul_13
where sub_genre_description = 'Rugby'
group by channel_name_inc_hd_staggercast_channel_families,programme_sub_genre_type , programme_instance_name , synopsis
order by tot_view desc

--where upper(synopsis)  like '%HEINEKEN CUP%'
--select top 100 * from  dbarnett.v223_sports_epg_lookup_aug_12_jul_13

create table v223_rugby_heineken_or_premiership
            (sub_genre_type varchar(100)
             ,programme_instance_name varchar(350)
             , synopsis varchar(350))

INSERT INTO v223_rugby_heineken_or_premiership
            (sub_genre_type 
             ,programme_instance_name 
             , synopsis )

select 'Heineken Cup','European Rugby','Ulster welcome Castres to Ravenhill in a Pool 4 match in the Heineken Cup. The hosts are in blistering form coming into their first European match of the season and have yet to lose a game.'
union select 'Heineken Cup','European Rugby','Ulster welcome Castres to Ravenhill in a Pool 4 match in the Heineken Cup. The hosts are in blistering form coming into their first European match of the season and have yet to lose a game. Also in HD'
union select 'Heineken Cup','European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors. (3D TV required)'
union select 'Heineken Cup','European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors. (3D TV required)'
union select 'Heineken Cup','European Rugby','Glasgow Warriors face Ulster at Scotstoun Stadium in pool four of the Heineken Cup. The hosts were beaten by Northampton in round one while Ulster outclassed Castres.'
union select 'Heineken Cup','European Rugby','Glasgow Warriors face Ulster at Scotstoun Stadium in pool four of the Heineken Cup. The hosts were beaten by Northampton in round one while Ulster outclassed Castres. Also in HD'
union select 'Heineken Cup','European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the pool stage of the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors.'
union select 'Heineken Cup','European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the pool stage of the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors. Also in HD'
union select 'Heineken Cup','Heineken Cup Final','Clermont Auvergne take on Toulon at Dublin''s Aviva Stadium in the Heineken Cup final. Clermont beat Munster in the semi-finals, while Toulon dispatched Saracens.'
union select 'Heineken Cup','Heineken Cup Final','Clermont Auvergne take on Toulon at Dublin''s Aviva Stadium in the Heineken Cup final. Clermont beat Munster in the semi-finals, while Toulon dispatched Saracens. Also in HD'
union select 'Heineken Cup','Heineken Cup Final','Clermont Auvergne face Toulon at Dublin''s Aviva Stadium in the 2013 Heineken Cup final. Clermont beat Munster in the semi-finals, while Toulon saw off Saracens. (3D TV required)'
union select 'Heineken Cup','Heineken Cup Final','Clermont Auvergne face Toulon at Dublin''s Aviva Stadium in the 2013 Heineken Cup final. Clermont beat Munster in the semi-finals, while Toulon saw off Saracens. (3D TV required)'
union select 'Heineken Cup','Heineken Cup Quarter Final','Saracens take on Ulster at Twickenham Stadium in the quarter-finals of the Heineken Cup. Both teams come into this game on the back of respective domestic victories. (3D TV required)'
union select 'Heineken Cup','Live Cardiff v Toulon','Cardiff Blues welcome Toulon to Cardiff Arms Park in a Pool Six match in the Heineken Cup. The Blues were edged out by Sale last weekend while Toulon overpowered Montpellier.'
union select 'Heineken Cup','Live Clermont v Exeter','Clermont Auvergne meet Exeter Chiefs at the Stade Marcel Michelin in round five of the Heineken Cup pool stage. The Chiefs must win to keep their quarter-final dreams alive.'
union select 'Heineken Cup','Live Clermont v Scarlets','Clermont Auvergne entertain Scarlets at the Stade Marcel Michelin in Pool 5 of the Heineken Cup. The Frenchmen won both matches between the sides when they met in the 2007/08 competition.'
union select 'Heineken Cup','Live Connacht v Biarritz','Connacht welcome Biarritz to The Sportsground in round three of the Heineken Cup pool stage. Connacht can move above their visitors in pool three with a victory this evening.'
union select 'Heineken Cup','Live Connacht v Harlequins','Connacht and the Harlequins meet at The Sportsground in Pool Three of the Heineken Cup. The Quins thrashed Biarritz last weekend and will look for another maximum haul here.'
union select 'Heineken Cup','Live Connacht v Zebre','Connacht host Zebre at the Sportsground in the sixth round of the Heineken Cup. The Italians will be hoping to end on a positive note after losing all of their five previous pool games.'
union select 'Heineken Cup','Live Edinburgh v Saracens','Edinburgh welcome Saracens to Murrayfield for their Pool 1 clash in the Heineken Cup. The Scots made history by reaching the semi-finals of last season''s competition.'
union select 'Heineken Cup','Live European Rugby','Leinster v Exeter at RDS in Pool 5 of the Heineken Cup. Press the red button for Clermont Auvergne''s clash with Scarlets and Western Province''s Currie Cup meeting with Free State Cheetahs.'
union select 'Heineken Cup','Live European Rugby','Leinster v Exeter at RDS in Pool 5 of the Heineken Cup. Press the red button for Clermont Auvergne''s clash with Scarlets and Western Province''s Currie Cup meeting with Free State Cheetahs. Also in HD'
union select 'Heineken Cup','Live European Rugby','Clermont Auvergne and Montpellier contest their Heineken Cup quarter-final at Stade Marcel Michelin. From 4pm, press the red button for the Super Rugby tie between Cheetahs and Stormers.'
union select 'Heineken Cup','Live European Rugby','Clermont Auvergne and Montpellier contest their Heineken Cup quarter-final at Stade Marcel Michelin. From 4pm, press the red button for the Super Rugby tie between Cheetahs and Stormers. Also in HD'
union select 'Heineken Cup','Live European Rugby','Munster meet Edinburgh at Thomond Park in pool one of the Heineken Cup. Press the red button for the pool six clash between Cardiff and Toulon at Cardiff Arms Park.'
union select 'Heineken Cup','Live European Rugby','Munster meet Edinburgh at Thomond Park in pool one of the Heineken Cup. Press the red button for the pool six clash between Cardiff and Toulon at Cardiff Arms Park. Also in HD'
union select 'Heineken Cup','Live European Rugby','Ulster welcome Castres to Ravenhill in a Pool 4 match in the Heineken Cup. Press the red button for Ospreys'' Pool 2 meeting with Benetton Treviso at the Liberty Stadium.'
union select 'Heineken Cup','Live European Rugby','Scarlets meet Leinster at Parc y Scarlets in pool five of the Heineken Cup. Press the red button for Benetton Treviso and Toulouse''s pool two clash at the Stadio Comunale di Monigo. Also in HD'
union select 'Heineken Cup','Live European Rugby','Scarlets meet Leinster at Parc y Scarlets in pool five of the Heineken Cup. Press the red button for Benetton Treviso and Toulouse''s pool two clash at the Stadio Comunale di Monigo.'
union select 'Heineken Cup','Live European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors. (3D TV required)'
union select 'Heineken Cup','Live European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors. (3D TV required)'
union select 'Heineken Cup','Live European Rugby','Munster meet Saracens at Thomond Park in the third round of the Heineken Cup. Press the red button for Racing Metro 92 v Edinburgh and day one of the IRB Sevens Series in South Africa.'
union select 'Heineken Cup','Live European Rugby','Munster meet Saracens at Thomond Park in the third round of the Heineken Cup. Press the red button for Racing Metro 92 v Edinburgh and day one of the IRB Sevens Series in South Africa. Also in HD'
union select 'Heineken Cup','Live European Rugby','Leicester Tigers face Ospreys at Welford Road in pool two of the Heineken Cup. Press the red button for the pool six contest between Montpellier and Sale at the Stade Yves du Manoir.'
union select 'Heineken Cup','Live European Rugby','Leicester Tigers face Ospreys at Welford Road in pool two of the Heineken Cup. Press the red button for the pool six contest between Montpellier and Sale at the Stade Yves du Manoir. Also in HD'
union select 'Heineken Cup','Live European Rugby','Toulon face Montpellier at Stade Mayol in the first round of the Heineken Cup pool stage. Montpellier will be out for revenge after losing out to Toulon in their Top 14 match last month. Also in HD'
union select 'Heineken Cup','Live European Rugby','Toulon face Montpellier at Stade Mayol in the first round of the Heineken Cup pool stage. Montpellier will be out for revenge after losing out to Toulon in their Top 14 match last month.'
union select 'Heineken Cup','Live European Rugby','Glasgow Warriors v Ulster at Scotstoun Stadium in pool four of the Heineken Cup. Press the red button for the pool''s other clash between Castres and Northampton at the Stade Ernest Wallon. Also in HD'
union select 'Heineken Cup','Live European Rugby','Glasgow Warriors face Ulster at Scotstoun Stadium in pool four of the Heineken Cup. The hosts were beaten by Northampton in round one while Ulster outclassed Castres.'
union select 'Heineken Cup','Live European Rugby','Clermont Auvergne v Leinster at Parc des Sports Marcel Michelin in the Heineken Cup. Press the red button for Leicester v Benetton Treviso and day two of the IRB Sevens in South Africa.'
union select 'Heineken Cup','Live European Rugby','Exeter Chiefs meet Clermont Auvergne at Sandy Park in pool five of the Heineken Cup. Press the red button for the Currie Cup semi-final between the Golden Lions and Western Province.'
union select 'Heineken Cup','Live European Rugby','Clermont Auvergne v Leinster at Parc des Sports Marcel Michelin in the Heineken Cup. Press the red button for Leicester v Benetton Treviso and day two of the IRB Sevens in South Africa. Also in HD'
union select 'Heineken Cup','Live European Rugby','Exeter Chiefs meet Clermont Auvergne at Sandy Park in pool five of the Heineken Cup. Press the red button for the Currie Cup semi-final between the Golden Lions and Western Province. Also in HD'
union select 'Heineken Cup','Live European Rugby','Harlequins host Munster at Twickenham Stoop in the quarter-finals of the Heineken Cup. The hosts will be hoping for a change in fortune after losing their last three league games.'
union select 'Heineken Cup','Live European Rugby','Harlequins host Munster at Twickenham Stoop in the quarter-finals of the Heineken Cup. The hosts will be hoping for a change in fortune after losing their last three league games. Also in HD'
union select 'Heineken Cup','Live European Rugby','Sale Sharks host Toulon at Salford City Stadium in the third round of the Heineken Cup. Press the red button for day one of the IRB Sevens Series in South Africa.'
union select 'Heineken Cup','Live European Rugby','Sale Sharks host Toulon at Salford City Stadium in the third round of the Heineken Cup. Press the red button for day one of the IRB Sevens Series in South Africa. Also in HD'
union select 'Heineken Cup','Live European Rugby','Saracens take on Ulster at Twickenham Stadium in the quarter-finals of the Heineken Cup. Both teams come into this game on the back of respective domestic victories.'
union select 'Heineken Cup','Live European Rugby','Saracens take on Ulster at Twickenham Stadium in the quarter-finals of the Heineken Cup. Both teams come into this game on the back of respective domestic victories. Also in HD and 3D'
union select 'Heineken Cup','Live European Rugby','Saracens face Racing Metro at the King Baudouin Stadium in pool one of the Heineken Cup. Press the red button for the Currie Cup semi-final between the Natal Sharks and the Blue Bulls.'
union select 'Heineken Cup','Live European Rugby','Saracens face Racing Metro at the King Baudouin Stadium in pool one of the Heineken Cup. Press the red button for the Currie Cup semi-final between the Natal Sharks and the Blue Bulls. Also in HD'
union select 'Heineken Cup','Live European Rugby','Toulon take on Leicester Tigers at Stade Mayol in the quarter-finals of the Heineken Cup. The Tigers come into this tie on the back of four wins in their last six league fixtures.'
union select 'Heineken Cup','Live European Rugby','Toulon take on Leicester Tigers at Stade Mayol in the quarter-finals of the Heineken Cup. The Tigers come into this tie on the back of four wins in their last six league fixtures. Also in HD'
union select 'Heineken Cup','Live European Rugby','Racing Metro take on Munster at Stade de France in this Pool 1 clash from the Heineken Cup. Press the red button for Edinburgh''s clash with Saracens at Murrayfield in Pool 1.'
union select 'Heineken Cup','Live European Rugby','Racing Metro take on Munster at Stade de France in this Pool 1 clash from the Heineken Cup. Press the red button for Edinburgh''s clash with Saracens at Murrayfield in Pool 1. Also in HD'
union select 'Heineken Cup','Live European Rugby','Cardiff Blues meet Montpellier at Cardiff Arms Park in the third round of the Heineken Cup. Press the red button for day two of the IRB Sevens Series in South Africa. Also in HD'
union select 'Heineken Cup','Live European Rugby','Cardiff Blues meet Montpellier at Cardiff Arms Park in the third round of the Heineken Cup. Press the red button for day two of the IRB Sevens Series in South Africa.'
union select 'Heineken Cup','Live European Rugby','Harlequins host Biarritz at Twickenham Stoop in the first round of the 2012/13 Heineken Cup. Press the red button for the Currie Cup clash between the Golden Lions and the Blue Bulls.'
union select 'Heineken Cup','Live European Rugby','Harlequins host Biarritz at Twickenham Stoop in the first round of the 2012/13 Heineken Cup. Press the red button for the Currie Cup clash between the Golden Lions and the Blue Bulls. Also in HD'
union select 'Heineken Cup','Live European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the pool stage of the Heineken Cup. Press the red button for Connacht''s clash with Biarritz at The Sportsground. Also in HD & 3D Also in HD'
union select 'Heineken Cup','Live European Rugby','Northampton Saints take on Ulster at Franklin''s Gardens in the pool stage of the Heineken Cup. The home side lost out to Castres in their last match, while Ulster saw off Glasgow Warriors.'
union select 'Heineken Cup','Live European Rugby','Northampton Saints face Glasgow Warriors at Franklin''s Gardens in round one of the Heineken Cup. Press the red button for Sale Sharks'' Pool 6 clash with the Cardiff Blues.'
union select 'Heineken Cup','Live European Rugby','Northampton Saints face Glasgow Warriors at Franklin''s Gardens in round one of the Heineken Cup. Press the red button for Sale Sharks'' Pool 6 clash with the Cardiff Blues. Also in HD'
union select 'Heineken Cup','Live European Rugby','Toulouse take on Leicester Tigers at Stadium Municipal in the pool stage of the 2012/13 Heineken Cup. Toulouse claimed a 22-11 win when the sides last met at this stage in December 2007.'
union select 'Heineken Cup','Live European Rugby','Toulouse take on Leicester Tigers at Stadium Municipal in the pool stage of the 2012/13 Heineken Cup. Toulouse claimed a 22-11 win when the sides last met at this stage in December 2007. Also in HD'
union select 'Heineken Cup','Live European Rugby SemiFinal','Clermont Auvergne take on Munster at Stade de la Mosson in the semi-finals of the Heineken Cup. Press the red button for Super Rugby and day three of the Zurich Classic of New Orleans.'
union select 'Heineken Cup','Live European Rugby SemiFinal','Clermont Auvergne take on Munster at Stade de la Mosson in the semi-finals of the Heineken Cup. Press the red button for Super Rugby and day three of the Zurich Classic of New Orleans. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Saracens meet Munster at Vicarage Road in the Heineken Cup. Press the red button for Toulon''s clash with Sale at the Stade Felix Mayol. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Edinburgh host Munster at Murrayfield in round five of the Heineken Cup pool stage. A victory for Munster should leave them well placed in pool one heading into the final round of games.'
union select 'Heineken Cup','Live European Rugby Union','Edinburgh host Munster at Murrayfield in round five of the Heineken Cup pool stage. A victory for Munster should leave them well placed in pool one heading into the final round of games. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Ulster face Northampton Saints at Ravenhill Stadium in the Heineken Cup. Press the red button for Montpellier''s meeting with Cardiff and, from 7pm, day two of the World Darts Championship.'
union select 'Heineken Cup','Live European Rugby Union','Leinster host Scarlets at the RDS Arena in the fifth round of the Heineken Cup. Press the red button for Clermont Auvergne''s meeting with Exeter Chiefs.'
union select 'Heineken Cup','Live European Rugby Union','Leinster host Scarlets at the RDS Arena in the fifth round of the Heineken Cup. Press the red button for Clermont Auvergne''s meeting with Exeter Chiefs. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Saracens host Edinburgh at Vicarage Road in the sixth round of the Heineken Cup. Press the red button for the game between Munster and Racing Metro at Thomond Park.'
union select 'Heineken Cup','Live European Rugby Union','Saracens host Edinburgh at Vicarage Road in the sixth round of the Heineken Cup. Press the red button for the game between Munster and Racing Metro at Thomond Park. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Exeter Chiefs face Leinster at Sandy Park in the sixth round of the Heineken Cup. The second pool five match, between Scarlets and Clermont Auvergne, is available via the red button.'
union select 'Heineken Cup','Live European Rugby Union','Exeter Chiefs face Leinster at Sandy Park in the sixth round of the Heineken Cup. The second pool five match, between Scarlets and Clermont Auvergne, is available via the red button. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Castres face Ulster at Stade Pierre-Antoine in the sixth round of the Heineken Cup. Press the red button for the match between between Glasgow Warriors and Northampton Saints.'
union select 'Heineken Cup','Live European Rugby Union','Castres face Ulster at Stade Pierre-Antoine in the sixth round of the Heineken Cup. Press the red button for the match between between Glasgow Warriors and Northampton Saints. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Harlequins host Connacht at Twickenham Stoop in the fifth round of the Heineken Cup. Press the red button for Zebre''s clash with Biarritz and Toulon''s meeting with Cardiff Blues.'
union select 'Heineken Cup','Live European Rugby Union','Harlequins host Connacht at Twickenham Stoop in the fifth round of the Heineken Cup. Press the red button for Zebre''s clash with Biarritz and Toulon''s meeting with Cardiff Blues. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Ospreys take on Toulouse at the Liberty Stadium in the fourth round of the Heineken Cup. Press the red button for Benetton Treviso''s clash with Leicester at the Stadio Comunale di Monigo.'
union select 'Heineken Cup','Live European Rugby Union','Montpellier take on Toulon at Stade Yves-du-Manoir in the sixth round of the Heineken Cup. The hosts still have a mathematical chance of overtaking Toulon at the top of the group.'
union select 'Heineken Cup','Live European Rugby Union','Montpellier take on Toulon at Stade Yves-du-Manoir in the sixth round of the Heineken Cup. The hosts still have a mathematical chance of overtaking Toulon at the top of the group. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Ospreys take on Leicester Tigers at Liberty Stadium in the fifth round of the Heineken Cup. Press the red button for Toulouse''s clash with Benetton Treviso.'
union select 'Heineken Cup','Live European Rugby Union','Ospreys take on Leicester Tigers at Liberty Stadium in the fifth round of the Heineken Cup. Press the red button for Toulouse''s clash with Benetton Treviso. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Biarritz host Harlequins at Parc des Sports Aguilera in the sixth round of the Heineken Cup. The home side need to win to keep their quarter-final hopes alive.'
union select 'Heineken Cup','Live European Rugby Union','Leinster face Clermont Auvergne at the Aviva Stadium in the fourth round of the Heineken Cup. Press the red button for Exeter Chiefs'' meeting with Scarlets at the Sandy Park Stadium.'
union select 'Heineken Cup','Live European Rugby Union','Racing Metro take on Saracens at Stade de la Beaujoire in the fifth round of the Heineken Cup. Victory will secure Sarries a place in the quarter-finals of the competition.'
union select 'Heineken Cup','Live European Rugby Union','Racing Metro take on Saracens at Stade de la Beaujoire in the fifth round of the Heineken Cup. Victory will secure Sarries a place in the quarter-finals of the competition. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Leicester Tigers take on Toulouse at Welford Road Stadium in the sixth round of the Heineken Cup. The match between Benetton Treviso and Ospreys is available via the red button.'
union select 'Heineken Cup','Live European Rugby Union','Leicester Tigers take on Toulouse at Welford Road Stadium in the sixth round of the Heineken Cup. The match between Benetton Treviso and Ospreys is available via the red button. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Ulster take on Glasgow Warriors at Ravenhill Stadium in the fifth round of the 2012/13 Heineken Cup. Press the red button for Northampton Saints'' clash with Castres at Franklin''s Gardens. Also in HD'
union select 'Heineken Cup','Live European Rugby Union','Ulster take on Glasgow Warriors at Ravenhill Stadium in the fifth round of the 2012/13 Heineken Cup. Press the red button for Northampton Saints'' clash with Castres at Franklin''s Gardens.'
union select 'Heineken Cup','Live Glasgow v Northampton','Glasgow Warriors take on Northampton Saints at Scotstoun Stadium in the group stage of the Heineken Cup. The Saints can still qualify for the last eight as one of the two best runners-up.'
union select 'Heineken Cup','Live Heineken Cup Final','Clermont Auvergne take on Toulon at Dublin''s Aviva Stadium in the Heineken Cup final. From 6pm, press the red button for Bulls v Highlanders and Cheetahs v Reds in Super Rugby. Also in 3D'
union select 'Heineken Cup','Live Heineken Cup Final','Clermont Auvergne take on Toulon at Dublin''s Aviva Stadium in the Heineken Cup final. From 6pm, press the red button for Bulls v Highlanders and Cheetahs v Reds in Super Rugby. Also in HD and 3D'
union select 'Heineken Cup','Live Heineken Cup Final','Clermont Auvergne face Toulon at Dublin''s Aviva Stadium in the 2013 Heineken Cup final. Clermont beat Munster in the semi-finals, while Toulon saw off Saracens. (3D TV required)'
union select 'Heineken Cup','Live Leicester v Treviso','Leicester play host to Benetton Treviso at Welford Road in the third round of the Heineken Cup pool stage. The Tigers fell to qualification rivals Toulouse in round two and need a win.'
union select 'Heineken Cup','Live Montpellier v Sale','Montpellier play host to Sale at the Stade Yves du Manoir in Pool Six of the Heineken Cup. The hosts fell to Toulon last weekend while Sale got the better of the Cardiff Blues.'
union select 'Heineken Cup','Live Munster v Racing Metro','Munster entertain Racing Metro at Thomond Park in the sixth round of the Heineken Cup.'
union select 'Heineken Cup','Live Munster v Racing Metro','Munster entertain Racing Metro at Thomond Park in the sixth round of the Heineken Cup.'
union select 'Heineken Cup','Live Sale v Cardiff','Cardiff Blues travel to Salford City Stadium to face Sale Sharks in Pool 6 of the Heineken Cup. The Sharks would love a winning start after ending their two-year exile from the competition.'
union select 'Heineken Cup','Live Scarlets v Clermont','Scarlets face Clermont Auvergne at Parc y Scarlets in the sixth round of the Heineken Cup. The visitors will secure a home quarter-final if they pick up any points here.'
union select 'Heineken Cup','Live Scarlets v Exeter','The Scarlets face Exeter at the Parc y Scarlets in the third round of the Heineken Cup pool stage. Both sides suffered defeats in their opening two games and cannot afford another setback.'
union select 'Heineken Cup','Live Toulon v Cardiff','Toulon play host to Cardiff Blues at the Stade Felix Mayol in round five of the Heineken Cup pool stage. The Frenchmen will secure top spot in pool six with a victory this afternoon.'
union select 'Heineken Cup','Live Toulouse v Treviso','Toulouse and Benetton Treviso meet at the Stade Ernest Wallon in round five of the Heineken Cup pool stage. Victory could seal Toulouse''s progression to the knockout phase.'
union select 'Heineken Cup','Live Treviso v Ospreys','Benetton Treviso battle Ospreys at Stadio Comunale di Monigo in the sixth round of the Heineken Cup. Both teams are playing for pride after already being knocked out of the competition.'
union select 'Heineken Cup','Live Treviso v Ospreys','Benetton Treviso battle Ospreys at Stadio Comunale di Monigo in the sixth round of the Heineken Cup. Both teams are playing for pride after already being knocked out of the competition.'
union select 'Heineken Cup','Live Treviso v Toulouse','Treviso take on Toulouse at the Stadio Comunale di Monigo in Pool Two of the Heineken Cup. The hosts were beaten by Ospreys on matchday one, while Toulouse saw off Leicester.'
union select 'Heineken Cup','Live Zebre v Biarritz','Zebre welcome Biarritz to the Stadio XXV Aprile in round five of the Heineken Cup pool stage. The visitors can put one foot in the quarter-finals with a victory this afternoon.'
union select 'Premiership Rugby','Exeter v Gloucester  Aviva...','...Premiership. Aviva Premiership action from Sandy Park as Exeter Chiefs host Gloucester Rugby with Heineken Cup berths up for grabs on the final day of the regular season.'
union select 'Premiership Rugby','Live Premiership Rugby','Bath take on the Exeter Chiefs at the Recreation Ground in this Aviva Premiership clash. The Chiefs need to pick themselves up after back-to-back defeats in the Heineken Cup.'
union select 'Premiership Rugby','Live Wasps v Exeter  Aviva...','...Premiership. Action from the Aviva Premiership as London Wasps take on Exeter Chiefs at Adams Park with Heineken Cup berths up for grabs.'
union select 'Premiership Rugby','Live Zebre v Harlequins','Aviva Premiership champions Harlequins travel to the Stadio XXV Aprile to meet Zebre in round three of the Heineken Cup pool stage. The Quins are looking for their third straight victory.'
union select 'Premiership Rugby','Wasps v Exeter  Aviva Premiership','Action from the Aviva Premiership as London Wasps take on Exeter Chiefs at Adams Park with Heineken Cup berths up for grabs.'
union select 'Premiership Rugby','Harlequins v London Irish ...','...Aviva Premiership. Champions Harlequins have been in impressive form in the defence of their title and will expect maximum points against London Irish at The Stoop.'
union select 'Premiership Rugby','Harlequins v London Wasps ...','...Aviva Premiership. Harlequins face London Wasps at The Stoop. Champions Harlequins staged a stunning fightback to beat Wasps 42-40 in their Premiership opener at Twickenham.'
union select 'Premiership Rugby','Harlequins v Northampton ...','...Aviva Premiership. Action from the the final round of league fixtures as champions Harlequins take on the Saints at The Stoop. Both sides have booked their places in the Premiership play-offs.'
union select 'Premiership Rugby','Leicester v Bath  Aviva...','...Premiership. Leicester host Bath at Welford Road. The Tigers recorded an easy 28-3 victory over their opponents here last season.'
union select 'Premiership Rugby','Leicester v Exeter  Aviva...','...Premiership. Leicester meet Exeter at Welford Road. The Chiefs recorded a stunning 30-28 win over the Tigers in a pulsating Premiership encounter here last season.'
union select 'Premiership Rugby','Leicester v Harlequins  Aviva...','...Premiership. In a repeat of last year''s Premership final, Leicester Tigers take on Harlequins. Quins won their first ever English title with a thrilling 30-23 victory at Twickenham in May.'
union select 'Premiership Rugby','Leicester v Northampton  Aviva...','...Premiership Final. Bitter rivals Leicester Tigers and Northampton Saints fought off all the challengers on the road to Twickenham, but which East Midlands club will take the crown?'
union select 'Premiership Rugby','Leicester v Northampton  Aviva...','...Premiership. Leicester take on Northampton at Welford Road. Horacio Agulla''s late try gave the Tigers the points in a thrilling 30-25 victory against the Saints here last season.'
union select 'Premiership Rugby','Leicester v Saracens  Aviva...','...Premiership. Leicester take on Saracens at Welford Road. These sides fought out a low-scoring 9-9 draw at Wembley back in September.'
union select 'Premiership Rugby','Live Bath v Gloucester  Aviva...','...Premiership. Coverage from the Recreation Ground as Bath meet West Country rivals Gloucester. Rob Cook''s second-half try helped Gloucester to a 16-10 victory when these sides met at Kingsholm.'
union select 'Premiership Rugby','Live Bath v Northampton ...','...Aviva Premiership. Coverage from the Recreation Ground as Bath Rugby meet Northampton Saints. Ryan Lamb kicked 14 points to help the Saints to a 26-6 win here last season.'
union select 'Premiership Rugby','Live Bath v Saracens  Aviva...','...Premiership. Coverage from the Recreation Ground as Bath meet Saracens. Owen Farrell gave Saracens a 28-26 victory here last season with the last kick of the game in a tight encounter.'
union select 'Premiership Rugby','Live Bath v Wasps  Aviva...','...Premiership. Coverage from the Recreation Ground as Bath meet London Wasps. Despite losing 17-12 here last season, Wasps claimed a vital losing bonus point to avoid Premiership relegation.'
union select 'Premiership Rugby','Live Exeter Chiefs v Saracens','Aviva Premiership. Exeter Chiefs meet Saracens at Sandy Park. A determined Sarries came through a hard-fought match to beat the Chiefs 17-13 here last season.'
union select 'Premiership Rugby','Live Exeter v Leicester ...','...Aviva Premiership. Exeter Chiefs meet Leicester Tigers at Sandy Park. A hat-trick by Leicester debutant Adam Thompstone saw the Tigers beat the Chiefs 30-8 at Welford Road in September.'
union select 'Premiership Rugby','Live Gloucester v Bath  Aviva...','...Premiership. Gloucester meet Bath at Kingsholm. A dominant forward display saw Gloucester claim a 23-6 victory over west country rivals Bath in a bruising encounter here last season.'
union select 'Premiership Rugby','Live Gloucester v Harlequins ...','...Aviva Premiership. Gloucester meet Harlequins at Kingsholm. The Quins recorded a narrow 28-25 victory over their opponents at the Stoop earlier this season.'
union select 'Premiership Rugby','Live Gloucester v Sale  Aviva...','...Premiership. Gloucester meet Sale at Kingsholm. Struggling Sharks replaced coach Bryan Redpath with chief executive Steve Diamond last month after losing their first seven games of the season.'
union select 'Premiership Rugby','Live Gloucester v Worcester ...','...Aviva Premiership. Gloucester meet Worcester at Kingsholm. Gloucester''s Freddie Burns kicked a last-minute penalty to tie the reverse fixture 16-16.'
union select 'Premiership Rugby','Live Harlequins v Bath  Aviva...','...Premiership. Harlequins meet Bath at the Stoop. Bath fly-half Stephen Donald kicked all his side''s points to earn a surprise 21-18 victory against the champions at the Rec.'
union select 'Premiership Rugby','Live Harlequins v London Irish...','...- Aviva Premiership. Champions Harlequins have been in impressive form in the defence of their title and will expect maximum points against London Irish at The Stoop.'
union select 'Premiership Rugby','Live Harlequins v London Wasps...','...- Aviva Premiership. Harlequins face London Wasps at The Stoop. Champions Harlequins staged a stunning fightback to beat Wasps 42-40 in their Premiership opener at Twickenham.'
union select 'Premiership Rugby','Live Harlequins v Northampton...','...- Aviva Premiership. Action from the the final round of league fixtures as champions Harlequins take on the Saints at The Stoop. Both sides have booked their places in the Premiership play-offs.'
union select 'Premiership Rugby','Live Leicester v Exeter ...','...Aviva Premiership. Leicester meet Exeter at Welford Road. The Chiefs recorded a stunning 30-28 win over the Tigers in a pulsating Premiership encounter here last season.'
union select 'Premiership Rugby','Live Leicester v Harlequins ...','...Aviva Premiership. In a repeat of last year''s Premership final, Leicester Tigers take on Harlequins. Quins won their first ever English title with a thrilling 30-23 victory at Twickenham in May.'
union select 'Premiership Rugby','Live Leicester v Northampton ...','...Aviva Premiership Final. Bitter rivals Leicester Tigers and Northampton Saints fought off all the challengers on the road to Twickenham, but which East Midlands club will take the crown?'
union select 'Premiership Rugby','Live Leicester v Northampton ...','...Aviva Premiership. Leicester take on Northampton at Welford Road. Horacio Agulla''s late try gave the Tigers the points in a thrilling 30-25 victory against the Saints here last season.'
union select 'Premiership Rugby','Live Leicester v Saracens ...','...Aviva Premiership. Leicester take on Saracens at Welford Road. These sides fought out a low-scoring 9-9 draw at Wembley back in September.'
union select 'Premiership Rugby','Live London Irish v Harlequins...','...- Aviva Premiership. Harlequins meet London Irish at the Madejski Stadium. Irish lost their opening three matches of the season, conceding 123 points, before picking up their first win against Bath'
union select 'Premiership Rugby','Live London Irish v Sale ...','...Aviva Premiership. London Irish host Sale Sharks at the Madejski Stadium. The Sharks secured their first victory of the season with a 21-9 win against Irish at Salford City Stadium in November.'
union select 'Premiership Rugby','Live London Welsh v Saracens ...','...Aviva Premiership. Newly promoted London Welsh scrum down against Saracens at Kassam Stadium. Welsh have struggled early this season and were thrashed 40-3 by Harlequins in their second game.'
union select 'Premiership Rugby','Live Northampton v Wasps ...','...Aviva Premiership. Coverage from the Franklin''s Gardens as the Saints meet Wasps. Ben Foden scored two tries to help Northampton to a 32-15 win over their opponents here last season.'
union select 'Premiership Rugby','Live Premiership Rugby','Saracens face Bath at Allianz Park in the Aviva Premiership. Press the red button for the IRB Sevens in Glasgow and from 4pm for the Super Rugby contest between the Kings and the Waratahs.'
union select 'Premiership Rugby','Live Premiership Rugby','Saracens face Bath at Allianz Park in the Aviva Premiership. Press the red button for the IRB Sevens in Glasgow and from 4pm for the Super Rugby contest between the Kings and the Waratahs. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','London Wasps face Bath at Adams Park in the Aviva Premiership. The Wasps are unbeaten in their last four games while a late penalty robbed Bath of the spoils at Exeter in their last outing. Also in H'
union select 'Premiership Rugby','Live Premiership Rugby','London Wasps face Bath at Adams Park in the Aviva Premiership. The Wasps are unbeaten in their last four games while a late penalty robbed Bath of the spoils at Exeter in their last outing.'
union select 'Premiership Rugby','Live Premiership Rugby','Saracens face Exeter Chiefs at Allianz Park in the Aviva Premiership. Sarries can move above Leicester and Harlequins, who meet later on this evening, with a victory this afternoon.'
union select 'Premiership Rugby','Live Premiership Rugby','Saracens face Exeter Chiefs at Allianz Park in the Aviva Premiership. Sarries can move above Leicester and Harlequins, who meet later on this evening, with a victory this afternoon. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Harlequins host Gloucester at Twickenham Stoop in the Aviva Premiership. Gloucester are unbeaten in eight games and sit just a point behind the Quins in the table coming into this match.'
union select 'Premiership Rugby','Live Premiership Rugby','Harlequins host Gloucester at Twickenham Stoop in the Aviva Premiership. Gloucester are unbeaten in eight games and sit just a point behind the Quins in the table coming into this match. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Sale face Leicester at the Salford City Stadium in the Aviva Premiership. The Sharks are without a win this season, while the Tigers beat Exeter in convincing fashion last weekend.'
union select 'Premiership Rugby','Live Premiership Rugby','Sale face Leicester at the Salford City Stadium in the Aviva Premiership. The Sharks are without a win this season, while the Tigers beat Exeter in convincing fashion last weekend. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Leicester Tigers take on Harlequins at Welford Road in the Aviva Premiership play-off semi-finals. Press the red button for the IRB Sevens Series and Kings v Highlanders in Super Rugby.'
union select 'Premiership Rugby','Live Premiership Rugby','Leicester Tigers take on Harlequins at Welford Road in the Aviva Premiership play-off semi-finals. Press the red button for the IRB Sevens Series and Kings v Highlanders in Super Rugby. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Harlequins meet Leicester Tigers at Twickenham Stoop in the Aviva Premiership. The Tigers have won their last five league games, while the Quins were beaten by London Wasps last weekend.'
union select 'Premiership Rugby','Live Premiership Rugby','Harlequins meet Leicester Tigers at Twickenham Stoop in the Aviva Premiership. The Tigers have won their last five league games, while the Quins were beaten by London Wasps last weekend. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Exeter Chiefs take on Northampton Saints at Sandy Park in the Aviva Premiership. Victory will take the Chiefs above their struggling opponents. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Exeter Chiefs take on Northampton Saints at Sandy Park in the Aviva Premiership. Victory will take the Chiefs above their struggling opponents.'
union select 'Premiership Rugby','Live Premiership Rugby','Northampton Saints host Harlequins at Franklin''s Gardens in the Aviva Premiership. The Saints have the chance to move above the Quins in the table with a victory here.'
union select 'Premiership Rugby','Live Premiership Rugby','Saracens welcome the Leicester Tigers to Vicarage Road for this Aviva Premiership clash. This should be a very close contest after both sides won their opening two games of the season.'
union select 'Premiership Rugby','Live Premiership Rugby','Northampton face the Exeter Chiefs at Franklin''s Gardens in the Aviva Premiership. The Chiefs thrashed Sale in their opening game of the season while the Saints edged past Gloucester.'
union select 'Premiership Rugby','Live Premiership Rugby','Northampton Saints host Harlequins at Franklin''s Gardens in the Aviva Premiership. The Saints have the chance to move above the Quins in the table with a victory here. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Saracens welcome the Leicester Tigers to Vicarage Road for this Aviva Premiership clash. This should be a very close contest after both sides won their opening two games of the season. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Northampton face the Exeter Chiefs at Franklin''s Gardens in the Aviva Premiership. The Chiefs thrashed Sale in their opening game of the season while the Saints edged past Gloucester. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Northampton Saints face Gloucester at Franklin''s Gardens in the Aviva Premiership. Just one point separates the teams in the table coming into this meeting. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Northampton Saints face Gloucester at Franklin''s Gardens in the Aviva Premiership. Just one point separates the teams in the table coming into this meeting.'
union select 'Premiership Rugby','Live Premiership Rugby','Gloucester face Leicester Tigers at the Kingsholm Stadium in the Aviva Premiership. The hosts will be full of confidence after victories in both of their games in the Amlin Challenge Cup.'
union select 'Premiership Rugby','Live Premiership Rugby','Gloucester Rugby take on London Wasps at Kingsholm Stadium in the Aviva Premiership. The hosts were beaten by Northampton last weekend while the Wasps were edged out by Harlequins.'
union select 'Premiership Rugby','Live Premiership Rugby','Gloucester Rugby take on London Wasps at Kingsholm Stadium in the Aviva Premiership. The hosts were beaten by Northampton last weekend while the Wasps were edged out by Harlequins. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','London Welsh take on Leicester Tigers at the Kassam Stadium in the Aviva Premiership. The home side will want to issue a statement of intent in their very first Premiership appearance.'
union select 'Premiership Rugby','Live Premiership Rugby','London Welsh take on Leicester Tigers at the Kassam Stadium in the Aviva Premiership. The home side will want to issue a statement of intent in their very first Premiership appearance. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Defending champions Harlequins face Saracens at Twickenham Stoop in the Aviva Premiership. The Quins have made an emphatic start to the season, winning all four of their opening games.'
union select 'Premiership Rugby','Live Premiership Rugby','Defending champions Harlequins face Saracens at Twickenham Stoop in the Aviva Premiership. The Quins have made an emphatic start to the season, winning all four of their opening games. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','London Irish lock horns with London Wasps at the Madejski Stadium in the Aviva Premiership. The Wasps have picked up big wins over Harlequins and Gloucester in their last two outings.'
union select 'Premiership Rugby','Live Premiership Rugby','London Irish lock horns with London Wasps at the Madejski Stadium in the Aviva Premiership. The Wasps have picked up big wins over Harlequins and Gloucester in their last two outings. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby','Leicester Tigers lock horns with Gloucester Rugby at Welford Road Stadium in the Aviva Premiership. The Tigers could move to the top of the table with a victory this afternoon.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Northampton Saints face Leicester Tigers in the Aviva Premiership. From 3pm, press the red button for Super Rugby ties between the Cheetahs and Rebels and the Stormers and Crusaders.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Northampton Saints face Leicester Tigers in the Aviva Premiership. From 3pm, press the red button for Super Rugby ties between the Cheetahs and Rebels and the Stormers and Crusaders. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby Union','Harlequins host Exeter Chiefs at Twickenham Stoop in the Aviva Premiership. The Quins top the table coming into this game after beating Sale last weekend.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Harlequins host Exeter Chiefs at Twickenham Stoop in the Aviva Premiership. The Quins top the table coming into this game after beating Sale last weekend. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby Union','London Wasps face Northampton Saints at Adams Park in the Aviva Premiership. Press the red button for Sharks v Rebels and Stormers v Brumbies in Super Rugby.'
union select 'Premiership Rugby','Live Premiership Rugby Union','London Wasps face Northampton Saints at Adams Park in the Aviva Premiership. Press the red button for Sharks v Rebels and Stormers v Brumbies in Super Rugby. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby Union','Bath Rugby host Harlequins at the Recreation Ground in the Aviva Premiership. The hosts will look to avenge the 21-12 defeat they suffered to the Quins in the LV= Cup last weekend. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby Union','Bath Rugby host Harlequins at the Recreation Ground in the Aviva Premiership. The hosts will look to avenge the 21-12 defeat they suffered to the Quins in the LV= Cup last weekend.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Saracens welcome Gloucester Rugby to Vicarage Road for this Aviva Premiership clash. The sides are level on points coming into this game.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Saracens welcome Gloucester Rugby to Vicarage Road for this Aviva Premiership clash. The sides are level on points coming into this game. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby Union','Northampton Saints face Sale Sharks at Franklin''s Gardens in the Aviva Premiership. From 6pm, press the red button for Kings v Bulls in Super Rugby and Fight Night Build Up.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Northampton Saints face Sale Sharks at Franklin''s Gardens in the Aviva Premiership. From 6pm, press the red button for Kings v Bulls in Super Rugby and Fight Night Build Up. Also in HD'
union select 'Premiership Rugby','Live Premiership Rugby Union','Leicester Tigers lock horns with London Wasps at Welford Road Stadium in the Aviva Premiership. Leicester could move to the top of the table by this evening.'
union select 'Premiership Rugby','Live Premiership Rugby Union','Leicester Tigers lock horns with London Wasps at Welford Road Stadium in the Aviva Premiership. Leicester could move to the top of the table by this evening. Also in HD'
union select 'Premiership Rugby','Live Rugby Exeter v Gloucester','Jill Douglas presents live Aviva Premiership action from Sandy Park as Exeter Chiefs host Gloucester Rugby. Commentary comes from Simon Ward and Matt Perry.'
union select 'Premiership Rugby','Live Rugby London v Gloucester','Action from the Aviva Premiership as London Wasps take on Gloucester Rugby live from Adams Park.'
union select 'Premiership Rugby','Live Rugby Worcester v Gloucester','Aviva Premiership. Worcester scrum down against Gloucester at Sixways Stadium. The Warriors claimed a 21-15 victory over their local rivals here last season.'
union select 'Premiership Rugby','Live Sale v Exeter  Aviva...','...Premiership. Rock bottom Sale welcome Exeter to Salford City Stadium. The Chiefs thrashed the Sharks 43-6 on the opening day of the season at Sandy Park.'
union select 'Premiership Rugby','Live Sale v Gloucester  Aviva...','...Premiership. Sale welcome Gloucester to Salford City Stadium. Fly-half Freddie Burns kicked 19 points for Gloucester in a 29-3 win when the teams met at Kingsholm.'
union select 'Premiership Rugby','Live Sale v Northampton ...','...Aviva Premiership. Struggling Sale Sharks welcome a strong Northampton Saints side to Salford City Stadium.'
union select 'Premiership Rugby','Live Sale v Saracens  Aviva...','...Premiership. Sale Sharks meet Saracens at Salford City Stadium. Sale have bolstered their squad with the signings of Aussie winger Cameron Shepherd and fly-half Danny Cipriani.'
union select 'Premiership Rugby','Live Saracens v Harlequins ...','...Aviva Premiership. Saracens scrum down against Harlequins at Allianz Park. Saracens fly-half Owen Farrell kicked all his side''s points as they beat champions Harlequins 18-16 at the Stoop.'
union select 'Premiership Rugby','Live Saracens v London Irish','Aviva Premiership. A rare treat for ESPN Classic viewers today: an exclusively live rugby match between powerful Saracens and spirited London Irish.'
union select 'Premiership Rugby','Live Saracens v Northampton ...','...Aviva Premiership. Coverage from the semi-finals of the Aviva Premiership as Saracens meet Northampton Saints at Allianz Park. Both these sides were knocked out at this stage last season.'
union select 'Premiership Rugby','Live Saracens v Wasps  Aviva...','...Premiership. Saracens scrum down against London Wasps at Vicarage Road. In this fixture last season, Sarries began the defence of their title with a surprise 20-15 home loss against Wasps.'
union select 'Premiership Rugby','Live Wasps v Harlequins ...','...Aviva Premiership. The Aviva Premiership 2012/13 season kicks off with the first game of the London Double Header between London Wasps and defending champions Harlequins at Twickenham.'
union select 'Premiership Rugby','Live Worcester v Leicester ...','...Aviva Premiership. Worcester scrum down against Leicester at Sixways Stadium. The Tigers ran out 32-13 winners here last season in a dominant display.'
union select 'Premiership Rugby','London Irish v Harlequins ...','...Aviva Premiership. Harlequins meet London Irish at the Madejski Stadium. Irish lost their opening three matches of the season, conceding 123 points, before picking up their first win against Bath.'
union select 'Premiership Rugby','Premier Rugby Union','Exeter Chiefs take on Northampton Saints at Sandy Park in the Aviva Premiership. Victory will take the Chiefs above their struggling opponents. Also in HD'
union select 'Premiership Rugby','Premier Rugby Union','Exeter Chiefs take on Northampton Saints at Sandy Park in the Aviva Premiership. Victory will take the Chiefs above their struggling opponents.'
union select 'Premiership Rugby','Premiership Rugby','Saturday''s Aviva Premiership double bill. Bath take on Exeter Chiefs at the Recreation Ground and Gloucester Rugby host Leicester Tigers at Kingsholm Stadium. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Saturday''s Aviva Premiership double bill. Bath take on Exeter Chiefs at the Recreation Ground and Gloucester Rugby host Leicester Tigers at Kingsholm Stadium.'
union select 'Premiership Rugby','Premiership Rugby','Another chance to catch Saturday''s Aviva Premiership double bill. Saracens take on Exeter Chiefs at Allianz Park and Harlequins host Leicester Tigers at Twickenham Stoop.'
union select 'Premiership Rugby','Premiership Rugby','Another chance to catch Saturday''s Aviva Premiership double bill. Saracens take on Exeter Chiefs at Allianz Park and Harlequins host Leicester Tigers at Twickenham Stoop. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Saracens tackle Bath at Allianz Park in the Aviva Premiership. If they avoid defeat here, the hosts will finish top of the table at the end of the regular season.'
union select 'Premiership Rugby','Premiership Rugby','Saracens tackle Bath at Allianz Park in the Aviva Premiership. If they avoid defeat here, the hosts will finish top of the table at the end of the regular season. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Harlequins host Gloucester at Twickenham Stoop in the Aviva Premiership. Gloucester are unbeaten in eight games and sit just a point behind the Quins in the table coming into this match. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Harlequins host Gloucester at Twickenham Stoop in the Aviva Premiership. Gloucester are unbeaten in eight games and sit just a point behind the Quins in the table coming into this match.'
union select 'Premiership Rugby','Premiership Rugby','Sale face Leicester at the Salford City Stadium in the Aviva Premiership. The Sharks are without a win this season, while the Tigers beat Exeter in convincing fashion last weekend.'
union select 'Premiership Rugby','Premiership Rugby','Sale face Leicester at the Salford City Stadium in the Aviva Premiership. The Sharks are without a win this season, while the Tigers beat Exeter in convincing fashion last weekend. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Leicester Tigers take on Harlequins at Welford Road in the Aviva Premiership play-off semi-finals. The Tigers finished five points clear of the Quins in the regular season.'
union select 'Premiership Rugby','Premiership Rugby','Leicester Tigers take on Harlequins at Welford Road in the Aviva Premiership play-off semi-finals. The Tigers finished five points clear of the Quins in the regular season. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Saracens welcome Gloucester Rugby to Vicarage Road for this Aviva Premiership clash. The sides are level on points coming into this game and it should be a very close contest. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Saracens welcome Gloucester Rugby to Vicarage Road for this Aviva Premiership clash. The sides are level on points coming into this game and it should be a very close contest. [HD]'
union select 'Premiership Rugby','Premiership Rugby','Saracens welcome Gloucester Rugby to Vicarage Road for this Aviva Premiership clash. The sides are level on points coming into this game and it should be a very close contest.'
union select 'Premiership Rugby','Premiership Rugby','Northampton face the Exeter Chiefs at Franklin''s Gardens in the Aviva Premiership. The Chiefs thrashed Sale in their opening game of the season while the Saints edged past Gloucester.'
union select 'Premiership Rugby','Premiership Rugby','Saracens welcome the Leicester Tigers to Vicarage Road for this Aviva Premiership clash. This should be a close contest after both sides won their opening two games of the season.'
union select 'Premiership Rugby','Premiership Rugby','Northampton face the Exeter Chiefs at Franklin''s Gardens in the Aviva Premiership. The Chiefs thrashed Sale in their opening game of the season while the Saints edged past Gloucester. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Saracens welcome the Leicester Tigers to Vicarage Road for this Aviva Premiership clash. This should be a close contest after both sides won their opening two games of the season. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Northampton Saints host Harlequins at Franklin''s Gardens in the Aviva Premiership. The Saints have the chance to move above the Quins in the table with a victory here.'
union select 'Premiership Rugby','Premiership Rugby','Northampton Saints host Harlequins at Franklin''s Gardens in the Aviva Premiership. The Saints have the chance to move above the Quins in the table with a victory here. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Northampton Saints face Gloucester at Franklin''s Gardens in the Aviva Premiership. Just one point separates the teams in the table coming into this meeting. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Northampton Saints face Gloucester at Franklin''s Gardens in the Aviva Premiership. Just one point separates the teams in the table coming into this meeting.'
union select 'Premiership Rugby','Premiership Rugby','Gloucester Rugby take on London Wasps at Kingsholm Stadium in the Aviva Premiership. The hosts were beaten by Northampton last weekend while the Wasps were edged out by Harlequins. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Gloucester Rugby take on London Wasps at Kingsholm Stadium in the Aviva Premiership. The hosts were beaten by Northampton last weekend while the Wasps were edged out by Harlequins.'
union select 'Premiership Rugby','Premiership Rugby','London Welsh take on Leicester Tigers at the Kassam Stadium in the Aviva Premiership. The home side will want to issue a statement of intent in their very first Premiership appearance.'
union select 'Premiership Rugby','Premiership Rugby','London Welsh take on Leicester Tigers at the Kassam Stadium in the Aviva Premiership. The home side will want to issue a statement of intent in their very first Premiership appearance. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Defending champions Harlequins face Saracens at Twickenham Stoop in the Aviva Premiership. The Quins have made an emphatic start to the season, winning all four of their opening games. Also in HD'
union select 'Premiership Rugby','Premiership Rugby','Defending champions Harlequins face Saracens at Twickenham Stoop in the Aviva Premiership. The Quins have made an emphatic start to the season, winning all four of their opening games.'
union select 'Premiership Rugby','Premiership Rugby','London Irish lock horns with London Wasps at the Madejski Stadium in the Aviva Premiership. The Wasps have picked up big wins over Harlequins and Gloucester in their last two outings.'
union select 'Premiership Rugby','Premiership Rugby','London Irish lock horns with London Wasps at the Madejski Stadium in the Aviva Premiership. The Wasps have picked up big wins over Harlequins and Gloucester in their last two outings. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Final','Leicester Tigers take on Northampton Saints at Twickenham in the Aviva Premiership final. The Tigers beat Harlequins in the semi-finals, while the Saints stunned Saracens.'
union select 'Premiership Rugby','Premiership Rugby Highlights','London Wasps face Northampton Saints at Adams Park in the Aviva Premiership. The Saints have won their last three games and are finding form at a crucial time.'
union select 'Premiership Rugby','Premiership Rugby Union','London Wasps face Bath at Adams Park in the Aviva Premiership. The Wasps are unbeaten in their last four games while a late penalty robbed Bath of the spoils at Exeter in their last outing.'
union select 'Premiership Rugby','Premiership Rugby Union','London Wasps face Bath at Adams Park in the Aviva Premiership. The Wasps are unbeaten in their last four games while a late penalty robbed Bath of the spoils at Exeter in their last outing. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','London Wasps face Bath at Adams Park in the Aviva Premiership. The Wasps are unbeaten in their last four games while a late penalty robbed Bath of the spoils at Exeter in their last outing. Also in H'
union select 'Premiership Rugby','Premiership Rugby Union','Harlequins host Exeter Chiefs at Twickenham Stoop in the Aviva Premiership. The Quins top the table coming into this game after beating Sale last weekend.'
union select 'Premiership Rugby','Premiership Rugby Union','Harlequins host Exeter Chiefs at Twickenham Stoop in the Aviva Premiership. The Quins top the table coming into this game after beating Sale last weekend. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','London Wasps face Northampton Saints at Adams Park in the Aviva Premiership. The Saints have won their last three games and are finding form at a crucial time.'
union select 'Premiership Rugby','Premiership Rugby Union','London Wasps face Northampton Saints at Adams Park in the Aviva Premiership. The Saints have won their last three games and are finding form at a crucial time. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','Bath Rugby host Harlequins at the Recreation Ground in the Aviva Premiership. The hosts will look to avenge the 21-12 defeat they suffered to the Quins in the LV= Cup last weekend.'
union select 'Premiership Rugby','Premiership Rugby Union','Bath Rugby host Harlequins at the Recreation Ground in the Aviva Premiership. The hosts will look to avenge the 21-12 defeat they suffered to the Quins in the LV= Cup last weekend. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','Exeter Chiefs take on Northampton Saints at Sandy Park in the Aviva Premiership. Victory will take the Chiefs above their struggling opponents. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','Northampton Saints face Gloucester at Franklin''s Gardens in the Aviva Premiership. Just one point separates the teams in the table coming into this meeting.'
union select 'Premiership Rugby','Premiership Rugby Union','Northampton Saints face Gloucester at Franklin''s Gardens in the Aviva Premiership. Just one point separates the teams in the table coming into this meeting. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','Northampton Saints welcome Leicester Tigers to Franklin''s Gardens for this Aviva Premiership clash. The Saints saw off London Wasps on their last outing, while the Tigers took down Exeter. Also in HD'
union select 'Premiership Rugby','Premiership Rugby Union','Northampton Saints face Sale Sharks at Franklin''s Gardens. The Saints recovered from their defeat to Leicester with a comprehensive victory over London Welsh last weekend.'
union select 'Premiership Rugby','Premiership Rugby Union','Northampton Saints face Sale Sharks at Franklin''s Gardens. The Saints recovered from their defeat to Leicester with a comprehensive victory over London Welsh last weekend. Also in HD'
union select 'Premiership Rugby','Saracens v Northampton  Aviva...','...Premiership. Coverage from the semi-finals of the Aviva Premiership as Saracens meet Northampton Saints at Allianz Park. Both these sides were knocked out at this stage last season.'
union select 'Premiership Rugby','Wasps v Gloucester  Aviva...','...Premiership. Action from the Aviva Premiership as London Wasps take on Gloucester at Adams Park. Gloucester edged out their opponents 29-22 when they met at Kingsholm.'
union select 'Premiership Rugby','Wasps v Harlequins  Aviva...','...Premiership. The Aviva Premiership 2012/13 season kicks off with the first game of the London Double Header between London Wasps and defending champions Harlequins at Twickenham.'



commit;

--Update Programme sub types with new details--


update dbarnett.v223_sports_epg_lookup_aug_12_jul_13
set programme_sub_genre_type = b.sub_genre_type
from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as a 
left outer join v223_rugby_heineken_or_premiership as b
on a.programme_instance_name=b.programme_instance_name and a.synopsis=b.synopsis
where b.synopsis is not null
;commit;

--select programme_sub_genre_type , count(*) from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 group by programme_sub_genre_type order by programme_sub_genre_type

---Create Summary Stats for Heineken/Premiership Rugby---
---Run Sub details to just return Rugby---
select a.*
,c.channel_name_inc_hd_staggercast_channel_families
,c.sub_genre_description
,c.programme_sub_genre_type
,c.programme_instance_duration
into dbarnett.v223_all_heineken_and_premiership_rugby_viewed
from dbarnett.v223_all_sports_programmes_viewed as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as c
on a.dk_programme_instance_dim=c.dk_programme_instance_dim
where c. programme_sub_genre_type in ('Premiership Rugby', 'Heineken Cup')
;
commit;

CREATE HG INDEX idx1 ON dbarnett.v223_all_heineken_and_premiership_rugby_viewed  (account_number);



select account_number
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Heineken Cup' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Heineken_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D') 
and programme_sub_genre_type='Heineken Cup' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_Heineken_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Heineken Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Heineken_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Heineken Cup' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_Heineken_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Heineken Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Heineken_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Heineken Cup' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_Heineken_Cup


,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premiership Rugby' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Premiership_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D') 
and programme_sub_genre_type='Premiership Rugby' then  viewing_duration else 0 end) as viewing_duration_non_Sky_Sports_Premiership_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premiership Rugby' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_Premiership_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premiership Rugby' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_Sky_Sports_Premiership_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premiership Rugby' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Premiership_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premiership Rugby' and (viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_Premiership_Rugby


into dbarnett.v223_rugby_heineken_and_premiership_viewed_by_account
from dbarnett.v223_all_heineken_and_premiership_rugby_viewed
group by account_number
;

---Add on Days with viewing 

alter table dbarnett.v223_rugby_heineken_and_premiership_viewed_by_account add days_with_viewing integer;

update dbarnett.v223_rugby_heineken_and_premiership_viewed_by_account
set days_with_viewing=b.days_with_viewing
from dbarnett.v223_rugby_heineken_and_premiership_viewed_by_account as a
left outer join v223_unbundling_viewing_summary_by_account as b
on a.account_number =b.account_number
;



commit;
CREATE HG INDEX idx1 ON dbarnett.v223_rugby_heineken_and_premiership_viewed_by_account  (account_number);
---Add On weighted Viewing details for Heineken Cup and Premiership Rugby----

/*
alter table dbarnett.v223_Unbundling_pivot_activity_data delete annualised_programmes_3min_plus_heineken_cup_rugby_total ;
alter table dbarnett.v223_Unbundling_pivot_activity_data delete annualised_programmes_3min_plus_sky_sports_aviva_premiership_rugby_total ;
alter table dbarnett.v223_Unbundling_pivot_activity_data delete annualised_programmes_3min_plus_non_sky_sports_aviva_premiership_rugby_total ;


alter table dbarnett.v223_Unbundling_pivot_activity_data delete annualised_programmes_engaged_heineken_cup_rugby_total ;
alter table dbarnett.v223_Unbundling_pivot_activity_data delete annualised_programmes_engaged_sky_sports_aviva_premiership_rugby_total ;
alter table dbarnett.v223_Unbundling_pivot_activity_data delete annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total ;
*/


alter table dbarnett.v223_Unbundling_pivot_activity_data add annualised_programmes_3min_plus_heineken_cup_rugby_total real default 0;
alter table dbarnett.v223_Unbundling_pivot_activity_data add annualised_programmes_3min_plus_sky_sports_aviva_premiership_rugby_total real default 0;
alter table dbarnett.v223_Unbundling_pivot_activity_data add annualised_programmes_3min_plus_non_sky_sports_aviva_premiership_rugby_total real default 0;


alter table dbarnett.v223_Unbundling_pivot_activity_data add annualised_programmes_engaged_heineken_cup_rugby_total real default 0;
alter table dbarnett.v223_Unbundling_pivot_activity_data add annualised_programmes_engaged_sky_sports_aviva_premiership_rugby_total real default 0;
alter table dbarnett.v223_Unbundling_pivot_activity_data add annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total real default 0;


update dbarnett.v223_Unbundling_pivot_activity_data
set annualised_programmes_3min_plus_heineken_cup_rugby_total=
(programmes_3min_plus_Sky_Sports_Heineken_Cup
)*(365/cast(days_with_viewing as real))*account_weight

,annualised_programmes_3min_plus_sky_sports_aviva_premiership_rugby_total=
(programmes_3min_plus_Sky_Sports_Premiership_rugby
)*(365/cast(days_with_viewing as real))*account_weight

,annualised_programmes_3min_plus_non_sky_sports_aviva_premiership_rugby_total=
(programmes_3min_plus_non_Sky_Sports_Premiership_rugby
)*(365/cast(days_with_viewing as real))*account_weight


,annualised_programmes_engaged_heineken_cup_rugby_total=
(programmes_engaged_Sky_Sports_Heineken_Cup
)*(365/cast(days_with_viewing as real))*account_weight

,annualised_programmes_engaged_sky_sports_aviva_premiership_rugby_total=
(programmes_engaged_Sky_Sports_Premiership_rugby
)*(365/cast(days_with_viewing as real))*account_weight

,annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total=
(programmes_engaged_non_Sky_Sports_Premiership_rugby
)*(365/cast(days_with_viewing as real))*account_weight



from dbarnett.v223_Unbundling_pivot_activity_data as a 
left outer join dbarnett.v223_rugby_heineken_and_premiership_viewed_by_account as b
on a.account_number = b.account_number
;
commit;




update dbarnett.v223_Unbundling_pivot_activity_data
set annualised_programmes_3min_plus_heineken_cup_rugby_total=case when annualised_programmes_3min_plus_heineken_cup_rugby_total is null then 0 else annualised_programmes_3min_plus_heineken_cup_rugby_total end

,annualised_programmes_3min_plus_sky_sports_aviva_premiership_rugby_total
=case when annualised_programmes_3min_plus_sky_sports_aviva_premiership_rugby_total is null then 0 else annualised_programmes_3min_plus_sky_sports_aviva_premiership_rugby_total end

,annualised_programmes_3min_plus_non_sky_sports_aviva_premiership_rugby_total
=case when annualised_programmes_3min_plus_non_sky_sports_aviva_premiership_rugby_total is null then 0 else annualised_programmes_3min_plus_non_sky_sports_aviva_premiership_rugby_total end


,annualised_programmes_engaged_heineken_cup_rugby_total
=case when annualised_programmes_engaged_heineken_cup_rugby_total is null then 0 else annualised_programmes_engaged_heineken_cup_rugby_total end

,annualised_programmes_engaged_sky_sports_aviva_premiership_rugby_total
=case when annualised_programmes_engaged_sky_sports_aviva_premiership_rugby_total is null then 0 else annualised_programmes_engaged_sky_sports_aviva_premiership_rugby_total end

,annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total
=case when annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total is null then 0 else annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total end



from dbarnett.v223_Unbundling_pivot_activity_data 
;
commit;


----select top 500 annualised_programmes_engaged_heineken_cup_rugby_total,account_weight from dbarnett.v223_Unbundling_pivot_activity_data where annualised_programmes_engaged_heineken_cup_rugby_total>0
--select annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total , count(*) from  dbarnett.v223_Unbundling_pivot_activity_data group by annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total  order by annualised_programmes_engaged_non_sky_sports_aviva_premiership_rugby_total  

--select sum(annualised_programmes_engaged_heineken_cup_rugby_total),sum(account_weight) from dbarnett.v223_Unbundling_pivot_activity_data




select * from sk_prod.Vespa_programme_schedule where service_key in (3625,3627,3661,3663) and cast(broadcast_start_date_time_local as date) = '2013-11-03'
order by broadcast_start_date_time_local




