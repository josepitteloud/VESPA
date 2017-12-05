
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








---Create Summary by Account---
--select top 100 * from dbarnett.v223_all_sports_programmes_viewed;
--select top 100 * from dbarnett.v223_sports_epg_lookup;
--drop table v223_unbundling_viewing_summary_by_account;

---Run on sample first

--Create version of EPG table that's just Aug'12-Jul'13
--select * from dbarnett.v223_sports_epg_lookup;
select * into dbarnett.v223_sports_epg_lookup_aug_12_jul_13 from dbarnett.v223_sports_epg_lookup 
where cast(broadcast_start_date_time_utc as date) between '2012-08-01' and '2013-07-31';
commit;

CREATE HG INDEX idx1 ON dbarnett.v223_sports_epg_lookup_aug_12_jul_13 (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_sports_epg_lookup_aug_12_jul_13 (channel_name_inc_hd_staggercast_channel_families);
CREATE HG INDEX idx3 ON dbarnett.v223_sports_epg_lookup_aug_12_jul_13 (programme_sub_genre_type);
commit;

--drop table v223_unbundling_viewing_summary_by_account_sample;
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='0';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--1
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='1';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;


--2
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='2';
commit;
--select count(*) from dbarnett.v223_all_sports_programmes_viewed_sample ;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--3
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='3';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--4
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='4';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--5
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='5';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--6
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='6';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--7
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='7';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

--8
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='8';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;
--select right(account_number,1) as last_char,count(*) from dbarnett.v223_all_sports_programmes_viewed_sample group by last_char;
--select right(account_number,1) as last_char from v223_unbundling_viewing_summary_by_account group by last_char;
--9
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='9';
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end) as viewing_duration_Pick_TV_premier_league

---Other Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_football_non_premier_league


,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_BT_Sport_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football' then  viewing_duration else 0 end) as viewing_duration_ESPN_non_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and sub_genre_description = 'Football'  then  viewing_duration else 0 end) as viewing_duration_football_other_channels
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

--into v223_unbundling_viewing_summary_by_account_sample
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;


-------End of Account Summary---

--select count(*) from v223_unbundling_viewing_summary_by_account;








select viewing_duration_Sky_Sports_inc_SBO_wrestling

, viewing_duration_non_Sky_Sports_or_SBO_wrestling






--sp_iqtablesize 'dbarnett.v223_sports_epg_lookup'
--select top 500 * from dbarnett.v223_all_sports_programmes_viewed
--select top 500 * from v223_unbundling_viewing_summary_by_account_sample
--sp_iqtablesize 'dbarnett.v223_sports_epg_lookup'
--select count(*) , count(distinct dk_programme_instance_dim ) from dbarnett.v223_sports_epg_lookup

CREATE HG INDEX idx1 ON  v223_unbundling_viewing_summary_by_account (account_number);
















































































select channel_name_inc_hd_staggercast_channel_families ,programme_sub_genre_type
, count(*) as records 
,sum(viewing_duration) as total_duration
,count(distinct account_number) as accounts
from dbarnett.v223_all_sports_programmes_viewed    as a
left outer join dbarnett.v223_sports_epg_lookup as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by channel_name_inc_hd_staggercast_channel_families,programme_sub_genre_type order by total_duration desc

;




-----------------------------TEST CODE-----------------------------










--select * from dbarnett.v223_sports_epg_lookup order by total_duration_viewed desc;
--select * from dbarnett.v223_sports_epg_lookup where programme_sub_genre_type='Unknown' order by total_duration_viewed desc;
--select * from dbarnett.v223_sports_epg_lookup where sub_genre_description='Wrestling' order by total_duration_viewed desc;
--select programme_sub_genre_type,sum(total_duration_viewed) as totview from dbarnett.v223_sports_epg_lookup group by programme_sub_genre_type order by totview desc;


/*
select round(tot_dur/3600,0) as minutes_dur 
,count(*) as programmes 
from #epg_programme_duration_summary 
group by minutes_dur 
order by minutes_dur
;
*/


--select * from shaha.F_Fixtures_EPG
--select count(*) from shaha.F_Fixtures_EPG

---Create lookup by programme of additional details---





/*

select channel_name , count(*) as records ,sum(viewing_duration) as total_duration
from dbarnett.v223_sports_epg_lookup  as a
left outer join dbarnett.v223_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by channel_name order by total_duration desc
*/


select * from dbarnett.v223_sports_epg_lookup where channel_name_inc_hd_staggercast_channel_families is null

select distinct channel_name from dbarnett.v223_sports_epg_lookup
 where channel_name_inc_hd_staggercast_channel_families is null order by channel_name




select channel_name_inc_hd_staggercast_channel_families , count(*) as records ,sum(viewing_duration) as total_duration
from dbarnett.v223_sports_epg_lookup  as a
left outer join dbarnett.v223_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by channel_name_inc_hd_staggercast_channel_families order by total_duration desc
;

select  channel_name_inc_hd_staggercast_channel_families 
, programme_instance_name
,synopsis
,broadcast_start_date_time_utc
, count(*) as records ,sum(viewing_duration) as total_duration
from dbarnett.v223_sports_epg_lookup  as a
left outer join dbarnett.v223_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by channel_name_inc_hd_staggercast_channel_families 
, programme_instance_name
,synopsis
,broadcast_start_date_time_utc
 order by total_duration desc
;







select channel_name_inc_hd_staggercast_channel_families ,service_key, count(*) as records ,sum(viewing_duration) as total_duration
from dbarnett.v223_sports_epg_lookup  as a
left outer join dbarnett.v223_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where channel_name_inc_hd_staggercast_channel_families is null
group by channel_name_inc_hd_staggercast_channel_families ,service_key order by total_duration desc
;


select channel_name_inc_hd_staggercast_channel_families , count(*) as records ,sum(viewing_duration) as total_duration
from dbarnett.v223_sports_epg_lookup  as a
left outer join dbarnett.v223_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
left outer join v200_channel_lookup_with_channel_family as c
on a.channel_name=c.channel_name
group by channel_name_inc_hd_staggercast_channel_families order by total_duration desc
;




--select top 100 * from  v200_channel_lookup_with_channel_family
--Taken from 'G:\RTCI\Lookup Tables\Project Gold Channel Name Lookup.csv' format ascii;

commit;



select top 100 * from dbarnett.v223_all_sports_programmes_viewed
select * from sk_prod.Vespa_programme_schedule where dk_programme_instance_dim = 811707476;
select cast(broadcast_start_date_time_utc as date) as dayval
,count(*) as records from dbarnett.v223_all_sports_programmes_viewed as a
left outer join sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by dayval
order by dayval desc
;

--sp_iqtablesize 'dbarnett.v223_all_sports_programmes_viewed'
--select count(*) from dbarnett.v223_all_sports_programmes_viewed;

commit;

--select service_key ,count(*) as records, count(distinct service_key) from vespa_analysts.Channel_map_prod_service_key_attributes group by service_key order by records desc
--select * from vespa_analysts.Channel_map_prod_service_key_attributes where service_key=1814 order by effective_from , effective_to;

commit;
--select @var_sql='vespa_analysts.VESPA_DAILY_AUGS_##^^*^*##'

--select @var_cntr
--select @day
--select top 100 * from ( replace(@var_sql,'##^^*^*##',@day) )

--select dk_programme_instance_dim,count(*) as records from dbarnett.v223_all_sports_programmes_viewed group by dk_programme_instance_dim order by records desc;

select a.dk_programme_instance_dim,cast(broadcast_start_date_time_utc as date) as dayval
,count(*) as records from dbarnett.v223_all_sports_programmes_viewed as a
left outer join sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.dk_programme_instance_dim,dayval
order by records desc
;



select count(*) from dbarnett.v223_all_sports_programmes_viewed where dk_programme_instance_dim is null




/*
select 
a.account_number
,programme_trans_sk as dk_programme_instance_dim
,max(left (cast(viewing_starts as varchar),10) ) as table_date
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
from  vespa_analysts.VESPA_DAILY_AUGS_20120801 a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where 
--panel_id = 12 and 
genre_description='Sports'
group by a.account_number
,dk_programme_instance_dim

select top 500 programme_trans_sk from vespa_analysts.VESPA_DAILY_AUGS_20120801

select * from sk_prod.Vespa_programme_schedule where pk_programme_instance_dim=44028101



*/




















commit;
sp_iqtablesize 'dbarnett.v223_unbundling_viewing_201308'
sp_iqtablesize 'dbarnett.adsmart'

Ownername,Tablename,Columns,KBytes,Pages,CompressedPages,NBlocks
'dbarnett','ADSMART','88','1282368','15442','13587','160296'
sp_iqtablesize 'dbarnett.v223_unbundling_viewing_201308'

--select count(*) from v223_unbundling_viewing_201308;
--select top 500 * from v223_unbundling_viewing_201308
--select * from  sk_prod.vespa_programme_schedule where pk_programme_instance_dim = 823615841


CREATE HG INDEX idx1 ON v223_unbundling_viewing_201308 (account_number);

CREATE HG INDEX idx2 ON v223_unbundling_viewing_201308 (channel_name_updated);
CREATE HG INDEX idx3 ON v223_unbundling_viewing_201308 (broadcast_start_date_time_utc );
CREATE HG INDEX idx4 ON v223_unbundling_viewing_201308 (genre_description);
CREATE HG INDEX idx5 ON v223_unbundling_viewing_201308 (sub_genre_description);
--Create Summary by Programme---
--drop table #summary_by_prog_201308;
select account_number
,channel_name_updated
,broadcast_start_date_time_utc
,genre_description
,case when sub_genre_description ='Undefined' and programme_name like '%UFC%' then 'UFC'   ---Not used due to low UFC Figures
 when sub_genre_description='Motor Sport' and 
    (programme_name like '%F1%' or  programme_name like '%Formula 1%') then 'Formula 1'
when channel_name_updated = 'Sky Sports F1' then 'Formula 1'
else sub_genre_description end as sub_genre
--,programme_name
,max(programme_instance_duration) as prog_duration
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_prog_201308
from v223_unbundling_viewing_201308
group by account_number
,channel_name_updated
,broadcast_start_date_time_utc
,genre_description
,sub_genre
--,programme_name
;





commit;
--select count(*) from v223_unbundling_viewing_201308;
CREATE HG INDEX idx1 ON #summary_by_prog_201308 (account_number);
CREATE HG INDEX idx2 ON #summary_by_prog_201308 (channel_name_updated);

--select sub_genre ,sum(seconds_viewed) as sec from   #summary_by_prog_201308 where genre_description='Sports' group by sub_genre order by sec desc

--select * from v200_zero_mix_viewing_201308;


--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---
--drop table v223_unbundling_viewing_201308_summary_by_account;
select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Football' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_football
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Formula 1' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Formula_1
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Motor Sport' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Motor_Sport_exc_F1
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Cricket' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Cricket
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Tennis' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Tennis
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Golf' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Golf
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Rugby' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Rugby
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Racing' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Racing
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Wrestling' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Wrestling
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Boxing' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Boxing
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='American Football' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_American_Football
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' 
and sub_genre='Athletics' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_Athletics
,sum(case when channel_category_inc_sports_movies='01: Sky Sports' then  seconds_viewed else 0 end) as seconds_viewed_Sky_Sports_ALL

,sum(case when channel_category_inc_sports_movies='05: BT Sport'
and sub_genre='Football' then  seconds_viewed else 0 end) as seconds_viewed_BT_Sport_football
,sum(case when channel_category_inc_sports_movies='05: BT Sport'
and sub_genre='Rugby' then  seconds_viewed else 0 end) as seconds_viewed_BT_Sport_Rugby
,sum(case when channel_category_inc_sports_movies='05: BT Sport' then  seconds_viewed else 0 end) as seconds_viewed_BT_Sport_ALL

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('ESPN','ESPN America','ESPN Classic')
and sub_genre='Football' then  seconds_viewed else 0 end) as seconds_viewed_ESPN_football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('ESPN','ESPN America','ESPN Classic') then  seconds_viewed else 0 end) as seconds_viewed_ESPN_ALL

 
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Football' then  seconds_viewed else 0 end) as seconds_viewed_FTA_football
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Formula 1' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Formula_1
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Motor Sport' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Motor_Sport_exc_F1
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Cricket' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Cricket
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Tennis' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Tennis
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Golf' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Golf
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Rugby' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Rugby
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Racing' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Racing
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Wrestling' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Wrestling
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Boxing' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Boxing
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='American Football' then  seconds_viewed else 0 end) as seconds_viewed_FTA_American_Football
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and sub_genre='Athletics' then  seconds_viewed else 0 end) as seconds_viewed_FTA_Athletics
,sum(case when channel_category_inc_sports_movies not in ('01: Sky Sports' ,'05 BT Sport') 
and channel_name_inc_hd_staggercast_channel_families not in ('ESPN','ESPN America','ESPN Classic')
and genre_description = 'Sports' then  seconds_viewed else 0 end) as seconds_viewed_Sports_FTA_ALL

,sum(case when channel_category_inc_sports_movies='02: Sky Movies' then seconds_viewed else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel' and genre_description <> 'Sports' then  seconds_viewed else 0 end) as seconds_viewed_non_premium_non_sport_Other_Pay
,sum(case when channel_category_inc_sports_movies='04: FTA Channel' and genre_description <> 'Sports' then  seconds_viewed else 0 end) as seconds_viewed_FTA_non_sport_Other_Pay

into v223_unbundling_viewing_201308_summary_by_account
from #summary_by_prog_201308 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name_updated = b.channel_name
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  v223_unbundling_viewing_201308_summary_by_account (account_number);
--select top 500 * from v223_unbundling_viewing_201308_summary_by_account;
commit;
--

--select top 500 * from  v223_unbundling_viewing_201308_summary_by_account


--drop table #days_viewing_by_account_201308;
select account_number 
,count (distinct cast(viewing_starts as date)) as distinct_viewing_days
into #days_viewing_by_account_201308
from v223_unbundling_viewing_201308 
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04')
group by account_number
;
commit;

CREATE HG INDEX idx1 ON  #days_viewing_by_account_201308 (account_number);

alter table v223_unbundling_viewing_201308_summary_by_account add distinct_viewing_days integer;

update v223_unbundling_viewing_201308_summary_by_account
set distinct_viewing_days=case when b.distinct_viewing_days is null then 0 else b.distinct_viewing_days end
from v223_unbundling_viewing_201308_summary_by_account as a
left outer join #days_viewing_by_account_201308 as b
on a.account_number = b.account_number
;
commit;

drop table v223_unbundling_viewing_201308;

commit;

--select top 100 * from v223_unbundling_viewing_201308_summary_by_account;

/*
select genre_description
,sub_genre_description
,channel_name_updated
,channel_category_inc_sports_movies
,sum(seconds_viewed) as total_dur
from #summary_by_prog_201309 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name_updated = b.channel_name
where channel_category_inc_sports_movies='01: Sky Sports' or genre_description='Sports'
or channel_name_updated='BT Sport'
group by genre_description
,sub_genre_description
,channel_name_updated
,channel_category_inc_sports_movies
order by total_dur desc

select channel_category_inc_sports_movies
,programme_name
,a.channel_name_updated
,genre_description
,sub_genre_description
,max(case when programme_name like '%UFC%' then 1 else 0 end) as UFC
,sum(seconds_viewed) as total_dur
from #summary_by_prog_201309 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name_updated = b.channel_name
--where channel_category_inc_sports_movies='01: Sky Sports' or genre_description='Sports'
--or 
where channel_name_updated='BT Sport'
group by channel_category_inc_sports_movies
,programme_name
,a.channel_name_updated
,genre_description
,sub_genre_description

order by total_dur desc

--drop table #service_keys;
select service_key
,count(*) as records
,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc)) as uncapped_seconds_viewed

into #service_keys
from sk_prod.vespa_dp_prog_viewed_201309
where  channel_name='Other TV' and genre_description='Sports'
group by service_key
;


select a.service_key
, epg_group_name
,channel_name
,bss_name
,a.uncapped_seconds_viewed
from #service_keys as a
left outer join #service_key_lookup2 as b
on a.service_key=b.service_key
order by a.uncapped_seconds_viewed desc




select epg_group_name
,channel_name
,bss_name
,service_key
,count(*) as records
into #service_key_lookup2
from sk_prod.Vespa_programme_schedule as b
group by  epg_group_name
,channel_name
,bss_name
,service_key
;

select service_key , count(*) as records from #service_key_lookup2 group by service_key order by records desc

select * from #service_key_lookup2 where service_key =1413



select channel_category_inc_sports_movies,programme_name, sub_genre_description ,channel_name_updated,
case when sub_genre_description ='Undefined' and programme_name like '%UFC%' then 'UFC' 
when sub_genre_description='Motor Sport' and 
    (programme_name like '%F1%' or  programme_name like '%Formula 1%') then 'Formula 1'
when channel_name_updated = 'Sky Sports F1' then 'Formula 1'
else sub_genre_description end as sub_genre,
sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
 from  v200_zero_mix_viewing_201309 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name_updated = b.channel_name
where genre_description='Sports' and sub_genre_description='Motor Sport' 
group by channel_category_inc_sports_movies,programme_name, sub_genre_description,channel_name_updated,sub_genre order by seconds_viewed desc



select 
case when sub_genre_description ='Undefined' and programme_name like '%UFC%' then 'UFC' 
when sub_genre_description='Motor Sport' and 
    (programme_name like '%F1%' or  programme_name like '%Formula 1%') then 'Formula 1'
when channel_name_updated = 'Sky Sports F1' then 'Formula 1'
else sub_genre_description end as sub_genre,
sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
 from  v200_zero_mix_viewing_201309 as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name_updated = b.channel_name
where genre_description='Sports' and channel_name_updated='BT Sport'
group by sub_genre order by seconds_viewed desc


update v200_channel_lookup_with_channel_family
set channel_category_inc_sports_movies= case    when channel_name_inc_hd_staggercast_channel_families =  'Sky Sports Channels' then '01: Sky Sports'
                                                when channel_name_inc_hd_staggercast_channel_families =  'Sky Movies Channels' then '02: Sky Movies'
                                                when channel_name_inc_hd_staggercast_channel_families =  'BT Sport' then '05: BT Sport'
                                                when pay_channel=1 then '03: Pay Channel' else '04: FTA Channel' end 

from v200_channel_lookup_with_channel_family as a
;
commit;


select * from v200_channel_lookup_with_channel_family order by channel_name_inc_hd_staggercast_channel_families


select count(distinct  dk_programme_dim )  from sk_prod.vespa_dp_prog_viewed_201308 where genre_description = 'Sports'

select top 100 * from sk_prod.vespa_dp_prog_viewed_201308



select 
a.account_number
,programme_trans_sk as dk_programme_instance_dim
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
from  vespa_analysts.VESPA_DAILY_AUGS_20120801 a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where 
--panel_id = 12 and 
genre_description='Sports'
group by a.account_number
,dk_programme_instance_dim




select * into dbarnett.v223_all_sports_programmes_viewed_copy from dbarnett.v223_all_sports_programmes_viewed
where account_number is null and account_number is not null


insert into dbarnett.v223_all_sports_programmes_viewed_copy
(account_number
,dk_programme_instance_dim 
,viewing_duration 
--,table_date
) 


select 
a.account_number
,pk_programme_instance_dim
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
from vespa_analysts.VESPA_DAILY_AUGS_20120801 a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where 
--panel_id = 12 and 
genre_description='Sports'
group by a.account_number
,pk_programme_instance_dim
;

select * from dbarnett.v223_all_sports_programmes_viewed_copy
--select count(*) from dbarnett.v223_all_sports_programmes_viewed_copy;
commit;
sp_iqtablesize 'dbarnett.v223_all_sports_programmes_viewed_copy'



select account_number
---Premier League (Mainly Live matches rather than related programmes--
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_premier_league

,case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end as viewing_duration_BT_Sport_premier_league

,case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end as viewing_duration_ESPN_premier_league

,case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' then  viewing_duration else 0 end as viewing_duration_Pick_TV_premier_league

---Other Football
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type<>'Premier League' and genre_description = 'Football' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_football_non_premier_league


,case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
and programme_sub_genre_type<>'Premier League' and genre_description = 'Football' then  viewing_duration else 0 end as viewing_duration_BT_Sport_non_premier_league

,case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type<>'Premier League' and genre_description = 'Football' then  viewing_duration else 0 end as viewing_duration_ESPN_non_premier_league

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky 3D','Sky Sports News','BT Sport','ESPN') 
and programme_sub_genre_type<>'Premier League' and genre_description = 'Football'  then  viewing_duration else 0 end as viewing_duration_football_other_channels
---SSN All
,case when channel_name_inc_hd_staggercast_channel_families='Sky Sports News' 
then  viewing_duration else 0 end as viewing_duration_Sky_Sports_News

----Cricket
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - non Ashes' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_cricket_exc_ashes

,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - Ashes' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_cricket_ashes

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - non Ashes' then  viewing_duration else 0 end as viewing_duration_cricket_exc_ashes_non_Sky_Sports_or_SSN

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - Ashes' then  viewing_duration else 0 end as viewing_duration_cricket_ashes_non_Sky_Sports_or_SSN

---Golf
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_golf_other

,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Ryder Cup' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_golf_ryder_cup

,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Major' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_golf_major

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_golf_other

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Ryder Cup' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_golf_ryder_cup

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Major' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_golf_major


---Tennis---
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Tennis' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_tennis

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Tennis' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_tennis_exc_wimbledon

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Wimbledon' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_wimbledon

---Motor Sport exc. F1--
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Motor Sport' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_motor_sport_exc_f1

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Motor Sport' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_motor_sport_exc_f1

---F1--
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Formula 1' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_motor_sport_Formula_1

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Formula 1' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_Formula_1

---Racing--
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Racing' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_horse_racing

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Racing' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_horse_racing

---Snooker/Pool--
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Snooker/Pool' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_snooker_pool

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Snooker/Pool' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SSN_snooker_pool

---Wrestling--
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Wrestling' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_inc_SBO_wrestling

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Wrestling' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SBO_wrestling




---Rugby----
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
 and genre_description = 'Rugby' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_Rugby

,case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
 and genre_description = 'Rugby'  then  viewing_duration else 0 end as viewing_duration_BT_Sport_rugby

,case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
 and genre_description = 'Rugby'  then  viewing_duration else 0 end as viewing_duration_ESPN_rugby

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','BT Sport','ESPN','Sky 3D') 
 and genre_description = 'Rugby'  then  viewing_duration else 0 end as viewing_duration_rugby_other_channels

---Darts
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Darts' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_Darts

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Darts' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_Darts

--Boxing
,case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Boxing' then  viewing_duration else 0 end as viewing_duration_Sky_Sports_inc_SBO_boxing

,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Boxing' then  viewing_duration else 0 end as viewing_duration_non_Sky_Sports_or_SBO_boxing



into #v223_unbundling_viewing_summary_by_account
from dbarnett.v223_all_sports_programmes_viewed  as a
left outer join dbarnett.v223_sports_epg_lookup as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;
*/