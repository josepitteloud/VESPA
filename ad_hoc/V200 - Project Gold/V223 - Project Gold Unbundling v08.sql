
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


update dbarnett.v223_sports_epg_lookup 
set programme_sub_genre_type= 'FA Cup'
where programme_sub_genre_type='FA CUP'
;

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

/*
update dbarnett.v223_sports_epg_lookup_aug_12_jul_13
set programme_sub_genre_type= 'FA Cup'
where programme_sub_genre_type='FA CUP'
;
*/
--drop table v223_unbundling_viewing_summary_by_account_sample;

--select top 100 *  from dbarnett.v223_all_sports_programmes_viewed;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit
drop table dbarnett.v223_all_sports_programmes_viewed_sample;

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed where right(account_number,1)='0';
commit;


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;
--select count(*) from v223_unbundling_viewing_summary_by_account;
select account_number
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

into v223_unbundling_viewing_summary_by_account
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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--select count(*) from dbarnett.v223_all_sports_programmes_viewed_sample ;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
commit;
--drop table v223_unbundling_viewing_summary_by_account;commit
--select * into v223_unbundling_viewing_summary_by_account from v223_unbundling_viewing_summary_by_account_sample where account_number is null and account_number is not null; commit
--select right(account_number,1) as lastchar,count(*) from v223_unbundling_viewing_summary_by_account group by lastchar order by lastchar;
--select top 10 * from v223_unbundling_viewing_summary_by_account;

insert into v223_unbundling_viewing_summary_by_account
select account_number
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
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_Football_League


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

select * into v223_broadcast_days_per_sub_type_cut_down from v223_broadcast_days_per_sub_type where programme_sub_genre_type_grouped in 
('Football League', 'Scottish Football','Cricket - Ashes','Formula 1','Premier League','Champions League','FA Cup'
,'World Cup Qualifiers','International Friendlies','Capital One Cup','La Liga')
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


commit;
CREATE HG INDEX idx2 ON  v223_broadcast_days_per_sub_type_watched_by_account (account_number);
/*
select account_number,count(*) as records from v223_broadcast_days_per_sub_type_watched_by_account group by account_number order by records desc;
select count(*),count(distinct account_number) from v223_broadcast_days_per_sub_type_watched_by_account;
select * from v223_broadcast_days_per_sub_type_watched_by_account order by account_number , days_viewing;
*/

--Rework to one record per account--
select account_number
,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Capital One Cup') then days_viewing else 0 end) as sky_capital_one_cup_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Champions League') then days_viewing else 0 end) as sky_champions_league_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Champions League') then days_viewing else 0 end) as other_champions_league_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Cricket - Ashes') then days_viewing else 0 end) as sky_ashes_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Cricket - Ashes') then days_viewing else 0 end) as other_ashes_days

,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('FA Cup') then days_viewing else 0 end) as other_fa_cup_days
,max(case when channel_type_grouped in ('ESPN/BT Sport') and programme_sub_genre_type_grouped in ('FA Cup') then days_viewing else 0 end) as espn_bt_fa_cup_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Football League') then days_viewing else 0 end) as sky_football_league_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('Formula 1') then days_viewing else 0 end) as sky_F1_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('Formula 1') then days_viewing else 0 end) as other_F1_days

,max(case when channel_type_grouped in ('Sky Sports Channels') and programme_sub_genre_type_grouped in ('International Friendlies') then days_viewing else 0 end) as sky_Inernational_friendlies_days
,max(case when channel_type_grouped in ('Other Channel') and programme_sub_genre_type_grouped in ('International Friendlies') then days_viewing else 0 end) as other_Inernational_friendlies_days

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

,66 as total_sky_football_league_days

,48 as total_sky_F1_days
,36 as total_other_F1_days

,6 as total_sky_international_friendlies_days
,15 as total_other_international_friendlies_days
,54 as total_sky_la_liga_days

,72 as total_sky_premier_league_days
,23 as total_espn_premier_league_days

,28 as total_sky_scottish_football_days
,13 as total_espn_scottish_football_days

into v223_days_viewing_genre_types_by_account
from v223_broadcast_days_per_sub_type_watched_by_account
group by account_number
;
commit;


CREATE HG INDEX idx1 ON  v223_days_viewing_genre_types_by_account (account_number);
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
--,minutes_sport_ESPN_BT real
--,minutes_sport_Other_Channels real

,minutes_football_sky_sports real
--,minutes_football_sky_sports_news real
,minutes_football_ESPN_BT real
--,minutes_football_Other_Channels real

,minutes_football_premier_league_sky_sports real
,minutes_football_premier_league_ESPN_BT real

,minutes_football_champions_league_sky_sports real
,minutes_football_champions_league_non_sky_sports real


,minutes_football_europa_league_ESPN_BT real
,minutes_football_europa_league_Other_Channels real

,minutes_football_fa_cup_sky_sports real
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

,minutes_cricket_sky_sports real
,minutes_cricket_non_sky_sports real

,minutes_golf_sky_sports real
,minutes_golf_non_sky_sports real

,minutes_tennis_sky_sports real
,minutes_tennis_non_sky_sports real

,minutes_motor_sport_sky_sports real
,minutes_motor_sport_non_sky_sports real

,minutes_golf_sky_sports real
,minutes_golf_non_sky_sports real

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

,minutes_darts_sky_sports real
,minutes_darts_non_sky_sports real

,minutes_boxing_sky_sports real
,minutes_boxing_non_sky_sports real

,rank_sport integer
,rank_sport_sky_sports integer
,rank_sport_sky_sports_news integer
,rank_sport_ESPN_BT integer
,rank_sport_Other_Channels integer

,rank_football_sky_sports integer
,rank_football_sky_sports_news integer
,rank_football_ESPN_BT integer
--,rank_football_Other_Channels integer

,rank_football_premier_league_sky_sports integer
,rank_football_premier_league_ESPN_BT integer

,rank_football_champions_league_sky_sports integer
,rank_football_champions_league_non_sky_sports integer

,rank_football_fa_cup_sky_sports integer
,rank_football_fa_cup_ESPN_BT integer
,rank_football_fa_cup_Other_Channels integer

,rank_football_world_cup_qualifier_sky_sports integer
,rank_football_world_cup_qualifier_non_sky_sports integer

,rank_football_international_friendly_sky_sports integer
,rank_football_international_friendly_non_sky_sports integer

,rank_football_Capital_One_Cup_sky_sports integer

,rank_football_La_Liga_sky_sports integer

,rank_football_football_league_sky_sports integer

,rank_cricket_sky_sports integer
,rank_cricket_non_sky_sports integer

,rank_golf_sky_sports integer
,rank_golf_non_sky_sports integer

,rank_tennis_sky_sports integer
,rank_tennis_non_sky_sports integer

,rank_motor_sport_sky_sports integer
,rank_motor_sport_non_sky_sports integer

,rank_golf_sky_sports integer
,rank_golf_non_sky_sports integer

,rank_F1_sky_sports integer
,rank_F1_non_sky_sports integer

,rank_horse_racing_sky_sports integer
,rank_horse_racing_non_sky_sports integer

,rank_snooker_pool_sky_sports integer
,rank_snooker_pool_non_sky_sports integer

,rank_rugby_sky_sports integer
,rank_rugby_non_sky_sports integer

,rank_wrestling_sky_sports integer
,rank_wrestling_non_sky_sports integer

,rank_darts_sky_sports integer
,rank_darts_non_sky_sports integer

,rank_boxing_sky_sports integer
,rank_boxing_non_sky_sports integer

)
;

update v223_unbundling_viewing_summary_by_account
set 

minutes_sport =(
viewing_duration_Sky_Sports_premier_league+
viewing_duration_BT_Sport_premier_league+
viewing_duration_ESPN_premier_league+
viewing_duration_Pick_TV_premier_league+
viewing_duration_Sky_Sports_champions_league+
viewing_duration_non_Sky_Sports_champions_league+
viewing_duration_BT_Sport_FA_Cup+
viewing_duration_ESPN_FA_Cup+
viewing_duration_other_FA_Cup+
viewing_duration_Sky_Sports_world_cup_qualifiers+
viewing_duration_non_Sky_Sports_world_cup_qualifiers+
viewing_duration_Sky_Sports_International_Friendlies+
viewing_duration_non_Sky_Sports_International_Friendlies+
viewing_duration_Sky_Sports_Scottish_Football+
viewing_duration_non_Sky_Sports_Scottish_Football+
viewing_duration_Sky_Sports_Capital_One_Cup+
viewing_duration_Sky_Sports_La_Liga+
viewing_duration_Sky_Sports_Football_League+
viewing_duration_Sky_Sports_News+
viewing_duration_Sky_Sports_cricket_exc_ashes+
viewing_duration_Sky_Sports_cricket_ashes+
viewing_duration_cricket_exc_ashes_non_Sky_Sports_or_SSN+
viewing_duration_cricket_ashes_non_Sky_Sports_or_SSN+
viewing_duration_Sky_Sports_golf_other+
viewing_duration_Sky_Sports_golf_ryder_cup+
viewing_duration_Sky_Sports_golf_major+
viewing_duration_non_Sky_Sports_or_SSN_golf_other+
viewing_duration_non_Sky_Sports_or_SSN_golf_ryder_cup+
viewing_duration_non_Sky_Sports_or_SSN_golf_major+
viewing_duration_Sky_Sports_tennis+
viewing_duration_non_Sky_Sports_or_SSN_tennis_exc_wimbledon+
viewing_duration_non_Sky_Sports_or_SSN_wimbledon+
viewing_duration_Sky_Sports_motor_sport_exc_f1+
viewing_duration_non_Sky_Sports_or_SSN_motor_sport_exc_f1+
viewing_duration_Sky_Sports_motor_sport_Formula_1+
viewing_duration_non_Sky_Sports_or_SSN_Formula_1+
viewing_duration_Sky_Sports_horse_racing+
viewing_duration_non_Sky_Sports_or_SSN_horse_racing+
viewing_duration_Sky_Sports_snooker_pool+
viewing_duration_non_Sky_Sports_or_SSN_snooker_pool+
viewing_duration_Sky_Sports_inc_SBO_wrestling+
viewing_duration_non_Sky_Sports_or_SBO_wrestling+
viewing_duration_Sky_Sports_Rugby+
viewing_duration_BT_Sport_rugby+
viewing_duration_ESPN_rugby+
viewing_duration_rugby_other_channels+
viewing_duration_Sky_Sports_Darts+
viewing_duration_non_Sky_Sports_Darts+
viewing_duration_Sky_Sports_inc_SBO_boxing+
viewing_duration_non_Sky_Sports_or_SBO_boxing
)/60*(365/days_with_viewing)


,minutes_sport_sky_sports =
(viewing_duration_Sky_Sports_premier_league+
viewing_duration_Sky_Sports_champions_league+
viewing_duration_Sky_Sports_world_cup_qualifiers+
viewing_duration_Sky_Sports_International_Friendlies+
viewing_duration_Sky_Sports_Scottish_Football+
viewing_duration_Sky_Sports_Capital_One_Cup+
viewing_duration_Sky_Sports_La_Liga+
viewing_duration_Sky_Sports_Football_League+
viewing_duration_Sky_Sports_cricket_exc_ashes+
viewing_duration_Sky_Sports_cricket_ashes+
viewing_duration_Sky_Sports_golf_other+
viewing_duration_Sky_Sports_golf_ryder_cup+
viewing_duration_Sky_Sports_golf_major+
viewing_duration_Sky_Sports_tennis+
viewing_duration_Sky_Sports_motor_sport_exc_f1+
viewing_duration_Sky_Sports_motor_sport_Formula_1+
viewing_duration_Sky_Sports_horse_racing+
viewing_duration_Sky_Sports_snooker_pool+
viewing_duration_Sky_Sports_inc_SBO_wrestling+
viewing_duration_Sky_Sports_Rugby
viewing_duration_Sky_Sports_Darts+
viewing_duration_Sky_Sports_inc_SBO_boxing
)/60*(365/days_with_viewing)


,minutes_sport_sky_sports_news =viewing_duration_Sky_Sports_News/60*(365/days_with_viewing)
--,minutes_sport_ESPN_BT =
--,minutes_sport_Other_Channels =


,minutes_football_sky_sports =
(viewing_duration_Sky_Sports_premier_league+
viewing_duration_Sky_Sports_champions_league+
viewing_duration_Sky_Sports_world_cup_qualifiers+
viewing_duration_Sky_Sports_International_Friendlies+
viewing_duration_Sky_Sports_Scottish_Football+
viewing_duration_Sky_Sports_Capital_One_Cup+
viewing_duration_Sky_Sports_La_Liga+
viewing_duration_Sky_Sports_Football_League)/60*(365/days_with_viewing)

,minutes_football_ESPN_BT =
(viewing_duration_BT_Sport_premier_league+
viewing_duration_ESPN_premier_league+
viewing_duration_BT_Sport_FA_Cup+
viewing_duration_ESPN_FA_Cup)/60*(365/days_with_viewing)

--,minutes_football_Other_Channels =

,minutes_football_premier_league_sky_sports =case when sky_premier_league_days= then -1 
when (total_sky_premier_league_days/sky_premier_league_days)< viewing_duration_Sky_Sports_premier_league/60*(total_sky_premier_league_days/sky_premier_league_days)

,minutes_football_premier_league_ESPN_BT =
(viewing_duration_BT_Sport_premier_league+
viewing_duration_ESPN_premier_league)/60*(total_espn_bt_premier_league_days/espn_bt_premier_league_days)


,minutes_football_champions_league_sky_sports =viewing_duration_Sky_Sports_champions_league/60*
(total_sky_champions_league_days/sky_champions_league_days)


,minutes_football_champions_league_non_sky_sports =

--commit;
,minutes_football_europa_league_ESPN_BT =
,minutes_football_europa_league_Other_Channels =

,minutes_football_fa_cup_sky_sports =
,minutes_football_fa_cup_ESPN_BT =
,minutes_football_fa_cup_Other_Channels =

,minutes_football_world_cup_qualifier_sky_sports =
,minutes_football_world_cup_qualifier_non_sky_sports =

,minutes_football_international_friendly_sky_sports =
,minutes_football_international_friendly_non_sky_sports =


,minutes_football_scottish_football_sky_sports =
,minutes_football_scottish_football_non_sky_sports =

,minutes_football_Capital_One_Cup_sky_sports =

,minutes_football_La_Liga_sky_sports =

,minutes_football_football_league_sky_sports =

,minutes_cricket_sky_sports =
,minutes_cricket_non_sky_sports =

,minutes_golf_sky_sports =
,minutes_golf_non_sky_sports =

,minutes_tennis_sky_sports =
,minutes_tennis_non_sky_sports =

,minutes_motor_sport_sky_sports =
,minutes_motor_sport_non_sky_sports =

,minutes_golf_sky_sports =
,minutes_golf_non_sky_sports =

,minutes_F1_sky_sports =
,minutes_F1_non_sky_sports =

,minutes_horse_racing_sky_sports =
,minutes_horse_racing_non_sky_sports =

,minutes_snooker_pool_sky_sports =
,minutes_snooker_pool_non_sky_sports =

,minutes_rugby_sky_sports =
,minutes_rugby_non_sky_sports =

,minutes_wrestling_sky_sports =
,minutes_wrestling_non_sky_sports =

,minutes_darts_sky_sports =
,minutes_darts_non_sky_sports =

,minutes_boxing_sky_sports =
,minutes_boxing_non_sky_sports =



















----Old code





minutes_football =(
viewing_duration_Sky_Sports_premier_league+viewing_duration_BT_Sport_premier_league+viewing_duration_ESPN_premier_league+viewing_duration_Pick_TV_premier_league+viewing_duration_Sky_Sports_football_non_premier_league
+ viewing_duration_BT_Sport_non_premier_league+viewing_duration_BT_Sport_non_premier_league+viewing_duration_ESPN_non_premier_league+viewing_duration_football_other_channels)/
cast((days_with_viewing*60/7) as real)
,minutes_cricket =(
viewing_duration_Sky_Sports_cricket_exc_ashes+viewing_duration_Sky_Sports_cricket_ashes
+viewing_duration_cricket_exc_ashes_non_Sky_Sports_or_SSN+viewing_duration_cricket_ashes_non_Sky_Sports_or_SSN)/
cast((days_with_viewing*60/7) as real)
,minutes_golf =(
viewing_duration_Sky_Sports_golf_other
+viewing_duration_Sky_Sports_golf_ryder_cup
+viewing_duration_Sky_Sports_golf_major
+viewing_duration_non_Sky_Sports_or_SSN_golf_other
+ viewing_duration_non_Sky_Sports_or_SSN_golf_ryder_cup
+viewing_duration_non_Sky_Sports_or_SSN_golf_major)/
cast((days_with_viewing*60/7) as real) 
,minutes_tennis =(
viewing_duration_Sky_Sports_tennis+viewing_duration_non_Sky_Sports_or_SSN_tennis_exc_wimbledon
+viewing_duration_non_Sky_Sports_or_SSN_wimbledon)/
cast((days_with_viewing*60/7) as real)
,minutes_motor_sport =(
viewing_duration_Sky_Sports_motor_sport_exc_f1+viewing_duration_non_Sky_Sports_or_SSN_motor_sport_exc_f1)/
cast((days_with_viewing*60/7) as real)
,minutes_F1 =(
viewing_duration_Sky_Sports_motor_sport_Formula_1+ viewing_duration_non_Sky_Sports_or_SSN_Formula_1)/
cast((days_with_viewing*60/7) as real)
,minutes_horse_racing =(
viewing_duration_Sky_Sports_horse_racing+viewing_duration_non_Sky_Sports_or_SSN_horse_racing)/
cast((days_with_viewing*60/7) as real)
,minutes_snooker_pool =(
viewing_duration_Sky_Sports_snooker_pool+viewing_duration_non_Sky_Sports_or_SSN_snooker_pool)/
cast((days_with_viewing*60/7) as real)
,minutes_rugby =(
viewing_duration_Sky_Sports_Rugby+viewing_duration_BT_Sport_rugby+viewing_duration_ESPN_rugby+viewing_duration_rugby_other_channels)/
cast((days_with_viewing*60/7) as real)
,minutes_wrestling =(
viewing_duration_Sky_Sports_inc_SBO_wrestling+viewing_duration_non_Sky_Sports_or_SBO_wrestling)/
cast((days_with_viewing*60/7) as real) 
,minutes_darts =(
viewing_duration_Sky_Sports_Darts+viewing_duration_non_Sky_Sports_Darts)/
cast((days_with_viewing*60/7) as real)
,minutes_boxing =(
viewing_duration_Sky_Sports_inc_SBO_boxing+viewing_duration_non_Sky_Sports_or_SBO_boxing)/
cast((days_with_viewing*60/7) as real)
from v223_unbundling_viewing_summary_by_account as a
left outer join v223_days_viewing_genre_types_by_account as b
on a.account_number =b.account_number
where days_with_viewing>=280
;
commit;
--select top 100 * from v223_unbundling_viewing_summary_by_account
---Update Ranks
--drop table #rank_details;
select account_number
,rank() over (  ORDER BY  minutes_football desc) as rank_football
,rank() over (  ORDER BY  minutes_cricket desc) as rank_cricket
,rank() over (  ORDER BY  minutes_golf desc) as rank_golf
,rank() over (  ORDER BY  minutes_motor_sport desc) as rank_motor_sport
,rank() over (  ORDER BY  minutes_F1 desc) as rank_F1
,rank() over (  ORDER BY  minutes_tennis desc) as rank_tennis
,rank() over (  ORDER BY  minutes_horse_racing desc) as rank_horse_racing
,rank() over (  ORDER BY  minutes_snooker_pool desc) as rank_snooker_pool
,rank() over (  ORDER BY  minutes_rugby desc) as rank_rugby
,rank() over (  ORDER BY  minutes_wrestling desc) as rank_wrestling
,rank() over (  ORDER BY  minutes_darts desc) as rank_darts
,rank() over (  ORDER BY  minutes_boxing desc) as rank_boxing
into #rank_details
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;


exec sp_create_tmp_table_idx '#rank_details', 'account_number';
commit;

update v223_unbundling_viewing_summary_by_account
set 
rank_football =b.rank_football
,rank_cricket =b.rank_cricket
,rank_golf =b.rank_golf
,rank_tennis =b.rank_tennis
,rank_motor_sport =b.rank_motor_sport
,rank_F1 =b.rank_F1
,rank_horse_racing =b.rank_horse_racing
,rank_snooker_pool =b.rank_snooker_pool
,rank_rugby =b.rank_rugby
,rank_wrestling =b.rank_wrestling
,rank_darts =b.rank_darts
,rank_boxing =b.rank_boxing
from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_details as b
on a.account_number = b.account_number
where days_with_viewing>=280
;
commit;


--drop table v223_unbundling_sports_groupings_all_channels; commit;

select case when minutes_per_week_football <5 then '04:Football Under 5 Minutes per Week Average'
when rank_football <=30000 then '01:Football  Top 10%'
when rank_football <=100000 then '02:Football  Medium 11% to 33%' else '03:Football Low (with 5+ minutes per Week Average)' end as grouping_football_rank

,case when minutes_per_week_cricket <5 then '04:Cricket Under 5 Minutes per Week Average'
when rank_cricket <=30000 then '01:Cricket Top 10%'
when rank_cricket <=100000 then '02:Cricket Medium 11% to 33%' else '03:Cricket Low (with 5+ minutes per Week Average)' end as grouping_cricket_rank

,case when minutes_per_week_golf <5 then '04:Golf Under 5 Minutes per Week Average'
when rank_golf <=30000 then '01:Golf Top 10%'
when rank_golf <=100000 then '02:Golf Medium 11% to 33%' else '03:Golf Low (with 5+ minutes per Week Average)' end as grouping_golf_rank

,case when minutes_per_week_tennis <5 then '04:Tennis Under 5 Minutes per Week Average'
when rank_tennis <=30000 then '01:Tennis Top 10%'
when rank_tennis <=100000 then '02:Tennis Medium 11% to 33%' else '03:Tennis Low (with 5+ minutes per Week Average)' end as grouping_tennis_rank

,case when minutes_per_week_motor_sport <5 then '04:Motor Sport Under 5 Minutes per Week Average'
when rank_motor_sport <=30000 then '01:Motor Sport Top 10%'
when rank_motor_sport <=100000 then '02:Motor Sport Medium 11% to 33%' else '03:Motor Sport Low (with 5+ minutes per Week Average)' end as grouping_motor_sport_rank

,case when minutes_per_week_F1 <5 then '04:F1 Under 5 Minutes per Week Average'
when rank_F1 <=30000 then '01:F1 Top 10%'
when rank_F1 <=100000 then '02:F1 Medium 11% to 33%' else '03:F1 Low (with 5+ minutes per Week Average)' end as grouping_F1_rank

,case when minutes_per_week_horse_racing <5 then '04:Horse Racing Under 5 Minutes per Week Average'
when rank_horse_racing <=30000 then '01:Horse Racing Top 10%'
when rank_horse_racing <=100000 then '02:Horse Racing Medium 11% to 33%' else '03:Horse Racing Low (with 5+ minutes per Week Average)' end as grouping_horse_racing_rank

,case when minutes_per_week_snooker_pool <5 then '04:Snooker/Pool Under 5 Minutes per Week Average'
when rank_snooker_pool <=30000 then '01:Snooker/Pool Top 10%'
when rank_snooker_pool <=100000 then '02:Snooker/Pool Medium 11% to 33%' else '03:Snooker/Pool Low (with 5+ minutes per Week Average)' end as grouping_snooker_pool_rank

,case when minutes_per_week_rugby <5 then '04:Rugby Under 5 Minutes per Week Average'
when rank_rugby <=30000 then '01:Rugby Top 10%'
when rank_rugby <=100000 then '02:Rugby Medium 11% to 33%' else '03:Rugby Low (with 5+ minutes per Week Average)' end as grouping_rugby_rank

,case when minutes_per_week_wrestling <5 then '04:Wrestling Under 5 Minutes per Week Average'
when rank_wrestling <=30000 then '01:Wrestling Top 10%'
when rank_wrestling <=100000 then '02:Wrestling Medium 11% to 33%' else '03:Wrestling Low (with 5+ minutes per Week Average)' end as grouping_wrestling_rank

,case when minutes_per_week_darts <5 then '04:Darts Under 5 Minutes per Week Average'
when rank_darts <=30000 then '01: Top 10%'
when rank_darts <=100000 then '02: Medium 11% to 33%' else '03: Low (with 5+ minutes per Week Average)' end as grouping_darts_rank

,case when minutes_per_week_boxing <5 then '04: Under 5 Minutes per Week Average'
when rank_boxing <=30000 then '01:Darts Top 10%'
when rank_boxing <=100000 then '02:Darts Medium 11% to 33%' else '03:Darts Low (with 5+ minutes per Week Average)' end as grouping_boxing_rank



,case when minutes_per_week_football <5 then '04:Football Under 5 Minutes per Week Average'
when minutes_per_week_football >=30 then '01:Football 30+ Minutes per Week Average'
when minutes_per_week_football   >=15 then '02:Football 15 to 29 Minutes per Week' else '03:Football 5-14 Minutes per Week Average' end as grouping_football_duration

,case when minutes_per_week_cricket <5 then '04:Cricket Under 5 Minutes per Week Average'
when minutes_per_week_cricket >=30 then '01:Cricket 30+ Minutes per Week Average'
when minutes_per_week_cricket   >=15 then '02:Cricket 15 to 29 Minutes per Week' else '03:Cricket 5-14 Minutes per Week Average' end as grouping_cricket_duration

,case when minutes_per_week_golf <5 then '04:Golf Under 5 Minutes per Week Average'
when minutes_per_week_golf >=30 then '01:Golf 30+ Minutes per Week Average'
when minutes_per_week_golf   >=15 then '02:Golf 15 to 29 Minutes per Week' else '03:Golf 5-14 Minutes per Week Average' end as grouping_golf_duration

,case when minutes_per_week_tennis <5 then '04:Tennis Under 5 Minutes per Week Average'
when minutes_per_week_tennis >=30 then '01:Tennis 30+ Minutes per Week Average'
when minutes_per_week_tennis   >=15 then '02:Tennis 15 to 29 Minutes per Week' else '03:Tennis 5-14 Minutes per Week Average' end as grouping_tennis_duration

,case when minutes_per_week_motor_sport <5 then '04:Motor Sport Under 5 Minutes per Week Average'
when minutes_per_week_motor_sport >=30 then '01:Motor Sport 30+ Minutes per Week Average'
when minutes_per_week_motor_sport   >=15 then '02:Motor Sport 15 to 29 Minutes per Week' else '03:Motor Sport 5-14 Minutes per Week Average' end as grouping_motor_sport_duration

,case when minutes_per_week_F1 <5 then '04:F1 Under 5 Minutes per Week Average'
when minutes_per_week_F1 >=30 then '01:F1 30+ Minutes per Week Average'
when minutes_per_week_F1   >=15 then '02:F1 15 to 29 Minutes per Week' else '03:F1 5-14 Minutes per Week Average' end as grouping_F1_duration

,case when minutes_per_week_horse_racing <5 then '04:Horse Racing Under 5 Minutes per Week Average'
when minutes_per_week_horse_racing >=30 then '01:Horse Racing 30+ Minutes per Week Average'
when minutes_per_week_horse_racing   >=15 then '02:Horse Racing 15 to 29 Minutes per Week' else '03:Horse Racing 5-14 Minutes per Week Average' end as grouping_horse_racing_duration

,case when minutes_per_week_snooker_pool <5 then '04:Snooker/Pool Under 5 Minutes per Week Average'
when minutes_per_week_snooker_pool >=30 then '01:Snooker/Pool 30+ Minutes per Week Average'
when minutes_per_week_snooker_pool   >=15 then '02:Snooker/Pool 15 to 29 Minutes per Week' else '03:Snooker/Pool 5-14 Minutes per Week Average' end as grouping_snooker_pool_duration

,case when minutes_per_week_rugby <5 then '04:Rugby Under 5 Minutes per Week Average'
when minutes_per_week_rugby >=30 then '01:Rugby 30+ Minutes per Week Average'
when minutes_per_week_rugby   >=15 then '02:Rugby 15 to 29 Minutes per Week' else '03:Rugby 5-14 Minutes per Week Average' end as grouping_rugby_duration

,case when minutes_per_week_wrestling <5 then '04:Wrestling Under 5 Minutes per Week Average'
when minutes_per_week_wrestling >=30 then '01:Wrestling 30+ Minutes per Week Average'
when minutes_per_week_wrestling   >=15 then '02:Wrestling 15 to 29 Minutes per Week' else '03:Wrestling 5-14 Minutes per Week Average' end as grouping_wrestling_duration

,case when minutes_per_week_darts <5 then '04:Darts Under 5 Minutes per Week Average'
when minutes_per_week_darts >=30 then '01:Darts 30+ Minutes per Week Average'
when minutes_per_week_darts   >=15 then '02:Darts 15 to 29 Minutes per Week' else '03:Darts 5-14 Minutes per Week Average' end as grouping_darts_duration

,case when minutes_per_week_boxing <5 then '04:Boxing Under 5 Minutes per Week Average'
when minutes_per_week_boxing >=30 then '01:Boxing 30+ Minutes per Week Average'
when minutes_per_week_boxing >=15 then '02:Boxing 15 to 29 Minutes per Week Average' else '03:Boxing 5-14 Minutes per Week Average' end as grouping_boxing_duration


,count(*) as accounts
,sum(minutes_per_week_football) as total_average_football_duration_per_week
,sum(minutes_per_week_cricket) as total_average_cricket_duration_per_week
,sum(minutes_per_week_golf) as total_average_golf_duration_per_week
,sum(minutes_per_week_tennis) as total_average_tennis_duration_per_week
,sum(minutes_per_week_motor_sport) as total_average_motor_sport_duration_per_week
,sum(minutes_per_week_F1) as total_average_F1_duration_per_week
,sum(minutes_per_week_horse_racing) as total_average_horse_racing_duration_per_week
,sum(minutes_per_week_snooker_pool) as total_average_snooker_pool_duration_per_week
,sum(minutes_per_week_rugby) as total_average_rugby_duration_per_week
,sum(minutes_per_week_wrestling) as total_average_wrestling_duration_per_week
,sum(minutes_per_week_darts) as total_average_darts_duration_per_week
,sum(minutes_per_week_boxing) as total_average_boxing_duration_per_week
into v223_unbundling_sports_groupings_all_channels
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by grouping_football_rank
,grouping_cricket_rank
,grouping_golf_rank
,grouping_tennis_rank
,grouping_motor_sport_rank
,grouping_F1_rank
,grouping_horse_racing_rank
,grouping_snooker_pool_rank
,grouping_rugby_rank
,grouping_wrestling_rank
,grouping_darts_rank
,grouping_boxing_rank

,grouping_football_duration
,grouping_cricket_duration
,grouping_golf_duration
,grouping_tennis_duration
,grouping_motor_sport_duration
,grouping_F1_duration
,grouping_horse_racing_duration
,grouping_snooker_pool_duration
,grouping_rugby_duration
,grouping_wrestling_duration
,grouping_darts_duration
,grouping_boxing_duration
;

commit;

grant all on v223_unbundling_sports_groupings_all_channels to public;
commit;


----Analysis of Programme Duration--

--select top 100 * from dbarnett.v223_all_sports_programmes_viewed;
--select top 100 * from dbarnett.v223_sports_epg_lookup_aug_12_jul_13;

select programme_instance_name
,broadcast_start_date_time_local
,programme_instance_duration
,channel_name_inc_hd_staggercast_channel_families
,programme_sub_genre_type
,abs(viewing_duration/60) as minutes_viewed
,count(*) as records
into #programme_viewing_distribution
from dbarnett.v223_all_sports_programmes_viewed  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where b.dk_programme_instance_dim is not null
group by programme_instance_name
,broadcast_start_date_time_local
,programme_instance_duration
,channel_name_inc_hd_staggercast_channel_families
,programme_sub_genre_type
,minutes_viewed
;

---Order by Number of Viewers--
--drop table #programme_viewing_distribution_summary_by_account;
select programme_instance_name
,broadcast_start_date_time_local
,programme_instance_duration
,channel_name_inc_hd_staggercast_channel_families
,programme_sub_genre_type
,rank () over (partition by programme_sub_genre_type order by accounts desc) as rank_programme_sub_genre_type
,sum(records) as accounts
into #programme_viewing_distribution_summary_by_account
from #programme_viewing_distribution
group by programme_instance_name
,broadcast_start_date_time_local
,programme_instance_duration
,channel_name_inc_hd_staggercast_channel_families
,programme_sub_genre_type
;


select * into #top_50_progs 
from #programme_viewing_distribution_summary_by_account  
where rank_programme_sub_genre_type<=50 order by accounts desc
;

select a.programme_sub_genre_type
,case when minutes_viewed>=600 then 600 else minutes_viewed end as minutes_total
,sum(records) as total_records
from #top_50_progs as a
left outer join #programme_viewing_distribution as b
on a.programme_instance_name=b.programme_instance_name
and a.broadcast_start_date_time_local=b.broadcast_start_date_time_local
and a.programme_instance_duration=b.programme_instance_duration
and a.channel_name_inc_hd_staggercast_channel_families=b.channel_name_inc_hd_staggercast_channel_families
and a.programme_sub_genre_type=b.programme_sub_genre_type
group by a.programme_sub_genre_type
,minutes_total
order by a.programme_sub_genre_type
,minutes_total
;

commit;


---Create table of Possible Viewing Days per sub genre type--
select broadcast_start_date_time_local
,case when channel_name_inc_hd_staggercast_channel_families = 'Sky Sports Channels' then 'Sky Sports' 
else 'non Sky Sports' end as channel_type
,programme_sub_genre_type
,abs(viewing_duration/60) as minutes_viewed
,count(*) as records
into #programme_viewing_distribution
from dbarnett.v223_all_sports_programmes_viewed  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where b.dk_programme_instance_dim is not null
group by programme_instance_name
,broadcast_start_date_time_local
,programme_instance_duration
,channel_name_inc_hd_staggercast_channel_families
,programme_sub_genre_type
,minutes_viewed
;








---Get Groupings---


select (
viewing_duration_Sky_Sports_premier_league+viewing_duration_BT_Sport_premier_league+viewing_duration_ESPN_premier_league+viewing_duration_Pick_TV_premier_league+viewing_duration_Sky_Sports_football_non_premier_league
+ viewing_duration_BT_Sport_non_premier_league+viewing_duration_BT_Sport_non_premier_league+viewing_duration_ESPN_non_premier_league+viewing_duration_football_other_channels)/
(days_with_viewing*60/7) as minutes_per_week_football
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_football
order by minutes_per_week_football
;

--Cricket
select (
viewing_duration_Sky_Sports_cricket_exc_ashes+viewing_duration_Sky_Sports_cricket_ashes
+viewing_duration_cricket_exc_ashes_non_Sky_Sports_or_SSN+viewing_duration_cricket_ashes_non_Sky_Sports_or_SSN)/
(days_with_viewing*60/7) as minutes_per_week_cricket
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_cricket
order by minutes_per_week_cricket
;

--Golf
select (
viewing_duration_Sky_Sports_golf_other
+viewing_duration_Sky_Sports_golf_ryder_cup
+viewing_duration_Sky_Sports_golf_major
+viewing_duration_non_Sky_Sports_or_SSN_golf_other
+ viewing_duration_non_Sky_Sports_or_SSN_golf_ryder_cup
+viewing_duration_non_Sky_Sports_or_SSN_golf_major)/
(days_with_viewing*60/7) as minutes_per_week_golf
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_golf
order by minutes_per_week_golf
;

--Tennis
select (
viewing_duration_Sky_Sports_tennis+viewing_duration_non_Sky_Sports_or_SSN_tennis_exc_wimbledon
+viewing_duration_non_Sky_Sports_or_SSN_wimbledon)/
(days_with_viewing*60/7) as 
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_tennis
order by minutes_per_week_tennis
;

--Motor Sport
select (
viewing_duration_Sky_Sports_motor_sport_exc_f1+viewing_duration_non_Sky_Sports_or_SSN_motor_sport_exc_f1)/
(days_with_viewing*60/7) as minutes_per_week_motor_sport
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_motor_sport
order by minutes_per_week_motor_sport
;

commit;

--F1
select (
viewing_duration_Sky_Sports_motor_sport_Formula_1+ viewing_duration_non_Sky_Sports_or_SSN_Formula_1)/
(days_with_viewing*60/7) as minutes_per_week_F1
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_F1
order by minutes_per_week_F1
;


--Horse Racing
select (
viewing_duration_Sky_Sports_horse_racing+viewing_duration_non_Sky_Sports_or_SSN_horse_racing)/
(days_with_viewing*60/7) as minutes_per_week_horse_racing
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_horse_racing
order by minutes_per_week_horse_racing
;


--Snooker Pool
select (
viewing_duration_Sky_Sports_snooker_pool+viewing_duration_non_Sky_Sports_or_SSN_snooker_pool)/
(days_with_viewing*60/7) as minutes_per_week_snooker_pool
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_snooker_pool
order by minutes_per_week_snooker_pool
;


--Rugby
select (
viewing_duration_Sky_Sports_Rugby+viewing_duration_BT_Sport_rugby+viewing_duration_ESPN_rugby+viewing_duration_rugby_other_channels)/
(days_with_viewing*60/7) as minutes_per_week_rugby
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_rugby
order by minutes_per_week_rugby
;

--Wrestling
select (
viewing_duration_Sky_Sports_inc_SBO_wrestling+viewing_duration_non_Sky_Sports_or_SBO_wrestling)/
(days_with_viewing*60/7) as minutes_per_week_wrestling
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_wrestling
order by minutes_per_week_wrestling
;


--Darts
select (
viewing_duration_Sky_Sports_Darts+viewing_duration_non_Sky_Sports_Darts)/
(days_with_viewing*60/7) as minutes_per_week_darts
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_darts
order by minutes_per_week_darts
;

--Boxing
select (
viewing_duration_Sky_Sports_inc_SBO_boxing+viewing_duration_non_Sky_Sports_or_SBO_boxing)/
(days_with_viewing*60/7) as minutes_per_week_boxing
,count(*) as records

from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
group by minutes_per_week_boxing
order by minutes_per_week_boxing
;

commit;



/*
channel_type_grouped,programme_sub_genre_type,broadcast_days
'ESPN/BT Sport      ','Scottish Football League',7
'Sky Sports Channels','Scottish Cup',8
'Other Channel      ','Scottish Cup',3
'Sky Sports Channels','SPL',17

select viewing_duration_Sky_Sports_premier_league
,days_with_viewing
,
(viewing_duration_Sky_Sports_premier_league)/60*(365/cast(days_with_viewing as real)) as sportdur
,cast (sky_premier_league_days  as real)/total_sky_premier_league_days
,sky_premier_league_days
,total_sky_premier_league_days
from v223_unbundling_viewing_summary_by_account as a
left outer join v223_days_viewing_genre_types_by_account as b
on a.account_number =b.account_number
where days_with_viewing>=280
*/