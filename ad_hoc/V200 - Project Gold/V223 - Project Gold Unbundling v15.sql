
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

select * into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed as a
where account_number is null and account_number is not null
;

insert into dbarnett.v223_all_sports_programmes_viewed_sample
(select a.* into dbarnett.v223_all_sports_programmes_viewed_sample 
from dbarnett.v223_all_sports_programmes_viewed as a
left outer join #count_by_day_v1 as b
on a.account_number = b.account_number
--where account_number = '621056141909'
where b.days_with_viewing>=280
);
commit;


CREATE HG INDEX idx1 ON dbarnett.v223_all_sports_programmes_viewed_sample (dk_programme_instance_dim);
CREATE HG INDEX idx2 ON dbarnett.v223_all_sports_programmes_viewed_sample (account_number);
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

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='WWE' then  viewing_duration else 0 end) as viewing_duration_Sky_Sports_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('SBO')
and programme_sub_genre_type='WWE' then  viewing_duration else 0 end) as viewing_duration_SBO_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky 1 or 2') 
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






---Repeat for Number of Programmes viewed>=180 sec
,sum(case when viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_total

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_BT_Sport_total

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_ESPN_total

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
,sum(case when programme_sub_genre_type='WWE' then viewing_duration else 0 end) as programmes_3min_plus_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='WWE' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_Sky_Sports_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('SBO')
and programme_sub_genre_type='WWE' and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_SBO_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky 1 or 2') 
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






---Repeat for Number where 60% or 1hr Viewed---
,sum(case when viewing_duration>=180 then 1 else 0 end) as programmes_engaged_sports

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
 and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_total

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_total

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_total

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
 and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Terrestrial_total

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('BBC ONE' ,'BBC TWO','ITV1','Channel 4','Channel 5')
and sub_genre_description='Football'
 and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Terrestrial_football


---Premier League (Mainly Live matches rather than related programmes--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Premier League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Premier League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Premier League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_premier_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky 1 or 2' 
and programme_sub_genre_type='Premier League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Pick_TV_premier_league

--Champions League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Champions League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_champions_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='Champions League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_champions_league

--FA Cup--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='FA Cup' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='FA Cup' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_FA_Cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports News') 
and programme_sub_genre_type='FA Cup' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_other_FA_Cup

--Europa League--
,sum(case when channel_name_inc_hd_staggercast_channel_families ='BT Sport' 
and programme_sub_genre_type='Europa League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
and programme_sub_genre_type='Europa League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_europa_league

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('BT Sport' ,'ESPN','Sky Sports Channels' ,'Sky 3D','Sky Sports News') 
and programme_sub_genre_type='Europa League' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_other_europa_league


--World Cup Qualifiers
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='World Cup Qualifiers' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_world_cup_qualifiers

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='World Cup Qualifiers' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_world_cup_qualifiers

--select distinct genre_description,programme_sub_genre_type from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 order by genre_description ,programme_sub_genre_type;

--International Friendlies
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='International Friendlies' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_International_Friendlies

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type='International Friendlies' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_International_Friendlies

--Scottish Football
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League') and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Scottish_Football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D','Sky Sports News')
and programme_sub_genre_type in ('SPL','Scottish Cup','Scottish Football League')  and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_Scottish_Football

--Capital One Cup
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('Capital One Cup') and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Capital_One_Cup

--La Liga
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('La Liga') and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_La_Liga

--Football League
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type in ('npower Championship','npower League One','npower League Two','Football League') 
and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Football_League


---SSN All
,sum(case when channel_name_inc_hd_staggercast_channel_families='Sky Sports News' 
and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_News

----Cricket
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - non Ashes' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_cricket_exc_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Cricket - Ashes' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_cricket_ashes

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - non Ashes' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_cricket_exc_ashes_non_Sky_Sports_or_SSN

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Cricket - Ashes' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_cricket_ashes_non_Sky_Sports_or_SSN

---Golf
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Ryder Cup' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and programme_sub_genre_type='Golf - Major' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_golf_major

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_golf_other

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Ryder Cup' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_golf_ryder_cup

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Golf - Major' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_golf_major


---Tennis---
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Tennis' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_tennis

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Tennis' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_tennis_exc_wimbledon

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Wimbledon' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_wimbledon

---Motor Sport exc. F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Motor Sport' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_motor_sport_exc_f1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Motor Sport' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_motor_sport_exc_f1

---F1--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Formula 1' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_motor_sport_Formula_1

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Formula 1' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_Formula_1

---Racing--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Racing' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_horse_racing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Racing' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_horse_racing

---Snooker/Pool--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Snooker/Pool' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_snooker_pool

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','Sky 3D') 
and programme_sub_genre_type='Snooker/Pool' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SSN_snooker_pool

---Wrestling--
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Wrestling' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_inc_SBO_wrestling

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Wrestling' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SBO_wrestling


---WWE--
,sum(case when programme_sub_genre_type='WWE' then viewing_duration else 0 end) as programmes_engaged_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='WWE' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('SBO')
and programme_sub_genre_type='WWE' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_SBO_WWE

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky 1 or 2') 
and programme_sub_genre_type='WWE' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_1_or_2_WWE


---Rugby----
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
 and sub_genre_description = 'Rugby' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='BT Sport' 
 and sub_genre_description = 'Rugby'  and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_BT_Sport_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families='ESPN' 
 and sub_genre_description = 'Rugby'  and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_ESPN_rugby

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','Sky Sports News','BT Sport','ESPN','Sky 3D') 
 and sub_genre_description = 'Rugby'  and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_rugby_other_channels

---Darts
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Darts' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_Darts

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Darts' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_Darts

--Boxing
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels','SBO','Sky 3D')
and programme_sub_genre_type='Boxing' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_Sky_Sports_inc_SBO_boxing

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Boxing' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) as programmes_engaged_non_Sky_Sports_or_SBO_boxing

into v223_unbundling_viewing_summary_by_account
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
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
(viewing_duration_Sky_Sports_premier_league+
viewing_duration_Sky_Sports_champions_league+
viewing_duration_Sky_Sports_world_cup_qualifiers+
viewing_duration_Sky_Sports_International_Friendlies+
viewing_duration_Sky_Sports_Scottish_Football+
viewing_duration_Sky_Sports_Capital_One_Cup+
viewing_duration_Sky_Sports_La_Liga+
viewing_duration_Sky_Sports_Football_League
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


/*

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
*/

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



from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details_engaged as b
on a.account_number = b.account_number
where days_with_viewing>=280
;
commit;






--select top 500 * from  v223_unbundling_viewing_summary_by_account where days_with_viewing>=280 
--select rank_minutes_football_capital_one_cup_sky_sports , count(*) as records from  v223_unbundling_viewing_summary_by_account where days_with_viewing>=280 group by rank_minutes_football_capital_one_cup_sky_sports order by records desc

--Get Rank Value where duration is 0 (none watched) and -1 (Insufficient data)

---Create Deciles---

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

from v223_unbundling_viewing_summary_by_account 
;
commit;
--select * from v223_unbundling_viewing_summary_by_account
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
select account_weight
,decile_sport 
,decile_sport_sky_sports 
,decile_sport_sky_sports_news 

,decile_football_sky_sports 

,decile_football_premier_league_sky_sports 
,decile_football_premier_league_ESPN_BT 

,decile_football_champions_league_sky_sports 
,decile_football_champions_league_non_sky_sports 

,decile_football_fa_cup_ESPN_BT 
,decile_football_fa_cup_Other_Channels 

,decile_football_world_cup_qualifier_sky_sports 
,decile_football_world_cup_qualifier_non_sky_sports 

,decile_football_international_friendly_sky_sports 
,decile_football_international_friendly_non_sky_sports 

,decile_football_Capital_One_Cup_sky_sports 

,decile_football_La_Liga_sky_sports 

,decile_football_football_league_sky_sports 

,decile_cricket_ashes_sky_sports 
,decile_cricket_ashes_non_sky_sports 

,decile_cricket_non_ashes_sky_sports 
,decile_cricket_non_ashes_non_sky_sports 

,decile_golf_sky_sports 
,decile_golf_non_sky_sports 

,decile_tennis_sky_sports 
,decile_tennis_non_sky_sports 

,decile_motor_sport_sky_sports 
,decile_motor_sport_non_sky_sports 

,decile_F1_sky_sports 
,decile_F1_non_sky_sports 

,decile_horse_racing_sky_sports 
,decile_horse_racing_non_sky_sports 

,decile_snooker_pool_sky_sports 
,decile_snooker_pool_non_sky_sports 

,decile_rugby_sky_sports 
,decile_rugby_non_sky_sports 

,decile_wrestling_sky_sports 
,decile_wrestling_non_sky_sports 

,decile_darts_sky_sports 
,decile_darts_non_sky_sports 

,decile_boxing_sky_sports 
,decile_boxing_non_sky_sports 


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


,(minutes_sport *account_weight) as minutes_sport_total
,(minutes_sport_sky_sports *account_weight) as minutes_sport_sky_sports_total
,(minutes_sport_sky_sports_news *account_weight) as minutes_sport_sky_sports_news_total

,(minutes_football_sky_sports *account_weight) as minutes_football_sky_sports_total

,(minutes_football_premier_league_sky_sports *account_weight) as minutes_football_premier_league_sky_sports_total
,(minutes_football_premier_league_ESPN_BT *account_weight) as minutes_football_premier_league_ESPN_BT_total

,(minutes_football_champions_league_sky_sports *account_weight) as minutes_football_champions_league_sky_sports_total
,(minutes_football_champions_league_non_sky_sports *account_weight) as minutes_football_champions_league_non_sky_sports_total

,(minutes_football_fa_cup_ESPN_BT *account_weight) as minutes_football_fa_cup_ESPN_BT_total
,(minutes_football_fa_cup_Other_Channels *account_weight) as minutes_football_fa_cup_Other_Channels_total

,(minutes_football_world_cup_qualifier_sky_sports *account_weight) as minutes_football_world_cup_qualifier_sky_sports_total
,(minutes_football_world_cup_qualifier_non_sky_sports *account_weight) as minutes_football_world_cup_qualifier_non_sky_sports_total

,(minutes_football_international_friendly_sky_sports *account_weight) as minutes_football_international_friendly_sky_sports_total
,(minutes_football_international_friendly_non_sky_sports *account_weight) as minutes_football_international_friendly_non_sky_sports_total

,(minutes_football_Capital_One_Cup_sky_sports *account_weight) as minutes_football_Capital_One_Cup_sky_sports_total

,(minutes_football_La_Liga_sky_sports *account_weight) as minutes_football_La_Liga_sky_sports_total

,(minutes_football_football_league_sky_sports *account_weight) as minutes_football_football_league_sky_sports_total

,(minutes_cricket_ashes_sky_sports *account_weight) as minutes_cricket_ashes_sky_sports_total
,(minutes_cricket_ashes_non_sky_sports *account_weight) as minutes_cricket_ashes_non_sky_sports_total

,(minutes_cricket_non_ashes_sky_sports *account_weight) as minutes_cricket_non_ashes_sky_sports_total
,(minutes_cricket_non_ashes_non_sky_sports *account_weight) as minutes_cricket_non_ashes_non_sky_sports_total

,(minutes_golf_sky_sports *account_weight) as minutes_golf_sky_sports_total
,(minutes_golf_non_sky_sports *account_weight) as minutes_golf_non_sky_sports_total

,(minutes_tennis_sky_sports *account_weight) as minutes_tennis_sky_sports_total
,(minutes_tennis_non_sky_sports *account_weight) as minutes_tennis_non_sky_sports_total

,(minutes_motor_sport_sky_sports *account_weight) as minutes_motor_sport_sky_sports_total
,(minutes_motor_sport_non_sky_sports *account_weight) as minutes_motor_sport_non_sky_sports_total

,(minutes_F1_sky_sports *account_weight) as minutes_F1_sky_sports_total
,(minutes_F1_non_sky_sports *account_weight) as minutes_F1_non_sky_sports_total

,(minutes_horse_racing_sky_sports *account_weight) as minutes_horse_racing_sky_sports_total
,(minutes_horse_racing_non_sky_sports *account_weight) as minutes_horse_racing_non_sky_sports_total

,(minutes_snooker_pool_sky_sports *account_weight) as minutes_snooker_pool_sky_sports_total
,(minutes_snooker_pool_non_sky_sports *account_weight) as minutes_snooker_pool_non_sky_sports_total

,(minutes_rugby_sky_sports *account_weight) as minutes_rugby_sky_sports_total
,(minutes_rugby_non_sky_sports *account_weight) as minutes_rugby_non_sky_sports_total

,(minutes_wrestling_sky_sports *account_weight) as minutes_wrestling_sky_sports_total
,(minutes_wrestling_non_sky_sports *account_weight) as minutes_wrestling_non_sky_sports_total

,(minutes_darts_sky_sports *account_weight) as minutes_darts_sky_sports_total
,(minutes_darts_non_sky_sports *account_weight) as minutes_darts_non_sky_sports_total

,(minutes_boxing_sky_sports *account_weight) as minutes_boxing_sky_sports_total
,(minutes_boxing_non_sky_sports *account_weight) as minutes_boxing_non_sky_sports_total

into dbarnett.v223_Unbundling_pivot_activity_data
from v223_unbundling_viewing_summary_by_account  as a
left outer join  v220_zero_mix_active_uk_accounts as b
on a.account_number = b.account_number
left outer join  v223_single_profiling_view as c
on a.account_number = c.account_number

where account_weight>0

;

commit;

grant all on dbarnett.v223_Unbundling_pivot_activity_data to public;

commit;

--select top 500 * from dbarnett.v223_Unbundling_pivot_activity_data;



/*
select a.*
,viewing_duration/cast(programme_instance_duration as real) as vavll
,viewing_duration
,programme_instance_duration
,case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels','SBO','Sky 3D') 
and programme_sub_genre_type='Boxing' and (viewing_duration>=180 or viewing_duration/cast(programme_instance_duration as real)>=0.6) 
then 1 else 0 end as programmes_engaged_non_Sky_Sports_or_SBO_boxing


from dbarnett.v223_all_sports_programmes_viewed_sample  as a
left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim

*/
