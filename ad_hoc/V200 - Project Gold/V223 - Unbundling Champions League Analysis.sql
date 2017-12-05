

select synopsis  from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 where programme_sub_genre_type='Champions League'

--drop table v223_champions_league_fixtures;
select broadcast_start_date_time_local
,cast(broadcast_start_date_time_local as date) as broadcast_date
,channel_name_inc_hd_staggercast_channel_families
,programme_instance_name
,min(synopsis) as synopsis_type
into v223_champions_league_fixtures
  from dbarnett.v223_sports_epg_lookup_aug_12_jul_13 
where programme_sub_genre_type='Champions League' and channel_name_inc_hd_staggercast_channel_families in ('ITV1','Sky Sports Channels')
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
 union select 'Man Utd v ClujLive','Man Utd','Cluj'
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

;

commit;

select * from v223_champions_league_fixtures_teams;

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
--drop table v223_all_champions_league_viewing;
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

select top 10 * from v223_all_accounts_possible_and_actual_CL_days



