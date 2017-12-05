
--select top 100 * from sk_prod.vespa_dp_prog_viewed_201308;
commit;
--drop table v223_unbundling_viewing_201308;
select 
a.account_number
,genre_description
,sub_genre_description
,case when channel_name='Other TV' and genre_description='Sports' then 'BT Sport'
when channel_name='Other TV' and genre_description='Movies' then 'Sky Movies Disney' else channel_name end as channel_name_updated
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,broadcast_start_date_time_utc
,programme_instance_duration
,programme_name
into v223_unbundling_viewing_201308
from  sk_prod.vespa_dp_prog_viewed_201308 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
--and right(account_number,3)='456'
;
commit;
--select count(*) from v223_unbundling_viewing_201308;
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
*/