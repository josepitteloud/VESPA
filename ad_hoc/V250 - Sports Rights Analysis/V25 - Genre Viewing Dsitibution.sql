
---Add Programme Duration on to table

--select distinct channel_name_inc_hd_staggercast_channel_families from dbarnett.v223_all_sports_programmes_viewed_sample order by channel_name_inc_hd_staggercast_channel_families


--drop table #data_for_pivot;
select sub_genre_description
--,programme_sub_genre_type
,case when floor(viewing_duration/300)*5 >=300 then 300 else floor(viewing_duration/300)*5 end as full_minutes_viewed_5_min_band
, floor(programme_instance_duration/900)*15 as programme_duration_15_min_band
,count(*) as records
into #data_for_pivot
from dbarnett.v223_all_sports_programmes_viewed_sample
where viewing_duration>=180 and programme_instance_duration is not null and channel_name_inc_hd_staggercast_channel_families is not null
group by 
sub_genre_description
--,programme_sub_genre_type
,full_minutes_viewed_5_min_band
,programme_duration_15_min_band
order by 
sub_genre_description
--,programme_sub_genre_type
,full_minutes_viewed_5_min_band
,programme_duration_15_min_band
;

commit;

--select top 500 * from #data_for_pivot;
drop table dbarnett.v250_genre_duration_distribution;
select * into dbarnett.v250_genre_duration_distribution from #data_for_pivot;
commit;

grant all on dbarnett.v250_genre_duration_distribution to public; commit;

--select * from dbarnett.v250_genre_duration_distribution where programme_duration_15_min_band=720
--select top 100 * from dbarnett.v250_genre_duration_distribution;
/*


select channel_name_inc_hd_staggercast_channel_families
,sub_genre_description
,int(viewing_duration/60) as full_minutes_viewed
from dbarnett.v223_all_sports_programmes_viewed_sample
where viewing_duration>=180
group by channel_name_inc_hd_staggercast_channel_families
,sub_genre_description
,full_minutes_viewed
;



select top 100 * from dbarnett.v223_sports_epg_lookup_aug_12_jul_13;



select top 100 * from dbarnett.v223_all_sports_programmes_viewed;

select top 100 * from dbarnett.v223_all_sports_programmes_viewed_sample;


select channel_name_inc_hd_staggercast_channel_families, count(*) as recs from dbarnett.v223_all_sports_programmes_viewed_sample group by channel_name_inc_hd_staggercast_channel_families order by recs desc;

commit;


select  from dbarnett.v223_all_sports_programmes_viewed_sample;account_number,dk_programme_instance_dim,viewing_duration,channel_name_inc_hd_staggercast_channel_families,sub_genre_description,programme_sub_genre_type,programme_instance_duration
'200000849535',820012745,3077,,,,
*/