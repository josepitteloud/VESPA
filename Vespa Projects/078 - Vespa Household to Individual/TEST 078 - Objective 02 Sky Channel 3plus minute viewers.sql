

---Run Uncapped and Unscaled Live viewing for 3 sky channels for 1 week to get profile details

select subscriber_id
,account_number
,channel_name
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
into dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
from  sk_prod.VESPA_EVENTS_VIEWED_ALL
where live_recorded = 'LIVE' and broadcast_start_date_time_utc between '2012-06-13 05:00:00' and '2012-06-20 04:59:59'
and datediff(second,event_start_date_time_utc,event_end_date_time_utc)>=180
and panel_id in (4,12)
and  channel_name in (
'Sky Arts 1'
,'Sky Arts 1 HD'
,'Sky1'
,'Sky1 HD'
,'Sky Living'
,'Sky Living HD')
and subscriber_id is not null and account_number is not null
;
commit;

-- add indexes to improve performance
create hg index idx1 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(subscriber_id);
--create hg index idx2 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(account_number);
create hg index idx3 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(channel_name);
create hg index idx4 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(programme_name);
create dttm index idx5 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(broadcast_start_date_time_utc);
create dttm index idx6 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(broadcast_end_date_time_utc);
---Dedupe to one record per programme per subscriber_id
commit;

--drop table dbarnett.project078_summary_sky_channels_programmes_viewed_deduped;
select subscriber_id
,account_number 
,case when channel_name = 'Sky Arts 1 HD' then 'Sky Arts 1' 
      when channel_name = 'Sky1 HD' then 'Sky1' 
      when channel_name = 'Sky Living HD' then 'Sky Living' else channel_name end as channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
into dbarnett.project078_summary_sky_channels_programmes_viewed_deduped
from dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
group by subscriber_id
,account_number 
, channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
;
commit;

select channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,count(*) as boxes
from dbarnett.project078_summary_sky_channels_programmes_viewed_deduped
group by channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
order by channel_name_grouped
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
;
commit;



--Create Summary of Consumerview Data-----

--drop table #consumerview_data;
select cb_key_household
,sum(case when p_gender = '0' then 1 else 0 end) as males
,sum(case when p_gender = '1' then 1 else 0 end) as females
,sum(case when p_gender = '0' and person_age = '0'  then 1 else 0 end) as males_aged_18_25
,sum(case when p_gender = '0' and person_age = '1'  then 1 else 0 end) as males_aged_26_35
,sum(case when p_gender = '0' and person_age = '2'  then 1 else 0 end) as males_aged_36_45
,sum(case when p_gender = '0' and person_age = '3'  then 1 else 0 end) as males_aged_46_55
,sum(case when p_gender = '0' and person_age = '4'  then 1 else 0 end) as males_aged_56_65
,sum(case when p_gender = '0' and person_age = '5'  then 1 else 0 end) as males_aged_66_plus
,sum(case when p_gender = '0' and person_age = 'U'  then 1 else 0 end) as males_aged_unk

,sum(case when p_gender = '1' and person_age = '0'  then 1 else 0 end) as females_aged_18_25
,sum(case when p_gender = '1' and person_age = '1'  then 1 else 0 end) as females_aged_26_35
,sum(case when p_gender = '1' and person_age = '2'  then 1 else 0 end) as females_aged_36_45
,sum(case when p_gender = '1' and person_age = '3'  then 1 else 0 end) as females_aged_46_55
,sum(case when p_gender = '1' and person_age = '4'  then 1 else 0 end) as females_aged_56_65
,sum(case when p_gender = '1' and person_age = '5'  then 1 else 0 end) as females_aged_66_plus
,sum(case when p_gender = '1' and person_age = 'U'  then 1 else 0 end) as females_aged_unk

,sum(case when p_gender = 'U' and person_age = '0'  then 1 else 0 end) as unknown_gender_aged_18_25
,sum(case when p_gender = 'U' and person_age = '1'  then 1 else 0 end) as unknown_gender_aged_26_35
,sum(case when p_gender = 'U' and person_age = '2'  then 1 else 0 end) as unknown_gender_aged_36_45
,sum(case when p_gender = 'U' and person_age = '3'  then 1 else 0 end) as unknown_gender_aged_46_55
,sum(case when p_gender = 'U' and person_age = '4'  then 1 else 0 end) as unknown_gender_aged_56_65
,sum(case when p_gender = 'U' and person_age = '5'  then 1 else 0 end) as unknown_gender_aged_66_plus
,sum(case when p_gender = 'U' and person_age = 'U'  then 1 else 0 end) as unknown_gender_aged_unk

,max(case when family_lifestage in ('02','03','06','07','10') then 1 else 0 end) as presence_of_children

into #consumerview_data_one_record_per_hh
from sk_prod.experian_consumerview as a
where cb_address_status = '1' and cb_address_dps is not null 
group by cb_key_household
;

/*
--- Active HH Keys from SAV as well as Accounts reurning Data from Vespa/Skyview

select distinct cb_key_household
into #active_hh
from sk_prod.cust_single_account_view
where cb_address_status = '1' and cb_address_dps is not null and cb_key_household <>0 and acct_status_code in ('AC','AB','PC')
;
*/

---Add flag for vespa panel users (13th-19th June)







--select count(*) from #active_hh;

---Households viewing between 13th-19th June (Panel 4/12 and Panel 1)

--select count(*) , sum(presence_of_children) from #consumerview_data_one_record_per_hh;













