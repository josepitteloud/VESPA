
---select top 100 * from dbarnett.v250_all_sports_programmes_viewed_deduped;

---Programme Duration by Right Type----

select case when analysis_right_new is null then a.channel_name +' '+ a.sub_genre_description else analysis_right_new end as analysis_right_full
,programme_instance_name
,sum(viewing_duration_total) as total_seconds_viewed
,count(distinct a.dk_programme_instance_dim) as programmes_viewed
,sum(viewing_events_total) as total_viewing_events

,sum(case when a.sub_genre_description in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=600 then 1 
when a.sub_genre_description not in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=900 then 1  
else 0 end) as programmes_viewed_over_threshold_value

into #rights_viewing_by_account
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
left outer join sk_prod.Vespa_programme_schedule  as c
on a.dk_programme_instance_dim=c.dk_programme_instance_dim

--where account_number is not null
group by 
analysis_right_full
,programme_instance_name
;

select * from #rights_viewing_by_account;

output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\programme match info 20140225.csv' format ascii;
commit;


select case when analysis_right_new is null then a.channel_name +' '+ a.sub_genre_description else analysis_right_new end as analysis_right_full
,programme_instance_name
,min(c.broadcast_start_date_time_local) as bcast_min
,max(c.broadcast_start_date_time_local) as bcast_max
,sum(viewing_duration_total) as total_seconds_viewed
,count(distinct a.dk_programme_instance_dim) as programmes_viewed
,sum(viewing_events_total) as total_viewing_events

,sum(case when a.sub_genre_description in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=600 then 1 
when a.sub_genre_description not in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration_total>=900 then 1  
else 0 end) as programmes_viewed_over_threshold_value

into dbarnett.v250_rights_viewing_by_account_v3
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
left outer join sk_prod.Vespa_programme_schedule  as c
on a.dk_programme_instance_dim=c.dk_programme_instance_dim

--where account_number is not null
group by 
analysis_right_full
,programme_instance_name
;
commit;
select count(*) from  dbarnett.v250_rights_viewing_by_account_v2

select * from  dbarnett.v250_rights_viewing_by_account_v3 where analysis_right_full = 'Sky Sports Cricket' order by total_seconds_viewed desc

select * from  dbarnett.v250_rights_viewing_by_account_v3 where analysis_right_full = 'Aviva Prem - BT Sport' order by total_seconds_viewed desc




select analysis_right
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,sum(programmes_broadcast) as total_programmes_broadcast
--into #summary_by_analysis_right
from dbarnett.v250_rights_broadcast_overall
group by analysis_right
order by analysis_right
;

commit;


select * from dbarnett.v250_sports_rights_epg_data_for_analysis where analysis_right = 'Aviva Prem - BT Sport'
select * from dbarnett.v250_sports_rights_with_possible_matches where analysis_right = 'Aviva Prem - BT Sport'

select * from dbarnett.v250_sports_rights_with_service_key where analysis_right = 'Aviva Prem - BT Sport'

select analysis_right, count(*) from dbarnett.v250_sports_rights_with_service_key where channel_name ='BT Sport 1' group by analysis_right order by analysis_right

select analysis_right, count(*) from dbarnett.v250_sports_rights_with_possible_matches where channel_name ='BT Sport 1' group by analysis_right order by analysis_right

select distinct analysis_right from dbarnett.v250_epg_live_non_live_lookup order by analysis_right


select analysis_right, count(*) from dbarnett.v250_sports_rights_epg_detail where channel_name ='BT Sport 1' group by analysis_right order by analysis_right
