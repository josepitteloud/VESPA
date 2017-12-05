/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 10 Details of Viewing Activity by Right
        
        Analyst: Dan Barnett
        SK Prod: 5
        Produce summary of viewing duration by right, where programme doesn’t match to one of the rights on the list, 
        viewing is grouped within Channel + sub genre 
        

*/------------------------------------------------------------------------------------------------------------------

select account_number
,case when analysis_right_new is null then a.channel_name +' '+ a.sub_genre_description else analysis_right_new end as analysis_right_full
,live
,a.broadcast_date
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
--where account_number is not null
group by account_number
,analysis_right_full
,live
,a.broadcast_date
;
commit;
--select top 100 *  from dbarnett.v250_all_sports_programmes_viewed_deduped;
---Summarise by Account by Right by Live/non Live---
drop table dbarnett.v250_sports_rights_viewed_by_right_and_live_status;
select account_number
,analysis_right_full as analysis_right
,live
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_and_live_status
from #rights_viewing_by_account
group by account_number
,analysis_right
,live
;
commit;


---Summarise by Account Overall---
drop table dbarnett.v250_sports_rights_viewed_by_right_overall
select account_number
,analysis_right_full as analysis_right
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_overall
from #rights_viewing_by_account
group by account_number
,analysis_right
;
commit;


----repeat for Grouped areas of analysis----
--drop table #rights_viewing_by_account_grouped;
select account_number
,case when analysis_right_grouped is null then a.channel_name +' '+ a.sub_genre_description else analysis_right_grouped end as analysis_right_full
,live
,a.broadcast_date
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

into #rights_viewing_by_account_grouped
from dbarnett.v250_all_sports_programmes_viewed_deduped  as a
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
where analysis_right_grouped in ('ECB Cricket Sky Sports'
,'England Football Internationals - ITV'
,'F1 - BBC'
,'F1 - Sky Sports'
,'Premier League Football - Sky Sports'
,'UEFA Champions League -  Sky Sports')
group by account_number
,analysis_right_full
,live
,a.broadcast_date
;
commit;

---Summarise by Account by Right by Live/non Live---
drop table dbarnett.v250_sports_rights_viewed_by_right_and_live_status_grouped;
select account_number
,analysis_right_full as analysis_right
,live
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_and_live_status_grouped
from #rights_viewing_by_account_grouped
group by account_number
,analysis_right
,live
;
commit;


---Summarise by Account Overall---
drop table dbarnett.v250_sports_rights_viewed_by_right_overall_grouped
select account_number
,analysis_right_full as analysis_right
,count(distinct broadcast_date) as broadcast_days_viewed
,sum(total_seconds_viewed) as total_duration_viewed_seconds
,sum(programmes_viewed_over_threshold_value) as total_programmes_viewed_over_threshold
,sum(total_viewing_events) as number_of_events_viewed
into dbarnett.v250_sports_rights_viewed_by_right_overall_grouped
from #rights_viewing_by_account_grouped
group by account_number
,analysis_right
;
commit;

---Add Regular and Grouped Viewing Activity into a single Table---


insert into  dbarnett.v250_sports_rights_viewed_by_right_overall
(select * from dbarnett.v250_sports_rights_viewed_by_right_overall_grouped)
; commit;


insert into  dbarnett.v250_sports_rights_viewed_by_right_and_live_status
(select * from dbarnett.v250_sports_rights_viewed_by_right_and_live_status_grouped)
; commit;

grant all on dbarnett.v250_sports_rights_viewed_by_right_overall to public;
grant all on dbarnett.v250_sports_rights_viewed_by_right_overall_grouped to public;

commit;