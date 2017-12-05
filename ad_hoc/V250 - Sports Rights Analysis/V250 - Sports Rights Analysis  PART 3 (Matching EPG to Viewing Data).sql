----V250 Part III Match Viewing Data to EPG Data--


---Test Version with 0.1% Sample---

---EPG Data (Interim) dbarnett.v250_sports_rights_epg_detail 
---Viewing Data (Interim) dbarnett.v250_all_programmes_viewed_sample

--select top 100 *  from dbarnett.v250_all_programmes_viewed_sample;
--select top 100 *  from dbarnett.v250_sports_rights_epg_detail ;


---Create Summary Table of Account/Number of Days/Total Sports duration/Total TV Duration--

alter table dbarnett.v250_all_programmes_viewed_sample add genre_description varchar(200);
alter table dbarnett.v250_all_programmes_viewed_sample add sub_genre_description varchar(200);
alter table dbarnett.v250_all_programmes_viewed_sample add analysis_right varchar(200);
alter table dbarnett.v250_all_programmes_viewed_sample add programme_viewed tinyint;


update dbarnett.v250_all_programmes_viewed_sample
set genre_description=b.genre_description
,sub_genre_description=b.sub_genre_description
from dbarnett.v250_all_programmes_viewed_sample as a
left outer join sk_prod.Vespa_programme_schedule as b on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;

update dbarnett.v250_all_programmes_viewed_sample
set analysis_right=b.analysis_right
from dbarnett.v250_all_programmes_viewed_sample as a
left outer join dbarnett.v250_sports_rights_epg_detail as b on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;

update  dbarnett.v250_all_programmes_viewed_sample 
set programme_viewed= case when sub_genre_description in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration>=600 then 1 
when sub_genre_description not in 

('Athletics'
,'American Football'
,'Baseball'
,'Basketball'
,'Boxing'
,'Fishing'
,'Ice Hockey'
,'Motor Sport'
,'Wrestling') and viewing_duration>=900 then 1  
else 0 end
from dbarnett.v250_all_programmes_viewed_sample 
;

commit;

---Top level Stats--
select account_number
,sum(case when genre_description='Sports' then viewing_duration else 0 end) as total_sports_duration
,sum(viewing_duration)  as total_viewing_duration
,count (distinct cast (broadcast_start_date_time_local as date) ) as viewing_days
into dbarnett.v250_account_summary_output_sample
from dbarnett.v250_all_programmes_viewed_sample
group by account_number
;

grant all on dbarnett.v250_account_summary_output_sample to public;

--select top 100 * from dbarnett.v250_account_summary_output_sample;

--Summary by Analysis Right

select
analysis_right
,sum(programme_instance_duration) as right_duration
,count (distinct cast (broadcast_start_date_time_local as date) ) as right_broadcast_days
,count(*) as rights_programmes_broadcast
into dbarnett.v250_sports_rights_broadcast_info
from dbarnett.v250_sports_rights_epg_detail
where analysis_right is not null
group by analysis_right
;

commit;


---By right and account

--drop table #summary_by_right_and_account;
select a.account_number
,a.analysis_right
,b.right_duration
,b.right_broadcast_days
,b.rights_programmes_broadcast
,count(*) as programmes_watched_for_right
,sum(viewing_events) as total_viewing_events_for_right
,count (distinct cast (broadcast_start_date_time_local as date) ) as viewing_days_for_right
,sum(viewing_duration)  as total_viewing_duration_for_right
,sum(programme_viewed) programmes_viewed_for_right
into #summary_by_right_and_account
from dbarnett.v250_all_programmes_viewed_sample as a
left outer join dbarnett.v250_sports_rights_broadcast_info as b
on a.analysis_right=b.analysis_right
where a.analysis_right is not null
group by a.account_number
,a.analysis_right
,b.right_duration
,b.right_broadcast_days
,b.rights_programmes_broadcast
;

commit;
----Join Account Activity with broadcast activity by right---

--select * from #summary_by_right_and_account;




--select * from dbarnett.v250_sports_rights_broadcast_info;
/*
account_number,analysis_right,right_duration,right_broadcast_days,rights_programmes_broadcast,programmes_watched_for_right,total_viewing_events_for_right,viewing_days_for_right,total_viewing_duration_for_right,programmes_viewed_for_right
'200001398086','Australia Home Cricket  2012-2016 Sky Sports',5876330,78,783,1,1,1,308,0
'200001398086','Boxing - Matchroom 2012-2014 Sky Sports',3230407,67,306,1,1,1,3686,1
'200001398086','British & Irish Lions 2013 - Sky Sports',1703998,33,235,4,15,3,18688,4
'200001398086','Champions League - ITV',2546100,12,276,5,12,5,16013,3
'200001398086','Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports',2902802,24,222,5,11,4,26904,5
*/

--drop table #account_summary_by_right;
select account_number
,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then cast(total_viewing_duration_for_right as real)/60 else 0 end) as Australia_Home_Cricket_Sky_Sports_Total_Duration_Minutes_Viewed
,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then programmes_watched_for_right else 0 end) as Australia_Home_Cricket_Sky_Sports_Programmes_Viewed
,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then total_viewing_events_for_right else 0 end) as Australia_Home_Cricket_Sky_Sports_Total_Viewing_Events
,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then total_viewing_events_for_right/cast(programmes_watched_for_right as real) else 0 end) 
as Australia_Home_Cricket_Sky_Sports_Average_Viewing_Events_per_programme
,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then viewing_days_for_right else 0 end) 
as Australia_Home_Cricket_Sky_Sports_Days_Right_viewed

,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then viewing_days_for_right/cast(right_broadcast_days as real) else 0 end) 
as Australia_Home_Cricket_Sky_Sports_SOC_viewing_days


,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then programmes_watched_for_right/cast(rights_programmes_broadcast as real) else 0 end) 
as Australia_Home_Cricket_Sky_Sports_SOC_programmes

,sum(case when analysis_right='Australia Home Cricket  2012-2016 Sky Sports' 
then total_viewing_duration_for_right/cast(right_duration as real) else 0 end) 
as Australia_Home_Cricket_Sky_Sports_SOC_Duration


----Boxing - Matchroom 2012-2014 Sky Sports
,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then cast(total_viewing_duration_for_right as real)/60 else 0 end) as Matchroom_Boxing_Sky_Sports_Total_Duration_Minutes_Viewed
,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then programmes_watched_for_right else 0 end) as Matchroom_Boxing_Sky_Sports_Programmes_Viewed
,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then total_viewing_events_for_right else 0 end) as Matchroom_Boxing_Sky_Sports_Total_Viewing_Events
,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then total_viewing_events_for_right/cast(programmes_watched_for_right as real) else 0 end) 
as Matchroom_Boxing_Sky_Sports_Average_Viewing_Events_per_programme
,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then viewing_days_for_right else 0 end) 
as Matchroom_Boxing_Sky_Sports_Days_Right_viewed

,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then viewing_days_for_right/cast(right_broadcast_days as real) else 0 end) 
as Matchroom_Boxing_Sky_Sports_SOC_viewing_days

,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then programmes_watched_for_right/cast(rights_programmes_broadcast as real) else 0 end) 
as Matchroom_Boxing_Sky_Sports_SOC_programmes

,sum(case when analysis_right='Boxing - Matchroom 2012-2014 Sky Sports' 
then total_viewing_duration_for_right/cast(right_duration as real) else 0 end) 
as Matchroom_Boxing_Sky_Sports_SOC_Duration


----British & Irish Lions 2013 - Sky Sports
,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then cast(total_viewing_duration_for_right as real)/60 else 0 end) as British_Irish_Lions_Sky_Sports_Total_Duration_Minutes_Viewed
,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then programmes_watched_for_right else 0 end) as British_Irish_Lions_Sky_Sports_Programmes_Viewed
,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then total_viewing_events_for_right else 0 end) as British_Irish_Lions_Sky_Sports_Total_Viewing_Events
,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then total_viewing_events_for_right/cast(programmes_watched_for_right as real) else 0 end) 
as British_Irish_Lions_Sky_Sports_Average_Viewing_Events_per_programme
,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then viewing_days_for_right else 0 end) 
as British_Irish_Lions_Sky_Sports_Days_Right_viewed

,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then viewing_days_for_right/cast(right_broadcast_days as real) else 0 end) 
as British_Irish_Lions_Sky_Sports_SOC_viewing_days

,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then programmes_watched_for_right/cast(rights_programmes_broadcast as real) else 0 end) 
as British_Irish_Lions_Sky_Sports_SOC_programmes

,sum(case when analysis_right='British & Irish Lions 2013 - Sky Sports' 
then total_viewing_duration_for_right/cast(right_duration as real) else 0 end) 
as British_Irish_Lions_Sky_Sports_SOC_Duration

----Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports
,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then cast(total_viewing_duration_for_right as real)/60 else 0 end) as Champions_Trophy_ICC_Sky_Sports_Total_Duration_Minutes_Viewed
,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then programmes_watched_for_right else 0 end) as Champions_Trophy_ICC_Sky_Sports_Programmes_Viewed
,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then total_viewing_events_for_right else 0 end) as Champions_Trophy_ICC_Sky_Sports_Total_Viewing_Events
,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then total_viewing_events_for_right/cast(programmes_watched_for_right as real) else 0 end) 
as Champions_Trophy_ICC_Sky_Sports_Average_Viewing_Events_per_programme
,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then viewing_days_for_right else 0 end) 
as Champions_Trophy_ICC_Sky_Sports_Days_Right_viewed

,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then viewing_days_for_right/cast(right_broadcast_days as real) else 0 end) 
as Champions_Trophy_ICC_Sky_Sports_SOC_viewing_days

,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then programmes_watched_for_right/cast(rights_programmes_broadcast as real) else 0 end) 
as Champions_Trophy_ICC_Sky_Sports_SOC_programmes

,sum(case when analysis_right='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' 
then total_viewing_duration_for_right/cast(right_duration as real) else 0 end) 
as Champions_Trophy_ICC_Sky_Sports_SOC_Duration

into #account_summary_by_right
from #summary_by_right_and_account
group by account_number
;

---Match to master list
--drop table dbarnett.v250_account_summary_analysis_dataset_sample;
select a.*
,30 as account_weight
,b.Australia_Home_Cricket_Sky_Sports_Total_Duration_Minutes_Viewed
,b.Australia_Home_Cricket_Sky_Sports_Programmes_Viewed
,b.Australia_Home_Cricket_Sky_Sports_Total_Viewing_Events
,b.Australia_Home_Cricket_Sky_Sports_Average_Viewing_Events_per_programme
,b.Australia_Home_Cricket_Sky_Sports_Days_Right_viewed
,b.Australia_Home_Cricket_Sky_Sports_SOC_viewing_days
,b.Australia_Home_Cricket_Sky_Sports_SOC_programmes
,b.Australia_Home_Cricket_Sky_Sports_SOC_Duration
,case when total_sports_duration=0 then 0 else
b.Australia_Home_Cricket_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_sports_duration as real) end
as Australia_Home_Cricket_Sky_Sports_SOV_Sports
,case when total_viewing_duration = 0 then 0 else
 b.Australia_Home_Cricket_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_viewing_duration as real) end
 as Australia_Home_Cricket_Sky_Sports_SOV_ALL

,b.Matchroom_Boxing_Sky_Sports_Total_Duration_Minutes_Viewed
,b.Matchroom_Boxing_Sky_Sports_Programmes_Viewed
,b.Matchroom_Boxing_Sky_Sports_Total_Viewing_Events
,b.Matchroom_Boxing_Sky_Sports_Average_Viewing_Events_per_programme
,b.Matchroom_Boxing_Sky_Sports_Days_Right_viewed
,b.Matchroom_Boxing_Sky_Sports_SOC_viewing_days
,b.Matchroom_Boxing_Sky_Sports_SOC_programmes
,b.Matchroom_Boxing_Sky_Sports_SOC_Duration
,case when total_sports_duration=0 then 0 else
b.Matchroom_Boxing_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_sports_duration as real) end
as Matchroom_Boxing_Sky_Sports_SOV_Sports
,case when total_viewing_duration = 0 then 0 else
 b.Matchroom_Boxing_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_viewing_duration as real) end
 as Matchroom_Boxing_Sky_Sports_SOV_ALL


,b.British_Irish_Lions_Sky_Sports_Total_Duration_Minutes_Viewed
,b.British_Irish_Lions_Sky_Sports_Programmes_Viewed
,b.British_Irish_Lions_Sky_Sports_Total_Viewing_Events
,b.British_Irish_Lions_Sky_Sports_Average_Viewing_Events_per_programme
,b.British_Irish_Lions_Sky_Sports_Days_Right_viewed
,b.British_Irish_Lions_Sky_Sports_SOC_viewing_days
,b.British_Irish_Lions_Sky_Sports_SOC_programmes
,b.British_Irish_Lions_Sky_Sports_SOC_Duration
,case when total_sports_duration=0 then 0 else
b.British_Irish_Lions_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_sports_duration as real) end
as British_Irish_Lions_Sky_Sports_SOV_Sports
,case when total_viewing_duration = 0 then 0 else
 b.British_Irish_Lions_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_viewing_duration as real) end
 as British_Irish_Lions_Sky_Sports_SOV_ALL

,b.Champions_Trophy_ICC_Sky_Sports_Total_Duration_Minutes_Viewed
,b.Champions_Trophy_ICC_Sky_Sports_Programmes_Viewed
,b.Champions_Trophy_ICC_Sky_Sports_Total_Viewing_Events
,b.Champions_Trophy_ICC_Sky_Sports_Average_Viewing_Events_per_programme
,b.Champions_Trophy_ICC_Sky_Sports_Days_Right_viewed
,b.Champions_Trophy_ICC_Sky_Sports_SOC_viewing_days
,b.Champions_Trophy_ICC_Sky_Sports_SOC_programmes
,b.Champions_Trophy_ICC_Sky_Sports_SOC_Duration
,case when total_sports_duration=0 then 0 else
b.Champions_Trophy_ICC_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_sports_duration as real) end
as Champions_Trophy_ICC_Sky_Sports_SOV_Sports
,case when total_viewing_duration = 0 then 0 else
 b.Champions_Trophy_ICC_Sky_Sports_Total_Duration_Minutes_Viewed*60/cast(total_viewing_duration as real) end
 as Champions_Trophy_ICC_Sky_Sports_SOV_ALL


into dbarnett.v250_account_summary_analysis_dataset_sample
from dbarnett.v250_account_summary_output_sample as a
left outer join #account_summary_by_right as b
on a.account_number = b.account_number
;

commit;

--select * from dbarnett.v250_account_summary_analysis_dataset_sample


grant all on dbarnett.v250_account_summary_analysis_dataset_sample to public;
commit;


----Test With Full Dataset and Daily Data--

---Create Full EPG Table (all activity in Period)



select case when b.analysis_right is null then (c.sub_genre_description + ' ' + channel_name) else analysis_right end as right_type
,sum(  datediff(second,viewing_starts,viewing_stops)) as viewing_duration
,count(*) as viewing_events
from  vespa_analysts.VESPA_DAILY_AUGS_20121110 a
left outer join dbarnett.v250_sports_rights_epg_detail as b
ON a.programme_trans_sk = b.dk_programme_instance_dim
left outer join sk_prod.Vespa_programme_schedule as c
ON a.programme_trans_sk = c.pk_programme_instance_dim
group by right_type
;

 select  (sub_genre_description + ' ' + channel_name)   as right_type
from  dbarnett.v250_sports_rights_epg_detail a










