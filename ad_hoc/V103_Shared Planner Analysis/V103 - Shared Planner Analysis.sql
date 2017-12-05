/*------------------------------------------------------------------------------
        Project: V103 - Shared Planner Analysis
        Version: 1
        Created: 20121008
        Lead: Sarah Moore
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
        Purpose
        -------
The PDD team are working on new shared planner proposition a product enabling customers to record content on one box and view on a different box within the HH.
One aspect of this is ensuring parental control measures are carried across the recorded content when viewed on a different box to which it was recorded on. 
This is currently completed through genre criteria (Genre / Sub Genre = ‘Adult’), however this does not cover all adult channels or content only that deemed sensitive via offcom. 

Code uses Disney_viewing_table_20120919 the base table derived from project 099 (Disney Analysis) which contains viewing activity for 1st-28th May 2012
*/


----


select top 100 * from Disney_viewing_table_20120919;

--drop table v103_shared_planner_summary_by_account;
select case when channel_name_inc_hd is not null then channel_name_inc_hd 
            when left(channel_name,4) in ('ITV ','ITV1') then 'ITV1' else channel_name end as channel_name_inc_hd_v2
,account_number
,max(scaling_weighting) as scaling
,case when sub_genre_description in ('Adult','Erotic','Gaming') then sub_genre_description else 'Other' end as sub_genre_group
,sum(case when recorded_time_utc is null then datediff(ss,viewing_starts,viewing_stops)*scaling_weighting else 0 end) as seconds_viewed_live
,sum(case when recorded_time_utc is not null then datediff(ss,viewing_starts,viewing_stops)*scaling_weighting else 0 end) as seconds_viewed_playback
,sum(datediff(ss,viewing_starts,viewing_stops)*scaling_weighting) as seconds_viewed_total
into v103_shared_planner_summary_by_account
from Disney_viewing_table_20120919
where scaling_weighting is not null
group by channel_name_inc_hd_v2
,account_number
,sub_genre_group
;
commit;
--where recorded_time_utc is not null
--select count(*) from v103_shared_planner_summary_by_account; 
--select top 500 * from v103_shared_planner_summary_by_account;

select sub_genre_group , count(*) from v103_shared_planner_summary_by_account group by sub_genre_group
commit;

select channel_name_inc_hd_v2
,count(distinct account_number) as accounts
,sum(scaling) as scaled_accounts

,sum(case when sub_genre_group in ('Adult') then seconds_viewed_live else 0 end) as adult_viewed_live
,sum(case when sub_genre_group in ('Erotic') then seconds_viewed_live else 0 end) as erotic_viewed_live
,sum(case when sub_genre_group in ('Gaming') then seconds_viewed_live else 0 end) as gaming_viewed_live
,sum(case when sub_genre_group in ('Other') then seconds_viewed_live else 0 end) as other_viewed_live

,sum(case when sub_genre_group in ('Adult') then seconds_viewed_playback else 0 end) as adult_viewed_playback
,sum(case when sub_genre_group in ('Erotic') then seconds_viewed_playback else 0 end) as erotic_viewed_playback
,sum(case when sub_genre_group in ('Gaming') then seconds_viewed_playback else 0 end) as gaming_viewed_playback
,sum(case when sub_genre_group in ('Other') then seconds_viewed_playback else 0 end) as other_viewed_playback
,sum(case when sub_genre_group in ('Erotic','Gaming') then seconds_viewed_playback else 0 end) as erotic_gaming_viewed_playback

,sum(case when sub_genre_group in ('Adult') then scaling else 0 end) as adult_viewed_accounts
,sum(case when sub_genre_group in ('Erotic') then scaling else 0 end) as erotic_viewed_accounts
,sum(case when sub_genre_group in ('Gaming') then scaling else 0 end) as gaming_viewed_accounts
,sum(case when sub_genre_group in ('Other') then scaling else 0 end) as other_viewed_accounts

,sum(case when sub_genre_group in ('Erotic','Gaming') then scaling else 0 end) as erotic_gaming_accounts
,sum(case when sub_genre_group in ('Erotic','Gaming') and seconds_viewed_live >0 then scaling else 0 end) as erotic_gaming_live_accounts
,sum(case when sub_genre_group in ('Erotic','Gaming') and seconds_viewed_playback >0 then scaling else 0 end) as erotic_gaming_playback_accounts

from v103_shared_planner_summary_by_account
group by channel_name_inc_hd_v2
order by erotic_gaming_viewed_playback desc ,gaming_viewed_playback desc,erotic_viewed_playback desc , adult_viewed_playback desc
;

---Create summary durations for live/playback for Erotic/Gaming to create bandings---

/*
select channel_name_inc_hd_v2
,floor(seconds_viewed_live/60) as minute_group
,sum(scaling) as total_accounts
from v103_shared_planner_summary_by_account
where sub_genre_group in ('Erotic','Gaming')
group by channel_name_inc_hd_v2 , minute_group
order by channel_name_inc_hd_v2
,minute_group
;
*/
select channel_name_inc_hd_v2
,case when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_live =0 then '01: No Live Viewing'
      when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_live <=600 then '02: 1-10 Minutes'
      when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_live <=1800 then '03: 11-30 Minutes'
      when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_live >1800 then '04: Over 30 Minutes' else '05: Other' end as live_viewing_grouped

,case when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_playback =0 then '01: No playback Viewing'
      when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_playback <=600 then '02: 1-10 Minutes'
      when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_playback <=1800 then '03: 11-30 Minutes'
      when  sub_genre_group in ('Erotic','Gaming') and seconds_viewed_playback >1800 then '04: Over 30 Minutes' else '05: Other' end as playback_viewing_grouped
,sum(scaling) as scaled_accounts
from v103_shared_planner_summary_by_account
where sub_genre_group in ('Erotic','Gaming')
group by channel_name_inc_hd_v2
,live_viewing_grouped
,playback_viewing_grouped
order by channel_name_inc_hd_v2
,live_viewing_grouped
,playback_viewing_grouped
;

---Create Summary by account---
--drop table  #summary_by_account;
select account_number
,max(scaling) as scaling_value
,sum(case when sub_genre_group in ('Erotic','Gaming') then seconds_viewed_live else 0 end) as erotic_gaming_viewed_live
,sum(case when sub_genre_group in ('Erotic','Gaming') then seconds_viewed_playback else 0 end) as erotic_gaming_viewed_playback
into #summary_by_account
from v103_shared_planner_summary_by_account
group by account_number
;

select sum(scaling_value) from #summary_by_account where erotic_gaming_viewed_live>0
select sum(scaling_value) from #summary_by_account where erotic_gaming_viewed_playback>0
select sum(scaling_value) from #summary_by_account where erotic_gaming_viewed_playback+erotic_gaming_viewed_live>0
commit;

/*

select distinct channel_name from Disney_viewing_table_20120919 where channel_name_inc_hd is null
commit;

select *  from Disney_viewing_table_20120919 where channel_name = 'ITV1+1' and  sub_genre_description in ('Gaming') and recorded_time_utc is not null

select * from sk_prod.vespa_epg_dim where channel_name = 'Movies 24' and sub_genre_description = 'Erotic' and tx_date_utc >= '2012-05-20' order by tx_date_time_utc
select * from sk_prod.vespa_epg_dim where sub_genre_description = 'Erotic' and tx_date_utc >= '2012-05-20' order by tx_date_time_utc

*/
