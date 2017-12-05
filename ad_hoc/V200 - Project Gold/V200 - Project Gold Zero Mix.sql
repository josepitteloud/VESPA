/*------------------------------------------------------------------------------------------------------------------
        Project:    V200-Project Gold
        Program:    Zero Mix Analysis
        Version:    1
        Created:    20130909
        Analyst:    Dan Barnett
        SK Prod:    5
        QA:         
------------------------------------------------------------------------------------------------------------------*/

----PART A -
/*
Viewing from Feb-Jul 2013

Viewing Of Pay Sports/Movies/Other Pay/Non Pay (Initial Breakdown) - May return to look at specific channels--

*/


---Feb 2013
select 
a.account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,dateadd(second,time_in_seconds_since_recording*-1,instance_start_date_time_utc) as viewing_broadcast_start_time
,dateadd(second,(datediff(second,viewing_stops,viewing_starts)),instance_start_date_time_utc) as viewing_broadcast_end_time
into v200_zero_mix_viewing_201302_sample
from  sk_prod.vespa_dp_prog_viewed_201302 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
and right(account_number,2) = '05'
;
commit;

CREATE HG INDEX idx1 ON v200_zero_mix_viewing_201302_sample (account_number);

CREATE HG INDEX idx2 ON v200_zero_mix_viewing_201302_sample (channel_name);
--PART B - EPG Data

---Load in CSV File containing Pay/Free Channel Info and Channel Grouping---
--drop table v200_channel_lookup_with_channel_family;

create table v200_channel_lookup_with_channel_family
(channel_name varchar(100)
,channel_name_inc_hd_staggercast_channel_families  varchar(100)
,pay_channel integer
)
;

input into v200_channel_lookup_with_channel_family
from 'C:\Users\barnetd\Documents\Project 200 - Project Gold\Project Gold Channel Name Lookup.csv' format ascii;


--select * from v200_channel_lookup_with_channel_family;


---Create Bespoke category for Sports/Movies seperated out for Zero Mix Project

alter table v200_channel_lookup_with_channel_family add channel_category_inc_sports_movies varchar(23);

update v200_channel_lookup_with_channel_family
set channel_category_inc_sports_movies= case    when channel_name_inc_hd_staggercast_channel_families =  'Sky Sports Channels' then '01: Sky Sports'
                                                when channel_name_inc_hd_staggercast_channel_families =  'Sky Movies Channels' then '02: Sky Movies'
                                                when pay_channel=1 then '03: Pay Channel' else '04: FTA Channel' end 

from v200_channel_lookup_with_channel_family as a
;
commit;


CREATE HG INDEX idx1 ON v200_channel_lookup_with_channel_family (channel_name);

--select channel_category , count(*)  from mawbya.v190_channels_lu_am group by channel_category order by channel_category;

---PART C - Add EPG Data to Viewing Data---

select account_number
,sum(case when channel_category_inc_sports_movies='01: Sky Sports'  then (datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Sports
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then (datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='03: Pay Channel'  then (datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
,sum(case when channel_category_inc_sports_movies='02: Sky Movies'  then (datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_Sky_Movies
into v200_zero_mix_viewing_201302_summary_by_account
from v200_zero_mix_viewing_201302_sample as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
;

select 
channel_category_inc_sports_movies
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
from v200_zero_mix_viewing_201302_sample as a
left outer join v200_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
--where b.channel_category='99)UNALLOCATED'
group by channel_category_inc_sports_movies
,channel_category_inc_sports_movies
order by seconds_viewed desc
;

commit;















