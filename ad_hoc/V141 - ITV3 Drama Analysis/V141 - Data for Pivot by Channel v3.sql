/*------------------------------------------------------------------------------
        Project: V141 - Data for Pivot by Channel
        Version: 1
        Created: 20130221
        Lead: Jitesh Patel
        Analyst: Dan Barnett
        SK Prod: 4
        Follow up analysis based on created a pivot with viewing splits for 'every' channel

*/------------------------------------------------------------------------------



----Extra Analysis---
--drop table  v141_channel_inc_staggercast_lookup;
--Create Lookup table to combine Regular/Staggercast---
select channel_name_inc_hd
,channel_name_inc_hd as channel_name_inc_staggercast
into v141_channel_inc_staggercast_lookup
from v141_summary_by_account_and_channel
where channel_name_inc_hd is not null
group by channel_name_inc_hd
,channel_name_inc_staggercast
;
commit;

--select * from v141_channel_inc_staggercast_lookup order by upper(channel_name_inc_hd);

---Update Staggercast to Regular Channel Name
update v141_channel_inc_staggercast_lookup
set channel_name_inc_staggercast = case 
when    channel_name_inc_hd = '5 USA +1' then '5 USA'
when    channel_name_inc_hd = '5* +1' then '5*'

when    channel_name_inc_hd = 'Alibi +1' then 'Alibi'
when    channel_name_inc_hd = 'alibi' then 'Alibi'
when    channel_name_inc_hd = 'Animal Planet +1' then 'Animal Planet'

when    channel_name_inc_hd = 'BET +1' then 'BET'
when    channel_name_inc_hd = 'Bio+1' then 'Bio'

when    channel_name_inc_hd = 'Boomerang +1' then 'Boomerang'
when    channel_name_inc_hd = 'CBS Reality +1' then 'CBS Reality'

when    channel_name_inc_hd = 'Challenge +1' then 'Challenge'
when    channel_name_inc_hd = 'Channel 4 +1' then 'Channel 4'

when    channel_name_inc_hd = 'Channel 5+1' then 'Channel 5'
when    channel_name_inc_hd = 'Christmas 24+' then 'Christmas 24'

when    channel_name_inc_hd = 'Comedy Central +1' then 'Comedy Central'
when    channel_name_inc_hd = 'Comedy Central Extra +1' then 'Comedy Central Extra'
when    channel_name_inc_hd = 'Crime & Investigation +1' then 'Crime & Investigation'
when    channel_name_inc_hd = 'DMax +1' then 'DMax'
when    channel_name_inc_hd = 'DMax +2' then 'DMax'
when    channel_name_inc_hd = 'Disc. History+1' then 'Disc. History'
when    channel_name_inc_hd = 'Disc.Science +1' then 'Disc.Science'
when    channel_name_inc_hd = 'Discovery +1hr' then 'Discovery'
when    channel_name_inc_hd = 'Discovery RealTime +1' then 'Discovery RealTime'
when    channel_name_inc_hd = 'Disney +1' then 'Disney'
when    channel_name_inc_hd = 'Disney Cinemagic +1' then 'Disney Cinemagic'
when    channel_name_inc_hd = 'Disney Junior+' then 'Disney Junior'
when    channel_name_inc_hd = 'Playhouse Disney' then 'Disney Junior'
when    channel_name_inc_hd = 'Disney XD +1' then 'Disney XD'
when    channel_name_inc_hd = 'Eden +1' then 'Eden'
when    channel_name_inc_hd = 'E4 +1' then 'E4'
when    channel_name_inc_hd = 'FX +' then 'FX'
when    channel_name_inc_hd = 'Film4 +1' then 'Film4'
when    channel_name_inc_hd = 'Food Network+1' then 'Food Network'
when    channel_name_inc_hd = 'GOLD +1' then 'GOLD  (TV)'
when    channel_name_inc_hd = 'Gems TV Extra' then 'Gems TV'
when    channel_name_inc_hd = 'Good Food +1' then 'Good Food'
when    channel_name_inc_hd = 'History +1 hour' then 'History'
when    channel_name_inc_hd = 'Home & Health +1' then 'Home & Health'
when    channel_name_inc_hd = 'Home+1' then 'Home'
when    channel_name_inc_hd = 'ITV - ITV3+1' then 'ITV3'
when    channel_name_inc_hd = 'ITV Channel Is' then 'ITV1'
when    channel_name_inc_hd = 'ITV1 Central SW' then 'ITV1'
when    channel_name_inc_hd = 'ITV1+1' then 'ITV1'
when    channel_name_inc_hd = 'ITV2+1' then 'ITV2'
when    channel_name_inc_hd = 'ITV4+1' then 'ITV4'
when    channel_name_inc_hd = 'MTV+1' then 'MTV'
when    channel_name_inc_hd = 'More4 +1' then 'More4'
when    channel_name_inc_hd = 'Movies 24 +' then 'Movies 24'

when    channel_name_inc_hd = 'Nat Geo+1hr' then 'Nat Geo'
when    channel_name_inc_hd = 'Nick Jr+1' then 'Nick Jr'
when    channel_name_inc_hd = 'Nick Replay' then 'Nick Jr'
when    channel_name_inc_hd = 'Nickelodeon+1' then 'Nickelodeon'
when    channel_name_inc_hd = 'Pick TV +1' then 'Pick TV'
when    channel_name_inc_hd = 'PopGirl+1' then 'Pop Girl'
when    channel_name_inc_hd = 'QUEST +1' then 'QUEST'
when    channel_name_inc_hd = 'RAMP''d +2' then 'RAMP''d'
when    channel_name_inc_hd = 'SONY TV +1' then 'Sony TV'
when    channel_name_inc_hd = 'Sony Movies+1' then 'Sony Movies'
when    channel_name_inc_hd = 'Sky Atlantic+1' then 'Sky Atlantic'
when    channel_name_inc_hd = 'Sky Living +1' then 'Sky Living'


when    channel_name_inc_hd = 'Sky Livingit +1' then 'Sky Livingit'
when    channel_name_inc_hd = 'Sky Prem+1' then 'Sky Premiere'
when    channel_name_inc_hd = 'Sky1 +1' then 'Sky1'
when    channel_name_inc_hd = 'Syfy +1' then 'Syfy'
when    channel_name_inc_hd = 'Tiny Pop +1' then 'Tiny Pop'
when    channel_name_inc_hd = 'Travel Channel +1' then 'Travel Channel'
when    channel_name_inc_hd = 'Universal +1' then 'Universal'
when    channel_name_inc_hd = 'Watch +1' then 'Watch'

when    channel_name_inc_hd = 'YeSTERDAY +1' then 'YeSTERDAY'
when    channel_name_inc_hd = 'horror channel +1' then 'horror channel'
when    channel_name_inc_hd = 'men&movs+1' then 'men&movies'
when    channel_name_inc_hd = 'mov4men+1' then 'movies4men'

when    channel_name_inc_hd = 'E! Entertainment' then 'E!'
when    channel_name_inc_hd = 'E! HD' then 'E!'
when    channel_name_inc_hd = 'BBC 1 NI HD' then 'BBC ONE'

when    channel_name_inc_hd = 'Sky ShowcseHD' then 'Sky Movies Showcase'

when    channel_name_inc_hd = 'RTE TWO HD' then 'RTE TWO'
when    channel_name_inc_hd = 'Sky1' then 'Sky 1'

when    channel_name_inc_hd = 'TCM HD' then 'TCM'
when    channel_name_inc_hd = 'E! HD' then 'E!'
when    channel_name_inc_hd = 'E! Entertainment' then 'E!'

when    channel_name_inc_hd = 'Star Plus' then 'STAR Plus'


else channel_name_inc_hd end 
from v141_channel_inc_staggercast_lookup
;

commit;
create hg index idx1 on v141_channel_inc_staggercast_lookup(channel_name_inc_hd);

commit;
create hg index idx2 on v141_channel_inc_staggercast_lookup(channel_name_inc_staggercast);

--select channel_name_inc_staggercast , count(*) from v141_channel_inc_staggercast_lookup group by channel_name_inc_staggercast order by channel_name_inc_staggercast
--select top 100 * from v141_live_playback_viewing
--select timeshifting , count(*) from v141_live_playback_viewing group by timeshifting;
--select timeshifting_v2 , count(*) from v141_live_playback_viewing group by timeshifting_v2;
--Expand Pivot to SoV for every Channel---
commit;
drop table v141_summary_by_account_and_channel;
select account_number
,channel_name_inc_staggercast
,max(overall_project_weighting) as account_weighting
,sum(viewing_duration) as total_duration
,sum(case when timeshifting_v2='LIVE' then viewing_duration else 0 end) as total_duration_live
,sum(case when timeshifting_v2='Within 15 min of Broadcast' then viewing_duration else 0 end) as total_duration_playback_within_15_min_of_tx
,sum(case when timeshifting_v2='Within 1 hour of Broadcast' then viewing_duration else 0 end) as total_duration_playback_16_to_60_minutes_after_tx
,sum(case when timeshifting_v2='VOSDAL' then viewing_duration else 0 end) as total_duration_playback_over_60_minutes_after_tx_VOSDAL
,sum(case when timeshifting_v2='PLAYBACK7' then viewing_duration else 0 end) as total_duration_playback_within_7_days_exc_VOSDAL
,sum(case when timeshifting_v2='VOSDAL' then viewing_duration else 0 end) as total_duration_playback_over_7_days_after_tx

into v141_summary_by_account_and_channel
from v141_live_playback_viewing as a
left outer join v141_channel_inc_staggercast_lookup as b
on a.channel_name_inc_hd=b.channel_name_inc_hd
where overall_project_weighting>0
group by account_number
,channel_name_inc_staggercast
;

commit;
create hg index idx1 on v141_summary_by_account_and_channel(account_number);

commit;
create hg index idx2 on v141_summary_by_account_and_channel(channel_name_inc_staggercast);
commit;

---Create Summary accross Channels---
drop table v141_summary_by_account_only;
select account_number
,sum(total_duration) as total_duration_for_account

,sum(total_duration_live) as duration_live
,sum(total_duration_playback_within_15_min_of_tx) as duration_playback_within_15_min_of_tx
,sum(total_duration_playback_16_to_60_minutes_after_tx) as duration_playback_16_to_60_minutes_after_tx
,sum(total_duration_playback_over_60_minutes_after_tx_VOSDAL) as duration_playback_over_60_minutes_after_tx_VOSDAL
,sum(total_duration_playback_within_7_days_exc_VOSDAL) as duration_playback_within_7_days_exc_VOSDAL
,sum(total_duration_playback_over_7_days_after_tx) as duration_playback_over_7_days_after_tx
into v141_summary_by_account_only
from v141_summary_by_account_and_channel
group by account_number
;

commit;
create hg index idx1 on v141_summary_by_account_only(account_number);

commit;
--select count(*) from v141_summary_by_account_only;
---Rank Total Volume By Channel----
--drop table #rank_account_per_channel;
select account_number
,channel_name_inc_staggercast
,total_duration
,rank() over(PARTITION BY channel_name_inc_staggercast ORDER BY total_duration desc) AS rank_id
into #rank_account_per_channel
from v141_summary_by_account_and_channel
group by account_number
,channel_name_inc_staggercast
,total_duration
;
commit;

--select top 500 * from #rank_account_per_channel where channel_name_inc_hd = 'ITV3' order by rank_id desc;

--Add Rank back to channel---
alter table v141_summary_by_account_and_channel add total_accounts_in_analysis bigint;
alter table v141_summary_by_account_and_channel add channel_viewing_rank bigint;

update v141_summary_by_account_and_channel
set total_accounts_in_analysis = (select count(*) from v141_accounts_for_profiling)
;
commit;


commit;
create hg index idx1 on #rank_account_per_channel(account_number);
create hg index idx2 on #rank_account_per_channel(channel_name_inc_staggercast);
commit;
update v141_summary_by_account_and_channel
set channel_viewing_rank = rank_id
from v141_summary_by_account_and_channel as a
left outer join #rank_account_per_channel as b
on a.account_number = b.account_number and a.channel_name_inc_staggercast=b.channel_name_inc_staggercast
;
commit;
--select top 100 * from v141_summary_by_account_and_channel;

---Summary by Total Duration---
select round(total_duration_for_account/(3600),0) as total_hours_viewed

,sum(account_weight) as weighted_accounts
from v141_summary_by_account_only as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
group by total_hours_viewed
order by total_hours_viewed;
commit;

alter table v141_summary_by_account_and_channel add total_duration_for_account bigint;


update v141_summary_by_account_and_channel
set total_duration_for_account = b.total_duration_for_account
from v141_summary_by_account_and_channel as a
left outer join v141_summary_by_account_only as b
on a.account_number = b.account_number
;
commit;

--select top 100 * from v141_summary_by_account_only;


---Create Summary Segmentation per channel---
--select top 100 * from  v141_summary_by_account_and_channel;
---Create Total Distributions for use in creating segments
drop table v141_hours_viewed_by_channel;
select channel_name_inc_staggercast
,round(total_duration/(3600),0) as total_hours_viewed
,sum(account_weighting) as weighted_accounts
into v141_hours_viewed_by_channel
from v141_summary_by_account_and_channel
group by channel_name_inc_staggercast,total_hours_viewed
order by channel_name_inc_staggercast,total_hours_viewed;
commit;

grant all on v141_hours_viewed_by_channel to public; commit;
--select * from #hours_viewed_by_channel where channel_name_inc_hd='BBC ONE'

--Create table of 1 record per account with Channel Viewing Splits
--select top 100 * from v141_summary_by_account_and_channel;

alter table v141_summary_by_account_and_channel add channel_sov decimal(8,6);

update v141_summary_by_account_and_channel
set channel_sov=total_duration/total_duration_for_account
from v141_summary_by_account_and_channel
;
commit;

---Calculate Average Share of Viewing and Average Hours watched per channel
drop table v141_average_sov_for_channel;
select channel_name_inc_staggercast
,avg(channel_sov) as average_share_of_viewing
,avg(total_duration) as average_duration
into v141_average_sov_for_channel
from v141_summary_by_account_and_channel
group by channel_name_inc_staggercast
order by channel_name_inc_staggercast
;
commit;
create hg index idx1 on v141_average_sov_for_channel(channel_name_inc_staggercast);
commit;

--select * from v141_average_sov_for_channel where  channel_name_inc_staggercast is not null ;
--select top 100 * from v141_summary_by_account_and_channel;
--select top 100 * from v141_high_low_channel_viewing_summary_by_account;
----
--drop table v141_high_low_channel_viewing_summary_by_account;
select account_number

,min(case when a.channel_name_inc_staggercast = '3e' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = '3e' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _3e_Viewing
,min(case when a.channel_name_inc_staggercast = '4Music' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = '4Music' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _4Music_Viewing
,min(case when a.channel_name_inc_staggercast = '4seven' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = '4seven' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _4seven_Viewing
,min(case when a.channel_name_inc_staggercast = '5 USA' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = '5 USA' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _5_USA_Viewing
,min(case when a.channel_name_inc_staggercast = '5*' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = '5*' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _5star_Viewing
,min(case when a.channel_name_inc_staggercast = '9XM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = '9XM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _9XM_Viewing
,min(case when a.channel_name_inc_staggercast = 'AAG' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'AAG' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as _AAG_Viewing
,min(case when a.channel_name_inc_staggercast = 'AAJ TAK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'AAJ TAK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as AAJ_TAK_Viewing
,min(case when a.channel_name_inc_staggercast = 'ABP News' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ABP News' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ABP_News_Viewing
,min(case when a.channel_name_inc_staggercast = 'AIT Int''l' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'AIT Int''l' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as AIT_Intl_Viewing
,min(case when a.channel_name_inc_staggercast = 'ARY Ent' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ARY Ent' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ARY_Ent_Viewing
,min(case when a.channel_name_inc_staggercast = 'ARY QTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ARY QTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ARY_QTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'ARY World' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ARY World' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ARY_World_Viewing
,min(case when a.channel_name_inc_staggercast = 'ATN Bangla UK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ATN Bangla UK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ATN_Bangla_UK_Viewing
,min(case when a.channel_name_inc_staggercast = 'Aastha' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Aastha' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Aastha_Viewing
,min(case when a.channel_name_inc_staggercast = 'Abu Dhabi TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Abu Dhabi TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Abu_Dhabi_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Africa Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Africa Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Africa_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'Ahlebait TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Ahlebait TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Ahlebait_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Ahlulbayt TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Ahlulbayt TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Ahlulbayt_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Al Jazeera Eng' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Al Jazeera Eng' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Al_Jazeera_Eng_Viewing
,min(case when a.channel_name_inc_staggercast = 'Alibi' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Alibi' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Alibi_Viewing
,min(case when a.channel_name_inc_staggercast = 'Amrit Bani' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Amrit Bani' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Amrit_Bani_Viewing
,min(case when a.channel_name_inc_staggercast = 'Animal Planet' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Animal Planet' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Animal_Planet_Viewing
,min(case when a.channel_name_inc_staggercast = 'Anytime' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Anytime' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Anytime_Viewing
,min(case when a.channel_name_inc_staggercast = 'Argos TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Argos TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Argos_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'At The Races' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'At The Races' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as At_The_Races_Viewing
,min(case when a.channel_name_inc_staggercast = 'B4U Movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'B4U Movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as B4U_Movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'B4U Music' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'B4U Music' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as B4U_Music_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC  London' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC  London' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC__London_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC ALBA' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC ALBA' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_ALBA_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC FOUR' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC FOUR' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_FOUR_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC NEWS' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC NEWS' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_NEWS_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC ONE' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC ONE' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_ONE_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC Parliament' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC Parliament' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_Parliament_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC THREE' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC THREE' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_THREE_Viewing
,min(case when a.channel_name_inc_staggercast = 'BBC TWO' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BBC TWO' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BBC_TWO_Viewing
,min(case when a.channel_name_inc_staggercast = 'BEN' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BEN' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BEN_Viewing
,min(case when a.channel_name_inc_staggercast = 'BET' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BET' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BET_Viewing
,min(case when a.channel_name_inc_staggercast = 'BET:BlackEntTv' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BET:BlackEntTv' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BET_BlackEntTv_Viewing
,min(case when a.channel_name_inc_staggercast = 'BFBS Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BFBS Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BFBS_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Baby TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Baby TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Baby_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Bangla TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Bangla TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Bangla_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Believe TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Believe TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Believe_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Best Direct' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Best Direct' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Best_Direct_Viewing
,min(case when a.channel_name_inc_staggercast = 'Bio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Bio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Bio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Blighty' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Blighty' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Blighty_Viewing
,min(case when a.channel_name_inc_staggercast = 'Bliss' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Bliss' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Bliss_Viewing
,min(case when a.channel_name_inc_staggercast = 'Bloomberg' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Bloomberg' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Bloomberg_Viewing
,min(case when a.channel_name_inc_staggercast = 'Blue Tube' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Blue Tube' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Blue_Tube_Viewing
,min(case when a.channel_name_inc_staggercast = 'Boomerang' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Boomerang' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Boomerang_Viewing
,min(case when a.channel_name_inc_staggercast = 'BoxNation' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BoxNation' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BoxNation_Viewing
,min(case when a.channel_name_inc_staggercast = 'Brit Asia TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Brit Asia TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Brit_Asia_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'BuzMuzik' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'BuzMuzik' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as BuzMuzik_Viewing
,min(case when a.channel_name_inc_staggercast = 'CBBC' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CBBC' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CBBC_Viewing
,min(case when a.channel_name_inc_staggercast = 'CBS Action' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CBS Action' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CBS_Action_Viewing
,min(case when a.channel_name_inc_staggercast = 'CBS Drama' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CBS Drama' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CBS_Drama_Viewing
,min(case when a.channel_name_inc_staggercast = 'CBS Reality' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CBS Reality' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CBS_Reality_Viewing
,min(case when a.channel_name_inc_staggercast = 'CBeebies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CBeebies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CBeebies_Viewing
,min(case when a.channel_name_inc_staggercast = 'CC Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CC Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CC_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'CCTV News' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CCTV News' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CCTV_News_Viewing
,min(case when a.channel_name_inc_staggercast = 'CH NINE UK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CH NINE UK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CH_NINE_UK_Viewing
,min(case when a.channel_name_inc_staggercast = 'CHSTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CHSTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CHSTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'CITV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CITV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CITV_Viewing
,min(case when a.channel_name_inc_staggercast = 'CNBC' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CNBC' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CNBC_Viewing
,min(case when a.channel_name_inc_staggercast = 'CNC World' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CNC World' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CNC_World_Viewing
,min(case when a.channel_name_inc_staggercast = 'CNN' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CNN' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CNN_Viewing
,min(case when a.channel_name_inc_staggercast = 'CNToo' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CNToo' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CNToo_Viewing
,min(case when a.channel_name_inc_staggercast = 'COLORS' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'COLORS' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as COLORS_Viewing
,min(case when a.channel_name_inc_staggercast = 'Capital FM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Capital FM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Capital_FM_Viewing
,min(case when a.channel_name_inc_staggercast = 'Capital TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Capital TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Capital_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Cartoon Network' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Cartoon Network' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Cartoon_Network_Viewing
,min(case when a.channel_name_inc_staggercast = 'Cartoonito' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Cartoonito' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Cartoonito_Viewing
,min(case when a.channel_name_inc_staggercast = 'CelebrityShop' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CelebrityShop' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CelebrityShop_Viewing
,min(case when a.channel_name_inc_staggercast = 'Challenge' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Challenge' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Challenge_Viewing
,min(case when a.channel_name_inc_staggercast = 'Channel 4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Channel 4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Channel_4_Viewing
,min(case when a.channel_name_inc_staggercast = 'Channel 5' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Channel 5' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Channel_5_Viewing
,min(case when a.channel_name_inc_staggercast = 'Channel AKA' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Channel AKA' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Channel_AKA_Viewing
,min(case when a.channel_name_inc_staggercast = 'Channel i' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Channel i' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Channel_i_Viewing
,min(case when a.channel_name_inc_staggercast = 'Chart Show TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Chart Show TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Chart_Show_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Chat Box' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Chat Box' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Chat_Box_Viewing
,min(case when a.channel_name_inc_staggercast = 'Cheeky Chat' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Cheeky Chat' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Cheeky_Chat_Viewing
,min(case when a.channel_name_inc_staggercast = 'Chelsea TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Chelsea TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Chelsea_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Choice FM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Choice FM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Choice_FM_Viewing
,min(case when a.channel_name_inc_staggercast = 'Christmas 24' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Christmas 24' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Christmas_24_Viewing
,min(case when a.channel_name_inc_staggercast = 'CineMoi Movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'CineMoi Movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as CineMoi_Movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'Classic FM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Classic FM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Classic_FM_Viewing
,min(case when a.channel_name_inc_staggercast = 'Clubland TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Clubland TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Clubland_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Comedy Central' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Comedy Central' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Comedy_Central_Viewing
,min(case when a.channel_name_inc_staggercast = 'Comedy Central Extra' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Comedy Central Extra' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Comedy_Central_Extra_Viewing
,min(case when a.channel_name_inc_staggercast = 'Community' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Community' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Community_Viewing
,min(case when a.channel_name_inc_staggercast = 'Controversial tv' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Controversial tv' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Controversial_tv_Viewing
,min(case when a.channel_name_inc_staggercast = 'Create & Craft' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Create & Craft' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Create_and_Craft_Viewing
,min(case when a.channel_name_inc_staggercast = 'Crime & Investigation' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Crime & Investigation' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Crime_and_Investigation_Viewing
,min(case when a.channel_name_inc_staggercast = 'DAYSTAR' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'DAYSTAR' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as DAYSTAR_Viewing
,min(case when a.channel_name_inc_staggercast = 'DM Digital' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'DM Digital' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as DM_Digital_Viewing
,min(case when a.channel_name_inc_staggercast = 'DMax' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'DMax' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as DMax_Viewing
,min(case when a.channel_name_inc_staggercast = 'DanceNationTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'DanceNationTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as DanceNationTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Dating Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Dating Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Dating_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'Dave' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Dave' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Dave_Viewing
,min(case when a.channel_name_inc_staggercast = 'Dave ja vu' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Dave ja vu' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Dave_ja_vu_Viewing
,min(case when a.channel_name_inc_staggercast = 'Desi Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Desi Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Desi_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Dirty Talk' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Dirty Talk' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Dirty_Talk_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disc. History' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disc. History' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disc_History_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disc. Shed' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disc. Shed' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disc_Shed_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disc.Science' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disc.Science' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disc_Science_Viewing
,min(case when a.channel_name_inc_staggercast = 'Discovery' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Discovery' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Discovery_Viewing
,min(case when a.channel_name_inc_staggercast = 'Discovery RealTime' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Discovery RealTime' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Discovery_RealTime_Viewing
,min(case when a.channel_name_inc_staggercast = 'Discovery Turbo' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Discovery Turbo' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Discovery_Turbo_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disney' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disney' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disney_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disney Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disney Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disney_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disney Cinemagic' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disney Cinemagic' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disney_Cinemagic_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disney Junior' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disney Junior' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disney_Junior_Viewing
,min(case when a.channel_name_inc_staggercast = 'Disney XD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Disney XD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Disney_XD_Viewing
,min(case when a.channel_name_inc_staggercast = 'Diverse' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Diverse' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Diverse_Viewing

,min(case when a.channel_name_inc_staggercast = 'E!' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'E!' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as E_Entertainment_Viewing

,min(case when a.channel_name_inc_staggercast = 'E4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'E4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as E4_Viewing
,min(case when a.channel_name_inc_staggercast = 'ESPN' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ESPN' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ESPN_Viewing
,min(case when a.channel_name_inc_staggercast = 'ESPN America' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ESPN America' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ESPN_America_Viewing
,min(case when a.channel_name_inc_staggercast = 'ESPN Classic' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ESPN Classic' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ESPN_Classic_Viewing
,min(case when a.channel_name_inc_staggercast = 'EWTN' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'EWTN' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as EWTN_Viewing
,min(case when a.channel_name_inc_staggercast = 'Eden' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Eden' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Eden_Viewing
,min(case when a.channel_name_inc_staggercast = 'Euronews' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Euronews' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Euronews_Viewing
,min(case when a.channel_name_inc_staggercast = 'Eurosport' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Eurosport' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Eurosport_Viewing
,min(case when a.channel_name_inc_staggercast = 'Eurosport 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Eurosport 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Eurosport_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Extreme' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Extreme' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Extreme_Viewing
,min(case when a.channel_name_inc_staggercast = 'Extreme Sports' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Extreme Sports' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Extreme_Sports_Viewing
,min(case when a.channel_name_inc_staggercast = 'FRANCE 24' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'FRANCE 24' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as FRANCE_24_Viewing
,min(case when a.channel_name_inc_staggercast = 'FX' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'FX' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as FX_Viewing
,min(case when a.channel_name_inc_staggercast = 'Faith World TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Faith World TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Faith_World_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Film4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Film4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Film4_Viewing
,min(case when a.channel_name_inc_staggercast = 'Fitness TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Fitness TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Fitness_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Flava' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Flava' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Flava_Viewing
,min(case when a.channel_name_inc_staggercast = 'Food Network' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Food Network' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Food_Network_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 3' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 3' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_3_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_4_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 5' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 5' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_5_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 6' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 6' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_6_Viewing
,min(case when a.channel_name_inc_staggercast = 'Football First 7' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Football First 7' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Football_First_7_Viewing
,min(case when a.channel_name_inc_staggercast = 'Fox News' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Fox News' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Fox_News_Viewing
,min(case when a.channel_name_inc_staggercast = 'GEO News' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'GEO News' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as GEO_News_Viewing
,min(case when a.channel_name_inc_staggercast = 'GEO UK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'GEO UK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as GEO_UK_Viewing
,min(case when a.channel_name_inc_staggercast = 'GOD Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'GOD Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as GOD_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'GOLD  (TV)' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'GOLD  (TV)' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as GOLD_Viewing
,min(case when a.channel_name_inc_staggercast = 'Gems TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Gems TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Gems_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Get Lucky TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Get Lucky TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Get_Lucky_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Glory TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Glory TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Glory_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Good Food' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Good Food' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Good_Food_Viewing
,min(case when a.channel_name_inc_staggercast = 'Gospel Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Gospel Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Gospel_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'Greatest Hits TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Greatest Hits TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Greatest_Hits_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Heart' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Heart' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Heart_Viewing
,min(case when a.channel_name_inc_staggercast = 'Heart TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Heart TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Heart_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Heat' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Heat' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Heat_Viewing
,min(case when a.channel_name_inc_staggercast = 'Hi TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Hi TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Hi_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Hidayat TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Hidayat TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Hidayat_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'High Street TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'High Street TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as High_Street_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'History' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'History' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as History_Viewing
,min(case when a.channel_name_inc_staggercast = 'Home' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Home' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Home_Viewing
,min(case when a.channel_name_inc_staggercast = 'Home & Health' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Home & Health' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Home_and_Health_Viewing
,min(case when a.channel_name_inc_staggercast = 'Horror' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Horror' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Horror_Viewing
,min(case when a.channel_name_inc_staggercast = 'Horse and Country TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Horse and Country TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Horse_and_Country_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'IQRA TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'IQRA TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as IQRA_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'ITV1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ITV1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ITV1_Viewing
,min(case when a.channel_name_inc_staggercast = 'ITV2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ITV2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ITV2_Viewing
,min(case when a.channel_name_inc_staggercast = 'ITV3' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ITV3' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ITV3_Viewing
,min(case when a.channel_name_inc_staggercast = 'ITV4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'ITV4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as ITV4_Viewing
,min(case when a.channel_name_inc_staggercast = 'Ideal Extra' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Ideal Extra' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Ideal_Extra_Viewing
,min(case when a.channel_name_inc_staggercast = 'Ideal World Shopping' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Ideal World Shopping' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Ideal_World_Shopping_Viewing
,min(case when a.channel_name_inc_staggercast = 'Information TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Information TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Information_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Inspiration' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Inspiration' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Inspiration_Viewing
,min(case when a.channel_name_inc_staggercast = 'Investigation' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Investigation' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Investigation_Viewing
,min(case when a.channel_name_inc_staggercast = 'Islam Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Islam Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Islam_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'JML Cookshop' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'JML Cookshop' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as JML_Cookshop_Viewing
,min(case when a.channel_name_inc_staggercast = 'JML Direct' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'JML Direct' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as JML_Direct_Viewing
,min(case when a.channel_name_inc_staggercast = 'JML Home & DIY' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'JML Home & DIY' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as JML_Home_and_DIY_Viewing
,min(case when a.channel_name_inc_staggercast = 'JML Living' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'JML Living' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as JML_Living_Viewing
,min(case when a.channel_name_inc_staggercast = 'Jazz FM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Jazz FM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Jazz_FM_Viewing
,min(case when a.channel_name_inc_staggercast = 'Jewellery Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Jewellery Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Jewellery_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'JewelleryMaker' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'JewelleryMaker' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as JewelleryMaker_Viewing
,min(case when a.channel_name_inc_staggercast = 'KICC TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'KICC TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as KICC_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Kanshi Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Kanshi Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Kanshi_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Kerrang!' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Kerrang!' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Kerrang_Viewing
,min(case when a.channel_name_inc_staggercast = 'Khushkhabri' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Khushkhabri' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Khushkhabri_Viewing
,min(case when a.channel_name_inc_staggercast = 'Kismat' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Kismat' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Kismat_Viewing
,min(case when a.channel_name_inc_staggercast = 'Kiss TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Kiss TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Kiss_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Kix!' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Kix!' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Kix_Viewing
,min(case when a.channel_name_inc_staggercast = 'Klear TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Klear TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Klear_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Liberty' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Liberty' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Liberty_Viewing
,min(case when a.channel_name_inc_staggercast = 'Liverpool FC TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Liverpool FC TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Liverpool_FC_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Lucky Star' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Lucky Star' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Lucky_Star_Viewing
,min(case when a.channel_name_inc_staggercast = 'Luxury Life' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Luxury Life' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Luxury_Life_Viewing
,min(case when a.channel_name_inc_staggercast = 'MATV National' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MATV National' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MATV_National_Viewing
,min(case when a.channel_name_inc_staggercast = 'MGM HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MGM HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MGM_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'MOTORS TV UK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MOTORS TV UK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MOTORS_TV_UK_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV BASE' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV BASE' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_BASE_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV CLASSIC' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV CLASSIC' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_CLASSIC_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV DANCE' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV DANCE' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_DANCE_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV HITS' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV HITS' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_HITS_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV Live' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV Live' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_Live_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV Live HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV Live HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_Live_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV Music' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV Music' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_Music_Viewing
,min(case when a.channel_name_inc_staggercast = 'MTV ROCKS' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MTV ROCKS' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MTV_ROCKS_Viewing
,min(case when a.channel_name_inc_staggercast = 'MUTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'MUTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as MUTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Madani Chnl' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Madani Chnl' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Madani_Chnl_Viewing
,min(case when a.channel_name_inc_staggercast = 'Magic' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Magic' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Magic_Viewing
,min(case when a.channel_name_inc_staggercast = 'Massive R&B' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Massive R&B' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Massive_R_and_B_Viewing
,min(case when a.channel_name_inc_staggercast = 'Military History' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Military History' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Military_History_Viewing
,min(case when a.channel_name_inc_staggercast = 'More4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'More4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as More4_Viewing
,min(case when a.channel_name_inc_staggercast = 'Movies 24' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Movies 24' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Movies_24_Viewing
,min(case when a.channel_name_inc_staggercast = 'Music India' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Music India' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Music_India_Viewing
,min(case when a.channel_name_inc_staggercast = 'My Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'My Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as My_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'N''Toons Replay' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'N''Toons Replay' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as NToons_Replay_Viewing
,min(case when a.channel_name_inc_staggercast = 'NDTV 24x7' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'NDTV 24x7' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as NDTV_24x7_Viewing
,min(case when a.channel_name_inc_staggercast = 'NHK World HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'NHK World HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as NHK_World_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'NTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'NTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as NTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nat Geo' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nat Geo' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nat_Geo_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nat Geo Wild' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nat Geo Wild' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nat_Geo_Wild_Viewing
,min(case when a.channel_name_inc_staggercast = 'Newstalk' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Newstalk' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Newstalk_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nick Jr' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nick Jr' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nick_Jr_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nick Jr 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nick Jr 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nick_Jr_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nickelodeon' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nickelodeon' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nickelodeon_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nicktoons TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nicktoons TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nicktoons_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Nollywood Movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Nollywood Movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Nollywood_Movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'Noor TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Noor TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Noor_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'OH TV [Open Heaven TV]' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'OH TV [Open Heaven TV]' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Open_Heaven_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Olive TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Olive TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Olive_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'PBS America' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'PBS America' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as PBS_America_Viewing
,min(case when a.channel_name_inc_staggercast = 'PCNE Chinese' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'PCNE Chinese' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as PCNE_Chinese_Viewing
,min(case when a.channel_name_inc_staggercast = 'POP' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'POP' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as POP_Viewing
,min(case when a.channel_name_inc_staggercast = 'PTV Global' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'PTV Global' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as PTV_Global_Viewing
,min(case when a.channel_name_inc_staggercast = 'PTV Prime' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'PTV Prime' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as PTV_Prime_Viewing
,min(case when a.channel_name_inc_staggercast = 'Panjab Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Panjab Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Panjab_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Paversshoes.tv' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Paversshoes.tv' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Paversshoes_tv_Viewing
,min(case when a.channel_name_inc_staggercast = 'Peace TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Peace TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Peace_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Peace TV Urdu' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Peace TV Urdu' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Peace_TV_Urdu_Viewing
,min(case when a.channel_name_inc_staggercast = 'Pick TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Pick TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Pick_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Pitch TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Pitch TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Pitch_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Pitch World' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Pitch World' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Pitch_World_Viewing
,min(case when a.channel_name_inc_staggercast = 'Planet Rock' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Planet Rock' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Planet_Rock_Viewing
,min(case when a.channel_name_inc_staggercast = 'Pop Girl' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Pop Girl' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Pop_Girl_Viewing
,min(case when a.channel_name_inc_staggercast = 'Prem Spts Xtra' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Prem Spts Xtra' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Prem_Spts_Xtra_Viewing
,min(case when a.channel_name_inc_staggercast = 'Premier' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Premier' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Premier_Viewing
,min(case when a.channel_name_inc_staggercast = 'Premier Sports' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Premier Sports' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Premier_Sports_Viewing
,min(case when a.channel_name_inc_staggercast = 'Primetime' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Primetime' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Primetime_Viewing
,min(case when a.channel_name_inc_staggercast = 'Propeller' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Propeller' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Propeller_Viewing
,min(case when a.channel_name_inc_staggercast = 'Psychic Today' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Psychic Today' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Psychic_Today_Viewing
,min(case when a.channel_name_inc_staggercast = 'Pub Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Pub Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Pub_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'QUEST' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'QUEST' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as QUEST_Viewing
,min(case when a.channel_name_inc_staggercast = 'QVC' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'QVC' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as QVC_Viewing
,min(case when a.channel_name_inc_staggercast = 'QVC Beauty' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'QVC Beauty' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as QVC_Beauty_Viewing
,min(case when a.channel_name_inc_staggercast = 'RAMP''d' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RAMP''d' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RAMPd_Viewing
,min(case when a.channel_name_inc_staggercast = 'RT' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RT' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RT_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE 2FM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE 2FM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_2FM_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE Lyric fm' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE Lyric fm' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_Lyric_fm_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE One' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE One' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_One_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE R na G' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE R na G' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_R_na_G_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE R1 Extra' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE R1 Extra' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_R1_Extra_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE Radio 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE Radio 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_Radio_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE TWO' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE TWO' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_TWO_Viewing
,min(case when a.channel_name_inc_staggercast = 'RTE TWO HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'RTE TWO HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as RTE_TWO_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'Racing UK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Racing UK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Racing_UK_Viewing
,min(case when a.channel_name_inc_staggercast = 'Rainbow' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Rainbow' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Rainbow_Viewing
,min(case when a.channel_name_inc_staggercast = 'Real Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Real Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Real_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Really' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Really' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Really_Viewing
,min(case when a.channel_name_inc_staggercast = 'Record TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Record TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Record_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Renault TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Renault TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Renault_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Retail TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Retail TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Retail_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Rishtey' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Rishtey' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Rishtey_Viewing
,min(case when a.channel_name_inc_staggercast = 'Rocks & Co 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Rocks & Co 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Rocks_and_Co_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'S4C' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'S4C' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as S4C_Viewing
,min(case when a.channel_name_inc_staggercast = 'SBO' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'SBO' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as SBO_Viewing
,min(case when a.channel_name_inc_staggercast = 'SONY MAX' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'SONY MAX' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as SONY_MAX_Viewing
,min(case when a.channel_name_inc_staggercast = 'SONY SAB' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'SONY SAB' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as SONY_SAB_Viewing
,min(case when a.channel_name_inc_staggercast = 'STAR Gold' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'STAR Gold' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as STAR_Gold_Viewing
,min(case when a.channel_name_inc_staggercast = 'STAR Plus' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'STAR Plus' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as STAR_Plus_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sahara One' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sahara One' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sahara_One_Viewing
,min(case when a.channel_name_inc_staggercast = 'Samaa' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Samaa' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Samaa_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sangat' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sangat' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sangat_Viewing
,min(case when a.channel_name_inc_staggercast = 'Scuzz' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Scuzz' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Scuzz_Viewing
,min(case when a.channel_name_inc_staggercast = 'Setanta Ireland' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Setanta Ireland' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Setanta_Ireland_Viewing
,min(case when a.channel_name_inc_staggercast = 'Setanta Sports1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Setanta Sports1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Setanta_Sports1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Showcase' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Showcase' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Showcase_Viewing
,min(case when a.channel_name_inc_staggercast = 'Showcase 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Showcase 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Showcase_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sikh Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sikh Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sikh_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky 3D' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky 3D' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_3D_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Arts 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Arts 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Arts_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Arts 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Arts 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Arts_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Atlantic' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Atlantic' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Atlantic_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Box Office' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Box Office' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Box_Office_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky DramaRom' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky DramaRom' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_DramaRom_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Insider HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Insider HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Insider_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Living' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Living' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Living_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Livingit' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Livingit' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Livingit_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies 007' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies 007' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_007_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Action' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Action' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Action_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Classics' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Classics' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Classics_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Comedy' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Comedy' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Comedy_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Family' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Family' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Family_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Indie' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Indie' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Indie_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Mdn Greats' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Mdn Greats' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Mdn_Greats_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Sci-Fi/Horror' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Sci-Fi/Horror' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Sci_Fi_Horror_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Showcase' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Showcase' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Showcase_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Movies Thriller' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Movies Thriller' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Movies_Thriller_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky News' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky News' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_News_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Premiere' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Premiere' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Premiere_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Sports 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Sports 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Sports_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Sports 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Sports 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Sports_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Sports 3' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Sports 3' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Sports_3_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Sports 4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Sports 4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Sports_4_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Sports F1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Sports F1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Sports_F1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sky Sports News' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sky Sports News' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sky_Sports_News_Viewing
,min(case when a.channel_name_inc_staggercast = 'SkyPoker.com' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'SkyPoker.com' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as SkyPoker_com_Viewing
,min(case when a.channel_name_inc_staggercast = 'SmartLive' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'SmartLive' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as SmartLive_Viewing
,min(case when a.channel_name_inc_staggercast = 'Smash Hits!' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Smash Hits!' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Smash_Hits_Viewing
,min(case when a.channel_name_inc_staggercast = 'Smooth' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Smooth' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Smooth_Viewing
,min(case when a.channel_name_inc_staggercast = 'Solar Radio' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Solar Radio' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Solar_Radio_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sonlife' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sonlife' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sonlife_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sony Movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sony Movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sony_Movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sony TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sony TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sony_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sony TV Asia' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sony TV Asia' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sony_TV_Asia_Viewing
,min(case when a.channel_name_inc_staggercast = 'Spectrum 1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Spectrum 1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Spectrum_1_Viewing
,min(case when a.channel_name_inc_staggercast = 'Star Life OK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Star Life OK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Star_Life_OK_Viewing

,min(case when a.channel_name_inc_staggercast = 'Starz TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Starz TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Starz_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Storm' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Storm' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Storm_Viewing
,min(case when a.channel_name_inc_staggercast = 'Style Network' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Style Network' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Style_Network_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sukh Sagar' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sukh Sagar' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sukh_Sagar_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sunrise' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sunrise' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sunrise_Viewing
,min(case when a.channel_name_inc_staggercast = 'Sunrise TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Sunrise TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Sunrise_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'SuperCasino' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'SuperCasino' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as SuperCasino_Viewing
,min(case when a.channel_name_inc_staggercast = 'Syfy' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Syfy' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Syfy_Viewing
,min(case when a.channel_name_inc_staggercast = 'TBN Europe' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TBN Europe' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TBN_Europe_Viewing
,min(case when a.channel_name_inc_staggercast = 'TCM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TCM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TCM_Viewing
,min(case when a.channel_name_inc_staggercast = 'TCM 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TCM 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TCM_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'TCM HD' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TCM HD' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TCM_HD_Viewing
,min(case when a.channel_name_inc_staggercast = 'TG4' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TG4' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TG4_Viewing
,min(case when a.channel_name_inc_staggercast = 'TRACE Sports' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TRACE Sports' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TRACE_Sports_Viewing
,min(case when a.channel_name_inc_staggercast = 'TV Shop' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TV Shop' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TV_Shop_Viewing
,min(case when a.channel_name_inc_staggercast = 'TV3' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TV3' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TV3_Viewing
,min(case when a.channel_name_inc_staggercast = 'TV5' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TV5' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TV5_Viewing
,min(case when a.channel_name_inc_staggercast = 'TVX Brits' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TVX Brits' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TVX_Brits_Viewing
,min(case when a.channel_name_inc_staggercast = 'TWR' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'TWR' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as TWR_Viewing
,min(case when a.channel_name_inc_staggercast = 'Takbeer TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Takbeer TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Takbeer_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Thane Direct' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Thane Direct' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Thane_Direct_Viewing
,min(case when a.channel_name_inc_staggercast = 'The Active Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'The Active Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as The_Active_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'The Box' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'The Box' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as The_Box_Viewing
,min(case when a.channel_name_inc_staggercast = 'The Dept Store' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'The Dept Store' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as The_Dept_Store_Viewing
,min(case when a.channel_name_inc_staggercast = 'The Horror Ch' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'The Horror Ch' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as The_Horror_Ch_Viewing
,min(case when a.channel_name_inc_staggercast = 'The Vault' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'The Vault' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as The_Vault_Viewing
,min(case when a.channel_name_inc_staggercast = 'Tiny Pop' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Tiny Pop' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Tiny_Pop_Viewing
,min(case when a.channel_name_inc_staggercast = 'Travel & Living' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Travel & Living' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Travel_and_Living_Viewing
,min(case when a.channel_name_inc_staggercast = 'Travel Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Travel Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Travel_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'True Ent' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'True Ent' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as True_Ent_Viewing
,min(case when a.channel_name_inc_staggercast = 'True Movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'True Movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as True_Movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'True Movies 2' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'True Movies 2' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as True_Movies_2_Viewing
,min(case when a.channel_name_inc_staggercast = 'UCB Bible' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UCB Bible' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UCB_Bible_Viewing
,min(case when a.channel_name_inc_staggercast = 'UCB Gospel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UCB Gospel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UCB_Gospel_Viewing
,min(case when a.channel_name_inc_staggercast = 'UCB Insp' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UCB Insp' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UCB_Insp_Viewing
,min(case when a.channel_name_inc_staggercast = 'UCB Ireland' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UCB Ireland' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UCB_Ireland_Viewing
,min(case when a.channel_name_inc_staggercast = 'UCB TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UCB TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UCB_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'UCB UK' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UCB UK' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UCB_UK_Viewing
,min(case when a.channel_name_inc_staggercast = 'UMMAH CHNL' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UMMAH CHNL' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UMMAH_CHNL_Viewing
,min(case when a.channel_name_inc_staggercast = 'UMP Movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UMP Movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UMP_Movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'UMP Stars' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UMP Stars' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UMP_Stars_Viewing
,min(case when a.channel_name_inc_staggercast = 'UTV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'UTV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as UTV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Universal' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Universal' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Universal_Viewing
,min(case when a.channel_name_inc_staggercast = 'V Channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'V Channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as V_Channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'VH1' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'VH1' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as VH1_Viewing
,min(case when a.channel_name_inc_staggercast = 'VIVA' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'VIVA' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as VIVA_Viewing
,min(case when a.channel_name_inc_staggercast = 'Venus TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Venus TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Venus_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Vintage TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Vintage TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Vintage_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Vox Africa' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Vox Africa' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Vox_Africa_Viewing
,min(case when a.channel_name_inc_staggercast = 'WRN Europe' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'WRN Europe' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as WRN_Europe_Viewing
,min(case when a.channel_name_inc_staggercast = 'Watch' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Watch' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Watch_Viewing
,min(case when a.channel_name_inc_staggercast = 'Wedding TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Wedding TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Wedding_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Word Network' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Word Network' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Word_Network_Viewing
,min(case when a.channel_name_inc_staggercast = 'XFM' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'XFM' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as XFM_Viewing
,min(case when a.channel_name_inc_staggercast = 'YeSTERDAY' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'YeSTERDAY' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as YeSTERDAY_Viewing
,min(case when a.channel_name_inc_staggercast = 'Yorkshire R' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Yorkshire R' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Yorkshire_R_Viewing
,min(case when a.channel_name_inc_staggercast = 'Zee Caf�' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Zee Caf�' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Zee_Cafe_Viewing
,min(case when a.channel_name_inc_staggercast = 'Zee Cinema' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Zee Cinema' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Zee_Cinema_Viewing
,min(case when a.channel_name_inc_staggercast = 'Zee Punjabi' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Zee Punjabi' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Zee_Punjabi_Viewing
,min(case when a.channel_name_inc_staggercast = 'Zee TV' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Zee TV' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Zee_TV_Viewing
,min(case when a.channel_name_inc_staggercast = 'Zing' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'Zing' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as Zing_Viewing
,min(case when a.channel_name_inc_staggercast = 'bid' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'bid' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as bid_Viewing
,min(case when a.channel_name_inc_staggercast = 'eNCA Africa' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'eNCA Africa' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as eNCA_Africa_Viewing
,min(case when a.channel_name_inc_staggercast = 'holiday+cruise' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'holiday+cruise' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as holiday_cruise_Viewing
,min(case when a.channel_name_inc_staggercast = 'horror channel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'horror channel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as horror_channel_Viewing
,min(case when a.channel_name_inc_staggercast = 'jazeerachildren' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'jazeerachildren' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as jazeerachildren_Viewing
,min(case when a.channel_name_inc_staggercast = 'men&movies' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'men&movies' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as men_and_movies_Viewing
,min(case when a.channel_name_inc_staggercast = 'movies4men' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'movies4men' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as movies4men_Viewing
,min(case when a.channel_name_inc_staggercast = 'mta-muslim tv' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'mta-muslim tv' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as mta_muslim_tv_Viewing
,min(case when a.channel_name_inc_staggercast = 'price drop' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'price drop' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as price_drop_Viewing
,min(case when a.channel_name_inc_staggercast = 'revelation' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'revelation' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as revelation_Viewing
,min(case when a.channel_name_inc_staggercast = 'speed auction' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'speed auction' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as speed_auction_Viewing
,min(case when a.channel_name_inc_staggercast = 'theDealchannel' and (channel_sov>=(average_share_of_viewing*1.5) or total_duration>=average_duration*1.5) then '01: High Viewing'  when a.channel_name_inc_staggercast = 'theDealchannel' and total_duration>0 then '02: Low Viewing' else '03: No Viewing' end) as theDealchannel_Viewing

into v141_high_low_channel_viewing_summary_by_account
from v141_summary_by_account_and_channel as a
left outer join v141_average_sov_for_channel as b
on a.channel_name_inc_staggercast=b.channel_name_inc_staggercast
group by account_number
;

commit;
create hg index idx1 on v141_high_low_channel_viewing_summary_by_account(account_number);
commit;

---Create Composite variables for all Movies Channels---
--drop table #movie_totals;
select account_number
,sum(total_duration) as total_dur_movies
,max(total_duration_for_account) as total_account_duration
,sum(channel_sov) as movie_sov
,1 as join_key
into #movie_totals
from v141_summary_by_account_and_channel
where channel_name_inc_staggercast in 
('Sky Movies 007',
'Sky Movies Action',
'Sky Movies Classics',
'Sky Movies Comedy',
'Sky Movies Family',
'Sky Movies Indie',
'Sky Movies Mdn Greats',
'Sky Movies Sci-Fi/Horror',
'Sky Movies Showcase',
'Sky Movies Thriller',
--'Sky News',
'Sky Premiere')
group by account_number
,join_key
;

---Create Table of Total Vals---
--drop table #movie_sov_totals;
select 1 as join_key
,sum(average_share_of_viewing) as total_av_share_viewing
,sum(average_duration) as total_dur_movies
into #movie_sov_totals
from v141_average_sov_for_channel
where channel_name_inc_staggercast in 
('Sky Movies 007',
'Sky Movies Action',
'Sky Movies Classics',
'Sky Movies Comedy',
'Sky Movies Family',
'Sky Movies Indie',
'Sky Movies Mdn Greats',
'Sky Movies Sci-Fi/Horror',
'Sky Movies Showcase',
'Sky Movies Thriller',
--'Sky News',
'Sky Premiere')
group by join_key
;

select a.account_number
,min(case when (movie_sov>=(total_av_share_viewing*1.5) or a.total_dur_movies>=b.total_dur_movies*1.5) then '01: High Viewing'  when a.total_dur_movies>0 then '02: Low Viewing' else '03: No Viewing' end) as sky_movies_combined_viewing
into #sky_movies_combined_viewing
from #movie_totals as a
left outer join #movie_sov_totals as b
on a.join_key=b.join_key
group by a.account_number
;

commit;
create hg index idx1 on #sky_movies_combined_viewing(account_number);
commit;


---Create Composite variables for all sports Channels---
--drop table #movie_totals;
select account_number
,sum(total_duration) as total_dur_sports
,max(total_duration_for_account) as total_account_duration
,sum(channel_sov) as sport_sov
,1 as join_key
into #sport_totals
from v141_summary_by_account_and_channel
where channel_name_inc_staggercast in 
('Football First 1'
,'Football First 2'
,'Football First 3'
,'Football First 4'
,'Football First 5'
,'Football First 6'
,'Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1')
group by account_number
,join_key
;

---Create Table of Total Vals---
--drop table #movie_sov_totals;
select 1 as join_key
,sum(average_share_of_viewing) as total_av_share_viewing
,sum(average_duration) as total_dur_sports
into #sports_sov_totals
from v141_average_sov_for_channel
where channel_name_inc_staggercast in 
('Football First 1'
,'Football First 2'
,'Football First 3'
,'Football First 4'
,'Football First 5'
,'Football First 6'
,'Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1')
group by join_key
;

select a.account_number
,min(case when (sport_sov>=(total_av_share_viewing*1.5) or a.total_dur_sports>=b.total_dur_sports*1.5) then '01: High Viewing'  when a.total_dur_sports>0 then '02: Low Viewing' else '03: No Viewing' end) as sky_sports_combined_viewing
into #sky_sports_combined_viewing
from #sport_totals as a
left outer join #sports_sov_totals as b
on a.join_key=b.join_key
group by a.account_number
;

commit;
create hg index idx1 on #sky_sports_combined_viewing(account_number);
commit;



--select sky_movies_combined_viewing , count(*) from #sky_movies_combined_viewing group by sky_movies_combined_viewing order by sky_movies_combined_viewing;



---Extra Customer Variables for Analysis----


-- Date range of programmes to capture
--SET @snapshot_start_dt  = '2012-10-01';
--SET @snapshot_end_dt    = '2012-11-14';

---Previous Cuscan - Prior to 14th Nov 2012----
select
                csh.account_number
                ,max(case when status_start_dt <= '2012-11-14' and status_code in ('PO') and prev_status_code in ('AC','AB','PC') then 1 else 0 end) as previous_cuscan
                ,max(case when status_start_dt <= '2012-11-14' and status_code in ('SC') and prev_status_code in ('AC','AB','PC') then 1 else 0 end) as previous_syscan
                ,max(case when status_start_dt between '2012-11-15' and '2013-02-14' and status_code in ('PO') and prev_status_code in ('AC','AB','PC') then 1 else 0 end) as subsequent_cuscan
                ,max(case when status_start_dt between '2012-11-15' and '2013-02-14' and status_code in ('SC') and prev_status_code in ('AC','AB','PC') then 1 else 0 end) as subsequent_syscan
                ,max(case when status_start_dt between '2012-11-15' and '2013-02-14' and status_code in ('AB') and prev_status_code in ('AC','PC') then 1 else 0 end) as subsequent_active_block
                ,max(case when status_start_dt between '2012-11-15' and '2013-02-14' and status_code in ('PC') and prev_status_code in ('PC') then 1 else 0 end) as subsequent_pending_cancel

                ,max(case when status_start_dt between '2012-07-01' and '2012-11-14' and status_code in ('AC') and prev_status_code in ('PO','SC') then 1 else 0 end) as recent_winback_reactivation

into           #previous_churn_winback
FROM            sk_prod.cust_subs_hist csh
WHERE           csh.status_code_changed = 'Y'
and             csh.subscription_sub_type = 'DTV Primary Viewing'
group by account_number
;
commit;
CREATE        hg INDEX idx1 on #previous_churn_winback (account_number);

SELECT  account_number
,max(case when attempt_date  <= '2012-11-14' then 1 else 0 end) as previous_ta
,max(case when attempt_date  between '2012-11-15' and '2013-02-14' then 1 else 0 end) as subsequent_ta   
,max(case when attempt_date  between '2012-11-15' and '2013-02-14' and  cca.Wh_Attempt_Outcome_Description_1 IN ( 'Turnaround Saved'
                                                                 ,'Legacy Save'
                                                                 ,'Home Move Saved'
                                                                 ,'Home Move Accept Saved') then 1 else 0 end) as subsequent_ta_saved
,max(case when attempt_date  between '2012-11-15' and '2013-02-14' and  cca.Wh_Attempt_Outcome_Description_1 not IN ( 'Turnaround Saved'
                                                                 ,'Legacy Save'
                                                                 ,'Home Move Saved'
                                                                 ,'Home Move Accept Saved') then 1 else 0 end) as subsequent_ta_failed
INTO #previous_post_ta
    FROM sk_prod.cust_change_attempt AS cca
--         inner join sk_prod.cust_subscriptions AS subs
--             ON cca.subscription_id = subs.subscription_id
   WHERE cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
     AND cca.subscription_sub_type       = 'DTV Primary Viewing'
     
     AND cca.created_by_id                  NOT IN ('dpsbtprd', 'batchuser')
     AND cca.Wh_Attempt_Outcome_Description_1 in (  'Turnaround Saved'
                                                   ,'Legacy Save'
                                                   ,'Turnaround Not Saved'
                                                   ,'Legacy Fail'
                                                   ,'Home Move Saved'
                                                   ,'Home Move Not Saved'
                                                   ,'Home Move Accept Saved')
group by account_number
;
commit;
CREATE        hg INDEX idx1 on #previous_post_ta (account_number);
commit;

alter table v141_accounts_for_profiling add previous_ta tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_ta tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_ta_saved tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_ta_failed tinyint default 0;

update v141_accounts_for_profiling
set previous_ta=case when b.previous_ta is null then 0 else b.previous_ta end
,subsequent_ta=case when b.subsequent_ta is null then 0 else b.subsequent_ta end
,subsequent_ta_saved=case when b.subsequent_ta_saved is null then 0 else b.subsequent_ta_saved end
,subsequent_ta_failed=case when b.subsequent_ta_failed is null then 0 else b.subsequent_ta_failed end
from v141_accounts_for_profiling as a
left outer join  #previous_post_ta as b
on a.account_number = b.account_number
;
commit;

---Previous Upgrades/Downgrades---
SELECT          csh.account_number
                ,max(case when effective_from_dt between '2012-07-01' and '2012-09-30' and now.revenue_gbp>prev.revenue_gbp then 1 else 0 end) as  previous_recent_upgrade
                ,max(case when effective_from_dt between '2012-07-01' and '2012-09-30' and now.revenue_gbp<prev.revenue_gbp then 1 else 0 end) as  previous_recent_downgrade

                ,max(case when effective_from_dt between '2012-11-15' and '2013-02-14' and now.revenue_gbp>prev.revenue_gbp then 1 else 0 end) as  subsequent_upgrade
                ,max(case when effective_from_dt between '2012-11-15' and '2013-02-14' and now.revenue_gbp<prev.revenue_gbp then 1 else 0 end) as  subsequent_downgrade

                ,max(case when effective_from_dt between '2012-11-15' and '2013-02-14' and now.revenue_gbp>prev.revenue_gbp and now.prem_movies>prev.prem_movies then 1 else 0 end) as  subsequent_movies_upgrade
                ,max(case when effective_from_dt between '2012-11-15' and '2013-02-14' and now.revenue_gbp<prev.revenue_gbp and now.prem_movies<prev.prem_movies then 1 else 0 end) as  subsequent_movies_downgrade

                ,max(case when effective_from_dt between '2012-11-15' and '2013-02-14' and now.revenue_gbp>prev.revenue_gbp and now.prem_sports>prev.prem_sports then 1 else 0 end) as  subsequent_sports_upgrade
                ,max(case when effective_from_dt between '2012-11-15' and '2013-02-14' and now.revenue_gbp<prev.revenue_gbp and now.prem_sports<prev.prem_sports  then 1 else 0 end) as  subsequent_sports_downgrade

INTO            #upgrade_downgrade -- DROP TABLE #start_accounts
FROM            sk_prod.cust_subs_hist AS csh
        LEFT OUTER JOIN sk_prod.cust_entitlement_lookup now
                    on  csh.current_short_description = now.short_description
        LEFT OUTER JOIN sk_prod.cust_entitlement_lookup prev
                    on  csh.previous_short_description = prev.short_description

WHERE           subscription_sub_type = 'DTV Primary Viewing'
        AND     csh.status_code IN ('AC','AB','PC') 
        AND     csh.effective_from_dt >= '2012-07-01'
        AND     csh.effective_from_dt <> csh.effective_to_dt
group by csh.account_number
;
--select top 100 * from sk_prod.cust_entitlement_lookup
commit;
CREATE        hg INDEX idx1 on #upgrade_downgrade (account_number);
commit;

alter table v141_accounts_for_profiling add previous_cuscan tinyint default 0;
alter table v141_accounts_for_profiling add previous_syscan tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_cuscan tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_syscan tinyint default 0;
alter table v141_accounts_for_profiling add recent_winback_reactivation tinyint default 0;

alter table v141_accounts_for_profiling add subsequent_pending_cancel tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_active_block tinyint default 0;

update v141_accounts_for_profiling
set previous_cuscan=case when b.previous_cuscan is null then 0 else b.previous_cuscan end
,previous_syscan=case when b.previous_syscan is null then 0 else b.previous_syscan end
,subsequent_cuscan=case when b.subsequent_cuscan is null then 0 else b.subsequent_cuscan end
,subsequent_syscan=case when b.subsequent_syscan is null then 0 else b.subsequent_syscan end
,subsequent_pending_cancel=case when b.subsequent_pending_cancel is null then 0 else b.subsequent_pending_cancel end
,subsequent_active_block=case when b.subsequent_active_block is null then 0 else b.subsequent_active_block end
,recent_winback_reactivation=case when b.recent_winback_reactivation is null then 0 else b.recent_winback_reactivation end
from v141_accounts_for_profiling as a
left outer join  #previous_churn_winback as b
on a.account_number = b.account_number
;

alter table v141_accounts_for_profiling add previous_recent_upgrade tinyint default 0;
alter table v141_accounts_for_profiling add previous_recent_downgrade tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_upgrade tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_downgrade tinyint default 0;

alter table v141_accounts_for_profiling add subsequent_movies_upgrade tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_movies_downgrade tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_sports_upgrade tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_sports_downgrade tinyint default 0;

update v141_accounts_for_profiling
set previous_recent_upgrade=case when b.previous_recent_upgrade is null then 0 else b.previous_recent_upgrade end
,previous_recent_downgrade=case when b.previous_recent_downgrade is null then 0 else b.previous_recent_downgrade end
,subsequent_upgrade=case when b.subsequent_upgrade is null then 0 else b.subsequent_upgrade end
,subsequent_downgrade=case when b.subsequent_downgrade is null then 0 else b.subsequent_downgrade end

,subsequent_movies_upgrade=case when b.subsequent_movies_upgrade is null then 0 else b.subsequent_movies_upgrade end
,subsequent_movies_downgrade=case when b.subsequent_movies_downgrade is null then 0 else b.subsequent_movies_downgrade end
,subsequent_sports_upgrade=case when b.subsequent_sports_upgrade is null then 0 else b.subsequent_sports_upgrade end
,subsequent_sports_downgrade=case when b.subsequent_sports_downgrade is null then 0 else b.subsequent_sports_downgrade end


from v141_accounts_for_profiling as a
left outer join  #upgrade_downgrade as b
on a.account_number = b.account_number
;
commit;

alter table v141_accounts_for_profiling add tenure varchar(15) default 'E) Unknown';

update v141_accounts_for_profiling
set tenure=CASE   WHEN datediff(day,acct_first_account_activation_dt,cast('2012-11-14' as date)) <=  365 THEN 'A) 0-1 Year'
                         WHEN datediff(day,acct_first_account_activation_dt,cast('2012-11-14' as date)) <=  730 THEN 'B) 1-2 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,cast('2012-11-14' as date)) <= 3650 THEN 'C) 3-10 Years'
                         WHEN datediff(day,acct_first_account_activation_dt,cast('2012-11-14' as date)) > 3650 THEN  'D) 10 Years+'
                         ELSE 'E) Unknown' end
from v141_accounts_for_profiling as a
left outer join  sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
--select top 100 * from sk_prod.cust_entitlement_lookup;

alter table v141_accounts_for_profiling add cable_area tinyint default 0;

   UPDATE v141_accounts_for_profiling
      SET cable_area = case when bb.cb_address_postcode is not null then 1 else 0 end
     FROM v141_accounts_for_profiling AS a
    left outer join  sk_prod.cust_single_account_view as b
    on a.account_number = b.account_number
          LEFT OUTER JOIN sk_prod.broadband_postcode_exchange  as bb
               ON replace(b.cb_address_postcode, ' ','') = replace(bb.cb_address_postcode,' ','')
;
commit;

alter table v141_accounts_for_profiling add fibre tinyint default 0;
UPDATE v141_accounts_for_profiling
   SET fibre = 1
  FROM v141_accounts_for_profiling AS base
    inner join  sk_prod.cust_single_account_view as b
    on base.account_number = b.account_number
       INNER JOIN sk_prod.BT_FIBRE_POSTCODE AS fib
                  ON REPLACE(b.cb_address_postcode,' ','') =  REPLACE(fib.cb_address_postcode,' ','')
                 AND fib.fibre_enabled_perc >= 75
 WHERE fib.first_fibre_enabled_date <='2013-02-14'
;
commit;

---HD/Multiroom Cancellation post viewing period

SELECT  csh.account_number
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                       AND csh.status_code in ('PO','SC') and csh.prev_status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS subsequent_multiroom_cancellation
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                       AND csh.status_code in ('PO','SC') and csh.prev_status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS subsequent_hd_cancellation
           
INTO #mr_hd_churn
      FROM sk_prod.cust_subs_hist AS csh 
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD')
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.effective_from_dt between '2012-11-15' and '2013-02-14'
and status_code_changed='Y'
GROUP BY csh.account_number;
commit;

commit;
CREATE        hg INDEX idx1 on #mr_hd_churn (account_number);

alter table v141_accounts_for_profiling add subsequent_multiroom_cancellation tinyint default 0;
alter table v141_accounts_for_profiling add subsequent_hd_cancellation tinyint default 0;

update v141_accounts_for_profiling
set subsequent_multiroom_cancellation=case when b.subsequent_multiroom_cancellation is null then 0 else b.subsequent_multiroom_cancellation end
,subsequent_hd_cancellation=case when b.subsequent_hd_cancellation is null then 0 else b.subsequent_hd_cancellation end
from v141_accounts_for_profiling as a
left outer join  #mr_hd_churn as b
on a.account_number = b.account_number
;

--select top 500 * from v141_average_sov_for_channel;
--select bbc_one_viewing,count(*) as records from  #channel_viewing_summary group by bbc_one_viewing
--select channel_name_inc_staggercast,sum(total_duration) as tot_dur from v141_summary_by_account_and_channel group by channel_name_inc_staggercast order by channel_name_inc_staggercast,tot_dur desc;

----Produce Pivot table of attributes and Channel Share----

--select top 100 * from v141_channel_sov;

---Enhanced version of Pivot with extra Demographic variables---
--drop table v141_churn_pivot_output_with_channel_splits;
select case when value_segment is null then 'Unstable' else value_segment end as value_seg
,case when  round(total_paid/(total_paid+total_free),3)>=0.3 then '01: High Proportion of Pay Viewing (30%' else '02: Low Proportion of Pay Viewing' end as pay_vs_free
,case when Sky_Go_Reg=1 then 1 else 0 end used_sky_go_L6M
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' else 'e) No Premiums' end as premiums_type
,case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
, case when hdtv is null then 0 else hdtv end as has_hdtv
,a.hd_viewing as has_viewed_hd_content
,case when multiroom is null then 0 else multiroom end as has_multiroom
,case when skyplus is null then 0 else skyplus end as has_skyplus
,a.account_weight
,a.subscription_3d as has_3d_subscription
,a.sky_id as has_sky_id
,a.mixes_type
,a.previous_cuscan
,a.previous_syscan
,a.subsequent_cuscan
,a.subsequent_syscan
,a.recent_winback_reactivation
,a.previous_recent_upgrade
,a.previous_recent_downgrade
,a.subsequent_upgrade
,a.subsequent_downgrade
,a.subsequent_multiroom_cancellation
,a.previous_ta
,a.subsequent_ta
,a.tenure
,a.cable_area
,a.fibre as fibre_broadband_area
,  CASE  hh_composition   WHEN     '00' THEN   'a) Families'
                                                WHEN     '01' THEN   'b) Extended family'
                                                WHEN     '02' THEN   'c) Extended household'
                                                WHEN     '03' THEN   'd) Pseudo family'
                                                WHEN     '04' THEN   'e) Single male'
                                                WHEN     '05' THEN   'f) Single female'
                                                WHEN     '06' THEN   'g) Male homesharers'
                                                WHEN     '07' THEN   'h) Female homesharers'
                                                WHEN     '08' THEN   'i) Mixed homesharers'
                                                WHEN     '09' THEN   'j) Abbreviated male families'
                                                WHEN     '10' THEN   'k) Abbreviated female families'
                                                WHEN     '11' THEN   'l) Multi-occupancy dwelling'
                                                WHEN     'U'  THEN   'm) Unclassified'
                                                ELSE                 'm) Unclassified'
                                                END as household_composition
,  case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' END as affluence
, case  when head_hh_age IN ('0')       THEN 'a) Age 18-25'
        when head_hh_age IN ('1')       THEN 'b) Age 26-35'
        when head_hh_age IN ('2')       THEN 'c) Age 36-45'
        when head_hh_age IN ('3')       THEN 'd) Age 46-55'
        when head_hh_age IN ('4')       THEN 'e) Age 56-65'
        when head_hh_age IN ('5')       THEN 'f) Age 66+' else 'g) Unknown'
                                                END as head_of_hh_age
, case  when oldest_female_in_hh IN ('0')       THEN 'a) Age 18-25'
        when oldest_female_in_hh IN ('1')       THEN 'b) Age 26-35'
        when oldest_female_in_hh IN ('2')       THEN 'c) Age 36-45'
        when oldest_female_in_hh IN ('3')       THEN 'd) Age 46-55'
        when oldest_female_in_hh IN ('4')       THEN 'e) Age 56-65'
        when oldest_female_in_hh IN ('5')       THEN 'f) Age 66+' else 'g) Unknown/No female in HH'
                                                END as oldest_female_age
,case  
        when num_children_in_hh in ('1','2','3','4') then 1 else 0 end any_kids_in_hh
,case  
        when isba_tv_region is null then 'Not Defined' else isba_tv_region end as tv_region

--(60*60*60=60 Hours)
,case when f.total_duration_for_account>=(60*60*400) then '01: 400+ Hours Viewed in period'
      when f.total_duration_for_account>=(60*60*300) then '02: 300-399 Hours Viewed in period'
      when f.total_duration_for_account>=(60*60*200) then '03: 200-299 Hours Viewed in period'
      when f.total_duration_for_account>=(60*60*100) then '04: 100-199 Hours Viewed in period'
      when f.total_duration_for_account>=(60*60*50) then '05: 50-99 Hours Viewed in period' else '06: Under 50 Hours Viewed in period' end as total_viewing_duration_split





,case when sky_movies_combined_viewing is null then '03: No Viewing' else sky_movies_combined_viewing end as sky_movie_channels_viewing
,g.*

--New Variables--
, case  when oldest_male_in_hh IN ('0')       THEN 'a) Age 18-25'
        when oldest_male_in_hh IN ('1')       THEN 'b) Age 26-35'
        when oldest_male_in_hh IN ('2')       THEN 'c) Age 36-45'
        when oldest_male_in_hh IN ('3')       THEN 'd) Age 46-55'
        when oldest_male_in_hh IN ('4')       THEN 'e) Age 56-65'
        when oldest_male_in_hh IN ('5')       THEN 'f) Age 66+' else 'g) Unknown/No female in HH'
                                                END as oldest_male_age
,total_adult_males_in_hh 
,total_adult_females_in_hh 
,case when child_aged_0_4='1' then 1 else 0 end as has_children_aged_0_4           
,case when child_aged_5_11='1' then 1 else 0 end as has_children_aged_5_11
,case when child_aged_12_17  ='1' then 1 else 0 end as has_children_aged_12_17
    
, case  when residence_type IN ('0')       THEN '1) Detached'
        when residence_type IN ('1')       THEN '2) Semi-detached'
        when residence_type IN ('2')       THEN '3) Bungalow'
        when residence_type IN ('3')       THEN '4) Terraced'
        when residence_type IN ('4')       THEN '5) Flat'
        when residence_type IN ('U')       THEN '6) Unclassified' else '6) Unclassified'
                                                END as property_type  

, case  when own_rent_status IN ('0')       THEN '1) Owner occupied'
        when own_rent_status IN ('1')       THEN '2) Privately rented'
        when own_rent_status IN ('2')       THEN '3) Council / housing association'
        when own_rent_status IN ('U')       THEN '4) Unclassified' else '4) Unclassified'
                                                END as own_rent_type  

,case when subsequent_ta_saved is null then 0 else subsequent_ta_saved end as has_subsequent_ta_saved
,case when subsequent_ta_failed is null then 0 else subsequent_ta_failed end as has_subsequent_ta_fail

,case when subsequent_movies_upgrade is null then 0 else subsequent_movies_upgrade end as has_subsequent_movies_upgrade
,case when subsequent_movies_downgrade is null then 0 else subsequent_movies_downgrade end as has_subsequent_movies_downgrade
,case when subsequent_sports_upgrade is null then 0 else subsequent_sports_upgrade end as has_subsequent_sports
,case when subsequent_sports_downgrade is null then 0 else subsequent_sports_downgrade end as has_subsequent_sports_downgrade

,case when sky_sports_combined_viewing is null then '03: No Viewing' else sky_sports_combined_viewing end as sky_sport_channels_viewing

,f.duration_live
,f.duration_playback_within_15_min_of_tx
,f.duration_playback_16_to_60_minutes_after_tx
,f.duration_playback_over_60_minutes_after_tx_VOSDAL
,f.duration_playback_within_7_days_exc_VOSDAL
,f.duration_playback_over_7_days_after_tx
,subsequent_pending_cancel
,subsequent_active_block

into v141_churn_pivot_output_with_channel_splits_v2
from v141_accounts_for_profiling as a
left outer join v141_channel_pay_free as b
on a.account_number=b.account_number
left outer join #sky_movies_combined_viewing as c
on a.account_number=c.account_number
left outer join #sky_sports_combined_viewing as d
on a.account_number=d.account_number
left outer join v141_summary_by_account_only as f
on a.account_number=f.account_number
left outer join v141_high_low_channel_viewing_summary_by_account as g
on a.account_number=g.account_number

;
grant all on v141_churn_pivot_output_with_channel_splits_v2 to public;

commit;
--alter table v141_churn_pivot_output_with_channel_splits delete account_number; commit;

--select top 500 * from v141_churn_pivot_output_with_channel_splits 

---Add on A reworked Movies version

--,case when sky_movies_combined_viewing is null then '03: No Viewing' else sky_movies_combined_viewing end as sky_movie_channels_viewing

alter table v141_churn_pivot_output_with_channel_splits add sky_movie_channels_viewing_updated_20130917 varchar(40); commit;

update v141_churn_pivot_output_with_channel_splits
set sky_movie_channels_viewing_updated_20130917=case when sky_movies_combined_viewing is null then '03: No Viewing' else sky_movies_combined_viewing end 
from v141_churn_pivot_output_with_channel_splits as a

left outer join #sky_movies_combined_viewing as c
on a.account_number=c.account_number
;
commit;

/*
select  sky_news_viewing,sky_movie_channels_viewing,sum(account_weight) from v141_churn_pivot_output_with_channel_splits_v2 group by sky_news_viewing,sky_movie_channels_viewing order by sky_news_viewing,sky_movie_channels_viewing
*/

/*Main Channel list for Pivot
select value_seg
,pay_vs_free
,used_sky_go_L6M
,premiums_type
,bb_talk_holdings
,has_hdtv
,has_viewed_hd_content
,has_multiroom
,has_skyplus
,account_weight
,has_3d_subscription
,has_sky_id
,mixes_type
,previous_cuscan
,previous_syscan
,subsequent_cuscan
,subsequent_syscan
,recent_winback_reactivation
,previous_recent_upgrade
,previous_recent_downgrade
,subsequent_upgrade
,subsequent_downgrade
,subsequent_multiroom_cancellation
,previous_ta
,subsequent_ta
,tenure
,cable_area
,fibre_broadband_area
,household_composition
,affluence
,head_of_hh_age
,any_kids_in_hh
,tv_region
,oldest_male_age
,oldest_female_age
,total_adult_males_in_hh 
,total_adult_females_in_hh 
,has_children_aged_0_4           
,has_children_aged_5_11
,has_children_aged_12_17
    
,property_type  

,own_rent_type  

,has_subsequent_ta_saved
,has_subsequent_ta_fail

,has_subsequent_movies_upgrade
,has_subsequent_movies_downgrade
,has_subsequent_sports
,has_subsequent_sports_downgrade


,subsequent_pending_cancel
,subsequent_active_block

,total_viewing_duration_split

,duration_live
,duration_playback_within_15_min_of_tx
,duration_playback_16_to_60_minutes_after_tx
,duration_playback_over_60_minutes_after_tx_VOSDAL
,duration_playback_within_7_days_exc_VOSDAL
,duration_playback_over_7_days_after_tx

,sky_movie_channels_viewing
,sky_sport_channels_viewing
,Sky_Atlantic_Viewing
,Sky_1_Viewing
,Sky_2_Viewing
,Sky_Living_Viewing
,Sky_LivingIt_Viewing
,Challenge_Viewing
,Sky_Arts_1_Viewing
,Sky_Arts_2_Viewing
,Pick_TV_Viewing
,BBC_ONE_Viewing
,BBC_TWO_Viewing
,ITV1_Viewing
,Channel_4_Viewing
,Channel_5_Viewing
,BBC_HD_Viewing
,Sky_News_Viewing
,BBC_News_Viewing

,Sky_Box_Office_Viewing
,Sky_DramaRom_Viewing
,Sky_Movies_007_Viewing
,Sky_Movies_Action_Viewing
,Sky_Movies_Classics_Viewing
,Sky_Movies_Comedy_Viewing
,Sky_Movies_Family_Viewing
,Sky_Movies_Indie_Viewing
,Sky_Movies_Mdn_Greats_Viewing
,Sky_Movies_Sci_Fi_Horror_Viewing
,Sky_Movies_Showcase_Viewing
,Sky_Movies_Thriller_Viewing
,Sky_Premiere_Viewing
,Sky_Sports_1_Viewing
,Sky_Sports_2_Viewing
,Sky_Sports_3_Viewing
,Sky_Sports_4_Viewing
,Sky_Sports_F1_Viewing

,Sky_Sports_News_Viewing
,Eurosport_Viewing
,E4_Viewing
,More4_Viewing
,Dave_Viewing
,Alibi_Viewing
,Watch_Viewing
,Gold_Viewing
,Home_Viewing
,Food_Network_Viewing
,Eden_Viewing
,Really_Viewing
,Yesterday_Viewing
,Quest_Viewing
,DMAX_Viewing
,Bio_Viewing
,ITV2_Viewing
,ITV3_Viewing
,ITV4_Viewing
,BBC_THREE_Viewing
,BBC_FOUR_Viewing
,Film4_Viewing
,_4seven_Viewing
,_5star_Viewing
,_5_USA_Viewing
,Discovery_Viewing
,History_Viewing
,FX_Viewing
,SyFy_Viewing
,MTV_Viewing
,Comedy_Central_Viewing
,Universal_Viewing
,CBS_Reality_Viewing
,TCM_Viewing
,Discovery_Turbo_Viewing
,Discovery_Realtime_Viewing
,Disc_Science_Viewing
,Disc_History_Viewing
,Nat_Geo_Viewing
,Nat_Geo_Wild_Viewing
,Animal_Planet_Viewing
,Military_History_Viewing
,Cbeebies_Viewing
,Nickelodeon_Viewing
,Nick_Jr_Viewing
,Boomerang_Viewing
,Disney_Viewing
,Disney_Junior_Viewing
,Disney_XD_Viewing
,CBBC_Viewing
,CITV_Viewing
,Nicktoons_TV_Viewing
,Cartoon_Network_Viewing
,_4Music_Viewing
,VIVA_Viewing
,VH1_Viewing
,The_Box_Viewing
,Magic_Viewing
,Smash_Hits_Viewing
from v141_churn_pivot_output_with_channel_splits
commit;

---Repeat for non-Main Channels

select value_seg
,pay_vs_free
,used_sky_go_L6M
,premiums_type
,bb_talk_holdings
,has_hdtv
,has_viewed_hd_content
,has_multiroom
,has_skyplus
,account_weight
,has_3d_subscription
,has_sky_id
,mixes_type
,previous_cuscan
,previous_syscan
,subsequent_cuscan
,subsequent_syscan
,recent_winback_reactivation
,previous_recent_upgrade
,previous_recent_downgrade
,subsequent_upgrade
,subsequent_downgrade
,subsequent_multiroom_cancellation
,previous_ta
,subsequent_ta
,tenure
,cable_area
,fibre_broadband_area
,household_composition
,affluence
,head_of_hh_age
,any_kids_in_hh
,tv_region
,total_viewing_duration_split
,sky_movie_channels_viewing
,Anytime_Viewing
,At_The_Races_Viewing
,Blighty_Viewing
,Bliss_Viewing
,Bloomberg_Viewing
,BoxNation_Viewing
,Cartoonito_Viewing
,CBS_Action_Viewing
,CBS_Drama_Viewing
,Chart_Show_TV_Viewing
,Chelsea_TV_Viewing
,CNBC_Viewing
,Comedy_Central_Extra_Viewing
,Crime_and_Investigation_Viewing
,DanceNationTV_Viewing
,Dave_ja_vu_Viewing
,Disc_Shed_Viewing
,Disney_Channel_Viewing
,Disney_Cinemagic_Viewing
,Diverse_Viewing
,E_Entertainment_Viewing
,ESPN_America_Viewing
,ESPN_Classic_Viewing
,ESPN_Viewing
,Euronews_Viewing
,Eurosport_2_Viewing
,Extreme_Viewing
,Flava_Viewing
,Fox_News_Viewing
,Good_Food_Viewing
,Greatest_Hits_TV_Viewing
,Heat_Viewing
,Home_and_Health_Viewing
,Horror_Viewing
,Ideal_Extra_Viewing
,Kiss_TV_Viewing
,Kix_Viewing
,Liverpool_FC_TV_Viewing
,men_and_movies_Viewing
,MGM_HD_Viewing
,MOTORS_TV_UK_Viewing
,movies4men_Viewing
,MTV_BASE_Viewing
,MTV_CLASSIC_Viewing
,MTV_DANCE_Viewing
,MTV_HITS_Viewing
,MTV_Live_HD_Viewing
,MTV_Live_Viewing
,MTV_Music_Viewing
,MTV_ROCKS_Viewing
,MUTV_Viewing
,NDTV_24x7_Viewing
,NHK_World_HD_Viewing
,Nick_Jr_2_Viewing
,NToons_Replay_Viewing
,NTV_Viewing
,PBS_America_Viewing
,POP_Viewing
,Premier_Sports_Viewing
,Primetime_Viewing
,S4C_Viewing
,SBO_Viewing
,Scuzz_Viewing
,Sky_3D_Viewing

,SkyPoker_com_Viewing
,SONY_MAX_Viewing
,SONY_SAB_Viewing
,Sony_TV_Asia_Viewing
,STAR_Gold_Viewing
,STAR_Plus_Viewing
,TCM_2_Viewing
,TCM_HD_Viewing
,The_Vault_Viewing
,Tiny_Pop_Viewing
,XFM_Viewing
,Zee_Cafe_Viewing
,Zee_Cinema_Viewing
,Zee_Punjabi_Viewing
,Zee_TV_Viewing
from v141_churn_pivot_output_with_channel_splits
commit;




*/

--exec gen_create_table 'v141_churn_pivot_output_with_channel_splits'
--drop variable @tablename;

--select top 100 * from v141_churn_pivot_output_with_channel_splits;
--select count(*) from v141_churn_pivot_output_with_channel_splits;
--select count(*) from v141_accounts_for_profiling;
--select max(account_weight) from v141_accounts_for_profiling