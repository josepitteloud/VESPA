/*------------------------------------------------------------------------------
        Project: V249 - Data for Pivot by Channel Rebuild
        Version: 1
        Created: 20131111
        Lead: 
        Analyst: Dan Barnett
        SK Prod: 5
        Follow up analysis based on created a pivot with viewing splits for 'every' channel

*/------------------------------------------------------------------------------


---Stages---
/*

1.	Create Profile table for attributes as at start of 13 week period (2nd August 2013) as well as activity during the quarter for Sky activity related variables (e.g., Sky Go activity).
2.	Create updated EPG related meta-data e.g.,
    a.	update EPG data to latest channel names/staggercast groupings e.g., Sky Sports 2/Sky Sports Ashes
    b.	correct channel names classed as ‘Other TV’ for BT Sport/Sky Movies Disney
3.	Create summary viewing table of all viewing activity (initially all viewing with the ability to isolate 3+ minute activity) by program
4.	Aggregate up from a program level view to a channel level view (by account), splits could be high/low/no viewing, Decile level viewing or absolute values e.g., ,Number of engaged/3min+programmes viewed
5.	Determine which accounts with activity in the 13 week period are suitable for inclusion in the analysis (sufficient logs returned) and create weights
6.	Append inclusion flag and weight value to profile table
7.	Join profile and viewing summary information to create output pivot (in reality number of combinations means that will be a flat file with approx. 500k records).  As analysis covers several hundred channels sheer number of columns means that output in a single PowerPivot is unlikely.  Output may have to be split into groups of channels or a different tool such as Tableau used.
*/




--select top 5000 * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES order by full_name



---Part I - EPG Data---
select  service_key
,full_name
,epg_name
,vespa_name
,channel_name
,pay_free_indicator
,format 
,timeshift_status
,timeshift_minutes
,channel_pack
,vespa_name as channel_name_inc_hd
,vespa_name as channel_name_inc_staggercast
into v249_service_key_lookup_from_channel_map
from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES 
where effective_to >= '2999-12-31'
order by full_name
;
--select * from v249_service_key_lookup_from_channel_map;
--Add On Staggercast Versions of channel Names---

update v249_service_key_lookup_from_channel_map
set channel_name_inc_staggercast=
Case when channel_name_inc_hd='5 USA +1' then '5 USA'
when channel_name_inc_hd='5* +1' then '5*'
when channel_name_inc_hd='Alibi +1' then 'Alibi'
when channel_name_inc_hd='Animal Planet +1' then 'Animal Planet'
when channel_name_inc_hd='BET +1' then 'BET'
when channel_name_inc_hd='Boomerang +1' then 'Boomerang'
when channel_name_inc_hd='CBS Reality+1' then 'CBS Reality'
when channel_name_inc_hd='Challenge+1' then 'Challenge'
when channel_name_inc_hd='Channel 5+1' then 'Channel 5'
when channel_name_inc_hd='Channel 4+1' then 'Channel 4'
when channel_name_inc_hd='Chart Show+1' then 'Chart Show TV'
when channel_name_inc_hd='Comedy Central +1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra +1' then 'Comedy Central Extra'
when channel_name_inc_hd='CI+1' then 'CI'
when channel_name_inc_hd='DMAX+1' then 'DMAX'
when channel_name_inc_hd='DMax +2' then 'DMAX'
when channel_name_inc_hd='Dave ja vu' then 'Dave'
when channel_name_inc_hd='Disc. History+1' then 'Disc. History'
when channel_name_inc_hd='Disc.Science +1' then 'Disc.Science'
when channel_name_inc_hd='Discovery +1hr' then 'Discovery'
when channel_name_inc_hd='Discovery+1' then 'Discovery'
when channel_name_inc_hd='Discovery RealTime +1' then 'Discovery RealTime'
when channel_name_inc_hd='Disney +1' then 'Disney'
when channel_name_inc_hd='Disney Cinemagic +1' then 'Disney Cinemagic'
when channel_name_inc_hd='Disney Junior+' then 'Playhouse Disney'
when channel_name_inc_hd='Disney XD+1' then 'Disney XD'
when channel_name_inc_hd='E4 +1' then 'E4'
when channel_name_inc_hd='Eden+1' then 'Eden'
when channel_name_inc_hd='FX+' then 'FX'
when channel_name_inc_hd='Film4 +1' then 'Film4'
when channel_name_inc_hd='Food Network+1' then 'Food Network'
when channel_name_inc_hd='GOLD +1' then 'GOLD  (TV)'
when channel_name_inc_hd='Good Food +1' then 'Good Food'
when channel_name_inc_hd='History +1 hour' then 'History'
when channel_name_inc_hd='Home & Health +1' then 'Home & Health'
when channel_name_inc_hd='Home+1' then 'Home'
when channel_name_inc_hd='ITV - ITV3+1' then 'ITV3'
when channel_name_inc_hd='ITV Channel Is' then 'ITV 1'
when channel_name_inc_hd='ITV HD' then 'ITV 1'
when channel_name_inc_hd='ITV1 Central SW' then 'ITV 1'
when channel_name_inc_hd='ITV1+1' then 'ITV 1'
when channel_name_inc_hd='ITV1 +1' then 'ITV 1'
when channel_name_inc_hd='ITV2+1' then 'ITV 2'
when channel_name_inc_hd='ITV3+1' then 'ITV 3'
when channel_name_inc_hd='ITV4+1' then 'ITV 4'
when channel_name_inc_hd='MTV+1' then 'MTV'
when channel_name_inc_hd='More4 +1' then 'More 4'
when channel_name_inc_hd='More4+2' then 'More 4'
when channel_name_inc_hd='Movies 24+' then 'Movies 24'
when channel_name_inc_hd='Nat Geo+1hr' then 'Nat Geo'
when channel_name_inc_hd='Nick Replay' then 'Nickelodeon'
when channel_name_inc_hd='N''Toons Replay' then 'Nicktoons TV'
when channel_name_inc_hd='Pick TV +1' then 'Pick TV'
when channel_name_inc_hd='PopGirl+1' then 'Pop Girl'
when channel_name_inc_hd='QUEST+1' then 'QUEST'
when channel_name_inc_hd='SONY TV +1' then 'SONY TV'
when channel_name_inc_hd='Showcase+1' then 'Showcase'
when channel_name_inc_hd='Sky1+1' then 'Sky1'
when channel_name_inc_hd='Sky Living +1' then 'Sky Living'
when channel_name_inc_hd='Sky Living+1' then 'Sky Living'
when channel_name_inc_hd='Sky Living +1 ROI' then 'Sky Living'
when channel_name_inc_hd='Sky Livingit+1' then 'Sky Livingit'
when channel_name_inc_hd='Sky Prem+1' then 'Sky Premiere'
when channel_name_inc_hd='Sony Movies+1' then 'Sony Movies'
when channel_name_inc_hd='Syfy +1' then 'Syfy'
when channel_name_inc_hd='Tiny Pop +1' then 'Tiny Pop'
when channel_name_inc_hd='Travel Channel +1' then 'Travel Channel'
when channel_name_inc_hd='Universal +1' then 'Universal'
when channel_name_inc_hd='Watch +1' then 'Watch'
when channel_name_inc_hd='YeSTERDAY +1' then 'YeSTERDAY'
when channel_name_inc_hd='horror channel +1' then 'horror channel'
when channel_name_inc_hd='men&movs+1' then 'men&movies'
when channel_name_inc_hd='mov4men+1' then 'movies4men'
when channel_name_inc_hd='mov4men2 +1' then 'movies4men 2'
when channel_name_inc_hd='5 USA+1' then '5 USA'
when channel_name_inc_hd='Africa Chnl+1' then 'Africa Channel'
when channel_name_inc_hd='Alibi+1' then 'Alibi'
when channel_name_inc_hd='Animal Planet+1' then 'Animal Planet'
when channel_name_inc_hd='BET+1' then 'BET'
when channel_name_inc_hd='Bio+1' then 'Bio'
when channel_name_inc_hd='Boomerang +1' then 'Boomerang'
when channel_name_inc_hd='CI +1' then 'CI'
when channel_name_inc_hd='Challenge +1' then 'Challenge'
when channel_name_inc_hd in ('Channel 4 +1'
,'Channel 4+1 (ROI)')
 then 'Channel 4'

when channel_name_inc_hd='Channel 5+1' then 'Channel 5'
when channel_name_inc_hd='Christmas 24+' then 'Christmas 24'
when channel_name_inc_hd='Comedy Central +1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra+1' then 'Comedy Central Extra'
when channel_name_inc_hd='Comedy Central+1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra+1' then 'Comedy Central Extra'
when channel_name_inc_hd='Discovery History+1' then 'Discovery History'
when channel_name_inc_hd='Discovery Home & Health +1' then 'Discovery Home & Health'
when channel_name_inc_hd='Discovery Home & Health +1' then 'Discovery Home & Health'
when channel_name_inc_hd='Discovery Real Time +1' then 'Discovery Real Time'
when channel_name_inc_hd='Discovery Science+1' then 'Discovery Science'
when channel_name_inc_hd='Disney Channel+1' then 'Disney Channel'
when channel_name_inc_hd in ('E4 (ROI)'
,'E4 +1'
,'E4 +1 (ROI)'
,'E4+1 ROI')
 then 'E4'
when channel_name_inc_hd='FOX+' then 'FOX'
when channel_name_inc_hd='GOLD+1' then 'GOLD'
when channel_name_inc_hd='Good Food+1' then 'Good Food'
 else channel_name_inc_hd end
;

--Second Part

update v249_service_key_lookup_from_channel_map
set channel_name_inc_staggercast= Case  
when channel_name_inc_hd='History +1hour' then 'History'
when channel_name_inc_hd='Horror Channel+1' then 'Horror'
when channel_name_inc_hd='ID+1' then 'ID'
when channel_name_inc_hd='Info TV+1' then 'Info TV'
when channel_name_inc_hd='More 4 +1' then 'More 4'
when channel_name_inc_hd='More 4+2' then 'More 4'
when channel_name_inc_hd='Movies4Men +1' then 'Movies4Men'
when channel_name_inc_hd='National Geographic+1' then 'National Geographic'
when channel_name_inc_hd='Nick Jr +1' then 'Nick Jr.'
when channel_name_inc_hd='Nick Jr+1' then 'Nick Jr.'

when channel_name_inc_hd In('Nickelodeon +1'
,'Nickelodeon EIRE'
,'Nickelodeon+1') then 'Nickelodeon'
when channel_name_inc_hd='RAMP''d +2' then 'RAMP''d +2'

when channel_name_inc_hd In('Sky Atlantic +1'
,'Sky Atlantic ROI'
,'Sky Atlantic+1') then 'Sky Atlantic'

when channel_name_inc_hd='Sky Premiere +1' then 'Sky Premiere'
when channel_name_inc_hd='Sky1 +1' then 'Sky1'
when channel_name_inc_hd='TCM+1' then 'TCM'
when channel_name_inc_hd='TLC +1' then 'TLC'
when channel_name_inc_hd='TLC+1' then 'TLC'
when channel_name_inc_hd='TLC+2' then 'TLC'
when channel_name_inc_hd='True Ent+1' then 'True Ent'
when channel_name_inc_hd='Watch+1' then 'Watch'
when channel_name_inc_hd='More 4 +1' then 'More 4'
when channel_name_inc_hd='YESTERDAY +1' then 'YESTERDAY'

when channel_name_inc_hd='5 USA +1' then '5 USA'
when channel_name_inc_hd='5 USA+1' then '5 USA'
when channel_name_inc_hd='5* +1' then '5*'
when channel_name_inc_hd='Africa Chnl+1' then 'Africa Channel'
when channel_name_inc_hd='Alibi+1' then 'Alibi'

when channel_name_inc_hd='Animal Planet+1' then 'Animal Planet'
when channel_name_inc_hd='BET +1' then 'BET'
when channel_name_inc_hd='BET+1' then 'BET'
when channel_name_inc_hd='Bio+1' then 'Bio'
when channel_name_inc_hd='Boomerang +1' then 'Boomerang'

when channel_name_inc_hd='CBS Reality +1' then 'CBS Reality'
when channel_name_inc_hd='CI +1' then 'CI'
when channel_name_inc_hd='ITV +1' then 'ITV1'
when channel_name_inc_hd='Disney Junior+' then 'Disney Junior'
when left(channel_name,21)='Sky Sports Ineractive' then 'Sky Sports Ineractive'

when channel_name_inc_hd='National Geographic' then 'Nat Geo'
when channel_name_inc_hd='National Geographic+1' then 'Nat Geo'
when channel_name_inc_hd='Sony TV' then 'SONY TV'

when channel_name_inc_hd='Star Plus' then 'STAR Plus'
when channel_name_inc_hd='True Entertainment' then 'True Ent'
when channel_name_inc_hd='Investigation Discovery+1' then 'Investigation Discovery'
when channel_name_inc_hd='PICK TV+1' then 'PICK TV'
when channel_name_inc_hd='Paramount Comedy Eire +1' then 'Paramount Comedy'

when channel_name_inc_hd='Universal+1' then 'Universal'
when channel_name_inc_hd='more>movies +1' then 'more>movies'
when channel_name_inc_hd='ITV1 HD' then 'ITV1'
when channel_name_inc_hd='Sky2 ROI' then 'Sky2'
 else channel_name_inc_staggercast end
;
commit;

---Update Missing Pay/Free channels---

--select * from v249_service_key_lookup_from_channel_map where pay_free_indicator =''

update v249_service_key_lookup_from_channel_map
set pay_free_indicator=case when left(vespa_name,3)='BBC' then 'FTA' 
when full_name in ('Channel 4 On Demand','Channel 5 On Demand','ITV1 On Demand','ITV2 On Demand','ITV3 On Demand','ITV4 On Demand')
then 'FTA' else 'PAY' end
where pay_free_indicator=''
;
commit;



CREATE HG INDEX idx1 ON v249_service_key_lookup_from_channel_map (service_key);


/*Check for any timeshift not correctly reclassified

select channel_name_inc_hd,channel_name_inc_staggercast 
from v249_service_key_lookup_from_channel_map where timeshift_minutes>0 and vespa_name=channel_name_inc_staggercast
group by channel_name_inc_hd,channel_name_inc_staggercast 
;
*/



--select * from v249_service_key_lookup_from_channel_map;
--select count (*) ,count(distinct service_key) from v249_service_key_lookup_from_channel_map

---Create EPG Table----
--drop table v249_epg_data ;
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-01-01';  --Date for use for
--SET @snapshot_start_dt  = '2012-09-01';  --Original
SET @snapshot_end_dt    = '2013-11-01';

IF object_ID ('v249_epg_data') IS NOT NULL THEN
            DROP TABLE  v249_epg_data
END IF;
SELECT      dk_programme_instance_dim 
            ,service_key
            ,Channel_Name
            ,programme_instance_name as epg_title
            ,programme_instance_duration 
            ,Genre_Description
            ,Sub_Genre_Description
            ,epg_group_Name
            ,network_indicator
            ,broadcast_start_date_time_utc 
            ,broadcast_daypart as x_broadcast_Time_Of_Day
INTO  v249_epg_data
FROM sk_prod.Vespa_programme_schedule
WHERE (cast (broadcast_start_date_time_utc as date) between @snapshot_start_dt  and  @snapshot_end_dt)
;
commit;
--select * from sk_prod.Vespa_programme_schedule where pk_programme_instance_dim=817143493
--select * from sk_prod.Vespa_programme_schedule where dk_programme_dim=336647848
---Add on latest naming details from Viewing Attributes Table--
alter table v249_epg_data add pay_channel tinyint;
alter table v249_epg_data add channel_name_inc_hd varchar(90);
alter table v249_epg_data add channel_name_inc_staggercast varchar(90);
alter table v249_epg_data add format varchar(2);

update v249_epg_data
set pay_channel=case when b.pay_free_indicator='PAY' then 1 else 0 end
,channel_name_inc_hd=b.channel_name_inc_hd
,channel_name_inc_staggercast=b.channel_name_inc_staggercast
,format=b.format
from v249_epg_data as a
left outer join v249_service_key_lookup_from_channel_map as b
on a.service_key=b.service_key
;
commit;
CREATE HG INDEX idx1 ON v249_epg_data (service_key);
CREATE HG INDEX idx2 ON v249_epg_data (dk_programme_instance_dim);
commit;

--select * from v249_epg_data
--select count(*) from v249_epg_data;



--Part II Create Interim viewing table---
--drop table v249_summary_by_account_and_programme;
--drop table #program_summary;

select account_number
,dk_programme_instance_dim
,instance_start_date_time_utc as viewing_starts
, case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then capping_end_date_time_utc
            else instance_end_date_time_utc end as viewing_stops
,case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end as viewing_duration
,broadcast_start_date_time_utc
,service_key
,time_in_seconds_since_recording
,subscriber_id
,case when live_recorded = 'LIVE' then 'a) LIVE'
when time_in_seconds_since_recording <=900 then 'b) Within 15 min of Broadcast'
when time_in_seconds_since_recording <=3600 then 'c) Within 1 hour of Broadcast'

when dateformat(instance_start_date_time_utc,'YYYY-MM-DD-HH') between '2013-03-31-02' and '2013-10-27-02' 
and cast(dateadd(hh,-1,instance_start_date_time_utc)as date)  <=
cast (dateadd(hh,-1,broadcast_start_date_time_utc)as date ) 
then 'd) Over 60 minutes after tx VOSDAL'

when dateformat(instance_start_date_time_utc,'YYYY-MM-DD-HH')  > '2013-10-27-02' 
and cast(dateadd(hh,-2,instance_start_date_time_utc)as date)  <=cast(dateadd(hh,-2,broadcast_start_date_time_utc)as date) 
then 'd) Over 60 minutes after tx VOSDAL'  

when instance_start_date_time_utc <= dateadd(hour, 170, cast(broadcast_start_date_time_utc as datetime)) 
then 'e) Playback within 7 days exc VOSDAL'

when instance_start_date_time_utc > dateadd(hour, 170, cast(broadcast_start_date_time_utc as datetime))  
then 'f) Playback over 7 days after tx'
end as viewing_type
into #program_summary
from  sk_prod.vespa_dp_prog_viewed_201310 as a
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
and cast (instance_start_date_time_utc as date)='2013-10-26'
and right(account_number,1)='7'
;
commit;
--select count(*)  from  sk_prod.vespa_dp_prog_viewed_201310 where channel_name='Anytime'
--select channel_name,count(*)  from  sk_prod.vespa_dp_prog_viewed_201310 group by channel_name order by channel_name
CREATE HG INDEX idx1 ON #program_summary (account_number);
CREATE HG INDEX idx2 ON #program_summary (subscriber_id);
CREATE HG INDEX idx3 ON #program_summary (service_key);

commit;

--drop table v249_summary_by_account_and_programme;
--Create Summary by Programme---
select account_number
,subscriber_id
,dk_programme_instance_dim
,service_key
,sum(viewing_duration) as total_viewing_duration
,sum(case when viewing_type='a) LIVE' then viewing_duration else 0 end) as total_viewing_duration_live
,sum(case when viewing_type='b) Within 15 min of Broadcast' then viewing_duration else 0 end) as total_viewing_duration_within_15_min_of_tx
,sum(case when viewing_type='c) Within 1 hour of Broadcast' then viewing_duration else 0 end) as total_viewing_duration_16_to_60_minutes_after_tx
,sum(case when viewing_type='d) Over 60 minutes after tx VOSDAL' then viewing_duration else 0 end) as total_viewing_duration_over_60_minutes_after_tx_VOSDAL
,sum(case when viewing_type='e) Playback within 7 days exc VOSDAL' then viewing_duration else 0 end) as total_viewing_duration_within_7_days_exc_VOSDAL
,sum(case when viewing_type='f) Playback over 7 days after tx' then viewing_duration else 0 end) as total_viewing_duration_over_7_days_after_tx
into v249_summary_by_account_and_programme
from #program_summary as a
group by account_number
,subscriber_id
,dk_programme_instance_dim
,service_key
;

CREATE HG INDEX idx1 ON v249_summary_by_account_and_programme (dk_programme_instance_dim);
---Add on Programme/Channel Attributes---
--select count(*) from v249_summary_by_account_and_programme

commit;

--CREATE HG INDEX idx2 ON v249_epg_data (dk_programme_instance_dim);
commit;
alter table v249_summary_by_account_and_programme add pay_channel tinyint;
alter table v249_summary_by_account_and_programme add channel_name_inc_hd varchar(90);
alter table v249_summary_by_account_and_programme add channel_name_inc_staggercast varchar(90);
alter table v249_summary_by_account_and_programme add epg_title varchar(90);
alter table v249_summary_by_account_and_programme add hd tinyint;
alter table v249_summary_by_account_and_programme add programme_instance_duration integer;


update v249_summary_by_account_and_programme
set pay_channel=b.pay_channel
,channel_name_inc_hd=b.channel_name_inc_hd
,channel_name_inc_staggercast=b.channel_name_inc_staggercast
,epg_title=b.epg_title
,hd=case when format='HD' then 1 else 0 end
,programme_instance_duration=b.programme_instance_duration
from v249_summary_by_account_and_programme as a
left outer join v249_epg_data as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
;
commit;

select account_number
,channel_name_inc_staggercast
,sum(total_viewing_duration) as overall_viewing_duration
,sum(hd*total_viewing_duration) as overall_viewing_duration_hd
,sum(total_viewing_duration_live) as overall_viewing_duration_live
,sum(total_viewing_duration_within_15_min_of_tx) as overall_viewing_duration_within_15_min_of_tx
,sum(total_viewing_duration_16_to_60_minutes_after_tx) as overall_viewing_duration_16_to_60_minutes_after_tx
,sum(total_viewing_duration_over_60_minutes_after_tx_VOSDAL) as overall_viewing_duration_over_60_minutes_after_tx_VOSDAL
,sum(total_viewing_duration_within_7_days_exc_VOSDAL) as overall_viewing_duration_within_7_days_exc_VOSDAL
,sum(total_viewing_duration_over_7_days_after_tx) as overall_viewing_duration_over_7_days_after_tx
,sum(case when total_viewing_duration>=180 then 1 else 0 end) as programmes_viewed_3min_plus
,sum(case when total_viewing_duration>=3600 or total_viewing_duration/cast(programme_instance_duration as real)>=0.6 then 1 else 0 end) as programmes_viewed_engaged
from v249_summary_by_account_and_programme
where total_viewing_duration>=180
group by account_number
,channel_name_inc_staggercast
;

select * from v249_epg_data where channel_name_inc_staggercast='Anytime' order by channel_name
select * from sk_prod.vespa_dp_prog_viewed_201310  where channel_name='Anytime' order by channel_name

select channel_name, channel_name_inc_staggercast,count(*) from v249_epg_data where epg_group_name = 'Sky Push VOD' group by  channel_name ,channel_name_inc_staggercast order by channel_name

select channel_name, count(*) from v249_epg_data where channel_name_inc_staggercast is null group by  channel_name order by channel_name

select service_key ,sum(total_viewing_duration) as overall_viewing_duration from  v249_summary_by_account_and_programme where channel_name_inc_staggercast is null group by service_key
order by overall_viewing_duration desc

select channel_name_inc_staggercast

/*
--select count(*) from v249_summary_by_account_and_programme;
select epg_title
,sum(total_viewing_duration) as total_dur
,sum(total_viewing_duration_3min_plus_events) as total_dur_3min_plus
,sum(total_viewing_duration_live) as total_dur_live
,sum(total_viewing_duration-total_viewing_duration_live) as total_dur_playback
,count(distinct account_number) as accounts
from v249_summary_by_account_and_programme
group by epg_title
order by total_dur desc
;

select channel_name_inc_staggercast
,sum(total_viewing_duration) as total_dur
,sum(total_viewing_duration_3min_plus_events) as total_dur_3min_plus
,sum(total_viewing_duration_live) as total_dur_live
,sum(total_viewing_duration-total_viewing_duration_live) as total_dur_playback
,count(distinct account_number) as accounts
from v249_summary_by_account_and_programme
where channel_name='Showcase'
group by channel_name_inc_staggercast
order by channel_name_inc_staggercast
;
*/