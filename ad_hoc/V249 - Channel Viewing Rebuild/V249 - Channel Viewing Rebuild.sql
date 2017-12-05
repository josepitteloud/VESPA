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


--Create Interim table---
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
,live_recorded
,service_key
,time_in_seconds_since_recording
,case when live_recorded = 'LIVE' then 'a) LIVE'
when time_in_seconds_since_recording <=900 then 'b) Within 15 min of Broadcast'
when time_in_seconds_since_recording <=3600 then 'c) Within 1 hour of Broadcast'

when dateformat(instance_start_date_time_utc,'YYYY-MM-DD-HH') between '2013-03-31-02' and '2013-10-27-02' 
and cast(dateadd(hh,-1,instance_start_date_time_utc)as date)  <=
cast (dateadd(hh,-1,broadcast_start_date_time_utc)as date ) 
then 'd) Over 60_minutes after tx VOSDAL'

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
and right(account_number,3)='097'
;
commit;

CREATE HG INDEX idx1 ON #program_summary (account_number);
CREATE HG INDEX idx2 ON #program_summary (service_key);

commit;

select top 5000 * from vespa_analysts.CHANNEL_MAP_prod_SERVICE_KEY_ATTRIBUTES order by full_name



---Part II - EPG Data---
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
when channel_name_inc_hd='Challenge +1' then 'Challenge'
when channel_name_inc_hd='Channel 5+1' then 'Channel 5'
when channel_name_inc_hd='Channel 4+1' then 'Channel 4'
when channel_name_inc_hd='Chart Show+1' then 'Chart Show TV'
when channel_name_inc_hd='Comedy Central +1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra +1' then 'Comedy Central Extra'
when channel_name_inc_hd='CI+1' then 'CI'
when channel_name_inc_hd='DMAX+1' then 'DMax'
when channel_name_inc_hd='DMax +2' then 'DMax'
when channel_name_inc_hd='Dave ja vu' then 'Dave'
when channel_name_inc_hd='Disc. History+1' then 'Disc. History'
when channel_name_inc_hd='Disc.Science +1' then 'Disc.Science'
when channel_name_inc_hd='Discovery +1hr' then 'Discovery'
when channel_name_inc_hd='Discovery RealTime +1' then 'Discovery RealTime'
when channel_name_inc_hd='Disney +1' then 'Disney'
when channel_name_inc_hd='Disney Cinemagic +1' then 'Disney Cinemagic'
when channel_name_inc_hd='Disney Junior+' then 'Playhouse Disney'
when channel_name_inc_hd='Disney XD +1' then 'Disney XD'
when channel_name_inc_hd='E4 +1' then 'E4'
when channel_name_inc_hd='Eden +1' then 'Eden'
when channel_name_inc_hd='FX +' then 'FX'
when channel_name_inc_hd='Film4 +1' then 'Film4'
when channel_name_inc_hd='Food Network+1' then 'Food Network'
when channel_name_inc_hd='GOLD +1' then 'GOLD  (TV)'
when channel_name_inc_hd='Good Food +1' then 'Good Food'
when channel_name_inc_hd='History +1 hour' then 'History'
when channel_name_inc_hd='Home & Health +1' then 'Home & Health'
when channel_name_inc_hd='Home+1' then 'Home'
when channel_name_inc_hd='ITV - ITV3+1' then 'ITV3'
when channel_name_inc_hd='ITV Channel Is' then 'ITV1'
when channel_name_inc_hd='ITV HD' then 'ITV1'
when channel_name_inc_hd='ITV1 Central SW' then 'ITV1'
when channel_name_inc_hd='ITV1+1' then 'ITV1'
when channel_name_inc_hd='ITV2+1' then 'ITV2'
when channel_name_inc_hd='ITV4+1' then 'ITV4'
when channel_name_inc_hd='MTV+1' then 'MTV'
when channel_name_inc_hd='More4 +1' then 'More4'
when channel_name_inc_hd='More4+2' then 'More4'
when channel_name_inc_hd='Movies 24 +' then 'Movies 24'
when channel_name_inc_hd='Nat Geo+1hr' then 'Nat Geo'
when channel_name_inc_hd='Nick Replay' then 'Nickelodeon'
when channel_name_inc_hd='N''Toons Replay' then 'Nicktoons TV'
when channel_name_inc_hd='Pick TV +1' then 'Pick TV'
when channel_name_inc_hd='PopGirl+1' then 'Pop Girl'
when channel_name_inc_hd='QUEST +1' then 'QUEST'
when channel_name_inc_hd='SONY TV +1' then 'SONY TV'
when channel_name_inc_hd='Showcase +1' then 'Showcase'
when channel_name_inc_hd='Sky Living +1' then 'Sky Living'
when channel_name_inc_hd='Sky Livingit +1' then 'Sky Livingit'
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
when channel_name_inc_hd='Nick Jr +1' then 'Nick Jr'
when channel_name_inc_hd='Nick Jr+1' then 'Nick Jr'

when channel_name_inc_hd In('Nickelodeon +1'
,'Nickelodeon EIRE'
,'Nickelodeon+1') then 'Nickelodeon'
when channel_name_inc_hd='RAMP''d +2' then 'RAMP''d +2'

when channel_name_inc_hd In('Sky Atlantic +1'
,'Sky Atlantic ROI'
,'Sky Atlantic+1') then 'Sky Atlantic'

when channel_name_inc_hd='Sky Premiere +1' then 'Sky Premiere'
when channel_name_inc_hd='Sky1 +1' then 'Sky1'
when channel_name_inc_hd='TLC +1' then 'TLC'
when channel_name_inc_hd='True Ent+1' then 'True Ent'
when channel_name_inc_hd='Watch+1' then 'Watch'
when channel_name_inc_hd='More 4 +1' then 'More 4'
when channel_name_inc_hd='YESTERDAY +1' then 'YeSTERDAY'


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
 else channel_name_inc_staggercast end
;


select channel_name_inc_hd,channel_name_inc_staggercast from v249_service_key_lookup_from_channel_map where timeshift_minutes>0 and vespa_name=channel_name_inc_staggercast




b.vespa_name
from v249_service_key_lookup_from_channel_map as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;

select * from channel_table;














--select count(*) from #program_summary;










select 
a.account_number
,dk_programme_instance_dim
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration_live
,sum( case  when capping_end_date_time_utc<instance_end_date_time_utc 
            then datediff(second,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) end) as viewing_duration



from  sk_prod.vespa_dp_prog_viewed_##^^*^*## as a
where capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
and right(account_number,3)='097'
group by a.account_number
,dk_programme_instance_dim
;

commit;

select top 10 cast(dateadd(hh,-1,instance_start_date_time_utc)as date)  from  sk_prod.vespa_dp_prog_viewed_201310 


select top 1000   
broadcast_start_date_time_utc
,instance_start_date_time_utc
,case
 when live_recorded = 'LIVE' then 'a) LIVE'
when   time_in_seconds_since_recording <=900 then 'b) Within 15 min of Broadcast'
when   time_in_seconds_since_recording <=3600 then 'c) Within 1 hour of Broadcast'

when dateformat(instance_start_date_time_utc,'YYYY-MM-DD-HH') between '2013-03-31-02' and '2013-10-27-02' 

and cast(dateadd(hh,-1,instance_start_date_time_utc)as date)  <=

cast (dateadd(hh,-1,broadcast_start_date_time_utc)as date ) 
then 'd) Over 60_minutes after tx VOSDAL'

 when dateformat(instance_start_date_time_utc,'YYYY-MM-DD-HH')  > '2013-10-27-02' 

and cast(dateadd(hh,-2,instance_start_date_time_utc)as date)  <=cast(dateadd(hh,-2,broadcast_start_date_time_utc)as date) 
then 'd) Over 60 minutes after tx VOSDAL'  
when instance_start_date_time_utc <= dateadd(hour, 170, cast(broadcast_start_date_time_utc as datetime)) then 'e) Playback within 7 days exc VOSDAL'
when instance_start_date_time_utc > dateadd(hour, 170, cast(broadcast_start_date_time_utc as datetime))  then 'f) Playback over 7 days after tx'
end as viewing_type

,time_in_seconds_since_recording
from  sk_prod.vespa_dp_prog_viewed_201310
where  dateformat(instance_start_date_time_utc,'DD-HH') in ('29-01','29-02')
commit;


,case 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc)
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2013-03-31-02' and '2013-10-27-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2014-03-30-02' and '2014-10-26-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 



                    else non_staggercast_broadcast_time_utc  end as non_staggercast_broadcast_time_local

