

---V180 Sample code of second by second analysis

---Top 50 Programmes by Day - Second by Second

--
/*
select epg.pk_programme_instance_dim
,
     epg.Channel_Name
,
     epg.programme_instance_name
,
     epg.programme_instance_duration
,
     epg.Genre_Description
,
     epg.Sub_Genre_Description
,
     epg.broadcast_start_date_time_utc
,
     epg.broadcast_end_date_time_utc
,
     epg.broadcast_daypart
,
     epg.service_type_description
,synopsis
FROM   sk_prod.Vespa_programme_schedule as epg
where upper(programme_instance_name)  like '%MONDAY NIGHT FOOTBALL%' and broadcast_start_date_time_utc >='2013-05-01'
  order by broadcast_start_date_time_utc
;
*/
commit;
--drop table #mnf_sk;
select epg.pk_programme_instance_dim as programme_trans_sk 
,channel_name
into #mnf_sk
FROM   sk_prod.Vespa_programme_schedule as epg
where upper(programme_instance_name)  like '%MONDAY NIGHT FOOTBALL%' and broadcast_start_date_time_utc >='2013-05-01'
and broadcast_start_date_time_utc= '2013-05-06 18:00:00'
 order by broadcast_start_date_time_utc
;
commit;
CREATE UNIQUE INDEX idx1 ON #mnf_sk (programme_trans_sk);
---Get all recpords with Matching SK---
SELECT cb_row_ID
,Account_Number
,Subscriber_Id
,a.programme_trans_sk
,timeshifting
,viewing_starts
,viewing_stops
,viewing_Duration
,capped_flag
,capped_event_end_time
,Scaling_Segment_Id
,Scaling_Weighting     
,b.Channel_name
into mnf_20130506_all_viewing
FROM vespa_analysts.VESPA_DAILY_AUGS_20130506 a
inner JOIN #mnf_sk b
         ON a.programme_trans_sk = b.programme_trans_sk
--where upper(timeshifting)='LIVE'
;

commit;
CREATE INDEX idx1 ON mnf_20130506_all_viewing (viewing_starts);
CREATE INDEX idx2 ON mnf_20130506_all_viewing (viewing_stops);
create hg index idx3 on mnf_20130506_all_viewing(cb_row_id);
--Time in seconds since recording----


commit;

alter table mnf_20130506_all_viewing add time_in_seconds_since_recording bigint default 0;


update mnf_20130506_all_viewing
set time_in_seconds_since_recording=b.time_in_seconds_since_recording
from mnf_20130506_all_viewing as a
left outer join sk_prod.vespa_events_all as b
on a.cb_row_id=b.pk_viewing_prog_instance_fact
;
commit;

--Create Combined Live/Plack View of period watched---
alter table mnf_20130506_all_viewing add viewing_starts_live_playback datetime;
alter table mnf_20130506_all_viewing add viewing_stops_live_playback datetime;

update mnf_20130506_all_viewing
set viewing_starts_live_playback=case when time_in_seconds_since_recording=0 then viewing_starts else dateadd(second,time_in_seconds_since_recording*-1,viewing_starts) end
,viewing_stops_live_playback=case when time_in_seconds_since_recording=0 then viewing_stops else dateadd(second,time_in_seconds_since_recording*-1,viewing_stops) end
from mnf_20130506_all_viewing as a
;
commit;




--select capped_flag , count(*),sum(viewing_Duration),avg(viewing_duration)  from #all_live_viewing group by capped_flag

--select viewing_Duration , count(*) as records,sum(case when capped_flag=2 then 1 else 0 end) as capped  from #all_live_viewing group by viewing_Duration order by viewing_Duration;


--select count(*) from  vespa_analysts.VESPA_DAILY_AUGS_20130506 a
--select top 100 * from  vespa_analysts.VESPA_DAILY_AUGS_20130506 a


---Create second by second log---
create variable @programme_time_start datetime;
create variable @programme_time_end datetime;
create variable @programme_time datetime;

set @programme_time_start = cast('2013-05-06 18:00:00' as datetime);  --Rerun as loop crashed part way through
--set @programme_time_start = cast('2013-05-06 18:00:00' as datetime);
set @programme_time_end =cast('2013-05-06 22:00:00' as datetime);
set @programme_time = @programme_time_start;

commit;
--drop table MNF_second_by_secondv2;
---Create table to insert into loop---
create table MNF_second_by_secondv2
(second_viewed                      datetime                
,viewed                             integer              

,weighted_viewed                             float               
,weighted_viewed_live                             float               
,weighted_viewed_playback                             float               
);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into MNF_second_by_secondv2
select @programme_time as second_viewed
,count(distinct account_number) as viewed
,sum(scaling_weighting) as weighted_viewed
,sum(case when timeshifting = 'LIVE' then scaling_weighting else 0 end) as weighted_viewed_live
,sum(case when timeshifting <> 'LIVE' then scaling_weighting else 0 end) as weighted_viewed_playback
from mnf_20130506_all_viewing
where  cast(viewing_starts as datetime)<=@programme_time and cast(viewing_stops as datetime)>@programme_time
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;

--select top 5000 * from MNF_second_by_secondv2;


select second_viewed
,sum(weighted_viewed) as weighted
,sum(weighted_viewed_live)
,sum( weighted_viewed_playback)
 from MNF_second_by_secondv2
group by second_viewed
order by second_viewed;


























