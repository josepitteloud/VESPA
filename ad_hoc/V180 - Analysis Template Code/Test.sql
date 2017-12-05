

----V180 Sample Code of Second by Second Analysis of a Programme



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
into #all_live_viewing
FROM vespa_analysts.VESPA_DAILY_AUGS_20130506 a
inner JOIN #mnf_sk b
         ON a.programme_trans_sk = b.programme_trans_sk
where upper(timeshifting)='LIVE'
;

commit;
CREATE INDEX idx1 ON #all_live_viewing (viewing_starts);
CREATE INDEX idx2 ON #all_live_viewing (viewing_stops);

--select capped_flag , count(*),sum(viewing_Duration),avg(viewing_duration)  from #all_live_viewing group by capped_flag

--select viewing_Duration , count(*) as records,sum(case when capped_flag=2 then 1 else 0 end) as capped  from #all_live_viewing group by viewing_Duration order by viewing_Duration;


--select count(*) from  vespa_analysts.VESPA_DAILY_AUGS_20130506 a
--select top 100 * from  vespa_analysts.VESPA_DAILY_AUGS_20130506 a


---Create second by second log---
create variable @programme_time_start datetime;
create variable @programme_time_end datetime;
create variable @programme_time datetime;

set @programme_time_start = cast('2013-05-06 18:41:38' as datetime);
--set @programme_time_start = cast('2013-05-06 18:00:00' as datetime);
set @programme_time_end =cast('2013-05-06 22:00:00' as datetime);
set @programme_time = @programme_time_start;

commit;
--drop table MNF_second_by_second;
---Create table to insert into loop---
create table MNF_second_by_second
(second_viewed                      datetime                
,viewed                             integer              
,weighted_viewed                             float               
);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into MNF_second_by_second
select @programme_time as second_viewed
,count(distinct account_number) as viewed
,sum(scaling_weighting) as weighted_viewed
from #all_live_viewing
where  cast(viewing_starts as datetime)<=@programme_time and cast(viewing_stops as datetime)>@programme_time
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;

--select top 5000 * from MNF_second_by_second;


select second_viewed
,sum(weighted_viewed) as weighted
 from MNF_second_by_second
group by second_viewed
order by second_viewed;





















