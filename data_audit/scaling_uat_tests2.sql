select 1 as Seq, 'Events' as Metric, count(distinct subscriberid||date(STBLOGCREATIONDATE)) as Cnt from capped_events where panelid=12 and DATEOFEVENT = '2012-11-08' and documentcreationdate <= '2012-11-09 02:00:00'
--21,683,291

union all

select 2 as Seq, 'Empty logs' as Metric, count(distinct subscriberid||date('2012-11-10')) as Cnt
  from NORMALIZED_EMPTY_LOGS
 where panelid = 12
   and documentcreationdate <  '2012-11-09 02:00:00'
   and documentcreationdate >= '2012-11-08'
   and stblogcreationdate   <  '2012-11-08'
--38,877

union all

select 3 as Seq, 'Combined' as Metric, count(distinct scms_subscriber_id||date(ADJUSTED_EVENT_START_DATE_VESPA)) as Cnt from td_scaling_events_0

union all

select 4 as Seq, 'Events (combined)' as Metric, count(distinct scms_subscriber_id||date(ADJUSTED_EVENT_START_DATE_VESPA)) as Cnt from td_scaling_events_0 where viewing_event_id is not null
--21,680,453

union all

select 5 as Seq, 'Empty logs (combined)' as Metric ,count(distinct scms_subscriber_id||date(ADJUSTED_EVENT_START_DATE_VESPA)) as Cnt from td_scaling_events_0 where viewing_event_id is null;
--38,823



select 5 as Seq, 'Empty logs (combined)' as Metric ,count(distinct scms_subscriber_id||date(ADJUSTED_EVENT_START_DATE_VESPA)) as Cnt from td_scaling_events_0 where viewing_event_id is null;






select '20120201' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120201 union all
select '20120202' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120202 union all
select '20120203' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120203 union all
select '20120204' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120204 union all
select '20120205' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120205 union all
select '20120206' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120206 union all
select '20120207' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120207 union all
select '20120208' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120208 union all
select '20120209' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120209 union all
select '20120210' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120210 union all
select '20120211' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120211 union all
select '20120212' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120212 union all
select '20120213' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120213 union all
select '20120214' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120214 union all
select '20120215' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120215 union all
select '20120216' as Dt, count(*) as Cnt, sum(case when event_type = 'evEmptyLog' then 1 else 0 end) as Empty_Logs from sk_prod.VESPA_STB_PROG_EVENTS_20120216

select count(*) as Cnt, sum(case when viewing_event_id is null then 1 else 0 end) as Empty_Logs from td_scaling_events_0;




select * from ADMIN.CAPPED_EVENTS       limit 100;
select * from ADMIN.NORMALIZED_EMPTY_LOGS       limit 100;
select * from ADMIN.td_scaling_events_0 limit 100;




select viewing_event_id
from capped_events where panelid=12
and DATEOFEVENT = '2012-11-08' order by viewing_event_id desc
limit 10


select * from td_scaling_events_0 where VIEWING_EVENT_ID in
(
9223371730032826993
,9223370984395621199
,9223367650382755153
,9223366696853339663
,9223365399545355084
,9223365343395583080
,9223364530923813354
,9223364386578788328
,9223364374098973625
,9223363250535221782
)

select viewing_event_id
from td_scaling_events_0
where VIEWING_EVENT_ID is not null
order by VIEWING_EVENT_ID desc
limit 10


select viewing_event_id
from capped_events where panelid=12
and DATEOFEVENT = '2012-11-08'
and VIEWING_EVENT_ID in (
9223371730032826993
,9223370984395621199
,9223367650382755153
,9223366696853339663
,9223365399545355084
,9223365343395583080
,9223364530923813354
,9223364386578788328
,9223364374098973625
,9223363250535221782
)

select * from NORMALIZED_EMPTY_LOGS
where panelid=12
limit 100

select count(1)
,count(distinct subscriberid)
,date(evemptylog_et) as dt
from NORMALIZED_EMPTY_LOGS
where panelid=12
group by dt
--16,579

select subscriberid
from NORMALIZED_EMPTY_LOGS
where panelid=12
and date(evemptylog_et) = '2012-11-08'
order by subscriberid
limit 10

select *
from td_scaling_events_0
--where viewing_event_id is null
where scms_subscriber_id in (
16698--
,16879--
,17071--
,19718
,20787
,22065
,23971--x
,24723
,25588
,27127
)








select count(1) from capped_events where panelid=12 and DATEOFEVENT = '2012-11-08'
select adjusted_event_start_date_vespa as dt,count(1) from td_scaling_events_0 group by dt


select *
  from NORMALIZED_EMPTY_LOGS
 where panelid = 12
   and documentcreationdate <  '2012-11-09 02:00:00'
   and documentcreationdate >= '2012-11-08'
   and stblogcreationdate   <  '2012-11-08'
limit 100
--38,877

select * from td_scaling_events_0 where viewing_event_id is null
limt 100


with t1(subscriberid) as
(select subscriberid
  from NORMALIZED_EMPTY_LOGS
 where panelid = 12
   and documentcreationdate <  '2012-11-09 02:00:00'
   and documentcreationdate >= '2012-11-08'
   and stblogcreationdate   <  '2012-11-08')
,t2(subscriberid) as
(select scms_subscriber_id from td_scaling_events_0 where viewing_event_id is null)
select t1.subscriberid from t1 left join t2 on t1.subscriberid=t2.subscriberid where t2.subscriberid is null limit 100
--0

select count(distinct subscriberid)
  from NORMALIZED_EMPTY_LOGS
 where panelid = 12
   and documentcreationdate <  '2012-11-09 02:00:00'
   and documentcreationdate >= '2012-11-08'
   and stblogcreationdate   <  '2012-11-08'
--33,478



6553148

select top 10 * from kinnairt.Capping2_01_Viewing_Records;
select count(1) from kinnairt.vespa_daily_augs_20121108


6551649


sp_iqcontext






