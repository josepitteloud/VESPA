



select cast(recorded_time_utc as date) as recorded_date
,count(*) as records
,sum(x_programme_viewed_duration) as seconds_viewed
,count(distinct subscriber_id) as boxes
from sk_prod.VESPA_STB_PROG_EVENTS_20120510
where play_back_speed =2
group by recorded_date
order by recorded_date
;

select count(*) as records
,sum(x_programme_viewed_duration) as seconds_viewed
,count(distinct subscriber_id) as boxes
from sk_prod.VESPA_STB_PROG_EVENTS_20120510
where play_back_speed =2 and recorded_time_utc between '2001-01-01' and '2011-11-10'
;

select x_epg_title
,x_channel_name
,count(*) as records
,sum(x_programme_viewed_duration) as seconds_viewed
,count(distinct subscriber_id) as boxes
from sk_prod.VESPA_STB_PROG_EVENTS_20120510
where play_back_speed =2 and recorded_time_utc between '2001-01-01' and '2011-11-10'
group by x_epg_title
,x_channel_name
order by boxes desc
;


select x_epg_title
,x_channel_name
,count(*) as records
,sum(x_programme_viewed_duration) as seconds_viewed
,count(distinct subscriber_id) as boxes
from sk_prod.VESPA_STB_PROG_EVENTS_20120512
where play_back_speed =2 and recorded_time_utc between '2001-01-01' and '2011-11-12'
group by x_epg_title
,x_channel_name
order by boxes desc
;



commit;
select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120510

select tx_date , count(*) as records from sk_prod.vespa_epg_dim group by tx_date order by tx_date
