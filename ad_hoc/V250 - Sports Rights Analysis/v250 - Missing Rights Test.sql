

select top 100 *   from dbarnett.v250_all_sports_programmes_viewed_deduped

commit;



select  dk_programme_instance_dim
,broadcast_datetime
,analysis_right_new
into #details
 from 
dbarnett.v250_sports_rights_epg_data_for_analysis as b

--where 
--analysis_right_new in (
--'Premier League Football - Sky Sports (Sun 4pm)')
--and live = 1

group by dk_programme_instance_dim
,broadcast_datetime
,analysis_right_new
order by broadcast_datetime
;
commit;
--drop table #dur_by_prog
select a.broadcast_datetime
,analysis_right_new
,sum(viewing_duration_total) as tot_dur
into #dur_by_prog
from #details as a
left outer join dbarnett.v250_all_sports_programmes_viewed_deduped as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.broadcast_datetime
,analysis_right_new
;
commit;
select * from #dur_by_prog order by analysis_right_new, broadcast_datetime;

output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\Missing Data Analysis\duration_by_right_details.csv' format ascii;
commit;

select distinct analysis_right_new
 from 
dbarnett.v250_sports_rights_epg_data_for_analysis
order by analysis_right_new


commit;
---Repeat using the new data

select a.broadcast_datetime
,analysis_right_new
,sum(viewing_duration) as tot_dur
into #dur_by_prog_viewed
from #details as a
left outer join dbarnett.v250_all_sports_programmes_viewed as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by a.broadcast_datetime
,analysis_right_new
;

select * from #dur_by_prog_viewed where analysis_right_new = 'Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' order by analysis_right_new, broadcast_datetime;

select cast(broadcast_start_date_time_local as date) as bcast_date
,count(*) as records

from dbarnett.v250_all_sports_programmes_viewed as a
left outer join sk_prod.Vespa_programme_schedule as b
on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by bcast_date
order by bcast_date



select count(distinct account_number) from  vespa_analysts.VESPA_DAILY_AUGS_20121226


select dk_programme_instance_dim,service_key,channel_name from sk_prod.Vespa_programme_schedule 
where left(channel_name,12)='Sky Sports 1' and broadcast_start_date_time_local='2012-12-11 19:30:00'
order by service_key


select dk_programme_instance_dim,service_key,channel_name from sk_prod.Vespa_programme_schedule 
where left(channel_name,12)='Sky Sports 1' and broadcast_start_date_time_local='2013-01-22 19:30:00'
order by service_key


select dk_programme_instance_dim,service_key,channel_name from sk_prod.Vespa_programme_schedule 
where 
left(channel_name,3)='ITV' and 
broadcast_start_date_time_local='2013-01-27 16:30:00'
order by service_key


dk_programme_instance_dim,service_key,channel_name
560653805,4002,'Sky Sports 1'
534638121,4002,'Sky Sports 1 HD'
 

select *
from 
dbarnett.v250_sports_rights_epg_data_for_analysis
where broadcast_datetime_text='2013-01-22 19:30:00'

commit;
---select * from  Augs_Tables_Dates_Available;


----Get 


select dk_programme_instance_dim,service_key,channel_name from sk_prod.Vespa_programme_schedule 
where 
left(channel_name,12)='Sky Sports 2' and 
broadcast_start_date_time_local='2013-01-12 17:00:00'
order by service_key

commit;
select * from dbarnett.v250_sports_rights_epg_data_for_analysis where channel_name = 'Sky Sports 2' and broadcast_start_date_time_local='2013-01-12 17:00:00'


select dk_programme_instance_dim,service_key,channel_name from sk_prod.Vespa_programme_schedule 
where 
left(channel_name,12)='Sky Sports 2' and 
broadcast_start_date_time_local='2013-01-12 17:00:00'



select 
service_key
,dk_programme_instance_dim
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,count(*) as viewing_events
from  sk_prod.vespa_dp_prog_viewed_201301 as a
where  panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
and left(channel_name,12)='Sky Sports 2' and 
broadcast_start_date_time_utc='2013-01-12 17:00:00'
group by 
service_key
,dk_programme_instance_dim
;

--select top 100 * from   sk_prod.vespa_dp_prog_viewed_201301;

select * into #test2 from dbarnett.v250_loop_counter02 

select * from #test2






