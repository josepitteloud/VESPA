
drop table #monthly_data_20130115;

select account_number
,service_key
,max(channel_name) as channel
,sum(case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_exc_null_capped_end_date
into #monthly_data_20130115
from  sk_prod.vespa_dp_prog_viewed_201301 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                           and capping_end_date_time_utc is not null -- only those records where the event has been given a capped event end time

and panel_id = 12  and capped_full_flag = 0
and cast (instance_start_date_time_utc as date) between '2013-01-15' and '2013-01-15'
--and account_number ='621432260563'

group by account_number
,service_key
;

select           account_number
                ,service_key
                ,sum(viewing_duration)             as duration_instance       
into #daily_data_20130115                                       
from            vespa_analysts.vespa_daily_augs_20130115 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
group by       account_number,service_key
;
commit;
--drop table #compare_20130115;
select a.*
,b.duration_instance
into #compare_20130115
from #monthly_data_20130115 as a
left outer join #daily_data_20130115 as b
on a.account_number = b.account_number and a.service_key=b.service_key
where b.duration_instance is not null
;
commit;

select top 500 * from #compare_20130115 order by duration_instance
select service_key,channel,sum(viewing_duration_where_cap_end_time_populated) as total_duration_monthly, sum(duration_instance) as total_duration_daily,sum(viewing_duration_where_cap_end_time_populated-duration_instance) as table_difference
from #compare_20130115
group by service_key,channel
order by table_difference desc  


select service_key,channel,sum(viewing_duration_where_cap_end_time_populated) as total_duration_monthly, sum(duration_instance) as total_duration_daily,sum(viewing_duration_where_cap_end_time_populated-duration_instance) as table_difference
from #compare_20130115
group by service_key,channel
order by table_difference desc  


select sum(duration_instance) from #daily_data_20130115 

select sum( viewing_duration_where_cap_end_time_populated)
from #monthly_data_20130115 
commit;

select count(*) from   sk_prod.vespa_dp_prog_viewed_20131;
select count(*) from vespa_analysts.vespa_daily_augs_20130115;