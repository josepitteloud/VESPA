
drop table #april2013_monthly;
drop table dbarnett.v250_compare_tables;
drop table #daily_20130401     ;
commit;
select 
a.account_number
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum(case when cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date) >0 then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_within_pre_tx
,sum(case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_exc_null_capped_end_date
into #april2013_monthly
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where  capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                           and capping_end_date_time_utc is not null -- only those records where the event has been given a capped event end time

and panel_id = 12  and capped_full_flag = 0
and cast (instance_start_date_time_utc as date) between '2013-04-15' and '2013-04-15'

group by a.account_number
;
commit;

--select sum(viewing_duration_exc_null_capped_end_date) from  #april2013_monthly


select           account_number
                ,sum(viewing_duration)             as total_duration_daily                                  
into #daily_20130401                
from            vespa_analysts.vespa_daily_augs_20130415 as a
where cast(viewing_starts as date) = '2013-04-15'
group by account_number
;

--Compare

select a.*
,b.total_duration_daily
into dbarnett.v250_compare_tables
from #april2013_monthly as a
left outer join #daily_20130401  as b
on a.account_number = b.account_number
;
commit;

---
alter table dbarnett.v250_compare_tables add viewing_difference integer;

update dbarnett.v250_compare_tables
set viewing_difference=viewing_duration_exc_null_capped_end_date-total_duration_daily
from dbarnett.v250_compare_tables
;
commit;

--drop table #april2013_monthly_old_version;
select 
a.account_number
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum(case when type_of_viewing_event not in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing') then 0  when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_exc_non_viewing_events
,sum(case when type_of_viewing_event not in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing') then 0 
          when  capping_end_date_time_utc is null then 0  when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_exc_non_viewing_events_exc_null_capped_end
 
into #april2013_monthly_old_version
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where   panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast (instance_start_date_time_utc as date) between '2013-04-15' and '2013-04-15'

group by a.account_number
;
commit;


select sum(viewing_duration)
,sum(viewing_duration_exc_non_viewing_events)
,sum(viewing_duration_exc_non_viewing_events_exc_null_capped_end)

from #april2013_monthly_old_version;



alter table dbarnett.v250_compare_tables add viewing_duration_old_version integer;
alter table dbarnett.v250_compare_tables add viewing_difference_old_version integer;

update dbarnett.v250_compare_tables
set viewing_difference_old_version=b.viewing_duration-total_duration_daily
,viewing_duration_old_version=b.viewing_duration
from dbarnett.v250_compare_tables as a
left outer join #april2013_monthly_old_version as b
on a.account_number = b.account_number
;
commit;

--select sum(viewing_difference_old_version) from dbarnett.v250_compare_tables



--select sum(viewing_duration),sum(total_duration_daily) from dbarnett.v250_compare_tables


--select * from dbarnett.v250_compare_tables where account_number ='210064464469';

select round(viewing_difference/60,1) as mins
,count(*)
,sum(viewing_difference) as total_difference
from dbarnett.v250_compare_tables
group by mins
order by mins
;


select round(viewing_difference_old_version/60,1) as mins
,count(*)
,sum(viewing_difference_old_version) as total_difference
from dbarnett.v250_compare_tables
group by mins
order by mins
;







select viewing_duration_exc_null_capped_end_date-total_duration_daily as diff , * from dbarnett.v250_compare_tables order by diff desc;


----Account Tests----

select           account_number
    ,subscriber_id
                ,viewing_starts
                ,viewing_stops
                ,channel_name
                ,viewing_duration             as duration_instance                                              
from            vespa_analysts.vespa_daily_augs_20130401 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where          account_number ='220011629783'
order by       subscriber_id, viewing_starts
;
commit;

select 
account_number
,subscriber_id
,instance_start_date_time_utc
,instance_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,capping_end_date_time_utc
,channel_name
,capped_full_flag
,capped_partial_flag
,case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end  as viewing_duration_exc_null_capped_end_date
,cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date)  as days_to_tx
--into #april2013_single_day
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast (instance_start_date_time_utc as date) between '2013-04-15' and '2013-04-15'
and account_number is not null   and subscriber_id is not null
  and (reported_playback_speed is null or reported_playback_speed = 2) and 
(
    (Service_type_description='Digital TV channel' and Type_of_viewing_event='TV Channel Viewing') or
    (Service_type_description='High Definition TV test service' and Type_of_viewing_event='HD Viewing Event') or
    (Service_type_description='Digital TV channel' and Type_of_viewing_event='Sky+ time-shifted viewing event') or
    (Service_type_description='High Definition TV test service' and Type_of_viewing_event='Sky+ time-shifted viewing event') or
    (Service_type_description='NVOD service' and Type_of_viewing_event='Sky+ time-shifted viewing event')
)
and cast(instance_start_date_time_utc as date)='2013-04-15' and account_number ='621432260563'
order by subscriber_id,instance_start_date_time_utc

;

commit;









select cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date) as days_to_log_received
,count(*) as records
,sum(capped_full_flag) as capped_full
,sum(capped_partial_flag) as capped_partial
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum(case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_where_cap_end_time_populated
,count(distinct account_number) as accounts
,sum(case when instance_start_date_time_utc < instance_end_date_time_utc  then 1 else 0 end) as start_before_end
from  sk_prod.vespa_dp_prog_viewed_201310 as a
where panel_id = 12  
--and instance_start_date_time_utc < instance_end_date_time_utc 
 and capped_full_flag=0
group by days_to_log_received
order by days_to_log_received


select top 5000  cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date) as days_to_log_received
,case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end  as viewing_duration
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,capped_partial_flag
,programme_name
,broadcast_start_time_local
from  sk_prod.vespa_dp_prog_viewed_201311 as a
where panel_id = 12  and days_to_log_received<-1
--and instance_start_date_time_utc < instance_end_date_time_utc 
group by days_to_log_received
order by days_to_log_received








select top 100 * from sk_prod.vespa_dp_prog_viewed_201304 where account_number ='210064464469'



select 
case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end  as viewing_duration
,cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date)  as days_to_tx
,count(*) as records
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as total_viewing_duration
into #v250_20130401_monthly_table
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast(instance_start_date_time_utc as date)='2013-04-01' 
group by viewing_duration,days_to_tx
;


select round(viewing_duration/60,0)*60 as minutes_
,sum(records) as total_records
,sum(total_viewing_duration) from #v250_20130401_monthly_table
group by minutes_
order by minutes_




select round(viewing_duration/60,0)*60 as minutes_
,count(*) as records
,sum(viewing_duration) from vespa_analysts.vespa_daily_augs_20130401
group by minutes_
order by minutes_


commit;




select top 500 account_number
,channel_name
,log_received_start_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,capped_partial_flag
from  sk_prod.vespa_dp_prog_viewed_201301
where capped_partial_flag=1 and panel_id = 12  
and instance_start_date_time_utc is null










select 
account_number
,subscriber_id
,log_received_start_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,channel_name
,capped_full_flag
,capped_partial_flag
,case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end  as viewing_duration
,cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date)  as days_to_tx
,programme_instance_name
,broadcast_start_date_time_utc
--into #april2013_single_day
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast(instance_start_date_time_utc as date)='2013-04-01' and account_number ='220011629783'
order by subscriber_id,instance_start_date_time_utc

;




select account_number
,subscriber_id
,event_start_date_time_utc
,capping_end_date_time_utc
,channel_name
,count(*) as records
,sum(case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     end)  as viewing_duration_where_cap_end_time_populated
into #april2013_single_day_test
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast (instance_start_date_time_utc as date) between '2013-04-15' and '2013-04-15'
and account_number is not null   and subscriber_id is not null
  and (reported_playback_speed is null or reported_playback_speed = 2) and 
(
    (Service_type_description='Digital TV channel' and Type_of_viewing_event='TV Channel Viewing') or
    (Service_type_description='High Definition TV test service' and Type_of_viewing_event='HD Viewing Event') or
    (Service_type_description='Digital TV channel' and Type_of_viewing_event='Sky+ time-shifted viewing event') or
    (Service_type_description='High Definition TV test service' and Type_of_viewing_event='Sky+ time-shifted viewing event') or
    (Service_type_description='NVOD service' and Type_of_viewing_event='Sky+ time-shifted viewing event')
)
and cast(instance_start_date_time_utc as date)='2013-04-15' 
--and account_number ='621432260563'

group by account_number
,subscriber_id
,event_start_date_time_utc
,capping_end_date_time_utc
,channel_name
;





select           sum(viewing_duration)             as total_duration_daily                                  
into #daily_20130401                
from            vespa_analysts.vespa_daily_augs_20130415 as a
where cast(viewing_starts as date) = '2013-04-15'
;
select * from #daily_20130401   ;


-----Comparison by Channel-----

drop table #april2013_single_day_test_by_channel;
select account_number
,service_key
,max(channel_name) as channel
,sum(case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     end)  as viewing_duration_where_cap_end_time_populated
into #april2013_single_day_test_by_channel
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast (instance_start_date_time_utc as date) between '2013-04-15' and '2013-04-15'
and account_number is not null   and subscriber_id is not null
  and (reported_playback_speed is null or reported_playback_speed = 2) and 
(
    (Service_type_description='Digital TV channel' and Type_of_viewing_event='TV Channel Viewing') or
    (Service_type_description='High Definition TV test service' and Type_of_viewing_event='HD Viewing Event') or
    (Service_type_description='Digital TV channel' and Type_of_viewing_event='Sky+ time-shifted viewing event') or
    (Service_type_description='High Definition TV test service' and Type_of_viewing_event='Sky+ time-shifted viewing event') or
    (Service_type_description='NVOD service' and Type_of_viewing_event='Sky+ time-shifted viewing event')
)
and cast(instance_start_date_time_utc as date)='2013-04-15' 
--and account_number ='621432260563'

group by account_number
,service_key
;

drop table #daily_summary_by_channel  ;
select           account_number
                ,service_key
                ,sum(viewing_duration)             as duration_instance       
into #daily_summary_by_channel                                       
from            vespa_analysts.vespa_daily_augs_20130415 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
group by       account_number,service_key
;
commit;

select a.*
,b.duration_instance
into #compare_tables_by_service_key
from #april2013_single_day_test_by_channel as a
left outer join #daily_summary_by_channel  as b
on a.account_number = b.account_number and a.service_key=b.service_key
where b.duration_instance is not null
;
commit;


select top 100 * from #compare_tables_by_service_key;

select service_key,channel,sum(viewing_duration_where_cap_end_time_populated) as total_duration_monthly,sum(viewing_duration_where_cap_end_time_populated-duration_instance) as table_difference
from #compare_tables_by_service_key
group by service_key,channel
order by table_difference desc





------Apr 15th by channel

--drop table #monthly_data_20130415;

select account_number
,service_key
,max(channel_name) as channel
,sum(case when capping_end_date_time_utc is null then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_exc_null_capped_end_date
into #monthly_data_20130415
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                           and capping_end_date_time_utc is not null -- only those records where the event has been given a capped event end time

and panel_id = 12  and capped_full_flag = 0
and cast (instance_start_date_time_utc as date) between '2013-04-15' and '2013-04-15'
--and account_number ='621432260563'

group by account_number
,service_key
;
--select sum(viewing_duration_exc_null_capped_end_date) from #monthly_data_20130415



select           account_number
                ,service_key
                ,sum(viewing_duration)             as duration_instance       
into #daily_data_20130415                                       
from            vespa_analysts.vespa_daily_augs_20130415 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
group by       account_number,service_key
;
commit;

--drop table  #compare_20130415;
select a.*
,b.duration_instance
into #compare_20130415
from #monthly_data_20130415 as a
left outer join #daily_data_20130415 as b
on a.account_number = b.account_number and a.service_key=b.service_key
where b.duration_instance is not null
;
commit;


select service_key,channel,sum(viewing_duration_exc_null_capped_end_date) as total_duration_monthly, sum(duration_instance) as total_duration_daily,sum(viewing_duration_exc_null_capped_end_date-duration_instance) as table_difference
from #compare_20130415
group by service_key,channel
order by table_difference desc

select account_number
,sum(viewing_duration_exc_null_capped_end_date) as total_duration_monthly
, sum(duration_instance) as total_duration_daily
into #compare_20130415_by_account
from #compare_20130415
group by account_number
;


select round((total_duration_monthly-total_duration_daily)/60,0) as mins
,count(*) as accounts
,sum(total_duration_monthly-total_duration_daily) as total_difference
from #compare_20130415_by_account
group by mins
order by mins
;