--drop table #april2013;
select 
a.account_number
,cast(instance_start_date_time_utc as date) as viewing_date
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum( case  when dateformat(instance_start_date_time_utc,'HH') in ('00','01','02','03','04') then 0 
            when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_post_5am 
,sum(case when genre_description<>'Sports' then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_sports
into #april2013
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
and cast (instance_start_date_time_utc as date) between '2013-04-01' and '2013-04-30'
group by a.account_number
,viewing_date
;
commit;

select account_number
,sum(case when viewing_duration_post_5am>0 then 1 else 0 end) as days_returned
,sum(viewing_duration) as total_duration
,sum(viewing_duration_sports) as total_duration_sports
into v250_april_2013_account_viewing
from #april2013
group by account_number
having days_returned<=30
;

alter table v250_april_2013_account_viewing add analysis_account tinyint;


update v250_april_2013_account_viewing
set analysis_account=case when b.account_number is not null then 1 else 0 end
from v250_april_2013_account_viewing as a
left outer join dbarnett.v250_Account_profiling  as b
on a.account_number = b.account_number
;
commit;

--select top 100 * from v250_april_2013_account_viewing

select distinct subscriber_id from 

select log_start_date_time_utc
,min(subscriber_id) as sub_id_min
,max(subscriber_id) as sub_id_max
,min(instance_start_date_time_utc) as min_dt
,max(instance_start_date_time_utc) as max_dt

from  sk_prod.vespa_dp_prog_viewed_201304 where account_number = '210120655605'
group by log_start_date_time_utc
order by log_start_date_time_utc

select log_start_date_time_utc
,min(subscriber_id) as sub_id_min
,max(subscriber_id) as sub_id_max
,min(instance_start_date_time_utc) as min_dt
,max(instance_start_date_time_utc) as max_dt

from  sk_prod.vespa_dp_prog_viewed_201303 where account_number = '210120655605'
group by log_start_date_time_utc
order by log_start_date_time_utc








--select * from #acc_summary4;

select days_returned
,count(*) as accounts
,sum(total_duration) as tot_dur
,sum(total_duration_sports) as tot_dur_sports

from #acc_summary4
where days_returned>0
group by days_returned
order by days_returned
;

commit;

select case when b.account_number is not null then 1 else 0 end as weight_match
,days_returned
,count(a.account_number) as accounts
,sum(days_returned) as tot_days_returned
,sum(total_duration) as tot_dur
,sum(total_duration_sports) as tot_dur_sports
from #acc_summary4 as a
left outer join dbarnett.v250_Account_profiling  as b
on a.account_number = b.account_number
where days_returned>0 and days_returned<=30
group by weight_match,days_returned
order by weight_match,days_returned
;

select 
a.account_number
--,a.scaling_weighting
,cast(instance_start_date_time_utc as date) as viewing_date
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
,sum( case  when dateformat(instance_start_date_time_utc,'HH') in ('00','01','02','03','04') then 0 
            when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_post_5am 
,sum(case when genre_description<>'Sports' then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_sports
into #april2013_single_day
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc and duration>=180
and viewing_date='2013-04-16'
group by a.account_number
--,a.scaling_weighting
,viewing_date
;

commit;

select dt ,panel,sum(data_received) from vespa_analysts.panel_data where panel=12 group by dt,panel order by dt desc,panel desc
--select top 100 * from  sk_prod.vespa_dp_prog_viewed_201304

select * from vespa_analysts.panel_data where  subscriber_id = 24999175  order by dt desc,panel desc


select 
account_number
,subscriber_id
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,channel_name
,capped_full_flag
,capped_partial_flag
,case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end  as viewing_duration

--into #april2013_single_day
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast(instance_start_date_time_utc as date)='2013-04-01' and account_number ='210064464469'
order by subscriber_id,instance_start_date_time_utc

;

select * from #april2013_single_day;
commit;

select           account_number
    ,subscriber_id
                ,viewing_starts
                ,viewing_stops
                ,channel_name
                ,viewing_duration             as duration_instance                                  

                
from            vespa_analysts.vespa_daily_augs_20130401 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where          account_number ='210064464469'
order by       subscriber_id, viewing_starts
;
commit;


select           account_number
    ,subscriber_id
                ,viewing_starts
                ,viewing_stops
                ,channel_name
                ,viewing_duration             as duration_instance                                  

                
from            vespa_analysts.vespa_daily_augs_20130402 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where          account_number ='210064464469'
order by       subscriber_id, viewing_starts
;



select sum(viewing_duration) from vespa_analysts.vespa_daily_augs_20130401
where cast(viewing_starts as date) = '2013-04-01'


select sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast(instance_start_date_time_utc as date)='2013-04-01' 

commit;



select * from  sk_prod.vespa_dp_prog_viewed_201304 where account_number = '210120655605' order by instance_start_date_time_utc

----Channel Splits---


select           
                channel_name
                ,service_key
                
                ,sum(viewing_duration ) as tot_dur                              
                ,sum(case when timeshifting = 'LIVE' then viewing_duration else 0 end) as tot_dur_live                              

                
from            vespa_analysts.vespa_daily_augs_20130401 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
--where          account_number ='210064464469'
group by channel_name,service_key
order by channel_name,service_key
;


select 
channel_name,service_key
,sum(case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration

--into #april2013_single_day
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast(instance_start_date_time_utc as date)='2013-04-01' 
group by channel_name,service_key
order by channel_name,service_key

;
commit;