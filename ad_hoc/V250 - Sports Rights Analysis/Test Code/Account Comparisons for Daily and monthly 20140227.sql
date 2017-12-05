
drop table #april2013_monthly;
drop table dbarnett.v250_compare_tables;
drop table #daily_20130401     ;
commit;
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
,sum(case when cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date) >3 then 0 when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end)  as viewing_duration_within_tx_3
into #april2013_monthly
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast (instance_start_date_time_utc as date) between '2013-04-01' and '2013-04-01'
group by a.account_number
,viewing_date
;
commit;


select           account_number
    
                ,sum(viewing_duration)             as total_duration_daily                                  

into #daily_20130401                
from            vespa_analysts.vespa_daily_augs_20130401 as a
where cast(viewing_starts as date) = '2013-04-01'
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

select * from dbarnett.v250_compare_tables where account_number ='210064464469';



select viewing_duration-total_duration_daily as diff , * from dbarnett.v250_compare_tables order by diff desc;













select           
                channel_name
                ,service_key
                
                ,sum(viewing_duration ) as tot_dur                              
                ,sum(case when timeshifting = 'LIVE' then viewing_duration else 0 end) as tot_dur_live                              

                
from            vespa_analysts.vespa_daily_augs_20121220 as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
where         service_key=4002
group by channel_name,service_key
order by channel_name,service_key
;

commit;


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
,capping_end_date_time_utc
,event_end_date_time_utc
,channel_name
,capped_full_flag
,capped_partial_flag
,case when a.capped_partial_flag = 1 then datediff(second, a.instance_start_date_time_utc, a.capping_end_date_time_utc)
     else datediff(second, a.instance_start_date_time_utc, a.instance_end_date_time_utc)
     end  as viewing_duration
,cast(log_received_start_date_time_utc as date) - cast( instance_start_date_time_utc as date)  as days_to_tx
--into #april2013_single_day
from  sk_prod.vespa_dp_prog_viewed_201304 as a
where panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc 
and cast(instance_start_date_time_utc as date)='2013-04-01' and account_number ='220011629783'
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
,count(distinct account_number) as accounts
,sum(case when instance_start_date_time_utc < instance_end_date_time_utc  then 1 else 0 end) as start_before_end
from  sk_prod.vespa_dp_prog_viewed_201311 as a
where panel_id = 12  
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




