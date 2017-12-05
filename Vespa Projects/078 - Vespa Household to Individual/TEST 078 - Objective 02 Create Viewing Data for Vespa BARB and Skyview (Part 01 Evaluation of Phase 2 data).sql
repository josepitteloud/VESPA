

---Project 078 - Vespa Household to Individual---

---Objective 2 - Compare Vespa, BARB and Skyview data for Single Demographic Households---

select top 100 * from sk_prod.VESPA_PROGRAMMES_VIEWED_DAILY;
select top 100 * from sk_prod.VESPA_PROGRAMMES_VIEWED_WEEKLY;

select top 100 * from sk_prod.VESPA_EVENTS_VIEWED_ALL;


select 
account_number 
, subscriber_id
,src_system_id
,panel_id
,channel_name
,programme_name
,live_recorded
,video_playing_flag
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc

,duration
,duration_since_last_viewing_event
,time_in_seconds_since_recording

from sk_prod.VESPA_EVENTS_VIEWED_ALL
where account_number ='220005153279'
order by event_start_date_time_utc
,instance_start_date_time_utc;

---Comapre With Old data version---
select
       vw.Account_Number,vw.Subscriber_Id,src_system_id,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,prog.Channel_Name,prog.Epg_Title
              

 from sk_prod.VESPA_STB_PROG_EVENTS_20120612 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
     account_number ='220005153279' and (play_back_speed is null or play_back_speed =2) and event_type = 'evChangeView'
order by vw.Adjusted_Event_Start_Time,vw.X_Viewing_Start_Time
 ;


---Look at All Events----
select 
account_number 
, subscriber_id
,src_system_id
,panel_id
,channel_name
,programme_name
,live_recorded
,video_playing_flag
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc

,duration
,duration_since_last_viewing_event
,time_in_seconds_since_recording

from sk_prod.VESPA_EVENTS_ALL
where account_number ='220005153279'
order by event_start_date_time_utc
,instance_start_date_time_utc;

commit;
















select panel_id
, count(*) as records 
,sum(case when subscriber_id is not null then 1 else 0 end) as records_with_subscriber_id 
,count(distinct subscriber_id) as distinct_subscriber_ids 
,count(distinct src_system_id) as distinct_src_system_id
,min(event_start_date_time_utc) as min_event_start
,max(event_start_date_time_utc) as max_event_start
from sk_prod.VESPA_EVENTS_VIEWED_ALL 
group by panel_id
order by panel_id;

commit;

--All records in teh daily table have subscrieber_id
select count(*) as records, sum(case when subscriber_id is not null then 1 else 0 end) as records_with_subscriber_id 
from sk_prod.VESPA_STB_PROG_EVENTS_20120612
;


select panel_id
,cast(event_start_date_time_utc as date) as event_date
,count(distinct subscriber_id) as distinct_subscriber_ids 
,count(distinct src_system_id) as distinct_src_system_id
from sk_prod.VESPA_EVENTS_VIEWED_ALL 
group by panel_id,event_date
order by panel_id,event_date;

commit;

select top 500 * from sk_prod.VESPA_EVENTS_VIEWED_ALL ;


select account_number
,panel_id
,cast(event_start_date_time_utc as date) as event_date
into #summary_by_account_panel_and_day
from sk_prod.VESPA_EVENTS_VIEWED_ALL 
group by account_number,panel_id,event_date
order by account_number,panel_id,event_date;

commit;
--

select count(*) from #summary_by_account_panel_and_day;

--drop table #summary_by_account_and_panel;
select account_number
,count( distinct event_date) as dates
into #summary_by_account_and_panel
from #summary_by_account_panel_and_day
where event_date between '2012-06-13' and '2012-06-19' and panel_id in (4,12)
group by account_number
;

select dates
,count(*) as accounts
from #summary_by_account_and_panel
group by dates
order by dates



--drop table #summary_by_account_and_panel;
select account_number
,count( distinct event_date) as dates
into #summary_by_account_and_panel_13_28_june
from #summary_by_account_panel_and_day
where event_date between '2012-06-13' and '2012-06-28' and panel_id in (4,12)
group by account_number
;

select dates
,count(*) as accounts
from #summary_by_account_and_panel_13_28_june
group by dates
order by dates
;

select event_date
,count(distinct account_number) as accounts
from #summary_by_account_panel_and_day
where panel_id in (4,12)
group by event_date


select count(*) as records
,sum(case when subscriber_id is null then 1 else 0 end) as unknown_subscriber_id
,sum(case when src_system_id is null then 1 else 0 end) as unknown_src_system_id
,sum(case when account_number is null then 1 else 0 end) as unknown_account_number
,sum(case when subscriber_id is null and account_number is null then 1 else 0 end) as unknown_subscriber_id_and_account_number
 from sk_prod.VESPA_EVENTS_VIEWED_ALL 

commit;

records,unknown_subscriber_id,unknown_src_system_id,unknown_account_number,unknown_subscriber_id_and_account_number
737199412,283890844,46984549,46984549,15193130

commit;
commit;
select cast(event_start_date_time_utc as date) as event_date
,count(*) as records
from sk_prod.VESPA_EVENTS_VIEWED_ALL 
where panel_id in (4,12)
group by event_date
order by event_date;


commit;
select cast(event_start_date_time_utc as date) as event_date
,count(distinct account_number) as records
from sk_prod.VESPA_EVENTS_VIEWED_ALL 
where panel_id in (4,12)
group by event_date
order by event_date;


select count(*) 
,sum(case when subscriber_id is null then 1 else 0 end) as unknown_subscriber_id
 from sk_prod.VESPA_STB_LOG_SUMMARY;
commit;

select top 100 instance_start_date_time_utc,instance_end_date_time_utc,datediff(second,instance_start_date_time_utc,instance_end_date_time_utc) as sec ,*from sk_prod.VESPA_EVENTS_VIEWED_ALL where account_number ='200000850798' ;


select distinct channel_name
from  sk_prod.VESPA_EVENTS_VIEWED_ALL
where live_recorded = 'LIVE' and broadcast_start_date_time_utc between '2012-06-13 05:00:00' and '2012-06-20 04:59:59'
order by channel_name

commit;commit;
/*


select *
from sk_prod.VESPA_EVENTS_ALL
where account_number ='220005153279'
and event_start_date_time_utc='2012-06-12 21:54:15'
order by event_start_date_time_utc
,instance_start_date_time_utc;


select
       vw.Account_Number,vw.Subscriber_Id,event_type,src_system_id,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,prog.Channel_Name,prog.Epg_Title
              

 from sk_prod.VESPA_STB_PROG_EVENTS_20120612 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
     account_number ='220005153279' 
order by vw.Adjusted_Event_Start_Time,vw.X_Viewing_Start_Time
 ;

select account_number
,count(distinct subscriber_id) as boxes
from sk_prod.VESPA_STB_PROG_EVENTS_20120615
group by account_number
having boxes>1

account_number,boxes
'200000847349',2
'200000850798',2
'200000898987',2
'200000908430',2
'200000935284',2
'200000946612',2
'200000947222',2
'200001001177',3

select subscriber_id , src_system_id,account_number, event_start_date_time_utc 

from sk_prod.VESPA_EVENTS_ALL
where account_number ='200000850798' 
order by event_start_date_time_utc;

select
       vw.Account_Number,vw.Subscriber_Id,src_system_id,Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,event_type

 from sk_prod.VESPA_STB_PROG_EVENTS_20120615 as vw
where account_number ='200000850798' 
order by Adjusted_Event_Start_Time ,vw.X_Viewing_Start_Time

select account_number , service_instance_id , box_installed_dt , box_replaced_dt,created_dt,x_description from sk_prod.cust_set_top_box where account_number = '200000850798' 

select account_number , service_instance_id , box_installed_dt , box_replaced_dt,created_dt,x_description from sk_prod.cust_set_top_box where service_instance_id='CH764847S'

commit;

select account_number ,service_instance_id,previous_service_instance_id
,effective_from_dt
,effective_to_dt
,manufacturer
,model
,si_owning_cust_account_id
,src_system_id
,si_start_dt
,si_service_instance_type
from
sk_prod.CUST_SERVICE_INSTANCE
 where account_number = '200000850798' 
and si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
order by effective_from_dt
;

select account_number
,active_box_flag
,box_installed_dt
,box_replaced_dt
,created_dt
,current_product_description
,owning_cust_account_id
,service_instance_id
,status
,status_start_dt
,x_description
from
sk_prod.CUST_SET_TOP_BOX
 where account_number = '200000850798' 
order by created_dt
;


-----Missing and Non Missing Subscriber_id
select top 100 subscriber_id , src_system_id,account_number, event_start_date_time_utc 

from sk_prod.VESPA_EVENTS_ALL
where subscriber_id is null and panel_id = 1;




select
       vw.Account_Number,vw.Subscriber_Id,src_system_id,Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,event_type

 from sk_prod.VESPA_STB_PROG_EVENTS_20120612 as vw
where account_number ='220018110068' 
order by Adjusted_Event_Start_Time ,vw.X_Viewing_Start_Time

select subscriber_id , src_system_id,account_number, event_start_date_time_utc 
from sk_prod.VESPA_EVENTS_ALL
where account_number ='220018110068' 
order by event_start_date_time_utc;


commit;



*/