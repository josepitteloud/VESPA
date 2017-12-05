
------Identify all Boxes that have any invalid viewing events-------


--drop table vespa_analysts.invalid_viewing_test;
-----Join on 11th to 18th---
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    
into vespa_analysts.invalid_viewing_test
     from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;
insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110812 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;

insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110813 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;

insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110814 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;

insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110815 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;

insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110816 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;

insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110817 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;

insert into vespa_analysts.invalid_viewing_test
select
    vw.Subscriber_Id,vw.panel_id , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110818 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by vw.Subscriber_Id,vw.panel_id
      ;
commit;

---Select All subscribers that have any invalid records----


select subscriber_id
into #subscribers_to_exclude
from vespa_analysts.invalid_viewing_test
where Panel_id in (4,5) and invalid_record>0
group by subscriber_id

;

--select top 100 * from vespa_analysts.daily_summary_by_subscriber_20110811

select a.*
,case when b.subscriber_id is null then 0 else 1 end as sub_to_exclude
into #add_exclusion_details
from vespa_analysts.daily_summary_by_subscriber_20110811 as a
left outer join #subscribers_to_exclude as b
on a.subscriber_id=b.subscriber_id
;

select sum(sub_to_exclude) , count(sub_to_exclude) from #add_exclusion_details;

left outer join vespa_analysts.sky_base_v2_2011_08_11 as b
on a.subscriber_id =b.subscriber_id
left outer join vespa_analysts.sky_base_2011_08_11_by_account as c
on b.account_number =c.account_number

--select top 100 * from vespa_analysts.sky_base_v2_2011_08_11;

select value_segment
,count(*) as records
,sum(case when days_returning_data =8 then 1 else 0 end) as full_box_data
,sum(case when b.subscriber_id is not null then 1 else 0 end) as sub_to_exclude
,sum(days_returning_data_2011_08_11) as boxes_returning_data_2011_08_11
,sum(weight_2011_08_11*days_returning_data_2011_08_11) as weighted_boxes_2011_08_11
from vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join #subscribers_to_exclude as b
on a.subscriber_id=b.subscriber_id
group by value_segment
;

select value_segment
,count(*) as records
,sum(case when days_returning_data =8 then 1 else 0 end) as full_box_data
,sum(case when b.subscriber_id is not null then 1 else 0 end) as sub_to_exclude
,sum(case when days_returning_data>0 then 1 else 0 end) as boxes_returning_data_during_period
,sum(days_returning_data_2011_08_11) as boxes_returning_data_2011_08_11
,sum(case when b.subscriber_id is not null then days_returning_data_2011_08_11 else 0 end ) as boxes_returning_data_2011_08_11_to_exclude
,sum(weight_2011_08_11*days_returning_data_2011_08_11) as weighted_boxes_2011_08_11

from vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join #subscribers_to_exclude as b
on a.subscriber_id=b.subscriber_id
group by value_segment
;

--select count(*) from  #subscribers_to_exclude;


-----------------------

/*
select subscriber_id
,panel_id
,count(*) as records
,sum(invalid_record) as invalid_records
into #sub_summary_20110811_20110818
from vespa_analysts.invalid_viewing_test

group by subscriber_id
,panel_id
;

select hd_sub
,count(*) as records
,sum(case when days_returning_data =8 then 1 else 0 end) as full_box_data
,sum(case when b.subscriber_id is not null then 1 else 0 end) as sub_to_exclude
,sum(case when days_returning_data>0 then 1 else 0 end) as boxes_returning_data_during_period
,sum(days_returning_data_2011_08_11) as boxes_returning_data_2011_08_11
,sum(case when b.subscriber_id is not null then days_returning_data_2011_08_11 else 0 end ) as boxes_returning_data_2011_08_11_to_exclude
,sum(weight_2011_08_11*days_returning_data_2011_08_11) as weighted_boxes_2011_08_11

from vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join #subscribers_to_exclude as b
on a.subscriber_id=b.subscriber_id
group by hd_sub
;


commit;










-----Investigation into 'Other Service Viewing''
--drop table #viewing_20110811;
select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description,vw.panel_id
        ,vw.original_network_id , vw.transport_stream_id,vw.si_service_id,vw.service_key,vw.video_playing_flag
,case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  and  vw.service_key is  null then 1 else 0 end as invalid_record
into #viewing_20110811
     from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
order by Subscriber_Id ,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time
      ;

select top 5000 * from #viewing_20110811 order by Subscriber_Id ,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time;

select subscriber_id
,panel_id
,count(*) as records
,sum(case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  and  service_key is  null then 1 else 0 end) as invalid_record
into #sub_summary
from #viewing_20110811
group by subscriber_id
,panel_id
;

select panel_id
,count(*) as subs
,sum(records) as all_records
,sum(invalid_record) as invalid_records
,sum(case when invalid_record >0 then 1 else 0 end) as subs_with_invalid_record
from #sub_summary
group by panel_id
order by panel_id
;

select * from 
#viewing_20110811
order by Subscriber_Id ,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time


----repeat for 13th Oct----

select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description,vw.panel_id
        ,vw.original_network_id , vw.transport_stream_id,vw.si_service_id,vw.service_key,vw.video_playing_flag
into #viewing_20111013
     from sk_prod.VESPA_STB_PROG_EVENTS_20111013 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
order by Subscriber_Id ,Adjusted_Event_Start_Time,X_Adjusted_Event_End_Time
      ;

select subscriber_id
,panel_id
,count(*) as records
,sum(case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  and  service_key is  null then 1 else 0 end) as invalid_record
into #sub_summary_20111013
from #viewing_20111013
group by subscriber_id
,panel_id
;

select panel_id
,count(*) as subs
,sum(records) as all_records
,sum(invalid_record) as invalid_records
,sum(case when invalid_record >0 then 1 else 0 end) as subs_with_invalid_record
from #sub_summary_20111013
group by panel_id
order by panel_id
;

select panel_id
,count(distinct subscriber_id) as subs
,sum(records) as all_records
,sum(invalid_records) as total_invalid_records
,sum(case when invalid_records >0 then 1 else 0 end) as subs_with_invalid_record
from #sub_summary_20110811_20110818
group by panel_id
order by panel_id
;



select
    video_playing_flag , count(*) as records
    ,sum(   case when original_network_id=0 and transport_stream_id=0 and si_service_id=0  
            and  vw.service_key is  null then 1 else 0 end) as invalid_record
    

     from sk_prod.VESPA_STB_PROG_EVENTS_20110818 as vw
          left outer join sk_prod.vespa_epg_dim as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where 
         
Panel_id in (1,4,5)
group by video_playing_flag
      ;


commit;
