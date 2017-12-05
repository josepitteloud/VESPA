
    select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
 ,vw.service_key                
    ,vw.original_network_id        
    ,vw.transport_stream_id        
    ,vw.si_service_id                 
,case when video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in ('TV Channel Viewing','Sky+ time-shifted viewing event','HD Viewing Event')
          or (x_type_of_viewing_event = ('Other Service Viewing Event')
              and x_si_service_type = 'High Definition TV test service'))
     and panel_id in (4,5,12) then 1 else 0 end as defined_viewing
into #viewing_may_28th
from sk_prod.VESPA_STB_PROG_EVENTS_20120528 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where panel_id in (4,5,12)
  ;

select top 100 * from #viewing_may_28th;


select Event_Type
,X_event_duration
,count(*) as records
from #viewing_may_28th
where event_type = 'evSurf'
group by  Event_Type
,X_event_duration
order by X_event_duration;


select Adjusted_Event_Start_Time
,Event_Type
,Channel_Name
,count(*) as records
,sum(defined_viewing)
from #viewing_may_28th
group by Adjusted_Event_Start_Time
,Event_Type
,Channel_Name
order by records desc
;


commit;

select Event_Type
,cast(X_Adjusted_Event_End_Time as datetime) - cast(Adjusted_Event_Start_Time as datetime) -  as seconds_of_event
,count(*) as records
from #viewing_may_28th
group by Event_Type
,seconds_of_event
order by records desc;
