
----Capping Phase 2 Comparison - Viewing 5th-18th feb Inclusive

select subscriber_id
,account_number
,programme_trans_sk
,scaling_segment_id
,scaling_weighting
,viewing_starts as viewing_starts_utc
,case 
when dateformat(viewing_starts,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,viewing_starts) 
                    else viewing_starts  end as viewing_starts_local
,viewing_stops as viewing_stops_utc
,case 
when dateformat(viewing_stops,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,viewing_stops) 
                    else viewing_stops  end as viewing_stops_local
into #capped_phase_2_viewing
from vespa_analysts.VESPA_DAILY_AUGS_20120208
where timeshifting = 'LIVE'
;

commit;

--select top 500 * from vespa_analysts.VESPA_DAILY_AUGS_20120208 ;
select * from vespa_analysts.VESPA_DAILY_AUGS_20120208 where subscriber_id = 17828608 order by viewing_starts , viewing_stops;

select 
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
, sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration 
 ,vw.service_key                
    ,vw.original_network_id        
    ,vw.transport_stream_id        
    ,vw.si_service_id                 

 from sk_prod.VESPA_STB_PROG_EVENTS_20120208 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in ('TV Channel Viewing','Sky+ time-shifted viewing event','HD Viewing Event')
          or (x_type_of_viewing_event = ('Other Service Viewing Event')
              and x_si_service_type = 'High Definition TV test service'))
     and panel_id in (4,5) and upper(prog.Channel_Name) = 'ANYTIME'
--subscriber_id = 17828608
order by Adjusted_Event_Start_Time ,X_Adjusted_Event_End_Time
;

commit;

--Select * from sk_prod.VESPA_EPG_DIM where upper(Channel_Name) = 'ANYTIME' and tx_date = '20120710' order by tx_start_datetime_utc
