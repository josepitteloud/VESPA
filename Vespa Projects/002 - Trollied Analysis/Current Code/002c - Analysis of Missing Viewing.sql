
----002c Analysis of Missing Data Issue


select subscriber_id 
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:09:00' and capped_x_viewing_end_time_local >'2011-08-11 21:09:00' and play_back_speed=2 then 1 else 0 end) as viewing_21_09_playback
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:09:00' and capped_x_viewing_end_time_local >'2011-08-11 21:09:00' and play_back_speed is null then 1 else 0 end) as viewing_21_09_live
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:16:00' and capped_x_viewing_end_time_local >'2011-08-11 21:16:00' and play_back_speed=2 then 1 else 0 end) as viewing_21_16_playback
, max(case when capped_x_viewing_start_time_local <'2011-08-11 21:16:00' and capped_x_viewing_end_time_local >'2011-08-11 21:16:00' and play_back_speed is null then 1 else 0 end) as viewing_21_16_live
into #sub_viewing
from VESPA_tmp_all_viewing_records_trollied_20110811
where programme_trans_sk in (201108120000014061
,201108120000000728
,201108120000002465)
--where play_back_speed=2
group by subscriber_idk;

--Output of Pivot

select programme_trans_sk,viewing_21_09_live , viewing_21_16_live ,viewing_21_09_playback , viewing_21_16_playback, count(*) as subs 
from #sub_viewing
group by programme_trans_sk,viewing_21_09_live , viewing_21_16_live ,viewing_21_09_playback , viewing_21_16_playback
order by programme_trans_sk,viewing_21_09_live , viewing_21_16_live ,viewing_21_09_playback , viewing_21_16_playback

commit;


---Produce list of those that have the issue
select * from #sub_viewing where viewing_21_09_playback = 0 and viewing_21_16_playback = 1


---Find which day they watched trollied
select * from  VESPA_tmp_all_viewing_records_trollied_20110811 where subscriber_id = 22307325 order by adjusted_event_start_time;


---Change sk_prod.VESPA_STB_PROG_EVENTS_20110811 to whatever day they viewed trollied to show all activity for that day

select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
into #example_viewing
     from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vw
          inner join VESPA_Programmes_20110811 as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
     where 
--subscriber_id = 26315398 and 
--(play_back_speed is null or play_back_speed = 2) and 
        
-- x_programme_viewed_duration > 0
        and 
Panel_id in (4,5)
--        and x_type_of_viewing_event <> 'Non viewing event'
order by Adjusted_Event_Start_Time
      ;
