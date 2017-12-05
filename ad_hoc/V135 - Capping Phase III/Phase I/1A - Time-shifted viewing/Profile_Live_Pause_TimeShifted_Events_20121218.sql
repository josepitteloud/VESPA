----------------------------------------------------
-- 
-- Profile VOSDAL events tha follow the sequence 
-- live->pause->time-shifted
--
--
--
----------------------------------------------------

-- Number of events and percentage of VOSDAL for each air-view delta

select air_view_delta
       ,sum(case when After_Live_Pause = 1 then 1 else 0 end) as Num_Events_After_Live_Pause
       ,count(*) as Num_Events
       ,Num_Events_After_Live_Pause*1.0/Num_Events as Percentage_Events_After_Live_Pause
from    (
        select cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int) as air_view_delta
                ,After_live_pause
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        where Viewing_Event_Type = 'VOSDAL'
        ) t
where air_view_delta >= 0
group by air_view_delta
order by air_view_delta

-- Calculate the ntiles profiles by DOW and start hour period

select median(duration)as Median_Duration
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntiles
into    Median_VOSDAL_DOWP
from   (  select duration
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntile(200) over (partition by Event_Start_DOW, Event_Start_Period order by duration) as ntiles
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        where Viewing_Event_Type = 'VOSDAL'
        and After_Live_Pause = 1
        ) VEA_V
group by  Event_Start_DOW, Event_Start_Period, ntiles
order by  Event_Start_DOW, Event_Start_Period, ntiles



-- Profile VOSDAL events based on the difference between air and viewing time

select air_view_delta
        ,ntiles
        ,median(Duration)/60.0 'Duration_Median_min'
into    Air_View_D
from
(
select air_view_delta
        ,Duration
        ,ntile(200) over (partition by air_view_delta order by Duration) as ntiles
from    (
        select cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_MIN,EVENT_START_DATE_TIME_UTC)/(60.0*60.0) as int) as air_view_delta
                ,Duration
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        where Viewing_Event_Type = 'VOSDAL'
        and After_Live_Pause = 1
        ) t2
where air_view_delta >= 0
) t2
group by air_view_delta
        ,ntiles
order by air_view_delta
        ,ntiles

-----selecting the air_view_delta with up to 200 ntiles----------

Select  *
from
  (select air_view_delta,
         max(ntiles) max_ntile
    from Air_View_D
        group by air_view_delta
        having max(ntiles) = 200
        ) ntiles_Up_to_200
        ,Air_View_D avd
where avd.air_view_delta = ntiles_Up_to_200.air_view_delta

----Running the above query in a different way-------------------
select  *
from Air_View_D
where air_view_delta in (
                select air_view_delta
                from Air_View_D
                    group by air_view_delta
                    having max(ntiles) = 200
                        )
------------------------------------------------------------------

