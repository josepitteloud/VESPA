
/*
Playback vs VOSDAL

-------60, 70, 80 and 90 minutes selection Ntiles-------------

Author: Patrick Igonor

*/
------------------------------------------------------------------------------------------
--Pulling the partition of viewing events type, Event start DOW and Period into a table (VEA_sample_5_11Nov_Time_Shifted_Events_ntiles)--

select   duration
        ,Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntile(200) over (partition by Viewing_Event_Type, Event_Start_DOW, Event_Start_Period order by duration) as ntiles
into VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
from limac.VEA_sample_5_11Nov_Time_Shifted_Events

----------------------------------------------------------------
--Ntile selection at 55 minutes----

select Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_55min
from VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
where duration >=55*60
group by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
order by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period

-----------------------------------------------------------
--Ntile selection at 60 minutes----

select Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_60min
from VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
where duration >=60*60
group by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
order by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period

-------------------------------------------------------------
--Ntile selection at 65 minutes----
select Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_65min
from VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
where duration >=65*60
group by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
order by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period

--------------------------------------------------------------
--Ntile selection at 70 minutes--

select Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_70min
from VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
where duration >=70*60
group by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
order by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
		
--------------------------------------------------------------
--Ntile selection at 80 minutes----

select Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_80min
from VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
where duration >=80*60
group by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
order by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period

----------------------------------------------------------------
--Ntile selection at 90 minutes----

select Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,min(ntiles) as ntile_90min
from VEA_sample_5_11Nov_Time_Shifted_Events_ntiles
where duration >=90*60
group by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
order by Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period

---------------------------------------------------------------------------
-- Profile VOSDAL events based on the difference between air and viewing time
select air_view_delta
        ,ntiles
        ,median(Duration)/60.0 'Duration_Median_min'
into    Air_View_Diff
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
        ) t1
where air_view_delta >= 0
) t2
group by air_view_delta
        ,ntiles
order by air_view_delta
        ,ntiles

---------------------------------------------------------------------------
--Ntile selection  based on Air_View_Delta ( Difference between Broadcast start time and Event Start time at 55 minutes---

select   air_view_delta
        ,min(ntiles) as ntile_55min
from Air_View_Diff
where duration_median_min >=55
group by air_view_delta
order by air_view_delta

----------------------------------------------------------------------------
--Ntile selection  based on Air_View_Delta ( Difference between Broadcast start time and Event Start time at 60 minutes---

select   air_view_delta
        ,min(ntiles) as ntile_66min
from Air_View_Diff
where duration_median_min >=60
group by air_view_delta
order by air_view_delta

-----------------------------------------------------------------------------
--Ntile selection  based on Air_View_Delta ( Difference between Broadcast start time and Event Start time at 65 minutes---

select   air_view_delta
        ,min(ntiles) as ntile_65min
from Air_View_Diff
where duration_median_min >=65
group by air_view_delta
order by air_view_delta

-----------------------------------------------------------------------------
--Ntile selection  based on Air_View_Delta ( Difference between Broadcast start time and Event Start time at 70 minutes---

select   air_view_delta
        ,min(ntiles) as ntile_55min
from Air_View_Diff
where duration_median_min >=70
group by air_view_delta
order by air_view_delta












