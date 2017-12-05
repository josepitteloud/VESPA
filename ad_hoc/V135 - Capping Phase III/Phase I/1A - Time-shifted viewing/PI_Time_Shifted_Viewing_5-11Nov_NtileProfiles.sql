/*

Playback vs VOSDAL

1. Create an additional variable to store Playback/VOSDAL
        a) cut-off at 24h (eg, Playback_VOSDAL_24h)
        b) cut-off at 04h (eg, Playback_VOSDAL_04h)

2. Create event duration ntiles profile for Playback vs VOSDAL
        a) cut-off at 24h
        b) cut-off at 04h

3. Create an additional variable to store:
        a) day of week for viewing (eg, EVENT_START_DOW)
        b) event start hour
        c) day of week for broadcast
        d) broadcast start hour

4. Create event duration ntiles profile for Playback vs VOSDAL
        a) breakdown by day of week and start hour
		
Author: Patrick Igonor

*/

/*---------------------------------------------------------------------------
Alter table to add two extra variables
-------------------------------------------------------------------------*/
-----1:


-- Step 02: build indexes to speed up the next Event_Star_Hour and the Broadcast Time)

create lf index idx1 on limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h(Playback_VOSDAL_24h)
create lf index idx2 on limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h(Playback_VOSDAL_04h)
create lf index idx3 on limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h(Day_of_Week_Event)
create lf index idx4 on limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h(Event_Start_Hour)
create lf index idx5 on limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h(Day_of_Week_Broadcast)
create lf index idx6 on limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h(Broadcast_Start_Hour)

--Step 3: Defining Vosdal and Playback--

Update limac.VEA_sample_5_12Nov_Time_Shifted_Events
set Playback_VOSDAL_24h =
        case
                when Broadcast_date = Event_Start_date then 'VOSDAL' else 'PLAYBACK'
        end;
--------------57176006 Row(s) affected -----

Update limac.VEA_sample_5_12Nov_Time_Shifted_Events
set Playback_VOSDAL_04h =
        case
                when date(dateadd(hh, -4, BROADCAST_START_DATE_TIME_UTC)) = date(dateadd(hh, -4, EVENT_START_DATE_TIME_UTC))
                then 'VOSDAL'
                else 'PLAYBACK'
        end;
----57176006 Row(s) affected -----hour(event_start_date_time_utc)as Event_Start_Hour

--Step 4: Defining different days of the week for events viewing --

Update limac.VEA_sample_5_12Nov_Time_Shifted_Events
set     Day_of_Week_Event =
        case
                when datepart(weekday,Event_Start_Date)=1 then 'Sunday'
                when datepart(weekday,Event_Start_Date)=2 then 'Monday'
                when datepart(weekday,Event_Start_Date)=3 then 'Tuesday'
                when datepart(weekday,Event_Start_Date)=4 then 'Wednesday'
                when datepart(weekday,Event_Start_Date)=5 then 'Thursday'
                when datepart(weekday,Event_Start_Date)=6 then 'Friday'
                when datepart(weekday,Event_Start_Date)=7 then 'Saturday'
        end


--Step 5: Defining different start hours for event viewing--

Update limac.VEA_sample_5_12Nov_Time_Shifted_Events
set     Event_Start_Hour =
        case
                when datepart(hour,Event_Start_Time) between 4  and 14 then 'HR_4_14'
                when datepart(hour,Event_Start_Time) between 15 and 19 then 'HR_15_19'
                when datepart(hour,Event_Start_Time) between 20 and 22 then 'HR_20_22'
                when datepart(hour,Event_Start_Time) in (23,0,1,2,3)  then 'HR_23_3'
        end

--Step 6: Defining different days of the week for Broadcast start days --

Update limac.VEA_sample_5_12Nov_Time_Shifted_Events
set     Day_of_Week_Broadcast =
        case
                when datepart(weekday,Broadcast_Date)=1 then 'Sunday'
                when datepart(weekday,Broadcast_Date)=2 then 'Monday'
                when datepart(weekday,Broadcast_Date)=3 then 'Tuesday'
                when datepart(weekday,Broadcast_Date)=4 then 'Wednesday'
                when datepart(weekday,Broadcast_Date)=5 then 'Thursday'
                when datepart(weekday,Broadcast_Date)=6 then 'Friday'
                when datepart(weekday,Broadcast_Date)=7 then 'Saturday'
        end


--Step 7: Defining different start hours for broadcast start hour--

Update limac.VEA_sample_5_12Nov_Time_Shifted_Events
set     Broadcast_Start_Hour =
        case
                when datepart(hour,Broadcast_Time) between 4  and 14 then 'HR_4_14'
                when datepart(hour,Broadcast_Time) between 15 and 19 then 'HR_15_19'
                when datepart(hour,Broadcast_Time) between 20 and 22 then 'HR_20_22'
                when datepart(hour,Broadcast_Time) in (23,0,1,2,3)  then 'HR_23_3'
        end




-- 2a: Calculate ntiles for Playback_VOSDAL_24h--

select median(duration)as Median_Duration
        ,Playback_VOSDAL_24h
        ,ntiles_24h
into Median_VOSDAL_24h
from   (  select duration
        ,Playback_VOSDAL_24h
        ,ntile(200) over (partition by Playback_VOSDAL_24h order by duration) as ntiles_24h
        from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
        ) VEA_VP
group by  Playback_VOSDAL_24h, ntiles_24h
order by  Playback_VOSDAL_24h, ntiles_24h


-- 2b: Calculate ntiles for Playback_VOSDAL_04h--

select median(duration)as Median_Duration
        ,Playback_VOSDAL_04h
        ,ntiles_4h
into Median_VOSDAL_4hr
from    (  select duration
        ,Playback_VOSDAL_04h
        ,ntile(200) over (partition by Playback_VOSDAL_04h order by duration) as ntiles_4h
        from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
        ) VEA_VP
group by  Playback_VOSDAL_04h, ntiles_4h
order by  Playback_VOSDAL_04h, ntiles_4h


-- 3b: Calculate ntiles for Playback_VOSDAL_24h and also by DOW and start hours--

select median(duration)as Median_Duration
        ,Playback_VOSDAL_24h
        ,Day_of_Week_Event
        ,Event_Start_Hour
        ,ntiles_24h
into Median_VOSDAL_24h_DOW_SHR
from   (  select duration
        ,Playback_VOSDAL_24h
        ,Day_of_Week_Event
        ,Event_Start_Hour
        ,ntile(200) over (partition by Playback_VOSDAL_24h, Day_of_Week_Event, Event_Start_Hour order by duration) as ntiles_24h
        from limac.VEA_sample_5_12Nov_Time_Shifted_Events_24h
        ) VEA_VA
group by  Playback_VOSDAL_24h,Day_of_Week_Event, Event_Start_Hour, ntiles_24h
order by  Playback_VOSDAL_24h,Day_of_Week_Event, Event_Start_Hour, ntiles_24h

-- 3c: Calculate ntiles for Playback_VOSDAL_24h and also by DOW and start hours--

select median(duration)as Median_Duration
        ,Playback_VOSDAL_24h
        ,Day_of_Week_Event
        ,Event_Start_Hour
        ,ntiles_24h
into Median_VOSDAL_24h_DOW_SHR
from   (  select duration
        ,Playback_VOSDAL_24h
        ,Day_of_Week_Event
        ,Event_Start_Hour
        ,ntile(200) over (partition by Playback_VOSDAL_24h, Day_of_Week_Event, Event_Start_Hour order by duration) as ntiles_24h
        from limac.VEA_sample_5_12Nov_Time_Shifted_Events
        ) VEA_VA
group by  Playback_VOSDAL_24h,Day_of_Week_Event, Event_Start_Hour, ntiles_24h
order by  Playback_VOSDAL_24h,Day_of_Week_Event, Event_Start_Hour, ntiles_24h



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



select top 10* from limac.VEA_sample_5_11Nov_Time_Shifted_Events



------------Replication of the above analysis using a different table and period limac.VEA_sample_5_11Nov_Time_Shifted_Events------------

-- Step 02: build indexes to speed up the next Event_Star_Hour and the Broadcast Time)

create lf index idx1_VE on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Viewing_Event_Type)
create lf index idx2_DO on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Event_Start_DOW)
create lf index idx3_BS on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Broadcast_Start_Period)
create lf index idx4_BH on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Broadcast_Start_Hour)
create lf index idx5_ES on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Event_Start_Period)
create lf index idx6_EH on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Event_Start_Hour)
create lf index idx7_BD on limac.VEA_sample_5_11Nov_Time_Shifted_Events(Broadcast_Start_DOW)


-- 2a: Calculate ntiles for Viewing_Event_Type--

select median(duration)as Median_Duration
        ,Viewing_Event_Type
        ,ntiles
into    Median_VET
from   (  select duration
        ,Viewing_Event_Type
        ,ntile(200) over (partition by Viewing_Event_Type order by duration) as ntiles
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        ) VEA_P
group by  Viewing_Event_Type, ntiles
order by  Viewing_Event_Type, ntiles

--2b: Calculate ntiles for Viewing_Event_Type and also by DOW and start period--

select median(duration)as Median_Duration
        ,Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntiles
into     Median_VOSDAL_PER
from   (  select duration
        ,Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Period
        ,ntile(200) over (partition by Viewing_Event_Type, Event_Start_DOW, Event_Start_Period order by duration) as ntiles
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        ) VEA_VA
group by  Viewing_Event_Type, Event_Start_DOW, Event_Start_Period, ntiles
order by  Viewing_Event_Type, Event_Start_DOW, Event_Start_Period, ntiles


-- 2c: Calculate ntiles for Viewing_Event_Type and also by DOW and start hours--


select median(duration)as Median_Duration
        ,Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,ntiles
into    Median_VOSDAL_DOW
from   (  select duration
        ,Viewing_Event_Type
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,ntile(200) over (partition by Viewing_Event_Type, Event_Start_DOW, Event_Start_Hour order by duration) as ntiles
        from limac.VEA_sample_5_11Nov_Time_Shifted_Events
        ) VEA_VA
group by  Viewing_Event_Type, Event_Start_DOW, Event_Start_Hour, ntiles
order by  Viewing_Event_Type, Event_Start_DOW, Event_Start_Hour, ntiles


