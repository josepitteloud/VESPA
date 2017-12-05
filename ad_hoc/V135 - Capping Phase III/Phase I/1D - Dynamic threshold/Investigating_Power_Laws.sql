/*

Investigating power laws

*/

select top 1000 * from igonorp.VEA_5_11Nov_Viewing_Events
select count(*) from igonorp.VEA_5_11Nov_Viewing_Events -- 145,778,451

----------------------------------------
-- Get distributions of event duration
----------------------------------------

-- Segment 1
drop table VEA_5_11Nov_Event_Duration_Distribution_SEG1
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,Duration
        ,count(*) as Number_Events
into VEA_5_11Nov_Event_Duration_Distribution_SEG1
from igonorp.VEA_5_11Nov_Viewing_Events
where Viewing_Type_Detailed <> 0
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,Duration
-- 1532880 row(s) affected

-- Segment 2
drop table VEA_5_11Nov_Event_Duration_Distribution_SEG2
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,Duration
        ,count(*) as Number_Events
into VEA_5_11Nov_Event_Duration_Distribution_SEG2
from igonorp.VEA_5_11Nov_Viewing_Events
where Viewing_Type_Detailed = 0
and EVENT_START_HOUR in (20,21,22,23,0,1,2,3)
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,Duration
-- 3511937 row(s) affected

-- Segment 3
drop table VEA_5_11Nov_Event_Duration_Distribution_SEG3
select Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,Duration
        ,count(*) as Number_Events
into VEA_5_11Nov_Event_Duration_Distribution_SEG3
from igonorp.VEA_5_11Nov_Viewing_Events
where Viewing_Type_Detailed = 0
and EVENT_START_HOUR between 4 and 19
group by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,Duration
-- 10873306 row(s) affected

-- Export data
alter table VEA_5_11Nov_Event_Duration_Distribution_SEG3 drop  number_events_log10

select * from VEA_5_11Nov_Event_Duration_Distribution_SEG1
select * from VEA_5_11Nov_Event_Duration_Distribution_SEG2
select * from VEA_5_11Nov_Event_Duration_Distribution_SEG3

----------------------------
-- Example from Live 20-3h
----------------------------

-- ntiles
select * 
from VEA_5_11Nov_Event_Duration_Distribution_SEG2
where Viewing_Type_Detailed = 0
and EVENT_START_DOW = 'Sun'
and EVENT_START_HOUR = 20
and pack_grp = 'Other'
and genre_description = 'Entertainment'
order by Duration

-- threshold
select Threshold_Curr,Min_Duration,Capping_Threshold
from igonorp.Current_Threshold_Seg2
where Viewing_Type_Detailed = 0
and EVENT_START_DOW = 'Sun'
and EVENT_START_HOUR = 20
and pack_grp = 'Other'
and genre_description = 'Entertainment'
-- 199.0,14417,7200

select *,sum(number_events) over(partition by Viewing_Type_Detailed
                                            ,EVENT_START_DOW
                                            ,EVENT_START_HOUR
                                            ,pack_grp
                                            ,genre_description
                                order by duration rows between unbounded preceding and current row)
        as cumulative_number_events
        ,sum(number_events) over(partition by Viewing_Type_Detailed
                                              ,EVENT_START_DOW
                                              ,EVENT_START_HOUR
                                              ,pack_grp
                                              ,genre_description)
        as total_number_events
        ,cumulative_number_events*1.0/total_number_events as cdf_number_events
from VEA_5_11Nov_Event_Duration_Distribution_SEG2
where Viewing_Type_Detailed = 0
and EVENT_START_DOW = 'Sun'
and EVENT_START_HOUR = 20
and pack_grp = 'Other'
and genre_description = 'Entertainment'
order by Duration

select * 
from igonorp.Current_Threshold_Seg2
where Viewing_Type_Detailed = 0
and EVENT_START_DOW = 'Sun'
and EVENT_START_HOUR = 20
and pack_grp = 'Other'
and genre_description = 'Entertainment'

select ntile,median_duration,second_gradient
from igonorp.Third_Segmentation_Gradient
where Viewing_Type_Detailed = 0
and EVENT_START_HOUR between 4 and 19
order by Viewing_Type_Detailed
        ,EVENT_START_DOW
        ,EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,ntile
