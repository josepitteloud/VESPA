/*

Analyse the impact of segmentation in number of events 
available to calculate ntile profiles

*/

-- Number of segments
select count(*) from igonorp.Num_Events_Seg_1 -- 672 (580)
select count(*) from igonorp.Num_Events_Seg_2 -- 22,125 (18,992)
select count(*) from igonorp.Num_Events_Seg_3 -- 58,960 (50,483)

-- Number of segments with no events
select count(*) from igonorp.Num_Events_Seg_1 where number_events = 0 -- 0
select count(*) from igonorp.Num_Events_Seg_2 where number_events = 0 -- 0
select count(*) from igonorp.Num_Events_Seg_3 where number_events = 0 -- 0

-- Ok, we need to get these into a table

-- Segment 1
select *,0 as Number_Events 
into Num_Events_0_Seg_1
from (
(select distinct viewing_type_detailed from igonorp.Num_Events_Seg_1) t1
,(select distinct event_start_dow from igonorp.Num_Events_Seg_1) t2
,(select distinct event_start_hour from igonorp.Num_Events_Seg_1) t3
)
where str(viewing_type_detailed) || '-' || event_start_dow || '-' || str(event_start_hour) not in
(select distinct str(viewing_type_detailed) || '-' || event_start_dow || '-' || str(event_start_hour) 
from igonorp.Num_Events_Seg_1)
-- 0 row(s) affected

-- Segment 2
select *,0 as Number_Events 
into Num_Events_0_Seg_2
from (
(select distinct viewing_type_detailed from igonorp.Num_Events_Seg_2) t1
,(select distinct event_start_dow from igonorp.Num_Events_Seg_2) t2
,(select distinct event_start_hour from igonorp.Num_Events_Seg_2) t3
,(select distinct pack_grp from igonorp.Num_Events_Seg_2) t4
,(select distinct genre_description from igonorp.Num_Events_Seg_2) t5
)
where str(viewing_type_detailed) || '-' || event_start_dow || '-' || str(event_start_hour) || pack_grp || coalesce(genre_description,'') not in
(select distinct str(viewing_type_detailed) || '-' || event_start_dow || '-' || str(event_start_hour) || pack_grp || coalesce(genre_description,'') 
from igonorp.Num_Events_Seg_2)
-- 20,211 row(s) affected

-- Segment 3
select *,0 as Number_Events 
into Num_Events_0_Seg_3
from (
(select distinct viewing_type_detailed from igonorp.Num_Events_Seg_3) t1
,(select distinct event_start_dow from igonorp.Num_Events_Seg_3) t2
,(select distinct event_start_hour from igonorp.Num_Events_Seg_3) t3
,(select distinct pack_grp from igonorp.Num_Events_Seg_3) t4
,(select distinct genre_description from igonorp.Num_Events_Seg_3) t5
,(select distinct box_subscription from igonorp.Num_Events_Seg_3) t6
)
where str(viewing_type_detailed) || '-' || event_start_dow || '-' || str(event_start_hour) || pack_grp || coalesce(genre_description,'') || box_subscription not in
(select distinct str(viewing_type_detailed) || '-' || event_start_dow || '-' || str(event_start_hour) || pack_grp || coalesce(genre_description,'') || box_subscription
from igonorp.Num_Events_Seg_3)
-- 68,048 row(s) affected

-----------------------------------------------------------------
-- Distribution of segments for Playback
-----------------------------------------------------------------

select base.sample_slot
        ,coalesce(playback1.Number_segments,0) as Number_Segments_Playback1
        ,coalesce(playback2.Number_segments,0) as Number_Segments_Playback2
        ,coalesce(playback3.Number_segments,0) as Number_Segments_Playback3
from (
select 0 as sample_slot union
select 1 as sample_slot union
select 10 as sample_slot union
select 50 as sample_slot union
select 100 as sample_slot union
select 200 as sample_slot union
select 500 as sample_slot union
select 1000 as sample_slot union
select 10000 as sample_slot union
select 100000 as sample_slot union
select 1000000 as sample_slot union
select 10000000 as sample_slot
) base
left join
(
select case 
            when Number_Events = 0 then 0
            when Number_Events between 1    and 9 then 1
            when Number_Events between 10   and 49 then 10
            when Number_Events between 50   and 99 then 50
            when Number_Events between 100  and 199 then 100
            when Number_Events between 200  and 499 then 200
            when Number_Events between 500  and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events between 100000 and 999999 then 100000
            when Number_Events between 1000000 and 9999999 then 1000000
            when Number_Events >= 10000000 then 10000000
        end as sample_slot
        ,count(*) as Number_Segments
from 
    (
    select case when viewing_type_detailed > 0 then 1 else 0 end as live_timeshifted
            ,event_start_dow
            ,event_Start_hour
            ,sum(number_events) as Number_Events
    from (
            select * from igonorp.Num_Events_Seg_1 union
            select * from Num_Events_0_Seg_1
        ) t1
    where viewing_type_detailed > 0
    group by live_timeshifted
            ,event_start_dow
            ,event_Start_hour
    ) t2
group by sample_slot
) playback1
on base.sample_slot = playback1.sample_slot
left join
(
select case 
            when Number_Events = 0 then 0
            when Number_Events between 1    and 9 then 1
            when Number_Events between 10   and 49 then 10
            when Number_Events between 50   and 99 then 50
            when Number_Events between 100  and 199 then 100
            when Number_Events between 200  and 499 then 200
            when Number_Events between 500  and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events between 100000 and 999999 then 100000
            when Number_Events between 1000000 and 9999999 then 1000000
            when Number_Events >= 10000000 then 10000000
        end as sample_slot
        ,count(*) as Number_Segments
from 
    (
    select case when viewing_type_detailed in (1,2) then 1 else viewing_type_detailed end as live_VOSDAL_Playback
            ,event_start_dow
            ,event_Start_hour
            ,sum(number_events) as Number_Events
    from (
            select * from igonorp.Num_Events_Seg_1 union
            select * from Num_Events_0_Seg_1
        ) t1
    where viewing_type_detailed > 0
    group by live_VOSDAL_Playback
            ,event_start_dow
            ,event_Start_hour
    ) t2
group by sample_slot
) playback2
on base.sample_slot = playback2.sample_slot
left join
(
select case 
            when Number_Events = 0 then 0
            when Number_Events between 1    and 9 then 1
            when Number_Events between 10   and 49 then 10
            when Number_Events between 50   and 99 then 50
            when Number_Events between 100  and 199 then 100
            when Number_Events between 200  and 499 then 200
            when Number_Events between 500  and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events between 100000 and 999999 then 100000
            when Number_Events between 1000000 and 9999999 then 1000000
            when Number_Events >= 10000000 then 10000000
        end as sample_slot
        ,count(*) as Number_Segments
from 
    (
    select viewing_type_detailed
            ,event_start_dow
            ,event_Start_hour
            ,sum(number_events) as Number_Events
    from (
            select * from igonorp.Num_Events_Seg_1 union
            select * from Num_Events_0_Seg_1
        ) t1
    where viewing_type_detailed > 0
    group by viewing_type_detailed
            ,event_start_dow
            ,event_Start_hour
    ) t2
group by sample_slot
) playback3
on base.sample_slot = playback3.sample_slot
order by base.sample_slot

-- List all buckets
select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,number_events
from (
      select * from igonorp.Num_Events_Seg_1 union
      select * from Num_Events_0_Seg_1
     ) t1
where viewing_type_detailed > 0

-----------------------------------------------------------------
-- Distribution of live for 20-3h
-----------------------------------------------------------------
select base.sample_slot
        ,coalesce(seg2.Number_segments,0) as Number_Segments_2
from (
select 0 as sample_slot union
select 1 as sample_slot union
select 10 as sample_slot union
select 50 as sample_slot union
select 100 as sample_slot union
select 200 as sample_slot union
select 500 as sample_slot union
select 1000 as sample_slot union
select 10000 as sample_slot union
select 100000 as sample_slot union
select 1000000 as sample_slot union
select 10000000 as sample_slot
) base
left join
(
select case 
            when Number_Events = 0 then 0
            when Number_Events between 1    and 9 then 1
            when Number_Events between 10   and 49 then 10
            when Number_Events between 50   and 99 then 50
            when Number_Events between 100  and 199 then 100
            when Number_Events between 200  and 499 then 200
            when Number_Events between 500  and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events between 100000 and 999999 then 100000
            when Number_Events between 1000000 and 9999999 then 1000000
            when Number_Events >= 10000000 then 10000000
        end as sample_slot
        ,count(*) as Number_Segments
from 
    (
    select viewing_type_detailed 
            ,event_start_dow
            ,event_Start_hour
            ,pack_grp
            ,genre_description
            ,sum(number_events) as Number_Events
    from (
            select * from igonorp.Num_Events_Seg_2 union
            select * from Num_Events_0_Seg_2
        ) t1
    where viewing_type_detailed = 0
    and event_start_hour in (20,21,22,23,0,1,2,3)
    group by viewing_type_detailed
            ,event_start_dow
            ,event_Start_hour
            ,pack_grp
            ,genre_description
    ) t2
group by sample_slot
) seg2
on base.sample_slot = seg2.sample_slot
order by base.sample_slot

-- List all buckets
select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,pack_grp
       ,genre_description
       ,number_events
from (
      select * from igonorp.Num_Events_Seg_2 union
      select * from Num_Events_0_Seg_2
     ) t1
where viewing_type_detailed = 0
and event_start_hour in (20,21,22,23,0,1,2,3)

-----------------------------------------------------------------
-- Distribution of live for 4-19h
-----------------------------------------------------------------
select base.sample_slot
        ,coalesce(seg3.Number_segments,0) as Number_Segments_3
from (
select 0 as sample_slot union
select 1 as sample_slot union
select 10 as sample_slot union
select 50 as sample_slot union
select 100 as sample_slot union
select 200 as sample_slot union
select 500 as sample_slot union
select 1000 as sample_slot union
select 10000 as sample_slot union
select 100000 as sample_slot union
select 1000000 as sample_slot union
select 10000000 as sample_slot
) base
left join
(
select case 
            when Number_Events = 0 then 0
            when Number_Events between 1    and 9 then 1
            when Number_Events between 10   and 49 then 10
            when Number_Events between 50   and 99 then 50
            when Number_Events between 100  and 199 then 100
            when Number_Events between 200  and 499 then 200
            when Number_Events between 500  and 999 then 500
            when Number_Events between 1000 and 9999 then 1000
            when Number_Events between 10000 and 99999 then 10000
            when Number_Events between 100000 and 999999 then 100000
            when Number_Events between 1000000 and 9999999 then 1000000
            when Number_Events >= 10000000 then 10000000
        end as sample_slot
        ,count(*) as Number_Segments
from 
    (
    select viewing_type_detailed 
            ,event_start_dow
            ,event_Start_hour
            ,pack_grp
            ,genre_description
            ,box_subscription
            ,sum(number_events) as Number_Events
    from (
        select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,pack_grp
       ,genre_description
       ,box_subscription
       ,number_events 
      from igonorp.Num_Events_Seg_3
        UNION
      select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,pack_grp
       ,genre_description
       ,box_subscription
       ,number_events 
      from Num_Events_0_Seg_3
        ) t1
    where viewing_type_detailed = 0
    and event_start_hour between 4 and 19
    group by viewing_type_detailed
            ,event_start_dow
            ,event_Start_hour
            ,pack_grp
            ,genre_description
            ,box_subscription
    ) t2
group by sample_slot
) seg3
on base.sample_slot = seg3.sample_slot
order by base.sample_slot

-- List all buckets
select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,pack_grp
       ,genre_description
       ,box_subscription
       ,number_events
from (
      select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,pack_grp
       ,genre_description
       ,box_subscription
       ,number_events 
      from igonorp.Num_Events_Seg_3
        UNION
      select viewing_type_detailed
       ,event_start_dow
       ,event_Start_hour
       ,pack_grp
       ,genre_description
       ,box_subscription
       ,number_events 
      from Num_Events_0_Seg_3
     ) t1
where viewing_type_detailed = 0
and event_start_hour between 4 and 19




