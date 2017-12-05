





------------------------------------------------
-- Capped Events Only: Duration saved / lost through algorithm

select
        case
                when save_lost_time < -100 then -999
                when save_lost_time > 120 then 999
                else save_lost_time
        end as duration_group
        ,sum(event_count) as tot_event_count
        ,sum(scaled_event_count) as tot_scaled_event_count
        ,sum(scaled_save_lost_time) as tot_scaled_save_lost_time
from

(select
        round(case
                when stb_cap >= datediff(ss, event_start_time, cap_end_time) /60 then
                        -- threshold > capped duration so ads saved when they should
                        case
                                when datediff(ss, event_start_time, event_end_time) /60 - stb_cap < 0 then
                                        0 -- event end is before threshold so no ads saved
                                else
                                        datediff(ss, event_start_time, event_end_time) /60 - stb_cap -- time saved by thresholds
                        end
                else
                        -- threshold < capped duration so ads lost when they shouldn't be
                         stb_cap - datediff(ss, event_start_time, cap_end_time) /60  -- time lost
        end, 0) as save_lost_time
        ,count(1) as event_count
        ,sum(scaling_weight) as scaled_event_count
      --  ,sum(scaling_weight * stb_cap) as scaled_stb_cap
      --  ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as scaled_capped
      --  ,sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60) as scaled_event
        ,sum(scaling_weight * save_lost_time) as scaled_save_lost_time
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
group by
        save_lost_time
) a

group by duration_group;



------------------------------------------------
-- UnCapped Events Only: Duration saved / lost through algorithm

select
        case
                when save_lost_time < -100 then -999
                when save_lost_time > 120 then 999
                else save_lost_time
        end as duration_group
        ,sum(event_count) as tot_event_count
        ,sum(scaled_event_count) as tot_scaled_event_count
        ,sum(scaled_save_lost_time) as tot_scaled_save_lost_time
from

(select
        round(stb_cap - datediff(ss, event_start_time, event_end_time) /60, 0) as save_lost_time
        ,count(1) as event_count
        ,sum(scaling_weight) as scaled_event_count
      --  ,sum(scaling_weight * stb_cap) as scaled_stb_cap
      --  ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as scaled_capped
      --  ,sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60) as scaled_event
        ,sum(scaling_weight * save_lost_time) as scaled_save_lost_time
from
        STBCAP_viewing_data
where
        (cap_end_time is null or cap_end_time >= event_end_time) -- uncapped events only
         and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
group by
        save_lost_time
) a

group by duration_group;


----------------------------------------------------------------------
-- Total Event Duration


select
        sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60) as event_duration
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as cap_duration
        ,sum(scaling_weight) as event_count
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
union

select
        sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60) as event_duration
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as cap_duration
        ,sum(scaling_weight) as event_count
from
        STBCAP_viewing_data
where
        (cap_end_time is null or cap_end_time >= event_end_time) -- uncapped events only
         and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
;



------------------------------------------------
-- Capped Events Only: Threshold - Capped

select
        case
                when stb_minus_cap < -100 then -999
                when stb_minus_cap > 120 then 999
                else stb_minus_cap
        end as duration_group
        ,sum(event_count) as tot_event_count
        ,sum(scaled_event_count) as tot_scaled_event_count
        ,sum(scaled_stb_cap) as tot_scaled_stb_cap
        ,sum(scaled_capped) as tot_scaled_capped
        ,sum(scaled_stb_minus_cap) as tot_scaled_stb_minus_cap
from

(select
        stb_cap - datediff(ss, event_start_time, cap_end_time) /60 as stb_minus_cap
        ,count(1) as event_count
        ,sum(scaling_weight) as scaled_event_count
        ,sum(scaling_weight * stb_cap) as scaled_stb_cap
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as scaled_capped
        ,sum(scaling_weight * stb_minus_cap) as scaled_stb_minus_cap
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universesand scaling_universe like 'AdSmartable Opted In'
group by
        stb_minus_cap
) a
group by duration_group;



--------------------------------------------------------------
--------------------------------------------------------------
--------------------------------------------------------------
-- More accurate Version


-- Capped: Duration saved/lost
select
        case
                when save_lost_time < -100 then -999
                when save_lost_time > 120 then 999
                else save_lost_time
        end as duration_group
        ,sum(event_count) as tot_event_count
        ,sum(scaled_event_count) as tot_scaled_event_count
        ,sum(scaled_save_lost_time) as tot_scaled_save_lost_time
from

(select
        round(case
                when stb_cap >= datediff(ss, event_start_time, cap_end_time) /60 then
                        -- threshold > capped duration so ads saved when they should
                        case
                                when datediff(ss, event_start_time, event_end_time) /60 - stb_cap < 0 then
                                        0 -- event end is before threshold so no ads saved
                                else
                                        datediff(ss, event_start_time, event_end_time) /60 - stb_cap -- time saved by thresholds
                        end
                else
                        -- threshold < capped duration so ads lost when they shouldn't be
                         stb_cap - datediff(ss, event_start_time, cap_end_time) /60  -- time lost
        end, 0) as save_lost_time
        ,count(1) as event_count
        ,sum(scaling_weight) as scaled_event_count
      --  ,sum(scaling_weight * stb_cap) as scaled_stb_cap
      --  ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as scaled_capped
      --  ,sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60) as scaled_event
        ,sum(scaling_weight *
                case
                when stb_cap >= datediff(ss, event_start_time, cap_end_time) /60.00 then
                        -- threshold > capped duration so ads saved when they should
                        case
                                when datediff(ss, event_start_time, event_end_time) /60.00 - stb_cap < 0 then
                                        0 -- event end is before threshold so no ads saved
                                else
                                        datediff(ss, event_start_time, event_end_time) /60.00 - stb_cap -- time saved by thresholds
                        end
                else
                        -- threshold < capped duration so ads lost when they shouldn't be
                         stb_cap - datediff(ss, event_start_time, cap_end_time) /60.00  -- time lost
                end
                ) as scaled_save_lost_time
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In'
group by
        save_lost_time
) a

group by duration_group;




-- Uncapped: Duration saved/lost
select
        case
                when save_lost_time < -100 then -999
                when save_lost_time > 120 then 999
                else save_lost_time
        end as duration_group
        ,sum(event_count) as tot_event_count
        ,sum(scaled_event_count) as tot_scaled_event_count
        ,sum(scaled_save_lost_time) as tot_scaled_save_lost_time
from

(select
        round(stb_cap - datediff(ss, event_start_time, event_end_time) /60, 0) as save_lost_time
        ,count(1) as event_count
        ,sum(scaling_weight) as scaled_event_count
      --  ,sum(scaling_weight * stb_cap) as scaled_stb_cap
      --  ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60) as scaled_capped
      --  ,sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60) as scaled_event
        ,sum(scaling_weight * (stb_cap - datediff(ss, event_start_time, event_end_time) /60.00)) as scaled_save_lost_time
from
        STBCAP_viewing_data
where
        (cap_end_time is null or cap_end_time >= event_end_time) -- uncapped events only
        and scaling_universe like 'AdSmartable Opted In'
group by
        save_lost_time
) a

group by duration_group;




-- Total Event Duration
select
        sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60.00) as event_duration
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60.00) as cap_duration
        ,sum(scaling_weight) as event_count
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
union

select
        sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60.00) as event_duration
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60.00) as cap_duration
        ,sum(scaling_weight) as event_count
from
        STBCAP_viewing_data
where
        (cap_end_time is null or cap_end_time >= event_end_time) -- uncapped events only
         and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
;


-- Total Event Duration: Includes STB Cap Duration
select
        sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60.00) as event_duration
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60.00) as cap_duration
        ,sum(scaling_weight *
                        case when stb_cap <= datediff(ss, event_start_time, event_end_time) /60.00 then -- check that STB cap is less than actual event duration
                                        stb_cap
                                  else
                                        datediff(ss, event_start_time, event_end_time) /60.00
                        end) as stb_cap_duration
        ,sum(scaling_weight) as event_count
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes
union

select
        sum(scaling_weight * datediff(ss, event_start_time, event_end_time) /60.00) as event_duration
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60.00) as cap_duration
        ,sum(scaling_weight *
                        case when stb_cap <= datediff(ss, event_start_time, event_end_time) /60.00 then -- check that STB cap is less than actual event duration
                                        stb_cap
                                  else
                                        datediff(ss, event_start_time, event_end_time) /60.00
                        end) as stb_cap_duration
        ,sum(scaling_weight) as event_count
from
        STBCAP_viewing_data
where
        (cap_end_time is null or cap_end_time >= event_end_time) -- uncapped events only
         and scaling_universe like 'AdSmartable Opted In' -- Pre 1 April we haven't got the Adsmart universes

;



-- Capped: Threshold - Capped
select
        case
                when stb_minus_cap < -100 then -999
                when stb_minus_cap > 120 then 999
                else stb_minus_cap
        end as duration_group
        ,sum(event_count) as tot_event_count
        ,sum(scaled_event_count) as tot_scaled_event_count
        ,sum(scaled_stb_cap) as tot_scaled_stb_cap
        ,sum(scaled_capped) as tot_scaled_capped
        ,sum(scaled_stb_minus_cap) as tot_scaled_stb_minus_cap
from

(select
        stb_cap - datediff(ss, event_start_time, cap_end_time) /60 as stb_minus_cap
        ,count(1) as event_count
        ,sum(scaling_weight) as scaled_event_count
        ,sum(scaling_weight * stb_cap) as scaled_stb_cap
        ,sum(scaling_weight * datediff(ss, event_start_time, cap_end_time) /60.00) as scaled_capped
        ,sum(scaling_weight * (stb_cap - datediff(ss, event_start_time, cap_end_time) /60.00)) as scaled_stb_minus_cap
from
        STBCAP_viewing_data
where
        not (cap_end_time is null or cap_end_time >= event_end_time) -- capped events only
        and scaling_universe like 'AdSmartable Opted In'
group by
        stb_minus_cap
) a
group by duration_group;
