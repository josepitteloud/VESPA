/*##################################################################################
*   FILE HEADER
*****************************************************************************
*   Product:          SQL
*   Version:          4.0
*   Author:           Gavin Meggs
*   Creation Date:    17/11/2011
*   Description:
*
*###################################################################################
*
*   Process depends on:	The code to perform the capping below we require access to the 
*			following two tables:
*				vespa_analysts.vespa_201108_max_caps
*				vespa_analysts.vespa_201108_min_cap
*
*###################################################################################
*   REVISION HISTORY
************************************************************************************
*   Date    Author Version   Description
*   04/11/2011   GM    1.0      Initial version
*   07/11/2011   GM    2.0      Updated code to include capping of recorded data
*   07/11/2011	 GM    3.0	Updated code to reference out to external table storing caps
*				(see 20111107 Viewing capping limits v1.0.sql)
*
*###################################################################################
*   DESCRIPTION
*   
*   Creates example capping tables for maximum length duration (based on date,
*   viewing start hour and live viewing flag) and then uses a sample of records
*   to demonstrate the addition of 4 new fields to those records to capture
*   capped viewing records
*
*   Additional fields added are:
*       capped_x_viewing_start_time
*       capped_x_viewing_end_time
*       capped_x_programme_viewed_duration
*       capped_flag
*   the first three fields are the capped variants of the associated x_* fields
*   the capped_flag field contains the following values
*       0 programme view not affected by capping
*       1 if programme view has been shortened by a long duration capping rule
*       2 if programme view has been excluded by a long duration capping rule
*       3 if programme view has been excluded by the short duration capping rule
*
*   once records have been capped those with a capped_flag value of 0 or 1 should
*   be included in analysis
*
*##################################################################################*/
-- create table including base records you need
IF object_id('vespa_analysts_gm_capping_test_dbarnett') IS NOT NULL DROP TABLE vespa_analysts_gm_capping_test_dbarnett;
select 
-- the following fields are required by subsequent code
    subscriber_id
    , adjusted_event_start_time
    , x_adjusted_event_end_time
    , recorded_time_utc
    , case when play_back_speed is null then 1 else 0 end as live
    , x_programme_viewed_duration
-- the following 2 fields are required by AND EFFECTED BY FOR PLAYBACK RECORDS in subsequent code
    , x_viewing_start_time
    , x_viewing_end_time
    
-- the following line is included here as it doesn't appear it's possible to run this within an update statement
    , sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration 
into
    vespa_analysts_gm_capping_test_dbarnett
from
-- test for a day in August where we have capping rules...
    sk_prod.VESPA_STB_PROG_EVENTS_20110801
where
    video_playing_flag = 1
     and adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in ('TV Channel Viewing','Sky+ time-shifted viewing event')
          or (x_type_of_viewing_event = ('Other Service Viewing Event')
              and x_si_service_type = 'High Definition TV test service'))
     and panel_id = 5
-- ** test case for a given subscriber
--     and subscriber_id = 21166009 -- (the most popular subscriber for 2011-08-01) - try also 8864136, 21166009, 16867263, 13876186
     and right(cast(subscriber_id as varchar),2) = '09' -- 1% Sample
;
commit;

--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20110801;

-- add indexes to improve performance
create hg index idx1 on vespa_analysts_gm_capping_test_dbarnett(subscriber_id);
create dttm index idx2 on vespa_analysts_gm_capping_test_dbarnett(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts_gm_capping_test_dbarnett(recorded_time_utc);
create lf index idx4 on vespa_analysts_gm_capping_test_dbarnett(live)
create dttm index idx5 on vespa_analysts_gm_capping_test_dbarnett(x_viewing_start_time);
create dttm index idx6 on vespa_analysts_gm_capping_test_dbarnett(x_viewing_end_time);
create hng index idx7 on vespa_analysts_gm_capping_test_dbarnett(x_cumul_programme_viewed_duration);

-- append fields to table to store additional metrics for capping
alter table vespa_analysts_gm_capping_test_dbarnett
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records
update vespa_analysts_gm_capping_test_dbarnett
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts_gm_capping_test_dbarnett
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts_gm_capping_test_dbarnett
    set capped_x_viewing_start_time =
        case  
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
        end
        , capped_x_viewing_end_time =
        case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
        end
from
        vespa_analysts_gm_capping_test_dbarnett base left outer join vespa_201108_max_caps caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts_gm_capping_test_dbarnett
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts_gm_capping_test_dbarnett
    set capped_flag = 
        case
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            when capped_x_viewing_start_time is null then 2
            else 0
        end
;
commit;

-- cap based on min duration of seconds (from min_cap) and set capping flag
-- this nullifies capped_x times as for long duration cap and sets capped_flag = 3
-- note that some capped_flag = 1 records may also be updated if the capping of the end of
-- a long view resulted in a very short view
update vespa_analysts_gm_capping_test_dbarnett
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
        vespa_201108_min_cap
    where
        capped_x_programme_viewed_duration < cap_secs 
;


select dateformat(adjusted_event_start_time,'hh') as hour , live, sum(datediff(minute,x_viewing_start_time,x_viewing_end_time)) as viewing_time , sum(datediff(minute,capped_x_viewing_start_time,capped_x_viewing_end_time)) as capped_viewing_time
 from vespa_analysts_gm_capping_test_dbarnett 
group by live,hour
order by live,hour

commit;

--select top 500 * from vespa_analysts_gm_capping_test_dbarnett where capped_flag = 1 order by adjusted_event_start_time desc
--select  * from vespa_analysts_gm_capping_test_dbarnett where subscriber_id = 14007009 order by adjusted_event_start_time; output to 'C:\Users\barnetd\Documents\Project 002 - Ad Analysis Trollied\capping example.xls' format excel; 

--select top 500 * from vespa_analysts_gm_capping_test_dbarnett where subscriber_id = 396309 order by adjusted_event_start_time 

--select * from sk_prod.VESPA_STB_PROG_EVENTS_20110801 where subscriber_id = 396309 order by adjusted_event_start_time 


--select top 500 * from vespa_analysts_gm_capping_test_dbarnett where live=0 and capped_flag=1
/** play below the line **/ 
-- select * from vespa_analysts_gm_capping_test_dbarnett order by adjusted_event_start_time, x_viewing_start_time
-- select capped_flag, count(*) as rec_count from vespa_analysts_gm_capping_test_dbarnett group by capped_flag
-- select subscriber_id, count(*) rec_count from sk_prod.VESPA_STB_PROG_EVENTS_20110801 where recorded_time_utc is not null group by subscriber_id order by rec_count desc


--select * from vespa_analysts.vespa_201108_max_caps where live = 0;


/*
select base.* , caps.min_dur_mins
    
     ,   case  
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
        end as capped_x_viewing_start_time
        , 
        case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
        end as capped_x_viewing_end_time
from
        vespa_analysts_gm_capping_test_dbarnett base left outer join vespa_201108_max_caps caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
where subscriber_id = 25787209
;

select panel_id , count(distinct subscriber_id) as subs , count(*) as rec_count from sk_prod.VESPA_STB_PROG_EVENTS_20111118 group by panel_id 

select * from sk_prod.vespa_epg_dim where programme_trans_sk
in (
201108120000000728,
201108120000014061,
201108120000002465,
201108120000000992

)

--also find prog on before trollied for pre-ad
commit;
 select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_end_datetime_utc = '2011-08-11 20:00:00'

select programme_trans_sk from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_end_datetime_utc = '2011-08-11 20:00:00'

programme_trans_sk
201108120000014047
201108120000000714
201108120000002451







*/
