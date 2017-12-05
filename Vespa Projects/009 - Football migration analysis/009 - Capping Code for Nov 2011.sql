/*##################################################################################
*   FILE HEADER
*****************************************************************************
*   Product:          SQL
*   Version:          1.0
*   Author:           Gavin Meggs
*   Creation Date:    07/11/2011
*   Description:      Reworked 19/12/11 for November 2011 Capping 
*
*###################################################################################
*
*   Process depends on: - 
*
*###################################################################################
*   REVISION HISTORY
************************************************************************************
*   Date    Author Version   Description
*   07/11/2011   GM    1.0   Initial version
*   19/12/2011   DB    1.1   November Capping Values
*###################################################################################
*   DESCRIPTION
*   
*   Creates two tables to store the capping thresholds for the month of August 2011
*
*   MAXIMUM duration lengths are in 'vespa_201111_max_caps'
*   Assumes that the cut-off point is the min of the 100th percentile & therefore
*   the views with the top 1% of duration length are flagged to be capped
*   Capping is currently performed at the level of
*       event_start_day (date)
*       event_start_hour (integer in range 0-23)
*       live (integer in range 0-1)
*   the threshold/ cutoff is stored in the variable min_dur_mins (integer)
*       
*   MINIMUM duration lenghts are in 'vespa_201108_min_cap'
*   This is currently set to a single value - 6 seconds. Any events less than 6
*   seconds can be flagged for capping
*
*##################################################################################*/
-- variable creation - run once only
CREATE VARIABLE @var_rep_period_start   date;
CREATE VARIABLE @var_rep_period_end     date;
CREATE VARIABLE @var_sql varchar(15000);
CREATE VARIABLE @var_cntr smallint;

SET @var_rep_period_start  = '2011-11-01';
SET @var_rep_period_end    = '2011-11-30';

-- drop the table if it exists
IF object_id('vespa_201111_max_caps') IS NOT NULL DROP TABLE vespa_201111_max_caps;
-- recreate table
create table vespa_201111_max_caps
(
    event_start_day as date
    , event_start_hour as integer
    , live as smallint
    , ntile_100 as integer
    , min_dur_mins as integer
)

-- start at 0 and loop until you reach the end of the reporting period
SET @var_cntr = 0;
FLT_1: LOOP

    -- drop the temp table if it's there already
    SET @var_sql = 'IF object_id(''gm_ntile_temp2011111'') IS NOT NULL DROP TABLE gm_ntile_temp2011111';
    EXECUTE(@var_sql);
    commit;

    -- create a temp table storing the relevant data for the given day
    SET @var_sql = 
    'select
        account_number
        , subscriber_id
        , adjusted_event_start_time 
        , x_event_duration
        , case when play_back_speed is null then 1 else 0 end as live
        , date(adjusted_event_start_time) as event_start_day
        , datepart(hour, adjusted_event_start_time) as event_start_hour
        , cast(x_event_duration/ 60 as int) as dur_mins
    into
        gm_ntile_temp2011111
    from
        sk_prod.VESPA_STB_PROG_EVENTS_' || replace(cast(dateadd(day, @var_cntr, @var_rep_period_start) as varchar(10)), '-', '') ||
    ' where
        video_playing_flag = 1
        and adjusted_event_start_time <> x_adjusted_event_end_time
        and (x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'') 
            or (x_type_of_viewing_event = (''Other Service Viewing Event'') and x_si_service_type = ''High Definition TV test service''))
        and panel_id in (4,5)
    -- limit view to event_views that are a day or less long
        and cast(x_event_duration/ 86400 as int) = 0
    group by account_number
            ,subscriber_id
            ,adjusted_event_start_time
            ,x_event_duration
            ,live
    ';
    EXECUTE(@var_sql);
    commit;
    
    -- create indexes to speed up the ntile creation
    create hng index idx1 on gm_ntile_temp2011111(event_start_day);
    create hng index idx2 on gm_ntile_temp2011111(event_start_hour);
    create hng index idx3 on gm_ntile_temp2011111(live);
    create hng index idx4 on gm_ntile_temp2011111(dur_mins);
    
    -- query ntiles for given date and insert into the persistent table
    insert into vespa_201111_max_caps
    (
    select
            event_start_day
            , event_start_hour
            , live
            , ntile_100
            , min(dur_mins) as min_dur_mins
        from
        (
            select
                event_start_day
                ,event_start_hour
                ,live
                ,dur_mins
                ,ntile(100) over (partition by event_start_day, event_start_hour, live order by dur_mins) as ntile_100
            into ntiles
            from gm_ntile_temp2011111
        ) a
        where
            ntile_100 = 100
        group by
            event_start_day
            , event_start_hour
            , live
            , ntile_100
    );
    commit;

    -- move along to next day
    SET @var_cntr = @var_cntr + 1;
    -- exit loop if we're going past the end date
    IF dateadd(day, @var_cntr, @var_rep_period_start) > @var_rep_period_end THEN LEAVE FLT_1; END IF;

END LOOP FLT_1;
-- tidy up
IF object_id('gm_ntile_temp2011111') IS NOT NULL DROP TABLE gm_ntile_temp2011111;

-- add indexes to vespa_201111_max_caps
create hng index idx1 on vespa_201111_max_caps(event_start_day);
create hng index idx2 on vespa_201111_max_caps(event_start_hour);
create hng index idx3 on vespa_201111_max_caps(live);

-- create min caps
IF object_id('vespa_201111_min_cap') IS NOT NULL DROP TABLE vespa_201111_min_cap;
create table vespa_201111_min_cap (
    cap_secs as integer
);
insert into vespa_201111_min_cap (cap_secs) values (6);


grant select on vespa_analysts.vespa_201111_max_caps to public;
grant select on vespa_analysts.vespa_201111_min_cap to public; 

---Add Values for 4 am on 7th and 3 and 4 am on 8th for Playback Activity - Set to 75min---

input into vespa_analysts.vespa_201111_max_caps from 
'C:\Users\barnetd\Documents\Project 009 - Retaining an audience\November Capping Data Additions.csv' format ascii;



--drop table vespa_analysts.vespa_201111_max_caps_test;

-- select count(*) from vespa_201111_max_caps
-- 1440 = 30*48 (48 records per day) - check   - Comes out at 1437
--select * from vespa_201111_max_caps order by event_start_day ,event_start_hour , live
--select  event_start_day , count(*) , sum(live) from vespa_201111_max_caps group by event_start_day order by event_start_day;
--select  event_start_day , count(*) , sum(live) from vespa_201111_max_caps_test group by event_start_day order by event_start_day;
