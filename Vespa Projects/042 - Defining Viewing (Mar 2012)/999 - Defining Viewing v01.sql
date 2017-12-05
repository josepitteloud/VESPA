---create Capping table for January and Feb---

------Revised to cap at 90% rather than 99% 

---Run Capping for Jan/feb first - do not remove any activity <=5 seconds for this activity--

/*
--------------------------------------------------------------------------------
-- A  - SET UP.
--------------------------------------------------------------------------------
-- create and populate variables
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;

CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @i                      integer;

SET @var_period_start           = '2012-01-01';
SET @var_period_end             = '2012-03-01';

--------------------------------------------------------------------------------
-- PART B - Capping
--------------------------------------------------------------------------------
--         B01 - Identify extream viewing and populate max and min daily caps
--         B02 - Apply capping to the viewing data


--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
-- B01 - Identify extreme viewing and populate max and min daily caps
--------------------------------------------------------------------------------

-- Max Caps:

IF object_id('vespa_max_caps_jan_feb_2012') IS NOT NULL DROP TABLE vespa_max_caps_jan_feb_2012;

create table vespa_max_caps_jan_feb_2012
(
    event_start_day as date
    , event_start_hour as integer
    , live as smallint
    , ntile_100 as integer
    , min_dur_mins as integer
);
*/
-- loop through the viewing data to identify caps
SET @var_cntr = 0;
set @i=datediff(dd,@var_period_start,@var_period_end);
--select @i;


WHILE @var_cntr <= @i

BEGIN

    SET @var_sql = 'IF object_id(''gm_ntile_temph_db'') IS NOT NULL DROP TABLE gm_ntile_temph_db'
    EXECUTE(@var_sql)
    commit

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
        gm_ntile_temph_db
    from
        sk_prod.VESPA_STB_PROG_EVENTS_' || replace(cast(dateadd(day, @var_cntr, @var_period_start) as varchar(10)), '-', '') ||
   '  where
        video_playing_flag = 1
        and adjusted_event_start_time <> x_adjusted_event_end_time
        and (x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
            or (x_type_of_viewing_event = (''Other Service Viewing Event'') and x_si_service_type = ''High Definition TV test service''))
        and panel_id in (4,5)
        and cast(x_event_duration/ 86400 as int) = 0
    group by account_number
            ,subscriber_id
            ,adjusted_event_start_time
            ,x_event_duration
            ,live
    '
    EXECUTE(@var_sql)
    commit

--select @var_sql


    -- create indexes to speed up the ntile creation
    create hng index idx1 on gm_ntile_temph_db(event_start_day)
    create hng index idx2 on gm_ntile_temph_db(event_start_hour)
    create hng index idx3 on gm_ntile_temph_db(live)
    create hng index idx4 on gm_ntile_temph_db(dur_mins)

    -- query ntiles for given date and insert into the persistent table
    insert into vespa_max_caps_jan_feb_2012
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
            into ntilesh
            from gm_ntile_temph_db
        ) a
        where ntile_100 = 91 -- modify this to adapt aggressiveness of capping, 91 means exclude top 10% of values
        group by
            event_start_day
            , event_start_hour
            , live
            , ntile_100
    )
    commit

    SET @var_cntr = @var_cntr + 1
END;

IF object_id('gm_ntile_temph_db') IS NOT NULL DROP TABLE gm_ntile_temph_db;
-- add indexes
create hng index idx1 on vespa_max_caps_jan_feb_2012(event_start_day);
create hng index idx2 on vespa_max_caps_jan_feb_2012(event_start_hour);
create hng index idx3 on vespa_max_caps_jan_feb_2012(live);

---Min Cap set to 1 so no viewing will be removed but code kept consistent--

-- Min Caps
IF object_id('vespa_min_cap_jan_feb_2012') IS NOT NULL DROP TABLE vespa_min_cap_jan_feb_2012;
create table vespa_min_cap_jan_feb_2012 (
    cap_secs as integer
);

insert into vespa_min_cap_jan_feb_2012 (cap_secs) values (1);

commit;


--select event_start_day , count(*) from vespa_max_caps_jan_feb_2012 group by event_start_day order by event_start_day

