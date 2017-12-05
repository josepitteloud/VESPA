---create Capping table for January and Feb---

------Revised to cap at 90% rather than 99% 

---Run Capping for Jan/feb first - do not remove any activity <=5 seconds for this activity--


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

insert into vespa_min_cap_jan_feb_2012 (cap_secs) values (6);

commit;


--select event_start_day , count(*) from vespa_max_caps_jan_feb_2012 group by event_start_day order by event_start_day

---Viewing---
----Part A02 Viewing Data for Programme ----


--------------------------------------------------------------------------------
-- PART A02 Viewing Data
--------------------------------------------------------------------------------

---Looking at 31 days worth of tables but only return viewing for 15th Jan 2012

---Also for initial part of query looking at all records, not just live/regular speed playback.



/*
PART A01 - Populate all viewing data between around 15th Jan--
--select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


--select programme_trans_sk from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc

*/
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-01-15';
SET @var_prog_period_end    = '2012-02-15';


SET @var_cntr = 0;
SET @var_num_days = 31;       -- 

-- To store all the viewing records:
create table vespa_analysts.VESPA_all_viewing_records_20120115 ( -- drop table vespa_analysts.VESPA_all_viewing_records_20120115
    cb_row_ID                       bigint      not null primary key
    ,Account_Number                 varchar(20) not null
    ,Subscriber_Id                  decimal(8,0) not null
    ,Cb_Key_Household               bigint
    ,Cb_Key_Family                  bigint
    ,Cb_Key_Individual              bigint
    ,Event_Type                     varchar(20) not null
    ,X_Type_Of_Viewing_Event        varchar(40) not null
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,X_Viewing_Start_Time           datetime
    ,X_Viewing_End_Time             datetime
    ,Tx_Start_Datetime_UTC          datetime
    ,Tx_End_Datetime_UTC            datetime
    ,Recorded_Time_UTC              datetime
    ,Play_Back_Speed                decimal(4,0)
    ,X_Event_Duration               decimal(10,0)
    ,X_Programme_Duration           decimal(10,0)
    ,X_Programme_Viewed_Duration    decimal(10,0)
    ,X_Programme_Percentage_Viewed  decimal(3,0)
    ,X_Viewing_Time_Of_Day          varchar(15)
    ,Programme_Trans_Sk             bigint      not null
    ,Channel_Name                   varchar(30)
    ,Epg_Title                      varchar(50)
    ,Genre_Description              varchar(30)
    ,Sub_Genre_Description          varchar(30)
    ,x_cumul_programme_viewed_duration bigint
);
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into vespa_analysts.VESPA_all_viewing_records_20120115
    select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
, sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration 
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
--video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
--     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
--          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
--              and x_si_service_type = ''High Definition TV test service''))
     and panel_id in (4,5)
and (cast(Adjusted_Event_Start_Time as date) between ''2012-01-14'' and ''2012-01-15'' or cast(Recorded_Time_UTC as date) between ''2012-01-14'' and ''2012-01-15'') '

      ;

--select top 1000 * from  sk_prod.VESPA_STB_PROG_EVENTS_20120115;
  -- ####### Loop through to populate table: Sybase Interactive style (not entirely tested) ######
--FLT_1: LOOP

    --EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd'));

    --SET @var_cntr = @var_cntr + 1;
    --IF @var_cntr > @var_num_days THEN LEAVE FLT_1;
    --END IF ;

--END LOOP FLT_1;
  -- ####### End of loop (this loop structure not tested yet) ######

  -- ####### Alternate Loop: WinSQL style (tested, good) ######
while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


--select play_back_speed , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by day_view order by day_view;


commit;

alter table vespa_analysts.VESPA_all_viewing_records_20120115 add live tinyint;

update vespa_analysts.VESPA_all_viewing_records_20120115
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.VESPA_all_viewing_records_20120115
;
commit;

if object_id('vespa_analysts.channel_name_lookup_old') is not null drop table vespa_analysts.channel_name_lookup_old;
create table vespa_analysts.channel_name_lookup_old 
(channel varchar(90)
,channel_name_grouped varchar(90)
,channel_name_inc_hd varchar(90)
)
;
commit;
input into vespa_analysts.channel_name_lookup_old from 'G:\RTCI\Sky Projects\Vespa\Phase1b\Channel Lookup\Channel Lookup Info Phase1b.csv' format ascii;
commit;

alter table vespa_analysts.VESPA_all_viewing_records_20120115 add channel_name_inc_hd varchar(40);

update vespa_analysts.VESPA_all_viewing_records_20120115
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.VESPA_all_viewing_records_20120115 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;


-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115(subscriber_id);
create dttm index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115(recorded_time_utc);
create lf index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115(live)
create dttm index idx5 on vespa_analysts.VESPA_all_viewing_records_20120115(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.VESPA_all_viewing_records_20120115(x_viewing_end_time);
create hng index idx7 on vespa_analysts.VESPA_all_viewing_records_20120115(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.VESPA_all_viewing_records_20120115(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.VESPA_all_viewing_records_20120115
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.VESPA_all_viewing_records_20120115
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.VESPA_all_viewing_records_20120115
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.VESPA_all_viewing_records_20120115
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
        vespa_analysts.VESPA_all_viewing_records_20120115 base left outer join vespa_max_caps_jan_feb_2012 caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.VESPA_all_viewing_records_20120115
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.VESPA_all_viewing_records_20120115
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
update vespa_analysts.VESPA_all_viewing_records_20120115
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
        vespa_201111_min_cap
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;

--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20120115 where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

---Deletion of capped records commented out initially - for evaluation purposes---


/*
delete from vespa_analysts.VESPA_all_viewing_records_20120115
where capped_flag in (2,3)
;
commit;
*/


---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.VESPA_all_viewing_records_20120115 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.VESPA_all_viewing_records_20120115 add viewing_record_start_time_local datetime;


alter table vespa_analysts.VESPA_all_viewing_records_20120115 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.VESPA_all_viewing_records_20120115 add viewing_record_end_time_local datetime;

update vespa_analysts.VESPA_all_viewing_records_20120115
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.VESPA_all_viewing_records_20120115
;
commit;


---
update vespa_analysts.VESPA_all_viewing_records_20120115
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.VESPA_all_viewing_records_20120115
;
commit;

--select top 100 * from vespa_analysts.VESPA_all_viewing_records_20120115;

update vespa_analysts.VESPA_all_viewing_records_20120115
set viewing_record_start_time_local= case 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,viewing_record_start_time_utc) 
                    else viewing_record_start_time_utc  end
,viewing_record_end_time_local=case 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,viewing_record_end_time_utc) 
                    else viewing_record_end_time_utc  end
from vespa_analysts.VESPA_all_viewing_records_20120115
;
commit;


--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115;

