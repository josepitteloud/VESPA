
----Project 060  -----
--Add Sharepoint Direct Details Here
/*
******************************************************************************************************************


from Brief:
Objective
To compare Share Of Commercial Impacts (SOCI) as reported by BARB and VESPA for the main sales houses.  
This is intended to be a “quick and dirty” exercise to understand if there are any issues that we should be aware of.




---PART A  - Viewing of All Channels (Including Non-Commercial channels as full data needed to attribute minutes)
---PART B  - Create Capping levels using version 1b
---PART C  - Apply Capping values to viewing data
---PART D  - Apply Scaling levels using version 1b
----Tables Used/Created-----


*****************************************
*/


---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-29';
SET @var_prog_period_end    = '2012-05-07';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select top 500 * from vespa_analysts.project060_all_viewing;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing ( -- drop table vespa_analysts.project060_all_viewing
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
    ,service_key                integer
    ,original_network_id        integer
    ,transport_stream_id        integer
    ,si_service_id              integer
);

---Summer time so add 1 hour to UTC to get local time on time qualifier below
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into vespa_analysts.project060_all_viewing
    select
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
, sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration 
 ,vw.service_key                
    ,vw.original_network_id        
    ,vw.transport_stream_id        
    ,vw.si_service_id                 

 from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
              and x_si_service_type = ''High Definition TV test service''))
     and panel_id in (4,5)



and (   Adjusted_Event_Start_Time between ''2012-04-29 05:00:00'' and ''2012-04-30 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-29 05:00:00'' and ''2012-04-30 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-29 05:00:00'' and ''2012-04-30 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-29 05:00:00'' and ''2012-04-30 04:59:59''  
    )
'     ;

while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


--select play_back_speed , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by day_view order by day_view;
--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120429;

commit;

alter table vespa_analysts.project060_all_viewing add live tinyint;

update vespa_analysts.project060_all_viewing
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing
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

--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;

--
/*
select  base.Channel_Name ,sum(X_Programme_Viewed_Duration) as duration_total
from vespa_analysts.project060_all_viewing as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
where base.Channel_Name is not null and det.Channel is null
group by base.channel_name ;
*/




--select 

--select count(*) from vespa_analysts.project060_all_viewing;
--------------------------------------------------------------------------------
-- PART B - Capping
--------------------------------------------------------------------------------
--         B00 - Set up macro variables and start/end dates 
--         B01 - Identify extream viewing and populate max and min daily caps
--         B02 - Apply capping to the viewing data
--------------------------------------------------------------------------------
-- B00  - SET UP.
--------------------------------------------------------------------------------
-- create and populate variables
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;

CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @i                      integer;

SET @var_period_start           = '2012-04-29';
SET @var_period_end             = '2012-05-07';
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- B01 - Identify extreme viewing and populate max and min daily caps
--------------------------------------------------------------------------------
--select * from vespa_max_caps_apr29_2012;
-- Max Caps:
--select * from vespa_max_caps_apr29_2012;
IF object_id('vespa_max_caps_apr29_2012') IS NOT NULL DROP TABLE vespa_max_caps_apr29_2012;

create table vespa_max_caps_apr29_2012
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
    insert into vespa_max_caps_apr29_2012
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

--IF object_id('gm_ntile_temph_db') IS NOT NULL DROP TABLE gm_ntile_temph_db;
-- add indexes
create hng index idx1 on vespa_max_caps_apr29_2012(event_start_day);
create hng index idx2 on vespa_max_caps_apr29_2012(event_start_hour);
create hng index idx3 on vespa_max_caps_apr29_2012(live);

---Min Cap set to 1 so no viewing will be removed but code kept consistent--

-- Min Caps
IF object_id('vespa_min_cap_apr29_2012') IS NOT NULL DROP TABLE vespa_min_cap_apr29_2012;
create table vespa_min_cap_apr29_2012 (
    cap_secs as integer
);
insert into vespa_min_cap_apr29_2012 (cap_secs) values (6);

commit;

--------PART B2 100% Ntile version



--CREATE VARIABLE @var_period_start       datetime;
--CREATE VARIABLE @var_period_end         datetime;

--CREATE VARIABLE @var_sql                varchar(15000);
--CREATE VARIABLE @var_cntr               smallint;
--CREATE VARIABLE @var_num_days           smallint;
--CREATE VARIABLE @i                      integer;

SET @var_period_start           = '2012-04-29';
SET @var_period_end             = '2012-05-07';
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- B01 - Identify extreme viewing and populate max and min daily caps
--------------------------------------------------------------------------------

-- Max Caps:

IF object_id('vespa_max_caps_apr29_2012_100_Ntile') IS NOT NULL DROP TABLE vespa_max_caps_apr29_2012_100_Ntile;

create table vespa_max_caps_apr29_2012_100_Ntile
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
    insert into vespa_max_caps_apr29_2012_100_Ntile
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
        where ntile_100 = 100 -- modify this to adapt aggressiveness of capping, 91 means exclude top 10% of values
        group by
            event_start_day
            , event_start_hour
            , live
            , ntile_100
    )
    commit

    SET @var_cntr = @var_cntr + 1
END;

--IF object_id('gm_ntile_temph_db') IS NOT NULL DROP TABLE gm_ntile_temph_db;
-- add indexes
create hng index idx1 on vespa_max_caps_apr29_2012_100_Ntile(event_start_day);
create hng index idx2 on vespa_max_caps_apr29_2012_100_Ntile(event_start_hour);
create hng index idx3 on vespa_max_caps_apr29_2012_100_Ntile(live);


--select top 100 * from vespa_max_caps_apr29_2012_100_Ntile;
--select top 100 *  from vespa_max_caps_apr29_2012;
---Combine two versions of capping together to get 10% cap for Live and 1% cap for Playback

select * into vespa_analysts.vespa_max_caps_live_playback from vespa_max_caps_apr29_2012 where live =1;

insert into vespa_analysts.vespa_max_caps_live_playback
select *   from vespa_max_caps_apr29_2012_100_Ntile where live =0;

commit;

--select * from vespa_analysts.vespa_max_caps_live_playback;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing(live)
create dttm index idx5 on vespa_analysts.project060_all_viewing(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing
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
        vespa_analysts.project060_all_viewing base left outer join vespa_analysts.vespa_max_caps_live_playback caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing
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
update vespa_analysts.project060_all_viewing
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
        vespa_min_cap_apr29_2012
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

---Deletion of capped records commented out initially - for evaluation purposes---


/*
delete from vespa_analysts.project060_all_viewing
where capped_flag in (2,3)
;
commit;
*/


---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.project060_all_viewing add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing
;
commit;


---
update vespa_analysts.project060_all_viewing
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing
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
from vespa_analysts.project060_all_viewing
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing
set capped_x_viewing_start_time_local= case 
when dateformat(capped_x_viewing_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,capped_x_viewing_start_time) 
when dateformat(capped_x_viewing_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,capped_x_viewing_start_time) 
when dateformat(capped_x_viewing_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,capped_x_viewing_start_time) 
                    else capped_x_viewing_start_time  end
,capped_x_viewing_end_time_local= case 
when dateformat(capped_x_viewing_end_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,capped_x_viewing_end_time) 
when dateformat(capped_x_viewing_end_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,capped_x_viewing_end_time) 
when dateformat(capped_x_viewing_end_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,capped_x_viewing_end_time) 
                    else capped_x_viewing_end_time  end
from vespa_analysts.project060_all_viewing
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing add weighting double;

update vespa_analysts.project060_all_viewing 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing add pvr tinyint;

update vespa_analysts.project060_all_viewing 
set affluence=case when b.affluence is null then 'Unknown' else b.affluence end
,pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;


--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing
; 

---Add Service Key on to Create Consistent Key---
select service_key 
, ssp_network_id
,transport_id
 ,service_id 
, min(channel_name) as channel  
into vespa_analysts.project060_service_key_triplet_lookup
from sk_prod.vespa_epg_dim where tx_date = '20120429' 
group by service_key 
, ssp_network_id
,transport_id
 ,service_id  ;
commit;
create hg index idx1 on vespa_analysts.project060_service_key_triplet_lookup(ssp_network_id);
create hg index idx2 on vespa_analysts.project060_service_key_triplet_lookup(transport_id);
create hg index idx3 on vespa_analysts.project060_service_key_triplet_lookup(service_id);
commit;

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present

--Split Viewing into 3 seperate tables---

---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
select top 500  subscriber_id
,service_key
,channel_name_inc_hd
,live_timeshifted_type
,weighting

,capped_x_viewing_start_time_local 
, capped_x_viewing_end_time_local 
, viewing_record_start_time_local
 , viewing_record_end_time_local
,dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)  as real_time_minute_start
,dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)  as real_time_minute_end

,datediff(second,real_time_minute_start,capped_x_viewing_start_time_local) as seconds_from_minute_start
, viewing_record_start_time_local as first_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, first_second_viewed_in_real_time_minute), first_second_viewed_in_real_time_minute) as first_minute_of_broadcast_viewed

, case when dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)=dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)
then datepart(second, capped_x_viewing_end_time_local)  - datepart(second, capped_x_viewing_start_time_local)
  else  60 - datepart(second, capped_x_viewing_start_time) end as seconds_viewed_in_minute
, dateadd(second,seconds_viewed_in_minute-1,first_second_viewed_in_real_time_minute) as last_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, last_second_viewed_in_real_time_minute), last_second_viewed_in_real_time_minute) as last_minute_of_broadcast_viewed
,case when first_minute_of_broadcast_viewed=last_minute_of_broadcast_viewed then seconds_viewed_in_minute
      else 60 - cast(datepart(second, first_second_viewed_in_real_time_minute) as integer) end as seconds_viewed_of_first_broadcast_minute
,seconds_viewed_in_minute - seconds_viewed_of_first_broadcast_minute   as seconds_viewed_of_second_broadcast_minute
--into vespa_analysts.project060_all_viewing_odd_seconds_start_minute
from vespa_analysts.project060_all_viewing 
--from vespa_analysts.project060_all_viewing_same_minutes
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
/*
and
(
        (capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>@minute)
    or
        (capped_x_viewing_start_time_local between @minute and dateadd(second,59,@minute)))
*/
order by subscriber_id , capped_x_viewing_start_time_local
;

---Part 2 Repeat for End Seconds

select top 500  subscriber_id
,service_key
,channel_name_inc_hd
,live_timeshifted_type
,weighting

,capped_x_viewing_start_time_local 
, capped_x_viewing_end_time_local 
, viewing_record_start_time_local
 , viewing_record_end_time_local
,dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)  as real_time_minute_start
,dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)  as real_time_minute_end

,0 as seconds_from_minute_start

,dateadd(second, - datepart(second, capped_x_viewing_end_time_local), viewing_record_end_time_local) as first_second_viewed_in_real_time_minute
--, viewing_record_start_time_local as first_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, first_second_viewed_in_real_time_minute), first_second_viewed_in_real_time_minute) as first_minute_of_broadcast_viewed

, datepart(second, capped_x_viewing_end_time_local)  as seconds_viewed_in_minute
, dateadd(second,seconds_viewed_in_minute-1,first_second_viewed_in_real_time_minute) as last_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, last_second_viewed_in_real_time_minute), last_second_viewed_in_real_time_minute) as last_minute_of_broadcast_viewed
,case when first_minute_of_broadcast_viewed=last_minute_of_broadcast_viewed then seconds_viewed_in_minute
      else 60 - cast(datepart(second, first_second_viewed_in_real_time_minute) as integer) end as seconds_viewed_of_first_broadcast_minute
,seconds_viewed_in_minute - seconds_viewed_of_first_broadcast_minute   as seconds_viewed_of_second_broadcast_minute
--into vespa_analysts.project060_all_viewing_odd_seconds_end_minute
from vespa_analysts.project060_all_viewing 
--from vespa_analysts.project060_all_viewing_same_minutes
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)



/*
and
(
        (capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>@minute)
    or
        (capped_x_viewing_start_time_local between @minute and dateadd(second,59,@minute)))
*/
order by subscriber_id , capped_x_viewing_start_time_local
;


commit;


--3) Full Minute Activity


select top 500  subscriber_id
,service_key
,channel_name_inc_hd
,live_timeshifted_type
,weighting

,capped_x_viewing_start_time_local 
, capped_x_viewing_end_time_local 
, viewing_record_start_time_local
 , viewing_record_end_time_local
,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)  as real_time_minute_start
,dateadd(second,  - ( datepart(second, capped_x_viewing_end_time_local)+60), capped_x_viewing_end_time_local)  as real_time_minute_end

,case when live_timeshifted_type='01: Live' then real_time_minute_start
--             dateadd(second, 60 - datepart(second, viewing_record_start_time_local), viewing_record_start_time_local)



            when ( 60 -  datepart(second, capped_x_viewing_start_time_local)) +  datepart(second, viewing_record_start_time_local) <30
                    then  dateadd(second,  - datepart(second, viewing_record_start_time_local), viewing_record_start_time_local)
            when ( 60- datepart(second, capped_x_viewing_start_time_local)) +  datepart(second, viewing_record_start_time_local) <90
                    then dateadd(second, 60 - datepart(second, viewing_record_start_time_local), viewing_record_start_time_local)
            
            else dateadd(second, 120 -( datepart(second, viewing_record_start_time_local)), viewing_record_start_time_local)



 end as start_broadcast_minute_viewed
,datediff(minute,real_time_minute_start,real_time_minute_end) as minutes_diff
,dateadd(minute,datediff(minute,real_time_minute_start,real_time_minute_end),start_broadcast_minute_viewed) as end_broadcast_minute_viewed
--into vespa_analysts.project060_full_allocated_minutes
from vespa_analysts.project060_all_viewing 
--from vespa_analysts.project060_all_viewing_same_minutes
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_start_time_local)<>0
and 
       (        dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
            <
                 dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
        )
/*
and
(
        (capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>@minute)
    or
        (capped_x_viewing_start_time_local between @minute and dateadd(second,59,@minute)))
*/
order by subscriber_id , capped_x_viewing_start_time_local
;

commit;

--select * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
--select * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;

--drop table  vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
--drop table  vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
--select * from vespa_analysts.project060_full_allocated_minutes;

---Create tables from code above to use to generate minute allocation---

---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
select subscriber_id
,service_key
,channel_name_inc_hd
,live_timeshifted_type
,weighting

,capped_x_viewing_start_time_local 
, capped_x_viewing_end_time_local 
, viewing_record_start_time_local
 , viewing_record_end_time_local
,dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)  as real_time_minute_start
,dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)  as real_time_minute_end

,datediff(second,real_time_minute_start,capped_x_viewing_start_time_local) as seconds_from_minute_start
, viewing_record_start_time_local as first_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, first_second_viewed_in_real_time_minute), first_second_viewed_in_real_time_minute) as first_minute_of_broadcast_viewed

, case when dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)=dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)
then datepart(second, capped_x_viewing_end_time_local)  - datepart(second, capped_x_viewing_start_time_local)
  else  60 - datepart(second, capped_x_viewing_start_time) end as seconds_viewed_in_minute
, dateadd(second,seconds_viewed_in_minute-1,first_second_viewed_in_real_time_minute) as last_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, last_second_viewed_in_real_time_minute), last_second_viewed_in_real_time_minute) as last_minute_of_broadcast_viewed
,case when first_minute_of_broadcast_viewed=last_minute_of_broadcast_viewed then seconds_viewed_in_minute
      else 60 - cast(datepart(second, first_second_viewed_in_real_time_minute) as integer) end as seconds_viewed_of_first_broadcast_minute
,seconds_viewed_in_minute - seconds_viewed_of_first_broadcast_minute   as seconds_viewed_of_second_broadcast_minute
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute
from vespa_analysts.project060_all_viewing 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
order by subscriber_id , capped_x_viewing_start_time_local
;

---Part 2 Repeat for End Seconds

select subscriber_id
,service_key
,channel_name_inc_hd
,live_timeshifted_type
,weighting

,capped_x_viewing_start_time_local 
, capped_x_viewing_end_time_local 
, viewing_record_start_time_local
 , viewing_record_end_time_local
,dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)  as real_time_minute_start
,dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)  as real_time_minute_end

,0 as seconds_from_minute_start

,dateadd(second, - datepart(second, capped_x_viewing_end_time_local), viewing_record_end_time_local) as first_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, first_second_viewed_in_real_time_minute), first_second_viewed_in_real_time_minute) as first_minute_of_broadcast_viewed

, datepart(second, capped_x_viewing_end_time_local)  as seconds_viewed_in_minute
, dateadd(second,seconds_viewed_in_minute-1,first_second_viewed_in_real_time_minute) as last_second_viewed_in_real_time_minute
,dateadd(second, - datepart(second, last_second_viewed_in_real_time_minute), last_second_viewed_in_real_time_minute) as last_minute_of_broadcast_viewed
,case when first_minute_of_broadcast_viewed=last_minute_of_broadcast_viewed then seconds_viewed_in_minute
      else 60 - cast(datepart(second, first_second_viewed_in_real_time_minute) as integer) end as seconds_viewed_of_first_broadcast_minute
,seconds_viewed_in_minute - seconds_viewed_of_first_broadcast_minute   as seconds_viewed_of_second_broadcast_minute
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute
from vespa_analysts.project060_all_viewing 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;

---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocxate minutes
---In these instances where not a single instance for a real time minute

---
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
into vespa_analysts.project060_partial_minutes_for_allocation
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute
;

insert into vespa_analysts.project060_partial_minutes_for_allocation
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute
;

insert into vespa_analysts.project060_partial_minutes_for_allocation
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute
where seconds_viewed_of_second_broadcast_minute>0
;

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation(real_time_minute);
---Sum Up viewing by broadcast minute--


select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute
from vespa_analysts.project060_partial_minutes_for_allocation
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute(real_time_minute);

----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute
;

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes
group by subscriber_id
,real_time_minute;

commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;


---Add on details of full minutes first create table of all instances

--drop table vespa_analysts.project060_full_allocated_minutes;
select  subscriber_id
,service_key
,channel_name_inc_hd
,live_timeshifted_type
,weighting

,capped_x_viewing_start_time_local 
, capped_x_viewing_end_time_local 
, viewing_record_start_time_local
 , viewing_record_end_time_local
,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)  as real_time_minute_start
,dateadd(second,  - ( datepart(second, capped_x_viewing_end_time_local)+60), capped_x_viewing_end_time_local)  as real_time_minute_end

,case when live_timeshifted_type='01: Live' then real_time_minute_start
--             dateadd(second, 60 - datepart(second, viewing_record_start_time_local), viewing_record_start_time_local)



            when ( 60 -  datepart(second, capped_x_viewing_start_time_local)) +  datepart(second, viewing_record_start_time_local) <30
                    then  dateadd(second,  - datepart(second, viewing_record_start_time_local), viewing_record_start_time_local)
            when ( 60- datepart(second, capped_x_viewing_start_time_local)) +  datepart(second, viewing_record_start_time_local) <90
                    then dateadd(second, 60 - datepart(second, viewing_record_start_time_local), viewing_record_start_time_local)
            
            else dateadd(second, 120 -( datepart(second, viewing_record_start_time_local)), viewing_record_start_time_local)


 end as start_broadcast_minute_viewed
,datediff(minute,real_time_minute_start,real_time_minute_end) as minutes_diff
,dateadd(minute,datediff(minute,real_time_minute_start,real_time_minute_end),start_broadcast_minute_viewed) as end_broadcast_minute_viewed
into vespa_analysts.project060_full_allocated_minutes
from vespa_analysts.project060_all_viewing 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_start_time_local)<>0
and 
       (        dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
            <
                 dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
        )
/*
and
(
        (capped_x_viewing_start_time_local<=@minute and capped_x_viewing_end_time_local>@minute)
    or
        (capped_x_viewing_start_time_local between @minute and dateadd(second,59,@minute)))
*/
order by subscriber_id , capped_x_viewing_start_time_local
;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes
;

commit;


commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total(last_broadcast_minute);
commit;

----Import BARB Data for 29th April-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120429
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120429
(  full_column_detail '\n')
FROM '/staging2/B20120429.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120429 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120429 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120429 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120429 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120429 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120429;
--drop table vespa_analysts.project060_spot_file_20120429;
select substr(full_column_detail,1,2) as record_type
,substr(full_column_detail,3,1) as insert_delete_amend_code
,substr(full_column_detail,4,8) as date_of_transmission
,substr(full_column_detail,12,5)  as reporting_panel_code
,substr(full_column_detail,17,5)  as log_station_code_for_break
,substr(full_column_detail,22,2)  as break_split_transmission_indicator
,substr(full_column_detail,24,2)  as break_platform_indicator
,substr(full_column_detail,26,6)  as break_start_time
,substr(full_column_detail,32,5)  as spot_break_total_duration
,substr(full_column_detail,37,2)  as break_type
,substr(full_column_detail,39,2)  as spot_type
,substr(full_column_detail,41,12)  as broadcaster_spot_number
,substr(full_column_detail,53,5)  as station_code
,substr(full_column_detail,58,5)  as log_station_code_for_spot
,substr(full_column_detail,63,2)  as split_transmission_indicator
,substr(full_column_detail,65,2)  as spot_platform_indicator
,substr(full_column_detail,67,2)  as hd_simulcast_spot_platform_indicator
,substr(full_column_detail,69,6)  as spot_start_time
,substr(full_column_detail,75,5)  as spot_duration
,substr(full_column_detail,80,15)  as clearcast_commercial_number
,substr(full_column_detail,95,35)  as sales_house_brand_description
,substr(full_column_detail,130,40)  as preceding_programme_name
,substr(full_column_detail,170,40)  as succeding_programme_name
,substr(full_column_detail,210,5)  as sales_house_identifier
,substr(full_column_detail,215,10)  as campaign_approval_id
,substr(full_column_detail,225,5)  as campaign_approval_id_version_number
,substr(full_column_detail,230,2)  as interactive_spot_platform_indicator
,substr(full_column_detail,232,17)  as blank_for_padding
into vespa_analysts.project060_spot_file_20120429
from vespa_analysts.project060_raw_spot_file_20120429
;

--select * from vespa_analysts.project060_spot_file_20120429 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120429 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;

commit;

alter table vespa_analysts.project060_spot_file_20120429 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120429
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120429 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120429 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120429 where spot_platform_indicator not in ( '00','0A','28');

commit;



--Load in Log Station Code/STI to Service Key Lookup
--drop table vespa_analysts.log_station_sti_to_service_key_lookup;
create table vespa_analysts.log_station_sti_to_service_key_lookup
(SERVICE_KEY integer
,LOG_STATION_CODE integer
,STI_CODE integer
);

input into vespa_analysts.log_station_sti_to_service_key_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\Log Station And STI to Service Key Lookup.csv' format ascii;

---Check for Dupes on Lookup---
/*
select SERVICE_KEY 
,LOG_STATION_CODE 
,STI_CODE
,count(*) as records 
from vespa_analysts.log_station_sti_to_service_key_lookup
group by SERVICE_KEY 
,LOG_STATION_CODE 
,STI_CODE

having records >1
order by service_key , log_station_code , sti_code
;

*/




commit;
--select * from vespa_analysts.log_station_sti_to_service_key_lookup order by log_station_code;
--select * from vespa_analysts.log_station_sti_to_service_key_lookup where service_key in (3358,6110,6127,6130) order by service_key;
---Load in list of Spot/Platform/Channels thare sold spots not just memorandum spots---
--drop table vespa_analysts.sold_spots_by_panel;
create table vespa_analysts.sold_spots_by_panel
(Panel_description varchar(128)
,Panel_code integer
,description_pt2 varchar(64)
,db2_station    integer
,log_station_code integer
,sti    integer
,ibt    varchar(1)
,prog   varchar(1)
,spot   varchar(1)
,break_y_n  varchar(1)
);
commit;

input into vespa_analysts.sold_spots_by_panel
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\sold spots by panelv3.csv' format ascii;

commit;

/*Check for Dupes on Sold Spots by Panel---
select Panel_code 
,log_station_code 
,sti    
,count(*) as records
from vespa_analysts.sold_spots_by_panel
where log_station_code is not null and spot='S'
group by Panel_code 
,log_station_code 
,sti    
having records>1
order by Panel_code 
,log_station_code 
,sti    
;
*/

--select * from vespa_analysts.sold_spots_by_panel  where log_station_code is not null order by log_station_code;
--Create table with only sold spots
--drop table vespa_analysts.sold_spots_by_panel_sold_only;
select panel_code
,log_station_code
,sti
into vespa_analysts.sold_spots_by_panel_sold_only
from vespa_analysts.sold_spots_by_panel
where spot = 'S'
;
commit;

--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120429_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120429_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120429_expanded
from vespa_analysts.project060_spot_file_20120429 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120429 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120429_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120429_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120429_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120429_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120429_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120429_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120429_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120429_expanded
;


alter table vespa_analysts.project060_spot_file_20120429_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120429_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120429_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120429_expanded
;

alter table  vespa_analysts.project060_spot_file_20120429_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120429_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120429_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120429_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120429_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120429_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120429_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;


select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into #spot_summary_values
from vespa_analysts.project060_spot_file_20120429_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from #spot_summary_values;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120429_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120429_expanded(corrected_spot_transmission_start_minute);

/*

update vespa_analysts.project060_spot_file_20120429_expanded
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120429_expanded as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;
*/

---Get Views by Spot
select a.service_key
,a.channel_name
,a.sales_house
,a.sales_house_identifier
,a.log_station_code_for_spot as log_station_code
,a.split_transmission_indicator
,a.spot_duration_integer
,b.live_timeshifted_type
,sum(case when a.corrected_spot_transmission_start_minute between b.first_broadcast_minute and b.last_broadcast_minute then 1 else 0 end) as unweighted_views
,sum(case when a.corrected_spot_transmission_start_minute between b.first_broadcast_minute and b.last_broadcast_minute then weighting_value else 0 end) as weighted_views
into vespa_analysts.project060_spot_summary_viewing_figures
from vespa_analysts.project060_spot_file_20120429_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total as b
on a.service_key=b.service_key
where a.service_key is not null
group by a.service_key
,a.channel_name
,a.sales_house
,a.sales_house_identifier
,log_station_code
,a.split_transmission_indicator
,a.spot_duration_integer
,b.live_timeshifted_type
;
commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total;


update vespa_analysts.project060_spot_summary_viewing_figures
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts  from vespa_analysts.project060_spot_summary_viewing_figures;
output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_29th_april.csv' format ascii;
--select count(*) from vespa_analysts.project060_spot_summary_viewing_figures;

--
/*
select log_station_code_for_spot ,split_transmission_indicator , count(*) as spots  
from vespa_analysts.project060_spot_file_20120429_expanded 
where service_key is null
group by log_station_code_for_spot ,split_transmission_indicator 
order by spots desc;
*/






---Delete Dupe Spots---



--Check for Dupes
--select service_key , spot_start_time , count(*) as records into #spot_dupes from vespa_analysts.project060_spot_file_20120429_expanded where service_key is not null group by service_key , spot_start_time order by records desc
--select spot_start_time from  #spot_dupes  where records >1 order by spot_start_time
--select service_key , count(*)  from #spot_dupes where records >1 group by service_key order by service_key


--select * from vespa_analysts.project060_spot_file_20120429_expanded where service_key = 6127 order by spot_start_time;

--select * from vespa_analysts.scaling_weightings where scaling_day = '2012-04-29'; output to 'c:\scaling.csv' format ascii;

commit;












































