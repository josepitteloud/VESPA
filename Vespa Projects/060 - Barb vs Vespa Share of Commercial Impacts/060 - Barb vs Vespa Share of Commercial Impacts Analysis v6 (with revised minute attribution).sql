
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
--select count(*) from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing group by live_timeshifted_type order by live_timeshifted_type;
--select top 100 * from vespa_analysts.project060_all_viewing

---Loop by Box and channel----


create variable @minute_start_time_local datetime;
create variable @minute_end_time_local datetime;
create variable @minute datetime;
set @minute_start_time_local = cast ('2011-08-11 00:00:00' as datetime);
set @minute_end_time_local = cast ('2011-08-12 00:00:00' as datetime);

--select @min_tx_start_time;
--select @minute_end_time_local;

---Loop by Channel---
if object_id('vespa_analysts.All_viewing_minute_by_minute_20110811') is not null drop table vespa_analysts.All_viewing_minute_by_minute_20110811;
commit;
create table vespa_analysts.All_viewing_minute_by_minute_20110811
(
subscriber_id  bigint           null
,channel_name_inc_hd  varchar(40)
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live          smallint            not null
,seconds_viewed_in_minute_playback_within_163_hours          smallint            not null
,weighted_boxes bigint NULL

);
commit;

---Start of Loop
--drop table vespa_analysts.All_viewing_minute_by_minute_20110811;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;
commit;

set @minute= @minute_start_time_local;


---Loop by Minute---
    WHILE @minute < @minute_end_time_local LOOP
    insert into vespa_analysts.All_viewing_minute_by_minute_20110811
    select a.subscriber_id
    ,channel_name_inc_hd
    ,@minute as minute

,sum(case when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute

    ,sum(case when live = 0 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_live

    ,sum(case when live =1 then 0 when  dateadd(hour,163,recorded_time_utc)<adjusted_event_start_time then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_playback_within_163_hours

,max(case   when cast (Adjusted_Event_Start_Time as date) ='2011-08-11' then weight_2011_08_11
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-12' then weight_2011_08_12
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-13' then weight_2011_08_13
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-14' then weight_2011_08_14
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-15' then weight_2011_08_15
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-16' then weight_2011_08_16
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-17' then weight_2011_08_17
            when cast (Adjusted_Event_Start_Time as date) ='2011-08-18' then weight_2011_08_18 else 0 end) as weighted_boxes

from vespa_analysts.VESPA_all_viewing_records_20110811_20110818 as a
left outer join vespa_analysts.sky_base_v2_2011_08_11 as b
on  a.subscriber_id=b.subscriber_id
where  (play_back_speed is null or play_back_speed = 2) and (
        (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
    or
        (viewing_record_start_time_local between @minute and dateadd(second,59,@minute)))
    group by a.subscriber_id
    ,channel_name_inc_hd
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;










































































































----BARB Minute Attribution----
----Minutes are attributed according to the activity that takes place during the 'clock minute' i.e., real rather than Broadcast Minute in which it occurs--
----Activity is assigned to the activity that takes place the most during the minute
----In the event of a tie, the activity that takes place first is given credit for the minute


--Step 1: Create a table that contains all instances where activity for a full minute takes place
--e.g., if event is from 18:04:07 to 18:06:27 then minute 18:05 is definitely attributed to this activity
--The incomplete minutes for this event will be combined with other activity in these minutes to help determine attribution

if object_id('vespa_analysts.project_060_allocated_minutes_20120429_activity') is not null drop table vespa_analysts.project_060_allocated_minutes_20120429_activity;

create table vespa_analysts.project_060_allocated_minutes_20120429_activity (
    subscriber_id                       decimal(10)
    ,epg_channel                        varchar(20)
    ,minute_started                     datetime        not null
    ,minute_stopped                     datetime        not null
    ,broadcast_datetime               datetime
    ,live_timeshifted_type            varchar(30)
    ,weighting double
);

---- Populate allocated minute table with activity of a full minute---

insert into vespa_analysts.project_060_allocated_minutes_20120429_activity 
select subscriber_id
    , epg_channel    
    ,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)   -- Couldn't find a single function to round a minute up
    ,dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time)          -- Nor for rounding a minute down
    ,viewing_record_end_time_local
    ,live_timeshifted_type
    ,weighting
from vespa_analysts.project060_all_viewing
where dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    <> dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time);-- Only want to add things which span a minute boundry

--select top 100 * from vespa_analysts.project060_all_viewing;
commit;


 
----PART E - Create Summary of Minute by Minute Viewing-----
create variable @minute_start_time_local datetime;
create variable @minute_end_time_local datetime;
create variable @minute datetime;



if object_id('vespa_analysts.All_viewing_minute_by_minute_20120429') is not null drop table vespa_analysts.All_viewing_minute_by_minute_20120429;
commit;
create table vespa_analysts.All_viewing_minute_by_minute_20120429
(
subscriber_id  bigint           null
,channel_name_inc_hd  varchar(40)
,minute                 datetime            not null
,seconds_viewed_in_minute          smallint            not null
,seconds_viewed_in_minute_live          smallint            not null
,seconds_viewed_in_minute_playback_vosdal          smallint            not null
,seconds_viewed_in_minute_playback_within_163_hours          smallint            not null
,weighted_boxes bigint NULL

);
commit;

---Start of Loop
--drop table vespa_analysts.All_viewing_minute_by_minute_20120429;
--select * from  vespa_analysts.interim_viewing_minute_by_minute_raw_selected_channel where order by log_id , adjusted_event_start_time,x_adjusted_event_end_time ;

---Vosdal figure includes all playback events that start pre 2 a.m as included as VOSDAL, at some point need to change to cap minutes as to those that would have been watched by 2 a.m.


commit;
set @minute_start_time_local = cast ('2012-04-29 06:00:00' as datetime);
set @minute_end_time_local = cast ('2012-04-30 06:00:00' as datetime);
set @minute= @minute_start_time_local;


---Loop by Minute---
    WHILE @minute < @minute_end_time_local LOOP
    insert into vespa_analysts.All_viewing_minute_by_minute_20120429
    select a.subscriber_id
    ,channel_name_inc_hd
    ,@minute as minute

,sum(case when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute

    ,sum(case when live = 0 then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_live

    ,sum(case   when live =1 then 0 when cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date) then 0
                when viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 
                when viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) 
                when viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) 
                when viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
                then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
                     end) as seconds_viewed_in_minute_playback_vosdal  

    ,sum(case when live =1 then 0 when
 dateadd(hour,163,recorded_time_utc)<adjusted_event_start_time then 0 when
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local>dateadd(second,59,@minute) then 60 when 
    viewing_record_start_time_local<=@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) then datediff(second,@minute,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local<=dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,viewing_record_end_time_local) when
    viewing_record_start_time_local>@minute and viewing_record_end_time_local>dateadd(second,59,@minute) 
        then datediff(second,viewing_record_start_time_local,dateadd(second,60,@minute)) else 0 
    end) as seconds_viewed_in_minute_playback_within_163_hours

,max(weighting) as weighted_boxes

from vespa_analysts.project060_all_viewing  as a
where  (    play_back_speed is null or play_back_speed = 2) and 
            (
                (viewing_record_start_time_local<=@minute and viewing_record_end_time_local>@minute)
                or
                (viewing_record_start_time_local between @minute and dateadd(second,59,@minute)
            )
        )
    group by a.subscriber_id
    ,channel_name_inc_hd
    ,minute
    ;
    SET @minute =dateadd(minute,1,@minute);
    COMMIT;

    END LOOP;
commit;

if object_id('vespa_analysts.minute_channel_summary_20120429_viewing') is not null drop table vespa_analysts.minute_channel_summary_20120429_viewing;
----Split out in to hierarchy  Vosdal - 163h - Live -  (+ also 163h inc vosdal)
select channel_name_inc_hd
,minute
,sum(case when seconds_viewed_in_minute >=31 then weighted_boxes else 0 end) as total_households

,sum(case   when seconds_viewed_in_minute_playback_vosdal  >=31 then weighted_boxes
             when   (seconds_viewed_in_minute >=31 
                        and seconds_viewed_in_minute_playback_vosdal>=seconds_viewed_in_minute_live
                        and seconds_viewed_in_minute_playback_vosdal>=seconds_viewed_in_minute_playback_within_163_hours
                    )  
            then weighted_boxes else 0 end) as total_households_playback_vosdal

,sum(case   when seconds_viewed_in_minute_playback_vosdal  >=31 then 0
             when   (seconds_viewed_in_minute >=31 
                        and seconds_viewed_in_minute_playback_vosdal>=seconds_viewed_in_minute_live
                        and seconds_viewed_in_minute_playback_vosdal>=seconds_viewed_in_minute_playback_within_163_hours
                    )  
            then 0 
            when seconds_viewed_in_minute_playback_within_163_hours>=31 then weighted_boxes
             when   (seconds_viewed_in_minute >=31 
                        and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live
                        and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_playback_vosdal
                    )  
            then weighted_boxes 
else 0 end) as total_households_playback_within_163_hours_exc_vosdal

,sum(case   when seconds_viewed_in_minute_playback_vosdal  >=31 then 0
             when   (seconds_viewed_in_minute >=31 
                        and seconds_viewed_in_minute_playback_vosdal>=seconds_viewed_in_minute_live
                        and seconds_viewed_in_minute_playback_vosdal>=seconds_viewed_in_minute_playback_within_163_hours
                    )  
            then 0 
            when seconds_viewed_in_minute_playback_within_163_hours>=31 then 0
             when   (seconds_viewed_in_minute >=31 
                        and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_live
                        and seconds_viewed_in_minute_playback_within_163_hours>=seconds_viewed_in_minute_playback_vosdal
                    )  
            then 0

            when seconds_viewed_in_minute_live>=31 then weighted_boxes
             when   (seconds_viewed_in_minute >=31 
                        and seconds_viewed_in_minute_live>=seconds_viewed_in_minute_playback_within_163_hours
                        and seconds_viewed_in_minute_live>=seconds_viewed_in_minute_playback_vosdal
                    )  
            then weighted_boxes
else 0 end) as total_households_live_exc_playback

,sum(case when seconds_viewed_in_minute_playback_vosdal >=31 then weighted_boxes else 0 end) as total_households_vosdal_non_hierarchy
,sum(case when seconds_viewed_in_minute_playback_within_163_hours-seconds_viewed_in_minute_playback_vosdal >=31 then weighted_boxes 
               else 0 end) as total_households_playback_within_163_hours_exc_vosdal_non_hierarchy
,sum(case when seconds_viewed_in_minute_live >=31 then weighted_boxes else 0 end) as total_households_live_non_hierarchy

,sum(case when seconds_viewed_in_minute_live >=60 and seconds_viewed_in_minute_playback_within_163_hours>=60 then weighted_boxes else 0 end) 
    as total_households_full_minute_live_and_playback

,sum(case when  seconds_viewed_in_minute_live >=60 and 
                seconds_viewed_in_minute_playback_within_163_hours-seconds_viewed_in_minute_playback_vosdal>=60 
                then weighted_boxes else 0 end) 
    as total_households_full_minute_live_and_playback_exc_vosdal

into vespa_analysts.minute_channel_summary_20120429_viewing
from vespa_analysts.All_viewing_minute_by_minute_20120429
group by channel_name_inc_hd
,minute
order by channel_name_inc_hd
,minute
;
 commit;


--select top 500 * from vespa_analysts.All_viewing_minute_by_minute_20120429 where seconds_viewed_in_minute_playback_vosdal<>seconds_viewed_in_minute_playback_within_163_hours;


--select count(*) from vespa_analysts.All_viewing_minute_by_minute_20120429;
--select count(*) from vespa_analysts.minute_channel_summary_20120429_viewing;
--select count(*) from vespa_analysts.minute_channel_summary_20120429_viewing where minute between '2012-04-29 19:00:00' and '2012-04-29 22:00:00';
--select * from vespa_analysts.minute_channel_summary_20120429_viewing order by channel_name_inc_hd,minute;
--select * from vespa_analysts.minute_channel_summary_20120429_viewing where channel_name_inc_hd = 'BBC ONE' order by channel_name_inc_hd,minute;
--select * from vespa_analysts.minute_channel_summary_20120429_viewing where channel_name_inc_hd = 'BBC TWO' order by channel_name_inc_hd,minute;
--select * from vespa_analysts.minute_channel_summary_20120429_viewing where channel_name_inc_hd = 'Sky Atlantic' order by channel_name_inc_hd,minute;
--select * from vespa_analysts.minute_channel_summary_20120429_viewing where channel_name_inc_hd = 'BBC NEWS' order by channel_name_inc_hd,minute;


--select * from vespa_analysts.project060_all_viewing where subscriber_id = 28675 and channel_name_inc_hd = 'Channel 5' order by viewing_record_start_time_local
--select top 500 *  from vespa_analysts.project060_all_viewing where  channel_name_inc_hd = 'Channel 5' order by viewing_record_start_time_local

--select * from vespa_analysts.All_viewing_minute_by_minute_20120429 where subscriber_id = 28675
--select top 500 *  from vespa_analysts.project060_all_viewing where  subscriber_id = 28675 order by viewing_record_start_time_local

---Output by Channel----

select channel_name_inc_hd
,minute
,total_households as total_hh_viewing_minute
from vespa_analysts.minute_channel_summary_20120429_viewing where minute between '2012-04-29 19:00:00' and '2012-04-29 22:00:00'
order by channel_name_inc_hd
,minute;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\channel_by_minute.csv' format ascii;

commit;





select minute
,sum(total_households) as hh
,sum(total_households_vosdal_non_hierarchy) as vosdal
,sum(total_households_playback_within_163_hours_exc_vosdal_non_hierarchy) as playback_exc_vosdal
,sum(total_households_live_non_hierarchy) as hh_live
from vespa_analysts.minute_channel_summary_20120429_viewing
group by minute
order by minute
;

select channel_name_inc_hd
,sum(total_households) as hh
from vespa_analysts.minute_channel_summary_20120429_viewing
group by channel_name_inc_hd
order by hh desc
;

commit;

/*
select * from sk_prod.vespa_epg_dim where tx_date = '20120429' and upper(left(channel_name,3))='ITV'  order by tx_time

select * from sk_prod.vespa_epg_dim where tx_date = '20120429' and upper(left(channel_name,3))='BBC' 
and tx_time = '210000' 
 order by tx_time 

select * from sk_prod.vespa_epg_dim where tx_date = '20120429' and channel_name='BBC 1 S West'  order by tx_time

---Analyse Capping Levels---

select * from vespa_max_caps_apr29_2012 order by event_start_day , event_start_hour , live
select * from vespa_max_caps_apr29_2012_100_Ntile order by event_start_day , event_start_hour , live

--vespa_max_caps_apr29_2012_100_Ntile
--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

select channel_name_inc_hd
,minute
,total_households as total_hh_viewing_minute
from vespa_analysts.minute_channel_summary_20120429_viewing where minute between '2012-04-29 12:00:00' and '2012-04-29 23:00:00'
and channel_name_inc_hd = 'BBC ONE'
order by channel_name_inc_hd
,minute;


commit;
--select top 500 * from vespa_analysts.scaling_dialback_intervals;
select * from vespa_analysts.project047_capping_phase2_caps where live_or_playback = 'Playback'


---Copy over from Prod10 to Prod4
create table vespa_analysts.scaling_dialback_intervals (
    account_number              varchar(20)     not null
    ,reporting_starts           date            not null
    ,reporting_ends             date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
    ,primary key (account_number, reporting_starts)             -- Won't bother forcing the no-overlap in DB constraints, but this is a good start
);

INSERT INTO vespa_analysts.scaling_dialback_intervals
   LOCATION 'DCSLOPSKPRD10_olive_prod.vespa_analysts' 'SELECT * FROM vespa_analysts.scaling_dialback_intervals'


-- Indices What do we need?
create index for_joining on scaling_dialback_intervals (scaling_segment_ID, reporting_starts);

-- Permissions
grant select on vespa_analysts.scaling_dialback_intervals to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj;


create table vespa_analysts.scaling_weightings (
    scaling_day                 date            not null
    ,scaling_segment_ID         int             not null        -- links to the segments lookup table
    ,vespa_accounts             bigint          default 0       -- Vespa panel accounts in this segment reporting back for this day
    ,sky_base_accounts          bigint          not null        -- Sky base accounts for this day by segment
    ,weighting                  double          default null    -- For the usual scaling with a day
    ,primary key (scaling_day, scaling_segment_ID)
);

INSERT INTO vespa_analysts.scaling_weightings
   LOCATION 'DCSLOPSKPRD10_olive_prod.vespa_analysts' 'SELECT * FROM vespa_analysts.scaling_weightings'
-- Permissions
grant select on vespa_analysts.scaling_weightings to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj;


--drop table vespa_analysts.scaling_segments_lookup;

create table vespa_analysts.scaling_segments_lookup
(
  scaling_segment_id   integer             not null
 ,isba_tv_region       varchar(20)             null
 ,ilu_hhlifestage      tinyint                 null
 ,lifestage            varchar(35)         not null
 ,low_ilu_hhafflu      tinyint                 null
 ,high_ilu_hhafflu     tinyint                 null
 ,affluence            varchar(10)         not null
 ,pvr                  bit                 not null
 ,package              varchar(20)         not null
 ,scaling_segment_name varchar(100)            null
);


INSERT INTO vespa_analysts.scaling_segments_lookup
   LOCATION 'DCSLOPSKPRD10_olive_prod.vespa_analysts' 'SELECT * FROM vespa_analysts.scaling_segments_lookup'
create HG index for_segment_identification on vespa_analysts.scaling_segments_lookup (affluence,isba_tv_region,lifestage,package,pvr);
create HG index for_segment_identification_raw on vespa_analysts.scaling_segments_lookup (high_ilu_hhafflu,ilu_hhlifestage,isba_tv_region,low_ilu_hhafflu,package,pvr);


-- Permissions
grant select on vespa_analysts.scaling_segments_lookup to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh, rombaoad, louredaj;






select Adjusted_Event_Start_Time
, program_air_date
,dateadd(hh, 26,cast( cast( program_air_date as date) as datetime)) 
from sk_prod.VESPA_STB_PROG_EVENTS_20120601



select
      programme_trans_sk
      ,Channel_Name
      ,epg_channel
      ,pay_free_indicator
      ,epg_group_name
      ,network_indicator
      ,Genre_Description
      ,Sub_Genre_Description
      ,EPG_title
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,tx_date_time_utc
      ,tx_date_utc as program_air_date
      ,tx_start_datetime_utc as program_air_datetime
--  into week_Programmes -- drop table vespa_programmes
  from sk_prod.VESPA_EPG_DIM
where tx_date_utc='20120611'

create table vespa_analysts.dbarnettpspfile
(  full_column_detail varchar(1100))
;

LOAD TABLE vespa_analysts.dbarnettpspfile
(  full_column_detail '\n')
FROM '/staging2/B20120401.PSP'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;

create table vespa_analysts.dbarnettcetfile
(  full_column_detail varchar(248))
;

LOAD TABLE vespa_analysts.dbarnettcetfile
(  full_column_detail '\n')
FROM '/staging2/B20120429.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;

select * from vespa_analysts.dbarnettcetfile;

commit;


select * from vespa_analysts.dbarnettpspfile;
commit;

select live_timeshifted_type
,recorded_time_utc
,capped_x_viewing_start_time
,capped_x_viewing_end_time

,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time) as minute_started
   ,dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time) as minute_stopped
,dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time) as min_start
, dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time) as min_end
,case when dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    <> dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time) then 1 else 0 end as include_as_whole_min
,dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time) as viewing_minute_start_time
,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time) end_of_viewing_minute_start_time
from vespa_analysts.project060_all_viewing

where dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    <> dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time);-- Only want to add things which span a minute boundry


--select * from sk_prod.VESPA_STB_PROG_EVENTS_20120429 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select top 500 * from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing where service_key = 4098
--select service_key , count(*) as records from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01' group by service_key order by records desc
--select service_key ,Channel_Name, count(*) as records from vespa_analysts.project060_all_viewing where cast(recorded_time_utc as date) = '1970-01-01' group by service_key ,Channel_Name order by records desc
--select service_key ,Channel_Name, count(*) as records from vespa_analysts.project060_all_viewing where service_key is not null group by service_key ,Channel_Name order by records desc

select sum ( case when cast(recorded_time_utc as date) = '1970-01-01' then x_event_duration else 0 end) as unknown_playback_date_duration
,sum (case when cast(recorded_time_utc as date) > '1970-01-01' then x_event_duration else 0 end) as known_playback_date_duration
from vespa_analysts.project060_all_viewing where play_back_speed=2
commit;

select channel_name
,sum(case when play_back_speed is null then x_event_duration else 0 end) as live_duration
,sum(case when play_back_speed =2 then x_event_duration else 0 end) as playback_duration
from  vespa_analysts.project060_all_viewing
group by channel_name
order by live_duration desc


select service_key
,channel_name
,sum(case when play_back_speed is null then x_event_duration else 0 end) as live_duration
,sum(case when play_back_speed =2 then x_event_duration else 0 end) as playback_duration
from  vespa_analysts.project060_all_viewing
group by service_key
,channel_name

order by live_duration desc



select service_key
,sum ( case when cast(recorded_time_utc as date) <= '1970-01-01' then x_event_duration else 0 end) as unknown_playback_date_duration
,sum (case when cast(recorded_time_utc as date) > '1970-01-01' then x_event_duration else 0 end) as known_playback_date_duration
from sk_prod.VESPA_STB_PROG_EVENTS_20120129 where play_back_speed=2
group by service_key
order by unknown_playback_date_duration desc

select service_key
,Channel_Name
,sum ( case when cast(recorded_time_utc as date) <= '1970-01-01' then x_event_duration else 0 end) as unknown_playback_date_duration
,sum (case when cast(recorded_time_utc as date) > '1970-01-01' then x_event_duration else 0 end) as known_playback_date_duration
from sk_prod.VESPA_STB_PROG_EVENTS_20120129 where play_back_speed=2
group by service_key ,Channel_Name
order by unknown_playback_date_duration desc

commit;

select  cast(recorded_time_utc as date) as recorded_date
,sum ( case when cast(recorded_time_utc as date) <= '1970-01-01' then x_event_duration else 0 end) as unknown_playback_date_duration
,sum (case when cast(recorded_time_utc as date) > '1970-01-01' then x_event_duration else 0 end) as known_playback_date_duration
from sk_prod.VESPA_STB_PROG_EVENTS_20120429 where play_back_speed=2
group by recorded_date
order by recorded_date
--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120429 where cast(recorded_time_utc as date) <= '1970-01-01';
*/