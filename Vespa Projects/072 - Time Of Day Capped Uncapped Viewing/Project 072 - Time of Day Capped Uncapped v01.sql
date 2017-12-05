
-----Project 072 - Adsmart Time of Day analysis

--Brief 

--http://rtci/Sky%20Projects/Forms/AllItems.aspx?RootFolder=%2fSky%20Projects%2fVespa%2fAnalysis%20Requests%2fV072%20%2d%20Adsmart%20Time%20of%20Day%20Capped%20Uncapped%20Viewing&FolderCTID=&View=%7b95B15B22%2d959B%2d4B62%2d809A%2dAD43E02001BD%7d

---2 Week Live viewing analysis by Hour By Channel for Adsmart only boxes for:

--Uncapped
--Capped v1b
--Capped 2

---Analysis period viewing 5th-18th Feb so activity in tables 4th/19th Feb alos included to be able to work using BARB days (i.e., to 6 a.m. next day) as well as events that started prior to 5th Feb


---Tables Created----


---PART A  - Create Capping levels using version 1b  10% capping for Live
---PART B  - Viewing of All Channels (Including Non-Commercial channels as full data needed to attribute minutes)
---PART C  - Apply Capping values to viewing data
---PART D  - Apply Scaling levels using version 1b

----Tables Used/Created-----
--------------------------------------------------------------------------------
-- PART A - Capping
--------------------------------------------------------------------------------
--         B00 - Set up macro variables and start/end dates 
--         B01 - Identify extream viewing and populate max and min daily caps
--         B02 - Apply capping to the viewing data
--------------------------------------------------------------------------------
-- A00  - SET UP.
--------------------------------------------------------------------------------
-- create and populate variables


CREATE VARIABLE @var_period_start_capping_1b       datetime;
CREATE VARIABLE @var_period_end_capping_1b         datetime;

CREATE VARIABLE @var_sql_capping_1b                varchar(15000);
CREATE VARIABLE @var_cntr_capping_1b               smallint;
CREATE VARIABLE @var_num_days_capping_1b           smallint;
CREATE VARIABLE @i_capping_1b                      integer;

SET @var_period_start_capping_1b           = '2012-02-04';
SET @var_period_end_capping_1b             = '2012-02-19';
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- A01 - Identify extreme viewing and populate max and min daily caps
-- Table produced for 10% capping (Live)
--------------------------------------------------------------------------------
--select * from dbarnett.project072_vespa_max_caps_05_18_2012;
-- Max Caps:
--select * from dbarnett.project072_vespa_max_caps_05_18_2012;
IF object_id('project072_vespa_max_caps_05_18_2012') IS NOT NULL DROP TABLE dbarnett.project072_vespa_max_caps_05_18_2012;

create table dbarnett.project072_vespa_max_caps_05_18_2012
(
    event_start_day as date
    , event_start_hour as integer
    , live as smallint
    , ntile_100 as integer
    , min_dur_mins as integer
);

-- loop through the viewing data to identify caps
SET @var_cntr_capping_1b = 0;
set @i_capping_1b=datediff(dd,@var_period_start_capping_1b,@var_period_end_capping_1b);
--select @i_capping_1b;


WHILE @var_cntr_capping_1b <= @i_capping_1b

BEGIN

    SET @var_sql_capping_1b = 'IF object_id(''gm_ntile_temph_db'') IS NOT NULL DROP TABLE gm_ntile_temph_db'
    EXECUTE(@var_sql_capping_1b)
    commit

    -- create a temp table storing the relevant data for the given day
    SET @var_sql_capping_1b =
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
        sk_prod.VESPA_STB_PROG_EVENTS_' || replace(cast(dateadd(day, @var_cntr_capping_1b, @var_period_start_capping_1b) as varchar(10)), '-', '') ||
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
    EXECUTE(@var_sql_capping_1b)
    commit

--select @var_sql_capping_1b


    -- create indexes to speed up the ntile creation
    create hng index idx1 on gm_ntile_temph_db(event_start_day)
    create hng index idx2 on gm_ntile_temph_db(event_start_hour)
    create hng index idx3 on gm_ntile_temph_db(live)
    create hng index idx4 on gm_ntile_temph_db(dur_mins)

    -- query ntiles for given date and insert into the persistent table
    insert into dbarnett.project072_vespa_max_caps_05_18_2012
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

    SET @var_cntr_capping_1b = @var_cntr_capping_1b + 1
END;

--IF object_id('gm_ntile_temph_db') IS NOT NULL DROP TABLE gm_ntile_temph_db;
-- add indexes
create hng index idx1 on dbarnett.project072_vespa_max_caps_05_18_2012(event_start_day);
create hng index idx2 on dbarnett.project072_vespa_max_caps_05_18_2012(event_start_hour);
create hng index idx3 on dbarnett.project072_vespa_max_caps_05_18_2012(live);

---Min Cap set to 1 so no viewing will be removed but code kept consistent--

-- Min Caps
IF object_id('dbarnett.project072_vespa_min_cap') IS NOT NULL DROP TABLE  dbarnett.project072_vespa_min_cap;

create table  dbarnett.project072_vespa_min_cap (
    cap_secs as integer
);
insert into  dbarnett.project072_vespa_min_cap (cap_secs) values (6);

commit;





---PART B  - Live Viewing of Sky Channels (All Live Viewing between 5th and 18th Jan Inclusive---
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-02-04';
SET @var_prog_period_end    = '2012-02-19';


SET @var_cntr = 0;
SET @var_num_days = 16;       -- 
--select top 500 * from vespa_analysts.project060_all_viewing;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table dbarnett.project072_all_viewing ( -- drop table dbarnett.project072_all_viewing
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
    insert into dbarnett.project072_all_viewing
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
and Play_Back_Speed is null
'     ;

while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;

--select count(distinct subscriber_id) as subscribers , count(distinct account_number) as accounts from dbarnett.project072_all_viewing
--select play_back_speed , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20111129_20111206 group by day_view order by day_view;
--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120429;

commit;

alter table dbarnett.project072_all_viewing add live tinyint;

update dbarnett.project072_all_viewing
set live = case when play_back_speed is null then 1 else 0 end
from dbarnett.project072_all_viewing
;
commit;


/* Table Already Available

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
*/


--grant all on vespa_analysts.channel_name_lookup_old to public; commit;
--select * from  vespa_analysts.channel_name_lookup_old;
alter table dbarnett.project072_all_viewing add channel_name_inc_hd varchar(40);

update dbarnett.project072_all_viewing
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from dbarnett.project072_all_viewing as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;


--select count(*) from dbarnett.project072_all_viewing;


--select * from vespa_analysts.vespa_max_caps_live_playback;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on dbarnett.project072_all_viewing(subscriber_id);
create dttm index idx2 on dbarnett.project072_all_viewing(adjusted_event_start_time);
create dttm index idx3 on dbarnett.project072_all_viewing(recorded_time_utc);
create lf index idx4 on dbarnett.project072_all_viewing(live)
create dttm index idx5 on dbarnett.project072_all_viewing(x_viewing_start_time);
create dttm index idx6 on dbarnett.project072_all_viewing(x_viewing_end_time);
create hng index idx7 on dbarnett.project072_all_viewing(x_cumul_programme_viewed_duration);
create hg index idx8 on dbarnett.project072_all_viewing(programme_trans_sk);
create hg index idx9 on dbarnett.project072_all_viewing(channel_name_inc_hd);
commit;
-- append fields to table to store additional metrics for capping
alter table dbarnett.project072_all_viewing
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update dbarnett.project072_all_viewing
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update dbarnett.project072_all_viewing
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update dbarnett.project072_all_viewing
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
        dbarnett.project072_all_viewing base left outer join dbarnett.project072_vespa_max_caps_05_18_2012 caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update dbarnett.project072_all_viewing
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update dbarnett.project072_all_viewing
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
update dbarnett.project072_all_viewing
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
         dbarnett.project072_vespa_min_cap
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;

--select capped_flag  , count(*) from dbarnett.project072_all_viewing where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from dbarnett.project072_all_viewing where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

---Deletion of capped records commented out initially - for evaluation purposes (as capped values have null capped times then OK to leave them in as won't get inlcuded in 
---Any minute by minute analysis---


/*
delete from dbarnett.project072_all_viewing
where capped_flag in (2,3)
;
commit;
*/


---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table dbarnett.project072_all_viewing add viewing_record_start_time_utc datetime;
alter table dbarnett.project072_all_viewing add viewing_record_start_time_local datetime;


alter table dbarnett.project072_all_viewing add viewing_record_end_time_utc datetime;
alter table dbarnett.project072_all_viewing add viewing_record_end_time_local datetime;

update dbarnett.project072_all_viewing
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from dbarnett.project072_all_viewing
;
commit;


---
update dbarnett.project072_all_viewing
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from dbarnett.project072_all_viewing
;
commit;

--select top 100 * from dbarnett.project072_all_viewing;

update dbarnett.project072_all_viewing
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
from dbarnett.project072_all_viewing
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table dbarnett.project072_all_viewing add adjusted_event_start_time_local datetime;

update dbarnett.project072_all_viewing
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from dbarnett.project072_all_viewing
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table dbarnett.project072_all_viewing add capped_x_viewing_start_time_local datetime;
alter table dbarnett.project072_all_viewing add capped_x_viewing_end_time_local datetime;
commit;
update dbarnett.project072_all_viewing
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
from dbarnett.project072_all_viewing
;
commit;

--select top 500 * from dbarnett.project072_all_viewing;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table dbarnett.project072_all_viewing delete scaling_segment_id
alter table dbarnett.project072_all_viewing add scaling_segment_id integer;

update dbarnett.project072_all_viewing 
set scaling_segment_id=b.scaling_segment_id
from dbarnett.project072_all_viewing  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(distinct account_number) from dbarnett.project072_all_viewing where scaling_segment_id is not null group by scaling_segment_id order by scaling_segment_id;
--select count(distinct subscriber_id) , count(distinct account_number) from dbarnett.project072_all_viewing where scaling_segment_id is not null ;
--select count(*) from vespa_analysts.scaling_weightings;
--select top 100 * from scaling_segments_lookup;
---Add weight for each scaling ID for each record

alter table dbarnett.project072_all_viewing add weighting double;

update dbarnett.project072_all_viewing 
set weighting=b.weighting
from dbarnett.project072_all_viewing  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

---Create a viewing Record end time local for uncapped
alter table dbarnett.project072_all_viewing add uncapped_x_viewing_end_time_local datetime;
commit;
update dbarnett.project072_all_viewing
set uncapped_x_viewing_end_time_local= case 
when dateformat(x_viewing_end_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,x_viewing_end_time) 
when dateformat(x_viewing_end_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,x_viewing_end_time) 
when dateformat(x_viewing_end_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,x_viewing_end_time) 
                    else x_viewing_end_time  end
from dbarnett.project072_all_viewing
;
commit;



--select count(*) from dbarnett.project072_all_viewing;
--select top 500 * from dbarnett.project072_all_viewing;


/*
alter table dbarnett.project072_all_viewing add affluence varchar(10) ;
alter table dbarnett.project072_all_viewing add pvr tinyint;

update dbarnett.project072_all_viewing 
set affluence=case when b.affluence is null then 'Unknown' else b.affluence end
,pvr=case when b.pvr =1 then 1 else 0 end
from dbarnett.project072_all_viewing  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from dbarnett.project072_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from dbarnett.project072_all_viewing ;
--select pvr, count(*) from dbarnett.project072_all_viewing group by pvr;
--select affluence, count(*) from dbarnett.project072_all_viewing group by affluence;
*/
commit;

----Total Viewed by Hour by Channel by Day

--drop table #hour_details;
select channel_name_inc_hd
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020500_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020500_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020501_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020501_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020502_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020502_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020503_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020503_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020504_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020504_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020505_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020505_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020506_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020506_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020507_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020507_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020508_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020508_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020509_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020509_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020510_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020510_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020511_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020511_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020512_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020512_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020513_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020513_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020514_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020514_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020515_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020515_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020516_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020516_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020517_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020517_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020518_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020518_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020519_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020519_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020520_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020520_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020521_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020521_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020522_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020522_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-05 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-05 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020523_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-05 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-05 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-05 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-05 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-05 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-05 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-05 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020523_uncapped


----20120206
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020600_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020600_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020601_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020601_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020602_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020602_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020603_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020603_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020604_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020604_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020605_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020605_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020606_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020606_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020607_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020607_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020608_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020608_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020609_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020609_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020610_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020610_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020611_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020611_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020612_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020612_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020613_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020613_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020614_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020614_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020615_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020615_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020616_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020616_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020617_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020617_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020618_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020618_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020619_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020619_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020620_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020620_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020621_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020621_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020622_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020622_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-06 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-06 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020623_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-06 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-06 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-06 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-06 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-06 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-06 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-06 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_2012020623_uncapped



---20120207
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0700_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0700_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0701_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0701_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0702_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0702_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0703_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0703_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0704_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0704_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0705_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0705_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0706_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0706_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0707_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0707_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0708_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0708_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0709_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0709_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0710_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0710_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0711_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0711_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0712_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0712_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0713_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0713_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0714_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0714_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0715_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0715_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0716_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0716_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0717_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0717_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0718_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0718_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0719_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0719_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0720_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0720_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0721_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0721_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0722_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0722_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-07 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-07 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0723_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-07 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-07 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-07 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-07 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-07 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-07 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-07 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0723_uncapped


--20120208
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0800_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0800_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0801_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0801_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0802_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0802_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0803_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0803_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0804_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0804_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0805_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0805_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0806_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0806_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0807_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0807_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0808_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0808_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0809_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0809_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0810_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0810_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0811_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0811_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0812_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0812_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0813_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0813_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0814_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0814_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0815_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0815_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0816_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0816_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0817_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0817_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0818_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0818_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0819_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0819_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0820_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0820_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0821_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0821_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0822_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0822_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-08 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-08 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0823_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-08 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-08 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-08 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-08 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-08 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-08 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-08 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0823_uncapped


---20120209
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0900_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0900_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0901_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0901_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0902_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0902_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0903_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0903_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0904_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0904_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0905_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0905_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0906_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0906_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0907_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0907_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0908_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0908_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0909_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0909_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0910_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0910_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0911_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0911_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0912_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0912_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0913_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0913_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0914_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0914_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0915_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0915_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0916_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0916_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0917_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0917_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0918_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0918_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0919_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0919_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0920_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0920_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0921_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0921_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0922_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0922_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-09 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-09 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0923_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-09 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-09 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-09 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-09 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-09 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-09 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-09 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_0923_uncapped


---20120210
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1000_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1000_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1001_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1001_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1002_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1002_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1003_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1003_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1004_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1004_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1005_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1005_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1006_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1006_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1007_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1007_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1008_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1008_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1009_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1009_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1010_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1010_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1011_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1011_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1012_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1012_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1013_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1013_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1014_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1014_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1015_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1015_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1016_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1016_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1017_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1017_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1018_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1018_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1019_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1019_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1020_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1020_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1021_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1021_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1022_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1022_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-10 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-10 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1023_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-10 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-10 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-10 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-10 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-10 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-10 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-10 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1023_uncapped


---20120211

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1100_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1100_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1101_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1101_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1102_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1102_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1103_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1103_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1104_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1104_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1105_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1105_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1106_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1106_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1107_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1107_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1108_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1108_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1109_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1109_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1110_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1110_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1111_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1111_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1112_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1112_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1113_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1113_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1114_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1114_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1115_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1115_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1116_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1116_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1117_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1117_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1118_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1118_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1119_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1119_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1120_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1120_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1121_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1121_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1122_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1122_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-11 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-11 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1123_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-11 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-11 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-11 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-11 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-11 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-11 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-11 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1123_uncapped


---20120212

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1200_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1200_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1201_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1201_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1202_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1202_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1203_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1203_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1204_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1204_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1205_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1205_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1206_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1206_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1207_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1207_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1208_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1208_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1209_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1209_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1210_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1210_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1211_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1211_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1212_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1212_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1213_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1213_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1214_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1214_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1215_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1215_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1216_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1216_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1217_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1217_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1218_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1218_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1219_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1219_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1220_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1220_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1221_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1221_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1222_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1222_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-12 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-12 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1223_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-12 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-12 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-12 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-12 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-12 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-12 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-12 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1223_uncapped


---20120213

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1300_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1300_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1301_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1301_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1302_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1302_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1303_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1303_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1304_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1304_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1305_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1305_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1306_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1306_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1307_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1307_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1308_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1308_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1309_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1309_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1310_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1310_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1311_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1311_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1312_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1312_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1313_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1313_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1314_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1314_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1315_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1315_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1316_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1316_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1317_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1317_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1318_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1318_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1319_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1319_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1320_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1320_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1321_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1321_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1322_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1322_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-13 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-13 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1323_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-13 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-13 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-13 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-13 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-13 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-13 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-13 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1323_uncapped


---20120214

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1400_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1400_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1401_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1401_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1402_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1402_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1403_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1403_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1404_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1404_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1405_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1405_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1406_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1406_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1407_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1407_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1408_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1408_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1409_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1409_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1410_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1410_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1411_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1411_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1412_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1412_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1413_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1413_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1414_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1414_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1415_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1415_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1416_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1416_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1417_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1417_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1418_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1418_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1419_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1419_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1420_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1420_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1421_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1421_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1422_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1422_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-14 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-14 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1423_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-14 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-14 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-14 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-14 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-14 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-14 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-14 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1423_uncapped


---20120215

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1500_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1500_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1501_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1501_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1502_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1502_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1503_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1503_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1504_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1504_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1505_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1505_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1506_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1506_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1507_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1507_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1508_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1508_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1509_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1509_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1510_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1510_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1511_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1511_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1512_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1512_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1513_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1513_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1514_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1514_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1515_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1515_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1516_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1516_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1517_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1517_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1518_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1518_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1519_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1519_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1520_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1520_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1521_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1521_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1522_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1522_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-15 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-15 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1523_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-15 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-15 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-15 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-15 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-15 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-15 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-15 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1523_uncapped


--20120216

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1600_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1600_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1601_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1601_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1602_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1602_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1603_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1603_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1604_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1604_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1605_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1605_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1606_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1606_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1607_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1607_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1608_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1608_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1609_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1609_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1610_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1610_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1611_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1611_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1612_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1612_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1613_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1613_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1614_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1614_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1615_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1615_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1616_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1616_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1617_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1617_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1618_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1618_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1619_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1619_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1620_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1620_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1621_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1621_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1622_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1622_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-16 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-16 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1623_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-16 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-16 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-16 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-16 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-16 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-16 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-16 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1623_uncapped


---20120217

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1700_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1700_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1701_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1701_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1702_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1702_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1703_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1703_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1704_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1704_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1705_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1705_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1706_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1706_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1707_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1707_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1708_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1708_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1709_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1709_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1710_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1710_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1711_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1711_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1712_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1712_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1713_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1713_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1714_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1714_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1715_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1715_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1716_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1716_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1717_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1717_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1718_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1718_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1719_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1719_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1720_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1720_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1721_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1721_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1722_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1722_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-17 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-17 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1723_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-17 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-17 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-17 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-17 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-17 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-17 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-17 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1723_uncapped


---20120218
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 00:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 00:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 00:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 00:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1800_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 00:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 00:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 00:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 00:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 00:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 00:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1800_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 01:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 01:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 01:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 01:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1801_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 01:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 01:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 01:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 01:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 01:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 01:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1801_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 02:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 02:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 02:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 02:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1802_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 02:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 02:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 02:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 02:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 02:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 02:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1802_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 03:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 03:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 03:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 03:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1803_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 03:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 03:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 03:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 03:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 03:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 03:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1803_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 04:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 04:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 04:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 04:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1804_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 04:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 04:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 04:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 04:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 04:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 04:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1804_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 05:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 05:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 05:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 05:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1805_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 05:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 05:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 05:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 05:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 05:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 05:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1805_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 06:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 06:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 06:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 06:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1806_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 06:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 06:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 06:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 06:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 06:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 06:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1806_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 07:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 07:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 07:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 07:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1807_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 07:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 07:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 07:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 07:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 07:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 07:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1807_uncapped

,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 08:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 08:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 08:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 08:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1808_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 08:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 08:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 08:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 08:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 08:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 08:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1808_uncapped


,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 09:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 09:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 09:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 09:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1809_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 09:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 09:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 09:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 09:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 09:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 09:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1809_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 10:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 10:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 10:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 10:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1810_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 10:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 10:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 10:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 10:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 10:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 10:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1810_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 11:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 11:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 11:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 11:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1811_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 11:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 11:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 11:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 11:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 11:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 11:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1811_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 12:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 12:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 12:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 12:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1812_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 12:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 12:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 12:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 12:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 12:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 12:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1812_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 13:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 13:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 13:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 13:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1813_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 13:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 13:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 13:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 13:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 13:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 13:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1813_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 14:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 14:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 14:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 14:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1814_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 14:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 14:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 14:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 14:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 14:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 14:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1814_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 15:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 15:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 15:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 15:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1815_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 15:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 15:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 15:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 15:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 15:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 15:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1815_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 16:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 16:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 16:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 16:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1816_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 16:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 16:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 16:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 16:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 16:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 16:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1816_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 17:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 17:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 17:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 17:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1817_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 17:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 17:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 17:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 17:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 17:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 17:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1817_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 18:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 18:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 18:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 18:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1818_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 18:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 18:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 18:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 18:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 18:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 18:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1818_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 19:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 19:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 19:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 19:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1819_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 19:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 19:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 19:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 19:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 19:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 19:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1819_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 20:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 20:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 20:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 20:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1820_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 20:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 20:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 20:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 20:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 20:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 20:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1820_uncapped





,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 21:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 21:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 21:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 21:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1821_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 21:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 21:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 21:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 21:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 21:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 21:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1821_uncapped



,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 22:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 22:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 22:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 22:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1822_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 22:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 22:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 22:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 22:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 22:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 22:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1822_uncapped




,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then 0
          when capped_x_viewing_end_time_local <cast('2012-02-18 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 23:00:00' as datetime) and capped_x_viewing_end_time_local >= cast('2012-02-18 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 23:00:00' as datetime),capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 23:00:00' as datetime) and capped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 23:00:00' as datetime) and capped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1823_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then 0
          when uncapped_x_viewing_end_time_local <cast('2012-02-18 23:00:00' as datetime) then 0
            when viewing_record_start_time_local <= cast('2012-02-18 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then 3600*weighting

            when viewing_record_start_time_local <= cast('2012-02-18 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= cast('2012-02-18 23:00:00' as datetime) then 
                    datediff(ss,cast('2012-02-18 23:00:00' as datetime),uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 23:00:00' as datetime) and uncapped_x_viewing_end_time_local <= dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= cast('2012-02-18 23:00:00' as datetime) and uncapped_x_viewing_end_time_local >= dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,cast('2012-02-18 23:00:00' as datetime)))*weighting
     else 0 end) as box_seconds_watched_201202_1823_uncapped

into dbarnett.project072_seconds_viewed_capped_uncapped_by_channel
from dbarnett.project072_all_viewing
group by channel_name_inc_hd
;

/*
select * from #hour_details order by box_seconds_watched_2012020500_capped desc;

select top 100 viewing_record_start_time_local,capped_x_viewing_end_time_local,  datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local) from dbarnett.project072_all_viewing

commit;
*/
