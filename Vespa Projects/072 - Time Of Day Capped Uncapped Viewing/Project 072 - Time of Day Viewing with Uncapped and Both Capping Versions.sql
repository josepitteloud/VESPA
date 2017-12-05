
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
create dttm index idx10 on dbarnett.project072_all_viewing(viewing_record_start_time_local);
create dttm index idx11 on dbarnett.project072_all_viewing(capped_x_viewing_end_time_local);
create dttm index idx12 on dbarnett.project072_all_viewing(uncapped_x_viewing_end_time_local);
commit;
--drop table dbarnett.project072_seconds_viewed_capped_uncapped_by_channel;

---Add on whether or not box is adsmartable----

--- Add in if box is adsmartable or not----

--Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.  
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

SELECT account_number
,service_instance_id
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
 WHERE (box_installed_dt <= cast('2012-02-05'  as date) 
   AND box_replaced_dt   > cast('2012-02-05'  as date)) or box_installed_dt is null
group by account_number
,service_instance_id
 ;

commit;
commit;
exec sp_create_tmp_table_idx '#boxes', 'account_number';
exec sp_create_tmp_table_idx '#boxes', 'service_instance_id';
exec sp_create_tmp_table_idx '#boxes', 'max_dw_created_dt';


---Create table of one record per service_instance_id---
SELECT acc.account_number
,acc.service_instance_id
,min(stb.x_pvr_type) as pvr_type
,min(stb.x_box_type) as box_type
,min(stb.x_description) as description_x
,min(stb.x_manufacturer) as manufacturer
,min(stb.x_model_number) as model_number
  INTO #boxes_with_model_info -- drop table #boxes
  FROM #boxes  AS acc left outer join sk_prod.CUST_SET_TOP_BOX AS stb 
        ON acc.account_number = stb.account_number
 and acc.max_dw_created_dt=stb.dw_created_dt
group by acc.account_number
,acc.service_instance_id
 ;

commit;
exec sp_create_tmp_table_idx '#boxes_with_model_info', 'service_instance_id';

---Create src_system_id lookup
--drop table  #subs_details;
select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
,max(case when si_service_instance_type in ('Primary DTV') then 1 else 0 end) as primary_box
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;
commit;

commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';
exec sp_create_tmp_table_idx '#subs_details', 'subscriberid';
commit;

select b.subscriberid as subscriber_id
,max(case when pvr_type in ('PVR5','PVR6','PVR7') 
            OR ( pvr_type='PVR4' AND  manufacturer in ('Pace','Samsung','Thomson')) 
          then 1 else 0 end) as adsmartable
into #adsmartable_box_detail_by_sub_id
from  #boxes_with_model_info as a
left outer join #subs_details as b
on a.service_instance_id =b.src_system_id
group by subscriber_id

;
commit;


commit;
exec sp_create_tmp_table_idx '#adsmartable_box_detail_by_sub_id', 'subscriber_id';
commit;

alter table dbarnett.project072_all_viewing add adsmartable_box tinyint;

update dbarnett.project072_all_viewing
set adsmartable_box = case when b.adsmartable is null then 0 else b.adsmartable end
from dbarnett.project072_all_viewing as a
left outer join #adsmartable_box_detail_by_sub_id as b
on a.subscriber_id=b.subscriber_id
;
commit;
--select count(*) , sum(adsmartable_box) from dbarnett.project072_all_viewing
--select count(*) , sum(adsmartable) from #adsmartable_box_detail_by_sub_id

------------------------------------
--select day_viewing , hour_viewing , count(*) from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel group by day_viewing , hour_viewing order by day_viewing , hour_viewing;
--drop table  dbarnett.project072_seconds_viewed_capped_uncapped_by_channel;


create table dbarnett.project072_seconds_viewed_capped_uncapped_by_channel
(channel_name_inc_hd varchar (90)
,day_viewing date
,hour_viewing varchar(2)
,box_seconds_watched_capped double
,box_seconds_watched_uncapped double
);
--@hour
--'2012-02-09 01:00:00.000'
create variable @hour datetime;
set @hour = '2012-02-05 00:00:00';
--set @hour = '2012-02-14 05:00:00';

WHILE @hour <= '2012-02-19 07:00:00'

BEGIN

insert into dbarnett.project072_seconds_viewed_capped_uncapped_by_channel

select channel_name_inc_hd
,cast(@hour as date) as day_viewing
,dateformat(@hour,'HH') as hour_viewing
,sum(case when capped_x_viewing_end_time_local is null then 0
          when viewing_record_start_time_local >=dateadd(minute,60,@hour) then 0
          when capped_x_viewing_end_time_local <@hour then 0
            when viewing_record_start_time_local <= @hour and capped_x_viewing_end_time_local >= dateadd(minute,60,@hour) then 3600*weighting

            when viewing_record_start_time_local <= @hour and capped_x_viewing_end_time_local >= @hour then 
                    datediff(ss,@hour,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= @hour and capped_x_viewing_end_time_local <= dateadd(minute,60,@hour) then 
            datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= @hour and capped_x_viewing_end_time_local >= dateadd(minute,60,@hour) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,@hour))*weighting
     else 0 end) as box_seconds_watched_capped
,sum(case 
          when viewing_record_start_time_local >=dateadd(minute,60,@hour) then 0
          when uncapped_x_viewing_end_time_local <@hour then 0
            when viewing_record_start_time_local <= @hour and uncapped_x_viewing_end_time_local >= dateadd(minute,60,@hour) then 3600*weighting

            when viewing_record_start_time_local <= @hour and uncapped_x_viewing_end_time_local >= @hour then 
                    datediff(ss,@hour,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= @hour and uncapped_x_viewing_end_time_local <= dateadd(minute,60,@hour) then 
            datediff(ss,viewing_record_start_time_local,uncapped_x_viewing_end_time_local)*weighting

            when viewing_record_start_time_local >= @hour and uncapped_x_viewing_end_time_local >= dateadd(minute,60,@hour) then
         datediff(ss,viewing_record_start_time_local,dateadd(minute,60,@hour))*weighting
     else 0 end) as box_seconds_watched_uncapped
--into dbarnett.project072_seconds_viewed_capped_uncapped_by_channel
from dbarnett.project072_all_viewing
where adsmartable_box=1
group by channel_name_inc_hd
,day_viewing
, hour_viewing


SET @hour  = dateadd(hour,1,@hour)
commit

end;

--select @hour
commit;
--select count(*) from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel;



---Derive V2 Capping Viewing-------
----Capping Phase 2 Comparison - Viewing 5th-18th feb Inclusive
--drop table dbarnett.project072_capping_phase_2_feb05_18;
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-02-05';
SET @var_prog_period_end    = '2012-02-19';


SET @var_cntr = 0;
SET @var_num_days = 14;

create table dbarnett.project072_capping_phase_2_feb05_18
(subscriber_id bigint
,account_number varchar(20)
,programme_trans_sk bigint
,scaling_segment_id bigint  
,scaling_weighting  real
,viewing_starts_local datetime
,viewing_stops_local datetime
);

SET @var_sql = '
insert into dbarnett.project072_capping_phase_2_feb05_18
select subscriber_id
,account_number
,programme_trans_sk
,scaling_segment_id
,scaling_weighting
--,viewing_starts as viewing_starts_utc
,case 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,viewing_starts) 
                    else viewing_starts  end as viewing_starts_local
--,viewing_stops as viewing_stops_utc
,case 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,viewing_stops) 
                    else viewing_stops  end as viewing_stops_local
--into #capped_phase_2_viewing
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*##
where timeshifting = ''LIVE''
'     ;

while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


commit;

--select count(*) from dbarnett.project072_capping_phase_2_feb05_18;

--Update scaling segment and weighting and add programme channel details---

update dbarnett.project072_capping_phase_2_feb05_18
set scaling_segment_id=b.scaling_segment_id
from dbarnett.project072_capping_phase_2_feb05_18  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (viewing_starts_local as date)  between b.reporting_starts and b.reporting_ends
commit;


update dbarnett.project072_capping_phase_2_feb05_18
set scaling_weighting=b.weighting
from dbarnett.project072_capping_phase_2_feb05_18  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (viewing_starts_local as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table dbarnett.project072_capping_phase_2_feb05_18 add Channel_Name    varchar(30);

update dbarnett.project072_capping_phase_2_feb05_18
set channel_name = b.Channel_Name
from dbarnett.project072_capping_phase_2_feb05_18 as a
left outer join sk_prod.vespa_epg_dim as b
on a.programme_trans_sk=b.programme_trans_sk

;

alter table dbarnett.project072_capping_phase_2_feb05_18 add channel_name_inc_hd varchar(40);

update dbarnett.project072_capping_phase_2_feb05_18
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from dbarnett.project072_capping_phase_2_feb05_18 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;

--select * from vespa_analysts.channel_name_lookup_old;

---Create Adsmartable Box Lookup

select subscriber_id
,max(adsmartable_box) as adsmartable_subscriber_id
into dbarnett.project072_adsmartable_box_lookup
from dbarnett.project072_all_viewing
group by subscriber_id
;
commit;
create hg index idx1 on dbarnett.project072_adsmartable_box_lookup(subscriber_id);
commit;

alter table dbarnett.project072_capping_phase_2_feb05_18 add adsmartable_box tinyint;

update dbarnett.project072_capping_phase_2_feb05_18
set adsmartable_box = case when b.adsmartable_subscriber_id is null then 0 else b.adsmartable_subscriber_id end
from dbarnett.project072_capping_phase_2_feb05_18 as a
left outer join dbarnett.project072_adsmartable_box_lookup  b
 on a.subscriber_id = b.subscriber_id
;
commit;

create lf index idx1 on dbarnett.project072_capping_phase_2_feb05_18(adsmartable_box);
create hg index idx2 on dbarnett.project072_capping_phase_2_feb05_18(channel_name_inc_hd);
create dttm index idx3 on dbarnett.project072_capping_phase_2_feb05_18(viewing_starts_local);
create dttm index idx4 on dbarnett.project072_capping_phase_2_feb05_18(viewing_stops_local);

--select count(*) , sum(adsmartable_box) from dbarnett.project072_all_viewing
--select count(*) , sum(adsmartable) from #adsmartable_box_detail_by_sub_id

------------------------------------
--select day_viewing , hour_viewing , count(*) from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel group by day_viewing , hour_viewing order by day_viewing , hour_viewing;
--drop table  dbarnett.project072_seconds_viewed_capped_uncapped_by_channel;


create table dbarnett.project072_seconds_viewed_capping_v2
(channel_name_inc_hd varchar (90)
,day_viewing date
,hour_viewing varchar(2)
,box_seconds_watched_capped_v2 double
);
--@hour
--'2012-02-09 01:00:00.000'
create variable @hour datetime;
set @hour = '2012-02-05 00:00:00';
--set @hour = '2012-02-14 05:00:00';

WHILE @hour <= '2012-02-19 07:00:00'

BEGIN

insert into dbarnett.project072_seconds_viewed_capping_v2

select channel_name_inc_hd
,cast(@hour as date) as day_viewing
,dateformat(@hour,'HH') as hour_viewing
,sum(case when viewing_stops_local is null then 0
          when viewing_starts_local >=dateadd(minute,60,@hour) then 0
          when viewing_stops_local <@hour then 0
            when viewing_starts_local <= @hour and viewing_stops_local >= dateadd(minute,60,@hour) then 3600*scaling_weighting

            when viewing_starts_local <= @hour and viewing_stops_local >= @hour then 
                    datediff(ss,@hour,viewing_stops_local)*scaling_weighting

            when viewing_starts_local >= @hour and viewing_stops_local <= dateadd(minute,60,@hour) then 
            datediff(ss,viewing_starts_local,viewing_stops_local)*scaling_weighting

            when viewing_starts_local >= @hour and viewing_stops_local >= dateadd(minute,60,@hour) then
         datediff(ss,viewing_starts_local,dateadd(minute,60,@hour))*scaling_weighting
     else 0 end) as box_seconds_watched_capped_v2
from dbarnett.project072_capping_phase_2_feb05_18
where adsmartable_box=1
group by channel_name_inc_hd
,day_viewing
, hour_viewing


SET @hour  = dateadd(hour,1,@hour)
commit

end;

--select @hour
commit;


---Match to the V2 capping created using C:\Users\barnetd\Documents\Git\Vespa\Vespa Projects\072 - Time Of Day Capped Uncapped Viewing\Project 072 - Time of Day Using Version 2 Capping v01.sql
--drop table dbarnett.project072_seconds_viewed_uncapped_and_both_capped_versions;
select a.* 
,b.box_seconds_watched_capped_v2
,a.box_seconds_watched_uncapped/3600 as box_hours_watched_uncapped
,a.box_seconds_watched_capped/3600 as box_hours_watched_capped
,b.box_seconds_watched_capped_v2/3600 as box_watched_capped_v2_hours
into dbarnett.project072_seconds_viewed_uncapped_and_both_capped_versions
from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel as a
left outer join dbarnett.project072_seconds_viewed_capping_v2 as b
on a.channel_name_inc_hd=b.channel_name_inc_hd
and a.day_viewing=b.day_viewing
and a.hour_viewing=b.hour_viewing ;
commit;
select * from dbarnett.project072_seconds_viewed_uncapped_and_both_capped_versions order by channel_name_inc_hd,day_viewing,hour_viewing ;

output to 'C:\Users\barnetd\Documents\Project 072 - Adsmart Time of Day Capped and Uncapped Viewing\viewing uncapped v1b and v2 capped.csv' format ascii;
commit;
--select * from dbarnett.project072_seconds_viewed_capping_v2
--select top 10 * from dbarnett.project072_seconds_viewed_uncapped_and_both_capped_versions order by channel_name_inc_hd,day_viewing,hour_viewing ;

--select * from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel order by box_seconds_watched_capped desc;

/*
select * from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel order by box_seconds_watched_2012020500_capped desc;

select top 100 viewing_record_start_time_local,capped_x_viewing_end_time_local,  datediff(ss,viewing_record_start_time_local,capped_x_viewing_end_time_local) from dbarnett.project072_all_viewing

commit;
*/
