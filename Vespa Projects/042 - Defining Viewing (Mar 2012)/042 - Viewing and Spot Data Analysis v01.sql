

---Project 042 - Definition of a view

--Project Name:	Adsmart Trading – Optimal Definition of a View
--Background:	AdSmart will launch in Phase 1 and the definition of one view or impact will be that at least 1 second of an advert was watched.  
--However in Phase 2, the business will look to change this very quickly to a more realistic measure of whether an advert has truly been viewed. 
--The project needs some support to aid the decision making process for how we should define a view (e.g 10%, 50%, 85% or 100% of an advert being viewed).

--Sharepoint
--http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=42&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2FProjectInceptionView%2Easpx

---PART1 - Create Capping Limits for Jan/Feb activity Revised to cap at 90% rather than 99% 
---PART2 - Creating table of viewing of Programmes broadcast on 15th Jan (viewed Live or up to 31 days later)
---PART3 - Create analysis of How much of each spot each box has viewed and weight up using scaling rules - for Sky channels
---PART4 - As part 3 but for non-Sky channels

--PART1



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




---PART2 - Viewing Data

---Run Capping for Jan/feb first - do not remove any activity <=5 seconds for this activity 
---(Cap for Live and Single Speed playback will be applied at a later point)--
-- Populate all viewing data between around 15th Jan--
--select * from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


--select programme_trans_sk from sk_prod.vespa_epg_dim where channel_name in ('Sky1','Sky1 HD') and tx_start_datetime_utc in ( '2011-08-11 19:30:00', '2011-08-11 20:00:00','2011-08-11 20:30:00') order by tx_start_datetime_utc


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

--select capped_flag  , count(*) from vespa_analysts.VESPA_all_viewing_records_20120115 where play_back_speed is null group by capped_flag order by capped_flag


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
--select count(*) from vespa_analysts.VESPA_all_viewing_records_20120115;


---Exclude where capped_flag is 2 or 3 or where record is not a viewing

--
/*    Commented out code Code not yet completed - do not run---
alter table vespa_analysts.VESPA_all_viewing_records_20120115 add records_to_exclude tinyint default 0;

update vespa_analysts.VESPA_all_viewing_records_20120115
set records_to_exclude = case when capped_flag in (2,3) then 1
                              when (
video_playing_flag = 1    and 
(    x_type_of_viewing_event in ('TV Channel Viewing','Sky+ time-shifted viewing event','HD Viewing Event')
         or (x_type_of_viewing_event = ('Other Service Viewing Event')
             and x_si_service_type = 'High Definition TV test service'))) then 0 else 1 end
;
commit;
*/


--create base table of related viewing events for second by second details for Sky1 to be used in loop---


alter table  vespa_analysts.VESPA_all_viewing_records_20120115  add row_num integer identity;
commit;

----rrr

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_1pc;

alter table  vespa_analysts.VESPA_all_viewing_records_20120115  add next_record_recorded_time_utc datetime;
commit;


create hg index idx9 on vespa_analysts.VESPA_all_viewing_records_20120115(row_num);
create hg index idx10 on vespa_analysts.VESPA_all_viewing_records_20120115(subscriber_id);

update vespa_analysts.VESPA_all_viewing_records_20120115 as a 
set next_record_recorded_time_utc = case    when a.subscriber_id<>b.subscriber_id then null
                                            when a.x_viewing_end_time<>b.x_viewing_start_time then null
                                            when a.recorded_time_utc=b.recorded_time_utc then null
                                            when a.recorded_time_utc is not null then b.recorded_time_utc else null end
from vespa_analysts.VESPA_all_viewing_records_20120115 as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115 as b
on a.row_num = b.row_num-1
;

--Update end of viewing time based on subsequent record

update vespa_analysts.VESPA_all_viewing_records_20120115
set viewing_record_end_time_local = case when next_record_recorded_time_utc is null then viewing_record_end_time_local
                                          
when dateformat(next_record_recorded_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,next_record_recorded_time_utc) 
when dateformat(next_record_recorded_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,next_record_recorded_time_utc) 
when dateformat(next_record_recorded_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,next_record_recorded_time_utc) 
                    else next_record_recorded_time_utc  end 
;
commit;

-------PART3 - Create list of Spots for analysis



---List of Spots ---

--drop table vespa_analysts.vespa_spot_data_15_jan;

select  break_start_time
, spot_start_time
, break_type 
, clearcast_commercial_no 
, date_of_transmission 
, station_code 
, preceding_programme_name 
, succeeding_programme_name
, spot_duration 
, spot_break_total_duration 
,case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
 as raw_corrected_spot_time 
,case 
      when left (break_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (break_start_time,2) as integer)-24 ||right (break_start_time,4) 
      else break_start_time end 
 as raw_corrected_break_start_time 

,case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then date_of_transmission+1
else date_of_transmission end as corrected_spot_transmission_date

into vespa_analysts.vespa_spot_data_15_jan
from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 
where 
corrected_spot_transmission_date='2012-01-15'
order by date_of_transmission , spot_start_time;

commit;

--select top 100 * from sk_prodreg.MDS_V01_20120214_CBAF_ACQUISITION_20120214 where corrected_spot_transmission_date='2012-01-15';


alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_date)
;
commit;

alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_break_start_time  datetime;
update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_break_start_time = dateadd(hour, cast(left(raw_corrected_break_start_time,2) as integer),corrected_spot_transmission_date )
;
commit;

--select distinct channel_name inc_hd from vespa_analysts.vespa_spot_data_15_jan
---Import lookup to match to viewing-----

--drop table vespa_analysts.barb_station_code_lookup_mar_2012;
create table vespa_analysts.barb_station_code_lookup_mar_2012
(
station_code_text                      varchar(5)
,station_code                            integer
,channel_name           varchar(64)
)
;

commit;
input into vespa_analysts.barb_station_code_lookup_mar_2012 from 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\Barbcode lookup march 2012.csv' format ascii;
commit;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

---Add Spot end time (end second not inclusive)

alter table  vespa_analysts.vespa_spot_data_15_jan add corrected_spot_transmission_end_datetime datetime;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_end_datetime = dateadd(second, cast(spot_duration as integer),corrected_spot_transmission_start_datetime)
;
commit;

---Add Break Start Time----



update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_break_start_time = dateadd(minute, cast(substr(raw_corrected_break_start_time,3,2) as integer),corrected_spot_transmission_break_start_time)
;
commit;

update vespa_analysts.vespa_spot_data_15_jan
set corrected_spot_transmission_break_start_time = dateadd(second, cast(right(raw_corrected_break_start_time,2) as integer),corrected_spot_transmission_break_start_time)
;
commit;



----

alter table  vespa_analysts.vespa_spot_data_15_jan add channel_name varchar(64);

update vespa_analysts.vespa_spot_data_15_jan
set channel_name = b.channel_name
from vespa_analysts.vespa_spot_data_15_jan as a
left outer join vespa_analysts.barb_station_code_lookup_mar_2012 as b
on a.station_code = b.station_code_text
;

--select * from vespa_analysts.barb_station_code_lookup_mar_2012;

---Create an equivalen of channel_name_inc_hd to match to viewing data---
--drop table vespa_analysts.spot_data_viewing_channel_lookup;
create table vespa_analysts.spot_data_viewing_channel_lookup 
( channel_name varchar(90)
,channel_name_inc_hd varchar(90)
)
;
commit;
input into vespa_analysts.spot_data_viewing_channel_lookup from 
'C:\Users\barnetd\Documents\Project 042 - Definition of a view\Spot to Viewing channel lookup.csv' format ascii;
commit;

alter table  vespa_analysts.vespa_spot_data_15_jan add channel_name_inc_hd varchar(64);

update vespa_analysts.vespa_spot_data_15_jan
set channel_name_inc_hd = b.channel_name_inc_hd
from vespa_analysts.vespa_spot_data_15_jan as a
left outer join vespa_analysts.spot_data_viewing_channel_lookup as b
on a.channel_name = b.channel_name
;

commit;

alter table  vespa_analysts.vespa_spot_data_15_jan add spot_position_in_break varchar(32);

update vespa_analysts.vespa_spot_data_15_jan
set spot_position_in_break = case  when break_start_time = spot_start_time 
            then '01: First Spot in break' 
        when  dateadd(second, cast(spot_break_total_duration as integer),corrected_spot_transmission_break_start_time)=corrected_spot_transmission_end_datetime 
            then '02: Last Spot in break'
        else '03: Mid Spot in break' end 
from vespa_analysts.vespa_spot_data_15_jan as a
;

commit;


--select * from vespa_analysts.barb_station_code_lookup_mar_2012;

--select channel_name , channel_name_inc_hd from vespa_analysts.vespa_spot_data_15_jan group by channel_name , channel_name_inc_hd;
--select * from  vespa_analysts.vespa_spot_data_sky_one_15_jan;
--select distinct channel_name_inc_hd from vespa_analysts.vespa_spot_data_15_jan

--select *  from vespa_analysts.vespa_spot_data_15_jan;

--drop table vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots;
select * into vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots from vespa_analysts.vespa_spot_data_15_jan
where channel_name_inc_hd in (
'Sky Living',
'Sky Living +1',
'Sky Living Loves',
'Sky Livingit',
'Sky Arts 1',
'Sky Arts 2',
'Sky Movies Action',
'Sky Movies Classics',
'Sky Movies Comedy',
'Sky Movies Thriller',
'Sky DramaRom',
'Sky Movies Family',
'Sky Movies Indie',
'Sky Movies Mdn Greats',
'Sky Premiere',
'Sky Prem+1',
'Sky Movies Sci-Fi/Horror',
'Sky Movies Showcase',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports 3',
'Sky Sports 4',
'Sky Sports News',
'Sky 1',
'Sky 2',
'Sky 3',
'Sky 3+1',
'Sky Atlantic'
	)
;

create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots(channel_name_inc_hd);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots(corrected_spot_transmission_end_datetime);

--select spot_duration , count(*) from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots group by spot_duration order by spot_duration;

commit;

/*
create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan(channel_name);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan(corrected_spot_transmission_end_datetime);
*/



--select distinct channel_name from vespa_analysts.vespa_spot_data_15_jan order by channel_name;


commit;
--select * from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots
--select channel_name_inc_hd , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20120115 group by channel_name_inc_hd order by records desc
--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20120115 where capped_flag = 3;


--drop table vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels ;
select *,dateformat(adjusted_event_start_time,'YYYY-MM-DD') as event_date 
into vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels 
from vespa_analysts.VESPA_all_viewing_records_20120115
where   (
            (play_back_speed is null and capped_flag in (0,1) )
                OR
            (play_back_speed = 2 and capped_flag in (0,1) )
                OR
            (play_back_speed in (4,12,24,60))
        )
and viewing_record_end_time_local is not null
and channel_name_inc_hd in (
'Sky Living',
'Sky Living +1',
'Sky Living Loves',
'Sky Livingit',
'Sky Arts 1',
'Sky Arts 2',
'Sky Movies Action',
'Sky Movies Classics',
'Sky Movies Comedy',
'Sky Movies Thriller',
'Sky DramaRom',
'Sky Movies Family',
'Sky Movies Indie',
'Sky Movies Mdn Greats',
'Sky Premiere',
'Sky Prem+1',
'Sky Movies Sci-Fi/Horror',
'Sky Movies Showcase',
'Sky News',
'Sky Sports 1',
'Sky Sports 2',
'Sky Sports 3',
'Sky Sports 4',
'Sky Sports News',
'Sky 1',
'Sky 2',
'Sky 3',
'Sky 3+1',
'Sky Atlantic')
;


commit;

create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(subscriber_id);
create hg index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(channel_name_inc_hd);
create hg index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(viewing_record_start_time_local);
create hg index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(viewing_record_end_time_local);
create hg index idx5 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(account_number);
create hg index idx6 on vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels(event_date);

--select distinct channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels;
--drop table vespa_analysts.vespa_spot_data_By_channel;

---Match to viewing data----
select account_number
, subscriber_id
, station_code
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
,min(event_date) as date_watched
, sum(case  when b.play_back_speed is not null then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_live
      
, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>2 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_playback
  
, sum(case  when (b.play_back_speed is not null and play_back_speed<>2) then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_live_or_playback

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>4 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_2x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>12 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_6x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>24 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_12x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>60 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_30x_speed


into vespa_analysts.vespa_spot_data_By_channel
from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels as b
on a.channel_name_inc_hd=b.channel_name_inc_hd
where   (viewing_record_start_time_local<corrected_spot_transmission_end_datetime and viewing_record_end_time_local>corrected_spot_transmission_start_datetime)
group by account_number
,subscriber_id
, station_code 
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
;

commit;

--select channel_name_inc_hd, count(*) from vespa_analysts.vespa_spot_data_By_channel group by channel_name_inc_hd order by channel_name_inc_hd;


--select top 500 * from vespa_analysts.vespa_spot_data_By_channel;
--select top 500 * from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots;

---Add other spot information back on to table ---
commit;
alter table vespa_analysts.vespa_spot_data_By_channel add break_type varchar(2);

update  vespa_analysts.vespa_spot_data_By_channel 
set break_type = b.break_type
from vespa_analysts.vespa_spot_data_By_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;

--select top 100 * from vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots;

alter table vespa_analysts.vespa_spot_data_By_channel add spot_position_in_break varchar(32);

update  vespa_analysts.vespa_spot_data_By_channel 
set spot_position_in_break = b.spot_position_in_break
from vespa_analysts.vespa_spot_data_By_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;



--select top 100 * from vespa_analysts.vespa_spot_data_By_channel;
--alter table vespa_analysts.vespa_spot_data_By_channel delete viewing_time_of_day ;
alter table vespa_analysts.vespa_spot_data_By_channel add viewing_time_of_day varchar(32);

update vespa_analysts.vespa_spot_data_By_channel 
set viewing_time_of_day = case  when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('00','01','02','03','04','05') 
                                    then '01: Night (00:00 - 05:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('06','07','08') 
                                    then '02: Breakfast (06:00 - 08:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('09','10','11') 
                                    then '03: Morning (09:00 - 11:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('12','13','14') 
                                    then '04: Lunch (12:00 - 14:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('15','16','17') 
                                    then '05: Early Prime (15:00 - 17:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('18','19','20') 
                                    then '06: Prime (18:00 - 20:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('21','22','23') 
                                    then '07: Late Night (21:00 - 23:59)' 

else '08: Other' end
from vespa_analysts.vespa_spot_data_By_channel 
;
commit;

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels ;



---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----

---Add Scaling ID each account is to be assigned to based on the day they view the spot
--alter table vespa_analysts.vespa_spot_data_By_channel delete scaling_segment_id
alter table vespa_analysts.vespa_spot_data_By_channel add scaling_segment_id integer;

update vespa_analysts.vespa_spot_data_By_channel 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.vespa_spot_data_By_channel  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.date_watched as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.vespa_spot_data_By_channel  group by scaling_segment_id order by scaling_segment_id;

---Add weight for each scaling ID for each record

alter table vespa_analysts.vespa_spot_data_By_channel add weighting double;

update vespa_analysts.vespa_spot_data_By_channel 
set weighting=b.weighting
from vespa_analysts.vespa_spot_data_By_channel  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.date_watched as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.vespa_spot_data_By_channel add affluence varchar(10) ;
alter table vespa_analysts.vespa_spot_data_By_channel add pvr tinyint;

update vespa_analysts.vespa_spot_data_By_channel 
set affluence=b.affluence
,pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.vespa_spot_data_By_channel  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;


--select weighting , count(*) from vespa_analysts.vespa_spot_data_By_channel  group by weighting;
--select affluence , count(*) from vespa_analysts.vespa_spot_data_By_channel  group by affluence;
--select viewing_time_of_day ,count(*) from vespa_analysts.vespa_spot_data_By_channel group by viewing_time_of_day;
--select break_type ,count(*) from vespa_analysts.vespa_spot_data_By_channel group by break_type;
--select top 500 * from vespa_analysts.vespa_spot_data_By_channel ;
--select top 500 * from vespa_analysts.scaling_segments_lookup ;
--select distinct channel_name from vespa_analysts.vespa_spot_data_15_jan_ad_sample order by channel_name;
--select channel_name, channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115_selected_channels group by channel_name, channel_name_inc_hd order by channel_name_inc_hd;


--select min(tx_date) from sk_prod.vespa_epg_dim where upper(channel_name) like '%DRAMA%'
--select channel_name_inc_hd , count(*) from vespa_analysts.vespa_spot_data_By_channel group by channel_name_inc_hd order by channel_name_inc_hd

---Pivot For Live or Playback Viewing

select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live_or_playback>spot_duration then spot_duration else seconds_of_spot_viewed_live_or_playback end as seconds_of_ad_viewed
,affluence
,pvr
,count(*) as boxes
,sum(weighting) as weighted_boxes
into #live_playback_channel_pivot
from vespa_analysts.vespa_spot_data_By_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;
commit;

--select channel_name_inc_hd , count(*) from #live_playback_channel_pivot group by channel_name_inc_hd;

select * from #live_playback_channel_pivot;
output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_playback_pivot_data.csv' format ascii;


---Pivot For Live Only Viewing
select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live>spot_duration then spot_duration else seconds_of_spot_viewed_live end as seconds_of_ad_viewed
,affluence
,pvr
,sum(weighting) as weighted_boxes
,count(*) as boxes
into #live_only_pivot
from vespa_analysts.vespa_spot_data_By_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;

select * from #live_only_pivot;
output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_only_pivot_data.csv' format ascii;

commit;

---PART4 - Repeat for non-Sky Channels
select * into vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots from vespa_analysts.vespa_spot_data_15_jan
where channel_name_inc_hd in (
'E4'
,'ITV2'
,'ITV3'
,'UKTV Dave'
,'Comedy Central'
,'Watch'
,'Nick Jr'
,'Cartoonito'
,'ESPN'
)
;

create hg index idx1 on vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots(channel_name_inc_hd);
create hg index idx2 on vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots(corrected_spot_transmission_start_datetime);
create hg index idx3 on vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots(corrected_spot_transmission_end_datetime);

--select spot_duration , count(*) from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots group by spot_duration order by spot_duration;

commit;


commit;
--select * from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots
--select channel_name_inc_hd , count(*) as records from vespa_analysts.VESPA_all_viewing_records_20120115 group by channel_name_inc_hd order by records desc
--select top 500 *  from vespa_analysts.VESPA_all_viewing_records_20120115 where capped_flag = 3;


--drop table vespa_analysts.VESPA_all_viewing_records_20120115_sky_channels ;
select *,dateformat(adjusted_event_start_time,'YYYY-MM-DD') as event_date 
into vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels 
from vespa_analysts.VESPA_all_viewing_records_20120115
where   (
            (play_back_speed is null and capped_flag in (0,1) )
                OR
            (play_back_speed = 2 and capped_flag in (0,1) )
                OR
            (play_back_speed in (4,12,24,60))
        )
and viewing_record_end_time_local is not null
and channel_name_inc_hd in (
'E4'
,'ITV2'
,'ITV3'
,'UKTV Dave'
,'Comedy Central'
,'Watch'
,'Nick Jr'
,'Cartoonito'
,'ESPN')
;


commit;

create hg index idx1 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(subscriber_id);
create hg index idx2 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(channel_name_inc_hd);
create hg index idx3 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(viewing_record_start_time_local);
create hg index idx4 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(viewing_record_end_time_local);
create hg index idx5 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(account_number);
create hg index idx6 on vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels(event_date);

--select distinct channel_name_inc_hd from vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels;
--drop table vespa_analysts.vespa_spot_data_By_channel;

---Match to viewing data----
select account_number
, subscriber_id
, station_code
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
,min(event_date) as date_watched
, sum(case  when b.play_back_speed is not null then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_live
      
, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>2 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_playback
  
, sum(case  when (b.play_back_speed is not null and play_back_speed<>2) then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_live_or_playback

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>4 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_2x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>12 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_6x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>24 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_12x_speed

, sum(case  when b.play_back_speed is null then 0
            when play_back_speed<>60 then 0
 
            when viewing_record_start_time_local>=corrected_spot_transmission_end_datetime  then 0
            when viewing_record_end_time_local<corrected_spot_transmission_start_datetime  then 0
           
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
                    and  
                 viewing_record_end_time_local>=corrected_spot_transmission_end_datetime then cast(spot_duration as integer)
            
            when viewing_record_start_time_local<corrected_spot_transmission_start_datetime 
            then datediff(second,corrected_spot_transmission_start_datetime,viewing_record_end_time_local)
        
            when viewing_record_end_time_local>=corrected_spot_transmission_end_datetime 
            then datediff(second,viewing_record_start_time_local,corrected_spot_transmission_end_datetime) else 0 end) as seconds_of_spot_viewed_30x_speed


into vespa_analysts.vespa_spot_data_By_non_sky_channel
from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots as a
left outer join vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels as b
on a.channel_name_inc_hd=b.channel_name_inc_hd
where   (viewing_record_start_time_local<corrected_spot_transmission_end_datetime and viewing_record_end_time_local>corrected_spot_transmission_start_datetime)
group by account_number
,subscriber_id
, station_code 
, a.channel_name_inc_hd
, corrected_spot_transmission_start_datetime
, corrected_spot_transmission_end_datetime
, spot_duration 
;

commit;

--select channel_name_inc_hd, count(*) from vespa_analysts.vespa_spot_data_By_non_sky_channel group by channel_name_inc_hd order by channel_name_inc_hd;


--select top 500 * from vespa_analysts.vespa_spot_data_By_non_sky_channel;
--select top 500 * from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots;

---Add other spot information back on to table ---
commit;
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add break_type varchar(2);

update  vespa_analysts.vespa_spot_data_By_non_sky_channel 
set break_type = b.break_type
from vespa_analysts.vespa_spot_data_By_non_sky_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;

--select top 100 * from vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots;

alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add spot_position_in_break varchar(32);

update  vespa_analysts.vespa_spot_data_By_non_sky_channel 
set spot_position_in_break = b.spot_position_in_break
from vespa_analysts.vespa_spot_data_By_non_sky_channel as a 
left outer join vespa_analysts.vespa_spot_data_15_jan_non_sky_channel_spots as b
on a.station_code=b.station_code and a.corrected_spot_transmission_start_datetime=b.corrected_spot_transmission_start_datetime
;
commit;



--select top 100 * from vespa_analysts.vespa_spot_data_By_non_sky_channel;
--alter table vespa_analysts.vespa_spot_data_By_non_sky_channel delete viewing_time_of_day ;
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add viewing_time_of_day varchar(32);

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set viewing_time_of_day = case  when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('00','01','02','03','04','05') 
                                    then '01: Night (00:00 - 05:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('06','07','08') 
                                    then '02: Breakfast (06:00 - 08:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('09','10','11') 
                                    then '03: Morning (09:00 - 11:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('12','13','14') 
                                    then '04: Lunch (12:00 - 14:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('15','16','17') 
                                    then '05: Early Prime (15:00 - 17:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('18','19','20') 
                                    then '06: Prime (18:00 - 20:59)' 
                                when dateformat(corrected_spot_transmission_start_datetime,'HH') in ('21','22','23') 
                                    then '07: Late Night (21:00 - 23:59)' 

else '08: Other' end
from vespa_analysts.vespa_spot_data_By_non_sky_channel 
;
commit;

--select top 500 * from vespa_analysts.VESPA_all_viewing_records_20120115_non_sky_channels ;



---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----

---Add Scaling ID each account is to be assigned to based on the day they view the spot
--alter table vespa_analysts.vespa_spot_data_By_non_sky_channel delete scaling_segment_id
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add scaling_segment_id integer;

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.vespa_spot_data_By_non_sky_channel  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.date_watched as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.vespa_spot_data_By_non_sky_channel  group by scaling_segment_id order by scaling_segment_id;

---Add weight for each scaling ID for each record

alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add weighting double;

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set weighting=b.weighting
from vespa_analysts.vespa_spot_data_By_non_sky_channel  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.date_watched as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add affluence varchar(10) ;
alter table vespa_analysts.vespa_spot_data_By_non_sky_channel add pvr tinyint;

update vespa_analysts.vespa_spot_data_By_non_sky_channel 
set affluence=b.affluence
,pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.vespa_spot_data_By_non_sky_channel  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;


---Pivot For Live or Playback Viewing

select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live_or_playback>spot_duration then spot_duration else seconds_of_spot_viewed_live_or_playback end as seconds_of_ad_viewed
,affluence
,pvr

,count(*) as boxes
,sum(weighting) as weighted_boxes
into #live_playback_channel_pivot_non_sky
from vespa_analysts.vespa_spot_data_By_non_sky_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;
commit;

--select channel_name_inc_hd , count(*) from #live_playback_channel_pivot group by channel_name_inc_hd;

select * from #live_playback_channel_pivot_non_sky;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_playback_pivot_data_non_sky.csv' format ascii;





---Pivot For Live Only Viewing
select  channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
, case when break_type = 'CB' then '01: Centre Break' when break_type = 'EB' then '02: End Break' else '03: Other' end as break_position
,spot_position_in_break
, case when seconds_of_spot_viewed_live>spot_duration then spot_duration else seconds_of_spot_viewed_live end as seconds_of_ad_viewed
,affluence
,pvr
,sum(weighting) as weighted_boxes
,count(*) as boxes
into #live_only_pivot_non_sky
from vespa_analysts.vespa_spot_data_By_non_sky_channel
group by channel_name_inc_hd
, spot_duration 
,viewing_time_of_day
,break_position
,spot_position_in_break
,seconds_of_ad_viewed
,affluence
,pvr
order by channel_name_inc_hd
;

select * from #live_only_pivot_non_sky;

output to 'C:\Users\barnetd\Documents\Project 042 - Definition of a view\live_only_pivot_data_non_sky.csv' format ascii;


commit;
















/*
create table input_test
(ch1 varchar(64) , ch2 varchar(64)
);

insert into input_test (ch1, ch2) 
values("Bliss","Bliss",
"CBS Action","CBS Action",
"CBS Drama","CBS Drama",
"CBS Reality","CBS Reality"
)
;
commit;





