

----Project 047 Part 2 -----
--http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=47&Source=http%3A%2F%2Frtci%2FLists%2FRTCI%2520IC%2520dev%2FProjectInceptionView%2Easpx

/*  From Sharepoint
To understand the viability of various AdSmart and linear trading approaches, we wish understand how much inventory could be created 
through just trading linear ‘wastage’.  In order to identify the reachable wastage from the BARB traded audience we have had to define a set of
 ‘Mirror Segments’, which define the reachable audiences for each BARB traded audience. 
The reason we cannot reach all of the wastage is that AdSmart cannot identify which householder is watching at any one point in time, 
therefore whilst there might be wastage in an advert bought against housewives with children if someone other than the housewife is 
viewing the television there is no way for us to identify and then serve a different advert to this person. However we could in this 
situation still deliver alternative advertising to any household with no children as nobody in that household (with the exception of visitors) 
would fall into the BARB traded demographic. The brief is still being refined by Rory Skrebowski. 
*/

---Two Week Live Viewing Activity (Mon 2nd - Sun 15th Jan 2012 inclusive)

---PART A  - Live Viewing of Sky Channels ---

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
SET @var_prog_period_start  = '2012-01-02';
SET @var_prog_period_end    = '2012-01-16';


SET @var_cntr = 0;
SET @var_num_days = 15;       -- 
--select top 500 * from vespa_analysts.project047_sky_channels_live_2nd_15th_jan;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project047_sky_channels_live_2nd_15th_jan;
-- To store all the viewing records:
create table vespa_analysts.project047_sky_channels_live_2nd_15th_jan ( -- drop table vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
    insert into vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
              and x_si_service_type = ''High Definition TV test service''))
     and panel_id in (4,5)
and play_back_speed is null
and (
        upper(left(channel_name,3)) = ''SKY''
         or 
        channel_name in (''PICK TV'',''PICK TV+1'')
    )
'     ;

--select distinct channel_name from  sk_prod.VESPA_EPG_DIM order by channel_name;
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

alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add live tinyint;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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

alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add channel_name_inc_hd varchar(40);

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;


-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(subscriber_id);
create dttm index idx2 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(recorded_time_utc);
create lf index idx4 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(live)
create dttm index idx5 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project047_sky_channels_live_2nd_15th_jan(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
        vespa_analysts.project047_sky_channels_live_2nd_15th_jan base left outer join vespa_max_caps_jan_feb_2012 caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
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

--select capped_flag  , count(*) from vespa_analysts.project047_sky_channels_live_2nd_15th_jan where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project047_sky_channels_live_2nd_15th_jan where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3---

---Deletion of capped records commented out initially - for evaluation purposes---


/*
delete from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
where capped_flag in (2,3)
;
commit;
*/


---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_start_time_local datetime;


alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add viewing_record_end_time_local datetime;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;


---
update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;

--select top 100 * from vespa_analysts.project047_sky_channels_live_2nd_15th_jan;

update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set viewing_record_start_time_local= case 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-00' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-00' then dateadd(hh,1,viewing_record_start_time_utc) 
when dateformat(viewing_record_start_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-00'  then dateadd(hh,1,viewing_record_start_time_utc) 
                    else viewing_record_start_time_utc  end
,viewing_record_end_time_local=case 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-00' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-00' then dateadd(hh,1,viewing_record_end_time_utc) 
when dateformat(viewing_record_end_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-00' then dateadd(hh,1,viewing_record_end_time_utc) 
                    else viewing_record_end_time_utc  end
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan
;
commit;

--select  dateformat(Adjusted_Event_Start_Time,'YYYY-MM-DD') as day_detail,count(*) from vespa_analysts.project047_sky_channels_live_2nd_15th_jan group by day_detail order by day_detail;



--Remove Capped Records
--Add weightings
--Create HH Profile Variables
--Add whether boxes are adsmartable or not


alter table vespa_analysts.project047_sky_channels_live_2nd_15th_jan add weighting double;

---Create an interim lookup t


update vespa_analysts.project047_sky_channels_live_2nd_15th_jan
set weighting = c.weighting
from vespa_analysts.project047_sky_channels_live_2nd_15th_jan as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
left outer join vespa_analysts.scaling_weightings as c
on b.scaling_segment_id=c.scaling_segment_id
where cast (a.Adjusted_Event_Start_Time as date)  between b.reporting_starts and b.reporting_ends
and c.scaling_day = cast (a.Adjusted_Event_Start_Time as date)
;
commit;




