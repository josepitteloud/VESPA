
----Repeat for 20120422


---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-22';
SET @var_prog_period_end    = '2012-04-30';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120422 ( -- drop table vespa_analysts.project060_all_viewing_20120422
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
    insert into vespa_analysts.project060_all_viewing_20120422
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



and (   Adjusted_Event_Start_Time between ''2012-04-22 05:00:00'' and ''2012-04-23 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-22 05:00:00'' and ''2012-04-23 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-22 05:00:00'' and ''2012-04-23 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-22 05:00:00'' and ''2012-04-23 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120422;

commit;

alter table vespa_analysts.project060_all_viewing_20120422 add live tinyint;

update vespa_analysts.project060_all_viewing_20120422
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120422
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120422 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120422
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120422 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120422(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120422(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120422(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120422(live)
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120422(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120422(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120422(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120422(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120422
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120422
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120422
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120422
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
        vespa_analysts.project060_all_viewing_20120422 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120422
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120422
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
update vespa_analysts.project060_all_viewing_20120422
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120422 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120422 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120422 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120422 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120422 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120422 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120422
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120422
;
commit;


---
update vespa_analysts.project060_all_viewing_20120422
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120422
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120422
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
from vespa_analysts.project060_all_viewing_20120422
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120422 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120422
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120422
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120422 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120422 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120422
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
from vespa_analysts.project060_all_viewing_20120422
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
--alter table vespa_analysts.project060_all_viewing_20120422 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120422 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120422 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120422  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select * into vespa_analysts.scaling_dialback_intervals_old from vespa_analysts.scaling_dialback_intervals;commit;
--select * into vespa_analysts.scaling_weightings_old from vespa_analysts.scaling_weightings;commit;
--delete  from vespa_analysts.scaling_dialback_intervals where reporting_starts is null or reporting_starts is not null; commit;
--delete  from  vespa_analysts.scaling_weightings where scaling_segment_id is null or scaling_segment_id is not null; commit;
--select * from vespa_analysts.scaling_dialback_intervals
--select * from vespa_analysts.scaling_weightings
--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120422  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120422 add weighting double;

update vespa_analysts.project060_all_viewing_20120422 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120422  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120422 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120422 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120422 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120422  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120422 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120422 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120422 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120422 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120422
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120422
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120422
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120422 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120422 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120422 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120422 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120422 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120422 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120422;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120422;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120422;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120422;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120422;
drop table vespa_analysts.project060_allocated_minutes_total_20120422;
drop table vespa_analysts.project060_full_allocated_minutes_20120422;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120422
from vespa_analysts.project060_all_viewing_20120422 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120422
from vespa_analysts.project060_all_viewing_20120422 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120422
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120422
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120422
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120422
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120422
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120422
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120422
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120422(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120422(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120422
from vespa_analysts.project060_partial_minutes_for_allocation_20120422
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120422(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120422(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120422
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120422
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120422(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120422(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120422
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120422 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120422 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120422
from vespa_analysts.project060_all_viewing_20120422 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120422

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120422
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120422(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120422(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120422(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120422-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120422
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120422
(  full_column_detail '\n')
FROM '/staging2/B20120422.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120422 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120422 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120422 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120422 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120422 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120422;
--drop table vespa_analysts.project060_spot_file_20120422;
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
into vespa_analysts.project060_spot_file_20120422
from vespa_analysts.project060_raw_spot_file_20120422
;

--select * from vespa_analysts.project060_spot_file_20120422 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120422 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120422 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120422
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120422 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120422 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120422 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120422_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120422_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120422_expanded
from vespa_analysts.project060_spot_file_20120422 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120422 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120422_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120422_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120422_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120422_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120422_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120422_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120422_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120422_expanded
;


alter table vespa_analysts.project060_spot_file_20120422_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120422_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120422_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120422_expanded
;

alter table  vespa_analysts.project060_spot_file_20120422_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120422_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120422_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120422_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120422_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120422_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120422_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120422;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120422
from vespa_analysts.project060_spot_file_20120422_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120422_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120422_expanded(corrected_spot_transmission_start_minute);


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
into vespa_analysts.project060_spot_summary_viewing_figures_20120422
from vespa_analysts.project060_spot_file_20120422_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120422 as b
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
--select top 100 * from vespa_analysts.project060_spot_summary_viewing_figures_20120422;


update vespa_analysts.project060_spot_summary_viewing_figures_20120422
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120422 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120422;
--commit;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120422.csv' format ascii;




---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-21';
SET @var_prog_period_end    = '2012-04-29';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120421 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120421
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



and (   Adjusted_Event_Start_Time between ''2012-04-21 05:00:00'' and ''2012-04-22 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-21 05:00:00'' and ''2012-04-22 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-21 05:00:00'' and ''2012-04-22 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-21 05:00:00'' and ''2012-04-22 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120421;

commit;

alter table vespa_analysts.project060_all_viewing_20120421 add live tinyint;

update vespa_analysts.project060_all_viewing_20120421
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120421
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120421 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120421
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120421 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120421(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120421(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120421(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120421(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120421(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120421(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120421(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120421(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120421
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120421
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120421
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120421
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
        vespa_analysts.project060_all_viewing_20120421 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120421
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120421
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
update vespa_analysts.project060_all_viewing_20120421
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120421 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120421 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120421 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120421 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120421 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120421 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120421
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120421
;
commit;


---
update vespa_analysts.project060_all_viewing_20120421
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120421
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120421
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
from vespa_analysts.project060_all_viewing_20120421
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120421 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120421
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120421
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120421 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120421 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120421
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
from vespa_analysts.project060_all_viewing_20120421
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
--alter table vespa_analysts.project060_all_viewing_20120421 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120421 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120421 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120421  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120421  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120421 add weighting double;

update vespa_analysts.project060_all_viewing_20120421 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120421  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120421 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120421 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120421 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120421  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120421 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120421 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120421 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120421 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120421
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120421
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120421
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120421 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120421 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120421 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120421 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120421 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120421 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120421;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120421;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120421;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120421;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120421;
drop table vespa_analysts.project060_allocated_minutes_total_20120421;
drop table vespa_analysts.project060_full_allocated_minutes_20120421;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120421
from vespa_analysts.project060_all_viewing_20120421 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120421
from vespa_analysts.project060_all_viewing_20120421 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120421
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120421
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120421
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120421
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120421
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120421
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120421
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120421(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120421(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120421
from vespa_analysts.project060_partial_minutes_for_allocation_20120421
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120421(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120421(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120421
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120421
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120421(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120421(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120421
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120421 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120421 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120421
from vespa_analysts.project060_all_viewing_20120421 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120421

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120421
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120421 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120421(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120421(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120421(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120421-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120421
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120421
(  full_column_detail '\n')
FROM '/staging2/B20120421.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120421 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120421 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120421 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120421 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120421 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120421;
--drop table vespa_analysts.project060_spot_file_20120421;
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
into vespa_analysts.project060_spot_file_20120421
from vespa_analysts.project060_raw_spot_file_20120421
;

--select * from vespa_analysts.project060_spot_file_20120421 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120421 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120421 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120421
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120421 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

--select top 10 * into  vespa_analysts.project060_spot_file_20120421_test from  vespa_analysts.project060_spot_file_20120421; commit;

--create table vespa_analysts.test_db (test varchar(10));

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120421 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120421 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120421_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120421_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120421_expanded
from vespa_analysts.project060_spot_file_20120421 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120421 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120421_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120421_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120421_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120421_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120421_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120421_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120421_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120421_expanded
;


alter table vespa_analysts.project060_spot_file_20120421_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120421_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120421_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120421_expanded
;

alter table  vespa_analysts.project060_spot_file_20120421_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120421_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120421_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120421_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120421_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120421_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120421_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120421;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120421
from vespa_analysts.project060_spot_file_20120421_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120421_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120421_expanded(corrected_spot_transmission_start_minute);


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
into vespa_analysts.project060_spot_summary_viewing_figures_20120421
from vespa_analysts.project060_spot_file_20120421_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120421 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120421
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120421 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120421;
--commit;
output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120421.csv' format ascii;





---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-20';
SET @var_prog_period_end    = '2012-04-28';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120420 ( -- drop table vespa_analysts.project060_all_viewing_20120420
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
    insert into vespa_analysts.project060_all_viewing_20120420
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



and (   Adjusted_Event_Start_Time between ''2012-04-20 05:00:00'' and ''2012-04-21 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-20 05:00:00'' and ''2012-04-21 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-20 05:00:00'' and ''2012-04-21 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-20 05:00:00'' and ''2012-04-21 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120420;

commit;

alter table vespa_analysts.project060_all_viewing_20120420 add live tinyint;

update vespa_analysts.project060_all_viewing_20120420
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120420
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120420 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120420
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120420 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120420(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120420(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120420(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120420(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120420(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120420(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120420(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120420(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120420
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120420
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120420
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120420
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
        vespa_analysts.project060_all_viewing_20120420 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120420
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120420
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
update vespa_analysts.project060_all_viewing_20120420
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120420 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120420 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120420 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120420 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120420 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120420 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120420
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120420
;
commit;


---
update vespa_analysts.project060_all_viewing_20120420
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120420
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120420
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
from vespa_analysts.project060_all_viewing_20120420
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120420 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120420
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120420
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120420 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120420 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120420
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
from vespa_analysts.project060_all_viewing_20120420
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
--alter table vespa_analysts.project060_all_viewing_20120420 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120420 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120420 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120420  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120420  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record
--alter table vespa_analysts.project060_all_viewing_20120420 delete weighting;
alter table vespa_analysts.project060_all_viewing_20120420 add weighting double;

update vespa_analysts.project060_all_viewing_20120420 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120420  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120420 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120420 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120420 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120420  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120420 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120420 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120420 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120420 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120420
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120420
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120420
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120420 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120420 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120420 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120420 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120420 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120420 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120420;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120420;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120420;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120420;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120420;
drop table vespa_analysts.project060_allocated_minutes_total_20120420;
drop table vespa_analysts.project060_full_allocated_minutes_20120420;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120420
from vespa_analysts.project060_all_viewing_20120420 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120420
from vespa_analysts.project060_all_viewing_20120420 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120420
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120420
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120420
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120420
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120420
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120420
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120420
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120420(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120420(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120420
from vespa_analysts.project060_partial_minutes_for_allocation_20120420
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120420(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120420(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120420
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120420
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120420(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120420(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120420
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120420 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120420 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120420
from vespa_analysts.project060_all_viewing_20120420 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120420

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120420
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120420 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120420(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120420(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120420(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120420-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120420
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120420
(  full_column_detail '\n')
FROM '/staging2/B20120420.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120420 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120420 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120420 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120420 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120420 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120420;
--drop table vespa_analysts.project060_spot_file_20120420;
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
into vespa_analysts.project060_spot_file_20120420
from vespa_analysts.project060_raw_spot_file_20120420
;

--select * from vespa_analysts.project060_spot_file_20120420 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120420 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120420 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120420
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120420 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120420 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120420 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120420_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120420_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120420_expanded
from vespa_analysts.project060_spot_file_20120420 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120420 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120420_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120420_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120420_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120420_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120420_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120420_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120420_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120420_expanded
;


alter table vespa_analysts.project060_spot_file_20120420_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120420_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120420_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120420_expanded
;

alter table  vespa_analysts.project060_spot_file_20120420_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120420_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120420_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120420_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120420_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120420_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120420_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120420;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120420
from vespa_analysts.project060_spot_file_20120420_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120420_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120420_expanded(corrected_spot_transmission_start_minute);


---Get Views by Spot
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120420;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120420
from vespa_analysts.project060_spot_file_20120420_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120420 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120420
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120420 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120420;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120420.csv' format ascii;
commit;
*/
commit;



---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-19';
SET @var_prog_period_end    = '2012-04-27';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120419 ( -- drop table vespa_analysts.project060_all_viewing_20120419
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
    insert into vespa_analysts.project060_all_viewing_20120419
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



and (   Adjusted_Event_Start_Time between ''2012-04-19 05:00:00'' and ''2012-04-20 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-19 05:00:00'' and ''2012-04-20 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-19 05:00:00'' and ''2012-04-20 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-19 05:00:00'' and ''2012-04-20 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120419;

commit;

alter table vespa_analysts.project060_all_viewing_20120419 add live tinyint;

update vespa_analysts.project060_all_viewing_20120419
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120419
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120419 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120419
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120419 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120419(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120419(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120419(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120419(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120419(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120419(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120419(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120419(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120419
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120419
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120419
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120419
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
        vespa_analysts.project060_all_viewing_20120419 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120419
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120419
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
update vespa_analysts.project060_all_viewing_20120419
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120419 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120419 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120419 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120419 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120419 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120419 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120419
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120419
;
commit;


---
update vespa_analysts.project060_all_viewing_20120419
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120419
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120419
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
from vespa_analysts.project060_all_viewing_20120419
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120419 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120419
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120419
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120419 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120419 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120419
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
from vespa_analysts.project060_all_viewing_20120419
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
--alter table vespa_analysts.project060_all_viewing_20120419 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120419 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120419 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120419  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120419  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record
--alter table vespa_analysts.project060_all_viewing_20120419 delete weighting;
alter table vespa_analysts.project060_all_viewing_20120419 add weighting double;

update vespa_analysts.project060_all_viewing_20120419 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120419  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120419 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120419 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120419 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120419  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120419 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120419 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120419 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120419 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120419
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120419
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120419
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120419 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120419 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120419 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120419 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120419 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120419 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120419;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120419;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120419;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120419;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120419;
drop table vespa_analysts.project060_allocated_minutes_total_20120419;
drop table vespa_analysts.project060_full_allocated_minutes_20120419;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120419
from vespa_analysts.project060_all_viewing_20120419 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120419
from vespa_analysts.project060_all_viewing_20120419 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120419
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120419
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120419
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120419
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120419
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120419
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120419
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120419(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120419(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120419
from vespa_analysts.project060_partial_minutes_for_allocation_20120419
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120419(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120419(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120419
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120419
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120419(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120419(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120419
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120419 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120419 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120419
from vespa_analysts.project060_all_viewing_20120419 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120419

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120419
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120419 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120419(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120419(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120419(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120419-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120419
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120419
(  full_column_detail '\n')
FROM '/staging2/B20120419.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120419 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120419 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120419 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120419 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120419 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120419;
--drop table vespa_analysts.project060_spot_file_20120419;
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
into vespa_analysts.project060_spot_file_20120419
from vespa_analysts.project060_raw_spot_file_20120419
;

--select * from vespa_analysts.project060_spot_file_20120419 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120419 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120419 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120419
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120419 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120419 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120419 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120419_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120419_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120419_expanded
from vespa_analysts.project060_spot_file_20120419 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120419 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120419_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120419_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120419_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120419_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120419_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120419_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120419_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120419_expanded
;


alter table vespa_analysts.project060_spot_file_20120419_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120419_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120419_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120419_expanded
;

alter table  vespa_analysts.project060_spot_file_20120419_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120419_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120419_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120419_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120419_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120419_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120419_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120419;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120419
from vespa_analysts.project060_spot_file_20120419_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120419_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120419_expanded(corrected_spot_transmission_start_minute);


---Get Views by Spot
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120419;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120419
from vespa_analysts.project060_spot_file_20120419_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120419 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120419
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120419 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120419;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120419.csv' format ascii;
commit;
*/





---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-18';
SET @var_prog_period_end    = '2012-04-26';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120418 ( -- drop table vespa_analysts.project060_all_viewing_20120418
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
    insert into vespa_analysts.project060_all_viewing_20120418
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



and (   Adjusted_Event_Start_Time between ''2012-04-18 05:00:00'' and ''2012-04-19 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-18 05:00:00'' and ''2012-04-19 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-18 05:00:00'' and ''2012-04-19 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-18 05:00:00'' and ''2012-04-19 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120418;

commit;

alter table vespa_analysts.project060_all_viewing_20120418 add live tinyint;

update vespa_analysts.project060_all_viewing_20120418
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120418
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120418 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120418
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120418 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120418(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120418(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120418(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120418(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120418(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120418(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120418(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120418(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120418
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120418
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120418
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120418
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
        vespa_analysts.project060_all_viewing_20120418 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120418
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120418
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
update vespa_analysts.project060_all_viewing_20120418
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120418 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120418 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120418 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120418 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120418 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120418 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120418
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120418
;
commit;


---
update vespa_analysts.project060_all_viewing_20120418
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120418
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120418
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
from vespa_analysts.project060_all_viewing_20120418
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120418 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120418
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120418
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120418 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120418 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120418
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
from vespa_analysts.project060_all_viewing_20120418
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
--alter table vespa_analysts.project060_all_viewing_20120418 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120418 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120418 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120418  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120418  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record
--alter table vespa_analysts.project060_all_viewing_20120418 delete weighting;
alter table vespa_analysts.project060_all_viewing_20120418 add weighting double;

update vespa_analysts.project060_all_viewing_20120418 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120418  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120418 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120418 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120418 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120418  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120418 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120418 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120418 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120418 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120418
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120418
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120418
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120418 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120418 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120418 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120418 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120418 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120418 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120418;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120418;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120418;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120418;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120418;
drop table vespa_analysts.project060_allocated_minutes_total_20120418;
drop table vespa_analysts.project060_full_allocated_minutes_20120418;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120418
from vespa_analysts.project060_all_viewing_20120418 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120418
from vespa_analysts.project060_all_viewing_20120418 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120418
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120418
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120418
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120418
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120418
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120418
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120418
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120418(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120418(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120418
from vespa_analysts.project060_partial_minutes_for_allocation_20120418
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120418(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120418(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120418
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120418
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120418(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120418(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120418
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120418 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120418 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120418
from vespa_analysts.project060_all_viewing_20120418 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120418

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120418
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120418 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120418(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120418(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120418(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120418-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120418
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120418
(  full_column_detail '\n')
FROM '/staging2/B20120418.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120418 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120418 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120418 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120418 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120418 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120418;
--drop table vespa_analysts.project060_spot_file_20120418;
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
into vespa_analysts.project060_spot_file_20120418
from vespa_analysts.project060_raw_spot_file_20120418
;

--select * from vespa_analysts.project060_spot_file_20120418 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120418 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120418 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120418
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120418 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120418 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120418 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120418_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120418_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120418_expanded
from vespa_analysts.project060_spot_file_20120418 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120418 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120418_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120418_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120418_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120418_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120418_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120418_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120418_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120418_expanded
;


alter table vespa_analysts.project060_spot_file_20120418_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120418_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120418_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120418_expanded
;

alter table  vespa_analysts.project060_spot_file_20120418_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120418_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120418_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120418_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120418_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120418_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120418_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120418;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120418
from vespa_analysts.project060_spot_file_20120418_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120418_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120418_expanded(corrected_spot_transmission_start_minute);


---Get Views by Spot
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120418;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120418
from vespa_analysts.project060_spot_file_20120418_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120418 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120418
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120418 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120418;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120418.csv' format ascii;
commit;
*/


---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-17';
SET @var_prog_period_end    = '2012-04-25';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120417 ( -- drop table vespa_analysts.project060_all_viewing_20120417
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
    insert into vespa_analysts.project060_all_viewing_20120417
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



and (   Adjusted_Event_Start_Time between ''2012-04-17 05:00:00'' and ''2012-04-18 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-17 05:00:00'' and ''2012-04-18 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-17 05:00:00'' and ''2012-04-18 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-17 05:00:00'' and ''2012-04-18 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120417;

commit;

alter table vespa_analysts.project060_all_viewing_20120417 add live tinyint;

update vespa_analysts.project060_all_viewing_20120417
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120417
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120417 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120417
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120417 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120417(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120417(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120417(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120417(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120417(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120417(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120417(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120417(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120417
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120417
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120417
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120417
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
        vespa_analysts.project060_all_viewing_20120417 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120417
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120417
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
update vespa_analysts.project060_all_viewing_20120417
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120417 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120417 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120417 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120417 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120417 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120417 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120417
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120417
;
commit;


---
update vespa_analysts.project060_all_viewing_20120417
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120417
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120417
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
from vespa_analysts.project060_all_viewing_20120417
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120417 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120417
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120417
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120417 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120417 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120417
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
from vespa_analysts.project060_all_viewing_20120417
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
--alter table vespa_analysts.project060_all_viewing_20120417 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120417 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120417 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120417  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120417  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record
--alter table vespa_analysts.project060_all_viewing_20120417 delete weighting;
alter table vespa_analysts.project060_all_viewing_20120417 add weighting double;

update vespa_analysts.project060_all_viewing_20120417 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120417  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120417 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120417 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120417 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120417  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120417 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120417 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120417 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120417 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120417
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120417
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120417
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120417 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120417 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120417 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120417 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120417 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120417 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120417;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120417;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120417;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120417;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120417;
drop table vespa_analysts.project060_allocated_minutes_total_20120417;
drop table vespa_analysts.project060_full_allocated_minutes_20120417;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120417
from vespa_analysts.project060_all_viewing_20120417 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120417
from vespa_analysts.project060_all_viewing_20120417 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120417
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120417
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120417
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120417
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120417
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120417
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120417
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120417(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120417(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120417
from vespa_analysts.project060_partial_minutes_for_allocation_20120417
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120417(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120417(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120417
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120417
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120417(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120417(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120417
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120417 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120417 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120417
from vespa_analysts.project060_all_viewing_20120417 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120417

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120417
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120417 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120417(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120417(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120417(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120417-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120417
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120417
(  full_column_detail '\n')
FROM '/staging2/B20120417.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120417 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120417 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120417 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120417 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120417 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120417;
--drop table vespa_analysts.project060_spot_file_20120417;
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
into vespa_analysts.project060_spot_file_20120417
from vespa_analysts.project060_raw_spot_file_20120417
;

--select * from vespa_analysts.project060_spot_file_20120417 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120417 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120417 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120417
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120417 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120417 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120417 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120417_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120417_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120417_expanded
from vespa_analysts.project060_spot_file_20120417 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120417 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120417_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120417_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120417_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120417_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120417_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120417_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120417_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120417_expanded
;


alter table vespa_analysts.project060_spot_file_20120417_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120417_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120417_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120417_expanded
;

alter table  vespa_analysts.project060_spot_file_20120417_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120417_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120417_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120417_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120417_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120417_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120417_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120417;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120417
from vespa_analysts.project060_spot_file_20120417_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120417_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120417_expanded(corrected_spot_transmission_start_minute);


---Get Views by Spot
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120417;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120417
from vespa_analysts.project060_spot_file_20120417_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120417 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120417
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120417 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120417;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120417.csv' format ascii;
commit;
*/


---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-16';
SET @var_prog_period_end    = '2012-04-24';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120416 ( -- drop table vespa_analysts.project060_all_viewing_20120416
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
    insert into vespa_analysts.project060_all_viewing_20120416
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



and (   Adjusted_Event_Start_Time between ''2012-04-15 05:00:00'' and ''2012-04-18 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-15 05:00:00'' and ''2012-04-18 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-15 05:00:00'' and ''2012-04-18 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-15 05:00:00'' and ''2012-04-18 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120416;

commit;

alter table vespa_analysts.project060_all_viewing_20120416 add live tinyint;

update vespa_analysts.project060_all_viewing_20120416
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120416
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120416 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120416
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120416 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120416(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120416(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120416(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120416(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120416(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120416(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120416(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120416(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120416
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120416
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120416
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120416
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
        vespa_analysts.project060_all_viewing_20120416 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120416
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120416
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
update vespa_analysts.project060_all_viewing_20120416
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120416 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120416 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120416 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120416 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120416 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120416 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120416
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120416
;
commit;


---
update vespa_analysts.project060_all_viewing_20120416
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120416
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120416
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
from vespa_analysts.project060_all_viewing_20120416
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120416 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120416
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120416
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120416 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120416 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120416
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
from vespa_analysts.project060_all_viewing_20120416
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
--alter table vespa_analysts.project060_all_viewing_20120416 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120416 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120416 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120416  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120416  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record
--alter table vespa_analysts.project060_all_viewing_20120416 delete weighting;
alter table vespa_analysts.project060_all_viewing_20120416 add weighting double;

update vespa_analysts.project060_all_viewing_20120416 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120416  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120416 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120416 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120416 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120416  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120416 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120416 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120416 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120416 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120416
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120416
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120416
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120416 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120416 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120416 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120416 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120416 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120416 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120416;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120416;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120416;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120416;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120416;
drop table vespa_analysts.project060_allocated_minutes_total_20120416;
drop table vespa_analysts.project060_full_allocated_minutes_20120416;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120416
from vespa_analysts.project060_all_viewing_20120416 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120416
from vespa_analysts.project060_all_viewing_20120416 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120416
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120416
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120416
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120416
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120416
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120416
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120416
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120416(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120416(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120416
from vespa_analysts.project060_partial_minutes_for_allocation_20120416
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120416(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120416(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120416
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120416
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120416(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120416(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120416
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120416 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120416 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120416
from vespa_analysts.project060_all_viewing_20120416 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120416

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120416
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120416 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120416(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120416(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120416(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120416-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120416
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120416
(  full_column_detail '\n')
FROM '/staging2/B20120416.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120416 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120416 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120416 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120416 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120416 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120416;
--drop table vespa_analysts.project060_spot_file_20120416;
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
into vespa_analysts.project060_spot_file_20120416
from vespa_analysts.project060_raw_spot_file_20120416
;

--select * from vespa_analysts.project060_spot_file_20120416 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120416 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120416 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120416
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120416 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120416 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120416 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120416_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120416_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120416_expanded
from vespa_analysts.project060_spot_file_20120416 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120416 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120416_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120416_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120416_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120416_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120416_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120416_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120416_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120416_expanded
;


alter table vespa_analysts.project060_spot_file_20120416_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120416_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120416_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120416_expanded
;

alter table  vespa_analysts.project060_spot_file_20120416_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120416_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120416_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120416_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120416_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120416_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120416_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120416;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120416
from vespa_analysts.project060_spot_file_20120416_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120416_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120416_expanded(corrected_spot_transmission_start_minute);


---Get Views by Spot
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120416;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120416
from vespa_analysts.project060_spot_file_20120416_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120416 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120416
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120416 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120416;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120416.csv' format ascii;
commit;
*/
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120416; commit;

---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-30';
SET @var_prog_period_end    = '2012-05-08';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120430 ( -- drop table vespa_analysts.project060_all_viewing_20120430
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
    insert into vespa_analysts.project060_all_viewing_20120430
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



and (   Adjusted_Event_Start_Time between ''2012-04-30 05:00:00'' and ''2012-05-01 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-30 05:00:00'' and ''2012-05-01 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-30 05:00:00'' and ''2012-05-01 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-30 05:00:00'' and ''2012-05-01 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120430;

commit;

alter table vespa_analysts.project060_all_viewing_20120430 add live tinyint;

update vespa_analysts.project060_all_viewing_20120430
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120430
;
commit;

/*
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
--select * from  vespa_analysts.channel_name_lookup_old;
alter table vespa_analysts.project060_all_viewing_20120430 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120430
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120430 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120430(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120430(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120430(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120430(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120430(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120430(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120430(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120430(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120430
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120430
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120430
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120430
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
        vespa_analysts.project060_all_viewing_20120430 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120430
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120430
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
update vespa_analysts.project060_all_viewing_20120430
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120430 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120430 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120430 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120430 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120430 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120430 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120430
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120430
;
commit;


---
update vespa_analysts.project060_all_viewing_20120430
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120430
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120430
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
from vespa_analysts.project060_all_viewing_20120430
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120430 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120430
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120430
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120430 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120430 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120430
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
from vespa_analysts.project060_all_viewing_20120430
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
--alter table vespa_analysts.project060_all_viewing_20120430 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120430 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120430 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120430  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120430  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record
--alter table vespa_analysts.project060_all_viewing_20120430 delete weighting;
alter table vespa_analysts.project060_all_viewing_20120430 add weighting double;

update vespa_analysts.project060_all_viewing_20120430 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120430  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120430 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120430 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120430 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120430  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--
--select top 500 * from vespa_analysts.project060_all_viewing_20120430;
--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120430 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120430 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120430 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----
--alter table  vespa_analysts.project060_all_viewing_20120430 delete live_timeshifted_type;
alter table  vespa_analysts.project060_all_viewing_20120430 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120430
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120430
; 
/*
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
*/

--select * from vespa_analysts.project060_service_key_triplet_lookup;

update vespa_analysts.project060_all_viewing_20120430
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120430 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120430 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120430 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120430 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120430 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120430 where capped_x_viewing_start_time_local is not null

--There are difefreing kinds of viewing activity

--1)    Activity which covers a full real time minute e.g., 18:05:27 to 18:07:16 The minute 18:06 will definitely be credited to the activity
--      taking place during that minute

--2)    Also include minutes Where part of a minute is more than 30 seconds 
--      i.e.,   start time < :30 and end time in a subsequent minute or
--              end_time >:30 and start time in previous minute

--3)    All Other viewing instances where only part of a minute is present


---Part 1 Start of minute where no whole real time minute is covered in the viewing event
--drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute;
/*
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120430;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120430;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120430;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120430;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120430;
drop table vespa_analysts.project060_allocated_minutes_total_20120430;
drop table vespa_analysts.project060_full_allocated_minutes_20120430;
commit;
*/


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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120430
from vespa_analysts.project060_all_viewing_20120430 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  

and (datepart(second, capped_x_viewing_start_time_local)<>0
        or (datepart(second, capped_x_viewing_start_time_local)=0
                and 
                (       dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
                    >=
                        dateadd(second, - (datepart(second, capped_x_viewing_end_time_local)), capped_x_viewing_end_time_local)
                )
            )
      )
order by subscriber_id , capped_x_viewing_start_time_local
;

--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_start_minute;

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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120430
from vespa_analysts.project060_all_viewing_20120430 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute;
---Split thes two tables each into two parts (1st and 2nd broadcast minute viewed during the clock minute) and append these 4 tables together to use to allocate minutes
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120430
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120430
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120430
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120430
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120430
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120430
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120430
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120430(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120430(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120430
from vespa_analysts.project060_partial_minutes_for_allocation_20120430
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120430(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120430(real_time_minute);

--select * from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute where subscriber_id =161640


----
select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,weighting_value
,total_seconds_viewed_of_broadcast_minute
,rank() over (partition by subscriber_id,real_time_minute order by total_seconds_viewed_of_broadcast_minute desc ,time_broadcast_minute_event_started, broadcast_minute) as most_watched_record
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120430
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120430
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120430(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120430(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120430
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120430 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120430 as b
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
,case when datepart(second, capped_x_viewing_start_time_local) = 0 then capped_x_viewing_start_time_local else  dateadd(second, 60 - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local) end  as real_time_minute_start
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
into vespa_analysts.project060_full_allocated_minutes_20120430
from vespa_analysts.project060_all_viewing_20120430 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
--and  datepart(second, capped_x_viewing_start_time_local)<>0
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120430

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120430
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_full_allocated_minutes_20120430 where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120430(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120430(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120430(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120430-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120430
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120430
(  full_column_detail '\n')
FROM '/staging2/B20120430.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120430 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120430 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120430 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120430 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120430 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120430;
--drop table vespa_analysts.project060_spot_file_20120430;
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
into vespa_analysts.project060_spot_file_20120430
from vespa_analysts.project060_raw_spot_file_20120430
;

--select * from vespa_analysts.project060_spot_file_20120430 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120430 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
---Load in Sales House Names and Channel Mapping---
--drop table vespa_analysts.barb_sales_house_lookup;
/*
create table vespa_analysts.barb_sales_house_lookup
(sales_house_identifier varchar(5)
,sales_house varchar (64)
);

input into vespa_analysts.barb_sales_house_lookup
from 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\BARB sales house codes.csv' format ascii;
*/
commit;

alter table vespa_analysts.project060_spot_file_20120430 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120430
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120430 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120430 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120430 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120430_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120430_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120430_expanded
from vespa_analysts.project060_spot_file_20120430 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120430 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120430_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120430_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120430_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120430_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120430_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120430_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120430_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120430_expanded
;


alter table vespa_analysts.project060_spot_file_20120430_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120430_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120430_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120430_expanded
;

alter table  vespa_analysts.project060_spot_file_20120430_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120430_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120430_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120430_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120430_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120430_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120430_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120430;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120430
from vespa_analysts.project060_spot_file_20120430_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120430_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120430_expanded(corrected_spot_transmission_start_minute);


---Get Views by Spot
--drop table vespa_analysts.project060_spot_summary_viewing_figures_20120430;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120430
from vespa_analysts.project060_spot_file_20120430_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120430 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120430
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120430 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120430;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120430.csv' format ascii;
commit;
*/

 


