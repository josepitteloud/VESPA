

---Start for 20120401


---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-01';
SET @var_prog_period_end    = '2012-04-09';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120401 ( -- drop table vespa_analysts.project060_all_viewing_20120401
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
    insert into vespa_analysts.project060_all_viewing_20120401
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



and (   Adjusted_Event_Start_Time between ''2012-04-01 05:00:00'' and ''2012-04-02 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-01 05:00:00'' and ''2012-04-02 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-01 05:00:00'' and ''2012-04-02 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-01 05:00:00'' and ''2012-04-02 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120401;

commit;

alter table vespa_analysts.project060_all_viewing_20120401 add live tinyint;

update vespa_analysts.project060_all_viewing_20120401
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120401
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
alter table vespa_analysts.project060_all_viewing_20120401 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120401
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120401 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120401(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120401(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120401(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120401(live)
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120401(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120401(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120401(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120401(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120401
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120401
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120401
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120401
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
        vespa_analysts.project060_all_viewing_20120401 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120401
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120401
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
update vespa_analysts.project060_all_viewing_20120401
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120401 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120401 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120401 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120401 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120401 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120401 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120401
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120401
;
commit;


---
update vespa_analysts.project060_all_viewing_20120401
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120401
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120401
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
from vespa_analysts.project060_all_viewing_20120401
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120401 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120401
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120401
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120401 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120401 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120401
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
from vespa_analysts.project060_all_viewing_20120401
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
--alter table vespa_analysts.project060_all_viewing_20120401 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120401 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120401 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120401  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120401  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120401 add weighting double;

update vespa_analysts.project060_all_viewing_20120401 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120401  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table vespa_analysts.project060_all_viewing_20120401 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120401 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120401 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120401  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120401 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120401 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120401 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120401 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120401
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120401
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

update vespa_analysts.project060_all_viewing_20120401
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120401 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120401 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120401 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120401 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120401 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120401 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120401;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120401;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120401;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120401;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120401;
drop table vespa_analysts.project060_allocated_minutes_total_20120401;
drop table vespa_analysts.project060_full_allocated_minutes_20120401;
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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120401
from vespa_analysts.project060_all_viewing_20120401 
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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120401
from vespa_analysts.project060_all_viewing_20120401 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120401
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120401
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120401
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120401
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120401
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120401
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120401
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120401(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120401(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120401
from vespa_analysts.project060_partial_minutes_for_allocation_20120401
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120401(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120401(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120401
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120401
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120401(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120401(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120401
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120401 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120401 as b
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
into vespa_analysts.project060_full_allocated_minutes_20120401
from vespa_analysts.project060_all_viewing_20120401 
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

insert into vespa_analysts.project060_allocated_minutes_total_20120401

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120401
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120401(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120401(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120401(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120401-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120401
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120401
(  full_column_detail '\n')
FROM '/staging2/B20120401.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120401 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120401 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120401 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120401 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120401 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120401;
--drop table vespa_analysts.project060_spot_file_20120401;
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
into vespa_analysts.project060_spot_file_20120401
from vespa_analysts.project060_raw_spot_file_20120401
;

--select * from vespa_analysts.project060_spot_file_20120401 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120401 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120401 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120401
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120401 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120401 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120401 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120401_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120401_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120401_expanded
from vespa_analysts.project060_spot_file_20120401 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120401 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120401_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120401_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120401_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120401_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120401_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120401_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120401_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120401_expanded
;


alter table vespa_analysts.project060_spot_file_20120401_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120401_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120401_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120401_expanded
;

alter table  vespa_analysts.project060_spot_file_20120401_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120401_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120401_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120401_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120401_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120401_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120401_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120401;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120401
from vespa_analysts.project060_spot_file_20120401_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120401_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120401_expanded(corrected_spot_transmission_start_minute);


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
into vespa_analysts.project060_spot_summary_viewing_figures_20120401
from vespa_analysts.project060_spot_file_20120401_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120401 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120401
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120401 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120401;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120401.csv' format ascii;
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
SET @var_prog_period_start  = '2012-04-02';
SET @var_prog_period_end    = '2012-04-10';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120425;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120402 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120402
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



and (   Adjusted_Event_Start_Time between ''2012-04-02 05:00:00'' and ''2012-04-03 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-02 05:00:00'' and ''2012-04-03 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-02 05:00:00'' and ''2012-04-03 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-02 05:00:00'' and ''2012-04-03 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120402;

commit;

alter table vespa_analysts.project060_all_viewing_20120402 add live tinyint;

update vespa_analysts.project060_all_viewing_20120402
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120402
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
alter table vespa_analysts.project060_all_viewing_20120402 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120402
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120402 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120402(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120402(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120402(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120402(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120402(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120402(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120402(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120402(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120402
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120402
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120402
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120402
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
        vespa_analysts.project060_all_viewing_20120402 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120402
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120402
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
update vespa_analysts.project060_all_viewing_20120402
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120402 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120402 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120402 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120402 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120402 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120402 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120402
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120402
;
commit;


---
update vespa_analysts.project060_all_viewing_20120402
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120402
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120402
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
from vespa_analysts.project060_all_viewing_20120402
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120402 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120402
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120402
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120402 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120402 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120402
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
from vespa_analysts.project060_all_viewing_20120402
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
--alter table vespa_analysts.project060_all_viewing_20120402 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120402 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120402 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120402  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120402  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120402 add weighting double;

update vespa_analysts.project060_all_viewing_20120402 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120402  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120402 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120402 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120402 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120402  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120402 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120402 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120402 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120402 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120402
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120402
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

update vespa_analysts.project060_all_viewing_20120402
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120402 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120402 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120402 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120402 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120402 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120402 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120402;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120402;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120402;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120402;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120402;
drop table vespa_analysts.project060_allocated_minutes_total_20120402;
drop table vespa_analysts.project060_full_allocated_minutes_20120402;
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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120402
from vespa_analysts.project060_all_viewing_20120402 
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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120402
from vespa_analysts.project060_all_viewing_20120402 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120402
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120402
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120402
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120402
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120402
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120402
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120402
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120402(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120402(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120402
from vespa_analysts.project060_partial_minutes_for_allocation_20120402
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120402(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120402(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120402
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120402
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120402(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120402(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120402
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120402 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120402 as b
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
into vespa_analysts.project060_full_allocated_minutes_20120402
from vespa_analysts.project060_all_viewing_20120402 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120402;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120402

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120402
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120402(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120402(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120402(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120402-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120402
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120402
(  full_column_detail '\n')
FROM '/staging2/B20120402.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120402 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120402 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120402 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120402 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120402 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120402;
--drop table vespa_analysts.project060_spot_file_20120402;
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
into vespa_analysts.project060_spot_file_20120402
from vespa_analysts.project060_raw_spot_file_20120402
;

--select * from vespa_analysts.project060_spot_file_20120402 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120402 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120402 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120402
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120402 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120402 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120402 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120402_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120402_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120402_expanded
from vespa_analysts.project060_spot_file_20120402 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120402 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120402_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120402_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120402_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120402_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120402_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120402_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120402_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120402_expanded
;


alter table vespa_analysts.project060_spot_file_20120402_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120402_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120402_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120402_expanded
;

alter table  vespa_analysts.project060_spot_file_20120402_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120402_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120402_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120402_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120402_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120402_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120402_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120402;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120402
from vespa_analysts.project060_spot_file_20120402_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120402_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120402_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120402;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120402
from vespa_analysts.project060_spot_file_20120402_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120402 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120402
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120402 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120402;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120402.csv' format ascii;
commit;
--drop table vespa_analysts.project060_all_viewing_20120401 ;
--drop table vespa_analysts.project060_all_viewing_20120402 ; commit;


---20120403----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-03';
SET @var_prog_period_end    = '2012-04-11';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120403 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120403
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



and (   Adjusted_Event_Start_Time between ''2012-04-03 05:00:00'' and ''2012-04-04 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-03 05:00:00'' and ''2012-04-04 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-03 05:00:00'' and ''2012-04-04 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-03 05:00:00'' and ''2012-04-04 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120403;

commit;

alter table vespa_analysts.project060_all_viewing_20120403 add live tinyint;

update vespa_analysts.project060_all_viewing_20120403
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120403
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
alter table vespa_analysts.project060_all_viewing_20120403 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120403
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120403 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120403(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120403(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120403(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120403(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120403(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120403(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120403(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120403(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120403
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120403
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120403
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120403
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
        vespa_analysts.project060_all_viewing_20120403 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120403
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120403
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
update vespa_analysts.project060_all_viewing_20120403
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120403 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120403 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120403 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120403 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120403 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120403 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120403
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120403
;
commit;


---
update vespa_analysts.project060_all_viewing_20120403
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120403
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120403
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
from vespa_analysts.project060_all_viewing_20120403
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120403 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120403
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120403
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120403 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120403 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120403
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
from vespa_analysts.project060_all_viewing_20120403
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
--alter table vespa_analysts.project060_all_viewing_20120403 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120403 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120403 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120403  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120403  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120403 add weighting double;

update vespa_analysts.project060_all_viewing_20120403 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120403  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120403 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120403 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120403 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120403  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120403 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120403 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120403 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120403 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120403
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120403
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

update vespa_analysts.project060_all_viewing_20120403
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120403 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120403 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120403 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120403 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120403 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120403 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120403;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120403;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120403;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120403;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120403;
drop table vespa_analysts.project060_allocated_minutes_total_20120403;
drop table vespa_analysts.project060_full_allocated_minutes_20120403;
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
into vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120403
from vespa_analysts.project060_all_viewing_20120403 
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
into vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120403
from vespa_analysts.project060_all_viewing_20120403 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120403
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120403
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120403
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120403
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120403
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120403
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
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120403
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120403(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120403(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120403
from vespa_analysts.project060_partial_minutes_for_allocation_20120403
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120403(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120403(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120403
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120403
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120403(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120403(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120403
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120403 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120403 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120404;

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
into vespa_analysts.project060_full_allocated_minutes_20120403
from vespa_analysts.project060_all_viewing_20120403 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120404;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120403

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120403
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120403(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120403(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120403(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120403-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120403
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120403
(  full_column_detail '\n')
FROM '/staging2/B20120403.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120403 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120403 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120403 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120403 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120403 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120403;
--drop table vespa_analysts.project060_spot_file_20120403;
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
into vespa_analysts.project060_spot_file_20120403
from vespa_analysts.project060_raw_spot_file_20120403
;

--select * from vespa_analysts.project060_spot_file_20120403 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120403 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120403 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120403
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120403 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120403 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120403 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120403_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120403_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120403_expanded
from vespa_analysts.project060_spot_file_20120403 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120403 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120403_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120403_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120403_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120403_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120403_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120403_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120403_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120403_expanded
;


alter table vespa_analysts.project060_spot_file_20120403_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120403_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120403_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120403_expanded
;

alter table  vespa_analysts.project060_spot_file_20120403_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120403_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120403_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120403_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120403_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120403_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120403_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120403;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120403
from vespa_analysts.project060_spot_file_20120403_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120403_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120403_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120403;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120403
from vespa_analysts.project060_spot_file_20120403_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120403 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120403
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120403 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120403;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120403.csv' format ascii;
commit;
--drop table vespa_analysts.project060_all_viewing_20120401 ;
--drop table vespa_analysts.project060_all_viewing_20120403 ; commit;



---20120404----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-04';
SET @var_prog_period_end    = '2012-04-12';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120404 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120404
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



and (   Adjusted_Event_Start_Time between ''2012-04-04 05:00:00'' and ''2012-04-05 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-04 05:00:00'' and ''2012-04-05 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-04 05:00:00'' and ''2012-04-05 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-04 05:00:00'' and ''2012-04-05 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;

commit;

alter table vespa_analysts.project060_all_viewing_20120404 add live tinyint;

update vespa_analysts.project060_all_viewing_20120404
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120404
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
alter table vespa_analysts.project060_all_viewing_20120404 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120404
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120404 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120404(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120404(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120404(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120404(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120404(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120404(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120404(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120404(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120404
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120404
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120404
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120404
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
        vespa_analysts.project060_all_viewing_20120404 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120404
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120404
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
update vespa_analysts.project060_all_viewing_20120404
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120404 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120404 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120404 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120404 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120404 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120404 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120404
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120404
;
commit;


---
update vespa_analysts.project060_all_viewing_20120404
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120404
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120404
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
from vespa_analysts.project060_all_viewing_20120404
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120404 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120404
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120404
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120404 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120404 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120404
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
from vespa_analysts.project060_all_viewing_20120404
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
--alter table vespa_analysts.project060_all_viewing_20120404 delete scaling_segment_id
alter table vespa_analysts.project060_all_viewing_20120404 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120404 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120404  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120404  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120404 add weighting double;

update vespa_analysts.project060_all_viewing_20120404 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120404  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120404 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120404 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120404 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120404  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120404 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120404 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120404 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120404 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120404
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120404
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

update vespa_analysts.project060_all_viewing_20120404
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120404 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120404;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120404 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120404 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120404 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120404 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120404 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120404;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120404;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120404;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120404;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120404;
drop table vespa_analysts.project060_allocated_minutes_total_20120404;
drop table vespa_analysts.project060_full_allocated_minutes_20120404;
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
into from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120404
from vespa_analysts.project060_all_viewing_20120404 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120404
from vespa_analysts.project060_all_viewing_20120404 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120404
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120404
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120404
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120404
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120404
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120404
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120404
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120404
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120404(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120404(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120404
from vespa_analysts.project060_partial_minutes_for_allocation_20120404
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120404(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120404(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120404
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120404
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120404(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120404(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120404
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120404 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120404 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120404;

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
into vespa_analysts.project060_full_allocated_minutes_20120404
from vespa_analysts.project060_all_viewing_20120404 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120405;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120404

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120404
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120404(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120404(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120404(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120404-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120404
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120404
(  full_column_detail '\n')
FROM '/staging2/B20120404.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120404 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120404 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120404 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120404 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120404 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120404;
--drop table vespa_analysts.project060_spot_file_20120404;
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
into vespa_analysts.project060_spot_file_20120404
from vespa_analysts.project060_raw_spot_file_20120404
;

--select * from vespa_analysts.project060_spot_file_20120404 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120404 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120404 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120404
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120404 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120404 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120404 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---

IF object_id('vespa_analysts.project060_spot_file_20120404_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120404_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120404_expanded
from vespa_analysts.project060_spot_file_20120404 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120404 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120404_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120404_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120404_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120404_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120404_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120404_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120404_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120404_expanded
;


alter table vespa_analysts.project060_spot_file_20120404_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120404_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120404_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120404_expanded
;

alter table  vespa_analysts.project060_spot_file_20120404_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120404_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120404_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120404_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120404_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120404_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120404_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120404;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120404
from vespa_analysts.project060_spot_file_20120404_expanded
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
create hg index idx1 on vespa_analysts.project060_spot_file_20120404_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120404_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120404;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120404
from vespa_analysts.project060_spot_file_20120404_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120404 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120404
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120404 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120404;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120404.csv' format ascii;
commit;
--drop table vespa_analysts.project060_all_viewing_20120401 ;
--drop table vespa_analysts.project060_all_viewing_20120404 ; commit;



---20120405----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-05';
SET @var_prog_period_end    = '2012-04-13';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120405 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120405
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



and (   Adjusted_Event_Start_Time between ''2012-04-05 05:00:00'' and ''2012-04-06 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-05 05:00:00'' and ''2012-04-06 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-05 05:00:00'' and ''2012-04-06 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-05 05:00:00'' and ''2012-04-06 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120405;

commit;

alter table vespa_analysts.project060_all_viewing_20120405 add live tinyint;

update vespa_analysts.project060_all_viewing_20120405
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120405
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
alter table vespa_analysts.project060_all_viewing_20120405 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120405
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120405 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120405(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120405(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120405(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120405(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120405(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120405(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120405(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120405(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120405
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120405
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120405
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120405
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
        vespa_analysts.project060_all_viewing_20120405 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120405
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120405
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
update vespa_analysts.project060_all_viewing_20120405
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120405 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120405 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120405 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120405 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120405 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120405 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120405
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120405
;
commit;


---
update vespa_analysts.project060_all_viewing_20120405
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120405
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120405
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
from vespa_analysts.project060_all_viewing_20120405
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120405 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120405
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120405
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120405 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120405 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120405
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
from vespa_analysts.project060_all_viewing_20120405
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
--alter table vespa_analysts.project060_all_viewing_20120405 delete scaling_segment_id
alter table select top 100 * from vespa_analysts.project060_all_viewing_20120405 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120405 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120405  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120405  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120405 add weighting double;

update vespa_analysts.project060_all_viewing_20120405 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120405  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120405 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120405 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120405 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120405  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120405 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120405 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120405 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120405 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120405
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120405
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

update vespa_analysts.project060_all_viewing_20120405
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120405 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120405;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120405 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120405 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120405 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120405 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120405 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120405;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120405;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120405;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120405;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120405;
drop table vespa_analysts.project060_allocated_minutes_total_20120405;
drop table vespa_analysts.project060_full_allocated_minutes_20120405;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120405
from vespa_analysts.project060_all_viewing_20120405 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120405
from vespa_analysts.project060_all_viewing_20120405 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120405
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120405
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120405
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120405
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120405
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120405
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120405
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120405
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120405(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120405(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120405
from vespa_analysts.project060_partial_minutes_for_allocation_20120405
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120405(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120405(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120405
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120405
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120405(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120405(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120405
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120405 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120405 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120405;

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
into vespa_analysts.project060_full_allocated_minutes_20120405
from vespa_analysts.project060_all_viewing_20120405 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120406;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120405

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120405
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120405(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120405(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120405(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120405-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120405
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120405
(  full_column_detail '\n')
FROM '/staging2/B20120405.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120405 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120405 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120405 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120405 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120405 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120405;
--drop table vespa_analysts.project060_spot_file_20120405;
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
into vespa_analysts.project060_spot_file_20120405
from vespa_analysts.project060_raw_spot_file_20120405
;

--select * from vespa_analysts.project060_spot_file_20120405 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120405 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120405 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120405
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120405 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120405 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120405 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120405_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120405_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120405_expanded
from vespa_analysts.project060_spot_file_20120405 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120405 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120405_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120405_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120405_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120405_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120405_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120405_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120405_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120405_expanded
;


alter table vespa_analysts.project060_spot_file_20120405_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120405_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120405_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120405_expanded
;

alter table  vespa_analysts.project060_spot_file_20120405_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120405_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120405_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120405_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120405_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120405_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120405_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120405;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120405
from vespa_analysts.project060_spot_file_20120405_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120405_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120405_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120405_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120405;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120405;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120405
from vespa_analysts.project060_spot_file_20120405_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120405 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120405
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120405 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120405;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120405.csv' format ascii;
commit;
--drop table vespa_analysts.project060_all_viewing_20120401 ;
--drop table vespa_analysts.project060_all_viewing_20120405 ; commit;





---20120406----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-06';
SET @var_prog_period_end    = '2012-04-14';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120406 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120406
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



and (   Adjusted_Event_Start_Time between ''2012-04-06 05:00:00'' and ''2012-04-07 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-06 05:00:00'' and ''2012-04-07 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-06 05:00:00'' and ''2012-04-07 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-06 05:00:00'' and ''2012-04-07 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120406;

commit;

alter table vespa_analysts.project060_all_viewing_20120406 add live tinyint;

update vespa_analysts.project060_all_viewing_20120406
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120406
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
alter table vespa_analysts.project060_all_viewing_20120406 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120406
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120406 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120406(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120406(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120406(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120406(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120406(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120406(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120406(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120406(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120406
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120406
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120406
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120406
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
        vespa_analysts.project060_all_viewing_20120406 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120406
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120406
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
update vespa_analysts.project060_all_viewing_20120406
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120406 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120406 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120406 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120406 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120406 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120406 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120406
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120406
;
commit;


---
update vespa_analysts.project060_all_viewing_20120406
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120406
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120406
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
from vespa_analysts.project060_all_viewing_20120406
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120406 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120406
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120406
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120406 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120406 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120406
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
from vespa_analysts.project060_all_viewing_20120406
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
--alter table vespa_analysts.project060_all_viewing_20120406 delete scaling_segment_id
alter table  vespa_analysts.project060_all_viewing_20120406 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120406 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120406  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120406  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120406 add weighting double;

update vespa_analysts.project060_all_viewing_20120406 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120406  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120406 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120406 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120406 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120406  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120406 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120406 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120406 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120406 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120406
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120406
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

update vespa_analysts.project060_all_viewing_20120406
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120406 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120406;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120406 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120406 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120406 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120406 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120406 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120406;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120406;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120406;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120406;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120406;
drop table vespa_analysts.project060_allocated_minutes_total_20120406;
drop table vespa_analysts.project060_full_allocated_minutes_20120406;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120406
from vespa_analysts.project060_all_viewing_20120406 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120406
from vespa_analysts.project060_all_viewing_20120406 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120406
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120406
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120406
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120406
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120406
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120406
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120406
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120406
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120406(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120406(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120406
from vespa_analysts.project060_partial_minutes_for_allocation_20120406
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120406(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120406(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120406
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120406
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120406(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120406(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120406
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120406 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120406 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120406;

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
into vespa_analysts.project060_full_allocated_minutes_20120406
from vespa_analysts.project060_all_viewing_20120406 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120407;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120406

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120406
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120406(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120406(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120406(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120406-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120406
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120406
(  full_column_detail '\n')
FROM '/staging2/B20120406.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120406 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120406 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120406 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120406 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120406 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120406;
--drop table vespa_analysts.project060_spot_file_20120406;
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
into vespa_analysts.project060_spot_file_20120406
from vespa_analysts.project060_raw_spot_file_20120406
;

--select * from vespa_analysts.project060_spot_file_20120406 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120406 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120406 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120406
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120406 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120406 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120406 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120406_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120406_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120406_expanded
from vespa_analysts.project060_spot_file_20120406 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120406 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120406_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120406_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120406_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120406_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120406_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120406_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120406_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120406_expanded
;


alter table vespa_analysts.project060_spot_file_20120406_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120406_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120406_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120406_expanded
;

alter table  vespa_analysts.project060_spot_file_20120406_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120406_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120406_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120406_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120406_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120406_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120406_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120406;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120406
from vespa_analysts.project060_spot_file_20120406_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120406_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120406_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120406_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120406;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120406;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120406
from vespa_analysts.project060_spot_file_20120406_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120406 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120406
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120406 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120406;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120406.csv' format ascii;
commit;
/*
--select Event_Type , count(*) as records , count(distinct subscriber_id) as boxes from sk_prod.VESPA_STB_PROG_EVENTS_20120413 group by Event_Type ;

Event_Type,records,boxes
'evChangeView',24461237,334968
'evEmptyLog',18761,18726
'evPowerUp',2,2
'evStandbyIn',528315,271826
'evStandbyOut',179731,94080
'evSurf',756885,77366


Event_Type,records,boxes
'evChangeView',16846429,223136
'evEmptyLog',17617,17577
'evStandbyIn',369027,179820
'evStandbyOut',105369,57541
'evSurf',566305,48223
*/




---20120407----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-07';
SET @var_prog_period_end    = '2012-04-15';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120407 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120407
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



and (   Adjusted_Event_Start_Time between ''2012-04-07 05:00:00'' and ''2012-04-08 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-07 05:00:00'' and ''2012-04-08 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-07 05:00:00'' and ''2012-04-08 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-07 05:00:00'' and ''2012-04-08 04:59:59''  
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
--select top 100 * from vespa_analysts.project060_all_viewing_20120407;

commit;

alter table vespa_analysts.project060_all_viewing_20120407 add live tinyint;

update vespa_analysts.project060_all_viewing_20120407
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120407
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
alter table vespa_analysts.project060_all_viewing_20120407 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120407
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120407 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120407(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120407(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120407(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120407(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120407(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120407(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120407(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120407(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120407
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120407
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120407
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120407
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
        vespa_analysts.project060_all_viewing_20120407 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120407
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120407
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
update vespa_analysts.project060_all_viewing_20120407
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120407 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120407 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120407 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120407 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120407 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120407 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120407
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120407
;
commit;


---
update vespa_analysts.project060_all_viewing_20120407
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120407
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120407
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
from vespa_analysts.project060_all_viewing_20120407
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120407 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120407
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120407
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120407 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120407 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120407
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
from vespa_analysts.project060_all_viewing_20120407
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
--alter table vespa_analysts.project060_all_viewing_20120407 delete scaling_segment_id
alter table  vespa_analysts.project060_all_viewing_20120407 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120407 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120407  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120407  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120407 add weighting double;

update vespa_analysts.project060_all_viewing_20120407 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120407  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120407 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120407 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120407 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120407  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120407 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120407 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120407 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120407 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120407
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120407
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

update vespa_analysts.project060_all_viewing_20120407
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120407 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120407;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120407 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120407 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120407 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120407 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120407 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120407;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120407;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120407;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120407;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120407;
drop table vespa_analysts.project060_allocated_minutes_total_20120407;
drop table vespa_analysts.project060_full_allocated_minutes_20120407;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120407
from vespa_analysts.project060_all_viewing_20120407 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120407
from vespa_analysts.project060_all_viewing_20120407 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120407
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120407
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120407
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120407
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120407
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120407
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120407
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120407
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120407(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120407(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120407
from vespa_analysts.project060_partial_minutes_for_allocation_20120407
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120407(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120407(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120407
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120407
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120407(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120407(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120407
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120407 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120407 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120407;

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
into vespa_analysts.project060_full_allocated_minutes_20120407
from vespa_analysts.project060_all_viewing_20120407 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120408;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120407

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120407
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120407(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120407(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120407(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120407-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120407
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120407
(  full_column_detail '\n')
FROM '/staging2/B20120407.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120407 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120407 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120407 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120407 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120407 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120407;
--drop table vespa_analysts.project060_spot_file_20120407;
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
into vespa_analysts.project060_spot_file_20120407
from vespa_analysts.project060_raw_spot_file_20120407
;

--select * from vespa_analysts.project060_spot_file_20120407 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120407 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120407 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120407
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120407 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120407 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120407 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120407_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120407_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120407_expanded
from vespa_analysts.project060_spot_file_20120407 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120407 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120407_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120407_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120407_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120407_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120407_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120407_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120407_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120407_expanded
;


alter table vespa_analysts.project060_spot_file_20120407_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120407_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120407_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120407_expanded
;

alter table  vespa_analysts.project060_spot_file_20120407_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120407_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120407_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120407_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120407_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120407_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120407_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120407;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120407
from vespa_analysts.project060_spot_file_20120407_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120407_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120407_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120407_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120407;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120407;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120407
from vespa_analysts.project060_spot_file_20120407_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120407 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120407
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120407 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120407;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120407.csv' format ascii;
commit;



---20120408----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-08';
SET @var_prog_period_end    = '2012-04-16';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120408 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120408
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



and (   Adjusted_Event_Start_Time between ''2012-04-08 05:00:00'' and ''2012-04-09 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-08 05:00:00'' and ''2012-04-09 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-08 05:00:00'' and ''2012-04-09 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-08 05:00:00'' and ''2012-04-09 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120408;

commit;

alter table vespa_analysts.project060_all_viewing_20120408 add live tinyint;

update vespa_analysts.project060_all_viewing_20120408
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120408
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
alter table vespa_analysts.project060_all_viewing_20120408 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120408
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120408 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120408(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120408(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120408(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120408(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120408(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120408(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120408(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120408(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120408
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120408
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120408
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120408
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
        vespa_analysts.project060_all_viewing_20120408 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120408
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120408
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
update vespa_analysts.project060_all_viewing_20120408
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120408 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120408 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120408 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120408 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120408 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120408 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120408
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120408
;
commit;


---
update vespa_analysts.project060_all_viewing_20120408
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120408
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120408
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
from vespa_analysts.project060_all_viewing_20120408
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120408 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120408
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120408
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120408 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120408 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120408
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
from vespa_analysts.project060_all_viewing_20120408
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120408;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120408 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120408 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120408 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120408  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120408  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120408 add weighting double;

update vespa_analysts.project060_all_viewing_20120408 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120408  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120408 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120408 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120408 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120408  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120408 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120408 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120408 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120408 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120408
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120408
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

update vespa_analysts.project060_all_viewing_20120408
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120408 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120408;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120408 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120408 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120408 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120408 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120408 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120408;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120408;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120408;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120408;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120408;
drop table vespa_analysts.project060_allocated_minutes_total_20120408;
drop table vespa_analysts.project060_full_allocated_minutes_20120408;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120408
from vespa_analysts.project060_all_viewing_20120408 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120408
from vespa_analysts.project060_all_viewing_20120408 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120408
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120408
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120408
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120408
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120408
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120408
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120408
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120408
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120408(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120408(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120408
from vespa_analysts.project060_partial_minutes_for_allocation_20120408
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120408(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120408(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120408
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120408
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120408(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120408(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120408
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120408 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120408 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120408;

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
into vespa_analysts.project060_full_allocated_minutes_20120408
from vespa_analysts.project060_all_viewing_20120408 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120409;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120408

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120408
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120408(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120408(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120408(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120408-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120408
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120408
(  full_column_detail '\n')
FROM '/staging2/B20120408.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120408 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120408 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120408 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120408 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120408 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120408;
--drop table vespa_analysts.project060_spot_file_20120408;
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
into vespa_analysts.project060_spot_file_20120408
from vespa_analysts.project060_raw_spot_file_20120408
;

--select * from vespa_analysts.project060_spot_file_20120408 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120408 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120408 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120408
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120408 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120408 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120408 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120408_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120408_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120408_expanded
from vespa_analysts.project060_spot_file_20120408 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120408 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120408_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120408_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120408_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120408_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120408_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120408_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120408_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120408_expanded
;


alter table vespa_analysts.project060_spot_file_20120408_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120408_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120408_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120408_expanded
;

alter table  vespa_analysts.project060_spot_file_20120408_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120408_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120408_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120408_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120408_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120408_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120408_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120408;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120408
from vespa_analysts.project060_spot_file_20120408_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120408_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120408_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120408_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120408;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120408;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120408
from vespa_analysts.project060_spot_file_20120408_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120408 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120408
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120408 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120408;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120408.csv' format ascii;
commit;

--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120408



---20120409----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-09';
SET @var_prog_period_end    = '2012-04-17';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120409 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120409
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



and (   Adjusted_Event_Start_Time between ''2012-04-09 05:00:00'' and ''2012-04-10 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-09 05:00:00'' and ''2012-04-10 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-09 05:00:00'' and ''2012-04-10 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-09 05:00:00'' and ''2012-04-10 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120409;

commit;

alter table vespa_analysts.project060_all_viewing_20120409 add live tinyint;

update vespa_analysts.project060_all_viewing_20120409
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120409
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
alter table vespa_analysts.project060_all_viewing_20120409 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120409
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120409 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120409(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120409(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120409(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120409(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120409(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120409(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120409(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120409(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120409
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120409
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120409
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120409
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
        vespa_analysts.project060_all_viewing_20120409 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120409
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120409
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
update vespa_analysts.project060_all_viewing_20120409
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120409 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120409 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120409 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120409 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120409 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120409 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120409
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120409
;
commit;


---
update vespa_analysts.project060_all_viewing_20120409
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120409
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120409
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
from vespa_analysts.project060_all_viewing_20120409
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120409 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120409
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120409
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120409 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120409 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120409
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
from vespa_analysts.project060_all_viewing_20120409
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120409;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120409 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120409 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120409 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120409  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120409  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120409 add weighting double;

update vespa_analysts.project060_all_viewing_20120409 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120409  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120409 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120409 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120409 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120409  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120409 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120409 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120409 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120409 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120409
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120409
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

update vespa_analysts.project060_all_viewing_20120409
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120409 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120409;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120409 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120409 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120409 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120409 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120409 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120409;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120409;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120409;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120409;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120409;
drop table vespa_analysts.project060_allocated_minutes_total_20120409;
drop table vespa_analysts.project060_full_allocated_minutes_20120409;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120409
from vespa_analysts.project060_all_viewing_20120409 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120409
from vespa_analysts.project060_all_viewing_20120409 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120409
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120409
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120409
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120409
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120409
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120409
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120409
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120409
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120409(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120409(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120409
from vespa_analysts.project060_partial_minutes_for_allocation_20120409
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120409(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120409(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120409
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120409
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120409(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120409(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120409
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120409 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120409 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120409;

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
into vespa_analysts.project060_full_allocated_minutes_20120409
from vespa_analysts.project060_all_viewing_20120409 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120410;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120409

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120409
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120409(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120409(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120409(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120409-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120409
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120409
(  full_column_detail '\n')
FROM '/staging2/B20120409.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120409 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120409 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120409 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120409 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120409 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120409;
--drop table vespa_analysts.project060_spot_file_20120409;
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
into vespa_analysts.project060_spot_file_20120409
from vespa_analysts.project060_raw_spot_file_20120409
;

--select * from vespa_analysts.project060_spot_file_20120409 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120409 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120409 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120409
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120409 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120409 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120409 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120409_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120409_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120409_expanded
from vespa_analysts.project060_spot_file_20120409 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120409 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120409_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120409_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120409_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120409_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120409_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120409_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120409_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120409_expanded
;


alter table vespa_analysts.project060_spot_file_20120409_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120409_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120409_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120409_expanded
;

alter table  vespa_analysts.project060_spot_file_20120409_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120409_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120409_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120409_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120409_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120409_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120409_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120409;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120409
from vespa_analysts.project060_spot_file_20120409_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120409_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120409_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120409_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120409;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120409;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120409
from vespa_analysts.project060_spot_file_20120409_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120409 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120409
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120409 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a

select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120409;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120409.csv' format ascii;
commit;

--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120409





---20120410----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-10';
SET @var_prog_period_end    = '2012-04-18';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120410 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120410
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



and (   Adjusted_Event_Start_Time between ''2012-04-10 05:00:00'' and ''2012-04-11 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-10 05:00:00'' and ''2012-04-11 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-10 05:00:00'' and ''2012-04-11 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-10 05:00:00'' and ''2012-04-11 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120410;

commit;

alter table vespa_analysts.project060_all_viewing_20120410 add live tinyint;

update vespa_analysts.project060_all_viewing_20120410
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120410
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
alter table vespa_analysts.project060_all_viewing_20120410 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120410
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120410 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120410(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120410(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120410(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120410(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120410(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120410(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120410(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120410(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120410
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120410
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120410
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120410
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
        vespa_analysts.project060_all_viewing_20120410 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120410
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120410
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
update vespa_analysts.project060_all_viewing_20120410
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120410 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120410 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120410 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120410 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120410 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120410 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120410
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120410
;
commit;


---
update vespa_analysts.project060_all_viewing_20120410
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120410
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120410
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
from vespa_analysts.project060_all_viewing_20120410
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120410 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120410
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120410
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120410 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120410 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120410
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
from vespa_analysts.project060_all_viewing_20120410
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120410;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120410 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120410 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120410 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120410  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120410  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120410 add weighting double;

update vespa_analysts.project060_all_viewing_20120410 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120410  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120410 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120410 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120410 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120410  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120410 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120410 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120410 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120410 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120410
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120410
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

update vespa_analysts.project060_all_viewing_20120410
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120410 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120410;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120410 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120410 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120410 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120410 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120410 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120410;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120410;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120410;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120410;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120410;
drop table vespa_analysts.project060_allocated_minutes_total_20120410;
drop table vespa_analysts.project060_full_allocated_minutes_20120410;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120410
from vespa_analysts.project060_all_viewing_20120410 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120410
from vespa_analysts.project060_all_viewing_20120410 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120410
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120410
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120410
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120410
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120410
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120410
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120410
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120410
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120410(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120410(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120410
from vespa_analysts.project060_partial_minutes_for_allocation_20120410
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120410(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120410(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120410
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120410
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120410(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120410(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120410
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120410 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120410 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120410;

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
into vespa_analysts.project060_full_allocated_minutes_20120410
from vespa_analysts.project060_all_viewing_20120410 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120411;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120410

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120410
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120410(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120410(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120410(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120410-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120410
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120410
(  full_column_detail '\n')
FROM '/staging2/B20120410.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120410 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120410 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120410 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120410 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120410 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120410;
--drop table vespa_analysts.project060_spot_file_20120410;
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
into vespa_analysts.project060_spot_file_20120410
from vespa_analysts.project060_raw_spot_file_20120410
;

--select * from vespa_analysts.project060_spot_file_20120410 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120410 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120410 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120410
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120410 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120410 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120410 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120410_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120410_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120410_expanded
from vespa_analysts.project060_spot_file_20120410 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120410 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120410_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120410_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120410_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120410_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120410_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120410_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120410_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120410_expanded
;


alter table vespa_analysts.project060_spot_file_20120410_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120410_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120410_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120410_expanded
;

alter table  vespa_analysts.project060_spot_file_20120410_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120410_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120410_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120410_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120410_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120410_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120410_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120410;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120410
from vespa_analysts.project060_spot_file_20120410_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120410_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120410_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120410_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120410;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120410;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120410
from vespa_analysts.project060_spot_file_20120410_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120410 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120410
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120410 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120410;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120410.csv' format ascii;
commit;
*/
--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120410



---20120411----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-11';
SET @var_prog_period_end    = '2012-04-19';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120411 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120411
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



and (   Adjusted_Event_Start_Time between ''2012-04-11 05:00:00'' and ''2012-04-12 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-11 05:00:00'' and ''2012-04-12 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-11 05:00:00'' and ''2012-04-12 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-11 05:00:00'' and ''2012-04-12 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120411;

commit;

alter table vespa_analysts.project060_all_viewing_20120411 add live tinyint;

update vespa_analysts.project060_all_viewing_20120411
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120411
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
alter table vespa_analysts.project060_all_viewing_20120411 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120411
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120411 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120411(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120411(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120411(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120411(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120411(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120411(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120411(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120411(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120411
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120411
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120411
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120411
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
        vespa_analysts.project060_all_viewing_20120411 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120411
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120411
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
update vespa_analysts.project060_all_viewing_20120411
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120411 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120411 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120411 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120411 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120411 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120411 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120411
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120411
;
commit;


---
update vespa_analysts.project060_all_viewing_20120411
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120411
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120411
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
from vespa_analysts.project060_all_viewing_20120411
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120411 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120411
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120411
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120411 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120411 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120411
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
from vespa_analysts.project060_all_viewing_20120411
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120411;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120411 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120411 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120411 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120411  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120411  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120411 add weighting double;

update vespa_analysts.project060_all_viewing_20120411 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120411  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120411 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120411 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120411 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120411  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120411 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120411 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120411 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120411 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120411
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120411
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

update vespa_analysts.project060_all_viewing_20120411
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120411 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120411;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120411 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120411 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120411 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120411 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120411 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120411;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120411;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120411;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120411;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120411;
drop table vespa_analysts.project060_allocated_minutes_total_20120411;
drop table vespa_analysts.project060_full_allocated_minutes_20120411;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120411
from vespa_analysts.project060_all_viewing_20120411 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120411
from vespa_analysts.project060_all_viewing_20120411 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120411
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120411
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120411
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120411
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120411
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120411
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120411
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120411
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120411(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120411(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120411
from vespa_analysts.project060_partial_minutes_for_allocation_20120411
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120411(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120411(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120411
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120411
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120411(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120411(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120411
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120411 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120411 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120411;

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
into vespa_analysts.project060_full_allocated_minutes_20120411
from vespa_analysts.project060_all_viewing_20120411 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120412;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120411

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120411
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120411(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120411(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120411(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120411-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120411
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120411
(  full_column_detail '\n')
FROM '/staging2/B20120411.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120411 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120411 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120411 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120411 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120411 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120411;
--drop table vespa_analysts.project060_spot_file_20120411;
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
into vespa_analysts.project060_spot_file_20120411
from vespa_analysts.project060_raw_spot_file_20120411
;

--select * from vespa_analysts.project060_spot_file_20120411 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120411 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120411 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120411
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120411 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120411 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120411 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120411_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120411_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120411_expanded
from vespa_analysts.project060_spot_file_20120411 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120411 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120411_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120411_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120411_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120411_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120411_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120411_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120411_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120411_expanded
;


alter table vespa_analysts.project060_spot_file_20120411_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120411_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120411_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120411_expanded
;

alter table  vespa_analysts.project060_spot_file_20120411_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120411_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120411_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120411_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120411_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120411_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120411_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120411;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120411
from vespa_analysts.project060_spot_file_20120411_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120411_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120411_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120411_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120411;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120411;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120411
from vespa_analysts.project060_spot_file_20120411_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120411 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120411
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120411 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120411;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120411.csv' format ascii;
commit;
*/
--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120411



---20120412----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-12';
SET @var_prog_period_end    = '2012-04-20';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120412 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120412
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



and (   Adjusted_Event_Start_Time between ''2012-04-12 05:00:00'' and ''2012-04-13 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-12 05:00:00'' and ''2012-04-13 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-12 05:00:00'' and ''2012-04-13 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-12 05:00:00'' and ''2012-04-13 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120412;

commit;

alter table vespa_analysts.project060_all_viewing_20120412 add live tinyint;

update vespa_analysts.project060_all_viewing_20120412
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120412
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
alter table vespa_analysts.project060_all_viewing_20120412 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120412
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120412 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120412(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120412(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120412(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120412(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120412(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120412(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120412(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120412(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120412
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120412
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120412
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120412
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
        vespa_analysts.project060_all_viewing_20120412 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120412
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120412
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
update vespa_analysts.project060_all_viewing_20120412
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120412 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120412 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120412 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120412 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120412 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120412 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120412
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120412
;
commit;


---
update vespa_analysts.project060_all_viewing_20120412
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120412
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120412
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
from vespa_analysts.project060_all_viewing_20120412
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120412 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120412
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120412
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120412 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120412 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120412
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
from vespa_analysts.project060_all_viewing_20120412
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120412;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120412 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120412 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120412 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120412  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120412  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120412 add weighting double;

update vespa_analysts.project060_all_viewing_20120412 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120412  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120412 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120412 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120412 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120412  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120412 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120412 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120412 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120412 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120412
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120412
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

update vespa_analysts.project060_all_viewing_20120412
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120412 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120412;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120412 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120412 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120412 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120412 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120412 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120412;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120412;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120412;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120412;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120412;
drop table vespa_analysts.project060_allocated_minutes_total_20120412;
drop table vespa_analysts.project060_full_allocated_minutes_20120412;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120412
from vespa_analysts.project060_all_viewing_20120412 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120412
from vespa_analysts.project060_all_viewing_20120412 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120412
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120412
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120412
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120412
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120412
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120412
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120412
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120412
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120412(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120412(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120412
from vespa_analysts.project060_partial_minutes_for_allocation_20120412
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120412(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120412(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120412
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120412
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120412(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120412(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120412
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120412 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120412 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120412;

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
into vespa_analysts.project060_full_allocated_minutes_20120412
from vespa_analysts.project060_all_viewing_20120412 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120413;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120412

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120412
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120412(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120412(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120412(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120412-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120412
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120412
(  full_column_detail '\n')
FROM '/staging2/B20120412.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120412 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120412 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120412 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120412 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120412 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120412;
--drop table vespa_analysts.project060_spot_file_20120412;
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
into vespa_analysts.project060_spot_file_20120412
from vespa_analysts.project060_raw_spot_file_20120412
;

--select * from vespa_analysts.project060_spot_file_20120412 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120412 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120412 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120412
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120412 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120412 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120412 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120412_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120412_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120412_expanded
from vespa_analysts.project060_spot_file_20120412 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120412 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120412_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120412_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120412_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120412_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120412_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120412_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120412_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120412_expanded
;


alter table vespa_analysts.project060_spot_file_20120412_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120412_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120412_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120412_expanded
;

alter table  vespa_analysts.project060_spot_file_20120412_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120412_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120412_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120412_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120412_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120412_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120412_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120412;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120412
from vespa_analysts.project060_spot_file_20120412_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120412_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120412_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120412_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120412;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120412;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120412
from vespa_analysts.project060_spot_file_20120412_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120412 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120412
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120412 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120412;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120412.csv' format ascii;
commit;
*/
--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120412



---20120413----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-13';
SET @var_prog_period_end    = '2012-04-21';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120413 ( -- drop table vespa_analysts.project060_all_viewing
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
    insert into vespa_analysts.project060_all_viewing_20120413
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



and (   Adjusted_Event_Start_Time between ''2012-04-13 05:00:00'' and ''2012-04-14 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-13 05:00:00'' and ''2012-04-14 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-13 05:00:00'' and ''2012-04-14 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-13 05:00:00'' and ''2012-04-14 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120413;

commit;

alter table vespa_analysts.project060_all_viewing_20120413 add live tinyint;

update vespa_analysts.project060_all_viewing_20120413
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120413
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
alter table vespa_analysts.project060_all_viewing_20120413 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120413
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120413 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120413(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120413(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120413(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120413(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120413(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120413(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120413(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120413(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120413
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120413
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120413
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120413
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
        vespa_analysts.project060_all_viewing_20120413 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120413
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120413
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
update vespa_analysts.project060_all_viewing_20120413
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120413 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120413 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120413 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120413 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120413 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120413 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120413
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120413
;
commit;


---
update vespa_analysts.project060_all_viewing_20120413
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120413
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120413
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
from vespa_analysts.project060_all_viewing_20120413
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120413 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120413
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120413
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120413 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120413 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120413
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
from vespa_analysts.project060_all_viewing_20120413
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120413;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120413 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120413 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120413 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120413  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120413  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120413 add weighting double;

update vespa_analysts.project060_all_viewing_20120413 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120413  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120413 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120413 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120413 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120413  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120413 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120413 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120413 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120413 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120413
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120413
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

update vespa_analysts.project060_all_viewing_20120413
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120413 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120413;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120413 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120413 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120413 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120413 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120413 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120413;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120413;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120413;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120413;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120413;
drop table vespa_analysts.project060_allocated_minutes_total_20120413;
drop table vespa_analysts.project060_full_allocated_minutes_20120413;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120413
from vespa_analysts.project060_all_viewing_20120413 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120413
from vespa_analysts.project060_all_viewing_20120413 
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120413
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120413
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120413
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120413
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120413
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120413
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120413
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120413
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120413(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120413(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120413
from vespa_analysts.project060_partial_minutes_for_allocation_20120413
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120413(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120413(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120413
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120413
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120413(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120413(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120413
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120413 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120413 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120413;

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
into vespa_analysts.project060_full_allocated_minutes_20120413
from vespa_analysts.project060_all_viewing_20120413 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120414;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120413

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120413
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120413(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120413(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120413(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120413-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120413
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120413
(  full_column_detail '\n')
FROM '/staging2/B20120413.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120413 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120413 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120413 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120413 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120413 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120413;
--drop table vespa_analysts.project060_spot_file_20120413;
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
into vespa_analysts.project060_spot_file_20120413
from vespa_analysts.project060_raw_spot_file_20120413
;

--select * from vespa_analysts.project060_spot_file_20120413 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120413 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120413 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120413
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120413 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120413 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120413 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120413_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120413_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120413_expanded
from vespa_analysts.project060_spot_file_20120413 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120413 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120413_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120413_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120413_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120413_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120413_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120413_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120413_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120413_expanded
;


alter table vespa_analysts.project060_spot_file_20120413_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120413_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120413_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120413_expanded
;

alter table  vespa_analysts.project060_spot_file_20120413_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120413_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120413_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120413_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120413_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120413_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120413_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120413;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120413
from vespa_analysts.project060_spot_file_20120413_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120413_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120413_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120413_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120413;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120413;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120413
from vespa_analysts.project060_spot_file_20120413_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120413 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120413
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120413 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120413;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120413.csv' format ascii;
commit;
*/
--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120413




---20120414----
---PART A  - Live Viewing of Sky Channels (All Live Viewing between 2nd and 15th Jan Inclusive---
  -- Set up parameters
--  Not used if previous days aleardy created
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-04-14';
SET @var_prog_period_end    = '2012-04-22';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--select count(*) from vespa_analysts.project060_all_viewing_20120404;
--select top 100 * from vespa_analysts.project060_all_viewing_20120404;
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing;
-- To store all the viewing records:
create table vespa_analysts.project060_all_viewing_20120414 ( -- drop table vespa_analysts.project060_all_viewing_20120414
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
    insert into vespa_analysts.project060_all_viewing_20120414
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



and (   Adjusted_Event_Start_Time between ''2012-04-14 05:00:00'' and ''2012-04-15 04:59:59''
    or  X_Adjusted_Event_End_Time between ''2012-04-14 05:00:00'' and ''2012-04-15 04:59:59''    
    or  Recorded_Time_UTC between ''2012-04-14 05:00:00'' and ''2012-04-15 04:59:59''  
    or  dateadd(second,X_Event_Duration,Recorded_Time_UTC) between ''2012-04-14 05:00:00'' and ''2012-04-15 04:59:59''  
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
--select max(Adjusted_Event_Start_Time) from vespa_analysts.project060_all_viewing_20120414;

commit;

alter table vespa_analysts.project060_all_viewing_20120414 add live tinyint;

update vespa_analysts.project060_all_viewing_20120414
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120414
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
alter table vespa_analysts.project060_all_viewing_20120414 add channel_name_inc_hd varchar(40);

update vespa_analysts.project060_all_viewing_20120414
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.project060_all_viewing_20120414 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;



--select * from vespa_analysts.vespa_max_caps_live_playback_apr_onwards;

-----Part C Applying capping details to viewing data

-------------------
-- add indexes to improve performance
create hg index idx1 on vespa_analysts.project060_all_viewing_20120414(subscriber_id);
create dttm index idx2 on vespa_analysts.project060_all_viewing_20120414(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project060_all_viewing_20120414(recorded_time_utc);
create lf index idx4 on vespa_analysts.project060_all_viewing_20120414(live);
create dttm index idx5 on vespa_analysts.project060_all_viewing_20120414(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project060_all_viewing_20120414(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project060_all_viewing_20120414(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.project060_all_viewing_20120414(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.project060_all_viewing_20120414
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

---Doesn't apply for records that are not programme viewing (e.g., Standby/fast forward etc.,)

update vespa_analysts.project060_all_viewing_20120414
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.project060_all_viewing_20120414
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.project060_all_viewing_20120414
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
        vespa_analysts.project060_all_viewing_20120414 base left outer join vespa_analysts.vespa_max_caps_live_playback_apr_onwards caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.project060_all_viewing_20120414
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.project060_all_viewing_20120414
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
update vespa_analysts.project060_all_viewing_20120414
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

--select capped_flag  , count(*) from vespa_analysts.project060_all_viewing_20120414 where play_back_speed is null group by capped_flag order by capped_flag


--select top 500 *  from vespa_analysts.project060_all_viewing_20120414 where capped_flag=1;

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
alter table vespa_analysts.project060_all_viewing_20120414 add viewing_record_start_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120414 add viewing_record_start_time_local datetime;


alter table vespa_analysts.project060_all_viewing_20120414 add viewing_record_end_time_utc datetime;
alter table vespa_analysts.project060_all_viewing_20120414 add viewing_record_end_time_local datetime;

update vespa_analysts.project060_all_viewing_20120414
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.project060_all_viewing_20120414
;
commit;


---
update vespa_analysts.project060_all_viewing_20120414
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.project060_all_viewing_20120414
;
commit;

--select top 100 * from vespa_analysts.project060_all_viewing;

update vespa_analysts.project060_all_viewing_20120414
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
from vespa_analysts.project060_all_viewing_20120414
;
commit;

--Create Local Time version o fwhen event starts to use for VOSDAL Calculation

alter table vespa_analysts.project060_all_viewing_20120414 add adjusted_event_start_time_local datetime;

update vespa_analysts.project060_all_viewing_20120414
set adjusted_event_start_time_local= case 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,adjusted_event_start_time) 
when dateformat(adjusted_event_start_time,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,adjusted_event_start_time) 
                    else adjusted_event_start_time  end
from vespa_analysts.project060_all_viewing_20120414
;
commit;

---Add on Local Time Versions for Capped start/end Times

alter table vespa_analysts.project060_all_viewing_20120414 add capped_x_viewing_start_time_local datetime;
alter table vespa_analysts.project060_all_viewing_20120414 add capped_x_viewing_end_time_local datetime;
commit;
update vespa_analysts.project060_all_viewing_20120414
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
from vespa_analysts.project060_all_viewing_20120414
;
commit;

--select top 500 * from vespa_analysts.project060_all_viewing_20120414;


----PART D - Add on scaling details-----
---Add on Box weight----
---Add on Weighted value and Affluence/Box Type splits----
commit;
--select top 100 * from vespa_analysts.scaling_dialback_intervals;
--select top 100 * from vespa_analysts.scaling_segments;                                                 
---Add Scaling ID each account is to be assigned to based on the day they view the broadcast not date recorded (if playback)
--alter table vespa_analysts.project060_all_viewing_20120414 delete scaling_segment_id;commit;
alter table  vespa_analysts.project060_all_viewing_20120414 add scaling_segment_id integer;

update vespa_analysts.project060_all_viewing_20120414 
set scaling_segment_id=b.scaling_segment_id
from vespa_analysts.project060_all_viewing_20120414  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (a.adjusted_event_start_time as date)  between b.reporting_starts and b.reporting_ends
;
commit;

--select scaling_segment_id , count(*) from vespa_analysts.project060_all_viewing_20120414  group by scaling_segment_id order by scaling_segment_id;
--select count(*) from vespa_analysts.scaling_weightings;
---Add weight for each scaling ID for each record

alter table vespa_analysts.project060_all_viewing_20120414 add weighting double;

update vespa_analysts.project060_all_viewing_20120414 
set weighting=b.weighting
from vespa_analysts.project060_all_viewing_20120414  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (a.adjusted_event_start_time as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
;
commit;

alter table vespa_analysts.project060_all_viewing_20120414 add affluence varchar(10) ;
alter table vespa_analysts.project060_all_viewing_20120414 add pvr tinyint;


/*
update vespa_analysts.project060_all_viewing_20120414 
set 
--affluence=case when b.affluence is null then 'Unknown' else b.affluence end
pvr=case when b.pvr =1 then 1 else 0 end
from vespa_analysts.project060_all_viewing_20120414  as a
left outer join vespa_analysts.scaling_segments_lookup as b
on a.scaling_segment_id=b.scaling_segment_id
commit;
*/

--

--select account_number ,scaling_segment_id into #accounts from vespa_analysts.scaling_dialback_intervals where reporting_starts<= '2012-04-29' and reporting_ends >='2012-04-29' order by account_number;

--select  sum(weighting) from #accounts as a left outer join vespa_analysts.scaling_weightings as b on a.scaling_segment_id=b.scaling_segment_id where scaling_day = '2012-04-29';
--select count(*),sum(case when weighting is null then 1 else 0 end) as missing from vespa_analysts.project060_all_viewing


--select top 500 * from vespa_analysts.scaling_segments_lookup ;

--select top 500 * from vespa_analysts.project060_all_viewing_20120414 ;
--select pvr, count(*) from vespa_analysts.project060_all_viewing_20120414 group by pvr;
--select affluence, count(*) from vespa_analysts.project060_all_viewing_20120414 group by affluence;

commit;


----Add on viewing type (Live/Vosdal/Playback) for each viewing event----

alter table  vespa_analysts.project060_all_viewing_20120414 add live_timeshifted_type varchar(30) ;

update vespa_analysts.project060_all_viewing_20120414
set live_timeshifted_type = case    when live =1 then '01: Live' 
                                    when live = 0 and cast(dateadd(hour,-2,adjusted_event_start_time_local) as date) <= cast(viewing_record_start_time_local as date)  then '02: VOSDAL'                                 
                                    when live=0 and dateadd(hour,164,recorded_time_utc)>adjusted_event_start_time then '03: Playback 2-7 Days'
                                    else '04: Not Within 164h Window'  end
from vespa_analysts.project060_all_viewing_20120414
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

update vespa_analysts.project060_all_viewing_20120414
set a.service_key = case    when a.service_key is not null then a.service_key else b.service_key end
from vespa_analysts.project060_all_viewing_20120414 as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.original_network_id=b.ssp_network_id and a.transport_stream_id=b.transport_id and a.si_service_id =b.service_id 
 
; 
commit;
--select top 100 *  from vespa_analysts.project060_all_viewing_20120414;
--select * into  vespa_analysts.project060_all_viewing_copy_for_minute_loop  from vespa_analysts.project060_all_viewing;commit;

--select count(*) from vespa_analysts.project060_all_viewing_20120414 where cast(recorded_time_utc as date) = '1970-01-01'
--select top 500 * from vespa_analysts.project060_all_viewing_20120414 where cast(recorded_time_utc as date) = '1970-01-01'
--select * from  vespa_analysts.project060_all_viewing_20120414 where subscriber_id = 6349 order by adjusted_event_start_time , x_adjusted_event_end_time
--select live_timeshifted_type , count(*) from vespa_analysts.project060_all_viewing_20120414 group by live_timeshifted_type order by live_timeshifted_type;
--select top 1000 capped_x_viewing_start_time_local , capped_x_viewing_end_time_local , viewing_record_start_time_local , viewing_record_end_time_local from vespa_analysts.project060_all_viewing_20120414 where capped_x_viewing_start_time_local is not null

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
drop table vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120414;
drop table vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120414;
drop table vespa_analysts.project060_partial_minutes_for_allocation_20120414;
drop table vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120414;
drop table vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414;
drop table vespa_analysts.project060_total_seconds_viewed_in_minute_20120414;
drop table vespa_analysts.project060_allocated_minutes_total_20120414;
drop table vespa_analysts.project060_full_allocated_minutes_20120414;
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
into  vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120414
from vespa_analysts.project060_all_viewing_20120414 
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
into  vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120414
from vespa_analysts.project060_all_viewing_20120414 
where capped_x_viewing_start_time_local is not null and live_timeshifted_type<>'04: Not Within 164h Window'
and (play_back_speed is null or play_back_speed = 2)  
and  datepart(second, capped_x_viewing_end_time_local)<>0
and dateadd(second,  - datepart(second, capped_x_viewing_start_time_local), capped_x_viewing_start_time_local)
<>dateadd(second,  - datepart(second, capped_x_viewing_end_time_local), capped_x_viewing_end_time_local)

order by subscriber_id , capped_x_viewing_start_time_local
;
commit;
--select top 500 * from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120414;
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
into vespa_analysts.project060_partial_minutes_for_allocation_20120414
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120414
;

insert into vespa_analysts.project060_partial_minutes_for_allocation_20120414
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_start_minute_20120414
where seconds_viewed_of_second_broadcast_minute>0
;

---Repeat for End Seconds table
insert into vespa_analysts.project060_partial_minutes_for_allocation_20120414
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,first_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_first_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120414
;

insert into  vespa_analysts.project060_partial_minutes_for_allocation_20120414
select  subscriber_id
,service_key
,live_timeshifted_type
,weighting
,real_time_minute_start as real_time_minute
,last_minute_of_broadcast_viewed as broadcast_minute
,seconds_viewed_of_second_broadcast_minute as seconds_viewed_in_minute
,capped_x_viewing_start_time_local
from vespa_analysts.project060_all_viewing_odd_seconds_end_minute_20120414
where seconds_viewed_of_second_broadcast_minute>0
;

--select * from vespa_analysts.project060_partial_minutes_for_allocation where subscriber_id = 161640 order by real_time_minute

----Group together by Subscriber_id , Service Key and Viewing Type---
commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_for_allocation_20120414(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_for_allocation_20120414(real_time_minute);
---Sum Up viewing by broadcast minute--

select subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute
,max(weighting) as weighting_value
,sum(seconds_viewed_in_minute) as total_seconds_viewed_of_broadcast_minute
,min(capped_x_viewing_start_time_local) as time_broadcast_minute_event_started
into  vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120414
from vespa_analysts.project060_partial_minutes_for_allocation_20120414
group by subscriber_id
,service_key
,live_timeshifted_type
,real_time_minute
,broadcast_minute;

commit;
create hg index idx1 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120414(subscriber_id);
create hg index idx2 on vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120414(real_time_minute);

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
into vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414
from vespa_analysts.project060_partial_minutes_grouped_by_broadcast_minute_20120414
;

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id =161640

commit;
create hg index idx1 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414(most_watched_record);
create hg index idx2 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414(subscriber_id);
create hg index idx3 on vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414(real_time_minute);

--select * from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes where subscriber_id = 95533

---Create Table of Total Viewed Seconds Accross all Channels per subscriber_id per minute

select subscriber_id
,real_time_minute
,sum(total_seconds_viewed_of_broadcast_minute) as total_seconds_viewed_in_minute_by_subscriber
into vespa_analysts.project060_total_seconds_viewed_in_minute_20120414
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414
group by subscriber_id
,real_time_minute;

--select total_seconds_viewed_in_minute_by_subscriber, count(*) from vespa_analysts.project060_total_seconds_viewed_in_minute group by total_seconds_viewed_in_minute_by_subscriber order by  total_seconds_viewed_in_minute_by_subscriber;


commit;
create hg index idx1 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120414(subscriber_id);
create hg index idx2 on vespa_analysts.project060_total_seconds_viewed_in_minute_20120414(real_time_minute);

delete from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414 where most_watched_record <>1;

--drop table vespa_analysts.project060_allocated_minutes_total;
select a.subscriber_id
,service_key
,a.real_time_minute as real_time_minute_start
,a.real_time_minute as real_time_minute_end
,broadcast_minute as first_broadcast_minute
,broadcast_minute as last_broadcast_minute
,live_timeshifted_type
,weighting_value
into vespa_analysts.project060_allocated_minutes_total_20120414
from vespa_analysts.project060_rank_viewing_in_minute_partial_minutes_20120414 as a
left outer join vespa_analysts.project060_total_seconds_viewed_in_minute_20120414 as b
on a.subscriber_id=b.subscriber_id and a.real_time_minute = b.real_time_minute
where  total_seconds_viewed_of_broadcast_minute>=15 and total_seconds_viewed_in_minute_by_subscriber>=30 ;

commit;
--select top 100 * from vespa_analysts.project060_allocated_minutes_total_20120414;

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
into vespa_analysts.project060_full_allocated_minutes_20120414
from vespa_analysts.project060_all_viewing_20120414 
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

--select top 100 * from vespa_analysts.project060_full_allocated_minutes_20120415;
commit;

--Then Append necessary data into minutes table

insert into vespa_analysts.project060_allocated_minutes_total_20120414

select subscriber_id
,service_key
,real_time_minute_start
,real_time_minute_end
,start_broadcast_minute_viewed as first_broadcast_minute
,end_broadcast_minute_viewed as last_broadcast_minute
,live_timeshifted_type
,weighting as weighting_value
from vespa_analysts.project060_full_allocated_minutes_20120414
;

commit;


--select * from vespa_analysts.project060_full_allocated_minutes where subscriber_id = 15849745;
--select * from vespa_analysts.project060_allocated_minutes_total where subscriber_id = 34903 order by real_time_minute_start;





commit;
create hg index idx1 on vespa_analysts.project060_allocated_minutes_total_20120414(service_key);
create hg index idx2 on vespa_analysts.project060_allocated_minutes_total_20120414(first_broadcast_minute);
create hg index idx3 on vespa_analysts.project060_allocated_minutes_total_20120414(last_broadcast_minute);
commit;


commit;
----Import BARB Data for _20120414-----

commit;
create table vespa_analysts.project060_raw_spot_file_20120414
(  full_column_detail varchar(248))
;

LOAD TABLE  vespa_analysts.project060_raw_spot_file_20120414
(  full_column_detail '\n')
FROM '/staging2/B20120414.CET'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
;
--alter table vespa_analysts.project060_raw_spot_file_20120414 delete row_number;
alter table vespa_analysts.project060_raw_spot_file_20120414 add row_number integer identity;

--select * from vespa_analysts.project060_raw_spot_file_20120414 where substr(full_column_detail,3,1) ='A';
--select * from vespa_analysts.project060_raw_spot_file_20120414 order by full_column_detail desc;
--delete first row as this contains header information not in same format as rest of data.
delete from vespa_analysts.project060_raw_spot_file_20120414 where row_number =1;

--select count(*) from vespa_analysts.project060_raw_spot_file_20120414;
--drop table vespa_analysts.project060_spot_file_20120414;
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
into vespa_analysts.project060_spot_file_20120414
from vespa_analysts.project060_raw_spot_file_20120414
;

--select * from vespa_analysts.project060_spot_file_20120414 where log_station_code_for_spot = '05027';
--select * from vespa_analysts.project060_spot_file_20120414 where clearcast_commercial_number= 'TTBEMAN213030' and sales_house = 'SKY SALES' order by spot_start_time 
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

alter table vespa_analysts.project060_spot_file_20120414 add sales_house varchar(64);

update vespa_analysts.project060_spot_file_20120414
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_file_20120414 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;

---Remove Records that don't relate to BARB Panel or Sky Platform---

--delete from vespa_analysts.project060_spot_file_20120414 where reporting_panel_code <> '00050';
delete from vespa_analysts.project060_spot_file_20120414 where spot_platform_indicator not in ( '00','0A','28');

commit;



commit;
--select count(*) from vespa_analysts.sold_spots_by_panel_sold_only;
--Create Expanded Spot Table to take into account multiple service keys served by a single Log Station and STI Code---
commit;
IF object_id('vespa_analysts.project060_spot_file_20120414_expanded') IS NOT NULL DROP TABLE vespa_analysts.project060_spot_file_20120414_expanded;
select a.*
,b.service_key
into vespa_analysts.project060_spot_file_20120414_expanded
from vespa_analysts.project060_spot_file_20120414 as a
left outer join vespa_analysts.log_station_sti_to_service_key_lookup as b
on cast(a.log_station_code_for_spot as integer)=b.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=b. STI_CODE

left outer join vespa_analysts.sold_spots_by_panel_sold_only as c
on cast(a.log_station_code_for_spot as integer)=c.LOG_STATION_CODE and cast(a.split_transmission_indicator as integer)=c. STI and 
cast(a.reporting_panel_code as integer)=c.panel_code
where c.panel_code is not null
;

--select * from vespa_analysts.project060_spot_file_20120414 where cast(log_station_code_for_spot as integer) = 4319 order by spot_start_time;
--select  log_station_code , sti , panel_code , count(*) as records from vespa_analysts.sold_spots_by_panel_sold_only group by  log_station_code , sti , panel_code order by records desc;
--select count(*) from vespa_analysts.project060_spot_file_20120414_expanded;
--select * from vespa_analysts.sold_spots_by_panel_sold_only where log_station_code = 4319
--Add Channel details (by using service_key)


alter table vespa_analysts.project060_spot_file_20120414_expanded add channel_name varchar(64);

update vespa_analysts.project060_spot_file_20120414_expanded
set channel_name = case when b.channel is null then 'Unknown' else b.channel end
from vespa_analysts.project060_spot_file_20120414_expanded as a
left outer join vespa_analysts.project060_service_key_triplet_lookup as b
on a.service_key=b.service_key
;
commit;

--select top 10000 * from vespa_analysts.project060_spot_file_20120414_expanded order by service_key ,break_start_time;

---Correct some of the Text fields in the orginal BARB file to Integer/Datetime etc.,

alter table vespa_analysts.project060_spot_file_20120414_expanded add spot_duration_integer integer;

update vespa_analysts.project060_spot_file_20120414_expanded
set spot_duration_integer = cast (spot_duration as integer)
from vespa_analysts.project060_spot_file_20120414_expanded
;


alter table vespa_analysts.project060_spot_file_20120414_expanded add raw_corrected_spot_time varchar(6);
alter table vespa_analysts.project060_spot_file_20120414_expanded add corrected_spot_transmission_date date;

update vespa_analysts.project060_spot_file_20120414_expanded
set raw_corrected_spot_time= case 
      when left (spot_start_time,2) in ('24','25','26','27','28','29') then '0'||cast(left (spot_start_time,2) as integer)-24 ||right (spot_start_time,4) 
      else spot_start_time end 
,corrected_spot_transmission_date  = case when left (spot_start_time,2) in ('24','25','26','27','28','29') then cast(date_of_transmission as date)+1
else cast(date_of_transmission as date) end
from vespa_analysts.project060_spot_file_20120414_expanded
;

alter table  vespa_analysts.project060_spot_file_20120414_expanded add corrected_spot_transmission_start_datetime datetime;

update vespa_analysts.project060_spot_file_20120414_expanded
set corrected_spot_transmission_start_datetime = corrected_spot_transmission_date
;
commit;

update vespa_analysts.project060_spot_file_20120414_expanded
set corrected_spot_transmission_start_datetime = dateadd(hour, cast(left(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120414_expanded
set corrected_spot_transmission_start_datetime = dateadd(minute, cast(substr(raw_corrected_spot_time,3,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

update vespa_analysts.project060_spot_file_20120414_expanded
set corrected_spot_transmission_start_datetime = dateadd(second, cast(right(raw_corrected_spot_time,2) as integer),corrected_spot_transmission_start_datetime)
;
commit;

alter table  vespa_analysts.project060_spot_file_20120414_expanded add corrected_spot_transmission_start_minute datetime;

update vespa_analysts.project060_spot_file_20120414_expanded
set corrected_spot_transmission_start_minute = dateadd(second,  - datepart(second, corrected_spot_transmission_start_datetime), corrected_spot_transmission_start_datetime) 
;
commit;

--drop table vespa_analysts.spot_summary_values_20120414;
select service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code_for_spot as log_station_code
,split_transmission_indicator
,count(*) as number_of_spots
,sum(spot_duration_integer) as total_spot_duration_seconds
into vespa_analysts.spot_summary_values_20120414
from vespa_analysts.project060_spot_file_20120414_expanded
group by service_key
,channel_name
--,channel_name_inc_hd
,sales_house
,sales_house_identifier
,log_station_code
,split_transmission_indicator
;
--select * from  vespa_analysts.project060_spot_file_20120414_expanded;

commit;
create hg index idx1 on vespa_analysts.project060_spot_file_20120414_expanded(service_key);
create hg index idx2 on vespa_analysts.project060_spot_file_20120414_expanded(corrected_spot_transmission_start_minute);

--select count(*) from vespa_analysts.project060_allocated_minutes_total_20120414;
---Get Views by Spot
--drop table  vespa_analysts.project060_spot_summary_viewing_figures_20120414;commit;
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
into vespa_analysts.project060_spot_summary_viewing_figures_20120414
from vespa_analysts.project060_spot_file_20120414_expanded as a
left outer join vespa_analysts.project060_allocated_minutes_total_20120414 as b
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


update vespa_analysts.project060_spot_summary_viewing_figures_20120414
set sales_house = case when b.sales_house is null then 'Unknown' else b.sales_house end
from vespa_analysts.project060_spot_summary_viewing_figures_20120414 as a
left outer join vespa_analysts.barb_sales_house_lookup as b
on cast(a.sales_house_identifier as integer)=cast(b.sales_house_identifier as integer)
;

commit;
--select  cast(a.sales_house_identifier as integer) from vespa_analysts.project060_spot_summary_viewing_figures as a
/*
select *,
cast(weighted_views as real) * cast((cast(spot_duration_integer as real)/30) as decimal(32,12)) as thirty_second_equivalent_impacts 
 from vespa_analysts.project060_spot_summary_viewing_figures_20120414;

output to 'C:\Users\barnetd\Documents\Project 060 - Share of Consolidated Impacts\soci_pivot_data_20120414.csv' format ascii;
commit;
*/
--select count(*) from  vespa_analysts.project060_spot_summary_viewing_figures_20120414

















