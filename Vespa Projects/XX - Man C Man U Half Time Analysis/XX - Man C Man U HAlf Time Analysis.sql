/*------------------------------------------------------------------------------
        Project: Vespa analysis VXXX Man C - Man U Half Time Analysis
        Version: 01
        Created: 20120511
        Analyst: Dan Barnett
        SK Prod: 10
*/------------------------------------------------------------------------------
/*
        Purpose (From Brief)
        -------
       Understand Sec by sec volumes for Man C Man U from game played 30 April 2012

        SECTIONS
        --------

        PART    A - Viewing Data
                B - UK Base and Profile Data
                C - Viewing Logs
                D - Output of Minute by Minute Results          
             
        Tables
        -------
        vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
        vespa_analysts.sky_base_2011_11_29
        vespa_analysts.stratifiedsampling_20111129
commit;

*/

---A01 Get EPG Data for Carling Cup Match between Arsenal and Man City---

select channel_name , epg_title,synopsis, tx_start_datetime_utc ,tx_end_datetime_utc,programme_trans_sk from sk_prod.vespa_epg_dim 
where (upper(channel_name) like ('%SPORT%') or upper(channel_name) like  '%3D%')
and tx_start_datetime_utc between
'2012-04-30 17:00:00' and  '2012-04-30 23:00:00'
order by tx_start_datetime_utc



select channel_name , epg_title,synopsis, tx_start_datetime_utc ,tx_end_datetime_utc,programme_trans_sk from sk_prod.vespa_epg_dim 
where (upper(channel_name) like ('%SPORT%') or upper(channel_name) like  '%3D%')
and tx_start_datetime_utc between
'2012-04-30 17:00:00' and  '2012-04-30 23:00:00' and epg_title = 'Live Ford Monday Night Football'
order by tx_start_datetime_utc


select programme_trans_sk from sk_prod.vespa_epg_dim 
where (upper(channel_name) like ('%SPORT%') or upper(channel_name) like  '%3D%')
and tx_start_datetime_utc between
'2012-04-30 17:00:00' and  '2012-04-30 23:00:00' and epg_title = 'Live Ford Monday Night Football'
order by tx_start_datetime_utc
;
/*

programme_trans_sk
201205010000003216
201205010000000814
201205010000000219
201205010000000831
201205010000016067
201205010000014776
201205010000016747
201205010000000587
201205010000012920
201205010000000939

*/

----Part A02 Viewing Data for Programme ----


--------------------------------------------------------------------------------
-- PART A02 Viewing Data
--------------------------------------------------------------------------------

/*
PART A01 - Populate all viewing data between Date of Broadcast 11th Aug and End August when Vespa Suspended--
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
SET @var_prog_period_start  = '2012-04-30';
SET @var_prog_period_end    = '2012-05-07';


SET @var_cntr = 0;
SET @var_num_days = 8;       -- 
--drop table #vespa_man_c_man_u_half_time;
select programme_trans_sk 
    ,Tx_Start_Datetime_UTC         
    ,Tx_End_Datetime_UTC    
    ,channel_name   
,Epg_Title                    
    ,Genre_Description           
    ,Sub_Genre_Description          
into #vespa_man_c_man_u_half_time
from sk_prod.vespa_epg_dim 
where (upper(channel_name) like ('%SPORT%') or upper(channel_name) like  '%3D%')
and tx_start_datetime_utc between
'2012-04-30 17:00:00' and  '2012-04-30 23:00:00' and epg_title = 'Live Ford Monday Night Football'
order by tx_start_datetime_utc
;



-- To store all the viewing records:
create table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time ( --  drop table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time;
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
    insert into vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
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
 left outer join   #vespa_man_c_man_u_half_time as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
              and x_si_service_type = ''High Definition TV test service''))
     and panel_id in (4,5)
and (play_back_speed is null or play_back_speed=2)
and prog.programme_trans_sk is not null
'     ;

while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;

--select play_back_speed , count(*) as records from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time group by play_back_speed;
--select cast(Adjusted_Event_Start_Time as date) as day_view , count(*) as records from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time group by day_view order by day_view;

--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120430


commit;

alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add live tinyint;

update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
set live = case when play_back_speed is null then 1 else 0 end
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
;
commit;

if object_id('vespa_analysts.channel_name_lookup_old') is not null drop table vespa_analysts.channel_name_lookup_old;
create table vespa_analysts.channel_name_lookup_old 
(channel varchar(90)
,channel_name_grouped varchar(90)
,channel_name_inc_hd varchar(90)
)
;

input into vespa_analysts.channel_name_lookup_old from 'G:\RTCI\Sky Projects\Vespa\Phase1b\Channel Lookup\Channel Lookup Info Phase1b.csv' format ascii;
commit;

alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add channel_name_inc_hd varchar(40);

update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;

----PART 1 Run TO HERE-----



-- add indexes to improve performance
create hg index idx1 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(subscriber_id);
create dttm index idx2 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(recorded_time_utc);
create lf index idx4 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(live)
create dttm index idx5 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(x_viewing_end_time);
create hng index idx7 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(x_cumul_programme_viewed_duration);
create hg index idx8 on vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time(programme_trans_sk);
commit;
-- append fields to table to store additional metrics for capping
alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
    add (
        capped_x_viewing_start_time datetime
        , capped_x_viewing_end_time   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records
update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null
; 
commit;
update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null
; 
commit;

-- update table to create capped start and end times        
update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
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

--Uses Dummy Value 30000 min as current capping of approx 42-45 min seems to low for Live football in particular---
        vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time base left outer join vespa_analysts.vespa_max_caps_20120430_Dummy_Values caps
    on (
        date(base.adjusted_event_start_time) = caps.event_start_day
        and datepart(hour, base.adjusted_event_start_time) = caps.event_start_hour
        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_analysts.vespa_max_caps_20120430;

-- calculate capped_x_programme_viewed_duration
update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
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
update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
    set capped_x_viewing_start_time = null
        , capped_x_viewing_end_time = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
        vespa_analysts.vespa_min_cap_20120430
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;

--select top 500 *  from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time where capped_flag=1;

-----Remove any Records that have a capped flag of 2 or 3 (Exclude As it currently uses old capping levels)---

delete from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
where capped_flag in (2,3)
;
commit;



---Add in Event start and end time and add in local time activity---
--select * from vespa_analysts.trollied_20110811_raw order by log_id;
alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add viewing_record_start_time_utc datetime;
alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add viewing_record_start_time_local datetime;


alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add viewing_record_end_time_utc datetime;
alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add viewing_record_end_time_local datetime;

update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
set viewing_record_start_time_utc=case  when recorded_time_utc  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when recorded_time_utc  >=tx_start_datetime_utc then recorded_time_utc
                                        when adjusted_event_start_time  <tx_start_datetime_utc then tx_start_datetime_utc
                                        when adjusted_event_start_time  >=tx_start_datetime_utc then adjusted_event_start_time else null end
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
;
commit;


---
update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
set viewing_record_end_time_utc= dateadd(second,capped_x_programme_viewed_duration,viewing_record_start_time_utc)
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
;
commit;

--select top 100 * from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time;

update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
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
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
;
commit;

--select * from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time;

---Add Weighting for each Record


---Run Second By Second Figures for each box




---A06 - Trollied with second by second and capping metrics-----

---Create second by second log---
create variable @programme_time_start datetime;
create variable @programme_time_end datetime;
create variable @programme_time datetime;

set @programme_time_start = cast('2012-04-30 20:30:00' as datetime);
set @programme_time_end =cast('2012-04-30 21:15:00' as datetime);
set @programme_time = @programme_time_start;

/*
--drop table vespa_analysts.manu_spurs_20110907_raw ;
select * into vespa_analysts.manu_spurs_20110907_raw 
from #sky_sports_man_u_spurs 
where right(cast(subscriber_id as varchar),2='45')
;
*/
commit;

--exec gen_create_table  'vespa_analysts.manu_spurs_20110907_raw';


commit;
--drop table vespa_analysts.man_u_man_city_second_by_second;
---Create table to insert into loop---
create table vespa_analysts.man_u_man_city_second_by_second
(

subscriber_id                       decimal(8)              not null
--,account_number                     varchar(20)             null
,second_viewed                      datetime                not null
,viewed                             smallint                not null
,viewed_live                        smallint                null
,viewed_playback                    smallint                null
);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into vespa_analysts.man_u_man_city_second_by_second
select subscriber_id
--,account_number
,@programme_time as second_viewed
,1 as viewed
,max(case when play_back_speed is null then 1 else 0 end) as viewed_live
,max(case when play_back_speed is not null then 1 else 0 end) as viewed_playback
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
where  cast(viewing_record_start_time_local as datetime)<=@programme_time and cast(viewing_record_end_time_local as datetime)>@programme_time
and (play_back_speed is null or play_back_speed = 2)
group by subscriber_id
--,account_number 
,second_viewed,viewed
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;



select second_viewed
,sum(viewed) as total_boxes_viewing
--,sum(case when viewed_playback=1 then 0 else viewed_live end) as total_boxes_live
--,sum(viewed_playback) as total_boxes_playback
from vespa_analysts.man_u_man_city_second_by_second
group by second_viewed
order by second_viewed;


----Repeat but with weightings---


--Add weightings
alter table vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time add weighting double;

update vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
set weighting = c.weighting
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
left outer join vespa_analysts.scaling_weightings as c
on b.scaling_segment_id=c.scaling_segment_id
where cast (a.Adjusted_Event_Start_Time as date)  between b.reporting_starts and b.reporting_ends
and c.scaling_day = cast (a.Adjusted_Event_Start_Time as date)
;
commit;

--select weighting , count(*) as records from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time group by weighting order by weighting;
--select count(*) as records from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time ;




set @programme_time_start = cast('2012-04-30 20:30:00' as datetime);
set @programme_time_end =cast('2012-04-30 21:15:00' as datetime);
set @programme_time = @programme_time_start;

commit;

--exec gen_create_table  'vespa_analysts.manu_spurs_20110907_raw';


commit;
--drop table vespa_analysts.man_u_man_city_second_by_second_weighted;
---Create table to insert into loop---
create table vespa_analysts.man_u_man_city_second_by_second_weighted
(

subscriber_id                       decimal(8)              not null
--,account_number                     varchar(20)             null
,second_viewed                      datetime                not null
,viewed                             smallint                not null
,viewed_live                        smallint                null
,viewed_playback                    smallint                null
);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into vespa_analysts.man_u_man_city_second_by_second_weighted
select subscriber_id
--,account_number
,@programme_time as second_viewed
,max(case when weighting is null then 0 else weighting end) as viewed
,max(case when play_back_speed is null then weighting else 0 end) as viewed_live
,max(case when play_back_speed is not null then weighting else 0 end) as viewed_playback
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
where  cast(viewing_record_start_time_local as datetime)<=@programme_time and cast(viewing_record_end_time_local as datetime)>@programme_time
and (play_back_speed is null or play_back_speed = 2)
group by subscriber_id
--,account_number 
,second_viewed
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;



select second_viewed
,sum(viewed) as total_boxes_viewing
--,sum(case when viewed_playback=1 then 0 else viewed_live end) as total_boxes_live
--,sum(viewed_playback) as total_boxes_playback
from vespa_analysts.man_u_man_city_second_by_second_weighted
group by second_viewed
order by second_viewed;



----repeat but for full match----




set @programme_time_start = cast('2012-04-30 19:00:00' as datetime);
set @programme_time_end =cast('2012-04-30 23:00:00' as datetime);
set @programme_time = @programme_time_start;

commit;

--exec gen_create_table  'vespa_analysts.manu_spurs_20110907_raw';


commit;
--drop table vespa_analysts.man_u_man_city_second_by_second_weighted;
---Create table to insert into loop---
create table vespa_analysts.man_u_man_city_second_by_second_weighted_full_match
(

subscriber_id                       decimal(8)              not null
--,account_number                     varchar(20)             null
,second_viewed                      datetime                not null
,viewed                             smallint                not null
,viewed_live                        smallint                null
,viewed_playback                    smallint                null
);
commit;

---Start of Loop
WHILE @programme_time <  @programme_time_end LOOP
insert into vespa_analysts.man_u_man_city_second_by_second_weighted_full_match
select subscriber_id
--,account_number
,@programme_time as second_viewed
,max(case when weighting is null then 0 else weighting end) as viewed
,max(case when play_back_speed is null then weighting else 0 end) as viewed_live
,max(case when play_back_speed is not null then weighting else 0 end) as viewed_playback
from vespa_analysts.VESPA_MAN_C_MAN_U_Half_Time
where  cast(viewing_record_start_time_local as datetime)<=@programme_time and cast(viewing_record_end_time_local as datetime)>@programme_time
and (play_back_speed is null or play_back_speed = 2)
group by subscriber_id
--,account_number 
,second_viewed
;

 SET @programme_time =dateadd(second,1,@programme_time);
    COMMIT;

END LOOP;
commit;



select second_viewed
,sum(viewed) as total_boxes_viewing
--,sum(case when viewed_playback=1 then 0 else viewed_live end) as total_boxes_live
--,sum(viewed_playback) as total_boxes_playback
from vespa_analysts.man_u_man_city_second_by_second_weighted_full_match
group by second_viewed
order by second_viewed;
























