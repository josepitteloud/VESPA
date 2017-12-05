/*##################################################################################
*   FILE HEADER
************************************************************************************
*   Product:          SQL
*   Version:          1.1
*   Author:           Julie Chung (This version D Barnett for 2nd-15th Jan)
*   Creation Date:    27/03/2012 - This version 23/4/12
*   Description:
*
*###################################################################################
*
*   Process depends on: To perform the capping below we require access to the
*                       following tables:
*                       jchung.channel_lookup
*
*
*###################################################################################
*   REVISION HISTORY
************************************************************************************
*   Date        Author  Version         Description
*   27/03/2012  JC      1.0             Initial version
*   23/04/2012  DB      1.1             Look at viewing between 12th and 15th Jan 2012
*
*###################################################################################
*   DESCRIPTION
*
*   Creates the capping table for maximum length duration (based on live/playback, day, hour, primary/secondary, pack, genre)
*
*##################################################################################*/

-- create table including base records you need
IF object_id('vespa_analysts.project047_capping_phase2_data') IS NOT NULL DROP TABLE vespa_analysts.project047_capping_phase2_data;

create table vespa_analysts.project047_capping_phase2_data
                                      (account_number                   varchar(20)
                                      ,subscriber_id                    bigint
                                      ,cb_key_household                 bigint
                                      ,event_type                       varchar(20)
                                      ,x_type_of_viewing_event          varchar(40)
                                      ,adjusted_event_start_time        datetime
                                      ,x_adjusted_event_end_time        datetime
                                      ,x_viewing_start_time             datetime
                                      ,x_viewing_end_time               datetime
                                      ,Tx_Start_Datetime_UTC            datetime
                                      ,Tx_End_Datetime_UTC              datetime
                                      ,tx_date_time_utc                 datetime
                                      ,recorded_time_UTC                datetime
                                      ,live_or_playback                 varchar(10)
                                      ,x_event_duration                 int
                                      ,x_programme_duration             int
                                      ,x_programme_viewed_duration      int
                                      ,X_Viewing_Time_Of_Day            varchar(15)
                                      ,programme_trans_sk               bigint
                                      ,cb_row_id                        bigint
                                      ,channel_name                     varchar(50)
                                      ,epg_title                        varchar(100)
                                      ,genre                            varchar(25)
                                      ,sub_genre                        varchar(25)
                                      ,);
commit;

--create procedure to insert
if object('insert_recs') is not null then drop procedure insert_recs;

create procedure insert_recs(@dset varchar(50)='tname') as

begin

execute('insert into vespa_analysts.project047_capping_phase2_data
select
      base.Account_Number,
      base.Subscriber_Id,
      base.Cb_Key_Household,

      base.Event_Type,
      base.X_Type_Of_Viewing_Event,

      base.Adjusted_Event_Start_Time,
      base.X_Adjusted_Event_End_Time,
      base.X_Viewing_Start_Time,
      base.X_Viewing_End_Time,
      det.Tx_Start_Datetime_UTC,
      det.Tx_End_Datetime_UTC,
      det.tx_date_time_utc,

      base.Recorded_Time_UTC,
      case when base.Play_Back_Speed is null then ''Live'' else ''Playback'' end as live_or_playback,

      base.X_Event_Duration,
      base.X_Programme_Duration,
      base.X_Programme_Viewed_Duration,

      base.X_Viewing_Time_Of_Day,
      base.Programme_Trans_Sk,
      base.cb_row_id,

      case when det.Channel_Name is null then ''Unknown'' else det.Channel_Name end as channel_name,
      case when det.Epg_Title is null then ''Unknown'' else det.Epg_Title end as epg_title,
      case when det.Genre_Description is null then ''Unknown'' else det.Genre_Description end as genre,
      case when det.Sub_Genre_Description is null then ''Unknown'' else det.Sub_Genre_Description end as sub_genre

from '||@dset||' as base
left outer join
sk_prod.vespa_epg_dim det
on base.Programme_Trans_Sk = det.Programme_Trans_Sk
where video_playing_flag = 1
and adjusted_event_start_time <> x_adjusted_event_end_time
and (x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'')
or  (x_type_of_viewing_event = (''Other Service Viewing Event'')
and x_si_service_type = ''High Definition TV test service'')
or x_type_of_viewing_event = (''HD Viewing Event''))
and x_programme_viewed_duration > 0
and panel_id in (4,5)')

commit
end;

execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120102''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120103''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120104''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120105''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120106''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120107''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120108''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120109''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120110''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120111''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120112''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120113''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120114''');  
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20120115''');  
--select count(*) from vespa_analysts.project047_capping_phase2_data;

--create indexes to improve processing
create hg index idx1 on vespa_analysts.project047_capping_phase2_data(subscriber_id);
create dttm index idx2 on vespa_analysts.project047_capping_phase2_data(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project047_capping_phase2_data(recorded_time_utc);
create lf index idx4 on vespa_analysts.project047_capping_phase2_data(live_or_playback)
create dttm index idx5 on vespa_analysts.project047_capping_phase2_data(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project047_capping_phase2_data(x_viewing_end_time);

----Comment out Index as field not in table---
--create hng index idx7 on vespa_analysts.project047_capping_phase2_data(x_cumul_programme_viewed_duration);

--create rank to reorder views as cb row id not ordered correctly
select t1.*
,rank() over (partition by subscriber_id, adjusted_event_start_time order by x_viewing_start_time,tx_start_datetime_utc) as prank
into --drop table
vespa_analysts.project047_capping_phase2_data2
from vespa_analysts.project047_capping_phase2_data t1;
commit;
--34724164

--use rank (instead of cb row id) to order views in correct order for cumulative duration
select t1.*
,sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by prank) as x_cumul_programme_viewed_duration
into --drop table
vespa_analysts.project047_capping_phase2_data3
from vespa_analysts.project047_capping_phase2_data2 t1;
commit;
--34724164

--drop ranks
alter table vespa_analysts.project047_capping_phase2_data3
drop prank;

--add indexes to improve performance
create hg index idx1 on vespa_analysts.project047_capping_phase2_data3(subscriber_id);
create dttm index idx2 on vespa_analysts.project047_capping_phase2_data3(adjusted_event_start_time);
create dttm index idx3 on vespa_analysts.project047_capping_phase2_data3(recorded_time_utc);
create lf index idx4 on vespa_analysts.project047_capping_phase2_data3(live_or_playback)
create dttm index idx5 on vespa_analysts.project047_capping_phase2_data3(x_viewing_start_time);
create dttm index idx6 on vespa_analysts.project047_capping_phase2_data3(x_viewing_end_time);
create hng index idx7 on vespa_analysts.project047_capping_phase2_data3(x_cumul_programme_viewed_duration);

-- update the viewing start and end times for playback records -- viewing start and end times not correctly populated for some playback events
update vespa_analysts.project047_capping_phase2_data3
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null;
commit;
--9879680

update vespa_analysts.project047_capping_phase2_data3
    set
        x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
    where
        recorded_time_utc is not null;
commit;
--9879680

--re-rank to identify duplicate epg entries - pick latest epg entry
select t1.*
,rank() over (partition by subscriber_id, adjusted_event_start_time, x_viewing_start_time order by tx_date_time_utc desc) as xrank
into --drop table
vespa_analysts.project047_capping_phase2_data4
from vespa_analysts.project047_capping_phase2_data3 t1;
commit;
--34724164

--remove duplicate programme titles
delete from vespa_analysts.project047_capping_phase2_data4
where xrank>1;
--782

--remove illegitimate playback views - these views are those that start on event end time and go beyond event end time
delete from vespa_analysts.project047_capping_phase2_data4
where X_Adjusted_Event_End_Time<x_viewing_end_time
and x_viewing_start_time>=X_Adjusted_Event_End_Time;
--3628

--reset x_viewing_end_times for playback views
update vespa_analysts.project047_capping_phase2_data4
set x_viewing_end_time=X_Adjusted_Event_End_Time
where X_Adjusted_Event_End_Time<x_viewing_end_time
and x_viewing_start_time<X_Adjusted_Event_End_Time;
commit;
--156

--add start day and start hour variables
alter table vespa_analysts.project047_capping_phase2_data4
add event_start_day integer,
add event_start_hour integer;

update vespa_analysts.project047_capping_phase2_data4
set event_start_hour= datepart (hour, adjusted_event_start_time)
   ,event_start_day = datepart(day,adjusted_event_start_time);
commit;
--34723382

--obtain event view
select Account_Number,
       Subscriber_Id,
       Cb_Key_Household,
       Event_Type,
       X_Type_Of_Viewing_Event,
       Adjusted_Event_Start_Time,
       X_Adjusted_Event_End_Time,
       X_Event_Duration,
       event_start_hour,
       event_start_day,
       Live_or_Playback,
       count(*) as num_views,
       count(distinct genre) as num_genre,
       count(distinct sub_genre) as num_sub_genre,
       sum(x_programme_viewed_duration) as viewed_duration
into --drop table
vespa_analysts.project047_capping_phase2_view_summary
from vespa_analysts.project047_capping_phase2_data4
group by Account_Number,
       Subscriber_Id,
       Cb_Key_Household,
       Event_Type,
       X_Type_Of_Viewing_Event,
       Adjusted_Event_Start_Time,
       X_Adjusted_Event_End_Time,
       X_Event_Duration,
       event_start_hour,
       event_start_day,
       Live_or_Playback;
--25940851

--add indexes to improve performance
create hg index idx1 on vespa_analysts.project047_capping_phase2_view_summary(subscriber_id);
create dttm index idx2 on vespa_analysts.project047_capping_phase2_view_summary(adjusted_event_start_time);
create lf index idx3 on vespa_analysts.project047_capping_phase2_view_summary(live_or_playback)

--obtain channel, genre, sub_genre at start of event
select t1.*
,rank() over(partition by subscriber_id, adjusted_event_start_time order by x_viewing_start_time,tx_start_datetime_utc) as trank
into --drop table
genre
from vespa_analysts.project047_capping_phase2_data4 t1;
commit;
--34723382

--add channel, genre and sub genre
alter table vespa_analysts.project047_capping_phase2_view_summary
add genre_at_event_start_time varchar(30),
add sub_genre_at_event_start_time varchar(30),
add channel_at_event_start_time varchar(30),
add pack varchar(100) default null,
add pack_grp varchar(20) default null,
add network varchar(100) default null;


commit;
create hg index idx1 on vespa_analysts.genre(subscriber_id);
create dttm index idx2 on vespa_analysts.genre(adjusted_event_start_time);
create hg index idx3 on vespa_analysts.genre(trank);

--Create seperate genre lookup as creating temp space issue---
drop table vespa_analysts.genre2;
select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-02'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-02'
;
commit;

---Repeat for each day---
---3rd Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;
commit;
select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-03'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-03'
;
commit;

---

---Repeat for each day---
---4th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-04'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-04'
;
commit;

---



---Repeat for each day---
---5th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-05'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-05'
;
commit;

---



---Repeat for each day---
---6th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-06'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-06'
;
commit;

---


---Repeat for each day---
---7th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-07'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-07'
;
commit;

---


---Repeat for each day---
---8th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-08'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-08'
;
commit;

---


---Repeat for each day---
---9th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-09'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-09'
;
commit;

---


---Repeat for each day---
---10th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-10'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-10'
;
commit;

---


---Repeat for each day---
---11th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-11'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-11'
;
commit;

---



---Repeat for each day---
---12th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-12'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-12'
;
commit;

---


---Repeat for each day---
---13th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-13'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-13'
;
commit;

---

---Repeat for each day---
---14th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-14'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-14'
;
commit;

---


---Repeat for each day---
---15th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-15'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-15'
;
commit;

---


---Repeat for each day---
---16th Jan
IF object_id('vespa_analysts.genre2') IS NOT NULL DROP TABLE  vespa_analysts.genre2;

select genre
,sub_genre
,channel_name
,subscriber_id
,adjusted_event_start_time
into vespa_analysts.genre2
from vespa_analysts.genre
where trank=1 and cast(adjusted_event_start_time as date)='2012-01-16'
;
commit;


create hg index idx1 on vespa_analysts.genre2(subscriber_id);
create dttm index idx2 on vespa_analysts.genre2(adjusted_event_start_time);

commit;


update vespa_analysts.project047_capping_phase2_view_summary t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from vespa_analysts.project047_capping_phase2_view_summary as t1
left outer join vespa_analysts.genre2 t2
on t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
where cast(t1.adjusted_event_start_time as date)='2012-01-16'
;
commit;

---

-----RAN to HERE----


--select trank , count(*) from genre group by trank;
--select count(*) from vespa_analysts.project047_capping_phase2_view_summary;



--25940851

--add pack & network
update vespa_analysts.project047_capping_phase2_view_summary t1
set pack=t2.pack
   ,network=t2.network
from jchung.channel_lookup t2
where upper(trim(t1.channel_at_event_start_time))=upper(trim(t2.epg_channel));
commit;
--25925333

--add pack groups
update vespa_analysts.project047_capping_phase2_view_summary
set pack_grp=case when pack in ('Diginets','Terrestrial') then pack else 'Other' end
from vespa_analysts.project047_capping_phase2_view_summary;
commit;
--25940851

--add event duration bands
alter table vespa_analysts.project047_capping_phase2_view_summary
add dur_mins            int,
add dur_days            int,
add band_dur_days       smallint;

update vespa_analysts.project047_capping_phase2_view_summary
set dur_mins   = cast(x_event_duration/ 60    as int)
   ,dur_days   = cast(x_event_duration/ 86400 as int);
commit;
--25940851

--new column band_dur_days which is 0 for events limited to 1 day in duration, 1 otherwise.
--this is due to durations longer than 1 day
update vespa_analysts.project047_capping_phase2_view_summary
set band_dur_days  = case when dur_days = 0 then 0 else 1 end;
commit;
--25940851

--set target date
create variable @target_date date;
set @target_date = '2012-01-15';

--get all primary and secondary sub details for all accounts with viewing on any box
select distinct a.account_number
,b.service_instance_id
,b.subscription_sub_type
into --drop table
vespa_analysts.project047_capping_phase2_all_boxes_info
from vespa_analysts.project047_capping_phase2_view_summary as a
left outer join
sk_prod.cust_subs_hist as b
on a.account_number = b.account_number
where b.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
and b.status_code in ('AC','AB','PC')
and b.effective_from_dt<=@target_date
and b.effective_to_dt>@target_date;
commit;
--122569

--create index
create hg index idx1 on vespa_analysts.project047_capping_phase2_all_boxes_info(service_instance_id);

--select count(*),count(distinct account_number||service_instance_id),count(distinct account_number) from vespa_analysts.project047_capping_phase2_all_boxes_info
--122569  114583
--select count(*),count(distinct account_number) from vespa_analysts.project047_capping_phase2_view_summary
--25940851        114585

--create src_system_id lookup
select src_system_id
,cast(si_external_identifier as integer) as subscriberid
,si_service_instance_type
,effective_from_dt
,effective_to_dt
,cb_row_id
,rank() over(partition by src_system_id order by effective_from_dt desc,cb_row_id desc) as xrank
into --drop table
vespa_analysts.project047_capping_phase2_subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
--and effective_from_dt<=@target_date
--and effective_to_dt>@target_date;
commit;
--27724595
--create index
create hg index idx1 on vespa_analysts.project047_capping_phase2_subs_details(src_system_id);

--remove dups
delete from vespa_analysts.project047_capping_phase2_subs_details
where xrank>1;
--7723855

--select * from vespa_analysts.project047_capping_phase2_subs_details where subscriberid is null
delete from vespa_analysts.project047_capping_phase2_subs_details
where subscriberid is null;
--1477

--select count(*),count(distinct src_system_id||subscriberid),count(distinct src_system_id),count(distinct subscriberid) from vespa_analysts.project047_capping_phase2_subs_details
--19999263        19997650

--add sub id
alter table vespa_analysts.project047_capping_phase2_all_boxes_info
add subscriber_id integer default null;

update vespa_analysts.project047_capping_phase2_all_boxes_info t1
set subscriber_id=t2.subscriberid
from vespa_analysts.project047_capping_phase2_subs_details t2
where t1.service_instance_id=t2.src_system_id;
commit;
--121386

--check data
--select count(*),count(distinct service_instance_id),count(distinct subscriber_id),count(distinct service_instance_id||subscriber_id) from vespa_analysts.project047_capping_phase2_all_boxes_info
--122569  122569  121386  122569
--select count(*) from vespa_analysts.project047_capping_phase2_all_boxes_info where subscriber_id is null
--1183

--add primary/secondary flag to events
alter table vespa_analysts.project047_capping_phase2_view_summary
add src_system_id varchar(50),
add box_subscription varchar(1) default 'U';

update vespa_analysts.project047_capping_phase2_view_summary
set src_system_id=b.service_instance_id
   ,box_subscription=case when b.SUBSCRIPTION_SUB_TYPE='DTV Primary Viewing' then 'P'
                          when b.SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' then 'S'
                          else 'U'
                     end
from vespa_analysts.project047_capping_phase2_view_summary as a
left outer join
vespa_analysts.project047_capping_phase2_all_boxes_info as b
on a.subscriber_id=b.subscriber_id;
commit;
--25940851

--select distinct box_subscription from vespa_analysts.project047_capping_phase2_view_summary

--calculate ntiles for caps
select   band_dur_days
        ,live_or_playback
        ,cast(adjusted_event_start_time as date) as event_date
        ,event_start_day
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_at_event_start_time
        ,dur_mins
        ,ntile(200) over (partition by live_or_playback,event_start_day order by x_event_duration) as ntile_lp
        ,ntile(200) over (partition by live_or_playback,event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time order by x_event_duration) as ntile_1
        ,ntile(200) over (partition by live_or_playback,event_start_day,event_start_hour,pack_grp,genre_at_event_start_time order by x_event_duration) as ntile_2
        ,x_event_duration
        ,viewed_duration
        ,num_views
into --drop table
vespa_analysts.project047_capping_phase2_ntiles_week
from vespa_analysts.project047_capping_phase2_view_summary
where band_dur_days = 0;
--25928067

--create indexes
create hng index idx1 on vespa_analysts.project047_capping_phase2_ntiles_week(event_start_day);
create hng index idx2 on vespa_analysts.project047_capping_phase2_ntiles_week(event_start_hour);
create hng index idx3 on vespa_analysts.project047_capping_phase2_ntiles_week(live_or_playback);
create hng index idx4 on vespa_analysts.project047_capping_phase2_ntiles_week(box_subscription);
create hng index idx5 on vespa_analysts.project047_capping_phase2_ntiles_week(pack_grp);
create hng index idx6 on vespa_analysts.project047_capping_phase2_ntiles_week(genre_at_event_start_time);

--select distinct event_date,event_start_day from vespa_analysts.project047_capping_phase2_ntiles_week

--check data
--select count(*),sum(num_views) from vespa_analysts.project047_capping_phase2_ntiles_week
--count(*)        sum(vespa_analysts.project047_capping_phase2_ntiles_week.num_views)
--25928067        34274204

--select count(*),sum(num_views) from vespa_analysts.project047_capping_phase2_view_summary where band_dur_days = 0
--25928067        34274204

--create capping limits for start hours 4-19
SELECT live_or_playback
,event_date
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
,ntile_1
,min(dur_mins) as min_dur_mins
,max(dur_mins) as max_dur_mins
,PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY dur_mins) as median_dur_mins
,count(*) as num_events
,sum(num_views) as tot_views
,sum(x_event_duration) as event_duration
,sum(viewed_duration) as viewed_duration
into --drop table
vespa_analysts.project047_capping_phase2_nt_4_19
FROM vespa_analysts.project047_capping_phase2_ntiles_week
where event_start_hour>=4
and event_start_hour<=19
and live_or_playback='Live'
group by live_or_playback,event_date,event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time,ntile_1;
--561850

--create indexes
create hng index idx1 on vespa_analysts.project047_capping_phase2_nt_4_19(event_start_day);
create hng index idx2 on vespa_analysts.project047_capping_phase2_nt_4_19(event_start_hour);
create hng index idx3 on vespa_analysts.project047_capping_phase2_nt_4_19(live_or_playback);
create hng index idx4 on vespa_analysts.project047_capping_phase2_nt_4_19(box_subscription);
create hng index idx5 on vespa_analysts.project047_capping_phase2_nt_4_19(pack_grp);
create hng index idx6 on vespa_analysts.project047_capping_phase2_nt_4_19(genre_at_event_start_time);

--create capping limits start hours 20-3
SELECT live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time
,ntile_2
,min(dur_mins) as min_dur_mins
,max(dur_mins) as max_dur_mins
,PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY dur_mins) as median_dur_mins
,count(*) as num_events
,sum(num_views) as tot_views
,sum(x_event_duration) as event_duration
,sum(viewed_duration) as viewed_duration
into --drop table
vespa_analysts.project047_capping_phase2_nt_20_3
FROM vespa_analysts.project047_capping_phase2_ntiles_week
where event_start_hour in (20,21,22,23,0,1,2,3)
and live_or_playback='Live'
group by live_or_playback,event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time,ntile_2;
--232206

--create indexes
create hng index idx1 on vespa_analysts.project047_capping_phase2_nt_20_3(event_start_day);
create hng index idx2 on vespa_analysts.project047_capping_phase2_nt_20_3(event_start_hour);
create hng index idx3 on vespa_analysts.project047_capping_phase2_nt_20_3(live_or_playback);
create hng index idx4 on vespa_analysts.project047_capping_phase2_nt_20_3(pack_grp);
create hng index idx5 on vespa_analysts.project047_capping_phase2_nt_20_3(genre_at_event_start_time);

--create capping limits for playback
SELECT live_or_playback
,event_start_day
,ntile_lp
,min(dur_mins) as min_dur_mins
,max(dur_mins) as max_dur_mins
,PERCENTILE_disc(0.5) WITHIN GROUP (ORDER BY dur_mins) as median_dur_mins
,count(*) as num_events
,sum(num_views) as tot_views
,sum(x_event_duration) as event_duration
,sum(viewed_duration) as viewed_duration
into --drop table
vespa_analysts.project047_capping_phase2_nt_lp
FROM vespa_analysts.project047_capping_phase2_ntiles_week
where live_or_playback='Playback'
group by live_or_playback,event_start_day,ntile_lp;
--1400

--create indexes
create hng index idx1 on vespa_analysts.project047_capping_phase2_nt_lp(event_start_day);
create hng index idx2 on vespa_analysts.project047_capping_phase2_nt_lp(live_or_playback);

--identify caps for each variable dimension
select distinct live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
into --drop table
vespa_analysts.project047_capping_phase2_caps
from vespa_analysts.project047_capping_phase2_ntiles_week;
commit;
--16321

--create indexes
create hng index idx1 on vespa_analysts.project047_capping_phase2_caps(event_start_day);
create hng index idx2 on vespa_analysts.project047_capping_phase2_caps(event_start_hour);
create hng index idx3 on vespa_analysts.project047_capping_phase2_caps(live_or_playback);
create hng index idx4 on vespa_analysts.project047_capping_phase2_caps(box_subscription);
create hng index idx5 on vespa_analysts.project047_capping_phase2_caps(pack_grp);
create hng index idx6 on vespa_analysts.project047_capping_phase2_caps(genre_at_event_start_time);

--select count(distinct genre_at_event_start_time) from vespa_analysts.project047_capping_phase2_ntiles_week
--9

--select * from vespa_analysts.project047_capping_phase2_caps;

--add max duration to threshold table
alter table vespa_analysts.project047_capping_phase2_caps
add max_dur_mins integer;

--obtain max cap limits for live events

--identify ntile threshold for event start hours 23-3
select live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time
,min(ntile_2) as ntile
,ntile-5 as cap_ntile
into --drop table
vespa_analysts.project047_capping_phase2_h23_3
from vespa_analysts.project047_capping_phase2_nt_20_3
where event_start_hour in (23,0,1,2,3)
and median_dur_mins>=122
and live_or_playback='Live'
group by live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time;
commit;
--631

--add min duration
alter table vespa_analysts.project047_capping_phase2_h23_3
add min_dur_mins integer;

update vespa_analysts.project047_capping_phase2_h23_3 t1
set min_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_nt_20_3 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_2;
commit;
--628

--select count(*) from vespa_analysts.project047_capping_phase2_h23_3 where min_dur_mins is null;
--3

--obtain smallest threshold for each branch at pack level for event start hours 23-3
select live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,min(min_dur_mins) as pack_threshold
into --drop table
vespa_analysts.project047_capping_phase2_pack_23_3
from vespa_analysts.project047_capping_phase2_h23_3
group by live_or_playback
,event_start_day
,event_start_hour
,pack_grp;
--105

--identify ntile threshold for event start hours 4-14
select live_or_playback
,event_date
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
,min(ntile_1) as ntile
,case when event_start_hour in (4,5,10,11,12,13,14) then ntile-10
      when event_start_hour in (6,7,8,9) and datepart(weekday,event_date) in (1,7) then ntile-10
      when event_start_hour in (6,7,8,9) and datepart(weekday,event_date) in (2,3,4,5,6) then ntile-12
 end as cap_ntile
into --drop table
vespa_analysts.project047_capping_phase2_h4_14
from vespa_analysts.project047_capping_phase2_nt_4_19
where event_start_hour in (4,5,6,7,8,9,10,11,12,13,14)
and median_dur_mins>=243
and live_or_playback='Live'
group by live_or_playback
,event_date
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time;
commit;
--2325

--add min duration
alter table vespa_analysts.project047_capping_phase2_h4_14
add min_dur_mins integer;

update vespa_analysts.project047_capping_phase2_h4_14 t1
set min_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_nt_4_19 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_1;
commit;
--2132

--select count(*) from vespa_analysts.project047_capping_phase2_h4_14 where min_dur_mins is null;
--193

--obtain smallest threshold for each branch at pack level for event start hours 4-14
select live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,min(min_dur_mins) as pack_threshold
into --drop table
vespa_analysts.project047_capping_phase2_pack_4_14
from vespa_analysts.project047_capping_phase2_h4_14
group by live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp;
--621

--identify ntile threshold for event start hours 15-19
select live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
,max(ntile_1) as ntile
,ntile-10 as cap_ntile
into --drop table
vespa_analysts.project047_capping_phase2_h15_19
from vespa_analysts.project047_capping_phase2_nt_4_19
where event_start_hour in (15,16,17,18,19)
and live_or_playback='Live'
group by live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time;
commit;
--1800

--add min duration
alter table vespa_analysts.project047_capping_phase2_h15_19
add min_dur_mins integer;

update vespa_analysts.project047_capping_phase2_h15_19 t1
set min_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_nt_4_19 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_1;
commit;
--1502

--select count(*) from vespa_analysts.project047_capping_phase2_h15_19 where min_dur_mins is null;
--298

--obtain smallest threshold for each branch at pack level for event start hours 15-19
select live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,min(min_dur_mins) as pack_threshold
into --drop table
vespa_analysts.project047_capping_phase2_pack_15_19
from vespa_analysts.project047_capping_phase2_h15_19
group by live_or_playback
,event_start_day
,event_start_hour
,box_subscription
,pack_grp;
--315

--identify ntile threshold for event start hours 20-22
select live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time
,min(ntile_2) as ntile
,ntile-5 as cap_ntile
into --drop table
vespa_analysts.project047_capping_phase2_h20_22
from vespa_analysts.project047_capping_phase2_nt_20_3
where event_start_hour in (20,21,22)
and median_dur_mins>=((23-event_start_hour-1)*60)+122
and live_or_playback='Live'
group by live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time;
commit;
--317

--add min duration
alter table vespa_analysts.project047_capping_phase2_h20_22
add min_dur_mins integer;

update vespa_analysts.project047_capping_phase2_h20_22 t1
set min_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_nt_20_3 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_2;
commit;
--317

--select count(*) from vespa_analysts.project047_capping_phase2_h20_22 where min_dur_mins is null;
--0

--obtain smallest threshold for each branch at pack level for event start hours 20-22
select live_or_playback
,event_start_day
,event_start_hour
,pack_grp
,min(min_dur_mins) as pack_threshold
into --drop table
vespa_analysts.project047_capping_phase2_pack_20_22
from vespa_analysts.project047_capping_phase2_h20_22
group by live_or_playback
,event_start_day
,event_start_hour
,pack_grp;
--63

--update threshold table with cap limits
update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_h23_3 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--1590

--select count(*) from vespa_analysts.project047_capping_phase2_h23_3
--631

update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_h4_14 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--2325

--select count(*) from vespa_analysts.project047_capping_phase2_h4_14
--2325

update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_h15_19 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--1800

--select count(*) from vespa_analysts.project047_capping_phase2_h15_19
--1800

update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_h20_22 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--923

--select count(*) from vespa_analysts.project047_capping_phase2_h20_22
--317

--select count(*) from vespa_analysts.project047_capping_phase2_caps where max_dur_mins is null and live_or_playback='Live'
--2192

--populate cells where ntiles are null with pack branch thresholds
update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.pack_threshold
from vespa_analysts.project047_capping_phase2_pack_23_3 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.max_dur_mins is null;
--88

--select count(*) from vespa_analysts.project047_capping_phase2_pack_23_3
--105

update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.pack_threshold
from vespa_analysts.project047_capping_phase2_pack_20_22 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.max_dur_mins is null;
--78

--select count(*) from vespa_analysts.project047_capping_phase2_pack_20_22
--63

update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.pack_threshold
from vespa_analysts.project047_capping_phase2_pack_4_14 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.max_dur_mins is null;
--1504

--select count(*) from vespa_analysts.project047_capping_phase2_pack_4_14
--621

update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.pack_threshold
from vespa_analysts.project047_capping_phase2_pack_15_19 t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.max_dur_mins is null;
--298

--select count(*) from vespa_analysts.project047_capping_phase2_pack_15_19
--315

--identify ntile threshold for playback events
select live_or_playback
,event_start_day
,max(ntile_lp) as ntile
,ntile-2 as cap_ntile
into --drop table
vespa_analysts.project047_capping_phase2_lp
FROM vespa_analysts.project047_capping_phase2_nt_lp
where live_or_playback='Playback'
group by live_or_playback,event_start_day;
--7

--add min duration
alter table vespa_analysts.project047_capping_phase2_lp
add min_dur_mins integer;

update vespa_analysts.project047_capping_phase2_lp t1
set min_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_nt_lp t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.cap_ntile=t2.ntile_lp;
commit;
--7

--select * from vespa_analysts.project047_capping_phase2_lp;

--update playback limits in caps table
update vespa_analysts.project047_capping_phase2_caps t1
set max_dur_mins=t2.min_dur_mins
from vespa_analysts.project047_capping_phase2_lp t2
where t1.live_or_playback=t2.live_or_playback
and t1.event_start_day=t2.event_start_day
and t1.max_dur_mins is null;
--7985

--select count(*) from vespa_analysts.project047_capping_phase2_caps where live_or_playback='Playback'
--7985

--reset capping limits that are less than 40 mins
update vespa_analysts.project047_capping_phase2_caps
set max_dur_mins=40
where (max_dur_mins is null
or max_dur_mins<40)
and live_or_playback='Live';
--4408

grant all on vespa_analysts.project047_capping_phase2_caps to public;

-----Drop Interim Tables------

drop table vespa_analysts.project047_capping_phase2_data;
drop table vespa_analysts.project047_capping_phase2_data2;
drop table vespa_analysts.project047_capping_phase2_data3;
drop table vespa_analysts.project047_capping_phase2_data4;
drop table vespa_analysts.project047_capping_phase2_view_summary;
drop table vespa_analysts.genre;
drop table vespa_analysts.genre2;
drop table vespa_analysts.project047_capping_phase2_subs_details;
drop table vespa_analysts.project047_capping_phase2_all_boxes_info;
drop table vespa_analysts.project047_capping_phase2_ntiles_week;
drop table vespa_analysts.project047_capping_phase2_nt_4_19;
drop table vespa_analysts.project047_capping_phase2_nt_20_3;
drop table vespa_analysts.project047_capping_phase2_nt_lp;
drop table vespa_analysts.project047_capping_phase2_h23_3;
drop table vespa_analysts.project047_capping_phase2_pack_23_3;
drop table vespa_analysts.project047_capping_phase2_h4_14;
drop table vespa_analysts.project047_capping_phase2_pack_4_14;
drop table vespa_analysts.project047_capping_phase2_h15_19;
drop table vespa_analysts.project047_capping_phase2_pack_15_19;
drop table vespa_analysts.project047_capping_phase2_h20_22;
drop table vespa_analysts.project047_capping_phase2_pack_20_22;
commit;
--select count(*) from vespa_analysts.project047_capping_phase2_caps where max_dur_mins is null
--0

--select * from vespa_analysts.project047_capping_phase2_caps 
commit;
--select count(*) from vespa_analysts.project047_capping_phase2_caps;
--16321

--select count(*),sum(num_views) from vespa_analysts.project047_capping_phase2_ntiles_week
--25928067        34274204

--select * from vespa_analysts.project047_capping_phase2_caps;
