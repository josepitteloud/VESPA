/* Creating a table of standby / Idle events taking from Netezza and importing this unto Olive
Data Period : 23rd to 29th of Sept 2013
Author : Patrick Igonor
Lead   : Claudio Lima
Date : 30-10-2013
*/

  drop table Standby_Idle_Events_Sep23_29_2013_data;
  create table Standby_Idle_Events_Sep23_29_2013_data(
                                                    DTH_VIEWING_EVENT_ID bigint
                                                   ,SCMS_SUBSCRIBER_ID Integer
                                                   ,EVENT_START_DATETIME varchar (30)
                                                   ,EVENT_END_DATETIME varchar (30)
                                                   ,EVENT_DURATION_SECONDS integer
                                                   ,EVENT_ACTION varchar (15)
                                                   );


create variable @sql varchar(10000);
create variable @counter tinyint;
set @counter =1;

while @counter <= 20
begin
     set @sql = '
     load table Standby_Idle_Events_Sep23_29_2013_data(
                                                    DTH_VIEWING_EVENT_ID '',''
                                                   ,SCMS_SUBSCRIBER_ID '',''
                                                   ,EVENT_START_DATETIME '',''
                                                   ,EVENT_END_DATETIME '',''
                                                   ,EVENT_DURATION_SECONDS '',''
                                                   ,EVENT_ACTION ''\n''
     )
     from ''/ETL013/prod/sky/olive/data/share/clarityq/export/PatrickI/Standby_Events/Standby_' || @counter || '.csv''
     QUOTES OFF
     ESCAPES OFF
     NOTIFY 1000
     SKIP 1'

     execute (@sql)


     set @counter = @counter + 1

     commit
end
;
--Checks ----
select count(*) from Standby_Idle_Events_Sep23_29_2013_data
-- count()
--8,928,691

--Putting the date into the right format (having it in the same format as what we have in Viewing events table)

select  DTH_VIEWING_EVENT_ID
       ,SCMS_SUBSCRIBER_ID
       ,EVENT_START_DATETIME
       ,cast(left(EVENT_START_DATETIME,10) || ' '|| right(EVENT_START_DATETIME,16) as timestamp) as Event_Start_Date_Time
       ,EVENT_END_DATETIME
       ,cast(left(EVENT_END_DATETIME,10) || ' '|| right(EVENT_END_DATETIME,16) as timestamp) as Event_End_Date_Time
       ,EVENT_DURATION_SECONDS
       ,EVENT_ACTION
into Standby_Idle_Events_Sep23_29_2013_data_Format
from Standby_Idle_Events_Sep23_29_2013_data
--8,928,691 Row(s) affected

--Dropping some fields that are no longer necessary and renaming the subscriber ID field --
alter table Standby_Idle_Events_Sep23_29_2013_data_Format;
drop EVENT_END_DATETIME;

alter table Standby_Idle_Events_Sep23_29_2013_data_Format;
drop EVENT_END_DATETIME;

alter table Standby_Idle_Events_Sep23_29_2013_data_Format;
rename SCMS_SUBSCRIBER_ID to Subscriber_ID;

select EVENT_ACTION, count(*) from Standby_Idle_Events_Sep23_29_2013_data_Format
group by EVENT_ACTION
order by EVENT_ACTION

--EVENT_ACTION    count()
Idle             1,663,685
Standby In       7,265,006

--Deduping the above table before matching to the Viewing Events table-----
select  DTH_VIEWING_EVENT_ID
       ,Subscriber_ID
       ,Event_Start_Date_Time
       ,Event_End_Date_Time
       ,EVENT_DURATION_SECONDS
       ,EVENT_ACTION
       ,row_number () over (partition by Subscriber_ID,Event_Start_Date_Time order by EVENT_ACTION desc) as Row_Order
into Standby_Idle_Events_Sep23_29_2013_data_Format_Dedup
from Standby_Idle_Events_Sep23_29_2013_data_Format
--8,928,691 Row(s) affected

--Checks--
select count(*) from Standby_Idle_Events_Sep23_29_2013_data_Format_Dedup
where Row_Order =1
--8,928,411 Row(s) affected

--Doing a few checks to make sure these duplicates are taken care of--(This is exactly what I am expecting to get - If Standy & Idle, pick Standby and if both standby, just pick one)
select * from Standby_Idle_Events_Sep23_29_2013_data_Format_Dedup
where Subscriber_ID in (77103,566474)

--------------------------------------------------------------------------------
---Pulling out a week worth of data for our Standby analysis
  'Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events', which will be updated later with Standby / Idle non viewing events)---

select  rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,BROADCAST_END_DATE_TIME_UTC
       ,duration
       ,channel_name
       ,genre_description
       ,sub_genre_description
       ,type_of_viewing_event
       ,live_recorded
       ,min(BROADCAST_START_DATE_TIME_UTC) over (partition by Subscriber_Id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC) as BROADCAST_START_DATE_TIME_UTC_min
       ,case
            when live_recorded = 'RECORDED' and service_key in (4094,4095,4096,4097,4098) then 4
            when live_recorded = 'LIVE' then 0
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) = date(BROADCAST_START_DATE_TIME_UTC_min)
            then case
                    when cast(datediff(second,BROADCAST_START_DATE_TIME_UTC_min,EVENT_START_DATE_TIME_UTC)/3600.0 as int) = 0
                    then 1
                    else 2 end
            when live_recorded = 'RECORDED' and date(EVENT_START_DATE_TIME_UTC) <> date(BROADCAST_START_DATE_TIME_UTC_min) then 3
        end as live_timeshifted_events
       ,account_number
       ,playback_speed
       ,playback_type
       ,reported_playback_speed
       ,service_key
       ,capped_full_flag
       ,capped_partial_flag
       ,instance_start_date_time_utc
       ,instance_end_date_time_utc
       ,capping_end_date_time_utc
into Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events
from sk_prod.vespa_dp_prog_VIEWED_201309
where live_recorded in ('LIVE','RECORDED')
and panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2013-09-23 00:00:00'
and EVENT_START_DATE_TIME_UTC   <  '2013-09-30 00:00:00'
and subscriber_id is not null
and account_number is not null
and duration > 6
--186,385,766 Row(s) affected

--Deleting the duplicates before matching
Delete from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events
where Program_Order > 1
--50,815,033 Row(s) affected

--Checks
select count(*) from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events
--count() - 135,570,733

--Lets create index to speed up our joins ---
create hg index idx1 on Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events(subscriber_id,EVENT_END_DATE_TIME_UTC);
create hg index idx2 on Standby_Idle_Events_Sep23_29_2013_data_Format_dedup(Subscriber_ID,Event_Start_Date_Time);

---Joining the Standby events to the viewing data from the 23rd to the 29th of September
select  VE.subscriber_id
       ,VE.EVENT_START_DATE_TIME_UTC
       ,VE.EVENT_END_DATE_TIME_UTC
       ,VE.BROADCAST_START_DATE_TIME_UTC
       ,VE.BROADCAST_END_DATE_TIME_UTC
       ,SB.Event_Start_Date_Time
       ,SB.Event_End_Date_Time
       ,VE.duration
       ,VE.channel_name
       ,VE.genre_description
       ,VE.sub_genre_description
       ,VE.type_of_viewing_event
       ,VE.live_recorded
       ,VE.live_timeshifted_events
       ,VE.account_number
       ,VE.playback_speed
       ,VE.playback_type
       ,VE.reported_playback_speed
       ,VE.service_key
       ,SB.DTH_VIEWING_EVENT_ID
       ,SB.EVENT_ACTION
       ,SB.EVENT_DURATION_SECONDS
       ,case when SB.EVENT_ACTION like '%Standby In%'
              and
              ((VE.duration between 120*60 and 123*60+59) or VE.duration >= 239*60)  then 1 else 0 end as 'Standby_Removal'
 into  Viewing_Events_23Sept_29Sept_Standby_Events
 from Vespa_dp_prog_VIEWED_23Sept_29Sept_Viewing_Events VE
 left join Standby_Idle_Events_Sep23_29_2013_data_Format_dedup SB
   on VE.subscriber_id = SB.Subscriber_ID
  and VE.EVENT_END_DATE_TIME_UTC = SB.Event_Start_Date_Time
  and Row_Order = 1
--135,570,733 Row(s) affected

--Checks --
select Standby_Removal, count(*) as Num from Viewing_Events_23Sept_29Sept_Standby_Events group by Standby_Removal
select top 100 duration, Standby_Removal from Viewing_Events_23Sept_29Sept_Standby_Events
where Standby_Removal = 0

--Updating the above table with Box Subscription and Pack Group

alter table Viewing_Events_23Sept_29Sept_Standby_Events add box_subscription varchar (2)   default 'U';
alter table Viewing_Events_23Sept_29Sept_Standby_Events add pack_grp         varchar (255) default 'Unknown';

-- Update box_subscription
update Viewing_Events_23Sept_29Sept_Standby_Events
set box_subscription =
        case
            when csh.subscription_sub_type = 'DTV Primary Viewing' then 'P'
            when csh.subscription_sub_type = 'DTV Extra Subscription' then 'S'
        end
        from Viewing_Events_23Sept_29Sept_Standby_Events as VE
    inner join sk_prod.cust_subs_hist as csh
    on VE.account_number = csh.account_number
    where csh.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription')
    and csh.status_code in ('AC','AB','PC')
    and csh.effective_from_dt <='2013-09-23 00:00:00'
    and csh.effective_to_dt    >'2013-09-29 23:59:59'

-- Update Pack group
update Viewing_Events_23Sept_29Sept_Standby_Events
set VE.pack_grp = trim(CM.channel_pack)
from Viewing_Events_23Sept_29Sept_Standby_Events as VE
inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as CM
on VE.service_key = CM.service_key
select top 10* from  vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
--Checks
select Standby_Removal, count(*) from Viewing_Events_23Sept_29Sept_Standby_Events
group by Standby_Removal

--Standby_Removal    count()
        0          134,242,633
        1            1,328,100

--Distribution of event duration by Standby / Idle events
select EVENT_ACTION
      ,duration/60 as Dur_min
      ,count(*)as Number_Events
from Viewing_Events_23Sept_29Sept_Standby_Events
where EVENT_ACTION is not null
group by EVENT_ACTION, duration

--Distribution of duration of non-viewing event  by Standby / Idle events
select EVENT_ACTION
      ,EVENT_DURATION_SECONDS/60 as Dur_min
      ,count(*)as Number_Events
from Standby_Idle_Events_Sep23_29_2013_data_Format
group by EVENT_ACTION, EVENT_DURATION_SECONDS


--Distribution of Event Action based on Start Hour
Select  hour(Event_Start_Date_Time) as Event_Start_Hour
       ,sum(case when EVENT_ACTION like '%Standby In%' then 1 else 0 end) as 'Standby'
       ,sum(case when EVENT_ACTION like '%Idle%' then 1 else 0 end) as 'Idle'
       ,count(*) as Number_of_Events
from    Standby_Idle_Events_Sep23_29_2013_data_Format
group by Event_Start_Hour
order by Event_Start_Hour


--Calculations of ntiles -------------------------------------------------*********************************************************

--Calculate ntiles for all viewing events including standby In events
select   live_timeshifted_events
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   ( select duration
        ,live_timeshifted_events
        ,ntile(200) over (partition by live_timeshifted_events order by duration) as ntiles
        from Viewing_Events_23Sept_29Sept_Standby_Events
        ) VEA_VA
group by  live_timeshifted_events, ntiles
order by  live_timeshifted_events, ntiles
;

--Calculate ntiles for all viewing events excluding standby In events

select   live_timeshifted_events
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   (  select duration
        ,live_timeshifted_events
        ,ntile(200) over (partition by live_timeshifted_events order by duration) as ntiles
        from Viewing_Events_23Sept_29Sept_Standby_Events
        where  Standby_Removal = 0
        ) VEA_VA
group by  live_timeshifted_events, ntiles
order by  live_timeshifted_events, ntiles
;

--Calculate ntiles for all viewing based on live, DOW, Start Hour including standby In events

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   (  select duration
        ,live_timeshifted_events
        ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
        ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,case
                when datepart(weekday,EVENT_START_DATE)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE)=7 then 'Sat'
        end as EVENT_START_DOW
        ,hour(EVENT_START_TIME) as EVENT_START_HOUR
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Hour order by duration) as ntiles
        from Viewing_Events_23Sept_29Sept_Standby_Events
        ) VEA_VA
group by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour, ntiles
order by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour, ntiles
;

--Calculate ntiles for all viewing based on live, DOW, Start Hour excluding standby In events

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
from   (  select duration
        ,live_timeshifted_events
        ,case
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE_TIME_UTC)=7 then 'Sat'
         end as EVENT_START_DOW
        ,hour(EVENT_START_DATE_TIME_UTC) as EVENT_START_HOUR
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Hour order by duration) as ntiles
         from Viewing_Events_23Sept_29Sept_Standby_Events
         where Standby_Removal = 0
        ) VEA_VA
group by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour, ntiles
order by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour, ntiles

--Calculate ntiles for all viewing based on live, DOW, Start Hour,Pack Group, Genre and Box_Subscription including standby In events

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
into    Segmentation_ntile_All_Fields
from   (  select duration
        ,live_timeshifted_events
        ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
        ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,case
                when datepart(weekday,EVENT_START_DATE)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE)=7 then 'Sat'
        end as EVENT_START_DOW
        ,hour(EVENT_START_TIME) as EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Hour,pack_grp,genre_description,box_subscription order by duration) as ntiles
        from Viewing_Events_23Sept_29Sept_Standby_Events
        ) VEA_VA
group by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour,pack_grp,genre_description,box_subscription,ntiles
order by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour,pack_grp,genre_description,box_subscription,ntiles
--5,764,698 Row(s) affected
;

--Calculate ntiles for all viewing based on live, DOW, Start Hour,Pack Group, Genre and Box_Subscription excluding standby In events

select   live_timeshifted_events
        ,Event_Start_DOW
        ,Event_Start_Hour
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,ntiles
        ,median(duration) /60.0 as Median_Duration
into     Segmentation_ntile_All_Fields_Excl_Standby
from   (  select duration
        ,live_timeshifted_events
        ,dateformat(EVENT_START_DATE_TIME_UTC,'YYYY-MM-DD') as EVENT_START_DATE
        ,dateformat(EVENT_START_DATE_TIME_UTC,'HH:MM:SS') as EVENT_START_TIME
       ,case
                when datepart(weekday,EVENT_START_DATE)=1 then 'Sun'
                when datepart(weekday,EVENT_START_DATE)=2 then 'Mon'
                when datepart(weekday,EVENT_START_DATE)=3 then 'Tue'
                when datepart(weekday,EVENT_START_DATE)=4 then 'Wed'
                when datepart(weekday,EVENT_START_DATE)=5 then 'Thu'
                when datepart(weekday,EVENT_START_DATE)=6 then 'Fri'
                when datepart(weekday,EVENT_START_DATE)=7 then 'Sat'
        end as EVENT_START_DOW
        ,hour(EVENT_START_TIME) as EVENT_START_HOUR
        ,pack_grp
        ,genre_description
        ,box_subscription
        ,ntile(200) over (partition by live_timeshifted_events, Event_Start_DOW, Event_Start_Hour,pack_grp,genre_description,box_subscription order by duration) as ntiles
        from Viewing_Events_23Sept_29Sept_Standby_Events
        where Standby_Removal = 0
        ) VEA_VA
group by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour,pack_grp,genre_description,box_subscription,ntiles
order by  live_timeshifted_events, Event_Start_DOW, Event_Start_Hour,pack_grp,genre_description,box_subscription,ntiles
;
--5,755,605 Row(s) affected
select * from Segmentation_ntile_All_Fields
where live_timeshifted_events = 0
and Event_Start_Hour between 18 and 19

select * from Segmentation_ntile_All_Fields_Excl_Standby
where live_timeshifted_events = 0
and Event_Start_Hour between 18 and 19


------------Granting Access
grant all on Standby_Idle_Events_Sep23_29_2013_data_Format to limac;
grant all on Standby_Idle_Events_Sep23_29_2013_data_Format_Dedup to limac;
grant all on Viewing_Events_23Sept_29Sept_Standby_Events to limac;
grant all on Segmentation_ntile_All_Fields to limac;
grant all on Segmentation_ntile_All_Fields_Excl_Standby to limac;
commit;
