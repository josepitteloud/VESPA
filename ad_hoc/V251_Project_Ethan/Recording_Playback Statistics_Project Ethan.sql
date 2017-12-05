--Project : Recording Live / Playback Statistics
--Lead : Jason Thompson
--Analyst : Patrick Igonor


--Identifying the tables of interest ---

select top 10* from sk_prod.cust_subs_hist -- This will help to identify Multi-room households and single box households
select top 10* from sk_prod.vespa_dp_prog_VIEWED_current -- Information about live, recorded, viewing hours etc
select top 1000* from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS -- This gives information on the number of downloads. PDL stands for Progressive Downloads

--Starting Now.-----Lets bring in all TV viewers for starters.....
select account_number
 into igonorp.All_TV_Viewers
  from sk_prod.cust_subs_hist
 where subscription_sub_type ='DTV Primary Viewing'  --the DTV sub Type
   and status_code in ('AC','AB','PC')               --Active Status Codes
   and effective_from_dt <= '2014-01-12' and effective_to_dt >= '2014-01-06'
   and currency_code = 'GBP'
group by account_number
--count() 9,457,246

--Getting Single box and Multi-box Households
select  account_number
       ,max (case when subscription_sub_type ='DTV Extra Subscription' then 1 else 0 end) as 'Max_Multiroom'
into    Temp_cust_subs_hist
 from sk_prod.cust_subs_hist
where effective_from_dt  <= '2014-01-12' and effective_to_dt >= '2014-01-06'
  and status_code in ('AC','AB','PC')
  and currency_code = 'GBP'
group by account_number
--10,405,502 Row(s) affected

--I want to add Multiroom and single box
Alter table All_TV_Viewers add Multi_Room tinyint;

--Updating the above table --
update All_TV_Viewers
set Multi_Room = Max_Multiroom
        from Temp_cust_subs_hist as cust
    inner join All_TV_Viewers as ATV
    on cust.account_number = ATV.account_number
--9,457,246 Row(s) affected

--Getting the number of downloads per account
select account_number
      ,sum(download_size_kb) Download_Size
      ,date(last_modified_dt)as Modified_date
      ,count(*) as Downloads
into   Ondemand_downloads
from  sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
where last_modified_dt   >= '2014-01-06 00:00:00'
  and last_modified_dt   <= '2014-01-12 23:59:59'
  and country_code = 'GBR'
group by account_number,Modified_date
--5,248,635 Row(s) affected

select ATV.account_number
      ,ATV.Multi_Room
      ,OD.Download_Size
      ,OD.Modified_date
      ,OD.Downloads
into  All_TV_Viewers_Ondemand_Downloads
from All_TV_Viewers ATV
left join Ondemand_downloads OD
on ATV.account_number = OD.account_number
--12,435,961 Row(s) affected

--Putting the number of downloads into intervals ---

select Modified_date
      ,Downloads
      ,Multi_Room
      ,case when Downloads between 0 and 100   then '0 - 500'
            when Downloads between 101 and 200 then '500 - 1000'
            when Downloads between 201 and 300 then '201 - 300'
            when Downloads between 301 and 400 then '301 - 400'
            when Downloads between 401 and 500 then '401 - 500'
            when Downloads between 501 and 600 then '501 - 600'
            when Downloads between 601 and 700 then '601 - 700'
            when Downloads between 701 and 800 then '701 - 800'
            when Downloads between 801 and 900 then '801 - 900'
            when Downloads between 901 and 1000 then '901 - 1000'
            when Downloads between 1001 and 1100 then '1001 - 1100'
            when Downloads between 1101 and 1200 then '1101 - 1200'
            when Downloads between 1201 and 1300 then '1201 - 1300'
            when Downloads between 1301 and 1400 then '1301 - 1400'
            when Downloads between 1401 and 1500 then '1401 - 1500'
            when Downloads between 1501 and 1600 then '1501 - 1600'
            when Downloads between 1601 and 1700 then '1601 - 1700'
            when Downloads between 1701 and 1800 then '1701 - 1800'
            when Downloads between 1801 and 1900 then '1801- 1900'
            when Downloads between 1901 and 2000 then '1901 - 2000'
            when Downloads between 2001 and 2100 then '2001 - 2100'
            when Downloads between 2101 and 2200 then '2101 - 2200'
            when Downloads between 2201 and 2300 then '2201 - 2300'
            when Downloads >= 2301          then '2301+'
       else null end as Downloads_Interval
into Ondemand_Downloads_Intervals
from All_TV_Viewers_Ondemand_Downloads

--Lets pull out the viewing data from the viewing events table (Table dropped to save space)
select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,account_number
       ,subscriber_id
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,INSTANCE_START_DATE_TIME_UTC
       ,INSTANCE_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,BROADCAST_END_DATE_TIME_UTC
       ,capping_end_date_time_utc
       ,capped_full_flag
       ,capped_partial_flag
       ,Duration
       , case when capped_full_flag = 1 then 0
                                         when capped_partial_flag = 1 then datediff(second, instance_start_date_time_utc, coalesce(capping_end_date_time_utc,instance_end_date_time_utc))
                                   else datediff(second, instance_start_date_time_utc, instance_end_date_time_utc)
                                 end as Capped_Duration
       ,live_recorded
into  drop table VEA_06_12_Dec2014_Viewing_Events
from sk_prod.vespa_dp_prog_VIEWED_current
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2014-01-06 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2014-01-12 23:59:59'
--195,664,457 Row(s) affected

--Report this to Tony Kinnaird
select EVENT_START_DATE_TIME_UTC, EVENT_END_DATE_TIME_UTC from sk_prod.vespa_dp_prog_VIEWED_current where EVENT_START_DATE_TIME_UTC > EVENT_END_DATE_TIME_UTC ---14 occurencies
and panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2014-01-06 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2014-01-12 23:59:59'


--Getting sum of duration across account_number, subscriber_id, live_recorded and day
select  account_number
       ,subscriber_id
       ,live_recorded
       ,date(EVENT_START_DATE_TIME_UTC) as Day
       ,sum(Duration)as duration
into Account_Subscriber_duration
from VEA_06_12_Dec2014_Viewing_Events
where Program_Order = 1 and EVENT_START_DATE_TIME_UTC <= EVENT_END_DATE_TIME_UTC
group by account_number
        ,subscriber_id
        ,live_recorded
        ,Day
--6,736,150 Row(s) affected

--Joining the above table Account_Subscriber_duration to All_TV_Viewers table
select ATV.account_number
      ,ATV.Multi_Room
      ,AD.subscriber_id
      ,AD.live_recorded
      ,AD.Day
      ,AD.duration
into   All_TV_Viewers_Account_Sub_Duration
from All_TV_Viewers ATV
left join Account_Subscriber_duration AD
on  ATV.account_number = AD.account_number

;
--Bringing in the PS flag from the Single_box_View table
select ATV.account_number
      ,ATV.subscriber_id
      ,ATV.Multi_Room
      ,ATV.live_recorded
      ,ATV.Day
      ,ATV.duration
      ,NV.PS_flag
into  All_TV_Viewers_Account_Sub_Duration_PS
from All_TV_Viewers_Account_Sub_Duration ATV
inner join Vespa_analysts.vespa_single_box_view NV
on  ATV.subscriber_id = NV.subscriber_id
--6,720,989 Row(s) affected
;
--(dropped) Just to create space---
--Getting the duration into minutes
select account_number
      ,subscriber_id
      ,Multi_Room
      ,live_recorded
      ,duration
      ,day
      ,duration / 60 as dur_Mins
into drop table All_TV_Viewers_Account_Sub_Duration_PS_Mins
from All_TV_Viewers_Account_Sub_Duration_PS
--6,720,989 Row(s) affected
;


;

Alter table All_TV_Viewers_Duration add Weights bigint;

--Updating the above table --
update All_TV_Viewers_Duration
set ATV.Weights = SC.scaling_weighting
        from  vespa_analysts.vespa_household_weighting SC
    inner join All_TV_Viewers_Duration as ATV
    on SC.account_number = ATV.account_number
    where SC.scaling_date = '2014-01-09'
--6,478,993 Row(s) affected
;

--Pivoting ---Do this again Patrick

select Day, Dur_Intervals_Mins, sum(Weights)as Number_HH
from All_TV_Viewers_Duration
where live_recorded = 'LIVE' and Multi_Room = 0
group by Day, Dur_Intervals_Mins
order by Number_HH

select Day, Dur_Intervals_Mins, sum(Weights)as Number_HH
from All_TV_Viewers_Duration
where live_recorded = 'RECORDED' and Multi_Room = 0
group by Day, Dur_Intervals_Mins
order by Number_HH


select Modified_date, Downloads_Interval, count(*) as Number_HH
from Ondemand_Downloads_Intervals
group by Modified_date, Downloads_Interval
order by Number_HH


select Day, Dur_Intervals_Mins,sum(Weights)as Number_HH
from All_TV_Viewers_Duration
where live_recorded = 'LIVE' and Multi_Room = 1
group by Day, Dur_Intervals_Mins
order by Number_HH

select Day, Dur_Intervals_Mins, sum(Weights)as Number_HH
from All_TV_Viewers_Duration
where live_recorded = 'RECORDED' and Multi_Room = 1
group by Day, Dur_Intervals_Mins
order by Number_HH


--Checks

select Modified_date, sum(Downloads) as Sum_Downloads
from All_TV_Viewers_Ondemand_Downloads
where Multi_Room = 0
group by Modified_date
order by Modified_date


--Granting Access
grant all on All_TV_Viewers_Ondemand_Downloads to thompsonja;
grant all on Ondemand_Downloads_Intervals to thompsonja;
grant all on All_TV_Viewers_Duration to thompsonja;


select Downloads, count(*) from Ondemand_Downloads
group by Downloads

select top 10* from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS

--The concurrency Questions --
--Table of interest
select top 10* from All_TV_Viewers_Ondemand_Downloads
select top 10* from sk_prod.vespa_dp_prog_VIEWED_current

select top 10 account_number,hour(last_modified_dt)
from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS

--Getting Single box and Multi-box Households for one day!!!
select  account_number
       ,max (case when subscription_sub_type ='DTV Extra Subscription' then 1 else 0 end) as 'Max_Multiroom'
into    Temp_cust_subs_hist_09_Jan
 from sk_prod.cust_subs_hist
where effective_from_dt  <= '2014-01-09' and effective_to_dt >= '2014-01-09'
  and status_code in ('AC','AB','PC')
  and currency_code = 'GBP'
group by account_number
--10,382,698 Row(s) affected

--Viewing events for one day - 09-Jan-2014

select rank() over ( partition by subscriber_id
                                 ,EVENT_START_DATE_TIME_UTC
                                 ,EVENT_END_DATE_TIME_UTC
                                 ,Duration
                         order by BROADCAST_START_DATE_TIME_UTC)
        as Program_Order
       ,account_number
       ,subscriber_id
       ,service_key
       ,pk_viewing_prog_instance_fact
       ,EVENT_START_DATE_TIME_UTC
       ,EVENT_END_DATE_TIME_UTC
       ,INSTANCE_START_DATE_TIME_UTC
       ,INSTANCE_END_DATE_TIME_UTC
       ,BROADCAST_START_DATE_TIME_UTC
       ,BROADCAST_END_DATE_TIME_UTC
       ,Duration
       ,live_recorded
into  VEA_09_Jan_2014_Viewing_Events
from sk_prod.vespa_dp_prog_VIEWED_current
where panel_id = 12
and EVENT_START_DATE_TIME_UTC   >= '2014-01-09 00:00:00'
and EVENT_START_DATE_TIME_UTC   <= '2014-01-09 23:59:59'
;
--Testing the uniqueness of the fields
select top 10* from VEA_09_Jan_2014_Viewing_Events
where account_number = '620052360307'

pk_viewing_prog_instance_fact

--25,989,550 Row(s) affected
--Removing the duplicates --
delete from VEA_09_Jan_2014_Viewing_Events where Program_Order > 1
--7,555,427 Row(s) affected

select count(*) from VEA_09_Jan_2014_Viewing_Events
--count() 18,434,123

--On demand for one day - 09-Jan 2014
select account_number
      ,cb_seq_id
      ,last_modified_dt as Download_start_date_time
      ,x_time_taken_secs
      ,Dateadd(second,x_time_taken_secs,last_modified_dt) as Download_end_date_time
into   Ondemand_09_Jan_2014
from  sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
where last_modified_dt   >= '2014-01-09 00:00:00'
  and last_modified_dt   <= '2014-01-09 23:59:59'
--1,870,489 Row(s) affected

---Dealing with all possible overlaps
--Overlap I

--Conditions - case when  ME > EE and ES < MS then start_time = MS and end time = EE

select ve.account_number
      ,ve.subscriber_id
      ,ve.EVENT_START_DATE_TIME_UTC
      ,ve.EVENT_END_DATE_TIME_UTC
      ,ve.pk_viewing_prog_instance_fact
      ,od.cb_seq_id
      ,od.Download_start_date_time
      ,od.Download_end_date_time
      ,case when Download_end_date_time >= EVENT_END_DATE_TIME_UTC
       and EVENT_START_DATE_TIME_UTC <= Download_start_date_time
       and EVENT_END_DATE_TIME_UTC >= Download_start_date_time then 1 else 0 end as Concurrency
into   First_Overlap
from VEA_09_Jan_2014_Viewing_Events ve
inner join Ondemand_09_Jan_2014 od
on ve.account_number = od.account_number
and Concurrency = 1
--48,970 Row(s) affected
-- select top 5 Download_start_date_time,Download_end_date_time,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC from First_Overlap
-- where Download_end_date_time <= EVENT_START_DATE_TIME_UTC
--Conditions - case when  ME < EE and ES < MS then start_time = MS and end_time = ME

--Overlap II

select ve.account_number
      ,ve.subscriber_id
      ,ve.EVENT_START_DATE_TIME_UTC
      ,ve.EVENT_END_DATE_TIME_UTC
      ,ve.pk_viewing_prog_instance_fact
      ,od.cb_seq_id
      ,od.Download_start_date_time
      ,od.Download_end_date_time
      ,case when Download_end_date_time <= EVENT_END_DATE_TIME_UTC
       and  EVENT_START_DATE_TIME_UTC <= Download_start_date_time
       and Download_end_date_time >= Download_start_date_time
       then 1 else 0 end as Concurrency
into   Second_Overlap
from VEA_09_Jan_2014_Viewing_Events ve
inner join Ondemand_09_Jan_2014 od
on ve.account_number = od.account_number
and  Concurrency = 1
;

--

--Conditions - case when  ME< EE and ES >  MS then start_time = ES  and end_time = ME
--Overlap III

select ve.account_number
      ,ve.subscriber_id
      ,ve.EVENT_START_DATE_TIME_UTC
      ,ve.EVENT_END_DATE_TIME_UTC
      ,ve.pk_viewing_prog_instance_fact
      ,od.cb_seq_id
      ,od.Download_start_date_time
      ,od.Download_end_date_time
      ,case when Download_end_date_time <= EVENT_END_DATE_TIME_UTC
       and EVENT_START_DATE_TIME_UTC >= Download_start_date_time
       and Download_end_date_time >= EVENT_START_DATE_TIME_UTC
       then 1 else 0 end as Concurrency
into   Third_Overlap
from VEA_09_Jan_2014_Viewing_Events ve
inner join Ondemand_09_Jan_2014 od
on ve.account_number = od.account_number
and Concurrency = 1
--
;
--Conditions - case when  ME > EE and ES > MS then start_time = ES  and end_time = EE
--Overlap IV

select ve.account_number
      ,ve.subscriber_id
      ,ve.EVENT_START_DATE_TIME_UTC
      ,ve.EVENT_END_DATE_TIME_UTC
      ,ve.pk_viewing_prog_instance_fact
      ,od.cb_seq_id
      ,od.Download_start_date_time
      ,od.Download_end_date_time
      ,case when Download_end_date_time >= EVENT_END_DATE_TIME_UTC
       and  EVENT_START_DATE_TIME_UTC >= Download_start_date_time
       and  EVENT_END_DATE_TIME_UTC >= EVENT_START_DATE_TIME_UTC
       then 1 else 0 end as Concurrency
into   Fourth_Overlap
from VEA_09_Jan_2014_Viewing_Events ve
inner join Ondemand_09_Jan_2014 od
on ve.account_number = od.account_number
and Concurrency = 1

--Creating the table with all Overlaps.
create table Combined_Overlaps(account_number varchar(20),subscriber_id int,EVENT_START_DATE_TIME_UTC timestamp ,EVENT_END_DATE_TIME_UTC timestamp,pk_viewing_prog_instance_fact bigint,cb_seq_id bigint
                               ,Download_start_date_time timestamp,Download_end_date_time timestamp,Concurrency tinyint,Row_1 tinyint)

insert into Combined_Overlaps
select account_number,subscriber_id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC,pk_viewing_prog_instance_fact,cb_seq_id,Download_start_date_time,Download_end_date_time,Concurrency, 4 as Row_1
from Fourth_Overlap
--191,961

--Creating Index on the required tables ---
create hg index idx11 on VEA_09_Jan_2014_Viewing_Events(account_number);
create hg index idx21 on Ondemand_09_Jan_2014(account_number);
create hg index idx1 on Combined_Overlaps(account_number);

-------------------------------------------------------------
--Bringing in the Non Overlaps (Viewing_Events)
select VE.account_number
      ,VE.subscriber_id
      ,VE.EVENT_START_DATE_TIME_UTC
      ,VE.EVENT_END_DATE_TIME_UTC
      ,VE.pk_viewing_prog_instance_fact
      ,5 as Row_1
into   Combined_Overlaps_Plus_Non_Overlaps_pk
from VEA_09_Jan_2014_Viewing_Events VE
left join Combined_Overlaps CO
on CO.pk_viewing_prog_instance_fact = VE.pk_viewing_prog_instance_fact
where CO.pk_viewing_prog_instance_fact is null
--18,267,519 Row(s) affected

--Non overlaps for Ondemand
select od.account_number
      ,od.cb_seq_id
      ,od.Download_start_date_time
      ,od.Download_end_date_time
      ,6 as Row_1
into  Combined_Overlaps_Plus_Non_Overlaps_seq
from Ondemand_09_Jan_2014 od
left join Combined_Overlaps CO
on od.cb_seq_id = CO.cb_seq_id
where CO.cb_seq_id is null
--1,792,150 Row(s) affected

insert into Combined_Overlaps (account_number,subscriber_id,EVENT_START_DATE_TIME_UTC,EVENT_END_DATE_TIME_UTC,pk_viewing_prog_instance_fact, Row_1)
select account_number
      ,subscriber_id
      ,EVENT_START_DATE_TIME_UTC
      ,EVENT_END_DATE_TIME_UTC
      ,pk_viewing_prog_instance_fact
      ,Row_1
from Combined_Overlaps_Plus_Non_Overlaps_pk
--18,267,519 Row(s) affected

insert into Combined_Overlaps (account_number,cb_seq_id ,Download_start_date_time,Download_end_date_time, Row_1)
select account_number
      ,cb_seq_id
      ,Download_start_date_time
      ,Download_end_date_time
      ,Row_1
from Combined_Overlaps_Plus_Non_Overlaps_seq
--1,792,150 Row(s) affected


--Updating the table with the Multi-Room Variable
alter table Combined_Overlaps add Multi_box tinyint;

Update Combined_Overlaps
set CO.Multi_box = TC.Max_Multiroom
from Temp_cust_subs_hist_09_Jan TC
inner join Combined_Overlaps CO
on CO.account_number = TC.account_number
;
--Updating the table above with Weights
Alter table Combined_Overlaps add Weights bigint;

update Combined_Overlaps
set CO.Weights = SC.scaling_weighting
        from  vespa_analysts.vespa_household_weighting SC
    inner join Combined_Overlaps CO
    on CO.account_number = SC.account_number
    where SC.scaling_date = '2014-01-09'
;

---Updating with New_Start_time and New_End_time

alter table  Combined_Overlaps add Start_time time;
alter table  Combined_Overlaps add End_time time;
;
Update Combined_Overlaps
set Start_time = Download_start_date_time
from Combined_Overlaps
where Row_1 = 1
;

Update Combined_Overlaps
set End_time = EVENT_END_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 1
;
Update Combined_Overlaps
set Start_time = Download_start_date_time
from Combined_Overlaps
where Row_1 = 2
;
Update Combined_Overlaps
set End_time = Download_end_date_time
from Combined_Overlaps
where Row_1 = 2
;
Update Combined_Overlaps
set Start_time = EVENT_START_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 3
;
Update Combined_Overlaps
set End_time = Download_end_date_time
from Combined_Overlaps
where Row_1 = 3
;
Update Combined_Overlaps
set Start_time = EVENT_START_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 4
;
Update Combined_Overlaps
set End_time = EVENT_END_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 4
;

alter table  Combined_Overlaps add Start_date timestamp;
alter table  Combined_Overlaps add End_date timestamp;
;
Update Combined_Overlaps
set Start_date = Download_start_date_time
from Combined_Overlaps
where Row_1 = 1
;

Update Combined_Overlaps
set End_date = EVENT_END_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 1
;
Update Combined_Overlaps
set Start_date = Download_start_date_time
from Combined_Overlaps
where Row_1 = 2
;
Update Combined_Overlaps
set End_date = Download_end_date_time
from Combined_Overlaps
where Row_1 = 2
;
Update Combined_Overlaps
set Start_date = EVENT_START_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 3
;
Update Combined_Overlaps
set End_date = Download_end_date_time
from Combined_Overlaps
where Row_1 = 3
;
Update Combined_Overlaps
set Start_date = EVENT_START_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 4
;
Update Combined_Overlaps
set End_date = EVENT_END_DATE_TIME_UTC
from Combined_Overlaps
where Row_1 = 4


select top 10* from Combined_Overlaps_Duration
select * from Hourly
select count(*) from Combined_Overlaps

----Creating a separate table based on Hours
drop table Hourly;
create table Hourly(Start_Hour time, End_Hour time);
insert into Hourly(Start_Hour,End_Hour)
values ('00:00', '01:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('01:00','02:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('02:00','03:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('03:00','04:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('04:00','05:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('05:00','06:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('06:00','07:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('07:00','08:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('08:00','09:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('09:00','10:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('10:00','11:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('11:00','12:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('12:00','13:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('13:00','14:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('14:00','15:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('15:00','16:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('16:00','17:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('17:00','18:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('18:00','19:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('19:00','20:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('20:00','21:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('21:00','22:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('22:00','23:00')
;
insert into Hourly(Start_Hour,End_Hour)
values ('23:00','00:00')
;
select top 100* from Combined_Overlaps_Duration
where Start_time = '23:00'
select * from Hourly

select top 100* from Final_Combined_Overlaps_Duration
where New_Start_time = '22:00'


select account_number
      ,subscriber_id
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,start_date
      ,end_date
      ,1 as New_Concurrency
into   New_Overlap_1
from   Combined_Overlaps
inner join Hourly
on     End_Hour >= End_time
       and Start_time <= Start_Hour
       and End_time >= Start_Hour
where date(start_date) = date(end_date)
;

select account_number
      ,subscriber_id
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,start_date
      ,end_date
      ,1  as New_Concurrency
into   New_Overlap_2
from   Combined_Overlaps
inner join Hourly
on     End_Hour <= End_time
       and Start_time <= Start_Hour
       and End_Hour >= Start_Hour
where date(start_date) = date(end_date)

;
--3
select account_number
      ,subscriber_id
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,start_date
      ,end_date
      ,1 as New_Concurrency
into   New_Overlap_3
from   Combined_Overlaps
inner join Hourly
on     End_Hour <= End_time
       and Start_time >= Start_Hour
       and End_Hour >= Start_time
where date(start_date) = date(end_date)

;
--4
select account_number
      ,subscriber_id
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,start_date
      ,end_date
      ,1 as New_Concurrency
into   New_Overlap_4
from   Combined_Overlaps
inner join Hourly
on     End_Hour >= End_time
       and Start_time >= Start_Hour
       and End_time >= Start_time
where date(start_date) = date(end_date)

--5
select account_number
      ,subscriber_id
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,start_date
      ,end_date
      ,1 as New_Concurrency
into   New_Overlap_5
from   Combined_Overlaps
inner join Hourly
on     Start_time >= Start_Hour
and   End_time >= end_Hour
where date(start_date) < date(end_date)
;
--6
select account_number
      ,subscriber_id
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,start_date
      ,end_date
      ,1 as New_Concurrency
into   New_Overlap_6
from   Combined_Overlaps
inner join Hourly
on     Start_time <= Start_Hour
       and End_time >= end_Hour
where date(start_date) < date(end_date)
;


--Creating the table with all Overlaps.
drop table Combined_Overlaps_New
create table Combined_Overlaps_New(account_number varchar(20),subscriber_id int,Start_time time ,End_time time,start_date datetime, end_date datetime,Start_Hour time,End_Hour time,New_Concurrency tinyint,Row tinyint)
;
insert into Combined_Overlaps_New(account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency,Row)
select account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency, 1 as Row
from New_Overlap_1
;
insert into Combined_Overlaps_New(account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency,Row)
select account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency, 2 as Row
from New_Overlap_2
;
insert into Combined_Overlaps_New(account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency,Row)
select account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency, 3 as Row
from New_Overlap_3
;
insert into Combined_Overlaps_New(account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency,Row)
select account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency, 4 as Row
from New_Overlap_4

;
insert into Combined_Overlaps_New(account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency,Row)
select account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency, 5 as Row
from New_Overlap_5

;
insert into Combined_Overlaps_New(account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency,Row)
select account_number,subscriber_id,Start_time,End_time,Start_Hour,End_Hour,start_date,end_date,New_Concurrency, 6 as Row
from New_Overlap_6


--Updating the table with the Multi-Room Variable
alter table Combined_Overlaps_New add Multi_box tinyint;

Update Combined_Overlaps_New
set CO.Multi_box = TC.Max_Multiroom
from Temp_cust_subs_hist_09_Jan TC
inner join Combined_Overlaps_New CO
on CO.account_number = TC.account_number
;


--Updating the table above with Weights
Alter table Combined_Overlaps_New add Weights bigint;

update Combined_Overlaps_New
set CO.Weights = SC.scaling_weighting
        from  vespa_analysts.vespa_household_weighting SC
    inner join Combined_Overlaps_New CO
    on CO.account_number = SC.account_number
    where SC.scaling_date = '2014-01-09'

--Calculating the New Duration
select account_number
      ,subscriber_id
      ,Row
      ,New_Concurrency
      ,Multi_box
      ,Weights
      ,Start_time
      ,End_time
      ,Start_Hour
      ,End_Hour
      ,Start_date
      ,end_date
      ,datediff(Second, start_date, end_date) as Duration
 into Final_Combined_Overlaps_New
from Combined_Overlaps_New

select top 10* from Combined_Overlaps_New
select top 10* from Final_Combined_Overlaps_New
--where New_Duration
where Start_Hour = '23:00'

****************************************************************************************************************
select top 100* from Final_Combined_Overlaps_New_Duration



