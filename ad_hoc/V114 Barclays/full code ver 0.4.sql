

/*------------------------------------------------------------------------------
        Project: Barclays Cash ISA Campaign
        Version: v1.
        Created: 23/10/2012
        Lead: Sarah Moore
        Analyst: Harry Gill
        SK Prod: 4

        Milestone: 4 **


        Introduction

        This code is part of a series of templates designed to reach milestones detailed below in preperation for the Barclays Cash ISA campaign.


        Milestone

        1.      Prepare Code to Link Spots and Programmes Data                  (Dan B)

                Check matching criteria for Spots and programs  4 (DB)
                Code linking programme and spots data
                Code to identify spot placement in break
                Test Run / QA

        2.      Prepare code to link client data and Sky base data              (Hannah)

                Create Dummy Data in accordance with file template
                Sanity Check matching criteria (cb keys)
                Code linking Client table and program data
                Test Run / QA

        3.      Prepare code to identify universes                              (Harry/Hannah)

                Code Flagging client / non client data in Sky and VESPA
                Define and code  flag for viewed spot
                Test Run / QA

        4.      Prepare code for Experian / client profiling                    (Harry)

                Identify Experian Variables to use
                Flag Experian variables
                Flag Client segments
                Prepare code for VESPA viewing Profile
                Test Run / QA

        5.      Prepare code for TV Profile                                     (Harry)

                Top programmes (eff and reach)
                Distribution of Impacts by components
                Test Run / QA

        6.      Prepare code for closed loop measurement                        (Harry)

                Check Distribution across Sky and VESPA
                Code to Flag responders and non responders
                Code Output metrics
                Test Run / QA

        7.      Design Templates                                                (Susanne)

                Match Rate Output – pre Diagnostic
                Standard Excel output for Diagnostic

        8.      Improve Efficiencies                                            (Susanne or Jon Green)

                Match Rate Output – pre Diagnostic
                Standard Excel output for Diagnostic
                Test Run / QA

        9.      Presentation Output                                             (Susanne?)

                Design Template




        CODE STRUCTURE AND SECTIONS
        --------

        Set-Up   -

        PART A   -
             A01 - IDENTIFY PRIMARY BOXES RETURNING DATA
             A02 - GET THE VIEWING DATA


        PART B   -
             B01 - ADD PACK TO THE VIEWING DATA
             B02 - ADD HS AND SKY+ FLAG

        PART C   - SCALING
             C01 - CREATE A BASE TABLE
             C02 - CALCULATE THE NORMALISED WEIGHT

        PART D - AVERAGE VIEWING PER DAY (MINUTES)
             D01 - SUMMERISE VIEWING FOR EACH CUSTOMER
             D02 - CALCULATE AVERAGE MINBUTES FOR EACH PACKAGE - BASED ON THE UK BASE
             D03 - CALCULATE EACH CUSTOMERS DEVIATION FROM THE PACKAGE  MEAN
             D04 - ALLOCATE EACH ACCOUNT TO THE RELEVANT DECILE

        PART E - SHARE OF VIEWING
             E01 - SUMMERISE SHARE OF VIEWING FOR EACH CUSTOMER
             E02 - CALCULATE SHARE OF VIEWING AVERAGE FOR EACH PACKAGE - BASED ON THE UK BASE
             E03 - CALCULATE EACH CUSTOMERS DEVIATION FROM THE PACKAGE  MEAN
             E04 - ALLOCATE EACH ACCOUNT TO THE RELEVANT DECILE


        Ouput Tables:   DO_NOT_DELETE_NBA_SCORING_2012XXXX -- THIS TABLE CONTAINS ALL SCORING for each customer
        -------

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------*/

create variable @mid_date date;

set @mid_date  = '2012-03-23'


-- DANS SPOTS CODE



/*--------------------------------------------------------------------------------
--  PART A - IDENTIFY EVERY SPOT WATCHED BY THE PANEL OVER THE PERIOD (Barclays Spots)
--------------------------------------------------------------------------------

             A01 - CREATE A LIST OF ALL SPOTS OVER THE PERIOD
             A02 - EXTRACT THE VIEWING OF EACH SPOT


--------------------------------------------------------------------------------*/


------------------------------------------------------------------------------------
-- A01 -- CREATE A LIST OF ALL SPOTS OVER THE PERIOD - ADD ON CHANNEL NAME DETAILS
------------------------- -----------------------------------------------------------

if object_id('all_barlcays_spots') is not null drop table all_barlcays_spots;
select * into all_barlcays_spots from neighbom.BARB_MASTER_SPOT_DATA            -- Martin Neighbors' table
where clearcast_commercial_no='BBHBBPR155030'

commit;

-- select count(*) from all_barlcays_spots -- 26k promo's were aired!


--- Add on Channel Name Details

alter table all_barlcays_spots add full_name varchar(255);
alter table all_barlcays_spots add vespa_name varchar(255);
alter table all_barlcays_spots add channel_name varchar(255);
alter table all_barlcays_spots add techedge_name varchar(255);
alter table all_barlcays_spots add infosys_name varchar(255);
-- 5.41minutes



update all_barlcays_spots
set a.full_name=b.full_name
,a.vespa_name=b.vespa_name
,a.channel_name=b.channel_name
,a.techedge_name=b.techedge_name
,a.infosys_name=b.infosys_name
from all_barlcays_spots as a
left outer join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as b
on a.service_key=b.service_key
where a.local_date_of_transmission between b.effective_from and b.effective_to
;
commit;
-- 8224179 Row(s) affected
--1.15minutes


---Remeove trailing spaces from Full_Name field to crete a field to match to lookup to be used to match to EPG data
alter table all_barlcays_spots add spot_channel_name varchar(255);
update all_barlcays_spots
set spot_channel_name = trim(full_name)
from all_barlcays_spots
;
commit;
create  hg index idx1 on all_barlcays_spots(service_key);
create  hg index idx2 on all_barlcays_spots(utc_spot_start_date_time);
create  hg index idx3 on all_barlcays_spots(utc_spot_end_date_time);

----Load in Channel details to create lookup from Spot Data to EPG Data---

--select epg_channel_match_name ,utc_spot_start_date_time ,count(*) as dupes  from all_barlcays_spots_DEDUPED group by epg_channel_match_name ,utc_spot_start_date_time having dupes>1;
--select * from all_barlcays_spots_DEDUPED where utc_spot_start_date_time='2012-03-22 23:53:24';



------------------------------------------------------------------------------------
-- A02 -- EXTRACT THE VIEWING OF EACH SPOT
------------------------------------------------------------------------------------

-- we need some variables
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;

--select panel_id , count(*) from sk_prod.VESPA_STB_PROG_EVENTS_20120301 group by panel_id order by panel_id;

-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-02-29';
--SET @var_prog_period_end    = '2012-03-01';
SET @var_prog_period_end    = '2012-04-20';
-- How many days (after end of broadcast period) to check for timeshifted viewing
--SET @var_num_days = 1;
SET @var_num_days = 52;
--select @var_num_days;


------
-- Step 1: identify the programes aired over the period
------

if object_id('VESPA_Programmes_project_108') is not null drop table VESPA_Programmes_project_108;

select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,synopsis
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,tx_date_utc
       ,service_key
      ,datediff(mi,Tx_Start_Datetime_UTC,Tx_End_Datetime_UTC) as programme_duration
  into VESPA_Programmes_project_108 -- drop table  VESPA_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc <= dateadd(day, 1, @var_prog_period_end) -- because @var_prog_period_end is a date and defaults to 00:00:00 when compared to datetimes
-- Add further filters to programmes here if required, eg, lower(channel_name) like '%bbc%'
   ;
--select top 500 * from VESPA_Programmes_project_108 where upper(channel_name) like '%ATLANTIC%';
commit;
create unique hg index idx1 on VESPA_Programmes_project_108(programme_trans_sk);
create  hg index idx2 on VESPA_Programmes_project_108(tx_date_utc);
create  hg index idx3 on VESPA_Programmes_project_108(service_key);
commit;
--6876004 Row(s) affected
--3minutes



------
-- Step 2: get the viewing data; create the table
------

if object_id('Barclays_spots_viewing_table_dump ') is not null drop table Barclays_spots_viewing_table_dump ;
create table Barclays_spots_viewing_table_dump  (
Viewing_date                    date
,Broadcast_date                 date
,cb_row_ID                      bigint          not null
,Account_Number                 varchar(20)     not null
,Subscriber_Id                  decimal(8,0)    not null
,Cb_Key_Household               bigint
,Cb_Key_Family                  bigint
,Cb_Key_Individual              bigint
,Event_Type                     varchar(20)
,X_Type_Of_Viewing_Event        varchar(40)     not null
,Event_Start_Time               datetime
,Event_end_time                 datetime
,Tx_Start_Datetime_UTC          datetime
,Tx_End_Datetime_UTC            datetime
,viewing_starts                 datetime
,viewing_stops                  datetime
,viewing_duration               integer
,Recorded_Time_UTC              datetime
,timeshifting                   varchar(10)
,programme_duration             decimal(2,1)
,X_Viewing_Time_Of_Day          varchar(15)
,Programme_Trans_Sk             bigint
,Channel_Name                   varchar(20)
,Epg_Title                      varchar(50)
,Genre_Description              varchar(20)
,Sub_Genre_Description          varchar(20)
,capped_flag                    tinyint
,utc_spot_start_date_time              datetime
,utc_spot_end_date_time              datetime
,utc_break_start_date_time              datetime
,utc_break_end_date_time              datetime
,full_name varchar(255)
,vespa_name varchar(255)
,techedge_name varchar(255)
,infosys_name varchar(255)
,service_key int
,spot_position int
,spots_in_break int
,spot_duration int
,clearcast_commercial_no        varchar(25)
);

commit;



-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Barclays_spots_viewing_table_dump (
Viewing_date
,Broadcast_date
,cb_row_ID
,Account_Number
,Subscriber_Id
,Cb_Key_Household
,Cb_Key_Family
,Cb_Key_Individual
,Event_Type
,X_Type_Of_Viewing_Event
,Event_Start_Time
,Event_end_time
,Tx_Start_Datetime_UTC
,Tx_End_Datetime_UTC
,viewing_starts
,viewing_stops
,viewing_duration
,Recorded_Time_UTC
,timeshifting
,programme_duration
,X_Viewing_Time_Of_Day
,Programme_Trans_Sk
,Channel_Name
,Epg_Title
,Genre_Description
,Sub_Genre_Description
,capped_flag
,utc_spot_start_date_time
,utc_spot_end_date_time
,utc_break_start_date_time
,utc_break_end_date_time
,full_name
,vespa_name
,techedge_name
,infosys_name
,service_key
,spot_position
,spots_in_break
,spot_duration
,clearcast_commercial_no

)
select
    cast(da.viewing_starts as date),cast(prog.Tx_Start_Datetime_UTC as date),vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id
    ,vw.Cb_Key_Household,vw.Cb_Key_Family,vw.Cb_Key_Individual
    ,vw.Event_Type,vw.X_Type_Of_Viewing_Event
    ,vw.Adjusted_Event_Start_Time
    ,da.capped_event_end_time,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC
    ,da.viewing_starts,da.viewing_stops,da.viewing_duration
    ,vw.Recorded_Time_UTC
    ,da.timeshifting
    ,prog.programme_duration, vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
    ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
    ,da.capped_flag
    ,spot.utc_spot_start_date_time
    ,spot.utc_spot_end_date_time
    ,spot.utc_break_start_date_time
    ,spot.utc_break_end_date_time
,spot.full_name
,spot.vespa_name
,spot.techedge_name
,spot.infosys_name
,spot.service_key
,spot.spot_position_in_break
,spot.no_spots_in_break
,spot.spot_duration
,spot.clearcast_commercial_no
from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join VESPA_Programmes_project_108 as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
inner join all_barlcays_spots as spot
on prog.service_key=spot.service_key
where
    (          dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops) between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) < utc_spot_start_date_time and  dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops)> utc_spot_end_date_time
    )
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;



SET @scanning_day = @var_prog_period_start;
--delete from Barclays_spots_viewing_table_dump ;
commit;
 while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
 begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

-- LETS HAVE A SENSE CHECK.
/*
select top 10 * from Barclays_spots_viewing_table_dump
select count(*) from Barclays_spots_viewing_table_dump
select min(viewing_date) as min, max(viewing_date) as max from Barclays_spots_viewing_table_dump
*/



------
-- Step 3: flag all COMPLETE AND PART the barclays spots in the viewing table and add a spot identifier
------


-- Identify all barclays spots and put them into a new table - give each spot an ID

if object_id('Barclays_spots') is not null drop table Barclays_spots;

select *
       ,flag = 1
        ,sum(flag) over (order by local_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier
into Barclays_spots from all_barlcays_spots    -- this table has ALL spots in it! - the new table MAY NOT!!!
where clearcast_commercial_no='BBHBBPR155030'


-----------------------------------------------------------------------------------------------
        -- Take a look --
-- select top 10 * from barclays_spots
-- select count(*), count(distinct(identifier)), max(identifier) from Barclays_spots


----------------------------------------- what is the spots distribution??      ---------------

select local_date_of_transmission
        ,count(*) as number_of_barclays_spots_aired
from  Barclays_spots
group by local_date_of_transmission
order by local_date_of_transmission

-----------------------------------------------------------------------------------------------


-- add the needed fields to the vieiwng table!
alter table Barclays_spots_viewing_table_dump
        add (Whole_spot integer default 0
           ,spot_identifier integer);


Update Barclays_spots_viewing_table_dump
        set vw.Whole_spot = case when (vw.recorded_time_utc < vw.utc_spot_start_date_time
                                 and  dateadd(second,vw.viewing_duration,vw.recorded_time_utc)> vw.utc_spot_end_date_time)

                                 OR (timeshifting = 'LIVE' and vw.viewing_starts < vw.utc_spot_start_date_time
                                 and  viewing_stops> vw.utc_spot_end_date_time)

                                 then 1 else 0 end
            ,vw.spot_identifier = spot.identifier
from Barclays_spots_viewing_table_dump  vw
join Barclays_spots       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_break_start_date_time = spot.utc_break_start_date_time
and  vw.service_key = spot.service_key
and  vw.vespa_name = spot.vespa_name


-- check what this looks like
select top 10 * from Barclays_spots_viewing_table_dump



-- how many Barclays spots were whole and part watched by the panel - 3.4million viewied instances, of which 2.7million were whole viewed
select count(case when whole_spot = 1 then 1 else null end) as whole_spots
        ,count(case when whole_spot = 0 then 1 else null end) as part_spots
        ,count(*) as all_spot_views
from Barclays_spots_viewing_table_dump



-- aired/watched distrib
SELECT COUNT(DISTINCT(spot_identifier)) FROM Barclays_spots_viewing_table_dump WHERE  whole_spot = 1 -- number of whole pots watched
select count(*) from Barclays_spots -- number of spots aired


----------------------------------
-- CLEAN UP THE CHANNEL NAMES SO WE HAVE A BETTER IDEA OF WHICH CHANNELS THE SPOTS WERE SEEN ON:
----------------------------------


--AGGREGATE +1 AND HD variations
-- drop table #channel1

select  service_key
        ,case when right(channel_name,2) = 'HD' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,2) = '+1' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,1) = '+' THEN LEFT(channel_name,(LEN(channel_name)-1))
                                                ELSE channel_name END AS Channel
        ,channel_name
INTO #channel1
FROM Barclays_spots_viewing_table_dump
group by channel_name,service_key
;--303channels



--drop table #channel2
--select * from #channel2 order by channel;

SELECT service_key
        ,RTRIM(channel) as Channel
        ,channel_name

INTO    #channel2
FROM    #channel1
;

-- now adjust the names that didn't work above - forn example +2's etc

if object_id('LkUpChannel') is not null drop table LkUpChannel

SELECT  service_key
        ,case when channel = 'BBC ONE'     THEN 'BBC ONE HD'
             when left(channel,5) = 'BBC 1' THEN 'BBC 1'
             when left(channel,5) = 'BBC 2' THEN 'BBC 2'
             when channel_name = 'BBC HD' THEN 'BBC HD'
             when left(channel,4) = 'ITV1' THEN 'ITV1'
             when channel = 'ComedyCtrl' THEN 'ComedyCentral'
             when channel = 'Comedy Cen' THEN 'ComedyCentral'
             when channel = 'Sky Sp News' THEN 'Sky Spts News'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'FX+' THEN 'FX'
             when channel = 'Nick Replay' THEN 'Nickelodeon'
             when channel = 'Sky Sports HD2' THEN 'Sky Sports 2'
             when channel = 'Sky Sports HD3' THEN 'Sky Sports 3'
             when channel = 'Sky Sports HD4' THEN 'Sky Sports 4'
             when channel = 'mov4men2' THEN 'movies4men 2'
             when channel = 'mov4men' THEN 'movies4men'
             when channel = 'ComedyCtlX' THEN 'ComedyCtralX'
             when channel = 'horror ch' THEN 'horror channel'
             when channel = 'History +1 hour' THEN 'History'
             when channel = 'Disc.RT' THEN 'Disc.RealTime'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Eurosport' THEN 'Eurosport UK'
             when channel = 'Food Netwrk' THEN 'Food Network'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Animal Plnt' THEN 'Animal Planet'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'ESPN AmrcaHD' THEN 'ESPN America'

             when channel = 'AnimalPlnt' THEN 'Animal Planet'
             when channel = 'Chart Show' THEN 'Chart Show TV'
             when channel = 'DMAX+2' THEN 'DMAX'
             when channel = 'Home & Health' THEN 'Home&Health'
             when channel = 'Nat Geo+1hr' THEN 'Nat Geo'
             when channel = 'NatGeoWild' THEN 'Nat Geo Wild'
             when channel = 'Sky ScFi/Hor' THEN 'Sky ScFi/Horror'
             when channel = 'Travel Ch' THEN 'Travel Channel'

                                                    ELSE channel END AS Channel
        ,channel_name
INTO LkUpChannel
FROM #channel2
group by channel_name, channel,service_key
order by channel
;



-- finally add the cleaned channel names to the main vieiwng table.
alter table Barclays_spots_viewing_table_dump
        add agg_channel_name varchar(50);


update Barclays_spots_viewing_table_dump
        set agg_channel_name = channel
from Barclays_spots_viewing_table_dump vw
join lkupchannel lk
on vw.service_key = lk.service_key




-- changes here need to be added to the output senction; the promos watched.
/*--------------------------------------------------------------------------------
--  PART B - Now we need viewing data on all of the other spots watched; this will be done for a 2 week period
--------------------------------------------------------------------------------

             A01 - CREATE A LIST OF ALL SPOTS OVER THE PERIOD
             A02 - EXTRACT THE VIEWING OF EACH SPOT

--------------------------------------------------------------------------------*/


------------------------------------------------------------------------------------
-- A01 -- CREATE A LIST OF ALL SPOTS OVER THE PERIOD - ADD ON CHANNEL NAME DETAILS
------------------------------------------------------------------------------------

if object_id('all_spots_2weeks_108') is not null drop table all_spots_2weeks_108;
select * into all_spots_2weeks_108 from neighbom.BARB_MASTER_SPOT_DATA            -- Martin Neighbors' table
where local_date_of_transmission between '2012-02-29' and '2012-03-13'
commit


grant select on all_spots_2weeks_108 to public

--- Add on Channel Name Details

alter table all_spots_2weeks_108 add full_name varchar(255);
alter table all_spots_2weeks_108 add vespa_name varchar(255);
alter table all_spots_2weeks_108 add channel_name varchar(255);
alter table all_spots_2weeks_108 add techedge_name varchar(255);
alter table all_spots_2weeks_108 add infosys_name varchar(255);
-- 5.41minutes


update all_spots_2weeks_108
set a.full_name=b.full_name
,a.vespa_name=b.vespa_name
,a.channel_name=b.channel_name
,a.techedge_name=b.techedge_name
,a.infosys_name=b.infosys_name
from all_spots_2weeks_108 as a
left outer join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as b
on a.service_key=b.service_key
where a.local_date_of_transmission between b.effective_from and b.effective_to



---Remeove trailing spaces from Full_Name field to crete a field to match to lookup to be used to match to EPG data
alter table all_spots_2weeks_108 add spot_channel_name varchar(255);
update all_spots_2weeks_108
set spot_channel_name = trim(full_name)
from all_spots_2weeks_108
;
commit;

create  hg index idx1 on all_spots_2weeks_108(service_key);
create  hg index idx2 on all_spots_2weeks_108(utc_spot_start_date_time);
create  hg index idx3 on all_spots_2weeks_108(utc_spot_end_date_time);


-- we need to add the sales house to the barclays spots table as there is nothing to match to in the vieiwng table
alter table all_spots_2weeks_108
        add sales_house varchar(25);

update all_spots_2weeks_108
        set spot.sales_house = chg.primary_sales_house
from all_spots_2weeks_108 spot
inner join neighbom.channel_map_dev_barb_channel_group chg
on spot.log_station_code = chg.log_station_code


-- LETS AND A UNIQUE IDENTIFIER --

select *
       ,flag = 1
        ,sum(flag) over (order by local_spot_start_date_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as identifier
into all_spots_2weeks_108_NEW from all_spots_2weeks_108


----Load in Channel details to create lookup from Spot Data to EPG Data---

--select epg_channel_match_name ,utc_spot_start_date_time ,count(*) as dupes  from all_spots_2weeks_108_DEDUPED group by epg_channel_match_name ,utc_spot_start_date_time having dupes>1;
--select * from all_spots_2weeks_108_DEDUPED where utc_spot_start_date_time='2012-03-22 23:53:24';

-- select count(*)  from all_spots_2weeks_108
-- select count(*) from all_spots_2weeks_108 where vespa_name is not null -- 1% lost in the match!



------------------------------------------------------------------------------------
-- A02 -- EXTRACT THE VIEWING OF EACH SPOT
------------------------------------------------------------------------------------

-- we need some variables
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;

--select panel_id , count(*) from sk_prod.VESPA_STB_PROG_EVENTS_20120301 group by panel_id order by panel_id;

-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-02-29';
--SET @var_prog_period_end    = '2012-03-01';
SET @var_prog_period_end    = '2012-03-13';
-- How many days (after end of broadcast period) to check for timeshifted viewing
--SET @var_num_days = 1;
SET @var_num_days = 7;
--select @var_num_days;


------
-- Step 1: identify the programes aired over the period
------

if object_id('VESPA_Programmes_project_108_2week') is not null drop table VESPA_Programmes_project_108_2week;

select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,synopsis
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,tx_date_utc
       ,service_key
      ,datediff(mi,Tx_Start_Datetime_UTC,Tx_End_Datetime_UTC) as programme_duration
  into VESPA_Programmes_project_108_2week -- drop table  VESPA_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc <= dateadd(day, 1, @var_prog_period_end) -- because @var_prog_period_end is a date and defaults to 00:00:00 when compared to datetimes
-- Add further filters to programmes here if required, eg, lower(channel_name) like '%bbc%'

--select top 500 * from VESPA_Programmes_project_108_2week where upper(channel_name) like '%ATLANTIC%';
commit;
create unique hg index idx1 on VESPA_Programmes_project_108_2week(programme_trans_sk);
create  hg index idx2 on VESPA_Programmes_project_108_2week(tx_date_utc);
create  hg index idx3 on VESPA_Programmes_project_108_2week(service_key);
commit;



------
-- Step 2: get the viewing data; create the table
------

if object_id('Project_108_viewing_table_dump_2weeks') is not null drop table Project_108_viewing_table_dump_2weeks;
create table Project_108_viewing_table_dump_2weeks (
Viewing_date                    date
,Broadcast_date                 date
,cb_row_ID                      bigint          not null
,Account_Number                 varchar(20)     not null
,Subscriber_Id                  decimal(8,0)    not null
,Cb_Key_Household               bigint
,Cb_Key_Family                  bigint
,Cb_Key_Individual              bigint
,Event_Type                     varchar(20)
,X_Type_Of_Viewing_Event        varchar(40)     not null
,Event_Start_Time               datetime
,Event_end_time                 datetime
,Tx_Start_Datetime_UTC          datetime
,Tx_End_Datetime_UTC            datetime
,viewing_starts                 datetime
,viewing_stops                  datetime
,viewing_duration               integer
,Recorded_Time_UTC              datetime
,timeshifting                   varchar(10)
,programme_duration             decimal(2,1)
,X_Viewing_Time_Of_Day          varchar(15)
,Programme_Trans_Sk             bigint
,Channel_Name                   varchar(20)
,Epg_Title                      varchar(50)
,Genre_Description              varchar(20)
,Sub_Genre_Description          varchar(20)
,capped_flag                    tinyint
,utc_spot_start_date_time              datetime
,utc_spot_end_date_time              datetime
,utc_break_start_date_time              datetime
,utc_break_end_date_time              datetime
,full_name varchar(255)
,vespa_name varchar(255)
,techedge_name varchar(255)
,infosys_name varchar(255)
,service_key int
,spot_position int
,spots_in_break int
,spot_duration int
,clearcast_commercial_no        varchar(25)
);

commit;



-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Project_108_viewing_table_dump_2weeks(
Viewing_date
,Broadcast_date
,cb_row_ID
,Account_Number
,Subscriber_Id
,Cb_Key_Household
,Cb_Key_Family
,Cb_Key_Individual
,Event_Type
,X_Type_Of_Viewing_Event
,Event_Start_Time
,Event_end_time
,Tx_Start_Datetime_UTC
,Tx_End_Datetime_UTC
,viewing_starts
,viewing_stops
,viewing_duration
,Recorded_Time_UTC
,timeshifting
,programme_duration
,X_Viewing_Time_Of_Day
,Programme_Trans_Sk
,Channel_Name
,Epg_Title
,Genre_Description
,Sub_Genre_Description
,capped_flag
,utc_spot_start_date_time
,utc_spot_end_date_time
,utc_break_start_date_time
,utc_break_end_date_time
,full_name
,vespa_name
,techedge_name
,infosys_name
,service_key
,spot_position
,spots_in_break
,spot_duration
,clearcast_commercial_no

)
select
    cast(da.viewing_starts as date),cast(prog.Tx_Start_Datetime_UTC as date),vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id
    ,vw.Cb_Key_Household,vw.Cb_Key_Family,vw.Cb_Key_Individual
    ,vw.Event_Type,vw.X_Type_Of_Viewing_Event
    ,vw.Adjusted_Event_Start_Time
    ,da.capped_event_end_time,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC
    ,da.viewing_starts,da.viewing_stops,da.viewing_duration
    ,vw.Recorded_Time_UTC
    ,da.timeshifting
    ,prog.programme_duration, vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
    ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
    ,da.capped_flag
    ,spot.utc_spot_start_date_time
    ,spot.utc_spot_end_date_time
    ,spot.utc_break_start_date_time
    ,spot.utc_break_end_date_time
,spot.full_name
,spot.vespa_name
,spot.techedge_name
,spot.infosys_name
,spot.service_key
,spot.spot_position_in_break
,spot.no_spots_in_break
,spot.spot_duration
,spot.clearcast_commercial_no
from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join VESPA_Programmes_project_108_2week as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
inner join all_spots_2weeks_108 as spot
on prog.service_key=spot.service_key
where
    (          dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops) between utc_spot_start_date_time and utc_spot_end_date_time
        or       dateadd(second,x_time_in_seconds_since_recording*-1,viewing_starts) < utc_spot_start_date_time and  dateadd(second,x_time_in_seconds_since_recording*-1,viewing_stops)> utc_spot_end_date_time
    )
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;



SET @scanning_day = @var_prog_period_start;
--delete from all_spots_2weeks_108;
commit;
 while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
 begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;


--------------------------------------------------------------------------------


--AGGREGATE +1 AND HD variations
-- drop table #channel1

select  service_key
        ,case when right(channel_name,2) = 'HD' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,2) = '+1' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,1) = '+' THEN LEFT(channel_name,(LEN(channel_name)-1))
                                                ELSE channel_name END AS Channel
        ,channel_name
INTO #channel1
FROM Project_108_viewing_table_dump_2weeks
group by channel_name,service_key
;--303channels



--drop table #channel2
--select * from #channel2 order by channel;

SELECT service_key
        ,RTRIM(channel) as Channel
        ,channel_name

INTO    #channel2
FROM    #channel1
;

-- now adjust the names that didn't work above - forn example +2's etc

if object_id('LkUpChannel') is not null drop table LkUpChannel

SELECT  service_key
        ,case when channel = 'BBC ONE'     THEN 'BBC ONE HD'
             when left(channel,5) = 'BBC 1' THEN 'BBC 1'
             when left(channel,5) = 'BBC 2' THEN 'BBC 2'
             when channel_name = 'BBC HD' THEN 'BBC HD'
             when left(channel,4) = 'ITV1' THEN 'ITV1'
             when channel = 'ComedyCtrl' THEN 'ComedyCentral'
             when channel = 'Comedy Cen' THEN 'ComedyCentral'
             when channel = 'Sky Sp News' THEN 'Sky Spts News'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'FX+' THEN 'FX'
             when channel = 'Nick Replay' THEN 'Nickelodeon'
             when channel = 'Sky Sports HD2' THEN 'Sky Sports 2'
             when channel = 'Sky Sports HD3' THEN 'Sky Sports 3'
             when channel = 'Sky Sports HD4' THEN 'Sky Sports 4'
             when channel = 'mov4men2' THEN 'movies4men 2'
             when channel = 'mov4men' THEN 'movies4men'
             when channel = 'ComedyCtlX' THEN 'ComedyCtralX'
             when channel = 'horror ch' THEN 'horror channel'
             when channel = 'History +1 hour' THEN 'History'
             when channel = 'Disc.RT' THEN 'Disc.RealTime'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Eurosport' THEN 'Eurosport UK'
             when channel = 'Food Netwrk' THEN 'Food Network'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Animal Plnt' THEN 'Animal Planet'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'ESPN AmrcaHD' THEN 'ESPN America'

             when channel = 'AnimalPlnt' THEN 'Animal Planet'
             when channel = 'Chart Show' THEN 'Chart Show TV'
             when channel = 'DMAX+2' THEN 'DMAX'
             when channel = 'Home & Health' THEN 'Home&Health'
             when channel = 'Nat Geo+1hr' THEN 'Nat Geo'
             when channel = 'NatGeoWild' THEN 'Nat Geo Wild'
             when channel = 'Sky ScFi/Hor' THEN 'Sky ScFi/Horror'
             when channel = 'Travel Ch' THEN 'Travel Channel'

                                                    ELSE channel END AS Channel
        ,channel_name
INTO LkUpChannel
FROM #channel2
group by channel_name, channel,service_key
order by channel




-- finally add the cleaned channel names to the main vieiwng table.
alter table Project_108_viewing_table_dump_2weeks
        add agg_channel_name varchar(50);


update Project_108_viewing_table_dump_2weeks
        set agg_channel_name = channel
from Project_108_viewing_table_dump_2weeks vw
join lkupchannel lk
on vw.service_key = lk.service_key



select top 10 * from Project_108_viewing_table_dump_2weeks

-- add indexes here

/*--------------------------------------------------------------------------------
--  PART B - GET VIEWING DATA -- this vieiwng data will be used to determine if the customer is heavy/light Tv viewer
--------------------------------------------------------------------------------

             A01 - FLAG VARIABLES FOR THE VESPA BASE
             A02 - FLAG VARIABLES FOR THE BARCLAYS BASE

--------------------------------------------------------------------------------*/






--------------------------------------------------------------------------------
-- SET UP.
--------------------------------------------------------------------------------
-- create and populate variables
CREATE VARIABLE @var_period_start_data  datetime;
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;
Create VARIABLE @var_period_start_dt    datetime;
Create VARIABLE @var_period_end_dt      datetime;


CREATE VARIABLE @var_barb_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @i                      integer;

-- Scaling Variables
Create variable @target_date date;
Create variable @sky_total numeric(28,20);
Create variable @Sample_total numeric(28,20);
Create variable @weightings_total numeric(28,20);
Create variable @scaling_factor numeric(28,20);

-- Set the date variables
SET @var_period_start_dt = '2012-02-29 06:00:00'
SET @var_period_end_dt   = '2012-04-20 06:00:00'


SET @var_period_start_data      = '2012-02-29'; -- Second date variable to be manipulated within a loop of @var_period_start
SET @var_period_start           = '2012-02-29'; -- Monday
SET @var_period_end             = '2012-04-20'; -- Mon
SET @var_barb_period_end        = '2012-04-20'; -- following Sunday (7 day BARB window for playback, we only need 163 hours)

-- this part will need to be automated at some point ***



/*
--------------------------------------------------------------------------------
-- PART B - IDENTIFY THE CUSTOMERS VIEWING BEHAVIOUR: high/medium/low viewer
--------------------------------------------------------------------------------

                    Set Up
             B01 - identify boxes returning data over the whole period
             B02 - Get the viewing data - cap it (daily_augs)

--------------------------------------------------------------------------------

-- so the Barclays campaign ran for 45 days (29thFeb - 13TH April 2012) - we will create customer viewing segments (high/med/low) based
-- on 2 weeks of viewing data. /This must encompas the mid-point for scaling purposes - this is 23rd March.
-- the two weeks of viewing data will then be: 16th - 30th March 2012.


*/


-- create new date variables to capture the correct viewing period:
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @scanning_day           datetime;

SET @var_prog_period_start  = '2012-03-16';
SET @var_prog_period_end    = '2012-03-30';
SET @var_num_days = 15;


--------------------------------------------------------------------------------
-- B01 - identify boxes returning data over the whole period
--------------------------------------------------------------------------------


--identify boxes that returned data over the entire 2 week period;
IF object_id('consistent_vespa_universe') IS NOT NULL DROP TABLE consistent_vespa_universe;

create table consistent_vespa_universe (
   -- subscriber_id decimal(8)
    account_number varchar(20)
    ,reporting_day varchar(8));



SET @var_sql = '
    insert into consistent_vespa_universe
    select distinct(account_number), ''##^^*^*##''
from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##
    where (play_back_speed is null or play_back_speed = 2)
      --  and x_programme_viewed_duration > 0
        and Panel_id in (4,5)';



-- loop though each days viewing logs to identify repeat data returners
SET @var_cntr = 0;
SET @i=datediff(dd,@var_period_start,@var_barb_period_end);

WHILE @var_cntr <= @i
BEGIN
        EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_period_start), 'yyyymmdd')))

        COMMIT

        SET @var_cntr = @var_cntr + 1           END;



-- and what is data return distribution??
select distinct(account_number)
        , count(distinct(reporting_day)) as number_days
 into #temp
 from consistent_vespa_universe
 group by account_number

                -- we want top determine if 80% of 45 days (campaign) or 52days (campaign +7day playback) offer a suitable number of accounts:
                -- given the distribution of data return we will take boxes that have returned data for 80% of the 45 day period campaign;



select distinct(number_days) as number_days
        ,count(distinct(account_number)) as number_accounts
from #temp
group by number_days
order by number_days


IF object_id('consistent_vespa_universe2') IS NOT NULL DROP TABLE consistent_vespa_universe2;

select distinct(account_number)
into gillh.consistent_vespa_universe2
from #temp
where number_days >= 36
-- 286,943 Good!



--- this is a check into the volumes for the consistent universe! - now we only want those accounts that returned data on the mid-point;

select account_number
  into gillh.consistent_vespa_universe3 from consistent_vespa_universe
where reporting_day = '20120323' and account_number in (select account_number from consistent_vespa_universe2)--those that returned for 80%


/*
--QA checked,
select max(number_days) from #temp

*/


-- service instance id is needed for scaling tables: lets create a new table that can be used for scaling

select distinct(account_number)
       ,service_instance_id
into table_for_scaling
from sk_prod.VESPA_STB_PROG_EVENTs_20120323
where account_number  in (select account_number from gillh.consistent_vespa_universe3) and service_instance_id is not null
group by account_number, service_instance_id

drop table table_for_scaling

--------------------------------------------------------------------------------
-- B02 - Get the Viewing Data
--------------------------------------------------------------------------------

if object_id('Project_108_customer_viewing_capped') is not null drop table Project_108_customer_viewing_capped;


create table Project_108_customer_viewing_capped (
Viewing_date                    date
,cb_row_ID                      bigint          not null
,Account_Number                 varchar(20)     not null
,Subscriber_Id                  decimal(8,0)    not null
,Cb_Key_Household               bigint
,Cb_Key_Family                  bigint
,Cb_Key_Individual              bigint
,Event_Type                     varchar(20)
,X_Type_Of_Viewing_Event        varchar(40)     not null
,Event_Start_Time               datetime
,Event_end_time                 datetime
--,Tx_Start_Datetime_UTC          datetime
--,Tx_End_Datetime_UTC            datetime
,viewing_starts                 datetime
,viewing_stops                  datetime
,viewing_duration               integer
,Recorded_Time_UTC              datetime
,timeshifting                   varchar(10)
,X_Viewing_Time_Of_Day          varchar(15)
,Programme_Trans_Sk             bigint
,capped_flag                    tinyint
,channel_name                    varchar(30)
,genre_description               varchar(25)
,sub_genre_description           varchar(25)
,x_broadcast_time_of_day         varchar(25)

);
commit;


-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Project_108_customer_viewing_capped(
Viewing_date
,cb_row_ID
,Account_Number
,Subscriber_Id
,Cb_Key_Household
,Cb_Key_Family
,Cb_Key_Individual
,Event_Type
,X_Type_Of_Viewing_Event
,Event_Start_Time
,Event_end_time
--,Tx_Start_Datetime_UTC
--,Tx_End_Datetime_UTC
,viewing_starts
,viewing_stops
,viewing_duration
,Recorded_Time_UTC
,timeshifting
,X_Viewing_Time_Of_Day
,Programme_Trans_Sk
,capped_flag
,channel_name
,genre_description
,sub_genre_description
,x_broadcast_time_of_day

)


select
    cast(da.viewing_starts as date),vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id
    ,vw.Cb_Key_Household,vw.Cb_Key_Family,vw.Cb_Key_Individual
    ,vw.Event_Type,vw.X_Type_Of_Viewing_Event
    ,vw.Adjusted_Event_Start_Time
    ,da.capped_event_end_time--,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC
    ,da.viewing_starts,da.viewing_stops,da.viewing_duration
    ,vw.Recorded_Time_UTC
    ,da.timeshifting, vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
    ,da.capped_flag
    ,epg.channel_name
    ,epg.genre_description
    ,epg.sub_genre_description
    ,epg.x_broadcast_time_of_day

from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join sk_prod.VESPA_EPG_DIM epg
on epg.Programme_Trans_Sk = vw.Programme_Trans_Sk
where vw.account_number in (select account_number from consistent_vespa_universe3)
-- no limits have been pla;ced on the playback speed or the minutes that must have been viewed - we want empty logs.
-- the augs table also limits for the active panels at this period of time. (4 and 5)
';
commit;


-- Loop over the days in the period, extracting all the data.

SET @scanning_day = @var_prog_period_start;

while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

-- The above is capped viewing for all customers in the consistent (80%) universe.

create hg index account_number_index on Project_108_customer_viewing_capped (account_number);




/*-- Checks
select top 10 * from Project_108_customer_viewing_capped

select count(*) from Project_108_customer_viewing_capped
-- 193,733,826
*/


-- we dont actually need any boxes that are non-primary for this: lets remove non-primary boxes from this part:


-- lets idetify all boxes and then flag the prim ary boxes only!

select subscriber_id
        ,account_number
        ,primary_flag = 0
into #accounts
from Project_108_customer_viewing_capped
group by subscriber_id, account_number


 update #accounts
 set rbs.primary_flag = case when sbv.ps_flag = 'P' then 1 else primary_flag end -- this can be adjusted to make it more thorough! -- ps olive
 from #accounts as rbs
 left join vespa_analysts.vespa_single_box_view as sbv
 on rbs.subscriber_id = sbv.subscriber_id -- this should be done at subscriber_id level



-- do a quick count and have a look:
select count(*) as boxes, count(case when primary_flag = 1 then 1 else null end) as primary_boxes from #accounts
select top 10 * from #accounts -- all looks fne



-- Now delete any records from the 2 weeks viewing table that are not needed.
delete from Project_108_customer_viewing_capped
 where subscriber_id not in (select subscriber_id from #accounts where primary_flag = 1)
--14810323 Row(s) affected




------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------


/*
--------------------------------------------------------------------------------
-- PART C - SCALING
--------------------------------------------------------------------------------

             C01 - identify boxes returning data over the whole period
             C02 - Get the viewing data
             C03 -

--------------------------------------------------------------------------------

-- so the Barclays campaign ran for 45 days (29thFeb - 13TH April 2012) - we will create customer viewing segments (high/med/low) based
-- on 2 weeks of viewing data. /This must encompas the mid-point for scaling purposes - this is 23rd March.
-- the two weeks of viewing data will then be: 16th - 30th March 2012.


*/

-- With scaling - we are using Phase 2 and require scaling to be run for the mid day; 23rd March on a subset of the vespa universe (consitent boxes)
-- an ad hoc scaling run is required for our particular universe.
-- scaling code is too large to be added here however code is available in the project folder.

-- The process is detailed below:

-- here are the procedures ---

--•       Run table creation script (script 1)
--•       Run the script to create segment id lookup (script 2)
--•       Run the script to create the procedures (script 3)

--
--    execute   SC2_do_weekly_segmentation '2012-03-23',0,'2012-11-07'
--    execute   SC2_prepare_panel_members '2012-03-23','','2012-11-07'
--    execute   SC2_make_weights '2012-03-23','2012-11-07',''
--



----------
-- add some new fields and add the scaling measures to the scaling table defined on line 621
----------


select top 10 * from table_for_scaling


   alter table table_for_scaling
     add (weighting_date        date
         ,weightings            float
         ,scaling_segment_ID    integer);
commit;


  update table_for_scaling
     set weighting_date =       @mid_date;


-- First, get the segmentation for the account at the time of viewing#
  update table_for_scaling as bas
     set bas.scaling_segment_ID = wei.scaling_segment_ID
    from SC2_intervals as wei -- my schema as its an ad hoc run
   where bas.account_number = wei.account_number
--     and cast(bas.weighting_date as date) between cast(wei.reporting_starts as date) and cast(wei.reporting_ends as date); -- will this work?? single day!



-- Second, find out the weight for that segment on that day
update table_for_scaling
     set weightings = wei.weighting
    from table_for_scaling as bas INNER JOIN SC2_weightings as wei
                                  --     bas.weighting_date = wei.scaling_day
                                        on bas.scaling_segment_ID = wei.scaling_segment_ID
;
commit;


---- Clean up the table

delete from table_for_scaling where weightings is null or weightings = 0

--
-- select top 10 * from SC2_weightings
-- select top 10 * from SC2_intervals
--
--
-- select sum(sum_of_weights) from SC2_weightings
--
-- select count(*), count(distinct(account_number)) from table_for_scaling
--
-- select top 10 * from table_for_scaling
--
-- select distinct(account_number)
--         ,max(service_instance_id) as service_instance_id
--         ,max(weighting_date) as weighting_date
--         ,max(weightings) as weightings
--         ,max(scaling_segment_ID) as scaling_segment_ID
-- into #temp
-- from table_for_scaling
-- group by account_number
--
-- drop table table_for_scaling
--
-- select * into table_for_scaling from #temp
--
--
--
-- drop table #temp


----
-- QA checks
----

-- check the total population for the mid point date
select sum(weightings) from table_for_scaling --                9,404,592
-- this is the total population watching TV on mid point day.




----------
-- Now add the weighting and some flags to the vieiwng table
----------

 alter table Project_108_customer_viewing_capped
     add (weightings            float
         ,decile_me_first       integer);
commit;


update Project_108_customer_viewing_capped
     set weightings = wei.weightings
        ,decile_me_first = 1 --
    from Project_108_customer_viewing_capped as bas
    INNER JOIN table_for_scaling as wei
               on bas.account_number = wei.account_number
;
commit;
-- 192million

-- this table will contain nulls where the customer has not been scaled.

select top 10 * from Project_108_customer_viewing_capped


------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------






/*
--------------------------------------------------------------------------------
-- PART D - AVERAGE VIEWING PER DAY (MINUTES) AND DECILE CUSTOMERS;
--------------------------------------------------------------------------------
             D01 - SUMMERISE VIEWING FOR EACH CUSTOMER
             D02 - CALCULATE AVERAGE MINBUTES FOR EACH PACKAGE - BASED ON THE UK BASE
             D03 - CALCULATE EACH CUSTOMERS DEVIATION FROM THE PACKAGE  MEAN
             D04 - ALLOCATE EACH ACCOUNT TO THE RELEVANT DECILE

--------------------------------------------------------------------------------
*/

-- the movies, sports, premium and exclusive channels are subject to change - check that these definitions are still true when re-running the code
-- there may be new channels - or epg channel name changes.


----------------------------------------------------------------------
-- DO1 -- LETS GET THE VIEWING DETAILS SUMMARISED FOR EACH CUSTOMER
----------------------------------------------------------------------



select top 10 * from daily_averages_108



-----------
-- STEP 1: LETS GET A CUSTOMER LEVEL SUMMARY OF VIEWING *MINUTES* PER DAY
-----------

If object_id('daily_averages_108') is not null drop table daily_averages_108


 SELECT account_number
       ,max(weightings) as normalised_weight -- need the correct weighting to do this part!
        ,total_minutes = (sum(viewing_duration)/60)
        ,count(distinct(viewing_date)) as days_data_return
        ,average_Total_minutes_day = ((sum(viewing_duration)/60)/days_data_return) -- check that this works correctly

 into daily_averages_108
 from Project_108_customer_viewing_capped
group by account_number


select avg(average_Total_minutes_day) from daily_averages_108 -- 319 minutes -- seems more realistic!


-- select top 10 * from daily_averages_108
-- select count(*), count(distinct(account_number)) from daily_averages_108
-- select distinct(average_Total_minutes_day), count(*) as freq from daily_averages_108 group by average_Total_minutes_day order by average_Total_minutes_day


--------------------------------------------------------------------------------
--DO2- NOW LETS CALCULATE THE AVERAGE SHARE OF VIEWING ACROSS MEASURES FOR EACH PACKAGE:
--------------------------------------------------------------------------------


---------------------------------------------------------------
--step 1: find the total number of customers and the total minutes watched each day to identify the average amount of viewing per day for each Sky Customer
---------------------------------------------------------------

create variable @universe bigint;
create variable @total_minutes bigint;
create variable @average_minutes float;


set @universe = (select sum(normalised_weight) from daily_averages_108)             -- this needs to be corrected to the correct table and field names
set @total_minutes = (select sum(viewing_duration*weightings/60) from Project_108_customer_viewing_capped where viewing_date = '2012-03-23') -- mid point day    -- this may give some nulls/zeros?
set @average_minutes = @total_minutes/@universe -- need a nullif??


--check it:
select @universe        -- this should reflect those boxes not watching TV on a particular day -- will this reflect the mid point day??
select @total_minutes
select @average_minutes -- this should be less than 24 hours!


---------------------------------------------------------------
--step 2: Find each customers deviation from the average amount of viewing
---------------------------------------------------------------


select account_number
       ,customer_minute_deviation = (average_Total_minutes_day - @average_minutes)
into #deviations
from daily_averages_108


alter table daily_averages_108
        add minute_deviation_from_average integer       -- this will be whole minutes


update daily_averages_108
        set minute_deviation_from_average = customer_minute_deviation
from daily_averages_108 as day
join #deviations as  dev
on day.account_number = dev.account_number


select top 10 * from daily_averages_108


----------------------------------------------------------------------
-- DO4: LETS DECILE THESE CUSTOMERS
---------------------------------------------------------------------

------------
-- STEP 1: LETS GET THE CUMULITIVE WEIGHTING TO ALLOCATE DECILES TO THE CUSTOMERS BASED ON THIER POPULATION REPRESENTATION
------------

--select top 10 * from phase2_viewing_capped_avg_mins_day

-- we only need to quintile 4things: total minutes, SOV pay, SOV PVR, SOV FTA Movies. Day of week and genre can be done in a different table (many rows)

drop table #temp22

select  account_number
        ,normalised_weight
        ,minute_deviation_from_average  -- this is the main measure

        ,sum(normalised_weight) over ( order by minute_deviation_from_average
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Minute_deviation_cumul_weighting --
into #temp22
from daily_averages_108
where normalised_weight is not null -- the accounts that were not scaled - they will be allocated to deciles via by looking at min/max thresholds

--- the figures above are arranged such that lower viewing is first and higher viewing last - will transalte to higher cumul weightings for more viewing



-- lets do some checks:
--QA--------------------------------------------------------------------

/*

SELECT MAX(Minute_deviation_cumul_weighting) FROM #TEMP22

*/



--------------------------------------------------------------------------------
--STEP 2: - CREATE DECILE TABLEs AND ADD WEIGHTING BANDINGS
--------------------------------------------------------------------------------

IF object_id('decile_weights_base') IS NOT NULL DROP TABLE decile_weights_base

create table decile_weights_base ( centile integer primary key);


create variable y int;
set y = 1;

while y <= 10
begin
        insert into decile_weights_base values (y)

        set y = y + 1
end


-- add a sample field to help decide which customers go into which decile based on how much of the sample they represent;
alter table decile_weights_base add sample float;

update decile_weights_base              set sample = ceil((centile) *   (select sum(normalised_weight) from #temp22)/10); -- all cusotmers


--check it:
select * from decile_weights_base;


--------------------------------------------------------------------------------
--STEP 3 - now allocate each account to the relevant quintile based on the cumulitave weighting
--------------------------------------------------------------------------------

IF object_id('customer_deciles_108') IS NOT NULL DROP TABLE customer_deciles_108

-- we need to copy the temp table into a real table so we can add columns etc -- these are the scaled customers
select * into customer_deciles_108 from #temp22;


-- we need to create 4 quintile allocations
alter table customer_deciles_108
add ( Total_tv_deciles integer default 0 );


-- now lets use the different cumul weightings to allocate the deciles.

-- TV VIEWING  CONTENT
update customer_deciles_108
        set Total_tv_deciles = centile
from customer_deciles_108 as vdw
 inner join decile_weights_base as cww
 on Minute_deviation_cumul_weighting <= sample;


select total_tv_deciles, count(*) from customer_deciles_108 group by total_tv_deciles order by total_tv_deciles
-- not many people in the top deciles but this is reasonable!



--- now the scaled customers have been deciled - lets scale the remaining c.70k customers based on the threshholds
---------------------------------------------------------------------------------
-- step 4: get the accounts that have not been scaled into one table - following the format above.

        -- tHESE WILL BE ALLOCATED TO A DECILE BASED ON THE THRESHOLDS OF THAT DECILE GROUP.
---------------------------------------------------------------------------------


if object_id('cust_deciles_allocation_108') is not null drop table cust_deciles_allocation_108

select  account_number
        ,normalised_weight
        ,minute_deviation_from_average
        ,Minute_deviation_cumul_weighting = null -- this is just a placeholder to make for an easy insert into a table of similar/same structure later
into cust_deciles_allocation_108
 from daily_averages_108
 where normalised_weight is null -- the accounts that were not scaled - they will be allocated to deciles via by looking at min/max thresholds


--------------------------------------------------------------------------------
--STEP 5 - now allocate each account to the relevant quintile based on the threasholds
--------------------------------------------------------------------------------

----
-- a: add some new columns
----

alter table cust_deciles_allocation_108
add ( Total_tv_deciles integer default 0);

select top 10 * from cust_deciles_allocation_108

----
-- b: identify the the threatholds for each of the decile groupings - make a table for each decile type
----


--Total TV
select Total_tv_deciles
        ,min(minute_deviation_from_average) as total_min_deviation_min
        ,max(minute_deviation_from_average) as total_min_deviation_max
into #total_tv_thresholds
from customer_deciles_108
group by Total_tv_deciles
order by Total_tv_deciles



----
-- c: allocate the customers to a decile
----

-- total TV
update cust_deciles_allocation_108
        set alo.Total_tv_deciles = ths.Total_tv_deciles
from cust_deciles_allocation_108 as alo
 inner join #total_tv_thresholds as ths
 on minute_deviation_from_average between total_min_deviation_min and total_min_deviation_max;

-- select Total_tv_deciles, count(*) from cust_deciles_allocation_108 group by Total_tv_deciles


----
-- d: now insert these records into customer_deciles_108 so we have all deciles in the same place.
----


insert into customer_deciles_108
        select * from cust_deciles_allocation_108 --





/*
--************
-- QA---
--************


select top 10 * from customer_deciles_108

--we are looking to check the number of customers in each quintile - we hope they are in the same ball park (not the same as traditionally done)
--also we want to make sure there are not huge differences between the average weighting of the customers within each Quintitle


select distinct(Total_tv_deciles), count(*) as count, avg(normalised_weight) from customer_deciles_108
group by Total_tv_deciles
order by Total_tv_deciles

0 decile means the customer is not eligible for that category e.g. not a HD customer


1 = very light/no viewing
10 = heavy viewing
*/


 -- ok so OUTPUT: customer_deciles_108 has all the deciles and deviations in minutes from package averages. Now lets do the same for share of viewing.

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------











/*--------------------------------------------------------------------------------
--  PART E - FLAG EXPERIAN VARIABLES
--------------------------------------------------------------------------------

             E01 - FLAG VARIABLES FOR THE VESPA BASE
             E02 - FLAG VARIABLES FOR THE BARCLAYS BASE

--------------------------------------------------------------------------------*/


-- TABLES TO BE USED IN THIS SECTION ARE: -- **** THE BARCLAYS TABLES **** --


-- DO SOME CHECKS --

SELECT TOP 10 * FROM om_prod.OM114_BARCLAYS_CUSTOMER
select count(*) from OM114_BARCLAYS_CUSTOMER -- 7,865,968

select count(distinct(cb_key_household)) from om_prod.OM114_BARCLAYS_CUSTOMER -- 7,063,525

select distinct(desired_campaign_customer) from OM114_BARCLAYS_CUSTOMER -- this is null.

select distinct(model_propensity_percentile) from om_prod.OM114_BARCLAYS_CUSTOMER -- both have not been populated!
select distinct(model_propensity_score) from om_prod.OM114_BARCLAYS_CUSTOMER


-- responders
SELECT TOP 10 * FROM om_prod.OM114_BARCLAYS_RESPONSE

select count(*) from om_prod.OM114_BARCLAYS_RESPONSE -- 559,360

select count(distinct(cb_key_household)) from om_prod.OM114_BARCLAYS_RESPONSE -- 459,022

select min(application_date) from om_prod.OM114_BARCLAYS_RESPONSE -- february 29th 2012

select max(application_date) from om_prod.OM114_BARCLAYS_RESPONSE -- May 6th 2012

select distinct(purchase) from om_prod.OM114_BARCLAYS_RESPONSE -- this should be a sum of purchases - customer level??



--------------------------------------------------------------------------------
--  E01 - FLAG VARIABLES FOR THE VESPA BASE
--------------------------------------------------------------------------------


------
-- STEP 1: create the base table (universe) and insert placeholders for values
------

-- lets the fields we need:
-- Add CB Keys to the consistent vespa boxes table

alter table consistent_vespa_universe3
        add (Cb_Key_Household bigint
            ,Cb_Key_Individual bigint );


update consistent_vespa_universe3
        set con.Cb_Key_Household = vt.Cb_Key_Household
           ,con.Cb_Key_Individual = vt.Cb_Key_Individual
from consistent_vespa_universe3 con
left join Project_108_customer_viewing_capped vt
on con.account_number = vt.account_number
-- 286943 Row(s) affected

select top 10 * from consistent_vespa_universe3

/*
select con.account_number
        ,Cb_Key_Household
        ,Cb_Key_Individual
from consistent_vespa_universe2 con
left join Project_108_customer_viewing_capped vt
on con.account_number = vt.account_number
*/



-- now lets create a new table with placeholders for the new fields that we want to add:
-- its just faster than creating a table and then inserting into it.

if object_id('v081_Vespa_Universe_demographics') is not null drop table v081_Vespa_Universe_demographics

select  cvu.account_number
        ,max(cvu.Cb_Key_Household) as Cb_Key_Household
        ,vespa_panel = 1
        ,Barclays_customer = max( case when cvu.Cb_Key_Household = cus.Cb_Key_Household then 1 else 0 end)
        ,barclays_responder = max( case when cvu.Cb_Key_Household = res.Cb_Key_Household then 1 else 0 end)

        -- additional details for barclays customers
       ,max(cus.unique_identifier) as unique_identifier
       ,max(cus.segment) as segment
  --     ,max(cus.model_propensity_score) as model_propensity_score
  --     ,max(cus.model_propensity_percentile) as model_propensity_percentile
       ,max(cus.date_acquired) as date_acquired
       ,desired_campaign_customer = 0                  -- this is not populated ut i can add something here later??
       ,max(cus.barclays_customer_before_campaign) as barclays_customer_before_campaign
       ,max(cus.barclays_ISA_before_campaign) as barclays_ISA_before_campaign
       ,max(cus.barclays_cash_ISA) as barclays_cash_ISA
       ,max(cus.unused_cash_ISA_balance) as unused_cash_ISA_balance
     --  ,max(cus.date_of_last_cash_ISA_balance) as date_of_last_cash_ISA_balance
       ,max(cus.home_branch) as home_branch

        -- responce data
        ,max(res.application_date) as max_application_date -- this will be a group by
        ,min(res.application_date) as min_application_date -- this will be a group by
        ,max(res.type_of_application) as max_type_of_application
        ,max(res.purchase) as purchased
        ,sum(purchase) as number_of_purchases
        ,sum(res.purchase_amount) as sum_purchase_amount

        --experian variable
        ,household_composition = 'inserted to give correct length'
--      ,shareholding_value = 'a 22 letter placeholder '
        ,FSS_V3_TYPE = 'placeholder placeholder'
        ,FSS_V3_group = 'placeholder placeholder'
        ,household_affluence = 'Unknown'
        ,social_grade = 'Unknown'
        ,Total_tv_deciles = 11 -- this is a place holder

        -- INCLUDE SOME VIEWING PROFILE: LIGHT/HEAVY/MEDIUM VIEIWNG

into v081_Vespa_Universe_demographics
from consistent_vespa_universe3 cvu-- name may have changed
 left join om_prod.OM114_BARCLAYS_CUSTOMER cus                  -- barclays customer table
 on cus.Cb_Key_Household = cvu.Cb_Key_Household
 left join om_prod.OM114_BARCLAYS_RESPONSE res
 on res.Cb_Key_Household = cvu.Cb_Key_Household
group by account_number
        ,cvu.Cb_Key_Household


-- add in the customers viewing decile here --
update v081_Vespa_Universe_demographics
set ud.Total_tv_deciles = tmp.Total_tv_deciles
from v081_Vespa_Universe_demographics ud
 join customer_deciles_108 tmp
on tmp.account_number = ud.account_number




-- first we need to bring in scaling weightings into the vespa demographics tables

alter table v081_Vespa_Universe_demographics
        add weighting float;

update v081_Vespa_Universe_demographics
        set weighting = weightings
from v081_Vespa_Universe_demographics uni
join table_for_scaling scale
on uni.account_number = scale.account_number

-- select sum(weighting) from gillh.v081_Vespa_Universe_demographics v


-- check it
select top 10 * from v081_Vespa_Universe_demographics


------
-- STEP 2: get customer dempgraphics from experians consumer view -- aggregate them
------

--drop table #vespa_experian_match

SELECT   CV.Cb_Key_Household
        ,max(CASE WHEN CV.h_household_composition = '00' THEN 'Families'
                WHEN CV.h_household_composition = '01' THEN 'Extended family'
                WHEN CV.h_household_composition = '02' THEN 'Extended household'
                WHEN CV.h_household_composition = '03' THEN 'Pseudo family'
                WHEN CV.h_household_composition = '04' THEN 'Single male'
                WHEN CV.h_household_composition = '05' THEN 'Single female'
                WHEN CV.h_household_composition = '06' THEN 'Male homesharers'
                WHEN CV.h_household_composition = '07' THEN 'Female homesharers'
                WHEN CV.h_household_composition = '08' THEN 'Mixed homesharers'
                WHEN CV.h_household_composition = '09' THEN 'Abbreviated male families'
                WHEN CV.h_household_composition = '10' THEN 'Abbreviated female families'
                WHEN CV.h_household_composition = '11' THEN 'Multi-occupancy dwelling'
                WHEN CV.h_household_composition = 'U' THEN  'Unclassified'
            ELSE                                            'Unknown'            END) as household_composition

--         ,max(CASE WHEN CV.h_shareholding_value = '0' THEN 'No shares'
--                   WHEN CV.h_shareholding_value = '1' THEN 'Low value (<£10,000)'
--                   WHEN CV.h_shareholding_value = '2' THEN 'High value (>£10,000)'
--             ELSE                                         'Unknown'               END) as shareholding_value


        ,max (CASE WHEN cv.h_fss_v3_type  =         '01'   THEN     'Equity Ambitions'
                WHEN cv.h_fss_v3_type  =         '02'   THEN     'Portable Assets'
                WHEN cv.h_fss_v3_type  =         '03'   THEN     'Early Settlers'
                WHEN cv.h_fss_v3_type  =         '04'   THEN     'First Foundations'
                WHEN cv.h_fss_v3_type  =         '05'   THEN     'Urban Opportunities'
                WHEN cv.h_fss_v3_type  =         '06'   THEN     'Flexible Margins'
                WHEN cv.h_fss_v3_type  =         '07'   THEN     'Tomorrows Earners'
                WHEN cv.h_fss_v3_type  =         '08'   THEN     'Entry-level Workers'
                WHEN cv.h_fss_v3_type  =         '09'   THEN     'Cash Stretchers'
                WHEN cv.h_fss_v3_type  =         '10'   THEN     'Career Priorities'
                WHEN cv.h_fss_v3_type  =         '11'   THEN     'Upward Movers'
                WHEN cv.h_fss_v3_type  =         '12'   THEN     'Family Progression'
                WHEN cv.h_fss_v3_type  =         '13'   THEN     'Savvy Switchers'
                WHEN cv.h_fss_v3_type  =         '14'   THEN     'New Nesters'
                WHEN cv.h_fss_v3_type  =         '15'   THEN     'Security Seekers'
                WHEN cv.h_fss_v3_type  =         '16'   THEN     'Premier Portfolios'
                WHEN cv.h_fss_v3_type  =         '17'   THEN     'Fast-track Fortunes'
                WHEN cv.h_fss_v3_type  =         '18'   THEN     'Asset Accruers'
                WHEN cv.h_fss_v3_type  =         '19'   THEN     'Self-made Success'
                WHEN cv.h_fss_v3_type  =         '20'   THEN     'Golden Outlook'
                WHEN cv.h_fss_v3_type  =         '21'   THEN     'Sound Positions'
                WHEN cv.h_fss_v3_type  =         '22'   THEN     'Single Accumulators'
                WHEN cv.h_fss_v3_type  =         '23'   THEN     'Mid-range Gains'
                WHEN cv.h_fss_v3_type  =         '24'   THEN     'Extended Outlay'
                WHEN cv.h_fss_v3_type  =         '25'   THEN     'Modest Mortgages'
                WHEN cv.h_fss_v3_type  =         '26'   THEN     'Overworked Resources'
                WHEN cv.h_fss_v3_type  =         '27'   THEN     'Self-reliant Realists'
                WHEN cv.h_fss_v3_type  =         '28'   THEN     'Canny Owners'
                WHEN cv.h_fss_v3_type  =         '29'   THEN     'Squeezed Families'
                WHEN cv.h_fss_v3_type  =         '30'   THEN     'Pooled Kitty'
                WHEN cv.h_fss_v3_type  =         '31'   THEN     'High Demands'
                WHEN cv.h_fss_v3_type  =         '32'   THEN     'Value Hunters'
                WHEN cv.h_fss_v3_type  =         '33'   THEN     'Low Cost Living'
                WHEN cv.h_fss_v3_type  =         '34'   THEN     'Guaranteed Provision'
                WHEN cv.h_fss_v3_type  =         '35'   THEN     'Steady Savers'
                WHEN cv.h_fss_v3_type  =         '36'   THEN     'Deferred Assurance'
                WHEN cv.h_fss_v3_type  =         '37'   THEN     'Practical Preparers'
                WHEN cv.h_fss_v3_type  =         '38'   THEN     'Persistent Workers'
                WHEN cv.h_fss_v3_type  =         '39'   THEN     'Lifelong Low-spenders'
                WHEN cv.h_fss_v3_type  =         '40'   THEN     'Experienced Renters'
                WHEN cv.h_fss_v3_type  =         '41'   THEN     'Sage Investors'
                WHEN cv.h_fss_v3_type  =         '42'   THEN     'Dignified Elders'
                WHEN cv.h_fss_v3_type  =         '43'   THEN     'Comfortable Legacy'
                WHEN cv.h_fss_v3_type  =         '44'   THEN     'Semi-retired Families'
                WHEN cv.h_fss_v3_type  =         '45'   THEN     'Cautious Stewards'
                WHEN cv.h_fss_v3_type  =         '46'   THEN     'Classic Moderation'
                WHEN cv.h_fss_v3_type  =         '47'   THEN     'Quiet Simplicity'
                WHEN cv.h_fss_v3_type  =         '48'   THEN     'Senior Sufficiency'
                WHEN cv.h_fss_v3_type  =         '49'   THEN     'Ageing Fortitude'
                WHEN cv.h_fss_v3_type  =         '50'   THEN     'State Veterans'
                WHEN cv.h_fss_v3_type  =         '99'   THEN     'Unallocated'
            ELSE                                                 'Unknown'               END) as FSS_V3_TYPE


     ,max (CASE WHEN cv.h_fss_v3_group  =        'A'    THEN     'Accumulated Wealth'
                WHEN cv.h_fss_v3_group  =        'B'    THEN     'Balancing Budgets'
                WHEN cv.h_fss_v3_group  =        'C'    THEN     'Bright Futures'
                WHEN cv.h_fss_v3_group  =        'D'    THEN     'Consolidating Assets'
                WHEN cv.h_fss_v3_group  =        'E'    THEN     'Established Reserves'
                WHEN cv.h_fss_v3_group  =        'F'    THEN     'Family Interest'
                WHEN cv.h_fss_v3_group  =        'G'    THEN     'Growing Rewards'
                WHEN cv.h_fss_v3_group  =        'H'    THEN     'Platinum Pensions'
                WHEN cv.h_fss_v3_group  =        'I'    THEN     'Seasoned Economy'
                WHEN cv.h_fss_v3_group  =        'J'    THEN     'Single Endeavours'
                WHEN cv.h_fss_v3_group  =        'K'    THEN     'Stretched Finances'
                WHEN cv.h_fss_v3_group  =        'L'    THEN     'Sunset Security'
                WHEN cv.h_fss_v3_group  =        'M'    THEN     'Traditional Thrift'
                WHEN cv.h_fss_v3_group  =        'N'    THEN     'Young Essentials'
                WHEN cv.h_fss_v3_group  =        'U'    THEN     'Unallocated'
          ELSE                                                 'Unknown'               END) as FSS_V3_group

     ,max(h_affluence_v2)    as household_affluence   -- *** this will be allocated into bandings later - standardised for all future analysis

INTO #vespa_experian_match
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv
where Cb_Key_Household in (select distinct(Cb_Key_Household) from v081_Vespa_Universe_demographics)
GROUP BY CV.Cb_Key_Household;


select top 10 * from #vespa_experian_match




------
-- STEP 3: get customers Social grade from CACI tables
------

--drop table #caci_sc1

select  c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into #caci_sc1
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
  and c.cb_key_household in (select cb_key_household from v081_Vespa_Universe_demographics)
order by c.cb_key_household;


--de-dupe!
delete from #caci_sc1 where rank_id > 1  -- more than half!


select count(*) from #caci_sc1 where social_grade <> 'Unknown'


------
-- STEP 4: copy all of the above aggregated data into the base table
------


-- NOW COPY THE ABOVE AGGREGATED DATA INTO THE UNIVERSE TABLE
update v081_Vespa_Universe_demographics
set ud.household_composition = tmp.household_composition
--   ,ud.shareholding_value = tmp.shareholding_value
   ,ud.FSS_V3_TYPE = tmp.FSS_V3_TYPE
   ,ud.FSS_V3_group = tmp.FSS_V3_group
   ,ud.household_affluence = tmp.household_affluence
from v081_Vespa_Universe_demographics ud
 join #vespa_experian_match tmp
on tmp.Cb_Key_Household = ud.Cb_Key_Household




-- then update for caci

update v081_Vespa_Universe_demographics
set ud.social_grade = cac.social_grade
from v081_Vespa_Universe_demographics ud
 join #caci_sc1 cac
on cac.cb_key_household = ud.cb_key_household




------
-- STEP 5: ensure no nulls and change placeholder values where they still exist!
------



-- ENSURE THERE ARE NO NULL FIELDS AND CHANGE THE PLACEHOLDER VALUES

update v081_Vespa_Universe_demographics
set household_composition = case when household_composition = 'inserted to give correct length' or household_composition is null
                                 then 'Unknown Sky' else household_composition end

--     ,shareholding_value = case when shareholding_value = 'a 22 letter placeholder ' or shareholding_value is null
--                                 then 'Unknown' else shareholding_value end
--
    ,FSS_V3_TYPE = case when FSS_V3_TYPE = 'placeholder placeholder' or FSS_V3_TYPE is null
                                then 'Unknown Sky' else FSS_V3_TYPE end

    ,FSS_V3_group = case when FSS_V3_group = 'placeholder placeholder' or FSS_V3_group is null
                                then 'Unknown Sky' else FSS_V3_group end

    ,household_affluence = case when household_affluence = 'Unknown' or household_affluence is null
                                then 'Unknown Sky' else household_affluence end

    ,social_grade = case when social_grade is null then 'Unknown Sky' else social_grade end

    ,Total_tv_deciles = case when Total_tv_deciles = 11 then 0 else Total_tv_deciles end



        -- DO SOMETHING ABOUT  -- desired_campaign_customer = 0





-- HAVE A LOOK LAT THE TABLE:
select top 100 * from v081_Vespa_Universe_demographics

-- lets do some high level checks - doesnt look good!
select count(*) from v081_Vespa_Universe_demographics where barclays_customer = 1
select count(*) from v081_Vespa_Universe_demographics where barclays_responder = 1


select distinct(fss_v3_group), count(*) from v081_Vespa_Universe_demographics group by fss_v3_group




--------------------------------------------------------------------------------
--  E02 - FLAG VARIABLES FOR THE BARCLAYS BASE
--------------------------------------------------------------------------------


------
-- STEP 1: create the base table (universe) and insert placeholders for values
------

-- make a copy of the response table and add a fields to sum sales
select *, n =1  into OM114_BARCLAYS_RESPONSE from om_prod.OM114_BARCLAYS_RESPONSE

select top 100 * from OM114_BARCLAYS_RESPONSE

-- the Barclays univlerse table will be slightly different than the Vespa Universe table.
-- in this table we want to take key barclays data - flag responders and detail response details and flag any customers that are also on the Vespa Panel:
-- now lets add all Barclays customers to the universe file - being carefull not to insert duplicates from above.

if object_id('v081_Barclays_Universe_demographics') is not null drop table v081_Barclays_Universe_demographics

select bc.cb_key_household
       ,max(bc.unique_identifier) as unique_identifier
       ,max(bc.segment) as segment
       ,max(bc.model_propensity_score) as model_propensity_score
       ,max(bc.model_propensity_percentile) as model_propensity_percentile
       ,max(bc.date_acquired) as date_acquired
       ,max(bc.desired_campaign_customer) as desired_campaign_customer
       ,max(bc.barclays_customer_before_campaign) as barclays_customer_before_campaign
       ,max(bc.barclays_ISA_before_campaign) as barclays_ISA_before_campaign
       ,max(bc.barclays_cash_ISA) as barclays_cash_ISA
       ,max(bc.unused_cash_ISA_balance) as unused_cash_ISA_balance
  --     ,max(bc.date_of_last_cash_ISA_balance) as date_of_last_cash_ISA_balance
       ,max(bc.home_branch) as home_branch
       ,Responder  = max(case when bc.Cb_Key_Household = res.Cb_Key_Household then 1 else 0 end)
       ,sum(res.n) as sales
       ,Vespa_panel = max(case when bc.Cb_Key_Household = ves.Cb_Key_Household then 1 else 0 end)

        -- responce data
        ,max(res.application_date) as max_application_date
        ,min(res.application_date) as min_application_date
        ,max(res.type_of_application) as type_of_application
        ,max(res.purchase) as purchase
        ,max(res.purchase_amount) as purchase_amount

         --experian variable
        ,household_composition = 'inserted to give correct length'
     --   ,shareholding_value = 'a 22 letter placeholder '
        ,FSS_V3_TYPE = 'placeholder placeholder'
        ,FSS_V3_group = 'placeholder placeholder'
        ,household_affluence = 'Unknown'
        ,social_grade = 'Unknown'--these are placeholders to prevent lengthly alter table statements later
        -- INCLUDE SOME VIEWING PROFILE: LIGHT/HEAVY/MEDIUM VIEIWNG

into v081_Barclays_Universe_demographics
from om_prod.OM114_BARCLAYS_CUSTOMER bc
 left join OM114_BARCLAYS_RESPONSE res
 on res.Cb_Key_Household = bc.Cb_Key_Household
 left join v081_Vespa_Universe_demographics ves
 on ves.Cb_Key_Household = bc.Cb_Key_Household
group by bc.Cb_Key_Household -- no account number



-- select count(*) from v081_Barclays_Universe_demographics
-- select count(distinct(cb_key_household)) from v081_Barclays_Universe_demographics

------
-- STEP 2: get customer dempgraphics from experians consumer view
------


-- lets put all of the experian consumer view fields into a new temp table in thier aggregated form;

drop table #barclays_experian_match

SELECT   CV.cb_key_household
        ,max(CASE WHEN CV.h_household_composition = '00' THEN 'Families'
                WHEN CV.h_household_composition = '01' THEN 'Extended family'
                WHEN CV.h_household_composition = '02' THEN 'Extended household'
                WHEN CV.h_household_composition = '03' THEN 'Pseudo family'
                WHEN CV.h_household_composition = '04' THEN 'Single male'
                WHEN CV.h_household_composition = '05' THEN 'Single female'
                WHEN CV.h_household_composition = '06' THEN 'Male homesharers'
                WHEN CV.h_household_composition = '07' THEN 'Female homesharers'
                WHEN CV.h_household_composition = '08' THEN 'Mixed homesharers'
                WHEN CV.h_household_composition = '09' THEN 'Abbreviated male families'
                WHEN CV.h_household_composition = '10' THEN 'Abbreviated female families'
                WHEN CV.h_household_composition = '11' THEN 'Multi-occupancy dwelling'
                WHEN CV.h_household_composition = 'U' THEN  'Unclassified'
            ELSE                                            'Unknown'            END) as household_composition

--         ,max(CASE WHEN CV.h_shareholding_value = '0' THEN 'No shares'
--                   WHEN CV.h_shareholding_value = '1' THEN 'Low value (<£10,000)'
--                   WHEN CV.h_shareholding_value = '2' THEN 'High value (>£10,000)'
--             ELSE                                         'Unknown'               END) as shareholding_value
--

        ,max (CASE WHEN cv.h_fss_v3_type  =         '01'   THEN     'Equity Ambitions'
                WHEN cv.h_fss_v3_type  =         '02'   THEN     'Portable Assets'
                WHEN cv.h_fss_v3_type  =         '03'   THEN     'Early Settlers'
                WHEN cv.h_fss_v3_type  =         '04'   THEN     'First Foundations'
                WHEN cv.h_fss_v3_type  =         '05'   THEN     'Urban Opportunities'
                WHEN cv.h_fss_v3_type  =         '06'   THEN     'Flexible Margins'
                WHEN cv.h_fss_v3_type  =         '07'   THEN     'Tomorrows Earners'
                WHEN cv.h_fss_v3_type  =         '08'   THEN     'Entry-level Workers'
                WHEN cv.h_fss_v3_type  =         '09'   THEN     'Cash Stretchers'
                WHEN cv.h_fss_v3_type  =         '10'   THEN     'Career Priorities'
                WHEN cv.h_fss_v3_type  =         '11'   THEN     'Upward Movers'
                WHEN cv.h_fss_v3_type  =         '12'   THEN     'Family Progression'
                WHEN cv.h_fss_v3_type  =         '13'   THEN     'Savvy Switchers'
                WHEN cv.h_fss_v3_type  =         '14'   THEN     'New Nesters'
                WHEN cv.h_fss_v3_type  =         '15'   THEN     'Security Seekers'
                WHEN cv.h_fss_v3_type  =         '16'   THEN     'Premier Portfolios'
                WHEN cv.h_fss_v3_type  =         '17'   THEN     'Fast-track Fortunes'
                WHEN cv.h_fss_v3_type  =         '18'   THEN     'Asset Accruers'
                WHEN cv.h_fss_v3_type  =         '19'   THEN     'Self-made Success'
                WHEN cv.h_fss_v3_type  =         '20'   THEN     'Golden Outlook'
                WHEN cv.h_fss_v3_type  =         '21'   THEN     'Sound Positions'
                WHEN cv.h_fss_v3_type  =         '22'   THEN     'Single Accumulators'
                WHEN cv.h_fss_v3_type  =         '23'   THEN     'Mid-range Gains'
                WHEN cv.h_fss_v3_type  =         '24'   THEN     'Extended Outlay'
                WHEN cv.h_fss_v3_type  =         '25'   THEN     'Modest Mortgages'
                WHEN cv.h_fss_v3_type  =         '26'   THEN     'Overworked Resources'
                WHEN cv.h_fss_v3_type  =         '27'   THEN     'Self-reliant Realists'
                WHEN cv.h_fss_v3_type  =         '28'   THEN     'Canny Owners'
                WHEN cv.h_fss_v3_type  =         '29'   THEN     'Squeezed Families'
                WHEN cv.h_fss_v3_type  =         '30'   THEN     'Pooled Kitty'
                WHEN cv.h_fss_v3_type  =         '31'   THEN     'High Demands'
                WHEN cv.h_fss_v3_type  =         '32'   THEN     'Value Hunters'
                WHEN cv.h_fss_v3_type  =         '33'   THEN     'Low Cost Living'
                WHEN cv.h_fss_v3_type  =         '34'   THEN     'Guaranteed Provision'
                WHEN cv.h_fss_v3_type  =         '35'   THEN     'Steady Savers'
                WHEN cv.h_fss_v3_type  =         '36'   THEN     'Deferred Assurance'
                WHEN cv.h_fss_v3_type  =         '37'   THEN     'Practical Preparers'
                WHEN cv.h_fss_v3_type  =         '38'   THEN     'Persistent Workers'
                WHEN cv.h_fss_v3_type  =         '39'   THEN     'Lifelong Low-spenders'
                WHEN cv.h_fss_v3_type  =         '40'   THEN     'Experienced Renters'
                WHEN cv.h_fss_v3_type  =         '41'   THEN     'Sage Investors'
                WHEN cv.h_fss_v3_type  =         '42'   THEN     'Dignified Elders'
                WHEN cv.h_fss_v3_type  =         '43'   THEN     'Comfortable Legacy'
                WHEN cv.h_fss_v3_type  =         '44'   THEN     'Semi-retired Families'
                WHEN cv.h_fss_v3_type  =         '45'   THEN     'Cautious Stewards'
                WHEN cv.h_fss_v3_type  =         '46'   THEN     'Classic Moderation'
                WHEN cv.h_fss_v3_type  =         '47'   THEN     'Quiet Simplicity'
                WHEN cv.h_fss_v3_type  =         '48'   THEN     'Senior Sufficiency'
                WHEN cv.h_fss_v3_type  =         '49'   THEN     'Ageing Fortitude'
                WHEN cv.h_fss_v3_type  =         '50'   THEN     'State Veterans'
                WHEN cv.h_fss_v3_type  =         '99'   THEN     'Unallocated'
            ELSE                                                 'Unknown'               END) as FSS_V3_TYPE


     ,max (CASE WHEN cv.h_fss_v3_group  =        'A'    THEN     'Accumulated Wealth'
                WHEN cv.h_fss_v3_group  =        'B'    THEN     'Balancing Budgets'
                WHEN cv.h_fss_v3_group  =        'C'    THEN     'Bright Futures'
                WHEN cv.h_fss_v3_group  =        'D'    THEN     'Consolidating Assets'
                WHEN cv.h_fss_v3_group  =        'E'    THEN     'Established Reserves'
                WHEN cv.h_fss_v3_group  =        'F'    THEN     'Family Interest'
                WHEN cv.h_fss_v3_group  =        'G'    THEN     'Growing Rewards'
                WHEN cv.h_fss_v3_group  =        'H'    THEN     'Platinum Pensions'
                WHEN cv.h_fss_v3_group  =        'I'    THEN     'Seasoned Economy'
                WHEN cv.h_fss_v3_group  =        'J'    THEN     'Single Endeavours'
                WHEN cv.h_fss_v3_group  =        'K'    THEN     'Stretched Finances'
                WHEN cv.h_fss_v3_group  =        'L'    THEN     'Sunset Security'
                WHEN cv.h_fss_v3_group  =        'M'    THEN     'Traditional Thrift'
                WHEN cv.h_fss_v3_group  =        'N'    THEN     'Young Essentials'
                WHEN cv.h_fss_v3_group  =        'U'    THEN     'Unallocated'
          ELSE                                                 'Unknown'               END) as FSS_V3_group

     ,max(h_affluence_v2)    as household_affluence   -- *** this will be allocated into bandings later - standardised for all future analysis

INTO #barclays_experian_match
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv
where cb_key_household in (select distinct(cb_key_household) from v081_barclays_Universe_demographics)
GROUP BY CV.cb_key_household;
-- appears to be >85% match rate



------
-- STEP 3: get customers Social grade from CACI tables
------

drop table #caci_sc

select  c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into #caci_sc
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
  and c.cb_key_household in (select cb_key_household from v081_Barclays_Universe_demographics)
order by c.cb_key_household;


--de-dupe!
delete from #caci_sc where rank_id > 1  -- more than half!


select count(*) from #caci_sc where social_grade <> 'Unknown'



------
-- STEP 4: copy all of the above aggregated data into the base table
------

-- first update for experian

update v081_Barclays_Universe_demographics
set ud.household_composition = tmp.household_composition
 --  ,ud.shareholding_value = tmp.shareholding_value
   ,ud.FSS_V3_TYPE = tmp.FSS_V3_TYPE
   ,ud.FSS_V3_group = tmp.FSS_V3_group
   ,ud.household_affluence = tmp.household_affluence
--   ,ud.social_grade = cac.social_grade
from v081_Barclays_Universe_demographics ud
 join #barclays_experian_match tmp
on tmp.cb_key_household = ud.cb_key_household
--  join #caci_sc cac
-- on cac.cb_key_household = ud.cb_key_household



-- then update for caci

update v081_Barclays_Universe_demographics
set ud.social_grade = cac.social_grade
from v081_Barclays_Universe_demographics ud
  join #caci_sc cac
on cac.cb_key_household = ud.cb_key_household
where cac.social_grade is not null



-- Next update each vespa customers vieiwng profile: high/med/low (total TV deviation from UK Sky average)
--
-- update v081_Barclays_Universe_demographics
-- set ud.Total_tv_deciles = cac.Total_tv_deciles
-- from v081_Barclays_Universe_demographics ud
--  join customer_deciles_108 cac
-- on cac.account_number = ud.account_number
--

--have a look:
select top 10 * from v081_Barclays_Universe_demographics




------
-- STEP 5: ensure no nulls and change placeholder values where they still exist!
------

update v081_Barclays_Universe_demographics
set household_composition = case when household_composition = 'inserted to give correct length' or household_composition is null
                                 then 'Unknown Sky' else household_composition end

--     ,shareholding_value = case when shareholding_value = 'a 22 letter placeholder ' or shareholding_value is null
--                                 then 'Unknown' else shareholding_value end

    ,FSS_V3_TYPE = case when FSS_V3_TYPE = 'placeholder placeholder' or FSS_V3_TYPE is null
                                then 'Unknown Sky' else FSS_V3_TYPE end

    ,FSS_V3_group = case when FSS_V3_group = 'placeholder placeholder' or FSS_V3_group is null
                                then 'Unknown Sky' else FSS_V3_group end

    ,household_affluence = case when household_affluence = 'Unknown' or household_affluence is null
                                then 'Unknown Sky' else household_affluence end

    ,social_grade = case when social_grade is null then 'Unknown Sky' else social_grade end

   -- ,Total_tv_deciles = case when Total_tv_deciles is null then 'Unknown' else Total_tv_deciles end


/* QA--  HAVE A LOOK AT THE TABLE:
select top 100 * from v081_Barclays_Universe_demographics

select count(*) from v081_Barclays_Universe_demographics where social_grade = 'Unknown' -- 237

*/




---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------

/*
THIS CODE IS PART 1: IT HAS IDENTIFIED ALL SPOTS WATCHED, DECILED CUSTOMERS ON THIER CAPPED VIEWING AND IDENTIFIED ALL OTHER REQUIRED
CUSTOMER DEMOGRAPHICS NEEDED FOR THE ROLL UPS WHICH ARE GOING TO BE COMPLETED IN PART 2.

SPOTS DATA IS AVAILABLE IN :  Barclays_spots_viewing_table_dump

VIEWING IS AVAILABLE IN TABLE: Project_108_customer_viewing_capped
THE CONSISTENT PANEL: consistent_vespa_universe3

CUSTOMER DATA:

                1. v081_Vespa_Universe_demographics
                2. v081_Barclays_Universe_demographics



-----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

Stage 1 Roll ups

For this part we want to understand the number of customer from Barclay that match Sky and the vespa panel.

We want to identify distributions of demogrphaics collected above
*/


-------------------------------------------------------------------------------------------------------------
-- 1: what are volumes and match rates
-------------------------------------------------------------------------------------------------------------


-- a: lets find all active Sky customers as of the start of the campaign; 29th Feb 2012


     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
       INTO v108_active_customer_base
       FROM sk_prod.cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @var_prog_period_start --'2012-02-29'
        AND effective_to_dt      > @var_prog_period_start
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL
--9935284 Row(s) affected

-- remove duplicates
delete from v108_active_customer_base where rank > 1

-- we only want to keep UK accounts

   UPDATE v108_active_customer_base
     SET
         uk_standard_account = CASE
             WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
             ELSE 0 END
     FROM v108_active_customer_base AS a
     inner join sk_prod.cust_single_account_view AS b
     ON a.account_number = b.account_number

     DELETE FROM v108_active_customer_base WHERE uk_standard_account = 0

     COMMIT

-- do a quick check --
select count(*) from v108_active_customer_base -- 9,375,559 -- UK accounts active at start of campaign
select count(distinct(cb_key_household)) from v108_active_customer_base


select top 10 * from v108_active_customer_base

-- b. do the counts and roll ups

-- some variables are needed for somes
create variable @vespa_base integer;
set @vespa_base = (select count(distinct(cb_key_household)) from v081_VESPA_Universe_demographics)


-- drop table #output1

select count (distinct(bar.cb_key_household)) as barclays_customers
        ,barclays_responders_individuals = (select count(cb_key_individual) from om_prod.OM114_BARCLAYS_RESPONSE)
       ,count (distinct(case when responder = 1 then bar.cb_key_household else null end)) as barclays_responders_households
       ,count (distinct(case when bar.cb_key_household = sky.cb_key_household then sky.cb_key_household else null end)) as Barclays_Sky_Household_Match
        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household and responder = 1 then sky.cb_key_household else null end)) as Barclays_Sky_responder_Household_Match
    --   ,not_matched_barclays_sky = (barclays_customers - Barclays_Sky_Household_Match)

       ,count(distinct (case when bar.cb_key_household = ves.cb_key_household then ves.cb_key_household else null end)) as Barclays_Vespa_household_Match
        ,count(distinct(case when bar.cb_key_household = ves.cb_key_household and responder = 1 then ves.cb_key_household else null end)) as Barclays_Vespa_responder_household_Match
    --   ,not_matched_barclays_vespa = (@vespa_base - Barclays_Vespa_household_Match)
into #output1
from v081_Barclays_Universe_demographics as bar
     left join v108_active_customer_base as sky
     on sky.cb_key_household = bar.cb_key_household
     left join v081_VESPA_Universe_demographics as ves
     on bar.cb_key_household = ves.cb_key_household

-- 547 barclays and Sky


-------------- OUTPUT --------------------
------------------------------------------
        select * from #output1
------------------------------------------

-------------------------------------------------------------------------------------------------------------
-- 2: this is a placeholder for 2
-------------------------------------------------------------------------------------------------------------


-- drop table #output2


        -- the sky_accounts
select count (distinct(case when bar.cb_key_household = sky.cb_key_household
                and bar.barclays_customer_before_campaign = 1 then sky.cb_key_household else null end)) as barclays_customer_before_campaign

        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
        and bar.barclays_ISA_before_campaign = 1 then sky.cb_key_household else null end)) as barclays_ISA_before_campaign

        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
        and bar.barclays_cash_isa = 1 then sky.cb_key_household else null end)) as barclays_cash_isa

        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
        and bar.social_grade in ('A','B','C1') then sky.cb_key_household else null end)) as _Brought_audience_ABC1


-- Sky and responders
  ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
  and bar.barclays_customer_before_campaign = 1 and responder = 1 then sky.cb_key_household else null end)) as barclays_customer_before_campaign_bar_res

        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
        and bar.barclays_ISA_before_campaign = 1 and responder = 1 then sky.cb_key_household else null end)) as barclays_ISA_before_campaign_bar_res

        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
        and bar.barclays_cash_isa = 1 and responder = 1 then sky.cb_key_household else null end)) as barclays_cash_isa_bar_res

        ,count (distinct(case when bar.cb_key_household = sky.cb_key_household
        and bar.social_grade in ('A','B','C1') and responder = 1 then sky.cb_key_household else null end)) as _Brought_audience_ABC1_bar_res


-- vespa

       ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
       and bar.barclays_customer_before_campaign = 1 then ves.cb_key_household else null end)) as barclays_customer_before_campaign_ves

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.barclays_ISA_before_campaign = 1 then ves.cb_key_household else null end)) as barclays_ISA_before_campaign_ves

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.barclays_cash_isa = 1 then ves.cb_key_household else null end)) as barclays_cash_isa_ves

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.social_grade in ('A','B','C1') then ves.cb_key_household else null end)) as _Brought_audience_ABC1_ves


-- Vespa and responders
  ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
  and bar.barclays_customer_before_campaign = 1 and responder = 1 then ves.cb_key_household else null end)) as barclays_customer_before_campaign_ves_res

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.barclays_ISA_before_campaign = 1 and responder = 1 then ves.cb_key_household else null end)) as barclays_ISA_before_campaign_ves_res

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.barclays_cash_isa = 1 and responder = 1 then ves.cb_key_household else null end)) as barclays_cash_isa_ves_res

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.social_grade in ('A','B','C1') and responder = 1 then ves.cb_key_household else null end)) as _Brought_audience_ABC1_ves_res

into #output2
from v081_Barclays_Universe_demographics as bar
     left join v108_active_customer_base as sky
     on sky.cb_key_household = bar.cb_key_household
     left join v081_VESPA_Universe_demographics as ves
     on bar.cb_key_household = ves.cb_key_household


-------------- OUTPUT --------------------
------------------------------------------
        select * from #output2
------------------------------------------




-------------------------------------------------------------------------------------------------------------
-- 3: what are the match rates from segments
-------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------
-- 4: campaigns and spots - whats the breakdown
-------------------------------------------------------------------------------------------------------------



select
        number_of_TV_camapigns =1
        ,number_of_spots_aired = (select count(*) from barclays_spots)
        ,number_of_spots_watched = (select count(distinct(spot_identifier)) from Barclays_spots_viewing_table_dump )
into #output4;


--- first spots output section
select * from #output4



--- this is the seconds spots output
select top 10 * from barclays_spots

select distinct(local_date_of_transmission) as dates, count(distinct(identifier)) as spots_aired
  from barclays_spots
group by dates
order by dates

-- the below was added as the project was coming to a close:

-- Lets find out how many spots were watched each day;

ALTER TABLE Barclays_spots_viewing_table_dump
ADD SPOT_AIR_DATE AS DATE

UPDATE Barclays_spots_viewing_table_dump
        SET SPOT_AIR_DATE = local_date_of_transmission
FROM Barclays_spots_viewing_table_dump
JOIN barclays_spots
ON identifier = SPOT_IDENTIFIER



-- do the count -- how many spots were watched each day??

select viewing_date
        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date then spot_identifier else null end)) as Spots_viewed_aired_today
        ,count(distinct(case when whole_spot = 1 then spot_identifier else null end)) as distinct_Spots_viewed_incl_pb
        ,sum(case when whole_spot = 1 then whole_spot else null end) as spot_impacts_panel
        ,sum(whole_spot * weighting) as spots_impcats_Sky
from Barclays_spots_viewing_table_dump
group by viewing_date
order by viewing_date

select top 10 * from Barclays_spots_viewing_table_dump






select top 10 * from barclays_spots


 select distinct(channel_name) from barclays_spots order by channel_name






-------------- OUTPUT --------------------
------------------------------------------
        select * from #output4
------------------------------------------





-------------------------------------------------------------------------------------------------------------
-- 5: distributions -- household composition
-------------------------------------------------------------------------------------------------------------


------
-- a. Get a table with all household comp possibilities
-------

-- we want the table to include all the possible household compositions --
select distinct(CASE WHEN CV.h_household_composition = '00' THEN 'Families'
                WHEN CV.h_household_composition = '01' THEN 'Extended family'
                WHEN CV.h_household_composition = '02' THEN 'Extended household'
                WHEN CV.h_household_composition = '03' THEN 'Pseudo family'
                WHEN CV.h_household_composition = '04' THEN 'Single male'
                WHEN CV.h_household_composition = '05' THEN 'Single female'
                WHEN CV.h_household_composition = '06' THEN 'Male homesharers'
                WHEN CV.h_household_composition = '07' THEN 'Female homesharers'
                WHEN CV.h_household_composition = '08' THEN 'Mixed homesharers'
                WHEN CV.h_household_composition = '09' THEN 'Abbreviated male families'
                WHEN CV.h_household_composition = '10' THEN 'Abbreviated female families'
                WHEN CV.h_household_composition = '11' THEN 'Multi-occupancy dwelling'
                WHEN CV.h_household_composition = 'U' THEN  'Unclassified'
            ELSE                                            'Unknown'            END) as household_composition

        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #output5
from sk_prod.EXPERIAN_CONSUMERVIEW cv

-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #output5(household_composition,No_barclays_Households,No_vespa_barclays_Households) values('Unknown',99.99999,99.99999)


------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH HOUSEHOLD COMP GROUP
-------


-- we will need the total population
create variable @barclays_base integer;
create variable @vespa_base integer;

set @barclays_base = (select count(distinct(cb_key_household)) from v081_BARCLAYS_Universe_demographics)
set @vespa_base = (select count(distinct(cb_key_household)) from v081_VESPA_Universe_demographics where barclays_customer = 1) -- this was created earlier


-- determine barclays household comp distribution
select bar.household_composition
        ,No_barclays_Households = cast(count(distinct(bar.cb_key_household))as float)/cast(@barclays_base as float)
into #barclays_households
from v081_BARCLAYS_Universe_demographics bar
group by bar.household_composition
order by bar.household_composition

-- determine vespa and barclays customers household comp distributionb
select ves.household_composition
        ,No_Vespa_barclays_Households = cast(count(distinct(ves.cb_key_household))as float)/cast(@vespa_base as float)
into #vespa_households
from v081_vespa_Universe_demographics ves
where barclays_customer = 1
group by ves.household_composition
order by ves.household_composition



-- now insert the distributions into the table holding all possble household comps
update #output5
        set tab.No_barclays_Households = case when tab.household_composition = bar.household_composition then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.household_composition = ves.household_composition then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #output5 tab
left join #barclays_households bar
on tab.household_composition = bar.household_composition
left join #vespa_households ves
on tab.household_composition = ves.household_composition


-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #output5
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end



-------------- OUTPUT --------------------
------------------------------------------
        select * from #output5 order by household_composition
------------------------------------------




-------------------------------------------------------------------------------------------------------------
-- 6: distributions -- FFS TYPE
-------------------------------------------------------------------------------------------------------------


------
-- a. Get a table with all possibilities
-------

-- we want the table to include all the possible household compositions --
select distinct (CASE WHEN cv.h_fss_v3_type  =         '01'   THEN     'Equity Ambitions'
                WHEN cv.h_fss_v3_type  =         '02'   THEN     'Portable Assets'
                WHEN cv.h_fss_v3_type  =         '03'   THEN     'Early Settlers'
                WHEN cv.h_fss_v3_type  =         '04'   THEN     'First Foundations'
                WHEN cv.h_fss_v3_type  =         '05'   THEN     'Urban Opportunities'
                WHEN cv.h_fss_v3_type  =         '06'   THEN     'Flexible Margins'
                WHEN cv.h_fss_v3_type  =         '07'   THEN     'Tomorrows Earners'
                WHEN cv.h_fss_v3_type  =         '08'   THEN     'Entry-level Workers'
                WHEN cv.h_fss_v3_type  =         '09'   THEN     'Cash Stretchers'
                WHEN cv.h_fss_v3_type  =         '10'   THEN     'Career Priorities'
                WHEN cv.h_fss_v3_type  =         '11'   THEN     'Upward Movers'
                WHEN cv.h_fss_v3_type  =         '12'   THEN     'Family Progression'
                WHEN cv.h_fss_v3_type  =         '13'   THEN     'Savvy Switchers'
                WHEN cv.h_fss_v3_type  =         '14'   THEN     'New Nesters'
                WHEN cv.h_fss_v3_type  =         '15'   THEN     'Security Seekers'
                WHEN cv.h_fss_v3_type  =         '16'   THEN     'Premier Portfolios'
                WHEN cv.h_fss_v3_type  =         '17'   THEN     'Fast-track Fortunes'
                WHEN cv.h_fss_v3_type  =         '18'   THEN     'Asset Accruers'
                WHEN cv.h_fss_v3_type  =         '19'   THEN     'Self-made Success'
                WHEN cv.h_fss_v3_type  =         '20'   THEN     'Golden Outlook'
                WHEN cv.h_fss_v3_type  =         '21'   THEN     'Sound Positions'
                WHEN cv.h_fss_v3_type  =         '22'   THEN     'Single Accumulators'
                WHEN cv.h_fss_v3_type  =         '23'   THEN     'Mid-range Gains'
                WHEN cv.h_fss_v3_type  =         '24'   THEN     'Extended Outlay'
                WHEN cv.h_fss_v3_type  =         '25'   THEN     'Modest Mortgages'
                WHEN cv.h_fss_v3_type  =         '26'   THEN     'Overworked Resources'
                WHEN cv.h_fss_v3_type  =         '27'   THEN     'Self-reliant Realists'
                WHEN cv.h_fss_v3_type  =         '28'   THEN     'Canny Owners'
                WHEN cv.h_fss_v3_type  =         '29'   THEN     'Squeezed Families'
                WHEN cv.h_fss_v3_type  =         '30'   THEN     'Pooled Kitty'
                WHEN cv.h_fss_v3_type  =         '31'   THEN     'High Demands'
                WHEN cv.h_fss_v3_type  =         '32'   THEN     'Value Hunters'
                WHEN cv.h_fss_v3_type  =         '33'   THEN     'Low Cost Living'
                WHEN cv.h_fss_v3_type  =         '34'   THEN     'Guaranteed Provision'
                WHEN cv.h_fss_v3_type  =         '35'   THEN     'Steady Savers'
                WHEN cv.h_fss_v3_type  =         '36'   THEN     'Deferred Assurance'
                WHEN cv.h_fss_v3_type  =         '37'   THEN     'Practical Preparers'
                WHEN cv.h_fss_v3_type  =         '38'   THEN     'Persistent Workers'
                WHEN cv.h_fss_v3_type  =         '39'   THEN     'Lifelong Low-spenders'
                WHEN cv.h_fss_v3_type  =         '40'   THEN     'Experienced Renters'
                WHEN cv.h_fss_v3_type  =         '41'   THEN     'Sage Investors'
                WHEN cv.h_fss_v3_type  =         '42'   THEN     'Dignified Elders'
                WHEN cv.h_fss_v3_type  =         '43'   THEN     'Comfortable Legacy'
                WHEN cv.h_fss_v3_type  =         '44'   THEN     'Semi-retired Families'
                WHEN cv.h_fss_v3_type  =         '45'   THEN     'Cautious Stewards'
                WHEN cv.h_fss_v3_type  =         '46'   THEN     'Classic Moderation'
                WHEN cv.h_fss_v3_type  =         '47'   THEN     'Quiet Simplicity'
                WHEN cv.h_fss_v3_type  =         '48'   THEN     'Senior Sufficiency'
                WHEN cv.h_fss_v3_type  =         '49'   THEN     'Ageing Fortitude'
                WHEN cv.h_fss_v3_type  =         '50'   THEN     'State Veterans'
                WHEN cv.h_fss_v3_type  =         '99'   THEN     'Unallocated'
            ELSE                                                 'Unknown'               END) as FSS_V3_TYPE
        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #output6
from sk_prod.EXPERIAN_CONSUMERVIEW cv


-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #output6(FSS_V3_TYPE,No_barclays_Households,No_vespa_barclays_Households) values('Unknown',99.99999,99.99999)



------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP
-------



-- determine barclays distribution
select bar.FSS_V3_TYPE
        ,No_barclays_Households = cast(count(distinct(bar.cb_key_household))as float)/cast(@barclays_base as float)
into #barclays_households2
from v081_BARCLAYS_Universe_demographics bar
group by bar.FSS_V3_TYPE
order by bar.FSS_V3_TYPE;

-- determine vespa and barclays customers distribution
select ves.FSS_V3_TYPE
        ,No_Vespa_barclays_Households = cast(count(distinct(ves.cb_key_household))as float)/cast(@vespa_base as float)
into #vespa_households2
from v081_vespa_Universe_demographics ves
where barclays_customer = 1
group by ves.FSS_V3_TYPE
order by ves.FSS_V3_TYPE;


-- now insert the distributions into the table holding all possble options
update #output6
        set tab.No_barclays_Households = case when tab.FSS_V3_TYPE = bar.FSS_V3_TYPE then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.FSS_V3_TYPE = ves.FSS_V3_TYPE then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #output6 tab
left join #barclays_households2 bar
on tab.FSS_V3_TYPE = bar.FSS_V3_TYPE
left join #vespa_households2 ves
on tab.FSS_V3_TYPE = ves.FSS_V3_TYPE




-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #output6
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end




-------------- OUTPUT --------------------
------------------------------------------
        select * from #output6 order by FSS_V3_TYPE
------------------------------------------







-------------------------------------------------------------------------------------------------------------
-- xx: Affluence --
-------------------------------------------------------------------------------------------------------------

drop table #output_affluence

-- we want the table to include all the possible household compositions --
select distinct (CASE WHEN h_affluence_v2 IN ('00','01','02')       THEN 'A) Very Low'
                        WHEN h_affluence_v2 IN ('03','04', '05')      THEN 'B) Low'
                        WHEN h_affluence_v2 IN ('06','07','08')       THEN 'C) Mid Low'
                        WHEN h_affluence_v2 IN ('09','10','11')       THEN 'D) Mid'
                        WHEN h_affluence_v2 IN ('12','13','14')       THEN 'E) Mid High'
                        WHEN h_affluence_v2 IN ('15','16','17')       THEN 'F) High'
                        WHEN h_affluence_v2 IN ('18','19')            THEN 'G) Very High'
            ELSE                                                 'Unknown'               END) as Affluence
        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #output_affluence
from sk_prod.EXPERIAN_CONSUMERVIEW cv


-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #output_affluence(affluence,No_barclays_Households,No_vespa_barclays_Households) values('Unknown',99.99999,99.99999)



------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP
-------



-- determine barclays distribution
select (CASE WHEN bar.household_affluence  IN ('00','01','02')       THEN 'A) Very Low'
                        WHEN  bar.household_affluence IN ('03','04', '05')      THEN 'B) Low'
                        WHEN  bar.household_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                        WHEN  bar.household_affluence IN ('09','10','11')       THEN 'D) Mid'
                        WHEN  bar.household_affluence IN ('12','13','14')       THEN 'E) Mid High'
                        WHEN  bar.household_affluence IN ('15','16','17')       THEN 'F) High'
                        WHEN  bar.household_affluence IN ('18','19')            THEN 'G) Very High'
            ELSE                                                 'Unknown'               END) as Affluence
        ,No_barclays_Households = cast(count(distinct(bar.cb_key_household))as float)/cast(@barclays_base as float)
into #barclays_households_aff
from v081_BARCLAYS_Universe_demographics bar
group by Affluence
order by Affluence;



-- determine vespa and barclays customers distribution
select (CASE WHEN ves.household_affluence  IN ('00','01','02')       THEN 'A) Very Low'
                        WHEN  ves.household_affluence IN ('03','04', '05')      THEN 'B) Low'
                        WHEN  ves.household_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                        WHEN  ves.household_affluence IN ('09','10','11')       THEN 'D) Mid'
                        WHEN  ves.household_affluence IN ('12','13','14')       THEN 'E) Mid High'
                        WHEN  ves.household_affluence IN ('15','16','17')       THEN 'F) High'
                        WHEN  ves.household_affluence IN ('18','19')            THEN 'G) Very High'
            ELSE                                                 'Unknown'               END) as Affluence

        ,No_Vespa_barclays_Households = cast(count(distinct(ves.cb_key_household))as float)/cast(@vespa_base as float)

into #vespa_households2_aff
from v081_vespa_Universe_demographics ves
where barclays_customer = 1
group by Affluence
order by Affluence;


-- now insert the distributions into the table holding all possble options
update #output_affluence
        set tab.No_barclays_Households = case when tab.Affluence = bar.Affluence then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.Affluence = ves.Affluence then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #output_affluence tab
left join #barclays_households_aff bar
on tab.Affluence = bar.Affluence
left join #vespa_households2_aff ves
on tab.Affluence = ves.Affluence




-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #output_affluence
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end





-------------- OUTPUT --------------------
------------------------------------------
        select * from #output_affluence order by Affluence
------------------------------------------







-------------------------------------------------------------------------------------------------------------
-- 7: distributions -- FFS GROUP
-------------------------------------------------------------------------------------------------------------


------
-- a. Get a table with all possibilities
-------


-- we want the table to include all the possible options --
select distinct  (CASE WHEN cv.h_fss_v3_group  =        'A'    THEN     'Accumulated Wealth'
                WHEN cv.h_fss_v3_group  =        'B'    THEN     'Balancing Budgets'
                WHEN cv.h_fss_v3_group  =        'C'    THEN     'Bright Futures'
                WHEN cv.h_fss_v3_group  =        'D'    THEN     'Consolidating Assets'
                WHEN cv.h_fss_v3_group  =        'E'    THEN     'Established Reserves'
                WHEN cv.h_fss_v3_group  =        'F'    THEN     'Family Interest'
                WHEN cv.h_fss_v3_group  =        'G'    THEN     'Growing Rewards'
                WHEN cv.h_fss_v3_group  =        'H'    THEN     'Platinum Pensions'
                WHEN cv.h_fss_v3_group  =        'I'    THEN     'Seasoned Economy'
                WHEN cv.h_fss_v3_group  =        'J'    THEN     'Single Endeavours'
                WHEN cv.h_fss_v3_group  =        'K'    THEN     'Stretched Finances'
                WHEN cv.h_fss_v3_group  =        'L'    THEN     'Sunset Security'
                WHEN cv.h_fss_v3_group  =        'M'    THEN     'Traditional Thrift'
                WHEN cv.h_fss_v3_group  =        'N'    THEN     'Young Essentials'
                WHEN cv.h_fss_v3_group  =        'U'    THEN     'Unallocated'
          ELSE                                                 'Unknown'               END) as FSS_V3_group

        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #output7
from sk_prod.EXPERIAN_CONSUMERVIEW cv


-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #output7(FSS_V3_group,No_barclays_Households,No_vespa_barclays_Households) values('Unknown Sky',99.99999,99.99999)


select * from #output7

------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP
-------



-- determine barclays distribution
select bar.FSS_V3_group
        ,No_barclays_Households = cast(count(distinct(bar.cb_key_household))as float)/cast(@barclays_base as float)
into #barclays_households3
from v081_BARCLAYS_Universe_demographics bar
group by bar.FSS_V3_group
order by bar.FSS_V3_group;

-- determine vespa and barclays customers distribution
select ves.FSS_V3_group
        ,No_Vespa_barclays_Households = cast(count(distinct(ves.cb_key_household))as float)/cast(@vespa_base as float)
into #vespa_households3
from v081_vespa_Universe_demographics ves
where barclays_customer = 1
group by ves.FSS_V3_group
order by ves.FSS_V3_group;


-- now insert the distributions into the table holding all possble options
update #output7
        set tab.No_barclays_Households = case when tab.FSS_V3_group = bar.FSS_V3_group then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.FSS_V3_group = ves.FSS_V3_group then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #output7 tab
left join #barclays_households3 bar
on tab.FSS_V3_group = bar.FSS_V3_group
left join #vespa_households3 ves
on tab.FSS_V3_group = ves.FSS_V3_group




-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #output7
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end




-------------- OUTPUT --------------------
------------------------------------------
        select * from #output7 order by FSS_V3_group
------------------------------------------




-------------------------------------------------------------------------------------------------------------
-- 8: distributions -- household affluence
-------------------------------------------------------------------------------------------------------------

/******************* ALL THAT IS

------
-- a. Get a table with all possibilities
-------


-- we want the table to include all the possible options --
select distinct

                -- INSERT AFFLUENCE CASE WHEN STATEMENTS --

        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #output8
from sk_prod.EXPERIAN_CONSUMERVIEW cv


-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #output8(household_affluence,No_barclays_Households,No_vespa_barclays_Households) values('Unknown',99.99999,99.99999)



------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP
-------



-- determine barclays distribution
select bar.household_affluence
        ,No_barclays_Households = cast(count(distinct(bar.cb_key_household))as float)/cast(@barclays_base as float)
into #barclays_households4
from v081_BARCLAYS_Universe_demographics bar
group by bar.household_affluence
order by bar.household_affluence;

-- determine vespa and barclays customers distribution
select ves.household_affluence
        ,No_Vespa_barclays_Households = cast(count(distinct(ves.cb_key_household))as float)/cast(@vespa_base as float)
into #vespa_households4
from v081_vespa_Universe_demographics ves
where barclays_customer = 1
group by ves.household_affluence
order by ves.household_affluence;


-- now insert the distributions into the table holding all possble options
update #output8
        set tab.No_barclays_Households = case when tab.household_affluence = bar.household_affluence then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.household_affluence = ves.household_affluence then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #output8 tab
left join #barclays_households4 bar
on tab.household_affluence = bar.household_affluence
left join #vespa_households4 ves
on tab.household_affluence = ves.household_affluence




-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #output8
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end




-------------- OUTPUT --------------------
------------------------------------------
        select * from #output8 order by household_affluence
------------------------------------------

*/






-------------------------------------------------------------------------------------------------------------
-- 9: distributions -- Social Grade
-------------------------------------------------------------------------------------------------------------


------
-- a. Get a table with all possibilities
-------

drop table #output9

-- we want the table to include all the possible options --
select distinct(lukcat_fr_de_nrs) AS social_grade1
        ,case when social_grade1 is null then 'Unknown' else social_grade1 end as social_grade
        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #output9
from sk_prod.CACI_SOCIAL_CLASS

select * from #output9

-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #output9(social_grade,No_barclays_Households,No_vespa_barclays_Households) values('Unknown',99.99999,99.99999)


------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP (socail grade)
-------



-- determine barclays distribution
select bar.social_grade
        ,No_barclays_Households = cast(count(distinct(bar.cb_key_household))as float)/cast(@barclays_base as float)
into #barclays_households5
from v081_BARCLAYS_Universe_demographics bar
group by bar.social_grade
order by bar.social_grade;

-- determine vespa and barclays customers distribution
select ves.social_grade
        ,No_Vespa_barclays_Households = cast(count(distinct(ves.cb_key_household))as float)/cast(@vespa_base as float)
into #vespa_households5
from v081_vespa_Universe_demographics ves
where barclays_customer = 1
group by ves.social_grade
order by ves.social_grade;


-- now insert the distributions into the table holding all possble options
update #output9
        set tab.No_barclays_Households = case when tab.social_grade = bar.social_grade then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.social_grade = ves.social_grade then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #output9 tab
left join #barclays_households5 bar
on tab.social_grade = bar.social_grade
left join #vespa_households5 ves
on tab.social_grade = ves.social_grade




-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #output9
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end




-------------- OUTPUT --------------------
------------------------------------------
        select social_grade, No_barclays_Households, No_vespa_barclays_Households  from #output9 order by social_grade
------------------------------------------






















----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
--***************************************************************************************************************************************
-- THIS PART DOES NOT FORM PART OF TEMPLATE 1 *******************************************************************************************
--***************************************************************************************************************************************
----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
-- THE BITS IN THE CODE BELOW CAM FROM ADHOC REQUESTS FOLLOWING MEETING FROM WB 19/NOVEMBER 2012


-- wE ARE INTERESTED IN POTENTIAL TARGET CUSTOMERS FOR THE BARCLAYS CAMPAIGN AS THEY HAVE REMOVED THE DESIRED CUSTOMER FLAG.

-- WE WILL


select top 10 * from v081_Barclays_Universe_demographics




-- get the barclays volumes splits
select
       count(distinct(cb_key_household)) as Barclays_households
       ,count (distinct(case when  barclays_ISA_before_campaign = 1  then cb_key_household else null end)) as has_an_isa
       ,count (distinct(case when barclays_cash_isa = 1  then cb_key_household else null end)) as has_cash_isa
from v081_Barclays_Universe_demographics






-------------------------------------------------------------------------------------------------------------
-- xxxxx: extra distributions ---- distributions -- FFS GROUP
-------------------------------------------------------------------------------------------------------------


------
-- a. Get a table with all possibilities
-------

drop table #fss_groups

-- we want the table to include all the possible options --
select distinct  (CASE WHEN cv.h_fss_v3_group  =        'A'    THEN     'Accumulated Wealth'
                WHEN cv.h_fss_v3_group  =        'B'    THEN     'Balancing Budgets'
                WHEN cv.h_fss_v3_group  =        'C'    THEN     'Bright Futures'
                WHEN cv.h_fss_v3_group  =        'D'    THEN     'Consolidating Assets'
                WHEN cv.h_fss_v3_group  =        'E'    THEN     'Established Reserves'
                WHEN cv.h_fss_v3_group  =        'F'    THEN     'Family Interest'
                WHEN cv.h_fss_v3_group  =        'G'    THEN     'Growing Rewards'
                WHEN cv.h_fss_v3_group  =        'H'    THEN     'Platinum Pensions'
                WHEN cv.h_fss_v3_group  =        'I'    THEN     'Seasoned Economy'
                WHEN cv.h_fss_v3_group  =        'J'    THEN     'Single Endeavours'
                WHEN cv.h_fss_v3_group  =        'K'    THEN     'Stretched Finances'
                WHEN cv.h_fss_v3_group  =        'L'    THEN     'Sunset Security'
                WHEN cv.h_fss_v3_group  =        'M'    THEN     'Traditional Thrift'
                WHEN cv.h_fss_v3_group  =        'N'    THEN     'Young Essentials'
                WHEN cv.h_fss_v3_group  =        'U'    THEN     'Unallocated'
          ELSE                                                 'Unknown'               END) as FSS_V3_group

        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
        ,no_vespa_barclays_households2 = 99.99999
into #fss_groups
from sk_prod.EXPERIAN_CONSUMERVIEW cv


-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #fss_groups(FSS_V3_group,No_barclays_Households,No_vespa_barclays_Households,no_vespa_barclays_households2) values('Unknown',99.99999,99.99999,99.99999)



------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP
-------

drop table #barclays_households3

-- determine barclays distribution
select bar.FSS_V3_group
        ,No_barclays_Households = count(distinct(bar.cb_key_household))
into #barclays_households3
from v081_BARCLAYS_Universe_demographics bar
where responder = 1
group by bar.FSS_V3_group
order by bar.FSS_V3_group;

-- determine vespa and barclays customers distribution
--set @vespa_base = (select sum(weighting) from v081_VESPA_Universe_demographics)

drop table #vespa_households3

Select ves.FSS_V3_group
        ,No_Vespa_barclays_Households = sum(case when barclays_customer = 1 then weighting else null end)
        ,no_vespa_barclays_households2 = sum(case when barclays_customer <> 1 then weighting else null end)
into #vespa_households3
from v081_vespa_Universe_demographics ves
group by ves.FSS_V3_group
order by ves.FSS_V3_group;

select * from #vespa_households3
select * from #barclays_households3


select top 10 * from v081_vespa_Universe_demographics


-- now insert the distributions into the table holding all possble options
update #fss_groups
        set tab.No_barclays_Households = case when tab.FSS_V3_group = bar.FSS_V3_group then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.FSS_V3_group = ves.FSS_V3_group then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #fss_groups tab
left join #barclays_households3 bar
on tab.FSS_V3_group = bar.FSS_V3_group
left join #vespa_households3 ves
on tab.FSS_V3_group = ves.FSS_V3_group





-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #fss_groups
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end




-------------- OUTPUT --------------------
------------------------------------------
        select * from #fss_groups order by FSS_V3_group
------------------------------------------




-------------------------------------------------------------------------------------------------------------
-- xxxxx: distributions -- Social Grade
-------------------------------------------------------------------------------------------------------------


------
-- a. Get a table with all possibilities
-------

drop table #social_grade

-- we want the table to include all the possible options --
select distinct(lukcat_fr_de_nrs) AS social_grade1
        ,case when social_grade1 is null then 'Unknown' else social_grade1 end as social_grade
        ,No_barclays_Households = 99.99999  -- placeholder to prevent alter table later
        ,No_vespa_barclays_Households = 99.99999 -- placeholder to prevent alter table later
into #social_grade
from sk_prod.CACI_SOCIAL_CLASS

select * from #social_grade

-- add the 'Unknown' column we defined easlier for those customers who had no match!
insert into #social_grade(social_grade,No_barclays_Households,No_vespa_barclays_Households) values('Unknown',99.99999,99.99999)


------
-- B. GET THE NUMBER/RATIO OF CUSTOMERS IN BARCLAYS AND BARCLAYS CUSTOMERS IN VESPA FALLING INTO EACH GROUP
-------



-- determine barclays distribution
select bar.social_grade
        ,No_barclays_Households = (count(distinct(bar.cb_key_household)))
into #barclays_households5
from v081_BARCLAYS_Universe_demographics bar
where responder = 1
group by bar.social_grade
order by bar.social_grade;

-- determine vespa and barclays customers distribution
select ves.social_grade
       ,vespa_barclays = sum(case when barclays_customer = 1 then weighting else null end)
        ,vespa_not_barclays = sum(case when barclays_customer <> 1 then weighting else null end)
into #vespa_households5
from v081_vespa_Universe_demographics ves
group by ves.social_grade
order by ves.social_grade;


select * from #barclays_households5
select * from #vespa_households5

-- now insert the distributions into the table holding all possble options
update #social_grade
        set tab.No_barclays_Households = case when tab.social_grade = bar.social_grade then  bar.No_barclays_Households
                else tab.No_barclays_Households end
            ,tab.No_vespa_barclays_Households = case when tab.social_grade = ves.social_grade then ves.No_vespa_barclays_Households
                else tab.No_vespa_barclays_Households end
from #social_grade tab
left join #barclays_households5 bar
on tab.social_grade = bar.social_grade
left join #vespa_households5 ves
on tab.social_grade = ves.social_grade




-- earlier we inserted 99.9999 placeholders for the %of households distrib figures - change these to zero's
update #social_grade
        set No_barclays_Households = case when  No_barclays_Households = 99.99999 then 0 else No_barclays_Households end
            , No_vespa_barclays_Households = case when No_vespa_barclays_Households = 99.99999 then 0 else No_vespa_barclays_Households end




-------------- OUTPUT --------------------
------------------------------------------
        select social_grade, No_barclays_Households, No_vespa_barclays_Households  from #social_grade order by social_grade
------------------------------------------



-------------------------------------------------------------------------------
--------------------------------------------------------------------------------


-- we want to flag customers who have a cash isa from the experian dad fields.


-- drop table #percentile

-- get the percentiles--
SELECT cb_key_household,max(have_a_cash_isa_percentile) as have_a_cash_isa_percentile
into #percentile
        FROM sk_prod.PERSON_PROPENSITIES_GRID_NEW pp
        JOIN sk_prod.EXPERIAN_CONSUMERVIEW cv
        ON pp.ppixel2011 = cv.p_pixel_v2 and pp.mosaic_uk_2009_type = cv.Pc_mosaic_uk_type
        GROUP BY cb_key_household


-- copy them into the vespa table
alter table v081_vespa_Universe_demographics
        add( have_cash_isa_percentile_experian integer
            ,have_cash_isa_experian integer);

-- does this have to be run twice to work??
update v081_vespa_Universe_demographics
        set have_cash_isa_percentile_experian = have_a_cash_isa_percentile
             ,have_cash_isa_experian = (case when have_cash_isa_percentile_experian >= 80 then 1 else 0 end )
from v081_vespa_Universe_demographics ves
left join #percentile per
on per.cb_key_household = ves.cb_key_household


select count(*) from
v081_vespa_Universe_demographics
where have_cash_isa_percentile_experian is not null

-- copy them into the Barclays_table
alter table v081_BARCLAYS_Universe_demographics
        add( have_cash_isa_percentile_experian integer
            ,have_cash_isa_experian integer);


update v081_BARCLAYS_Universe_demographics
        set have_cash_isa_percentile_experian = have_a_cash_isa_percentile
             ,have_cash_isa_experian = (case when have_cash_isa_percentile_experian >= 80 then 1 else 0 end )
from v081_BARCLAYS_Universe_demographics ves
left join #percentile per
on per.cb_key_household = ves.cb_key_household

select top 10 * from v081_BARCLAYS_Universe_demographics where have_cash_isa_percentile_experian > 80 and responder = 1



-- Now get the outputs

--drop table #barclays_households_isa
-- determine barclays distribution
select bar.have_cash_isa_experian
        ,No_barclays_Households = (count(distinct(bar.cb_key_household)))
into #barclays_households_isa
from v081_BARCLAYS_Universe_demographics bar
where responder = 1
group by bar.have_cash_isa_experian
order by bar.have_cash_isa_experian;

--drop table #vespa_have_sach_isa

Select ves.have_cash_isa_experian
        ,all_sky = (sum(weighting))
        ,vespa_barclays_responders = sum(case when barclays_customer = 1 then weighting else null end)
        ,vespa_barclays_non_responders = sum(case when barclays_customer <> 1 then weighting else null end)
into #vespa_have_sach_isa
from v081_vespa_Universe_demographics ves
group by ves.have_cash_isa_experian
order by ves.have_cash_isa_experian;


select * from #barclays_households_isa

select * from #vespa_have_sach_isa



-- % of barclays_base
select bar.have_cash_isa_experian
        ,No_barclays_Households = (count(distinct(bar.cb_key_household)))
from v081_BARCLAYS_Universe_demographics bar
group by bar.have_cash_isa_experian
order by bar.have_cash_isa_experian;




select max(have_a_cash_isa_percentile) from #percentile

select top 10 * from sk_prod.EXPERIAN_CONSUMERVIEW

select max(have_a_cash_isa_percentile) from sk_prod.EXPERIAN_CONSUMERVIEW



-- GET THE BARCLAYS DISTRIBUTIONS
select fss_v3_group
        ,count(*)
from v081_BARCLAYS_Universe_demographics
where have_cash_isa_experian = 1
group by fss_v3_group
order by fss_v3_group



-- GET THE sKY DISTRIBUTIONS
select fss_v3_group
        ,sum(weighting)
from v081_VESPA_Universe_demographics
where have_cash_isa_experian = 1
group by fss_v3_group
order by fss_v3_group

have_cash_isa_experian


select top 10 * from v081_vespa_Universe_demographics

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- NOW WE WANT A CUBE -- THE CUBE WILL SHOW FSS SEGMENT SPLIT BY PROPENSITY TO HAVE CASH ISA ACROSS BARCLAYS AND SKY --
-- DATA MATCH - TEM[PLATE 1 BUT THE SEPERATE FILE USED FOR DIAGNOSTICS

-- FROM THIS CUBE WE ENDED UP ONLY DETERMINING THE BEST FSS SEGMENTS FOR THE SKY BASE - HAVING ALREADY DETERMINED THE SAME FOR THE BARCLAYS RESPONDERS FROM OUTPUTS IN
-- TEMPALTE 1.




-- BELOW LETS GET BARCLAYS AND SKY DETAILS TO BUILD AN ADHOC CUBE TO BE US,ED IN A PIVOT TABLE
-- this is FOR THE CUBE
drop table #fss_barclays

-- determine barclays distribution
select bar.FSS_V3_group
        bar,have_cash_isa_experian as experian_have_ISA
        ,barclays_responders = count(distinct(bar.cb_key_household))
into #fss_barclays
from v081_BARCLAYS_Universe_demographics bar
where responder = 1
group by bar.FSS_V3_group, experian_have_ISA
order by bar.FSS_V3_group, experian_have_ISA ;



select top 10 * from v081_BARCLAYS_Universe_demographics

-- determine vespa and barclays customers distribution
--set @vespa_base = (select sum(weighting) from v081_VESPA_Universe_demographics)

drop table #FFS_sky

Select ves.FSS_V3_group
        ,have_cash_isa_experian as experian_have_ISA
        ,Sky_barclays_HH = sum(case when barclays_customer = 1 then weighting else null end)
        ,Sky_not_barclays_HH = sum(case when barclays_customer <> 1 then weighting else null end)
        ,vespa_barclays = count(distinct(case when barclays_customer = 1 then cb_key_household else null end))
into #FFS_sky
from v081_vespa_Universe_demographics ves
group by ves.FSS_V3_group,experian_have_ISA
order by ves.FSS_V3_group,experian_have_ISA;


select * from #fss_barclays
select * from #FFS_sky

-- THIS IS ADDITIOANL AND NOT REALLY NEEDED - DIDNT END UP SPLITTING BY SOCIAL GRADE
SELECT FSS_V3_group
       ,Sky_PURCHASED_ABC1 = sum(case when social_grade in ('A','B','C1') then weighting else null end)
from v081_vespa_Universe_demographics ves
group by ves.FSS_V3_group
order by ves.FSS_V3_group


-- FROM ABOVE WE DETAILED FSS GROUPS THAT OVER INDEXED IN TERMS OF RESPONDERS RELATIVE TO THE BARCLAYS BASE.... NOW WE WANT TO GET STATISTICS ONLY FOR THESE FSS GROUPS

-- UNIVERSE 1 = BARCLAYS RESPONDERS BEST FSS GROUPS (FROM TEMPLATE 1)
-- UNIVERSE 2 = CUSTOEMRS FLAGGED WITH EXPERIANS HAVE ISA FLAG (PERCENTILE > 80) - THIS IS MARKET STANDARD
-- UNIVERSE 3 = PURCHASED UNIVERSE - ABC1 CUSTOMERS

-- WE WANT TO KNOW THE RESPONSE RATES FOR THESE GROUPS AND WE WANT TO KNOW VOLUMES IN SKY AND VESPA


-- FINAL OUTSPUTS - AT THIS POINT WE ARE UNSURE ABOUT WHAT TO DO WITH FSS 'UNKNOWN'S -- WE WILL PRODUCE THE SAME OUTPUT 1- WITH UNKNOWS 2- WITHOUT UNKNONWS

-- OUT 1 -- WITH UNKNOWS
--A -- final outputs: -- THIS IS THE BARCLAYS PART
select count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') then cb_key_household else null end)) as barclays_TA1

 ,count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') and responder = 1
                then cb_key_household else null end)) as barclays_TA1_responder

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then cb_key_household else null end)) as Barclays_ta2

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and responder = 1
                then cb_key_household else null end)) as Barclays_ta2_responder

  ,count( distinct(case when  social_grade in ('A','B','C1')
                then cb_key_household else null end)) as Barclays_ta3

  ,count( distinct(case when social_grade in ('A','B','C1') and responder = 1
                then cb_key_household else null end)) as Barclays_ta3_responder

from v081_barclays_Universe_demographics ves



-- B -- THIS IS THE SKY AND VESPA PARTS;

select count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') then cb_key_household else null end)) as vespa_TA1

 ,count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_TA1_responder

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then cb_key_household else null end)) as Barclays_ta2

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_ta2_responder

  ,count( distinct(case when  social_grade in ('A','B','C1')
                then cb_key_household else null end)) as vespa_ta3

  ,count( distinct(case when social_grade in ('A','B','C1') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_ta3_responder




  ,sum((case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') then weighting else null end)) as vespa_TA1_sky

 ,sum((case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') and barclays_responder = 1
                then weighting else null end)) as vespa_TA1_responder_sky

 ,sum( (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then weighting else null end)) as Barclays_ta2_sky

 ,sum( (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and barclays_responder = 1
                then weighting else null end)) as vespa_ta2_responder_sky

  ,sum( (case when  social_grade in ('A','B','C1')
                then weighting else null end)) as vespa_ta3_sky

  ,sum( distinct(case when social_grade in ('A','B','C1') and barclays_responder = 1
                then weighting else null end)) as vespa_ta3_responder_sky

from v081_vespa_Universe_demographics ves








-- OUT 1 -- WITH UNKNOWS
--A -- final outputs: -- THIS IS THE BARCLAYS PART
select count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') then cb_key_household else null end)) as barclays_TA1

 ,count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') and responder = 1
                then cb_key_household else null end)) as barclays_TA1_responder

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then cb_key_household else null end)) as Barclays_ta2

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and responder = 1
                then cb_key_household else null end)) as Barclays_ta2_responder

  ,count( distinct(case when  social_grade in ('A','B','C1')
                then cb_key_household else null end)) as Barclays_ta3

  ,count( distinct(case when social_grade in ('A','B','C1') and responder = 1
                then cb_key_household else null end)) as Barclays_ta3_responder

from v081_barclays_Universe_demographics ves



-- B -- THIS IS THE SKY AND VESPA PARTS;

select count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') then cb_key_household else null end)) as vespa_TA1

 ,count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_TA1_responder

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then cb_key_household else null end)) as Barclays_ta2

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_ta2_responder

  ,count( distinct(case when  social_grade in ('A','B','C1')
                then cb_key_household else null end)) as vespa_ta3

  ,count( distinct(case when social_grade in ('A','B','C1') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_ta3_responder




  ,sum((case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') then weighting else null end)) as vespa_TA1_sky

 ,sum((case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Unknown Sky','Young Essentials') and barclays_responder = 1
                then weighting else null end)) as vespa_TA1_responder_sky

 ,sum( (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then weighting else null end)) as Barclays_ta2_sky

 ,sum( (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and barclays_responder = 1
                then weighting else null end)) as vespa_ta2_responder_sky

  ,sum( (case when  social_grade in ('A','B','C1')
                then weighting else null end)) as vespa_ta3_sky

  ,sum( distinct(case when social_grade in ('A','B','C1') and barclays_responder = 1
                then weighting else null end)) as vespa_ta3_responder_sky

from v081_vespa_Universe_demographics ves





-- OUT 2 -- WITHOUT THE UNKNOWS
--2A -- final outputs: -- THIS IS THE BARCLAYS PART

select count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials') then cb_key_household else null end)) as barclays_TA1

 ,count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials') and responder = 1
                then cb_key_household else null end)) as barclays_TA1_responder

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then cb_key_household else null end)) as Barclays_ta2

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and responder = 1
                then cb_key_household else null end)) as Barclays_ta2_responder

  ,count( distinct(case when  social_grade in ('A','B','C1')
                then cb_key_household else null end)) as Barclays_ta3

  ,count( distinct(case when social_grade in ('A','B','C1') and responder = 1
                then cb_key_household else null end)) as Barclays_ta3_responder

from v081_barclays_Universe_demographics ves



-- 2B -- THIS IS THE SKY AND VESPA PARTS;

select count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials') then cb_key_household else null end)) as vespa_TA1

 ,count(distinct(case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_TA1_responder

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then cb_key_household else null end)) as Barclays_ta2

 ,count( distinct(case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_ta2_responder

  ,count( distinct(case when  social_grade in ('A','B','C1')
                then cb_key_household else null end)) as vespa_ta3

  ,count( distinct(case when social_grade in ('A','B','C1') and barclays_responder = 1
                then cb_key_household else null end)) as vespa_ta3_responder




  ,sum((case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials') then weighting else null end)) as vespa_TA1_sky

 ,sum((case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials') and barclays_responder = 1
                then weighting else null end)) as vespa_TA1_responder_sky

 ,sum( (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then weighting else null end)) as Barclays_ta2_sky

 ,sum( (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                ,'Sunset Security','Traditional Thrift') and barclays_responder = 1
                then weighting else null end)) as vespa_ta2_responder_sky

  ,sum( (case when  social_grade in ('A','B','C1')
                then weighting else null end)) as vespa_ta3_sky

  ,sum( distinct(case when social_grade in ('A','B','C1') and barclays_responder = 1
                then weighting else null end)) as vespa_ta3_responder_sky

from v081_vespa_Universe_demographics ves




--- the last step of the above exercise is to identif y the overlap between the various target groups
-- this will be displayed by a venn diagram -- below lets idetnify which customers fall into each group
-- THIS IS ECLUDING UNKNOWNS
-- drop table #table_for_venn

Select account_number, weighting
       , ta1 = (case when FSS_v3_group in('Single Endeavours','Sunset Security',
                'Traditional Thrift','Young Essentials')  THEN 1 ELSE 0 END)

       ,ta2 = (case when FSS_v3_group in('Family Interest','Growing Rewards','Single Endeavours'
                 ,'Sunset Security','Traditional Thrift') then 1 else 0 end)

       ,ta3 = (case when  social_grade in ('A','B','C1')
                then 1 else 0 end)
into #table_for_venn
from v081_vespa_Universe_demographics


-- get the numbers;
Select sum(case when TA1 = 1 and Ta2 = 0 and ta3 = 0 then weighting else null end) as TA1_ONLY
        ,sum(case when TA1 = 0 and Ta2 = 1 and ta3 = 0 then weighting else null end) as TA2_ONLY
        ,sum(case when TA1 = 0 and Ta2 = 0 and ta3 = 1 then weighting else null end) as TA3_ONLY
        ,sum(case when TA1 = 1 and Ta2 = 1 and ta3 = 0 then weighting else null end) as TA1_AND_TA2
        ,sum(case when TA1 = 1 and Ta2 = 0 and ta3 = 1 then weighting else null end) as TA1_AND_TA3
        ,sum(case when TA1 = 0 and Ta2 = 1 and ta3 = 1 then weighting else null end) as TA2_AND_TA3
        ,sum(case when TA1 = 1 and Ta2 = 1 and ta3 = 1 then weighting else null end) as TA1_AND_TA2_AND_TA3
FROM #table_for_venn



-- NOW WE WANT FSS BY SOCIAL GRADE FOR THE WHOLE SKY POPULATION IN ABC1 -->

select distinct(social_grade) from v081_vespa_Universe_demographics order by (social_grade)

social_grade
A
B
C1
C2
D
E
Unknown

select fss_v3_group
        , sum(case when social_grade = 'A' then weighting else null end) as Sky_Population_A
        , sum(case when social_grade = 'B' then weighting else null end) as Sky_Population_B
        , sum(case when social_grade = 'C1' then weighting else null end) as Sky_Population_C1
        , sum(case when social_grade = 'C2' then weighting else null end) as Sky_Population_C2
        , sum(case when social_grade = 'D' then weighting else null end) as Sky_Population_D
        , sum(case when social_grade = 'E' then weighting else null end) as Sky_Population_E
        , sum(case when social_grade = 'Unknown' then weighting else null end) as Sky_Population_Unknown
from v081_vespa_Universe_demographics
group by fss_v3_group
order by fss_v3_group

-- CHECK THE SUM TIES UP WITH THE SUM OF ALL ABC1 CUSTOMERS IN THE SKY BASE! FROM THE SSHEET.



select fss_v3_group
        , count(case when social_grade = 'A' then cb_key_household else null end) as barclays_Population_A
        , count(case when social_grade = 'B' then cb_key_household else null end) as barclays_Population_B
        , count(case when social_grade = 'C1' then cb_key_household else null end) as barclays_Population_C1
        , count(case when social_grade = 'C2' then cb_key_household else null end) as barclays_Population_C2
        , count(case when social_grade = 'D' then cb_key_household else null end) as barclays_Population_D
        , count(case when social_grade = 'E' then cb_key_household else null end) as barclays_Population_E
        , count(case when social_grade = 'Unknown' then cb_key_household else null end) as Sky_Population_Unknown
from v081_barclays_Universe_demographics
group by fss_v3_group
order by fss_v3_group


-- USEFUL INDEX TOOL
-- sp_iqindex 'sk_prod.cust_set_top_box'


--------------------------------------------------------
--------------------------------------------------------
-- TABLE FOR SARAH - WHAT DOES THE BARCLAYS-CUSTOMER_BEFORE_CAMPAIGN LOOK LIKE? ACROSS BARCLAYS, SKY AND VESPA -- NEEDED FOR AN EMAIL
-- IT TURNS OUT THIS FEILD IS NOT A BOOLEAN - IT HAS VALUES 1-9 -- THE UIVERSE DEMOGRAPHICS ROLL UP TO THE MAX OF THIS FIELD FROM LTHE BARCLAYS FILE WHICH CONTAINED
-- DUCPLICATE CB KEY HOUSEHOLDS WITH DIFFERENT VALUES FOR THESE FIELDS


select barclays_customer_before_campaign
        ,count(distinct(cb_key_household)) as barclays_customers
from v081_Barclays_Universe_demographics
group by barclays_customer_before_campaign
order by barclays_customer_before_campaign


select barclays_customer_before_campaign
        ,count(distinct(cb_key_household)) as vespa_customers
        ,sum(weighting) as sky_customers
from v081_vespa_Universe_demographics
group by barclays_customer_before_campaign
order by barclays_customer_before_campaign


--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------


--- WE NEED TO ADDRESS THE NUMBER OF NULL CB_KEY_HOUSEHOLDS IN THE VESPA UNIVERSE DEMOGRAPHICS TABLE -- MAYBE WE CAN MATCH TO SAV??


--
select count(*) as total
        , count(case when cb_key_household is null then 1 else null end) as null_cb
        , sum(case when cb_key_household is null then weighting else null end) null_cb_sky
        ,count(case when fss_v3_group = 'Unknown Sky' then 1 else null end) as unknown_skys
 from v081_vespa_Universe_demographics
-- there are 628 null cb_key_hosueholds!


select account_number, cb_key_household
into #sav
from sk_prod.CUST_SINGLE_ACCOUNT_VIEW
where account_number in (select account_number from v081_vespa_Universe_demographics where cb_key_household is null)


select top 100 * from #sav

select count(case when cb_key_household is null then 1 else null end) as null_cb from #sav -- none are null
-- single account view

-- so we have IDENTIFIED THAT THESE CAN BE FIXED - BEFORE WE STAR DOING THAT LETS IDENTIFY THE PROBLEM WITH THE BARCLAYS CUSTOMERS THAT HAVE BE MARKED AS UNKNOWN! FSS



------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------
-- LETS IDENTIFY THE PROFILE OF THE CUSTOMERS FROM BARCLAYS AND sKY THAT HAVE BEEN MARKED WITH AN UNKNOWN FSS GROUP

-- lets add account_numberm and barclays customer flag to this table
alter table v081_barclays_Universe_demographics
        add (Sky_customer integer
               ,account_number varchar(20));


update v081_barclays_Universe_demographics
set bar.account_number = sky.account_number
        ,sky_customer = 1
from v081_barclays_Universe_demographics bar
  join v108_active_customer_base sky
 on bar.cb_key_household = sky.cb_key_household


select  count(*)
        ,count (case when account_number is not null then 1 else null end) as account_numbers
        ,count(case when sky_customer = 1 then 1 else null end) as sky_customers
        ,count(case when sky_customer = 1 and fss_v3_group = 'Unknown Sky' then 1 else null end) as sky_customers_unknown_fss
from v081_barclays_Universe_demographics
-- the abvoe should be the same number sky_customers and account_numbers should be the same --


-- count()         account_numbers         sky_customers           sky_customers_unknown_fss
-- 7063525         2554241                 2554241                 97273

-- so now we know which Barclays customers are sky customers and we know which have an 'Unknown Sky' fss grou (i.e. they are not in experian consumer view)


SELECT TOP 10 * FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW

-- WE WANT TO KNOW WHAT IS IN THIS TABLE-----------------------------------------------------------------------------------------------------

sp_columns 'CUST_SINGLE_ACCOUNT_VIEW'
sp_columns 'VESPA_EVENTS_ALL'



if object_id ('unknown_details') is not null drop table unknown_details


SELECT bar.account_number
        ,bar.cb_key_household

        ,case when datediff(day,acct_first_account_activation_dt,today()) <=   91 then 'A) 0-3 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  182 then 'B) 4-6 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  365 then 'C) 6-12 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  730 then 'D) 1-2 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 1095 then 'E) 2-3 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 1825 then 'F) 3-5 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 3650 then 'G) 5-10 Years'
              else                                                                     'H) 10 Years+ '
          end as tenure

        ,prod_latest_entitlement_code AS SUBSCRIPTION
        ,CUST_ACTIVE_DTV AS ACTIVE_DTV
        ,CUST_PREV_DTV AS PREVIOUS_DTV_CUSTOMER
        ,ACCT_ACTIVE_SUBS_CATEGORY AS ACTIVE_PRODUCTS
        ,PROP_DELPHI_CREDIT_RISK_SCORE -- propensity

into unknown_details

FROM v081_barclays_Universe_demographics BAR
left JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW SAV
 ON BAR.ACCOUNT_NUMBER = SAV.ACCOUNT_NUMBER
  where fss_v3_group ='Unknown Sky' and sky_customer = 1



select top 10 * from unknown_details

select count(*) from unknown_details


select distinct(PROP_DELPHI_CREDIT_RISK_SCORE) from unknown_details -- did not pick anything up!!!



-- ADD CQM

--Add columns to nodupes for population
alter table unknown_details             add     cqm_score               tinyint         default         null
                                                ,add    cqm_group               varchar(30) default     null
                                                ,add    cqm_indicator   varchar(20) default     null



update unknown_details as base
set base.cqm_score = zz.model_score -- this is the raw score bcos people can change thier minds
,base.cqm_group = case when zz.model_score between 1 and 10 then 'a) Low Risk' -- these are standard groupings - aquisition??
                       when zz.model_score between 11 and 26 then 'b) Medium Risk'
                       when zz.model_score between 27 and 36 then 'c) High Risk'
                       else 'd) Unknown'
                       end
,base.cqm_indicator = case when zz.model_score between 1 and 22 then 'High quality'-- from Matt Oakman via email via tom
                       when zz.model_score between 23 and 36 then 'Low quality'
                       else 'No Score!'
                       end
from sk_prod.id_v_universe_all zz
where base.cb_key_household = zz.cb_key_household;


-- ADD VALUE SEGMENT

alter table unknown_details
add     VALUE_SEGMENT VARCHAR(30)

UPDATE unknown_details
SET value_segment = tgt.value_seg
FROM unknown_details AS base
       INNER JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number



----- add tv package



SELECT          csh.account_number
                ,max(case when cel.prem_sports + cel.prem_movies  = 4   then 'Top Tier'
                     when cel.prem_sports = 2 and cel.prem_movies = 1   then 'Dual Sports Single Movies'
                     when cel.prem_sports = 2 and cel.prem_movies = 0   then 'Dual Sports'
                     when cel.prem_sports = 1 and cel.prem_movies = 2   then 'Single Sports Dual Movies'
                     when cel.prem_sports = 0 and cel.prem_movies = 2   then 'Dual Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 1   then 'Single Sports Single Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 0   then 'Single Sports'
                     when cel.prem_sports = 0 and cel.prem_movies = 1   then 'Single Movies'
                     when cel.prem_sports + cel.prem_movies = 0         then 'Basic'
                     else                                                    'Unknown'
                end) as tv_premiums,
                max(case when (music = 0 AND news_events = 0 AND kids = 0 AND knowledge = 0)
                     then 'Entertainment'
                     when (music = 1 or news_events = 1 or kids = 1 or knowledge = 1)
                     then 'Entertainment Extra'
                     else 'Unknown' end) as tv_package
into            #tvpackage
FROM            sk_prod.cust_subs_hist as csh
        inner join sk_prod.cust_entitlement_lookup as cel
                on csh.current_short_description = cel.short_description
        inner join unknown_details as base   --------------------------------------- THE PRIMARY BOXES - WHO HAVE RETURNED DATA: OUR UNIVERSE!
                on csh.account_number = base.account_number
WHERE           csh.subscription_sub_type ='DTV Primary Viewing'
AND             csh.subscription_type = 'DTV PACKAGE'
AND             csh.status_code in ('AC','AB','PC')
AND             csh.effective_from_dt < today() -- i.e. they had the same package for the whole period
AND             csh.effective_to_dt   >= today()
AND             csh.effective_from_dt != csh.effective_to_dt
group by csh.account_number ;



-- add the fields to the table:
alter table     unknown_details
add(            tv_package varchar(50) default 'Unknown',
                tv_premiums varchar (100) default 'Unknown');


update          unknown_details as base
set             base.tv_package = tvp.tv_package,
                base.tv_premiums = tvp.tv_premiums
from            #tvpackage as tvp
where           base.account_number = tvp.account_number
commit



select top 10 * from unknown_details

------ now lets add the other products

-- add the fields to the table:
alter table     unknown_details
add(            hd integer
                ,mr integer
                ,sp integer
                ,bb integer
                ,talk integer
                ,wlr integer
                ,movies integer);



UPDATE unknown_details
   SET HD        = tgt.hdtv
      ,MR        = tgt.multiroom
      ,SP        = tgt.skyplus
      ,BB        = tgt.broadband
      ,talk      = tgt.skytalk
      ,WLR       = tgt.wlr
      ,movies    = tgt.movies
 FROM unknown_details AS base
      INNER JOIN (
                    SELECT  csh.account_number
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS skyplus
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS multiroom
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END)  AS hdtv
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                       AND (       status_code in ('AC','AB')
                                               OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                               OR (status_code='CF' AND prev_status_code='PC'                                  )
                                               OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                            )                                    THEN 1 ELSE 0 END)  AS broadband
                           ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                       AND (     csh.status_code = 'A'
                                             OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                             OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                             OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                            )                                  THEN 1 ELSE 0 END)   AS skytalk
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                       AND csh.status_code in ('A','CRQ','R')  THEN 1 ELSE 0 END) AS wlr
                           ,MAX(cel.prem_movies)      AS movies
                      FROM sk_prod.cust_subs_hist AS csh
                           INNER JOIN unknown_details AS base ON csh.account_number = base.account_number
                           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
                     WHERE csh.effective_from_dt <= today()
                       AND csh.effective_to_dt    > today()
                       AND csh.subscription_sub_type  IN ( 'DTV Primary Viewing'
                                                          ,'DTV Sky+'
                                                          ,'DTV Extra Subscription'
                                                          ,'DTV HD'
                                                          ,'Broadband DSL Line'
                                                          ,'SKY TALK SELECT'
                                                          ,'SKY TALK LINE RENTAL'  )  --< Optimises the code, limit to what is needed
                       AND csh.effective_from_dt <> csh.effective_to_dt
                  GROUP BY csh.account_number
        )AS tgt ON base.account_number = tgt.account_number;

COMMIT;

--4780437 Row(s) affected



select top 10 * from unknown_details
select count(*) from unknown_details


----------------------------------------------------------------------------------------------------------------------------------------
---------------------------------- SO KNOW WE HAVE THE FIELDS THAT WE NEED LETS ,GET SOME OUTPUTS ---------------------------------- --
----------------------------------------------------------------------------------------------------------------------------------------

-- BEFORE WE DO THAT LETS GET THE SAME MEASURES FOR THE SKY BASE SO WE CAN COMPARE CUSTOMERS FROM SKY TO THOISE CUSTOMERS IN BARCLAYS THAT HAVE UNKNOWN FSS



-- VESPA CUSTOMERS


if object_id ('unknown_details_VESPA') is not null drop table unknown_details_VESPA


SELECT bar.account_number
        ,bar.cb_key_household
        ,bar.weighting

        ,case when datediff(day,acct_first_account_activation_dt,today()) <=   91 then 'A) 0-3 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  182 then 'B) 4-6 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  365 then 'C) 6-12 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  730 then 'D) 1-2 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 1095 then 'E) 2-3 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 1825 then 'F) 3-5 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 3650 then 'G) 5-10 Years'
              else                                                                     'H) 10 Years+ '
          end as tenure

        ,prod_latest_entitlement_code AS SUBSCRIPTION
        ,CUST_ACTIVE_DTV AS ACTIVE_DTV
        ,CUST_PREV_DTV AS PREVIOUS_DTV_CUSTOMER
        ,ACCT_ACTIVE_SUBS_CATEGORY AS ACTIVE_PRODUCTS
        ,PROP_DELPHI_CREDIT_RISK_SCORE -- propensity

into unknown_details_VESPA

FROM v081_VESPA_Universe_demographics BAR
left JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW SAV
 ON BAR.ACCOUNT_NUMBER = SAV.ACCOUNT_NUMBER


select top 10 * from unknown_details_VESPA

select count(*) from unknown_details_VESPA


select distinct(PROP_DELPHI_CREDIT_RISK_SCORE) from unknown_details_VESPA -- did not pick anything up!!!



-- ADD CQM

--Add columns to nodupes for population
alter table unknown_details_VESPA             add     cqm_score               tinyint         default         null
                                                ,add    cqm_group               varchar(30) default     null
                                                ,add    cqm_indicator   varchar(20) default     null



update unknown_details_VESPA as base
set base.cqm_score = zz.model_score -- this is the raw score bcos people can change thier minds
,base.cqm_group = case when zz.model_score between 1 and 10 then 'a) Low Risk' -- these are standard groupings - aquisition??
                       when zz.model_score between 11 and 26 then 'b) Medium Risk'
                       when zz.model_score between 27 and 36 then 'c) High Risk'
                       else 'd) Unknown'
                       end
,base.cqm_indicator = case when zz.model_score between 1 and 22 then 'High quality'-- from Matt Oakman via email via tom
                       when zz.model_score between 23 and 36 then 'Low quality'
                       else 'No Score!'
                       end
from sk_prod.id_v_universe_all zz
where base.cb_key_household = zz.cb_key_household


-- ADD VALUE SEGMENT

alter table unknown_details_VESPA
add     VALUE_SEGMENT VARCHAR(30)

UPDATE unknown_details_VESPA
SET value_segment = tgt.value_seg
FROM unknown_details_VESPA AS base
       INNER JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number



----- add tv package

drop table #tvpackage

SELECT          csh.account_number
                ,max(case when cel.prem_sports + cel.prem_movies  = 4   then 'Top Tier'
                     when cel.prem_sports = 2 and cel.prem_movies = 1   then 'Dual Sports Single Movies'
                     when cel.prem_sports = 2 and cel.prem_movies = 0   then 'Dual Sports'
                     when cel.prem_sports = 1 and cel.prem_movies = 2   then 'Single Sports Dual Movies'
                     when cel.prem_sports = 0 and cel.prem_movies = 2   then 'Dual Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 1   then 'Single Sports Single Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 0   then 'Single Sports'
                     when cel.prem_sports = 0 and cel.prem_movies = 1   then 'Single Movies'
                     when cel.prem_sports + cel.prem_movies = 0         then 'Basic'
                     else                                                    'Unknown'
                end) as tv_premiums,
                max(case when (music = 0 AND news_events = 0 AND kids = 0 AND knowledge = 0)
                     then 'Entertainment'
                     when (music = 1 or news_events = 1 or kids = 1 or knowledge = 1)
                     then 'Entertainment Extra'
                     else 'Unknown' end) as tv_package
into            #tvpackage
FROM            sk_prod.cust_subs_hist as csh
        inner join sk_prod.cust_entitlement_lookup as cel
                on csh.current_short_description = cel.short_description
        inner join unknown_details_VESPA as base   --------------------------------------- THE PRIMARY BOXES - WHO HAVE RETURNED DATA: OUR UNIVERSE!
                on csh.account_number = base.account_number
WHERE           csh.subscription_sub_type ='DTV Primary Viewing'
AND             csh.subscription_type = 'DTV PACKAGE'
AND             csh.status_code in ('AC','AB','PC')
AND             csh.effective_from_dt < today() -- i.e. they had the same package for the whole period
AND             csh.effective_to_dt   >= today()
AND             csh.effective_from_dt != csh.effective_to_dt
group by csh.account_number


-- add the fields to the table:
alter table     unknown_details_VESPA
add(            tv_package varchar(50) default 'Unknown',
                tv_premiums varchar (100) default 'Unknown');


update          unknown_details_VESPA as base
set             base.tv_package = tvp.tv_package,
                base.tv_premiums = tvp.tv_premiums
from            #tvpackage as tvp
where           base.account_number = tvp.account_number
commit



select top 10 * from unknown_details_VESPA

------ now lets add the other products

-- add the fields to the table:
alter table     unknown_details_VESPA
add(            hd integer
                ,mr integer
                ,sp integer
                ,bb integer
                ,talk integer
                ,wlr integer
                ,movies integer);



UPDATE unknown_details_VESPA
   SET HD        = tgt.hdtv
      ,MR        = tgt.multiroom
      ,SP        = tgt.skyplus
      ,BB        = tgt.broadband
      ,talk      = tgt.skytalk
      ,WLR       = tgt.wlr
      ,movies    = tgt.movies
 FROM unknown_details_VESPA AS base
      INNER JOIN (
                    SELECT  csh.account_number
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS skyplus
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS multiroom
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END)  AS hdtv
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                       AND (       status_code in ('AC','AB')
                                               OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                               OR (status_code='CF' AND prev_status_code='PC'                                  )
                                               OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                            )                                    THEN 1 ELSE 0 END)  AS broadband
                           ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                       AND (     csh.status_code = 'A'
                                             OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                             OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                             OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                            )                                  THEN 1 ELSE 0 END)   AS skytalk
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                       AND csh.status_code in ('A','CRQ','R')  THEN 1 ELSE 0 END) AS wlr
                           ,MAX(cel.prem_movies)      AS movies
                      FROM sk_prod.cust_subs_hist AS csh
                           INNER JOIN unknown_details_VESPA AS base ON csh.account_number = base.account_number
                           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
                     WHERE csh.effective_from_dt <= today()
                       AND csh.effective_to_dt    > today()
                       AND csh.subscription_sub_type  IN ( 'DTV Primary Viewing'
                                                          ,'DTV Sky+'
                                                          ,'DTV Extra Subscription'
                                                          ,'DTV HD'
                                                          ,'Broadband DSL Line'
                                                          ,'SKY TALK SELECT'
                                                          ,'SKY TALK LINE RENTAL'  )  --< Optimises the code, limit to what is needed
                       AND csh.effective_from_dt <> csh.effective_to_dt
                  GROUP BY csh.account_number
        )AS tgt ON base.account_number = tgt.account_number;

COMMIT;


select top 10 * from unknown_details_VESPA

----------------------------------------------------------------------------------------------------------------------------------------
---------------------------------- SO KNOW WE HAVE THE FIELDS THAT WE NEED LETS ,GET SOME OUTPUTS ---------------------------------- --
----------------------------------------------------------------------------------------------------------------------------------------


-- lets make a barclays pivot table


select top 10 * from v081_barclays_Universe_demographics

alter table unknown_details
        add responder integer

update unknown_details
        set un.responder = bar.responder
from unknown_details un
join v081_barclays_Universe_demographics bar
on un.cb_key_household = bar.cb_key_household

drop table #BARCLAYS_ROLL_UP


-- responder --  tenure split --
select active_products
        ,tenure
        ,value_segment
        ,tv_package
        ,tv_premiums
        ,HD
        ,MR
        ,SP
        ,BB
        ,TALK
        ,WLR
        ,MOVIES
        ,responder
        ,COUNT(CB_KEY_HOUSEHOLD) barclays_unknown_customers
INTO #BARCLAYS_ROLL_UP
FROM unknown_details
GROUP BY active_products
        ,tenure
        ,value_segment
        ,tv_package
        ,tv_premiums
        ,HD
        ,MR
        ,SP
        ,BB
        ,TALK
        ,WLR
        ,MOVIES
        ,responder



select * from #barclays_roll_up

select top 10 * from unknown_details

select distinct(tenure) from unknown_details





----- lets make a Vespa panel roll up pivot

drop table #Vespa_ROLL_UP

select active_products
        ,tenure
        ,value_segment
        ,tv_package
        ,tv_premiums
        ,HD
        ,MR
        ,SP
        ,BB
        ,TALK
        ,WLR
        ,MOVIES
        ,sum(weighting) Vespa_scaled_Sky_Customers
INTO #Vespa_ROLL_UP
FROM unknown_details_vespa
GROUP BY active_products
        ,tenure
        ,value_segment
        ,tv_package
        ,tv_premiums
        ,HD
        ,MR
        ,SP
        ,BB
        ,TALK
        ,WLR
        ,MOVIES


---- we need tenure of the Active Sky base: doesnt work via scaling - not built into weightings --


     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
             ,tenure = 'this is a placeholder value'
       INTO Sky_base
       FROM sk_prod.cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= today()
        AND effective_to_dt      > today()
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL
--9935284 Row(s) affected

-- remove duplicates
delete from Sky_base where rank > 1

-- we only want to keep UK accounts

   UPDATE Sky_base
     SET
         uk_standard_account = CASE
             WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
             ELSE 0 END
     FROM Sky_base AS a
     inner join sk_prod.cust_single_account_view AS b
     ON a.account_number = b.account_number

     DELETE FROM Sky_base WHERE uk_standard_account = 0


update sky_base
   set tenure = case when datediff(day,acct_first_account_activation_dt,today()) <=   91 then 'A) 0-3 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  182 then 'B) 4-6 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  365 then 'C) 6-12 Months'
              when datediff(day,acct_first_account_activation_dt,today()) <=  730 then 'D) 1-2 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 1095 then 'E) 2-3 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 1825 then 'F) 3-5 Years'
              when datediff(day,acct_first_account_activation_dt,today()) <= 3650 then 'G) 5-10 Years'
              else                                                                     'H) 10 Years+ '
          end

FROM sky_base sky
left JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW SAV
 ON sky.ACCOUNT_NUMBER = SAV.ACCOUNT_NUMBER



-- LETS GET THE TENURE DISTRIBUTION FOR THE SKY BASE -- as tenure via scaling doesnt work (not built into weighting system)
select tenure
        ,count(*)
from sky_base
group by tenure
order by tenure




------------------------------------------------------------------------------
------------------------------------------------------------------------------

--NOW WE ALSO NEED MARKETING OPT IN--- are barclays unknowns more likely to have opted out of direct marketing??

-- the opt outs will be where Any_Mkt_OptIn = 0

SELECT  account_number
        ,cb_key_household
       ,CASE WHEN sav.cust_email_allowed             = 'Y' THEN 1 ELSE 0 END AS Email_Mkt_OptIn
       ,CASE WHEN sav.cust_postal_mail_allowed       = 'Y' THEN 1 ELSE 0 END AS Mail_Mkt_OptIn
       ,CASE WHEN sav.cust_telephone_contact_allowed = 'Y' THEN 1 ELSE 0 END AS Tel_Mkt_OptIn
       --,CASE WHEN sav.cust_sms_allowed               = 'Y' THEN 1 ELSE 0 END AS Txt_Mkt_OptIn  **Do not include as these are for service msg only
       ,CASE WHEN sav.cust_email_allowed             = 'Y'
               OR sav.cust_postal_mail_allowed       = 'Y'
               --OR sav.cust_sms_allowed               = 'Y'
               OR sav.cust_telephone_contact_allowed = 'Y'
             THEN 1
             ELSE 0
         END AS Any_Mkt_OptIn
  INTO #Opt_Ins
  FROM sk_prod.cust_single_account_view AS sav
;--24425281 Row(s) affected


select a.account_number
        ,max(ANY_Mkt_OptIn) as ANY_Mkt_OptIn
into #DMout
from #opt_ins a INNER JOIN sky_base b
        ON a.account_number = b.account_number
group by a.account_number
;


-- update the barclays roll up
alter table unknown_details
 add ANY_Mkt_OptIn integer

update unknown_details
 set un.ANY_Mkt_OptIn = opt.ANY_Mkt_OptIn
from unknown_details un
 join #dmout opt
 on opt.account_number = un.account_number




-- update the sky base roll up
alter table sky_base
 add ANY_Mkt_OptIn integer

update sky_base
 set un.ANY_Mkt_OptIn = opt.ANY_Mkt_OptIn
from sky_base un
 join #dmout opt
 on opt.account_number = un.account_number


 -- NOW GET THE OUTPUT --

select ANY_Mkt_OptIn
        ,count(*)
from sky_base
group by ANY_Mkt_OptIn
order by ANY_Mkt_OptIn



select ANY_Mkt_OptIn
        ,count(*)
from unknown_details
group by ANY_Mkt_OptIn
order by ANY_Mkt_OptIn


select ANY_Mkt_OptIn
        ,count(*)
from unknown_details
where responder = 1
group by ANY_Mkt_OptIn
order by ANY_Mkt_OptIn





------------------------------------------------
------------------ ADMIN -----------------------
------------------------------------------------

-- drop table Sky_base


-- LETS GET A LOOK AT WHAT THE FSS SEGMENTS LOOK LIKE FOR ALL UK HOUSEHOLDS

select CASE WHEN cv.h_fss_v3_group  =        'A'    THEN     'Accumulated Wealth'
                WHEN cv.h_fss_v3_group  =        'B'    THEN     'Balancing Budgets'
                WHEN cv.h_fss_v3_group  =        'C'    THEN     'Bright Futures'
                WHEN cv.h_fss_v3_group  =        'D'    THEN     'Consolidating Assets'
                WHEN cv.h_fss_v3_group  =        'E'    THEN     'Established Reserves'
                WHEN cv.h_fss_v3_group  =        'F'    THEN     'Family Interest'
                WHEN cv.h_fss_v3_group  =        'G'    THEN     'Growing Rewards'
                WHEN cv.h_fss_v3_group  =        'H'    THEN     'Platinum Pensions'
                WHEN cv.h_fss_v3_group  =        'I'    THEN     'Seasoned Economy'
                WHEN cv.h_fss_v3_group  =        'J'    THEN     'Single Endeavours'
                WHEN cv.h_fss_v3_group  =        'K'    THEN     'Stretched Finances'
                WHEN cv.h_fss_v3_group  =        'L'    THEN     'Sunset Security'
                WHEN cv.h_fss_v3_group  =        'M'    THEN     'Traditional Thrift'
                WHEN cv.h_fss_v3_group  =        'N'    THEN     'Young Essentials'
                WHEN cv.h_fss_v3_group  =        'U'    THEN     'Unallocated'
          ELSE                                                 'Unknown'               END as FSS_V3_group
        ,count(distinct(cb_key_household))
into experian_households
from sk_prod.EXPERIAN_CONSUMERVIEW cv
where cb_data_date = '2012-11-23'
group by FSS_V3_group



select * from experian_households



SELECT TOP 10 * FROM gillh.v081_Vespa_Universe_demographics v

-- LETS GET A LOOK AT WHAT THE FSS GROUPS LOOK LIKE FOR THE VESPA PANEL THAT WE ARE USING FOR THIS ANALYSIS:

SELECT FSS_V3_GROUP
        ,COUNT(*)
FROM v081_Vespa_Universe_demographics
GROUP BY FSS_V3_GROUP
ORDER BY FSS_V3_GROUP





---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

-- We still need to know more about these unknown FSS households
-- maybe we can match at postcode level and get some more details about these customers using mosaic segments - are they transient, poor, rich? British?
-- they may not be on consumer view because they may be non british - i.e. not on the electorate roll.

select top 10 * from v081_barclays_Universe_demographics where fss_v3_group  = 'Unknown Sky'

SELECT top 10 * from gillh.v081_Vespa_Universe_demographics


-- how many unknown FSS customers do we have?
select count(distinct(account_number)), count(distinct(cb_key_household)) as households, sum(weighting) as weightings from
v081_Vespa_Universe_demographics
where barclays_customer = 1
        and barclays_responder = 1
        and fss_v3_group = 'Unknown Sky'
--281


-- now lets get the postcodes of the customers who are barclays responders
drop table #postcodes

select  cb_key_household
        ,postcode
into #postcodes
from OM114_BARCLAYS_RESPONSE
where cb_key_household in (select cb_key_household from v081_Vespa_Universe_demographics where fss_v3_group = 'Unknown Sky')
group by cb_key_household, postcode

select count(*) from #postcodes -- 403
select distinct(cb_key_household),count(*) as count,min(postcode), max(postcode) from #postcodes group by cb_key_household order by count desc
select distinct(cb_key_household),count(*) as count,min(postcode), max(postcode) from #postcodes group by cb_key_household order by count desc


select distinct(postcode)
        ,count(cb_key_household) as barclays_unknown_reponders
from #postcodes
group by postcode
order by barclays_unknown_reponders desc



drop table #test

select distinct(upper(mailable_postcode)) as postcode
        --, h_mosaic_uk_group,h_mosaic_uk_type,
        ,pc_mosaic_uk_type
        ,pc_mosaic_uk_type_desc = Case when pc_mosaic_uk_type = '01' then  'Global Power Brokers'
                                        when pc_mosaic_uk_type = '02' then  'Voices of Authority'
                                        when pc_mosaic_uk_type = '03' then  'Business Class'
                                        when pc_mosaic_uk_type = '04' then  'Serious Money'
                                        when pc_mosaic_uk_type = '05' then  'Mid-Career Climbers'
                                        when pc_mosaic_uk_type = '06' then  'Yesterdays Captains'
                                        when pc_mosaic_uk_type = '07' then  'Distinctive Success'
                                        when pc_mosaic_uk_type = '08' then  'Dormitory Villagers'
                                        when pc_mosaic_uk_type = '09' then  'Escape to the Country'
                                        when pc_mosaic_uk_type = '10' then  'Parish Guardians'
                                        when pc_mosaic_uk_type = '11' then  'Squires Among Locals'
                                        when pc_mosaic_uk_type = '12' then  'Country Loving Elders'
                                        when pc_mosaic_uk_type = '13' then  'Modern Agribusiness'
                                        when pc_mosaic_uk_type = '14' then  'Farming Today'
                                        when pc_mosaic_uk_type = '15' then  'Upland Struggle'
                                        when pc_mosaic_uk_type = '16' then  'Side Street Singles'
                                        when pc_mosaic_uk_type = '17' then  'Jacks of All Trades'
                                        when pc_mosaic_uk_type = '18' then  'Hardworking Families'
                                        when pc_mosaic_uk_type = '19' then  'Innate Conservatives'
                                        when pc_mosaic_uk_type = '20' then  'Golden Retirement'
                                        when pc_mosaic_uk_type = '21' then  'Bungalow Quietude'
                                        when pc_mosaic_uk_type = '22' then  'Beachcombers'
                                        when pc_mosaic_uk_type = '23' then  'Balcony Downsizers'
                                        when pc_mosaic_uk_type = '24' then  'Garden Suburbia'
                                        when pc_mosaic_uk_type = '25' then  'Production Managers'
                                        when pc_mosaic_uk_type = '26' then  'Mid-Market Families'
                                        when pc_mosaic_uk_type = '27' then  'Shop Floor Affluence'
                                        when pc_mosaic_uk_type = '28' then  'Asian Attainment'
                                        when pc_mosaic_uk_type = '29' then  'Footloose Managers'
                                        when pc_mosaic_uk_type = '30' then  'Soccer Dads and Mums'
                                        when pc_mosaic_uk_type = '31' then  'Domestic Comfort'
                                        when pc_mosaic_uk_type = '32' then  'Childcare Years'
                                        when pc_mosaic_uk_type = '33' then  'Military Dependants'
                                        when pc_mosaic_uk_type = '34' then  'Buy-to-Let Territory'
                                        when pc_mosaic_uk_type = '35' then  'Brownfield Pioneers'
                                        when pc_mosaic_uk_type = '36' then  'Foot on the Ladder'
                                        when pc_mosaic_uk_type = '37' then  'First to Move In'
                                        when pc_mosaic_uk_type = '38' then  'Settled Ex-Tenants'
                                        when pc_mosaic_uk_type = '39' then  'Choice Right to Buy'
                                        when pc_mosaic_uk_type = '40' then  'Legacy of Labour'
                                        when pc_mosaic_uk_type = '41' then  'Stressed Borrowers'
                                        when pc_mosaic_uk_type = '42' then  'Worn-Out Workers'
                                        when pc_mosaic_uk_type = '43' then  'Streetwise Kids'
                                        when pc_mosaic_uk_type = '44' then  'New Parents in Need'
                                        when pc_mosaic_uk_type = '45' then  'Small Block Singles'
                                        when pc_mosaic_uk_type = '46' then  'Tenement Living'
                                        when pc_mosaic_uk_type = '47' then  'Deprived View'
                                        when pc_mosaic_uk_type = '48' then  'Multicultural Towers'
                                        when pc_mosaic_uk_type = '49' then  'Re-Housed Migrants'
                                        when pc_mosaic_uk_type = '50' then  'Pensioners in Blocks'
                                        when pc_mosaic_uk_type = '51' then  'Sheltered Seniors'
                                        when pc_mosaic_uk_type = '52' then  'Meals on Wheels'
                                        when pc_mosaic_uk_type = '53' then  'Low Spending Elders'
                                        when pc_mosaic_uk_type = '54' then  'Clocking Off'
                                        when pc_mosaic_uk_type = '55' then  'Backyard Regeneration'
                                        when pc_mosaic_uk_type = '56' then  'Small Wage Owners'
                                        when pc_mosaic_uk_type = '57' then  'Back-to-Back Basics'
                                        when pc_mosaic_uk_type = '58' then  'Asian Identities'
                                        when pc_mosaic_uk_type = '59' then  'Low-Key Starters'
                                        when pc_mosaic_uk_type = '60' then  'Global Fusion'
                                        when pc_mosaic_uk_type = '61' then  'Convivial Homeowners'
                                        when pc_mosaic_uk_type = '62' then  'Crash Pad Professionals'
                                        when pc_mosaic_uk_type = '63' then  'Urban Cool'
                                        when pc_mosaic_uk_type = '64' then  'Bright Young Things'
                                        when pc_mosaic_uk_type = '65' then  'Anti-Materialists'
                                        when pc_mosaic_uk_type = '66' then  'University Fringe'
                                        when pc_mosaic_uk_type = '67' then  'Study Buddies'
                                        when pc_mosaic_uk_type = '99' then  'Unclassified'
                                        else null end
into #test
from sk_prod.experian_consumerview
where mailable_postcode in (select upper(postcode) from #postcodes)


select top 10 * from #test

select * from #Test order by postcode




select  count(distinct(cb_key_household))
from OM114_BARCLAYS_RESPONSE
where cb_key_household in (select cb_key_household from v081_Vespa_Universe_demographics where fss_v3_group = 'Unknown Sky')




select distinct(fss_v3_group) from v081_Vespa_Universe_demographics





--  social grade vs unknown FSS segment



select social_grade
        ,sum(weighting)
from v081_Vespa_Universe_demographics
where fss_v3_group = 'Unknown Sky'
group by social_grade



drop table #caci_sc1




select  c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into #caci_sc1
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
  and c.cb_key_individual in (select cb_key_household from v081_Vespa_Universe_demographics where fss_v3_group = 'Unknown Sky')
order by c.cb_key_household;




select








