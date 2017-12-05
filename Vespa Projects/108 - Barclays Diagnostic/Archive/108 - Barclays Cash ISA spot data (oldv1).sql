/*------------------------------------------------------------------------------
        Project: V108 - Barclays Spot Analysis
        Version: 1
        Created: 20121016
        Lead: Sarah Moore
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
        Purpose
        -------
        Create a template process for Spot analysis (in This case for Barclays Cash ISA)

        SECTIONS
        --------
        PART A   - Create List of relevant Spots
        PART B   - Add on Channel details (according to Service Key Lookup)
        PART C   - Extract Viewing data for relevant period
             C01 - Viewing table for period
             C03 - Clean data
             C03 - Scale - TBC As Universe Needs to 
             C04 - Categorise Viewing

        PART D  - Output

        Tables
        -------
      
*/
--select top 100 * from vespa_analysts.ph1_VESPA_DAILY_AUGS_20120301;
--select top 100 * from  sk_prod.VESPA_STB_PROG_EVENTS_20120301;

--select count(*) from Project_108_viewing_table_dump ;
--select max(broadcast_date) from Project_108_viewing_table_dump ;
----Project 108 - Barclays Diagnostic---


---Aim of code is to create a template version to enable analysis of Spot Data---


---Part A - Create a List of relevant Spots---
if object_id('BARB_SPOT_DATA_PROJECT_108') is not null drop table BARB_SPOT_DATA_PROJECT_108;
select * into BARB_SPOT_DATA_PROJECT_108 from neighbom.BARB_MASTER_SPOT_DATA
where clearcast_commercial_no='BBHBBPR155030'
;

commit;

---Part B - Add on Channel Name Details

alter table BARB_SPOT_DATA_PROJECT_108 add full_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_108 add vespa_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_108 add channel_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_108 add techedge_name varchar(255);
alter table BARB_SPOT_DATA_PROJECT_108 add infosys_name varchar(255);


update BARB_SPOT_DATA_PROJECT_108 
set a.full_name=b.full_name
,a.vespa_name=b.vespa_name
,a.channel_name=b.channel_name
,a.techedge_name=b.techedge_name
,a.infosys_name=b.infosys_name
from BARB_SPOT_DATA_PROJECT_108 as a
left outer join VESPA_ANALYSTS.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as b
on a.service_key=b.service_key
where a.local_date_of_transmission between b.effective_from and b.effective_to
;
commit; 

---Remeove trailing spaces from Full_Name field to crete a field to match to lookup to be used to match to EPG data
alter table BARB_SPOT_DATA_PROJECT_108 add spot_channel_name varchar(255);
update BARB_SPOT_DATA_PROJECT_108 
set spot_channel_name = trim(full_name)
from BARB_SPOT_DATA_PROJECT_108 
;
commit;
create  hg index idx1 on BARB_SPOT_DATA_PROJECT_108(local_date_of_transmission);
create  hg index idx2 on BARB_SPOT_DATA_PROJECT_108(spot_channel_name);

----Load in Channel details to create lookup from Spot Data to EPG Data---

if object_id('PROJECT_108_SPOT_EPG_LOOKUP') is not null drop table PROJECT_108_SPOT_EPG_LOOKUP;
create table PROJECT_108_SPOT_EPG_LOOKUP
(spot_channel_name varchar(255)
,epg_channel_name varchar(255)
)
;
commit;
input into PROJECT_108_SPOT_EPG_LOOKUP from 'C:\Users\barnetd\Documents\V108 - Barclays Diagnostic\Spot to EPG Lookup.csv' format ascii;
commit;

create  hg index idx5 on PROJECT_108_SPOT_EPG_LOOKUP(spot_channel_name);

alter table BARB_SPOT_DATA_PROJECT_108 add epg_channel_match_name varchar(255);
update BARB_SPOT_DATA_PROJECT_108 
set epg_channel_match_name =epg_channel_name
from BARB_SPOT_DATA_PROJECT_108 as a
left outer join PROJECT_108_SPOT_EPG_LOOKUP as b
on a.spot_channel_name=b.spot_channel_name
commit;

create  hg index idx3 on BARB_SPOT_DATA_PROJECT_108(epg_channel_match_name);

select local_break_start_date_time
,local_break_end_date_time
,utc_break_start_date_time
,utc_break_end_date_time
,spot_break_total_duration
,break_type
,spot_type
,local_spot_start_date_time
,local_spot_end_date_time
,utc_spot_start_date_time
,utc_spot_end_date_time
,spot_duration
,clearcast_commercial_no
,sales_house_identifier
,spot_position_in_break
,no_spots_in_break
,full_name
,vespa_name
,channel_name
,techedge_name
,infosys_name
,spot_channel_name
,epg_channel_match_name
into BARB_SPOT_DATA_PROJECT_108_DEDUPED
from BARB_SPOT_DATA_PROJECT_108
group by local_break_start_date_time
,local_break_end_date_time
,utc_break_start_date_time
,utc_break_end_date_time
,spot_break_total_duration
,break_type
,spot_type
,local_spot_start_date_time
,local_spot_end_date_time
,utc_spot_start_date_time
,utc_spot_end_date_time
,spot_duration
,clearcast_commercial_no
,sales_house_identifier
,spot_position_in_break
,no_spots_in_break
,full_name
,vespa_name
,channel_name
,techedge_name
,infosys_name
,spot_channel_name
,epg_channel_match_name
;

commit;
create  hg index idx1 on BARB_SPOT_DATA_PROJECT_108_DEDUPED(epg_channel_match_name);
create  hg index idx2 on BARB_SPOT_DATA_PROJECT_108_DEDUPED(utc_spot_start_date_time);
create  hg index idx3 on BARB_SPOT_DATA_PROJECT_108_DEDUPED(utc_spot_end_date_time);
commit;

--select top 100 * from BARB_SPOT_DATA_PROJECT_108 where upper(channel_name) like '%SKY%';
--------------------------------------------------------------------------------
-- PART C SETUP - Extract Viewing data
--------------------------------------------------------------------------------

/*
PART C   - Extract Viewing data
     C01 - Viewing table for period
     C03 - Clean data
     
*/

CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;


-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-02-29';
SET @var_prog_period_end    = '2012-04-13';
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @var_num_days = 45;
--select @var_num_days;
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
create  hg index idx3 on VESPA_Programmes_project_108(channel_name);
commit;
------ C01 - Viewing table for period
-- C01 - Viewing table for period
commit;

if object_id('Project_108_viewing_table_dump') is not null drop table Project_108_viewing_table_dump;
create table Project_108_viewing_table_dump (
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
);

commit;
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Project_108_viewing_table_dump(
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
from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join VESPA_Programmes as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
inner join BARB_SPOT_DATA_PROJECT_108_DEDUPED as spot
on prog.Channel_Name=spot.epg_channel_match_name
where  
    (           viewing_starts between utc_spot_start_date_time and utc_spot_end_date_time 
        or      viewing_stops between utc_spot_start_date_time and utc_spot_end_date_time 
        or      viewing_starts < utc_spot_start_date_time and viewing_stops> utc_spot_end_date_time 
    )
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @scanning_day = @var_prog_period_start;
--delete from Project_108_viewing_table_dump;
commit;
while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

--select top 500 subscriber_id,channel_name, viewing_starts,viewing_stops ,utc_spot_start_date_time,utc_spot_end_date_time, utc_break_start_date_time,utc_break_end_date_time from Project_108_viewing_table_dump;
--select infosys_name ,Epg_Title,count(distinct subscriber_id), count(*) as records from Project_108_viewing_table_dump group by infosys_name,Epg_Title order by records desc;
--select * from BARB_SPOT_DATA_PROJECT_108  where utc_spot_start_date_time='2012-02-29 12:07:38'

--------------------------------------------------------------- A02 - Clean data
-- A02 - Clean data

-- Clean viewing data
create table Project_108_viewing_table_20120919 (
Viewing_date                    date
,Broadcast_date                 date
,cb_row_ID                      bigint          not null
,Account_Number                 varchar(30)     not null
,Subscriber_Id                  decimal(8,0)    not null
,Cb_Key_Household               bigint
,Cb_Key_Family                  bigint
,Cb_Key_Individual              bigint
,Event_Type                     varchar(30)
,X_Type_Of_Viewing_Event        varchar(40)     not null
,Event_Start_Time               datetime
,Event_end_time                 datetime
,Tx_Start_Datetime_UTC          datetime
,Tx_End_Datetime_UTC            datetime
,viewing_starts                 datetime
,viewing_stops                  datetime
,viewing_duration               decimal(3,1)
,Recorded_Time_UTC              datetime
,timeshifting                   varchar(10)
,X_Viewing_Time_Of_Day          varchar(15)
,Programme_Trans_Sk             bigint
,Channel_Name                   varchar(30)
,Epg_Title                      varchar(50)
,Genre_Description              varchar(30)
,Sub_Genre_Description          varchar(30)
,capped_flag                    tinyint
,Programme_Duration             decimal(3,1)
,Perc_prog_viewed               decimal(3,1)
,scaling_date                   date
,scaling_segment_id             bigint
,scaling_weighting              float
,channel_name_inc_hd            varchar(40)
,HD_channel                     bit             default 0
,Pay_Channel                    bit             default 0
,viewing_category               varchar(40)
,niche_category                 varchar(40)
,rank                           int
);
-- Clear up data - one record per start time per box

insert into Project_108_viewing_table_20120919(
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
,rank)
select
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
,rank() over (partition by subscriber_id, viewing_starts order by viewing_duration desc, cb_row_id desc) as rank -- take largest duration per box per start time
from Project_108_viewing_table_dump
order by subscriber_id, viewing_starts, viewing_stops;

-- investigate rank > 1

delete from Project_108_viewing_table_20120919 where rank > 1;
--24644 Row(s) affected
commit;

--select count(*) from Project_108_viewing_table_20120919;


--select top 100 * from vespa_analysts.ph1_VESPA_DAILY_AUGS_20120501;