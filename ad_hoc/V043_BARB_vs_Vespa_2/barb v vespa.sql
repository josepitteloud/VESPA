


/*
--------------------------------------------------------------------------------
BARB V VESPA PHASE II
--------------------------------------------------------------------------------
             A) SET UP - CREATE VARIABLES
             B) GET THE VIEWING DATA -  B01 - Identify which programs were shown over the BARB week
                                        B02 - Identify acocunts returning data over the whole period
                                        B03 - Get the viewing data from VESPA
             C) NEW CAPPING CODE        C01 - Create the caps
                                                    C01 a* - Collect viewing data
                                                    C01 b - Build "one_week-new" table of viewing data for caps
                                                    C01 c - Add the customer DB metadata
                                                    C01 d - Make all the Ntiles
                                                    C01 e - These are tables of ntiles?
                                                    C01 f - All the different tables of caps
                                                    C01 g - Moving caps back onto central tables
                                                    C01 h - Global capping bounds
                                                    C01 i - Distribution of capping bounds (just for QA)
                                        C02 - Apply the caps to the viewing data
                                                    C02 a* - Adding customer metadata to viewing
                                                    C02 b - Determine capping application
                                                    C02 c - Get bounds on duration-replacement lookup
                                                    C02 d - Randomly choose replacement duration for capped events
                                                    C02 e - Assign new end time
                                                    C02 f* - Put capping on original main viewing table
                                                    C02 g - Investigation of total viewing during process
                                                    C02 h - Profiling durations before and after capping
             D) ADD ADDITIONAL FIELDS TO THE VIEWING DATA
                                        D01 - Add Playback and Vosdal flags
                                        D02 - Add Barb day and weighted_viewing_duration to the viewing data (Barb day = 4am - 4am)
                                        D03 - Add Value Segments
                                        D04 - Add time of day
                                        D05 - Add HD Box/Subscription Flag
                                        D06 - Add current pack
             E) NEW SCALING CODE
                                        E01 - Create scaling tables
                                                    E01 a - scaling_weights
                                                    E01 b - scaling_weekly_sample
                                                    E01 c - Scaling_segments
                                                    E01 d - Scaling_categories
                                                    E01 e - Scaling_box_level_viewing
                                                    E01 f - scaling_metrics
                                        E02   - Get weekly sample
                                                    E02 a - Clear tables
                                                    E02 b - Declare variables
                                                    E02 c - Get weekly sample
                                                    E02 d - Populate scaling variables
                                                    E02 e - Append segment id
                                        E03  - Get daily weights
                                                    E03 a - Create Scaling variables table temp table
                                                    E03 b - Declare variables
                                                    E03 c - loop across 7 days of scaling week
                                                    E03 d - Clear tables
                                                    E03 e - Flag accounts with complete viewing data
                                                    E03 f - Rim-weighting
                                                    E03 g - Update historical weights table with weights for this scaling date
                                                    E03 h - Update QA tables with scaling metrics
             F) SCALE UP BARB MINS/ CREATE AUDIENCE OF 1+ MINS PAY TV
             G) OUTPUTS
                                                    G01 - Create variables and daily viewing viewing totals
                                                    G02 - Output: paid_free SOV table
                                                    G03 - Output: SOV by Channel
                                                    G04 - Add Paid/Free Deciles (proportion of paid viewing)
                                                    G05 - OUTPUT: SOV by Deciles 1
                                                    G06 - Add Paid/Free Deciles (total viewing)
                                                    G07 - OUTPUT: SOV by Deciles 2

*: control totals on viewing data total duration taken after this section

Also: QA Extraction are marked by "##QA##EQ##" so do a search for that string to find the
queries to run for control totals, etc. They'll be commented out as they're not part of
the main build, but everything they need should be constructed.
                                                    
*/

--------------------------------------------------------------------------------
-- A) SET UP.
--------------------------------------------------------------------------------
-- create and populate variables
CREATE VARIABLE @var_period_start_data  datetime;
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;
Create VARIABLE @var_period_start_dt    datetime;
Create VARIABLE @var_scan_start_dt      datetime;
Create VARIABLE @var_period_end_dt      datetime;


CREATE VARIABLE @var_barb_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @i                      integer;

-- Scaling Variables
Create variable @target_date date; -- This guy is not actually scaling, this guy determines Primary / Secondary for Capping Metadata


-- Set the date variables
SET @var_period_start_dt = '2012-01-30 06:00:00'; -- Mon
SET @var_scan_start_dt   = dateadd(hour, -3, @var_period_start_dt); -- need to go back a bit from the period start because we'll need to cap things that spill into our period of analysis when they shouldn't
SET @var_period_end_dt   = '2012-02-06 06:00:00'; -- Mon
-- the above are used to get the correct viewing information out of vespa

SET @var_period_start_data      = '2012-01-30'; -- Second date variable to be manipulated within a loop of @var_period_start
SET @var_period_start           = '2012-01-30'; -- Monday
SET @var_period_end             = '2012-02-06'; -- Mon
SET @var_barb_period_end        = '2012-02-12'; -- following Sunday (7 day BARB window for playback, we only need 163 hours)

-- Dunno what target date should be, but let's start with something in the period of analysis...
SET @target_date                = @var_period_end;

/*
--------------------------------------------------------------------------------
-- B) - Get The viewing Data
--------------------------------------------------------------------------------
             B01 - Identify which programs were shown over the BARB week
             B02 - Identify acocunts returning data over the whole period
             B03 - Get the viewing data from VESPA

--------------------------------------------------------------------------------
*/


--------------------------------------------------------------------------------
-- B01 - Identify all programs shown over the BARB week
--------------------------------------------------------------------------------
-- this ensures only the weeks data is collected from the viewing data

IF object_id('week_Programmes') IS NOT NULL DROP TABLE week_Programmes;

select
      programme_trans_sk
      ,Channel_Name
      ,epg_channel
      ,pay_free_indicator
      ,epg_group_name
      ,network_indicator
      ,Genre_Description
      ,Sub_Genre_Description
      ,EPG_title
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,tx_date_time_utc
      ,tx_date_utc as program_air_date
      ,tx_start_datetime_utc as program_air_datetime
  into week_Programmes -- drop table vespa_programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_end_datetime_utc >= @var_scan_start_dt
   and tx_start_datetime_utc <=@var_period_end_dt;


create unique hg index idx1 on week_Programmes(programme_trans_sk);



--------------------------------------------------------------------------------
-- B02 - identify boxes returning data over the whole period
--------------------------------------------------------------------------------

-------------------------------------------------------------------------------

--identify boxes that returned data over the entire 2 week period;

IF object_id('reporting_boxes_store') IS NOT NULL DROP TABLE reporting_boxes_store;

create table reporting_boxes_store (
   -- subscriber_id decimal(8)
    account_number varchar(20)
    ,reporting_day varchar(8)
);

SET @var_sql = '
    insert into reporting_boxes_store
    select distinct(account_number), ''##^^*^*##''
from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##
    where (play_back_speed is null or play_back_speed = 2)
        and x_programme_viewed_duration > 0
        and Panel_id in (4,5)
      --  and programme_trans_sk in (select programme_trans_sk from week_Programmes)
';

-- loop though each days viewing logs to identify repeat data returners
SET @var_cntr = 0;
SET @i=datediff(dd,@var_period_start,@var_barb_period_end);

WHILE @var_cntr <= @i
BEGIN
        EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_period_start), 'yyyymmdd')))

        COMMIT

        SET @var_cntr = @var_cntr + 1
END;
--259213 Row(s) affected



-- count the number of distict days each account returned data for
select distinct(account_number)
        , count(distinct(reporting_day)) as number_days
into #temp
from reporting_boxes_store
group by account_number;
-- 297014 Row(s) affected

-- put the accounts that returned data for the whole period into a new table

IF object_id('enabled_and_returning') IS NOT NULL DROP TABLE enabled_and_returning;

select distinct(account_number)
into poveys.enabled_and_returning
from #temp
where number_days = 14;
-- 205859 Row(s) affected






--------------------------------------------------------------------------------
-- B03 - Get the viewing data
--------------------------------------------------------------------------------


IF object_id('sov_daily_records') IS NOT NULL DROP TABLE sov_daily_records;

create table sov_daily_records (
    cb_row_ID                       bigint      not null primary key
    ,Account_Number                 varchar(20) not null
    ,Subscriber_Id                  integer
    ,Service_instance_id            varchar(30)                 -- Added for scaling purposes
    ,Cb_Key_Household               bigint
    ,Cb_Key_Family                  bigint
    ,Cb_Key_Individual              bigint
    ,Event_Type                     varchar(20) not null
    ,X_Type_Of_Viewing_Event        varchar(40) not null
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,X_Viewing_Start_Time           datetime
    ,X_Viewing_End_Time             datetime
    ,X_Viewing_Time_Of_Day            varchar(15)
     ,Tx_Start_Datetime_UTC            datetime
    ,Tx_End_Datetime_UTC              datetime
    ,tx_date_time_utc                 datetime
    ,Recorded_Time_UTC              datetime
    ,Play_Back_Speed                decimal(4,0)
    ,X_Event_Duration               decimal(10,0)
    ,X_Programme_Duration           decimal(10,0)
    ,X_Programme_Viewed_Duration    decimal(10,0)
    ,X_Programme_Percentage_Viewed  decimal(3,0)
    ,Programme_Trans_Sk             bigint      not null
    ,daily_table_date               date
    ,x_channel_name                 varchar(20)
    ,original_event_time            datetime
    ,live                           integer
    ,genre                          varchar(25)
    ,sub_genre                      varchar(25)
    ,epg_title                      varchar(100)
    ,panel_id                       integer
    ,pay_free_indicator             varchar(10)
    ,network_indicator              varchar(55)
    ,epg_channel                    varchar(30)
    ,epg_group_name                 varchar(40)
    ,channel_name                   varchar(30)
    ,program_air_date               date
    ,program_air_datetime           datetime
);
-- use original event time +168 hours to determine the barb viewing window viewing, if made


-- Populate the table
SET @var_sql = '
    insert into sov_daily_records
    select
       vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Service_instance_id, vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time,vw.X_Viewing_Time_Of_Day, prog.Tx_Start_Datetime_UTC, prog.Tx_End_Datetime_UTC, prog.tx_date_time_utc, vw.Recorded_Time_UTC
        ,vw.Play_Back_Speed,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.Programme_Trans_Sk, dateadd(day, @var_cntr, @var_period_start),vw.x_channel_name
        ,vw.original_event_time, case when play_back_speed is null then 1 else 0 end as live, case when prog.Genre_Description is null then ''Unknown'' else prog.Genre_Description end as genre,
        case when prog.Sub_Genre_Description is null then ''Unknown'' else prog.Sub_Genre_Description end as sub_genre, case when prog.Epg_Title is null then ''Unknown'' else prog.Epg_Title end as epg_title, vw.panel_id
        ,prog.pay_free_indicator,prog.network_indicator,prog.epg_channel,prog.epg_group_name,prog.channel_name,prog.program_air_date
        ,prog.program_air_datetime
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
          inner join week_Programmes as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
     where vw.account_number in (select account_number from enabled_and_returning)
        and (play_back_speed is null or play_back_speed = 2)
        and x_programme_viewed_duration > 0
        and Panel_id in (4,5)
     '
              ;

-- loop through the time period to get all relevant viewing events
SET @var_cntr = 0;
SET @i=datediff(dd,@var_period_start,@var_barb_period_end);

WHILE @var_cntr <= @i
        BEGIN
         EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_period_start_data), 'yyyymmdd')))-- the previous day
        COMMIT

         SET @var_cntr = @var_cntr + 1
END;


-- Index the resulting table
create hg index subscriber_id_index on sov_daily_records (subscriber_id);
create hg index account_number_index on sov_daily_records (account_number);
-- Added for scaling purposes
create hg index service_instance_id_index on sov_daily_records (service_instance_id);


-------------------------------------------------------------------------------------------------
-- C01) CREATE THE CAPS
-------------------------------------------------------------------------------------------------


-------------------------------------------------------------------------------------------------
-- C01 a) ASSEMBLE REQUIRED DAILY VIEWING DATA
-------------------------------------------------------------------------------------------------


IF object_id('sov_daily_records_new2') IS NOT NULL DROP TABLE sov_daily_records_new2;
IF object_id('sov_daily_records_new3') IS NOT NULL DROP TABLE sov_daily_records_new3;
IF object_id('sov_daily_records_new4') IS NOT NULL DROP TABLE sov_daily_records_new4;

--create rank to reorder views as cb row id not ordered correctly
select t1.*
,rank() over (partition by subscriber_id, adjusted_event_start_time order by x_viewing_start_time,tx_start_datetime_utc) as prank
into --drop table
sov_daily_records_new2
from sov_daily_records t1;
commit;
--34724164

--use rank (instead of cb row id) to order views in correct order for cumulative duration
select t1.*
,sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by prank) as x_cumul_programme_viewed_duration
into --drop table
sov_daily_records_new3
from sov_daily_records_new2 t1;
commit;
--34724164

--drop ranks
alter table sov_daily_records_new3
drop prank;

--add indexes to improve performance
create hg index idx1 on sov_daily_records_new3(subscriber_id);
create dttm index idx2 on sov_daily_records_new3(adjusted_event_start_time);
create dttm index idx3 on sov_daily_records_new3(recorded_time_utc);
create lf index idx4 on sov_daily_records_new3(Live);
create dttm index idx5 on sov_daily_records_new3(x_viewing_start_time);
create dttm index idx6 on sov_daily_records_new3(x_viewing_end_time);
create hng index idx7 on sov_daily_records_new3(x_cumul_programme_viewed_duration);

-- update the viewing start and end times for playback records -- viewing start and end times not correctly populated for some playback events
update sov_daily_records_new3
    set
        x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
    where
        recorded_time_utc is not null;
commit;
--9879680

update sov_daily_records_new3
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
sov_daily_records_new4
from sov_daily_records_new3 t1;
commit;
--34724164

--remove duplicate programme titles
delete from sov_daily_records_new4
where xrank>1;
--782

--remove illegitimate playback views - these views are those that start on event end time and go beyond event end time
delete from sov_daily_records_new4
where X_Adjusted_Event_End_Time<x_viewing_end_time
and x_viewing_start_time>=X_Adjusted_Event_End_Time;
--3628

--reset x_viewing_end_times for playback views
update sov_daily_records_new4
set x_viewing_end_time=X_Adjusted_Event_End_Time
where X_Adjusted_Event_End_Time<x_viewing_end_time
and x_viewing_start_time<X_Adjusted_Event_End_Time;
commit;
--156

-- So indices are good if we expect to be able to process at any decent rate:
create index index1 on sov_daily_records_new4 (x_viewing_start_time);
create index index2 on sov_daily_records_new4 (adjusted_event_start_time);
create index index3 on sov_daily_records_new4 (subscriber_id,adjusted_event_start_time);
create index index4 on sov_daily_records_new4 (Account_Number);
create index index5 on sov_daily_records_new4 (Cb_Key_Household);

--add start day and start hour variables
alter table sov_daily_records_new4
add event_start_day integer,
add event_start_hour integer;

update sov_daily_records_new4
set event_start_hour= datepart (hour, adjusted_event_start_time)
   ,event_start_day = datepart(day,adjusted_event_start_time);
commit;
--34723382

-- That table "sov_daily_records_new4" is the one that we take as our ball of viewing data.
-- It also gets used later in the actual report section, which is a funny way of doing stuff,
-- but whatever.

-- Okay, so we want some basic counts of total events and things like that, maybe even a
-- profile of event duration distribution...

IF object_id('V043_viewing_control_totals') IS NOT NULL DROP TABLE V043_viewing_control_totals;

select
    convert(varchar(20), '1.) Collect') as data_state
    ,program_air_date
    ,live
    ,genre
    ,count(1) as viewing_records
    ,round(sum(datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0 / 60 / 24.0, 2) as total_viewing_in_days
into V043_viewing_control_totals
from sov_daily_records_new4
group by program_air_date, live, genre;

commit;
create unique index fake_pk on V043_viewing_control_totals (data_state, program_air_date, live, genre);
grant select on V043_viewing_control_totals to public;
commit;

-- Distribution of event profiles will get done later...

-------------------------------------------------------------------------------------------------
-- C01 b) ASSEMBLE DATA USED TO ACTUALLY BUILD CAPS
-------------------------------------------------------------------------------------------------

IF object_id('one_week_new') IS NOT NULL DROP TABLE one_week_new;

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
       Live,
       count(*) as num_views,
       count(distinct genre) as num_genre,
       count(distinct sub_genre) as num_sub_genre,
       sum(x_programme_viewed_duration) as viewed_duration
into --drop table
one_week_new
from sov_daily_records_new4
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
       Live;
--25940851

--add indexes to improve performance
create index idx1 on one_week_new(subscriber_id, adjusted_event_start_time);
create dttm index idx2 on one_week_new(adjusted_event_start_time);
create lf index idx3 on one_week_new(Live);

-------------------------------------------------------------------------------------------------
-- C01 b) ATTACH METADATA TO CAPPING TABLE
-------------------------------------------------------------------------------------------------

IF object_id('genre_new') IS NOT NULL DROP TABLE genre_new;

--obtain channel, genre, sub_genre at start of event
select
    -- OK, so we're clipping genre_new down to things that actually get referenced:
    subscriber_id
    ,adjusted_event_start_time
    ,genre
    ,sub_genre
    ,channel_name
    -- Things needed to assign caps to end of first program viewed (sectino C02.e)
    ,X_Adjusted_Event_End_Time
    ,x_viewing_end_time
    ,rank() over(partition by subscriber_id, adjusted_event_start_time order by x_viewing_start_time,tx_start_datetime_utc) as trank
into --drop table
genre_new
from sov_daily_records_new4 t1;
commit;
--34723382

-- delete all records which aren't necessary due to trank
delete from genre_new
where trank <> 1;

--add indexes to improve performance
create unique index idx1 on genre_new(subscriber_id, adjusted_event_start_time);
-- Will this unique thing fail? we hope not, though findout we've been processing
-- duplicates this time would also be pretty bad...

--add channel, genre and sub genre
alter table one_week_new
add genre_at_event_start_time varchar(30),
add sub_genre_at_event_start_time varchar(30),
add channel_at_event_start_time varchar(30),
add pack varchar(100) default null,
add pack_grp varchar(20) default null;

update one_week_new t1
set genre_at_event_start_time=t2.genre
   ,sub_genre_at_event_start_time=t2.sub_genre
   ,channel_at_event_start_time=t2.channel_name
from genre_new t2
where t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time;
commit;
--25940851

--add pack & network [Update: Network no longer in play]
update one_week_new t1
set pack=t2.pack
from channel_lookup t2
where upper(trim(t1.channel_at_event_start_time))=upper(trim(t2.epg_channel));
commit;
--25925333

--add pack groups
update one_week_new
set pack_grp=case when pack in ('Diginets','Terrestrial') then pack else 'Other' end
from one_week_new;
commit;
--25940851

--add event duration bands
alter table one_week_new
add dur_mins            int,
add dur_days            int,
add band_dur_days       smallint;

update one_week_new
set dur_mins   = cast(x_event_duration/ 60    as int)
   ,dur_days   = cast(x_event_duration/ 86400 as int);
commit;
--25940851

--new column band_dur_days which is 0 for events limited to 1 day in duration, 1 otherwise.
--this is due to durations longer than 1 day
update one_week_new
set band_dur_days  = case when dur_days = 0 then 0 else 1 end;
commit;
--25940851

IF object_id('all_boxes_info_new') IS NOT NULL DROP TABLE all_boxes_info_new;

--get all primary and secondary sub details for all accounts with viewing on any box
select distinct a.account_number
,b.service_instance_id
,b.subscription_sub_type
into --drop table
all_boxes_info_new
from one_week_new as a
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
create hg index idx1 on all_boxes_info_new(service_instance_id);

--select count(*),count(distinct account_number||service_instance_id),count(distinct account_number) from all_boxes_info_new
--122569  114583
--select count(*),count(distinct account_number) from one_week_new
--25940851        114585


IF object_id('subs_details_new') IS NOT NULL DROP TABLE subs_details_new;

--create src_system_id lookup
select src_system_id
,cast(si_external_identifier as integer) as subscriberid
,si_service_instance_type
,effective_from_dt
,effective_to_dt
,cb_row_id
,rank() over(partition by src_system_id order by effective_from_dt desc,cb_row_id desc) as xrank
into --drop table
subs_details_new
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
and effective_from_dt<=@target_date
and effective_to_dt>@target_date;
commit;
--27724595

--create index
create hg index idx1 on subs_details_new(src_system_id);

--remove dups
delete from subs_details_new
where xrank>1;
--7723855

--select * from subs_details_new where subscriberid is null
delete from subs_details_new
where subscriberid is null;
--1477

--select count(*),count(distinct src_system_id||subscriberid),count(distinct src_system_id),count(distinct subscriberid) from subs_details_new
--19999263        19997650

--add sub id
alter table all_boxes_info_new
add subscriber_id integer default null;

update all_boxes_info_new t1
set subscriber_id=t2.subscriberid
from subs_details_new t2
where t1.service_instance_id=t2.src_system_id;
commit;
--121386

--check data
--select count(*),count(distinct service_instance_id),count(distinct subscriber_id),count(distinct service_instance_id||subscriber_id) from all_boxes_info_new
--122569  122569  121386  122569
--select count(*) from all_boxes_info_new where subscriber_id is null
--1183

--add primary/secondary flag to events
alter table one_week_new
add src_system_id varchar(50),
add box_subscription varchar(1) default 'U';

update one_week_new
set src_system_id=b.service_instance_id
   ,box_subscription=case when b.SUBSCRIPTION_SUB_TYPE='DTV Primary Viewing' then 'P'
                          when b.SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' then 'S'
                          else 'U'
                     end
from one_week_new as a
left outer join
all_boxes_info_new as b
on a.subscriber_id=b.subscriber_id;
commit;
--25940851

--select distinct box_subscription from one_week_new

-------------------------------------------------------------------------------------------------
-- C01 d) BUILDING N-TILES FOR CAPPING
-------------------------------------------------------------------------------------------------

IF object_id('ntiles_week_new') IS NOT NULL DROP TABLE ntiles_week_new;

--calculate ntiles for caps
select   band_dur_days
        ,Live
        ,cast(adjusted_event_start_time as date) as event_date
        ,event_start_day
        ,event_start_hour
        ,box_subscription
        ,pack_grp
        ,genre_at_event_start_time
        ,dur_mins
        ,ntile(200) over (partition by Live,event_start_day order by x_event_duration) as ntile_lp
        ,ntile(200) over (partition by Live,event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time order by x_event_duration) as ntile_1
        ,ntile(200) over (partition by Live,event_start_day,event_start_hour,pack_grp,genre_at_event_start_time order by x_event_duration) as ntile_2
        ,x_event_duration
        ,viewed_duration
        ,num_views
into --drop table
ntiles_week_new
from one_week_new
where band_dur_days = 0;
--25928067

--create indexes
create hng index idx1 on ntiles_week_new(event_start_day);
create hng index idx2 on ntiles_week_new(event_start_hour);
create hng index idx3 on ntiles_week_new(Live);
create hng index idx4 on ntiles_week_new(box_subscription);
create hng index idx5 on ntiles_week_new(pack_grp);
create hng index idx6 on ntiles_week_new(genre_at_event_start_time);

--select distinct event_date,event_start_day from ntiles_week_new

--check data
--select count(*),sum(num_views) from ntiles_week_new
--count(*)        sum(ntiles_week_new.num_views)
--25928067        34274204

--select count(*),sum(num_views) from one_week_new where band_dur_days = 0
--25928067        34274204

-------------------------------------------------------------------------------------------------
-- C01 f) TABLES OF N-TILES (?)
-------------------------------------------------------------------------------------------------

IF object_id('nt_4_19_new') IS NOT NULL DROP TABLE nt_4_19_new;

--create capping limits for start hours 4-19
SELECT Live
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
nt_4_19_new
FROM ntiles_week_new
where event_start_hour>=4
and event_start_hour<=19
and Live=1
group by Live,event_date,event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time,ntile_1;
--561850

--create indexes
create hng index idx1 on nt_4_19_new(event_start_day);
create hng index idx2 on nt_4_19_new(event_start_hour);
create hng index idx3 on nt_4_19_new(Live);
create hng index idx4 on nt_4_19_new(box_subscription);
create hng index idx5 on nt_4_19_new(pack_grp);
create hng index idx6 on nt_4_19_new(genre_at_event_start_time);

IF object_id('nt_20_3_new') IS NOT NULL DROP TABLE nt_20_3_new;

--create capping limits start hours 20-3
SELECT Live
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
nt_20_3_new
FROM ntiles_week_new
where event_start_hour in (20,21,22,23,0,1,2,3)
and Live=1
group by Live,event_start_day,event_start_hour,box_subscription,pack_grp,genre_at_event_start_time,ntile_2;
--232206

--create indexes
create hng index idx1 on nt_20_3_new(event_start_day);
create hng index idx2 on nt_20_3_new(event_start_hour);
create hng index idx3 on nt_20_3_new(Live);
create hng index idx4 on nt_20_3_new(pack_grp);
create hng index idx5 on nt_20_3_new(genre_at_event_start_time);

IF object_id('nt_lp_new') IS NOT NULL DROP TABLE nt_lp_new;

--create capping limits for playback
SELECT Live
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
nt_lp_new
FROM ntiles_week_new
where Live=0
group by Live,event_start_day,ntile_lp;
--1400

--create indexes
create hng index idx1 on nt_lp_new(event_start_day);
create hng index idx2 on nt_lp_new(Live);

IF object_id('week_caps_new') IS NOT NULL DROP TABLE week_caps_new;

--identify caps for each variable dimension
select distinct Live
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
into --drop table
week_caps_new
from ntiles_week_new;
commit;
--16321

--create indexes
create hng index idx1 on week_caps_new(event_start_day);
create hng index idx2 on week_caps_new(event_start_hour);
create hng index idx3 on week_caps_new(Live);
create hng index idx4 on week_caps_new(box_subscription);
create hng index idx5 on week_caps_new(pack_grp);
create hng index idx6 on week_caps_new(genre_at_event_start_time);

--select count(distinct genre_at_event_start_time) from ntiles_week_new
--9

--select * from week_caps_new;

--add max duration to threshold table
alter table week_caps_new
add max_dur_mins integer;

-------------------------------------------------------------------------------------------------
-- C01 f) ALL KINDS OF DIFFERENT CAPPING TABLES
-------------------------------------------------------------------------------------------------

--obtain max cap limits for live events

IF object_id('h23_3_new') IS NOT NULL DROP TABLE h23_3_new;

--identify ntile threshold for event start hours 23-3
select Live
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time
,min(case when median_dur_mins>=122 then ntile_2 else null end) as pri_ntile
,max(ntile_2) as sec_ntile
,case when pri_ntile is null then sec_ntile-20 else pri_ntile-10 end as cap_ntile
into --drop table
h23_3_new
from nt_20_3_new
where event_start_hour in (23,0,1,2,3)
and Live=1
group by Live
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time;
commit;
--692

--add min duration
alter table h23_3_new
add min_dur_mins integer;

update h23_3_new t1
set min_dur_mins=t2.min_dur_mins
from nt_20_3_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_2;
commit;
--628

--select count(*) from h23_3_new where min_dur_mins is null;
--29

IF object_id('h4_14_new') IS NOT NULL DROP TABLE h4_14_new;

--identify ntile threshold for event start hours 4-14
select Live
,event_date
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
,min(case when median_dur_mins>=243 then ntile_1 else null end) as pri_ntile
,max(ntile_1) as sec_ntile
,case when pri_ntile is null then sec_ntile-20
      when event_start_hour in (4,5,10,11,12,13,14) then pri_ntile-20
      when event_start_hour in (6,7,8,9) and datepart(weekday,event_date) in (1,7) then pri_ntile-18
      when event_start_hour in (6,7,8,9) and datepart(weekday,event_date) in (2,3,4,5,6) then pri_ntile-20
 end as cap_ntile
into --drop table
h4_14_new
from nt_4_19_new
where event_start_hour in (4,5,6,7,8,9,10,11,12,13,14)
and Live=1
group by Live
,event_date
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time;
commit;
--2325

--add min duration
alter table h4_14_new
add min_dur_mins integer;

update h4_14_new t1
set min_dur_mins=t2.min_dur_mins
from nt_4_19_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_1;
commit;
--2417

--select count(*) from h4_14_new where min_dur_mins is null;
--1443


IF object_id('h15_19_new') IS NOT NULL DROP TABLE h15_19_new;

--identify ntile threshold for event start hours 15-19
select Live
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time
,max(ntile_1) as ntile
,ntile-20 as cap_ntile
into --drop table
h15_19_new
from nt_4_19_new
where event_start_hour in (15,16,17,18,19)
and Live=1
group by Live
,event_start_day
,event_start_hour
,box_subscription
,pack_grp
,genre_at_event_start_time;
commit;
--1800

--add min duration
alter table h15_19_new
add min_dur_mins integer;

update h15_19_new t1
set min_dur_mins=t2.min_dur_mins
from nt_4_19_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_1;
commit;
--1328

--select count(*) from h15_19_new where min_dur_mins is null;
--472

IF object_id('h20_22_new') IS NOT NULL DROP TABLE h20_22_new;

--identify ntile threshold for event start hours 20-22
select Live
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time
,min(case when median_dur_mins>=((23-event_start_hour-1)*60)+122 then ntile_2 else null end) as pri_ntile
,max(ntile_2) as sec_ntile
,case when pri_ntile is null then sec_ntile-20 else pri_ntile-15 end as cap_ntile
into --drop table
h20_22_new
from nt_20_3_new
where event_start_hour in (20,21,22)
and Live=1
group by Live
,event_start_day
,event_start_hour
,pack_grp
,genre_at_event_start_time;
commit;
--354

--add min duration
alter table h20_22_new
add min_dur_mins integer;

update h20_22_new t1
set min_dur_mins=t2.min_dur_mins
from nt_20_3_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time
and t1.cap_ntile=t2.ntile_2;
commit;
--348

--select count(*) from h20_22_new where min_dur_mins is null;
--6

-------------------------------------------------------------------------------------------------
-- C01 g) BUILDING CENTRAL LISTING OF DERIVED CAPS
-------------------------------------------------------------------------------------------------

--update threshold table with cap limits
update week_caps_new t1
set max_dur_mins=t2.min_dur_mins
from h23_3_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--1675

--select count(*) from h23_3_new
--692

update week_caps_new t1
set max_dur_mins=t2.min_dur_mins
from h4_14_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--3860

--select count(*) from h4_14_new
--3860

update week_caps_new t1
set max_dur_mins=t2.min_dur_mins
from h15_19_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.box_subscription=t2.box_subscription
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--1800

--select count(*) from h15_19_new
--1800

update week_caps_new t1
set max_dur_mins=t2.min_dur_mins
from h20_22_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.event_start_hour=t2.event_start_hour
and t1.pack_grp=t2.pack_grp
and t1.genre_at_event_start_time=t2.genre_at_event_start_time;
--1001

--select count(*) from h20_22_new
--354

--select count(*) from week_caps_new where max_dur_mins is null and Live=1
--1953

IF object_id('lp_new') IS NOT NULL DROP TABLE lp_new;

--identify ntile threshold for playback events
select Live
,event_start_day
,max(ntile_lp) as ntile
,ntile-2 as cap_ntile
into --drop table
lp_new
FROM nt_lp_new
where Live=0
group by Live,event_start_day;
--7

--add min duration
alter table lp_new
add min_dur_mins integer;

update lp_new t1
set min_dur_mins=t2.min_dur_mins
from nt_lp_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.cap_ntile=t2.ntile_lp;
commit;
--7

--select * from lp;

--update playback limits in caps table
update week_caps_new t1
set max_dur_mins=t2.min_dur_mins
from lp_new t2
where t1.Live=t2.Live
and t1.event_start_day=t2.event_start_day
and t1.max_dur_mins is null;
--7985

--select count(*) from week_caps_new where Live=0
--7985

-------------------------------------------------------------------------------------------------
-- C01 h) GLOBAL CAPPING BOUNDS
-------------------------------------------------------------------------------------------------

--reset capping limits that are less than 20 mins
update week_caps_new
set max_dur_mins=20
where (max_dur_mins is null
or max_dur_mins<20)
and Live=1;
--4408

--reset capping limits that are more than 120 mins
update week_caps_new
set max_dur_mins=120
where max_dur_mins>120
and Live=1;
--72

grant all on week_caps_new to public;

--select count(*) from week_caps_new where max_dur_mins is null
--0

--select count(*) from week_caps_new;
--16321

--select count(*),sum(num_views) from ntiles_week_new
--25928067        34274204

--select * from week_caps_new;

-------------------------------------------------------------------------------------------------
-- C01 i) DISTRIBUTION OF CAPPING BOUNDS JUST FOR QA
-------------------------------------------------------------------------------------------------

-- Note that this isn't a profile over the use of caps, it's just on the caps that as get built;
-- there's no extra weight here for caps that get used more often. Oh, also, all our caps should
-- be between 20 and 120, so that's just a hundred entries that we can just go out and graph...

IF object_id('V043_viewing_control_cap_distrib')            IS NOT NULL DROP TABLE V043_viewing_control_cap_distrib;
-- OK, here we're using the cumulative ranking duplication trick since we don't have any unique
-- keys to force the rank to be unique over entries;
select max_dur_mins
    ,count(1) as hits
into V043_viewing_control_cap_distrib
from week_caps_new
group by max_dur_mins;

grant select on V043_viewing_control_cap_distrib to public;
commit;

/* ##QA##EQ##: Extraction query: graph this guy in Excel I guess
select * from V043_viewing_control_cap_distrib
order by max_dur_mins;
*/

-------------------------------------------------------------------------------------------------
-- C02 APPLYING CAPS TO VIEWING DATA
-------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------
-- C02 a) ATTACH CUSTOMER METADATA TO VIEWING
-------------------------------------------------------------------------------------------------

--add primary/secondary flag to views so thresholds can be applied
alter table sov_daily_records_new4
add src_system_id varchar(50),
add box_subscription varchar(1) default 'U';

update sov_daily_records_new4
set src_system_id=b.service_instance_id
   ,box_subscription=case when b.SUBSCRIPTION_SUB_TYPE='DTV Primary Viewing' then 'P'
                          when b.SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' then 'S'
                          else 'U'
                     end
from sov_daily_records_new4 as a
left outer join
all_boxes_info_new as b
on a.subscriber_id=b.subscriber_id;
commit;
--34723382

--add genre, channel and pack to views so thresholds can be applied
alter table sov_daily_records_new4
add genre_at_event_start_time varchar(30),
add channel_at_event_start_time varchar(30),
add pack varchar(100) default null,
add pack_grp varchar(20) default null;

update sov_daily_records_new4 t1
set genre_at_event_start_time=t2.genre
   ,channel_at_event_start_time=t2.channel_name
from genre_new t2
where t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time;
commit;
--34723382

-- So as to avoid having to join on something as we're converting it...
alter table sov_daily_records_new4
add uppercase_channel varchar(40);

update sov_daily_records_new4
set uppercase_channel = upper(trim(channel_at_event_start_time));

create index up_chan_index on sov_daily_records_new4 (uppercase_channel);
commit;

-- OK, now we can update without the DB dying:
update sov_daily_records_new4 t1
set pack=t2.pack
from channel_lookup t2
where t1.uppercase_channel=upper(trim(t2.epg_channel));
commit;
--34703991

--add pack group so thresholds can be applied
update sov_daily_records_new4
set pack_grp=case when pack in ('Diginets','Terrestrial') then pack else 'Other' end
from sov_daily_records_new4;
commit;
--34723382

-- Okay, so at this point, the control totals should till be identical...
delete from V043_viewing_control_totals where data_state like '2%' or data_state like '3%' or data_state like '4%';
-- (clearing out all future control totals to so as to not cause any confusion)
insert into V043_viewing_control_totals
select
    convert(varchar(20), '2.) Pre-Cap') -- aliases are handled in table construction
    ,program_air_date
    ,live
    ,genre
    ,count(1)
    ,round(sum(datediff(second, X_Viewing_Start_Time, X_Viewing_End_Time)) / 60.0 / 60 / 24.0, 2)
from sov_daily_records_new4
group by program_air_date, live, genre;

commit;

-------------------------------------------------------------------------------------------------
-- C02 b) COMPARE VIEWING DATA TO CAPS; DETERMINE CAPPING APPLICATION
-------------------------------------------------------------------------------------------------

IF object_id('all_events_new') IS NOT NULL DROP TABLE all_events_new;

--identify capped flags for each event

-- No with specific joins for each table:
create index for_the_joining_group on week_caps_new
    (event_start_hour, event_start_day, genre_at_event_start_time, box_subscription, pack_grp, Live);

create index for_the_joining_group on sov_daily_records_new4
    (event_start_hour, event_start_day, genre_at_event_start_time, box_subscription, pack_grp, Live);
       
alter table sov_daily_records_new4 add max_dur_mins integer;

commit;

-- Also separating the max_dur_mins update and the grouping into all_events_new separately
update sov_daily_records_new4
set max_dur_mins = caps.max_dur_mins
from sov_daily_records_new4 as base
inner join week_caps_new as caps
on (base.Live = caps.Live
and base.event_start_day = caps.event_start_day
and base.event_start_hour = caps.event_start_hour
and base.box_subscription = caps.box_subscription
and base.pack_grp = caps.pack_grp
and base.genre_at_event_start_time = caps.genre_at_event_start_time);

commit;

select Account_Number,
       Subscriber_Id,
       Adjusted_Event_Start_Time,
       X_Adjusted_Event_End_Time,
       X_Event_Duration,
       event_start_hour,
       event_start_day,
       Live,
       box_subscription,
       pack_grp,
       channel_at_event_start_time,
       genre_at_event_start_time,
       case when dateadd(minute, max_dur_mins, adjusted_event_start_time) >= X_Adjusted_Event_End_Time then 0 else 1 end as capped_event,
       count(*) as num_views,
       sum(x_programme_viewed_duration) as viewed_duration
into all_events_new
from sov_daily_records_new4 base
group by Account_Number,
       Subscriber_Id,
       Adjusted_Event_Start_Time,
       X_Adjusted_Event_End_Time,
       X_Event_Duration,
       event_start_hour,
       event_start_day,
       Live,
       box_subscription,
       pack_grp,
       channel_at_event_start_time,
       genre_at_event_start_time,
       capped_event;

commit;
--25940851

--create indexes to speed up processing
create hng index idx1 on all_events_new(event_start_day);
create hng index idx2 on all_events_new(event_start_hour);
create hng index idx3 on all_events_new(Live);
create dttm index idx4 on all_events_new(adjusted_event_start_time);
create dttm index idx5 on all_events_new(x_adjusted_event_end_time);

--identify uncapped universe
if object_id('uncapped_new') is not null drop table uncapped_new;

select *
into --drop table
uncapped_new
from all_events_new
where capped_event=0
order by Live,event_start_day,event_start_hour,channel_at_event_start_time,X_Adjusted_Event_End_Time,adjusted_event_start_time;
commit;
--24702453

--add row id
alter table uncapped_new
add rownum bigint identity;

--create indexes to speed up processing
create hng index idx1 on uncapped_new(event_start_day);
create hng index idx2 on uncapped_new(event_start_hour);
create hng index idx3 on uncapped_new(Live);
create hng index idx4 on uncapped_new(rownum);

--identify capped universe
if object_id('capped_new') is not null drop table capped_new;

select *
into --drop table
capped_new
from all_events_new
where capped_event=1;
commit;
--1238398

--create indexes to speed up processing
create hng index idx1 on capped_new(event_start_day);
create hng index idx2 on capped_new(event_start_hour);
create hng index idx3 on capped_new(Live);

-------------------------------------------------------------------------------------------------
-- C02 c) INDEX THE DURATION-REPLACEMENT LOOKUP
-------------------------------------------------------------------------------------------------

--identify first and last row id in uncapped_new events that have same profile as capped event
if object_id('capped_new2') is not null drop table capped_new2;

-- need somewhere to put the results:
create table capped_new2 (
    subscriber_id                   integer
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,event_start_hour               integer
    ,event_start_day                integer
    ,live                           integer
    ,channel_at_event_start_time    varchar(30)
    ,firstrow                       integer
    ,lastrow                        integer
);

-- OK, we're going to be even more aggressive than batching it into days; we're going to batch it
-- into start hours, otherwise it'll still have scaling issues with panel expansion:

-- still not with a huge amount of index support, but it's bit.
create hng index idx_channel on uncapped_new    (channel_at_event_start_time);
create hng index idx_channel on capped_new      (channel_at_event_start_time);
-- These guys would be what I'd use, except we're getting data type errors, probably those weird "timestamp" columns
--create index big on uncapped_new    (event_start_hour, event_start_day, channel_at_event_start_time, Live, adjusted_event_start_time, X_Adjusted_Event_End_Time);
--create index big on capped_new      (event_start_hour, event_start_day, channel_at_event_start_time, Live, adjusted_event_start_time, X_Adjusted_Event_End_Time);


create variable @the_start_day int;
create variable @the_start_hour int;

if object_id('batching_lookup') is not null drop table batching_lookup;

select distinct event_start_hour, event_start_day
into batching_lookup
from capped_new;

alter table batching_lookup
add id bigint identity;

create variable @loop_tracker int;
create variable @loop_max int;

-- Here is the start of the work loop:

select @loop_tracker = min(id), @loop_max = max(id) from batching_lookup;
delete from capped_new2;

while @loop_tracker <= @loop_max
begin

    select @the_start_day = event_start_day
        ,@the_start_hour = event_start_hour
    from batching_lookup
    where @loop_tracker = id

    commit

    insert into capped_new2
    select t1.subscriber_id
    ,t1.adjusted_event_start_time
    ,t1.X_Adjusted_Event_End_Time
    ,@the_start_hour
    ,@the_start_day
    ,t1.Live
    ,t1.channel_at_event_start_time
    ,min(t2.rownum) as firstrow
    ,max(t2.rownum) as lastrow
    from capped_new t1
    left join
    uncapped_new t2
    on  t1.channel_at_event_start_time=t2.channel_at_event_start_time
    and @the_start_day = t1.event_start_day
    and @the_start_day = t2.event_start_day
    and @the_start_hour = t1.event_start_hour
    and @the_start_hour = t2.event_start_hour
    and t2.X_Adjusted_Event_End_Time>t1.adjusted_event_start_time
    and t2.X_Adjusted_Event_End_Time<=t1.X_Adjusted_Event_End_Time
    and t1.Live=t2.Live
    where @the_start_day = t1.event_start_day
    and @the_start_day = t2.event_start_day
    and @the_start_hour = t1.event_start_hour
    and @the_start_hour = t2.event_start_hour
    group by t1.subscriber_id
    ,t1.adjusted_event_start_time
    ,t1.X_Adjusted_Event_End_Time
    ,t1.Live
    ,t1.channel_at_event_start_time

    commit

    set @loop_tracker = @loop_tracker + 1

end;

commit;

-------------------------------------------------------------------------------------------------
-- C02 d) RANDOMLY CHOOSE REPLACEMENT DURATION FOR CAPPED EVENTS
-------------------------------------------------------------------------------------------------

--add new variables
alter table capped_new2
add rand_num decimal(22,20),
add uncap_row_num integer,
add capped_event_end_time datetime default null;

--create a pretty random multiplier
CREATE VARIABLE multiplier bigint; --has to be a bigint if you are dealing with millions of records.
SET multiplier = DATEPART(millisecond,now())+1; -- pretty random number between 1 and 1000

--generate random number for each capped event
update capped_new2
set rand_num = rand(number(*)*multiplier); --the number(*) function just gives a sequential number.
commit;
--1238398

--identify row id in uncapped universe to select
update capped_new2
set uncap_row_num=case when firstrow>0 then round(((lastrow - firstrow) * rand_num + firstrow),0) else null end;
commit;
--1238398

--create index
CREATE hg INDEX idx1 ON capped_new2(uncap_row_num);
create index idx2 on capped_new2(subscriber_id, adjusted_event_start_time);
commit;

-------------------------------------------------------------------------------------------------
-- C02 e) ASSIGN NEW END TIMES
-------------------------------------------------------------------------------------------------

--assign new event end time to capped events
update capped_new2 t1
set capped_event_end_time=t2.X_Adjusted_Event_End_Time
from uncapped_new t2
where t1.uncap_row_num=t2.rownum
and t1.event_start_hour=t2.event_start_hour
and t1.event_start_day=t2.event_start_day
and t1.channel_at_event_start_time=t2.channel_at_event_start_time
and t1.Live=t2.Live;
commit;
--1235897

--assign end time of first programme to capped events if no uncapped distribution is available
update capped_new2 t1
set capped_event_end_time=case when t1.capped_event_end_time is null then t2.x_viewing_end_time else t1.capped_event_end_time end
from genre_new t2
where t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
and t1.X_Adjusted_Event_End_Time=t2.X_Adjusted_Event_End_Time -- do we really need this join clause?
and t1.firstrow is null;
commit;
--2501
-- Joining to genre_new again? Should the broadcast end time be already in the record
-- we're treating, ie, can do with a single table update, no join?


--select count(*) from capped_new2 where capped_event_end_time is null --<x_adjusted_event_end_time

-------------------------------------------------------------------------------------------------
-- C02 f) PUSH CAPPING BACK ONTO INITIAL VIEWING TABLE
-------------------------------------------------------------------------------------------------

--append fields to table to store additional metrics for capping
alter table sov_daily_records_new4
add (capped_event_end_time datetime
    ,capped_x_viewing_start_time datetime
    ,capped_x_viewing_end_time datetime
    ,capped_x_programme_viewed_duration integer
    ,capped_flag integer );

--update daily view table with revised end times for capped events
update sov_daily_records_new4 t1
set capped_event_end_time=t2.capped_event_end_time
from capped_new2 t2
where t1.subscriber_id=t2.subscriber_id
and t1.adjusted_event_start_time=t2.adjusted_event_start_time
and t1.X_Adjusted_Event_End_Time=t2.X_Adjusted_Event_End_Time
and t1.Live=t2.Live;
commit;
--6624399

--update table to create revised start and end viewing times
update sov_daily_records_new4
set     capped_x_viewing_start_time =
        case
            -- if start of viewing_time is beyond capped end time then flag as null
            when capped_event_end_time <= x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
        end
       ,capped_x_viewing_end_time =
        case
            -- if start of viewing_time is beyond capped end time then flag as null
            when capped_event_end_time <= x_viewing_start_time then null
            -- if capped event end time is beyond end time then leave end time unchanged
            when capped_event_end_time > x_viewing_end_time then x_viewing_end_time
            -- if capped event end time is null then leave end time unchanged
            when capped_event_end_time is null then x_viewing_end_time
            -- otherwise set end time to capped event end time
            else capped_event_end_time
        end
from sov_daily_records_new4;
commit;
--34719754

--calculate revised programme viewed duration
update sov_daily_records_new4
    set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time);
commit;
--34719754

--set capped_flag based on nature of capping
--0 programme view not affected by capping
--1 if programme view has been shortened by a long duration capping rule
--2 if programme view has been excluded by a long duration capping rule

--identify views which need to be capped
update sov_daily_records_new4
    set capped_flag =
        case
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            when capped_x_viewing_start_time is null then 2
            else 0
        end;
commit;
--34719754

--calculate revised event end times and event durations for all events
alter table sov_daily_records_new4
add adjusted_event_end_time datetime,
add adjusted_event_duration integer;

update sov_daily_records_new4
set adjusted_event_end_time=case when capped_event_end_time is null then X_Adjusted_Event_End_Time else capped_event_end_time end;
commit;
--34719754

update sov_daily_records_new4
set adjusted_event_duration=datediff(second,Adjusted_Event_Start_Time,adjusted_event_end_time);
commit;
--34719754

--select count(distinct subscriber_id||adjusted_event_start_time) from sov_daily_records_new4 where capped_event_end_time is not null
--select distinct capped_flag,count(distinct subscriber_id||adjusted_event_start_time) from sov_daily_records_new4 group by capped_flag
--select count(distinct subscriber_id||adjusted_event_start_time) from sov_daily_records_new4 where capped_flag=0 and capped_event_end_time is not null

-- Now the total viewing should be different... though there's no midpoint, it just *chunk* turns up all at once
delete from V043_viewing_control_totals where data_state like '3%' or data_state like '4%';
insert into V043_viewing_control_totals
select
    convert(varchar(20), '3.) Capped') -- aliases are handled in table construction
    ,program_air_date
    ,live
    ,genre
    ,count(1)
    ,round(sum(datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)) / 60.0 / 60 / 24.0, 2)
from sov_daily_records_new4
group by program_air_date, live, genre;

-- OK, so that's the total of what's left, but we also want the breakdown by
-- each capping action, so we can check that they all add up:

-- First clear out the marks in case we're rerunning this section without starting
-- from the top of the script:
delete from V043_viewing_control_totals where data_state like '4%';

-- The total time in events that were not changed by capping:
insert into V043_viewing_control_totals
select
    convert(varchar(20), '4a.) Uncapped')
    ,program_air_date
    ,live
    ,genre
    ,count(1)
    ,round(sum(datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)) / 60.0 / 60 / 24.0, 2)
from sov_daily_records_new4
where capped_flag = 0
group by program_air_date, live, genre;

-- Total time in events that were just dropped:
insert into V043_viewing_control_totals
select
    convert(varchar(20), '4b.) Excluded')
    ,program_air_date
    ,live
    ,genre
    ,count(1)
    ,round(sum(datediff(second, x_viewing_start_time, x_viewing_end_time)) / 60.0 / 60 / 24.0, 2)
from sov_daily_records_new4
where capped_flag = 2
group by program_air_date, live, genre;

commit;

-- The total time left in events that were capped:
insert into V043_viewing_control_totals
select
    convert(varchar(20), '4c.) Truncated')
    ,program_air_date
    ,live
    ,genre
    ,count(1)
    ,round(sum(datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)) / 60.0 / 60 / 24.0, 2)
from sov_daily_records_new4
where capped_flag = 1
group by program_air_date, live, genre;

-- Total time removed from events that were capped
insert into V043_viewing_control_totals
select
    convert(varchar(20), '4d.) T-Margin')
    ,program_air_date
    ,live
    ,genre
    ,count(1)
    ,round((sum(datediff(second, x_viewing_start_time, x_viewing_end_time))
        - sum(datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time))) / 60.0 / 60 / 24.0, 2)
from sov_daily_records_new4
where capped_flag = 1
group by program_air_date, live, genre;

commit;

-- No visibility of how the different rules are played other than tracing through all
-- the metadata again, and we're not going to do that. No visibility even of when the
-- defaults are used instead of the n-tile rules. Oh well.

-- Also, here, we'd do another distribution of viewing durations...


-------------------------------------------------------------------------------------------------
-- C02 g) CHECKING TOTAL VIEWING BEFORE AND AFTER CAPING
-------------------------------------------------------------------------------------------------

/* ##QA##EQ##: Pivot the results of this extraction query in Excel I guess:
select * from sov_daily_records_new4
order by data_state, program_air_date, live, genre
*/

/* What we expect: 
    *. '1.) Collect' should match '2.) Pre-Cap'
    *. '4a.) Uncapped' + '4c.) Truncated' should add up to '3.) Capped',
    *. '4a.) Uncapped' + '4b.) Excluded' + '4c.) Truncated' + '4d.) T-Margin' should add up to '1.) Collect'
They should match pretty much exactly, since we've rounded everything to 2dp in hours.
*/

-------------------------------------------------------------------------------------------------
-- C02 h) LOOKING AT VIEWING DURATION PROFILE BEFORE AND AFTER CAPPING
-------------------------------------------------------------------------------------------------

-- Okay, but we're going to batch it into 10s histogram thing, because other these tables will be
-- huge, and we should still be able to get all the detail want from this view even:

if object_id('V043_viewing_control_distribs') is not null drop table V043_viewing_control_distribs;

select
    convert(varchar(20), '1.) Uncapped') as data_state
    ,(x_programme_viewed_duration / 10) * 10 as duration_interval -- batched into 10s chunks, so 0 means viewing durations between 0s and 10s
    ,count(1) as viewing_events
into V043_viewing_control_distribs
from sov_daily_records_new4
where x_programme_viewed_duration > 0
group by duration_interval;

commit;
create unique index fake_pk on V043_viewing_control_distribs (data_state, duration_interval);
commit;

insert into V043_viewing_control_distribs
select
    convert(varchar(20), '2.) Capped')
    ,(capped_x_programme_viewed_duration / 10) * 10 as duration_interval
    ,count(1) as viewing_events
from sov_daily_records_new4
where capped_x_programme_viewed_duration > 0
group by duration_interval;

commit;

grant select on V043_viewing_control_distribs to public;

/* ##QA##EQ##: Extraction query: make a graph in Excel or something
select * from V043_viewing_control_distribs
order by data_state, duration_interval
*/

/*
--------------------------------------------------------------------------------
-- D - Add Additional Feilds to the Viewing data
--------------------------------------------------------------------------------

         D01 - Add Playback and Vosdal flags
         D02 - Add Barb day and         weighted_viewing_duration to the viewing data (Barb day = 4am - 4am)
         D03 - Add Value Segments
         D04 - Add time of day
         D05 - Add HD Box/Subscription Flag
         D06 - Add current pack

--------------------------------------------------------------------------------
*/


--------------------------------------------------------------------------------
-- D01  Add Playback and Vosdal flags to the viewing data
--------------------------------------------------------------------------------

/*
 vosdal (viewed on same day as live) and playback = within 7 days of air

 capped_x_viewing_start_time - adjusted for playback and capped events, otherwise it is the original viewing time

*/

--Add the additional fields to the viewing table
ALTER TABLE sov_daily_records_new4
        add (VOSDAL             as integer default 0
            ,Playback           as integer default 0
            ,playback_date      as date
            ,playback_post7     as integer default 0 );


-- Update the fields:
Update sov_daily_records_new4

 set  VOSDAL        = (case when capped_x_viewing_start_time <= dateadd(hh, 26,cast( cast( program_air_date as date) as datetime)) and live = 0
                                                                                                                                 then 1 else 0 end)

     ,Playback       =       (case when capped_x_viewing_start_time <= (dateadd(hour, 170, program_air_date))
                                   and  capped_x_viewing_start_time > (cast(dateadd(hour,26,program_air_date)  as datetime))and live = 0 then 1 else 0 end)

     ,playback_post7 =       (case when capped_x_viewing_start_time > (dateadd(day, 170, program_air_date))and live = 0
                                                                                                                        then 1 else 0 end); -- flag these so identifying mismatchs is easy later

--select top 10 * from sov_daily_records_new4 where vosdal = 1
--select top 10 * from sov_daily_records_new4 where playback = 1
--select top 10 * from sov_daily_records_new4 where playback_post7 = 1


/*


*/






--------------------------------------------------------------------------------
-- D02  Add BARB min
--------------------------------------------------------------------------------

-- first we nede to allocate a barb minute:
-- Logic: Create 2 tables, table one looks at the vieiwng data and catures all complete minutes watched. Table 2 captures the begining and end of those viewing records to determine if the part minutes
--        is infact a BARB minute of viewing (at least 30 seconds long and the majority viewed in that minutes). Once we know the complete minutes and the BARB minutes these can be merged into a
--        single table to determine how many minutes of each channel, per day were watched by each subscriber.


-- alter table Consolidated_table
--  add program_air_datetime as datetime
--
--  update Consolidated_table
--  set base.program_air_datetime = sk.program_air_datetime
--  from Consolidated_table base
--  left join week_Programmes120212 sk
--  on base.programme_trans_sk = sk.programme_trans_sk
--
--

-- First create the two tables, the first table will have complete minutes


if object_id('internal_capped_viewing_new') is not null drop table internal_capped_viewing_new;

create table internal_capped_viewing_new (
    subscriber_id                       decimal(10)
    ,epg_channel                        varchar(20)
    ,minute_started                     datetime        not null
    ,minute_stopped                     datetime        not null
    ,program_air_datetime               datetime
    ,live                               smallint
    ,vosdal                             smallint
    ,playback                           smallint

);
-- This index will let you quickly find out who's watching each minute.
create index for_MBM on internal_capped_viewing_new (epg_channel, minute_started, minute_stopped);



-- Create the second table for part minutes

if object_id('capped_viewing_endpoints_new') is not null drop table capped_viewing_endpoints_new;

create table capped_viewing_endpoints_new (
    subscriber_id                       decimal(10)
    ,epg_channel                       varchar(20)
    ,minute_start                       datetime        not null
    ,viewing_starts                     datetime        not null
    ,viewing_ends                       datetime        not null
    ,program_air_datetime               datetime
    ,live                               smallint
    ,vosdal                             smallint
    ,playback                           smallint

);

-- This index is what we want to help group all the viewing into allocated minutes
create index for_groupings1 on capped_viewing_endpoints_new (subscriber_id, epg_channel, minute_start);
create index for_groupings11 on capped_viewing_endpoints_new (subscriber_id, epg_channel, minute_start,live,vosdal,playback);


commit;

------ Populate the tables --------------------------------------------------------------------------------------------------------

-- Populate Table 1:
-- Next, put the 100% minutes into the "internal" table:
insert into internal_capped_viewing_new
select subscriber_id, epg_channel
    -- VESPA viewing data always starts and stops on a second, so we don't have to wory about time smaller than that
    ,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)   -- Couldn't find a single function to round a minute up
    ,dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time)          -- Nor for rounding a minute down
    ,program_air_datetime
    ,live
    ,vosdal
    ,playback
from sov_daily_records_new4
where dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    <> dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time);-- Only want to add things which span a minute boundry

--select top 10 * from sov_daily_records_new4

-- Populate Table 2
-- three different things to go into the sub-minutes table:

-- 1/ The initial section of viewing events

insert into capped_viewing_endpoints_new
select subscriber_id, epg_channel
    ,dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)      -- Allocated to whichever minute viewing starts in
    ,capped_x_viewing_start_time                                                                        -- Start when the viewing starts
    ,dateadd(second, 60 - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)   -- Go up to the end of that minute
    ,program_air_datetime
    ,live
    ,vosdal
    ,playback
from sov_daily_records_new4
where dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    <> dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time);

-- 2/ The final sections of viewing:
insert into capped_viewing_endpoints_new
select subscriber_id, epg_channel
    ,dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time)
    ,dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time)          -- Viewing starts with the minute
    ,capped_x_viewing_end_time                  -- and goes to the end of the capped event
    ,program_air_datetime
    ,live
    ,vosdal
    ,playback
from sov_daily_records_new4
where dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    <> dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time);

-- 3/ Smaller events which never crossed a whole minute boundry
insert into capped_viewing_endpoints_new
select subscriber_id, epg_channel
    ,dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    ,capped_x_viewing_start_time
    ,capped_x_viewing_end_time
    ,program_air_datetime
    ,live
    ,vosdal
    ,playback
from sov_daily_records_new4
where dateadd(second, - datepart(second, capped_x_viewing_start_time), capped_x_viewing_start_time)
    = dateadd(second, - datepart(second, capped_x_viewing_end_time), capped_x_viewing_end_time); -- within the same minute
-- ^^ not the = instead of the <> and we're pulling out everythign that's not been treated thus far.

commit;

-- select top 100 * from capped_viewing_endpoints_new


------ checks --------------------------------------------------------------------------------------------------------
/*
-- OK, so at this point, all the things in the endpoints table should have durations less than a minute:
select count(1) from capped_viewing_endpoints_new where datediff(second, viewing_starts, viewing_ends) > 60; -- 0
-- If this isn't zero, you've got issues.

select sum(datediff(second,capped_x_viewing_start_time, capped_x_viewing_end_time))
from sov_daily_records_new4;
-- TYhat's the total viewing, and it should match the sum of the viewing in the two derived tables: 65062263868

select sum(datediff(second,minute_started, minute_stopped))
from internal_capped_viewing_new;

select sum(datediff(second,viewing_starts, viewing_ends))
from capped_viewing_endpoints_new;


-- 60991314480 + 4070949388 = 65062263868
*/
-----------------------------------------------------------------------------------------------------------------------


-- So now you can get from the endpoints table the total viewing in each minute:

-- for each minute identify the total amount of time watched on each channel, then remove anything that is not the majority of that minute.
select subscriber_id
       ,epg_channel
       ,minute_start
       ,sum(datediff(second,viewing_starts, viewing_ends)) as time_watched
       ,max(program_air_datetime) as program_air_datetime-- use max here
       ,live
       ,vosdal
       ,playback
into #edge_minutes1
from capped_viewing_endpoints_new
group by subscriber_id
         ,epg_channel
         ,minute_start
       -- ,program_air_datetime
         ,live
        ,vosdal
        ,playback
;
commit;


--
-- select count(*) from #edge_minutes -- before the max fix 138,458,559
-- select count(*) from #edge_minutes1 -- after the max fix: expect a 25% drop in this for it to have a significant change on the figures
--                                         -- 116,948,216 -- 22 million less records - so 22 million mins?
--
--
--
-- select top 10 * from #edge_minutes1


--select top 100 * from #edge_minutes1 order by subscriber_id, minute_start -- there should be duplicate minutes and subs (channel surfing)
-- duplicates seem ot be coming from program air date time, where minutes may have been rounded up from the end of last vieiwng and down, for the start
-- of next viewing.

create unique index fake_pk on #edge_minutes1 (subscriber_id, epg_channel, minute_start,program_air_datetime,live,vosdal,playback);
commit;


-- identify how much time was watched on each channel
select subscriber_id
       ,epg_channel
       ,minute_start
       ,time_watched
       ,rank() over (partition by subscriber_id, minute_start order by time_watched desc) as most_watched -- ****this needs to be updated**** -- does it?
       ,program_air_datetime -- program_air_datetime
       ,live
       ,vosdal
       ,playback
into #most_watched_edges
from #edge_minutes1;



--select top 100 * from #most_watched_edges order by subscriber_id, minute_start,epg_channel
-- this table shouldnt have duplicates, and this should have cases where there are multiple instances of a channel, per subscriber -
-- if anything a screw up here would result in fewer minutes than actually achieved - not the cause of massive min allocation
--- this is not working correctly as there are some minute that have the smae channel however they have different program start times.

-- delete anything that was not the majority of the minute.
delete from #most_watched_edges where most_watched <> 1;
commit;

create hg index fake_pk on #most_watched_edges (subscriber_id, minute_start);



-- insert all the barb minutes into a single table

insert into internal_capped_viewing_new
select subscriber_id
       ,epg_channel
       ,minute_start
       ,dateadd(minute, 1, minute_start)
       ,program_air_datetime
       ,live
       ,vosdal
       ,playback
from #most_watched_edges
where time_watched >= 30; -- barb requires a minimum viewing of 30 seconds, any viewing less than 15 seconds have been removed from the data


--- QA ------------------------------------------------------------------------------------------------------------------------
/*
select top 10 * from internal_capped_viewing_new

select count(*) from internal_capped_viewing_new -- 134,032,609  Millions and Not Billions - hats off to Rob! 23 hours + processing down to 2!!!!!

-- All the intervals should line up exactly on minute ends:
select count(1) from internal_capped_viewing_new
where datepart(second, minute_started) <> 0
or datepart(second, minute_stopped) <> 0;
-- Should be zero, if it's not, look into why
-- its zero!


-- From here, you've got all your viewing allocated to minutes to particular chanels.
-- There's a stored procedure for doing this in the repository in the minute-by-miinute
-- section, but it's not to transparent because of how it works with the daily tables.

create variable @test_point datetime;
set @test_point = '2012-01-03 13:42:10';
-- Loop and change this variable, but make sure there's always some seconds in it so
-- you're polling the middle of the minute somewhere. Exactly where doesn't matter as
-- all the boundaries are on a minute, but yeah.

Select epg_channel, count(distinct subscriber_id) as boxes_watching
from internal_capped_viewing_new
where minute_started > @test_point and minute_stopped < @test_point
group by epg_channel;

*/

--select count(*) from internal_capped_viewing_new where vosdal = 1



--------------------------------------------------------------------------------
-- D03  Add Time of Day
--------------------------------------------------------------------------------

-- time of day reflects at which time of day the content was viewed

alter table internal_capped_viewing_new
        add viewing_time_of_day as varchar(15);

update internal_capped_viewing_new
set viewing_time_of_day =
        case when cast(minute_started as time) is not null and cast(minute_started as time) >= '06:00:00'
                and cast(minute_started as time) <= '08:59:59' then 'Breakfast'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '09:00:00'
                and cast(minute_started as time) <= '11:59:59' then 'Coffee'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '12:00:00'
                and cast(minute_started as time) <= '17:59:59' then 'Daytime'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '18:00:00'
                and cast(minute_started as time) <= '19:59:59' then 'Early Peak'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '20:00:00'
                and cast(minute_started as time) <= '22:59:59' then 'Late Peak'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '23:00:00' -- bug fix
                and cast(minute_started as time) <= '23:59:59' then 'Post Peak'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '00:00:00' -- bug fix
                and cast(minute_started as time) <= '01:59:59' then 'Post Peak'
         when cast(minute_started as time) is not null and cast(minute_started as time) >= '02:00:00'
                and cast(minute_started as time) <= '05:59:59' then 'Night Time'
         else  'NO TIME'
End;


-- add a second time of day flag to reflect when the content was aired so as to attribute playback to when content was aired and not consumed:
-- needed???
-- when consolidating viewing behaviour how is playback attributed back? - it is attributed to the day.


alter table internal_capped_viewing_new
        add aired_time_of_day as varchar(15);

update internal_capped_viewing_new
set aired_time_of_day =
        case when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '06:00:00'
                and cast(program_air_datetime as time) <= '08:59:59' then 'Breakfast'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '09:00:00'
                and cast(program_air_datetime as time) <= '11:59:59' then 'Coffee'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '12:00:00'
                and cast(program_air_datetime as time) <= '17:59:59' then 'Daytime'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '18:00:00'
                and cast(program_air_datetime as time) <= '19:59:59' then 'Early Peak'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '20:00:00'
                and cast(program_air_datetime as time) <= '22:59:59' then 'Late Peak'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '23:00:00' -- bug fix
                and cast(program_air_datetime as time) <= '23:59:59' then 'Post Peak'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '00:00:00' -- bug fix
                and cast(program_air_datetime as time) <= '01:59:59' then 'Post Peak'
         when cast(program_air_datetime as time) is not null and cast(program_air_datetime as time) >= '02:00:00'
                and cast(program_air_datetime as time) <= '05:59:59' then 'Night Time'
         else  'NO TIME'
End; -- 88,202,443 Row(s) affected


--  select top 1000 * from internal_capped_viewing_new


-- QA --

--SELECT DISTINCT(viewing_time_of_day),COUNT(*) FROM internal_capped_viewing_new GROUP BY viewing_time_of_day


--SELECT DISTINCT(aired_time_of_day),COUNT(*) FROM internal_capped_viewing_new GROUP BY aired_time_of_day



-- QA------------------------------------------------------------
-- VIEWING_TIME_OF_DAY     COUNT(*)
-- Post Peak       14679699
-- Coffee          15095757
-- Early Peak      20665648
-- Late Peak       31156165
-- Night Time      4087983
-- Daytime         40417991
-- Breakfast       7929366

-- select top 10 * from internal_capped_viewing_new

--------------------------------------------------------------------------------
-- D04  Add BARB day
--------------------------------------------------------------------------------

-- remove any data that is not relevant
delete from internal_capped_viewing_new where minute_started < @var_period_start_dt;
-- data was taken from a day prior, its not needed now


alter table internal_capped_viewing_new
        add barb_day as smallint;

update internal_capped_viewing_new
        set barb_day = case when minute_started >= (dateadd(day,0,@var_period_start_dt))
                             and minute_started < (dateadd(day,1,@var_period_start_dt)) then 1
                            when minute_started >= (dateadd(day,1,@var_period_start_dt))
                             and minute_started < (dateadd(day,2,@var_period_start_dt)) then 2
                            when minute_started >= (dateadd(day,2,@var_period_start_dt))
                             and minute_started < (dateadd(day,3,@var_period_start_dt)) then 3
                            when minute_started >= (dateadd(day,3,@var_period_start_dt))
                             and minute_started < (dateadd(day,4,@var_period_start_dt)) then 4
                            when minute_started >= (dateadd(day,4,@var_period_start_dt))
                             and minute_started < (dateadd(day,5,@var_period_start_dt)) then 5
                            when minute_started >= (dateadd(day,5,@var_period_start_dt))
                             and minute_started < (dateadd(day,6,@var_period_start_dt)) then 6
                            when minute_started >= (dateadd(day,6,@var_period_start_dt))
                             and minute_started < (dateadd(day,7,@var_period_start_dt)) then 7
                            when minute_started >= (dateadd(day,7,@var_period_start_dt))
                             and minute_started < (dateadd(day,8,@var_period_start_dt)) then 8
                            when minute_started >= (dateadd(day,8,@var_period_start_dt))
                             and minute_started < (dateadd(day,9,@var_period_start_dt)) then 9
                            when minute_started >= (dateadd(day,9,@var_period_start_dt))
                             and minute_started < (dateadd(day,10,@var_period_start_dt)) then 10
                            when minute_started >= (dateadd(day,10,@var_period_start_dt))
                             and minute_started < (dateadd(day,11,@var_period_start_dt)) then 11
                            when minute_started >= (dateadd(day,11,@var_period_start_dt))
                             and minute_started < (dateadd(day,12,@var_period_start_dt)) then 12
                            when minute_started >= (dateadd(day,12,@var_period_start_dt))
                             and minute_started < (dateadd(day,13,@var_period_start_dt)) then 13
                            when minute_started >= (dateadd(day,13,@var_period_start_dt))
                             and minute_started < (dateadd(day,14,@var_period_start_dt)) then 14
                            when minute_started >= (dateadd(day,14,@var_period_start_dt))
                             and minute_started < (dateadd(day,15,@var_period_start_dt)) then 15
                             else 0

                             End;

-- check

--SELECT DISTINCT(barb_day),COUNT(*) FROM internal_capped_viewing_new GROUP BY barb_day order by barb_day

-- barb_day        COUNT(*)
-- 1       17455412
-- 2       18648124
-- 3       17737653
-- 4       17979192
-- 5       18524828
-- 6       19001931
-- 7       19417576
-- 8       1934199
-- 9       963852
-- 10      627591
-- 11      483585
-- 12      383100
-- 13      409721
-- 14      329818


--------------------------------------------------------------------------------
-- D05  Add Account number
--------------------------------------------------------------------------------
-- added here to save processing earlier

-- account number needed to import the weightings
alter table internal_capped_viewing_new
 add account_number varchar(30);


select distinct subscriber_id, account_number into #account from sov_daily_records_new4 group by subscriber_id, account_number;

Update internal_capped_viewing_new
set base.account_number = sb.account_number
from internal_capped_viewing_new as base
     inner join #account as sb
    on sb.subscriber_id = base.subscriber_id;

---------------------------------------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------
-- E NEW SCALING CODE
-----------------------------------------------------------------------------------
/*    E01)  -- - Create scaling tables

     E01 a - scaling_weights
     E01 b - scaling_weekly_sample
     E01 c - Scaling_segments
     E01 d - Scaling_categories
     E01 e - Scaling_box_level_viewing
     E01 f - scaling_metrics
--------------------------------------------------------------------------------
-- E01 CREATE SCALING TABLES
--------------------------------------------------------------------------------




*/

--------------------------------------------------------------- A01 - scaling_weights
-- E01a - scaling_weights



CREATE TABLE poveys.scaling_weights (
     scaling_date           DATE
    ,scaling_segment_id     int
    ,account_number         varchar(30)
    ,weight                 float
    ,primary key (scaling_date, account_number)
);

INSERT INTO poveys.scaling_weights
SELECT '2012-01-30'         -- Start of Analysis
       ,280801              -- non-scalable segment_id
       ,'Dummy account'
       ,0

--------------------------------------------------------------- A02 - scaling_weekly_sample
-- E01b - scaling_weekly_sample

CREATE TABLE poveys.scaling_weekly_sample (
     account_number                     VARCHAR(20)
    ,ilu_cb_row_id                      BIGINT
    ,universe                           VARCHAR(20)                         -- Single, Dual or Multiple box household
    ,isba_tv_region                     VARCHAR(20)                         -- Scaling variable 1 : Region
    ,ilu_hhcomposition                  VARCHAR(2)
    ,hhcomposition                      VARCHAR(70)     DEFAULT 'L) Unknown'-- Scaling variable 2: Household composition
    ,tenure                             VARCHAR(15)     DEFAULT 'E) Unknown'-- Scaling variable 3: Tenure
    ,num_mix                            INT
    ,mix_pack                           VARCHAR(20)
    ,package                            VARCHAR(20)                         -- Scaling variable 4: Package
    ,boxtype                            VARCHAR(35)                         -- Scaling variable 5: Household boxtype (ranked)
    ,scaling_segment_id                 INT             DEFAULT NULL        -- segment scaling id for identifying segments
    ,mr_boxes                           INT
    ,complete_viewing                   TINYINT         DEFAULT 0           -- Flag for all accounts with complete viewing data
);

CREATE UNIQUE HG INDEX indx_ac ON scaling_weekly_sample(account_number);
CREATE INDEX for_segment_identification_raw ON poveys.scaling_weekly_sample
    (universe, isba_tv_region,hhcomposition, tenure, package, boxtype);
CREATE INDEX ilu_joining ON poveys.scaling_weekly_sample (ilu_cb_row_id);
CREATE INDEX for_grouping ON poveys.scaling_weekly_sample (scaling_segment_ID);
COMMIT;


--------------------------------------------------------------- A03 - Scaling_segments
-- E01c - Scaling_segments

CREATE TABLE poveys.Scaling_segments (
     universe               VARCHAR(50)
    ,scaling_segment_id     INT
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,segment_weight         DOUBLE
    ,indices_actual         DOUBLE
    ,indices_weighted       DOUBLE
);


CREATE HG INDEX indx_un on poveys.scaling_segments(universe)
CREATE HG INDEX indx_seg on poveys.scaling_segments(scaling_segment_id)

--------------------------------------------------------------- A04 - Scaling_categories
-- E01d - Scaling_categories

CREATE TABLE poveys.Scaling_categories (
     universe               VARCHAR(50)
    ,profile                VARCHAR(50)
    ,value                  VARCHAR(70)
    ,sky_base_accounts      DOUBLE
    ,vespa_panel            DOUBLE
    ,category_weight        DOUBLE
    ,sum_of_weights         DOUBLE
    ,convergence_flag       TINYINT     DEFAULT 1
);


create hg index indx_universe on poveys.Scaling_categories(universe);
create hg index indx_profile on poveys.Scaling_categories(profile);
create hg index indx_value on poveys.Scaling_categories(value);

--------------------------------------------------------------- A05 - Scaling_box_level_viewing
-- E01e - Scaling_box_level_viewing

CREATE TABLE poveys.Scaling_box_level_viewing (
    service_instance_id                 varchar(30)
    ,account_number                     varchar(20)
    ,universe                           varchar(30)
    ,viewing_flag                       tinyint
    ,MR                                 tinyint
    ,SP                                 tinyint
    ,HD                                 tinyint
    ,HDstb                              tinyint
    ,HD1TBstb                           tinyint
);

CREATE UNIQUE hg INDEX indx_ac ON Scaling_box_level_viewing(service_instance_id);
CREATE hg INDEX indx_serv_inst_id ON Scaling_box_level_viewing(account_number);
COMMIT;

--------------------------------------------------------------- A06 - scaling_metrics
-- E01f - scaling_metrics

CREATE TABLE poveys.scaling_metrics (
     scaling_date           DATE
     ,iterations            int
     ,convergence           tinyint
     ,max_weight            float
     ,av_weight             float
     ,sum_of_weights        float
     ,sky_base              bigint
     ,vespa_panel           bigint
     ,non_scalable_accounts bigint
);

commit

create index indx_date on poveys.scaling_metrics(scaling_date);


--------------------------------------------------------------------------------
-- E02 GET WEEKLY SAMPLE
--------------------------------------------------------------------------------

/*
E02   - Get weekly sample
     E02 a - Clear tables
     E02 b - Declare variables
     E02 c - Get weekly sample
     E02 d - Populate scaling variables
     E02 e - Append segment id

*/

--------------------------------------------------------------- B01 - Clear tables
-- E02 a - Clear tables

DELETE FROM scaling_weekly_sample
DELETE FROM Scaling_box_level_viewing
COMMIT

--------------------------------------------------------------- B02 - Declare variables
-- E02 b - Declare variables


DECLARE @weekly_reference_date DATE
SELECT @weekly_reference_date = dateadd(day,1,MAX(scaling_date)) FROM scaling_weights
-- Identifies the latest date in the scaling_weights table and adds a day to
-- give the start of the next scaling week.


--------------------------------------------------------------- B03 - Get weekly sample
-- E02 c - Get weekly sample

-- Captures all active accounts in cust_subs_hist
SELECT   account_number
        ,cb_key_household
        ,current_short_description
        ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
        ,convert(bit, 0)  AS uk_standard_account
        ,convert(VARCHAR(20), NULL) AS isba_tv_region
  INTO #weekly_sample
  FROM sk_prod.cust_subs_hist as csh
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= @weekly_reference_date
   AND effective_to_dt > @weekly_reference_date
   AND EFFECTIVE_FROM_DT IS NOT NULL
   AND cb_key_household > 0
   AND cb_key_household IS NOT NULL
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL

-- De-dupes accounts
COMMIT
DELETE FROM #weekly_sample WHERE rank > 1
COMMIT

-- Create indices
CREATE UNIQUE INDEX fake_pk ON #weekly_sample (account_number)
CREATE INDEX for_ilu_joining ON #weekly_sample (cb_key_household)
CREATE INDEX for_package_join ON #weekly_sample (current_short_description)
COMMIT


-- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
UPDATE #weekly_sample
SET
    uk_standard_account = CASE
        WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
        ELSE 0 END
    ,isba_tv_region = b.isba_tv_region
FROM #weekly_sample AS a
inner join sk_prod.cust_single_account_view AS b
ON a.account_number = b.account_number

COMMIT
DELETE FROM #weekly_sample WHERE uk_standard_account=0
COMMIT
--------------------------------------------------------------- B04 - Populate scaling variables
-- E02 d - Populate scaling variables

-- Use household key to create a linking table from scaling_weekly_sample to sk_prod.ilu
SELECT
    ilu.cb_row_id
   ,base.account_number
   ,base.cb_key_household
   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P1' THEN 1 ELSE 0 END) AS P1
   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P2' THEN 1 ELSE 0 END) AS P2
   ,MAX(CASE WHEN ilu.ilu_correspondent = 'OR' THEN 1 ELSE 0 END) AS OR1
INTO #temp1 -- drop table #temp1
FROM sk_prod.ilu AS ilu
inner join #weekly_sample AS base
    ON base.cb_key_household = ilu.cb_key_household
        AND base.cb_key_household IS NOT NULL
        AND  base.cb_key_household > 0
GROUP BY ilu.cb_row_id, base.account_number, base.cb_key_household
HAVING P1 + P2 + OR1 > 0

COMMIT

SELECT  cb_row_id
       ,account_number
       ,cb_key_household
       ,CASE WHEN P1 = 1  THEN 1
             WHEN P2 = 1  THEN 2
             ELSE              3
         END AS Correspondent
       ,rank() over(PARTITION BY account_number ORDER BY Correspondent asc, cb_row_id desc) AS rank
 INTO  #ILU -- drop table #ilu
 FROM #temp1

--de-dupe ilu linking table
COMMIT
DELETE FROM #ILU WHERE rank > 1
COMMIT

--Create index on account_number
CREATE UNIQUE INDEX index_ac on #ILU (account_number)
COMMIT

-- Populate Package & ISBA TV Region

INSERT INTO scaling_weekly_sample (
    account_number
    ,ilu_cb_row_id
    ,universe
    ,isba_tv_region
    ,ilu_hhcomposition
    ,hhcomposition
    ,tenure
    ,num_mix
    ,mix_pack
    ,package
    ,boxtype
)
SELECT
    fbp.account_number
    ,ilu.cb_row_id
    ,'A) Single box HH' -- universe
    ,fbp.isba_tv_region -- isba_tv_region
    ,NULL -- ilu_hhcomposition
    ,'L) Unknown'  -- hhcomposition
    ,'E) Unknown' -- tenure
    ,cel.Variety + cel.Knowledge + cel.Kids + cel.Style_Culture + cel.Music + cel.News_Events as num_mix
    ,CASE
                    WHEN Num_Mix IS NULL OR Num_Mix=0                           THEN 'Entertainment Pack'
                    WHEN (cel.variety=1 OR cel.style_culture=1)  AND Num_Mix=1  THEN 'Entertainment Pack'
                    WHEN (cel.variety=1 AND cel.style_culture=1) AND Num_Mix=2  THEN 'Entertainment Pack'
                    WHEN Num_Mix > 0                                            THEN 'Entertainment Extra'
                END AS mix_pack -- Basic package has recently been split into the Entertainment and Entertainment Extra packs
    ,CASE
        WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 'Top Tier'
        WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN 'Dual Sports'
        WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN 'Dual Movies'
        WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN 'Single Sports'
        WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN 'Single Movies'
        WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN 'Other Premiums'
        WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Pack'  THEN 'Basic - Ent'
        WHEN cel.prem_movies = 0 AND cel.prem_sports = 0 AND mix_pack = 'Entertainment Extra' THEN 'Basic - Ent Extra'
        ELSE                                                  'Basic - Ent' END -- package
    ,'D) FDB & No_secondary_box' -- boxtype
FROM #weekly_sample AS fbp
left join #ILU AS ilu
    ON fbp.account_number = ilu.account_number
left join sk_prod.cust_entitlement_lookup AS cel
    ON fbp.current_short_description = cel.short_description

-- HHcomposition

-- Household Composition has been grouped according to its relationship with viewing behaviour

UPDATE scaling_weekly_sample
SET
    stws.ilu_hhcomposition = ilu.ilu_hhcomposition
    ,hhcomposition    =     CASE        WHEN ilu.ilu_hhcomposition IN ('A1') THEN 'A) Female Single Parent'
                                        WHEN ilu.ilu_hhcomposition IN ('A2','B2') THEN 'B) Single Pensioner'
                                        WHEN ilu.ilu_hhcomposition IN ('A3') THEN 'C) Female Single Other'
                                        WHEN ilu.ilu_hhcomposition IN ('B3') THEN 'D) Male Single Other'
                                        WHEN ilu.ilu_hhcomposition IN ('C1','D1') THEN 'E) Couple - dependent children'
                                        WHEN ilu.ilu_hhcomposition IN ('C3','D3','E3','H3','I3','J3','K3') THEN 'F) Two person or more - no dependent children'
                                        WHEN ilu.ilu_hhcomposition IN ('C2','E2','F2','H1','I1') THEN 'G) Two Person Household - pensioners'
                                        WHEN ilu.ilu_hhcomposition IN ('F1') THEN 'H) Couple - Grown up and dependent children'
                                        WHEN ilu.ilu_hhcomposition IN ('F3','G3') THEN 'I) Couple - Grown up children at home'
                                        WHEN ilu.ilu_hhcomposition IN ('E1','K1') THEN 'J) Two or more Person Household - dependent children'
                                        WHEN ilu.ilu_hhcomposition IN ('H2','I2','J1') THEN 'K) Couple and 1+ other adults - dependent children'
                             ELSE 'L) Unknown' END
FROM scaling_weekly_sample AS stws
inner join sk_prod.ilu AS ilu
ON stws.ilu_cb_row_id = ilu.cb_row_id


-- Tenure

-- Tenure has been grouped according to its relationship with viewing behaviour

UPDATE scaling_weekly_sample t1
SET
    tenure = CASE   WHEN datediff(day,acct_first_account_activation_dt,@weekly_reference_date) <=  365 THEN 'A) 0-1 Year'
                    WHEN datediff(day,acct_first_account_activation_dt,@weekly_reference_date) <=  730 THEN 'B) 1-2 Years'
                    WHEN datediff(day,acct_first_account_activation_dt,@weekly_reference_date) <= 3650 THEN 'C) 2-10 Years'
                    WHEN datediff(day,acct_first_account_activation_dt,@weekly_reference_date) > 3652 THEN  'D) 10 Years+'
                    ELSE 'E) Unknown'
             END
FROM sk_prod.cust_single_account_view sav
WHERE t1.account_number=sav.account_number
COMMIT

-- Boxtype & Universe

-- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
-- 1) HD, 2) HDx, 3) Skyplus, 4) FDB

-- Capture all active boxes for this week
SELECT    csh.service_instance_id
        , csh.account_number
        , subscription_sub_type
        , rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
  INTO #accounts -- drop table #accounts
  FROM sk_prod.cust_subs_hist as csh
        inner join scaling_weekly_sample AS ss ON csh.account_number = ss.account_number
 WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
   AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
   AND csh.effective_from_dt <= @weekly_reference_date
   AND csh.effective_to_dt > @weekly_reference_date

-- De-dupe active boxes
DELETE FROM #accounts WHERE rank>1
COMMIT


-- Create indices on list of boxes
CREATE UNIQUE hg INDEX idx1 ON #accounts(service_instance_id)
CREATE hg INDEX idx2 ON #accounts(account_number)


-- Identify HD boxes
SELECT  stb.service_instance_id
       ,MAX(CASE WHEN current_product_description LIKE '%HD%' THEN 1
                ELSE 0
             END) AS HD
       ,MAX(CASE WHEN current_product_description LIKE '%HD%1TB%' THEN 1
                        ELSE 0
             END) AS HD1TB
INTO #hda -- drop table #hda
FROM sk_prod.CUST_SET_TOP_BOX AS stb INNER JOIN #accounts AS acc
                                             ON stb.service_instance_id = acc.service_instance_id
WHERE box_installed_dt <= @weekly_reference_date
AND box_replaced_dt   > @weekly_reference_date
AND current_product_description like '%HD%'
GROUP BY stb.service_instance_id

-- Create index on HD table
COMMIT
CREATE UNIQUE hg INDEX idx1 ON #hda(service_instance_id)

INSERT INTO scaling_box_level_viewing (service_instance_id, account_number, MR, SP, HD, HDstb, HD1TBstb)
SELECT  acc.service_instance_id
       ,acc.account_number
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
       ,MAX(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
       ,MAX(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
  FROM sk_prod.cust_subs_hist AS csh
       INNER JOIN #accounts AS acc ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
       LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                       ON csh.current_short_description = cel.short_description
       LEFT OUTER JOIN #hda ON csh.service_instance_id = #hda.service_instance_id --< Links to the HD Set Top Boxes
 WHERE csh.effective_FROM_dt <= @weekly_reference_date
   AND csh.effective_to_dt    > @weekly_reference_date
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY acc.service_instance_id ,acc.account_number


-- Identify boxtype of each box and whether it is a primary or a secondary box
SELECT  tgt.account_number
       ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
       ,MAX(CASE WHEN MR=0 AND  (       (tgt.HD =1 AND HD1TBstb = 1 AND SP =1)
                                OR      (tgt.HD =1 AND HDstb = 1    AND SP =1)  ) THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=0 AND  (       (tgt.SP =1 AND tgt.HD1TBstb = 1)
                                OR      (tgt.SP =1 AND tgt.HDstb = 1)           ) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=0 AND tgt.SP =1                                          THEN 2 -- Skyplus
                 ELSE                                                                  1 END) AS pb -- FDB
       ,MAX(CASE WHEN MR=1 AND  (       (tgt.HD =1 AND HD1TBstb = 1 AND SP =1)
                                OR      (tgt.HD =1 AND HDstb = 1    AND SP =1)  ) THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=1 AND  (       (tgt.SP =1 AND tgt.HD1TBstb = 1)
                                OR      (tgt.SP =1 AND tgt.HDstb = 1)           ) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=1 AND tgt.SP =1                                          THEN 2 -- Skyplus
                 ELSE                                                                  1 END) AS sb -- FDB
  INTO #boxtype_ac -- drop table #boxtype_ac
  FROM scaling_box_level_viewing AS tgt
GROUP BY tgt.account_number



-- Create indices on box-level boxtype temp table
COMMIT
CREATE hg INDEX idx_ac ON #boxtype_ac(account_number)


-- Append universe and boxtype

UPDATE scaling_weekly_sample AS stws
SET stws.universe = CASE WHEN ac.mr_boxes = 0 THEN 'A) Single box HH'
                         WHEN ac.mr_boxes = 1 THEN 'B) Dual box HH'
                         ELSE 'C) Multiple box HH' END
    ,stws.boxtype  =
        CASE WHEN       ac.mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'A) HDx & No_secondary_box'
             WHEN       ac.mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'B) HD & No_secondary_box'
             WHEN       ac.mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'C) Skyplus & No_secondary_box'
             WHEN       ac.mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'D) FDB & No_secondary_box'
             WHEN       ac.mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'E) HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
             WHEN       ac.mr_boxes > 0 AND ((pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4))  THEN  'E) HD & HD'
             WHEN       ac.mr_boxes > 0 AND ((pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4))  THEN  'F) HD & Skyplus'
             WHEN       ac.mr_boxes > 0 AND ((pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4))  THEN  'G) HD & FDB'
             WHEN       ac.mr_boxes > 0 AND  pb =  3 AND sb =  3                              THEN  'H) HDx & HDx'
             WHEN       ac.mr_boxes > 0 AND ((pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3))  THEN  'I) HDx & Skyplus'
             WHEN       ac.mr_boxes > 0 AND ((pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3))  THEN  'J) HDx & FDB'
             WHEN       ac.mr_boxes > 0 AND  pb =  2 AND sb =  2                              THEN  'K) Skyplus & Skyplus'
             WHEN       ac.mr_boxes > 0 AND ((pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2))  THEN  'L) Skyplus & FDB'
                        ELSE   'M) FDB & FDB' END
    ,stws.mr_boxes = ac.mr_boxes
FROM #boxtype_ac AS ac
WHERE ac.account_number = stws.account_number

--------------------------------------------------------------- B05 - Append segment id
-- E02 e - Append segment id

-- The vespa_analysts.scaling_segments_lookup_table table can be used to append a segment_id to
-- the scaling_weekly_sample table by matching on universe and each of the
-- five scaling variables (hhcomposition, isba_tv_region, package, boxtype and tenure)

UPDATE scaling_weekly_sample
   SET scaling_segment_ID = ssl.scaling_segment_ID
  FROM scaling_weekly_sample AS stws
        inner join vespa_analysts.scaling_segments_lookup_table AS ssl
                             ON stws.universe = ssl.universe
                            AND stws.hhcomposition = ssl.hhcomposition
                            AND stws.isba_tv_region = ssl.isba_tv_region
                            AND stws.Package = ssl.Package
                            AND stws.boxtype = ssl.boxtype
                            AND stws.tenure = ssl.tenure

COMMIT

--------------------------------------------------------------------------------
-- E03 GET DAILY WEIGHTS
--------------------------------------------------------------------------------

/*
E03  - Get daily weights
     E03 a - Create Scaling variables table temp table
     E03 b - Declare variables
     E03 c - loop across 7 days of scaling week
     E03 d - Clear tables
     E03 e - Flag accounts with complete viewing data
     E03 f - Rim-weighting
     E03 g - Update historical weights table with weights for this scaling date
     E03 h - Update QA tables with scaling metrics

*/


---------------------------------------------------------------
-- E03 a - Create Scaling variables table temp table

-- This table is required for the iterative process of the rim-weighting.
-- The Rim-weighting iterates through each of these variables individually until
-- all the category sum of weights have converged to the population category subtotals

SELECT 1 AS id
               ,'hhcomposition' as scaling_variables         INTO #scaling_variables -- drop table #scaling_variables
UNION SELECT 2,'package'
UNION SELECT 3,'isba_tv_region'
UNION SELECT 4,'tenure'
UNION SELECT 5,'boxtype'

---------------------------------------------------------------
-- E03 b - Declare variables

-- DECLARE @weekly_reference_date DATE - WTF multiple declarations again? this is soooo sad!
DECLARE @cntr           INT
DECLARE @iteration      INT
DECLARE @cntr_var       SMALLINT
DECLARE @scaling_var    VARCHAR(30)
DECLARE @convergence    TINYINT
DECLARE @sky_base       DOUBLE
DECLARE @vespa_panel    DOUBLE
DECLARE @sum_of_weights DOUBLE

-- This takes the maximum date from the scaling_weights table and adds a day to
-- give the first day of the current scaling week.

set @weekly_reference_date = (select MAX(dateadd(day,1,scaling_date)) from scaling_weights)

-- The scaling_date represents the date of the viewing day. The Rim-weighting process
-- will loop through each day of the currently scaling week using this variable.
SET @weekly_reference_date   = @weekly_reference_date


--------------------------------------------------------------- C03 - Clear tables
-- E03 d - Clear tables

DELETE FROM scaling_segments
DELETE FROM scaling_categories
COMMIT


--------------------------------------------------------------- C04 - Flag accounts with complete viewing data
-- E03 e - Flag accounts with complete viewing data

-- Complete viewing data is defined as the primary box returning viewing and all
-- the secondary boxes in the households (if multiroom).


-- reset viewing flag for each day of week

UPDATE Scaling_box_level_viewing
   SET  viewing_flag = 0


UPDATE Scaling_box_level_viewing
SET  viewing_flag = 1
FROM Scaling_box_level_viewing AS base
    inner join poveys.sov_daily_records AS vw ON vw.service_instance_id = base.service_instance_id
WHERE base.viewing_flag = 0


-- This complete_viewing flag represents our vespa panel for this scaling day. Our vespa panel consists
-- of accounts where the primary box and all the secondary boxes have returned viewing for that day.

select sblv.account_number
       ,sws.universe
       ,max(case when sblv.mr = 0 and sblv.viewing_flag = 1 then 1 else 0 end) as primary_box_viewing
       ,sum(case when sblv.mr = 1 and sblv.viewing_flag = 1 then 1 else 0 end) as secondary_box_viewing
  into #viewing_account_level
  from Scaling_box_level_viewing as sblv
        inner join scaling_weekly_sample as sws on sws.account_number = sblv.account_number
group by sblv.account_number, sws.universe

UPDATE scaling_weekly_sample
SET base.complete_viewing = CASE WHEN (val.universe = 'A) Single box HH'   AND val.primary_box_viewing = 1)                                                THEN 1
                                 WHEN (val.universe = 'B) Dual box HH'     AND val.primary_box_viewing = 1 AND val.secondary_box_viewing = 1)              THEN 1
                                 WHEN (val.universe = 'C) Multiple box HH' AND val.primary_box_viewing = 1 AND val.secondary_box_viewing = base.mr_boxes)  THEN 1
                                                                                                                                             ELSE 0 END
from scaling_weekly_sample as base
        inner join #viewing_account_level as val on val.account_number = base.account_number

--------------------------------------------------------------- C05 - Rim-weighting
-- E03 f - Rim-weighting


-- Rim-weighting is an iterative process that iterates through each of the scaling variables
-- individually until the category sum of weights converge to the population category subtotals


SET @cntr           = 1
SET @iteration      = 0
SET @cntr_var       = 1
SET @scaling_var    = (SELECT scaling_variables FROM #scaling_variables WHERE id = @cntr)

-- The scaling_segments table contains subtotals and sum_of_weights for all segments represented by
-- the sky base.
-- Some segments are not represented by the vespa panel, these are allocated an arbitrary value of 0.000001
-- to ensure convergence.


INSERT INTO scaling_segments (universe,scaling_segment_id, sky_base_accounts, vespa_panel, sum_of_weights)
SELECT   universe
        ,scaling_segment_id
        ,COUNT(*)
        ,CASE WHEN SUM(complete_viewing) = 0 THEN 0.000001 ELSE SUM(complete_viewing) END
        ,CASE WHEN SUM(complete_viewing) = 0 THEN 0.000001 ELSE SUM(complete_viewing) END -- prepare for first iteration
 FROM scaling_weekly_sample
GROUP BY universe, scaling_segment_id



-- The iterative part.
-- This works by choosing a particular scaling variable and then summing across the categories
-- of that scaling variable for the sky base, the vespa panel and the sum of weights.
-- A Category weight is calculated by dividing the sky base subtotal by the vespa panel subtotal
-- for that category.
-- This category weight is then applied back to the segments table and the process repeats until
-- the sum_of_weights in the category table converges to the sky base subtotal.

-- Category Convergence is defined as the category sum of weights being +/- 3 away from the sky
-- base category subtotal within 100 iterations.
-- Overall Convergence for that day occurs when each of the categories has converged, or the @convergence variable = 0

-- The @convergence variable represents how many categories did not converge.
-- If the number of iterations = 100 and the @convergence > 0 then this means that the Rim-weighting
-- has not converged for this particular day.
-- In this scenario, the person running the code should send the results of the scaling_metrics for that
-- week to analytics team for review.


WHILE @cntr <6
BEGIN
        DELETE FROM scaling_categories

        SET @cntr_var = 1
        WHILE @cntr_var < 6
        BEGIN
                    SET @scaling_var = (SELECT scaling_variables FROM #scaling_variables WHERE id = @cntr_var)

                    EXECUTE('
                    INSERT INTO scaling_categories (universe,profile,value,sky_base_accounts,vespa_panel,sum_of_weights)
                        SELECT  srs.universe
                               ,@scaling_var
                               ,ssl.'||@scaling_var||'
                               ,SUM(srs.sky_base_accounts)
                               ,SUM(srs.vespa_panel)
                               ,SUM(srs.sum_of_weights)
                        FROM scaling_segments AS srs
                                inner join vespa_analysts.scaling_segments_lookup_table AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                        GROUP BY srs.universe,ssl.'||@scaling_var||'
                        ORDER BY srs.universe
                    ')

                    SET @cntr_var = @cntr_var + 1
        COMMIT
        END


        UPDATE scaling_categories
        SET  category_weight = sky_base_accounts / sum_of_weights
            ,convergence_flag = CASE WHEN abs(sky_base_accounts - sum_of_weights) < 3 THEN 0 ELSE 1 END

        SET @convergence = (SELECT SUM(convergence_flag) FROM scaling_categories)
        SET @iteration = @iteration + 1
        SET @scaling_var = (SELECT scaling_variables FROM #scaling_variables WHERE id = @cntr)

        EXECUTE('
        UPDATE scaling_segments
        SET  srs.category_weight = sc.category_weight
            ,srs.sum_of_weights  = srs.sum_of_weights * sc.category_weight
        FROM scaling_segments AS srs
                inner join vespa_analysts.scaling_segments_lookup_table AS ssl ON srs.scaling_segment_id = ssl.scaling_segment_id
                inner join scaling_categories AS sc ON sc.value = ssl.'||@scaling_var||'
                                                                 AND sc.universe = ssl.universe
        ')

IF @iteration = 100 OR @convergence = 0 SET @cntr = 6
ELSE

IF @cntr = 5  SET @cntr = 1
ELSE
SET @cntr = @cntr+1

COMMIT

END



-- Calculate segment weight and corresponding indices

-- This section calculates the segment weight which is the weight that should be applied to viewing data
-- A couple of indices are also calculated so that we can keep track of the performance of the rim-weighting


SELECT @sky_base = SUM(sky_base_accounts) FROM scaling_segments
SELECT @vespa_panel = SUM(vespa_panel) FROM scaling_segments
SELECT @sum_of_weights = SUM(sum_of_weights) FROM scaling_segments

UPDATE scaling_segments
SET  segment_weight = sum_of_weights / vespa_panel
    ,indices_actual = 100*(vespa_panel / @vespa_panel) / (sky_base_accounts / @sky_base)
    ,indices_weighted = 100*(sum_of_weights / @sum_of_weights) / (sky_base_accounts / @sky_base)



--------------------------------------------------------------- C06 - Update Viewing table
-- E03 g - Update Viewing table

-- Weights table (keep separate for QA purposes)

INSERT INTO Scaling_weights (scaling_date, scaling_segment_id, account_number, weight)
SELECT  @weekly_reference_date
        ,ss.scaling_segment_id
        ,sws.account_number
        ,ss.segment_weight
FROM scaling_weekly_sample AS sws
        inner join scaling_segments AS ss ON ss.scaling_segment_id = sws.scaling_segment_id
WHERE sws.complete_viewing = 1

-- Metrics

INSERT INTO scaling_metrics (scaling_date, iterations, convergence,max_weight, av_weight,
                             sum_of_weights ,sky_base, vespa_panel, non_scalable_accounts)
SELECT  @weekly_reference_date
       ,@iteration
       ,@convergence
       ,MAX(weight)
       ,AVG(weight)
       ,SUM(weight)
       ,@sky_base
       ,COUNT(DISTINCT CASE WHEN weight <> 0 THEN account_number ELSE NULL END)
       ,COUNT(DISTINCT CASE WHEN weight = 0  THEN account_number ELSE NULL END)
FROM scaling_weights
WHERE scaling_date = @weekly_reference_date

-- Update viewing table


alter table internal_capped_viewing_new
 add phaseII_weight as float default 0;

Update internal_capped_viewing_new
set base.phaseII_weight = sb.weight
from internal_capped_viewing_new as base
    inner join scaling_weights as sb on base.account_number = sb.account_number
and scaling_date = @weekly_reference_date;

delete from internal_capped_viewing_new
where phaseII_weight = 0;



-----------------------------------------------------------------------------------
-- F) - Scale up BARB mins/ create audience of 1+ mins pay TV
-----------------------------------------------------------------------------------


alter table internal_capped_viewing_new
add pay_free_indicator as    varchar(10);

update internal_capped_viewing_new
set icv.pay_free_indicator = wp.pay_free_indicator
from internal_capped_viewing_new as icv
        inner join week_Programmes as wp on icv.epg_channel = wp.epg_channel
        and icv.program_air_datetime = wp.program_air_datetime;


-- look at everyone who's watched pay TV

select distinct (account_number)
into #pay
from internal_capped_viewing_new
where pay_free_indicator = 'PAY TV';


-- scale up BARB mins

alter table internal_capped_viewing_new
 add barb_minutes integer;

alter table internal_capped_viewing_new
 add barb_minutes_weighted bigint;


update internal_capped_viewing_new
 set barb_minutes = (datediff(minute,minute_started, minute_stopped));

update internal_capped_viewing_new
 set barb_minutes = 1 where barb_minutes = 0; -- every instance in the table is a min of one minute
-- 15,187,408 Row(s) affected

update internal_capped_viewing_new
 set barb_minutes_weighted = barb_minutes * phaseII_weight;

-- select * from internal_capped_viewing_new where barb_minutes > 400
-- select top 10 * from internal_capped_viewing_new

-- only take households that have watched 1+ mins pay TV

IF object_id('final_viewing_new') IS NOT NULL DROP TABLE final_viewing_new;

select *
into final_viewing_new
from internal_capped_viewing_new
where account_number in (select account_number from #pay)


/*
--------------------------------------------------------------------------------
-- PART G - outputs
--------------------------------------------------------------------------------

        PART H   - Ouputs
             G01 - Create variables and daily viewing viewing totals
             G02 - Output: paid_free SOV table
             G03 - Output: SOV by Channel
             G04 - Add Paid/Free Deciles (proportion of paid viewing)
             G05 - OUTPUT: SOV by Deciles 1
             G06 - Add Paid/Free Deciles (total viewing)
             G07 - OUTPUT: SOV by Deciles 2

--------------------------------------------------------------------------------
*/



--------------------------------------------------------------------------------
-- G01 -- Create variables and daily viewing viewing totals
--------------------------------------------------------------------------------

-- select top 100 * from final_viewing2

-- Create variables to store daily live and vosdal viewing, and playback viewing, then sum these for total days viewing (used for SOV calculation)
-- bigint: max = 9,223,372,036,854,775,807

--Create the variables
create variable @monday_lv           bigint;    -- live
create variable @monday_vos          bigint;    -- vosdal
create variable @monday_pb           bigint;    -- playback
create variable @total_monday        bigint;    -- sum of the above

create variable @tuesday_lv          bigint;
create variable @tuesday_vos         bigint;
create variable @tuesday_pb          bigint;
create variable @total_tuesday       bigint;

create variable @wednesday_lv        bigint;
create variable @wednesday_vos        bigint;
create variable @wednesday_pb        bigint;
create variable @total_wednesday     bigint;

create variable @thursday_lv         bigint;
create variable @thursday_vos        bigint;
create variable @thursday_pb         bigint;
create variable @total_thursday      bigint;

create variable @friday_lv           bigint;
create variable @friday_vos          bigint;
create variable @friday_pb           bigint;
create variable @total_friday        bigint;

create variable @saturday_lv         bigint;
create variable @saturday_vos        bigint;
create variable @saturday_pb         bigint;
create variable @total_saturday      bigint;

create variable @sunday_lv           bigint;
create variable @sunday_vos          bigint;
create variable @sunday_pb           bigint;
create variable @total_sunday        bigint;

create variable @total_week          bigint;
create variable @total_week_live     bigint;
create variable @total_week_vos      bigint;
create variable @total_week_pb       bigint;

-- *Populate the variables*
--  Playback is calculated based on the barb day

-- Monday --------------------------------------------------
set @monday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1  and barb_day = 1)

set @monday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 1)

set @monday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,0,@var_period_start_dt)) and (dateadd(day,1,@var_period_start_dt)))

set @total_monday = @monday_lv + @monday_pb + @monday_vos

-- @total_monday
-- 52148962995

-- tuesday --------------------------------------------------
set @tuesday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1 and barb_day = 2)

set @tuesday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 2)

set @tuesday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,1,@var_period_start_dt)) and (dateadd(day,2,@var_period_start_dt)))

set @total_tuesday = @tuesday_lv + @tuesday_pb + @tuesday_vos



-- wednesday --------------------------------------------------
set @wednesday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1 and barb_day = 3)

set @wednesday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 3)

set @wednesday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,2,@var_period_start_dt)) and (dateadd(day,3,@var_period_start_dt)))

set @total_wednesday = @wednesday_lv + @wednesday_pb + @wednesday_vos



-- thursday --------------------------------------------------
set @thursday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1 and barb_day = 4)

set @thursday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 4)


set @thursday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,3,@var_period_start_dt)) and (dateadd(day,4,@var_period_start_dt)))

set @total_thursday = @thursday_lv + @thursday_pb + @thursday_vos



-- friday --------------------------------------------------
set @friday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1 and barb_day = 5)

set @friday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 5)

set @friday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,4,@var_period_start_dt)) and (dateadd(day,5,@var_period_start_dt)))

set @total_friday = @friday_lv + @friday_pb + @friday_vos



-- saturday --------------------------------------------------
set @saturday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1 and barb_day = 6)

set @saturday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 6)

set @saturday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,5,@var_period_start_dt)) and (dateadd(day,6,@var_period_start_dt)))

set @total_saturday = @saturday_lv + @saturday_pb + @saturday_vos



-- sunday --------------------------------------------------
set @sunday_lv = (select sum(barb_minutes_weighted) from final_viewing2 where live = 1 and barb_day = 7)

set @sunday_vos = (select sum(barb_minutes_weighted) from final_viewing2 where vosdal = 1 and barb_day = 7)

set @sunday_pb = (select sum(barb_minutes_weighted) from final_viewing2 where playback = 1 and program_air_datetime
                                         between (dateadd(day,6,@var_period_start_dt)) and (dateadd(day,7,@var_period_start_dt)))

set @total_sunday = @sunday_lv + @sunday_pb + @sunday_vos


-- calculate week totals

--live week
set @total_week_live =(@monday_lv + @tuesday_lv + @wednesday_lv + @thursday_lv + @friday_lv + @saturday_lv + @sunday_lv)

--vosdal week
set @total_week_vos =(@monday_vos + @tuesday_vos + @wednesday_vos + @thursday_vos + @friday_vos + @saturday_vos + @sunday_vos)

--playback week
set @total_week_pb =(@monday_pb + @tuesday_pb + @wednesday_pb + @thursday_pb + @friday_pb + @saturday_pb + @sunday_pb)

-- Total viewing for the week
set @total_week = (@total_monday + @total_tuesday + @total_wednesday + @total_thursday + @total_friday + @total_saturday + @total_sunday)



select @total_week -- 53,807,728,071 -- extra 10 billion minutes!!



-------------------------------------------------------------------------------------------------------------------------------------------------
-- create a quick output table of the total viewing minutes (barb) for each dimension

IF object_id('day_totals') IS NOT NULL DROP TABLE day_totals;



create table day_totals
( day varchar(10)
, live bigint
, vosdal bigint
, playback bigint
, total bigint);

insert into day_totals(day) values ('monday');
insert into day_totals(day) values ('tuesday');
insert into day_totals(day) values ('wednesday');
insert into day_totals(day) values ('thursday');
insert into day_totals(day) values ('friday');
insert into day_totals(day) values ('saturday');
insert into day_totals(day) values ('sunday');
insert into day_totals(day) values ('week');

update day_totals
set live = case when day = 'monday' then @monday_lv
                when day = 'tuesday' then @tuesday_lv
                when day = 'wednesday' then @wednesday_lv
                when day = 'thursday' then @thursday_lv
                when day = 'friday' then @friday_lv
                when day = 'saturday' then @saturday_lv
                when day = 'sunday' then @sunday_lv
                when day = 'week' then @total_week_live end

    ,vosdal = case when day = 'monday' then @monday_vos
                when day = 'tuesday' then @tuesday_vos
                when day = 'wednesday' then @wednesday_vos
                when day = 'thursday' then @thursday_vos
                when day = 'friday' then @friday_vos
                when day = 'saturday' then @saturday_vos
                when day = 'sunday' then @sunday_vos
                when day = 'week' then @total_week_vos end

    ,playback = case when day = 'monday' then @monday_pb
                when day = 'tuesday' then @tuesday_pb
                when day = 'wednesday' then @wednesday_pb
                when day = 'thursday' then @thursday_pb
                when day = 'friday' then @friday_pb
                when day = 'saturday' then @saturday_pb
                when day = 'sunday' then @sunday_pb
                when day = 'week' then @total_week_pb end

    ,total = case when day = 'monday' then @total_monday
                when day = 'tuesday' then @total_tuesday
                when day = 'wednesday' then @total_wednesday
                when day = 'thursday' then @total_thursday
                when day = 'friday' then @total_friday
                when day = 'saturday' then @total_saturday
                when day = 'sunday' then @total_sunday
                when day = 'week' then @total_week end

select * from day_totals




--------------------------------------------------------------------------------
-- G02  Output: paid_free SOV table
--------------------------------------------------------------------------------


IF object_id('sov_paid_free') IS NOT NULL DROP TABLE sov_paid_free;



select (case when pay_free_indicator = 'PAY TV' then 1 else 0 end) as paid
       -- Free = (Case when paid_free_indicator in ('FREE TV','RADIO') then 1 else 0 end)
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))/@monday_lv   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))/@tuesday_lv   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))/@wednesday_lv   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))/@thursday_lv   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))/@friday_lv   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))/@saturday_lv   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))/@sunday_lv   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))/@total_week_live   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))/@monday_vos   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))/@tuesday_vos  as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))/@wednesday_vos   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))/@thursday_vos   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))/@friday_vos   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))/@saturday_vos   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))/@sunday_vos   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))/@total_week_vos   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@monday_pb   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@tuesday_pb   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@wednesday_pb   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@thursday_pb   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@friday_pb   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@saturday_pb   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@sunday_pb   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))/@total_week_vos   as Playback_week


        ,(sum(case when live = 1 and barb_day = 1 or vosdal = 1 and barb_day = 1  or playback = 1
                                 and program_air_datetime between (dateadd(day,0,@var_period_start_dt)) and (dateadd(day,1,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_monday          as consolidated_monday

        ,(sum(case when live = 1 and barb_day = 2 or vosdal = 1 and barb_day = 2  or playback = 1
                                 and program_air_datetime between (dateadd(day,1,@var_period_start_dt)) and (dateadd(day,2,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_tuesday          as consolidated_tuesday

        ,(sum(case when live = 1 and barb_day = 3 or vosdal = 1 and barb_day = 3  or playback = 1
                                 and program_air_datetime between (dateadd(day,2,@var_period_start_dt)) and (dateadd(day,3,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_wednesday        as consolidated_wednesday

        ,(sum(case when live = 1 and barb_day = 4 or vosdal = 1 and barb_day = 4  or playback = 1
                                 and program_air_datetime between (dateadd(day,3,@var_period_start_dt)) and (dateadd(day,4,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_thursday         as consolidated_thursday

        ,(sum(case when live = 1 and barb_day = 5 or vosdal = 1 and barb_day = 5  or playback = 1
                                 and program_air_datetime between (dateadd(day,4,@var_period_start_dt)) and (dateadd(day,5,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_friday           as consolidated_friday

        ,(sum(case when live = 1 and barb_day = 6 or vosdal = 1 and barb_day = 6  or playback = 1
                                 and program_air_datetime between (dateadd(day,5,@var_period_start_dt)) and (dateadd(day,6,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_saturday         as consolidated_saturday

        ,(sum(case when live = 1 and barb_day = 7 or vosdal = 1 and barb_day = 7  or playback = 1
                                 and program_air_datetime between (dateadd(day,6,@var_period_start_dt)) and (dateadd(day,7,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_sunday           as consolidated_sunday

        ,(sum(case when playback = 1 or live = 1 and barb_day in (1,2,3,4,5,6,7) or vosdal = 1 and barb_day in (1,2,3,4,5,6,7)
                        then (cast(barb_minutes_weighted as float)) else null end))/@total_week              as consolidated_week

into sov_paid_free
from final_viewing2
group by paid;


-- add on last weeks data to the table
-- add the fields
alter table sov_paid_free
add (last_week_live             as float
    ,last_week_vosdal           as float
    ,last_week_playback         as float
    ,last_week_consolidated     as float);


-- make a copy of the table and rename it for use on the next run

IF object_id('sov_paid_free_lw') IS NOT NULL DROP TABLE sov_paid_free_lw;

select *
 into sov_paid_free_lw
 from sov_paid_free;

-- populate the fields
update sov_paid_free
        set last_week_live      = ( lw.live_week)
            ,last_week_vosdal    = ( lw.vosdal_week)
            ,last_week_playback   = ( lw.playback_week)
            ,last_week_consolidated = ( lw.consolidated_week)

from sov_paid_free    as curr
 join sov_paid_free_lw as lw
on lw.paid = curr.paid;


-- OUTPUT--
select * from sov_paid_free order by paid

--will be this table will be dropped on the next run



--------------------------------------------------------------------------------
-- G03  Output: SOV by Channel
--------------------------------------------------------------------------------

-- when consolidating playback viewing, its consolidated back to the barb air day.
-- viewing time of day reflects when the content was consumed
-- if the viewing time of day of broadcast is required and viewing needs to be consolidated to aired daypart then use aired_time_of day

IF object_id('sov_channel') IS NOT NULL DROP TABLE sov_channel;



select  distinct(epg_channel)
        ,viewing_time_of_day
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))/@monday_lv   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))/@tuesday_lv   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))/@wednesday_lv   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))/@thursday_lv   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))/@friday_lv   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))/@saturday_lv   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))/@sunday_lv   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))/@total_week_live   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))/@monday_vos   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))/@tuesday_vos  as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))/@wednesday_vos   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))/@thursday_vos   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))/@friday_vos   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))/@saturday_vos   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))/@sunday_vos   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))/@total_week_vos   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@monday_pb   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@tuesday_pb   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@wednesday_pb   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@thursday_pb   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@friday_pb   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@saturday_pb   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))/@sunday_pb   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))/@total_week_vos   as Playback_week
-- need to add total week

        ,(sum(case when live = 1 and barb_day = 1 or vosdal = 1 and barb_day = 1  or playback = 1
                                 and program_air_datetime between (dateadd(day,0,@var_period_start_dt)) and (dateadd(day,1,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_monday          as consolidated_monday

        ,(sum(case when live = 1 and barb_day = 2 or vosdal = 1 and barb_day = 2  or playback = 1
                                 and program_air_datetime between (dateadd(day,1,@var_period_start_dt)) and (dateadd(day,2,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_tuesday          as consolidated_tuesday

        ,(sum(case when live = 1 and barb_day = 3 or vosdal = 1 and barb_day = 3  or playback = 1
                                 and program_air_datetime between (dateadd(day,2,@var_period_start_dt)) and (dateadd(day,3,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_wednesday        as consolidated_wednesday

        ,(sum(case when live = 1 and barb_day = 4 or vosdal = 1 and barb_day = 4  or playback = 1
                                 and program_air_datetime between (dateadd(day,3,@var_period_start_dt)) and (dateadd(day,4,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_thursday         as consolidated_thursday

        ,(sum(case when live = 1 and barb_day = 5 or vosdal = 1 and barb_day = 5  or playback = 1
                                 and program_air_datetime between (dateadd(day,4,@var_period_start_dt)) and (dateadd(day,5,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_friday           as consolidated_friday

        ,(sum(case when live = 1 and barb_day = 6 or vosdal = 1 and barb_day = 6  or playback = 1
                                 and program_air_datetime between (dateadd(day,5,@var_period_start_dt)) and (dateadd(day,6,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_saturday         as consolidated_saturday

        ,(sum(case when live = 1 and barb_day = 7 or vosdal = 1 and barb_day = 7  or playback = 1
                                 and program_air_datetime between (dateadd(day,6,@var_period_start_dt)) and (dateadd(day,7,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))/@total_sunday           as consolidated_sunday

        ,(sum(case when playback = 1 or live = 1 and barb_day in (1,2,3,4,5,6,7) or vosdal = 1 and barb_day in (1,2,3,4,5,6,7)
                        then (cast(barb_minutes_weighted as float)) else null end))/@total_week              as consolidated_week

into sov_channel
from final_viewing2
group by epg_channel, viewing_time_of_day;
--630 Row(s) affected

--226289 Row(s) affected

-- roll up the channels data, so that regional variations of channels are captured as a single channel as per BARB
IF object_id('sov_channels') IS NOT NULL DROP TABLE sov_channels;


select
epg_channels=
case when  epg_channel in
                        ('BBC 1 CI','BBC 1 Cambridge','BBC 1 E Mids','BBC 1 E Yrks & L','BBC 1 East','BBC 1 N West','BBC 1 N Yorks'
                        ,'BBC 1 NE & C','BBC 1 Oxford','BBC 1 S East','BBC 1 S West','BBC 1 South','BBC 1 W Mids','BBC 1 West'
                        ,'BBC ONE Digital England (Londo','BBC ONE Digital for N Ireland','BBC ONE Digital for Scotland'
                        ,'BBC ONE Digital for Wales')                                                                           then 'BBC 1'

        when epg_channel in
                        ('BBC TWO Digital for England','BBC TWO N.Ireland','BBC TWO Scotland','BBC TWO Wales')                  then 'BBC 2'

        when epg_channel in
                        ('ITV Central SW','ITV1 Anglia E','ITV1 Anglia South NEW','ITV1 Anglia W'
                        ,'ITV1 Border','ITV1 BorderSco','ITV1 Central E','ITV1 Central S','ITV1 Central W','ITV1 Channel Is'
                        ,'ITV1 Granada','ITV1 London'
                        ,'ITV1 Meridian E','ITV1 Meridian N','ITV1 Meridian S','ITV1 Meridian SE','ITV1 STV Grampian'
                        ,'ITV1 STV Scottish','ITV1 Scottish E','ITV1 Tyne Tees','ITV1 Tyne Tees South','ITV1 UTV'
                        ,'ITV1 W Country','ITV1 Wales','ITV1 West','ITV1 Yorkshire','ITV1 Yorkshire East')                      then 'ITV1'

        when epg_channel in
                        ('ITV HD STV','ITV1 HD London','ITV1 HD Mid West','ITV1 HD North','ITV1 HD S East','ITV1 3D Test')      then 'ITV HD'

        when epg_channel in
                        ('ITV1 Central+1','ITV1 Granada+1','ITV1 London+1','ITV1 UTV+1','ITV1 West+1','ITV1 Yorkshire/Tyne Tees +1'
                        ,'ITV1 South East+1','ITV1 STV +1')                                                                     then 'ITV1 +1'

        when epg_channel in
                        ('Channel 4 London','Channel 4 Midlands','Channel 4 North','Channel 4 Scotland'
                        ,'Channel 4 South','Channel 4 Ulster')                                                                  then 'Channel 4'

        when epg_channel in
                        ('Channel 4 +1 London','Channel 4 +1 Midlands','Channel 4 +1 North','Channel 4 +1 Scotland'
                        ,'Channel 4 +1 South','Channel 4 +1 Ulster')                                                            then 'Channel 4 + 1'

        when epg_channel in
                        ('Channel 5','Channel 5  Northern Ireland','Channel 5 London'
                        ,'Channel 5 North','Channel 5 Scotland')                                                                then 'Channel 5'

        when epg_channel in
                        ('Sky Sports News','Sky Sports News Eire'  ,'Sky Spts News COMM')                                       then 'Sky Sports News'

         when epg_channel in
                        ('Sky Sports News HD','Sky Sports NewsHD COMM')                                                         then 'Sky Sports News HD'

        when epg_channel in
                        ('Sky Sports 1','Sky Sports 1 COMM','Sky Sports 1 EIRE','Sky Sports 1 HOTEL')                           then 'Sky Sports 1'

        when epg_channel in
                        ('Sky Sports 2','Sky Sports 2 COMM','Sky Sports 2 EIRE','Sky Sports 2 HOTEL')                           then 'Sky Sports 2'
        when epg_channel in
                        ('Sky Sports 3','Sky Sports 3 COMM','Sky Sports 3 HOTEL')                                               then 'Sky Sports 3'

        when epg_channel in
                        ('Sky Sports 4','Sky Sports 4 COMM','Sky Sports 4 HOTEL','Sky Sports 4 Pub')                           then 'Sky Sports 4'

        when epg_channel in
                        ('Sky Sports HD1 COMM','Sky Sports HD1 PUB','Sky Sports HD1')                                           then 'Sky Sports HD1'

        when epg_channel in
                        ('Sky Sports HD2','Sky Sports HD2 PUB')                                                                 then 'Sky Sports HD2'

        when epg_channel in
                        ('Sky Sports HD3','Sky Sports HD3 COMM','Sky Sports HD3 PUB')                                           then 'Sky Sports HD3'

        when epg_channel in
                        ('Sky Sports HD4','Sky Sports 4 Pub HD')                                                                then 'Sky Sports HD4'

        when epg_channel in
                        ('ESPN','ESPN COMM')                                                                                    then 'ESPN'

        when epg_channel in
                        ('ESPN COMM HD','ESPN HD')                                                                              then 'ESPN HD'

        when epg_channel in
                        ('Sky 1','Sky 1 Eire','Sky 1 UK COMM')                                                                  then 'Sky 1'

        when epg_channel in
                        ('Sky Living','Sky Living (ROI)')                                                                       then 'Sky Living'

        when epg_channel in
                        ('SBO 01','SBO 02','SBO 03','SBO 04','SBO 05'
                        ,'SBO 06','SBO 07','SBO 08','SBO 09','SBO 10','SBO 11','SBO 12','SBO 13','SBO 14','SBO 15'
                        ,'SBO 16','SBO 17','SBO 18','SBO 19','SBO 20','SBO 21','SBO 22','SBO 23','SBO 24','SBO 25'
                        ,'SBO 26','SBO 27','SBO 28','SBO 29','SBO 30','SBO 31','SBO 32','SBO 33','SBO 34','SBO 35','SBO 36'
                        ,'SBO 37','SBO 38','SBO 39','SBO 44','SBO 54','SBO 55','SBO 56','SBO 57','SBO 58','SBO 59','SBO 60'
                        ,'SBO 61','SBO 743 PPV Event','SBO Preview Ch 700')                                                     then 'Sky Box Office'

        when epg_channel in
                        ('SBO 1 - HD','SBO 2 - HD')                                                                             then 'Sky Box Office HD'

        else epg_channel end

        ,viewing_time_of_day

        ,sum(Live_Monday) as Live_Monday
        ,sum(Live_Tuesday) as Live_Tuesday
        ,sum(Live_Wednesday) as Live_Wednesday
        ,sum(Live_Thursday) as Live_Thursday
        ,sum(Live_Friday) as Live_Friday
        ,sum(Live_Saturday) as Live_Saturday
        ,sum(Live_Sunday) as Live_Sunday
        ,sum(Live_Week) as Live_Week

        ,sum(Vosdal_Monday) as Vosdal_Monday
        ,sum(Vosdal_Tuesday) as Vosdal_Tuesday
        ,sum(Vosdal_Wednesday) as Vosdal_Wednesday
        ,sum(Vosdal_Thursday) as Vosdal_Thursday
        ,sum(Vosdal_Friday) as Vosdal_Friday
        ,sum(Vosdal_Saturday) as Vosdal_Saturday
        ,sum(Vosdal_Sunday) as Vosdal_Sunday
        ,sum(Vosdal_week) as Vosdal_week

        ,sum(Playback_Monday) as Playback_Monday
        ,sum(Playback_Tuesday) as Playback_Tuesday
        ,sum(Playback_Wednesday) as Playback_Wednesday
        ,sum(Playback_Thursday) as Playback_Thursday
        ,sum(Playback_Friday) as Playback_Friday
        ,sum(Playback_Saturday) as Playback_Saturday
        ,sum(Playback_Sunday) as Playback_Sunday
        ,sum(Playback_week) as Playback_week

        ,sum(consolidated_monday) as consolidated_monday
        ,sum(consolidated_tuesday) as consolidated_tuesday
        ,sum(consolidated_wednesday) as consolidated_wednesday
        ,sum(consolidated_thursday) as consolidated_thursday
        ,sum(consolidated_friday) as consolidated_friday
        ,sum(consolidated_saturday) as consolidated_saturday
        ,sum(consolidated_sunday) as consolidated_sunday
        ,sum(consolidated_week) as consolidated_week

into sov_channels
from sov_channel
group by epg_channels, viewing_time_of_day;



-- drop the un-needed table
drop table sov_channel;


-- add on last weeks data to the table
-- add the fields
alter table sov_channels
add (last_week_live             as float
    ,last_week_vosdal           as float
    ,last_week_playback         as float
    ,last_week_consolidated     as float);

-- populate the fields
update sov_channels
        set last_week_live      = ( lw.live_week)
            ,last_week_vosdal    = ( lw.vosdal_week)
            ,last_week_playback   = ( lw.playback_week)
            ,last_week_consolidated = ( lw.consolidated_week)

from sov_channels    as curr                                                    -- what if more channels are added? what kind of join?
 join sov_channels_lw as lw
on lw.epg_channels = curr.epg_channels
and lw.viewing_time_of_day = curr.viewing_time_of_day;


-- make a copy of the table and rename it for use on the next run

IF object_id('sov_channels_lw') IS NOT NULL DROP TABLE sov_channels_lw;

select *
 into sov_channels_lw
 from sov_channels;

-- sov_channels will be dropped on the next run (only 504 rows)

-- OUTPUT--
select * from sov_channels order by consolidated_week desc,epg_channels, viewing_time_of_day

-- can split the above table out into seperate tables for live,playback and vosdal


/*--- QA-----#

make sure a large number of channels are not blank


*/






------------------------------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- HO6 - Add Paid/Free Deciles (total viewing)
--------------------------------------------------------------------------------

-- Percentiles alternative -- based on total viewing  over the week in question

-- Create PERCENTILES BASED ON TOTAL VIEWING


--Get cumulative weights

IF object_id('cumulative_weights') IS NOT NULL DROP TABLE cumulative_weights;

SELECT
        account_number
        ,SUM (barb_minutes) as total_viewing
        ,phaseII_weight
INTO cumulative_weights
FROM final_viewing2
where barb_day between 1 and 7
GROUP BY account_number, phaseII_weight;

-- 206441 Row(s) affected

create index cumulative_index on cumulative_weights (phaseII_weight, total_viewing);

IF object_id('cumulative_weights_table') IS NOT NULL DROP TABLE cumulative_weights_table;

select
        account_number
        ,total_viewing
        ,phaseII_weight
        ,sum(phaseII_weight)over (order by total_viewing
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_weighting
into cumulative_weights_table
from cumulative_weights
order by total_viewing;


------------------------------------------------------------------------------------------------------------------------------------

-- Get numbers 0 - 100 into a table:



IF object_id('centile_weights') IS NOT NULL DROP TABLE centile_weights;

create table centile_weights (
        centile         int          primary key
);

create variable y int;
set y = 1;


while y <= 100
begin
        insert into centile_weights values (y)
        set y = y + 1
end;

--select * from centile_weights

--add sky base figures

alter table centile_weights
add sample integer;

update centile_weights
set sample = (centile-1)*94000;


-------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- Join cumulative weights to centiles

IF object_id('centile_table') IS NOT NULL DROP TABLE centile_table;

select a2.centile, a2.sample
        , min(a1.total_viewing) as viewing
        , min(a1.cumulative_weighting) as cumultive_weighting
        ,account_number
into centile_table
from cumulative_weights_table a1
inner join centile_weights a2
on a2.sample <= a1.cumulative_weighting
group by a2.centile, a2.sample,  account_number






--------------------------------------------------------------------------------
-- HO7 - Add Paid/Free Deciles (total viewing)
--------------------------------------------------------------------------------


-- the code below create total viewing per percentile

select
pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))  as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))   as Playback_week
-- need to add total week

        , (Live_Monday) + (Vosdal_Monday) + (Playback_Monday) as consolidated_monday

        , (Live_tuesday) + (Vosdal_tuesday) + (Playback_tuesday) as consolidated_tuesday

        , (Live_wednesday) + (Vosdal_wednesday) + (Playback_wednesday) as consolidated_wednesday

        , (Live_thursday) + (Vosdal_thursday) + (Playback_thursday) as consolidated_thursday

        , (Live_friday) + (Vosdal_friday) + (Playback_friday) as consolidated_friday

        , (Live_saturday) + (Vosdal_saturday) + (Playback_saturday) as consolidated_saturday

        , (Live_sunday) + (Vosdal_sunday) + (Playback_sunday) as consolidated_sunday

        , (Live_Week) + (Vosdal_week) + (Playback_week) as consolidated_week

into #vieiwng_rank_totals
from final_viewing2 base
inner join centile_table pf
on pf.account_number = base.account_number
group by pf.centile
--100 Row(s) affected


select * from #vieiwng_rank_totals order by centile



-- the code below finds the total share of viewing of paid TV for each percentile

IF object_id('sov_paid_total_viewing') IS NOT NULL DROP TABLE sov_paid_total_viewing;



select  pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Monday else null end))
                                                                                                                                        as Live_Monday1
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Tuesday else null end))
                                                                                                                                        as Live_Tuesday1
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Wednesday else null end))
                                                                                                                                        as Live_Wednesday1
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Thursday else null end))
                                                                                                                                        as Live_Thursday1
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Friday else null end))
                                                                                                                                        as Live_Friday1
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Saturday else null end))
                                                                                                                                         as Live_Saturday1
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Sunday else null end))
                                                                                                                                        as Live_Sunday1
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Week else null end))
                                                                                                                                        as Live_Week1
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Monday else null end))
                                                                                                                                        as Vosdal_Monday1
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Tuesday else null end))
                                                                                                                                        as Vosdal_Tuesday1
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Wednesday else null end))
                                                                                                                                        as Vosdal_Wednesday1
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Thursday else null end))
                                                                                                                                        as Vosdal_Thursday1
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Friday else null end))
                                                                                                                                        as Vosdal_Friday1
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Saturday else null end))
                                                                                                                                        as Vosdal_Saturday1
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Sunday else null end))
                                                                                                                                        as Vosdal_Sunday1
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_week else null end))
                                                                                                                                        as Vosdal_week1
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Monday else null end))
                                                                                                                                        as Playback_Monday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Tuesday else null end))
                                                                                                                                        as Playback_Tuesday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Wednesday else null end))
                                                                                                                                        as Playback_Wednesday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Thursday else null end))
                                                                                                                                        as Playback_Thursday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Friday else null end))
                                                                                                                                        as Playback_Friday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Saturday else null end))
                                                                                                                                        as Playback_Saturday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Sunday else null end))
                                                                                                                                        as Playback_Sunday1

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_week else null end))
                                                                                                                                           as Playback_week1
-- need to add total week

        ,(sum(case when live = 1 and barb_day = 1 or vosdal = 1 and barb_day = 1  or playback = 1
                                 and program_air_datetime between (dateadd(day,0,@var_period_start_dt)) and (dateadd(day,1,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_monday else null end))
                                                                                                                        as consolidated_monday1

        ,(sum(case when live = 1 and barb_day = 2 or vosdal = 1 and barb_day = 2  or playback = 1
                                 and program_air_datetime between (dateadd(day,1,@var_period_start_dt)) and (dateadd(day,2,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_tuesday else null end))
                                                                                                                        as consolidated_tuesday1

        ,(sum(case when live = 1 and barb_day = 3 or vosdal = 1 and barb_day = 3  or playback = 1
                                 and program_air_datetime between (dateadd(day,2,@var_period_start_dt)) and (dateadd(day,3,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_wednesday else null end))
                                                                                                                        as consolidated_wednesday1

        ,(sum(case when live = 1 and barb_day = 4 or vosdal = 1 and barb_day = 4  or playback = 1
                                 and program_air_datetime between (dateadd(day,3,@var_period_start_dt)) and (dateadd(day,4,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_thursday else null end))
                                                                                                                        as consolidated_thursday1

        ,(sum(case when live = 1 and barb_day = 5 or vosdal = 1 and barb_day = 5  or playback = 1
                                 and program_air_datetime between (dateadd(day,4,@var_period_start_dt)) and (dateadd(day,5,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_friday else null end))
                                                                                                                        as consolidated_friday1

        ,(sum(case when live = 1 and barb_day = 6 or vosdal = 1 and barb_day = 6  or playback = 1
                                 and program_air_datetime between (dateadd(day,5,@var_period_start_dt)) and (dateadd(day,6,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_saturday else null end))
                                                                                                                        as consolidated_saturday1

        ,(sum(case when live = 1 and barb_day = 7 or vosdal = 1 and barb_day = 7  or playback = 1
                                 and program_air_datetime between (dateadd(day,6,@var_period_start_dt)) and (dateadd(day,7,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_sunday else null end))
                                                                                                                        as consolidated_sunday1

        ,(sum(case when playback = 1 or live = 1 and barb_day in (1,2,3,4,5,6,7) or vosdal = 1 and barb_day in (1,2,3,4,5,6,7)
                        then (cast(barb_minutes_weighted as float)) else null end))
                        /(max(case when pf.centile = dt.centile then dt.consolidated_week else null end))          as consolidated_week1

into sov_paid_total_viewing
from final_viewing2 base
inner join centile_table pf
on pf.account_number = base.account_number
left join #vieiwng_rank_totals as dt
on pf.centile = dt.centile
where pay_free_indicator = 'PAY TV'
group by pf.centile
--100 Row(s) affected


select * from sov_paid_total_viewing order by centile


-- add a column containing the average paid viewing for each week, split by live, vos, pb, and consolidated

alter table sov_paid_total_viewing
add (week_average_live as float
    ,week_average_vos as float
    ,week_average_plat as float
    ,week_average_consol as float)

update sov_paid_total_viewing
        set week_average_live   = (select live_week            from sov_paid_free where paid = 1)
            ,week_average_vos    = (select vosdal_week          from sov_paid_free where paid = 1)
            ,week_average_plat   = (select playback_week         from sov_paid_free where paid = 1)
            ,week_average_consol = (select consolidated_week     from sov_paid_free where paid = 1)


-- OUTPUT--
select * from sov_paid_total_viewing order by centile, consolidated_week1



-------------------------- the total minutes percentiles


select
pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))  as Playback_week
-- need to add total week

        , (Live_Monday) + (Vosdal_Monday) + (Playback_Monday) as consolidated_monday

        , (Live_tuesday) + (Vosdal_tuesday) + (Playback_tuesday) as consolidated_tuesday

        , (Live_wednesday) + (Vosdal_wednesday) + (Playback_wednesday) as consolidated_wednesday

        , (Live_thursday) + (Vosdal_thursday) + (Playback_thursday) as consolidated_thursday

        , (Live_friday) + (Vosdal_friday) + (Playback_friday) as consolidated_friday

        , (Live_saturday) + (Vosdal_saturday) + (Playback_saturday) as consolidated_saturday

        , (Live_sunday) + (Vosdal_sunday) + (Playback_sunday) as consolidated_sunday

        , (Live_Week) + (Vosdal_week) + (Playback_week) as consolidated_week

into percentile2_minutes
from final_viewing2 base
inner join centile_table pf
on pf.account_number = base.account_number
where pay_free_indicator = 'PAY TV'
group by pf.centile
--100 Row(s) affected


-- OUTPUT
select * from percentile2_minutes order by centile

--------------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------
-- Percentiles by total *paid* viewing
------------------------------------------------------------------------------------------------------------------------------------



-- Percentiles alternative -- based on total viewing  over the week in question

-- Create PERCENTILES BASED ON TOTAL VIEWING




--Get cumulative weights

IF object_id('cumulative_weights_paid') IS NOT NULL DROP TABLE cumulative_weights_paid;

SELECT
        DISTINCT (account_number)
        ,SUM (barb_minutes) as total_viewing
        ,phaseII_weight
INTO cumulative_weights_paid
FROM final_viewing2
where barb_day between 1 and 7 and pay_free_indicator = 'PAY TV'
GROUP BY account_number, phaseII_weight

-- 206441 Row(s) affected

create index cumulative_paid on cumulative_weights_paid (phaseII_weight, total_viewing);

IF object_id('cumulative_weights_table_paid') IS NOT NULL DROP TABLE cumulative_weights_table_paid;

select
        DISTINCT (account_number)
        ,total_viewing
        ,phaseII_weight
        ,sum(phaseII_weight)over (order by total_viewing
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_weighting
into cumulative_weights_table_paid
from cumulative_weights_paid
order by total_viewing

select top 10 * from cumulative_weights_table
------------------------------------------------------------------------------------------------------------------------------------


-------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- Join cumulative weights to centiles

IF object_id('centile_table_paid') IS NOT NULL DROP TABLE centile_table_paid;

select a2.centile, a2.sample
        , min(a1.total_viewing) as viewing
        , min(a1.cumulative_weighting) as cumultive_weighting
        , account_number
into centile_table_paid
from cumulative_weights_table_paid a1
inner join centile_weights a2
on a2.sample <= a1.cumulative_weighting
group by a2.centile, a2.sample, account_number



select * from centile_table_paid




--------------------------------------------------------------------------------
-- HO7 - Add Paid/Free Deciles (total viewing)
--------------------------------------------------------------------------------


-- the code below create total viewing per percentile

select
pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))  as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))   as Playback_week
-- need to add total week

        , (Live_Monday) + (Vosdal_Monday) + (Playback_Monday) as consolidated_monday

        , (Live_tuesday) + (Vosdal_tuesday) + (Playback_tuesday) as consolidated_tuesday

        , (Live_wednesday) + (Vosdal_wednesday) + (Playback_wednesday) as consolidated_wednesday

        , (Live_thursday) + (Vosdal_thursday) + (Playback_thursday) as consolidated_thursday

        , (Live_friday) + (Vosdal_friday) + (Playback_friday) as consolidated_friday

        , (Live_saturday) + (Vosdal_saturday) + (Playback_saturday) as consolidated_saturday

        , (Live_sunday) + (Vosdal_sunday) + (Playback_sunday) as consolidated_sunday

        , (Live_Week) + (Vosdal_week) + (Playback_week) as consolidated_week

into #vieiwng_rank_totals1
from final_viewing2 base
inner join centile_table_paid pf
on pf.account_number = base.account_number
group by pf.centile
--100 Row(s) affected


select * from #vieiwng_rank_totals1 order by centile



-- the code below finds the total share of viewing of paid TV for each percentile

IF object_id('sov_paid_total_viewing2') IS NOT NULL DROP TABLE sov_paid_total_viewing2;



select  pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Monday else null end))
                                                                                                                                        as Live_Monday1
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Tuesday else null end))
                                                                                                                                        as Live_Tuesday1
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Wednesday else null end))
                                                                                                                                        as Live_Wednesday1
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Thursday else null end))
                                                                                                                                        as Live_Thursday1
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Friday else null end))
                                                                                                                                        as Live_Friday1
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Saturday else null end))
                                                                                                                                         as Live_Saturday1
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Sunday else null end))
                                                                                                                                        as Live_Sunday1
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Week else null end))
                                                                                                                                        as Live_Week1
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Monday else null end))
                                                                                                                                        as Vosdal_Monday1
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Tuesday else null end))
                                                                                                                                        as Vosdal_Tuesday1
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Wednesday else null end))
                                                                                                                                        as Vosdal_Wednesday1
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Thursday else null end))
                                                                                                                                        as Vosdal_Thursday1
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Friday else null end))
                                                                                                                                        as Vosdal_Friday1
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Saturday else null end))
                                                                                                                                        as Vosdal_Saturday1
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Sunday else null end))
                                                                                                                                        as Vosdal_Sunday1
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_week else null end))
                                                                                                                                        as Vosdal_week1
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Monday else null end))
                                                                                                                                        as Playback_Monday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Tuesday else null end))
                                                                                                                                        as Playback_Tuesday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Wednesday else null end))
                                                                                                                                        as Playback_Wednesday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Thursday else null end))
                                                                                                                                        as Playback_Thursday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Friday else null end))
                                                                                                                                        as Playback_Friday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Saturday else null end))
                                                                                                                                        as Playback_Saturday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Sunday else null end))
                                                                                                                                        as Playback_Sunday1

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_week else null end))
                                                                                                                                           as Playback_week1
-- need to add total week

        ,(sum(case when live = 1 and barb_day = 1 or vosdal = 1 and barb_day = 1  or playback = 1
                                 and program_air_datetime between (dateadd(day,0,@var_period_start_dt)) and (dateadd(day,1,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_monday else null end))
                                                                                                                        as consolidated_monday1

        ,(sum(case when live = 1 and barb_day = 2 or vosdal = 1 and barb_day = 2  or playback = 1
                                 and program_air_datetime between (dateadd(day,1,@var_period_start_dt)) and (dateadd(day,2,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_tuesday else null end))
                                                                                                                        as consolidated_tuesday1

        ,(sum(case when live = 1 and barb_day = 3 or vosdal = 1 and barb_day = 3  or playback = 1
                                 and program_air_datetime between (dateadd(day,2,@var_period_start_dt)) and (dateadd(day,3,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_wednesday else null end))
                                                                                                                        as consolidated_wednesday1

        ,(sum(case when live = 1 and barb_day = 4 or vosdal = 1 and barb_day = 4  or playback = 1
                                 and program_air_datetime between (dateadd(day,3,@var_period_start_dt)) and (dateadd(day,4,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_thursday else null end))
                                                                                                                        as consolidated_thursday1

        ,(sum(case when live = 1 and barb_day = 5 or vosdal = 1 and barb_day = 5  or playback = 1
                                 and program_air_datetime between (dateadd(day,4,@var_period_start_dt)) and (dateadd(day,5,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_friday else null end))
                                                                                                                        as consolidated_friday1

        ,(sum(case when live = 1 and barb_day = 6 or vosdal = 1 and barb_day = 6  or playback = 1
                                 and program_air_datetime between (dateadd(day,5,@var_period_start_dt)) and (dateadd(day,6,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_saturday else null end))
                                                                                                                        as consolidated_saturday1

        ,(sum(case when live = 1 and barb_day = 7 or vosdal = 1 and barb_day = 7  or playback = 1
                                 and program_air_datetime between (dateadd(day,6,@var_period_start_dt)) and (dateadd(day,7,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else null end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_sunday else null end))
                                                                                                                        as consolidated_sunday1

        ,(sum(case when playback = 1 or live = 1 and barb_day in (1,2,3,4,5,6,7) or vosdal = 1 and barb_day in (1,2,3,4,5,6,7)
                        then (cast(barb_minutes_weighted as float)) else null end))
                        /(max(case when pf.centile = dt.centile then dt.consolidated_week else null end))          as consolidated_week1

into sov_paid_total_viewing2
from final_viewing2 base
inner join centile_table_paid pf
on pf.account_number = base.account_number
left join #vieiwng_rank_totals1 as dt
on pf.centile = dt.centile
where pay_free_indicator = 'PAY TV'
group by pf.centile
--100 Row(s) affected


-- add a column containing the average paid viewing for each week, split by live, vos, pb, and consolidated

alter table sov_paid_total_viewing2
add (week_average_live as float
    ,week_average_vos as float
    ,week_average_plat as float
    ,week_average_consol as float)

update sov_paid_total_viewing2
        set week_average_live   = (select live_week            from sov_paid_free where paid = 1)
            ,week_average_vos    = (select vosdal_week          from sov_paid_free where paid = 1)
            ,week_average_plat   = (select playback_week         from sov_paid_free where paid = 1)
            ,week_average_consol = (select consolidated_week     from sov_paid_free where paid = 1)


-- OUTPUT--
select * from sov_paid_total_viewing2 order by centile


-------------------------------------------------------------------------------

-------------------------------------------------------------------------------

-- now we need the TOTAL minutes watched - not total *paid* minutes as before


select
pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))  as Playback_week
-- need to add total week

        , (Live_Monday) + (Vosdal_Monday) + (Playback_Monday) as consolidated_monday

        , (Live_tuesday) + (Vosdal_tuesday) + (Playback_tuesday) as consolidated_tuesday

        , (Live_wednesday) + (Vosdal_wednesday) + (Playback_wednesday) as consolidated_wednesday

        , (Live_thursday) + (Vosdal_thursday) + (Playback_thursday) as consolidated_thursday

        , (Live_friday) + (Vosdal_friday) + (Playback_friday) as consolidated_friday

        , (Live_saturday) + (Vosdal_saturday) + (Playback_saturday) as consolidated_saturday

        , (Live_sunday) + (Vosdal_sunday) + (Playback_sunday) as consolidated_sunday

        , (Live_Week) + (Vosdal_week) + (Playback_week) as consolidated_week

into percentile2_minutes_all
from final_viewing2 base
inner join centile_table_paid pf -- modified for new percentiles based on totla paid viewing
on pf.account_number = base.account_number
-- where pay_free_indicator = 'PAY TV' -- we dont want paid minutes this time - we want total minutes.
group by pf.centile
--100 Row(s) affected



select * from percentile2_minutes_all order by centile


-- note this is total minutes and applying the percentage from the first table (sov_paid_total_viewing2) to these numbers will reveal the
-- amount of paid viewing percentile.





--------------------------------------------------------------------------------
-- HO4 - Add Paid/Free Deciles (proportion of paid viewing)
--------------------------------------------------------------------------------

-- first lets identify the paid and free channels using the epg data:
-- we will use the pay_free_indicator

-- select top 10 * from week_Programmes where epg_channel = '4Music' -- free
-- select top 10 * from week_Programmes where epg_channel = 'Dave' -- pay


-- identify which channels are paid or free and input into a table
select distinct epg_channel
       ,pay_free_indicator
 into #channel_status
 from week_programmes


-- use this to flag the channels in the barb minutes table

update final_viewing2
  set brb.pay_free_indicator = st.pay_free_indicator
  from final_viewing2 brb
  left join #channel_status st
  on st.epg_channel = brb.epg_channel




select top 10 * from final_viewing2


-- use that table to calculate how many minutes were watched on paid and how many on free to create percentiles
-- create a table to hold the paid and free minutes
if object_id('paid_free') is not null drop table paid_free

create table paid_free (
         account_number         varchar(20) not null
        ,paid_viewing           as integer
        ,free_viewing           as integer
        ,phaseII_weight         as integer
        )

-- Populate the duration of pay TV watched by each account (subject to capping)
Insert into paid_free (
         account_number
         ,paid_viewing
         ,phaseII_weight)
select
        distinct(account_number)
        ,phaseII_weight
       ,sum(case when barb_minutes is not null then barb_minutes else  null end) as paid_viewing
from final_viewing2
where pay_free_indicator = 'PAY TV'
and barb_day between 1 and 7 -- all viewing has been captured for this period
group by account_number,phaseII_weight;


-- Populate the duration of FREE TV watched by each account (subject to capping)
Insert into paid_free (
         account_number
         ,free_viewing)
select
        distinct(account_number)
       ,sum(case when barb_minutes is not null then barb_minutes else null end) as free_viewing
from final_viewing2
where pay_free_indicator in ('FREE TV','RADIO') -- Radio is available on freeview
and barb_day between 1 and 7
group by account_number




select top 100 * from paid_free3



-- ensure there are no null fields for the calculation below
update paid_free
 set paid_viewing = 0 where paid_viewing is null;

 update paid_free
 set free_viewing = 0 where free_viewing is null



-- the table above will have duplicates - consolidate information in a new table
-- create a new table to hold all of the information for each account
if object_id('paid_free3') is not null drop table paid_free3

create table paid_free3 (
         account_number         varchar(20) not null
        ,paid_viewings          as float default 0
        ,free_viewings          as float default 0
        ,sum_viewing            as float
        ,phaseII_weight         as integer
               )

insert into paid_free3
select distinct(account_number)
        ,sum(paid_viewing) as paid_viewings
        ,sum(free_viewing) as free_viewings
        ,sum(free_viewing) + sum(paid_viewing) as sum_viewing
        ,phaseII_weight
from paid_free
group by account_number, phaseII_weight





--Get cumulative weights

IF object_id('cumulative_weights_paysov') IS NOT NULL DROP TABLE cumulative_weights_paysov;

SELECT
        DISTINCT (account_number)
        ,SUM (paid_viewings) as paid_viewing
        ,phaseII_weight
INTO cumulative_weights_paysov
FROM paid_free3
GROUP BY account_number, phaseII_weight

-- 206441 Row(s) affected


create index cumulative_paysov on cumulative_weights_paysov (phaseII_weight, paid_viewing);

IF object_id('cumulative_weights_table_paysov') IS NOT NULL DROP TABLE cumulative_weights_table_paysov;

select
        DISTINCT (account_number)
        ,paid_viewing
        ,phaseII_weight
        ,sum(phaseII_weight)over (order by paid_viewing
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_weighting
into cumulative_weights_table_paysov
from cumulative_weights_paysov
order by paid_viewing


------------------------------------------------------------------------------------------------------------------------------------


-------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------
-- Join cumulative weights to centiles

IF object_id('centile_table_paysov') IS NOT NULL DROP TABLE centile_table_paysov;

select a2.centile, a2.sample
        , min(a1.paid_viewing) as viewing
        , min(a1.cumulative_weighting) as cumultive_weighting
        , account_number
into centile_table_paysov
from cumulative_weights_table_paysov a1
inner join centile_weights a2
on a2.sample < a1.cumulative_weighting
group by a2.centile, a2.sample, account_number




select top 10 * from centile_table_paysov








-----------------QA-----------------------------------
--select paid_ntile,count(*) from paid_free2 group by paid_ntile order by paid_ntile
-- have been grouped correctly where 1 indicates a large proportion of paid viewing, and 100 a small or nill proportion of paid vieiwng
--
-- select distinct(base.account_number)
--         ,max(paid_ntile) as paid_ntile
--         into #temp
-- from sov_daily_records_capped base
-- left join paid_free2 pf
-- on pf.account_number = base.account_number
-- group by base.account_number
--
-- select paid_ntile, count(*) from #temp group by paid_ntile order by paid_ntile
--
-- select count(distinct account_number) from sov_daily_records_capped where account_number in (select account_number from #temp where paid_ntile is null)
--
--
-- select * from sov_daily_records_capped where account_number in (select account_number from #temp where paid_ntile is null) order by account_number
--

-- -- THEREWILL BE SOME ACCOUNTS THAT WILL NOT BE INCLUDED HERE AS they do not have paid/free indicators
-- -- or all viewing records within the viewing week have been deleted due to capping, or EPG vieiwngs and only (playback > 7 days)s viewing data remains
--
------------------------------------------------------

--------------------------------------------------------------------------------
-- G05 - OUTPUT: SOV by Deciles
--------------------------------------------------------------------------------


-- the code below create total viewing per percentile

select
pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))   as Playback_week
-- need to add total week

        , (Live_Monday) + (Vosdal_Monday) + (Playback_Monday) as consolidated_monday

        , (Live_tuesday) + (Vosdal_tuesday) + (Playback_tuesday) as consolidated_tuesday

        , (Live_wednesday) + (Vosdal_wednesday) + (Playback_wednesday) as consolidated_wednesday

        , (Live_thursday) + (Vosdal_thursday) + (Playback_thursday) as consolidated_thursday

        , (Live_friday) + (Vosdal_friday) + (Playback_friday) as consolidated_friday

        , (Live_saturday) + (Vosdal_saturday) + (Playback_saturday) as consolidated_saturday

        , (Live_sunday) + (Vosdal_sunday) + (Playback_sunday) as consolidated_sunday

        , (Live_Week) + (Vosdal_week) + (Playback_week) as consolidated_week

into #deciled_totals
from final_viewing2 base
inner join centile_table_paysov pf
on pf.account_number = base.account_number
group by pf.centile
--100 Row(s) affected

-- where pay_free_indicator = 'PAY TV'
select top 10 * from #deciled_totals
select * from #deciled_totals order by centile

-- the code below finds the total share of viewing of paid TV for each percentile



IF object_id('sov_paid') IS NOT NULL DROP TABLE sov_paid;



select  pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Monday else 0 end))
                                                                                                                                        as Live_Monday1
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Tuesday else 0 end))
                                                                                                                                        as Live_Tuesday1
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Wednesday else 0 end))
                                                                                                                                        as Live_Wednesday1
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Thursday else 0 end))
                                                                                                                                        as Live_Thursday1
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Friday else 0 end))
                                                                                                                                        as Live_Friday1
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Saturday else 0 end))
                                                                                                                                         as Live_Saturday1
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Sunday else 0 end))
                                                                                                                                        as Live_Sunday1
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Live_Week else 0 end))
                                                                                                                                        as Live_Week1
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Monday else 0 end))
                                                                                                                                        as Vosdal_Monday1
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Tuesday else 0 end))
                                                                                                                                        as Vosdal_Tuesday1
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Wednesday else 0 end))
                                                                                                                                        as Vosdal_Wednesday1
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Thursday else 0 end))
                                                                                                                                        as Vosdal_Thursday1
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Friday else 0 end))
                                                                                                                                        as Vosdal_Friday1
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Saturday else 0 end))
                                                                                                                                        as Vosdal_Saturday1
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_Sunday else 0 end))
                                                                                                                                        as Vosdal_Sunday1
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Vosdal_week else 0 end))
                                                                                                                                        as Vosdal_week1
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Monday else 0 end))
                                                                                                                                        as Playback_Monday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Tuesday else 0 end))
                                                                                                                                        as Playback_Tuesday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Wednesday else 0 end))
                                                                                                                                        as Playback_Wednesday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Thursday else 0 end))
                                                                                                                                        as Playback_Thursday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Friday else 0 end))
                                                                                                                                        as Playback_Friday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Saturday else 0 end))
                                                                                                                                        as Playback_Saturday1

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_Sunday else 0 end))
                                                                                                                                        as Playback_Sunday1

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else 0 end))
                                                                         /(max(case when pf.centile = dt.centile then dt.Playback_week else 0 end))
                                                                                                                                           as Playback_week1
-- need to add total week

        ,(sum(case when live = 1 and barb_day = 1 or vosdal = 1 and barb_day = 1  or playback = 1
                                 and program_air_datetime between (dateadd(day,0,@var_period_start_dt)) and (dateadd(day,1,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_monday else 0 end))
                                                                                                                        as consolidated_monday1

        ,(sum(case when live = 1 and barb_day = 2 or vosdal = 1 and barb_day = 2  or playback = 1
                                 and program_air_datetime between (dateadd(day,1,@var_period_start_dt)) and (dateadd(day,2,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_tuesday else 0 end))
                                                                                                                        as consolidated_tuesday1

        ,(sum(case when live = 1 and barb_day = 3 or vosdal = 1 and barb_day = 3  or playback = 1
                                 and program_air_datetime between (dateadd(day,2,@var_period_start_dt)) and (dateadd(day,3,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_wednesday else 0 end))
                                                                                                                        as consolidated_wednesday1

        ,(sum(case when live = 1 and barb_day = 4 or vosdal = 1 and barb_day = 4  or playback = 1
                                 and program_air_datetime between (dateadd(day,3,@var_period_start_dt)) and (dateadd(day,4,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_thursday else 0 end))
                                                                                                                        as consolidated_thursday1

        ,(sum(case when live = 1 and barb_day = 5 or vosdal = 1 and barb_day = 5  or playback = 1
                                 and program_air_datetime between (dateadd(day,4,@var_period_start_dt)) and (dateadd(day,5,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_friday else 0 end))
                                                                                                                        as consolidated_friday1

        ,(sum(case when live = 1 and barb_day = 6 or vosdal = 1 and barb_day = 6  or playback = 1
                                 and program_air_datetime between (dateadd(day,5,@var_period_start_dt)) and (dateadd(day,6,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_saturday else 0 end))
                                                                                                                        as consolidated_saturday1

        ,(sum(case when live = 1 and barb_day = 7 or vosdal = 1 and barb_day = 7  or playback = 1
                                 and program_air_datetime between (dateadd(day,6,@var_period_start_dt)) and (dateadd(day,7,@var_period_start_dt))
                                        then (cast(barb_minutes_weighted as float)) else 0 end))
                                             /(max(case when pf.centile = dt.centile then dt.consolidated_sunday else 0 end))
                                                                                                                        as consolidated_sunday1

        ,(sum(case when playback = 1 or live = 1 and barb_day in (1,2,3,4,5,6,7) or vosdal = 1 and barb_day in (1,2,3,4,5,6,7)
                        then (cast(barb_minutes_weighted as float)) else 0 end))
                        /(max(case when pf.centile = dt.centile then dt.consolidated_week else 0 end))          as consolidated_week1

into sov_paid
from final_viewing2 base
inner join centile_table_paysov pf
on pf.account_number = base.account_number
left join #deciled_totals as dt
on pf.centile = dt.centile
where pay_free_indicator = 'PAY TV'
group by pf.centile
--100 Row(s) affected


select * from sov_paid order by centile


-- add a column containing the average paid viewing for each week, split by live, vos, pb, and consolidated

alter table sov_paid
add (week_average_live as float
    ,week_average_vos as float
    ,week_average_plat as float
    ,week_average_consol as float)

update sov_paid
        set week_average_live   = (select live_week            from sov_paid_free where paid = 1)
            ,week_average_vos    = (select vosdal_week          from sov_paid_free where paid = 1)
            ,week_average_plat   = (select playback_week         from sov_paid_free where paid = 1)
            ,week_average_consol = (select consolidated_week     from sov_paid_free where paid = 1)



-- OUTPUT--
select * from sov_paid order by centile

---



/*---- QA ----


*/




------------------------------------------------------------------------------------------------------------------------------------------------

-- Show the viewing minutes (not share for the two deciles)

-- the percentiles based on share of paid viewing.


select
pf.centile
        --live--
        ,(sum(case when live = 1         and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Monday
        ,(sum(case when live = 1         and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Tuesday
        ,(sum(case when live = 1         and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Wednesday
        ,(sum(case when live = 1         and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Thursday
        ,(sum(case when live = 1         and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Friday
        ,(sum(case when live = 1         and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Saturday
        ,(sum(case when live = 1         and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Live_Sunday
        ,(sum(case when live = 1         and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Live_Week
        --Vosadal--
        ,(sum(case when vosdal = 1       and barb_day = 1                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Monday
        ,(sum(case when vosdal = 1       and barb_day = 2                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Tuesday
        ,(sum(case when vosdal = 1       and barb_day = 3                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Wednesday
        ,(sum(case when vosdal = 1       and barb_day = 4                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Thursday
        ,(sum(case when vosdal = 1       and barb_day = 5                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Friday
        ,(sum(case when vosdal = 1       and barb_day = 6                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Saturday
        ,(sum(case when vosdal = 1       and barb_day = 7                then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_Sunday
        ,(sum(case when vosdal = 1       and barb_day in (1,2,3,4,5,6,7) then (cast(barb_minutes_weighted as float)) else null end))   as Vosdal_week
        --playback--
        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,0,@var_period_start_dt))
                                                                        and (dateadd(day,1,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Monday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,1,@var_period_start_dt))
                                                                        and (dateadd(day,2,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Tuesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,2,@var_period_start_dt))
                                                                        and (dateadd(day,3,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Wednesday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,3,@var_period_start_dt))
                                                                        and (dateadd(day,4,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Thursday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,4,@var_period_start_dt))
                                                                        and (dateadd(day,5,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Friday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,5,@var_period_start_dt))
                                                                        and (dateadd(day,6,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Saturday

        ,(sum(case when playback = 1     and program_air_datetime        between (dateadd(day,6,@var_period_start_dt))
                                                                        and (dateadd(day,7,@var_period_start_dt))
                                                                        then (cast(barb_minutes_weighted as float)) else null end))   as Playback_Sunday

        ,(sum(case when playback = 1                                     then (cast(barb_minutes_weighted as float)) else null end))   as Playback_week
-- need to add total week

        , (Live_Monday) + (Vosdal_Monday) + (Playback_Monday) as consolidated_monday

        , (Live_tuesday) + (Vosdal_tuesday) + (Playback_tuesday) as consolidated_tuesday

        , (Live_wednesday) + (Vosdal_wednesday) + (Playback_wednesday) as consolidated_wednesday

        , (Live_thursday) + (Vosdal_thursday) + (Playback_thursday) as consolidated_thursday

        , (Live_friday) + (Vosdal_friday) + (Playback_friday) as consolidated_friday

        , (Live_saturday) + (Vosdal_saturday) + (Playback_saturday) as consolidated_saturday

        , (Live_sunday) + (Vosdal_sunday) + (Playback_sunday) as consolidated_sunday

        , (Live_Week) + (Vosdal_week) + (Playback_week) as consolidated_week

into percentile1_mins
from final_viewing2 base
inner join centile_table_paysov pf
on pf.account_number = base.account_number
where pay_free_indicator = 'PAY TV'
group by pf.centile
--100 Row(s) affected


select * from percentile1_mins order by centile


select * from #deciled_totals order by centile







----------admin:------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
--********************************************************************************************************************************************




select count(subscriber_id) from scaling_base_table_barb

---------------------------------admin


Grant select on sov_paid_total_viewing                        to public;
Grant select on sov_paid                     to public;
Grant select on centile_table_paysov                        to public; -- percentiles
Grant select on paid_free        to public;
Grant select on final_viewing2        to public;
Grant select on sov_daily_records4       to public;





grant select on sov_daily_records2 to public;
Grant select on sov_daily_records3 to public;
Grant select on sov_daily_records4 to public;
Grant select on one_week to public;
Grant select on genre to public;
Grant select on all_boxes_info to public;
Grant select on subs_details to public;
Grant select on ntiles_week to public;
Grant select on nt_4_19 to public;
Grant select on nt_20_3 to public;
Grant select on nt_lp to public;
Grant select on week_caps to public;
Grant select on h23_3 to public;
Grant select on pack_23_3 to public;
Grant select on h4_14 to public;
Grant select on pack_4_14 to public;
Grant select on h15_19 to public;
Grant select on pack_15_19 to public;
Grant select on h20_22 to public;
Grant select on pack_20_22 to public;
Grant select on lp to public;
Grant select on all_events to public;
Grant select on capped to public;
Grant select on uncapped to public;
Grant select on capped2 to public;
Grant select on    internal_capped_viewing    to public;
Grant select on    final_viewing2   to public;

-- percentile2_minutes
--         percentile1_mins
--         sov_paid_total_viewing
--         total_viewing
--         sov_paid
--         centile_table_paysov
--         paid_free
--         final_viewing2
--

grant select on sov_daily_records_new2 to public;
grant select on sov_daily_records_new3 to public;
grant select on sov_daily_records_new4 to public;
grant select on one_week_new to public;
grant select on genre_new to public;
grant select on all_boxes_info_new to public;
grant select on subs_details_new to public;
grant select on ntiles_week_new to public;
grant select on nt_4_19_new to public;
grant select on nt_20_3_new to public;
grant select on nt_lp_new to public;
grant select on week_caps_new to public;
grant select on h23_3_new to public;
grant select on h4_14_new to public;
grant select on h15_19_new to public;
grant select on h20_22_new to public;
grant select on lp_new to public;
grant select on all_events_new  to public;
grant select on uncapped_new  to public;
grant select on capped_new  to public;
grant select on capped_new2  to public;
grant select on capped_viewing_endpoints_new  to public;
Grant select on    internal_capped_viewing_new    to public;
Grant select on    final_viewing_new   to public;

