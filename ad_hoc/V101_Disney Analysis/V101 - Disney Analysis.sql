/*------------------------------------------------------------------------------
        Project: V099 - Sky Disney Analysis
        Version: 1
        Created: 20120919
        Lead: Sarah Moore
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
        Purpose
        -------
        To understand the impact on the customer base of losing the Sky Movies
        Indie channel

        SECTIONS
        --------

        PART A   - Extract Viewing data
             A01 - Viewing table for period Mon 10th May – Sun 10th June
             A03 - Clean data
             A03 - Scale
             A04 - Categorise Viewing

        PART B  - Output

        Tables
        -------
       
        Disney_viewing_table_20120919 - USing the Vespa_Daily_augs tables (which contains viewing with 28 days of Broadcast)
        Disney_viewing_table_over_28_day_playback (Using raw daily tables (uncapped) for viewing beyond 28 days of Broadcast

*/

--------------------------------------------------------------------------------
-- PART A SETUP - Extract Viewing data
--------------------------------------------------------------------------------

/*
PART A   - Extract Viewing data
     A01 - Viewing table for period Mon 10th May – Sun 10th June
     A03 - Clean data
     A03 - Scale
     A04 - Categorise Viewing
*/

CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;


-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-05-01';
SET @var_prog_period_end    = '2012-05-28';
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @var_num_days = 7;


select
      programme_trans_sk
      ,Channel_Name
      ,Epg_Title
      ,synopsis
      ,Genre_Description
      ,Sub_Genre_Description
      ,Tx_Start_Datetime_UTC
      ,Tx_End_Datetime_UTC
      ,datediff(mi,Tx_Start_Datetime_UTC,Tx_End_Datetime_UTC) as programme_duration
  into VESPA_Programmes -- drop table  VESPA_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc <= dateadd(day, 1, @var_prog_period_end) -- because @var_prog_period_end is a date and defaults to 00:00:00 when compared to datetimes
-- Add further filters to programmes here if required, eg, lower(channel_name) like '%bbc%'
   ;

commit;
create unique hg index idx1 on VESPA_Programmes(programme_trans_sk);
commit;
--select Tx_Start_Datetime_UTC , count(*) as records from VESPA_Programmes group by Tx_Start_Datetime_UTC order by Tx_Start_Datetime_UTC desc;
--select count(*) from  vespa_analysts.ph1_VESPA_DAILY_AUGS_20120515;
/*
--Average duration of a film

select avg(programme_duration) from VESPA_Programmes where Genre_Description = 'Movies'
*/

--------------------------------------------------------------- A01 - Viewing table for period Mon 10th May – Sun 10th June

-- A01 - Viewing table for period 10th-28th May
commit;

if object_id('Disney_viewing_table_dump') is not null drop table Disney_viewing_table_dump;
create table Disney_viewing_table_dump (
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
);

--select top 10 * from Disney_viewing_table_dump
commit;
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Disney_viewing_table_dump(
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
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join VESPA_Programmes as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @scanning_day = @var_prog_period_start;
--delete from Disney_viewing_table_dump;
commit;
while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

--------------------------------------------------------------- A02 - Clean data

-- A02 - Clean data

-- Clean viewing data



create table Disney_viewing_table_20120919 (
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

insert into Disney_viewing_table_20120919(
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
from Disney_viewing_table_dump
order by subscriber_id, viewing_starts, viewing_stops;

-- investigate rank > 1

delete from Disney_viewing_table_20120919 where rank > 1;
--24644 Row(s) affected
commit;

--select max(rank) from Disney_viewing_table_20120919;
/*
- Check records do not overlap...
- Check viewing duration is an int.
- Check sum of durations on one day per box is not greater than 1440

select top 100 subscriber_id, viewing_starts, viewing_ends
from Disney_viewing_table_20120919
*/

-- Universal channel name for hd channels (i.e. hd channel name = sd channel name)
Update Disney_viewing_table_20120919
set base.channel_name_inc_hd = ct.channel_name_inc_hd
from Disney_viewing_table_20120919 as base
        inner join vespa_analysts.channel_name_and_techedge_channel as ct on base.Channel_Name = ct.channel;

-- Update Programme durations
Update Disney_viewing_table_20120919
set base.programme_duration = ct.programme_duration
from Disney_viewing_table_20120919 as base
        inner join VESPA_Programmes as ct on base.programme_trans_sk = ct.programme_trans_sk;

/*
Delete from Disney_viewing_table_20120919
where Broadcast_date > '20120528';

Delete from Disney_viewing_table_20120919
where viewing_date > '20120604';
*/
-- delete viewing durations greater than programme_duration
/*
Update Disney_viewing_table_20120919
set viewing_duration = case when viewing_duration > programme_duration then programme_duration else viewing_duration end;
*/
--44,1926,227 Row(s) affected
-- Add indices

alter table Disney_viewing_table_20120919 add primary key (cb_row_ID);

create        index for_profiling_1 on Disney_viewing_table_20120919 (subscriber_id, viewing_starts); -- This one should be unique, but there are still duplicates in the raw data on the daily tables
create        index for_profiling_2 on Disney_viewing_table_20120919 (account_number);
create        index for_joining_1   on Disney_viewing_table_20120919 (scaling_segment_id);
create        index for_joining_2   on Disney_viewing_table_20120919 (programme_trans_sk);
create        index for_MBM         on Disney_viewing_table_20120919 (Channel_Name);

--------------------------------------------------------------- A02 - Scale

-- A02 - Scale

-- Use weights from mid point of analysis

Create variable @scaling_date date;
--set @scaling_date = dateadd(day,datediff(day,@var_prog_period_start,@var_prog_period_end)/2,@var_prog_period_start)

---Hard code @scaling_date as 14th May not fully populated
set @scaling_date = '2012-05-18'
--select @scaling_date;

update Disney_viewing_table_20120919 set scaling_segment_ID=null;
update Disney_viewing_table_20120919 set scaling_weighting=null;
commit;
 update Disney_viewing_table_20120919 as bas
     set  bas.scaling_date = @scaling_date
         ,bas.scaling_segment_ID = wei.scaling_segment_ID
    from vespa_analysts.ph1_scaling_dialback_intervals as wei
   where bas.account_number = wei.account_number
     and @scaling_date between wei.reporting_starts and wei.reporting_ends
; --1m

commit;

-- Find out the weight for that segment on that day
  update Disney_viewing_table_20120919 as bas
     set bas.scaling_weighting = wei.weighting
    from vespa_analysts.ph1_scaling_weightings as wei
   where bas.scaling_date     = wei.scaling_day
     and bas.scaling_segment_ID = wei.scaling_segment_ID
;
commit;
/*
select account_number, scaling_weighting
into temp -- drop table temp
from Disney_viewing_table_20120919
group by account_number, scaling_weighting;
*/


--------------------------------------------------------------- A02 - Categorise Viewing

-- A02 - Categorise Viewing

--HD Channel -- not necessary for this analysis

  update Disney_viewing_table_20120919
     set hd_channel = 1
   where channel_name like '%HD%'
;
--94222227 Row(s) affected
-- Pay Channel

  update Disney_viewing_table_20120919 as bas
     set Pay_channel = 1
    from vespa_analysts.channel_name_and_techedge_channel as det
   where det.Channel_Name_Inc_Hd in
          ('Animal Planet','Animal Plnt+1','Attheraces','BET','BET+1','Bio','Bio HD','Blighty','Bliss','Boomerang',
           'Boomerang +1','Bravo','Bravo Player','Bravo+1','Cartoon Network','Cartoonito','CBS Action','CBS Drama',
           'CBS Reality','CBS Reality +1','Challenge TV','Challenge TV+1','Channel AKA','Channel One','Channel One+1',
           'Chart Show TV','Clubland TV','CNTOO','Comedy Central','Comedy Central Extra','Comedy Central Extra+1',
           'ComedyCentral+1','Crime & Investigation','Crime & Investigation +1','Current TV','DanceNationTV','Dave ja vu',
           'Disc.Knowldge','Disc.knowledge +1','Disc.RealTime','Disc.RT+1','Disc.Sci+1','Disc.Science','Discovery',
           'Discovery Shed','Discovery Turbo','Discovery+1','Disney','Disney +1','Disney Cinemagic','Disney Cinemagic +1',
           'Disney Playhouse','Disney Playhouse+','Disney XD','Disney XD+1','Diva TV','Diva TV +1','DMAX','DMAX +1',
           'DMAX+2','E! Entertainment','Eden','Eden+1','ESPN','ESPN Classic','Euronews','Extreme Sports','Film24',
           'Flava','Food Network','Food Network+1','Fox FX','FX','FX+','Good Food','History','History +1','Home and Health',
           'Home&Health+','horror ch+1','horror channel','Horror Channel','Horse & Country TV','Kerrang','Kiss TV',
           'Kix!','Liverpool FCTV','LIVING','Living','LIVING +1','LIVING2','Living2+1','LIVINGit','LIVINGit+1','Magic',
           'MGM','Military History','Motors TV','Mov4Men2 +1','movies 24','Movies 24','Movies 24+1','Movies4Men +1',
           'Movies4Men.','Movies4Men2','MTV','MTV Base','MTV CLASSIC','MTV Dance','MTV Hits','MTV ROCKS','MTV SHOWS',
           'MTV+1','NatGeo + 1hr','NatGeo Wild','National Geo','Nick Jr','Nick Jr 2','Nick Toons','Nick Toonster',
           'Nickelodeon','Nickelodeon Replay','NME TV','N''Toons Replay','POP','Pop Girl','Pop Girl+1','Q Channel',
           'Quest','QUEST +1','Really','Scuzz NEW','Sky Arts 1','Sky Arts 2','Sky Movies Action & Adventure','Sky Movies Classics',
           'Sky Movies Comedy','Sky Movies Crime & Thriller','Sky Movies Drama & Romance','Sky Movies Family',
           'Sky Movies Indie','Sky Movies Modern','Sky Movies Modern Greats','Sky Movies Premiere','Sky Movies Premiere +1',
           'Sky Movies Sci-Fi / Horror','Sky Movies Sci-Fi/Horror','Sky Movies Showcase','Sky News','Sky News Enhanced',
           'Sky Sports 1','Sky Sports 2','Sky Sports 2','Sky Sports 4','Sky Sports News','Sky Spts News','Sky Thriller',
           'Sky Thriller HD','Sky1','Sky2','Sky2','Sky2+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
           'The Box','The Music Factory','The Vault','Tiny Pop','Tiny Pop +1','Travel','Travel & Living','Travel Ch +1',
           'True Ent','True Movies','True Movies 2','UKTV Alibi','UKTV Alibi+1','UKTV Dave','UKTV Dave+1','UKTV Food',
           'UKTV Food+1','UKTV G.O.L.D','UKTV G.O.L.D +1','UKTV GOLD','UKTV Style','UKTV Style Plus','Universal',
           'Universal +1','VH1','VH1 Classic','Vintage TV','Virgin 1','Virgin 1 +1','VIVA','Watch','Watch +1','Wedding TV',
           'wedding tv','Nat Geo','MTV Music','Sky Movies Mdn Greats','GOLD  (TV)','Sky DramaRom','Good Food +1',
           'Sky Living +1','Discovery +1hr','Premier Sports','Discovery RealTime +1','Nat Geo+1hr','Nick Replay',
           'Football First 4','Challenge','Football First 6','MUTV','Showcase','ESPN America','Chelsea TV','Alibi',
           'YeSTERDAY +1','Sky Movies Thriller','Eden +1','CineMoi Movies','Sky 1','Sky Living Loves','5* +1','Challenge +1',
           'Home+1','Home & Health +1','HD Retail Info','Home & Health','FX +','Disc. Shed','Discovery RealTime',
           'Sky Premiere','Sky Prem+1','Football First 7','Disney XD +1','Playhouse Disney','YeSTERDAY','Nat Geo Wild',
           'DMax','Home','HD MTV','Sky Movies Action','SBO','MGM HD','Animal Planet +1','Sky Box Office','TCM 2',
           'Sky Livingit +1','Dave','At The Races','History +1 hour','Sky 2D','horror channel +1','TCM','Anytime',
           'Comedy Central Extra +1','PopGirl+1','Smash Hits!','Nicktoons TV','Comedy Central +1','5*','Football First 2',
           'Alibi +1','MTV BASE','Sky Atlantic','Sky 2','MTV HITS','Disc. History','Disc. History+1','Sky Livingit',
           'Football First 2','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo')
        and bas.Channel_Name = det.Channel
;
--167026575 Row(s) affected
-- Categorise viewing events



Update Disney_viewing_table_20120919
set viewing_category =
        case when lower(av.channel_name) like '%sky indie%'  then 'A) Sky Movies Indie'
             when lower(av.channel_name) in     ('sky action'
                                                ,'sky action hd'
                                                ,'sky classics'
                                                ,'sky classics hd'
                                                ,'sky comedy'
                                                ,'sky comedy hd'
                                                ,'sky dramarom'
                                                ,'sky draromhd'
                                                ,'sky family'
                                                ,'sky family hd'
                                                ,'sky indie'
                                                ,'sky indie hd'
                                                ,'sky mdn greats'
                                                ,'sky mdngrtshd'
                                                ,'sky prem+1'
                                                ,'sky premiere'
                                                ,'sky scfi/horhd'
                                                ,'sky scfi/horror'
                                                ,'sky showcase'
                                                ,'sky thriller'
                                                ,'sky thriller hd'
                                                ,'skypremierehd'
                                                ,'skyshowcsehd')    then 'B) Non-Indie Sky Movies'
             when lower(av.channel_name) like '%sky sports%'        then 'C) Sky Sports'
             when av.pay_channel = 1                                then 'D) Other Pay TV'
                                                                    else 'E) Other Free TV' end
from Disney_viewing_table_20120919 as av
        inner join vespa_programmes as vp on av.programme_trans_sk = vp.programme_trans_sk;

--441926227 Row(s) affected

Update Disney_viewing_table_20120919
set niche_category =
        case when lower(av.channel_name) like '%sky indie%'  then 'A) Sky Movies Indie'
             when lower(av.channel_name) like '%sky classics%'  then 'B) Sky Movies Classics'
             when lower(av.channel_name) like '%sky mdn greats%' then 'C) Sky Modern Greats' else 'E) Other TV' end
from Disney_viewing_table_20120919 as av
        inner join vespa_programmes as vp on av.programme_trans_sk = vp.programme_trans_sk;
commit;

grant select on Disney_viewing_table_20120919 to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;

--441936337 Row(s) affected
/*
-----QA of Dataset------


--select count(*) from Disney_viewing_table_20120919;

--select top 500 * from Disney_viewing_table_20120919;

--where x_type_of_viewing_event = 'Sky+ time-shifted viewing event'
select count(*) as records ,sum(case when scaling_weighting is null then 1 else 0 end) as no_scaling
from  Disney_viewing_table_20120919
;


----Find out how many distinct days a box returns data for

select subscriber_id
,account_number
,count(distinct viewing_date) as distinct_viewing_dates
into #box_summary_dates
from Disney_viewing_table_20120919
group by subscriber_id
,account_number
;

select distinct_viewing_dates
,count(subscriber_id) as boxes
,count(account_number) as accounts
from #box_summary_dates
group by distinct_viewing_dates
order by distinct_viewing_dates
;
commit;



---Return Distinct boxes/accounts thatreturn data each day
select viewing_date
,count(distinct subscriber_id) as distinct_boxes
,count(distinct account_number) as distinct_accounts
from Disney_viewing_table_20120919
group by viewing_date
order by viewing_date
;

---Count by Broadcast date
select broadcast_date
,count(distinct subscriber_id) as distinct_boxes
,count(distinct account_number) as distinct_accounts
from Disney_viewing_table_20120919
group by broadcast_date
order by broadcast_date
;
*/

-------Run viewing for playback over 38 days post broadcast---

SET @var_prog_period_start  = '2012-05-01';
SET @var_prog_period_end    = '2012-05-28';

if object_id('Disney_viewing_table_over_28_day_playback') is not null drop table Disney_viewing_table_over_28_day_playback
create table Disney_viewing_table_over_28_day_playback (

Account_Number                 varchar(30)     not null
,Subscriber_Id                  decimal(8,0)    not null
,recorded_time_utc              datetime
,adjusted_event_start_time               datetime
,x_adjusted_event_end_time                 datetime
,x_Channel_Name                   varchar(30)
,x_Epg_Title                      varchar(50)
,x_programme_viewed_duration               integer
,Genre_Description              varchar(30)
,Sub_Genre_Description          varchar(30)
,Tx_Start_Datetime_UTC          datetime
,Tx_End_Datetime_UTC            datetime
,programme_trans_sk             bigint
);

--select top 10* from Disney_viewing_table_dump_over_38_day_playback
commit;
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Disney_viewing_table_over_28_day_playback

select vw.account_number
,vw.subscriber_id
,vw.recorded_time_utc
,vw.adjusted_event_start_time
,vw.x_adjusted_event_end_time
,vw.x_channel_name
,vw.x_epg_title
,vw.x_programme_viewed_duration
,prog.Genre_Description
,prog.Sub_Genre_Description
,prog.Tx_Start_Datetime_UTC
,prog.Tx_End_Datetime_UTC
,vw.programme_trans_sk
from 
sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
inner join VESPA_Programmes as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
where play_back_speed =2 and video_playing_flag = 1
and dateadd(dd,28,recorded_time_utc)<adjusted_event_start_time
and recorded_time_utc >=''2011-01-01''
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @scanning_day = @var_prog_period_start;
--delete from Disney_viewing_table_dump_over_38_day_playback;
commit;
while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

-----Categorise viewing from the over 28 days viewing-----
-- A03 - Categorise Viewing

--HD Channel -- not necessary for this analysis

alter table Disney_viewing_table_over_28_day_playback add hd_channel integer default 0;
alter table Disney_viewing_table_over_28_day_playback add pay_channel integer default 0;
alter table Disney_viewing_table_over_28_day_playback add viewing_category varchar(50);
alter table Disney_viewing_table_over_28_day_playback add channel_name_inc_hd varchar(40);
commit;

Update Disney_viewing_table_over_28_day_playback
set base.channel_name_inc_hd = ct.channel_name_inc_hd
from Disney_viewing_table_over_28_day_playback as base
        inner join vespa_analysts.channel_name_and_techedge_channel as ct on base.x_Channel_Name = ct.channel;

  update Disney_viewing_table_over_28_day_playback
     set hd_channel = 1
   where x_channel_name like '%HD%'
;
--94333337 Row(s) affected
-- Pay Channel
--select * from Disney_viewing_table_over_28_day_playback;
  update Disney_viewing_table_over_28_day_playback as bas
     set Pay_channel = 1
    from vespa_analysts.channel_name_and_techedge_channel as det
   where det.Channel_Name_Inc_Hd in
          ('Animal Planet','Animal Plnt+1','Attheraces','BET','BET+1','Bio','Bio HD','Blighty','Bliss','Boomerang',
           'Boomerang +1','Bravo','Bravo Player','Bravo+1','Cartoon Network','Cartoonito','CBS Action','CBS Drama',
           'CBS Reality','CBS Reality +1','Challenge TV','Challenge TV+1','Channel AKA','Channel One','Channel One+1',
           'Chart Show TV','Clubland TV','CNTOO','Comedy Central','Comedy Central Extra','Comedy Central Extra+1',
           'ComedyCentral+1','Crime & Investigation','Crime & Investigation +1','Current TV','DanceNationTV','Dave ja vu',
           'Disc.Knowldge','Disc.knowledge +1','Disc.RealTime','Disc.RT+1','Disc.Sci+1','Disc.Science','Discovery',
           'Discovery Shed','Discovery Turbo','Discovery+1','Disney','Disney +1','Disney Cinemagic','Disney Cinemagic +1',
           'Disney Playhouse','Disney Playhouse+','Disney XD','Disney XD+1','Diva TV','Diva TV +1','DMAX','DMAX +1',
           'DMAX+2','E! Entertainment','Eden','Eden+1','ESPN','ESPN Classic','Euronews','Extreme Sports','Film24',
           'Flava','Food Network','Food Network+1','Fox FX','FX','FX+','Good Food','History','History +1','Home and Health',
           'Home&Health+','horror ch+1','horror channel','Horror Channel','Horse & Country TV','Kerrang','Kiss TV',
           'Kix!','Liverpool FCTV','LIVING','Living','LIVING +1','LIVING2','Living2+1','LIVINGit','LIVINGit+1','Magic',
           'MGM','Military History','Motors TV','Mov4Men2 +1','movies 24','Movies 24','Movies 24+1','Movies4Men +1',
           'Movies4Men.','Movies4Men2','MTV','MTV Base','MTV CLASSIC','MTV Dance','MTV Hits','MTV ROCKS','MTV SHOWS',
           'MTV+1','NatGeo + 1hr','NatGeo Wild','National Geo','Nick Jr','Nick Jr 2','Nick Toons','Nick Toonster',
           'Nickelodeon','Nickelodeon Replay','NME TV','N''Toons Replay','POP','Pop Girl','Pop Girl+1','Q Channel',
           'Quest','QUEST +1','Really','Scuzz NEW','Sky Arts 1','Sky Arts 2','Sky Movies Action & Adventure','Sky Movies Classics',
           'Sky Movies Comedy','Sky Movies Crime & Thriller','Sky Movies Drama & Romance','Sky Movies Family',
           'Sky Movies Indie','Sky Movies Modern','Sky Movies Modern Greats','Sky Movies Premiere','Sky Movies Premiere +1',
           'Sky Movies Sci-Fi / Horror','Sky Movies Sci-Fi/Horror','Sky Movies Showcase','Sky News','Sky News Enhanced',
           'Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports News','Sky Spts News','Sky Thriller',
           'Sky Thriller HD','Sky1','Sky2','Sky3','Sky2+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
           'The Box','The Music Factory','The Vault','Tiny Pop','Tiny Pop +1','Travel','Travel & Living','Travel Ch +1',
           'True Ent','True Movies','True Movies 2','UKTV Alibi','UKTV Alibi+1','UKTV Dave','UKTV Dave+1','UKTV Food',
           'UKTV Food+1','UKTV G.O.L.D','UKTV G.O.L.D +1','UKTV GOLD','UKTV Style','UKTV Style Plus','Universal',
           'Universal +1','VH1','VH1 Classic','Vintage TV','Virgin 1','Virgin 1 +1','VIVA','Watch','Watch +1','Wedding TV',
           'wedding tv','Nat Geo','MTV Music','Sky Movies Mdn Greats','GOLD  (TV)','Sky DramaRom','Good Food +1',
           'Sky Living +1','Discovery +1hr','Premier Sports','Discovery RealTime +1','Nat Geo+1hr','Nick Replay',
           'Football First 4','Challenge','Football First 6','MUTV','Showcase','ESPN America','Chelsea TV','Alibi',
           'YeSTERDAY +1','Sky Movies Thriller','Eden +1','CineMoi Movies','Sky 1','Sky Living Loves','5* +1','Challenge +1',
           'Home+1','Home & Health +1','HD Retail Info','Home & Health','FX +','Disc. Shed','Discovery RealTime',
           'Sky Premiere','Sky Prem+1','Football First 7','Disney XD +1','Playhouse Disney','YeSTERDAY','Nat Geo Wild',
           'DMax','Home','HD MTV','Sky Movies Action','SBO','MGM HD','Animal Planet +1','Sky Box Office','TCM 2',
           'Sky Livingit +1','Dave','At The Races','History +1 hour','Sky 3D','horror channel +1','TCM','Anytime',
           'Comedy Central Extra +1','PopGirl+1','Smash Hits!','Nicktoons TV','Comedy Central +1','5*','Football First 2',
           'Alibi +1','MTV BASE','Sky Atlantic','Sky 2','MTV HITS','Disc. History','Disc. History+1','Sky Livingit',
           'Football First 2','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo')
        and bas.x_Channel_Name = det.Channel
;
--167036575 Row(s) affected
-- Categorise viewing events



Update Disney_viewing_table_over_28_day_playback
set viewing_category =
        case when lower(av.x_channel_name) like '%sky indie%'  then 'A) Sky Movies Indie'
             when lower(av.x_channel_name) in     ('sky action'
                                                ,'sky action hd'
                                                ,'sky classics'
                                                ,'sky classics hd'
                                                ,'sky comedy'
                                                ,'sky comedy hd'
                                                ,'sky dramarom'
                                                ,'sky draromhd'
                                                ,'sky family'
                                                ,'sky family hd'
                                                ,'sky indie'
                                                ,'sky indie hd'
                                                ,'sky mdn greats'
                                                ,'sky mdngrtshd'
                                                ,'sky prem+1'
                                                ,'sky premiere'
                                                ,'sky scfi/horhd'
                                                ,'sky scfi/horror'
                                                ,'sky showcase'
                                                ,'sky thriller'
                                                ,'sky thriller hd'
                                                ,'skypremierehd'
                                                ,'skyshowcsehd')    then 'B) Non-Indie Sky Movies'
             when lower(av.x_channel_name) like '%sky sports%'        then 'C) Sky Sports'
             when av.pay_channel = 1                                then 'D) Other Pay TV'
                                                                    else 'E) Other Free TV' end
from Disney_viewing_table_over_28_day_playback as av
        inner join vespa_programmes as vp on av.programme_trans_sk = vp.programme_trans_sk;
commit;



--select top 3000 * from Disney_viewing_table_over_28_day_playback;

---Create Summary by Programme by Box---
----select * into Disney_viewing_28_plus_days_from_playback_by_programmecopy from Disney_viewing_28_plus_days_from_playback_by_programme; commit;drop table Disney_viewing_28_plus_days_from_playback_by_programme;
select subscriber_id
,account_number
,x_channel_name
,x_epg_title
,Genre_Description
,Sub_Genre_Description
,Tx_Start_Datetime_UTC
,Tx_End_Datetime_UTC
,null as scaling_segment_id
,null as scaling_weighting
,@scaling_date as scaling_date
,0 as Pay_channel
,sum(x_programme_viewed_duration) as total_duration_viewed
,sum(case when dateadd(mm,1,recorded_time_utc)>adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_01_month_of_broadcast
,sum(case when dateadd(mm,1,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,2,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_01_to_02_months_of_broadcast

,sum(case when dateadd(mm,2,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,3,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_02_to_03_months_of_broadcast
,sum(case when dateadd(mm,3,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,4,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_03_to_04_months_of_broadcast
,sum(case when dateadd(mm,4,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,5,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_04_to_05_months_of_broadcast
,sum(case when dateadd(mm,5,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,6,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_05_to_06_months_of_broadcast
,sum(case when dateadd(mm,6,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,7,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_06_to_07_months_of_broadcast
,sum(case when dateadd(mm,7,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,8,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_07_to_08_months_of_broadcast
,sum(case when dateadd(mm,8,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,9,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_08_to_09_months_of_broadcast
,sum(case when dateadd(mm,9,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,10,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_09_to_10_months_of_broadcast
,sum(case when dateadd(mm,10,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,11,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_10_to_11_months_of_broadcast
,sum(case when dateadd(mm,11,recorded_time_utc)>adjusted_event_start_time then 0 when dateadd(mm,12,recorded_time_utc)>=adjusted_event_start_time then x_programme_viewed_duration else 0 end) as total_duration_viewed_within_11_to_12_months_of_broadcast

into Disney_viewing_28_plus_days_from_playback_by_programme
from  Disney_viewing_table_over_28_day_playback
group by subscriber_id
,account_number
,x_channel_name
,x_epg_title
,Genre_Description
,Sub_Genre_Description
,Tx_Start_Datetime_UTC
,Tx_End_Datetime_UTC
,pay_channel
;
commit;
--select top 500 * from Disney_viewing_28_plus_days_from_playback_by_programme;
--select top 500 * from Disney_viewing_table_over_28_day_playback;
--Commented out due to Sybase issue around alter table statements
--alter table Disney_viewing_28_plus_days_from_playback_by_programme add scaling_segment_ID bigint;
--alter table Disney_viewing_28_plus_days_from_playback_by_programme add scaling_weighting float;
--alter table Disney_viewing_28_plus_days_from_playback_by_programme add scaling_date date;
--alter table Disney_viewing_28_plus_days_from_playback_by_programme add scaling_date_test date;
--alter table Disney_viewing_28_plus_days_from_playback_by_programme add pay_channel bigint default 0;
commit;
 update Disney_viewing_28_plus_days_from_playback_by_programme as bas
     set  bas.scaling_date = @scaling_date
         ,bas.scaling_segment_ID = wei.scaling_segment_ID
    from vespa_analysts.ph1_scaling_dialback_intervals as wei
   where bas.account_number = wei.account_number
     and @scaling_date between wei.reporting_starts and wei.reporting_ends
; --1m
--select @scaling_date
commit;

-- Find out the weight for that segment on that day
  update Disney_viewing_28_plus_days_from_playback_by_programme as bas
     set bas.scaling_weighting = wei.weighting
    from vespa_analysts.ph1_scaling_weightings as wei
   where bas.scaling_date     = wei.scaling_day
     and bas.scaling_segment_ID = wei.scaling_segment_ID
;
commit;

--alter table Disney_viewing_28_plus_days_from_playback_by_programme delete pay_channel;


 update Disney_viewing_28_plus_days_from_playback_by_programme as bas
     set Pay_channel = 1
    from vespa_analysts.channel_name_and_techedge_channel as det
   where det.Channel_Name_Inc_Hd in
          ('Animal Planet','Animal Plnt+1','Attheraces','BET','BET+1','Bio','Bio HD','Blighty','Bliss','Boomerang',
           'Boomerang +1','Bravo','Bravo Player','Bravo+1','Cartoon Network','Cartoonito','CBS Action','CBS Drama',
           'CBS Reality','CBS Reality +1','Challenge TV','Challenge TV+1','Channel AKA','Channel One','Channel One+1',
           'Chart Show TV','Clubland TV','CNTOO','Comedy Central','Comedy Central Extra','Comedy Central Extra+1',
           'ComedyCentral+1','Crime & Investigation','Crime & Investigation +1','Current TV','DanceNationTV','Dave ja vu',
           'Disc.Knowldge','Disc.knowledge +1','Disc.RealTime','Disc.RT+1','Disc.Sci+1','Disc.Science','Discovery',
           'Discovery Shed','Discovery Turbo','Discovery+1','Disney','Disney +1','Disney Cinemagic','Disney Cinemagic +1',
           'Disney Playhouse','Disney Playhouse+','Disney XD','Disney XD+1','Diva TV','Diva TV +1','DMAX','DMAX +1',
           'DMAX+2','E! Entertainment','Eden','Eden+1','ESPN','ESPN Classic','Euronews','Extreme Sports','Film24',
           'Flava','Food Network','Food Network+1','Fox FX','FX','FX+','Good Food','History','History +1','Home and Health',
           'Home&Health+','horror ch+1','horror channel','Horror Channel','Horse & Country TV','Kerrang','Kiss TV',
           'Kix!','Liverpool FCTV','LIVING','Living','LIVING +1','LIVING2','Living2+1','LIVINGit','LIVINGit+1','Magic',
           'MGM','Military History','Motors TV','Mov4Men2 +1','movies 24','Movies 24','Movies 24+1','Movies4Men +1',
           'Movies4Men.','Movies4Men2','MTV','MTV Base','MTV CLASSIC','MTV Dance','MTV Hits','MTV ROCKS','MTV SHOWS',
           'MTV+1','NatGeo + 1hr','NatGeo Wild','National Geo','Nick Jr','Nick Jr 2','Nick Toons','Nick Toonster',
           'Nickelodeon','Nickelodeon Replay','NME TV','N''Toons Replay','POP','Pop Girl','Pop Girl+1','Q Channel',
           'Quest','QUEST +1','Really','Scuzz NEW','Sky Arts 1','Sky Arts 2','Sky Movies Action & Adventure','Sky Movies Classics',
           'Sky Movies Comedy','Sky Movies Crime & Thriller','Sky Movies Drama & Romance','Sky Movies Family',
           'Sky Movies Indie','Sky Movies Modern','Sky Movies Modern Greats','Sky Movies Premiere','Sky Movies Premiere +1',
           'Sky Movies Sci-Fi / Horror','Sky Movies Sci-Fi/Horror','Sky Movies Showcase','Sky News','Sky News Enhanced',
           'Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports News','Sky Spts News','Sky Thriller',
           'Sky Thriller HD','Sky1','Sky2','Sky3','Sky2+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
           'The Box','The Music Factory','The Vault','Tiny Pop','Tiny Pop +1','Travel','Travel & Living','Travel Ch +1',
           'True Ent','True Movies','True Movies 2','UKTV Alibi','UKTV Alibi+1','UKTV Dave','UKTV Dave+1','UKTV Food',
           'UKTV Food+1','UKTV G.O.L.D','UKTV G.O.L.D +1','UKTV GOLD','UKTV Style','UKTV Style Plus','Universal',
           'Universal +1','VH1','VH1 Classic','Vintage TV','Virgin 1','Virgin 1 +1','VIVA','Watch','Watch +1','Wedding TV',
           'wedding tv','Nat Geo','MTV Music','Sky Movies Mdn Greats','GOLD  (TV)','Sky DramaRom','Good Food +1',
           'Sky Living +1','Discovery +1hr','Premier Sports','Discovery RealTime +1','Nat Geo+1hr','Nick Replay',
           'Football First 4','Challenge','Football First 6','MUTV','Showcase','ESPN America','Chelsea TV','Alibi',
           'YeSTERDAY +1','Sky Movies Thriller','Eden +1','CineMoi Movies','Sky 1','Sky Living Loves','5* +1','Challenge +1',
           'Home+1','Home & Health +1','HD Retail Info','Home & Health','FX +','Disc. Shed','Discovery RealTime',
           'Sky Premiere','Sky Prem+1','Football First 7','Disney XD +1','Playhouse Disney','YeSTERDAY','Nat Geo Wild',
           'DMax','Home','HD MTV','Sky Movies Action','SBO','MGM HD','Animal Planet +1','Sky Box Office','TCM 2',
           'Sky Livingit +1','Dave','At The Races','History +1 hour','Sky 3D','horror channel +1','TCM','Anytime',
           'Comedy Central Extra +1','PopGirl+1','Smash Hits!','Nicktoons TV','Comedy Central +1','5*','Football First 2',
           'Alibi +1','MTV BASE','Sky Atlantic','Sky 2','MTV HITS','Disc. History','Disc. History+1','Sky Livingit',
           'Football First 2','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo')
        and bas.x_Channel_Name = det.Channel
;

commit;
--select top 500 * from Disney_viewing_28_plus_days_from_playback_by_programme;
--select x_channel_name, count(*), sum(case when scaling_weighting is  null then 1 else 0 end) as not_null_values from Disney_viewing_28_plus_days_from_playback_by_programme group by x_channel_name order by x_channel_name;

if object_id('Disney_viewing_table_dump') is not null drop table Disney_viewing_table_dump;

---Base Table--

---All Subscribers who have a playback event (where they having a scaling figure)--

----create list of all movies viewed to attribute if they are Disney or not---

select Epg_Title
,count(distinct subscriber_id) as boxes
from Disney_viewing_table_30130919
where genre_description='Movies'
group by Epg_Title
order by boxes desc
; 

select x_epg_title
,count(distinct subscriber_id) as boxes
from Disney_viewing_28_plus_days_from_playback_by_programme
where genre_description='Movies'
group by x_epg_title
order by boxes desc
; 
commit;



---        PART B  - Output


--select genre_description , count(*) as records from  Disney_viewing_table_30130919 group by genre_description;
--drop table Disney_viewing_box_account_up_to_28_days_post_broadcast;
select 
account_number
,scaling_weighting
,max(case when recorded_time_utc is not null then 1 else 0 end) as box_with_playback_activity
,max(case when recorded_time_utc is not null and genre_description = 'Movies' then 1 else 0 end) as box_with_movies_playback_activity
,max(case when recorded_time_utc is not null and genre_description = 'Movies' and channel_name in (
'Disney Cine'
,'Disney Cine HD'
,'Disney Cine+1'
,'Sky Classics'
,'Sky Classics HD'
,'Sky Comedy'
,'Sky Comedy HD'
,'Sky DraRomHD'
,'Sky DramaRom'
,'Sky Family'
,'Sky Family HD'
,'Sky Indie'
,'Sky Indie HD'
,'Sky Mdn Greats'
,'Sky MdnGrtsHD'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ScFi/HorHD'
,'Sky ScFi/Horror'
,'Sky Showcase'
,'Sky Thriller'
,'Sky Thriller HD'
,'SkyPremiereHD'
,'SkyShowcaseHD'
,'SkyShowcseHD'
)
 
then 1 else 0 end) as box_with_movies_playback_activity_on_sky_movies_or_cinemagic
,max(case when recorded_time_utc is not null and lower(epg_title) in 
(
'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) then 1 else 0 end) as disney_film_viewed

,max(case when  recorded_time_utc is not null and lower(epg_title) in 
(
'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and channel_name in (
'Disney Cine'
,'Disney Cine HD'
,'Disney Cine+1'
,'Sky Classics'
,'Sky Classics HD'
,'Sky Comedy'
,'Sky Comedy HD'
,'Sky DraRomHD'
,'Sky DramaRom'
,'Sky Family'
,'Sky Family HD'
,'Sky Indie'
,'Sky Indie HD'
,'Sky Mdn Greats'
,'Sky MdnGrtsHD'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ScFi/HorHD'
,'Sky ScFi/Horror'
,'Sky Showcase'
,'Sky Thriller'
,'Sky Thriller HD'
,'SkyPremiereHD'
,'SkyShowcaseHD'
,'SkyShowcseHD'
) then 1 else 0 end) as disney_film_viewed_on_sky_movies_disney_cinemagic

---Add in Pay/Free Split for Disney

,max(case when  recorded_time_utc is not null and lower(epg_title) in 
(

'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and Pay_Channel =1
 then 1 else 0 end) as disney_film_viewed_on_pay_channel


,max(case when  recorded_time_utc is not null and lower(epg_title) in 
(

'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and Pay_Channel =0
 then 1 else 0 end) as disney_film_viewed_on_free_channel


--,max(case when recorded_time_utc is not null and genre_description = 'Movies' then 1 else 0 end) as box_with_a
into Disney_viewing_box_account_up_to_28_days_post_broadcast
from Disney_viewing_table_30130919
where scaling_weighting is not null 
group by account_number
,scaling_weighting
;

/*
select top 500 * from Disney_viewing_box_account_up_to_28_days_post_broadcast;
*/
select sum(box_with_playback_activity*scaling_weighting) as playback_accounts
,sum(box_with_movies_playback_activity*scaling_weighting) as movies_accounts
,sum(box_with_movies_playback_activity_on_sky_movies_or_cinemagic*scaling_weighting) as sky_movies_or_cinemagic_accounts
,sum(disney_film_viewed*scaling_weighting) as disney_movie_accounts
,sum(disney_film_viewed_on_sky_movies_disney_cinemagic*scaling_weighting) as disney_movie_on_sky_movies_or_cinemagic_accounts
,sum(disney_film_viewed_on_free_channel*scaling_weighting) as disney_movie_on_free_channel
,sum(disney_film_viewed_on_pay_channel*scaling_weighting) as disney_movie_on_pay_channel
from Disney_viewing_box_account_up_to_28_days_post_broadcast


--Repeat for Post 28 Day Playback---

--drop table Disney_viewing_box_account_over_6_months_post_broadcast;
select 
account_number
,scaling_weighting
,max(case when account_number is not null then 1 else 0 end) as box_with_playback_activity
,max(case when genre_description = 'Movies' then 1 else 0 end) as box_with_movies_playback_activity
,max(case when genre_description = 'Movies' and x_channel_name in (
'Disney Cine'
,'Disney Cine HD'
,'Disney Cine+1'
,'Sky Classics'
,'Sky Classics HD'
,'Sky Comedy'
,'Sky Comedy HD'
,'Sky DraRomHD'
,'Sky DramaRom'
,'Sky Family'
,'Sky Family HD'
,'Sky Indie'
,'Sky Indie HD'
,'Sky Mdn Greats'
,'Sky MdnGrtsHD'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ScFi/HorHD'
,'Sky ScFi/Horror'
,'Sky Showcase'
,'Sky Thriller'
,'Sky Thriller HD'
,'SkyPremiereHD'
,'SkyShowcaseHD'
,'SkyShowcseHD'
)
 
then 1 else 0 end) as box_with_movies_playback_activity_on_sky_movies_or_cinemagic
,max(case when  lower(x_epg_title) in 
(
'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) then 1 else 0 end) as disney_film_viewed

,max(case when lower(x_epg_title) in 
(
'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and x_channel_name in (
'Disney Cine'
,'Disney Cine HD'
,'Disney Cine+1'
,'Sky Classics'
,'Sky Classics HD'
,'Sky Comedy'
,'Sky Comedy HD'
,'Sky DraRomHD'
,'Sky DramaRom'
,'Sky Family'
,'Sky Family HD'
,'Sky Indie'
,'Sky Indie HD'
,'Sky Mdn Greats'
,'Sky MdnGrtsHD'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ScFi/HorHD'
,'Sky ScFi/Horror'
,'Sky Showcase'
,'Sky Thriller'
,'Sky Thriller HD'
,'SkyPremiereHD'
,'SkyShowcaseHD'
,'SkyShowcseHD'
) then 1 else 0 end) as disney_film_viewed_on_sky_movies_disney_cinemagic

---Add in Disney Pay/Free Splits---
,max(case when  lower(x_epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and Pay_Channel =0 then 1 else 0 end) as disney_film_viewed_free_channel

,max(case when  lower(x_epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and Pay_Channel =1 then 1 else 0 end) as disney_film_viewed_pay_channel




--,max(case when recorded_time_utc is not null and genre_description = 'Movies' then 1 else 0 end) as box_with_a
into Disney_viewing_box_account_over_6_months_post_broadcast
from Disney_viewing_28_plus_days_from_playback_by_programme
where scaling_weighting is not null and total_duration_viewed_6_plus_months_after_broadcast>0
group by account_number
,scaling_weighting
;

commit;



select top 500 * from Disney_viewing_box_account_over_6_months_post_broadcast;
select top 500 * from Disney_viewing_28_plus_days_from_playback_by_programme;

select sum(box_with_playback_activity*scaling_weighting) as playback_accounts
,sum(box_with_movies_playback_activity*scaling_weighting) as movies_accounts
,sum(box_with_movies_playback_activity_on_sky_movies_or_cinemagic*scaling_weighting) as sky_movies_or_cinemagic_accounts
,sum(disney_film_viewed*scaling_weighting) as disney_movie_accounts
,sum(disney_film_viewed_on_sky_movies_disney_cinemagic*scaling_weighting) as disney_movie_on_sky_movies_or_cinemagic_accounts
,sum(disney_film_viewed_free_channel*scaling_weighting) as disney_film_viewed_on_free_channel
,sum(disney_film_viewed_pay_channel*scaling_weighting) as disney_film_viewed_on_pay_channel
from Disney_viewing_box_account_over_6_months_post_broadcast
;

--select top 100 *  from Disney_viewing_box_account_over_6_months_post_broadcast
;



select x_channel_name , x_epg_title   , count(distinct account_number) as accounts
from Disney_viewing_28_plus_days_from_playback_by_programme
where scaling_weighting is not null and total_duration_viewed_6_plus_months_after_broadcast>0
group by x_channel_name , x_epg_title 
order by accounts desc
--select count(*) from Disney_viewing_box_account_over_6_months_post_broadcast;






---Summary by Programme----
select x_channel_name , x_epg_title
,Tx_Start_Datetime_UTC          
,sum(case when  lower(x_epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) then scaling_weighting else 0 end) as disney_film_viewed

---Add Free/Pay Split for Disney viewing


,sum(scaling_weighting) as total_viewed
,sum(total_duration_viewed*scaling_weighting) as total_seconds_viewed
from Disney_viewing_28_plus_days_from_playback_by_programme
where scaling_weighting is not null and total_duration_viewed_6_plus_months_after_broadcast>0 and x_channel_name is not null
group by x_channel_name , x_epg_title,Tx_Start_Datetime_UTC          
order by total_viewed desc
;

commit;
--select top 100 *  from Disney_viewing_28_plus_days_from_playback_by_programme where x_epg_title = 'Daddy Day Camp';

---Pt3 Total Viewing by Time since Broadcast---
--<1mth,1-3mth etc

----Within 28 Days Activity
--select top 100 * from Disney_viewing_table_20120919;
--select top 100 * from Disney_viewing_table_20120919;

select 
sum(datediff(ss,viewing_starts,viewing_stops)*scaling_weighting) as _01_month 
from Disney_viewing_table_20120919

where x_type_of_viewing_event = 'Sky+ time-shifted viewing event' and scaling_weighting is not null
and dateadd(dd,28,recorded_time_utc)>=event_start_time
;


---Over 28 Days Activity
select  sum(total_duration_viewed_within_01_month_of_broadcast*scaling_weighting) as _01_month
,sum(total_duration_viewed_within_01_to_02_months_of_broadcast*scaling_weighting) as _02_month
,sum(total_duration_viewed_within_02_to_03_months_of_broadcast*scaling_weighting) as _03_month
,sum(total_duration_viewed_within_03_to_04_months_of_broadcast*scaling_weighting) as _04_month
,sum(total_duration_viewed_within_04_to_05_months_of_broadcast*scaling_weighting) as _05_month
,sum(total_duration_viewed_within_05_to_06_months_of_broadcast*scaling_weighting) as _06_month
,sum(total_duration_viewed_within_06_to_07_months_of_broadcast*scaling_weighting) as _07_month
,sum(total_duration_viewed_within_07_to_08_months_of_broadcast*scaling_weighting) as _08_month
,sum(total_duration_viewed_within_08_to_09_months_of_broadcast*scaling_weighting) as _09_month
,sum(total_duration_viewed_within_09_to_10_months_of_broadcast*scaling_weighting) as _10_month
,sum(total_duration_viewed_within_10_to_11_months_of_broadcast*scaling_weighting) as _11_month
,sum(total_duration_viewed_within_11_to_12_months_of_broadcast*scaling_weighting) as _12_month

from Disney_viewing_28_plus_days_from_playback_by_programme
where scaling_weighting is not null
;

--Repeat but add in splits for Disney Films
select case when   lower(epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) then '01: Disney Movie' when Genre_Description = 'Movies' then '02: Other Movie' else '03: Other programme' end as programme_type
,case when  Pay_Channel =1 then '01: Pay Channel' else '02: Free Channel' end as channel_type
,sum(datediff(ss,viewing_starts,viewing_stops)*scaling_weighting) as _01_month 
from Disney_viewing_table_20120919
where x_type_of_viewing_event = 'Sky+ time-shifted viewing event' and scaling_weighting is not null
and dateadd(dd,28,recorded_time_utc)>=event_start_time
group by programme_type
,channel_type
;

---repeat for Over 28 Viewing---
select case when   lower(x_epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) then '01: Disney Movie' when Genre_Description = 'Movies' then '02: Other Movie' else '03: Other programme' end as programme_type
,case when  Pay_Channel =1 then '01: Pay Channel' else '02: Free Channel' end as channel_type
,sum(total_duration_viewed_within_01_month_of_broadcast*scaling_weighting) as _01_month
,sum(total_duration_viewed_within_01_to_02_months_of_broadcast*scaling_weighting) as _02_month
,sum(total_duration_viewed_within_02_to_03_months_of_broadcast*scaling_weighting) as _03_month
,sum(total_duration_viewed_within_03_to_04_months_of_broadcast*scaling_weighting) as _04_month
,sum(total_duration_viewed_within_04_to_05_months_of_broadcast*scaling_weighting) as _05_month
,sum(total_duration_viewed_within_05_to_06_months_of_broadcast*scaling_weighting) as _06_month
,sum(total_duration_viewed_within_06_to_07_months_of_broadcast*scaling_weighting) as _07_month
,sum(total_duration_viewed_within_07_to_08_months_of_broadcast*scaling_weighting) as _08_month
,sum(total_duration_viewed_within_08_to_09_months_of_broadcast*scaling_weighting) as _09_month
,sum(total_duration_viewed_within_09_to_10_months_of_broadcast*scaling_weighting) as _10_month
,sum(total_duration_viewed_within_10_to_11_months_of_broadcast*scaling_weighting) as _11_month
,sum(total_duration_viewed_within_11_to_12_months_of_broadcast*scaling_weighting) as _12_month

from Disney_viewing_28_plus_days_from_playback_by_programme
where scaling_weighting is not null
group by programme_type
,channel_type
order by programme_type
,channel_type
;
commit;

---Follow Up 2nd Nov 2012----
---request from SPG (Via Lisa Payne)
--Out of the 2% who watch Disney movies 6 months after recording, what was the average # of titles watched post 6 months?---




--drop table Disney_viewing_box_account_over_6_months_post_broadcast_by_disney_film;

select 
account_number
,scaling_weighting
,max(case when account_number is not null then 1 else 0 end) as box_with_playback_activity
,max(case when genre_description = 'Movies' then 1 else 0 end) as box_with_movies_playback_activity
,max(case when genre_description = 'Movies' and x_channel_name in (
'Disney Cine'
,'Disney Cine HD'
,'Disney Cine+1'
,'Sky Classics'
,'Sky Classics HD'
,'Sky Comedy'
,'Sky Comedy HD'
,'Sky DraRomHD'
,'Sky DramaRom'
,'Sky Family'
,'Sky Family HD'
,'Sky Indie'
,'Sky Indie HD'
,'Sky Mdn Greats'
,'Sky MdnGrtsHD'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ScFi/HorHD'
,'Sky ScFi/Horror'
,'Sky Showcase'
,'Sky Thriller'
,'Sky Thriller HD'
,'SkyPremiereHD'
,'SkyShowcaseHD'
,'SkyShowcseHD'
)
 
then 1 else 0 end) as box_with_movies_playback_activity_on_sky_movies_or_cinemagic
,max(case when  lower(x_epg_title) in 
(
'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) then 1 else 0 end) as disney_film_viewed

,max(case when lower(x_epg_title) in 
(
'101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and x_channel_name in (
'Disney Cine'
,'Disney Cine HD'
,'Disney Cine+1'
,'Sky Classics'
,'Sky Classics HD'
,'Sky Comedy'
,'Sky Comedy HD'
,'Sky DraRomHD'
,'Sky DramaRom'
,'Sky Family'
,'Sky Family HD'
,'Sky Indie'
,'Sky Indie HD'
,'Sky Mdn Greats'
,'Sky MdnGrtsHD'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ScFi/HorHD'
,'Sky ScFi/Horror'
,'Sky Showcase'
,'Sky Thriller'
,'Sky Thriller HD'
,'SkyPremiereHD'
,'SkyShowcaseHD'
,'SkyShowcseHD'
) then 1 else 0 end) as disney_film_viewed_on_sky_movies_disney_cinemagic

---Add in Disney Pay/Free Splits---
,max(case when  lower(x_epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and Pay_Channel =0 then 1 else 0 end) as disney_film_viewed_free_channel

,max(case when  lower(x_epg_title) in 
('101 dalmatians'
,'102 dalmatians'
,'a bug''s life'
,'addams family reunion'
,'adventureland'
,'aladdin & the king of thieves'
,'alice in wonderland'
,'alice in wonderland 3d'
,'an extremely goofy movie'
,'annie'
,'armageddon'
,'atlantis 2: milo''s return'
,'atlantis: the lost empire'
,'bambi'
,'bolt'
,'bridge to terabithia'
,'brink!'
,'brother bear'
,'brother bear 2'
,'bruce almighty'
,'calendar girls'
,'cars 2'
,'chicken little'
,'chronicles of narnia...'
,'chronicles of narnia: the lion,'
,'chronicles of narnia: the lion...'
,'cinderella'
,'city of god'
,'clerks'
,'cocktail'
,'cool runnings'
,'coyote ugly'
,'crazy/beautiful'
,'crimson tide'
,'d2: the mighty ducks'
,'d3: the mighty ducks'
,'deja vu'
,'disney''s a christmas carol'
,'disney''s pocahontas ii'
,'ducktales: the movie'
,'dumbo'
,'eight below'
,'enchanted'
,'enemy of the state'
,'everybody''s fine'
,'evita'
,'finding nemo'
,'flightplan'
,'fright night'
,'from dusk till dawn'
,'genius'
,'george of the jungle'
,'george of the jungle 2'
,'g-force'
,'gone in 60 seconds'
,'good will hunting'
,'halloween: h20'
,'hannah montana: the movie'
,'h-e double hockey sticks'
,'herbie goes bananas'
,'herbie goes to monte carlo'
,'herbie rides again'
,'hercules'
,'high heels and low lifes'
,'holes'
,'homeward bound'
,'homeward bound:the incredible journey'
,'homeward bound:the incredible...'
,'horse sense'
,'hounded'
,'i am number four'
,'ice princess'
,'insomnia'
,'inspector gadget'
,'inspector gadget 2'
,'iron will'
,'jackie brown'
,'jay & silent bob strike back'
,'judge dredd'
,'jumping ship'
,'jungle 2 jungle'
,'kid in king arthur''s court'
,'kronk''s new groove'
,'lady & the tramp 2'
,'leroy & stitch'
,'life is beautiful'
,'lilo & stitch'
,'lilo & stitch 2: stitch has a glitch'
,'lilo & stitch 2: stitch has a...'
,'lilo & stitch 2:...'
,'lion king 2: simba''s pride'
,'lion king 3: hakuna matata'
,'mars needs moms'
,'mary poppins'
,'max keeble''s big move'
,'meet the robinsons'
,'mickey, donald, goofy: the…'
,'mickey''s twice upon a christmas'
,'miracle in lane two'
,'monsters, inc'
,'motocrossed'
,'mr magoo'
,'mulan'
,'mulan 2'
,'nancy drew'
,'old dogs'
,'operation dumbo drop'
,'peter pan in return to neverland'
,'phantom of the megaplex'
,'pirates of the caribbean'
,'pirates of the caribbean...'
,'pirates of the caribbean: at...'
,'pirates of the caribbean: curse...'
,'pirates of the caribbean: dead...'
,'pirates of the caribbean: on...'
,'pirates of the caribbean: the...'
,'pocahontas'
,'pocahontas ii: journey to a...'
,'pretty woman'
,'prince of persia: the sands...'
,'prom'
,'pulp fiction'
,'quints'
,'race to witch mountain'
,'rebecca'
,'reign of fire'
,'return to oz'
,'right on track'
,'robin hood'
,'rushmore'
,'scream'
,'scream 2'
,'scream 3'
,'secretariat'
,'signs'
,'sister act 2: back in the habit'
,'sky high'
,'snake eyes'
,'snow dogs'
,'space buddies'
,'spy kids'
,'spy kids 2: island of lost...'
,'spy kids 2: the island of lost...'
,'stitch! the movie'
,'surrogates'
,'tangled'
,'tarzan'
,'tarzan 2'
,'the 13th warrior'
,'the aristocats'
,'the black cauldron'
,'the boys are back'
,'the count of monte cristo'
,'the crimson wing'
,'the emperor''s new groove'
,'the english patient'
,'the faculty'
,'the fox and the hound'
,'the fox and the hound 2'
,'the horse whisperer'
,'the hunchback of notre dame ii'
,'the incredibles'
,'the jungle book 2'
,'the kid'
,'the lady & the tramp'
,'the last song'
,'the legend of drunken master'
,'the lion king'
,'the many advs of winnie the pooh'
,'the mighty ducks'
,'the muppets wizard of oz'
,'the other me'
,'the princess and the frog'
,'the proposal'
,'the rescuers'
,'the rescuers down under'
,'the return of jafar'
,'the rookie'
,'the royal tenenbaums'
,'the search for santa paws'
,'the sorcerer''s apprentice'
,'the talented mr. ripley'
,'the tempest'
,'the three caballeros'
,'the tigger movie'
,'the waterboy'
,'the wild'
,'three men and a baby'
,'tinker bell'
,'tinker bell and the great fairy...'
,'tinker bell and the lost treasure'
,'tinkerbell'
,'tinkerbell: the great fairy rescue'
,'tinkerbell: the lost treasure'
,'toy story 2'
,'toy story 3'
,'treasure island'
,'treasure planet'
,'tron'
,'tron legacy'
,'unbreakable'
,'up'
,'when in rome'
,'white fang'
,'winnie the pooh'
,'winnie the pooh''s most grand...'
,'you again'
,'you lucky dog'
) and Pay_Channel =1 then 1 else 0 end) as disney_film_viewed_pay_channel

,lower(x_epg_title) as film_title


--,max(case when recorded_time_utc is not null and genre_description = 'Movies' then 1 else 0 end) as box_with_a
into Disney_viewing_box_account_over_6_months_post_broadcast_by_disney_film
from Disney_viewing_28_plus_days_from_playback_by_programme
where scaling_weighting is not null and 
dateadd(mm,6,recorded_time_utc)>adjusted_event_start_time
/*
    (   total_duration_viewed_within_06_to_07_months_of_broadcast+
        total_duration_viewed_within_07_to_08_months_of_broadcast+
        total_duration_viewed_within_08_to_09_months_of_broadcast+
        total_duration_viewed_within_09_to_10_months_of_broadcast+
        total_duration_viewed_within_10_to_11_months_of_broadcast+
        total_duration_viewed_within_11_to_12_months_of_broadcast)>0 
*/
group by account_number
,scaling_weighting
,film_title
having  disney_film_viewed>0 
;
--select top 100 * from Disney_viewing_28_plus_days_from_playback_by_programme
select account_number
,scaling_weighting
,count(distinct film_title) as distinct_films
into #summary_by_account
from Disney_viewing_box_account_over_6_months_post_broadcast_by_disney_film
group by account_number
,scaling_weighting
;

select distinct_films
,sum(scaling_weighting)
from #summary_by_account
group by distinct_films
order by distinct_films
;

