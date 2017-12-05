-- So we're interested in cases where boxes (usually HD) showing playback
-- of sequential episodes in the same series show significant dropoffs in
-- viewing in the transition into subsequent episodes, and we want to find
-- out why. Refer also to:
--      http://rtci/Lists/RTCI%20IC%20dev/DispForm.aspx?ID=33
--
-- So this is a one-off guy which we're still turning into a stored procedure
-- just so we can add it to the queue overnight. Not that much is going on in
-- the server right now, the thing is quite unhapily unresponsive.
--

drop procedure V033_assemble_sample_data;
create procedure V033_assemble_sample_data
as
begin

declare @pull_log_ID integer

EXECUTE citeam.logger_create_run 'AdHocDataPull', convert(varchar(10),today(),123) || ' V033 Viewing pull', @pull_log_ID output

IF object_id('V033_programme_listings') IS NOT NULL
    DROP TABLE V033_programme_listings
IF object_id('V033_viewing_cues') IS NOT NULL
    DROP TABLE V033_viewing_cues
IF object_id('V033_viewing_cb_keys') IS NOT NULL
    DROP TABLE V033_viewing_cb_keys
IF object_id('V033_Viewing_Records') IS NOT NULL
    DROP TABLE V033_Viewing_Records

select
        programme_trans_sk
--        ,epg_title
--        ,channel_name
--        ,tx_start_datetime_utc
--        ,tx_end_datetime_utc
into stafforr.V033_programme_listings
from  sk_prod.Vespa_EPG_Dim
where epg_title = 'Futurama'
and tx_start_datetime_utc >= '2011-07-22'
and tx_start_datetime_utc < '2011-08-27'
and channel_name in ('Sky1', 'Sky 1', 'Sky1 HD')    -- Only take Sky1 and Sky1HD viewing
and series_id = '249892'
order by tx_start_datetime_utc
-- There are still dupes at each time, but we'll have to group those out later.

commit
create unique index fake_pk on V033_programme_listings (programme_trans_sk)
commit

-- select count(1) from V033_programme_listings
-- 1036 shows. Over a bunch of channels etc.

-- OK, steps to proceed:
-- 1/ Identify people and event times relating to said project keys
-- 2/ pull out all events appropriately close to said keys
-- 3/ Add profiling stuff? Is it HD boxes or just HD Channels that manifest? Is there enough of HD boxes on HD chanels to see anything?

create table V033_viewing_cues (
        subscriber_id                   decimal(10,0)
        ,Adjusted_Event_Start_Time      datetime
        ,X_Adjusted_Event_End_Time      datetime
        ,primary key (subscriber_id, Adjusted_Event_Start_Time)
)

create table V033_viewing_cb_keys (
        cb_row_ID                       bigint not null primary key
)

create table V033_Viewing_Records (
    cb_row_ID                       bigint      not null primary key
    ,Account_Number                 varchar(20) not null
    ,Subscriber_Id                  decimal(8,0) not null
    ,panel_id                       tinyint
    ,Adjusted_Event_Start_Time      datetime
    ,X_Adjusted_Event_End_Time      datetime
    ,Tx_Start_Datetime_UTC          datetime
    ,Tx_End_Datetime_UTC            datetime
    ,Recorded_Time_UTC              datetime
    ,Play_Back_Speed                decimal(4,0)
    ,X_Event_Duration               decimal(10,0)
    ,X_Programme_Viewed_Duration    decimal(10,0)
    ,Programme_Trans_Sk             bigint      not null
    ,Channel_Name                   varchar(30)
    ,Epg_Title                      varchar(50)
    ,episode_number                 decimal(10)
    ,series_id                      varchar(10)
)

EXECUTE citeam.logger_add_event @pull_log_ID, 4, '1: Structures reset.'
COMMIT

-- Can't just do 1 pull, we're being pretty specific and weeding out duplicates,
-- so it needs to go in three passes...
declare @SQL_hunk_1     varchar(2000)
declare @SQL_hunk_2     varchar(2000)
declare @SQL_hunk_3     varchar(2000)
declare @scan_date      date
declare @datebound      date

set @SQL_hunk_1 = '
insert into V033_viewing_cues
select distinct subscriber_id, Adjusted_Event_Start_Time, X_Adjusted_Event_End_Time
from sk_prod.VESPA_STB_PROG_EVENTS_#$£$&# as pe
inner join V033_programme_listings as pl
on pe.programme_trans_sk = pl.programme_trans_sk
'

-- Build string with placeholder for changing daily table reference
SET @SQL_hunk_2 = '
    insert into V033_viewing_cb_keys
    select distinct
        vw.cb_row_ID
     from sk_prod.VESPA_STB_PROG_EVENTS_#$£$&# as vw
--          inner join V033_viewing_cues as vc
--          on vc.subscriber_id = vw.subscriber_id
        -- Filter for viewing events during extraction
     where (play_back_speed is null or play_back_speed = 2)
        and x_programme_viewed_duration > 0
        and Panel_id in (1,4)
        and x_type_of_viewing_event <> ''Non viewing event''

        -- Filter for viewing events that are close to the viewing of the show we care about...
--        and vw.Adjusted_Event_Start_Time between dateadd(minute, -5, vc.Adjusted_Event_Start_Time)
--                                                and dateadd(hour, 1, vc.X_Adjusted_Event_End_Time)
        and Epg_Title = ''Futurama'' -- being a lot more specific for this pull
'

-- OK, and now resolving the duplication, we can pull out the viewing data itself
-- Update: for this build, we're only pullout Futurama, not the adjacent things,
-- so we dont need any of the other stuff.
set @SQL_hunk_3 = '
insert into V033_Viewing_Records
select 
    vw.cb_row_ID
    ,vw.Account_Number
    ,vw.Subscriber_Id
    ,vw.panel_id
    ,vw.Adjusted_Event_Start_Time
    ,vw.X_Adjusted_Event_End_Time
    ,prog.Tx_Start_Datetime_UTC
    ,prog.Tx_End_Datetime_UTC
    ,vw.Recorded_Time_UTC
    ,vw.Play_Back_Speed
    ,vw.X_Event_Duration
    ,vw.X_Programme_Viewed_Duration
    ,prog.Programme_Trans_Sk
    ,prog.Channel_Name
    ,prog.Epg_Title
    ,prog.episode_number
    ,prog.series_id
from sk_prod.VESPA_STB_PROG_EVENTS_#$£$&# as vw
--inner join V033_viewing_cb_keys as vk
--        on vk.cb_row_id = vw.cb_row_id
inner join sk_prod.Vespa_EPG_Dim as prog
        on vw.programme_trans_sk = prog.programme_trans_sk
     where (play_back_speed is null or play_back_speed = 2)
        and x_programme_viewed_duration > 0
        and Panel_id in (1,4,5)
        and x_type_of_viewing_event <> ''Non viewing event''
        and Epg_Title = ''Futurama'' -- being a lot more specific for this pull
'

-- OK, and now, loop through all those daily tables...

select @scan_date = min(tx_date), @datebound = dateadd(day, 30, max(tx_date))
from sk_prod.Vespa_EPG_Dim as epg
inner join V033_programme_listings as pl
on epg.programme_trans_sk = pl.programme_trans_sk

commit

--select @scan_date, @datebound
-- 2011-11-17 and 2012-02-10 - so we're basically pulling the whole period
-- from the first show to now. The 10th is three days ago, sooner than that
-- and they might not be fully updated so yeah.

delete from V033_Viewing_Records
commit

EXECUTE citeam.logger_add_event @pull_log_ID, 4, '2: Loop prepared.'
COMMIT

while @scan_date < @datebound
    begin
        --delete from V033_viewing_cues
        --delete from V033_viewing_cb_keys
        --commit
        --EXECUTE(replace(@SQL_hunk_1,'#$£$&#',dateformat(@scan_date, 'yyyymmdd')))
        --commit
        --EXECUTE(replace(@SQL_hunk_2,'#$£$&#',dateformat(@scan_date, 'yyyymmdd')))
        --commit
        EXECUTE(replace(@SQL_hunk_3,'#$£$&#',dateformat(@scan_date, 'yyyymmdd')))
        commit

        set @scan_date = dateadd(day,1,@scan_date)
        
        execute citeam.logger_add_event @pull_log_ID, 4, '3: Daily table scanned... (' || dateformat(@scan_date,'yyyymmdd') || ')'
        COMMIT
        
    end

commit

drop table V033_viewing_cues
drop table V033_viewing_cb_keys

EXECUTE citeam.logger_add_event @pull_log_ID, 4, '4: Success! Bailing.'
COMMIT

end;

-- To put into the scheduler:
grant execute on V033_assemble_sample_data to CITeam;

commit;
