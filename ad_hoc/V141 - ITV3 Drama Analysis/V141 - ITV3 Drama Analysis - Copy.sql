/*------------------------------------------------------------------------------
        Project: V141 - ITV3 Drama Analysis
        Version: 1
        Created: 20130122
        Lead: Jitesh Patel
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--PART A: Viewing Data (For Customers active in the snapshot period)
--------------------------------------------------------------------------------

---Get details of Programmes Watched 3+ Minutes of---
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
--CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-10-01';
SET @snapshot_end_dt    = '2012-11-14';

/*
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @viewing_var_num_days = 29;
commit;
*/

IF object_ID ('V141_Viewing') IS NOT NULL THEN
            DROP TABLE V141_Viewing
END IF;

CREATE TABLE V141_Viewing
    ( cb_row_ID                                         bigint       not null --primary key
            ,Account_Number                             varchar(20)  not null
            ,Subscriber_Id                              bigint
            ,programme_trans_sk                         bigint
            ,timeshifting                               varchar(4)
            ,viewing_starts                             datetime
            ,viewing_stops                              datetime
            ,viewing_Duration                           decimal(10,0)
            ,capped_flag                                tinyint
            ,capped_event_end_time                      datetime
            ,Scaling_Segment_Id                         int
            ,Scaling_Weighting                          float
            ,service_key                                int
            ,Channel_Name                               varchar(30)
            ,epg_title                                  varchar(50)
            ,duration                                   int
            ,Genre_Description                          varchar(30)
            ,Sub_Genre_Description                      varchar(30)
            ,epg_group_Name                             varchar(30)
            ,network_indicator                          varchar(50)
            ,tx_date_utc                                date
            ,x_broadcast_Time_Of_Day                    varchar(15)
            ,pay_free_indicator                         varchar(50)
)
;

--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20121001

-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into V141_Viewing(
                cb_row_ID
                ,Account_Number
                ,Subscriber_Id
                ,programme_trans_sk
                ,timeshifting
                ,viewing_starts
                ,viewing_stops
                ,viewing_Duration
                ,capped_flag
               ,capped_event_end_time
                ,Scaling_Segment_Id
                ,Scaling_Weighting
)
        select
                cb_row_ID
                ,Account_Number
                ,Subscriber_Id
                ,programme_trans_sk
                ,timeshifting
                ,viewing_starts
                ,viewing_stops
                ,viewing_Duration
                ,capped_flag
                ,capped_event_end_time
                ,Scaling_Segment_Id
                ,Scaling_Weighting
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*##
where timeshifting<>''LIVE'' and timeshifting is not null and viewing_duration>=180
'
;

-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end

--select top 100 * from V141_Viewing
--select count(*) from V141_Viewing
--select count(distinct(account_number)) from V141_Viewing

create hg index idx2 on V141_Viewing(programme_trans_sk);

--------------------------------------------------------------------------------------------------------------------------------------------------
-- PART B: Get programme data from sk_prod.VESPA_EPG_DIM
--------------------------------------------------------------------------------------------------------------------------------------------------

IF object_id('V141_Viewing_detail') IS NOT NULL THEN
        DROP TABLE V141_Viewing_detail
END IF;

--select top 100 * from sk_prod.Vespa_programme_schedule

SELECT      pk_programme_instance_dim as programme_trans_sk
            ,service_key
            ,Channel_Name
            ,programme_instance_name as epg_title
            ,programme_instance_duration as duration
            ,Genre_Description
            ,Sub_Genre_Description
            ,epg_group_Name
            ,network_indicator
            ,broadcast_start_date_time_utc as tx_date_utc
            ,broadcast_daypart as x_broadcast_Time_Of_Day
            ,pay_free_indicator
INTO  V141_Viewing_detail
FROM sk_prod.Vespa_programme_schedule
WHERE (tx_date_utc between @snapshot_start_dt  and  @snapshot_end_dt)
;
--893680 Row(s) affected

--select top 10 * from sk_prod.Vespa_programme_schedule
--select top 10 * from sk_prod.VESPA_EPG_DIM
--select TOP 10 * from sk_prod.VESPA_EPG_DIM where tx_date_utc = '2012-10-01' AND upper(GENRE_DESCRIPTION) like '%SPORT%' AND UPPER(CHANNEL_NAME) LIKE '%1%'
/*
select pk_programme_instance_dim
        ,programme_instance_name
        ,synopsis
from sk_prod.Vespa_programme_schedule
where CAST(broadcast_start_date_time_utc AS DATE) = '2012-10-21'
AND upper(GENRE_DESCRIPTION) like '%SPORT%'
and upper(Sub_Genre_Description) like '%FOOTBALL%'
AND UPPER(synopsis) LIKE '%EVE%'
AND UPPER(CHANNEL_NAME) LIKE '%ESPN%'
*/


--tx_date_time_utc >= '2012-10-01 12:00:00.000000' AND tx_date_time_utc <= '2012-10-06 12:45:00.000000' and

--select top 100 * from sk_prod.Vespa_programme_schedule where broadcast_start_date_time_utc = '2012-10-01'
--select programme_instance_name, count(*) from sk_prod.Vespa_programme_schedule where broadcast_start_date_time_utc = '2012-10-06' group by programme_instance_name


--select top 100 * from V141_Viewing_detail where epg_title is not null
--select epg_title, count(*) from V141_Viewing_detail group by epg_title

create hg index idx2 on V141_Viewing_detail(programme_trans_sk);

--------------------------------------------------------------------------------
--PART C: Append EPG channel Detail to the viewing data
--------------------------------------------------------------------------------

--select top 10 * from V141_Viewing

--select top 100 * from V141_Viewing_detail where programme_trans_sk = 100196282
--select top 100 * from sk_prod.Vespa_programme_schedule where dk_programme_instance_dim = 101578963

update V141_Viewing
set v.service_key              = dt.service_key
     ,v.Channel_Name            = dt.Channel_Name
     ,v.epg_title               = dt.epg_title
     ,v.duration                = dt.duration
     ,v.Genre_Description       = dt.Genre_Description
     ,v.Sub_Genre_Description   = dt.Sub_Genre_Description
     ,v.epg_group_Name          = dt.epg_group_Name
     ,v.network_indicator       = dt.network_indicator
     ,v.tx_date_utc             = dt.tx_date_utc
     ,v.x_broadcast_Time_Of_Day = dt.x_broadcast_Time_Of_Day
     ,v.pay_free_indicator      = dt.pay_free_indicator
from V141_Viewing as v
inner join V141_Viewing_detail as dt
on v.programme_trans_sk = dt.programme_trans_sk
;

--select top 100 * from V141_Viewing

--------------------------------------------------------------------------------
--PART D: Data manipulation and append
--------------------------------------------------------------------------------

  --select count(*) from V141_Viewing
  --select top 100 * from V141_Viewing
  --select lower(Epg_Title) as epg_title_lowercase, count(*) from V141_Viewing group by epg_title_lowercase

  -- Add the following fields to the viewing table
  Alter table V141_Viewing Add hd_channel       tinyint     default 0;
  Alter table V141_Viewing Add Pay_channel      tinyint     default 0;
  Alter table V141_Viewing Add viewing_category varchar(50);

  update V141_Viewing
  set hd_channel = 1
  where upper(channel_name) like '%HD%'
  ;

  --select top 100 * from V141_Viewing

  --select * from vespa_analysts.channel_name_and_techedge_channel
  --NOTE
  --dedupe from the vespa_analysts.channel_name_and_techedge_channel

  --drop table channel_name_and_techedge_channel
  select distinct channel
         ,channel_name_grouped
         ,channel_name_inc_hd
  into channel_name_and_techedge_channel
  from vespa_analysts.channel_name_and_techedge_channel;
  ;

  --drop table channel_table
  select *
         ,rank() over (partition by channel order by channel_name_grouped, Channel_Name_Inc_Hd) as rank_id
   into channel_table
   from channel_name_and_techedge_channel
  ;

  --select * from channel_table

  delete from channel_table where rank_id > 1;

  --select count(channel) from #channel_table
  --select count(distinct(channel)) from channel_name_and_techedge_channel

   update V141_Viewing as bas
    set Pay_channel = 1
    from channel_table as det   --vespa_analysts.channel_name_and_techedge_channel as det
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
           'Sky Thriller HD','Sky1','Sky2','Sky3','Sky3+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
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
           'Football First 3','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo')
    and upper(bas.Channel_Name) = upper(det.Channel)
    ;

commit;
select count(*)
from V141_Viewing

select  timeshifting , count(*) as records from V141_Viewing group by timeshifting;

select tx_date_utc, count(*) from V141_Viewing  group by tx_date_utc order by tx_date_utc;

