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

--Create Extra Variable to return EPG data for programmes broadcast pre start of analysis period

CREATE VARIABLE @playback_snapshot_start_dt              datetime;
SET @playback_snapshot_start_dt  = '2012-09-01';

--select count(*) from V141_Viewing_detail
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
WHERE (tx_date_utc between @playback_snapshot_start_dt  and  @snapshot_end_dt)
;
--893680 Row(s) affected
--select count(*) from  V141_Viewing_detail;
--select count(distinct programme_trans_sk) from  V141_Viewing_detail;
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
commit;
--select top 100 * from V141_Viewing
--select count(*) from V141_Viewing

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

  --select * from channel_table order by channel;

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

alter table  V141_Viewing Add channel_name_inc_hd       varchar(90);

update V141_Viewing
set channel_name_inc_hd=b.channel_name_inc_hd
from V141_Viewing as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;

----Create a single table of Playback and Live---

--select top 100 * from MUSTAPHS.V125_Viewing_with_prog_duration 
--drop table v141_live_playback_viewing;
select account_number
,subscriber_id
,programme_trans_sk
,timeshifting
,viewing_starts
,viewing_stops
,viewing_duration
,scaling_weighting
,channel_name
,epg_title
,genre_description
,tx_date_utc
,hd_channel
,pay_channel
into v141_live_playback_viewing
from MUSTAPHS.V125_Viewing_with_prog_duration 
;
--select count(*) from v141_live_playback_viewing;
commit;
alter table  v141_live_playback_viewing Add channel_name_inc_hd       varchar(90);

update v141_live_playback_viewing
set channel_name_inc_hd=b.channel_name_inc_hd
from v141_live_playback_viewing as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;

insert into v141_live_playback_viewing
select  account_number
,subscriber_id
,programme_trans_sk
,timeshifting
,viewing_starts
,viewing_stops
,viewing_duration
,scaling_weighting
,channel_name
,epg_title
,genre_description
,tx_date_utc
,hd_channel
,pay_channel
,channel_name_inc_hd
from V141_Viewing
;
commit;

update v141_live_playback_viewing
set channel_name_inc_hd=b.channel_name_inc_hd
from v141_live_playback_viewing as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;
Update v141_live_playback_viewing
set channel_name_inc_hd =  
        case    when channel_name ='Sky Sports 1 HD' then 'Sky Sports 1'
                when channel_name ='Disney Junior' then 'Playhouse Disney'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='ITV1 Tyne Tees' then 'ITV1'
                when channel_name ='Watch HD' then 'Watch'
                when channel_name ='Dave HD' then 'Dave'
                when channel_name ='Disney Chnl HD' then 'Disney Channel'
                when channel_name ='Sky Sports 3 HD' then 'Sky Sports 3'
                when channel_name ='Sky Sports 4 HD' then 'Sky Sports 4'
                when channel_name ='Sky 007 HD' then 'Sky Movies 007'
                when channel_name ='Sky Spts F1 HD' then 'Sky Sports F1'
                when channel_name ='MTV HD' then 'MTV'
                when channel_name ='alibi HD' then 'alibi'
                when channel_name ='Cartoon Net HD' then 'Cartoon Network'
                when channel_name ='Star Plus HD' then 'Star Plus'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'               
                when channel_name ='Eurosport 2 HD' then 'Eurosport 2'
                when channel_name ='AnimalPlnt HD' then 'Animal Planet' 
            when channel_name_inc_hd is not null then channel_name_inc_hd else channel_name end
;
commit;
---
--select  Channel_Name, count(*) as records from v141_live_playback_viewing where channel_name_inc_hd is null group by Channel_Name order by  records desc;
--select  channel_name_inc_hd, count(*) as records from v141_live_playback_viewing  group by channel_name_inc_hd order by  records desc;

---

--As Viewing Looking at account level details need to take account weighting from a single day--

--create table of all accounts for 23rd Oct 2012

select a.account_number
,b.weighting
into #accounts
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-10-23' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-10-23' as date) between a.reporting_starts and a.reporting_ends
;
create  hg index idx1 on #accounts(account_number);
commit;

alter table  v141_live_playback_viewing add overall_project_weighting double;

update v141_live_playback_viewing
set overall_project_weighting=b.weighting
from  v141_live_playback_viewing as a
left outer join  #accounts as b
on a.account_number =b.account_number 
;
commit;

select account_number
,max(overall_project_weighting) as account_weight
,sum(viewing_duration) as total_duration
,sum(case when timeshifting= 'LIVE' then viewing_duration else 0 end) as total_live
,sum(case when timeshifting<> 'LIVE' then viewing_duration else 0 end) as total_playback
into #itv3_viewing
from v141_live_playback_viewing
where overall_project_weighting>0
and channel_name_inc_hd in ('ITV3','ITV - ITV3+1')
group by account_number
;

commit;

--select top 100 *,floor(total_duration/60) from #itv3_viewing order by total_duration desc;

select floor(total_duration/60) as full_minutes
,count(*) as records
,sum(account_weight) as weighted_accounts
from #itv3_viewing
group by full_minutes
order by full_minutes
;
commit;

----ITV3 SoV--

create  hg index idx1 on  v141_live_playback_viewing(account_number);
create  lf index idx2 on  v141_live_playback_viewing(channel_name_inc_hd);

create  lf index idx3 on  v141_live_playback_viewing(timeshifting);
commit;
select account_number
,max(overall_project_weighting) as account_weight
,sum(viewing_duration) as total_duration
,sum(case when timeshifting= 'LIVE' and channel_name_inc_hd in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_live_ITV3
,sum(case when timeshifting<> 'LIVE' and channel_name_inc_hd in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_playback_ITV3
,sum(case when channel_name_inc_hd in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_ITV3

,sum(case when timeshifting= 'LIVE' and channel_name_inc_hd not in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_live_non_ITV3
,sum(case when timeshifting<> 'LIVE' and channel_name_inc_hd not in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_playback_non_ITV3
,sum(case when channel_name_inc_hd not in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_non_ITV3
into v141_itv3_sov
from v141_live_playback_viewing
where overall_project_weighting>0
group by account_number
;
commit;

--select top 500 * from v141_itv3_sov; 

select round(total_ITV3/(total_ITV3+total_non_ITV3),2) as prop_itv3
,count(*) as accounts
,sum(account_weight) as weighted_accounts
from v141_itv3_sov
where total_ITV3+total_non_ITV3>0
group by prop_itv3
order by prop_itv3
;

alter table v141_live_playback_viewing add pay_free_indicator  varchar(50);
update v141_live_playback_viewing
set 
     v.pay_free_indicator      = dt.pay_free_indicator
from v141_live_playback_viewing as v
inner join V141_Viewing_detail as dt
on v.programme_trans_sk = dt.programme_trans_sk
;
commit;
--select pay_free_indicator  ,count(*) from V141_Viewing_detail group by pay_free_indicator  ;
----PART B----

----Create table of account attributes---

select account_number
,max(overall_project_weighting) as account_weight

into v141_accounts_for_profiling
from v141_live_playback_viewing
where overall_project_weighting>0
group by account_number
;
commit;
create  hg index idx1 on v141_accounts_for_profiling (account_number);
commit;

--Create Package Details for actual date of analysis (14th Nov 2012)


SELECT csh.account_number
      ,csh.cb_key_household
      ,csh.first_activation_dt
      ,CASE WHEN  cel.mixes = 0                     THEN 'A) 0 Mixes'
            WHEN  cel.mixes = 1
             AND (style_culture = 1 OR variety = 1) THEN 'B) 1 Mix - Variety or Style&Culture'
            WHEN  cel.mixes = 1                     THEN 'C) 1 Mix - Other'
            WHEN  cel.mixes = 2
             AND  style_culture = 1
             AND  variety = 1                       THEN 'D) 2 Mixes - Variety and Style&Culture'
            WHEN  cel.mixes = 2
             AND (style_culture = 0 OR variety = 0) THEN 'E) 2 Mixes - Other Combination'
            WHEN  cel.mixes = 3                     THEN 'F) 3 Mixes'
            WHEN  cel.mixes = 4                     THEN 'G) 4 Mixes'
            WHEN  cel.mixes = 5                     THEN 'H) 5 Mixes'
            WHEN  cel.mixes = 6                     THEN 'I) 6 Mixes'
            ELSE                                         'J) Unknown'
        END as mix_type
       ,CAST(NULL AS VARCHAR(20)) AS new_package
       ,cel.prem_sports
        ,cel.prem_movies
        
  INTO #mixes
  FROM sk_prod.cust_subs_hist as csh
       INNER JOIN sk_prod.cust_entitlement_lookup as cel
               ON csh.current_short_description = cel.short_description
 WHERE csh.subscription_sub_type ='DTV Primary Viewing'
   AND csh.subscription_type = 'DTV PACKAGE'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2012-11-14'
   AND csh.effective_to_dt   >  '2012-11-14'
   AND csh.effective_from_dt != csh.effective_to_dt
;

UPDATE #mixes
   Set new_package = CASE WHEN mix_type IN ( 'A) 0 Mixes'
                                            ,'B) 1 Mix - Variety or Style&Culture'
                                            ,'D) 2 Mixes - Variety and Style&Culture')
                          THEN 'Entertainment'

                          WHEN mix_type IN ( 'C) 1 Mix - Other'
                                            ,'E) 2 Mixes - Other Combination'
                                            ,'F) 3 Mixes'
                                            ,'G) 4 Mixes'
                                            ,'H) 5 Mixes'
                                            ,'I) 6 Mixes')
                          THEN  'Entertainment Extra'
                          ELSE  'Unknown'
                     END;

commit;

exec sp_create_tmp_table_idx '#mixes', 'account_number';

--select top 500 * from sk_prod.cust_entitlement_lookup;
alter table v141_accounts_for_profiling add prem_sports integer default 0;
alter table v141_accounts_for_profiling add prem_movies integer default 0;
alter table v141_accounts_for_profiling add mixes_type varchar(30) default 'Unknown';

update v141_accounts_for_profiling 
set prem_sports=b.prem_sports
,prem_movies=b.prem_movies
,mixes_type=b.new_package
from v141_accounts_for_profiling  as a
left outer join #mixes as b
on a.account_number=b.account_number
;
commit;


select account_number
into #accounts_with_3d  
FROM sk_prod.cust_subs_hist as csh
      
 WHERE subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2012-11-14'
   AND csh.effective_to_dt   >  '2012-11-14'
group by account_number
;

exec sp_create_tmp_table_idx '#accounts_with_3d', 'account_number';

alter table v141_accounts_for_profiling add subscription_3d integer default 0;

update v141_accounts_for_profiling
set subscription_3d=case when b.account_number is not null then 1 else 0 end
from v141_accounts_for_profiling as a
left outer join #accounts_with_3d  as b
on a.account_number = b.account_number
;

----Add on extra variables from product holdings and consumerview---

alter table v141_accounts_for_profiling add talk_product              VARCHAR(50)     default 'NA' ;        -- Current Sky Talk product
alter table v141_accounts_for_profiling add sky_id                    bigint          default 0    ;        -- Sky id created
alter table v141_accounts_for_profiling add Sky_Go_Reg                INTEGER         default 0     ;       -- Sky Go used in last 6 months
alter table v141_accounts_for_profiling add BB_type                   VARCHAR(50)     default 'NA'  ;       -- Current BB product
alter table v141_accounts_for_profiling add Anytime_plus              INTEGER         default 0    ;        -- Anytime+ activated
alter table v141_accounts_for_profiling add hh_composition             VARCHAR(2)     default 'U'         ;   
alter table v141_accounts_for_profiling add hh_affluence             VARCHAR(2)     default 'U'         ;   
alter table v141_accounts_for_profiling add head_hh_age             VARCHAR(1)     default 'U'         ;   
alter table v141_accounts_for_profiling add num_children_in_hh             VARCHAR(1)     default 'U'         ;   
alter table v141_accounts_for_profiling add oldest_female_in_hh             VARCHAR(1)     default 'U'         ;   
alter table v141_accounts_for_profiling add isba_tv_region             VARCHAR(50)     default 'Unknown'         ;   
alter table v141_accounts_for_profiling add cb_key_household           bigint   ;        -- Current Sky Talk product
--drop table nodupes;
commit;

update v141_accounts_for_profiling
set isba_tv_region=b.isba_tv_region
,cb_key_household=b.cb_key_household
from v141_accounts_for_profiling as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;

select cb_key_household
,max(h_household_composition) as hh_composition
,max(h_affluence_v2) as hh_affluence
,max(h_age_coarse) as head_hh_age
,max(h_number_of_children_in_household_2011) as num_children_in_hh
,max(case when p_gender ='1' then p_age_coarse else null end) as oldest_female_in_hh
into #experian_hh_summary
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV
where cb_change_date='2013-01-23'
and cb_address_status = '1' and cb_address_dps IS NOT NULL and cb_address_organisation IS NULL
group by cb_key_household;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary', 'cb_key_household';
commit;

update v141_accounts_for_profiling
set hh_composition=b.hh_composition
,hh_affluence=b.hh_affluence
,head_hh_age=b.head_hh_age
,num_children_in_hh=b.num_children_in_hh
,oldest_female_in_hh=b.oldest_female_in_hh
from v141_accounts_for_profiling as a
left outer join #experian_hh_summary as b
on a.cb_key_household=b.cb_key_household
;
--select top 100 * from v141_accounts_for_profiling;
-------------------------------------------------  02 - Active Sky Talk
--code_location_09
--drop table talk;
--commit;

SELECT DISTINCT base.account_number
       ,CASE WHEN UCASE(current_product_description) LIKE '%UNLIMITED%'
             THEN 'Unlimited'
             ELSE 'Freetime'
          END as talk_product
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
      ,effective_to_dt
         INTO talk
FROM sk_prod.cust_subs_hist AS CSH
    inner join AdSmart AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= '2012-11-14'
     AND csh.effective_to_dt > '2012-11-14'
GROUP BY base.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update AdSmart file
UPDATE v141_accounts_for_profiling
SET  talk_product = talk.talk_product
FROM v141_accounts_for_profiling  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
commit;

-------------------------------------------------------------- D01 - Sky id
-- code_location_27

--drop table sky_id;
--commit;

select account_number
        ,dw_created_dt
        ,1 AS samprofileid
        ,rank() over (partition by account_number order by dw_created_dt desc) as rank
into sky_id
from sk_prod.sam_registrant
where account_number is not null
group by account_number, dw_created_dt, samprofileid;
commit;

delete from sky_id where rank>1;
commit;

Update v141_accounts_for_profiling
set base.sky_id = samprofileid
from v141_accounts_for_profiling as base
        inner join sky_id as si on base.account_number = si.account_number;
commit;



-------------------------------------------------  02 - Sky Go and Downloads
--code_location_06
/*SELECT base.account_number
       ,count(distinct base.account_number) AS Sky_Go_Reg
INTO Sky_Go
FROM   sk_prod.SKY_PLAYER_REGISTRANT  AS Sky_Go
        inner join AdSmart as Base
         on Sky_Go.account_number = Base.account_number
GROUP BY base.account_number;
*/
select base.account_number,
        1 AS SKY_GO_USAGE
--        ,sum(SKY_GO_USAGE)
into skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
        inner join v141_accounts_for_profiling AS Base
         ON usage.account_number = Base.account_number
where cb_data_date >= '2012-05-14'
        AND cb_data_date <'2012-11-14'
group by base.account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update AdSmart file
UPDATE v141_accounts_for_profiling
SET Sky_Go_Reg = sky_go.SKY_GO_USAGE
FROM v141_accounts_for_profiling  AS Base
       INNER JOIN skygo_usage AS sky_go
        ON base.account_number = sky_go.account_number
ORDER BY base.account_number;
commit;

DROP TABLE skygo_usage;
commit;



-------------------------------------------------  02 - Active BB Type
--code_location_10
--drop table bb;
--commit;

Select distinct base.account_number
           ,CASE WHEN current_product_sk=43373 THEN '1) Unlimited (New)'
                 WHEN current_product_sk=42128 THEN '2) Unlimited (Old)'
                 WHEN current_product_sk=42129 THEN '3) Everyday'
                 WHEN current_product_sk=42130 THEN '4) Everyday Lite'
                 WHEN current_product_sk=42131 THEN '5) Connect'
                 ELSE 'NA'
                 END AS BB_type
               ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
               ,effective_to_dt
        ,count(*) AS total
INTO bb
FROM sk_prod.cust_subs_hist AS CSH
    inner join v141_accounts_for_profiling AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= '2012-11-14'
   AND csh.effective_to_dt > '2012-11-14'
      AND effective_from_dt != effective_to_dt
      AND (status_code IN ('AC','AB') OR (status_code='PC' AND prev_status_code NOT IN ('?','RQ','AP','UB','BE','PA') )
            OR (status_code='CF' AND prev_status_code='PC')
            OR (status_code='AP' AND sale_type='SNS Bulk Migration'))
GROUP BY base.account_number, bb_type, effective_to_dt;
commit;

--select top 10 * from bb

DELETE FROM bb where rank_id >1;
commit;

--drop table bbb;
--commit;

select distinct account_number, BB_type
               ,rank() over(PARTITION BY account_number ORDER BY BB_type desc) AS rank_id
into bbb
from bb;
commit;

DELETE FROM bbb where rank_id >1;
commit;

--      create index on BB
CREATE   HG INDEX idx10 ON BB(account_number);
commit;

--      update v141_accounts_for_profiling file
UPDATE v141_accounts_for_profiling
SET  BB_type = BB.BB_type
FROM v141_accounts_for_profiling  AS Base
  INNER JOIN BB AS BB
        ON base.account_number = BB.account_number
            ORDER BY base.account_number;
commit;


drop table bb; commit;
DROP TABLE BBB; commit;


-------------------------------------------------  02 - Anytime + activated
--code_location_05     code changed in line with changes to Wiki
/*SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join AdSmart as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND    status_code = 'AC'
AND    Aplus.effective_from_dt >= @today
AND    Aplus.effective_to_dt > @today
GROUP BY base.account_number;
*/


SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join v141_accounts_for_profiling as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<'2012-11-14'              -- (END)
AND        first_activation_dt>='2012-11-14'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update AdSmart file
UPDATE v141_accounts_for_profiling
SET Anytime_plus = Aplus.Anytime_plus
FROM v141_accounts_for_profiling  AS Base
       INNER JOIN Anytime_plus AS Aplus
        ON base.account_number = APlus.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Anytime_plus;
commit;

---Add ITV3 Activity Segment---
alter table v141_accounts_for_profiling add ITV3_Viewing_segment varchar(50);

update v141_accounts_for_profiling
set ITV3_Viewing_segment=case when total_ITV3=0 then 'f) No viewing of 3+ continuous minutes of ITV3'
                        when total_itv3>=21600 then 'a) 6+hrs ITV3 or over 5% SoV'
                        when  round(total_ITV3/(total_ITV3+total_non_ITV3),3)>0.05 then 'a) 6+hrs ITV3 or over 5% SoV'
                        when total_itv3>=7200 then 'b) >=2hrs and <6hrs of ITV3'
                        when total_itv3>=3600 then 'c) >=1hrs and <2hrs of ITV3'
                        when total_itv3>=800 then 'd) >=15 minutes and <1hr of ITV3'
                        when total_itv3>=1 then 'e) >=3 minutes and <15 Minutes of ITV3' else 'f) No viewing of 3+ continuous minutes of ITV3' end
from v141_accounts_for_profiling as a
left outer join v141_itv3_sov as b
on a.account_number = b.account_number
;
commit;

---Add on details of proportion of viewing from HD/Paid Viewing---
update v141_live_playback_viewing 
    set pay_free_indicator = case when Channel_Name_Inc_Hd in
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
   then 'a) Pay' else 'b) Free' end
    ;
commit;


select account_number
,max(overall_project_weighting) as account_weight
,sum(viewing_duration) as total_duration
,sum(case when hd_channel=1 then viewing_duration else 0 end) as total_hd_viewing
,sum(case when pay_channel=1 then viewing_duration else 0 end) as total_pay_viewing
into v141_hd_paid_viewing
from v141_live_playback_viewing
where overall_project_weighting>0
group by account_number
;
commit;

/*Create Splits
select round(total_hd_viewing/(total_duration),2) as prop_hd
,count(*) as accounts
,sum(account_weight) as weighted_accounts
from v141_hd_paid_viewing
group by prop_hd
order by prop_hd
;


select round(total_pay_viewing/(total_duration),2) as prop_pay
,count(*) as accounts
,sum(account_weight) as weighted_accounts
from v141_hd_paid_viewing
group by prop_pay
order by prop_pay
;
*/
--select top 100 * from v141_live_playback_viewing
---PART C - Create Outputs---


--select hd_channel , count(*) from v141_live_playback_viewing group by hd_channel order by hd_channel;

--select  ITV3_Viewing_segment ,sum(account_weight) from v141_accounts_for_profiling group by  ITV3_Viewing_segment order by  ITV3_Viewing_segment

select  ITV3_Viewing_segment ,case when mixes_type is null then 'Entertainment' else mixes_type end as mix_type,sum(account_weight) as accounts 
from v141_accounts_for_profiling 
group by  ITV3_Viewing_segment ,mix_type
order by  ITV3_Viewing_segment,mix_type
;

select  ITV3_Viewing_segment ,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' else 'e) No Premiums' end as prem_type,sum(account_weight) as accounts 
from v141_accounts_for_profiling 
group by  ITV3_Viewing_segment ,prem_type
order by  ITV3_Viewing_segment,prem_type
;

commit;

select  ITV3_Viewing_segment 
,case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
 ,sum(account_weight) as accounts 
from v141_accounts_for_profiling 
group by  ITV3_Viewing_segment ,bb_talk_holdings
order by  ITV3_Viewing_segment,bb_talk_holdings
;


select  ITV3_Viewing_segment 
,case   when round(total_hd_viewing/(total_duration),2)=0 then 'a) No HD watched'
        when round(total_hd_viewing/(total_duration),2)<0.05 then 'b) Under 5% of viewing on HD'
        when round(total_hd_viewing/(total_duration),2)<=0.15 then 'c) 5%-15% of viewing on HD'
        when round(total_hd_viewing/(total_duration),2)<=0.4 then 'd) 16%-40% of viewing on HD'
        when round(total_hd_viewing/(total_duration),2)>0.4 then 'e) Over 40% of viewing on HD' else 'a) No HD watched' end as hd_prop
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,hd_prop
order by  ITV3_Viewing_segment,hd_prop
;

select  ITV3_Viewing_segment 
,case  
        when round(total_pay_viewing/(total_duration),2)<0.1 then 'a) Under 10% of viewing on Pay Channels'
        when round(total_pay_viewing/(total_duration),2)<=0.25 then 'b) 10%-25% of viewing on Pay Channels'
        when round(total_pay_viewing/(total_duration),2)<=0.4 then 'c) 26%-40% of viewing on Pay Channels'
        when round(total_pay_viewing/(total_duration),2)>0.4 then 'd) Over 40% of viewing on Pay Channels' else 'a) Under 5% of viewing on Pay Channels' end as pay_prop
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,pay_prop
order by  ITV3_Viewing_segment,pay_prop
;

--select bb_type ,count(*) from v141_accounts_for_profiling group by bb_type order by bb_type
--select anytime_plus ,count(*) from v141_accounts_for_profiling group by anytime_plus order by anytime_plus
--select talk_product ,count(*) from v141_accounts_for_profiling group by talk_product order by talk_product


commit;

---Create a summary of one record per programme per account--

select programme_trans_sk
,account_number
,channel_name_inc_hd
,max(scaling_weighting) as accounts
,count(distinct account_number) as distinct_accounts
into v141_summary_by_account_and_programme        
from  v141_live_playback_viewing
group by programme_trans_sk
,account_number,
channel_name_inc_hd;
--drop table v141_summary_by_account_and_programme  ;
commit;
--select top 100 * from v141_summary_by_account_and_programme 

CREATE   HG INDEX idx11 ON v141_summary_by_account_and_programme (account_number);

CREATE   HG INDEX idx12 ON v141_summary_by_account_and_programme (programme_trans_sk);
CREATE   HG INDEX idx13 ON v141_summary_by_account_and_programme (channel_name_inc_hd);
grant all on v141_accounts_for_profiling to public;
grant all on v141_summary_by_account_and_programme to public;

commit;
---Create Channel Lookups
--drop table #rank_details_by_channel;
select programme_trans_sk
,channel_name_inc_hd
,ITV3_Viewing_segment
,sum(accounts) as total_accounts
,rank() over (partition by channel_name_inc_hd ,ITV3_Viewing_segment order by total_accounts desc) as rank_id
into #rank_details_by_channel
from v141_summary_by_account_and_programme as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
group by programme_trans_sk,channel_name_inc_hd,ITV3_Viewing_segment
;
--having rank_id<50
--select count(*) from #rank_details_by_channel;
--select * from #rank_details_by_channel where rank_id<=10 order by ITV3_Viewing_segment,rank_id , total_accounts desc;
--drop table v141_top10_by_segment
--select * into v141_top10_by_segment  from #rank_details_by_channel where rank_id<=100 order by ITV3_Viewing_segment,rank_id , total_accounts desc;
commit;


commit;
CREATE        hg INDEX idx1 on  #rank_details_by_channel (programme_trans_sk);
select channel_name_inc_hd
,ITV3_Viewing_segment
,b.epg_title
,tx_date_utc
,sum(total_accounts) as accounts
,rank() over (partition by channel_name_inc_hd ,ITV3_Viewing_segment order by accounts desc) as rank_id
into #combined_hd_non_hd_top_channels
from #rank_details_by_channel as a
left outer join V141_Viewing_detail as b
on a.programme_trans_sk=b.programme_trans_sk
where channel_name_inc_hd = 'ITV3' and ITV3_Viewing_segment is not null 
group by channel_name_inc_hd
,ITV3_Viewing_segment
,b.epg_title
,tx_date_utc
order by ITV3_Viewing_segment,rank_id , accounts desc;

select * from #combined_hd_non_hd_top_channels where rank_id<=20 order by  ITV3_Viewing_segment,rank_id ;
--select * from V141_Viewing_detail where programme_trans_sk in (231560812,224549073)



commit;
--drop table #combined_hd_non_hd_top_channels_all;
select channel_name_inc_hd
,ITV3_Viewing_segment
,b.epg_title
,tx_date_utc
,sum(total_accounts) as accounts
,rank() over (partition by ITV3_Viewing_segment order by accounts desc) as rank_id
into #combined_hd_non_hd_top_channels_all
from #rank_details_by_channel as a
left outer join V141_Viewing_detail as b
on a.programme_trans_sk=b.programme_trans_sk
where  ITV3_Viewing_segment is not null and channel_name_inc_hd is not null
group by channel_name_inc_hd
,ITV3_Viewing_segment
,b.epg_title
,tx_date_utc
order by ITV3_Viewing_segment,rank_id , accounts desc;


select * from #combined_hd_non_hd_top_channels_all where rank_id<=20;

---Repeat but by distinct programme----
--select top 100 * from V141_Viewing_detail;
--drop table #v141_summary_by_account_and_distinct_programme--

alter table v141_summary_by_account_and_programme add epg_title varchar(40);
alter table v141_summary_by_account_and_programme add genre_description varchar(20);

update v141_summary_by_account_and_programme
set epg_title=b.epg_title
,genre_description=b.genre_description
from v141_summary_by_account_and_programme as a 
left outer join V141_Viewing_detail as b
on a.programme_trans_sk=b.programme_trans_sk
;
--select * from V141_Viewing_detail where epg_title = 'Murder, She Wrote' and left(channel_name,4)='ITV3' order by tx_date_utc
commit;

CREATE   hg INDEX idx14 ON v141_summary_by_account_and_programme (epg_title);
CREATE   hg INDEX idx15 ON v141_summary_by_account_and_programme (genre_description);

alter table v141_summary_by_account_and_programme add ITV3_Viewing_segment varchar(50);
update v141_summary_by_account_and_programme
set ITV3_Viewing_segment=b.ITV3_Viewing_segment
from v141_summary_by_account_and_programme as a 
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
;
--select top 100 * from v141_accounts_for_profiling;
commit;

CREATE   hg INDEX idx16 ON v141_summary_by_account_and_programme (ITV3_Viewing_segment);


commit;
select epg_title
,account_number
,channel_name_inc_hd
,ITV3_Viewing_segment
,max(accounts) as account_weight
--,count(distinct account_number) as distinct_accounts
into v141_summary_by_account_and_distinct_programme        
from  v141_summary_by_account_and_programme as a
group by epg_title
,account_number
,channel_name_inc_hd
,ITV3_Viewing_segment;
--select top 100 * from v141_summary_by_account_and_programme;
--select * into v141_summary_by_account_and_distinct_programme  from #v141_summary_by_account_and_distinct_programme ;
--drop table v141_summary_by_account_and_distinct_programme;
commit;
select channel_name_inc_hd
,ITV3_Viewing_segment
,epg_title
,sum(account_weight) as accounts
,rank() over (partition by ITV3_Viewing_segment order by accounts desc) as rank_id
into v141_combined_hd_non_hd_top_channels_all_distinct
from v141_summary_by_account_and_distinct_programme   as a
where  ITV3_Viewing_segment is not null and channel_name_inc_hd is not null
group by channel_name_inc_hd
,ITV3_Viewing_segment
,epg_title
order by ITV3_Viewing_segment,rank_id , accounts desc;

select * from  v141_combined_hd_non_hd_top_channels_all_distinct where rank_id<=20;

select channel_name_inc_hd
,ITV3_Viewing_segment
,epg_title
,sum(account_weight) as accounts
,rank() over (partition by ITV3_Viewing_segment order by accounts desc) as rank_id
into v141_combined_hd_non_hd_top_channels_all_distinct_ITV3
from v141_summary_by_account_and_distinct_programme   as a
where  channel_name_inc_hd = 'ITV3' and ITV3_Viewing_segment is not null and channel_name_inc_hd is not null
group by channel_name_inc_hd
,ITV3_Viewing_segment
,epg_title
order by ITV3_Viewing_segment,rank_id , accounts desc;
commit;
select * from  v141_combined_hd_non_hd_top_channels_all_distinct_ITV3;



---repeat for Genre---

select account_number
,channel_name_inc_hd
,ITV3_Viewing_segment
,genre_description
,max(accounts) as account_weight
--,count(distinct account_number) as distinct_accounts
into v141_summary_by_account_and_distinct_genre        
from  v141_summary_by_account_and_programme as a
group by account_number
,channel_name_inc_hd
,ITV3_Viewing_segment
,genre_description;
commit;

select channel_name_inc_hd
,ITV3_Viewing_segment
,genre_description
,sum(account_weight) as accounts
,rank() over (partition by ITV3_Viewing_segment order by accounts desc) as rank_id
into v141_combined_hd_non_hd_top_genre_all_distinct
from v141_summary_by_account_and_distinct_genre   as a
where  ITV3_Viewing_segment is not null and channel_name_inc_hd is not null
group by channel_name_inc_hd
,ITV3_Viewing_segment
,genre_description
order by ITV3_Viewing_segment,rank_id , accounts desc;

--select * from  v141_combined_hd_non_hd_top_genre_all_distinct where genre_description is not null and channel_name_inc_hd = 'ITV3' and rank_id<=21;

select channel_name_inc_hd
,ITV3_Viewing_segment
,genre_description
,sum(account_weight) as accounts
,rank() over (partition by ITV3_Viewing_segment order by accounts desc) as rank_id
into v141_combined_hd_non_hd_top_genre_all_distinct_ITV3
from v141_summary_by_account_and_distinct_genre   as a
where  channel_name_inc_hd = 'ITV3' and ITV3_Viewing_segment is not null and channel_name_inc_hd is not null
group by channel_name_inc_hd
,ITV3_Viewing_segment
,genre_description
order by ITV3_Viewing_segment,rank_id , accounts desc;
commit;

select * from v141_combined_hd_non_hd_top_genre_all_distinct_ITV3 where genre_description is not null;


---repeat for Channel---

select account_number
,channel_name_inc_hd
,ITV3_Viewing_segment
,max(accounts) as account_weight
--,count(distinct account_number) as distinct_accounts
into v141_summary_by_account_and_distinct_channel        
from  v141_summary_by_account_and_programme as a
group by account_number
,channel_name_inc_hd
,ITV3_Viewing_segment;
commit;

select channel_name_inc_hd
,ITV3_Viewing_segment
,sum(account_weight) as accounts
,rank() over (partition by ITV3_Viewing_segment order by accounts desc) as rank_id
into #rank_by_channel
from v141_summary_by_account_and_distinct_channel  
where   ITV3_Viewing_segment is not null and channel_name_inc_hd is not null
group by channel_name_inc_hd
,ITV3_Viewing_segment

select * from #rank_by_channel where rank_id<=30 order by ITV3_Viewing_segment , rank_id

commit;


select  ITV3_Viewing_segment 
,case  
        when sky_id=1 then 1 else 0 end sky_go_registered
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,sky_go_registered
order by  ITV3_Viewing_segment,sky_go_registered
;

select  ITV3_Viewing_segment 
,case  
        when Sky_Go_Reg=1 then 1 else 0 end sky_go_user
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,sky_go_user
order by  ITV3_Viewing_segment,sky_go_user
;

select  ITV3_Viewing_segment 
,case  
        when Anytime_plus=1 then 1 else 0 end on_demand
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,on_demand
order by  ITV3_Viewing_segment,on_demand
;

select  ITV3_Viewing_segment 
,case  
        when subscription_3d=1 then 1 else 0 end sub3d
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,sub3d
order by  ITV3_Viewing_segment,sub3d
;

select  ITV3_Viewing_segment 
,  CASE  hh_composition   WHEN     '00' THEN   'a) Families'
                                                WHEN     '01' THEN   'b) Extended family'
                                                WHEN     '02' THEN   'c) Extended household'
                                                WHEN     '03' THEN   'd) Pseudo family'
                                                WHEN     '04' THEN   'e) Single male'
                                                WHEN     '05' THEN   'f) Single female'
                                                WHEN     '06' THEN   'g) Male homesharers'
                                                WHEN     '07' THEN   'h) Female homesharers'
                                                WHEN     '08' THEN   'i) Mixed homesharers'
                                                WHEN     '09' THEN   'j) Abbreviated male families'
                                                WHEN     '10' THEN   'k) Abbreviated female families'
                                                WHEN     '11' THEN   'l) Multi-occupancy dwelling'
                                                WHEN     'U'  THEN   'm) Unclassified'
                                                ELSE                 'm) Unclassified'
                                                END as composition
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,composition
order by  ITV3_Viewing_segment,composition
;

select  ITV3_Viewing_segment 
,  case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' END as affluence
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,affluence
order by  ITV3_Viewing_segment,affluence
;

select  ITV3_Viewing_segment 
, case  when head_hh_age IN ('0')       THEN 'a) Age 18-25'
        when head_hh_age IN ('1')       THEN 'b) Age 26-35'
        when head_hh_age IN ('2')       THEN 'c) Age 36-45'
        when head_hh_age IN ('3')       THEN 'd) Age 46-55'
        when head_hh_age IN ('4')       THEN 'e) Age 56-65'
        when head_hh_age IN ('5')       THEN 'f) Age 66+' else 'g) Unknown'
                                                END as head_age
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,head_age
order by  ITV3_Viewing_segment,head_age
;

select  ITV3_Viewing_segment 
, case  when oldest_female_in_hh IN ('0')       THEN 'a) Age 18-25'
        when oldest_female_in_hh IN ('1')       THEN 'b) Age 26-35'
        when oldest_female_in_hh IN ('2')       THEN 'c) Age 36-45'
        when oldest_female_in_hh IN ('3')       THEN 'd) Age 46-55'
        when oldest_female_in_hh IN ('4')       THEN 'e) Age 56-65'
        when oldest_female_in_hh IN ('5')       THEN 'f) Age 66+' else 'g) Unknown'
                                                END as female_age
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,female_age
order by  ITV3_Viewing_segment,female_age
;



select  ITV3_Viewing_segment 
,case  
        when num_children_in_hh in ('1','2','3','4') then 1 else 0 end kids_in_hh
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,kids_in_hh
order by  ITV3_Viewing_segment,kids_in_hh
;

select  ITV3_Viewing_segment 
,case  
        when isba_tv_region is null then 'Not Defined' else isba_tv_region end as tv_region
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,tv_region
order by  ITV3_Viewing_segment,tv_region
;

----Add Current Account Status to account table---

alter table v141_accounts_for_profiling add dtv_status           varchar(2)   ;

update v141_accounts_for_profiling
set dtv_status= acct_status_code
from  v141_accounts_for_profiling as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

select  ITV3_Viewing_segment 
,dtv_status
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,dtv_status
order by  ITV3_Viewing_segment,dtv_status
;


---Add Value Segment (as at 14th Nov)
--alter table v141_accounts_for_profiling delete value_segment;
alter table v141_accounts_for_profiling add value_segment           varchar(30)   ;

update v141_accounts_for_profiling
set value_segment=b.value_segment
from v141_accounts_for_profiling as a 
left outer join  sk_prod.VALUE_SEGMENTS_FIVE_YRS as b
on a.account_number = b.account_number
where value_seg_date='2012-10-29'
;
commit;

--select value_seg_date , count(*) as records from sk_prod.VALUE_SEGMENTS_FIVE_YRS  group by value_seg_date order by value_seg_date desc

select  ITV3_Viewing_segment 
,case when value_segment is null then 'Unstable' else value_segment end as segment
 ,sum(a.account_weight) as accounts 
from v141_accounts_for_profiling  as a
left outer join v141_hd_paid_viewing as b
on a.account_number = b.account_number
group by  ITV3_Viewing_segment ,segment
order by  ITV3_Viewing_segment,segment
;

commit;


--Create Overall Pivot Table--

select case when value_segment is null then 'Unstable' else value_segment end as value_segment_group
,ITV3_Viewing_segment
,sum(account_weight) as accounts
,sum(case when dtv_status in ('SC','PO') then account_weight else 0 end) as churned
,sum(case when dtv_status in ('PO') then account_weight else 0 end) as cuscan
,sum(case when dtv_status in ('SC') then account_weight else 0 end) as syscan


----Add splits for Sky Movies/Sky Sports Viewing----


--select top 100 * from v141_live_playback_viewing;
--drop table  v141_live_playback_viewing_sports_movies_30min_sessions;
select account_number
,programme_trans_sk
,max(case when  channel_name_inc_hd in ('Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1') then 1 else 0 end) as sports_programmes
max(case when  channel_name_inc_hd not in ('Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1') then 1 else 0 end) as movies_programmes
into v141_live_playback_viewing_sports_movies_30min_sessions
from v141_live_playback_viewing
where viewing_duration >=1800 and channel_name_inc_hd in (
'Sky DramaRom'
,'Sky Movies 007'
,'Sky Movies Action'
,'Sky Movies Classics'
,'Sky Movies Comedy'
,'Sky Movies Family'
,'Sky Movies Indie'
,'Sky Movies Mdn Greats'
,'Sky Movies Sci-Fi/Horror'
,'Sky Movies Showcase'
,'Sky Movies Thriller'
,'Sky Prem+1'
,'Sky Premiere'
,'Sky ShowcseHD'
,'Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1')
group by account_number
,programme_trans_sk
;
--select top 100 * from v141_accounts_for_profiling;
select a.account_number
,b.account_weight
,sum(sports_programmes) as total_sports
,sum(movies_programmes) as total_movies
into v141_accounts_with_sport_movies_activity_summary
from v141_live_playback_viewing_sports_movies_30min_sessions as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
where b.account_weight>0
group by a.account_number
,b.account_weight
;

CREATE   HG INDEX idx01 ON v141_accounts_with_sport_movies_activity_summary(account_number);
commit;

select case when total_sports>=50 then 50 else total_sports end as sports_sessions
,sum(account_weight)
from v141_accounts_with_sport_movies_activity_summary
group by sports_sessions
order by sports_sessions
;


select case when total_movies>=50 then 50 else total_movies end as movies_sessions
,sum(account_weight)
from v141_accounts_with_sport_movies_activity_summary
group by movies_sessions
order by movies_sessions
;
commit;

--select distinct channel_name_inc_hd from v141_live_playback_viewing order by channel_name_inc_hd;
--select top 100 * from v141_live_playback_viewing ;
commit;
select account_number
,max(overall_project_weighting) as account_weight
,sum(viewing_duration) as total_duration
,sum(case when channel_name_inc_hd in ('ITV3','ITV - ITV3+1') then viewing_duration else 0 end) as total_ITV3
,sum(case when channel_name_inc_hd in ('Sky Atlantic','Sky Atlantic+1') then viewing_duration else 0 end) as total_sky_atlantic
,sum(case when channel_name_inc_hd in ('Alibi','Alibi +1') then viewing_duration else 0 end) as total_alibi
,sum(case when channel_name_inc_hd in ('GOLD  (TV)','GOLD +1') then viewing_duration else 0 end) as total_gold
,sum(case when channel_name_inc_hd not in ('ITV3','ITV - ITV3+1','Sky Atlantic','Sky Atlantic+1','GOLD  (TV)','GOLD +1','Alibi','Alibi +1') and pay_channel=1 then viewing_duration else 0 end) as total_other_paid
,sum(case when channel_name_inc_hd not in ('ITV3','ITV - ITV3+1','Sky Atlantic','Sky Atlantic+1','GOLD  (TV)','GOLD +1','Alibi','Alibi +1')  and pay_channel=0 then viewing_duration else 0 end) as total_other_free
into v141_channel_sov
from v141_live_playback_viewing
where overall_project_weighting>0
group by account_number
;
commit;


select account_number
,max(overall_project_weighting) as account_weight
,sum(case when pay_channel=1 then viewing_duration else 0 end) as total_paid
,sum(case when pay_channel=0 then viewing_duration else 0 end) as total_free
into v141_channel_pay_free
from v141_live_playback_viewing
where overall_project_weighting>0
group by account_number
;
commit;

create hg index idx2 on v141_channel_pay_free(account_number);
commit;
/*
select round(total_sky_atlantic/(total_duration),2) as prop_sky_atlantic
,count(*) as accounts
,sum(account_weight) as weighted_accounts
from v141_channel_sov
where total_duration>0
group by prop_sky_atlantic
order by prop_sky_atlantic
;
*/

select case when total_sky_atlantic=0 then 'a) No Sky Atlantic viewed'
            when round(total_sky_atlantic/(total_duration),3)<=0.02 then 'b) <=2% Sky Atlantic'
            when round(total_sky_atlantic/(total_duration),3)<=0.05 then 'c) >2% and <=5% Sky Atlantic'
            when round(total_sky_atlantic/(total_duration),3)>0.05 then 'd) Over 5% Sky Atltantic' else 'a) No Sky Atlantic viewed' end as prop_sky_atlantic
,ITV3_Viewing_segment
,sum(b.account_weight) as weighted_accounts
from v141_channel_sov as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
where total_duration>0
group by prop_sky_atlantic,ITV3_Viewing_segment
order by prop_sky_atlantic,ITV3_Viewing_segment
;

select case when total_alibi=0 then 'a) No Alibi viewed'
            when round(total_alibi/(total_duration),3)<=0.02 then 'b) <=2% Alibi'
            when round(total_alibi/(total_duration),3)<=0.05 then 'c) >2% and <=5% Alibi'
            when round(total_alibi/(total_duration),3)>0.05 then 'd) Over 5% Alibi' else 'a) No Alibi viewed' end as prop_alibi
,ITV3_Viewing_segment
,sum(b.account_weight) as weighted_accounts
from v141_channel_sov as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
where total_duration>0
group by prop_alibi,ITV3_Viewing_segment
order by prop_alibi,ITV3_Viewing_segment
;
commit;

select case when total_gold=0 then 'a) No Gold viewed'
            when round(total_gold/(total_duration),3)<=0.02 then 'b) <=2% Gold'
            when round(total_gold/(total_duration),3)<=0.05 then 'c) >2% and <=5% Gold'
            when round(total_gold/(total_duration),3)>0.05 then 'd) Over 5% Gold' else 'a) No Gold viewed' end as prop_Gold
,ITV3_Viewing_segment
,sum(b.account_weight) as weighted_accounts
from v141_channel_sov as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
where total_duration>0
group by prop_Gold,ITV3_Viewing_segment
order by prop_Gold,ITV3_Viewing_segment
;

commit;


Select a.account_number
,itv3_viewing_segment
,b.account_weight
,round(total_gold/(total_duration),3) as gold_sov
,round(total_alibi/(total_duration),3)  as alibi_sov
,round(total_ITV3/(total_duration),3) as ITV3_sov
into #sov_split
From v141_channel_sov as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
where total_duration>0

;

select itv3_viewing_segment
,sum(gold_sov*account_weight) as gold
,sum(alibi_sov*account_weight) as alibi
,sum(ITV3_sov*account_weight) as ITV3
,sum(account_weight) as accounts 
from #sov_split
group by itv3_viewing_segment
order by itv3_viewing_segment
;
--
commit; 


---Create Output Pivot Table----
select case when ITV3_Viewing_segment in ('e) >=3 minutes and <15 Minutes of ITV3','f) No viewing of 3+ continuous minutes of ITV3') then '01: No ITV3 Activity'
    when ITV3_Viewing_segment='d) >=15 minutes and <1hr of ITV3' and value_segment not in ('Bedding In','Unstable') then '02: v. Low ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='d) >=15 minutes and <1hr of ITV3' and value_segment  in ('Bedding In','Unstable') then '03: v. Low ITV3 Unstable Value Segment'
    when ITV3_Viewing_segment='c) >=1hrs and <2hrs of ITV3' and value_segment not in ('Bedding In','Unstable') then '04: Low ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='c) >=1hrs and <2hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '05: Low ITV3 Unstable Value Segment'
    when ITV3_Viewing_segment='b) >=2hrs and <6hrs of ITV3' and value_segment not in ('Bedding In','Unstable') then '06: Medium ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='b) >=2hrs and <6hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '07: Medium ITV3 Unstable Value Segment'
    when ITV3_Viewing_segment='a) 6+hrs ITV3 or over 5% SoV' and value_segment not in ('Bedding In','Unstable') then '08: High ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='a) 6+hrs ITV3 or over 5% SoV' and value_segment  in ('Bedding In','Unstable') then '09: High ITV3 Unstable Value Segment' else '01: No ITV3 Activity' end
  as risk_segment
,case when value_segment is null then 'Unstable' else value_segment end as value_seg
, case when ITV3_Viewing_segment in ('e) >=3 minutes and <15 Minutes of ITV3','f) No viewing of 3+ continuous minutes of ITV3') then '01: No ITV3 Activity'
when ITV3_Viewing_segment='d) >=15 minutes and <1hr of ITV3' and value_segment  in ('Bedding In','Unstable') then '02: v. Low ITV3 Activity'
when ITV3_Viewing_segment='c) >=1hrs and <2hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '03: Low ITV3 Unstable Activity'
when ITV3_Viewing_segment='b) >=2hrs and <6hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '04: Medium ITV3 Activity'
when ITV3_Viewing_segment='a) 6+hrs ITV3 or over 5% SoV' and value_segment  in ('Bedding In','Unstable') then '05: High ITV3 Activity' else '01: No ITV3 Activity' end
  as ITV3_Segment
,case when  round(total_paid/(total_paid+total_free),3)>=0.25 then '01: High Proportion of Pay Viewing' else '02: Low Proportion of Pay Viewing' end as pay_vs_free

,case when round(total_sky_atlantic/(total_duration),3)>0.05 then '01) Over 5% Sky Atltantic SoV' else '02) <=5% Sky Atlantic SoV' end as prop_sky_atlantic
,case when Sky_Go_Reg=1 then 1 else 0 end used_sky_go_L6M
,case when total_sports>=12 then '01: High Sports Viewer' else '02: Low Sports Viewer' end as sky_sports_viewing_type
,case when total_movies>=12 then '01: High Movies Viewer' else '02: Low Movies Viewer' end as sky_movies_viewing_type
,sum(a.account_weight) as weighted_accounts
,sum(case when dtv_status in ('SC') then a.account_weight else 0 end) as syscan_status
,sum(case when dtv_status in ('PO') then a.account_weight else 0 end) as cuscan_status
,sum(case when dtv_status in ('SC','PO') then a.account_weight else 0 end) as churn_status
,sum(case when dtv_status in ('SC','PO','AB','PC') then a.account_weight else 0 end) as churn_or_pending_churn_status
from v141_accounts_for_profiling as a
left outer join v141_channel_pay_free as b
on a.account_number=b.account_number
left outer join v141_channel_sov as c
on a.account_number=c.account_number
left outer join v141_accounts_with_sport_movies_activity_summary as d
on a.account_number=d.account_number
group by risk_segment
,value_seg
,ITV3_Segment
,pay_vs_free
,prop_sky_atlantic
,used_sky_go_L6M
,sky_sports_viewing_type
,sky_movies_viewing_type
order by risk_segment,value_seg,pay_vs_free
;
commit;

---Enhanced version of Pivot with extra Demographic variables---
select case when ITV3_Viewing_segment in ('e) >=3 minutes and <15 Minutes of ITV3','f) No viewing of 3+ continuous minutes of ITV3') then '01: No ITV3 Activity'
    when ITV3_Viewing_segment='d) >=15 minutes and <1hr of ITV3' and value_segment not in ('Bedding In','Unstable') then '02: v. Low ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='d) >=15 minutes and <1hr of ITV3' and value_segment  in ('Bedding In','Unstable') then '03: v. Low ITV3 Unstable Value Segment'
    when ITV3_Viewing_segment='c) >=1hrs and <2hrs of ITV3' and value_segment not in ('Bedding In','Unstable') then '04: Low ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='c) >=1hrs and <2hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '05: Low ITV3 Unstable Value Segment'
    when ITV3_Viewing_segment='b) >=2hrs and <6hrs of ITV3' and value_segment not in ('Bedding In','Unstable') then '06: Medium ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='b) >=2hrs and <6hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '07: Medium ITV3 Unstable Value Segment'
    when ITV3_Viewing_segment='a) 6+hrs ITV3 or over 5% SoV' and value_segment not in ('Bedding In','Unstable') then '08: High ITV3 Loyal Value Segment'
when ITV3_Viewing_segment='a) 6+hrs ITV3 or over 5% SoV' and value_segment  in ('Bedding In','Unstable') then '09: High ITV3 Unstable Value Segment' else '01: No ITV3 Activity' end
  as risk_segment
,case when value_segment is null then 'Unstable' else value_segment end as value_seg
, case when ITV3_Viewing_segment in ('e) >=3 minutes and <15 Minutes of ITV3','f) No viewing of 3+ continuous minutes of ITV3') then '01: No ITV3 Activity'
when ITV3_Viewing_segment='d) >=15 minutes and <1hr of ITV3' and value_segment  in ('Bedding In','Unstable') then '02: v. Low ITV3 Activity'
when ITV3_Viewing_segment='c) >=1hrs and <2hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '03: Low ITV3 Unstable Activity'
when ITV3_Viewing_segment='b) >=2hrs and <6hrs of ITV3' and value_segment  in ('Bedding In','Unstable') then '04: Medium ITV3 Activity'
when ITV3_Viewing_segment='a) 6+hrs ITV3 or over 5% SoV' and value_segment  in ('Bedding In','Unstable') then '05: High ITV3 Activity' else '01: No ITV3 Activity' end
  as ITV3_Segment
,case when  round(total_paid/(total_paid+total_free),3)>=0.25 then '01: High Proportion of Pay Viewing' else '02: Low Proportion of Pay Viewing' end as pay_vs_free

,case when round(total_sky_atlantic/(total_duration),3)>0.05 then '01) Over 5% Sky Atltantic SoV' else '02) <=5% Sky Atlantic SoV' end as prop_sky_atlantic
,case when Sky_Go_Reg=1 then 1 else 0 end used_sky_go_L6M
,case when total_sports>=12 then '01: High Sports Viewer' else '02: Low Sports Viewer' end as sky_sports_viewing_type
,case when total_movies>=12 then '01: High Movies Viewer' else '02: Low Movies Viewer' end as sky_movies_viewing_type








,sum(a.account_weight) as weighted_accounts
,sum(case when dtv_status in ('SC') then a.account_weight else 0 end) as syscan_status
,sum(case when dtv_status in ('PO') then a.account_weight else 0 end) as cuscan_status
,sum(case when dtv_status in ('SC','PO') then a.account_weight else 0 end) as churn_status
,sum(case when dtv_status in ('SC','PO','AB','PC') then a.account_weight else 0 end) as churn_or_pending_churn_status
from v141_accounts_for_profiling as a
left outer join v141_channel_pay_free as b
on a.account_number=b.account_number
left outer join v141_channel_sov as c
on a.account_number=c.account_number
left outer join v141_accounts_with_sport_movies_activity_summary as d
on a.account_number=d.account_number
group by risk_segment
,value_seg
,ITV3_Segment
,pay_vs_free
,prop_sky_atlantic
,used_sky_go_L6M
,sky_sports_viewing_type
,sky_movies_viewing_type
order by risk_segment,value_seg,pay_vs_free
;



--select top 100 *  from v141_accounts_for_profiling














--drop table v141_summary_by_account_and_distinct_genre ; commit;

/*

,rank() over (partition by channel_name_inc_hd order by distinct_accounts) as rank_id

select ITV3_Viewing_segment
,sum(scaling_weighting) as viewers_watching

from v141_live_playback_viewing
where overall_project_weighting>0

--select top 100 * from  V141_Viewing_detail;


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
*/


--select top 100 * from  v141_summary_by_account_and_programme
--select top 100 * from v141_accounts_for_profiling;
--select top 100 * from  v141_live_playback_viewing;
--select sum(subscription_3d) from v141_accounts_for_profiling;
 
/*
select round(total_ITV3/(total_ITV3+total_non_ITV3),3) as prop_itv3
,count(*) as accounts
,sum(account_weight) as weighted_accounts
from v141_itv3_sov
where total_ITV3+total_non_ITV3>0
group by prop_itv3
order by prop_itv3
;


select round(total_ITV3/(total_ITV3+total_non_ITV3),3) as prop_itv3
,count(*) as accounts
,sum(account_weight) as weighted_accounts
from v141_itv3_sov
where total_ITV3+total_non_ITV3>0
group by prop_itv3
order by prop_itv3
;
*/





















/*
Affluence_group =  case         WHEN H_AFFLUENCE IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN H_AFFLUENCE IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN H_AFFLUENCE IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN H_AFFLUENCE IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN H_AFFLUENCE IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN H_AFFLUENCE IN ('15','16','17')       THEN 'F) High'
                                                WHEN H_AFFLUENCE IN ('18','19')            THEN 'G) Very High' END



        ,talk_product              VARCHAR(50)     default 'NA'         -- Current Sky Talk product
        ,sky_id                    bigint          default 0            -- Sky id created
        ,Sky_Go_Reg                INTEGER         default 0            -- Sky Go number of downloads 12 months
        ,BB_type                   VARCHAR(50)     default 'NA'         -- Current BB product
        ,Anytime_plus              INTEGER         default 0            -- Anytime+ activated
Also add experian variables
commit;
*/
/*
select channel_name_inc_hd, hd_channel,pay_free_indicator,count(*) as records
from v141_live_playback_viewing
group by channel_name_inc_hd, hd_channel,pay_free_indicator


--select count(*),sum(account_weight) from v141_accounts_for_profiling;
/*
select floor(total_playback/60) as full_minutes
,count(*) as records
,sum(account_weight) as weighted_accounts
from #itv3_viewing
group by full_minutes
order by full_minutes
;

*/


--select distinct channel_name_inc_hd from v141_live_playback_viewing order by channel_name_inc_hd
--select distinct timeshifting from v141_live_playback_viewing order by timeshifting



/*
select count(*)
from V141_Viewing

select  Channel_Name  , count(*) as records from V141_Viewing group by Channel_Name order by  records desc;

select  channel_name_inc_hd  ,Channel_Name, count(*) as records from v141_live_playback_viewing group by channel_name_inc_hd,Channel_Name order by  records desc;


select tx_date_utc, count(*) from V141_Viewing  group by tx_date_utc order by tx_date_utc;
select top 500 * from v141_live_playback_viewing;
select top 500 * from V141_Viewing;
select top 500 *
from vespa_analysts.VESPA_DAILY_AUGS_20121001
where timeshifting<>'LIVE' and timeshifting is not null and viewing_duration>=180

--drop table #sov_split;
--select top 100 * from #sov_split;



*/