


--select scaling_day,sum(sum_of_weights) as weighted_base_value,sum(vespa_accounts) as total_vespa_accounts from vespa_analysts.SC2_weightings group by scaling_day order by scaling_day;
--select top 100 * from Vespa_analysts.SC2_Segments_Lookup_v2_0;
--select top 100 * from 	Vespa_analysts.SC2_Variables_Lookup_v2_0;
--select count(distinct account_number) from vespa_analysts.SC2_intervals ;
---Get List of all on Vespa with Cuscan---

--select top 100 * from account_status_at_period_start;

---Generate a list of all those who cuscan since Jan 2012
select
                account_number
                ,csh.status_start_dt as po_dt
                ,csh.prev_status_start_dt as pre_po_dt
                ,csh.status_code
                ,csh.prev_status_code
                ,csh.status_reason
                ,rank() over (partition by account_number order by po_dt) as rank_id
into           #cuscan_in_period
from  sk_prod.cust_subs_hist csh
WHERE           csh.status_start_dt between '2012-01-01' and '2013-05-03'
AND             csh.status_code_changed = 'Y'
AND             csh.status_code in ('PO')
AND             csh.prev_status_code in ('AC','PC','AB')
and             csh.effective_from_dt < effective_to_dt
and             csh.subscription_sub_type = 'DTV Primary Viewing'
;
Commit;

delete from #cuscan_in_period where rank_id>1
commit;

--select status_reason , count(*) as records from #cuscan_in_period group by status_reason order by records desc;

--select rank_id , count(*) as records from #cuscan_in_period group by rank_id

---All Vespa Panel Accounts Ever---

select distinct account_number into #vespa_accounts from vespa_analysts.SC2_intervals;

commit;
---Create table of those with Cuscan in the period who are on Vespa and Active as at end 25/12/13
--drop table v159_all_vespa_churners;
select a.account_number
,po_dt
,pre_po_dt
into #v159_all_vespa_churners_since_2012
from #vespa_accounts as a 
left outer join #cuscan_in_period as b
on a.account_number=b.account_number
where b.account_number is not null
;
commit;

select * into v159_all_vespa_churners_since_2012 from #v159_all_vespa_churners_since_2012; commit;
commit;

create hg index idx1 on v159_all_vespa_churners_since_2012(account_number);
commit;

--select count(*) from v159_all_vespa_churners_since_2012;
-----------------------------------------------------------------------------------------------------------------------------------------------------



--------------------------------------------------------------------------------
--PART A: Viewing Data (For Customers active in the snapshot period)
--------------------------------------------------------------------------------

---Get details of Programmes Watched 3+ Minutes of---
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;
--CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-01-01';  --Had to restart loop half way through
--SET @snapshot_start_dt  = '2012-01-01';  --Original
SET @snapshot_end_dt    = '2012-07-31';

SET @playback_snapshot_start_dt  = '2011-10-01';

/*
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @viewing_var_num_days = 29;
commit;
*/
----Phase I EPG Data---

IF object_ID ('V159_epg_data_since_oct_2011_PHASE_1') IS NOT NULL THEN
            DROP TABLE  V159_epg_data_since_oct_2011_PHASE_1
END IF;
SELECT       programme_trans_sk
            ,service_key
            ,Channel_Name
            , epg_title
            , duration
            ,Genre_Description
            ,Sub_Genre_Description
            ,epg_group_Name
            ,network_indicator
            , tx_date_utc
            , x_broadcast_Time_Of_Day
            ,pay_free_indicator
INTO  V159_epg_data_since_oct_2011_PHASE_1
FROM sk_prod.vespa_epg_dim
--select top 100 * from sk_prod.vespa_epg_dim where programme_trans_sk = 201204020000015017

WHERE (tx_date_utc between @playback_snapshot_start_dt  and  @snapshot_end_dt)
;
commit;

--select cast(tx_date_utc as date) as day_info , count(*) from V159_epg_data_since_oct_2011_PHASE_1 group by day_info order by day_info
--select cast(tx_date_utc as date) as day_info , count(*) from V159_epg_data_phase_2 group by day_info order by day_info


create hg index idx2 on V159_epg_data_since_oct_2011_PHASE_1(programme_trans_sk);
alter table  V159_epg_data_since_oct_2011_PHASE_1 Add channel_name_inc_hd       varchar(90);

update V159_epg_data_since_oct_2011_PHASE_1
set channel_name_inc_hd=b.channel_name_inc_hd
from V159_epg_data_since_oct_2011_PHASE_1 as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;
--select top 100 * from V159_epg_data_since_oct_2011_PHASE_1 where tx_date_utc<'2012-04-01'
--select top 500 * FROM sk_prod.Vespa_programme_schedule  where broadcast_start_date_time_utc <'2012-04-01'
Update V159_epg_data_since_oct_2011_PHASE_1
set channel_name_inc_hd =  
        case    when channel_name ='Sky Sports 1 HD' then 'Sky Sports 1'
                when channel_name ='Playhouse Disney' then 'Disney Junior'
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
                when channel_name ='alibi HD' then 'Alibi'
                when channel_name ='Cartoon Net HD' then 'Cartoon Network'
                when channel_name ='Star Plus HD' then 'Star Plus'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'               
                when channel_name ='Eurosport 2 HD' then 'Eurosport 2'
                when channel_name ='AnimalPlnt HD' then 'Animal Planet' 

               when channel_name ='ITV Wales' then 'ITV1' 
               when channel_name ='ITV1 Central SW' then 'ITV1' 
               when channel_name ='ITV HD' then 'ITV1' 
               when channel_name ='ITV' then 'ITV1' 
               when channel_name ='ITV Border' then 'ITV1' 
               when channel_name ='ITV Channel Is' then 'ITV1' 
               when channel_name ='MTV Live HD' then 'MTV Live' 
     when channel_name ='RTE TWO HD' then 'RTE TWO' 
     when channel_name ='Sky Oscars HD' then 'Sky Oscars' 
     when channel_name ='Sky ShowcseHD' then 'Sky Movies Showcase'
            when channel_name_inc_hd is not null then channel_name_inc_hd else channel_name end
;
commit;
--select Channel_Name_Inc_Hd , count(*) as records from V159_epg_data_since_oct_2011_PHASE_1 group by Channel_Name_Inc_Hd order by Channel_Name_Inc_Hd;
alter table  V159_epg_data_since_oct_2011_PHASE_1 Add Pay_channel       tinyint default 0;

   update V159_epg_data_since_oct_2011_PHASE_1
    set Pay_channel = 1
    from V159_epg_data_since_oct_2011_PHASE_1  
    where Channel_Name_Inc_Hd in
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
           'Ideal & More','CNToo','Disney Junior','Disney Junior+','Christmas 24','Christmas 24+','Sky Sports F1','Football First 1'
,'Football First 2','Football First 3','Football First 4','Football First 5','Football First 6','Sky Oscars')
    ;

commit;

--Add on media Pack Type--
alter table  V159_epg_data_since_oct_2011_PHASE_1 Add pack    varchar(25);

select channel_name
,max(media_pack) as pack
into #pack
from Project_161_viewing_table
group by channel_name
;

update V159_epg_data_since_oct_2011_PHASE_1
set pack = b.pack
from V159_epg_data_since_oct_2011_PHASE_1 as a
left outer join #pack as b
on a.channel_name=b.channel_name
;
commit;







----Phase II EPG Data
/*
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;
*/
--CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-01-01';  --Date for use for
--SET @snapshot_start_dt  = '2012-09-01';  --Original
SET @snapshot_end_dt    = '2013-05-01';

SET @playback_snapshot_start_dt  = '2012-01-01';

IF object_ID ('V159_epg_data_phase_2') IS NOT NULL THEN
            DROP TABLE  V159_epg_data_phase_2
END IF;
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
INTO  V159_epg_data_phase_2
FROM sk_prod.Vespa_programme_schedule
WHERE (tx_date_utc between @playback_snapshot_start_dt  and  @snapshot_end_dt)
;
commit;
--select max(tx_date_utc) from V159_epg_data_phase_2
--
/*
select channel_name 
, sum(case when  cast(broadcast_start_date_time_utc as date)='2013-01-26' then 1 else 0 end) as jan_26 
, sum(case when  cast(broadcast_start_date_time_utc as date)='2013-01-28' then 1 else 0 end) as jan_28 
from sk_prod.Vespa_programme_schedule
group by channel_name 
order by jan_26 desc

select *  
from sk_prod.Vespa_programme_schedule
where cast(broadcast_start_date_time_utc as date) between '2013-01-26' and '2013-01-28' and channel_name='Sky1'
order by broadcast_start_date_time_utc
*/


create hg index idx2 on V159_epg_data_phase_2(programme_trans_sk);
alter table  V159_epg_data_phase_2 Add channel_name_inc_hd       varchar(90);

update V159_epg_data_phase_2
set channel_name_inc_hd=b.channel_name_inc_hd
from V159_epg_data_phase_2 as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;
--select top 100 * from V159_epg_data_phase_2 where tx_date_utc<'2012-04-01'
--select top 500 * FROM sk_prod.Vespa_programme_schedule  where broadcast_start_date_time_utc <'2012-04-01'
Update V159_epg_data_phase_2
set channel_name_inc_hd =  
        case    when channel_name ='Sky Sports 1 HD' then 'Sky Sports 1'
                when channel_name ='Playhouse Disney' then 'Disney Junior'
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
                when channel_name ='alibi HD' then 'Alibi'
                when channel_name ='Cartoon Net HD' then 'Cartoon Network'
                when channel_name ='Star Plus HD' then 'Star Plus'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'
                when channel_name ='Sky Sports 2 HD' then 'Sky Sports 2'               
                when channel_name ='Eurosport 2 HD' then 'Eurosport 2'
                when channel_name ='AnimalPlnt HD' then 'Animal Planet' 

               when channel_name ='ITV Wales' then 'ITV1' 
               when channel_name ='ITV1 Central SW' then 'ITV1' 
               when channel_name ='ITV HD' then 'ITV1' 
               when channel_name ='ITV' then 'ITV1' 
               when channel_name ='ITV Border' then 'ITV1' 
               when channel_name ='ITV Channel Is' then 'ITV1' 
               when channel_name ='MTV Live HD' then 'MTV Live' 
     when channel_name ='RTE TWO HD' then 'RTE TWO' 
     when channel_name ='Sky Oscars HD' then 'Sky Oscars' 
     when channel_name ='Sky ShowcseHD' then 'Sky Movies Showcase'





            when channel_name_inc_hd is not null then channel_name_inc_hd else channel_name end
;
commit;
--select Channel_Name_Inc_Hd , count(*) as records from V159_epg_data_phase_2 group by Channel_Name_Inc_Hd order by Channel_Name_Inc_Hd;
alter table  V159_epg_data_phase_2 Add Pay_channel       tinyint default 0;

   update V159_epg_data_phase_2
    set Pay_channel = 1
    from V159_epg_data_phase_2  
    where Channel_Name_Inc_Hd in
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
           'Ideal & More','CNToo','Disney Junior','Disney Junior+','Christmas 24','Christmas 24+','Sky Sports F1','Football First 1'
,'Football First 2','Football First 3','Football First 4','Football First 5','Football First 6','Sky Oscars')
    ;

commit;

--Add on media Pack Type--
alter table  V159_epg_data_phase_2 Add pack    varchar(25);

select channel_name
,max(media_pack) as pack
into #pack
from Project_161_viewing_table
group by channel_name
;

update V159_epg_data_phase_2
set pack = b.pack
from V159_epg_data_phase_2 as a
left outer join #pack as b
on a.channel_name=b.channel_name
;
commit;

---Create Updates for Pack Type---
--select channel_name_inc_hd , genre_description, sub_genre_description , count(*) as records,sum(Pay_channel)  from V159_epg_data_since_oct_2011 group by channel_name_inc_hd,genre_description, sub_genre_description order by channel_name_inc_hd ,records desc;


--select channel_name ,pack , count(*),sum(Pay_channel) from V159_epg_data_since_oct_2011 group by channel_name,pack order by channel_name;
--create initial backup --
--select * into V159_Daily_viewing_summary_churners_backup from V159_Daily_viewing_summary_churners; commit;

------------

IF object_ID ('V159_Daily_viewing_summary_churners_since_2012_all') IS NOT NULL THEN
            DROP TABLE  V159_Daily_viewing_summary_churners_since_2012_all
END IF;

CREATE TABLE  V159_Daily_viewing_summary_churners_since_2012_all
    ( cb_row_ID                                         bigint       not null --primary key
            ,Account_Number                             varchar(20)  not null
            ,Subscriber_Id                              bigint
            ,programme_trans_sk                         bigint
            ,timeshifting                               varchar(15)
            ,viewing_starts                             datetime
            ,viewing_stops                              datetime
            ,viewing_Duration                           decimal(10,0)
            ,capped_flag                                tinyint
            ,capped_event_end_time                      datetime
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
commit;

IF object_ID ('V159_Daily_viewing_summary_churners_since_2012') IS NOT NULL THEN
            DROP TABLE  V159_Daily_viewing_summary_churners_since_2012
END IF;
--select * from V159_Daily_viewing_summary_churners_since_2012;
CREATE TABLE  V159_Daily_viewing_summary_churners_since_2012
    ( 
            Account_Number                             varchar(20)  not null
            ,viewing_day                      date
            ,viewing_post_6am                      tinyint
            ,viewing_Duration                     bigint
            ,viewing_Duration_live                     bigint
            ,total_duration_pay_exc_premiums                     bigint
            ,total_duration_premiums                     bigint
            ,total_duration_terrestrial                     bigint
            ,total_duration_free_non_terrestrial bigint
            ,total_duration_pay_football bigint
            ,total_duration_free_football bigint
            ,total_duration_pay_sport_exc_football bigint
            ,total_duration_free_sport_exc_football bigint
            ,total_duration_pay_movies bigint
            ,total_duration_free_movies bigint
            ,total_duration_pay_kids bigint
            ,total_duration_free_kids bigint

)
;
--delete from V159_Daily_viewing_summary_churners_since_2012 where viewing_day<'2012-08-01'; commit;


delete from V159_Daily_viewing_summary_churners_since_2012_all where account_number is null or account_number is not null;

commit;
create hg index idx1 on V159_Daily_viewing_summary_churners_since_2012_all(Account_Number);
create hg index idx2 on V159_Daily_viewing_summary_churners_since_2012_all(programme_trans_sk);
commit;
--select count(*) from  v159_all_vespa_churners_since_2012;
--select viewing_day , count(*) from V159_Daily_viewing_summary_churners_since_2012 group by viewing_day order by viewing_day;
-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-08-01';  --Had to restart loop half way through
--SET @snapshot_start_dt  = '2012-01-01';  --Original
SET @snapshot_end_dt    = '2012-08-13';

--select top 100 * from vespa_analysts.ph1_VESPA_DAILY_AUGS_20120130
--select top 100 * from V159_Tenure_10_16mth_Viewing
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into V159_Daily_viewing_summary_churners_since_2012_all(
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
)
        select
                a.cb_row_ID
                ,a.Account_Number
                ,a.Subscriber_Id
                ,a.programme_trans_sk
                ,a.timeshifting
                ,a.viewing_starts
                ,a.viewing_stops
                ,a.viewing_Duration
                ,a.capped_flag
                ,a.capped_event_end_time
from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as a

insert into V159_Daily_viewing_summary_churners_since_2012
select a.account_number
,min(cast(viewing_starts as date)) as viewing_date
,max(case when dateformat(viewing_starts,''HH'') in (''00'',''01'',''01'',''03'',''04'',''05'') then 0 else 1 end) as viewing_post_6am
,sum(viewing_duration) as total_duration
,sum(case when timeshifting=''LIVE'' then viewing_duration else 0 end) as viewing_duration_live

,sum(case when pay_channel = 1 and channel_name_inc_hd not in  (
''Football First 1''
,''Football First 2''
,''Football First 3''
,''Football First 4''
,''Football First 5''
,''Football First 6''
,''Sky Christmas''
,''Sky ChristmsHD''
,''Sky DramaRom''
,''Sky Movies 007''
,''Sky Movies Action''
,''Sky Movies Classics''
,''Sky Movies Comedy''
,''Sky Movies Family''
,''Sky Movies Indie''
,''Sky Movies Mdn Greats''
,''Sky Movies Sci-Fi/Horror''
,''Sky Movies Showcase''
,''Sky Movies Thriller''
,''Sky Prem+1''
,''Sky Premiere''
,''Sky ShowcseHD''
,''Sky Sports 1''
,''Sky Sports 2''
,''Sky Sports 3''
,''Sky Sports 4''
,''Sky Sports F1''
,''Christmas 24''
,''Christmas 24+''
,''Sky Oscars''
)
then viewing_duration else 0 end) as total_duration_pay_exc_premiums
,sum(case when channel_name_inc_hd in  (
''Football First 1''
,''Football First 2''
,''Football First 3''
,''Football First 4''
,''Football First 5''
,''Football First 6''
,''Sky Christmas''
,''Sky ChristmsHD''
,''Sky DramaRom''
,''Sky Movies 007''
,''Sky Movies Action''
,''Sky Movies Classics''
,''Sky Movies Comedy''
,''Sky Movies Family''
,''Sky Movies Indie''
,''Sky Movies Mdn Greats''
,''Sky Movies Sci-Fi/Horror''
,''Sky Movies Showcase''
,''Sky Movies Thriller''
,''Sky Prem+1''
,''Sky Premiere''
,''Sky ShowcseHD''
,''Sky Sports 1''
,''Sky Sports 2''
,''Sky Sports 3''
,''Sky Sports 4''
,''Sky Sports F1''
,''Christmas 24''
,''Christmas 24+''
,''Sky Oscars''
)
then viewing_duration else 0 end) as total_duration_premiums

,sum(case when channel_name_inc_hd in (''ITV1'',
''BBC ONE'',
''Channel 4'',
''BBC TWO'',
''Channel 5'')
then viewing_duration else 0 end) as total_duration_terrestrial

,sum(case when channel_name_inc_hd not in (''ITV1'',
''BBC ONE'',
''Channel 4'',
''BBC TWO'',
''Channel 5'') and pay_channel=0
then viewing_duration else 0 end) as total_duration_free_non_terrestrial

,sum(case when pay_channel = 1 and b.sub_genre_description=''Football'' then viewing_duration else 0 end) as total_duration_pay_football
,sum(case when pay_channel = 0 and b.sub_genre_description=''Football'' then viewing_duration else 0 end) as total_duration_free_football


,sum(case when pay_channel = 1 and b.genre_description=''Sports'' and b.sub_genre_description<>''Football'' then viewing_duration else 0 end) as total_duration_pay_sport_exc_football
,sum(case when pay_channel = 0 and b.genre_description=''Sports'' and b.sub_genre_description<>''Football'' then viewing_duration else 0 end) as total_duration_free_sport_exc_football

,sum(case when pay_channel = 1 and b.genre_description=''Movies''  then viewing_duration else 0 end) as total_duration_pay_movies
,sum(case when pay_channel = 0 and b.genre_description=''Movies''  then viewing_duration else 0 end) as total_duration_free_movies

,sum(case when pay_channel = 1 and b.genre_description=''Children''  then viewing_duration else 0 end) as total_duration_pay_kids
,sum(case when pay_channel = 0 and b.genre_description=''Children''  then viewing_duration else 0 end) as total_duration_free_kids

from V159_Daily_viewing_summary_churners_since_2012_all as a
left outer join V159_epg_data_since_oct_2011_PHASE_1 as b 
on a.programme_trans_sk=b.programme_trans_sk
left outer join  v159_all_vespa_churners_since_2012 as c
on a.account_number = c.account_number
where  c.account_number is not null
group by a.account_number

delete from V159_Daily_viewing_summary_churners_since_2012_all where account_number is null or account_number is not null
'
;
--select max (viewing_starts) from vespa_analysts.VESPA_DAILY_AUGS_20121104;
--select genre_description , count(*) from V159_epg_data_since_oct_2011 group by genre_description
--select top 100 * from V159_Tenure_10_16mth_Viewing;
--select top 100 * from  vespa_analysts.ph1_VESPA_DAILY_AUGS_20120814;

-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end


--select viewing_day , count(*) from V159_Daily_viewing_summary_churners_since_2012 group by viewing_day order by viewing_day ;
--select top 5000 * into V159_Daily_viewing_summary_churners_since_2012_test from V159_Daily_viewing_summary_churners_since_2012;commit;
commit;
--select dateformat(viewing_starts,'HH') as hourss , count(*) from vespa_analysts.VESPA_DAILY_AUGS_20130126 group by hourss
--select top 5000 * from V159_Daily_viewing_summary_churners_since_2012
--select viewing_day,count(*) from V159_Daily_viewing_summary_churners_since_2012 group by viewing_day order by viewing_day
--viewing_day,count()
--'2012-05-28',1778

--select * from V159_Daily_viewing_summary_churners_since_2012;format
commit;

--Create Single Account Summaries--

commit;
create  hg index idx1 on V159_Daily_viewing_summary_churners_since_2012 (account_number);
commit;

----Repeat Using Phase II data----
/* Recreate Variables as run on seperate day to code above
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;
*/


--'2013-02-04'
SET @snapshot_start_dt  = '2013-04-01'; --Rerrun from here (phase II starts 14th Aug 2012--
--SET @snapshot_start_dt  = '2012-08-14';  --Original
SET @snapshot_end_dt    = '2013-04-23';  -- Current Date of EPG Data
delete from V159_Daily_viewing_summary_churners_since_2012_all where account_number is null or account_number is not null
--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20130423
--select top 100 * from V159_Tenure_10_16mth_Viewing
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into V159_Daily_viewing_summary_churners_since_2012_all(
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
)
        select
                a.cb_row_ID
                ,a.Account_Number
                ,a.Subscriber_Id
                ,a.programme_trans_sk
                ,a.timeshifting
                ,a.viewing_starts
                ,a.viewing_stops
                ,a.viewing_Duration
                ,a.capped_flag
                ,a.capped_event_end_time
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as a

insert into V159_Daily_viewing_summary_churners_since_2012
select a.account_number
,min(cast(viewing_starts as date)) as viewing_date
,max(case when dateformat(viewing_starts,''HH'') in (''00'',''01'',''01'',''03'',''04'',''05'') then 0 else 1 end) as viewing_post_6am
,sum(viewing_duration) as total_duration
,sum(case when timeshifting=''LIVE'' then viewing_duration else 0 end) as viewing_duration_live

,sum(case when pay_channel = 1 and channel_name_inc_hd not in  (
''Football First 1''
,''Football First 2''
,''Football First 3''
,''Football First 4''
,''Football First 5''
,''Football First 6''
,''Sky Christmas''
,''Sky ChristmsHD''
,''Sky DramaRom''
,''Sky Movies 007''
,''Sky Movies Action''
,''Sky Movies Classics''
,''Sky Movies Comedy''
,''Sky Movies Family''
,''Sky Movies Indie''
,''Sky Movies Mdn Greats''
,''Sky Movies Sci-Fi/Horror''
,''Sky Movies Showcase''
,''Sky Movies Thriller''
,''Sky Prem+1''
,''Sky Premiere''
,''Sky ShowcseHD''
,''Sky Sports 1''
,''Sky Sports 2''
,''Sky Sports 3''
,''Sky Sports 4''
,''Sky Sports F1''
,''Christmas 24''
,''Christmas 24+''
,''Sky Oscars''
)
then viewing_duration else 0 end) as total_duration_pay_exc_premiums
,sum(case when channel_name_inc_hd in  (
''Football First 1''
,''Football First 2''
,''Football First 3''
,''Football First 4''
,''Football First 5''
,''Football First 6''
,''Sky Christmas''
,''Sky ChristmsHD''
,''Sky DramaRom''
,''Sky Movies 007''
,''Sky Movies Action''
,''Sky Movies Classics''
,''Sky Movies Comedy''
,''Sky Movies Family''
,''Sky Movies Indie''
,''Sky Movies Mdn Greats''
,''Sky Movies Sci-Fi/Horror''
,''Sky Movies Showcase''
,''Sky Movies Thriller''
,''Sky Prem+1''
,''Sky Premiere''
,''Sky ShowcseHD''
,''Sky Sports 1''
,''Sky Sports 2''
,''Sky Sports 3''
,''Sky Sports 4''
,''Sky Sports F1''
,''Christmas 24''
,''Christmas 24+''
,''Sky Oscars''
)
then viewing_duration else 0 end) as total_duration_premiums

,sum(case when channel_name_inc_hd in (''ITV1'',
''BBC ONE'',
''Channel 4'',
''BBC TWO'',
''Channel 5'')
then viewing_duration else 0 end) as total_duration_terrestrial

,sum(case when channel_name_inc_hd not in (''ITV1'',
''BBC ONE'',
''Channel 4'',
''BBC TWO'',
''Channel 5'') and pay_channel=0
then viewing_duration else 0 end) as total_duration_free_non_terrestrial

,sum(case when pay_channel = 1 and b.sub_genre_description=''Football'' then viewing_duration else 0 end) as total_duration_pay_football
,sum(case when pay_channel = 0 and b.sub_genre_description=''Football'' then viewing_duration else 0 end) as total_duration_free_football


,sum(case when pay_channel = 1 and b.genre_description=''Sports'' and b.sub_genre_description<>''Football'' then viewing_duration else 0 end) as total_duration_pay_sport_exc_football
,sum(case when pay_channel = 0 and b.genre_description=''Sports'' and b.sub_genre_description<>''Football'' then viewing_duration else 0 end) as total_duration_free_sport_exc_football

,sum(case when pay_channel = 1 and b.genre_description=''Movies''  then viewing_duration else 0 end) as total_duration_pay_movies
,sum(case when pay_channel = 0 and b.genre_description=''Movies''  then viewing_duration else 0 end) as total_duration_free_movies

,sum(case when pay_channel = 1 and b.genre_description=''Children''  then viewing_duration else 0 end) as total_duration_pay_kids
,sum(case when pay_channel = 0 and b.genre_description=''Children''  then viewing_duration else 0 end) as total_duration_free_kids

from V159_Daily_viewing_summary_churners_since_2012_all as a
left outer join V159_epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
left outer join  v159_all_vespa_churners_since_2012 as c
on a.account_number = c.account_number
where  c.account_number is not null
group by a.account_number

delete from V159_Daily_viewing_summary_churners_since_2012_all where account_number is null or account_number is not null
'
;
--select max (viewing_starts) from vespa_analysts.VESPA_DAILY_AUGS_20130228;
--select genre_description , count(*) from V159_epg_data_since_oct_2011 group by genre_description
--select top 100 * from V159_Tenure_10_16mth_Viewing;
--select top 100 * from  vespa_analysts.VESPA_DAILY_AUGS_20130418;

-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end


--select viewing_day , count(*) from V159_Daily_viewing_summary_churners_since_2012 group by viewing_day order by viewing_day ;

--select count(*) from  vespa_analysts.VESPA_DAILY_AUGS_20130201;
--select top 5000 * into V159_Daily_viewing_summary_churners_since_2012_test from V159_Daily_viewing_summary_churners_since_2012;commit;
commit;

---Add on Date of Churn----

alter table V159_Daily_viewing_summary_churners_since_2012 add churn_date date;

update V159_Daily_viewing_summary_churners_since_2012
set churn_date = b.po_dt
from V159_Daily_viewing_summary_churners_since_2012 as a
left outer join v159_all_vespa_churners_since_2012 as b
on a.account_number = b.account_number
--Only Add on where customer has the full 31 day notice period
where po_dt-pre_po_dt>=32
;

commit;





---Get list of All Phase 1 accounts

select account_number
,min (viewing_day) as first_day_return_data
into #first_date
from V159_Daily_viewing_summary_churners_since_2012
group by account_number
;


create hg index idx1 on #first_date(account_number);
commit;

alter table V159_Daily_viewing_summary_churners_since_2012 add first_day_return_data date;

update V159_Daily_viewing_summary_churners_since_2012
set first_day_return_data = b.first_day_return_data
from V159_Daily_viewing_summary_churners_since_2012 as a
left outer join #first_date as b
on a.account_number = b.account_number
;

---Add on Reason for churn---

--v159_all_vespa_churners_since_2012

--Create Reason lookup from TA activity

 SELECT    Wh_Attempt_Reason_Description_1
            ,Wh_Attempt_Reason_Description_2
         ,count(*) AS TA_attempts
into #ta_lookup
    FROM sk_prod.cust_change_attempt AS cca
   WHERE cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'

     AND cca.attempt_date                        >= '2012-01-01'
     AND cca.created_by_id                  NOT IN ('dpsbtprd', 'batchuser')
     AND cca.Wh_Attempt_Outcome_Description_1 in (  'Turnaround Saved'
                                                   ,'Legacy Save'
                                                   ,'Turnaround Not Saved'
                                                   ,'Legacy Fail'
                                                   ,'Home Move Saved'
                                                   ,'Home Move Not Saved'
                                                   ,'Home Move Accept Saved')
   GROUP BY Wh_Attempt_Reason_Description_1
            ,Wh_Attempt_Reason_Description_2
order by ta_attempts desc;

select * from  #ta_lookup;



create table project159_churn_reason_lookup (
level_2_reason varchar (150)
,level_1_reason varchar (150)
)


insert into project159_churn_reason_lookup (
level_2_reason
,level_1_reason
)

select 'Need to Reduce outgoings', 'Financial Situation'
union select 'Children Have Left Home', 'Changed Circumstances'
union select 'Unwilling to discuss', 'Unknown Reason'
union select 'Can''t have Sky at new address', 'Moving Home'
union select 'Deceased', 'Accessibility'
union select 'Bundle - Cost', 'Financial Situation'
union select 'Moving Abroad', 'Moving Home'
union select 'Not watching enough', 'Content Dissatisfaction'
union select 'Tarriff Offer', 'Moving Home'
union select 'Equipment Offer', 'Moving Home'
union select 'Canceling and Re-Subscribing at New Address', 'Moving Home'
union select 'Reduced Hours', 'Financial Situation'
union select 'Existing Subscriber Moving In', 'Changed Circumstances'
union select 'Competitor - Cost', 'Financial Situation'
union select 'Divorced', 'Changed Circumstances'
union select 'Other', 'Other'
union select 'Redundancy', 'Financial Situation'
union select 'Redundancy/ Retirement', 'Financial Situation'
union select 'TV Cost', 'Financial Situation'
union select 'Long Term Incapacity', 'Changed Circumstances'
union select 'Fraud or Bankruptcy', 'Financial Situation'
union select 'AWOL', 'Non standard Account'
union select 'Cooling Off Period', 'Customer Relations ONLY'
union select '31 Day Notice Period', 'Other Dissatisfaction'
union select 'Indeterminate Reason (9100)', 'Cancel Other'
union select 'Flooding', 'Other'
union select 'Armed forces', 'Changed Circumstances'
union select 'Temporary Accommodation', 'Moving Home'
union select 'Broadband', 'Product Dissatisfaction'
union select 'Already in contract', 'Other'
union select 'Don''t Watch Enough', 'Other'
union select 'Moving to Old Persons Home', 'Accessibility'
union select 'Bundle - Features', 'Other'
union select 'Viewing Cards Admin', 'Other'
union select 'Subs Offer', 'Other'
union select 'TV Content', 'Other'
union select 'Long term incapacity (6 months or over)', 'Changed Circumstances'
union select 'Not enough choice', 'Content Dissatisfaction'
union select 'Repeated Technical Issue', 'Other'
union select 'Customer mislead (9160/13E)', 'Cancel Other'
union select 'Virgin', 'Competitor Offerings'
union select 'Package Downgrade', 'Turnaround Other'
union select 'TV Equipment', 'Product Dissatisfaction'
union select 'Alternative Product/Package/Offer', 'Other'
union select 'End of favourite show', 'Content Dissatisfaction'
union select 'Box Office', 'Other'
union select 'Unable to reinstall', 'Other'
union select 'Financial Circumstances', 'Financial Situation'
union select 'First time issue', 'Financial Situation'
union select 'Can''t afford it', 'Financial Situation'
;

commit;
--drop table #churn_reason;
select account_number
,min(level_1_reason) as churn_reason
into #churn_reason
from #cuscan_in_period as a
left outer join project159_churn_reason_lookup as b
on a.status_reason = b.level_2_reason
group by account_number
;
commit;
--alter table V159_Daily_viewing_summary_churners_since_2012 delete churn_reason;
alter table V159_Daily_viewing_summary_churners_since_2012 add churn_reason varchar(150);

update V159_Daily_viewing_summary_churners_since_2012
set churn_reason = b.churn_reason
from V159_Daily_viewing_summary_churners_since_2012 as a
left outer join #churn_reason as b
on a.account_number = b.account_number
;
commit;
--select * from project159_churn_reason_lookup;
--select * from v159_all_vespa_churners_since_2012;

----Add on Account Demographics/Package Info----



/*
select first_day_return_data
,count(*) as accounts
from #first_date
group by first_day_return_data
order by first_day_return_data
;
*/

--Proportion pay by Day


---repeat but with days since Cuscan as a metric---
--drop table v159_summary_by_day_pre_churn;
select a.account_number 
,churn_date
,churn_date-viewing_day as days_pre_churn
,first_day_return_data
,churn_reason
,sum(viewing_duration) as total_viewed
,sum(total_duration_terrestrial+total_duration_free_non_terrestrial) as total_free
,sum( viewing_duration-(total_duration_terrestrial+total_duration_free_non_terrestrial)) as total_pay
,cast(total_pay as real)/(cast(total_free as real)+cast(total_pay as real)) as proportion_pay
,max(viewing_post_6am) as any_viewing_post_6am
,min(case when first_day_return_data<'2012-08-14' then '01: Phase 1 Account' else '02: Phase 2 Account' end) as account_type
into v159_summary_by_day_pre_churn
from V159_Daily_viewing_summary_churners_since_2012 as a
where  churn_date is not null
GROUP BY a.account_number 
,churn_date
,days_pre_churn
,first_day_return_data
,churn_reason
;
commit;

---Add on Flag to indicate if Phase 1 or Phase 2 Panel Member--
commit;


--select count(*) from v159_summary_by_day_pre_churn where churn_date is null







--select top 1000 * from v159_summary_by_day_pre_churn order by churn_date desc, account_number desc, days_pre_churn  -- where days_pre_churn=1;


/*
select a.*, b.channel_name_inc_hd from vespa_analysts.VESPA_DAILY_AUGS_20130419  as a
left outer join V159_epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
where account_number = '630128553843'
order by viewing_starts


*/



select days_pre_churn
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1
group by days_pre_churn
order by days_pre_churn
;

---Split by churn reason--
select days_pre_churn
,case when churn_reason in ('Financial Situation','Moving Home') then churn_reason else 'Other' end as reason
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1
group by days_pre_churn
,reason
order by days_pre_churn
;

---Panel 1 only----
select days_pre_churn
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1 and account_type = '01: Phase 1 Account'
group by days_pre_churn
order by days_pre_churn
;


--Panel Phase 2--
select days_pre_churn
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1 and account_type <> '01: Phase 1 Account'
group by days_pre_churn
order by days_pre_churn
;

commit;




commit;

select churn_date
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1
group by churn_date
order by churn_date
;


select churn_date
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn=14
group by churn_date
order by churn_date
;

select days_pre_churn
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1 and churn_date between '2012-12-01' and '2012-12-31'
group by days_pre_churn
order by days_pre_churn
;


commit;



select po_dt ,count(*) ,sum(case when po_dt-pre_po_dt=32 then 1 else 0 end) as days_to from v159_all_vespa_churners_since_2012 as b group by po_dt order by po_dt

select po_dt-pre_po_dt as days_to ,count(*)  from v159_all_vespa_churners_since_2012 as b group by days_to order by days_to

select pre_po_dt as days_to ,pre_po_dt+32 as extra ,count(*)  from v159_all_vespa_churners_since_2012 as b where po_dt = '2013-04-02' group by days_to,extra order by days_to

where po_dt<='2013-02-24' and po_dt-pre_po_dt>=32

select top 100 * from sk_prod.sky_calendar

select shs_year_quarter, min(calendar_date) from sk_prod.sky_calendar group by shs_year_quarter order by shs_year_quarter


select po_dt , count(*)  from v159_all_vespa_churners_since_2012 as b
where po_dt-pre_po_dt>=32
group by po_dt order by po_dt

commit;



select churn_reason
--,count(distinct ) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1
group by churn_reason
order by accounts desc
;
