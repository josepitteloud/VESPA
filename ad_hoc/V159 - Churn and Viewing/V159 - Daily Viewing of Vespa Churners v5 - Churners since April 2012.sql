


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

----Add on Account Demographics/Package Info----

--select count(*) , count(distinct account_number) from v159_all_vespa_churners_since_2012;

---Add on Package, Tenure, Products as at Start of PC date---

--Create Package Details as at day prior to Pending Cancel---
--drop table #mixes;
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
       left outer JOIN sk_prod.cust_entitlement_lookup as cel
               ON csh.current_short_description = cel.short_description
left outer join v159_all_vespa_churners_since_2012 as c
on csh.account_number = c.account_number
where c.account_number is not null
 and csh.subscription_sub_type ='DTV Primary Viewing'
   AND csh.subscription_type = 'DTV PACKAGE'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= pre_po_dt-1
   AND csh.effective_to_dt   >  pre_po_dt-1
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
alter table v159_all_vespa_churners_since_2012 add prem_sports integer default 0;
alter table v159_all_vespa_churners_since_2012 add prem_movies integer default 0;
alter table v159_all_vespa_churners_since_2012 add mixes_type varchar(30) default 'Unknown';

update v159_all_vespa_churners_since_2012 
set prem_sports=b.prem_sports
,prem_movies=b.prem_movies
,mixes_type=b.new_package
from v159_all_vespa_churners_since_2012  as a
left outer join #mixes as b
on a.account_number=b.account_number
;
commit;

-------------------------------------------------  02 - Active MR AND HD Subscription
--code_location_08
SELECT  csh.account_number
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS multiroom
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS hdtv
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS skyplus
INTO v159_MR_HD
      FROM sk_prod.cust_subs_hist AS csh 
left outer join v159_all_vespa_churners_since_2012 as c
on csh.account_number = c.account_number
where c.account_number is not null
     and csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD'
                                         ,'DTV Sky+')
       AND csh.effective_from_dt <> csh.effective_to_dt
   AND csh.effective_from_dt <= pre_po_dt-1
   AND csh.effective_to_dt   >  pre_po_dt-1
GROUP BY csh.account_number;
commit;

commit;
create  hg index idx1 on v159_MR_HD (account_number);
alter table v159_all_vespa_churners_since_2012 add hdtv                    tinyint          default 0    ;     
alter table v159_all_vespa_churners_since_2012 add multiroom                    tinyint          default 0    ;     
alter table v159_all_vespa_churners_since_2012 add skyplus                    tinyint          default 0    ;     
commit;


update v159_all_vespa_churners_since_2012
set hdtv=b.hdtv
,multiroom=b.multiroom
,skyplus=b.skyplus
from v159_all_vespa_churners_since_2012 as a
left outer join v159_MR_HD as b
on a.account_number=b.account_number
;
commit;
drop table v159_MR_HD;
commit;

--select top 100 * from v159_live_playback_viewing;

----Add on extra variables from product holdings and consumerview---

alter table v159_all_vespa_churners_since_2012 add talk_product              VARCHAR(50)     default 'NA' ;        -- Current Sky Talk product
--alter table v159_all_vespa_churners_since_2012 add sky_id                    bigint          default 0    ;        -- Sky id created
alter table v159_all_vespa_churners_since_2012 add distinct_usage_days                INTEGER         default 0     ;       -- Sky Go days in 3mth period
alter table v159_all_vespa_churners_since_2012 add usage_records                INTEGER         default 0     ;       -- Sky Go usage records in 3mth period
alter table v159_all_vespa_churners_since_2012 add BB_type                   VARCHAR(50)     default 'NA'  ;       -- Current BB product
alter table v159_all_vespa_churners_since_2012 add Anytime_plus              INTEGER         default 0    ;        -- Anytime+ activated
alter table v159_all_vespa_churners_since_2012 add isba_tv_region             VARCHAR(50)     default 'Unknown'         ;   
alter table v159_all_vespa_churners_since_2012 add cb_key_household           bigint   ;        -- Current Sky Talk product
--drop table nodupes;
commit;

update v159_all_vespa_churners_since_2012
set isba_tv_region=b.isba_tv_region
,cb_key_household=b.cb_key_household
from v159_all_vespa_churners_since_2012 as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;
--select top 100 * from v159_all_vespa_churners_since_2012;
-------------------------------------------------  02 - Active Sky Talk
--code_location_09
--drop table talk;
--commit;

SELECT DISTINCT csh.account_number
       ,CASE WHEN UCASE(current_product_description) LIKE '%UNLIMITED%'
             THEN 'Unlimited'
             ELSE 'Freetime'
          END as talk_product
      ,rank() over(PARTITION BY csh.account_number ORDER BY effective_to_dt desc) AS rank_id
      ,effective_to_dt
         INTO talk
FROM sk_prod.cust_subs_hist AS CSH
left outer join v159_all_vespa_churners_since_2012 as c
on csh.account_number = c.account_number
where c.account_number is not null
and subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
   AND csh.effective_from_dt <= pre_po_dt-1
   AND csh.effective_to_dt   >  pre_po_dt-1
GROUP BY csh.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update AdSmart file
UPDATE v159_all_vespa_churners_since_2012
SET  talk_product = talk.talk_product
FROM v159_all_vespa_churners_since_2012  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
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
    inner join v159_all_vespa_churners_since_2012 AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= pre_po_dt-1
   AND csh.effective_to_dt > pre_po_dt-1
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
--select top 500 * from  v159_all_vespa_churners_since_2012;
--      update v159_all_vespa_churners_since_2012 file
UPDATE v159_all_vespa_churners_since_2012
SET  BB_type = BB.BB_type
FROM v159_all_vespa_churners_since_2012  AS Base
  INNER JOIN BB AS BB
        ON base.account_number = BB.account_number
            ORDER BY base.account_number;
commit;


drop table bb; commit;
DROP TABLE BBB; commit;


--select top 500 * from v159_all_vespa_churners_since_2012;
----Update Nulls to 0---

update v159_all_vespa_churners_since_2012
set hdtv=case when hdtv is null then 0 else hdtv end
,multiroom=case when multiroom is null then 0 else multiroom end
,skyplus=case when skyplus is null then 0 else skyplus end
from v159_all_vespa_churners_since_2012
;
commit;

--select sum(hdtv) from v159_all_vespa_churners_since_2012

---Create Table With Affluence HH Details (Current status)----
--select *  FROM sk_prod.EXPERIAN_CONSUMERVIEW where cb_address_postcode = 'HP23 5PS' and cb_address_buildingno='6'
--select cb_change_date , count(*) from sk_prod.EXPERIAN_CONSUMERVIEW group by cb_change_date;

select cb_key_household
,max(h_household_composition) as hh_composition
,max(h_affluence_v2) as hh_affluence
,max(h_age_coarse) as head_hh_age
,max(h_number_of_children_in_household_2011) as num_children_in_hh
,max(h_number_of_adults) as number_of_adults
,max(h_number_of_bedrooms) as number_of_bedrooms
,max(h_length_of_residency) as length_of_residency
,max(h_residence_type_v2) as residence_type
,max(h_tenure_v2) as own_rent_status
into #experian_hh_summary
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV
where cb_change_date='2013-04-23'
and cb_address_status = '1' and cb_address_dps IS NOT NULL and cb_address_organisation IS NULL
group by cb_key_household;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary', 'cb_key_household';
commit;

---Add HH Key to Account Table---
--alter table v159_all_vespa_churners_since_2012 add cb_key_household           bigint   ;        -- Current Sky Talk product

update v159_all_vespa_churners_since_2012
set cb_key_household=b.cb_key_household
from v159_all_vespa_churners_since_2012 as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;

---Add Experian Values to main account table
alter table v159_all_vespa_churners_since_2012 add hh_composition             VARCHAR(2)     default 'U'         ;   
alter table v159_all_vespa_churners_since_2012 add hh_affluence             VARCHAR(2)     default 'U'         ;   
alter table v159_all_vespa_churners_since_2012 add head_hh_age             VARCHAR(1)     default 'U'         ;   
alter table v159_all_vespa_churners_since_2012 add num_children_in_hh             VARCHAR(1)            ;   

alter table v159_all_vespa_churners_since_2012 add number_of_adults            bigint         ;   
alter table v159_all_vespa_churners_since_2012 add number_of_bedrooms             VARCHAR(1)            ;   
alter table v159_all_vespa_churners_since_2012 add length_of_residency             VARCHAR(2)           ;  
alter table v159_all_vespa_churners_since_2012 add residence_type             VARCHAR(1)            ;   
alter table v159_all_vespa_churners_since_2012 add own_rent_status             VARCHAR(1)            ;   


update v159_all_vespa_churners_since_2012
set hh_composition=b.hh_composition
,hh_affluence=b.hh_affluence
,head_hh_age=b.head_hh_age
,num_children_in_hh=b.num_children_in_hh

,number_of_adults=b.number_of_adults
,number_of_bedrooms=b.number_of_bedrooms
,length_of_residency=b.length_of_residency

,residence_type=b.residence_type
,own_rent_status=b.own_rent_status

from v159_all_vespa_churners_since_2012 as a
left outer join #experian_hh_summary as b
on a.cb_key_household=b.cb_key_household
;
commit;

case when cast(dateformat(activation_date,'DD') as integer)>26 then 
     datediff(mm,activation_date,cast('2012-12-26' as date))-1 else datediff(mm,activation_date,cast('2012-12-26' as date)) end as full_months_tenure

alter table  v159_all_vespa_churners_since_2012 add activation_date date;
update v159_all_vespa_churners_since_2012
set  activation_date =b.ph_subs_first_activation_dt
from v159_all_vespa_churners_since_2012  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

alter table  v159_all_vespa_churners_since_2012 add full_months_tenure integer;
update v159_all_vespa_churners_since_2012
set  full_months_tenure =case when cast(dateformat(activation_date,'DD') as integer)> cast(dateformat(pre_po_dt-1,'DD') as integer) then 
     datediff(mm,activation_date,pre_po_dt)-1 else datediff(mm,activation_date,pre_po_dt) end
from v159_all_vespa_churners_since_2012  
;
commit;

--select top 500 * from v159_all_vespa_churners_since_2012  ;




--select full_months_tenure , count(*) from v159_all_vespa_churners_since_2012 group by full_months_tenure order by full_months_tenure;


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

commit;


--select count(*) from v159_summary_by_day_pre_churn where churn_date is null

----Output----

---Overall--

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

----Split by Churn Reason 
select days_pre_churn
,case when churn_reason in ('Financial Situation','Moving Home') then churn_reason else 'Other' end as reason
,count(*) as accounts
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1
group by days_pre_churn,reason
order by days_pre_churn,reason
;


commit;

----Split by DTV Package---


select days_pre_churn
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' when mixes_type ='Entertainment Extra' 
                                   then 'e) No Premium, Entertainment Extra' else 'f) No Premiums, Entertainment Only'
                                   end as dtv_package_type
,count(*) as accounts
,count(distinct a.account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn as a
left outer join v159_all_vespa_churners_since_2012 as b 
on a.account_number=b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1
group by days_pre_churn,dtv_package_type
order by days_pre_churn,dtv_package_type
;




---Split by Tenure---

select days_pre_churn
,full_months_tenure
,count(*) as accounts
,count(distinct a.account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn as a
left outer join v159_all_vespa_churners_since_2012 as b 
on a.account_number=b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1 and days_pre_churn<=90
group by days_pre_churn,full_months_tenure
order by days_pre_churn,full_months_tenure
;

select full_months_tenure
,sum(case when days_pre_churn=32 then 1 else 0 end) as accounts_with_viewing_32_days_pre_churn
,sum(case when days_pre_churn=1 then 1 else 0 end) as accounts_with_viewing_01_day_pre_churn
from v159_summary_by_day_pre_churn as a
left outer join v159_all_vespa_churners_since_2012 as b 
on a.account_number=b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1
group by full_months_tenure
order by full_months_tenure
;


---SPlit by Own Rent Status

select days_pre_churn
,case when own_rent_status = '0' then 'a) Owner occupied'
when own_rent_status = '1' then 'b) Privately rented'
when own_rent_status = '2' then 'c) Council / housing association' else 'd) Unknown' end as own_rent_type
,count(*) as accounts
,count(distinct a.account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn as a
left outer join v159_all_vespa_churners_since_2012 as b 
on a.account_number=b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1
group by days_pre_churn,own_rent_type
order by days_pre_churn,own_rent_type
;

commit;

----Pivot looking at number of records returning data on day -32 and day 1 pre churn----

select case when own_rent_status = '0' then 'a) Owner occupied'
when own_rent_status = '1' then 'b) Privately rented'
when own_rent_status = '2' then 'c) Council / housing association' else 'd) Unknown' end as own_rent_type

,case  when residence_type IN ('0')       THEN '1) Detached'
        when residence_type IN ('1')       THEN '2) Semi-detached'
        when residence_type IN ('2')       THEN '3) Bungalow'
        when residence_type IN ('3')       THEN '4) Terraced'
        when residence_type IN ('4')       THEN '5) Flat'
        when residence_type IN ('U')       THEN '6) Unclassified' else '6) Unclassified'
                                                END as property_type 
,  case         WHEN hh_affluence IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN hh_affluence IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN hh_affluence IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN hh_affluence IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN hh_affluence IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN hh_affluence IN ('15','16','17')       THEN 'F) High'
                                                WHEN hh_affluence IN ('18','19')            THEN 'G) Very High' else 'H) Unknown' END as affluence
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' else 'e) No Premiums' end as premiums_type
,case   when bb_type<>'NA' and talk_product<>'NA' then 'a) BB/Talk/TV'
        when bb_type<>'NA' then 'b) BB/TV'
        when talk_product<>'NA' then 'c) Talk/TV'
        else 'd) TV Only' end as bb_talk_holdings
, case when hdtv is null then 0 else hdtv end as has_hdtv
,case when multiroom is null then 0 else multiroom end as has_multiroom
,case when skyplus is null then 0 else skyplus end as has_skyplus
,case when length_of_residency in ('00','01') then 'a) <2 Years'
when length_of_residency in ('02') then 'b) 2 Years'
when length_of_residency in ('03','04','05') then 'c) 3-5 Years'
when length_of_residency in ('06','07','08','09','10','11') then 'd) 6+ Years' else 'e) Unknown' end as residency_length
,case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' when mixes_type ='Entertainment Extra' 
                                   then 'e) No Premium, Entertainment Extra' else 'f) No Premiums, Entertainment Only'
                                   end as dtv_package_type
,case when churn_reason in ('Financial Situation','Moving Home') then churn_reason else 'Other' end as reason_for_churn
,sum(case when days_pre_churn=32 then 1 else 0 end) as accounts_with_viewing_32_days_pre_churn
,sum(case when days_pre_churn=1 then 1 else 0 end) as accounts_with_viewing_01_day_pre_churn
from v159_summary_by_day_pre_churn as a
left outer join v159_all_vespa_churners_since_2012 as b 
on a.account_number=b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1
group by own_rent_type
,property_type 
,affluence
, premiums_type
,bb_talk_holdings
,has_hdtv
,has_multiroom
,has_skyplus
,residency_length
,dtv_package_type
,reason_for_churn
;


---Cust Bills---














/*
---Test of Code--
select case when prem_sports=2 and prem_movies=2 then 'a) All Premiums'
                                   when prem_sports=2 then 'b) Duel Sports'
                                   when prem_movies=2 then 'c) Duel Movies'
                                   when prem_sports+prem_movies>0 then 'd) Other Premiums' when mixes_type ='Entertainment Extra' 
                                   then 'e) No Premium, Entertainment Extra' else 'f) No Premiums, Entertainment Only'
                                   end as dtv_package_type
,count(*) as records
,count(distinct a.account_number) as accounts
from v159_summary_by_day_pre_churn as a
left outer join v159_all_vespa_churners_since_2012 as b 
on a.account_number=b.account_number
group by dtv_package_type
order by dtv_package_type
;

select * from v159_summary_by_day_pre_churn where account_number = '630128553843' order by days_pre_churn
select * from sk_prod.cust_bills where account_number = '630128553843' order by sequence_num
*/

















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
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1 and churn_date between '2012-12-01' and '2012-12-31'
and days_pre_churn<=210
group by days_pre_churn
order by days_pre_churn
;


commit;

----Figures for Gavin/Tony 17th June 2013
--Panel Phase 2--
select days_pre_churn
,count(distinct account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn
where days_pre_churn>0 and any_viewing_post_6am=1 and account_type <> '01: Phase 1 Account'
and days_pre_churn<=211
group by days_pre_churn
order by days_pre_churn
;

commit;

--select top 100 * from v159_summary_by_day_pre_churn;

---Get Value Segment for 210 Days Pre Churn Date---

select account_number
,churn_date
,churn_date-211 as value_segment_date
into #dates_for_value_segment
from v159_summary_by_day_pre_churn
group by account_number
,churn_date
,value_segment_date
;
commit;
create hg index idx1 on #dates_for_value_segment(account_number);
commit;

--drop table #nearest_value_seg_date;
select a.account_number
,max(value_seg_date) as nearest_value_seg_date
into #nearest_value_seg_date
from #dates_for_value_segment as a
left outer join sk_prod.VALUE_SEGMENTS_FIVE_YRS as b
on a.account_number = b.account_number
where value_seg_date<value_segment_date
group by a.account_number
;

commit;
create hg index idx1 on #nearest_value_seg_date(account_number);
commit;

--drop table #value_segment;
select a.account_number
,min(value_segment) as nearest_value_segment
into #value_segment
from #nearest_value_seg_date as a
left outer join sk_prod.VALUE_SEGMENTS_FIVE_YRS as b
on a.account_number = b.account_number and a.nearest_value_seg_date = b.value_seg_date
group by a.account_number
;

commit;
create hg index idx1 on #value_segment(account_number);
commit;
--select top 100 * from sk_prod.VALUE_SEGMENTS_FIVE_YRS
--select top 100 * from #value_segment

--select nearest_value_segment, count(*)  from #value_segment group by nearest_value_segment order by nearest_value_segment;

commit;

-----Look at Pay Viewing % split by value Segment---

--Bedding In
select days_pre_churn
,count(distinct a.account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn as a
left outer join #value_segment as b
on a.account_number = b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1 and account_type <> '01: Phase 1 Account'
and days_pre_churn<=211
and b.nearest_value_segment in ('Bedding In','F) Bedding In','G) Bedding In')
group by days_pre_churn
order by days_pre_churn
;

--Basic Loyal
select days_pre_churn
,count(distinct a.account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn as a
left outer join #value_segment as b
on a.account_number = b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1 and account_type <> '01: Phase 1 Account'
and days_pre_churn<=211
and b.nearest_value_segment in ('Bronze'    
,'Copper'    
,'D) Bronze'
,'E) Copper'
,'Tin')
group by days_pre_churn
order by days_pre_churn
;


--Premium Loyal
select days_pre_churn
,count(distinct a.account_number) as dist_ac
,avg(proportion_pay) as av_proportion_pay
,sum(total_pay) as total_pay_seconds
,sum(total_free) as total_free_seconds
from v159_summary_by_day_pre_churn as a
left outer join #value_segment as b
on a.account_number = b.account_number
where days_pre_churn>0 and any_viewing_post_6am=1 and account_type <> '01: Phase 1 Account'
and days_pre_churn<=211
and b.nearest_value_segment in ('A) Platinum'  
,'B) Gold'      
,'C) Silver'    
,'Gold'      
,'Platinum'  
,'Silver')
group by days_pre_churn
order by days_pre_churn
;
commit;
/*
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
*/