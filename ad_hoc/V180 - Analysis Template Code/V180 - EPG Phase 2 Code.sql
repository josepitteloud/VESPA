
--V180 EPG Phase 2 Code---

---Rather than creating EPG tables for Every project, this creates a table that can be used accross projects that use Phase 2 Data--

---Will Need to be re-run (with end date changed for any newer projects using data beyond that currently in the table)---


---Created by D Barnett---

----Phase II EPG Data

CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;

--CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @snapshot_start_dt  = '2012-01-01';  --Date for use for
--SET @snapshot_start_dt  = '2012-09-01';  --Original
SET @snapshot_end_dt    = '2013-12-01';

SET @playback_snapshot_start_dt  = '2012-01-01';

IF object_ID ('epg_data_phase_2') IS NOT NULL THEN
            DROP TABLE  epg_data_phase_2
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
INTO  epg_data_phase_2
FROM sk_prod.Vespa_programme_schedule
WHERE (tx_date_utc between @playback_snapshot_start_dt  and  @snapshot_end_dt)
;
commit;
--select max(tx_date_utc) from epg_data_phase_2
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


create hg index idx2 on epg_data_phase_2(programme_trans_sk);
alter table  epg_data_phase_2 Add channel_name_inc_hd       varchar(90);

update epg_data_phase_2
set channel_name_inc_hd=b.channel_name_inc_hd
from epg_data_phase_2 as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;
--select top 100 * from epg_data_phase_2 where tx_date_utc<'2012-04-01'
--select  * from channel_table
--select top 500 * FROM sk_prod.Vespa_programme_schedule  where broadcast_start_date_time_utc <'2012-04-01'
Update epg_data_phase_2
set channel_name_inc_hd =  
        case    when channel_name ='Sky Sports 1 HD' then 'Sky Sports 1'
                when channel_name ='Sky Premiere HD' then 'Sky Premiere'
                when channel_name ='Sky Sci-Fi & Horror HD' then 'Sky SciFi/Horror'

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
                when channel_name ='Sky Sports F1 HD' then 'Sky Sports F1'
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


     when channel_name ='ARISE News HD' then 'ARISE News'
     when channel_name in 
('BBC One Channel lslands'
,'BBC One East (E)'
,'BBC One East Midlands'
,'BBC One London'
,'BBC One North East & Cumbria'
,'BBC One North West'
,'BBC One Northern Ireland'
,'BBC One Oxford'
,'BBC One Scotland'
,'BBC One South'
,'BBC One South East'
,'BBC One South West'
,'BBC One Wales'
,'BBC One West'
,'BBC One West Midlands'
,'BBC One Yorkshire'
,'BBC One Yorkshire & Lincolnshire'
,'BBC 1 Cambridge'
,'BBC 1 HD NI'
,'BBC 1 NI HD'
,'BBC 1 Scot HD'
,'BBC 1 Scotland HD'
,'BBC 1 Wal HD') then 'BBC ONE'    
when channel_name in 
(
'BBC Two England'
,'BBC Two HD'
,'BBC Two Northern Ireland'
,'BBC Two Scotland'
,'BBC Two Wales') then 'BBC TWO'
     when channel_name ='Cartoon Network HD' then 'Cartoon Network'


     when channel_name ='Comedy Central HD' then 'Comedy Central'
     when channel_name ='Disney Channel HD' then 'Disney Channel'

     when channel_name ='Disney Cinemagic HD' then 'Disney Cinemagic'
     when channel_name ='Disney Jnr HD' then 'Disney Junior'

     when channel_name ='Eurosport2 HD' then 'Eurosport2'
     when channel_name ='Sky ShowcseHD' then 'Sky Movies Showcase'
when channel_name in 
(
'ITV1 Channel Is'
,'ITV1 HD London'
,'ITV1 HD Mid West'
,'ITV1 HD North'
,'ITV1 HD S East'
,'ITV1 Meridian N'
,'ITV1 Meridian SE'
,'ITV1 STV Grampian'
,'ITV1 STV Scottish'
,'ITV1 Scottish E'
,'ITV1 UTV'
,'ITV1 Yorkshire'
,'ITV1 Yorkshire East') then 'ITV1'
when channel_name in 
('ITV1 Anglia+1'
,'ITV1 Central+1'
,'ITV1 Granada+1'
,'ITV1 London+1'
,'ITV1 Meridian+1'
,'ITV1 STV +1'
,'ITV1 Tyne Tees+1'
,'ITV1 Wales+1'
,'ITV1 West +1'
,'ITV1 West Country+1'
,'ITV1 Yorkshire +1') then 'ITV1+1'

     when channel_name ='Russia Today HD' then 'Russia Today'
     when channel_name ='Nickelodeon HD' then 'Nickelodeon'
     when channel_name ='National Geographic HD' then 'National Geographic'
     when channel_name ='Nat Geo Wild HD' then 'Nat Geo Wild'

     when channel_name ='Eurosport2' then 'Eurosport 2'
     when channel_name ='Eurosport2 HD' then 'Eurosport 2'

     when channel_name ='NHK World HD' then 'NHK World'
     when channel_name ='More4 HD' then 'More4'
     when channel_name ='FOX HD' then 'FOX'
     when channel_name ='ESPN America HD' then 'ESPN America'

     when channel_name ='E! HD' then 'E!'
     when channel_name ='Disney Jnr HD' then 'Disney Junior'
     when channel_name ='Disney Cinemagic HD' then 'Disney Cinemagic'
     when channel_name ='Disney Channel HD' then 'Disney Channel'
     when channel_name ='Comedy Central HD' then 'Comedy Central'
     when channel_name ='Channel 4HD' then 'Channel 4'
     when channel_name ='Cartoon Network HD' then 'Cartoon Network'
     when channel_name ='Animal Planet HD' then 'Animal Planet'
     when channel_name ='ARISE News HD' then 'ARISE News'
     when channel_name ='Nickelodeon HD' then 'Nickelodeon'
     when channel_name ='Nat Geo Wild HD' then 'Nat Geo Wild'
     when channel_name ='Russia Today HD' then 'Russia Today'
     when channel_name ='Sky Arts Rieu HD' then 'Sky Arts 2'
     when channel_name ='Sky Arts Rieu' then 'Sky Arts 2'
     when channel_name ='Sky ChristmsHD' then 'Sky Christmas'
     when channel_name ='Sky Crime & Thriller HD' then 'Sky Crime & Thriller'
     when channel_name ='Sky Disney HD' then 'Sky Disney'
     when channel_name ='Sky Drama & Romance HD' then 'Sky Drama & Romance'

            when channel_name_inc_hd is not null then channel_name_inc_hd else channel_name end
;
commit;
--select count(*) from epg_data_phase_2;
--select Channel_Name_Inc_Hd , count(*) as records from epg_data_phase_2 group by Channel_Name_Inc_Hd order by Channel_Name_Inc_Hd;
alter table  epg_data_phase_2 Add Pay_channel       tinyint default 0;

   update epg_data_phase_2
    set Pay_channel = 1
    from epg_data_phase_2  
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
alter table  epg_data_phase_2 Add pack    varchar(25);

select channel_name
,max(media_pack) as pack
into #pack
from Project_161_viewing_table
group by channel_name
;

update epg_data_phase_2
set pack = b.pack
from epg_data_phase_2 as a
left outer join #pack as b
on a.channel_name=b.channel_name
;
commit;
--select pack , count(*) from epg_data_phase_2 group by pack
alter table  epg_data_phase_2 Add channel_name_inc_hd_staggercast       varchar(90);

update epg_data_phase_2
set channel_name_inc_hd_staggercast= Case when channel_name_inc_hd='5 USA +1' then '5 USA'
when channel_name_inc_hd='5* +1' then '5*'
when channel_name_inc_hd='Alibi +1' then 'Alibi'
when channel_name_inc_hd='Animal Planet +1' then 'Animal Planet'
when channel_name_inc_hd='BET +1' then 'BET'
when channel_name_inc_hd='Boomerang +1' then 'Boomerang'
when channel_name_inc_hd='CBS Reality +1' then 'CBS Reality'
when channel_name_inc_hd='Challenge +1' then 'Challenge'
when channel_name_inc_hd='Channel 4 +1' then 'Channel 4'
when channel_name_inc_hd='Channel 5+1' then 'Channel 5'
when channel_name_inc_hd='Chart Show+1' then 'Chart Show TV'
when channel_name_inc_hd='Comedy Central +1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra +1' then 'Comedy Central Extra'
when channel_name_inc_hd='Crime & Investigation +1' then 'Crime & Investigation'
when channel_name_inc_hd='DMax +1' then 'DMax'
when channel_name_inc_hd='DMax +2' then 'DMax'
when channel_name_inc_hd='Dave ja vu' then 'Dave'
when channel_name_inc_hd='Disc. History+1' then 'Disc. History'
when channel_name_inc_hd='Disc.Science +1' then 'Disc.Science'
when channel_name_inc_hd='Discovery +1hr' then 'Discovery'
when channel_name_inc_hd='Discovery RealTime +1' then 'Discovery RealTime'
when channel_name_inc_hd='Disney +1' then 'Disney'
when channel_name_inc_hd='Disney Cinemagic +1' then 'Disney Cinemagic'
when channel_name_inc_hd='Disney Junior+' then 'Playhouse Disney'
when channel_name_inc_hd='Disney XD +1' then 'Disney XD'
when channel_name_inc_hd='E4 +1' then 'E4'
when channel_name_inc_hd='Eden +1' then 'Eden'
when channel_name_inc_hd='FX +' then 'FX'
when channel_name_inc_hd='Film4 +1' then 'Film4'
when channel_name_inc_hd='Food Network+1' then 'Food Network'
when channel_name_inc_hd='GOLD +1' then 'GOLD  (TV)'
when channel_name_inc_hd='Good Food +1' then 'Good Food'
when channel_name_inc_hd='History +1 hour' then 'History'
when channel_name_inc_hd='Home & Health +1' then 'Home & Health'
when channel_name_inc_hd='Home+1' then 'Home'
when channel_name_inc_hd='ITV - ITV3+1' then 'ITV3'
when channel_name_inc_hd='ITV Channel Is' then 'ITV1'
when channel_name_inc_hd='ITV HD' then 'ITV1'
when channel_name_inc_hd='ITV1 Central SW' then 'ITV1'
when channel_name_inc_hd='ITV1+1' then 'ITV1'
when channel_name_inc_hd='ITV2+1' then 'ITV2'
when channel_name_inc_hd='ITV4+1' then 'ITV4'
when channel_name_inc_hd='MTV+1' then 'MTV'
when channel_name_inc_hd='More4 +1' then 'More4'
when channel_name_inc_hd='More4+2' then 'More4'
when channel_name_inc_hd='Movies 24 +' then 'Movies 24'
when channel_name_inc_hd='Nat Geo+1hr' then 'Nat Geo'
when channel_name_inc_hd='Nick Replay' then 'Nickelodeon'
when channel_name_inc_hd='N''Toons Replay' then 'Nicktoons TV'
when channel_name_inc_hd='Pick TV +1' then 'Pick TV'
when channel_name_inc_hd='PopGirl+1' then 'Pop Girl'
when channel_name_inc_hd='QUEST +1' then 'QUEST'
when channel_name_inc_hd='SONY TV +1' then 'SONY TV'
when channel_name_inc_hd='Showcase +1' then 'Showcase'
when channel_name_inc_hd='Showcase 2' then 'Showcase'
when channel_name_inc_hd='Sky Living +1' then 'Sky Living'
when channel_name_inc_hd='Sky Livingit +1' then 'Sky Livingit'
when channel_name_inc_hd='Sky Prem+1' then 'Sky Premiere'
when channel_name_inc_hd='Sony Movies+1' then 'Sony Movies'
when channel_name_inc_hd='Syfy +1' then 'Syfy'
when channel_name_inc_hd='Tiny Pop +1' then 'Tiny Pop'
when channel_name_inc_hd='Travel Channel +1' then 'Travel Channel'
when channel_name_inc_hd='Universal +1' then 'Universal'
when channel_name_inc_hd='Watch +1' then 'Watch'
when channel_name_inc_hd='YeSTERDAY +1' then 'YeSTERDAY'
when channel_name_inc_hd='horror channel +1' then 'horror channel'
when channel_name_inc_hd='men&movs+1' then 'men&movies'
when channel_name_inc_hd='mov4men+1' then 'movies4men'
when channel_name_inc_hd='mov4men2 +1' then 'movies4men 2'

when channel_name_inc_hd='5 USA+1' then '5 USA'

when channel_name_inc_hd='Africa Chnl+1' then 'Africa Channel'
when channel_name_inc_hd='Alibi+1' then 'Alibi'
when channel_name_inc_hd='Animal Planet+1' then 'Animal Planet'
when channel_name_inc_hd='BET+1' then 'BET'
when channel_name_inc_hd='Bio+1' then 'Bio'
when channel_name_inc_hd='Boomerang +1' then 'Boomerang'
when channel_name_inc_hd='CI +1' then 'CI'
when channel_name_inc_hd='Challenge +1' then 'Challenge'
when channel_name_inc_hd in ('Channel 4 +1'
,'Channel 4 +1 London'
,'Channel 4 +1 Midlands'
,'Channel 4 +1 North'
,'Channel 4 +1 Scotland'
,'Channel 4 +1 South'
,'Channel 4 +1 Ulster'
,'Channel 4+1 (ROI)')
 then 'Channel 4'

when channel_name_inc_hd in ('Channel 4'
,'Channel 4 London'
,'Channel 4 Midlands'
,'Channel 4 North'
,'Channel 4 Scotland'
,'Channel 4 South'
,'Channel 4 Ulster'
,'Channel 4 (ROI)')
 then 'Channel 4'


when channel_name_inc_hd in ('Channel 5 London'
,'Channel 5 North'
,'Channel 5 Scotland'
,'Channel 5  Northern Ireland')
 then 'Channel 5'

when channel_name_inc_hd='Channel 5+1' then 'Channel 5'
when channel_name_inc_hd='Christmas 24+' then 'Christmas 24'
when channel_name_inc_hd='Comedy Central +1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra +1' then 'Comedy Central Extra'
when channel_name_inc_hd='Comedy Central Extra+1' then 'Comedy Central Extra'
when channel_name_inc_hd='Comedy Central+1' then 'Comedy Central'
when channel_name_inc_hd='Comedy Central Extra+1' then 'Comedy Central Extra'

when channel_name_inc_hd='Crime & Investigation +1' then 'Crime & Investigation'
when channel_name_inc_hd='Crime & Investigation Network' then 'Crime & Investigation'
when channel_name_inc_hd='Discovery History+1' then 'Discovery History'
when channel_name_inc_hd='Discovery Home & Health +1' then 'Discovery Home & Health'
when channel_name_inc_hd='Discovery Home & Health +1' then 'Discovery Home & Health'
when channel_name_inc_hd='Discovery Real Time +1' then 'Discovery Real Time'
when channel_name_inc_hd='Discovery Science+1' then 'Discovery Science'
when channel_name_inc_hd='Disney Channel+1' then 'Disney Channel'
when channel_name_inc_hd in ('E4 (ROI)'
,'E4 +1'
,'E4 +1 (ROI)'
,'E4+1 ROI')
 then 'E4'
when channel_name_inc_hd='FOX+' then 'FOX'
when channel_name_inc_hd='GOLD+1' then 'GOLD'
when channel_name_inc_hd='Good Food+1' then 'Good Food'
 else channel_name_inc_hd end
;

--Second Part

update epg_data_phase_2
set channel_name_inc_hd_staggercast= Case  
when channel_name_inc_hd='History +1hour' then 'History'
when channel_name_inc_hd='Horror Channel+1' then 'Horror'
when channel_name_inc_hd='ID+1' then 'ID'
when channel_name_inc_hd='Info TV+1' then 'Info TV'



when channel_name_inc_hd='More 4 +1' then 'More 4'
when channel_name_inc_hd='More 4+2' then 'More 4'
when channel_name_inc_hd='Movies4Men +1' then 'Movies4Men'
when channel_name_inc_hd='National Geographic+1' then 'National Geographic'

when channel_name_inc_hd='Nick Jr +1' then 'Nick Jr'
when channel_name_inc_hd='Nick Jr+1' then 'Nick Jr'




when channel_name_inc_hd In('Nickelodeon +1'
,'Nickelodeon EIRE'
,'Nickelodeon+1') then 'Nickelodeon'
when channel_name_inc_hd='RAMP''d +2' then 'RAMP''d +2'

when channel_name_inc_hd In('Sky Atlantic +1'
,'Sky Atlantic ROI'
,'Sky Atlantic+1') then 'Sky Atlantic'

when channel_name_inc_hd='Sky Premiere +1' then 'Sky Premiere'
when channel_name_inc_hd='Sky1 +1' then 'Sky1'
when channel_name_inc_hd='TLC +1' then 'TLC'
when channel_name_inc_hd='True Ent+1' then 'True Ent'
when channel_name_inc_hd='Watch+1' then 'Watch'
when channel_name_inc_hd='More 4 +1' then 'More 4'
when channel_name_inc_hd='YESTERDAY +1' then 'YeSTERDAY'


when channel_name_inc_hd='5 USA +1' then '5 USA'
when channel_name_inc_hd='5 USA+1' then '5 USA'
when channel_name_inc_hd='5* +1' then '5*'
when channel_name_inc_hd='Africa Chnl+1' then 'Africa Channel'
when channel_name_inc_hd='Alibi +1' then 'Alibi'
when channel_name_inc_hd='Alibi+1' then 'Alibi'

when channel_name_inc_hd='Animal Planet +1' then 'Animal Planet'
when channel_name_inc_hd='Animal Planet+1' then 'Animal Planet'
when channel_name_inc_hd='BET +1' then 'BET'
when channel_name_inc_hd='BET+1' then 'BET'
when channel_name_inc_hd='Bio+1' then 'Bio'
when channel_name_inc_hd='Boomerang +1' then 'Boomerang'




when channel_name_inc_hd='CBS Reality +1' then 'CBS Reality'
when channel_name_inc_hd='CI +1' then 'CI'
when channel_name_inc_hd='ITV +1' then 'ITV1'
when channel_name_inc_hd='ITV 2' then 'ITV2'
when channel_name_inc_hd='ITV 3' then 'ITV3'
when channel_name_inc_hd='ITV 4' then 'ITV4'
when channel_name_inc_hd='Disney Junior+' then 'Disney Junior'
when channel_name_inc_hd='E! Entertainment' then 'E!'

when channel_name_inc_hd='Football First 1' then 'Football First'
when channel_name_inc_hd='Football First 2' then 'Football First'
when channel_name_inc_hd='Football First 3' then 'Football First'
when channel_name_inc_hd='Football First 4' then 'Football First'
when channel_name_inc_hd='Football First 5' then 'Football First'
when channel_name_inc_hd='Football First 6' then 'Football First'
when channel_name_inc_hd='Football First 7' then 'Football First'
when channel_name_inc_hd='Football First 8' then 'Football First'
when channel_name_inc_hd='Football First 9' then 'Football First'

when channel_name_inc_hd='GOLD+1' then 'GOLD  (TV)'

when channel_name_inc_hd='HD Sky Disney' then 'Sky Disney'

when channel_name_inc_hd='History Channel' then 'History'

when channel_name_inc_hd='National Geographic' then 'Nat Geo'
when channel_name_inc_hd='National Geographic+1' then 'Nat Geo'

when channel_name_inc_hd='HD Anytime 1' then 'Anytime'
when channel_name_inc_hd='HD Anytime 2' then 'Anytime'
when channel_name_inc_hd='HD Anytime 3' then 'Anytime'

when channel_name_inc_hd='Sky Sports News HD' then 'Sky Sports News'
when channel_name_inc_hd='Sky Summer HD' then 'Sky Summer'
when channel_name_inc_hd='Sony TV' then 'SONY TV'

when channel_name_inc_hd='Star Plus' then 'STAR Plus'
when channel_name_inc_hd='TCM HD' then 'TCM'
when channel_name_inc_hd='True Entertainment' then 'True Ent'


 else channel_name_inc_hd_staggercast end
;
commit;

grant all on epg_data_phase_2 to public; commit;

--
/*
select upper(channel_name_inc_hd) as chup,channel_name_inc_hd,channel_name_inc_hd_staggercast from epg_data_phase_2 group by chup,channel_name_inc_hd,channel_name_inc_hd_staggercast 
order by chup,channel_name_inc_hd,channel_name_inc_hd_staggercast

select * from epg_data_phase_2 where channel_name_inc_hd_staggercast = 'Sky'

*/
