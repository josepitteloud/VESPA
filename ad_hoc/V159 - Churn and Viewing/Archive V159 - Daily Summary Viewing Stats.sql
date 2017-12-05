

---Add >= 6 am split for viewing days summary---


---V159 Daily Viewing Summary---

--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20130210;
--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20120518;

commit;

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
SET @snapshot_start_dt  = '2012-12-14';  --Had to restart loop half way through
--SET @snapshot_start_dt  = '2012-09-01';  --Original
SET @snapshot_end_dt    = '2013-02-28';

SET @playback_snapshot_start_dt  = '2012-06-01';

/*
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @viewing_var_num_days = 29;
commit;
*/
IF object_ID ('V159_epg_data') IS NOT NULL THEN
            DROP TABLE  V159_epg_data
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
INTO  V159_epg_data
FROM sk_prod.Vespa_programme_schedule
WHERE (tx_date_utc between @playback_snapshot_start_dt  and  @snapshot_end_dt)
;
commit;
create hg index idx2 on V159_epg_data(programme_trans_sk);
alter table  V159_epg_data Add channel_name_inc_hd       varchar(90);

update V159_epg_data
set channel_name_inc_hd=b.channel_name_inc_hd
from V159_epg_data as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;

Update V159_epg_data
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
--select Channel_Name_Inc_Hd , count(*) as records from V159_epg_data group by Channel_Name_Inc_Hd order by Channel_Name_Inc_Hd;
alter table  V159_epg_data Add Pay_channel       tinyint default 0;

   update V159_epg_data
    set Pay_channel = 1
    from V159_epg_data  
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

------------

IF object_ID ('V159_Daily_viewing_summary_all') IS NOT NULL THEN
            DROP TABLE  V159_Daily_viewing_summary_all
END IF;

CREATE TABLE  V159_Daily_viewing_summary_all
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

IF object_ID ('V159_Daily_viewing_summary') IS NOT NULL THEN
            DROP TABLE  V159_Daily_viewing_summary
END IF;

CREATE TABLE  V159_Daily_viewing_summary
    ( 
            Account_Number                             varchar(20)  not null
            ,viewing_day                      date
            ,viewing_Duration                     bigint
            ,viewing_Duration_live                     bigint
            ,total_duration_pay_exc_premiums                     bigint
            ,total_duration_premiums                     bigint
            ,total_duration_terrestrial                     bigint
            ,total_duration_free_non_terrestrial bigint
            ,post_6_am_viewing tinyint
)
;

--select top 100 * from vespa_analysts.VESPA_DAILY_AUGS_20121001
--select top 100 * from V159_Tenure_10_16mth_Viewing
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into V159_Daily_viewing_summary_all(
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
                ,post_6_am_viewing
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

insert into V159_Daily_viewing_summary
select account_number
,min(cast(viewing_starts as date)) as viewing_date
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
,max(case when dateformat(viewing_starts,HH) in (''00'',''01'',''02'',''03'',''04'',''05'') then 0 else 1 end) as post_6_am_viewing

from V159_Daily_viewing_summary_all as a
left outer join V159_epg_data as b 
on a.programme_trans_sk=b.programme_trans_sk
group by account_number

delete from V159_Daily_viewing_summary_all where account_number is null or account_number is not null
'
;
--select max (viewing_starts) from vespa_analysts.VESPA_DAILY_AUGS_20121104;

--select top 100 * from V159_Tenure_10_16mth_Viewing;
--select top 100 * from  vespa_analysts.VESPA_DAILY_AUGS_20121104;

-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end


--select viewing_day , count(*) from V159_Daily_viewing_summary group by viewing_day order by viewing_day desc;
--select top 5000 * into V159_Daily_viewing_summary_test from V159_Daily_viewing_summary;commit;
commit;
--select count(*) from vespa_analysts.VESPA_DAILY_AUGS_20130126
--select count(*) from vespa_analysts.VESPA_DAILY_AUGS_20130127

--select * from V159_Daily_viewing_summary_test;
commit;

--Create Single Account Summaries--

commit;
create  hg index idx1 on V159_Daily_viewing_summary (account_number);
commit;
--drop table V159_account_level_viewing_summary;
select account_number
,count(*) as viewing_days
,sum(viewing_duration) as total_duration
,sum(viewing_duration_live) as total_duration_live

into V159_account_level_viewing_summary
from V159_Daily_viewing_summary
group by account_number
;

commit;

--select top 100 * from V159_account_level_viewing_summary;

---Get List of all accounts with a Pending Cancel Event From Oct '12- Jan '13---


select
                csh.account_number
                ,min(csh.status_start_dt) as pc_dt
into            #v159_churn_viewing_pc_events
FROM            sk_prod.cust_subs_hist csh
WHERE           csh.status_start_dt between '2012-10-01' and '2013-01-28'
AND             csh.status_code_changed = 'Y'
AND             csh.status_code in ('PC')
AND             csh.prev_status_code in ('AC')
and             csh.subscription_sub_type = 'DTV Primary Viewing'
group by csh.account_number
;
Commit;

commit;
create  hg index idx1 on #v159_churn_viewing_pc_events (account_number);


alter table V159_account_level_viewing_summary add first_pc_date date;

update V159_account_level_viewing_summary
set first_pc_date=b.pc_dt
from V159_account_level_viewing_summary as a
left outer join #v159_churn_viewing_pc_events as b
on a.account_number = b.account_number
;

commit;

---Create List of All that do enter churn rather than reactivate--
select
                csh.account_number
                ,min(csh.status_start_dt) as po_dt
into            #v159_churn_viewing_po_events
FROM            sk_prod.cust_subs_hist csh
WHERE           csh.status_start_dt between '2012-10-01' and '2013-03-28'
AND             csh.status_code_changed = 'Y'
AND             csh.status_code in ('PO')
AND             csh.prev_status_code in ('PC')
and             csh.subscription_sub_type = 'DTV Primary Viewing'
group by csh.account_number
;
Commit;

commit;
create  hg index idx1 on #v159_churn_viewing_po_events (account_number);

alter table V159_account_level_viewing_summary add first_po_date date;

update V159_account_level_viewing_summary
set first_po_date=b.po_dt
from V159_account_level_viewing_summary as a
left outer join #v159_churn_viewing_po_events as b
on a.account_number = b.account_number
;

commit;

--select top 100 * from V159_account_level_viewing_summary;

--select count(*),sum(case when first_po_date-28>first_pc_date then 1 else 0 end) as over_28_days from V159_account_level_viewing_summary where first_pc_date is not null;
--select  top 500 * from V159_account_level_viewing_summary where first_pc_date is not null;

/*
select status_start_dt
,status_code
FROM            sk_prod.cust_subs_hist csh
WHERE           csh.status_start_dt between '2012-10-01' and '2013-03-28'
AND             csh.status_code_changed = 'Y'
and             csh.subscription_sub_type = 'DTV Primary Viewing'
and account_number = '210053240797' order by effective_from_dt
*/
---Get details of Weekly Viewing Volume pre and post
--drop table #v159_viewing_around_pc_date;
select a.account_number
,first_pc_date
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-1 then 1 else 0 end) as viewing_days_01_to_28_days_pre_pc
,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+28 then 1 else 0 end) as viewing_days_01_to_28_days_post_pc

--Viewing Summary---
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-1 then viewing_duration else 0 end) as pre_pc_viewing_duration
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-1 then total_duration_pay_exc_premiums else 0 end) as pre_pc_viewing_duration_exc_premiums
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-1 then total_duration_premiums else 0 end) as pre_pc_viewing_premiums
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-1 then total_duration_terrestrial else 0 end) as pre_pc_viewing_terrestrial
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-1 then total_duration_free_non_terrestrial else 0 end) as pre_pc_viewing_free_non_terrestrial

,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+28 then viewing_duration else 0 end) as post_pc_viewing_duration
,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+28 then total_duration_pay_exc_premiums else 0 end) as post_pc_viewing_duration_exc_premiums
,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+28 then total_duration_premiums else 0 end) as post_pc_viewing_premiums
,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+28 then total_duration_terrestrial else 0 end) as post_pc_viewing_terrestrial
,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+28 then total_duration_free_non_terrestrial else 0 end) as post_pc_viewing_free_non_terrestrial



into #v159_viewing_around_pc_date
from  V159_account_level_viewing_summary as a
left outer join V159_Daily_viewing_summary as b
on a.account_number=b.account_number
where  first_pc_date is not null and first_po_date-28>first_pc_date
group by a.account_number
,first_pc_date

;

--select top 100 * from V159_Daily_viewing_summary;
--select top 100 * from #v159_viewing_around_pc_date;

---Overall Viewing Split
select case when tot/viewing_days_01_to_28_days_pre_pc<18000 then  'a) Under 5 Hours per Day'
when pre_pc_viewing_duration/viewing_days_01_to_28_days_pre_pc<25200 then  'b) >=5 and <7 Hours Hours per Day'
else 'c) 7+ Hours Viewing' end as daily_average_viewing
--,
,sum(pre_pc_viewing_duration) as total_pre_pc_duration
,sum(post_pc_viewing_duration) as total_post_pc_duration
,sum(viewing_days_01_to_28_days_pre_pc) as days_viewing_pre_pc
,sum(viewing_days_01_to_28_days_post_pc) as days_viewing_post_pc
,count(*) as accounts
from #v159_viewing_around_pc_date
where viewing_days_01_to_28_days_pre_pc>=20 and viewing_days_01_to_28_days_post_pc>=20 
group by daily_average_viewing
order by daily_average_viewing
;

---Premium Split

select case when pre_pc_viewing_premiums = 0 then 'a) No Premiums viewed' when pre_pc_viewing_premiums/viewing_days_01_to_28_days_pre_pc<600 then  'b) Under 10 Min per Day'
when pre_pc_viewing_premiums/viewing_days_01_to_28_days_pre_pc<2700 then  'c) >=10 and <45 Minutes per Day'
else 'd) 45+ Minutes per day' end as daily_average_viewing
--,
,sum(pre_pc_viewing_premiums) as total_pre_pc_duration
,sum(post_pc_viewing_premiums) as total_post_pc_duration
,sum(viewing_days_01_to_28_days_pre_pc) as days_viewing_pre_pc
,sum(viewing_days_01_to_28_days_post_pc) as days_viewing_post_pc
,count(*) as accounts
from #v159_viewing_around_pc_date
where viewing_days_01_to_28_days_pre_pc>=20 and viewing_days_01_to_28_days_post_pc>=20 
group by daily_average_viewing
order by daily_average_viewing
;

commit;


---Pay TV Exc Premium Split

select case when pre_pc_viewing_duration_exc_premiums/viewing_days_01_to_28_days_pre_pc<3600 then  'a) Under 1hr per Day'
when pre_pc_viewing_duration_exc_premiums/viewing_days_01_to_28_days_pre_pc<7200 then  'b) >=1hr and 2hrs per Day'
else 'c) 2+ Hours per day' end as daily_average_viewing
--,
,sum(pre_pc_viewing_duration_exc_premiums) as total_pre_pc_duration
,sum(post_pc_viewing_duration_exc_premiums) as total_post_pc_duration
,sum(viewing_days_01_to_28_days_pre_pc) as days_viewing_pre_pc
,sum(viewing_days_01_to_28_days_post_pc) as days_viewing_post_pc
,count(*) as accounts
from #v159_viewing_around_pc_date
where viewing_days_01_to_28_days_pre_pc>=20 and viewing_days_01_to_28_days_post_pc>=20 
group by daily_average_viewing
order by daily_average_viewing
;

commit;

---Terrestrial Split

select case when pre_pc_viewing_terrestrial/viewing_days_01_to_28_days_pre_pc<5400 then  'a) Under 90 mins per Day'
when pre_pc_viewing_terrestrial/viewing_days_01_to_28_days_pre_pc<10800 then  'b) >=90 mins and <3 hrs per Day'
else 'c) 3+ Hours per day' end as daily_average_viewing
--,
,sum(pre_pc_viewing_terrestrial) as total_pre_pc_duration
,sum(post_pc_viewing_terrestrial) as total_post_pc_duration
,sum(viewing_days_01_to_28_days_pre_pc) as days_viewing_pre_pc
,sum(viewing_days_01_to_28_days_post_pc) as days_viewing_post_pc
,count(*) as accounts
from #v159_viewing_around_pc_date
where viewing_days_01_to_28_days_pre_pc>=20 and viewing_days_01_to_28_days_post_pc>=20 
group by daily_average_viewing
order by daily_average_viewing
;

commit;



/*
,sum(case when viewing_day  between first_pc_date-28 and first_pc_date-22 then 1 else 0 end) as viewing_days_22_to_28_days_pre_pc
,sum(case when viewing_day  between first_pc_date-21 and first_pc_date-15 then 1 else 0 end) as viewing_days_15_to_12_days_pre_pc
,sum(case when viewing_day  between first_pc_date-14 and first_pc_date-8 then 1 else 0 end) as viewing_days_08_to_14_days_pre_pc
,sum(case when viewing_day  between first_pc_date-7 and first_pc_date-1 then 1 else 0 end) as viewing_days_01_to_07_days_pre_pc

,sum(case when viewing_day  between first_pc_date+22 and first_pc_date+28 then 1 else 0 end) as viewing_days_22_to_28_days_post_pc
,sum(case when viewing_day  between first_pc_date+15 and first_pc_date+21 then 1 else 0 end) as viewing_days_15_to_12_days_post_pc
,sum(case when viewing_day  between first_pc_date+8 and first_pc_date+14 then 1 else 0 end) as viewing_days_08_to_14_days_post_pc
,sum(case when viewing_day  between first_pc_date+1 and first_pc_date+7 then 1 else 0 end) as viewing_days_01_to_07_days_post_pc
*/
--select top 100 * from V159_Daily_viewing_summary;
--select top 100 * from #v159_viewing_around_pc_date;

--select * from V159_Daily_viewing_summary where account_number = '210015714012' order by viewing_day

--select viewing_days_22_to_28_days_post_pc , count(*) from #v159_viewing_around_pc_date group by viewing_days_22_to_28_days_post_pc order by viewing_days_22_to_28_days_post_pc;
--select viewing_days_01_to_07_days_post_pc , count(*) from #v159_viewing_around_pc_date group by viewing_days_01_to_07_days_post_pc order by viewing_days_01_to_07_days_post_pc;

--select viewing_days_01_to_07_days_pre_pc , count(*) from #v159_viewing_around_pc_date group by viewing_days_01_to_07_days_pre_pc order by viewing_days_01_to_07_days_pre_pc;

/*
select  viewing_days_22_to_28_days_pre_pc
,viewing_days_15_to_12_days_pre_pc
,viewing_days_08_to_14_days_pre_pc
,viewing_days_01_to_07_days_pre_pc

,viewing_days_22_to_28_days_post_pc
,viewing_days_15_to_12_days_post_pc
,viewing_days_08_to_14_days_post_pc
,viewing_days_01_to_07_days_post_pc
,count(*) as accounts
from #v159_viewing_around_pc_date
group by viewing_days_22_to_28_days_pre_pc
,viewing_days_15_to_12_days_pre_pc
,viewing_days_08_to_14_days_pre_pc
,viewing_days_01_to_07_days_pre_pc

,viewing_days_22_to_28_days_post_pc
,viewing_days_15_to_12_days_post_pc
,viewing_days_08_to_14_days_post_pc
,viewing_days_01_to_07_days_post_pc
order by accounts desc
;

--

select  viewing_days_22_to_28_days_pre_pc
+viewing_days_15_to_12_days_pre_pc
+viewing_days_08_to_14_days_pre_pc
+viewing_days_01_to_07_days_pre_pc as days_pre

,viewing_days_22_to_28_days_post_pc
+viewing_days_15_to_12_days_post_pc
+viewing_days_08_to_14_days_post_pc
+viewing_days_01_to_07_days_post_pc as days_post
,count(*) as accounts
from #v159_viewing_around_pc_date
group by days_pre ,days_post
order by accounts desc
;

commit;
*/




