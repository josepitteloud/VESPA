/*------------------------------------------------------------------------------
        Project: V161 - Customer Value ATL
        Version: 1
        Created: 20130326
        Lead: Susanne Chan
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
        Purpose
        -------
In summer 2013 a Sky Sports campaign will be aired with the aim to upgrade existing customers to Sky Sports.  

This will be part of an on-going Sky Sports upgrade campaign over the next year

Objectives

To identify:
1.	The target audience: Existing Sky customers without Sky Sports, who are most likely to upgrade 
2.	The optimum placement of spots for the value message to reach the target audience


        SECTIONS
        --------

        PART A - Define Active Customers at analysis period start

        PART B - Extract Viewing data

        PART C - Efficiency Summaries
*/



----V161 - Sky Sports ATL Analysis---

--PART A   - Define Active Customers at analysis period start
select csh.account_number
,max(cel.prem_sports) as sports_premiums
into            v161_active_ac_accounts
FROM            sk_prod.cust_subs_hist csh
LEFT OUTER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
WHERE          csh.subscription_sub_type ='DTV Primary Viewing'
                and csh.subscription_type = 'DTV PACKAGE'
                and effective_from_dt<= '2012-05-16' and effective_to_dt>'2012-05-16'
                and status_code in ('AC')
group by csh.account_number
;

---Add on Sports Model Decile as at May 2012

alter table v161_active_ac_accounts add sports_model_decile tinyint;

update v161_active_ac_accounts
set sports_model_decile = b.decile
from v161_active_ac_accounts as a
left outer join models.model_scores as b
on a.account_number = b.account_number
where model_name = 'Sports Mailed'
and model_run_date =   '2012-05-16'
;
commit;


--Add Country Code---
alter table  v161_active_ac_accounts add UK_Account tinyint;

update v161_active_ac_accounts 
set UK_Account=case when pty_country_code ='GBR' then 1 else 0 end
from v161_active_ac_accounts as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number;
commit;

---Add in region----
--alter table v161_active_ac_accounts  delete region;
alter table v161_active_ac_accounts  add region VARCHAR(40)     DEFAULT 'UNKNOWN';
UPDATE v161_active_ac_accounts 
SET     Region                     = CASE WHEN sav.isba_tv_region = 'Not Defined'
                                       THEN 'UNKNOWN'
                                       ELSE sav.isba_tv_region
                                   END
FROM v161_active_ac_accounts  AS base
        INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number
;
commit;

--Add Segment to Base File---

alter table  v161_active_ac_accounts add segment varchar(50);

update v161_active_ac_accounts 
set segment=case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then '01: No Sports Premiums and High Model Decile'
            when sports_premiums=0 and sports_model_decile not in (1,2,3,4) then '02: No Sports Premiums and not High Model Decile'
            when sports_premiums>0  then '03: Has Sports Premiums'
--Put Unknowns in Group 2--
 else '02: No Sports Premiums and not High Model Decile'
 end

from v161_active_ac_accounts ;
commit;

---Create counts of target groups---
select case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then '01: No Sports Premiums and High Model Decile'
            when sports_premiums=0 and sports_model_decile not in (1,2,3,4) then '02: No Sports Premiums and not High Model Decile'
            when sports_premiums>0  then '03: Has Sports Premiums'
--Put Unknowns in Group 2--
 else '02: No Sports Premiums and not High Model Decile'
 end as segment

, count(*) as accounts
,sum(UK_Account) as UK_Accounts
from v161_active_ac_accounts
group by segment 
order by segment
;


--select pty_country_code ,count(*) from sk_prod.cust_single_account_view group by pty_country_code 
--------------------------------------------------------------------------------
-- PART B SETUP - Extract Viewing data
--------------------------------------------------------------------------------

/*
PART B   - Extract Viewing data
     B01 - Viewing table for period
     B03 - Clean data
     
*/

CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
--CREATE VARIABLE @var_num_days           smallint;

--select panel_id , count(*) from sk_prod.VESPA_STB_PROG_EVENTS_20120301 group by panel_id order by panel_id;

-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-05-17';
--SET @var_prog_period_end    = '2012-03-01';
SET @var_prog_period_end    = '2012-05-31';
--SET @var_num_days = 14;
--select @var_num_days;
if object_id('VESPA_Programmes_project_161') is not null drop table VESPA_Programmes_project_161;
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
       ,service_key
      ,datediff(mi,Tx_Start_Datetime_UTC,Tx_End_Datetime_UTC) as programme_duration
  into VESPA_Programmes_project_161 -- drop table  VESPA_Programmes
  from sk_prod.VESPA_EPG_DIM
 where tx_date_time_utc between  dateadd(day, -60, @var_prog_period_start)  and dateadd(day, 1, @var_prog_period_end) 


-- because @var_prog_period_end is a date and defaults to 00:00:00 when compared to datetimes
-- Add further filters to programmes here if required, eg, lower(channel_name) like '%bbc%'
   ;
--select top 500 * from VESPA_Programmes_project_161 where upper(channel_name) like '%ATLANTIC%';
commit;
create unique hg index idx1 on VESPA_Programmes_project_161(programme_trans_sk);
create  hg index idx2 on VESPA_Programmes_project_161(tx_date_utc);
create  hg index idx3 on VESPA_Programmes_project_161(service_key);
commit;
------ B01 - Viewing table for period
-- B01 - Viewing table for period
commit;

if object_id('Project_161_viewing_table') is not null drop table Project_161_viewing_table;
create table Project_161_viewing_table (
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

commit;
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into Project_161_viewing_table(
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

from vespa_analysts.ph1_VESPA_DAILY_AUGS_##^^*^*## as da
inner join sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*## as vw
    on da.cb_row_ID = vw.cb_row_ID
inner join VESPA_Programmes_project_161 as prog
    on vw.programme_trans_sk = prog.programme_trans_sk
';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @scanning_day = @var_prog_period_start;
--delete from Project_161_viewing_table;
commit;
while @scanning_day <= dateadd(dd,0,@var_prog_period_end)
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(@scanning_day, 'yyyymmdd')))
--    commit

    set @scanning_day = dateadd(day, 1, @scanning_day)
end;
commit;

grant select on Project_161_viewing_table to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts;
commit;


--select count(*) , count(distinct account_number) from Project_161_viewing_table;

commit;
create  hg index idx1 on Project_161_viewing_table (account_number);
create  hg index idx2 on Project_161_viewing_table (Viewing_date);

commit;

---Add on Service Key (Not in Original code but required for media pack info)
--select top 100 * from VESPA_Programmes_project_161;

alter table Project_161_viewing_table add service_key int;

update Project_161_viewing_table 
set service_key = prog.service_key
from Project_161_viewing_table  as a
left outer join VESPA_Programmes_project_161 as prog
    on a.programme_trans_sk = prog.programme_trans_sk
;
commit;


----Add on Vespa Figures for May 2012----

--select scaling_segment_id , count(distinct account_number) from vespa_analysts.project060_all_viewing where scaling_segment_id is not null group by scaling_segment_id order by scaling_segment_id;
--select count(distinct subscriber_id) , count(distinct account_number) from vespa_analysts.project060_all_viewing where scaling_segment_id is not null ;
--select count(*) from vespa_analysts.scaling_weightings;
--select top 100 * from scaling_segments_lookup;
---Add weight for each scaling ID for each record

alter table Project_161_viewing_table add weighting double;
update Project_161_viewing_table
set weighting=c.weighting
from Project_161_viewing_table  as a
left outer join vespa_analysts.SC2_intervals as b
on a.account_number = b.account_number
left outer join vespa_analysts.SC2_weightings as c
on  a.Viewing_date = c.scaling_day
where b.scaling_segment_ID = c.scaling_segment_ID
and a.Viewing_date between b.reporting_starts and b.reporting_ends
commit;

---Aprox 15-20% of accounts don't have weights so will be excluded from subsequent analysis---

--Remove Activity where weight is unknown or where duration is <180 seconds

delete from Project_161_viewing_table where viewing_duration<180; commit;
delete from Project_161_viewing_table where weighting is null or weighting<=0; commit;
--select count(*) from Project_161_viewing_table
--select viewing_duration , count(*) from  Project_161_viewing_table group by viewing_duration order by viewing_duration

---Add on meta data (Channel Name inc hd/staggercast etc.,)----
alter table  Project_161_viewing_table Add channel_name_inc_hd       varchar(90);

update Project_161_viewing_table
set channel_name_inc_hd=b.channel_name_inc_hd
from Project_161_viewing_table as a
left outer join channel_table as b
on  upper(a.Channel_Name) = upper(b.Channel)
;
commit;

Update Project_161_viewing_table
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
                when channel_name ='alibi HD' then 'Alibi'
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

--select channel_name_inc_hd , count(*) from Project_161_viewing_table group by channel_name_inc_hd order by channel_name_inc_hd

--select channel_name_inc_hd_staggercast , count(*) from Project_161_viewing_table group by channel_name_inc_hd_staggercast order by channel_name_inc_hd_staggercast

--Create Combined Channel Name to Include Staggercast and Regular

alter table  Project_161_viewing_table Add channel_name_inc_hd_staggercast       varchar(90);

update Project_161_viewing_table
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
when channel_name_inc_hd='Sky Prem+1' then 'Sky News'
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
when channel_name_inc_hd='mov4men2 +1' then 'movies4men 2' else channel_name_inc_hd end
;
commit;

--Add A 'Original Broadcast Time' to Dataset--
alter table  Project_161_viewing_table Add non_staggercast_broadcast_time_utc       datetime;

update Project_161_viewing_table
set non_staggercast_broadcast_time_utc =
case  when channel_name_inc_hd in 
('5 USA +1'
,'5* +1'
,'Alibi +1'
,'Animal Planet +1'
,'BET +1'
,'Boomerang +1'
,'CBS Reality +1'
,'Challenge +1'
,'Channel 4 +1'
,'Channel 5+1'
,'Chart Show+1'
,'Comedy Central +1'
,'Comedy Central Extra +1'
,'Crime & Investigation +1'
,'DMax +1'
,'Dave ja vu'
,'Disc. History+1'
,'Disc.Science +1'
,'Discovery +1hr'
,'Discovery RealTime +1'
,'Disney +1'
,'Disney Cinemagic +1'
,'Disney Junior+'
,'Disney XD +1'
,'E4 +1'
,'Eden +1'
,'FX +'
,'Film4 +1'
,'Food Network+1'
,'GOLD +1'
,'Good Food +1'
,'History +1 hour'
,'Home & Health +1'
,'Home+1'
,'ITV - ITV3+1'
,'ITV1+1'
,'ITV2+1'
,'ITV4+1'
,'MTV+1'
,'More4 +1'
,'Movies 24 +'
,'Nat Geo+1hr'
,'Nick Replay'
,'N''Toons Replay'
,'Pick TV +1'
,'PopGirl+1'
,'QUEST +1'
,'SONY TV +1'
,'Showcase +1'
,'Sky Living +1'
,'Sky Livingit +1'
,'Sky Prem+1'
,'Sony Movies+1'
,'Syfy +1'
,'Tiny Pop +1'
,'Travel Channel +1'
,'Universal +1'
,'Watch +1'
,'YeSTERDAY +1'
,'horror channel +1'
,'men&movs+1'
,'mov4men+1'
,'mov4men2 +1')
then dateadd(hh,-1,Tx_Start_Datetime_UTC) 

when channel_name_inc_hd in ('DMax +2','More4+2')
then dateadd(hh,-2,Tx_Start_Datetime_UTC) 
else Tx_Start_Datetime_UTC end
from Project_161_viewing_table
;
commit;

---Add Media Pack details---
--select * into LkUpPack_old from LkUpPack; commit;
---------------------------------------------------------------------------------------------------------------
-- TEMPLATE 3: OUTPUT: RESPONDERS BY MEDIA PACK
---------------------------------------------------------------------------------------------------------------

select ska.service_key as service_key, ska.full_name, PACK.NAME,cgroup.primary_sales_house,
                (case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
into #packs
from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES ska
left join
        (select a.service_key, b.name
         from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK a
                join neighbom.CHANNEL_MAP_DEV_LANDMARK_CHANNEL_PACK_LOOKUP b
                        on a.sare_no between b.sare_no and b.sare_no + 999
        where a.service_key <> 0
         ) pack
        on ska.service_key = pack.service_key
left join
        (select distinct a.service_key, b.primary_sales_house, b.channel_group
         from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB a
                join neighbom.CHANNEL_MAP_DEV_BARB_CHANNEL_GROUP b
                        on a.log_station_code = b.log_station_code
                        and a.sti_code = b.sti_code
        where service_key <>0) cgroup
        on ska.service_key = cgroup.service_key
where cgroup.primary_sales_house is not null
order by cgroup.primary_sales_house, channel_category
;--438 Row(s) affected

--select * from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES order by full_name;

-----------------------------Correct channel category anomolies -- media pack

if object_id('LkUpPack') is not null drop table LkUpPack

SELECT  primary_sales_house
        ,service_key
        ,full_name
        ,(case
                when service_key = 3777 OR service_key = 6756 then 'LIFESTYLE & CULTURE'
                when service_key = 4040 then 'SPORTS'
                when service_key = 1845 OR service_key = 4069 OR service_key = 1859 then 'KIDS'
                when service_key = 4006 then 'MUSIC'
                when service_key = 3621 OR service_key = 4080 then 'ENTERTAINMENT'
                when service_key = 3760 then 'DOCUMENTARIES'
                when service_key = 1757 then 'MISCELLANEOUS'
                when service_key = 3639 OR service_key = 4057 then 'Media Partners'
                                                                                ELSE channel_category END) AS channel_category
INTO LkUpPack
FROM #packs
order by primary_sales_house, channel_category
;

----------------------------------------------------------------------------------------------------------------------------

-- Update media pack 
alter table Project_161_viewing_table
        add media_pack varchar(25);


update Project_161_viewing_table
        set cub.media_pack = tmp.channel_category
from Project_161_viewing_table as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key
;
commit;

alter table Project_161_viewing_table
        add primary_sales_house varchar(255);


update Project_161_viewing_table
        set cub.primary_sales_house = tmp.primary_sales_house
from Project_161_viewing_table as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key
;
commit;

update Project_161_viewing_table
set media_pack = case when media_pack = 'SKY ENTERTAINMENT' then 'ENTERTAINMENT' else media_pack end
from Project_161_viewing_table
;
commit;

--select channel_name_inc_hd_staggercast , media_pack, primary_sales_house , count(*) as records from Project_161_viewing_table group by channel_name_inc_hd_staggercast, media_pack, primary_sales_house order by records desc

----Update Missing Channels/Service Keys that are not held in current lookups--

update Project_161_viewing_table
set media_pack = case   when channel_name_inc_hd_staggercast = 'Sky Sports News' then 'SPORTS'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 1' then 'SPORTS'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 2' then 'SPORTS'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 3' then 'SPORTS'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 4' then 'SPORTS'
                        when channel_name_inc_hd_staggercast = 'BBC ONE' then 'BBC'
                        when channel_name_inc_hd_staggercast = 'More4' then 'C4 Digital'
                        when channel_name_inc_hd_staggercast = 'Sky Movies Mdn Greats' then 'MOVIES'
                        when channel_name_inc_hd_staggercast = 'Sky Box Office' then 'MOVIES'
                        when media_pack is null then 'Other' 
                        else media_pack end
,primary_sales_house = case   when channel_name_inc_hd_staggercast = 'Sky Sports News' then 'Sky'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 1' then 'Sky'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 2' then 'Sky'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 3' then 'Sky'
                        when channel_name_inc_hd_staggercast = 'Sky Sports 4' then 'Sky'
                        when channel_name_inc_hd_staggercast = 'BBC ONE' then 'BBC'
                        when channel_name_inc_hd_staggercast = 'More4' then 'C4'
                        when channel_name_inc_hd_staggercast = 'Sky Movies Mdn Greats' then 'Sky'
                        when channel_name_inc_hd_staggercast = 'Sky Box Office' then 'Sky' 
                        when primary_sales_house is null then 'Other' else primary_sales_house end
from Project_161_viewing_table
;
commit;

---Use Account Weight as at Midpoint of Analysis (24th May 2012)
select b.account_number
,max(c.weighting) as weight
into #weights
from vespa_analysts.SC2_intervals as b
left outer join vespa_analysts.SC2_weightings as c
on   cast('2012-05-24' as date)= c.scaling_day
where b.reporting_starts<= '2012-05-24'  and b.reporting_ends >= '2012-05-24' and b.scaling_segment_ID = c.scaling_segment_ID
group by b.account_number
;


create unique hg index idx1 on #weights(account_number);
commit;
--select sum(weight) from #weights


--alter table Project_161_viewing_table delete weighting_mid_point;

alter table Project_161_viewing_table add weighting_mid_point double;
update Project_161_viewing_table
set weighting_mid_point=b.weight
from Project_161_viewing_table  as a
left outer join  #weights as b
on a.account_number = b.account_number
commit;

--alter table v161_active_ac_accounts delete account_weight;
alter table v161_active_ac_accounts add account_weight double;

update v161_active_ac_accounts 
set account_weight=b.weight
from v161_active_ac_accounts as a
left outer join  #weights as b
on a.account_number = b.account_number
;
commit;

--select sum(account_weight) from  v161_active_ac_accounts

---Create counts of target groups using vespa weighting---
select case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then '01: No Sports Premiums and High Model Decile'
            when sports_premiums=0 and sports_model_decile not in (1,2,3,4) then '02: No Sports Premiums and not High Model Decile'
            when sports_premiums>0  then '03: Has Sports Premiums'
--Put Unknowns in Group 2--
 else '02: No Sports Premiums and not High Model Decile'
 end as segment
,sum(account_weight) as UK_Accounts
--,count(*) as records
from v161_active_ac_accounts
group by segment 
order by segment
;

--Count by Target/Wastage

select case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then '01: Target No Sports Premiums and High Model Decile'
            when sports_premiums=0 and sports_model_decile not in (1,2,3,4) then '02: Wastage'
            when sports_premiums>0  then '02: Wastage'
--Put Unknowns in Group 2--
 else '02: Wastage'
 end as segment
,sum(account_weight) as UK_Accounts
--,count(*) as records
from v161_active_ac_accounts
group by segment 
order by segment
;

commit;

---Region Count----
select case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then '01: Target No Sports Premiums and High Model Decile'
            when sports_premiums=0 and sports_model_decile not in (1,2,3,4) then '02: Wastage'
            when sports_premiums>0  then '02: Wastage'
--Put Unknowns in Group 2--
 else '02: Wastage'
 end as segment
,region
,sum(account_weight) as UK_Accounts
--,count(*) as records
from v161_active_ac_accounts
group by segment ,region
order by segment,region
;





---Apply Target/Not Target Split to all viewing---

alter table Project_161_viewing_table add target tinyint;
update Project_161_viewing_table
set target=case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then 1 else 0 end
from Project_161_viewing_table  as a
left outer join v161_active_ac_accounts as b
on a.account_number = b.account_number
commit;

---PART C - Efficiency Summaries---
select account_number
,media_pack
,primary_sales_house
,target
,max(weighting_mid_point) as account_weight
into #viewing_by_media_pack_and_sales_house
from Project_161_viewing_table as a
group by account_number
,media_pack
,primary_sales_house
,target
;

--Calculate Efficiency Figures
select media_pack
,primary_sales_house
,sum(target*account_weight) as target_accounts
,sum(case when target =0 then account_weight else 0 end) as wastage_accounts
from #viewing_by_media_pack_and_sales_house
group by media_pack
,primary_sales_house
;


---Efficiency by Channel---
select account_number
,media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,target
,max(weighting_mid_point) as account_weight
into #viewing_by_channel_media_pack_and_sales_house
from Project_161_viewing_table as a
group by account_number
,media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,target
;


select media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,sum(target*account_weight) as target_accounts
,sum(case when target =0 then account_weight else 0 end) as wastage_accounts
from #viewing_by_channel_media_pack_and_sales_house
group by media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
order by target_accounts desc
;

---Efficiency by Programme---
--drop table #viewing_by_programme_media_pack_and_sales_house;
select account_number
,media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,case 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc)
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
                    else non_staggercast_broadcast_time_utc  end as non_staggercast_broadcast_time_local
,Epg_Title
,target
,max(weighting_mid_point) as account_weight
into #viewing_by_programme_media_pack_and_sales_house
from Project_161_viewing_table as a
group by account_number
,media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,non_staggercast_broadcast_time_local
,Epg_Title
,target
;

select media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,non_staggercast_broadcast_time_local
,Epg_Title
,sum(target*account_weight) as target_accounts
,sum(case when target =0 then account_weight else 0 end) as wastage_accounts
,rank() over  (partition by media_pack,primary_sales_house order by target_accounts desc) as rank_pack
into #rank_by_programme
from #viewing_by_programme_media_pack_and_sales_house

group by media_pack
,primary_sales_house
,channel_name_inc_hd_staggercast
,non_staggercast_broadcast_time_local
,Epg_Title

order by target_accounts desc

;

select * from #rank_by_programme
where rank_pack<=300
order by target_accounts desc
;

----Create Output Pivot----

---Run Efficency Index at individual Media Level---
--drop table project134_3_plus_minute_summary_by_media_pack;
select a.account_number
,max(weighting_mid_point) as overall_project_weighting
,target
,region
,max(case when media_pack = 'DOCUMENTARIES' then 1 else 0 end) as documentaries
,max(case when media_pack = 'ENTERTAINMENT' then 1 else 0 end) as entertainment
,max(case when media_pack = 'NEWS' then 1 else 0 end) as news
,max(case when media_pack = 'MOVIES' then 1 else 0 end) as movies
,max(case when media_pack = 'KIDS' then 1 else 0 end) as kids
,max(case when media_pack = 'MUSIC' and primary_sales_house ='C4' then 1 else 0 end) as music_c4
,max(case when media_pack = 'MUSIC' and primary_sales_house ='Sky' then 1 else 0 end) as music_sky
,max(case when media_pack = 'LIFESTYLE & CULTURE' then 1 else 0 end) as Lifestyle_Culture
,max(case when media_pack = 'SPORTS' then 1 else 0 end) as Sports
,max(case when media_pack = 'C4' then 1 else 0 end) as C4
,max(case when media_pack = 'C4 Digital' then 1 else 0 end) as C4_Digital
,max(case when media_pack = 'FIVE' then 1 else 0 end) as FIVE
,max(case when media_pack = 'FIVE Digital' then 1 else 0 end) as FIVE_Digital
,max(case when media_pack = 'ITV' then 1 else 0 end) as ITV
,max(case when media_pack = 'ITV Digital' then 1 else 0 end) as ITV_Digital
,max(case when media_pack = 'UKTV' then 1 else 0 end) as UKTV
into project161_summary_by_media_pack
from Project_161_viewing_table as a
left outer join v161_active_ac_accounts as b
on a.account_number = b.account_number
where weighting_mid_point>0
group by  a.account_number
,target
,region
;
commit;

--select distinct region from  project161_summary_by_media_pack order by region;

---Create Output for Pivot----

select target
,case when region is null then 'UNKNOWN' else region end as account_region
,documentaries
,entertainment
,news
,movies
,kids
,music_C4
,music_sky
,Lifestyle_Culture
,Sports
,C4
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV

, sum(documentaries*overall_project_weighting) as documentary_total
, sum(entertainment*overall_project_weighting) as entertainment_total
, sum(news*overall_project_weighting) as news_total
, sum(movies*overall_project_weighting) as movies_total
, sum(kids*overall_project_weighting) as kids_total
, sum(music_c4*overall_project_weighting) as music_C4_total
, sum(music_sky*overall_project_weighting) as music_sky_total
, sum(Lifestyle_Culture*overall_project_weighting) as Lifestyle_Culture_total
, sum(Sports*overall_project_weighting) as Sports_total

, sum(C4*overall_project_weighting) as C4_total
, sum(C4_Digital*overall_project_weighting) as C4_Digital_total
, sum(FIVE*overall_project_weighting) as FIVE_total
, sum(FIVE_Digital*overall_project_weighting) as FIVE_Digital_total
, sum(ITV*overall_project_weighting) as ITV_total
, sum(ITV_Digital*overall_project_weighting) as ITV_Digital_total
, sum(UKTV*overall_project_weighting) as UKTV_total

from project161_summary_by_media_pack
group by  target
,account_region
,documentaries
,entertainment
,news
,movies
,kids
,music_C4
,music_sky
,Lifestyle_Culture
,Sports
,C4
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
order by target
,account_region
,documentaries
,entertainment
,news
,movies
,kids
,music_C4
,music_sky
,Lifestyle_Culture
,Sports
,C4
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
;
output to 'C:\Users\barnetd\Documents\V161 - Sky Sports ATL\efficiency pivot data.csv' format ascii;
commit;

--select top 100 * from SK_PROD.VESPA_DP_PROG_VIEWED_CURRENT where account_number = '210049989283' order by src_system_id,event_start_date_time_utc


--select count(*) , sum(case when weighting_mid_point>0 then 1 else 0 end) from Project_161_viewing_table ;
/* Test Code

--select * from neighbom.CHANNEL_MAP_DEV_BARB_CHANNEL_GROUP;
--select * from LkUpPack_old
---Run count by Programme---
select channel_name_inc_hd_staggercast
,epg_title
,non_staggercast_broadcast_time_utc
,count(distinct account_number) as accounts
from Project_161_viewing_table
where timeshifting = 'LIVE' and channel_name_inc_hd_staggercast is not null and channel_name_inc_hd_staggercast='ITV2'
group by channel_name_inc_hd_staggercast
,epg_title
,non_staggercast_broadcast_time_utc
order by accounts desc
;

*/



/*
--select count(*), sum(case when weighting>0 then 1 else 0 end) as weights from Project_161_viewing_table;

select account_number, count(*), sum(case when weighting>0 then 1 else 0 end) as weights
 into #has_weights
 from Project_161_viewing_table
 group by account_number
order by weights;

select * from #has_weights order by weights desc
select case when weights=0 then 'none' else 'other' end as weight_type , sum(weights) , count(*) from #has_weights group by weight_type order by weight_type;



select * from vespa_analysts.SC2_intervals where account_number ='220017056908' order by reporting_starts

select * from vespa_analysts.SC2_intervals where account_number ='220006611424' order by reporting_starts

select * from vespa_analysts.SC2_intervals where account_number ='621074859730' order by reporting_starts





/*

select top 100 * from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-10-23' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-10-23' as date) between a.reporting_starts and a.reporting_ends


select top 100 * from models.model_scores 
where model_name = 'Sports Mailed'
and model_run_date =   '2012-05-16'

select decile , count(*) from models.model_scores 
where model_name = 'Sports Mailed'
and model_run_date =   '2012-05-16'
group by decile order by decile;

commit;



select top 100 * from vespa_analysts.ph1_vespa_daily_augs_20120517

commit;

commit;

drop table dbarnett.v141_live_playback_viewing
;
commit;


---Create counts of target groups---
select case when sports_premiums=0 and sports_model_decile in (1,2,3,4) then '01: No Sports Premiums and High Model Decile'
            when sports_premiums=0 and sports_model_decile not in (1,2,3,4) then '02: No Sports Premiums and not High Model Decile'
            when sports_premiums>0  then '03: Has Sports Premiums'
--Put Unknowns in Group 2--
 else '02: No Sports Premiums and not High Model Decile'
 end as segment
,sports_model_decile

, count(*) as accounts
,sum(UK_Account) as UK_Accounts
from v161_active_ac_accounts

group by segment ,sports_model_decile
order by segment,sports_model_decile
;
commit;

select b.account_number
,c.weighting
into #counts
from vespa_analysts.SC2_intervals as b
left outer join vespa_analysts.SC2_weightings as c
on  cast ('2012-05-24' as date) = c.scaling_day
where b.scaling_segment_ID = c.scaling_segment_ID
group by b.account_number
,c.weighting

select top 500 * from #counts order by account_number
select top 500 * from vespa_analysts.SC2_weightings where scaling_day=cast ('2012-05-24' as date)
--select sum(weighting) from #counts

select top 500 * from vespa_analysts.SC2_weightings
commit;


select top 500


select scaling_day, sum(vespa_accounts) , sum(weighting*vespa_accounts) from vespa_analysts.SC2_weightings group by scaling_day order by scaling_day

*/