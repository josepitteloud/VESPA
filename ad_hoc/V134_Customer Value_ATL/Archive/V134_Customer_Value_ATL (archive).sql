/*------------------------------------------------------------------------------
        Project: V134 - Customer Value ATL
        Version: 1
        Created: 20120919
        Lead: Susanne Chan
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
        Purpose
        -------
In Jan / Feb an ATL campaign will be aired with the aim to reduce platform churn. 
The unproven hypothesis is that by improving the perceived value of Sky with the 
existing customer base, we will increase retention.  Specifically by raising awareness of new content, SkyGo and Sky+ 

        SECTIONS
        --------

        PART A   - Generate Segments around Likely/Unlikely to churn and Sky Go Usage and Sky+ Usage
             A01 - Likely/Unlikely to Churn
             A02 - Sky Go Usage
             A03 - Sky+ Usage

        Part B - Generate Overall HH base from which Segment Sizes are determined e.g., Midpoint population and weights

        Part C - Run Figures for 3+ minutes Live continuous viewing of any programme

        Part D - Aggregate HD/SD/+1 channel figures together
*/

---A01 Likely/Unlikely to Churn

---Value Segment Code Taken from cust_value_segments at 
---http://mktskyportal/Shared%20Documents/Forms/AllItems.aspx?RootFolder=/Shared Documents/Analytics/Forms and Templates/Common Scripts 
/*
          SECTIONS
          --------
          01 - Create table
          02 - Populate table
          03 - SAV Updates
          04 - Long term events
          05 - TA Events
          06 - Min Max Premiums
          07 - Make Value Segments


*/



-------------------------------------------------  01 - Create table

CREATE TABLE #value_segments(
        id                      BIGINT          IDENTITY PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,subscription_id         VARCHAR(50)     NULL
       ,target_date             DATE            NOT NULL
       ,segment                 VARCHAR(20)     NULL

       ,first_activation_dt     DATE            NULL
       ,active_days             INTEGER         NULL
       ,CUSCAN_ever             INTEGER         DEFAULT 0
       ,CUSCAN_2Yrs             INTEGER         DEFAULT 0
       ,SYSCAN_ever             INTEGER         DEFAULT 0
       ,SYSCAN_2Yrs             INTEGER         DEFAULT 0
       ,AB_ever                 INTEGER         DEFAULT 0
       ,AB_2Yrs                 INTEGER         DEFAULT 0
       ,PC_ever                 INTEGER         DEFAULT 0
       ,PC_2Yrs                 INTEGER         DEFAULT 0
       ,TA_2yrs                 INTEGER         DEFAULT 0
       ,min_prem_2yrs           INTEGER         DEFAULT 0
       ,max_prem_2yrs           INTEGER         DEFAULT 0
       ,segment_cuscan_only                 VARCHAR(20)     NULL
);

CREATE   HG INDEX idx01 ON #value_segments(account_number);
CREATE DATE INDEX idx02 ON #value_segments(target_date);
CREATE   LF INDEX idx03 ON #value_segments(segment);
CREATE   HG INDEX idx04 ON #value_segments(subscription_id);


-------------------------------------------------  02 - Populate table

-- Alter this query to append the accounts and specific dates you want to identify the segments for.

  INSERT INTO #value_segments (account_number, target_date)
  SELECT account_number, cast('2012-10-15' as date)
    FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
 ;



------------------------------------------------ 03 - SAV = First Activation Date & Subscription ID


UPDATE  #value_segments
   SET  first_activation_dt   = sav.ph_subs_first_activation_dt
       ,subscription_id       = sav.prod_ph_subs_subscription_id
  FROM #value_segments AS acc
       INNER JOIN sk_prod.cust_single_account_view AS sav ON acc.account_number = sav.account_number;


UPDATE #value_segments
   SET active_days = DATEDIFF(day,first_activation_dt,target_date);


----------------------------------------------- 04 - Long term events

--unique list of accounts
  SELECT account_number, subscription_id, MAX(target_date) as maxDate, MIN(target_date) as minDate
    INTO #account_list
    FROM #value_segments
GROUP BY account_number, subscription_id;

CREATE  HG INDEX idx01 ON #account_list(account_number);
CREATE HG INDEX idx02 ON #account_list(subscription_id);

--historic status event changes
CREATE TABLE #status_events (
        id                      BIGINT          IDENTITY     PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,effective_from_dt       DATE            NOT NULL
       ,status_code             VARCHAR(2)      NOT NULL
       ,event_type              VARCHAR(20)     NOT NULL
);

CREATE   HG INDEX idx01 ON #status_events(account_number);
CREATE   LF INDEX idx02 ON #status_events(event_type);
CREATE DATE INDEX idx03 ON #status_events(effective_from_dt);


INSERT INTO #status_events (account_number, effective_from_dt, status_code, event_type)
SELECT  csh.account_number
       ,csh.effective_from_dt
       ,csh.status_code
       ,CASE WHEN status_code = 'PO'              THEN 'CUSCAN'
             WHEN status_code = 'SC'              THEN 'SYSCAN'
             WHEN status_code = 'AB'              THEN 'ACTIVE BLOCK'
             WHEN status_code = 'PC'              THEN 'PENDING CANCEL'
         END AS event_type
  FROM sk_prod.cust_subs_hist AS csh
       INNER JOIN #account_list AS al ON csh.account_number = al.account_number
 WHERE csh.subscription_sub_type = 'DTV Primary Viewing'
   AND csh.status_code_changed = 'Y'
   AND csh.effective_from_dt <= al.maxDate
   AND (    (csh.status_code IN ('AB','PC') AND csh.prev_status_code = 'AC')
         OR (csh.status_code IN ('PO','SC') AND csh.prev_status_code IN ('AC','AB','PC'))
       );


-- Update value Segments

UPDATE  #value_segments
   SET  CUSCAN_ever             = tgt.CUSCAN_ever
       ,CUSCAN_2Yrs             = tgt.CUSCAN_2Yrs
       ,SYSCAN_ever             = tgt.SYSCAN_ever
       ,SYSCAN_2Yrs             = tgt.SYSCAN_2Yrs
       ,AB_ever                 = tgt.AB_ever
       ,AB_2Yrs                 = tgt.AB_2Yrs
       ,PC_ever                 = tgt.PC_ever
       ,PC_2Yrs                 = tgt.PC_2Yrs
  FROM #value_segments AS base
       INNER JOIN (
                    SELECT vs.id

                           --CUSCAN
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND  se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_2Yrs

                           --SYSCAN
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND  se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_2Yrs

                           --Active Block
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS AB_ever
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS AB_2Yrs

                           --Pending Cancel
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS PC_ever
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS PC_2Yrs
                      FROM #value_segments AS vs
                           INNER JOIN #status_events AS se ON vs.account_number = se.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id;


------------------------------------------------------------ 05 - TA Events


--List all unique days with TA event
SELECT  DISTINCT
        cca.account_number
       ,cca.attempt_date
  INTO #ta
  FROM sk_prod.cust_change_attempt AS cca
       INNER JOIN #account_list AS al  on cca.account_number = al.account_number
                                      AND cca.subscription_id = al.subscription_id
 WHERE change_attempt_type = 'CANCELLATION ATTEMPT'
   AND created_by_id NOT IN ('dpsbtprd', 'batchuser')
   AND Wh_Attempt_Outcome_Description_1 in ( 'Turnaround Saved'
                                            ,'Legacy Save'
                                            ,'Turnaround Not Saved'
                                            ,'Legacy Fail'
                                            ,'Home Move Saved'
                                            ,'Home Move Not Saved'
                                            ,'Home Move Accept Saved')
   AND cca.attempt_date BETWEEN DATEADD(day,-729,al.minDate) AND al.maxDate ;


CREATE HG INDEX idx01 ON #ta(account_number);

-- Update TA flags
UPDATE  #value_segments
   SET  TA_2Yrs = tgt.ta_2Yrs
  FROM #value_segments AS base
       INNER JOIN (
                    SELECT vs.id
                          ,SUM(CASE WHEN ta.attempt_date BETWEEN DATEADD(day,-729,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS ta_2Yrs
                      FROM #value_segments AS vs
                           INNER JOIN #ta AS ta ON vs.account_number = ta.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id;

------------------------------------------------------ 06 - Min Max Premiums

UPDATE  #value_segments
   SET  min_prem_2Yrs = tgt.min_prem_lst_2_yrs
       ,max_prem_2Yrs = tgt.max_prem_lst_2_yrs
  FROM  #value_segments AS acc
        INNER JOIN (
                   SELECT  base.id
                          ,MAX(cel.prem_movies + cel.prem_sports ) as max_prem_lst_2_yrs
                          ,MIN(cel.prem_movies + cel.prem_sports ) as min_prem_lst_2_yrs
                     FROM sk_prod.cust_subs_hist as csh
                          INNER JOIN #value_segments as base on csh.account_number = base.account_number
                          INNER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
                    WHERE csh.subscription_type      =  'DTV PACKAGE'
                      AND csh.subscription_sub_type  =  'DTV Primary Viewing'
                      AND status_code in ('AC','AB','PC')
                      AND ( -- During 2 year Period
                            (    csh.effective_from_dt BETWEEN DATEADD(day,-729,base.target_date) AND base.target_date
                             AND csh.effective_to_dt >= csh.effective_from_dt
                             )
                            OR -- at start of 2 yr period
                            (    csh.effective_from_dt <= DATEADD(day,-729,base.target_date)
                             AND csh.effective_to_dt   > DATEADD(day,-729,base.target_date)  -- limit to report period
                             )
                          )
                  GROUP BY base.id
        )AS tgt ON acc.id = tgt.id;



------------------------------------------------------ 07 - Make Value Segments


UPDATE #value_segments
   SET       segment =     CASE WHEN active_days < 729                            -- All accounts in first 2 Years
                                THEN 'BEDDING IN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No TA's in last 2 years
                                 AND min_prem_2yrs = 4                            -- Always top tier for last 2 years
                                THEN 'PLATINUM'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER'

                                WHEN CUSCAN_ever + SYSCAN_ever > 0                -- All Churners
                                  OR AB_2Yrs + PC_2Yrs + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE'

                                ELSE 'COPPER'                                        -- everyone else
                            END;
---Added code for Project 134
UPDATE #value_segments
   SET       segment_cuscan_only =     CASE WHEN active_days < 729                            -- All accounts in first 2 Years
                                THEN 'BEDDING IN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No TA's in last 2 years
                                 AND min_prem_2yrs = 4                            -- Always top tier for last 2 years
                                THEN 'PLATINUM'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER'

                                WHEN CUSCAN_ever  > 0                -- All Cuscan Churners
                                  OR  PC_2Yrs + ta_2Yrs >= 3             -- Pending cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE CUSCAN'

                                WHEN CUSCAN_ever + SYSCAN_ever > 0                -- All Churners
                                  OR AB_2Yrs + PC_2Yrs + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE NON CUSCAN'

                                WHEN max_prem_2Yrs > 0 and  AB_2Yrs=0           -- Has Had prems in last 2 years and no AB
                                THEN 'BRONZE CUSCAN'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE NON CUSCAN'

                                WHEN  AB_2Yrs=0           -- No AB in last 2 years
                                THEN 'COPPER CUSCAN'

                                ELSE 'COPPER NON CUSCAN'                                        -- everyone else
                            END;

/*
select segment
,segment_cuscan_only
,count(*) as records
from #value_segments
group by segment
,segment_cuscan_only;
*/


---A02 Sky Go Usage in 3mths ending 11th Nov
--Som

if object_id('project134_sky_go_3mth') is not null drop table project134_sky_go_3mth;
select  account_number ,max(case when site_name = 'XBOX' then 1 else 0 end) as xbox_user into project134_sky_go_3mth from sk_prod.SKY_PLAYER_USAGE_DETAIL where  activity_dt between '2012-08-12' and '2012-11-11' group by account_number;

--select site_name ,count(*)  from sk_prod.SKY_PLAYER_USAGE_DETAIL where  activity_dt between '2012-08-12' and '2012-11-11' group by site_name ;

--select 
commit;
--select * into project134_sky_go_3mth from #project134_sky_go_3mth;
commit;

commit;


create  hg index idx1 on project134_sky_go_3mth(account_number);

---A03 Accounts that have Used Sky+ During viewing period
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(3000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;


-- Date range of programmes to capture
SET @var_prog_period_start  = '2012-10-15';
SET @var_prog_period_end    = '2012-11-11';
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @var_num_days = 29;
commit;

if object_id('project134_sky_plus_user') is not null drop table project134_sky_plus_user;
create table project134_sky_plus_user (
Account_Number                 varchar(20)     not null
);

--select top 10 * from Disney_viewing_table_dump
commit;
-- Build string with placeholder for changing daily table reference
SET @var_sql = '
insert into project134_sky_plus_user(
Account_Number
)
select
    distinct da.Account_Number
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as da
where da.timeshifting<>''LIVE''
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

create  hg index idx1 on project134_sky_plus_user(account_number);

select account_number 
into #project134_sky_plus_user_deduped
from project134_sky_plus_user
group by account_number
;

commit;

select * into project134_sky_plus_user_deduped from  #project134_sky_plus_user_deduped;
commit;

create  hg index idx1 on project134_sky_plus_user_deduped(account_number);


---Part B-----
---Create Base table of all accounts used for analysis

if object_id('project_134_base_Accounts') is not null drop table project_134_base_Accounts;

create table project_134_base_Accounts
(account_number varchar(20)
,overall_project_weighting double
,value_segment varchar(30)
,value_segment_cuscan_only varchar (30)
,sky_go_last_3m tinyint default 0
,Sky_plus_usage_last_3m tinyint default 0
)
;
commit;

insert into project_134_base_Accounts
select a.account_number
,b.weighting as overall_project_weighting
,null
,null
,0
,0
--into project_134_base_Accounts
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-10-29' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-10-29' as date) between a.reporting_starts and a.reporting_ends
;
commit;


create  hg index idx1 on project_134_base_Accounts(account_number);
commit;

---Add in Extra Variables---
update project_134_base_Accounts
set value_segment=case when b.account_number is null then 'Unknown' else b.segment end
,value_segment_cuscan_only=case when b.account_number is null then 'Unknown' else b.segment_cuscan_only end
from project_134_base_Accounts as a
left outer join #value_segments as b
on a.account_number = b.account_number
;
commit;

---Add in Sky Go Status---
update project_134_base_Accounts
set sky_go_last_3m=case when b.account_number is not null then 1 else 0 end
from project_134_base_Accounts as a
left outer join project134_sky_go_3mth as b
on a.account_number = b.account_number
;
commit;

---Add in Sky Plus User Status---
update project_134_base_Accounts
set Sky_plus_usage_last_3m=case when b.account_number is not null then 1 else 0 end
from project_134_base_Accounts as a
left outer join project134_sky_plus_user_deduped as b
on a.account_number = b.account_number
;
commit;

--drop table project134_sky_plus_user_deduped;
--drop table project134_sky_go_3mth;
--drop table project134_sky_plus_user;


--select count(*) , sum(overall_project_weighting) from  project_134_base_Accounts
--select value_segment_cuscan_only , count(*) , sum(overall_project_weighting) from  project_134_base_Accounts group by value_segment_cuscan_only
--select sky_go_last_3m , count(*) , sum(overall_project_weighting) from  project_134_base_Accounts group by sky_go_last_3m
--select Sky_plus_usage_last_3m , count(*) , sum(overall_project_weighting) from  project_134_base_Accounts group by Sky_plus_usage_last_3m
--select top 100 * from vespa_analysts.SC2_weightings;
--select top 100 * from vespa_analysts.SC2_intervals;

----Create Segments for Waterfall---

select case 
when value_segment_cuscan_only in ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN') then '1: Cuscan Churn Risk' else '2: Other' end as cuscan_churn_risk
, sky_go_last_3m
,Sky_plus_usage_last_3m
,sum(overall_project_weighting) as accounts
from  project_134_base_Accounts
group by cuscan_churn_risk
, sky_go_last_3m
,Sky_plus_usage_last_3m
order by cuscan_churn_risk
, sky_go_last_3m
,Sky_plus_usage_last_3m
;

commit;


----------------------------------
-- CLEAN UP THE CHANNEL NAMES SO WE HAVE A BETTER IDEA OF WHICH CHANNELS THE SPOTS WERE SEEN ON:
--Code from Susanne Chan/Harry Gill but changed to run for all Channels not just the Barclays spot related channels
----------------------------------

--AGGREGATE +1 AND HD variations
-- drop table #channel1

select  service_key
        ,case when right(channel_name,2) = 'HD' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,2) = '+1' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,1) = '+' THEN LEFT(channel_name,(LEN(channel_name)-1))
                                                ELSE channel_name END AS Channel
        ,channel_name
INTO #channel1
FROM sk_prod.vespa_epg_dim
group by channel_name,service_key
;

--drop table #channel2
--select * from #channel2 order by channel;

SELECT service_key
        ,RTRIM(channel) as Channel
        ,channel_name

INTO    #channel2
FROM    #channel1
;

-- now adjust the names that didn't work above - forn example +2's etc

if object_id('LkUpChannel') is not null drop table LkUpChannel

SELECT  service_key
        ,case when channel = 'BBC ONE'     THEN 'BBC ONE HD'
             when left(channel,5) = 'BBC 1' THEN 'BBC 1'
             when left(channel,5) = 'BBC 2' THEN 'BBC 2'
             when channel_name = 'BBC HD' THEN 'BBC HD'
             when left(channel,4) = 'ITV1' THEN 'ITV1'
             when channel = 'ComedyCtrl' THEN 'ComedyCentral'
             when channel = 'Comedy Cen' THEN 'ComedyCentral'
             when channel = 'Sky Sp News' THEN 'Sky Spts News'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'Sky Sports HD1' THEN 'Sky Sports 1'
             when channel = 'FX+' THEN 'FX'
             when channel = 'Nick Replay' THEN 'Nickelodeon'
             when channel = 'Sky Sports HD2' THEN 'Sky Sports 2'
             when channel = 'Sky Sports HD3' THEN 'Sky Sports 3'
             when channel = 'Sky Sports HD4' THEN 'Sky Sports 4'
             when channel = 'mov4men2' THEN 'movies4men 2'
             when channel = 'mov4men' THEN 'movies4men'
             when channel = 'ComedyCtlX' THEN 'ComedyCtralX'
             when channel = 'horror ch' THEN 'horror channel'
             when channel = 'History +1 hour' THEN 'History'
             when channel = 'Disc.RT' THEN 'Disc.RealTime'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Cartoon Net' THEN 'Cartoon Netwrk'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Eurosport' THEN 'Eurosport UK'
             when channel = 'Food Netwrk' THEN 'Food Network'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'Animal Plnt' THEN 'Animal Planet'
             when channel = 'Disc.Sci' THEN 'Disc.Science'
             when channel = 'ESPN AmrcaHD' THEN 'ESPN America'

             when channel = 'AnimalPlnt' THEN 'Animal Planet'
             when channel = 'Chart Show' THEN 'Chart Show TV'
             when channel = 'DMAX+2' THEN 'DMAX'
             when channel = 'Home & Health' THEN 'Home&Health'
             when channel = 'Nat Geo+1hr' THEN 'Nat Geo'
             when channel = 'NatGeoWild' THEN 'Nat Geo Wild'
             when channel = 'Sky ScFi/Hor' THEN 'Sky ScFi/Horror'
             when channel = 'Travel Ch' THEN 'Travel Channel'
             when channel = 'Sky Prem' THEN 'Sky Premiere'
             when channel = 'Sky MdnGrts' THEN 'Sky Mdn Greats'
             when channel = 'Sky 1' THEN 'Sky1'
             when channel = 'Sky DraRom' THEN 'Sky DramaRom'
             when channel = 'Sky Spts F1' THEN 'Sky Sports F1'
             when channel = 'SkyPremiere' THEN 'Sky Premiere'
             when channel = 'SkyShowcase' THEN 'Sky Showcase'
             when channel = 'SkyShowcse' THEN 'Sky Showcase'



                                                    ELSE channel END AS Channel
        ,channel_name
INTO LkUpChannel
FROM #channel2
group by channel_name, channel,service_key
order by channel
;

--select * from LkUpChannel order by upper(channel);





---Get details of Programmes Watched 3+ Minutes of---
CREATE VARIABLE @viewing_var_prog_period_start  datetime;
CREATE VARIABLE @viewing_var_prog_period_end    datetime;
CREATE VARIABLE @viewing_var_sql                varchar(3000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @viewing_var_num_days           smallint;


-- Date range of programmes to capture
SET @viewing_var_prog_period_start  = '2012-10-15';
SET @viewing_var_prog_period_end    = '2012-11-11';
-- How many days (after end of broadcast period) to check for timeshifted viewing
SET @viewing_var_num_days = 29;
commit;

if object_id('project134_3_plus_minute_prog_viewed') is not null drop table project134_3_plus_minute_prog_viewed;
create table project134_3_plus_minute_prog_viewed (
Account_Number                 varchar(20)     not null
,cb_row_id                      bigint
,programme_trans_sk             bigint
,service_key                    bigint
,channel_name                   varchar(60)
,grouped_channel                varchar(60)
,broadcast_time_utc                 datetime
,non_staggercast_broadcast_time_utc                 datetime
);

--select top 10 * from Disney_viewing_table_dump
commit;
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
insert into project134_3_plus_minute_prog_viewed(
Account_Number
,programme_trans_sk
,null
,null
,null
,null
,null
)
select
     da.Account_Number , programme_trans_sk
,null
,null
,null
,null
,null
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as da
left outer join sk_prod.VESPA_EVENTS_ALL as ve
on da.cb_row_id = ve.pk_viewing_prog_instance_fact
left outer join LkUpChannel as lkup
on ve.service_key=lkup.service_key
where da.timeshifting=''LIVE'' and viewing_duration>=180
group by da.Account_Number , programme_trans_sk






update project134_3_plus_minute_prog_viewed
set service_key = b.service_key
,channel_name=b.channel_name
,broadcast_time_utc=b.tx_date_time_utc
,non_staggercast_broadcast_time_utc=case    when right(b.channel_name,2) = ''+1'' then dateadd(hh,-1,b.tx_date_time_utc) 
                                            when right(channel_name,1) = ''+'' then dateadd(hh,-1,b.tx_date_time_utc) 
from project134_3_plus_minute_prog_viewed as a
left outer join sk_prod.vespa_epg_dim as b
on a.programme_trans_sk=b.programme_trans_sk
;

update project134_3_plus_minute_prog_viewed
set grouped_channel = b.channel
from project134_3_plus_minute_prog_viewed as a
left outer join LkUpChannel as b
on a.service_key=b.service_key
;



';
-- Filter for viewing events is applied on the daily augs table already.
-- Loop over the days in the period, extracting all the data.
commit;

SET @viewing_scanning_day = @viewing_var_prog_period_start;
--delete from Disney_viewing_table_dump;
commit;
while @viewing_scanning_day <= dateadd(dd,0,@viewing_var_prog_period_end)
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end;
commit;


--select top 100 * from  vespa_analysts.VESPA_DAILY_AUGS_20121110
--select top 100 * from sk_prod.vespa_epg_dim where dk_programme_instance_dim=233032299

Cb_Row_Id,Account_Number,Subscriber_Id,Programme_Trans_Sk,Timeshifting,Viewing_Starts,Viewing_Stops,Viewing_Duration,Capped_Flag,Capped_Event_End_Time,Scaling_Segment_Id,Scaling_Weighting,BARB_Minute_Start,BARB_Minute_End
18799243773,'210003070568',110593,233032299,'LIVE','2012-11-10 00:00:00.000','2012-11-10 00:02:54.000',174,0,,183152,17.580593,,
select * from vespa_analysts.VESPA_DAILY_AUGS_20121110 where cb_row_id = 18799243773
select top 500 *  from sk_prod.VESPA_EVENTS_ALL where pk_viewing_prog_instance_fact=18799243773

select top 500 *  from sk_prod.VESPA_EVENTS_ALL where subscriber_id = 110593 and 
select top 500 dk_programme_instance_dim  from sk_prod.VESPA_EVENTS_ALL

select * from sk_prod.vespa_epg_dim
where programme_trans_sk = 2346029100


select top 500 dk_programme_instance_dim ,channel_name  from sk_prod.VESPA_EVENTS_ALL where 
dk_programme_instance_dim =201111170000000366

select top 500 subscriber_id,dk_programme_instance_dim,service_key,channel_name
,programme_instance_name,pk_viewing_prog_instance_fact,instance_start_date_time_utc,instance_end_date_time_utc  from sk_prod.VESPA_EVENTS_ALL
where subscriber_id is not null and instance_start_date


select * from sk_prod.vespa_epg_dim where service_key = 6020 and tx_date = '20120611' order by tx_date_time_utc

































