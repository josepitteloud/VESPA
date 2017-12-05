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
                                 AND ta_2Yrs > 0
                                THEN 'GOLD CUSCAN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD NON CUSCAN'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                 AND ta_2Yrs > 0
                                THEN 'SILVER CUSCAN'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER NON CUSCAN'

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

---A02b - Sky Go Usage (records and distinct days within period
if object_id('project134_sky_go_usage_by_account') is not null drop table project134_sky_go_usage_by_account;
select  account_number ,count(*) as records , count(distinct activity_dt) as days_used  into project134_sky_go_usage_by_account from sk_prod.SKY_PLAYER_USAGE_DETAIL 
where  activity_dt between '2012-08-12' and '2012-11-11' 
and x_usage_type = 'Live Viewing'
group by account_number;

commit;

select days_used
,count(*) as accounts
from project134_sky_go_usage_by_account
group by days_used
order by days_used
;

select case when records>=500 then 500 else records end as total_sessions
,count(*) as accounts
from project134_sky_go_usage_by_account
group by total_sessions
order by total_sessions
;
commit;


---A02c - Sky Go Usage (records and distinct days within period - Excluding Sports Activity
if object_id('project134_sky_go_usage_by_account_exc_sports') is not null drop table project134_sky_go_usage_by_account_exc_sports;
select  account_number ,count(*) as records , count(distinct activity_dt) as days_used  into project134_sky_go_usage_by_account_exc_sports from sk_prod.SKY_PLAYER_USAGE_DETAIL 
where  activity_dt between '2012-08-12' and '2012-11-11' 
and x_usage_type = 'Live Viewing' and right(channel,6)<>'SPORTS'
group by account_number;

commit;


select days_used
,count(*) as accounts
from project134_sky_go_usage_by_account_exc_sports
group by days_used
order by days_used
;


select case when records>=500 then 500 else records end as total_sessions
,count(*) as accounts
from project134_sky_go_usage_by_account_exc_sports
group by total_sessions
order by total_sessions
;
commit;

--select top 100 * from sk_prod.SKY_PLAYER_USAGE_DETAIL where account_number = '620041578563' order by activity_dt

--select broadcast_channel,channel,count(*) from sk_prod.SKY_PLAYER_USAGE_DETAIL where  x_usage_type = 'Live Viewing' group by  broadcast_channel,channel,genre,x_usage_type;
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

---Add in Cable Area Detail (extra Request 4/12/12)

alter table project_134_base_Accounts add cable_area tinyint;

update project_134_base_Accounts
set cable_area=case when c.cable_postcode = 'y' then 1 else 0 end
from project_134_base_Accounts as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
left outer join sk_prod.BROADBAND_POSTCODE_EXCHANGE as c
on b.cb_address_postcode=c.cb_address_postcode
;
commit;

---Add in definition of 'Heavy' Sky Go User (excluding Sports)
alter table project_134_base_Accounts add heavy_sky_go_user_non_sport tinyint;

update project_134_base_Accounts
set heavy_sky_go_user_non_sport=case when b.records>=6 then 1 else 0 end
from project_134_base_Accounts as a
left outer join project134_sky_go_usage_by_account_exc_sports as b
on a.account_number = b.account_number
;
commit;


--select top 100 * from project_134_base_Accounts;
--select heavy_sky_go_user_non_sport , sum(overall_project_weighting) as accounts from  project_134_base_Accounts group by heavy_sky_go_user_non_sport

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
when value_segment_cuscan_only in ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN') then '1: Cuscan Churn Risk' 
when value_segment_cuscan_only in ('BEDDING IN') then '2: Bedding In' 

else '3: Other' end as cuscan_churn_risk
, sky_go_last_3m
,Sky_plus_usage_last_3m

,sum(overall_project_weighting) as accounts
,sum(overall_project_weighting*cable_area) as cable_area_accounts
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
);

--select top 10 * from Disney_viewing_table_dump
commit;
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
insert into project134_3_plus_minute_prog_viewed(
Account_Number
,cb_row_id
)
select
     da.Account_Number , cb_row_id
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as da
where da.timeshifting=''LIVE'' and viewing_duration>=180
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

alter table project134_3_plus_minute_prog_viewed add (
service_key                    bigint
,channel_name                   varchar(60)
,grouped_channel                varchar(60)
,broadcast_time_utc                 datetime
,non_staggercast_broadcast_time_utc                 datetime);
commit;

create  hg index idx1 on project134_3_plus_minute_prog_viewed(cb_row_id);
update  project134_3_plus_minute_prog_viewed
set service_key                    =b.service_key
,channel_name                   =b.channel_name
,broadcast_time_utc            =b.broadcast_start_date_time_utc
,non_staggercast_broadcast_time_utc =case    when right(b.channel_name,2) = '+1' then dateadd(hh,-1,b.broadcast_start_date_time_utc) 
                                            when right(b.channel_name,1) = '+' then dateadd(hh,-1,b. broadcast_start_date_time_utc) else b.broadcast_start_date_time_utc end
from project134_3_plus_minute_prog_viewed as a
left outer join sk_prod.VESPA_EVENTS_ALL as b
on a.cb_row_id= b.pk_viewing_prog_instance_fact 
;
commit;
--select channel_name , count(*) as records from project134_3_plus_minute_prog_viewed group by channel_name order by records desc;
---Correct BBC One to BBC 1 in Lookup---

update project134_3_plus_minute_prog_viewed
set grouped_channel = case when b.channel ='BBC One' then 'BBC 1' else b.channel end
from project134_3_plus_minute_prog_viewed as a
left outer join LkUpChannel as b
on a.service_key=b.service_key
;
commit;
--select * from  LkUpChannel order by channel_name;
--select grouped_channel ,channel_name, count(*) as records from project134_3_plus_minute_prog_viewed group by grouped_channel,channel_name order by records desc;


--select * from LkUpChannel where channel = 'Sky Summer'
create  hg index idx2 on project134_3_plus_minute_prog_viewed(account_number);
create  lf index idx3 on project134_3_plus_minute_prog_viewed(grouped_channel);
create hg index idx4 on project134_3_plus_minute_prog_viewed(non_staggercast_broadcast_time_utc);
commit;
--Create Deduped version----
--drop table project134_3_plus_minute_prog_viewed_deduped
select account_number
,grouped_channel
,non_staggercast_broadcast_time_utc
,min(service_key) as service_key_detail
into project134_3_plus_minute_prog_viewed_deduped
from project134_3_plus_minute_prog_viewed
group by account_number
,grouped_channel
,non_staggercast_broadcast_time_utc
;

commit;
--select grouped_channel ,service_key_detail,non_staggercast_broadcast_time_utc,count(*) from project134_3_plus_minute_prog_viewed_deduped where non_staggercast_broadcast_time_utc between '2012-10-27 17:00:00' and '2012-10-27 19:00:00' group by  grouped_channel ,service_key_detail,non_staggercast_broadcast_time_utc order by grouped_channel,non_staggercast_broadcast_time_utc;
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

-- now lets put the media pack into the cube.
alter table project134_3_plus_minute_prog_viewed_deduped
        add media_pack varchar(25);


update project134_3_plus_minute_prog_viewed_deduped
        set cub.media_pack = tmp.channel_category
from project134_3_plus_minute_prog_viewed_deduped as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key_detail
;
commit;

alter table project134_3_plus_minute_prog_viewed_deduped
        add primary_sales_house varchar(255);


update project134_3_plus_minute_prog_viewed_deduped
        set cub.primary_sales_house = tmp.primary_sales_house
from project134_3_plus_minute_prog_viewed_deduped as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key_detail
;
commit;

update project134_3_plus_minute_prog_viewed_deduped
set media_pack = case when media_pack = 'SKY ENTERTAINMENT' then 'ENTERTAINMENT' else media_pack end
from project134_3_plus_minute_prog_viewed_deduped
;
commit;

--select * from project134_3_plus_minute_prog_viewed_deduped;
--select * from LkUpPack order by channel
--select  grouped_channel,count(*) from project134_3_plus_minute_prog_viewed_deduped group by grouped_channel order by grouped_channel



--select top 1000 * from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES ;
--select distinct channel_owner  from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES order by channel_owner;

--Count Per Programme (Add on Weighting and Sky Go/Sky+ etc., ifno from Base Table)--
--drop table project134_3_plus_minute_summary_by_programme;
select grouped_channel
,non_staggercast_broadcast_time_utc
,service_key_detail
,case   when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN') 
        then '1: Cuscan Churn Risk' else '2: Other' end as cuscan_churn_risk
,sky_go_last_3m
,Sky_plus_usage_last_3m
,cable_area
,media_pack
,c.pay_free_indicator
,primary_sales_house
,sum(overall_project_weighting) as accounts
into project134_3_plus_minute_summary_by_programme
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_134_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
group by grouped_channel
,non_staggercast_broadcast_time_utc
,service_key_detail
,cuscan_churn_risk
,sky_go_last_3m
,Sky_plus_usage_last_3m
,cable_area
,media_pack
,pay_free_indicator
,primary_sales_house
;
commit;

alter table project134_3_plus_minute_summary_by_programme add programme_name varchar(255);

update project134_3_plus_minute_summary_by_programme
set programme_name=b.programme_instance_name
from project134_3_plus_minute_summary_by_programme as a
left outer join sk_prod.VESPA_PROGRAMME_SCHEDULE as b
on a.service_key_detail = b.service_key and a.non_staggercast_broadcast_time_utc=b.broadcast_start_date_time_utc
;
commit;


alter table project134_3_plus_minute_summary_by_programme add account_segment varchar(255);

update project134_3_plus_minute_summary_by_programme
set account_segment=case 
        when cuscan_churn_risk='1: Cuscan Churn Risk' and sky_go_last_3m =0 then '01: Likely Cuscan Churn, Non Sky Go User'
        when cuscan_churn_risk='1: Cuscan Churn Risk' and Sky_plus_usage_last_3m =0 then '02: Likely Cuscan Churn, Non Sky+ User'
        when cuscan_churn_risk='1: Cuscan Churn Risk' and Sky_plus_usage_last_3m =1 then '03: Likely Cuscan Churn, Sky+ User'
        when cuscan_churn_risk='2: Other' and sky_go_last_3m =0 then '04: Non-Likely Cuscan Churn, Non Sky Go User'
        when cuscan_churn_risk='2: Other' and Sky_plus_usage_last_3m =0 then '05: Non-Likely Cuscan Churn, Non Sky+ User'
        when cuscan_churn_risk='2: Other' and Sky_plus_usage_last_3m =1 then '06: Non-Likely Cuscan Churn, Sky+ User' else '99: Other' end
from project134_3_plus_minute_summary_by_programme 
;
commit;

--select distinct service_key_detail from  project134_3_plus_minute_summary_by_programme where grouped_channel is null

--select * from sk_prod.vespa_epg_dim where service_key =2081



--
select case 
        when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and sky_go_last_3m =0 then '01: Likely Cuscan Churn, Non Sky Go User'
        when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =0 then '02: Likely Cuscan Churn, Non Sky+ User'
        when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =1 then '03: Likely Cuscan Churn, Sky+ User'
        when value_segment_cuscan_only not in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and sky_go_last_3m =0 then '04: Non-Likely Cuscan Churn, Non Sky Go User'
        when value_segment_cuscan_only not in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =0 then '05: Non-Likely Cuscan Churn, Non Sky+ User'
        when value_segment_cuscan_only not in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =1 then '06: Non-Likely Cuscan Churn, Sky+ User' else '99: Other' end as account_segment 
, sum(overall_project_weighting) as total_accounts
from project_134_base_Accounts where overall_project_weighting>0 group by account_segment order by account_segment;


---repeat but with split by value segment---
select case 
        when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and sky_go_last_3m =0 then '01: Likely Cuscan Churn, Non Sky Go User'
        when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =0 then '02: Likely Cuscan Churn, Non Sky+ User'
        when value_segment_cuscan_only in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =1 then '03: Likely Cuscan Churn, Sky+ User'
        when value_segment_cuscan_only not in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and sky_go_last_3m =0 then '04: Non-Likely Cuscan Churn, Non Sky Go User'
        when value_segment_cuscan_only not in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =0 then '05: Non-Likely Cuscan Churn, Non Sky+ User'
        when value_segment_cuscan_only not in 
        ('UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN','BEDDING IN')  and Sky_plus_usage_last_3m =1 then '06: Non-Likely Cuscan Churn, Sky+ User' else '99: Other' end as account_segment 
,value_segment_cuscan_only
, sum(overall_project_weighting) as total_accounts
from project_134_base_Accounts where overall_project_weighting>0 
group by account_segment ,value_segment_cuscan_only
order by account_segment;

commit;
---Repeat but With Gold/Silver Details Added - Non Bedding In Segments Only--
select case when value_segment_cuscan_only in ('BEDDING IN') then '01: Bedding In'
        when value_segment_cuscan_only in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN')  and sky_go_last_3m =0 then '02: Likely Cuscan Churn, Non Sky Go User'
        when value_segment_cuscan_only in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN')  then '03: Likely Cuscan Churn, Sky Go User'
        when value_segment_cuscan_only in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN')  and sky_go_last_3m =0 then '04: Non-Likely Cuscan Churn, Non Sky Go User'
        when value_segment_cuscan_only not in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN')  then '05: Non-Likely Cuscan Churn, Sky Go User' else '99: Other' end as account_segment 
,value_segment_cuscan_only
, sum(overall_project_weighting) as total_accounts
from project_134_base_Accounts where overall_project_weighting>0 
group by account_segment ,value_segment_cuscan_only
order by account_segment;
/*account_segment,count()
account_segment,total_accounts
'01: Likely Cuscan Churn, Non Sky Go User',3970971.002349396
'02: Likely Cuscan Churn, Non Sky+ User',24105.40150809986
'03: Likely Cuscan Churn, Sky+ User',921478.3009952992
'04: Non-Likely Cuscan Churn, Non Sky Go User',3014713.441020955
'05: Non-Likely Cuscan Churn, Non Sky+ User',84355.71550009938
'06: Non-Likely Cuscan Churn, Sky+ User',1431232.1913158207

*/

update project134_3_plus_minute_summary_by_programme 
set grouped_channel= case when grouped_channel='Sky Summer' then 'Sky Movies Showcase' else grouped_channel end
from project134_3_plus_minute_summary_by_programme 
;
commit;
--select top 500 * from project134_3_plus_minute_summary_by_programme where grouped_channel ='Sky Premiere' ;

--select distinct grouped_channel from project134_3_plus_minute_prog_viewed_deduped order by grouped_channel
--drop table #all_programme_summary;
select programme_name
, grouped_channel 
,non_staggercast_broadcast_time_utc
,media_pack
,primary_sales_house
,pay_free_indicator
,sum(accounts) as total_accounts_viewed
,sum(case when account_segment='01: Likely Cuscan Churn, Non Sky Go User' then accounts else 0 end) as segment_01_viewers
,sum(case when account_segment='02: Likely Cuscan Churn, Non Sky+ User' then accounts else 0 end) as segment_02_viewers
,sum(case when account_segment='03: Likely Cuscan Churn, Sky+ User' then accounts else 0 end) as segment_03_viewers
,sum(case when account_segment='04: Non-Likely Cuscan Churn, Non Sky Go User' then accounts else 0 end) as segment_04_viewers
,sum(case when account_segment='05: Non-Likely Cuscan Churn, Non Sky+ User' then accounts else 0 end) as segment_05_viewers
,sum(case when account_segment='06: Non-Likely Cuscan Churn, Sky+ User' then accounts else 0 end) as segment_06_viewers
into #all_programme_summary
from project134_3_plus_minute_summary_by_programme 
group by programme_name
, grouped_channel 
,non_staggercast_broadcast_time_utc
,media_pack
,primary_sales_house
,pay_free_indicator
order by total_accounts_viewed desc;

--select top 500 * from #all_programme_summary where media_pack='MOVIES' and Primary_sales_house='Sky' and pay_free_indicator='Pay' order by total_accounts_viewed desc;
--drop table #all_programme_rank_summary;
select programme_name
, grouped_channel 
,case 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
                    else non_staggercast_broadcast_time_utc  end as non_staggercast_broadcast_time_local

,media_pack
,primary_sales_house
,pay_free_indicator
,rank() over (order by total_accounts_viewed desc) as rank_all_overall
,segment_01_viewers
,segment_01_viewers/3970971 as segment_01_efficency_index
,rank() over 
(partition by media_pack
,primary_sales_house
,pay_free_indicator order by segment_01_viewers desc) as rank_segment_01_by_pack_sales_house_pay_free
,rank() over (order by segment_01_viewers desc) as rank_segment_01_overall


,segment_02_viewers
,segment_02_viewers/24105 as segment_02_efficency_index
,rank() over 
(partition by media_pack
,primary_sales_house
,pay_free_indicator order by segment_02_viewers desc) as rank_segment_02_by_pack_sales_house_pay_free
,rank() over (order by segment_02_viewers desc) as rank_segment_02_overall


,segment_03_viewers
,segment_03_viewers/921478 as segment_03_efficency_index
,rank() over 
(partition by media_pack
,primary_sales_house
,pay_free_indicator order by segment_03_viewers desc) as rank_segment_03_by_pack_sales_house_pay_free
,rank() over (order by segment_03_viewers desc) as rank_segment_03_overall

,segment_04_viewers
,segment_04_viewers/3014713 as segment_04_efficency_index
,rank() over 
(partition by media_pack
,primary_sales_house
,pay_free_indicator order by segment_04_viewers desc) as rank_segment_04_by_pack_sales_house_pay_free
,rank() over (order by segment_04_viewers desc) as rank_segment_04_overall

,segment_05_viewers
,segment_05_viewers/84356 as segment_05_efficency_index
,rank() over 
(partition by media_pack
,primary_sales_house
,pay_free_indicator order by segment_05_viewers desc) as rank_segment_05_by_pack_sales_house_pay_free
,rank() over (order by segment_05_viewers desc) as rank_segment_05_overall


,segment_06_viewers
,segment_06_viewers/1431232 as segment_06_efficency_index
,rank() over 
(partition by media_pack
,primary_sales_house
,pay_free_indicator order by segment_06_viewers desc) as rank_segment_06_by_pack_sales_house_pay_free
,rank() over (order by segment_06_viewers desc) as rank_segment_06_overall

into #all_programme_rank_summary
from #all_programme_summary
where total_accounts_viewed>0 and  grouped_channel is not null 
--and media_pack is not null and primary_sales_house is not null
order by total_accounts_viewed desc
;
--select  * from #all_programme_rank_summary where media_pack='MOVIES' and Primary_sales_house='Sky' and pay_free_indicator='Pay' order by rank_segment_01_by_pack_sales_house_pay_free;
--select * from #all_programme_rank_summary where media_pack = 'MOVIES' order by rank_segment_01_by_pack_sales_house_pay_free
--#all_programme_rank_summary
--drop table project134_top_programmes;
select * into  project134_top_programmes from #all_programme_rank_summary where 
(rank_segment_01_by_pack_sales_house_pay_free<=100 or 
rank_segment_02_by_pack_sales_house_pay_free<=100 or 
rank_segment_03_by_pack_sales_house_pay_free<=100 or 
rank_segment_04_by_pack_sales_house_pay_free<=100 or 
rank_segment_05_by_pack_sales_house_pay_free<=100 or 
rank_segment_06_by_pack_sales_house_pay_free<=100)
--and pay_free_indicator in ('Pay','FTA') and media_pack is not null and primary_sales_house is not null
;
commit;
grant all on project134_top_programmes to greenj, dbarnett, jacksons, stafforr, sarahm, poveys, gillh
, rombaoad, louredaj, patelj, kinnairt, sbednaszynski, vespa_group_low_security, sk_prodreg,vespa_analysts,neighbom;
commit;

select * from project134_top_programmes order by rank_all_overall;

output to 'C:\Users\barnetd\Documents\Project 134 - Customer Value ATL\top programmes.csv' format ascii;



---Run Efficacy Index at individual Media Level---
--drop table project134_3_plus_minute_summary_by_media_pack;
select case   when value_segment_cuscan_only in 
        ('BEDDING IN') 
 and sky_go_last_3m=0 then 1 else 0 end as target_segment_bedding_in
,case   when value_segment_cuscan_only in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN') 
 and sky_go_last_3m=0 then 1 else 0 end as target_segment_exc_bedding_in
,heavy_sky_go_user_non_sport
,a.account_number
,overall_project_weighting
, sky_go_last_3m
,Sky_plus_usage_last_3m

,max(case when media_pack = 'DOCUMENTARIES' then 1 else 0 end) as documentaries
,max(case when media_pack = 'ENTERTAINMENT' then 1 else 0 end) as entertainment
,max(case when media_pack = 'NEWS' then 1 else 0 end) as news
,max(case when media_pack = 'MOVIES' then 1 else 0 end) as movies
,max(case when media_pack = 'KIDS' then 1 else 0 end) as kids
,max(case when media_pack = 'MUSIC' then 1 else 0 end) as music
,max(case when media_pack = 'LIFESTYLE & CULTURE' then 1 else 0 end) as Lifestyle_Culture
,max(case when media_pack = 'SPORTS' then 1 else 0 end) as Sports
,max(case when media_pack = 'C4' then 1 else 0 end) as C4
,max(case when media_pack = 'C4 Digital' then 1 else 0 end) as C4_Digital
,max(case when media_pack = 'FIVE' then 1 else 0 end) as FIVE
,max(case when media_pack = 'FIVE Digital' then 1 else 0 end) as FIVE_Digital
,max(case when media_pack = 'ITV' then 1 else 0 end) as ITV
,max(case when media_pack = 'ITV Digital' then 1 else 0 end) as ITV_Digital
,max(case when media_pack = 'UKTV' then 1 else 0 end) as UKTV
into project134_3_plus_minute_summary_by_media_pack
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_134_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
where overall_project_weighting>0
group by target_segment_bedding_in
,target_segment_exc_bedding_in
,a.account_number
,overall_project_weighting
, sky_go_last_3m
,Sky_plus_usage_last_3m
,heavy_sky_go_user_non_sport
;
commit;

--select distinct media_pack from  project134_3_plus_minute_prog_viewed_deduped order by media_pack;


select target_segment_bedding_in 
,target_segment_exc_bedding_in
,heavy_sky_go_user_non_sport
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
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
, sum(music*overall_project_weighting) as music_total
, sum(Lifestyle_Culture*overall_project_weighting) as Lifestyle_Culture_total
, sum(Sports*overall_project_weighting) as Sports_total

, sum(C4*overall_project_weighting) as C4_total
, sum(C4_Digital*overall_project_weighting) as C4_Digital_total
, sum(FIVE*overall_project_weighting) as FIVE_total
, sum(FIVE_Digital*overall_project_weighting) as FIVE_Digital_total
, sum(ITV*overall_project_weighting) as ITV_total
, sum(ITV_Digital*overall_project_weighting) as ITV_Digital_total
, sum(UKTV*overall_project_weighting) as UKTV_total

from project134_3_plus_minute_summary_by_media_pack
group by target_segment_bedding_in ,target_segment_exc_bedding_in
,heavy_sky_go_user_non_sport
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
order by target_segment_bedding_in ,target_segment_exc_bedding_in,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
;

commit;

---Repeat but just for heavy Sky Go Figures
select heavy_sky_go_user_non_sport
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
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
, sum(music*overall_project_weighting) as music_total
, sum(Lifestyle_Culture*overall_project_weighting) as Lifestyle_Culture_total
, sum(Sports*overall_project_weighting) as Sports_total

, sum(C4*overall_project_weighting) as C4_total
, sum(C4_Digital*overall_project_weighting) as C4_Digital_total
, sum(FIVE*overall_project_weighting) as FIVE_total
, sum(FIVE_Digital*overall_project_weighting) as FIVE_Digital_total
, sum(ITV*overall_project_weighting) as ITV_total
, sum(ITV_Digital*overall_project_weighting) as ITV_Digital_total
, sum(UKTV*overall_project_weighting) as UKTV_total

from project134_3_plus_minute_summary_by_media_pack
group by 
heavy_sky_go_user_non_sport
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
order by documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
;

commit;

--select count(*) from project134_3_plus_minute_summary_by_media_pack;

---Account Totals---
select  case   when value_segment_cuscan_only in 
        ('BEDDING IN') 
 and sky_go_last_3m=0 then 1 else 0 end as account_segment

, sum(overall_project_weighting) as total_accounts
from project_134_base_Accounts where overall_project_weighting>0 group by account_segment order by account_segment;



select case   when value_segment_cuscan_only in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN') 
 and sky_go_last_3m=0 then 1 else 0 end as account_segment

, sum(overall_project_weighting) as total_accounts
from project_134_base_Accounts where overall_project_weighting>0 group by account_segment order by account_segment;

--Heavy Sky Go Non Sports User
select heavy_sky_go_user_non_sport  as account_segment

, sum(overall_project_weighting) as total_accounts
from project_134_base_Accounts where overall_project_weighting>0 group by account_segment order by account_segment;



commit;

----Split By Channel----
--drop table  project134_3_plus_minute_summary_by_channel;
select case   when value_segment_cuscan_only in 
        ('BEDDING IN') 
 and sky_go_last_3m=0 then 1 else 0 end as target_segment_bedding_in
,case   when value_segment_cuscan_only in 
        ('GOLD CUSCAN','SILVER CUSCAN','UNSTABLE CUSCAN','COPPER CUSCAN','BRONZE CUSCAN') 
 and sky_go_last_3m=0 then 1 else 0 end as target_segment_exc_bedding_in
,heavy_sky_go_user_non_sport
,a.account_number
,overall_project_weighting
, sky_go_last_3m
,Sky_plus_usage_last_3m
,grouped_channel
,media_pack

into project134_3_plus_minute_summary_by_channel
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_134_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
where overall_project_weighting>0
group by target_segment_bedding_in
,target_segment_exc_bedding_in
,a.account_number
,overall_project_weighting
, sky_go_last_3m
,Sky_plus_usage_last_3m
,heavy_sky_go_user_non_sport
,grouped_channel
,media_pack
;
commit;

select media_pack
,grouped_channel
,sum(case when target_segment_bedding_in =1 then overall_project_weighting else 0 end) as total_accounts_bedding_in_target
,sum(case when target_segment_bedding_in =0 then overall_project_weighting else 0 end) as total_accounts_bedding_in_non_target


,sum(case when target_segment_exc_bedding_in =1 then overall_project_weighting else 0 end) as total_accounts_exc_bedding_in_target
,sum(case when target_segment_exc_bedding_in =0 then overall_project_weighting else 0 end) as total_accounts_exc_bedding_in_non_target

,sum(case when heavy_sky_go_user_non_sport =1 then overall_project_weighting else 0 end) as total_accounts_heavy_sky_go_target
,sum(case when heavy_sky_go_user_non_sport =0 then overall_project_weighting else 0 end) as total_accounts_heavy_sky_go_non_target

from project134_3_plus_minute_summary_by_channel
group by media_pack
,grouped_channel;




--select top 100 * from project134_3_plus_minute_prog_viewed_deduped;




/*
select 
media_pack
,case when cuscan_churn_risk='1: Cuscan Churn Risk' and sky_go_last_3m =0 then 1 else 0 end as target_segment
,count(distinct account_number) as accounts
from project134_3_plus_minute_summary_by_programme  where primary_sales_house = 'Sky'
group by media_pack
,target_segment
order by media_pack,target_segment;
*/
--select  media_pack,count(*) as records from project134_3_plus_minute_prog_viewed_deduped where primary_sales_house = 'Sky' group by media_pack;


--select * from project134_top_programmes where grouped_channel='Sky Premiere';

/*

--select * from project134_3_plus_minute_prog_viewed where service_key = 2002 and broadcast_time_utc between '2012-10-27 17:00:00' and '2012-10-27 21:00:00' order by broadcast_time_utc
select * from sk_prod.VESPA_PROGRAMME_SCHEDULE  where broadcast_start_date_time_utc between '2012-10-27 17:00:00' and '2012-10-27 21:00:00' and service_key = 2002
select * from sk_prod.VESPA_PROGRAMME_SCHEDULE  where broadcast_start_date_time_utc between '2012-10-27 17:00:00' and '2012-10-27 21:00:00' and left(channel_name,5) = 'BBC 1' order by broadcast_start_date_time_utc
select * from sk_prod.VESPA_EPG_DIM  
where
-- tx_date_time_utc between '2012-10-27 17:00:00' and '2012-10-27 21:00:00' and 
service_key = 2002
order by tx_date_time_utc desc



select programme_name
, grouped_channel 
,non_staggercast_broadcast_time_utc
,service_key_detail
,sum(accounts) as totval 
from project134_3_plus_minute_summary_by_programme 
where grouped_channel = 'BBC 1' and programme_name is null
group by programme_name,grouped_channel , programme_name,non_staggercast_broadcast_time_utc ,service_key_detail
order by totval desc;

select * from sk_prod.VESPA_PROGRAMME_SCHEDULE where broadcast_start_date_time_utc between '2012-10-27 17:00:00' and '2012-10-27 21:00:00' order by channel_name ,broadcast_start_date_time_utc

select top 500 * from vespa_analysts.VESPA_DAILY_AUGS_20121027
--select * from project134_3_plus_minute_summary_by_programme order by accounts desc;
--select grouped_channel , programme_name,non_staggercast_broadcast_time_utc,sum(accounts) as totval from project134_3_plus_minute_summary_by_programme group by grouped_channel , programme_name,non_staggercast_broadcast_time_utc order by totval desc;
select * from sk_prod.VESPA_PROGRAMME_SCHEDULE where channel_name = 'Sky Summer'
select top 500 * from project134_3_plus_minute_prog_viewed;
select count(*) from project134_3_plus_minute_prog_viewed;

select account_number , grouped_channel, non_staggercast_broadcast_time_utc
into #deduped_list
from project134_3_plus_minute_prog_viewed
group by  account_number , grouped_channel, non_staggercast_broadcast_time_utc

dk_programme_instance_dim
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


select top 500 * from sk_prod.VESPA_PROGRAMME_SCHEDULE where programme_name = 'RIO'

select top 500 * from sk_prod.VESPA_PROGRAMME_SCHEDULE where channel_name = 'Sky Summer' order by broadcast_start_date_time_utc desc
*/