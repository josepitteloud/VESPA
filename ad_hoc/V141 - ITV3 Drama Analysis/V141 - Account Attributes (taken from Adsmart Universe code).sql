
-----Code below reworked for Project 141 - ITV3 Analysis
---Based on Original code for project141_universe but analysis date changed to 14th Nov 2012




/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        SKY project141_universe CUSTOMER BASE FILE CREATION
        --------------------------------
        Author  : Jitesh Patel
        Date    : 23rd April 2012

SECTIONS
----------------
code_location - Create local variables
Change_needed - Code needs to be changed
                1) access issues,
                2) source currently not available on ConsumerView,
                3) data not uploaded to DWH

LOOKUP VARIABLES
-----------------
        --      code_location_01        Account_number
        --      code_location_01        Currency_code
        --      code_location_01        household key
        --      code_location_02        Turnaround Events
        --      code_location_02        Turnaround Events
        --      code_location_03        Active Sky Rewards User
        --      code_location_04        Value Segments
        --      code_location_05        Anytime Plus
        --      code_location_06        Sky Go
        --      code_location_07        ESPN
        --      code_location_08        High Level HD Product Holding
        --      code_location_08        number of MR
        --      code_location_08        Sky Plus Subs
        --      code_location_09        High Level Talk Product Holding
        --      code_location_10        High Level BB Product Holding
        --      code_location_11        Previous Movies Downgrade
        --      code_location_11        Previous Sports Downgrade
        --      code_location_12        Recently or Currently on Discount
        --      code_location_13        Box_type
        --      code_location_14        Multiroom
        --      code_location_15        number of MR adsmartable
        --      code_location_16        Previous Missed Payments
        --      code_location_17        Vespa Panel
        --      code_location_18        Cable Availability
        --      code_location_18        Postcode
        --      code_location_19        Platform 1 Month Notice
        --      code_location_19        Platform 1 Month Notice
        --      code_location_20        Demographic & Lifestyle
        --      code_location_20        Financial Outlook
        --      code_location_20        Home Owner?
        --      code_location_20        Life-stage Bands
        --      code_location_20        Mirror has children
        --      code_location_20        Mirror is ABC1
        --      code_location_21        Mirror Men
        --      code_location_21        Mirror Women
        --      code_location_22        Affluence Bands
        --      code_location_23        Sky customer lifestage
        --      code_location_24        Kids Ages 10-15
        --      code_location_24        Kids Ages 4-9
        --      code_location_24        Kids Ages below 4
        --      code_location_25        Disney
        --      code_location_26        Sky Sports 1 & 2
        --      code_location_26        Sky Movies 1 & 2
        --      code_location_27        Sky ID
        --      code_location_28        Disney Subs
        --      code_location_29        TV Region data
        --      code_location_30        Home Movers
        --      code_location_31        ACCT_CUST_ACCOUNT_ID


        */

--     A01 - Create local variables

SELECT cb_change_date, count(*)
FROM sk_prod.EXPERIAN_CONSUMERVIEW
GROUP BY cb_change_date
ORDER BY cb_change_date;
commit;

CREATE VARIABLE @date_minus__12   date;
SET             @date_minus__12 = '20111114';
commit;

CREATE VARIABLE @today            date;
SET             @today =          '20121114';
commit;

CREATE VARIABLE @experian         date;
SET             @experian =       '20130123';
commit;



-------------------------------------------------  01 - Create table

--if object_id('project141_universe') is not null then drop table project141_universe end if
drop table project141_universe; commit;

CREATE TABLE project141_universe(                                                   -- Create table with a list of variables
         account_number            VARCHAR(20)     NOT NULL             -- Account Number
        ,cb_key_household          bigint                               -- Used for Experian Data
        ,cb_key_DB_Person          INTEGER
        ,ACCT_CUST_ACCOUNT_ID      VARCHAR(50)
        ,cb_address_postcode       VARCHAR(10)     default 'missing'    -- postcode of account
        ,Cable_area                VARCHAR(3)      default 'NA'         -- Cable Area Flag
        ,currency_code             VARCHAR(5)      NULL                 -- Distiguish ROI accounts
        ,TA_attempts               INTEGER         default 0            -- TA attempts Last 12 months
        ,TA_Saves                  INTEGER         default 0            -- TA saves Last 12 months
        ,Pending_cancel            INTEGER         default 0            -- Pending Cancelation flag
        ,Pend_Can_date             DATE            default '9999-99-99' -- Cancelation Date
        ,Premiums_downgrades       INTEGER         default 0            -- Premium downgrades L12
        ,Movies_downgrades         INTEGER         default 0            -- Movies downgrades L12
        ,Sports_downgrades         INTEGER         default 0            -- Sports downgrades L12
        ,current_offer             INTEGER         default 0            -- account currently in offer
        ,Offer_count               INTEGER         default 0            -- Number of offers
        ,offer_expires30           INTEGER         default 0            -- account offer expires next 30 days
        ,value_segment             VARCHAR(50)     default 'missing'    -- Current Value Segment
        ,sky_id                    bigint          default 0            -- Sky id created
        ,Sky_Go_Reg                INTEGER         default 0            -- Sky Go number of downloads 12 months
        ,HDTV                      INTEGER         default 0            -- Current HD TV subscription
        ,Ent_Extra                 INTEGER         default 0            -- Current Entertainment Extra
        ,sky_sports_1              tinyint         default 0            -- Current Sky Sports 1 Subs
        ,sky_sports_2              tinyint         default 0            -- Current Sky Sports 2 Subs
        ,movies_1                  tinyint         default 0            -- Current Sky Movies 1 Subs
        ,movies_2                  tinyint         default 0            -- Current Sky Movies 1 Subs
        ,Disney                    integer         default 0            -- Current Disney Subs
        ,ESPN_Subscribers          INTEGER         default 0            -- Current ESPN subscription
        ,Anytime_plus              INTEGER         default 0            -- Anytime+ activated
        ,multiroom                 INTEGER         default 0            -- Current Multiroom Subscription
        ,skyplus                   INTEGER         default 0            -- customer has skyplus
        ,Total_MR                  INTEGER         default 0            -- Total number of MR boxes
        ,T_AdSm_box                INTEGER         default 0            -- Total Adsmartable boxes
        ,talk_product              VARCHAR(50)     default 'NA'         -- Current Sky Talk product
        ,BB_type                   VARCHAR(50)     default 'NA'         -- Current BB product
        ,box_type                  VARCHAR(50)     default 'missing'    -- Primary Box Type
        ,vespa                     INTEGER         default 0            -- Box returning data
        ,panel_id_vespa            INTEGER                              -- panel_id
        ,Sky_cust_life             VARCHAR(20)     default 'E) missing' -- based on Sky Tenure
        ,Total_miss_pmt            INTEGER         default 0            -- Number of unbilled payments Last 12 months
        ,h_lifestage               VARCHAR(50)     default 'missing'    -- household lifestage
        ,HomeOwner                 VARCHAR(50)     default 'missing'    -- data defined from Experians ConsumerView
        ,Financial_outlook         VARCHAR(50)     default 'missing'    -- data defined from Experians ConsumerView
        ,Demographic               VARCHAR(50)     default 'missing'    -- data defined from Experians ConsumerView
        ,Lifestage                 VARCHAR(2)      default 'M'          -- data defined from Experians ConsumerView
        ,Family_Lifestage          VARCHAR(2)      default 'M'          -- data defined from Experians ConsumerView
        ,tenure                    VARCHAR(2)      default 'M'          -- data defined from Experians ConsumerView
        ,h_fss_v3_group            VARCHAR(2)      default 'Z'          -- data defined from Experians ConsumerView --change to Z because there is a M category
        ,h_mosaic_uk_2009_group    VARCHAR(2)      default 'M'          -- data defined from Experians ConsumerView
        ,h_mosaic_uk_2009_type     VARCHAR(2)      default 'M'          -- data defined from Experians ConsumerView
        ,PAF                       INTEGER         default 0            -- data defined from Experians ConsumerView
        ,H_AFFLUENCE               VARCHAR(2)      default 'M'          -- data defined from Experians ConsumerView
        ,Sky_Reward_L12            INTEGER         default 0            -- Sky Rewards Last 12 months
        ,Sky_Events_L12            INTEGER         default 0            -- Sky Events Last 12 months
        ,Sky_T_Rewards             INTEGER         default 0            -- total Sky Reward (rewards + events)
        ,Kids_Aged_LE4             varchar(1)      default 'N'          -- data defined from AXCIOM
        ,Kids_Aged_5to11           varchar(1)      default 'N'          -- data defined from AXCIOM
        ,Kids_Aged_12to17          varchar(1)      default 'N'          -- data defined from AXCIOM
        ,MIRROR_MEN_MIN            VARCHAR(5)                           -- data defined from Experians ConsumerView
        ,MIRROR_WOMEN_MIN          VARCHAR(5)                           -- data defined from Experians ConsumerView
        ,Mirror_has_children       VARCHAR(50)     default 'missing'    -- data defined from Experians ConsumerView
        ,Mirror_ABC1               VARCHAR(1)      default 'M'          -- data defined from Experians ConsumerView
        ,barb_id_bbc               INTEGER                              -- BARB BBC Id
        ,barb_desc_bbc             VARCHAR(50)                          -- BARB BBC Description
        ,barb_id_itv               INTEGER                              -- BARB ITV Id
        ,barb_desc_itv             VARCHAR(50)                          -- BARB ITV Description
        ,Home_mover                INTEGER                              -- Identify Homemovers - if this is flagged,
                                                                        --  and no consumer view data, use previous file
        ,cb_key_individual         bigint
        ,government_region         varchar(50)
        ,bt_fibre_area             varchar(10)
        ,exchange_id               varchar(10)
        ,exchange_status           varchar(10)
        ,exchange_unbundled        varchar(10)
        ,isba_tv_region            varchar(50)
);
commit;

--      create index on project141_universe
CREATE   HG INDEX idx01 ON project141_universe(account_number);
commit;

-------------------------------------------------  02 - Temp File of DTV Customers
--- code_location_01
--drop table temp_AdSmart;
--commit;

SELECT DISTINCT account_number, currency_code,cb_key_household
INTO temp_AdSmart
FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')
   AND effective_from_dt <= @today
   AND effective_to_dt > @today
   AND EFFECTIVE_FROM_DT IS NOT NULL
   AND cb_key_household > 0             --UK Only
   AND cb_key_household IS NOT NULL
   AND account_number IS NOT NULL
   AND service_instance_id IS NOT NULL;
commit;

--      create index on temp_AdSmart file
CREATE   HG INDEX idx02 ON temp_AdSmart(account_number);
commit;

--      insert into project141_universe file
INSERT INTO project141_universe (account_number, currency_code,cb_key_household)
SELECT account_number, currency_code, cb_key_household
FROM temp_AdSmart;
commit;

drop table temp_AdSmart;
commit;

--select top 100 * from project141_universe;

-------------------------------------------------  02 - TA attempts and Saves last 12 months
---code_location_02
SELECT    cca.account_number
         ,count(*) AS TA_attempts
         ,sum(CASE WHEN cca.Wh_Attempt_Outcome_Description_1 IN ( 'Turnaround Saved'
                                                                 ,'Legacy Save'
                                                                 ,'Home Move Saved'
                                                                 ,'Home Move Accept Saved')
                   THEN 1
                   ELSE 0
          END) AS TA_saves
INTO TA_attempts
    FROM sk_prod.cust_change_attempt AS cca
         inner join sk_prod.cust_subscriptions AS subs
             ON cca.subscription_id = subs.subscription_id
   WHERE cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
     AND subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
     AND cca.attempt_date                        >= @date_minus__12
     AND cca.created_by_id                  NOT IN ('dpsbtprd', 'batchuser')
     AND cca.Wh_Attempt_Outcome_Description_1 in (  'Turnaround Saved'
                                                   ,'Legacy Save'
                                                   ,'Turnaround Not Saved'
                                                   ,'Legacy Fail'
                                                   ,'Home Move Saved'
                                                   ,'Home Move Not Saved'
                                                   ,'Home Move Accept Saved')
   GROUP BY cca.account_number;
commit;

--      create index on TA_attempts file
CREATE   HG INDEX idx03 ON TA_attempts(account_number);
commit;


--      update project141_universe file
UPDATE project141_universe
SET TA_attempts = TA.TA_attempts
    ,TA_Saves = TA.TA_Saves
FROM project141_universe  AS Base
       INNER JOIN TA_attempts AS TA
        ON base.account_number = TA.account_number
ORDER BY base.account_number;
commit;

--select top 100 * from project141_universe;
drop table TA_attempts;
commit;


-------------------------------------------------  02 - Customer has used Sky Rewards
---code_location_03
SELECT    base.account_number
         ,count(*) as Sky_Reward_L12
INTO Sky_rewards
    FROM sk_prod.SKY_REWARDS_COMPETITIONS as sky
         inner join project141_universe as Base
                    on sky.account_number = base.account_number
    WHERE Date_entered >= @date_minus__12
    GROUP BY base.account_number;
commit;

--      create index on Sky_rewards file
CREATE   HG INDEX idx04 ON Sky_rewards(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET Sky_Reward_L12 = sky.Sky_Reward_L12
FROM project141_universe  AS Base
       INNER JOIN Sky_Rewards AS sky
        ON base.account_number = sky.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Sky_rewards;
commit;

SELECT    base.account_number
         ,count(*) as Sky_Events_L12
INTO Sky_events
    FROM sk_prod.SKY_REWARDS_EVENTS as sky
         inner join project141_universe as Base
                    on sky.account_number = base.account_number
    WHERE Date_registered >= @date_minus__12
    GROUP BY base.account_number;
commit;

--select top 500 * from project141_universe;
--      create index on Sky_events file
CREATE   HG INDEX idx04 ON Sky_events(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET Sky_Events_L12 = sky.Sky_Events_L12
FROM project141_universe  AS Base
       INNER JOIN Sky_events AS sky
        ON base.account_number = sky.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Sky_events;
commit;

UPDATE project141_universe
SET SKY_T_Rewards = Sky_Events_L12+Sky_Reward_L12
FROM project141_universe;
commit;

-------------------------------------------------  02 - Value Segments
--code_location_04
UPDATE project141_universe
   SET value_segment = tgt.value_seg
  FROM project141_universe AS base
       INNER JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt
       ON base.account_number = tgt.account_number;
commit;

-------------------------------------------------  02 - Anytime + activated
--code_location_05     code changed in line with changes to Wiki
/*SELECT base.account_number
       ,1 AS Anytime_plus
INTO Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS Aplus
        inner join project141_universe as Base
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
        inner join project141_universe as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<@today               -- (END)
AND        first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET Anytime_plus = Aplus.Anytime_plus
FROM project141_universe  AS Base
       INNER JOIN Anytime_plus AS Aplus
        ON base.account_number = APlus.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Anytime_plus;
commit;


-------------------------------------------------  02 - Sky Go and Downloads
--code_location_06
/*SELECT base.account_number
       ,count(distinct base.account_number) AS Sky_Go_Reg
INTO Sky_Go
FROM   sk_prod.SKY_PLAYER_REGISTRANT  AS Sky_Go
        inner join project141_universe as Base
         on Sky_Go.account_number = Base.account_number
GROUP BY base.account_number;
*/
select base.account_number,
        1 AS SKY_GO_USAGE
--        ,sum(SKY_GO_USAGE)
into skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
        inner join project141_universe AS Base
         ON usage.account_number = Base.account_number
where cb_data_date >= @date_minus__12
        AND cb_data_date <@today
group by base.account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET Sky_Go_Reg = sky_go.SKY_GO_USAGE
FROM project141_universe  AS Base
       INNER JOIN skygo_usage AS sky_go
        ON base.account_number = sky_go.account_number
ORDER BY base.account_number;
commit;

DROP TABLE skygo_usage;
commit;

-------------------------------------------------  02 - Active ESPN Subscription
--code_location_07
SELECT  base.account_number
       ,1 AS ESPN_Subscribers
INTO ESPN
  FROM sk_prod.cust_subs_hist AS ESPN
        inner join project141_universe AS Base
         ON ESPN.account_number = Base.account_number
 WHERE subscription_type ='A-LA-CARTE'               --A La Carte Stack
   AND subscription_sub_type = 'ESPN'                --ESPN Subscriptions
   AND status_code in ('AC','AB','PC')               --Active Status Codes
   AND ESPN.effective_from_dt <= @today
   AND ESPN.effective_to_dt > @today
   AND ESPN.effective_from_dt <> ESPN.effective_to_dt
 GROUP BY base.account_number;
commit;

--      create index on ESPN file
CREATE   HG INDEX idx07 ON ESPN(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET ESPN_Subscribers = espn.ESPN_Subscribers
FROM project141_universe  AS Base
       INNER JOIN ESPN AS ESPN
        ON base.account_number = ESPN.account_number
ORDER BY base.account_number;
commit;

DROP TABLE ESPN;
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
INTO MR_HD
      FROM project141_universe as ad
                inner join sk_prod.cust_subs_hist AS csh on ad.account_number = csh.account_number
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD'
                                         ,'DTV Sky+')
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.effective_from_dt <= @today
       AND csh.effective_to_dt    > @today
GROUP BY csh.account_number;
commit;

--      create index on MR_HD
CREATE   HG INDEX idx08 ON MR_HD(account_number);
commit;


--      update project141_universe file
UPDATE project141_universe
SET  multiroom = hdmr.multiroom
    ,HDTV      = hdmr.HDTV
    ,skyplus   = hdmr.skyplus
FROM project141_universe  AS Base
  INNER JOIN MR_HD AS hdmr
        ON base.account_number = hdmr.account_number
            ORDER BY base.account_number;
commit;

DROP TABLE MR_HD;
commit;

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
    inner join project141_universe AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'SKY TALK SELECT'
     AND(     status_code = 'A'
          OR (status_code = 'FBP' AND prev_status_code IN ('PC','A'))
          OR (status_code = 'RI'  AND prev_status_code IN ('FBP','A'))
          OR (status_code = 'PC'  AND prev_status_code = 'A'))
     AND effective_to_dt != effective_from_dt
     AND csh.effective_from_dt <= @today
     AND csh.effective_to_dt > @today
GROUP BY base.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET  talk_product = talk.talk_product
FROM project141_universe  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
commit;

--select count(*) from project141_universe;
--select top 100 * from project141_universe;


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
    inner join project141_universe AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= @today
   AND csh.effective_to_dt > @today
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

--      update project141_universe file
UPDATE project141_universe
SET  BB_type = BB.BB_type
FROM project141_universe  AS Base
  INNER JOIN BB AS BB
        ON base.account_number = BB.account_number
            ORDER BY base.account_number;
commit;


drop table bb; commit;
DROP TABLE BBB; commit;


--drop table downgrades;
--commit;

/*
select BB_type, count(*)
from project141_universe
group by BB_type

--select top 100 * from project141_universe;
*/

-------------------------------------------------  02 - Downgrades to packages
--code_location_11
SELECT    csh.Account_number
         ,ncel.prem_movies + ncel.prem_sports AS current_premiums
         ,ocel.prem_movies + ocel.prem_sports AS old_premiums
         ,ncel.prem_movies                    AS current_movies
         ,ocel.prem_movies                    AS old_movies
         ,                   ncel.prem_sports AS current_sports
         ,                   ocel.prem_sports AS old_sports
         ,rank() over(PARTITION BY base.account_number ORDER BY effective_to_dt desc) AS rank_id
         ,effective_to_dt
         ,effective_from_dt
                    INTO downgrades
    FROM sk_prod.cust_subs_hist AS csh
         inner join sk_prod.cust_entitlement_lookup AS ncel
                    ON csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup AS ocel
                    ON csh.previous_short_description = ocel.short_description
         inner join project141_universe AS Base
                    ON csh.account_number = base.account_number
WHERE csh.effective_from_dt >= @date_minus__12  -- Date range
    AND csh.effective_to_dt > csh.effective_from_dt
--    AND csh.effective_to_dt >= @date_minus__12  -- Date range
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code IN ('AC','PC','AB')   -- Active records
    AND (current_premiums  < old_premiums  -- Decrease in premiums
        OR current_movies < old_movies    -- Decrease in movies
        OR current_sports < old_sports)    -- Decrease in sports
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;
commit;

DELETE FROM downgrades where rank_id >1;
commit;

ALTER table     downgrades ADD   Premiums_downgrades  integer; commit;
ALTER table     downgrades ADD   Movies_downgrades  integer; commit;
ALTER table     downgrades ADD   Sports_downgrades  integer; commit;


-- case statement to work out movie, sports and total downgrades
UPDATE downgrades
SET
 Premiums_downgrades =   CASE WHEN old_premiums > current_premiums THEN 1  ELSE 0  END
,Movies_downgrades  =    CASE WHEN old_movies > current_movies     THEN 1  ELSE 0  END
,Sports_downgrades  =    CASE WHEN old_sports > current_Sports     THEN 1  ELSE 0  END
FROM downgrades;
commit;

--      create index on downgrades
CREATE   HG INDEX idx13 ON downgrades(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET  Premiums_downgrades = BB.Premiums_downgrades
, Movies_downgrades = BB.Movies_downgrades
, Sports_downgrades = BB.Sports_downgrades
FROM project141_universe  AS Base
  INNER JOIN Downgrades AS BB
        ON base.account_number = BB.account_number
            ORDER BY base.account_number;
commit;

-- delete temp file
drop table downgrades;
commit;

-- select top 100 * from project141_universe;

--drop table offers;
--commit;

-------------------------------------------------  02 - In Offers
--code_location_12
SELECT  base.account_number
         ,Max(CASE WHEN offer_end_dt          > @today THEN '1'
                                ELSE '0'        END) AS Current_offer
         ,Max(CASE WHEN offer_end_dt          < DATEADD(day,+30,@today) THEN '1'
                                ELSE '0'        END) AS offer_expires30
         ,count(*) as InOffer
INTO     offers
FROM     sk_prod.cust_product_offers AS CPO  inner join project141_universe AS Base
                    ON CPO.account_number = base.account_number
WHERE    offer_id                NOT IN (SELECT offer_id
                                         FROM citeam.sk2010_offers_to_exclude)
        AND offer_end_dt          > @today
        AND offer_amount          < 0
        AND offer_dim_description   NOT IN ('PPV 1 Administration Charge','PPV EURO1 Administration Charge')
        AND UPPER (offer_dim_description) NOT LIKE '%VIP%'
        AND UPPER (offer_dim_description) NOT LIKE '%STAFF%'
        AND UPPER (offer_dim_description) NOT LIKE 'PRICE PROTECTION%'
GROUP BY base.account_number;
commit;


--      create index on offers
CREATE   HG INDEX idx14 ON offers(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET  current_offer   = offer.current_offer
    ,offer_expires30 = offer.offer_expires30
    ,Offer_count     = offer.InOffer
FROM project141_universe  AS Base
  INNER JOIN offers AS offer
        ON base.account_number = offer.account_number
            ORDER BY base.account_number;
commit;


-- delete temp file
drop table offers;
commit;



-------------------------------------------------  02 - set top boxes
-- Boxtype & Universe

-- Boxtype is defined as the top two boxtypes held by a household ranked in the following order
-- 1) HD, 2) HDx, 3) Skyplus, 4) FDB

-- Capture all active boxes for this week
SELECT    csh.service_instance_id
          ,csh.account_number
          ,subscription_sub_type
          ,rank() over (PARTITION BY csh.service_instance_id ORDER BY csh.account_number, csh.cb_row_id desc) AS rank
  INTO accounts -- drop table accounts
  FROM sk_prod.cust_subs_hist as csh
        INNER JOIN project141_universe AS ss
        ON csh.account_number = ss.account_number
 WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
   AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
   AND csh.effective_from_dt <= @today
   AND csh.effective_to_dt > @today
   AND csh.effective_from_dt <> effective_to_dt;
commit;

-- De-dupe active boxes
DELETE FROM accounts WHERE rank>1;
commit;

CREATE HG INDEX idx14 ON accounts(service_instance_id);
commit;

-- Identify HD boxes
SELECT  stb.service_instance_id
       ,SUM(CASE WHEN current_product_description LIKE '%HD%'     THEN 1  ELSE 0 END) AS HD
       ,SUM(CASE WHEN current_product_description LIKE '%HD%1TB%'
                   or current_product_description LIKE '%HD%2TB%' THEN 1  ELSE 0 END) AS HD1TB -- combine 1 and 2 TB
INTO hda -- drop table hda
FROM sk_prod.CUST_SET_TOP_BOX AS stb
        INNER JOIN accounts AS acc
        ON stb.service_instance_id = acc.service_instance_id
WHERE box_installed_dt <= @today
        AND box_replaced_dt   > @today
        AND current_product_description like '%HD%'
GROUP BY stb.service_instance_id;
commit;

CREATE HG INDEX idx14 ON hda(service_instance_id);
commit;

--select top 100 * from hda;


drop table scaling_box_level_viewing;
commit;

SELECT  --acc.service_instance_id,
       acc.account_number
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
       ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
       ,MAX(CASE  WHEN hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
       ,MAX(CASE  WHEN hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
INTO scaling_box_level_viewing
FROM sk_prod.cust_subs_hist AS csh
        INNER JOIN accounts AS acc
        ON csh.service_instance_id = acc.service_instance_id --< Limits to your universe
                LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel
                ON csh.current_short_description = cel.short_description
                        LEFT OUTER JOIN hda
                        ON csh.service_instance_id = hda.service_instance_id --< Links to the HD Set Top Boxes
 WHERE csh.effective_FROM_dt <= @today
   AND csh.effective_to_dt    > @today
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY acc.service_instance_id ,acc.account_number;
commit;

--select top 100 * from accounts;
--select top 100 * from scaling_box_level_viewing;

drop table accounts; commit;
drop table hda; commit;



-- Identify boxtype of each box and whether it is a primary or a secondary box
SELECT  tgt.account_number
       ,SUM(CASE WHEN MR=1 THEN 1 ELSE 0 END) AS mr_boxes
       ,MAX(CASE WHEN MR=0 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=0 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=0 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS pb -- FDB
       ,MAX(CASE WHEN MR=1 AND ((tgt.HD =1 AND HD1TBstb = 1) OR (tgt.HD =1 AND HDstb = 1))         THEN 4 -- HD ( inclusive of HD1TB)
                 WHEN MR=1 AND ((tgt.SP =1 AND tgt.HD1TBstb = 1) OR (tgt.SP =1 AND tgt.HDstb = 1)) THEN 3 -- HDx ( inclusive of HD1TB)
                 WHEN MR=1 AND tgt.SP =1                                                           THEN 2 -- Skyplus
                 ELSE                                                                              1 END) AS sb -- FDB
        ,convert(varchar(20), null) as universe
        ,convert(varchar(30), null) as boxtype
  INTO boxtype_ac -- drop table boxtype_ac
  FROM scaling_box_level_viewing AS tgt
GROUP BY tgt.account_number;
commit;

--select top 100 * from boxtype_ac;


-- Build the combined flags
update boxtype_ac
set universe = CASE WHEN mr_boxes = 0 THEN 'Single box HH'
                         WHEN mr_boxes = 1 THEN 'Dual box HH'
                         ELSE 'Multiple box HH' END
    ,boxtype  =
        CASE WHEN       mr_boxes = 0 AND  pb =  3 AND sb =  1   THEN  'HDx & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  4 AND sb =  1   THEN  'HD & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  2 AND sb =  1   THEN  'Skyplus & No_secondary_box'
             WHEN       mr_boxes = 0 AND  pb =  1 AND sb =  1   THEN  'FDB & No_secondary_box'
             WHEN       mr_boxes > 0 AND  pb =  4 AND sb =  4   THEN  'HD & HD' -- If a hh has HD  then all boxes have HD (therefore no HD and HDx)
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  3) OR (pb =  3 AND sb =  4)  THEN  'HD & HD'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  2) OR (pb =  2 AND sb =  4)  THEN  'HD & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  4 AND sb =  1) OR (pb =  1 AND sb =  4)  THEN  'HD & FDB'
             WHEN       mr_boxes > 0 AND  pb =  3 AND sb =  3                            THEN  'HDx & HDx'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  2) OR (pb =  2 AND sb =  3)  THEN  'HDx & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  3 AND sb =  1) OR (pb =  1 AND sb =  3)  THEN  'HDx & FDB'
             WHEN       mr_boxes > 0 AND  pb =  2 AND sb =  2                            THEN  'Skyplus & Skyplus'
             WHEN       mr_boxes > 0 AND (pb =  2 AND sb =  1) OR (pb =  1 AND sb =  2)  THEN  'Skyplus & FDB'
                        ELSE   'FDB & FDB' END
;
commit;

--select top 100 * from boxtype_ac;
--select top 100 * from project141_universe;


--update project141_universe file
UPDATE project141_universe
SET  base.Box_type = bt.Boxtype
FROM project141_universe AS Base INNER JOIN boxtype_ac AS bt
ON base.account_number = bt.account_number;
commit;

-- delete temp file
drop table box_type;
commit;

--select top 100 * from project141_universe


--------------------------------------------------- number of MR boxes
--code_location_14
SELECT  csh.account_number
           ,1 AS multiroom
           ,SUM(multiroom) AS total_MR
INTO MR_HD_count
      FROM sk_prod.cust_subs_hist AS csh
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup AS cel
           ON csh.current_short_description = cel.short_description
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription')
       AND csh.effective_from_dt <= @today
       AND csh.effective_to_dt    > @today
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.status_code in  ('AC','AB','PC')
       AND account_number in (SELECT account_number
                                FROM project141_universe)
GROUP BY csh.account_number, multiroom;
commit;

--      create index on MR_HD_count
CREATE   HG INDEX idx13 ON MR_HD_count(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET  total_MR = MR.total_MR
FROM project141_universe  AS Base
  INNER JOIN MR_HD_count AS MR
        ON base.account_number = MR.account_number
            ORDER BY base.account_number;
commit;

-- delete temp file
drop table MR_HD_count;
commit;

-- delete temp file
drop table SetTop;
commit;

-------------------------------------------------  02 - Number of Set top boxes that are AdSmartable
--code_location_15
SELECT base.account_number
--      ,x_pvr_type
--      ,x_manufacturer
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
--             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Thomson' THEN 1
                                                                    ELSE 0
       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO SetTop
FROM   sk_prod.CUST_SET_TOP_BOX  AS SetTop
        inner join project141_universe as Base
         on SetTop.account_number = Base.account_number
         where box_replaced_dt = '9999-09-09'
         GROUP BY base.account_number
                ,x_pvr_type
                ,x_manufacturer
                ,box_replaced_dt;
commit;

DROP TABLE kjdl;
commit;

select distinct(account_number), sum(T_AdSm_box) AS T_ADMS
into kjdl
from SetTop
GROUP BY account_number;
commit;

--      create index on SetTop
CREATE   HG INDEX idx10 ON kjdl(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET  T_AdSm_box = ST.T_ADMS
FROM project141_universe  AS Base
  INNER JOIN kjdl AS ST
        ON base.account_number = ST.account_number;
commit;


-- delete temp file
drop table SetTop; commit;
drop table kjdl; commit;

--select top 100 * from project141_universe;

-------------------------------------------------  02 - Unbilled accounts
-- previous missed payments need to fine tune (status = Unbilled)
--code_location_16
SELECT account_number, 1 AS miss, SUM(miss) AS Total_missed
INTO missed
FROM sk_prod.cust_bills
WHERE payment_due_dt between @date_minus__12 AND @today
        AND Status = 'Unbilled'
GROUP BY account_number;
commit;

--select top 100 * from missed;
--select Total_missed, count(*) from missed group by Total_missed;

--      create index on missed
CREATE   HG INDEX idx10 ON missed(account_number);
commit;

--      update project141_universe file
UPDATE project141_universe
SET  Total_miss_pmt = miss.Total_missed
FROM project141_universe  AS Base
  INNER JOIN missed AS miss
        ON base.account_number = miss.account_number
            ORDER BY base.account_number;
commit;

drop table missed;
commit;

--  02 - VESPA returning data boxes
--code_location_17

--select panel,count(1) as cow from vespa_analysts.vespa_single_box_view group by panel

drop table panel_ids;
commit;

select distinct (account_number),panel_id_vespa
into panel_ids
from Vespa_Analysts.Vespa_Single_Box_View;
commit;

drop table vespa;
commit;

SELECT DISTINCT account_number
                        ,1 AS VESPA
                        ,panel_id_vespa
INTO vespa
FROM panel_ids
WHERE panel_id_vespa = 12; --in (4,12,6,7); daily panel only
commit;

--      update project141_universe file
UPDATE project141_universe
SET  VESPA = ves.VESPA
,panel_id_vespa = ves.panel_id_vespa
FROM project141_universe  AS Base
  inner JOIN VESPA AS ves
        ON base.account_number = ves.account_number;
commit;

--select panel_id_vespa,count(*) from project141_universe group by panel_id_vespa;
--select count(*) from vespa;

/*
--      update project141_universe file
UPDATE project141_universe
SET  panel_id_vespa = ves.panel_id_vespa
FROM project141_universe  AS Base
  inner JOIN panel_ids AS ves
        ON base.account_number = ves.account_number;
*/

-- delete temp file
drop table panel_ids; commit;
drop table vespa; commit;

-------------------------------------------------  02 - Cable Area and Postcode
--code_location_18
-- get postcode

drop table postcode;
commit;

SELECT  distinct base.account_number
        ,sav.cb_address_postcode
      ,rank() over(PARTITION BY SAV.account_number ORDER BY SAV.cb_address_postcode desc) AS rank_id
INTO postcode
  FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
      inner join project141_universe as base
on base.account_number = SAV.account_number
where cust_active_dtv = 1;
commit;

DELETE FROM postcode where rank_id > 1;
commit;

--      create index on BB
CREATE   HG INDEX idx10 ON postcode(account_number);
commit;


--      update project141_universe file
UPDATE project141_universe
SET  cb_address_postcode = ves.cb_address_postcode
FROM project141_universe  AS Base
  INNER JOIN postcode AS ves
        ON base.account_number = ves.account_number;
commit;

--cable area
SELECT account_number
      ,CASE  WHEN cable_postcode ='N' THEN 'N'
             WHEN cable_postcode ='n' THEN 'N'
             WHEN cable_postcode ='Y' THEN 'Y'
             WHEN cable_postcode ='y' THEN 'Y'
                                      ELSE 'N/A'
       END AS Cable_area
into cable
  FROM project141_universe as ads
       LEFT OUTER JOIN sk_prod.broadband_postcode_exchange  AS bb
       ON replace(ads.cb_address_postcode, ' ','') = replace(bb.cb_address_postcode,' ','')
;
commit;

--      update project141_universe file
UPDATE project141_universe
SET  Cable_area = cab.Cable_area
FROM project141_universe  AS Base
  INNER JOIN cable AS cab
        ON base.account_number = cab.account_number;
commit;


-- delete temp file
drop table cable; commit;
drop table postcode; commit;


-------------------------------------------------  02 - Platform Notice
--code_location_19
--drop table pending;
--commit;

SELECT distinct account_number
        ,FUTURE_SUB_EFFECTIVE_DT AS Pend_Can_date
        ,1 AS Pending_cancel
INTO pending
FROM sk_prod.cust_subs_hist
WHERE subscription_sub_type = 'DTV Primary Viewing'
  AND account_number IS NOT NULL
  AND status_code IN  ('PC')
  AND status_end_dt = '9999-09-09'
  AND FUTURE_SUB_EFFECTIVE_DT >=@today
  AND FUTURE_SUB_EFFECTIVE_DT <=@today+30;
commit;


--      update project141_universe file
UPDATE project141_universe
SET   Pend_Can_date = cnx.Pend_Can_date
     ,Pending_cancel = cnx.Pending_cancel
     FROM project141_universe  AS Base
  INNER JOIN pending AS cnx
        ON base.account_number = cnx.account_number;
commit;


-- delete temp file
drop table pending;
commit;

--select count(*) from project141_universe;

------------------------------------------------------------------------------------  M01 -- Experian ConsumerView
-- select top 100 * from sk_prod.EXPERIAN_CONSUMERVIEW;

--find this in the code
--code_location_20

drop table nodupes;
commit;

SELECT   CV.cb_key_household
        ,CV.h_lifestage
        ,CV.h_family_lifestage
        ,CV.h_tenure
        ,CV.h_fss_v3_group
        ,CV.h_affluence_v2
        --,CV.h_mosaic_uk_2009_group
        ,CV.h_mosaic_uk_group
        ,CV.cb_change_date
        --,CV.h_mosaic_uk_2009_type
        ,CV.h_mosaic_uk_type
        ,CV.h_presence_of_child_aged_0_4_2011
        ,CV.h_presence_of_child_aged_5_11_2011
        ,CV.h_presence_of_child_aged_12_17_2011
        ,CV.p_affluence
        ,CV.h_number_of_children_in_household_2011
        ,CV.p_actual_age
        ,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id desc) AS rank_id
--       ,(RTRIM(Lifestage)||RTRIM(Family_Lifestage)||RTRIM(tenure)||RTRIM(h_mosaic_uk_2009_group)||RTRIM(h_mosaic_uk_2009_type)||RTRIM(h_fss_v3_group)) AS exp
        ,CASE WHEN (cb_address_status = '1' and cb_address_dps IS NOT NULL and cb_address_organisation IS NULL) THEN 1
              ELSE 0
              END as PAF
INTO nodupes
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS CV, project141_universe AS base
WHERE base.cb_key_household = CV.cb_key_household
AND cb_change_date=@experian;
commit;

/*
GROUP BY cv.cb_key_household
        ,CV.h_lifestage
        ,CV.h_family_lifestage
        ,CV.h_tenure
        ,CV.h_fss_v3_group
        ,CV.h_affluence_v2
        --,CV.h_mosaic_uk_2009_group
        ,CV.h_mosaic_uk_group
        ,CV.cb_change_date
        --,CV.h_mosaic_uk_2009_type
        ,CV.h_mosaic_uk_type
        ,CV.h_presence_of_child_aged_0_4_2011
        ,CV.h_presence_of_child_aged_5_11_2011
        ,CV.h_presence_of_child_aged_12_17_2011
        ,CV.p_affluence
        ,CV.h_number_of_children_in_household_2011
        ,CV.p_actual_age
--        ,exp
        ,CV.cb_address_status
        ,CV.cb_address_dps
        ,CV.cb_address_organisation
        ,CV.cb_row_id
        ,PAF
ORDER BY cv.cb_key_household;
*/

DELETE FROM nodupes where rank_id >1;
commit;

--select count(*) from nodupes;

-- select top 100 * from sk_prod.EXPERIAN_CONSUMERVIEW;


--alter table to include grouped data names
ALTER table     NoDupes ADD   h_lifestage_desc             varchar(50)   default 'missing'; commit;
ALTER table     NoDupes ADD   Mirror_has_children          varchar(50)   default 'missing'; commit;
ALTER table     NoDupes ADD   HomeOwner                    varchar(50)   default 'missing'; commit;
ALTER table     NoDupes ADD   Financial_outlook            varchar(50)   default 'missing'; commit;
ALTER table     NoDupes ADD   Demographic                  varchar(50)   default 'missing'; commit;
ALTER table     NoDupes ADD   Mirror_ABC1                  varchar(1)    default 'M'; commit;
ALTER table     NoDupes ADD   Kids_Aged_LE4                varchar(1)    default 'M'; commit;
ALTER table     NoDupes ADD   Kids_Aged_5to11              varchar(1)    default 'M'; commit;
ALTER table     NoDupes ADD   Kids_Aged_12to17             varchar(1)    default 'M'; commit;
ALTER table     NoDupes ADD   H_AFFLUENCE                  varchar(2)    default 'M'; commit;


-- Do some QA
--select h_lifestage, count(*) from nodupes group by h_lifestage;
--select h_family_lifestage, count(*) from nodupes group by h_family_lifestage;
--select h_tenure, count(*) from nodupes group by h_tenure;
--select h_fss_v3_group, count(*) from nodupes group by h_fss_v3_group;
--select h_mosaic_uk_group, count(*) from nodupes group by h_mosaic_uk_group;
--select h_mosaic_uk_type, count(*) from nodupes group by h_mosaic_uk_type;
--select h_presence_of_child_aged_0_4_2011, count(*) from nodupes group by h_presence_of_child_aged_0_4_2011;
--select h_presence_of_child_aged_5_11_2011, count(*) from nodupes group by h_presence_of_child_aged_5_11_2011;
--select h_presence_of_child_aged_12_17_2011, count(*) from nodupes group by h_presence_of_child_aged_12_17_2011;
--select p_affluence, count(*) from nodupes group by p_affluence;

--CROSS CHECK THE EXPERIAN DESCRIPTIONS IN THEIR DOC.

/*
7 values  - Up to 16 possible. Final list TBC with Sky IQ (Very young adults (Age 16-24)
Young adults (25-35)
Mature adults (36-45)
Middle-aged adults (46-55)
Older adults (56-65)
Elderly adults(65+)
Unknown) - extra space kept for other features (retired, kids living at home etc)
*/


-- hardcode files
update NoDupes
   SET
h_lifestage_desc      = CASE h_lifestage                    WHEN '00'  THEN 'a) Very young family'
                                                            WHEN '01'  THEN 'b) Very young single'
                                                            WHEN '02'  THEN 'c) Very young homesharers'
                                                            WHEN '03'  THEN 'd) Young family'
                                                            WHEN '04'  THEN 'e) Young single'
                                                            WHEN '05'  THEN 'f) Young homesharers'
                                                            WHEN '06'  THEN 'g) Mature family'
                                                            WHEN '07'  THEN 'h) Mature singles'
                                                            WHEN '08'  THEN 'i) Mature homesharers'
                                                            WHEN '09'  THEN 'j) Older family'
                                                            WHEN '10'  THEN 'k) Older single'
                                                            WHEN '11'  THEN 'l) Older homesharers'
                                                            WHEN '12'  THEN 'm) Elderly family'
                                                            WHEN '13'  THEN 'n) Elderly single'
                                                            WHEN '14'  THEN 'o) Elderly homesharers'
                                                            WHEN 'U'   THEN 'p) Unclassified'
                                                            WHEN '99'  THEN 'q) Duplicate Experian Data'
                                                            ELSE            'q) missing'
                                                            END


/*
,Mirror_has_children  =  CASE  h_family_lifestage           WHEN '02'  THEN 'a) Children' --'c) Young family with children <18'
                                                            WHEN '03'  THEN 'a) Children' --'d) Young household with children <18'
                                                            WHEN '06'  THEN 'a) Children' --'g) Mature family with children <18'
                                                            WHEN '07'  THEN 'a) Children' --'h) Mature household with children <18'
                                                            WHEN '10'  THEN 'a) Children' --'j) Older family/household with children<18'
                                                            WHEN  'U'  THEN 'b) Unclassified'
                                                            WHEN '99'  THEN 'c) Duplicate Experian Data'
                                                            ELSE            'd) missing'
                                                            END
*/

,Mirror_has_children  =  CASE WHEN convert(integer, h_number_of_children_in_household_2011) > 0 THEN 'Y'
                              WHEN convert(integer, h_number_of_children_in_household_2011) = 0 THEN 'N'
                         ELSE 'M' END



,HomeOwner = CASE h_tenure                                  WHEN    '0'  THEN  'a) Owner occupied'
                                                            WHEN    '1'  THEN  'b) Privately rented'
                                                            WHEN    '2'  THEN  'c) Council / housing association'
                                                            WHEN    'U'  THEN  'd) Unclassified'
                                                            WHEN    '9'  THEN  'e) Duplicate Experian Data'
                                                            ELSE               'f) missing'
                                                            END

/*
,Financial_outlook = CASE h_fss_v3_group                    WHEN    'A' THEN    'a )Accumulated Wealth'
                                                            WHEN    'B' THEN    'b )Balancing Budgets'
                                                            WHEN    'C' THEN    'c )Bright Futures'
                                                            WHEN    'D' THEN    'd )Consolidating Assets'
                                                            WHEN    'E' THEN    'e )Established Reserves'
                                                            WHEN    'F' THEN    'f )Family Interest'
                                                            WHEN    'G' THEN    'g )Growing Rewards'
                                                            WHEN    'H' THEN    'h )Platinum Pensions'
                                                            WHEN    'I' THEN    'h )Seasoned Economy'
                                                            WHEN    'J' THEN    'i )Single Endeavours'
                                                            WHEN    'K' THEN    'j )Stretched Finances'
                                                            WHEN    'L' THEN    'k )Sunset Security'
                                                            WHEN    'M' THEN    'l )Traditional Thrift'
                                                            WHEN    'N' THEN    'm )Young Essentials'
                                                            WHEN    'U' THEN    'n) Unclassified'
                                                            WHEN    '9' THEN    'o) Duplicate Experian Data'
                                                            ELSE                'p) missing'
                                                            END
*/
,Financial_outlook = CASE h_fss_v3_group                    WHEN    'A' THEN    'A Bright Futures'
                                                            WHEN    'B' THEN    'B Single Endeavours'
                                                            WHEN    'C' THEN    'C Young Essentials'
                                                            WHEN    'D' THEN    'D Growing Rewards'
                                                            WHEN    'E' THEN    'E Family Interest'
                                                            WHEN    'F' THEN    'F Accumulated Wealth'
                                                            WHEN    'G' THEN    'G Consolidating Assets'
                                                            WHEN    'H' THEN    'H Balancing Budgets'
                                                            WHEN    'I' THEN    'I Stretched Finances'
                                                            WHEN    'J' THEN    'J Established Reserves'
                                                            WHEN    'K' THEN    'K Seasoned Economy'
                                                            WHEN    'L' THEN    'L Platinum Pensions'
                                                            WHEN    'M' THEN    'M Sunset Security'
                                                            WHEN    'N' THEN    'N Traditional Thrift'
                                                            WHEN    'U' THEN    'U Unallocated'
                                                            ELSE                'Missing'
                                                            END

,Demographic = CASE h_mosaic_uk_group                       WHEN    'A' THEN    'a )Alpha Territory'
                                                            WHEN    'B' THEN    'b )Professional Rewards'
                                                            WHEN    'C' THEN    'c )Rural Solitude'
                                                            WHEN    'D' THEN    'd )Small Town Diversity'
                                                            WHEN    'E' THEN    'e )Active Retirement'
                                                            WHEN    'F' THEN    'f )Suburban Mindsets'
                                                            WHEN    'G' THEN    'g )Careers and Kids'
                                                            WHEN    'H' THEN    'h )New Homemakers'
                                                            WHEN    'I' THEN    'i )Ex-Council Community'
                                                            WHEN    'J' THEN    'j )Claimant Cultures'
                                                            WHEN    'K' THEN    'k )Upper Floor Living'
                                                            WHEN    'L' THEN    'l )Elderly Needs'
                                                            WHEN    'M' THEN    'm )Industrial Heritage'
                                                            WHEN    'N' THEN    'n )Terraced Melting Pot'
                                                            WHEN    'O' THEN    'o )Liberal Opinions'
                                                            WHEN    'U' THEN    'p )Unclassified'
                                                            WHEN    '9' THEN    'q) Duplicate Experian Data'
                                                            ELSE                'r) missing'
                                                            END

/* use CACI social class
,Mirror_ABC1 = CASE h_mosaic_uk_type                        WHEN    '01'  THEN    'Y'
                                                            WHEN    '02'  THEN    'Y'
                                                            WHEN    '03'  THEN    'Y'
                                                            WHEN    '04'  THEN    'Y'
                                                            WHEN    '05'  THEN    'Y'
                                                            WHEN    '06'  THEN    'Y'
                                                            WHEN    '07'  THEN    'Y'
                                                            WHEN    '08'  THEN    'Y'
                                                            WHEN    '09'  THEN    'Y'
                                                            WHEN    '10'  THEN    'Y'
                                                            WHEN    '11'  THEN    'Y'
                                                            WHEN    '15'  THEN    'Y'
                                                            WHEN    '20'  THEN    'Y'
                                                            WHEN    '22'  THEN    'Y'
                                                            WHEN    '29'  THEN    'Y'
                                                            WHEN    '30'  THEN    'Y'
                                                            WHEN    '31'  THEN    'Y'
                                                            WHEN    '33'  THEN    'Y'
                                                            WHEN    '61'  THEN    'Y'
                                                            WHEN    '62'  THEN    'Y'
                                                            WHEN    '63'  THEN    'Y'
                                                            WHEN    '65'  THEN    'Y'
                                                            WHEN    '66'  THEN    'Y'
                                                            WHEN    '99'  THEN    'Z'
                                                            ELSE                  'N'
                                                            END
*/

,Kids_Aged_LE4 = CASE h_presence_of_child_aged_0_4_2011         WHEN    '1'       THEN    'Y'
                                                                WHEN    '0'       THEN    'N'
                                                                ELSE                      'M'
                                                                END
,Kids_Aged_5to11 = CASE h_presence_of_child_aged_5_11_2011      WHEN    '1'       THEN    'Y'
                                                                WHEN    '0'       THEN    'N'
                                                                ELSE                      'M'
                                                                END
,Kids_Aged_12to17 = CASE h_presence_of_child_aged_12_17_2011    WHEN    '1'       THEN    'Y'
                                                                WHEN    '0'       THEN    'N'
                                                                ELSE                      'M'
                                                                END
,H_AFFLUENCE =                                                  h_affluence_v2
;
commit;

CREATE   HG INDEX idx14 ON Nodupes(cb_key_household);
commit;

--select top 100 * from Nodupes;

--      update project141_universe file
UPDATE project141_universe
SET      h_lifestage             = EXP.h_lifestage_desc
        ,Mirror_has_children     = EXP.Mirror_has_children
        ,HomeOwner               = EXP.HomeOwner
        ,Financial_outlook       = EXP.Financial_outlook
        ,Demographic             = EXP.Demographic
        --,Mirror_ABC1             = EXP.Mirror_ABC1
        ,Lifestage               = EXP.h_lifestage
        ,Family_Lifestage        = EXP.h_family_lifestage
        ,tenure                  = EXP.h_tenure
        ,h_fss_v3_group          = EXP.h_fss_v3_group
        ,h_mosaic_uk_2009_group  = EXP.h_mosaic_uk_group
        ,h_mosaic_uk_2009_type   = EXP.h_mosaic_uk_type
        ,Kids_Aged_LE4           = EXP.Kids_Aged_LE4
        ,Kids_Aged_5to11         = EXP.Kids_Aged_5to11
        ,Kids_Aged_12to17        = EXP.Kids_Aged_12to17
        ,PAF                     = EXP.PAF
        ,H_AFFLUENCE             = EXP.H_AFFLUENCE
     FROM project141_universe  AS Base
  INNER JOIN NoDupes AS EXP
        ON base.cb_key_household = EXP.cb_key_household;
commit;

--select top 100 * from project141_universe;
--select count(*) from project141_universe;
--select h_fss_v3_group, count(*) from project141_universe group by h_fss_v3_group;

-----------------------------------------------------------------------------------------
--Derive age bands from Experian, would be useful later
-----------------------------------------------------------------------------------------
SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then '16 to <25'
                when p_actual_age >= 25 and p_actual_age < 35 then '25 to <35'
                when p_actual_age >= 35 and p_actual_age < 45 then '35 to <45'
                when p_actual_age >= 45 and p_actual_age < 55 then '45 to <55'
                when p_actual_age >= 55 and p_actual_age < 65 then '55 to <65'
                when p_actual_age >= 65                       then '65 Plus'
                else null end) as age_band
        ,(case when age_band = '16 to <25' then 1 else 0 end) as age16to25
        ,(case when age_band = '25 to <35' then 1 else 0 end) as age25to35
        ,(case when age_band = '35 to <45' then 1 else 0 end) as age35to45
        ,(case when age_band = '45 to <55' then 1 else 0 end) as age45to55
        ,(case when age_band = '55 to <65' then 1 else 0 end) as age55to65
        ,(case when age_band = '65 Plus'   then 1 else 0 end) as age65plus
INTO ageband
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @experian;
commit;
--select cb_change_date , count(*) from sk_prod.EXPERIAN_CONSUMERVIEW group by cb_change_date;
select top 10 cb_key_household
        ,max(age16to25) as HH_has_age16to25
        ,max(age25to35) as HH_has_age25to35
        ,max(age35to45) as HH_has_age35to45
        ,max(age45to55) as HH_has_age45to55
        ,max(age55to65) as HH_has_age55to65
        ,max(age65plus) as HH_has_age65plus
from ageband
group by cb_key_household;
commit;

-----------------------------------------------------------------------------------------
/*
(WI - All men in HH are in 16-24
WII - There is at least 1 man <35 in HH and not in WI
WIII - There is at least 1 man <45 in HH and not in WI or WII
WIV - There is at least 1 man <55 in HH and not in WI or WII or WIII
WV - Household contains only men >55
WVI - No men in HH
WVII - Unknown HH status)"
*/


-- create who lives in the household data
--code_location_21

--drop table TEMP_HOUSE;
--commit;

SELECT cb_key_household
        ,MF =  (CASE WHEN p_gender = '0' then 1                 --male
                    WHEN p_gender = '1' then 100                --female
                    WHEN p_gender = 'U' then 1000               --unknow
                    ELSE 9000                                   --missing data
               END)
INTO TEMP_HOUSE
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @experian
GROUP BY cb_key_household, MF
ORDER BY cb_key_household;
commit;

--select top 10 * from TEMP_HOUSE;

drop table TEMP_HOUSE_COUNT;
commit;

select cb_key_household, sum(MF) AS House_binary, count(MF) AS House_num
INTO TEMP_HOUSE_COUNT
from TEMP_HOUSE
group by cb_key_household;
commit;
--drop table TEMP_HOUSE;


/*
house_binary    UFM FLAG
1               M only household
100             F only household
101             FM household
1001            UM household
1100            UF household
1101            UFM household
1000            U household
*/


--select top 10 * from sk_prod.EXPERIAN_CONSUMERVIEW;
-- MALE
----age of 2nd male in household to replace Max
--drop table temp_AGEM;
--commit;

SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then 0
                when p_actual_age >= 25 and p_actual_age < 35 then 1
                when p_actual_age >= 35 and p_actual_age < 45 then 2
                when p_actual_age >= 45 and p_actual_age < 55 then 3
                when p_actual_age >= 55 and p_actual_age < 65 then 4
                when p_actual_age >= 65                       then 5
                else null end) as person_age_band
      ,rank() over(PARTITION BY cb_key_household ORDER BY person_age_band) AS rank_id
--        ,MAX(cast(person_age AS integer )) AS MAX_AGE
        ,p_gender
        --person_age
INTO temp_AGEM
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @experian
AND p_gender = '0'
GROUP BY cb_key_household, person_age_band, p_gender;
commit;

--select top 10 * from temp_AGEM;

--party 1

drop table TempAge1M;
commit;

SELECT cb_key_household, p_gender AS Male, person_age_band AS M1_AGE
INTO TempAge1M
FROM temp_ageM
WHERE rank_id = 1;
commit;

--party 2

drop table TempAge2M;
commit;

SELECT cb_key_household, p_gender AS Male, person_age_band AS M2_AGE
INTO TempAge2M
FROM temp_ageM
WHERE rank_id = 2;
commit;


--FEMALE
--add rank to data
--drop table temp_AGEF;
--commit;

SELECT cb_key_household
        ,(case when p_actual_age  >= 16 and p_actual_age < 25 then 0
                when p_actual_age >= 25 and p_actual_age < 35 then 1
                when p_actual_age >= 35 and p_actual_age < 45 then 2
                when p_actual_age >= 45 and p_actual_age < 55 then 3
                when p_actual_age >= 55 and p_actual_age < 65 then 4
                when p_actual_age >= 65                       then 5
                else null end) as person_age_band
        ,rank() over(PARTITION BY cb_key_household ORDER BY person_age_band) AS rank_id
--        ,MAX(cast(person_age AS integer )) AS MAX_AGE
        ,p_gender
        --person_age
INTO temp_AGEF
FROM sk_prod.EXPERIAN_CONSUMERVIEW
where cb_change_date = @experian
AND p_gender = '1'
GROUP BY cb_key_household, person_age_band, p_gender;
commit;

--party 1
drop table TempAge1F;
commit;

SELECT cb_key_household, p_gender AS Female, person_age_band AS F1_AGE
INTO TempAge1F
FROM temp_ageF
WHERE rank_id = 1;
commit;

--party 2
drop table TempAge2F;
commit;

SELECT cb_key_household, p_gender AS Female, person_age_band AS F2_AGE
INTO TempAge2F
FROM temp_ageF
WHERE rank_id = 2;
commit;


--- temp Age and household file
--drop table AGE_HOUSE;
--commit;

create table AGE_HOUSE (
        cb_key_household                bigint
        ,House_binary                   integer
        ,male                           integer
        ,M1_Age                         integer
        ,M2_Age                         integer
        ,F1_Age                         integer
        ,F2_Age                         integer
        ,Female                         integer
        ,MIRROR_MEN_MIN                 varchar(4)
        ,MIRROR_WOMEN_MIN               varchar(4)
);
commit;

Insert into AGE_HOUSE (cb_key_household,House_binary,male,M1_Age)
SELECT A.cb_key_household,House_binary,male,M1_Age
FROM  TEMP_HOUSE_COUNT AS A,  TempAge1M AS B
WHERE A.cb_key_household *= B.cb_key_household; --left join
commit;

--select top 100 * from AGE_HOUSE;

/*
SELECT A.cb_key_household,House_binary,male,M1_Age
INTo  AGE_HOUSE
FROM  TEMP_HOUSE_COUNT AS A,  TempAge1M AS B
WHERE A.cb_key_household *= B.cb_key_household;


ALTER table     AGE_HOUSE ADD   M2_Age             integer;
ALTER table     AGE_HOUSE ADD   F1_Age             integer;
ALTER table     AGE_HOUSE ADD   F2_Age             integer;
ALTER table     AGE_HOUSE ADD   Female             integer;
ALTER table     AGE_HOUSE ADD   MIRROR_MEN_MIN     varchar(4);
ALTER table     AGE_HOUSE ADD   MIRROR_WOMEN_MIN   varchar(4);
*/


--date queries
UPDATE AGE_HOUSE
SET    M2_Age             = aff.M2_Age
      FROM AGE_HOUSE  AS Base
         INNER JOIN TempAge2M AS aff
         ON base.cb_key_household = aff.cb_key_household;
commit;

UPDATE AGE_HOUSE
SET    F1_Age             = aff.F1_Age
     , female             = aff.female
      FROM AGE_HOUSE  AS Base
         INNER JOIN TempAge1F AS aff
         ON base.cb_key_household = aff.cb_key_household;
commit;

UPDATE AGE_HOUSE
SET    F2_Age             = aff.F2_Age
      FROM AGE_HOUSE  AS Base
         INNER JOIN TempAge2F AS aff
         ON base.cb_key_household = aff.cb_key_household;
commit;

drop table temp_AGEM; commit;
drop table temp_AGEF; commit;
drop table TEMP_HOUSE_COUNT; commit;

--define mirror segments
CREATE HG INDEX idx10 ON AGE_HOUSE(cb_key_household);
commit;

--select top 100 * from AGE_HOUSE;

update AGE_HOUSE
   SET
        MIRROR_MEN_MIN  = CASE WHEN house_binary in (1)               AND M1_Age in (0) AND M2_Age is null             THEN 'WI'
                               WHEN house_binary in (1)               AND M1_Age in (1)                                THEN 'WII'
                               WHEN house_binary in (1)               AND M1_Age in (0) AND M2_Age in (1)              THEN 'WII'
                               WHEN house_binary in (101,1001,1101)   AND M1_Age in (0,1)                              THEN 'WII'
                               WHEN house_binary in (1)               AND M1_Age in (0) AND M2_Age IN (2)              THEN 'WIII'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (2)                                THEN 'WIII'
                               WHEN house_binary in (1)               AND M1_Age in (0) AND M2_Age in (3)              THEN 'WIV'
                               WHEN house_binary in (1,101,1001,1101) AND M1_Age in (3)                                THEN 'WIV'
                               WHEN house_binary in (1)               AND M1_Age in (0) AND M2_Age in (4,5)            THEN 'WV'
                               WHEN house_binary in (1)               AND M1_Age in (4,5)                              THEN 'WV'
                               WHEN house_binary in (100,1100)                                                         THEN 'WVI'
                               WHEN house_binary in (1000)                                                             THEN 'WVII'
                               END

    , MIRROR_WOMEN_MIN  = CASE WHEN house_binary in (100)               AND F1_Age in (0) AND F2_Age is null            THEN 'WI'
                               WHEN house_binary in (100)               AND F1_Age in (1)                               THEN 'WII'
                               WHEN house_binary in (100)               AND F1_Age in (0) AND F2_Age in (1)             THEN 'WII'
                               WHEN house_binary in (101,1100,1101)     AND F1_Age in (0,1)                             THEN 'WII'
                               WHEN house_binary in (100)               AND F1_Age in (0) AND F2_Age IN (2)             THEN 'WIII'
                               WHEN house_binary in (100,101,1100,1101) AND F1_Age in (2)                               THEN 'WIII'
                               WHEN house_binary in (100)               AND F1_Age in (0) AND F2_Age in (3)             THEN 'WIV'
                               WHEN house_binary in (100,101,1100,1101) AND F1_Age in (3)                               THEN 'WIV'
                               WHEN house_binary in (100)               AND F1_Age in (0) AND F2_Age in (4,5)           THEN 'WV'
                               WHEN house_binary in (100)               AND F1_Age in (4,5)                             THEN 'WV'
                               WHEN house_binary in (1,1001)                                                            THEN 'WVI'
                               WHEN house_binary in (1000)                                                              THEN 'WVII'
                               END;
commit;


        --      update project141_universe file
UPDATE project141_universe
SET      MIRROR_MEN_MIN       = EXP.MIRROR_MEN_MIN
        ,MIRROR_WOMEN_MIN     = EXP.MIRROR_WOMEN_MIN
FROM project141_universe  AS Base INNER JOIN AGE_HOUSE AS EXP
ON base.cb_key_household = EXP.cb_key_household;
commit;



-- select top 100 * from sk_prod.EXPERIAN_CONSUMERVIEW;
-- select top 100 * from project141_universe;
-- select count(*) from project141_universe;

------------------------------------------------------------------------------------------
--                                                                                      --
--   create affluence file this will need to be changed once file is into production    --
--                                                                                       --
------------------------------------------------------------------------------------------
--Change_needed
--code_location_22
/*
drop table H_AFFLUENCE;
SELECT cb_key_household, H_AFFLUENCE
INTO H_AFFLUENCE
FROM sk_prodreg.EXP_AFFLUENCE_MODEL_20120416 /*sk_prod.EXPERIAN_CONSUMERVIEW*/
/*
GROUP BY cb_key_household, H_AFFLUENCE
ORDER BY cb_key_household;

CREATE   HG INDEX idx10 ON H_AFFLUENCE(cb_key_household);

--update file with affluence data
UPDATE project141_universe
SET    H_AFFLUENCE             = aff.H_AFFLUENCE
      FROM project141_universe  AS Base
         INNER JOIN H_AFFLUENCE AS aff
         ON base.cb_key_household = aff.cb_key_household;

*/


-- sky customer lifestage - based on customer tenure
--code_location_23
--drop table life;
--commit;

select distinct a.account_number
        ,case when datediff(day,acct_first_account_activation_dt,@today) <=   91 then 'A) Welcome'
              when datediff(day,acct_first_account_activation_dt,@today) <=  300 then 'B) Mid'
              when datediff(day,acct_first_account_activation_dt,@today) <=  420 then 'C) End'
              when datediff(day,acct_first_account_activation_dt,@today) >   420 then 'D) 15+'
              else                                                                    'E) missing'
              end as Sky_cust_life
        ,rank() over(PARTITION BY a.account_number ORDER BY acct_first_account_activation_dt desc) AS rank_id
         INTO life
    from project141_universe AS A LEFT JOIN sk_prod.cust_single_account_view as SAV
                 ON A.account_number = SAV.Account_number
    where cust_active_dtv = 1
    group by a.account_number, Sky_cust_life,acct_first_account_activation_dt;
commit;

DELETE FROM  life where rank_id >1;
commit;

--update file with Sky_cust_life data
UPDATE project141_universe
SET    Sky_cust_life             = SCL.Sky_cust_life
      FROM project141_universe  AS Base
         INNER JOIN life AS SCL
         ON base.account_number = SCL.account_number;
commit;

/*
----------adding IUL data
--Change_needed
--code_location_24
IF object_id('tmpDSOdb_ilu') IS NOT NULL THEN DROP TABLE tmpDSOdb_ilu END IF;
SELECT          cb_row_id, account_number,
                CASE    WHEN P1 = 1  THEN 1
                        WHEN P2 = 1  THEN 2
                        ELSE              3        END AS Correspondent,
                rank() over(PARTITION BY account_number ORDER BY Correspondent asc
 , cb_row_id desc) AS rank
INTO            tmpDSOdb_ilu
FROM            (SELECT         ilu.cb_row_id, base.account_number,
                            MAX(CASE WHEN ilu.ilu_correspondent = 'P1' THEN 1 ELSE 0 END) AS P1,
                            MAX(CASE WHEN ilu.ilu_correspondent = 'P2' THEN 1 ELSE 0 END) AS P2,
                            MAX(CASE WHEN ilu.ilu_correspondent = 'OR' THEN 1 ELSE 0 END) AS OR1
                FROM            sk_prod.ilu_BAK AS ilu
                        INNER JOIN project141_universe AS base
                                ON ilu.cb_key_household = base.cb_key_household
                WHERE           ilu.cb_key_household IS NOT NULL
                AND             ilu.cb_key_household <>0
                GROUP BY        ilu.cb_row_id, base.account_number
                HAVING          P1 + P2 + OR1 > 0) AS tgt;

DELETE FROM tmpDSOdb_ilu where rank > 1;

CREATE  HG INDEX idx01 on tmpDSOdb_ilu(cb_row_id);
CREATE  HG INDEX idx02 on tmpDSOdb_ilu(account_number);
*/

-----------------------------------------------------------------------------------------------
--                                                                                           --
--   adding on Kids this will need to be changed once data has been sourced from Experian    --
--                                                                                           --
-----------------------------------------------------------------------------------------------
--Change_needed
/*
UPDATE          project141_universe AS base
SET             Kids_Age_LE4    = case when (cast(ilu_ikid0004  AS integer ))>0 THEN 'Y'
                                                                ElSE 'N'
                                                                END
               ,Kids_Age_4to9   = case when (cast(ilu_ikid0507 AS integer ))>0
                                         OR (cast(ilu_ikid0810 AS integer ))>0  THEN 'Y'
                                                                ElSE 'N'
                                                                END
               ,Kids_Age_10to15 = case when (cast(ilu_ikid1116 AS integer ))>0  THEN 'Y'
                                                                ElSE 'N'
                                                                END
FROM            sk_prod.ilu_BAK AS ilu INNER JOIN tmpDSOdb_ilu
ON              ilu.cb_row_id = tmpDSOdb_ilu.cb_row_id
WHERE           base.account_number = tmpDSOdb_ilu.account_number;
*/


-- select top 1000 * from patelj.tmpDSOdb_ilu;


--code_location_25
-------------------------------------------------------------- C01 - Entertainment extra
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
  INTO mixes
  FROM sk_prod.cust_subs_hist as csh
       INNER JOIN sk_prod.cust_entitlement_lookup as cel
               ON csh.current_short_description = cel.short_description
 WHERE csh.subscription_sub_type ='DTV Primary Viewing'
   AND csh.subscription_type = 'DTV PACKAGE'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= @today
   AND csh.effective_to_dt   >  @today
   AND csh.effective_from_dt != csh.effective_to_dt
;
commit;

UPDATE mixes
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

Update project141_universe
set ent_extra = case when new_package = 'Entertainment Extra' then 1 else 0 end
from project141_universe as base
        inner join mixes as mi on base.account_number = mi.account_number;
commit;


--drop table sports_movies_active;
--commit;

-------------------------------------------------------------- C02 - Sky Sports 1 and 2 & Movies 1 and 2
-- code_location_26
Select  csh.account_number
        ,max(cel.sport_1) as sky_sports_1
        ,max(cel.sport_2) as sky_sports_2
        ,Max(cel.movie_1) as movies_1
        ,Max(cel.movie_2) as movies_2
into sports_movies_active
 FROM sk_prod.cust_subs_hist AS csh
           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
     WHERE csh.status_code in ('AC','AB','PC')
       AND csh.effective_from_dt <= @today
       AND csh.effective_to_dt    > @today
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND account_number is not null
  GROUP BY csh.account_number;
commit;

Update project141_universe
SET  base.sky_sports_1 = sm.sky_sports_1
    ,base.sky_sports_2 = sm.sky_sports_2
    ,base.movies_1 = sm.movies_1
    ,base.movies_2 = sm.movies_2
FROM project141_universe as base INNER JOIN sports_movies_active AS sm
ON base.account_number = sm.account_number;
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

Update project141_universe
set base.sky_id = samprofileid
from project141_universe as base
        inner join sky_id as si on base.account_number = si.account_number;
commit;


--------------------------------------------------------------------DISNEY
--code_location_28

Update project141_universe
set Disney = 1
  from sk_prod.cust_subs_hist as csh
        inner join project141_universe as base on base.account_number = csh.account_number
   and subscription_type ='A-LA-CARTE'                          --A La Carte Stack
   and subscription_sub_type in ('DTV Disney Channel')          --ESPN or disney Subscriptions
   and status_code in ('AC','AB','PC')                          --Active Status Codes
   and effective_from_dt <= @today                              --Start on or before 1st Jan
   and effective_to_dt > @today                                 --ends after 1st Jan
   and effective_from_dt<>effective_to_dt;                      --remove duplicate records
commit;



----------------------------------------   adding in TV region data
--code_location_29
Update project141_universe
set      base.barb_id_bbc   = si.barb_id_bbc
        ,base.barb_desc_bbc = si.barb_desc_bbc
        ,base.barb_id_itv   = si.barb_id_itv
        ,base.barb_desc_itv = si.barb_desc_itv
from project141_universe as base inner join sk_prod.BARB_TV_REGIONS as si
on base.cb_address_postcode = si.postcode;
commit;

--select barb_desc_itv, count(*) from project141_universe group by barb_desc_itv;

------------------------ Home Movers need to check the defintion
-- code_location_30
--drop table HM;

select       nw.account_number
            ,nw.ad_effective_from_dt    as addr_change_dt
            ,nw.ad_effective_to_dt
            ,nw.change_reason           as change_reason
            ,rank() over(partition by nw.account_number order by addr_change_dt DESC, nw.ad_effective_to_dt DESC, nw.cb_row_id DESC) as rank1
into        HM
from        sk_prod.cust_all_address AS nw
where       nw.ad_effective_from_dt <= @today
    and     nw.address_role = 'INSTALL'
    and     nw.account_number <> '?'
    and     nw.account_number IS NOT NULL
    AND     nw.change_reason LIKE ('Move Home%')
    and     nw.account_number in (select account_number from project141_universe);
commit;

DELETE FROM HM where rank1 >1;
commit;

Update project141_universe
SET  base.Home_mover = sm.rank1
FROM project141_universe as base INNER JOIN HM AS sm
ON base.account_number = sm.account_number;
commit;


----------------------- sourcing Account_cust_account_id
-- code_location_31
--drop table Acct_cust;

select base.account_number
           ,exp.ACCT_CUST_ACCOUNT_ID
into Acct_cust
from  project141_universe  AS Base inner join sk_prod.CUST_SINGLE_ACCOUNT_VIEW as exp
        ON base.account_number = EXP.account_number;
commit;


Update project141_universe
SET  base.ACCT_CUST_ACCOUNT_ID = sm.ACCT_CUST_ACCOUNT_ID
FROM project141_universe as base INNER JOIN Acct_cust AS sm
ON base.account_number = sm.account_number;
commit;

--Drop table TMP_PREV_SUB_LKP;
--commit;

------------------------ Person Key & Individual Key
-- code_location_32    ---- taken and adapted from Tiken 22/6/2012 code originated from Dinesh
--Create a temporary lookup table (valid only for current sql session) for derivation of prev_sports_subscription and prev_movies_subscription fields

select account_number,
      if curr_sports = 1 and prev_sports = 1 then 1 else 0 endif AS prev_sports_subscriber,
      if curr_movies = 1 and prev_movies = 1 then 1 else 0 endif AS prev_movies_subscriber
into TMP_PREV_SUB_LKP
from (
       select account_number,
             count(*) x,
             max(if sh.effective_to_dt = '9999-09-09' and sport_1 = 0 and sport_2 = 0 and prem_sports = 0 then 1 else 0 endif) curr_sports,
             max(if sh.effective_to_dt < '9999-09-09' and (sport_1 <> 0 or sport_2 <> 0 or prem_sports <> 0)  then 1 else 0 endif) prev_sports,
             max(if sh.effective_to_dt = '9999-09-09' and movie_1 = 0 and movie_2 = 0 and prem_movies = 0 then 1 else 0 endif) curr_movies,
             max(if sh.effective_to_dt < '9999-09-09' and (movie_1 <> 0 or movie_2 <> 0 or prem_movies <> 0) then 1 else 0 endif) prev_movies
        from sk_prod.CUST_SUBS_HIST sh,
             sk_prod.CUST_ENTITLEMENT_LOOKUP el
        where sh.ent_cat_prod_sk = el.product_sk
             and sh.subscription_sub_type = 'DTV Primary Viewing'
             and
                (
                    (sh.effective_to_dt = '9999-09-09' and sport_1 = 0 and sport_2 = 0 and prem_sports = 0) -- Not taken sports
                    or (sh.effective_to_dt < '9999-09-09' and (sport_1 <> 0 or sport_2 <> 0 or prem_sports <> 0)) -- Taken sports
                    or (sh.effective_to_dt = '9999-09-09' and movie_1 = 0 and movie_2 = 0 and prem_movies = 0) -- Not taken movies
                    or (sh.effective_to_dt < '9999-09-09' and (movie_1 <> 0 or movie_2 <> 0 or prem_movies <> 0)) -- Taken movies
                )
        group by account_number
      ) as x
where (curr_sports = 1 and prev_sports = 1) or (curr_movies = 1 and prev_movies = 1);
commit;

--drop table temp_dinesh; commit;
--Create the extract

select
    CUST_SINGLE_ACCOUNT_VIEW.account_number,
    CUST_SINGLE_ACCOUNT_VIEW.cb_key_household,
    CUST_SINGLE_ACCOUNT_VIEW.cb_key_db_person,
    CUST_SINGLE_ACCOUNT_VIEW.cb_key_individual,
    ( case
        when upper(CUST_SINGLE_ACCOUNT_VIEW.isba_tv_region) = 'NOT DEFINED' then NULL
        else
        CUST_SINGLE_ACCOUNT_VIEW.isba_tv_region
      end
    ) as isba_tv_region,
    ( case upper(substring(trim(county_lkp.county_masked_value), 1, 1))
        when 'E' then 'England'
        when 'W' then 'Wales'
        when 'N' then 'Northern Ireland'
        when 'S' then 'Scotland'
        else
            case
            when CUST_SINGLE_ACCOUNT_VIEW.pty_country = '?' then NULL
            else
            CUST_SINGLE_ACCOUNT_VIEW.pty_country
            end
    end ) as country,
    ( case upper(bpe.cable_postcode)
         when 'Y' then 'Yes'
         when 'N' then 'No'
         else NULL
    end ) as cable_area_flag,
    ( case
        when lkp.prev_sports_subscriber = 1 then 'Yes'
        when lkp.prev_sports_subscriber = 0 then 'No'
        when lkp.prev_sports_subscriber is NULL then 'No'
        else NULL
    end ) as prev_sports_subscription,
    ( case
        when lkp.prev_movies_subscriber = 1 then 'Yes'
        when lkp.prev_movies_subscriber = 0 then 'No'
        when lkp.prev_movies_subscriber is NULL then 'No'
        else NULL
    end ) as prev_movies_subscription,
    bpe.government_region,
    ( case
        when bfp.fibre_enabled_perc >= 50 then 'Yes'
        when bfp.fibre_enabled_perc < 50 then 'No'
       else NULL
    end ) as bt_fibre_area,
    CUST_SINGLE_ACCOUNT_VIEW.prod_broadband_exchange_id as exchange_id,
    CUST_SINGLE_ACCOUNT_VIEW.prod_broadband_network_type as exchange_status,
    CUST_SINGLE_ACCOUNT_VIEW.prod_broadband_unbundled as exchange_unbundled
    INTO temp_Dinesh
from
    sk_prod.CUST_SINGLE_ACCOUNT_VIEW LEFT OUTER JOIN TMP_PREV_SUB_LKP lkp ON (lkp.account_number = CUST_SINGLE_ACCOUNT_VIEW.account_number),
    sk_prod.CUST_SINGLE_ACCOUNT_VIEW LEFT OUTER JOIN sk_prod.BROADBAND_POSTCODE_EXCHANGE bpe ON (bpe.cb_address_postcode = CUST_SINGLE_ACCOUNT_VIEW.cb_address_postcode),
    sk_prod.CUST_SINGLE_ACCOUNT_VIEW LEFT OUTER JOIN sk_prod.BT_FIBRE_POSTCODE bfp ON (bfp.cb_address_postcode = CUST_SINGLE_ACCOUNT_VIEW.cb_address_postcode),
    sk_prod.CUST_SINGLE_ACCOUNT_VIEW LEFT OUTER JOIN sk_prod.TSA_COUNTY_MASKING_LOOKUP county_lkp ON (county_lkp.county_name = CUST_SINGLE_ACCOUNT_VIEW.cb_address_county),
where CUST_SINGLE_ACCOUNT_VIEW.account_number <> '99999999999999'
and CUST_SINGLE_ACCOUNT_VIEW.account_number not like '%.%'
and CUST_SINGLE_ACCOUNT_VIEW.ph_subs_status_code in ('AC','AB','PC')
and upper(CUST_SINGLE_ACCOUNT_VIEW.prod_ph_subs_subscription_sub_type) = 'DTV PRIMARY VIEWING'
;
commit;


CREATE   HG INDEX idx01 ON temp_Dinesh(account_number);
commit;

/*
ALTER TABLE project141_universe     ADD (cb_key_db_person       varchar(50));
ALTER TABLE project141_universe     ADD (cb_key_individual      bigint);
ALTER TABLE project141_universe     ADD (government_region      varchar(50));
ALTER TABLE project141_universe     ADD (bt_fibre_area          varchar(10));
ALTER TABLE project141_universe     ADD (exchange_id            varchar(10));
ALTER TABLE project141_universe     ADD (exchange_status        varchar(10));
ALTER TABLE project141_universe     ADD (exchange_unbundled     varchar(10));
ALTER TABLE project141_universe     ADD (isba_tv_region         varchar(50));
*/

Alter table project141_universe Add Country varchar(50);
commit;

Update project141_universe
SET  base.cb_key_db_person = sm.cb_key_db_person
    ,base.cb_key_individual = sm.cb_key_individual
    ,base.government_region = sm.government_region
    ,base.bt_fibre_area = sm.bt_fibre_area
    ,base.exchange_id = sm.exchange_id
    ,base.exchange_status = sm.exchange_status
    ,base.exchange_unbundled = sm.exchange_unbundled
    ,base.isba_tv_region = sm.isba_tv_region
    ,base.country = sm.country
FROM project141_universe as base INNER JOIN temp_Dinesh AS sm
ON base.account_number = sm.account_number;
commit;

/*
select isba_tv_region, count(*)
from project141_universe
group by isba_tv_region;
*/

-- Make amendments to the data because of CBI requirements
Update project141_universe
set isba_tv_region = case when isba_tv_region in ('HTV Wales','HTV West')         then 'Wales and West'
                         when isba_tv_region = 'Ulster'                          then 'Northern Ireland'
                         when isba_tv_region = 'North Scotland'                  then 'Northern Scotland'
                         when isba_tv_region = 'North West'                      then 'Lancashire'
                         when isba_tv_region = 'Meridian (exc. Channel Islands)' then 'Southern'
                         when isba_tv_region = 'East Of England'                 then 'East of England'
                         else isba_tv_region
                     end
;
commit;


------------------------ Customer ID & V Score
-- code_location_33
--drop table modelscore;
--commit;

SELECT  distinct base.cb_key_household
        ,model.model_score
INTO modelscore
  FROM sk_prod.ID_V_Universe_all as model
      inner join project141_universe as base
on base.cb_key_household = model.cb_key_household;
commit;

ALTER TABLE project141_universe     ADD (model_score  integer);
commit;

Update project141_universe
SET  base.model_score = sm.model_score
FROM project141_universe as base INNER JOIN modelscore AS sm
ON base.cb_key_household = sm.cb_key_household;
commit;


------------------------ Household Composition
-- code_location_




/*
-----------adding on IUL affluence to compare to Experian data
----------- this will need to be dropped when ConsumerView comes into production
ALTER TABLE project141_universe     ADD (IUL_Affluence_group  varchar(25) default 'X) Missing');

UPDATE          project141_universe AS base
SET             IUL_Affluence_group =  case WHEN ilu.ILU_HHAfflu IN (01,02,03,04) THEN 'A) Very Low'
                                            WHEN ilu.ILU_HHAfflu IN (05,06)       THEN 'B) Low'
                                            WHEN ilu.ILU_HHAfflu IN (07,08)       THEN 'C) Mid Low'
                                            WHEN ilu.ILU_HHAfflu IN (09,10)       THEN 'D) Mid'
                                            WHEN ilu.ILU_HHAfflu IN (11,12)       THEN 'E) Mid High'
                                            WHEN ilu.ILU_HHAfflu IN (13,14,15)    THEN 'F) High'
                                            WHEN ilu.ILU_HHAfflu IN (16,17)       THEN 'G) Very High' end
FROM            sk_prod.ilu_BAK AS ilu INNER JOIN tmpDSOdb_ilu
ON              ilu.cb_row_id = tmpDSOdb_ilu.cb_row_id
WHERE           base.account_number = tmpDSOdb_ilu.account_number;

drop table tmpDSOdb_ilu;
*/


ALTER TABLE project141_universe     ADD (Affluence_group  varchar(25) default 'X) Missing');
commit;

UPDATE          project141_universe
SET             Affluence_group =  case         WHEN H_AFFLUENCE IN ('00','01','02')       THEN 'A) Very Low'
                                                WHEN H_AFFLUENCE IN ('03','04', '05')      THEN 'B) Low'
                                                WHEN H_AFFLUENCE IN ('06','07','08')       THEN 'C) Mid Low'
                                                WHEN H_AFFLUENCE IN ('09','10','11')       THEN 'D) Mid'
                                                WHEN H_AFFLUENCE IN ('12','13','14')       THEN 'E) Mid High'
                                                WHEN H_AFFLUENCE IN ('15','16','17')       THEN 'F) High'
                                                WHEN H_AFFLUENCE IN ('18','19')            THEN 'G) Very High' END
;
commit;

--select top 100 * from project141_universe;
--select currency_code, count(*) from project141_universe group by currency_code;

/***************************************
 *****   SET  [CURRENT_PACKAGE]    *****
 ***************************************/

-- prepare...
--drop table cur_package;

CREATE TABLE cur_package(
        ACCOUNT_NUMBER          varchar(20) default NULL,
        prem_sports             integer     default NULL,
        prem_movies             integer     default NULL,
        ent_cat_prod_start_dt   date        NOT NULL,
        Variety                 integer     default NULL,
        Knowledge               integer     default NULL,
        Kids                    integer     default NULL,
        Style_Culture           integer     default NULL,
        Music                   integer     default NULL,
        News_Events             integer     default NULL,
        rank                    integer     NOT NULL,
        Num_PremSports          tinyint     default 0,
        Num_PremMovies          tinyint     default 0,
        Num_Premiums            tinyint     default 0,
        Num_Mix                 tinyint     default 0,
        TV_Package              varchar(50) default 'UNKNOWN',
        Mix_Pack                varchar(20) default 'UNKNOWN'
);
commit;


INSERT INTO cur_package (ACCOUNT_NUMBER, prem_sports, prem_movies,
        ent_cat_prod_start_dt, Variety, Knowledge, Kids, Style_Culture,
        Music, News_Events, rank)
SELECT ar.account_number
        --,effective_from_dt
        ,cel.prem_sports
        ,cel.prem_movies
        ,ent_cat_prod_start_dt
        ,cel.Variety
        ,cel.Knowledge
        ,cel.Kids
        ,cel.Style_Culture
        ,cel.Music
        ,cel.News_Events
        ,rank() over(partition by ar.account_number ORDER BY csh.effective_from_dt, csh.cb_row_id desc) as rank
--INTO --drop table
--        tempdb..viq_current_pkg_tmp
FROM project141_universe ar
        left join sk_prod.cust_subs_hist as csh
            on csh.account_number = ar.account_number
        inner join sk_prod.cust_entitlement_lookup as cel
            on csh.current_short_description = cel.short_description
WHERE csh.subscription_sub_type ='DTV Primary Viewing'
       AND csh.subscription_type = 'DTV PACKAGE'
       AND csh.status_code in ('AC','AB','PC')
       AND csh.effective_from_dt < today()
       AND csh.effective_to_dt   >=  today()
       AND csh.effective_from_dt != csh.effective_to_dt;
commit;

-- select top 100 * from cur_package;

DELETE FROM cur_package WHERE rank > 1;
commit;

----add index
CREATE INDEX idx1 ON cur_package(account_number);
commit;


--Add mix detail to the table
--update new columns

UPDATE cur_package a
SET
a.Num_PremSports = b.prem_sports,
a.Num_PremMovies = b.prem_Movies,
a.Num_Premiums = b.prem_sports + b.prem_Movies,
a.Num_Mix = (b.Variety + b.Knowledge + b.Kids + b.Style_Culture + b.Music + b.News_Events)
FROM cur_package b
WHERE b.account_number = a.account_number;
commit;

-- this update depends on data derived by the previous update
UPDATE cur_package a
SET
a.mix_pack=case
                when b.Num_Mix is null or b.Num_Mix=0                     then 'Entertainment Pack'
                when (b.variety=1 or b.style_culture=1)  and b.Num_Mix=1  then 'Entertainment Pack'
                when (b.variety=1 and b.style_culture=1) and b.Num_Mix=2  then 'Entertainment Pack'
                when b.Num_Mix > 0                                        then 'Entertainment Extra'
            end
FROM cur_package b
WHERE b.account_number = a.account_number;
commit;

-- this update depends on data derived by the previous update
UPDATE cur_package a
SET
a.TV_Package = case
                when b.prem_movies=2 and b.prem_sports=2 then 'Top Tier (4 Premiums)'
                when b.prem_movies=0 and b.prem_sports=2 then 'Dual Sports (2 Sports – 0 Movies)'
                when b.prem_movies=2 and b.prem_sports=0 then 'Dual Movies (0 Sports – 2 Movies)'
                when b.prem_movies=0 and b.prem_sports=1 then 'Single sports (1 Sports – 0 Movies)'
                when b.prem_movies=1 and b.prem_sports=0 then 'Single Movies (0 Sports – 1 Movies)'
                when b.prem_movies>0 and b.prem_sports>0 then 'Other Premium (1 & 1, 2 & 1, 1 & 2)'
                when b.prem_movies=0 and b.prem_sports=0 and b.mix_pack = 'Entertainment Pack'  then 'Basic'
                when b.prem_movies=0 and b.prem_sports=0 and b.mix_pack = 'Entertainment Extra' then 'Basic Entertainment'
                end
FROM cur_package b
WHERE b.account_number = a.account_number;
commit;

Alter table project141_universe add CURRENT_PACKAGE varchar(50);
commit;


--update  tv_package  into main HOUSEHOLD table
UPDATE project141_universe v
   SET v.CURRENT_PACKAGE = a.tv_package
   from cur_package a
   where (a.account_number = v.account_number
   and a.tv_package is not null);
commit;

-- select top 100 * from project141_universe;

--drop table
drop table cur_package;
commit;

/*
select CURRENT_PACKAGE, count(*)
from project141_universe
group by CURRENT_PACKAGE;
*/


---------------------------------------------------------------------------------
--  HOUSEHOLD COMPOSITION
---------------------------------------------------------------------------------
--select top 100 * from sk_prod.EXPERIAN_CONSUMERVIEW;

--drop table hh_composition;
--commit;

select cb_key_household
       ,cb_row_id
       ,cb_address_town
       ,h_household_composition
       ,rank() over(PARTITION BY cv.cb_key_household ORDER BY cv.CB_ROW_ID desc) as rank_id
into hh_composition
from sk_prod.EXPERIAN_CONSUMERVIEW cv
WHERE cb_change_date= @experian
ORDER BY cv.cb_key_household;
commit;

DELETE FROM hh_composition where rank_id >1;
commit;

CREATE HG INDEX idx14 ON hh_composition(cb_key_household);
commit;

Alter table project141_universe Add  HOUSEHOLD_COMPOSITION varchar(35)  default 'UNCLASSIFIED'; commit;
Alter table project141_universe Add  cb_address_town       varchar(50)  default 'Missing'; commit;

update project141_universe
set base.HOUSEHOLD_COMPOSITION  =
        (CASE  hhcomp.h_household_composition   WHEN     '00' THEN   'Families'
                                                WHEN     '01' THEN   'Extended family'
                                                WHEN     '02' THEN   'Extended household'
                                                WHEN     '03' THEN   'Pseudo family'
                                                WHEN     '04' THEN   'Single male'
                                                WHEN     '05' THEN   'Single female'
                                                WHEN     '06' THEN   'Male homesharers'
                                                WHEN     '07' THEN   'Female homesharers'
                                                WHEN     '08' THEN   'Mixed homesharers'
                                                WHEN     '09' THEN   'Abbreviated male families'
                                                WHEN     '10' THEN   'Abbreviated female families'
                                                WHEN     '11' THEN   'Multi-occupancy dwelling'
                                                WHEN     'U'  THEN   'UNCLASSIFIED'
                                                ELSE                 'UNCLASSIFIED'
                                                END)
    ,base.cb_address_town        =              hhcomp.cb_address_town
from project141_universe as base
     inner join hh_composition as hhcomp
on base.cb_key_household = hhcomp.cb_key_household;
commit;

-- select top 100 * from hh_composition;
-- select HOUSEHOLD_COMPOSITION, count(*) from project141_universe group by HOUSEHOLD_COMPOSITION;
-- select cb_address_town, count(*) from project141_universe group by cb_address_town;

--select top 100 * from sk_prod.EXPERIAN_CONSUMERVIEW;
--select top 100 * from project141_universe;
--select top 100 * from sk_prod.CUST_SINGLE_ACCOUNT_VIEW;

--------------------------------------------------------------------------
-- METROPOLITAN AREA & TV REGION
--------------------------------------------------------------------------

-- Some of the Experian data have missing postcode towns, derive postcode areas directly from postcode
--drop table pc_area_table;
--commit;

--select top 100 * from project141_universe;

select account_number
        ,cb_key_household
        ,barb_desc_itv
        ,trim(upper(cb_address_town))           as pc_town
        ,substr(cb_address_postcode,1,2)        as pc_area
into pc_area_table
from project141_universe;
commit;

--select top 100 * from pc_area_table;

drop table pc_area_table2;
commit;

select *
        ,(case when substr(pc_area,2,1) in ('1','2','3','4','5','6','7','8','9','0')
               then substr(pc_area,1,1) else pc_area end) as pc_area2
into pc_area_table2
from pc_area_table;
commit;

--select top 100 * from pc_area_table2;

-- Do lookup table for postcode areas
drop table temp;
commit;

select *,(case when pc_area2= 'AB' then 'Aberdeen'
                when pc_area2= 'AL' then 'St. Albans'
                when pc_area2= 'B' then 'Birmingham'
                when pc_area2= 'BA' then 'Bath'
                when pc_area2= 'BB' then 'Blackburn'
                when pc_area2= 'BD' then 'Bradford'
                when pc_area2= 'BH' then 'Bournemouth'
                when pc_area2= 'BL' then 'Bolton'
                when pc_area2= 'BN' then 'Brighton'
                when pc_area2= 'BR' then 'Bromley'
                when pc_area2= 'BS' then 'Bristol'
                when pc_area2= 'BT' then 'Belfast'
                when pc_area2= 'CA' then 'Carlisle'
                when pc_area2= 'CB' then 'Cambridge'
                when pc_area2= 'CF' then 'Cardiff'
                when pc_area2= 'CH' then 'Chester'
                when pc_area2= 'CM' then 'Chelmsford'
                when pc_area2= 'CO' then 'Colchester'
                when pc_area2= 'CR' then 'Croydon'
                when pc_area2= 'CT' then 'Canterbury'
                when pc_area2= 'CV' then 'Coventry'
                when pc_area2= 'CW' then 'Crewe'
                when pc_area2= 'DA' then 'Dartford'
                when pc_area2= 'DD' then 'Dundee'
                when pc_area2= 'DE' then 'Derby'
                when pc_area2= 'DG' then 'Dumfries'
                when pc_area2= 'DH' then 'Durham'
                when pc_area2= 'DL' then 'Darlington'
                when pc_area2= 'DN' then 'Doncaster'
                when pc_area2= 'DT' then 'Dorchester'
                when pc_area2= 'DY' then 'Dudley'
                when pc_area2= 'E' then 'London East'
                when pc_area2= 'EC' then 'London East Central'
                when pc_area2= 'EH' then 'Edinburgh'
                when pc_area2= 'EN' then 'Enfield'
                when pc_area2= 'EX' then 'Exeter'
                when pc_area2= 'FK' then 'Falkirk'
                when pc_area2= 'FY' then 'Fylde (Blackpool)'
                when pc_area2= 'G' then 'Glasgow'
                when pc_area2= 'GL' then 'Gloucester'
                when pc_area2= 'GU' then 'Guildford'
                when pc_area2= 'GY' then 'Guernsey & Alderney'
                when pc_area2= 'HA' then 'Harrow'
                when pc_area2= 'HD' then 'Huddersfield'
                when pc_area2= 'HG' then 'Harrogate'
                when pc_area2= 'HP' then 'Hemel Hempstead'
                when pc_area2= 'HR' then 'Hereford'
                when pc_area2= 'HS' then 'HEBRIDES'
                when pc_area2= 'HU' then 'Hull'
                when pc_area2= 'HX' then 'Halifax'
                when pc_area2= 'IG' then 'Ilford'
                when pc_area2= 'IM' then 'Isle of Man'
                when pc_area2= 'IP' then 'Ipswich'
                when pc_area2= 'IV' then 'Inverness'
                when pc_area2= 'JE' then 'Jersey'
                when pc_area2= 'KA' then 'Kilmarnock'
                when pc_area2= 'KT' then 'Kingston Upon Thames'
                when pc_area2= 'KW' then 'Kirkwall'
                when pc_area2= 'KY' then 'Kirkcaldy'
                when pc_area2= 'L' then 'Liverpool'
                when pc_area2= 'LA' then 'Lancaster'
                when pc_area2= 'LD' then 'Llandrindod Wells'
                when pc_area2= 'LE' then 'Leicester'
                when pc_area2= 'LL' then 'Llandudno'
                when pc_area2= 'LN' then 'Lincoln'
                when pc_area2= 'LS' then 'Leeds'
                when pc_area2= 'LU' then 'Luton'
                when pc_area2= 'M'  then 'Manchester'
                when pc_area2= 'ME' then 'Medway (Rochester)'
                when pc_area2= 'MK' then 'Milton Keynes'
                when pc_area2= 'ML' then 'Motherwell'
                when pc_area2= 'N'  then 'London North'
                when pc_area2= 'NE' then 'Newcastle on Tyne'
                when pc_area2= 'NG' then 'Nottingham'
                when pc_area2= 'NN' then 'Northampton'
                when pc_area2= 'NP' then 'Newport'
                when pc_area2= 'NR' then 'Norwich'
                when pc_area2= 'NW' then 'London North West'
                when pc_area2= 'OL' then 'Oldham'
                when pc_area2= 'OX' then 'Oxford'
                when pc_area2= 'PA' then 'Paisley'
                when pc_area2= 'PE' then 'Peterborough'
                when pc_area2= 'PH' then 'Perth'
                when pc_area2= 'PL' then 'Plymouth'
                when pc_area2= 'PO' then 'Portsmouth'
                when pc_area2= 'PR' then 'Preston'
                when pc_area2= 'RG' then 'Reading'
                when pc_area2= 'RH' then 'Redhill'
                when pc_area2= 'RM' then 'Romford'
                when pc_area2= 'S' then 'Sheffield'
                when pc_area2= 'SA' then 'Swansea'
                when pc_area2= 'SE' then 'London South East'
                when pc_area2= 'SG' then 'Stevenage'
                when pc_area2= 'SK' then 'Stockport'
                when pc_area2= 'SL' then 'Slough'
                when pc_area2= 'SM' then 'Sutton'
                when pc_area2= 'SN' then 'Swindon'
                when pc_area2= 'SO' then 'Southampton'
                when pc_area2= 'SP' then 'Salisbury'
                when pc_area2= 'SR' then 'Sunderland'
                when pc_area2= 'SS' then 'Southend on Sea'
                when pc_area2= 'ST' then 'Stoke On Trent'
                when pc_area2= 'SW' then 'London South West'
                when pc_area2= 'SY' then 'Shrewsbury'
                when pc_area2= 'TA' then 'Taunton'
                when pc_area2= 'TD' then 'Berwick upon Tweed'
                when pc_area2= 'TF' then 'Telford'
                when pc_area2= 'TN' then 'Tunbridge Wells'
                else null
                end) as pc_area3
into temp
from pc_area_table2;
commit;

-- Because of overflow error msg, repeat lookup for the remaining postcode areas

drop table pc_area_table3;
commit;

select *, (case when pc_area2= 'TQ' then 'Torquay'
                when pc_area2= 'TR' then 'Truro'
                when pc_area2= 'TS' then 'Teesside (Middlesbrough)'
                when pc_area2= 'TW' then 'Twickenham'
                when pc_area2= 'UB' then 'Uxbridge'
                when pc_area2= 'W' then 'London West'
                when pc_area2= 'WA' then 'Warrington'
                when pc_area2= 'WC' then 'London West Central'
                when pc_area2= 'WD' then 'Watford'
                when pc_area2= 'WF' then 'Wakefield'
                when pc_area2= 'WN' then 'Wigan'
                when pc_area2= 'WR' then 'Worcester'
                when pc_area2= 'WS' then 'Walsall'
                when pc_area2= 'WV' then 'Wolverhampton'
                when pc_area2= 'YO' then 'York'
                when pc_area2= 'ZE' then 'Lerwick'
                else null
                end) as pc_area4,
                trim(upper(coalesce(pc_area3, pc_area4))) as pc_area_desc
into pc_area_table3
from temp;
commit;

-- QA for any missing cases
/*
select top 100 * from pc_area_table3;

select * from pc_area_table3
where pc_area_desc is null;
*/
-- group postcode areas into metropolitan areas from the lookup table

--drop table metro_area;
--commit;

select *,(case when pc_town is null then pc_area_desc else pc_town end) as postcode_town,
         (case when postcode_town=  'BIRMINGHAM' then 'Birmingham metropolitan area'
            when postcode_town=  'WOLVERHAMPTON' then 'Birmingham metropolitan area'
            when postcode_town=  'COVENTRY' then 'Birmingham metropolitan area'
            when postcode_town=  'NUNEATON' then 'Birmingham metropolitan area'
            when postcode_town=  'WARWICK' then 'Birmingham metropolitan area'
            when postcode_town=  'LEAMINGTON' then 'Birmingham metropolitan area'
            when postcode_town=  'REDDITCH' then 'Birmingham metropolitan area'
            when postcode_town=  'BROMSGROVE' then 'Birmingham metropolitan area'
            when postcode_town=  'TAMWORTH' then 'Birmingham metropolitan area'

            when postcode_town=  'MANCHESTER' then 'Manchester metropolitan area'
            when postcode_town=  'MACCLESFIELD' then 'Manchester metropolitan area'

            when postcode_town=  'LEEDS' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'BRADFORD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'HUDDERSFIELD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'HALIFAX' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'QUEENSBURY' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'WAKEFIELD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'CASTLEFORD' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'PONTEFRACT' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'HARROGATE' then 'Leeds-Bradford metropolitan area'
            when postcode_town=  'DEWSBURY' then 'Leeds-Bradford metropolitan area'

            when postcode_town=  'LIVERPOOL' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'BIRKENHEAD' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'WIGAN' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'ASHTON' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'WARRINGTON' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'WIDNES' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'RUNCORN' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'CHESTER' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'SOUTHPORT' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'ELLESMERE PORT' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'ORMSKIRK' then 'Liverpool/Birkenhead metropolitan area'
            when postcode_town=  'SKELMERSDALE' then 'Liverpool/Birkenhead metropolitan area'

            when postcode_town=  'NEWCASTLE' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'SUNDERLAND' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'BLYTH' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'CRAMLINGTON' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'PETERLEE' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'ASHINGTON' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'SEAHAM' then 'Newcastle-Sunderland metropolitan area'
            when postcode_town=  'CHESTER-LE-STREET' then 'Newcastle-Sunderland metropolitan area'

            when postcode_town=  'SHEFFIELD' then 'Sheffield metropolitan area'
            when postcode_town=  'ROTHERHAM' then 'Sheffield metropolitan area'
            when postcode_town=  'DONCASTER' then 'Sheffield metropolitan area'
            when postcode_town=  'DARFIELD' then 'Sheffield metropolitan area'
            when postcode_town=  'CHESTERFIELD' then 'Sheffield metropolitan area'
            when postcode_town=  'BARNSLEY' then 'Sheffield metropolitan area'

            when postcode_town=  'PORTSMOUTH' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'SOUTHAMPTON' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'BOGNOR REGIS' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'SALISBURY' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'WINCHESTER' then 'Portsmouth/Southampton metropolitan area'
            when postcode_town=  'ANDOVER' then 'Portsmouth/Southampton metropolitan area'

            when postcode_town=  'NOTTINGHAM' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'DERBY' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'LLKESTON' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'ALFRETON' then 'Nottingham-Derby metropolitan area'

            when postcode_town=  'MANSFIELD' then 'Nottingham-Derby metropolitan area'
            when postcode_town=  'NEWARK' then 'Nottingham-Derby metropolitan area'

            when postcode_town=  'GLASGOW' then 'Glasgow metropolitan area'
            when postcode_town=  'EAST KILBRIDE' then 'Glasgow metropolitan area'
            when postcode_town=  'CUMBERNAULD' then 'Glasgow metropolitan area'
            when postcode_town=  'KILMARNOCK' then 'Glasgow metropolitan area'
            when postcode_town=  'DUMBARTON' then 'Glasgow metropolitan area'

            when postcode_town=  'CARDIFF' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'NEWPORT' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'MERTHYR TYDFIL' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'PONTYPRIDD' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'CAERPHILLY' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'BRIDGEND' then 'Cardiff and South Wales valleys metropolitan area'
            when postcode_town=  'EBBW VALE' then 'Cardiff and South Wales valleys metropolitan area'

            when postcode_town=  'BRISTOL' then 'Bristol metropolitan area'
            when postcode_town=  'WESTON-SUPER-MARE' then 'Bristol metropolitan area'
            when postcode_town=  'BATH' then 'Bristol metropolitan area'
            when postcode_town=  'CLEVEDON' then 'Bristol metropolitan area'

            when postcode_town=  'BELFAST' then 'Belfast metropolitan area'
            when postcode_town=  'BANGOR' then 'Belfast metropolitan area'

            when postcode_town=  'EDINBURGH' then 'Edinburgh metropolitan area'
            when postcode_town=  'LIVINGSTON' then 'Edinburgh metropolitan area'

            when postcode_town=  'BRIGHTON' then 'Brighton/Worthing/Littlehampton metropolitan area'
            when postcode_town=  'WORTHING' then 'Brighton/Worthing/Littlehampton metropolitan area'
            when postcode_town=  'EASTBOURNE' then 'Brighton/Worthing/Littlehampton metropolitan area'
            when postcode_town=  'LITTLEHAMPTON' then 'Brighton/Worthing/Littlehampton metropolitan area'

            when postcode_town=  'LEICESTER' then 'Leicester metropolitan area'
            when postcode_town=  'LOUGHBOROUGH' then 'Leicester metropolitan area'
            when postcode_town=  'SHEPSHED' then 'Leicester metropolitan area'
            when postcode_town=  'HINCKLEY' then 'Leicester metropolitan area'
            when postcode_town=  'COALVILLE' then 'Leicester metropolitan area'
            when postcode_town=  'MELTON MOWBRAY' then 'Leicester metropolitan area'
            else null
            end) as metropolitan_area
into metro_area
from pc_area_table3;
commit;

-- QA for any missing cases

--select * from metro_area where postcode_town is null;

drop table metro_area_n_tv_region;
commit;

select *, (case when metropolitan_area is null
                then barb_desc_itv else metropolitan_area end)
                as metropolitan_area_and_itv_region
into metro_area_n_tv_region
from metro_area;
commit;

drop table metro_area;
commit;

CREATE HG INDEX idx15 ON metro_area_n_tv_region(cb_key_household);
commit;

--select top 100 * from project141_universe;

Alter table project141_universe Add metropolitan_area_and_itv_region varchar(70); commit;
Alter table project141_universe Add pc_area                          varchar(5); commit;
Alter table project141_universe Add postcode_town                    varchar(70); commit;

Update project141_universe
set base.metropolitan_area_and_itv_region  = metro.metropolitan_area_and_itv_region
        ,base.pc_area                      = metro.pc_area2
        ,base.postcode_town                = metro.postcode_town
from project141_universe as base inner join metro_area_n_tv_region as metro
on base.cb_key_household = metro.cb_key_household;
commit;

-- QA

--select top 100 * from metro_area_n_tv_region where pc_area2 = 'GY'

/*
select barb_desc_itv, count(*)
from project141_universe
group by barb_desc_itv;
*/

--select top 100 * from project141_universe;
/*
select country, count(*)
from project141_universe
where metropolitan_area_and_itv_region = 'Border'
group by country

*/

--Make corrections to the data because of CBI requirements

Update project141_universe base
set base.metropolitan_area_and_itv_region = case when base.pc_area in ('JE','GY') then 'Channel Islands'
                                            else base.metropolitan_area_and_itv_region end -- Jersey, Guernsey & Alderney
;
commit;

Update project141_universe base
set base.metropolitan_area_and_itv_region = case when base.metropolitan_area_and_itv_region = 'Meridian (exc. Channel Islands)' then 'Meridian'
                                                 when base.metropolitan_area_and_itv_region = 'North East' then 'North-East'
                                                 when base.metropolitan_area_and_itv_region = 'North West' then 'North-West'
                                                 when base.metropolitan_area_and_itv_region = 'South West' then 'South-West'
                                                 when base.metropolitan_area_and_itv_region = 'East Of England' then 'East-of-England'
                                                 when base.metropolitan_area_and_itv_region = 'North Scotland' then 'Northern Scotland'
                                            else base.metropolitan_area_and_itv_region end
;
commit;

Update project141_universe base
set base.metropolitan_area_and_itv_region = case when (base.metropolitan_area_and_itv_region = 'Border') and
                                                     (base.pc_area in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     or upper(base.country) = 'SCOTLAND') then 'Border-Scotland'
                                                 when (base.metropolitan_area_and_itv_region = 'Border') and
                                                     (base.pc_area not in ('AB','DD','DG','EH','FK','G','HS','IV','KA','KY','ML','PA','PH','TD')
                                                     or upper(base.country) = 'ENGLAND') then 'Border-England'
                                            else base.metropolitan_area_and_itv_region end

,base.government_region = case when base.government_region = 'Yorkshire and the Humber' then 'Yorkshire and The Humber'
                              else base.government_region end
;
commit;

/*
select metropolitan_area_and_itv_region, count(*)
from project141_universe
--where metropolitan_area_and_itv_region = 'Border'
group by metropolitan_area_and_itv_region

*/


-------------------------------------------------------------------------------
-- CACI SOCIAL CLASS
-------------------------------------------------------------------------------

--SELECT top 100 * from project141_universe;

drop table caci_sc;
commit;

select  c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into caci_sc
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
order by c.cb_key_household;
commit;

/*
select  c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY cb_key_household ORDER BY playpen.p_head_of_household desc, c.cb_row_id) as rank_id
into caci_sc
from sk_prod.CACI_SOCIAL_CLASS as c
inner join sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen
on c.cb_key_individual = playpen.exp_cb_key_individual
where cb_address_dps is NOT NULL;
*/

delete from caci_sc where rank_id > 1;
commit;

--select count(cb_key_household) from caci_sc;
--select count(distinct(cb_key_household)) from caci_sc;
--select top 10 * from sk_prod.CACI_SOCIAL_CLASS;


CREATE HG INDEX idx16 ON caci_sc(cb_key_household);
commit;

Alter table project141_universe Add  social_grade varchar(15)  default 'UNCLASSIFIED';
commit;

Update project141_universe
set base.social_grade = sc.social_grade
from project141_universe as base inner join caci_sc as sc
on base.cb_key_household = sc.cb_key_household;
commit;

--select count(*) from project141_universe;
--select top 100 * from project141_universe;

--Overwrite Mirror_ABC1 with CACI Social Class

Update project141_universe
set Mirror_ABC1 = case when social_grade in ('A','B','C1') then 'Y' else 'N' end;
commit;

--------------------------------------------------------------------------------
--Tenure of the Customer
--------------------------------------------------------------------------------
--drop table custenure;
--commit;

select account_number
        ,cb_key_household
        ,cb_key_db_person
        ,cb_key_individual
        ,acct_first_account_activation_dt
        ,(case when datediff(year,acct_first_account_activation_dt,today())     <= 1      then '0-1 Year'
               when datediff(year,acct_first_account_activation_dt,today())     > 1
                   and datediff(year,acct_first_account_activation_dt,today())  <= 2      then '1-2 Years'
               when datediff(year,acct_first_account_activation_dt,today())     > 2
                   and datediff(year,acct_first_account_activation_dt,today())  <= 10     then '2-10 Years'
               when datediff(year,acct_first_account_activation_dt,today())     > 10      then '10+ Years'
               else null
               end) as cust_tenure
into custenure
from sk_prod.cust_single_account_view
where cust_active_dtv = 1;
commit;

CREATE HG INDEX idx17 ON custenure(account_number);
commit;

Alter table project141_universe Add cust_tenure varchar(20);
commit;

Update project141_universe
set base.cust_tenure = ct.cust_tenure
from project141_universe as base inner join custenure as ct
on base.account_number = ct.account_number;
commit;

---------------------------------------------------------------------------------------------
/*
select * from sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD
where exp_cb_key_individual =  2523098036722800938;

select * from sk_prod.EXPERIAN_CONSUMERVIEW
where exp_cb_key_individual =  2523098036722800938;
*/

--select top 100 * from sk_prod.EXPERIAN_CONSUMERVIEW;
--select top 100 * from sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD;

drop table head_of_household;
commit;

select cv.cb_row_id
        ,cv.cb_key_individual
        ,cv.cb_key_household
        ,cv.cb_key_family
        ,cv.p_actual_age
        ,cv.p_gender
        ,cv.h_lifestage
        ,playpen.p_head_of_household
        ,playpen.p_employment_status
        ,playpen.p_employment_status_v2
into head_of_household
from sk_prod.EXPERIAN_CONSUMERVIEW as cv
inner join sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen
on cv.exp_cb_key_individual = playpen.exp_cb_key_individual
where cv.cb_change_date = @experian;
commit;

--select top 100 * from head_of_household;


---------------------------------------------------------------------------------------------------------------
-- get person age and gender for the head of household from the Experian data

drop table lifestage_age;
commit;

SELECT   cb_key_household
         ,p_actual_age
         ,p_gender
         ,rank() over(PARTITION BY cb_key_household ORDER BY p_head_of_household desc,cb_row_id desc) AS rank_id
INTO lifestage_age
FROM head_of_household;
commit;

delete from lifestage_age where rank_id > 1;
commit;

CREATE HG INDEX idx15 ON lifestage_age(cb_key_household);
commit;

/*
--select count(*) from project141_universe;
--select top 100 * from project141_universe;

drop table AdSmart_lifestage_age;
select cb_key_household
                ,h_lifestage
                ,null as p_gender
                ,null as p_actual_age
into AdSmart_lifestage_age
from AdSmart_20121005;

--select top 100 * from AdSmart_lifestage_age;
*/

Alter table project141_universe Add head_of_HH_gender          integer; commit;
Alter table project141_universe Add head_of_HH_actual_age      integer; commit;
Alter table project141_universe Add head_of_HH_age_band        varchar(20); commit;

update project141_universe
set head_of_HH_gender            = temp.p_gender
    ,head_of_HH_actual_age       = temp.p_actual_age
from project141_universe as base inner join lifestage_age as temp
on base.cb_key_household = temp.cb_key_household;
commit;

Update project141_universe
set head_of_HH_age_band = (case when head_of_HH_actual_age >= 16 and head_of_HH_actual_age <= 24 then '16 to 24'
                                when head_of_HH_actual_age >= 25 and head_of_HH_actual_age <= 35 then '25 to 35'
                                when head_of_HH_actual_age >= 36 and head_of_HH_actual_age <= 45 then '36 to 45'
                                when head_of_HH_actual_age >= 46 and head_of_HH_actual_age <= 55 then '46 to 55'
                                when head_of_HH_actual_age >= 56 and head_of_HH_actual_age <= 65 then '56 to 65'
                                when head_of_HH_actual_age >= 66                                 then '66 Plus'
                                else null end)
;
commit;

--select top 100 * from project141_universe;

/*
select h_lifestage
                ,(case when p_gender = 0 then 'Male'
                       when p_gender = 1 then 'Female'
                       else null end) as person_gender
                ,(case when p_actual_age  >= 16 and p_actual_age < 25 then '16 to <25'
                        when p_actual_age >= 25 and p_actual_age < 35 then '25 to <35'
                        when p_actual_age >= 35 and p_actual_age < 45 then '35 to <45'
                        when p_actual_age >= 45 and p_actual_age < 55 then '45 to <55'
                        when p_actual_age >= 55 and p_actual_age < 65 then '55 to <65'
                        when p_actual_age >= 65                       then '65 Plus'
                        else null end) as person_age_band
                ,count(*)
from AdSmart_lifestage_age
group by h_lifestage
         ,person_gender
         ,person_age_band;
*/

--select top 1000 * from project141_universe

-------------------------------------------------------------------------------------------------------------

---- FILE NAMES NEED TO BE CHANGED TO REFLECT CHANGE IN DATA DATE

-- grant access to most recent project141_universe table
GRANT SELECT ON project141_universe TO PUBLIC;
commit;

-- create date stamped version of project141_universe table
drop table AdSmart_20121030;                                    -- date of run
commit;

select *
into AdSmart_20121030
from project141_universe;
commit;

GRANT SELECT ON AdSmart_20121030 TO PUBLIC;
commit;

/*
-- create date stamped version of Experians Consumer View table --- may not be needed if Sarah Jackson doesnt need it
select *
into Nodupes_20121024
from Nodupes;
commit;

GRANT SELECT ON Nodupes_20121024 TO PUBLIC;
commit;

drop table Nodupes;
commit;

-- create date stamped version of Experians Consumer View table --- may not be needed if Sarah Jackson doesnt need it
select *
into AGE_HOUSE_20121024
from AGE_HOUSE;
commit;

GRANT SELECT ON AGE_HOUSE_20121024 TO PUBLIC;
commit;

drop table AGE_HOUSE ;
commit;
*/

select top 100 *
from project141_universe;
commit;

select count(*), count(distinct(account_number))
from project141_universe
commit;

----------------------------------------------------------------------------------------

select metropolitan_area_and_itv_region, count(*)
from project141_universe
group by metropolitan_area_and_itv_region;
commit;


/*
select isba_tv_region, barb_desc_itv, count(*)
from project141_universe
group by isba_tv_region, barb_desc_itv

select isba_tv_region, count(*)
from project141_universe
group by isba_tv_region

select barb_desc_itv, count(*)
from project141_universe
group by barb_desc_itv

select government_region, count(*)
from project141_universe
group by government_region

*/

--select top 100 * from project141_universe







