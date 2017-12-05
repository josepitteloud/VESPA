/*



                Id:             V083
                Name:           ATL BB upgrade
                Lead:           Sarah Moore
                Date:           17/09/2012
                Analyst:        Susanne Chan

                QA date:
                QA analyst:

                Notes:  - used 13-19 Aug to avoid unresolved data issues with phase 2 at time of running of analysis
                        - Only 13- 26 Aug data time period available as an option,
                        AFTER panel ramp-up to include non-BB customers AND to avoid the Olympic period
**CODE SECTIONS**

PART A: Create Base Table
----A0 Create Table
----A1 Populate with active customers during 13-19 Aug 2012
----A2 Flag whether Sky BB customer or not
----A3 Region & Postcode
----A4 Flag whether Onnet or Offnet / Cable / Fibre
----A5 Anytime Plus
----A6 Sky Go
----A7 BB Propensity Model



PART B: Viewing Data (For Customers active over 13th to 19th August 2012)
----B0 Create Table
---- B1: Get programme data from sk_prod.VESPA_EVENTS_VIEWED_ALL
---- B2: Get viewing data from vespa_analysts.Vespa_daily_augs_##^^*^*##

PART C: Reduce to universe in Vespa panel, aggregate profiling and viewing data
---- C0: Create base table for Universe including target audience as agreed with Sky ATL media planning team
---- C1: Add DM opt in
---- C2: Purge accounts to VESPA PANEL and add viewing data
---- C3: Add Scaling Weights
---- C4: Add media pack / sales house
---- C5: Clean Data - remove viewing records under the min cap of 6 seconds


----Z0 OUTPUT


**TABLES**

V083_Base:              All active customers over 13th to 19th August 2012 flagged into universes
V083_Viewing:           Viewing data for all customers in V083_Base
V083_Target_Base:       Active customers on Vespa panel, flagged into agreed target universe

-----------------------------------------------------------------------------------------------------------------------------------------------------

-- PART A: Sky Base (Customers active over 13th to 19th August 2012)
*/

CREATE variable @snapshot_start_dt DATE;
CREATE variable @snapshot_end_dt DATE ;

SET @snapshot_start_dt = '2012-08-13' ;
SET @snapshot_end_dt = '2012-08-19' ;

---------------------------------------------------------------------------------------------------------------------------------------------------------
----A0 Create Table
---------------------------------------------------------------------------------------------------------------------------------------------------------

IF object_ID ('V083_Base') IS NOT NULL THEN
            DROP TABLE V083_Base
END IF;

CREATE TABLE V083_Base

    ( account_number                            date(viewing_starts)
     ,BB_Sky                                    TINYINT DEFAULT 0
     ,Area_Onnet                                TINYINT DEFAULT 0
     ,Area_Cable                                TINYINT DEFAULT 0
     ,Area_Fibre                                TINYINT DEFAULT 0
     ,Area_Sky_Fibre                            TINYINT DEFAULT 0
     ,Likely_upgrade                            TINYINT DEFAULT 0
     ,Anytime_activated                         TINYINT DEFAULT 0
     ,Anytime_user_3m                           TINYINT DEFAULT 0
     ,Sky_Go_registered                         TINYINT DEFAULT 0
     ,Sky_Go_user_3m                            TINYINT DEFAULT 0
     ,Region                                    VARCHAR(20)     DEFAULT 'UNKNOWN'
     ,postcode_no_space                         VARCHAR(10)     NULL
)
;

COMMIT;



CREATE HG INDEX idx_account_number_hg ON V083_Base(account_number);

GRANT SELECT ON V083_Base TO PUBLIC;


--select top 100 * from chans.V083_Base
select top 100 * from chans.V083_allinone;
------------------------------------------------------------------------------------------------------------------------------------------------------------
----A1 Populate with active customers during 13-19 Aug 2012
------------------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO V083_Base (account_number)
SELECT   account_number
FROM     sk_prod.cust_subs_hist csh
WHERE subscription_sub_type = 'DTV Primary Viewing'
      and status_code IN ('AC','AB','PC')
      and (csh.effective_from_dt  <= @snapshot_start_dt and csh.effective_to_dt > @snapshot_end_dt)
;
COMMIT;
--10012868 Row(s) affected

------------------------------------------------------------------------------------------------------------------------------------------------------------
----A2 Flag whether Sky BB customer or not
------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE V083_Base
   SET BB_Sky        = tgt.broadband
 FROM V083_Base AS base
      INNER JOIN (
                    SELECT  csh.account_number
                          ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                       AND (       status_code in ('AC','AB')
                                               OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                               OR (status_code='CF' AND prev_status_code='PC'                                  )
                                               OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                            )                                    THEN 1 ELSE 0 END)  AS broadband
                      FROM sk_prod.cust_subs_hist AS csh
                           INNER JOIN V083_Base AS base ON csh.account_number = base.account_number
                     WHERE csh.effective_from_dt <= @snapshot_start_dt
                       AND csh.effective_to_dt    > @snapshot_start_dt
                       AND csh.subscription_sub_type = 'Broadband DSL Line'  --< Optimises the code, limit to what is needed
                       AND csh.effective_from_dt <> csh.effective_to_dt
                  GROUP BY csh.account_number
        )AS tgt ON base.account_number = tgt.account_number;

COMMIT;
--4366382 Row(s) affected

------------------------------------------------------------------------------------------------------------------------------------------------------------
----A3 Region & Postcode
------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE V083_Base
SET     postcode_no_space          = REPLACE(sav.cb_address_postcode,' ','')
        ,Region                     = CASE WHEN sav.isba_tv_region = 'Not Defined'
                                       THEN 'UNKNOWN'
                                       ELSE sav.isba_tv_region
                                   END
FROM V083_Base AS base
        INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number
;
--10012861 Row(s) affected


------------------------------------------------------------------------------------------------------------------------------------------------------------
----A4 Flag whether Onnet or Offnet / Cable / Fibre
------------------------------------------------------------------------------------------------------------------------------------------------------------

-- ONNET
---------


-- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
    SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
      INTO #bpe
      FROM sk_prod.BROADBAND_POSTCODE_EXCHANGE
  GROUP BY postcode;

  UPDATE #bpe SET postcode = REPLACE(postcode,' ',''); -- Remove spaces for matching
--1776825 Row(s) affected


-- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
    SELECT postcode, MAX(exchange_id) as exchID
      INTO #p2e
      FROM sk_prod.BB_POSTCODE_TO_EXCHANGE
  GROUP BY postcode;


  UPDATE #p2e SET postcode = REPLACE(postcode,' ','');  -- Remove spaces for matching
--1702687 Row(s) affected


-- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible

SELECT #bpe.postcode, COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id, 'OFFNET' as exchange
  INTO #onnet_lookup
  FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode;
--1482958 Row(s) affected


-- 4) Update with latest Easynet exchange information

UPDATE #onnet_lookup
   SET exchange = 'ONNET'
  FROM #onnet_lookup AS base
       INNER JOIN sk_prod.easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
WHERE easy.exchange_status = 'ONNET';
--10012868 Row(s) affected


-- 5) Flag base table with onnet exchange data. Note that this uses a postcode field with

UPDATE V083_Base
   SET Area_onnet = CASE WHEN tgt.exchange = 'ONNET'
                    THEN 1
                    ELSE 0
                END
  FROM V083_Base AS base
       INNER JOIN #onnet_lookup AS tgt on base.postcode_no_space = tgt.postcode;
--9204402 Row(s) affected


-- CABLE
---------
   UPDATE V083_Base
      SET Area_cable = 1
     FROM V083_Base AS base
          INNER JOIN sk_prod.broadband_postcode_exchange as bb ON base.postcode_no_space = replace(bb.cb_address_postcode,' ','')
                                                              AND UPPER(bb.cable_postcode) = 'Y';
--3417266 Row(s) affected



-- FIBRE
---------

UPDATE V083_Base
   SET Area_fibre = 1
  FROM V083_Base AS base
       INNER JOIN sk_prod.BT_FIBRE_POSTCODE AS fib
                  ON base.postcode_no_space =  REPLACE(fib.cb_address_postcode,' ','')
                 AND fib.fibre_enabled_perc >= 75
 WHERE fib.first_fibre_enabled_date <= @snapshot_start_dt ;
--3521304 Row(s) affected

-- SKY FIBRE
---------

UPDATE V083_Base
   SET Area_Sky_fibre = case when area_onnet = 1 and Area_fibre = 1 then 1 else 0 end
;
--10012868 Row(s) affected


--====
-- QA
--====

-- SELECT count(*) as total, SUM(Area_onnet), SUM(Area_cable), SUM(Area_fibre) ,SUM(Area_Sky_fibre) FROM V083_Base
/*
total   SUM(V083_Base.Area_onnet)       SUM(V083_Base.Area_cable)       SUM(V083_Base.Area_fibre)       SUM(V083_Base.Area_Sky_fibre)
9857219 7534748                         3355836                         3466503                         3313026
*/

------------------------------------------------------------------------------------------------------------------------------------------------------------
----A5 Anytime Plus
------------------------------------------------------------------------------------------------------------------------------------------------------------

--A+ active
SELECT base.account_number
       ,1 AS Anytime_plus
INTO #Anytime_plus
FROM   sk_prod.CUST_SUBS_HIST AS csh
                                        inner join V083_Base as Base
                                                on csh.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<@snapshot_start_dt   -- (END)
AND        first_activation_dt>='2010-10-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
--2438806 Row(s) affected

UPDATE V083_Base bas
SET Anytime_activated = 1
FROM #Anytime_plus a
WHERE a.account_number = bas.account_number
;--2443964 Row(s) affected


--DOWNLOAD USAGE
SELECT distinct account_number
               ,1 AS Anytime_Active_L6M
INTO #APLUS_DOWN
FROM   sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
WHERE  last_modified_dt BETWEEN @snapshot_start_dt - 90 AND @snapshot_start_dt
AND    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND    x_actual_downloaded_size_mb > 1    -- to exclude any spurious header/trailer download records
;--969404 Row(s) affected

UPDATE V083_Base bas
SET Anytime_user_3m   = 1
FROM #APLUS_DOWN a
WHERE a.account_number = bas.account_number
;--941591 Row(s) affected

------------------------------------------------------------------------------------------------------------------------------------------------------------
----A6 Sky Go
------------------------------------------------------------------------------------------------------------------------------------------------------------

--Sky Go registered

  select sav.account_number
        ,acct_sam_registered_for_skyanytime_on_pc
        ,acct_sam_registered_for_skyanytime_on_mobile
    into #sav_cut
    from sk_prod.cust_single_account_view as sav
         inner join V083_Base as bas on sav.account_number = bas.account_number
;
--10013509 Row(s) affected


UPDATE V083_Base bas
SET Sky_Go_registered = 1
FROM #sav_cut sav
WHERE sav.account_number = bas.account_number
and (acct_sam_registered_for_skyanytime_on_pc = '1'
or acct_sam_registered_for_skyanytime_on_mobile = '1');
--4207478 Row(s) affected



--DOWNLOAD USAGE
select account_number
into #skygo
from sk_prod.SKY_PLAYER_USAGE_DETAIL
where activity_dt BETWEEN @snapshot_start_dt - 90 AND @snapshot_start_dt
;--101665934 Row(s) affected

UPDATE V083_Base bas
SET Sky_Go_user_3m = 1
FROM #skygo go
WHERE go.account_number = bas.account_number
;--2261150 Row(s) affected
------------------------------------------------------------------------------------------------------------------------------------------------------------
----A7 BB Propensity Model
------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT  account_number
        ,Decile
INTO    #bb
FROM    models.model_scores
WHERE   model_run_date = '2012-08-16' AND model_name = 'BB TALK WLR'  --uses closest model run date to the period analysed
;--6240293 Row(s) affected

UPDATE V083_Base
SET Likely_upgrade = CASE WHEN Decile <5 THEN 1 ELSE 0 END
FROM V083_Base bas INNER JOIN #bb bb
        ON bas.account_number = bb.account_number
;
--6156226 Row(s) affected



/* CHECKS
SELECT bas.account_number
        ,Decile
        ,CASE WHEN Decile <5 THEN 1 ELSE 0 END as likely_upgrade
INTO   #up
FROM V083_Base bas INNER JOIN #bb bb
        ON bas.account_number = bb.account_number
;
--6156226 Row(s) affected

select count (distinct(account_number))
        ,decile
        ,likely_upgrade
from #up
group by decile
        ,likely_upgrade;

select count (distinct(bas.account_number))
,decile
from V083_Base bas INNER JOIN #bb bb
        ON bas.account_number = bb.account_number
group by decile
;

select distinct(model_run_date)
from models.model_scores
where model_name = 'BB TALK WLR';

select distinct (current_product_description)
,subscription_sub_type
from sk_prod.CUST_SUBS_HIST
order by current_product_description
;

select  count(*)
        ,Decile
from models.model_scores
WHERE   model_run_date = '2012-08-16' AND model_name = 'BB TALK WLR'
group by decile
;
*/
-----------------------------------------------------------------------------------------------------------------------------------------------------

-- PART B: Viewing Data (For Customers active over 13th to 19th August 2012)


CREATE variable @snapshot_start_dt DATE;
CREATE variable @snapshot_end_dt DATE ;

CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_date_counter      datetime;
CREATE VARIABLE @dt                    char(8);

-- Scaling Variables
Create variable @target_date            date;
Create variable @sky_total              numeric(28,20);
Create variable @Sample_total           numeric(28,20);
Create variable @weightings_total       numeric(28,20);
Create variable @scaling_factor         numeric(28,20);

SET @snapshot_start_dt = '2012-08-15' ;
SET @snapshot_end_dt = '2012-08-19' ;


---------------------------------------------------------------------------------------------------------------------------------------------------------
----B0 Create Table
---------------------------------------------------------------------------------------------------------------------------------------------------------

IF object_ID ('V083_Viewing') IS NOT NULL THEN
            DROP TABLE V083_Viewing
END IF;

CREATE TABLE V083_Viewing

    ( cb_row_ID                                    bigint       not null --primary key
            ,Account_Number                        varchar(20)  not null
            ,viewing_starts                        datetime
            ,viewing_stops                         datetime
            ,viewing_Duration                      decimal(10,0)
            ,timeshifting                          varchar(4)
            ,PK_viewing_prog_instance_fact         bigint       not null
            ,programme_instance_name               varchar(50)
            ,Programme_instance_Duration           decimal(10,0)
            ,broadcast_Time_Of_Day                 varchar(15)
            ,spot_standard_daypart_UK              varchar(15)
            ,Channel_Name                          varchar(30)
            ,epg_group_Name                        varchar(30)
            ,service_key                           int
            ,Genre_Description                     varchar(30)
            ,Sub_Genre_Description                 varchar(30)
            ,network_indicator                     varchar(50)
            ,prog_date                             date
)
;

COMMIT;





GRANT SELECT ON V083_Viewing TO PUBLIC;

--------------------------------------------------------------------------------------------------------------------------------------------------
---- B1: Get programme data from sk_prod.VESPA_EVENTS_VIEWED_ALL
--------------------------------------------------------------------------------------------------------------------------------------------------

IF object_id('V083_Viewing_detail') IS NOT NULL DROP TABLE V083_Viewing_detail;

SELECT
            account_number
            ,PK_viewing_prog_instance_fact
            ,programme_instance_name
            ,Programme_instance_Duration
            ,broadcast_Time_Of_Day
            ,spot_standard_daypart_UK
            ,Channel_Name
            ,epg_group_Name
            ,service_key
            ,Genre_Description
            ,Sub_Genre_Description
            ,network_indicator
            ,date(broadcast_start_date_time_utc)
INTO  V083_Viewing_detail
FROM sk_prod.VESPA_EVENTS_VIEWED_ALL
WHERE (broadcast_start_date_time_utc between @snapshot_start_dt  and  @snapshot_end_dt)
        and reported_playback_speed is null  --LIVE viewing events only
        and Panel_id = 12
;--165309959 Row(s) affected
--66mins

create hg index idx2 on V083_Viewing_detail(account_number);

create index tk_tst_idx1 on V083_Viewing_detail (pk_viewing_prog_instance_fact);

GRANT SELECT ON V083_Viewing_detail TO PUBLIC;

--select top 10 * from V083_Viewing_detail;

/*
select *
INTO V083_Viewing_detail
from kinnairt.V083_Viewing_detail_2;
*/
--------------------------------------------------------------------------------------------------------------------------------------------------
---- B2: Get viewing data from vespa_analysts.Vespa_daily_augs_##^^*^*##
--------------------------------------------------------------------------------------------------------------------------------------------------

-- Populate the table
SET @var_sql = '
    insert into V083_Viewing
    select
            vie.cb_row_ID
            ,vie.Account_Number
            ,vie.viewing_starts
            ,vie.viewing_stops
            ,vie.viewing_Duration
            ,vie.timeshifting
            ,deet.PK_viewing_prog_instance_fact
            ,deet.programme_instance_name
            ,deet.Programme_instance_Duration
            ,deet.broadcast_Time_Of_Day
            ,deet.spot_standard_daypart_UK
            ,deet.Channel_Name
            ,deet.epg_group_Name
            ,deet.service_key
            ,deet.Genre_Description
            ,deet.Sub_Genre_Description
            ,deet.network_indicator
            ,@var_date_counter
     from vespa_analysts.Vespa_daily_augs_##^^*^*## as vie
        inner join V083_Viewing_detail as deet
        on vie.cb_row_id = deet.pk_viewing_prog_instance_fact
     '
     ;



-- loop through the time period to get all relevant viewing events
set @var_date_counter = @snapshot_start_dt;

  while @var_date_counter <= @snapshot_end_dt
  begin
      set @dt = left(@var_date_counter,4) || substr(@var_date_counter,6,2) || substr(@var_date_counter,9,2)
      EXECUTE(replace(@var_sql,'##^^*^*##',@dt))
      commit
      set @var_date_counter = dateadd(day, 1, @var_date_counter)
  end;

--25mins

CREATE HG INDEX idx_account_number_hg ON V083_Viewing(account_number);
create index tk_tst_idx1 on V083_Viewing (pk_viewing_prog_instance_fact);

---------------------------------------------------------------Dedupe cb_row_id until permanent fix in place
--create table

create table viewing_capped_dupes
(pk_viewing_prog_instance_fact bigint
,daily_table_date date
,rank int);

--insert those duplicates values

insert into viewing_capped_dupes
select * from
(select pk_viewing_prog_instance_fact, prog_date,
rank () over (partition by pk_viewing_prog_instance_fact order by prog_date) rank
from v083_viewing) t
where rank > 1;
--1161445 Row(s) affected
commit;

--create indexes on viewing_capped_dupes table

create hg index idx1_viewing_capped_dupes on viewing_capped_dupes(pk_viewing_prog_instance_fact);
create lf index idx2_viewing_capped_dupes on viewing_capped_dupes(daily_table_date);

--create indexes on v083_viewing table if not already existing

create hg index idx1_TA_viewing_capped on v083_viewing(pk_viewing_prog_instance_fact);
create lf index idx2_TA_viewing_capped on v083_viewing(prog_date);

--delete duplicates from viewing table

delete from v083_viewing
from v083_viewing a, viewing_capped_dupes b
where a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact
and a.prog_date = b.daily_table_date;

commit;
--1161445 Row(s) affected
--check that no duplicate pk_ids in the table

select count(1) from v083_viewing
union all
select count(distinct pk_viewing_prog_instance_fact) from v083_viewing

--drop table


drop table viewing_capped_dupes;

---------------------------------------------------------------------------------------------------checks




--select top 10 * from sk_prod.VESPA_STB_PROG_EVENTS_20120510
--select top 10 * from sk_prod.vespa_programme_schedule
select top 10 * from sk_prod.VESPA_EVENTS_VIEWED_ALL
select top 100 * from vespa_analysts.vespa_daily_augs_20120813
order by account_number, viewing_starts, viewing_stops

select count(*) from v083_viewing;


--------------------------------------------------------------------------------------------------------------------------------------------------
---- C0: Create base table for Universe including target audience as agreed with Sky ATL media planning team
--------------------------------------------------------------------------------------------------------------------------------------------------
/*
Target audience =       Segment: Likely to upgrade
                        Segment: A+ / SkyGo user with last 3 months

De-dupe accounts
*/



SELECT
             account_number
             ,BB_Sky
             ,Area_Onnet
             ,Area_Cable
             ,Area_Fibre
             ,Area_Sky_Fibre
             ,Likely_upgrade
             ,Anytime_activated
             ,Anytime_user_3m
             ,Sky_Go_registered
             ,Sky_Go_user_3m
             ,Region
             ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
             ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
             ,case      when (Area_Onnet = 0)                                   then 'Offnet'
                        when (BB_sky = 1)                                       then 'Sky_BB'
                        when (U_likely_upgraders = 1)                           then 'Likely upgraders'
                        when (U_anytime_go = 1)                                 then 'A+ SkyGo'
                        when (U_likely_upgraders = 0)                           then 'No potential'
                        else 'error' end as universes
INTO #tb
FROM chans.V083_base;
--10012868 Row(s) affected

select distinct(account_number)
             ,BB_Sky
             ,Area_Onnet
             ,Area_Cable
             ,Area_Fibre
             ,Area_Sky_Fibre
             ,Likely_upgrade
             ,Anytime_activated
             ,Anytime_user_3m
             ,Sky_Go_registered
             ,Sky_Go_user_3m
             ,Region
             ,case      when (universes = 'Offnet' or universes = 'Sky_BB' or universes = 'No potential')         then 'WASTAGE'
                        when (universes = 'Likely upgraders' or universes = 'A+ SkyGo')                           then 'TARGET'
                                                                                                                  else 'error'
             end as universes
INTO V083_target_base
FROM #Tb;
--9999612 Row(s) affected




--------------------------------------------------------------checks
select count(account_number) from v083_target_base
union all
select count(distinct account_number) from v083_target_base

select count(account_number) as vol, account_number from v083_target_base
group by account_number
order by vol desc;

--delete wierd account, it's not in VESPA Panel anyway
delete from V083_target_base
where account_number = '621021029205';

--check splits match results from sizing work
select count(distinct(account_number)), universes from V083_target_base
group by universes
;

--------------------------------------------------------------------------------------------------------------------------------------------------
---- C1: Add DM opt in
--------------------------------------------------------------------------------------------------------------------------------------------------
SELECT  account_number
       ,CASE WHEN sav.cust_email_allowed             = 'Y' THEN 1 ELSE 0 END AS Email_Mkt_OptIn
       ,CASE WHEN sav.cust_postal_mail_allowed       = 'Y' THEN 1 ELSE 0 END AS Mail_Mkt_OptIn
       ,CASE WHEN sav.cust_telephone_contact_allowed = 'Y' THEN 1 ELSE 0 END AS Tel_Mkt_OptIn
       --,CASE WHEN sav.cust_sms_allowed               = 'Y' THEN 1 ELSE 0 END AS Txt_Mkt_OptIn  **Do not include as these are for service msg only
       ,CASE WHEN sav.cust_email_allowed             = 'Y'
               OR sav.cust_postal_mail_allowed       = 'Y'
               --OR sav.cust_sms_allowed               = 'Y'
               OR sav.cust_telephone_contact_allowed = 'Y'
             THEN 1
             ELSE 0
         END AS Any_Mkt_OptIn
  INTO #Opt_Ins
  FROM sk_prod.cust_single_account_view AS sav
;--24425281 Row(s) affected


select a.account_number
        ,max(ANY_Mkt_OptIn) as ANY_Mkt_OptIn
into #DMout
from #opt_ins a INNER JOIN V083_target_base b
        ON a.account_number = b.account_number
group by a.account_number
;
--9999603 Row(s) affected


ALTER TABLE V083_target_base
ADD DM_opt_in_ANY tinyint
;

UPDATE V083_target_base
SET     DM_opt_in_ANY  = Any_Mkt_OptIn
FROM V083_target_base a LEFT JOIN #DMout b
        on a.account_number = b.account_number
;
--9999612 Row(s) affected


--------------------------------------------------------------------------------------------------------------------------------------------------
---- C2: Purge accounts to VESPA PANEL and add viewing data
--------------------------------------------------------------------------------------------------------------------------------------------------

SELECT       a.account_number
             ,a.BB_Sky
             ,a.Area_Onnet
             ,a.Area_Cable
             ,a.Area_Fibre
             ,a.Area_Sky_Fibre
             ,a.Likely_upgrade
             ,a.Anytime_activated
             ,a.Anytime_user_3m
             ,a.Sky_Go_registered
             ,a.Sky_Go_user_3m
             ,a.Region
             ,a.universes
             ,a.DM_opt_in_ANY
             ,b.cb_row_ID
            ,b.viewing_starts
            ,b.viewing_stops
            ,b.viewing_Duration
            ,b.timeshifting
            ,b.PK_viewing_prog_instance_fact
            ,b.programme_instance_name
            ,b.Programme_instance_Duration
            ,b.broadcast_Time_Of_Day
            ,b.spot_standard_daypart_UK
            ,b.Channel_Name
            ,b.epg_group_Name
            ,b.service_key
            ,b.Genre_Description
            ,b.Sub_Genre_Description
            ,b.network_indicator
INTO V083_AllInOne
FROM v083_target_base a inner join V083_Viewing b
        ON a.account_number = b.account_number
;--103843520 Row(s) affected
--22mins

select top 100 * from V083_AllInOne
order by account_number


--drop table V083_AllInOne;
---------------------------------------------------------------------------CHECKS

--Only want viewing data of just those that started / ended between snapshot period

SELECT * FROM V083_AllInOne
WHERE (date(viewing_starts) < '20120813' OR date(viewing_stops) > '20120819');
--no records, so it's OK

select count(distinct(account_number))
--,universes
from V083_AllInOne
group by universes
;
/*
count(distinct(V083_AllInOne.account_number))   universes
53485   TARGET
521415  WASTAGE
*/

--------------------------------------------------------------------------------------------------------------------------------------------------
---- C3: Add Scaling Weights
--------------------------------------------------------------------------------------------------------------------------------------------------


-- Create a scaling base table for each target account on the VESPA panel
drop table V083_scaling







ALTER TABLE V083_allinone
ADD (
         weighting_date         date
         ,weightings            float default 0
         ,new_weight            float default 0
         ,scaling_segment_ID    bigint
);

-----------------------------------------------------get weights across all days to check everything is ok
--get the date to be weighted
UPDATE  V083_allinone
set weighting_date = date(viewing_starts)
;--103843520 Row(s) affected

--get the segmentation for the account at the time of viewing
UPDATE V083_allinone as bas
SET bas.scaling_segment_ID = wei.scaling_segment_ID
FROM vespa_analysts.SC2_intervals as wei
WHERE bas.account_number = wei.account_number
        and bas.weighting_date between cast(wei.reporting_starts as date) and cast(wei.reporting_ends as date)
;--103682171 Row(s) affected

-- Find out the weight for that segment on that day
UPDATE V083_allinone
     set weightings = wei.weighting
    from V083_allinone as bas INNER JOIN vespa_analysts.SC2_weightings as wei
                                        ON bas.weighting_date = wei.scaling_day
                                        and bas.scaling_segment_ID = wei.scaling_segment_ID
;
commit;
--103681473 Row(s) affected

------------------------------------------------------set weight as middle day - this is what is actually used for analysis



select account_number, weightings
into #tempweight
from V083_allinone
where date(viewing_starts) = '2012-08-16'  --middle day in time period analysed
group by account_number, weightings;
--513852 Row(s) affected

-- update table with new single day weights
UPDATE V083_allinone
SET  base.new_weight = si.weightings
FROM V083_allinone  AS base
     INNER JOIN #tempweight AS si
        ON base.account_number = si.account_number;
--100719442 Row(s) affected

/*
-- check total weights add up to total

select distinct(account_number),new_weight
into #cha
from V083_allinone;

select sum(new_weight)
from #cha;
--8852090 records  --yep this matches the scaled up VESPA population: GB only, who have opted-in

*/



/*   ------------------------------------------------------------------------------------------checking
select top 100 * from vespa_analysts.SC2_intervals
select top 100 * from vespa_analysts.SC2_weightings
select top 100 * from V083_scaling
where scaling_segment_ID is null
order by account_number, weighting_date


grant select on chans.V083_AllInOne to public

select * from chans.V083_AllInOne
where account_number = '200001574793'
order by weighting_date;

select * from vespa_analysts.SC2_intervals
where account_number = '200001574793'


select count(distinct(account_number)) from V083_allinone
where scaling_segment_ID is null;
--9756

select count(*) from V083_allinone
where scaling_segment_ID is null;
--161349

select count(distinct(account_number)) from V083_allinone
--574900

*/

---  incorporate the BARB defintion of using middle weight for the period
-- round up if needed

--drop table #tempweight;



/*(
select count(*) from V083_allinone
where (scaling_segment_ID is null AND new_weight is not null);
--161349

select count(*) from V083_allinone
where (new_weight = 0);
--3427078

select count(distinct(account_number)) from V083_allinone
where (new_weight = 0);
--63056
*/










--------------------------------------------------------------------------------------------------------------------------------------------------
---- C4: Add media pack / sales house
--------------------------------------------------------------------------------------------------------------------------------------------------

Alter table V083_AllInOne
Add     (Media_pack varchar (20)
        ,SalesHouse varchar (20)
        );


-----------------------------Map using SARE and service key

select  ska.service_key as service_key,
        ska.full_name,
        cgroup.primary_sales_house,
        (case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
INTO #packs
FROM  patelj.channel_map_DEV_service_key_attributes ska
LEFT JOIN
        (SELECT a.service_key, b.name
         FROM patelj.channel_map_DEV_service_key_landmark a
                join patelj.channel_map_landmark_channel_pack_lookup b
                        on a.sare_no between b.sare_no and b.sare_no + 999
        where a.service_key <> 0
         ) pack
        ON ska.service_key = pack.service_key
LEFT JOIN
        (SELECT DISTINCT a.service_key,
                         b.primary_sales_house,
                         b.channel_group
        FROM patelj.channel_map_dev_service_key_barb a
        JOIN patelj.channel_map_barb_channel_group b
           ON  a.log_station_code = b.log_station_code
             AND a.sti_code = b.sti_code
WHERE a.service_key <>0) cgroup
        on ska.service_key = cgroup.service_key
WHERE cgroup.primary_sales_house is not null
ORDER BY cgroup.primary_sales_house, channel_category
;--513 Row(s) affected

/* ---checking
select * from #packs
order by service_key
*/


-----Add to final table
Update V083_AllInOne bas
SET Media_pack = channel_category
FROM #packs p
WHERE bas.service_key = p.service_key
;--99385022 Row(s) affected

Update V083_AllInOne bas
SET SalesHouse = primary_sales_house
FROM #packs p
WHERE bas.service_key = p.service_key
;--99385022 Row(s) affected


/*
select distinct(service_key), channel_name from v083_allinone
where media_pack is null ;

select * from patelj.channel_map_DEV_service_key_landmark
where service_key = '1309' or service_key = '1319'  - these are sky sports 1 and sky spts news in viewing data
--CHECKED with Martin Neighbours: OK to leave as they are without media pack as these are EIRE channels
*/

--------------------------------------------------------------------------------------------------------------------------------------------------
---- C5: Clean Data
--------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------- remove viewing records under the min cap of 6 seconds
/*
Manual fix until bug in viewing tables are fixed
*/

DELETE FROM V083_AllInOne
WHERE viewing_duration <6;--722530

SELECT count (*) from V083_AllInOne
where new_weight >0;
--99,716,667

SELECT count (distinct(account_number)) from V083_AllInOne
where new_weight >0;
--511,844

select count(*) from v083_allinone
where timeshifting <> 'LIVE';



--------------------------------------------------------------------------- Correct media pack anomolies

Update v083_allinone
set media_pack = (case when service_key = 3619 then 'ENTERTAINMENT' else media_pack end)
;--103120990 Row(s) affected

select
--top 10 *
distinct(channel_name), service_key, saleshouse, media_pack
from v083_allinone
where service_key = 3619;

Alter table v083_allinone
Add Media_Pack_fix varchar(20)
;

Update v083_allinone
set media_pack_fix = (case when media_pack = 'SKY ENTERTAINMENT' THEN 'ENTERTAINMENT' ELSE media_pack end)
;--103120990 Row(s) affected


GRANT ALL ON v083_allinone to starmerh;
------------------------------------------------------------------------------------------------------------------------------------------------------------
----Z0 OUTPUT
------------------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------------------------------
----Z1 OUTPUT: Sizing universes
------------------------------------------------------------------------------------------------------------------------------------------------------------

/*
CHECKING propensity model distribution of A+ & SkyGo users outside of top 4 deciles of likely upgraders, to infer how far they are from 'likely'
in order to decide treatment of group.

DECISION: keep A+ and skygo users outside of likely upgraders as traget audience because proportionately less in higher deciles and A+ not used as a
predictor in model
*/

--using activity within last 3 mths A+ & SkyGo
select count(distinct(bas.account_number))
,decile
FROM V083_Base bas
INNER JOIN #bb bb
        ON bas.account_number = bb.account_number
where
        area_onnet = 1
        and bb_sky = 0
        and likely_upgrade = 0
        and
        (Anytime_user_3m = 1
        or Sky_Go_user_3m = 1)
group by decile
;

select top 100 * from V083_Base

;

--using registerd A+ & SkyGo

select count(distinct(bas.account_number))
--,decile
FROM V083_Base bas
--INNER JOIN #bb bb        ON bas.account_number = bb.account_number
where
        area_onnet = 1
        and bb_sky = 0
        and likely_upgrade = 1
        and
        (Anytime_activated = 1
        and Sky_Go_registered = 1)
--group by decile
;


-------------------SIZING OUTPUT**********************************************************************************************

--using activity within last 3 mths A+ & SkyGo
Select  account_number
        ,Region
        ,area_onnet
        ,bb_sky
        ,likely_upgrade
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go__user_3m = 1))   then 1 else 0 end as U_anytime_go
into #uni
from V083_Base
;--9857219 Row(s) affected

Select  account_number
        ,Region
        ,area_onnet
        ,bb_sky
        ,likely_upgrade
        ,U_likely_upgraders
        ,U_anytime_go
        ,case   when (Area_Onnet = 0)                                   then 'Offnet'
                when (BB_sky = 1)                                       then 'Sky_BB'
                when (U_likely_upgraders = 1)                           then 'Likely upgraders'
                when (U_anytime_go = 1)                                 then 'A+ SkyGo'
                when (U_likely_upgraders = 0)                           then 'No potential'
                else 'error' end as universes
into #sizing
from #uni
;--9857219 Row(s) affected


select count(distinct(account_number)), universes from #sizing
group by universes
;

/*
count(distinct(#sizing.account_number)) universes
823716  A+ SkyGo
1474082 Likely upgraders
1879296 No potential
2319611 Offnet
3348098 Sky_BB
*/
------------------------------------------------------------------------------------------------------------------------------------------------------------
----Z1 OUTPUT: Household Overall populations
------------------------------------------------------------------------------------------------------------------------------------------------------------

--UNIVERSES

SELECT   account_number
        ,max(new_weight) as new_weight
        ,universes
INTO #all
FROM V083_AllInOne
WHERE viewing_duration >=180
        AND new_weight >0
GROUP BY account_number
        ,universe
;--511475 Row(s) affected

--drop table #all

SELECT
        sum(new_weight)
        ,universes
FROM #all
GROUP BY
        universes
        ;

-- LIKELIHOOD UNIVERSES --

SELECT   account_number
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
         ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
        ,max(new_weight)
INTO #all2
FROM CHANS.V083_AllInOne
WHERE viewing_duration >=180
        AND new_weight >0
GROUP BY account_number
        ,U_likely_upgraders
        ,U_anytime_go
        ,likelihood_universes
;

SELECT
        likelihood_universes
       ,sum(expression)

FROM #all2
GROUP BY
        likelihood_universes;



--DM opt in / out

SELECT  account_number
        ,max(new_weight)
        ,DM_opt_In_any
        ,universes
INTO #all
FROM V083_AllInOne
WHERE viewing_duration >=180
        AND new_weight >0
GROUP BY account_number
        ,DM_opt_In_any
        ,universes
;--511475 Row(s) affected

--drop table #all

SELECT
        sum(expression)
        ,DM_opt_In_any
        ,universes
FROM #all
GROUP BY
        DM_opt_In_any
        ,universes
        ;
-- DM OPT OUT BY LIKELIHOOD --

SELECT   account_number
         ,DM_opt_In_any
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
         ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
        ,max(new_weight)
INTO #all4
FROM CHANS.V083_AllInOne
WHERE viewing_duration >=180
        AND new_weight >0
GROUP BY account_number
         ,DM_opt_In_any
        ,U_likely_upgraders
        ,U_anytime_go
        ,likelihood_universes
;

SELECT
        likelihood_universes
       ,DM_opt_In_any
       ,sum(expression)

FROM #all4
GROUP BY
        likelihood_universes
        ,DM_opt_In_any;

--Region

SELECT  account_number
        ,max(new_weight)
        ,Region
INTO #all
FROM V083_AllInOne
WHERE viewing_duration >=180
        AND new_weight >0
GROUP BY account_number
        ,Region
;--511475 Row(s) affected

--drop table #all

SELECT
        sum(expression)
        ,Region
FROM #all
GROUP BY
        Region
        ;
-- REGION BY UNNIVERSE --

SELECT   account_number
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
         ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
        ,max(new_weight)
        ,Region
INTO #all3
FROM CHANS.V083_AllInOne
WHERE viewing_duration >=180
        AND new_weight >0
GROUP BY account_number
        ,Region
        ,U_likely_upgraders
        ,U_anytime_go
        ,likelihood_universes
;--511475 Row(s) affected

--drop table #all

SELECT
        Region
       ,likelihood_universes
       ,sum(expression)

FROM #all3
GROUP BY
        Region
        ,likelihood_universes;

------------------------------------------------------------------------------------------------------------------------------------------------------------
----Z3 OUTPUT: Household Reach
------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------ CREATE NEW UNIVERSE SPLIT

select    *
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
         ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
         ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
into #v083_all
from chans.V083_AllInOne ;

----------------------------------------------------HH viewed by media pack

----------------Overall Efficiencies
---- TARGET VERSES WASTAGE --
select  media_pack_fix
        ,account_number
        ,max(new_weight) as vol_weight
        ,universes
        ,region
        ,dm_opt_in_any
into #pack
from chans.V083_AllInOne
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by media_pack_fix
        ,account_number
        ,universes
        ,region
        ,dm_opt_in_any
;
--4721921 Row(s) affected

select  media_pack_fix
        ,sum(vol_weight) as vol_weight
        ,universes
        ,region
        ,dm_opt_in_any
into #pack2
from #pack
group by media_pack_fix
        ,universes
        ,region
        ,dm_opt_in_any
;--1240 Row(s) affected

--select  * from #pack2;
--drop table #pack


---- LIKELIHOOD VERSES WASTAGE --
select  media_pack_fix
        ,account_number
        ,max(new_weight) as vol_weight
        ,likelihood_universes
        ,region
        ,dm_opt_in_any
into #pack_likelihood
from #V083_All
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by media_pack_fix
        ,account_number
        ,likelihood_universes
        ,region
        ,dm_opt_in_any
;

select   media_pack_fix
        ,likelihood_universes
        ,region
        ,dm_opt_in_any
        ,sum(vol_weight) as vol_weight
into #pack_likelihood2
from #pack_likelihood
group by media_pack_fix
        ,likelihood_universes
        ,region
        ,dm_opt_in_any;

--select * from #pack_likelihood2;


----------------Waterfall Efficiencies

/*
create flag for each media pack reached by account number
*/
--select top 100 * from chans.V083_AllInOne where account_number in ('210039468447');

select account_number
                  ,max(new_weight) as vol_weight
                  ,universes
                  ,region
                  ,dm_opt_in_any
                  ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
                  ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
                  ,case when (Area_Onnet = 0)                                   then 'Wastage'
                        when (BB_sky = 1)                                       then 'Wastage'
                        when (U_likely_upgraders = 1)                           then 'Likely upgraders'
                        when (U_anytime_go = 1)                                 then 'A+ SkyGo'
                        when (U_likely_upgraders = 0)                           then 'Wastage'
                        else 'error'
                   end as likelihood_universes
into #media_pack1
from chans.V083_AllInOne
group by account_number,U_likely_upgraders,U_anytime_go,universes,likelihood_universes,region,dm_opt_in_any;




select account_number
       ,a.media_pack_fix
       ,max(a.viewing) as viewing
into #acc_media_pack
from
(select     account_number
           ,media_pack_fix
           ,case when viewing_duration<180 then 0
                 when viewing_duration>180 then 1
                 else 0
           end as viewing

 from      chans.V083_AllInOne) A
group by a.account_number,a.media_pack_fix;

--select count(*),count(distinct account_number) from #acc_media_pack;

--select top 100 * from  #acc_media_pack;

select     a.account_number
          ,a.vol_weight
          ,a.universes
          ,a.likelihood_universes
          ,a.region
          ,a.dm_opt_in_any
          ,d.viewing as c4_pack
          ,e.viewing as c4_digital_pack
          ,f.viewing as documentaries_pack
          ,g.viewing as entertainment_pack
          ,h.viewing as five_pack
          ,i.viewing as five_digital_pack
          ,j.viewing as itv_pack
          ,k.viewing as itv_digital_pack
          ,l.viewing as kids_pack
          ,m.viewing as life_culture_pack
          ,n.viewing as movies_pack
          ,o.viewing as music_pack
          ,q.viewing  as news_pack
          ,u.viewing  as sports_pack
          ,v.viewing as uktv_pack

into      #media_pack
from      #media_pack1 A
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('C4')) D
on         a.account_number=d.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('C4 Digital'))  e
on         a.account_number=e.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('DOCUMENTARIES')) f
on         a.account_number=f.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('ENTERTAINMENT')) g
on         a.account_number=g.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('FIVE')) h
on         a.account_number=h.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('FIVE Digital')) i
on         a.account_number=i.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('ITV')) j
on         a.account_number=j.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('ITV Digital')) k
on         a.account_number=k.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('KIDS')) l
on         a.account_number=l.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('LIFESTYLE & CULTURE ')) m
on         a.account_number=m.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('MOVIES')) n
on         a.account_number=n.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('MUSIC')) o
on         a.account_number=o.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('NEWS')) q
on         a.account_number=q.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('SPORTS')) u
on         a.account_number=u.account_number
left outer join
(select    account_number
          ,media_pack_fix
          ,viewing
 from     #acc_media_pack
 where     media_pack_fix in ('UKTV')) v
on         a.account_number=v.account_number;

select count(*),count(distinct account_number) from #media_pack;

select * from #media_pack;
---- CHECK COUNTS BY MEDIA PACK MATCH WHAT IS IN VO83_ALLINONE --

-- COUNTS FROM TRANSFORMED TABLE --
/*select     count(*)
          ,count(distinct account_number)
          ,sum(box_office_pack)
          ,sum(bbc_pack)
          ,sum(c4_pack)
          ,sum(c4_digital_pack)
          ,sum(documentaries_pack)
          ,sum(entertainment_pack)
          ,sum(five_pack)
          ,sum(five_digital_pack)
          ,sum(itv_pack)
          ,sum(itv_digital_pack)
          ,sum(kids_pack)
          ,sum(lifestyle_culture_pack)
          ,sum(movies_pack)
          ,sum(music_pack)
          ,sum(media_partners_pack)
          ,sum(news_pack)
          ,sum(other_pack)
          ,sum(other_wholly_owned_pack)
          ,sum(sky_entertainment_pack)
          ,sum(sports_pack)
          ,sum(uktv_pack)
from      #media_pack;

select top 100 * from #media_pack;
-- COUNTS FROM VO83_ALLINONE --

select     media_pack
          ,count(distinct account_number)
from       chans.V083_AllInOne
where      viewing_duration >=180
group by   media_pack;
*/



------------ region--
-- TARGET VERSUS WASTAGE --
-- overall - before waterfall --
select  media_pack_fix
        ,universes
        ,region
        ,sum(vol_weight) as vol_weight
from #pack
group by media_pack_fix
        ,universes
        ,region;
-- LIKELIHOOD TARGET VS WASTAGE --

select  media_pack_fix
        ,likelihood_universes
        ,region
        ,sum(vol_weight) as vol_weight
from #pack_likelihood
group by media_pack_fix
        ,likelihood_universes
        ,region;


------------- programme --

--- TARGET VS WASTAGE --
select   media_pack_fix
        ,programme_instance_name
        ,account_number
        ,max(new_weight) as vol_weight
        ,universes
into #pack_programme
from chans.V083_AllInOne
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by media_pack_fix
        ,programme_instance_name
        ,account_number
        ,universes
;

select   media_pack_fix
        ,programme_instance_name
        ,sum(vol_weight) as vol_weight
        ,universes
into #pack_programme2
from #pack_programme
group by media_pack_fix
        ,programme_instance_name
        ,universes;

select   A.media_pack_fix
        ,A.programme_instance_name
        ,A.target
        ,b.wastage
from
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as target
 from #pack_programme2
 where universes in ('TARGET') and vol_weight>100) A
left outer join
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as wastage
 from #pack_programme2
 where universes in ('WASTAGE') and vol_weight>100) B;

--- LIKELIHOOD TARGET VS WASTAGE --
select   media_pack_fix
        ,programme_instance_name
        ,account_number
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
        ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
        ,max(new_weight) as vol_weight
into #pack_programme_like
from chans.V083_AllInOne
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by media_pack_fix
        ,programme_instance_name
        ,account_number
        ,likelihood_universes,U_likely_upgraders,U_anytime_go
;

select   media_pack_fix
        ,programme_instance_name
        ,likelihood_universes
        ,sum(vol_weight) as vol_weight
into #pack_programme_like2
from #pack_programme_like
group by media_pack_fix
        ,programme_instance_name
        ,likelihood_universes;

--select top 1000 * from  #pack_programme_like2;

select   COALESCE(A.media_pack_fix,b.media_pack_fix,c.media_pack_fix) as media_pack_fix
        ,COALESCE(A.programme_instance_name,b.programme_instance_name,c.programme_instance_name) as programme_instance_name
        ,A.Likely_Upgraders
        ,b.SkyGo_plus
        ,c.wastage
from
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as Likely_Upgraders
 from #pack_programme_like2
 where likelihood_universes in ('Likely upgraders') and vol_weight>100) A
left outer join
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as skygo_plus
 from #pack_programme_like2
 where likelihood_universes in ('A+ SkyGo') and vol_weight>100) B
on a.media_pack_fix=b.media_pack_fix
and a.programme_instance_name=b.programme_instance_name
 left outer join
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as wastage
 from #pack_programme_like2
 where likelihood_universes in ('Wastage') and vol_weight>100) C
on a.media_pack_fix=c.media_pack_fix
and a.programme_instance_name=c.programme_instance_name;

-- GET A SUMMARY OF MEDIA PACK VIEWING BY ACCOUNT --

select account_number
       ,a.media_pack_fix
       ,max(a.viewing) as viewing
into #acc_media_pack
from
(select     account_number
           ,media_pack_fix
           ,case when viewing_duration<180 then 0
                 when viewing_duration>180 then 1
                else 0 end as viewing
 from      chans.V083_AllInOne) A
group by a.account_number,a.media_pack_fix;

--select count(*),count(distinct account_number) from #acc_media_pack;

--select top 100 * from  #acc_media_pack;


select     a.account_number
          ,a.vol_weight
          ,a.universes
          ,a.likelihood_universes
          ,a.dm_opt_in_any
          ,d.c4_pack
          ,e.c4_digital_pack
          ,f.documentaries_pack
          ,g.entertainment_pack
          ,h.five_pack
          ,i.five_digital_pack
          ,j.itv_pack
          ,k.itv_digital_pack
          ,l.kids_pack
          ,m.life_culture_pack
          ,n.movies_pack
          ,o.music_pack
          ,q.news_pack
          ,u.sports_pack
          ,v.uktv_pack

into      #media_pack
from       (select account_number
                  ,max(new_weight) as vol_weight
                  ,universes
                  ,dm_opt_in_any
                  ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
                  ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
                  ,case when (Area_Onnet = 0)                                   then 'Wastage'
                        when (BB_sky = 1)                                       then 'Wastage'
                        when (U_likely_upgraders = 1)                           then 'Likely upgraders'
                        when (U_anytime_go = 1)                                 then 'A+ SkyGo'
                        when (U_likely_upgraders = 0)                           then 'Wastage'
                        else 'error'
                   end as likelihood_universes
            from chans.V083_AllInOne
            group by account_number,U_likely_upgraders,U_anytime_go,universes,likelihood_universes,dm_opt_in_any ) A
left outer join
(select    account_number
          ,viewing as box_office_pack
 from      #acc_media_pack
 where     media_pack_fix in ('BOX OFFICE')
) B
on         a.account_number=b.account_number
left outer join
(select    account_number
          ,viewing as bbc_pack
 from      #acc_media_pack
 where     media_pack_fix in ('BBC')
) C
on         a.account_number=c.account_number
left outer join
(select    account_number
          ,viewing as c4_pack
 from      #acc_media_pack
 where     media_pack_fix in ('C4')
) D
on         a.account_number=d.account_number
left outer join
(select    account_number
          ,viewing as c4_digital_pack
 from      #acc_media_pack
 where     media_pack_fix in ('C4 Digital')
) e
on         a.account_number=e.account_number
left outer join
(select    account_number
          ,viewing as documentaries_pack
 from      #acc_media_pack
 where     media_pack_fix in ('DOCUMENTARIES')
) f
on         a.account_number=f.account_number
left outer join
(select    account_number
          ,viewing as entertainment_pack
 from      #acc_media_pack
 where     media_pack_fix in ('ENTERTAINMENT')
) g
on         a.account_number=g.account_number
left outer join
(select    account_number
          ,viewing as five_pack
 from      #acc_media_pack
 where     media_pack_fix in ('FIVE')
) h
on         a.account_number=h.account_number
left outer join
(select    account_number
          ,viewing as five_digital_pack
 from      #acc_media_pack
 where     media_pack_fix in ('FIVE Digital')
) i
on         a.account_number=i.account_number
left outer join
(select    account_number
          ,viewing as itv_pack
 from      #acc_media_pack
 where     media_pack_fix in ('ITV')
) j
on         a.account_number=j.account_number
left outer join
(select    account_number
          ,viewing as itv_digital_pack
 from      #acc_media_pack
 where     media_pack_fix in ('ITV Digital')
) k
on         a.account_number=k.account_number
left outer join
(select    account_number
          ,viewing as kids_pack
 from      #acc_media_pack
 where     media_pack_fix in ('KIDS')
) l
on         a.account_number=l.account_number
left outer join
(select    account_number
          ,viewing as life_culture_pack
 from      #acc_media_pack
 where     media_pack_fix in ('LIFESTYLE & CULTURE ')
) m
on         a.account_number=m.account_number
left outer join
(select    account_number
          ,viewing as movies_pack
 from      #acc_media_pack
 where     media_pack_fix in ('MOVIES')
) n
on         a.account_number=n.account_number
left outer join
(select    account_number
          ,viewing as music_pack
 from      #acc_media_pack
 where     media_pack_fix in ('MUSIC')
) o
on         a.account_number=o.account_number
left outer join
(select    account_number
          ,viewing as media_partners_pack
 from      #acc_media_pack
 where     media_pack_fix in ('Media Partners')
) p
on         a.account_number=p.account_number
left outer join
(select    account_number
          ,viewing as news_pack
 from      #acc_media_pack
 where     media_pack_fix in ('NEWS')
) q
on         a.account_number=q.account_number
left outer join
(select    account_number
          ,viewing as other_pack
 from      #acc_media_pack
 where     media_pack_fix in ('Other')
) r
on         a.account_number=r.account_number
left outer join
(select    account_number
          ,viewing as other_wo_pack
 from      #acc_media_pack
 where     media_pack_fix in ('Other wholly-owned')
) s
on         a.account_number=s.account_number
left outer join
(select    account_number
          ,viewing as sports_pack
 from      #acc_media_pack
 where     media_pack_fix in ('SPORTS')
) u
on         a.account_number=u.account_number
left outer join
(select    account_number
          ,viewing as uktv_pack
 from      #acc_media_pack
 where     media_pack_fix in ('UKTV')
) v
on         a.account_number=v.account_number;
select count(*),count(distinct account_number) from #media_pack;
select * from #media_pack;
---- CHECK COUNTS BY MEDIA PACK MATCH WHAT IS IN VO83_ALLINONE --

-- COUNTS FROM TRANSFORMED TABLE --
/*select     count(*)
          ,count(distinct account_number)
          ,sum(box_office_pack)
          ,sum(bbc_pack)
          ,sum(c4_pack)
          ,sum(c4_digital_pack)
          ,sum(documentaries_pack)
          ,sum(entertainment_pack)
          ,sum(five_pack)
          ,sum(five_digital_pack)
          ,sum(itv_pack)
          ,sum(itv_digital_pack)
          ,sum(kids_pack)
          ,sum(lifestyle_culture_pack)
          ,sum(movies_pack)
          ,sum(music_pack)
          ,sum(media_partners_pack)
          ,sum(news_pack)
          ,sum(other_pack)
          ,sum(other_wholly_owned_pack)
          ,sum(sky_entertainment_pack)
          ,sum(sports_pack)
          ,sum(uktv_pack)
from      #media_pack;

select top 100 * from #media_pack;
-- COUNTS FROM VO83_ALLINONE --

select     media_pack
          ,count(distinct account_number)
from       chans.V083_AllInOne
where      viewing_duration >=180
group by   media_pack;
*/



------------ region--
-- TARGET VERSUS WASTAGE --
-- overall - before waterfall --
select  media_pack_fix
        ,universes
        ,region
        ,sum(vol_weight) as vol_weight
from #pack
group by media_pack_fix
        ,universes
        ,region;
-- LIKELIHOOD TARGET VS WASTAGE --

select  media_pack_fix
        ,likelihood_universes
        ,region
        ,sum(vol_weight) as vol_weight
from #pack_likelihood
group by media_pack_fix
        ,likelihood_universes
        ,region;


------------- programme --

--- TARGET VS WASTAGE --
select   media_pack_fix
        ,programme_instance_name
        ,account_number
        ,max(new_weight) as vol_weight
        ,universes
into #pack_programme
from chans.V083_AllInOne
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by media_pack_fix
        ,programme_instance_name
        ,account_number
        ,universes
;

select   media_pack_fix
        ,programme_instance_name
        ,sum(vol_weight) as vol_weight
        ,universes
into #pack_programme2
from #pack_programme
group by media_pack_fix
        ,programme_instance_name
        ,universes;

select   A.media_pack_fix
        ,A.programme_instance_name
        ,A.target
        ,b.wastage
from
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as target
 from #pack_programme2
 where universes in ('TARGET') and vol_weight>100) A
left outer join
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as wastage
 from #pack_programme2
 where universes in ('WASTAGE') and vol_weight>100) B;

--- LIKELIHOOD TARGET VS WASTAGE --
select   media_pack_fix
        ,programme_instance_name
        ,account_number
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
        ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
        ,max(new_weight) as vol_weight
into #pack_programme_like
from chans.V083_AllInOne
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by media_pack_fix
        ,programme_instance_name
        ,account_number
        ,likelihood_universes,U_likely_upgraders,U_anytime_go
;

select   media_pack_fix
        ,programme_instance_name
        ,likelihood_universes
        ,sum(vol_weight) as vol_weight
into #pack_programme_like2
from #pack_programme_like
group by media_pack_fix
        ,programme_instance_name
        ,likelihood_universes;

--select top 1000 * from  #pack_programme_like2;

select   COALESCE(A.media_pack_fix,b.media_pack_fix,c.media_pack_fix) as media_pack_fix
        ,COALESCE(A.programme_instance_name,b.programme_instance_name,c.programme_instance_name) as programme_instance_name
        ,A.Likely_Upgraders
        ,b.SkyGo_plus
        ,c.wastage
from
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as Likely_Upgraders
 from #pack_programme_like2
 where likelihood_universes in ('Likely upgraders') and vol_weight>100) A
left outer join
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as skygo_plus
 from #pack_programme_like2
 where likelihood_universes in ('A+ SkyGo') and vol_weight>100) B
on a.media_pack_fix=b.media_pack_fix
and a.programme_instance_name=b.programme_instance_name
 left outer join
(select  media_pack_fix
        ,programme_instance_name
        ,vol_weight as wastage
 from #pack_programme_like2
 where likelihood_universes in ('Wastage') and vol_weight>100) C
on a.media_pack_fix=c.media_pack_fix
and a.programme_instance_name=c.programme_instance_name;



----------------------------------------------------HH viewed by channel

----aggregate channels ( HD, +1)

select
        case when right(channel_name,2) = 'HD' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,2) = '+1' THEN LEFT(channel_name,(LEN(channel_name)-2))
             when right(channel_name,1) = '+' THEN LEFT(channel_name,(LEN(channel_name)-1))
                                                ELSE channel_name END AS Channel
        ,channel_name
INTO #channel
FROM chans.V083_AllInOne
group by channel_name
;--528 Row(s) affected

SELECT RTRIM(channel) as Channel
        ,channel_name
INTO    #channel2
FROM    #channel
;--528 Row(s) affected



/*
SELECT
        case when channel = 'BBC ONE'     THEN 'BBC ONE HD'
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
                                                    ELSE channel END AS Channel
        ,channel_name
INTO V083_LkUpChannel
FROM #channel2
group by channel_name, channel
order by channel*/
;--528 Row(s) affected


select *,
       case when channel_name in ('ITV1 Central SW','ITV1 Border','ITV1 West'
                                  ,'ITV1 Central S', 'ITV1 Central E' ,'ITV1 Anglia E'
                                  ,'ITV1 Wales','ITV1 Yorks W','ITV1 Anglia W'
                                  ,'ITV1 Meridian S','ITV1 Yorks E','ITV1 HD',
                                  'ITV1 Mer SE','ITV1 London','ITV1+1','ITV1 Central W',
                                  'ITV Channel Is','ITV1 Tyne Tees','ITV1 Mer N','ITV1 Granada','ITV1 W Country')
            then channel_name
            else channel
       end as channel1

into #channel_fix
from  chans.V083_LkUpChannel
;
--select * from #channel_fix;

--select * from chans.V083_LkUpChannel;

---- HH viewing
select  channel
        ,media_pack_fix
        ,account_number
        ,max(new_weight) as vol_weight
        ,universes
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND likely_upgrade = 1) then 1 else 0 end as U_likely_upgraders
        ,case when (BB_sky = 0 AND Area_Onnet = 1 AND (Anytime_user_3m = 1 OR Sky_Go_user_3m = 1))   then 1 else 0 end as U_anytime_go
         ,case when (Area_Onnet = 0)                                   then 'Wastage'
               when (BB_sky = 1)                                       then 'Wastage'
               when (U_likely_upgraders = 1)                           then 'Likely upgraders'
               when (U_anytime_go = 1)                                 then 'A+ SkyGo'
               when (U_likely_upgraders = 0)                           then 'Wastage'
               else 'error'
          end as likelihood_universes
        ,region
        ,dm_opt_in_any
into #chan
from  chans.V083_AllInOne one INNER JOIN #channel_fix lkup
        ON one.channel_name = lkup.channel1
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by channel
        ,media_pack_fix
        ,account_number
        ,universes
        ,U_likely_upgraders
        ,U_anytime_go
        ,likelihood_universes
        ,region
        ,dm_opt_in_any
;--5075029 Row(s) affected
--select * from chans.V083_AllInOne where channel_name in ('ITV Channel Is','ITV1');
--select * from #chan where epg_group_name in ('ITV1 & Regions Only') and channel not in ('ITV Channel Is','UTV','STV');
;
-- TARGET VS WASTAGE --
select  channel
        ,media_pack_fix
        ,sum(vol_weight) as vol_weight
        ,universes
        ,region
        ,dm_opt_in_any
into #chan2
from #chan
WHERE ((media_pack_fix is not null) OR (media_pack_fix <> 'Other')  OR (media_pack_fix <> 'Other wholly-owned') OR (media_pack_fix <> 'BBC')
OR  (media_pack_fix <> 'Media Partners'))
group by channel
        ,media_pack_fix
        ,universes
        ,region
        ,dm_opt_in_any
;--10705 Row(s) affected

--drop table #chan2

select  * from #chan2;

-- LIKELOHOOD TARGET VS WASTAGE --
select  channel
        ,media_pack_fix
        ,sum(vol_weight) as vol_weight
        ,likelihood_universes
        ,region
        ,dm_opt_in_any
into #chan3
from #chan
WHERE ((media_pack_fix is not null) OR (media_pack_fix <> 'Other')  OR (media_pack_fix <> 'Other wholly-owned') OR (media_pack_fix <> 'BBC')
OR  (media_pack_fix <> 'Media Partners'))
group by channel
        ,media_pack_fix
        ,likelihood_universes
        ,region
        ,dm_opt_in_any
;--10705 Row(s) affected

--drop table #chan3

select  * from #chan3;


----------------------------------------------------HH viewed by Sales House

select  saleshouse
        ,account_number
        ,max(new_weight) as vol_weight
        ,universes
        ,region
        ,dm_opt_in_any
into #sale
from v083_allinone
WHERE viewing_duration >=180  --only count as viewed where event prog view duration is at least 3 mins
group by saleshouse
        ,account_number
        ,universes
        ,region
        ,dm_opt_in_any
;--2896700 Row(s) affected

select  saleshouse
        ,sum(vol_weight) as vol_weight
        ,universes
        ,region
        ,dm_opt_in_any
into #sale2
from #sale
group by saleshouse
        ,universes
        ,region
        ,dm_opt_in_any
;--420 Row(s) affected

select  * from #sale2;
--drop table #sale2








--------------------------------------------------------------------------------------------phase 2 data checks
--CHECK on duplicate cb_row_id reveals duplicates are present
select Top 10 cb_row_id
from rombaoad.V98_CappedViewing
group by cb_row_id
having count(*) > 1;


--Sample of cb_row_id duplicate
select *
FROM rombaoad.V98_CappedViewing
WHERE cb_row_id=10504567781;

select * --account_number(10504567781)
from sk_prod.VESPA_EVENTS_VIEWED_ALL
where event_start_date_time_utc between '2012-08-13 00:00:00.000000' and '2012-08-13 23:59:00.000000'
and account_number = '10504567781'
;

select * from sk_prod.VESPA_EVENTS_VIEWED_ALL
where pk_viewing_prog_instance_fact = 10504567781

select a.*, '2012-08-13' dt from vespa_analysts.vespa_daily_augs_20120813 a
where cb_row_id = 10504567781
union
select  a.*, '2012-08-14' dt from vespa_analysts.vespa_daily_augs_20120814 a
where cb_row_id = 10504567781
