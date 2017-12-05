
----V183 - Evaluation of current weighting with regards to factors that could influence churn----

----Get Account status at 1st Feb 2013  and 1st May 2013
select
                csh.account_number
                ,max( case when effective_from_dt< '2013-02-01' and effective_to_dt>='2013-02-01' 
                then status_code else null end) as status_at_20130201
                ,max( case when  effective_from_dt< '2013-05-01' and effective_to_dt>='2013-05-01'
                then status_code else null end) as status_at_20130501             
into            v183_all_active_accounts
FROM            sk_prod.cust_subs_hist csh
WHERE          csh.subscription_sub_type = 'DTV Primary Viewing'
group by csh.account_number
;
Commit;

---Only include accounts AC/AB/PC as at 1st Feb

delete from v183_all_active_accounts where status_at_20130201  not in ('AC','AB','PC');
delete from v183_all_active_accounts where status_at_20130201  is null;

commit;

create hg index idx1 on v183_all_active_accounts(account_number);
commit;

---Get Account Attributes as at 1st Feb 2013----
select a.account_number
,b.weighting
into #account_weight_20130201
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2013-02-01' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2013-02-01' as date) between a.reporting_starts and a.reporting_ends
;
create  hg index idx1 on #account_weight_20130201(account_number);
commit;

alter table  v183_all_active_accounts add vespa_weight_20130201 double;

update v183_all_active_accounts
set  vespa_weight_20130201 =b.weighting
from v183_all_active_accounts  as a
left outer join #account_weight_20130201 as b
on a.account_number = b.account_number
;
commit;


----get All those with Weights as at 1st Feb 2013----



alter table  v183_all_active_accounts add country_code varchar(3);
update v183_all_active_accounts
set  country_code =b.pty_country_code
from v183_all_active_accounts  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

alter table  v183_all_active_accounts add acct_type varchar(10);
update v183_all_active_accounts
set  acct_type =b.acct_type
from v183_all_active_accounts  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

Delete from  v183_all_active_accounts where acct_type<>'Standard'; commit;
Delete from  v183_all_active_accounts where country_code not in ('GBR','IRL'); commit;
Delete from  v183_all_active_accounts where country_code is null; commit;

alter table  v183_all_active_accounts add activation_date date;
update v183_all_active_accounts
set  activation_date =b.ph_subs_first_activation_dt
from v183_all_active_accounts  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;


-------------------------Update Account Attributes-------------




--Create Package Details for actual date of analysis (1st Feb 2013)


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
   AND csh.effective_from_dt <= '2013-01-31'
   AND csh.effective_to_dt   >  '2013-01-31'
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
alter table v183_all_active_accounts add prem_sports integer default 0;
alter table v183_all_active_accounts add prem_movies integer default 0;
alter table v183_all_active_accounts add mixes_type varchar(30) default 'Unknown';

update v183_all_active_accounts 
set prem_sports=b.prem_sports
,prem_movies=b.prem_movies
,mixes_type=b.new_package
from v183_all_active_accounts  as a
left outer join #mixes as b
on a.account_number=b.account_number
;
commit;


select account_number
into #accounts_with_3d  
FROM sk_prod.cust_subs_hist as csh
      
 WHERE subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2013-01-31'
   AND csh.effective_to_dt   >  '2013-01-31'
group by account_number
;
commit;
exec sp_create_tmp_table_idx '#accounts_with_3d', 'account_number';

alter table v183_all_active_accounts add subscription_3d integer default 0;

update v183_all_active_accounts
set subscription_3d=case when b.account_number is not null then 1 else 0 end
from v183_all_active_accounts as a
left outer join #accounts_with_3d  as b
on a.account_number = b.account_number
;

-------------------------------------------------  02 - Active MR AND HD Subscription
--code_location_08
SELECT  csh.account_number
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS multiroom
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS hdtv
           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS skyplus
INTO v141_MR_HD
      FROM sk_prod.cust_subs_hist AS csh 
     WHERE csh.subscription_sub_type  IN ('DTV Extra Subscription'
                                         ,'DTV HD'
                                         ,'DTV Sky+')
       AND csh.effective_from_dt <> csh.effective_to_dt
       AND csh.effective_from_dt <= '2013-01-31'
       AND csh.effective_to_dt    >  '2013-01-31'
GROUP BY csh.account_number;
commit;

commit;
create  hg index idx1 on v141_MR_HD (account_number);
alter table v183_all_active_accounts add hdtv                    tinyint          default 0    ;     
alter table v183_all_active_accounts add multiroom                    tinyint          default 0    ;     
alter table v183_all_active_accounts add skyplus                    tinyint          default 0    ;     
commit;


update v183_all_active_accounts
set hdtv=b.hdtv
,multiroom=b.multiroom
,skyplus=b.skyplus
from v183_all_active_accounts as a
left outer join v141_MR_HD as b
on a.account_number=b.account_number
;
commit;
drop table v141_MR_HD;
commit;

commit;
--select top 100 * from v141_live_playback_viewing;

----Add on extra variables from product holdings and consumerview---

alter table v183_all_active_accounts add talk_product              VARCHAR(50)     default 'NA' ;        -- Current Sky Talk product
--alter table v183_all_active_accounts add sky_id                    bigint          default 0    ;        -- Sky id created
alter table v183_all_active_accounts add distinct_usage_days                INTEGER         default 0     ;       -- Sky Go days in 3mth period
alter table v183_all_active_accounts add usage_records                INTEGER         default 0     ;       -- Sky Go usage records in 3mth period
alter table v183_all_active_accounts add BB_type                   VARCHAR(50)     default 'NA'  ;       -- Current BB product
alter table v183_all_active_accounts add Anytime_plus              INTEGER         default 0    ;        -- Anytime+ activated
alter table v183_all_active_accounts add isba_tv_region             VARCHAR(50)     default 'Unknown'         ;   
alter table v183_all_active_accounts add cb_key_household           bigint   ;        -- Current Sky Talk product
--drop table nodupes;
commit;

update v183_all_active_accounts
set isba_tv_region=b.isba_tv_region
,cb_key_household=b.cb_key_household
from v183_all_active_accounts as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;
--select top 100 * from v183_all_active_accounts;
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
     AND csh.effective_from_dt <= '2013-01-31'
     AND csh.effective_to_dt > '2013-01-31'
GROUP BY base.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update AdSmart file
UPDATE v183_all_active_accounts
SET  talk_product = talk.talk_product
FROM v183_all_active_accounts  AS Base
  INNER JOIN talk AS talk
        ON base.account_number = talk.account_number
ORDER BY base.account_number;
commit;

DROP TABLE talk;
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
select account_number
        ,count(distinct cb_data_date) as distinct_usage_days
        ,count(*) as usage_records
--        ,sum(SKY_GO_USAGE)
into skygo_usage
from sk_prod.SKY_PLAYER_USAGE_DETAIL AS usage
--        inner join v183_all_active_accounts AS Base
--         ON usage.account_number = Base.account_number
where cb_data_date >= '2012-11-01'
        AND cb_data_date <'2013-01-31'
group by account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update AdSmart file
UPDATE v183_all_active_accounts
SET distinct_usage_days = sky_go.distinct_usage_days
,usage_records=sky_go.usage_records
FROM v183_all_active_accounts  AS Base
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
    inner join v183_all_active_accounts AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= '2013-01-31'
   AND csh.effective_to_dt > '2013-01-31'
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
--select top 500 * from  v183_all_active_accounts;
--      update v183_all_active_accounts file
UPDATE v183_all_active_accounts
SET  BB_type = BB.BB_type
FROM v183_all_active_accounts  AS Base
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
        inner join v183_all_active_accounts as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<'2013-01-31'              -- (END)
AND        first_activation_dt>='2010-01-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update AdSmart file
UPDATE v183_all_active_accounts
SET Anytime_plus = Aplus.Anytime_plus
FROM v183_all_active_accounts  AS Base
       INNER JOIN Anytime_plus AS Aplus
        ON base.account_number = APlus.account_number
ORDER BY base.account_number;
commit;

DROP TABLE Anytime_plus;
commit;

---Anytime Plus Used---
--select top 100 * from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS;
SELECT  account_number
,count(distinct cast(last_modified_dt as date)) as unique_dates_with_anytime_plus_downloads
,count(*) as total_anytime_plus_download_records
into anytime_plus_downloads
--into v141_anytime_plus_users
FROM   sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS
WHERE  last_modified_dt BETWEEN '2012-11-01' and '2013-01-31'
AND    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND    download_count=1    -- to exclude any spurious header/trailer download records
group by account_number
;
commit;


CREATE   HG INDEX idx01 ON anytime_plus_downloads(account_number);
commit;
alter table v183_all_active_accounts add unique_dates_with_anytime_plus_downloads tinyint default 0;
alter table v183_all_active_accounts add total_anytime_plus_download_records tinyint default 0;

update  v183_all_active_accounts
set unique_dates_with_anytime_plus_downloads = b.unique_dates_with_anytime_plus_downloads
,total_anytime_plus_download_records=b.total_anytime_plus_download_records
from v183_all_active_accounts as a
left outer join anytime_plus_downloads as b
on a.account_number=b.account_number
;
commit;

DROP TABLE anytime_plus_downloads;
commit;

--select top 500 * from v183_all_active_accounts;
----Update Nulls to 0---

update v183_all_active_accounts
set hdtv=case when hdtv is null then 0 else hdtv end
,multiroom=case when multiroom is null then 0 else multiroom end
,skyplus=case when skyplus is null then 0 else skyplus end
,unique_dates_with_anytime_plus_downloads=case when unique_dates_with_anytime_plus_downloads is null then 0 else unique_dates_with_anytime_plus_downloads end
,total_anytime_plus_download_records=case when total_anytime_plus_download_records is null then 0 else total_anytime_plus_download_records end
from v183_all_active_accounts
;
commit;

--select sum(hdtv) from v183_all_active_accounts

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
where cb_change_date='2013-05-23'
and cb_address_status = '1' and cb_address_dps IS NOT NULL and cb_address_organisation IS NULL
group by cb_key_household;
commit;

exec sp_create_tmp_table_idx '#experian_hh_summary', 'cb_key_household';
commit;



---Add Experian Values to main account table
alter table v183_all_active_accounts add hh_composition             VARCHAR(2)     default 'U'         ;   
alter table v183_all_active_accounts add hh_affluence             VARCHAR(2)     default 'U'         ;   
alter table v183_all_active_accounts add head_hh_age             VARCHAR(1)     default 'U'         ;   
alter table v183_all_active_accounts add num_children_in_hh             VARCHAR(1)            ;   

alter table v183_all_active_accounts add number_of_adults            bigint         ;   
alter table v183_all_active_accounts add number_of_bedrooms             VARCHAR(1)            ;   
alter table v183_all_active_accounts add length_of_residency             VARCHAR(2)           ;  
alter table v183_all_active_accounts add residence_type             VARCHAR(1)            ;   
alter table v183_all_active_accounts add own_rent_status             VARCHAR(1)            ;   


update v183_all_active_accounts
set hh_composition=b.hh_composition
,hh_affluence=b.hh_affluence
,head_hh_age=b.head_hh_age
,num_children_in_hh=b.num_children_in_hh

,number_of_adults=b.number_of_adults
,number_of_bedrooms=b.number_of_bedrooms
,length_of_residency=b.length_of_residency

,residence_type=b.residence_type
,own_rent_status=b.own_rent_status

from v183_all_active_accounts as a
left outer join #experian_hh_summary as b
on a.cb_key_household=b.cb_key_household
;
commit;

delete from v183_all_active_accounts where activation_date >= '2013-02-01';
delete from v183_all_active_accounts where activation_date is null;
commit;

-----Box Type------------
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
        INNER JOIN AdSmart AS ss
        ON csh.account_number = ss.account_number
 WHERE  csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Extra Subscription')     --the DTV sub Type
   AND csh.status_code IN ('AC','AB','PC')                  --Active Status Codes
   AND csh.effective_from_dt <= '2013-01-31'
   AND csh.effective_to_dt > '2013-01-31'
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
WHERE box_installed_dt <= '2013-01-31'
        AND box_replaced_dt   > '2013-01-31'
        AND current_product_description like '%HD%'
GROUP BY stb.service_instance_id;
commit;

CREATE HG INDEX idx14 ON hda(service_instance_id);
commit;

--select top 100 * from hda;


--drop table scaling_box_level_viewing;
--commit;

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
 WHERE csh.effective_FROM_dt <= '2013-01-31'
   AND csh.effective_to_dt    > '2013-01-31'
   AND csh.status_code IN  ('AC','AB','PC')
   AND csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
   AND csh.effective_FROM_dt <> csh.effective_to_dt
GROUP BY acc.service_instance_id ,acc.account_number;
commit;

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


--Add Box Types in HH back to main table---

alter table v183_all_active_accounts add boxtype varchar(50);

update v183_all_active_accounts
set boxtype=b.boxtype
from v183_all_active_accounts as a
left outer join boxtype_ac as b
on a.account_number = b.account_number
;

commit;


alter table v183_all_active_accounts add cb_address_postcode varchar(10);
alter table v183_all_active_accounts add cb_address_postcode_area varchar(2);

update v183_all_active_accounts
set cb_address_postcode=b.cb_address_postcode
,cb_address_postcode_area=b.cb_address_postcode_area
from v183_all_active_accounts as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

commit;


alter table v183_all_active_accounts add cable_area varchar(3);

--cable area


update v183_all_active_accounts
set cable_area=CASE  WHEN cable_postcode ='N' THEN 'N'
             WHEN cable_postcode ='n' THEN 'N'
             WHEN cable_postcode ='Y' THEN 'Y'
             WHEN cable_postcode ='y' THEN 'Y'
                                      ELSE 'N/A'
       END
from v183_all_active_accounts as a
left outer join sk_prod.broadband_postcode_exchange as b
 ON replace(a.cb_address_postcode, ' ','') = replace(b.cb_address_postcode,' ','')
;
commit;



alter table v183_all_active_accounts add full_months_tenure integer;
alter table v183_all_active_accounts add tenure_group varchar(20);

update v183_all_active_accounts
set full_months_tenure=  case when cast(dateformat(activation_date,'DD') as integer)>1 then 
     datediff(mm,activation_date,cast('2013-02-01' as date))-1 else datediff(mm,activation_date,cast('2013-02-01' as date)) end 
from v183_all_active_accounts
;
commit;

update v183_all_active_accounts
set tenure_group=
case when full_months_tenure<=12 then '01) 0-12mths' 
when full_months_tenure<=24 then '02) 13-24mths' 
when full_months_tenure<=36 then '03) 25-36mths' 
when full_months_tenure<=60 then '04) 37-60mths' 
when full_months_tenure>60 then '05) 61+mths' else '06) Other' end
from v183_all_active_accounts
;
commit;


alter table v183_all_active_accounts add value_segment varchar(20);

update v183_all_active_accounts
set value_segment=b.value_segment
from v183_all_active_accounts as a
left outer join sk_prod.value_segments_five_yrs as b
 ON a.account_number = b.account_number
where value_seg_date='2013-01-29'
;
commit;

alter table v183_all_active_accounts add acct_finance_rtm varchar(30);

update v183_all_active_accounts
set acct_finance_rtm=b.acct_finance_rtm
from v183_all_active_accounts as a
left outer join sk_prod.cust_single_account_view  as b
 ON a.account_number = b.account_number
;
commit;




--select tenure_group , count(*) from v183_all_active_accounts group by tenure_group order by tenure_group
---------------
--drop table v183_weighted_unweighted_account_details;
select tenure_group
,case when left(boxtype,2)='HD' then '01) HD' when  left(boxtype,7)='Skyplus' then '02) Sky+' when left(boxtype,3)='FDB' then '03) FDB' 
else '04) Unknown Box Type' end as highest_box_type
,case when bb_type<>'NA' then '01) Has BB' else '02) No BB' end as broadband_type
,own_rent_status
,hdtv
,multiroom
,residence_type
,hh_affluence
,cable_area
,status_at_20130201
,value_segment
,count(*) as accounts
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted
into v183_weighted_unweighted_account_details
from v183_all_active_accounts
where country_code = 'GBR'
group by 
tenure_group
, highest_box_type
,own_rent_status
,hdtv
,multiroom
,broadband_type
,residence_type
,hh_affluence
,cable_area
,status_at_20130201
,value_segment
;

commit;

grant all on v183_weighted_unweighted_account_details to public;
commit;






---Analysis Splits---

select case when cast(dateformat(activation_date,'DD') as integer)>1 then 
     datediff(mm,activation_date,cast('2013-02-01' as date))-1 else datediff(mm,activation_date,cast('2013-02-01' as date)) end as full_months_tenure
,case when full_months_tenure<24 then '01) Under 24mths' else '02) Over 24ths' end as tenure_group
,count(*) as records
,sum(case when vespa_weight_20130201>0 then 1 else 0 end) as accounts_with_weight
--,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as accounts_weight_value
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
--,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then 1 else 0 end) as accounts_with_churn_with_weight
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted
/*
,sum(case when status_at_20130501 in ('PO') then 1 else 0 end) as accounts_with_churn_cuscan
,sum(case when status_at_20130501 in ('PO') and vespa_weight_20130201>0  then 1 else 0 end) as accounts_with_churn_with_weight_cuscan
,sum(case when status_at_20130501 in ('PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted_cuscan

,sum(case when status_at_20130501 in ('SC') then 1 else 0 end) as accounts_with_churn_syscan
,sum(case when status_at_20130501 in ('SC') and vespa_weight_20130201>0  then 1 else 0 end) as accounts_with_churn_with_weight_syscan
,sum(case when status_at_20130501 in ('SC') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted_syscan
*/
from v183_all_active_accounts
where country_code = 'GBR'
group by full_months_tenure,tenure_group
order by full_months_tenure,tenure_group
;

commit;


select case when bb_type<>'NA' then '01) Has BB' else '02) No BB' end as broadband_type
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by broadband_type
order by broadband_type
;


select boxtype
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by boxtype
order by boxtype
;


select cable_area
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by cable_area
order by cable_area
;


select value_segment
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR' and status_at_20130201='AC'
group by value_segment
order by value_segment
;

select value_segment, tenure_group
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by value_segment, tenure_group
order by value_segment, tenure_group
;




select head_hh_age
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by head_hh_age
order by head_hh_age
;



select length_of_residency
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by length_of_residency
order by length_of_residency
;

---Create Pivot----
select own_rent_status
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by own_rent_status
order by own_rent_status
;


select residence_type
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by residence_type
order by residence_type
;



select acct_finance_rtm
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by acct_finance_rtm
order by records desc
;


select hdtv
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by hdtv
order by hdtv
;


select hdtv
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by hdtv
order by hdtv
;




select skyplus
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by skyplus
order by skyplus
;


select multiroom
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by multiroom
order by multiroom
;



select hh_composition
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted

from v183_all_active_accounts
where country_code = 'GBR'
group by hh_composition
order by hh_composition
;







/*


select tenure_group,status_at_20130201
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted
from v183_all_active_accounts

where country_code = 'GBR'
group by tenure_group,status_at_20130201
order by tenure_group,status_at_20130201


select status_at_20130201
,count(*) as records
,sum(case when vespa_weight_20130201>0 then vespa_weight_20130201 else 0 end) as weighted_accounts
,sum(case when status_at_20130501 in ('SC','PO') then 1 else 0 end) as accounts_with_churn
,sum(case when status_at_20130501 in ('SC','PO') and vespa_weight_20130201>0  then vespa_weight_20130201 else 0 end) as accounts_with_churn_weighted
from v183_all_active_accounts

where country_code = 'GBR'
group by status_at_20130201
order by status_at_20130201






select top 100 * from v183_all_active_accounts;

select dateformat(activation_date,'YYYYMM') as mth
,count(*)
from v183_all_active_accounts
group by mth
order by mth


select country_code
,count(*)
from v183_all_active_accounts
group by country_code
order by country_code

select status_at_20130201
,count(*)
from v183_all_active_accounts
group by status_at_20130201
order by status_at_20130201



commit;

*/
