


---Create Table of Account Attributes as at 26th Dec 2011-----


select account_number
into #v159_accounts_for_profiling_dec2011_active
from account_status_at_period_start
where  country_code = 'GBR' and status_at_2011_12_26='AC' and activation_date<='2011-12-26'
group by account_number
;
select * into v159_accounts_for_profiling_dec2011_active from #v159_accounts_for_profiling_dec2011_active;
commit;
create  hg index idx1 on v159_accounts_for_profiling_dec2011_active (account_number);
commit;

--Create Package Details for actual date of analysis (14th Nov 2012)


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
   AND csh.effective_from_dt <= '2011-12-25'
   AND csh.effective_to_dt   >  '2011-12-25'
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
alter table v159_accounts_for_profiling_dec2011_active add prem_sports integer default 0;
alter table v159_accounts_for_profiling_dec2011_active add prem_movies integer default 0;
alter table v159_accounts_for_profiling_dec2011_active add mixes_type varchar(30) default 'Unknown';

update v159_accounts_for_profiling_dec2011_active 
set prem_sports=b.prem_sports
,prem_movies=b.prem_movies
,mixes_type=b.new_package
from v159_accounts_for_profiling_dec2011_active  as a
left outer join #mixes as b
on a.account_number=b.account_number
;
commit;


select account_number
into #accounts_with_3d  
FROM sk_prod.cust_subs_hist as csh
      
 WHERE subscription_type = 'A-LA-CARTE' and subscription_sub_type = '3DTV'
   AND csh.status_code in ('AC','AB','PC')
   AND csh.effective_from_dt <= '2011-12-25'
   AND csh.effective_to_dt   >  '2011-12-25'
group by account_number
;

exec sp_create_tmp_table_idx '#accounts_with_3d', 'account_number';

alter table v159_accounts_for_profiling_dec2011_active add subscription_3d integer default 0;

update v159_accounts_for_profiling_dec2011_active
set subscription_3d=case when b.account_number is not null then 1 else 0 end
from v159_accounts_for_profiling_dec2011_active as a
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
       AND csh.effective_from_dt <= '2011-12-25'
       AND csh.effective_to_dt    >  '2011-12-25'
GROUP BY csh.account_number;
commit;

commit;
create  hg index idx1 on v141_MR_HD (account_number);
alter table v159_accounts_for_profiling_dec2011_active add hdtv                    tinyint          default 0    ;     
alter table v159_accounts_for_profiling_dec2011_active add multiroom                    tinyint          default 0    ;     
alter table v159_accounts_for_profiling_dec2011_active add skyplus                    tinyint          default 0    ;     
commit;


update v159_accounts_for_profiling_dec2011_active
set hdtv=b.hdtv
,multiroom=b.multiroom
,skyplus=b.skyplus
from v159_accounts_for_profiling_dec2011_active as a
left outer join v141_MR_HD as b
on a.account_number=b.account_number
;
commit;
drop table v141_MR_HD;
commit;

--select top 100 * from v141_live_playback_viewing;

----Add on extra variables from product holdings and consumerview---

alter table v159_accounts_for_profiling_dec2011_active add talk_product              VARCHAR(50)     default 'NA' ;        -- Current Sky Talk product
--alter table v159_accounts_for_profiling_dec2011_active add sky_id                    bigint          default 0    ;        -- Sky id created
alter table v159_accounts_for_profiling_dec2011_active add distinct_usage_days                INTEGER         default 0     ;       -- Sky Go days in 3mth period
alter table v159_accounts_for_profiling_dec2011_active add usage_records                INTEGER         default 0     ;       -- Sky Go usage records in 3mth period
alter table v159_accounts_for_profiling_dec2011_active add BB_type                   VARCHAR(50)     default 'NA'  ;       -- Current BB product
alter table v159_accounts_for_profiling_dec2011_active add Anytime_plus              INTEGER         default 0    ;        -- Anytime+ activated
alter table v159_accounts_for_profiling_dec2011_active add isba_tv_region             VARCHAR(50)     default 'Unknown'         ;   
alter table v159_accounts_for_profiling_dec2011_active add cb_key_household           bigint   ;        -- Current Sky Talk product
--drop table nodupes;
commit;

update v159_accounts_for_profiling_dec2011_active
set isba_tv_region=b.isba_tv_region
,cb_key_household=b.cb_key_household
from v159_accounts_for_profiling_dec2011_active as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number=b.account_number
;
commit;
--select top 100 * from v159_accounts_for_profiling_dec2011_active;
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
     AND csh.effective_from_dt <= '2011-12-25'
     AND csh.effective_to_dt > '2011-12-25'
GROUP BY base.account_number, talk_product,effective_to_dt;
commit;

DELETE FROM talk where rank_id >1;
commit;


--      create index on talk
CREATE   HG INDEX idx09 ON talk(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2011_active
SET  talk_product = talk.talk_product
FROM v159_accounts_for_profiling_dec2011_active  AS Base
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
--        inner join v159_accounts_for_profiling_dec2011_active AS Base
--         ON usage.account_number = Base.account_number
where cb_data_date >= '2011-09-26'
        AND cb_data_date <'2011-12-25'
group by account_number;
commit;

--      create index on Sky_Go file
CREATE   HG INDEX idx06 ON skygo_usage(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2011_active
SET distinct_usage_days = sky_go.distinct_usage_days
,usage_records=sky_go.usage_records
FROM v159_accounts_for_profiling_dec2011_active  AS Base
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
    inner join v159_accounts_for_profiling_dec2011_active AS Base
    ON csh.account_number = base.account_number
WHERE subscription_sub_type = 'Broadband DSL Line'
   AND csh.effective_from_dt <= '2011-12-25'
   AND csh.effective_to_dt > '2011-12-25'
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

--      update v159_accounts_for_profiling_dec2011_active file
UPDATE v159_accounts_for_profiling_dec2011_active
SET  BB_type = BB.BB_type
FROM v159_accounts_for_profiling_dec2011_active  AS Base
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
        inner join v159_accounts_for_profiling_dec2011_active as Base
         on Aplus.account_number = Base.account_number
WHERE  subscription_sub_type = 'PDL subscriptions'
AND        status_code='AC'
AND        first_activation_dt<'2011-12-25'              -- (END)
AND        first_activation_dt>='2010-01-01'        -- (START) Oct 2010 was the soft launch of A+, no one should have it before then
AND        base.account_number is not null
AND        base.account_number <> '?'
GROUP BY   base.account_number;
commit;


--      create index on Anytime_plus file
CREATE   HG INDEX idx05 ON Anytime_plus(account_number);
commit;

--      update AdSmart file
UPDATE v159_accounts_for_profiling_dec2011_active
SET Anytime_plus = Aplus.Anytime_plus
FROM v159_accounts_for_profiling_dec2011_active  AS Base
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
WHERE  last_modified_dt BETWEEN '2011-09-26' and '2011-12-25'
AND    x_content_type_desc = 'PROGRAMME'  --  to exclude trailers
AND    download_count=1    -- to exclude any spurious header/trailer download records
group by account_number
;
commit;


CREATE   HG INDEX idx01 ON anytime_plus_downloads(account_number);
commit;

alter table v159_accounts_for_profiling_dec2011_active add unique_dates_with_anytime_plus_downloads tinyint default 0;
alter table v159_accounts_for_profiling_dec2011_active add total_anytime_plus_download_records tinyint default 0;
update  v159_accounts_for_profiling_dec2011_active
set unique_dates_with_anytime_plus_downloads = b.unique_dates_with_anytime_plus_downloads
,total_anytime_plus_download_records=b.total_anytime_plus_download_records
from v159_accounts_for_profiling_dec2011_active as a
left outer join anytime_plus_downloads as b
on a.account_number=b.account_number
;
commit;

DROP TABLE anytime_plus_downloads;
commit;

--select unique_dates_with_anytime_plus_downloads , count(*) from v159_accounts_for_profiling_dec2011_active  group by unique_dates_with_anytime_plus_downloads;

--select top 500 * from v159_accounts_for_profiling_dec2011_active;

----Update Nulls to 0---

update v159_accounts_for_profiling_dec2011_active
set hdtv=case when hdtv is null then 0 else hdtv end
,multiroom=case when multiroom is null then 0 else multiroom end
,skyplus=case when skyplus is null then 0 else skyplus end
,unique_dates_with_anytime_plus_downloads=case when unique_dates_with_anytime_plus_downloads is null then 0 else unique_dates_with_anytime_plus_downloads end
,total_anytime_plus_download_records=case when total_anytime_plus_download_records is null then 0 else total_anytime_plus_download_records end
from v159_accounts_for_profiling_dec2011_active
;
commit;


--select sum(hdtv) from v159_accounts_for_profiling_dec2011_active


