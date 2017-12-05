----------------------------------------------------------------------------------------------------------

--      Monthly Model Scoring Script
      ----------------------------

--      Author   : R Simmons
 --     Date     : 2015-08-14
      --------------------------------------
      --------------------------------------/*
--------------------------------------------------------------------------------------------------------------

-- STEP 1  -- SELECT RELVANT SCORING DATE AND SUBMIT FROM HERE
--CHECK DATA UP TO DATE
select max(effective_from_dt) from cust_subs_hist
                      -- SHOULD BE THE PREVIOUS THURSDAY's DATE
select event_dt, count(*) from citeam.view_cust_package_movements_hist group by event_dt order by event_dt
                      -- SHOULD BE THE PREVIOUS THURSDAY's DATE
select offer_start_dt_actual, count(*) from citeam. offer_usage_all group by offer_start_dt_actual order by offer_start_dt_actual
                      -- SHOULD BE THE PREVIOUS THURSDAY's DATE - MAY BE FUTURE DATES BUT SHOULD BE CRITICAL VOLUME OF DATA FOR LAST THURSDAY
select subs_activation_date, count(*) from CITEAM.VIEW_CUST_FREE_PRODUCTS_HIST group by subs_activation_date order by subs_activation_date
                        -- SHOULD BE THE PREVIOUS THURSDAY's DATE - LOOKS TO BE COMPLETELY OUT OF DATE

/*
CREATE VARIABLE @endDate DATE;
CREATE VARIABLE @monthYear VARCHAR(20);

SET  @endDate   = '2017-06-30';
SET  @monthYear = '201706';
select @endDate; select @monthYear;
*/
-- TO HERE


SET OPTION query_temp_space_limit =0;
--------------------------------------------------------------------------------------------------------------*/
-- STEP 2 - CHANGE THE TABLE NAME 'MMS_YYYY_MM' to relevant DATE AND SUBMIT REMAINING CODE
IF object_id('MMS_2017_06') IS NOT NULL
BEGIN
    DROP TABLE MMS_2017_06
END


--Base Set Up
CREATE TABLE MMS_2017_06
     (
         MonthYear                                       INTEGER         NOT NULL
         ,Observation_dt                                 DATE            NULL
--####---Account Info
         ,account_number                                 VARCHAR(20)     NOT NULL
         ,Status_Code                                    VARCHAR(10)     NULL
         ,Country                                        CHAR(3)         NULL
         ,cb_key_individual                              BIGINT          DEFAULT NULL
         ,cb_key_household                               BIGINT          DEFAULT NULL
         ,CL_Current_Age                                 INTEGER         DEFAULT 0
         ,postcode                                       VARCHAR(20)     DEFAULT NULL
         ,gender                                         VARCHAR(1)      DEFAULT NULL
         ,FibreArea                                      BIT             DEFAULT 0
         ,Onnet                                          BIT             DEFAULT 0
         ,CableArea                                      BIT             DEFAULT 0
         ,acct_sam_registered                            tinyint         null
         --,MDU                                          VARCHAR(100)    NULL
--####---Third Party (Experian)
         ,affluence                                      VARCHAR(20)     DEFAULT NULL
         ,life_stage                                     VARCHAR(50)     DEFAULT NULL
         ,kids                                           INTEGER         DEFAULT 0
         ,h_mosaic_uk_group                              VARCHAR(20)     DEFAULT NULL
         ,h_income_band_v2                               VARCHAR(20)     DEFAULT NULL
         ,hh_composition                                 VARCHAR(50)     DEFAULT NULL
         ,h_fss_v3_group                                 VARCHAR(20)     DEFAULT NULL
         ,h_age_coarse                                   INTEGER         DEFAULT NULL
         ,h_affluence_v2                                 VARCHAR(20)     DEFAULT NULL
--####--Marketing
--####---Scores
--####---PayProducts
         ,DTV                                            SMALLINT        DEFAULT 0 --PROD: DTV Primary Viewing
         ,MR                                             SMALLINT        DEFAULT 0 --PROD: MR
         ,HDTV                                           SMALLINT        DEFAULT 0 --PROD: HDTV
         ,SkyGoExtra                                     SMALLINT        DEFAULT 0 --PROD: SkyGoExtra RS
         --Note: To define box type we need  below columns
         ,SkyPlus                                        SMALLINT        DEFAULT 0
         --History
         --,MR_Count                                     INTEGER         DEFAULT 0
         --Dates History
         ,sav_acct_first_act_date                        DATE            NULL
         ,dtv_first_act_date                             DATE            NULL
         ,dtv_latest_act_date                            DATE            NULL
         ,BB_first_act_date                              DATE            NULL
         ,BB_latest_act_date                             DATE            NULL
         ,SkyTalk_first_act_date                         DATE            NULL
         ,SkyTalk_latest_act_date                        DATE            NULL
         ,WLR_first_act_date                             DATE            NULL
         ,WLR_latest_act_date                            DATE            NULL
--####---FreeProducts
         --History
--####---Comms
         ,BroadBand                                      SMALLINT        DEFAULT 0
         ,SABB                                           SMALLINT        DEFAULT 0
         ,SkyTalk                                        SMALLINT        DEFAULT 0
         ,WLR                                            SMALLINT        DEFAULT 0
         ,BB_type                                        VARCHAR(156)     NULL
         ,SkyTalk_type                                   VARCHAR(156)     NULL
--####---Content
         ,Sports                                         SMALLINT        DEFAULT 0
         ,Movies                                         SMALLINT        DEFAULT 0
         ,TopTier                                        SMALLINT        DEFAULT 0
         ,package_detail_desc                            VARCHAR(255)    NULL
         ,package_desc                                   VARCHAR(255)    NULL
         ,hdtv_premium                                   SMALLINT        DEFAULT 0
         ,hdtv_sub_type                                  VARCHAR(25)     NULL
--####---Contract
--####---Reinstates
--####---Churn
--####---Movies/Sports Upgrades and DownGrades
--####---Summary Columns
         ,h_fss_v3_group_Description                     VARCHAR(156)  DEFAULT 'UNKNOWN'
         ,h_age_coarse_Description               VARCHAR(156)  DEFAULT 'UNKNOWN'
         ,h_income_band_v2_Description           VARCHAR(156)  DEFAULT 'UNKNOWN'
         ,BB_type_Description                    VARCHAR(156)  DEFAULT 'UNKNOWN'
         ,SkyTalk_type_Description               VARCHAR(156)  DEFAULT 'UNKNOWN'
         ,Product_Holding                        VARCHAR(156)  DEFAULT 'UNKNOWN'
         ,OD_RF                                  varchar(20)   default null
-- CVS Segments
         ,CVS_segment                                    VARCHAR(20)   NULL
         )

GRANT ALL ON MMS_2017_06 TO PUBLIC
--Base Indexing
CREATE UNIQUE HG INDEX idx_account_number       ON MMS_2017_06(account_number)
CREATE        LF INDEX idx_Country              ON MMS_2017_06(Country)
CREATE        HG INDEX idx_cbkeyindividual      ON MMS_2017_06(cb_key_individual)
CREATE        HG INDEX idx_cbkeyhousehold       ON MMS_2017_06(cb_key_household)
CREATE        HG INDEX idx_postcode             ON MMS_2017_06(postcode)
CREATE        LF INDEX idx_affluence            ON MMS_2017_06(affluence)
CREATE        LF INDEX idx_lifestage            ON MMS_2017_06(life_stage)
CREATE        LF INDEX idx_hmosaicukgroup       ON MMS_2017_06(h_mosaic_uk_group)
CREATE        LF INDEX idx_hincomebandv2        ON MMS_2017_06(h_income_band_v2)
CREATE        LF INDEX idx_hhcomposition        ON MMS_2017_06(hh_composition)
CREATE        LF INDEX idx_hfssv3group          ON MMS_2017_06(h_fss_v3_group)
SELECT 'A02 - Create Base table and Indexes'
--------------------------------------------------------------- A03 - Populate Base Table with AccountNumber
--------------------------------------------------------------------------------------------------------------*/
INSERT INTO MMS_2017_06 (account_number, MonthYear, Country ,DTV ,SABB)
SELECT CSH.account_number ,@monthyear, MAX(CASE WHEN CSH.currency_code = 'EUR'  THEN 'ROI' ELSE 'UK' END)
       , MAX(CASE WHEN CSH.subscription_sub_type = 'DTV Primary Viewing' AND CSH.status_code IN ('AB','AC','PC')  THEN 1 ELSE 0 END) AS DTV
       , MAX (CASE  WHEN csh.subscription_sub_type = 'Broadband DSL Line'
                         AND (csh.status_code in ('AC','AB')
                               OR (csh.status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                               OR (csh.status_code='CF' AND prev_status_code='PC'                                  )
                               OR (csh.status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                             )
        THEN 1 ELSE 0 END)  AS SABB
  FROM  cust_subs_hist AS CSH
       INNER JOIN  cust_single_account_view SAV ON SAV.account_number = CSH.account_number
 WHERE CSH.effective_from_dt <= @endDate
   AND CSH.effective_to_dt    > @endDate
   AND CSH.effective_from_dt <> csh.effective_to_dt
   AND CSH.subscription_sub_type IN ('DTV Primary Viewing','Broadband DSL Line') AND CSH.status_code IN ('AB','AC','PC','CF','AP')
  GROUP BY CSH.account_number

  --SABB
 UPDATE  MMS_2017_06
    SET  SABB = 0
  WHERE  DTV = 1
 --Status Code

        SELECT  csh.account_number
         ,csh.effective_from_dt
         ,csh.cb_row_id
         ,csh.status_code
         ,RANK() OVER (PARTITION BY  csh.account_number
                                     ORDER BY  csh.effective_from_dt desc
                                              ,csh.cb_row_id         desc
                                 ) AS 'RANK'
     INTO #attachments_status_code
    FROM  cust_subs_hist as csh
         INNER JOIN MMS_2017_06 att ON att.account_number = CSH.account_number
WHERE CSH.effective_from_dt <= @endDate
   AND CSH.effective_to_dt    > @endDate
   AND CSH.effective_from_dt <> csh.effective_to_dt
   AND CSH.subscription_sub_type = 'DTV Primary Viewing'
   AND CSH.status_code IN ('AB','AC','PC')

  DELETE FROM #attachments_status_code WHERE RANK > 1

--update base table
  UPDATE MMS_2017_06
     SET  base.Status_Code  = base_status.status_code
    from  MMS_2017_06 AS BASE
          INNER JOIN #attachments_status_code as base_status on base_status.account_number = BASE.account_number
          where dtv = 1

SELECT  csh.account_number
         ,csh.effective_from_dt
         ,csh.cb_row_id
         ,csh.status_code
         ,RANK() OVER (PARTITION BY  csh.account_number
                                     ORDER BY  csh.effective_from_dt desc
                                              ,csh.cb_row_id         desc
                                 ) AS 'RANK'
     INTO #attachments_status_code_bb
    FROM  cust_subs_hist as csh
         INNER JOIN MMS_2017_06 att ON att.account_number = CSH.account_number
WHERE CSH.effective_from_dt <= @endDate
   AND CSH.effective_to_dt    > @endDate
   AND CSH.effective_from_dt <> csh.effective_to_dt
   AND CSH.subscription_sub_type = 'Broadband DSL Line'
   AND CSH.status_code IN ('AB','AC','PC','CF','AP')

  DELETE FROM #attachments_status_code_bb WHERE RANK > 1

--update base table
  UPDATE MMS_2017_06
     SET  base.Status_Code  = base_status.status_code
    from  MMS_2017_06 AS BASE
          INNER JOIN #attachments_status_code_bb as base_status on base_status.account_number = BASE.account_number
          where sabb = 1
  DELETE FROM  MMS_2017_06 WHERE DTV = 0 AND SABB = 0
  DROP TABLE #attachments_status_code
  DROP TABLE #attachments_status_code_bb
  SELECT 'A03 - Populate Base Table with AccountNumber'
--------------------------------------------------------------- A04 - Populate Base Table with Account Details
--------------------------------------------------------------------------------------------------------------*/
UPDATE MMS_2017_06
   SET  BASE.cb_key_individual           = SAV.cb_key_individual
        ,BASE.cb_key_household           = SAV.cb_key_household
        ,BASE.postcode                   = SAV.cb_address_postcode
        ,BASE.CL_Current_Age             = SAV.CL_Current_Age
        ,BASE.acct_sam_registered        = SAV.acct_sam_registered
        ,BASE.gender                     = SAV.acc_gender
  FROM  MMS_2017_06 AS  BASE
        INNER JOIN  cust_single_account_view SAV on SAV.account_number = BASE.account_number

    SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
    INTO #BPE  --BROADBAND_POSTCODE_EXCHANGE
    FROM  BROADBAND_POSTCODE_EXCHANGE
GROUP BY postcode
UPDATE #BPE SET postcode = REPLACE(postcode,' ','')

  SELECT postcode, MAX(exchange_id) as exchID
    INTO #PTE  --BB_POSTCODE_TO_EXCHANGE
    FROM  BB_POSTCODE_TO_EXCHANGE
GROUP BY postcode
UPDATE #PTE SET postcode = REPLACE(postcode,' ','')

SELECT COALESCE(#PTE.postcode, #BPE.postcode) AS postcode
       ,COALESCE(#PTE.exchID, #BPE.exchID) AS exchange_id
       ,'OFFNET' AS exchange
  INTO #ONNET_LOOKUP
  FROM #BPE FULL JOIN #PTE ON #BPE.postcode = #PTE.postcode

UPDATE #ONNET_LOOKUP
   SET exchange = 'ONNET'
  FROM #ONNET_LOOKUP AS OL
         INNER JOIN  easynet_rollout_data AS ERD ON OL.exchange_id = ERD.exchange_id
 WHERE ERD.exchange_status = 'ONNET'
--###ONNET
UPDATE MMS_2017_06
   SET BASE.onnet = CASE WHEN OL.exchange = 'ONNET' THEN 1 ELSE 0 END
  FROM MMS_2017_06 AS BASE
       INNER JOIN #ONNET_LOOKUP AS OL ON REPLACE(ISNULL(BASE.postcode,''),' ','') = OL.postcode
--###FIBRE
UPDATE MMS_2017_06
   SET BASE.FibreArea = 1
  FROM MMS_2017_06 AS BASE
       INNER JOIN  BT_FIBRE_POSTCODE AS BFP
    ON REPLACE(BASE.postcode,' ','') = REPLACE(BFP.cb_address_postcode,' ','')
   AND BFP.fibre_enabled_perc >= 75
 WHERE BFP.first_fibre_enabled_date <= @enddate
--DROP TEMPORARY TABLE
  DROP TABLE #BPE
  DROP TABLE #PTE
  DROP TABLE #ONNET_LOOKUP
--Cable Area
UPDATE MMS_2017_06
   SET BASE.CableArea = CASE WHEN COALESCE(lower(bb.cable_postcode),'n') = 'y' THEN 1 ELSE 0 END
  FROM MMS_2017_06 AS BASE
      LEFT OUTER JOIN  broadband_postcode_exchange  as bb
      ON REPLACE(ISNULL(BASE.postcode,''),' ','') = replace(bb.cb_address_postcode,' ','')
WHERE cb_address_postcode IS NOT NULL

SELECT 'A04 - Populate Base Table with Account Details'
--------------------------------------------------------------- A05 - Populate Base Table with Experian (Third Party)
--------------------------------------------------------------------------------------------------------------*/
UPDATE  MMS_2017_06
   SET  BASE.h_fss_v3_group      = CASE WHEN EC.cb_key_household IS NULL THEN '' ELSE EC.h_fss_v3_group END
        ,BASE.h_age_coarse       =  EC.h_age_coarse
        ,BASE.h_income_band_v2   = CASE WHEN EC.cb_key_household IS NULL THEN '' ELSE EC.h_income_band_v2 END
        ,BASE.h_mosaic_uk_group  = CASE WHEN EC.cb_key_household IS NULL THEN '' ELSE EC.h_mosaic_uk_group END
        ,BASE.kids               = CASE WHEN EC.cb_key_household IS NULL THEN 0 ELSE CAST(EC.h_number_of_children_in_household_2011 AS INTEGER) END
        ,BASE.h_affluence_v2     = EC.h_affluence_v2
        ,BASE.affluence          = CASE  WHEN EC.h_affluence_v2 IN('00','01','02') THEN   '1.Very Low'
                                         WHEN EC.h_affluence_v2 IN('03','04','05') THEN   '2.Low'
                                         WHEN EC.h_affluence_v2 IN('06','07','08') THEN   '3.Mid Low'
                                         WHEN EC.h_affluence_v2 IN('09','10','11') THEN   '4.Mid'
                                         WHEN EC.h_affluence_v2 IN('12','13','14') THEN   '5.Mid High'
                                         WHEN EC.h_affluence_v2 IN('15','16','17') THEN   '6.High'
                                         WHEN EC.h_affluence_v2 IN('18','19'     ) THEN   '7.Very High'
                                   ELSE '8. Unknown' END
        ,BASE.life_stage         = CASE  WHEN EC.h_family_lifestage_2011 = '00' THEN '1.Young singles/homesharers'
                                         WHEN EC.h_family_lifestage_2011 = '01' THEN '2.Young family no children <18'
                                         WHEN EC.h_family_lifestage_2011 = '02' THEN '3.Young family with children <18'
                                         WHEN EC.h_family_lifestage_2011 = '03' THEN '4.Young household with children <18'
                                         WHEN EC.h_family_lifestage_2011 = '04' THEN '5.Mature singles/homesharers'
                                         WHEN EC.h_family_lifestage_2011 = '05' THEN '6.Mature family no children <18'
                                         WHEN EC.h_family_lifestage_2011 = '06' THEN '7.Mature family with children <18'
                                         WHEN EC.h_family_lifestage_2011 = '07' THEN '8.Mature household with children <18'
                                         WHEN EC.h_family_lifestage_2011 = '08' THEN '9.Older single'
                                         WHEN EC.h_family_lifestage_2011 = '09' THEN '10.Older family no children <18'
                                         WHEN EC.h_family_lifestage_2011 = '10' THEN '11.Older family/household with children<18'
                                         WHEN EC.h_family_lifestage_2011 = '11' THEN '12.Elderly single'
                                         WHEN EC.h_family_lifestage_2011 = '12' THEN '13.Elderly family no children <18'
                                         WHEN EC.h_family_lifestage_2011 =  'U' THEN '14.Unclassified'
                                   ELSE NULL END
        ,BASE.hh_composition     = CASE WHEN EC.h_household_composition = '00' THEN '1.Families '
                                        WHEN EC.h_household_composition = '01' THEN '2.Extended family '
                                        WHEN EC.h_household_composition = '02' THEN '3.Extended household '
                                        WHEN EC.h_household_composition = '03' THEN '4.Pseudo family '
                                        WHEN EC.h_household_composition = '04' THEN '5.Single male '
                                        WHEN EC.h_household_composition = '05' THEN '6.Single female '
                                        WHEN EC.h_household_composition = '06' THEN '7.Male homesharers '
                                        WHEN EC.h_household_composition = '07' THEN '8.Female homesharers '
                                        WHEN EC.h_household_composition = '08' THEN '9.Mixed homesharers '
                                        WHEN EC.h_household_composition = '09' THEN '10.Abbreviated male families '
                                        WHEN EC.h_household_composition = '10' THEN '11.Abbreviated female families '
                                        WHEN EC.h_household_composition = '11' THEN '12.Multi-occupancy dwelling '
                                        WHEN EC.h_household_composition = 'U'  THEN '13.Unclassified '
                                   ELSE NULL END
  FROM  MMS_2017_06 AS BASE
        LEFT JOIN  experian_consumerview AS EC ON BASE.cb_key_household = EC.cb_key_household

SELECT 'A05 - Populate Base Table with Experian (Third Party)'

--------------------------------------------------------------- A06 - Populate Base Table with Products hd,mr,3dtv,bb,sky talk,SkyPlus ,Entertainment Extra , Box Type
--------------------------------------------------------------------------------------------------------------*/

UPDATE  MMS_2017_06
        --BASE.HDTV          = CSHP_T.hdtv
        --,BASE.HD            = CSHP_T.HD
   SET  BASE.MR            = CSHP_T.MR
        --,BASE.MR_Count      = CSHP_T.MR
        ,BASE.BroadBand     = CSHP_T.broadband
        ,BASE.BB_type       = CSHP_T.broadband_type
        ,BASE.SkyTalk       = CSHP_T.skytalk
        ,BASE.SkyTalk_type  = CSHP_T.skytalk_type
        ,BASE.WLR           = CSHP_T.WLR
        ,BASE.SkyPlus       = CSHP_T.SkyPlus
        ,BASE.Sports        = CSHP_T.prem_sports
        ,BASE.movies        = CSHP_T.prem_movies
        ,BASE.SkyGoExtra    = CSHP_T.SkyGoExtra --RS
        --,BASE.TopTier       = CSHP_T.top_tier
  FROM  MMS_2017_06 AS BASE
        INNER JOIN (
                      SELECT    CASE WHEN currency_code = 'EUR'  THEN 'ROI' ELSE 'UK' END AS country
                               ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Sky+'              AND csh.status_code IN  ('AC','AB','PC') THEN 1 ELSE 0 END) AS SkyPlus
                               ,SUM (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV HD'                AND csh.status_code IN  ('AC','AB','PC') THEN 1 ELSE 0 END) AS HD
                               ,MAX (CASE  WHEN csh.subscription_sub_type = 'DTV HD'                AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END) AS hdtv
                               ,SUM (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Extra Subscription'AND csh.status_code IN  ('AC','AB','PC') THEN 1 ELSE 0 END) AS MR_Count
                               ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Extra Subscription'AND csh.status_code IN  ('AC','AB','PC') THEN 1 ELSE 0 END) AS MR
                               ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = '3DTV'                  AND csh.status_code IN  ('AC','AB','PC') THEN 1 ELSE 0 END) AS Three_DTV
                               ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'SKY TALK LINE RENTAL'  AND csh.status_code IN  ('A','CRQ','PAX')THEN 1 ELSE 0 END) AS WLR
                               ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'ESPN'                  AND csh.status_code IN  ('AC','AB','PC') THEN 1 ELSE 0 END) AS ESPN
                               ,MAX (CASE  WHEN csh.subscription_sub_type = 'Broadband DSL Line'    AND (csh.status_code in ('AC','AB')
                                                                                                          OR (csh.status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                                                                                          OR (csh.status_code='CF' AND prev_status_code='PC'                                  )
                                                                                                          OR (csh.status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                                                                                         )
                                           THEN 1 ELSE 0 END)  AS broadband
                              ,MAX(CASE  WHEN csh.subscription_sub_type = 'Broadband DSL Line'      AND ( csh.status_code in ('AC','AB')
                                                                                                          OR (csh.status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                                                                                          OR (csh.status_code='CF' AND prev_status_code='PC'                                  )
                                                                                                          OR (csh.status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                                                                                         )
                                         THEN current_product_description ELSE null END)  AS broadband_type
                              ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'         AND (csh.status_code = 'A'
                                                                                                          OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                                                                                          OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                                                                                          OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                                                                                         )
                                        THEN 1 ELSE 0 END)   AS skytalk
                              ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'         AND (csh.status_code = 'A'
                                                                                                          OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                                                                                          OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                                                                                          OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                                                                                         )
                                         THEN current_product_description ELSE null END)   AS skytalk_type
                              ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Primary Viewing'    AND csh.status_code IN  ('AC','AB','PC') THEN prem_sports ELSE 0 END) AS prem_sports
                              ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'DTV Primary Viewing'    AND csh.status_code IN  ('AC','AB','PC') THEN prem_movies ELSE 0 END) AS prem_movies
                              ,MAX (CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE = 'Sky Go Extra'           AND csh.status_code IN  ('AC','AB','PC') AND CSH.status_code_changed = 'Y' AND CSH.subscription_type = 'A-LA-CARTE' THEN 1 ELSE 0 END) as SKYGOEXTRA    -- RS
                              --,MAX(CASE  WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN 1 ELSE 0 END) AS top_tier
                               ,csh.account_number
                        FROM    cust_subs_hist AS csh
                               INNER JOIN  MMS_2017_06 AS Base on csh.account_number = Base.account_number
                               LEFT OUTER JOIN  cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
                       WHERE   csh.effective_from_dt <= @enddate
                         AND   csh.effective_to_dt    > @enddate
                         AND   csh.effective_from_dt <> csh.effective_to_dt
                         AND   csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV HD', 'DTV Extra Subscription','3DTV','DTV HD' ,'SKY TALK LINE RENTAL'
                                                              ,'ESPN', 'Broadband DSL Line', 'SKY TALK SELECT','Sky Go Extra'  )
                    GROUP BY csh.account_number,country
                    ) AS CSHP_T ON CSHP_T.account_number  = BASE.account_number

UPDATE MMS_2017_06
   SET  Observation_dt  =  @endDate
        ,toptier        =  (case when movies = 2 and sports = 2 then 1 else 0 end )


--TV PACKAGE

  UPDATE MMS_2017_06
     SET base.package_detail_desc = csh.current_product_description
    FROM MMS_2017_06 as base
         INNER JOIN  cust_subs_hist as csh on csh.account_number  = base.account_number
     AND   csh.effective_from_dt <= base.Observation_dt
         AND csh.effective_to_dt    > base.Observation_dt
     AND csh.effective_to_dt    > csh.effective_from_dt
     AND csh.subscription_sub_type = 'DTV Primary Viewing'
     AND csh.status_code in  ('AC','AB','PC')

  UPDATE MMS_2017_06
     SET package_desc  = CASE WHEN package_detail_desc LIKE 'Multiscreen%' THEN 'Multiscreen'
                              WHEN package_detail_desc LIKE 'Family%'   THEN 'Family'
                              WHEN package_detail_desc LIKE 'Variety%'  THEN 'Variety'
                              WHEN package_detail_desc LIKE 'Original%' THEN 'Original'
                              ELSE NULL END
        FROM MMS_2017_06

UPDATE  MMS_2017_06
   SET  BASE.hdtv              = CSHP_T.HDTV
        ,BASE.hdtv_premium     = CSHP_T.HDTV_premium
        ,BASE.hdtv_sub_type    = CSHP_T.HD_SUB_Type
  FROM  MMS_2017_06 AS BASE
        INNER JOIN (
                      SELECT    MAX(CASE WHEN subscription_sub_type in ('DTV HD') AND current_product_sk in (687, 43678) THEN 1 ELSE 0 END) HDTV
                               ,MAX(CASE WHEN subscription_sub_type in ('HD Pack') AND current_product_sk = 43679 THEN 1 ELSE 0 END) HDTV_premium
                               ,MAX(case when subscription_sub_type = 'DTV HD' and current_product_sk =   687 then 'HD Mix'    -- HD original, mix of entertainment and premium channels
                                         when subscription_sub_type = 'DTV HD' and current_product_sk = 43678 then 'HD Basic'  -- 48 HD channels, Feature of EntExtra+

                                   else NULL end) AS HD_SUB_Type
                               ,csh.account_number
                        FROM    cust_subs_hist AS csh
                               INNER JOIN  MMS_2017_06 AS Base on csh.account_number = Base.account_number
                       WHERE   csh.SUBSCRIPTION_SUB_TYPE IN ('DTV HD', 'HD Pack')
                         AND   csh.status_code in ('AC','AB','PC')
                         AND   csh.current_product_sk in (687, 43678, 43679)
                         and first_activation_dt < '9999-09-09'
                         AND   csh.effective_to_dt > csh.effective_from_dt
                         AND   csh.effective_from_dt <= base.Observation_dt
                         AND   csh.effective_to_dt    > base.Observation_dt
                    GROUP BY csh.account_number
                    ) AS CSHP_T ON CSHP_T.account_number  = BASE.account_number


SELECT 'A06 - Populate Base Table with Products hd,mr,3dtv,bb,sky talk,SkyPlus ,Entertainment Extra , Box Type'
--------------------------------------------------------------- A07 - Populate Base Table with Sky go And On Demand
--------------------------------------------------------------------------------------------------------------*/


--------------------------------------------------------------- A08 - Populate Base Table with History Dates
--------------------------------------------------------------------------------------------------------------*/
-----**********DTV*******************---
UPDATE MMS_2017_06
   SET BASE.sav_acct_first_act_date = SAV.acct_first_account_activation_dt
  FROM MMS_2017_06 AS BASE
       INNER JOIN  cust_single_account_view SAV on SAV.account_number = BASE.account_number

 SELECT CSH.account_number
       ,CSH.prev_status_code
       ,CSH.effective_from_dt
 INTO  #DTV_DATES_HISTORY
 FROM   cust_subs_hist AS CSH
       INNER JOIN MMS_2017_06 AS BASE  ON BASE.account_number = CSH.account_number
 WHERE CSH.subscription_sub_type = 'DTV Primary Viewing' AND CSH.status_code = 'AC' AND status_code_changed = 'Y'
 AND  CSH.effective_from_dt <= @endDate

UPDATE MMS_2017_06
   SET BASE.dtv_first_act_date    = DDH.dtv_first_act_date
       ,BASE.dtv_latest_act_date  = DDH.dtv_latest_act_date
  FROM MMS_2017_06 AS BASE
       INNER JOIN (
                   SELECT account_number
                          ,MAX(CASE WHEN prev_status_code IN ('PO','SC') AND effective_from_dt <= @endDate THEN effective_from_dt ELSE NULL END) AS dtv_latest_act_date
                          ,MIN(effective_from_dt) AS dtv_first_act_date
                    FROM  #DTV_DATES_HISTORY
                GROUP BY  account_number
                  )AS DDH ON DDH.account_number = BASE.account_number


UPDATE MMS_2017_06
   SET dtv_latest_act_date = CASE WHEN dtv_latest_act_date IS NULL THEN dtv_first_act_date ELSE dtv_latest_act_date END

-----**********BB*******************---
-- BB_first_act_date
-- BB_latest_act_date
UPDATE MMS_2017_06
   SET BASE.BB_first_act_date   = BB.BB_first_act_date
       ,BASE.BB_latest_act_date = BB.BB_latest_act_date
  FROM MMS_2017_06 AS BASE
       INNER JOIN
                 (
                   SELECT CSH.account_number
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS BB_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MMS_2017_06 AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'Broadband DSL Line'
                        AND (csh.status_code in ('AC','AB')
                             OR (csh.status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                             OR (csh.status_code='CF' AND prev_status_code='PC'                                  )
                             OR (csh.status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                            )
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt <= @endDate
                   GROUP BY  CSH.account_number
                 )AS BB ON BB.account_number = BASE.account_number


UPDATE MMS_2017_06
   SET BB_latest_act_date = CASE WHEN BB_latest_act_date IS NULL THEN BB_first_act_date ELSE BB_latest_act_date END
-----**********SKYTALK*******************---
-- SkyTalk_first_act_date
-- SkyTalk_latest_act_date
UPDATE MMS_2017_06
   SET BASE.SkyTalk_first_act_date   = SkyTalk.SkyTalk_first_act_date
       ,BASE.SkyTalk_latest_act_date = SkyTalk.SkyTalk_latest_act_date
  FROM MMS_2017_06 AS BASE
       INNER JOIN
                 (
                   SELECT CSH.account_number
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS SkyTalk_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS SkyTalk_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MMS_2017_06 AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'SKY TALK SELECT'
                          AND (csh.status_code = 'A'
                                OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                              )
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt <= @endDate
                   GROUP BY  CSH.account_number
                 )AS SkyTalk ON SkyTalk.account_number = BASE.account_number


UPDATE MMS_2017_06
   SET SkyTalk_latest_act_date = CASE WHEN SkyTalk_latest_act_date IS NULL THEN SkyTalk_first_act_date ELSE SkyTalk_latest_act_date END
-----**********WLR*******************---
-- WLR_first_act_date
-- WLR_latest_act_date
UPDATE MMS_2017_06
   SET BASE.WLR_first_act_date   = SkyTalk.WLR_first_act_date
       ,BASE.WLR_latest_act_date = SkyTalk.WLR_latest_act_date
  FROM MMS_2017_06 AS BASE
       INNER JOIN
                 (
                   SELECT CSH.account_number
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS WLR_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS WLR_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MMS_2017_06 AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'SKY TALK LINE RENTAL'
                          AND csh.status_code IN  ('A','CRQ','PAX')
                          and CSH.status_code_changed = 'Y'
                                                  AND CSH.effective_from_dt <= @endDate
                   GROUP BY  CSH.account_number
                 )AS SkyTalk ON SkyTalk.account_number = BASE.account_number


UPDATE MMS_2017_06
   SET WLR_latest_act_date = CASE WHEN WLR_latest_act_date IS NULL THEN WLR_first_act_date ELSE WLR_latest_act_date END
SELECT 'A08 - Populate Base Table with History Dates'



--------------------------------------------------------------- A11 - Populate Base Table with Summary Columns
--------------------------------------------------------------------------------------------------------------*/
--h_fss_v3_group_Description
/*
UPDATE MMS_2017_06
   SET h_fss_v3_group_Description  = 'U Unknown'
UPDATE MMS_2017_06
   SET BASE.h_fss_v3_group_Description  = EVL.value_description
  FROM MMS_2017_06 AS BASE
       INNER JOIN yarlagaddar.Experian_Variable_Lookup AS EVL ON EVL.variable_name  = 'h_fss_v3_group'
       AND EVL.value = BASE.h_fss_v3_group
--h_age_coarse
UPDATE MMS_2017_06
   SET h_age_coarse_Description  = 'Unclassified'
UPDATE MMS_2017_06
   SET BASE.h_age_coarse_Description  = EVL.value_description
  FROM MMS_2017_06 AS BASE
       INNER JOIN yarlagaddar.Experian_Variable_Lookup AS EVL ON EVL.variable_name  = 'h_age_coarse'
       AND EVL.value = CAST(BASE.h_age_coarse AS VARCHAR(100))
--h_income_band_v2
UPDATE MMS_2017_06
   SET h_income_band_v2_Description  = 'Unallocated'
UPDATE MMS_2017_06
   SET BASE.h_income_band_v2_Description  = EVL.value_description
  FROM MMS_2017_06 AS BASE
       INNER JOIN yarlagaddar.Experian_Variable_Lookup AS EVL ON EVL.variable_name  = 'h_income_band_v2'
       AND EVL.value = CAST(BASE.h_income_band_v2 AS VARCHAR(100))

--BB_type
UPDATE MMS_2017_06
   SET BB_type_Description  = ''
UPDATE MMS_2017_06
   SET BASE.BB_type_Description  = EVL.value_description
  FROM MMS_2017_06 AS BASE
       INNER JOIN yarlagaddar.Experian_Variable_Lookup AS EVL ON EVL.variable_name  = 'BB_talk_type'
       AND EVL.value = BASE.BB_type

--SkyTalk_type
UPDATE MMS_2017_06
   SET SkyTalk_type_Description  = ''
UPDATE MMS_2017_06
   SET BASE.SkyTalk_type_Description  = EVL.value_description
  FROM MMS_2017_06 AS BASE
       INNER JOIN yarlagaddar.Experian_Variable_Lookup AS EVL ON EVL.variable_name  = 'Sky_talk_type'
       AND EVL.value = BASE.SkyTalk_type
*/
--Product Holding
UPDATE MMS_2017_06
   SET Product_Holding =  CASE WHEN DTV = 1 AND  BroadBand = 1 AND SkyTalk = 1  AND WLR = 1     THEN 'B. DTV + Triple play'
                               WHEN DTV = 1 AND  BroadBand = 1 AND SkyTalk = 0  AND WLR = 0     THEN 'C. DTV + BB Only'
                               WHEN DTV = 1 AND  (BroadBand+SkyTalk + WLR) > 0                  THEN 'D. DTV + Other Comms'
                               WHEN DTV = 1 AND  BroadBand = 0 AND SkyTalk = 0  AND WLR = 0     THEN 'A. DTV Only'
                               WHEN DTV = 0 AND  BroadBand = 1                                  THEN 'E. SABB'
                            ELSE 'Other' END
SELECT 'A11 - Populate Base Table with Summary Columns'
--------------------------------------------------------------- A13 - Populate Base Table with CVS Segment
--------------------------------------------------------------------------------------------------------------*/

CREATE TABLE #Value_Segments_Temp(
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
       ,churn                   integer         default 0
       ,product_holding         varchar(20)     null
)
COMMIT
CREATE   HG INDEX idx01 ON #Value_Segments_Temp(account_number)
CREATE DATE INDEX idx02 ON #Value_Segments_Temp(target_date)
CREATE   LF INDEX idx03 ON #Value_Segments_Temp(segment)
CREATE   HG INDEX idx04 ON #Value_Segments_Temp(subscription_id)

--Insert accounts into Base Table
INSERT INTO #Value_Segments_Temp (account_number, target_date)
 select   account_number , Observation_dt
     from  MMS_2017_06
   -- where status_code = 'AC'
       --   AND  DTV = 1 AND COUNTRY <> 'ROI'
--Define Activation Dates
UPDATE  #Value_Segments_Temp
   SET  first_activation_dt   = sav.ph_subs_first_activation_dt
       ,subscription_id       = sav.prod_ph_subs_subscription_id
  FROM #Value_Segments_Temp AS acc
       INNER JOIN  cust_single_account_view AS sav ON acc.account_number = sav.account_number

UPDATE #Value_Segments_Temp
   SET active_days = DATEDIFF(day,first_activation_dt,target_date)

--Create Events Table
  SELECT account_number, subscription_id, MAX(target_date) as maxDate, MIN(target_date) as minDate
    INTO #account_list_CVS_Segment
    FROM #Value_Segments_Temp
GROUP BY account_number, subscription_id
commit
CREATE UNIQUE HG INDEX idx01 ON #account_list_CVS_Segment(account_number)
CREATE HG INDEX idx02 ON #account_list_CVS_Segment(subscription_id)


CREATE TABLE #status_events_temp (
        id                      BIGINT          IDENTITY     PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,effective_from_dt       DATE            NOT NULL
       ,status_code             VARCHAR(2)      NOT NULL
       ,event_type              VARCHAR(20)     NOT NULL
)

CREATE   HG INDEX idx01 ON #status_events_temp(account_number)
CREATE   LF INDEX idx02 ON #status_events_temp(event_type)
CREATE DATE INDEX idx03 ON #status_events_temp(effective_from_dt)

INSERT INTO #status_events_temp (account_number, effective_from_dt, status_code, event_type)
SELECT  csh.account_number
       ,csh.effective_from_dt
       ,csh.status_code
       ,CASE WHEN status_code = 'PO'              THEN 'CUSCAN'
             WHEN status_code = 'SC'              THEN 'SYSCAN'
             WHEN status_code = 'AB'              THEN 'ACTIVE BLOCK'
             WHEN status_code = 'PC'              THEN 'PENDING CANCEL'
         END AS event_type
  FROM  cust_subs_hist AS csh
       INNER JOIN #account_list_CVS_Segment AS al ON csh.account_number = al.account_number
 WHERE csh.subscription_sub_type = 'DTV Primary Viewing'
   AND csh.status_code_changed = 'Y'
   AND csh.effective_from_dt <= al.maxDate
   AND (    (csh.status_code IN ('AB','PC') AND csh.prev_status_code = 'AC')
         OR (csh.status_code IN ('PO','SC') AND csh.prev_status_code IN ('AC','AB','PC'))
       )
UPDATE  #Value_Segments_Temp
   SET  CUSCAN_ever             = tgt.CUSCAN_ever
       ,CUSCAN_2Yrs             = tgt.CUSCAN_2Yrs
       ,SYSCAN_ever             = tgt.SYSCAN_ever
       ,SYSCAN_2Yrs             = tgt.SYSCAN_2Yrs
       ,AB_ever                 = tgt.AB_ever
       ,AB_2Yrs                 = tgt.AB_2Yrs
       ,PC_ever                 = tgt.PC_ever
       ,PC_2Yrs                 = tgt.PC_2Yrs
  FROM #Value_Segments_Temp AS base
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
                      FROM #Value_Segments_Temp AS vs
                           INNER JOIN #status_events_temp AS se ON vs.account_number = se.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id


SELECT  DISTINCT
        cca.account_number
       ,cca.attempt_date
  INTO #ta_temp
  FROM  cust_change_attempt AS cca
       INNER JOIN #account_list_CVS_Segment AS al  on cca.account_number = al.account_number
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
   AND cca.attempt_date BETWEEN DATEADD(day,-729,al.minDate) AND al.maxDate

COMMIT
CREATE HG INDEX idx01 ON #ta_temp(account_number)

UPDATE  #Value_Segments_Temp
   SET  TA_2Yrs = tgt.ta_2Yrs
  FROM #Value_Segments_Temp AS base
       INNER JOIN (
                    SELECT vs.id
                          ,SUM(CASE WHEN #ta_temp.attempt_date BETWEEN DATEADD(day,-729,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS ta_2Yrs
                      FROM #Value_Segments_Temp AS vs
                           INNER JOIN #ta_temp AS #ta_temp ON vs.account_number = #ta_temp.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id


UPDATE  #Value_Segments_Temp
   SET  min_prem_2Yrs = tgt.min_prem_lst_2_yrs
       ,max_prem_2Yrs = tgt.max_prem_lst_2_yrs
  FROM  #Value_Segments_Temp AS acc
        INNER JOIN (
                   SELECT  base.id
                          ,MAX(cel.prem_movies + cel.prem_sports ) as max_prem_lst_2_yrs
                          ,MIN(cel.prem_movies + cel.prem_sports ) as min_prem_lst_2_yrs
                     FROM  cust_subs_hist as csh
                          INNER JOIN #Value_Segments_Temp as base on csh.account_number = base.account_number
                          INNER JOIN  cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
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
        )AS tgt ON acc.id = tgt.id

UPDATE #Value_Segments_Temp
   SET       segment =     CASE WHEN active_days < 729                            -- All accounts in first 2 Years
                                THEN 'BEDDING IN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No #ta_temp's in last 2 years
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
                                  OR AB_2Yrs + PC_2Yrs + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + #ta_temp in last 2 years >= 3
                                THEN 'UNSTABLE'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE'

                                ELSE 'COPPER'                                        -- everyone else
                            END

-- Update Base Monthly Attachment Table
UPDATE MMS_2017_06
   SET CVS_segment  = NULL

UPDATE MMS_2017_06
   SET BASE.CVS_segment  = Temp.segment
  FROM MMS_2017_06 AS BASE
       INNER JOIN  #Value_Segments_Temp AS Temp
       ON BASE.account_number = Temp.account_number
  --where BASE.status_code = 'AC' AND BASE.DTV = 1 AND BASE.COUNTRY <> 'ROI'
DROP TABLE #Value_Segments_Temp
DROP TABLE #ta_temp
DROP TABLE #status_events_temp
DROP TABLE #account_list_CVS_Segment

  SELECT 'A13 - Populate Base Table with CVS Segment'

----------------------------------------------------------------------------------------------------------------------------------

-- PHASE II - RS MODELS

alter table MMS_2017_06
  add movies_num_downgrade_24m          integer         default 0,
  add HD_upgrade                        integer         default 0,
  add HD_downgrade                      integer         default 0,
  add num_sports_events                 integer         DEFAULT 0,
  add num_cust_calls_in_12m             integer         DEFAULT 0,
  add sp_device_vol_12m                 integer         DEFAULT 0,
  add ppv_count                         integer         DEFAULT 0,
  add last_MU_FP_date                   date            null,
  add last_SU_FP_date                   date            null,
  add num_sports_num_downgrade_24m      integer         null,
  add num_movies_num_upgrade_24m        integer         null,
  add num_premium_upgrade_ever          integer         null,
  add tv_offer_end_dt                   date            null,
  add nlp                               tinyint         default 0,
  add implied_local_loop                decimal(20,10)  null,
  add x_skyfibre_enabled_date           date            null


-- Fibre Enablement Date
 update MMS_2017_06 a
  set  a.x_skyfibre_enabled_date = b. x_skyfibre_enabled_date
   from BT_FIBRE_POSTCODE b
    where a.postcode = b.cb_address_postcode -- postcode is cb_address_postcode


-- Implied Local Loop Length
SELECT  base.account_number
       ,cns.implied_local_loop_length
INTO #temp_exchange_dist
FROM MMS_2017_06 base inner join Broadband_postcode_exchange cns
                                      on base.postcode = cns.cb_address_postcode

UPDATE MMS_2017_06 base
SET base.implied_local_loop = source.implied_local_loop_length
FROM #temp_exchange_dist source
WHERE base.account_number = source.account_number



-- NLP
  select  bbs.account_number
         ,cns.status_start_dt
         ,cns.effective_from_dt
         ,cns.current_product_description as NLP_type
         ,rank() over(partition by bbs.account_number, current_product_description ORDER BY effective_from_dt desc, cb_row_id desc) as prank
    INTO #temp_nlp_sales
    FROM CUST_NON_SUBSCRIPTIONS as cns
         inner join MMS_2017_06 bbs on cns.account_number = bbs.account_number
   WHERE current_product_description in ('Additional Phone Line Installation'
                                        ,'New Phone Line Installation'
                                        ,'Phone Line Activation'
                                        ,'Transfer from Unbundled Phone Line')
     and effective_from_dt <= @endDate
     and effective_from_dt < effective_to_dt

DELETE FROM  #temp_nlp_sales WHERE prank > 1

UPDATE MMS_2017_06 as a
SET a.NLP=1
FROM #temp_nlp_sales as b
WHERE a.account_number=b.account_number


-- TV OFFER EXPIRY
select distinct a.account_number
      ,offer_dim_description
      ,initial_effective_dt
      ,offer_start_dt_actual as offer_start_dt
      ,offer_end_dt_actual as offer_end_dt
      ,subs_type as offer_type
      ,offer_dim_description as sky_product
      ,offer_id
      ,rank () over (partition by a.account_number  order by intended_offer_end_dt desc, offer_start_dt_actual desc, sky_product ) as rank1
into --drop table
        #tv_offers
from citeam. offer_usage_all a
  inner join MMS_2017_06 b
   on a.account_number=b.account_number
   and a.offer_start_dt_actual <= @endDate
   and a.offer_end_dt_actual   > @endDate
   and a.intended_offer_end_dt > @endDate
   where a.offer_dim_description not like '%PRICE PROTECTION%'
   and a.offer_segment <> '3.Price Protection'
   and a.offer_value < 0
   and a.subs_type='DTV Primary Viewing'

delete from #tv_offers where rank1>1

update MMS_2017_06 a
 set a.tv_offer_end_dt          = b.offer_end_dt
 from #tv_offers b
  where a.account_number=b.account_number


-- Number of Premium Upgrades ever /  Number of Movie Upgrades in last 2 years / sports_num_downgrade_24m
 select csh.account_number
         ,csh.effective_from_dt
         ,csh.effective_to_dt
         ,csh.status_code
         ,csh.prev_status_code
         ,csh.status_code_changed
         ,csh.subscription_sub_type
         ,csh.status_start_dt
         ,csh.current_product_sk
         ,csh.first_activation_dt
         ,csh.created_dt
         ,csh.order_id
         ,csh.current_product_description
         ,csh.cb_row_id
         ,csh.current_short_description
         ,csh.previous_short_description
         ,csh.ent_cat_product_id
         ,csh.prev_ent_cat_product_id
         ,csh.ent_cat_prod_start_dt
         ,csh.ent_cat_prod_end_dt
         ,csh.sale_type
         ,csh.ent_cat_prod_changed
         ,csh.technology_code
         ,csh.subscription_id
         ,csh.CREATED_BY_ID
         ,csh.SALE_RETAILER_ID
         ,csh.CURRENCY_CODE
         ,case when subscription_sub_type = 'DTV Primary Viewing'
                and csh.status_code = 'AC'
                and csh.prev_status_code NOT IN ('AB','PC')
                and status_code_changed = 'Y'
                and effective_from_dt <= @endDate then effective_from_dt else null end
                 as activation_date --for C1
     into --drop table
                csh_cut_MASTER
     from cust_subs_hist  as csh
          inner join MMS_2017_06 as bas
       on csh.account_number = bas.account_number

SELECT 'END CSH_CUT'

SELECT account_number
,sale_type
,effective_from_dt
,effective_to_dt
,current_short_description
,previous_short_description
,ent_cat_product_id
,prev_ent_cat_product_id
,ent_cat_prod_start_dt
,ent_cat_prod_end_dt
,csh.cb_row_id
,ccl.REVENUE_GBP as new_rev
,ccl.REVENUE_GBP as old_rev
,ccl.prem_sports as new_prem_sports
,pcl.prem_sports as old_prem_sports
,ccl.prem_movies as new_prem_movies
,pcl.prem_movies as old_prem_movies
,ccl.mixes as new_mixes
,pcl.mixes as old_mixes
,case when (new_prem_sports + new_prem_movies) > (old_prem_sports + old_prem_movies) then 1 else 0 end as premium_upgrade
,case when (new_prem_sports + new_prem_movies) < (old_prem_sports + old_prem_movies) then 1 else 0 end as premium_downgrade
,case when new_mixes > old_mixes then 1 else 0 end as mix_upgrade
,case when new_mixes < old_mixes then 1 else 0 end as mix_downgrade
,case when new_prem_sports < old_prem_sports  then 1 else 0 end as Sports_downgrade
,case when new_prem_movies < old_prem_movies  then 1 else 0 end as Movies_downgrade --DP amended
,case when old_prem_sports + old_prem_movies > 1 and new_prem_sports + new_prem_movies = 0 then 1 else 0 end as Prem_2_no_prem
,case when new_prem_sports > 1 and new_prem_movies = 0 and old_prem_sports + old_prem_movies = 0 then 1 else 0 end as Sports_2_no_prem
,case when new_prem_sports = 0 and new_prem_movies > 1 and old_prem_sports + old_prem_movies = 0 then 1 else 0 end as Movies_2_no_prem
,case when new_prem_movies > old_prem_movies then 1 else 0 end as Movies_upgrade
,case when new_prem_sports > old_prem_sports then 1 else 0 end as Sports_upgrade
INTO #temp_package_movements
FROM csh_cut_MASTER as csh
inner join cust_entitlement_lookup as ccl on csh.current_short_description  = ccl.short_description
inner join cust_entitlement_lookup as pcl on csh.previous_short_description = pcl.short_description
WHERE subscription_sub_type = 'DTV Primary Viewing'
  and status_code in ('AC','AB','PC')
  and effective_from_dt       <= @endDate
  and ent_cat_prod_changed    = 'Y'
  and prev_ent_cat_product_id <> '?'
  and effective_from_dt       <> effective_to_dt
  and first_activation_dt     < '9999-09-09'


SELECT account_number
      ,sum(case when (effective_from_dt <= @endDate and premium_upgrade   = 1) then 1 else 0 end)
                 as num_premium_upgrade_ever
      ,sum(case when (effective_from_dt between dateadd(mm,-24,@endDate) and @endDate and movies_upgrade=1)   then 1 else 0 end)
                 as movies_num_upgrade_24m --DP
      ,sum(case when (effective_from_dt between dateadd(mm,-24,@endDate) and @endDate and sports_downgrade=1) then 1 else 0 end)
                 as sports_num_downgrade_24m --DP
      ,sum(case when (effective_from_dt between dateadd(mm,-24,@endDate) and @endDate and movies_downgrade=1) then 1 else 0 end)
                 as movies_num_downgrade_24m --DP
  INTO #temp_package_movements1
FROM #temp_package_movements
GROUP BY account_number


UPDATE MMS_2017_06 as a
SET  a.num_premium_upgrade_ever         = b.num_premium_upgrade_ever,
     a.num_movies_num_upgrade_24m       = b.movies_num_upgrade_24m, --DP
     a.num_sports_num_downgrade_24m     = b.sports_num_downgrade_24m, --DP
     a.movies_num_downgrade_24m         = b.movies_num_downgrade_24m
FROM #temp_package_movements1 as b
WHERE a.account_number = b.account_number

drop table #temp_package_movements1
drop table #temp_package_movements


-- Recency of Last Movies / Sports FP Upgrade select event_dt, count(*) from citeam.view_cust_package_movements_hist group by event_dt
select distinct a.account_number,
       a.event_dt,
       b.campaign_code,
       b.campaign_id,
       b.offer_dim_description,
       case when upper(offer_dim_description) like '%6 MONTH%' then '6'
            when upper(offer_dim_description) like '%3 MONTH%' then '3'
            when upper(offer_dim_description) like '%2 MONTH%' then '2'
            when upper(offer_dim_description) like '%12 MONTH%' then '12'
            when upper(offer_dim_description) like '%24 MONTH%' then '24'
            when upper(offer_dim_description) like '%1 MONTH%' then '1'
            when upper(offer_dim_description) like '%PRICE PROTECTION%' then 'PP'
            when upper(offer_dim_description) is not null then 'Other'
            end as offer_type,
       b.offer_amount,
       b.offer_Start_dt,
       b.offer_end_dt
       into --drop table
                #SU_Offers
       from citeam.view_cust_package_movements_hist a
       left join cust_product_offers b
       on a.account_number = b.account_number
       and b.offer_start_dt = a.event_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%SPORTS%')
        and a.typeofevent = 'SU'


select a.account_number,
       max(case when offer_type is null and event_dt<@endDate then event_dt end)     as last_SU_FP_date
into --drop table
        #su_offers2
from #SU_Offers a
 inner join MMS_2017_06 c
        on a.account_number= c.account_number
         and a.event_dt    < @endDate
 group by a.account_number

update MMS_2017_06 a
 set   a.last_SU_FP_date    = b.last_SU_FP_date
 from #su_offers2 b
  where a.account_number=b.account_number


-- Movies
select distinct a.account_number,
       a.event_dt,
       b.campaign_code,
       b.campaign_id,
       b.offer_dim_description,
       case when upper(offer_dim_description) like '%6 MONTH%' then '6'
            when upper(offer_dim_description) like '%3 MONTH%' then '3'
            when upper(offer_dim_description) like '%2 MONTH%' then '2'
            when upper(offer_dim_description) like '%12 MONTH%' then '12'
            when upper(offer_dim_description) like '%24 MONTH%' then '24'
            when upper(offer_dim_description) like '%1 MONTH%' then '1'
            when upper(offer_dim_description) like '%PRICE PROTECTION%' then 'PP'
            when upper(offer_dim_description) is not null then 'Other'
            end as offer_type,
       b.offer_amount,
       b.offer_Start_dt,
       b.offer_end_dt
       into --drop table
                #MU_Offers
       from citeam.view_cust_package_movements_hist a
       left join cust_product_offers b
       on a.account_number = b.account_number
       and b.offer_start_dt = a.event_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%MOVIES%'
			OR upper(b.offer_dim_description) like '%CINEMA%')
        and a.typeofevent = 'MU'

select a.account_number,
       max(case when offer_type is null and event_dt<@endDate then event_dt end)     as last_MU_FP_date
into --drop table
        #MU_offers2
from #MU_Offers a
 inner join MMS_2017_06 c
        on a.account_number = c.account_number
         and a.event_dt     < @endDate
 group by a.account_number

update MMS_2017_06 a
 set    a.last_MU_FP_date    = b.last_MU_FP_date
 from #MU_offers2 b
  where a.account_number=b.account_number


-- History of PPV Usage
select a.account_number, count(*) as volumes
 into --drop table
        #ppv
 FROM MMS_2017_06 a
 inner join CUST_PRODUCT_CHARGES_PPV b
        on a.account_number=b.account_number
WHERE b.ppv_cancelled_dt='9999-09-09'
  and b.ppv_viewed_dt<=@endDate
  group by a.account_number

update MMS_2017_06  a
 set ppv_count = b.volumes
 from #ppv b
 where a.account_number=b.account_number


-- SP Devices Used 12 Months
select a.account_number,
       count(distinct site_name) as sp_device_vol_12m
into #sp
from SKY_PLAYER_USAGE_DETAIL a
 inner join MMS_2017_06 b
  on a.account_number=b.account_number
 where activity_dt>= (@endDate-365)
   and activity_dt<= @endDate
  group by a.account_number

update MMS_2017_06 a
 set a.sp_device_vol_12m     = b.sp_device_vol_12m
from #sp b
 where a.account_number=b.account_number


--Inbound Customer Calls in last 12 months
  SELECT base.account_number
        ,cc.created_dt
    INTO #temp_call_density
    FROM MMS_2017_06 as base
         inner join CUST_CONTACT as cc
                on base.account_number     = cc.account_number

   WHERE contact_channel = 'I PHONE COMMUNICATION' --SELECT only inbound calls
     and contact_grouping_identity is not null       --
                and cc.created_dt<=@endDate

SELECT account_number
      ,sum(case when created_dt between dateadd(mm,-12,@endDate) and @endDate then 1 else 0 end) as num_cust_calls_in_12m
INTO #temp_call_density_1
FROM #temp_call_density
GROUP BY account_number


UPDATE MMS_2017_06 base
SET     base.num_cust_calls_in_12m = source.num_cust_calls_in_12m
FROM #temp_call_density_1 source
WHERE base.account_number = source.account_number


-- PPV Sports Events
SELECT a.account_number
      ,sum(case when ppv_viewed_dt between dateadd(mm,-12,@endDate) and @endDate and ppv_service='EVENT'
                      --and  ppv_genre = 'BOXING, FOOTBALL or WRESTLING'
                      and ppv_cancelled_dt = '9999-09-09' then 1 else 0 end) as num_sport_events_12m
   INTO --drop table
        #temp_ppv
FROM MMS_2017_06 a
inner join CUST_PRODUCT_CHARGES_PPV b
on a.account_number=b.account_number
WHERE b.ppv_cancelled_dt='9999-09-09'
   and b.ppv_viewed_dt<=@endDate
   and b.ppv_viewed_dt>=(@endDate-365)
GROUP BY a.account_number

UPDATE MMS_2017_06 as a
SET  a.num_sports_events = b.num_sport_events_12m
FROM #temp_ppv as b
WHERE a.account_number = b.account_number



-- PHASE II MH Models
-- Sports
alter table MMS_2017_06
 add PremMovies2                                tinyint         default 0,
 add Flag_BAK_12m                               Smallint        default 0,
 add ppv_12m                                    Smallint        default 0,
 add PPV_BAK_Flag                               varchar(20)     default null,
 Add Movies_sp_device_vol_12m_Cap               Varchar(20)     default null,
 Add Movies_sp_device_vol_12m_Cap4              Varchar(2)      default null,
 Add sp_device_vol_12m_mh                       Integer         default 0,
 Add Flag_Movies_DG_12m                         smallint        default 0,
 add Movies_Num_pat_12m_Cap_3                   integer         default 0,
 Add Movies_Previous_Target_Upgrade_Type        Varchar(20)     default 'Never Upgrade',
 add HD_Sub                                     tinyint         default 0,
 add Sports_num_pat_12m_Flag                    tinyint         default 0,
 add Sports_Last_Target_Upgrade_Date            Date            default Null,
 add Sports_Last_Target_Upgrade_Offer_Date      Date            default Null,
 add Sports_Previous_Target_Upgrade_Type        Varchar(20)     default 'Never Sports',
 add Sports_DG_Segment                          VarChar(4)      default '0',
 Add SkyPlayer                                  smallint        default 0

-- HD Box
select su.account_number
  into #hda
  from CUST_SET_TOP_BOX  stb
       inner join MMS_2017_06 su
    on su.account_number = stb.account_number
 where box_installed_dt <= @endDate
   and box_replaced_dt   > @endDate
   and current_product_description like '%HD%'

UPDATE MMS_2017_06 b
SET HD_Sub = 1
FROM #hda as s
WHERE s.account_number = b.account_number


-- Sports PAT Attempts
SELECT distinct a.account_number
INTO #temp_pat_attempts
FROM MMS_2017_06 a
         inner join cust_change_attempt as b on a.account_number=b.account_number
WHERE b.change_attempt_type='DOWNGRADE ATTEMPT'
    and b.attempt_date between dateadd(mm,-12,@endDate) and @endDate -- 12 Months
    and b.created_by_id not in ('dpsbtprd','batchuser')
    and b.Wh_Attempt_Outcome_Description_1 in ('Legacy Fail','Legacy Save','Save','No Save','PAT No Save','PAT Other','PAT Save')
    and b.Wh_Attempt_Reason_Description_1<>'Turnaround'

Update MMS_2017_06 as b
Set Sports_num_pat_12m_Flag = 1
  From #temp_pat_attempts as s
   Where b.account_number=s.account_number


-- Sporst Previous Upgrade Type
select distinct
  a.account_number,
  b.campaign_code,
  b.campaign_id,
  b.offer_dim_description,
  b.offer_amount,
  b.offer_Start_dt,
  b.offer_end_dt
into --drop table
        #Upgrade_Offers
from MMS_2017_06 a
   left join cust_product_offers b
     on a.account_number = b.account_number
    and b.offer_start_dt <= @endDate
    and b.offer_end_dt > b.offer_start_dt
    and b.offer_amount < 0
Where (
      (upper(b.offer_dim_description) like '%SPORTS%')
   or (upper(b.offer_dim_description) like '%TOP TIER%')
   or (upper(b.offer_dim_description) like '%TOPTIER%')
      )

SELECT
   csh.account_number
  ,case when ncel.prem_sports > ocel.prem_sports then csh.effective_from_dt else null end as U_Date
  ,case when ncel.prem_sports < ocel.prem_sports then csh.effective_from_dt else null end as D_Date
  ,case when ncel.prem_sports > ocel.prem_sports and csh.effective_from_dt = d.offer_start_dt then 1 else 0 end as U_On_Offer
into #Movements
FROM cust_subs_hist as csh inner join MMS_2017_06  as b on csh.account_number=b.account_number
    Left Join cust_entitlement_lookup as ncel on csh.current_short_description = ncel.short_description
    Left Join cust_entitlement_lookup as ocel on csh.previous_short_description = ocel.short_description
    Left Join #Upgrade_Offers as d on csh.account_number=d.account_number and csh.effective_from_dt = d.offer_start_dt
WHERE   csh.effective_from_dt <= @enddate
    And csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type = 'DTV Primary Viewing'
    AND csh.status_code IN ('AC','PC','AB' )  -- Active records
    AND csh.ent_cat_prod_changed = 'Y' -- The package has changed - VERY IMPORTANT
    And ncel.prem_sports <> ocel.prem_sports


Update MMS_2017_06 as base
Set base.Sports_Last_Target_Upgrade_Date        = source.U
   ,base.Sports_Last_Target_Upgrade_Offer_Date  = source.U_Offer
from (Select Account_Number,  max(U_Date) as U,
             max(case when U_On_Offer = 1 then U_Date end) as U_Offer
             from #Movements Group by Account_Number) as source
Where   base.Account_Number=source.Account_Number


Update MMS_2017_06
Set Sports_Previous_Target_Upgrade_Type = case when Sports_Last_Target_Upgrade_Date is null                                   then 'Never Upgrade'
                                               when Sports_Last_Target_Upgrade_Date = Sports_Last_Target_Upgrade_Offer_Date   then 'Upgrade on OP'
                                               when Sports_Last_Target_Upgrade_Date is not null                               then 'Upgrade on FP' end

-- Sports DG Segment

SELECT
 a.account_number
,csh.effective_from_dt
,ccl.prem_sports as new_prem_sports
,pcl.prem_sports as old_prem_sports
,case when new_prem_sports < old_prem_sports  then 1 else 0 end as Sports_downgrade
INTO #temp_package_movements
FROM MMS_2017_06 as a
    left  join cust_subs_hist          as csh on a.account_number  = csh.account_number
    inner join cust_entitlement_lookup as ccl on csh.current_short_description  = ccl.short_description
    inner join cust_entitlement_lookup as pcl on csh.previous_short_description = pcl.short_description
WHERE csh.subscription_sub_type = 'DTV Primary Viewing'
  and csh.status_code in ('AC','AB','PC')
  and csh.effective_from_dt    <= @enddate
  and csh.ent_cat_prod_changed    = 'Y'
  and csh.prev_ent_cat_product_id <> '?'
  and csh.effective_from_dt     <> effective_to_dt
  and csh.first_activation_dt   < '9999-09-09'
  and Sports_downgrade=1


Update MMS_2017_06 as b
Set Sports_DG_Segment = case when sports_num_downgrade_6m > 0 then 'L6M' when sports_num_downgrade_12m > 0 then 'L12M' else '0' end
from  (SELECT account_number
      ,sum(case when (effective_from_dt between dateadd(mm, -6,@enddate) and @enddate and sports_downgrade=1) then 1 else 0 end) as sports_num_downgrade_6m --DP
      ,sum(case when (effective_from_dt between dateadd(mm,-12,@enddate) and @enddate and sports_downgrade=1) then 1 else 0 end) as sports_num_downgrade_12m --DP
from #temp_package_movements
      Group By
       account_number) as s
Where b.account_number=s.account_number

drop table #temp_package_movements

-- Skyplayer

UPDATE  MMS_2017_06
   SET  BASE.SkyPlayer                    = 1
  FROM MMS_2017_06 AS BASE
       INNER JOIN
                (
                  SELECT CFPH.account_number
                   FROM  CITEAM.VIEW_CUST_FREE_PRODUCTS_HIST AS  CFPH
                         INNER JOIN MMS_2017_06 AS BASE_T ON BASE_T.account_number = CFPH.account_number
                  WHERE  CFPH.TypeOfEvent = 'SG' AND CFPH.subs_activation_date <= @enddate
                  GROUP BY CFPH.account_number
                )AS CFPH_T  ON BASE.account_number = CFPH_T.account_number


-- Movies
select distinct
  a.account_number,
  b.campaign_code,
  b.campaign_id,
  b.offer_dim_description,
  b.offer_amount,
  b.offer_Start_dt,
  b.offer_end_dt
into --drop table
        #Movies_Upgrade_Offers
from MMS_2017_06 a left join cust_product_offers b
  on a.account_number = b.account_number
    and b.offer_start_dt <= @enddate
    and b.offer_end_dt > b.offer_start_dt
    and b.offer_amount < 0
Where (
      (upper(b.offer_dim_description) like '%MOVIES%')
   or (upper(b.offer_dim_description) like '%TOP TIER%')
   or (upper(b.offer_dim_description) like '%CINEMA%')
   or (upper(b.offer_dim_description) like '%TOPTIER%')
      )

SELECT
   csh.account_number
  ,case when ncel.Prem_Movies > ocel.Prem_Movies then csh.effective_from_dt else null end as U_Date
  ,case when ncel.Prem_Movies < ocel.Prem_Movies then csh.effective_from_dt else null end as D_Date
  ,case when ncel.Prem_Movies > ocel.Prem_Movies and csh.effective_from_dt = d.offer_start_dt then 1 else 0 end as U_On_Offer
into #Movies_Movements
FROM cust_subs_hist as csh inner join MMS_2017_06  as b on csh.account_number=b.account_number
    LEFT JOIN cust_entitlement_lookup as ncel on csh.current_short_description = ncel.short_description
    LEFT JOIN cust_entitlement_lookup as ocel on csh.previous_short_description = ocel.short_description
    Left join #Movies_Upgrade_Offers as d on csh.account_number=d.account_number and csh.effective_from_dt = d.offer_start_dt
WHERE   csh.effective_from_dt <= @enddate
    And csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type = 'DTV Primary Viewing'
    AND csh.status_code IN ('AC','PC','AB' )  -- Active records
    AND csh.ent_cat_prod_changed = 'Y' -- The package has changed - VERY IMPORTANT
    And ncel.Prem_Movies <> ocel.Prem_Movies


Select
    account_number
   ,source.U as Movies_Last_Target_Upgrade_Date
   ,source.D as Movies_Last_Target_Downgrade_Date
   ,source.U_Offer as Movies_Last_Target_Upgrade_Offer_Date
   ,datediff(mm,source.U,source.D) as Movies_Last_Target_Tenure_Months
   ,datediff(mm,source.D,@enddate) as Movies_Mths_Since_Target_Downgrade
into #t2
from (Select Account_Number, max(U_Date) as U, max(D_Date) as D, max(case when U_On_Offer = 1 then U_Date end) as U_Offer from #Movies_Movements Group by Account_Number ) as source


Update MMS_2017_06 as b
Set Movies_Previous_Target_Upgrade_Type =
    case when Movies_Last_Target_Upgrade_Date is null                           then 'Never Upgrade'
         when Movies_Last_Target_Upgrade_Date = Movies_Last_Target_Upgrade_Offer_Date  then 'Upgrade on OP'
         when Movies_Last_Target_Upgrade_Date is not null                       then 'Upgrade on FP'
    end
from #T2 as s
Where b.account_number=s.account_number






--Movies num PAT Calls
SELECT a.account_number
      ,b.attempt_date
      ,change_attempt_sk
      ,case when b.Wh_Attempt_Outcome_Description_1 in ('Legacy Save','Save','PAT Save') then 1 else 0 end as saved
INTO --drop table
        #temp_pat_attemptsb
FROM MMS_2017_06 a
    left outer join cust_change_attempt as b
      on a.account_number=b.account_number
WHERE b.change_attempt_type='DOWNGRADE ATTEMPT'
    and b.attempt_date<=@enddate
    and b.created_by_id not in ('dpsbtprd','batchuser')
    and b.Wh_Attempt_Outcome_Description_1 in ('Legacy Fail','Legacy Save','Save','No Save','PAT No Save','PAT Other','PAT Save')
    and b.Wh_Attempt_Reason_Description_1<>'Turnaround'
ORDER BY a.account_number, b.attempt_date, saved desc

SELECT t1.* ,rank() over(partition by account_number, attempt_date ORDER BY change_attempt_sk desc) as sk_rank
INTO --drop table
       #temp_pat_attemptsb1
FROM #temp_pat_attemptsb t1

DELETE FROM #temp_PAT_attemptsb1 WHERE sk_rank>1

SELECT account_number
      ,sum(case when attempt_date between dateadd(mm,-12,@enddate) and @enddate then 1 else 0 end) as num_pat_12m
INTO --drop table
        #temp_pat_attemptsb2
FROM #temp_pat_attemptsb1
GROUP BY account_number

Update MMS_2017_06 as b
Set b.Movies_Num_pat_12m_Cap_3 = case when s.num_pat_12m is null then 0 when s.num_pat_12m >= 1 then 1 else s.num_pat_12m end
from #temp_pat_attemptsb2 as s
where b.account_number=s.account_number


-- Movies Downgrades in Last 12 Months
SELECT a.account_number, max(1) as movies_downgrade_12m
INTO #Sports_dg
FROM MMS_2017_06 as a inner join cust_subs_hist as csh on a.account_number=csh.account_number
      inner join cust_entitlement_lookup as ccl on csh.current_short_description  = ccl.short_description
      inner join cust_entitlement_lookup as pcl on csh.previous_short_description = pcl.short_description
WHERE subscription_sub_type = 'DTV Primary Viewing'
  and ccl.prem_movies < pcl.prem_movies
  and csh.status_code in ('AC','AB','PC')
  and effective_from_dt between dateadd(mm,-12,@enddate) and @enddate
  and ent_cat_prod_changed    = 'Y'
  and prev_ent_cat_product_id <> '?'
  and effective_from_dt     <> effective_to_dt
  and first_activation_dt   < '9999-09-09'
Group by a.account_number


Update MMS_2017_06 as b
Set Flag_Movies_DG_12m =  s.movies_downgrade_12m
from #Sports_dg as s
where b.account_number=s.account_number


-- Movies SP Device Volume
update MMS_2017_06 as b
set b.sp_device_vol_12m_MH = s.n
from (select a.account_number, count(distinct site_name) as n
    from SKY_PLAYER_USAGE_DETAIL a inner join MMS_2017_06 b on a.account_number=b.account_number
    where activity_dt>=(@enddate-365)
      and activity_dt< (@enddate)
    group by a.account_number) as s
Where b.account_number=s.account_number

Update MMS_2017_06
Set Movies_sp_device_vol_12m_Cap4 = case when sp_device_vol_12m_MH > 3 then '4+' else cast(sp_device_vol_12m_MH as varchar(2)) end

Update MMS_2017_06
Set Movies_sp_device_vol_12m_Cap = case when Movies_sp_device_vol_12m_cap4 in ('0','1') then 'a) <=1'
                                        when Movies_sp_device_vol_12m_cap4 in (    '2') then 'b)  =2'
                                                                                        else 'c) >2' end

-- PPV BAK
Update MMS_2017_06 as b
Set b.Flag_BAK_12m = s.Flag_BAK_12m
from (Select Account_Number, max(1) as Flag_BAK_12m, count(*) as N_BAK_12m
      FROM SKY_STORE_TRANSACTIONS  cod
      where   product='EST'
          and cast(order_date as date) > @enddate-365
          and cast(order_date as date) < @enddate
      Group by Account_Number) as s
where b.account_number=s.account_number

Update MMS_2017_06 as b
Set b.ppv_12m=s.ppv_12m
from (SELECT a.account_number ,max(case when ppv_viewed_dt between dateadd(mm,-12,@enddate) and @enddate then 1 else 0 end) as ppv_12m
      FROM MMS_2017_06 a left join CUST_PRODUCT_CHARGES_PPV b on a.account_number=b.account_number
      WHERE b.ppv_cancelled_dt='9999-09-09' and b.ppv_viewed_dt <= @enddate and b.ppv_viewed_dt >= (@enddate-365)
      GROUP BY a.account_number
     ) as s
Where b.account_number=s.account_number

Update MMS_2017_06 as b
Set PPV_BAK_Flag = case when Flag_BAK_12m+ppv_12m = 0 then 'None'
                        when Flag_BAK_12m+ppv_12m = 2 then 'Both'
                                                      else 'Either' end



---- MH TT MODEL ----
alter table MMS_2017_06
 add Movies_Last_Target_Upgrade_Offer_Date              Date Default Null,
 add num_pat_12m                                        integer default 0,
 add TopTier_Previous_Target_Upgrade_Type               Varchar(20) Default 'Never',
 add TopTier_Last_Target_Upgrade_Date                   Date Default Null,
 Add TopTier_Mths_Since_Target_Downgrade_Grouped        varchar(100),
 add TopTier_Mths_Since_Target_Downgrade                integer default 9999



-- PAT
 SELECT a.account_number
      ,b.owning_cust_account_id
      ,b.created_by_id
      ,b.attempt_date
      ,b.attempt_datetime
      ,b.cb_row_id
      ,b.change_attempt_sk
      ,b.change_attempt_type
      ,b.cb_key_household
      ,b.cb_key_family
      ,b.cb_key_individual
      ,b.Wh_Attempt_Outcome_Description_1
      ,b.Wh_Attempt_Outcome_Description_2
      ,b.Wh_Attempt_Outcome_Description_3
      ,b.Wh_Attempt_Outcome_Description_4
      ,b.Wh_Attempt_Reason_Description_1
      ,b.Wh_Attempt_Reason_Description_2
      ,b.Wh_Attempt_Reason_Description_3
      ,b.Wh_Attempt_Reason_Description_4
      ,b.order_id
      ,case when b.Wh_Attempt_Outcome_Description_1 in ('Legacy Save','Save','PAT Save') then 1 else 0 end as saved
INTO --drop table
        #temp_pat_attempts_TT
FROM MMS_2017_06 a
left outer join
cust_change_attempt as b
on a.account_number=b.account_number
WHERE b.change_attempt_type='DOWNGRADE ATTEMPT'
and b.attempt_date<=@enddate
and b.created_by_id not in ('dpsbtprd','batchuser')
and b.Wh_Attempt_Outcome_Description_1 in ('Legacy Fail','Legacy Save','Save','No Save','PAT No Save','PAT Other','PAT Save')
and b.Wh_Attempt_Reason_Description_1<>'Turnaround'
ORDER BY a.account_number, @enddate, b.attempt_date, saved desc

SELECT t1.*
      ,rank() over(partition by account_number, attempt_date ORDER BY change_attempt_sk desc) as sk_rank
INTO --drop table
        #temp_pat_attempts_TT1
FROM #temp_pat_attempts_TT t1

DELETE FROM #temp_pat_attempts_TT1 WHERE sk_rank>1

--pat attempt summary
SELECT account_number
      ,sum(case when attempt_date between dateadd(mm,-12,@enddate) and @enddate then 1 else 0 end) as num_pat_12m
INTO --drop table
        #temp_pat_attempts_TT2
FROM #temp_pat_attempts_TT1
GROUP BY account_number

update MMS_2017_06 a
 set a.num_pat_12m = b.num_pat_12m
 from #temp_pat_attempts_TT2 b
  where a.account_number=b.account_number

-- MOVEMENTS
select distinct
  a.account_number,
  b.campaign_code,
  b.campaign_id,
  b.offer_dim_description,
  b.offer_amount,
  b.offer_Start_dt,
  b.offer_end_dt
into --drop table
        #Upgrade_Offers2
from MMS_2017_06 a left join cust_product_offers b
  on a.account_number = b.account_number
    and b.offer_start_dt <= @enddate
    and b.offer_end_dt > b.offer_start_dt
    and b.offer_amount < 0
Where ((upper(b.offer_dim_description) like '%SPORTS%')
   or (upper(b.offer_dim_description) like '%MOVIES%')
   or (upper(b.offer_dim_description) like '%CINEMA%')
   or (upper(b.offer_dim_description) like '%TOP TIER%')
   or (upper(b.offer_dim_description) like '%TOPTIER%') )

SELECT
   csh.account_number
  ,case when ncel.Prem_Movies > ocel.Prem_Movies then csh.effective_from_dt else null end as Movies_Upg_Date
  ,case when ncel.Prem_Sports > ocel.Prem_Sports then csh.effective_from_dt else null end as Sports_Upg_Date
  ,case when ncel.Prem_Movies < ocel.Prem_Movies then csh.effective_from_dt else null end as Movies_Dg_Date
  ,case when ncel.Prem_Sports < ocel.Prem_Sports then csh.effective_from_dt else null end as Sports_Dg_Date

  ,case when ncel.Prem_Movies > ocel.Prem_Movies or ncel.Prem_Sports > ocel.Prem_Sports then csh.effective_from_dt else null end as Any_Prem_UG_Date
  ,case when ncel.Prem_Movies < ocel.Prem_Movies or ncel.Prem_Sports < ocel.Prem_Sports then csh.effective_from_dt else null end as Any_Prem_DG_Date

  ,case when ncel.Prem_Movies > ocel.Prem_Movies or ncel.Prem_Sports > ocel.Prem_Sports
                                           and csh.effective_from_dt = d.offer_start_dt then 1 else 0 end as Last_UG_On_Offer
  ,case when ncel.Prem_Movies > ocel.Prem_Movies
                                           and csh.effective_from_dt = d.offer_start_dt then csh.effective_from_dt end as Last_MOVIES_UG_On_Offer -- CHECK
into --drop table
        #Movements_TT
FROM cust_subs_hist as csh inner join MMS_2017_06  as b on csh.account_number=b.account_number
    LEFT JOIN cust_entitlement_lookup as ncel on csh.current_short_description = ncel.short_description
    LEFT JOIN cust_entitlement_lookup as ocel on csh.previous_short_description = ocel.short_description
    left join #Upgrade_Offers2 as d on csh.account_number=d.account_number and csh.effective_from_dt = d.offer_start_dt
WHERE   csh.effective_from_dt <= @enddate
    And csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type = 'DTV Primary Viewing'
    AND csh.status_code IN ('AC','PC','AB' )  -- Active records
    AND csh.ent_cat_prod_changed = 'Y' -- The package has changed - VERY IMPORTANT


Update MMS_2017_06 as base
Set base.TopTier_Last_Target_Upgrade_Date = source.U,
    base.TopTier_Mths_Since_Target_Downgrade = datediff(mm,source.D,@enddate),
    base.Movies_Last_Target_Upgrade_Offer_Date = source.x -- CHECK
from (Select Account_Number, max(Any_Prem_UG_Date) as U, max(Any_Prem_DG_Date) as D, max(Last_MOVIES_UG_On_Offer) as x,
             max(case when Last_UG_On_Offer = 1 then Any_Prem_UG_Date end) as U_Offer
             from #Movements_TT Group by Account_Number) as source
Where   base.Account_Number=source.Account_Number


Update MMS_2017_06
Set TopTier_Previous_Target_Upgrade_Type = case when TopTier_Last_Target_Upgrade_Date is null                           then 'Never Upgrade'
                                                when TopTier_Last_Target_Upgrade_Date = Movies_Last_Target_Upgrade_Offer_Date  then 'Upgrade on OP'
                                                when TopTier_Last_Target_Upgrade_Date is not null                       then 'Upgrade on FP' end


-- TT Downgrade Months
Update MMS_2017_06
Set TopTier_Mths_Since_Target_Downgrade_Grouped = case
                                  when TopTier_Mths_Since_Target_Downgrade <= 2  then 'a) DG  <= 2M'
                                  when TopTier_Mths_Since_Target_Downgrade <= 6  then 'b) DG  3-6M'
                                  when TopTier_Mths_Since_Target_Downgrade <= 18 then 'c) DG 7-18M'
                                  else 'd) DG 18+ / Never ' end




-- MS Model
  SELECT account_number
        ,MAX(CASE WHEN subscription_sub_type = 'DTV HD'                 THEN 1 ELSE 0 END) AS HD_downgrade
    INTO #temp_hd_mr_downgrades
    FROM csh_cut_MASTER
   WHERE (subscription_sub_type = 'DTV HD')
     and status_code in ('PO','SC')
     and prev_status_code in ('PC','AB','AC')
     and status_code_changed = 'Y'
     and effective_from_dt <= @enddate
GROUP BY account_number

UPDATE MMS_2017_06 base
SET      base.HD_downgrade = source.HD_downgrade
FROM #temp_hd_mr_downgrades source
WHERE base.account_number = source.account_number


SELECT account_number
       ,CASE WHEN subscription_sub_type = 'DTV HD'                 THEN 'HD upgrade'
                  ELSE 'Unknown'
             END AS Upgrade_type
       ,effective_from_dt as upgrade_dt
       ,order_id
INTO #temp_hd_mr_upgrades
FROM csh_cut_MASTER
WHERE (subscription_sub_type = 'DTV HD')
and   status_code in ('AC','IT')
and   prev_status_code not in ('AC','IT')
and   status_code_changed = 'Y'
and   effective_from_dt <= @enddate

SELECT  account_number
       ,min(first_activation_dt) dtv_activation_dt
INTO #temp_dtv_hd
FROM csh_cut_MASTER
WHERE (subscription_sub_type = 'DTV Primary Viewing')
and   effective_from_dt <= @enddate
GROUP BY account_number

UPDATE MMS_2017_06 base
SET      base.HD_upgrade = source.HD_upgrade
FROM (SELECT  up.account_number
       ,MAX(CASE WHEN up.Upgrade_type = 'HD upgrade' and upgrade_dt > dtv_activation_dt THEN 1 ELSE 0 END) AS HD_upgrade
     FROM  #temp_hd_mr_upgrades up inner join #temp_dtv_hd dtv
                              on   up.account_number = dtv.account_number
     GROUP BY up.account_number) source
WHERE base.account_number = source.account_number


-- Prem Movies 2
  SELECT account_number
        ,prem_sports
        ,prem_movies
        ,Variety
        ,Knowledge
        ,Kids
        ,Style_Culture
        ,Music
        ,News_Events
        ,rank() over(partition by account_number  ORDER BY csh.effective_from_dt desc) as rank
    INTO #temp_AR_3224_Packages
    FROM csh_cut_MASTER as csh
         inner join cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   WHERE csh.subscription_sub_type ='DTV Primary Viewing'
     AND csh.status_code in ('AC','AB','PC')
     AND csh.effective_from_dt <= @enddate
     AND csh.effective_to_dt   >  @enddate

DELETE FROM #temp_AR_3224_Packages WHERE rank > 1

UPDATE MMS_2017_06 a
SET a.PremMovies2 = case when b.prem_Movies = 2 then 1 else 0 end
FROM #temp_AR_3224_Packages b
WHERE b.account_number = a.account_number

drop table csh_cut_MASTER
select 'drop csh_cut_MASTER'

--DS -> TT MODEL
SELECT
 a.account_number
,max(last_modified_dt) as date_last_od
,months_since_last_od = case
 when date_last_od between dateadd(mm, - 3,@enddate) and                 @enddate  then '00-03'
 when date_last_od between dateadd(mm, -12,@enddate) and dateadd(mm, - 3,@enddate) then '03-12'
 when date_last_od between dateadd(mm, -99,@enddate) and dateadd(mm, -12,@enddate) then '12-99'
 else 'Never' end
,sum(case when cast(last_modified_dt as date) between dateadd(mm,- 3,@enddate) and @enddate then 1 else 0 end) as num_od_3m
,sum(case when cast(last_modified_dt as date) between dateadd(mm,-12,@enddate) and @enddate then 1 else 0 end) as num_od_12m
,od_RF = case
 when months_since_last_od  = '00-03' and num_od_3m between 1 and 4 then '00-03:1-4'
 when months_since_last_od  = '00-03' and num_od_3m      >= 5       then '00-03:5--'
 when months_since_last_od <> '00-03'                               then substr(months_since_last_od,1,5) -- trim
 end
INTO #temp_od
FROM MMS_2017_06 a
inner join CUST_ANYTIME_PLUS_DOWNLOADS b
  on a.account_number=b.account_number
WHERE b.last_modified_dt  <= @enddate
GROUP BY a.account_number


update MMS_2017_06 a
set a.od_RF = b.od_RF
from  #temp_od b
where a.account_number = b.account_number



-- FAMILY VARIABLES
alter table MMS_2017_06
 add SP_Live_Recency                    Varchar(12)     default 'c)12+/Never',
 add Flag_premium_upgrade_12m           SmallInt        Default 0,
 add num_ppv_12m                        Integer         default 0


--SP_LIVE_RECENCY
select a.account_number,
       max(case when x_usage_type='Live Viewing' and activity_dt<=@enddate and activity_dt>=(@enddate-180) then 1 else 0 end) as sp_Live_flag_6m,
       max(case when x_usage_type='Live Viewing' and activity_dt<=@enddate and activity_dt>=(@enddate-360) then 1 else 0 end) as sp_Live_flag_12m
into #sp2
from SKY_PLAYER_USAGE_DETAIL
        a inner join MMS_2017_06 b
 on a.account_number=b.account_number
where activity_dt>=(@enddate-365)
group by a.account_number

Update MMS_2017_06 as b
Set b.SP_Live_Recency = case
        when s.sp_Live_flag_6m  > 0  then 'a)L6M'
        when s.sp_Live_flag_12m > 0 then 'b)7-12M'
                                 else 'c)12+/Never' end
from #sp2 as s
where b.account_number=s.account_number


--FLAG PREMIUM UPGRADE 12M
SELECT csh.account_number,
        max(case when (ccl.prem_sports + ccl.prem_movies) > (pcl.prem_sports + pcl.prem_movies) then 1 else 0 end) as premium_upgrade
INTO #temp_package_movements3
FROM  MMS_2017_06 as a
        inner join cust_subs_hist as csh on a.account_number=csh.account_number
      inner join cust_entitlement_lookup as ccl on csh.current_short_description  = ccl.short_description
      inner join cust_entitlement_lookup as pcl on csh.previous_short_description = pcl.short_description
WHERE subscription_sub_type = 'DTV Primary Viewing'
  and (ccl.prem_sports + ccl.prem_movies) > (pcl.prem_sports + pcl.prem_movies)
  and csh.status_code in ('AC','AB','PC')
  and csh.effective_from_dt    <= @enddate
  and csh.effective_to_dt     > @enddate
  and csh.ent_cat_prod_changed    = 'Y'
  and csh.prev_ent_cat_product_id <> '?'
  and csh.effective_from_dt     <> effective_to_dt
  and csh.first_activation_dt   < '9999-09-09'
group by csh.account_number

Update MMS_2017_06 as b
Set b.Flag_premium_upgrade_12m=s.premium_upgrade
from #temp_package_movements3 as s
Where b.account_number=s.account_number

--NUM PPV 12 M
Update MMS_2017_06 as b
Set b.num_ppv_12m=s.num_ppv_12m
from (
      SELECT a.account_number ,sum(case when ppv_viewed_dt between dateadd(mm,-12,@enddate) and @enddate then 1 else 0 end) as num_ppv_12m
      FROM MMS_2017_06 a left join CUST_PRODUCT_CHARGES_PPV b on a.account_number=b.account_number
      WHERE b.ppv_cancelled_dt='9999-09-09' and b.ppv_viewed_dt <= @enddate and b.ppv_viewed_dt >= @enddate-365
      GROUP BY a.account_number
     ) as s
Where b.account_number=s.account_number



----------------------------
-- BT SPORTS VIEWER MODEL --
----------------------------
alter table MMS_2017_06
add dtv_act_date_BT             date            null,
add tenure_BT                   smallint        null,
add Tenure_yrs_BT               DECIMAL (16,2)  null,
add dtv_first_act_date_BT       date            null,
add dtv_latest_act_date_BT      date            null,
add sports_downgrade_date       date            null,
add bb_active                   tinyint         default 0,
add bb_provider                 varchar(20)     null,
add sports_grp                  varchar(20)     null,
add espn                        tinyint         default 0,
add skygoe                      tinyint         default 0,
add dtv_activation_date         date            null,
add dtv_tenure                  integer         null


--ESPN
UPDATE MMS_2017_06
set ESPN = 1
from MMS_2017_06 a
inner join  cust_subs_hist b
on a.account_number = b.account_number
and b.subscription_sub_type = 'ESPN'
and effective_from_dt <= @enddate
and b.status_code = 'AC'


--BB Provider
select account_number,
    cb_key_household,
    case      when subscription_sub_type = 'DTV Primary Viewing' then 1
              else 2
              end as Subs_Type,
    effective_from_dt,
    effective_to_dt,
    status_code,
    prev_status_code,
    status_code_changed,
    case when subscription_sub_type = 'Broadband DSL Line' then current_product_description else null end as bb_package,
    case when currency_code = 'EUR' then 1 else 0 end as ROI
into --drop table
        #active_ever
from cust_subs_hist a
where status_code in('AC','PC','AB')
    and subscription_sub_type in ('DTV Primary Viewing','Broadband DSL Line')
group by account_number,cb_key_household,subs_type,
    effective_from_dt,
    effective_to_dt,
    status_code,
    prev_status_code,
    status_code_changed,
    bb_package,
    ROI


update MMS_2017_06 as a
set a.bb_active = 1
from
    (select a.account_number
         from MMS_2017_06 a
         inner join #active_ever b
         on a.account_number = b.account_number
              and b.effective_from_dt  <= @enddate
              and b.effective_to_dt    >  @enddate
         where b.subs_type = 2
         group by a.account_number ) as b
Where a.account_number = b.account_number


UPDATE MMS_2017_06 SET BB_provider =
CASE WHEN BB_active = 1 THEN 'SKY'
ELSE 'OTHER' END


UPDATE MMS_2017_06  SET a.BB_provider  = b.BB_provider
FROM MMS_2017_06 AS a
INNER JOIN(SELECT a.account_number
                ,'BT' AS BB_provider
                ,Max(b.dw_last_modified_dt) as dt
                FROM MMS_2017_06  AS a
                INNER JOIN cust_anytime_plus_downloads b
                ON a.account_number = b.account_number
                WHERE UPPER(b.network_code) like '%BT%'
                AND CAST(b.dw_last_modified_dt AS DATE) >(@enddate-365)
                and a.bb_active = 0
                GROUP BY a.account_number,BB_provider) AS b
ON a.account_number = b.account_number

--SGE
SELECT a.account_number ,
       max(CASE WHEN subscription_type = 'A-LA-CARTE' and subscription_sub_type ='Sky Go Extra' THEN 1 else 0 end) as SGE_subs
INTO #temp_sge
FROM cust_subs_hist a
 inner join MMS_2017_06 b
 on a.account_number=b.account_number
WHERE  a.subscription_sub_type in( 'Sky Go Extra')
 and   a.subscription_type = 'A-LA-CARTE'
 and   a.status_code in ('AC','AB','PC')
 and   a.status_code_changed = 'Y'
 and a.effective_from_dt <= @enddate
 and a.effective_to_dt > @enddate
 and a.effective_from_dt<>effective_to_dt
  group by a.account_number

update MMS_2017_06 a
 set skygoe = b.SGE_subs
 from #temp_sge b
 where a.account_number=b.account_number

-- Tenure (Years)
update MMS_2017_06
set dtv_first_act_date_BT = b.dt1,
    dtv_latest_act_date_BT = b.dt2
    from MMS_2017_06 a
    inner join (select a.account_number,
                       min(b.effective_from_dt) as dt1,
                       max(case when b.prev_status_code in ('PO','SC') and status_code_changed = 'Y' then effective_from_dt else null end) as dt2
                  from MMS_2017_06 a
                  inner join #active_ever b
                  on a.account_number = b.account_number
                  and b.effective_from_dt <= @enddate
                  and b.status_code = 'AC'
                  and subs_type = 1
                  group by a.account_number ) as b
                  on a.account_number = b.account_number

update MMS_2017_06
set dtv_act_date_BT = case when dtv_latest_act_date_BT is null then dtv_first_act_date_BT else dtv_latest_act_date_BT end

update MMS_2017_06 set Tenure_BT = datediff(day,dtv_act_date_BT,@enddate)
update MMS_2017_06 set Tenure_yrs_BT = CAST(Tenure_bt AS FLOAT)/365


-- SPORTS GROUP
update MMS_2017_06
set sports_downgrade_date = b.dt1
    from MMS_2017_06 a
    inner join (select a.account_number,
                       max(case when typeofevent = 'SD' then event_dt else null end) as dt1
                  from MMS_2017_06 a
                 inner join citeam.view_cust_package_movements_hist b
                  on a.account_number = b.account_number
                  and b.event_dt between a.dtv_act_date_BT and @enddate
                  and b.typeofevent in ('SD')
                  group by a.account_number ) as b
                  on a.account_number = b.account_number

update MMS_2017_06
 set sports_grp =
CASE WHEN sports = 1 THEN 'Active'
     WHEN sports = 2 THEN 'Active'
     WHEN sports = 0 and sports_downgrade_date is null  THEN 'NH'
     ELSE 'Lapsed' END

  SELECT 'END'
  
  select top 100 * from mms_2017_06
