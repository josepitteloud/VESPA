----------------- EXPLORATORY
SELECT top 10 * FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA ;
SELECT top 10 * FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       ;
SELECT top 10 * FROM sk_uat_data.EXPERIAN_MOVER_ALERTS          ;
SELECT top 10 * FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH
----------------- TOTAL ROWS
SELECT 'predition', count(*) FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA  UNION
SELECT 'rightmove', count(*) FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       UNION
SELECT 'alerts', count(*) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS          UNION
SELECT 'alertsperhh', count(*) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH
---------------- TOTAL UNIQUE HH
SELECT 'predition', count(DISTINCT cb_address_udprn) FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA  UNION
SELECT 'rightmove', count(DISTINCT cb_address_udprn) FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       UNION
SELECT 'alerts', count(DISTINCT cb_address_udprn) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS          UNION
SELECT 'alertsperhh', count(DISTINCT cb_address_udprn) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH
---------------- TOTAL UNIQUE HH
SELECT 'predition', count(DISTINCT cb_key_urn_family) FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA  UNION
SELECT 'rightmove', count(DISTINCT cb_key_urn_family) FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       UNION
SELECT 'alerts', count(DISTINCT cb_key_urn_family) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS          UNION
SELECT 'alertsperhh', count(DISTINCT cb_key_urn_family) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH
---------------- TOTAL UNIQUE cb_key_urn_household
SELECT 'predition', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA  UNION
SELECT 'rightmove', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       UNION
SELECT 'alerts', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS          UNION
SELECT 'alertsperhh', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH
---------------- TOTAL UNIQUE HH
SELECT 'predition', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA  UNION
SELECT 'rightmove', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       UNION
SELECT 'alerts', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS          UNION
SELECT 'alertsperhh', count(DISTINCT cb_key_urn_household) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH

----------------- Match rates prediciton/rightmove
SELECT count(DISTINCT a.cb_key_urn_household)
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
JOIN sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       as b ON a.cb_key_urn_household = b.cb_key_urn_household AND a.cb_address_barcode = b.cb_address_barcode
----------------- Match rates prediciton/alerts
SELECT count(DISTINCT a.cb_key_urn_household)
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
JOIN sk_uat_data.EXPERIAN_MOVER_ALERTS       as b ON a.cb_key_urn_household = b.cb_key_urn_household AND a.cb_address_barcode = b.cb_address_barcode
----------------- Match rates prediciton/alerts
SELECT count(DISTINCT a.cb_key_urn_household)
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
JOIN sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH      as b ON a.cb_key_urn_household = b.cb_key_urn_household AND a.cb_address_barcode = b.cb_address_barcode
----------------- Match rates prediciton/alerts
SELECT count(DISTINCT a.cb_key_urn_household)
FROM sk_uat_data.EXPERIAN_MOVER_ALERTS as a
JOIN sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH      as b ON a.cb_key_urn_household = b.cb_key_urn_household AND a.cb_address_barcode = b.cb_address_barcode
----------------- Match rates prediciton/alerts
SELECT count(DISTINCT a.cb_key_urn_household)
FROM sk_uat_data.EXPERIAN_MOVER_ALERTS as a
JOIN sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       as b ON a.cb_key_urn_household = b.cb_key_urn_household AND a.cb_address_barcode = b.cb_address_barcode
----------------- Extracting shared keys
CREATE TABLE HM_match_keys (key_1 bigint, ID int IDENTITY)
INSERT INTO HM_match_keys (key_1)
SELECT  DISTINCT a.cb_key_urn_household AS key_1
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
JOIN sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE       as b ON a.cb_key_urn_household = b.cb_key_urn_household AND a.cb_address_barcode = b.cb_address_barcode
JOIN sk_uat_data.EXPERIAN_MOVER_ALERTS          as c ON a.cb_key_urn_household = c.cb_key_urn_household AND a.cb_address_barcode = c.cb_address_barcode
JOIN sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH   as d ON a.cb_key_urn_household = d.cb_key_urn_household AND a.cb_address_barcode = d.cb_address_barcode
commit
--------------------- CHECKING total rows per source
SELECT count(*) FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
JOIN HM_match_keys as b ON a.cb_key_urn_household = b.key_1
---------------------
SELECT count(*) FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE as a
JOIN HM_match_keys as b ON a.cb_key_urn_household = b.key_1
---------------------
SELECT count(*) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS as a
JOIN HM_match_keys as b ON a.cb_key_urn_household = b.key_1
---------------------
SELECT count(*) FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH as a
JOIN HM_match_keys as b ON a.cb_key_urn_household = b.key_1

--------------------- Checking duplicate rows Prediction
SELECT top 100 'x'||v.key_2, d.*
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as d
JOIN (SELECT a.cb_key_urn_household key_2
        FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
        JOIN HM_match_keys As b ON a.cb_key_urn_household = b.key_1
        GROUP BY a.cb_key_urn_household
        HAVING count(*) =1)
        as v ON v.key_2 = d. cb_key_urn_household
ORDER BY d.cb_key_urn_household
------------------- CREATING WORKING VIEWS
CREATE VIEW HM_prediction
AS
SELECT
          a.cb_key_urn_household                AS hh_key
        , max(a.actual_completion_date)         AS actual_completion_date
        , max(a.projected_completion_date)      AS projected_completion_date
        , max(a.rented_projection)              AS rented_projection
        , maX(a.sold_projection)                AS sold_projection
FROM    sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA      AS a
JOIN    HM_match_keys                                   AS b ON a.cb_key_urn_household = b.key_1
GROUP BY a.cb_key_urn_household
COMMIT

SELECt count (hH)
from (SELECT cb_key_urn_household hh, count(*) hits
        FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as a
        JOIN HM_match_keys As b ON a.cb_key_urn_household = b.key_1
        GROUP BY a.cb_key_urn_household
        HAVING count(*) =1)
        as v 
------------------------------------------ CREATING TABLE HM_ALERTS w/o duplicates
SELECT x.*
INTO HM_alerts
FROM (SELECT DISTINCT a.cb_key_urn_household key_3, count(cb_key_urn_household) hits
        FROM sk_uat_data.EXPERIAN_MOVER_ALERTS as a
        JOIN HM_match_keys As b ON a.cb_key_urn_household = b.key_1
        GROUP BY key_3
        HAVING count(cb_key_urn_household) =1)          as v
JOIN sk_uat_data.EXPERIAN_MOVER_ALERTS                  as x ON x.cb_key_urn_household = v.key_3
COMMIT
------------------------------------------ INSERTING Clean HH
INSERT INTO HM_alerts (cb_key_urn_household, add1, add2, add3, experian_ref, cb_row_id)
SELECT c.cb_key_urn_household
        , c.add1
        , c.add2
        , c.add3
        , c.experian_ref
        , cb_row_id
FROM sk_uat_data.EXPERIAN_MOVER_ALERTS as c
JOIN   (select a.key_1
        FROM HM_match_keys as a
        LEFT JOIN HM_alerts as b on b.cb_key_urn_household = a.key_1
        WHERE b.cb_key_urn_household is null) as v ON v.key_1 = c.cb_key_urn_household
JOIN    (SELECT add1, add2, add3, cb_key_urn_household, count(*) cd
        FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA
        GROUP BY add1, add2, add3, cb_key_urn_household) as x on x.cb_key_urn_household = c.cb_key_urn_household AND c.add1 = x.add1 AND c.add2 = x.add2
-------------------------------------------- CREATING HM_alerts_x_HH View
CREATE VIEW HM_alerts_x_HH
AS
SELECT c.*
FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH as c
JOIN HM_alerts as b on b.cb_key_urn_household = c.cb_key_urn_household AND c.experian_ref = b.experian_ref
COMMIT
------------------------------------------
CREATE VIEW HM_alerts_x_HH2
AS
SELECT
           RANK() OVER (PARTITION BY experian_ref ORDER BY dt DESC) rank1
         , CASE  WHEN status IN ('FOR SALE' , ' FOR SALE & TO RENT' )                                                    THEN 'FOR SALE'
                WHEN status IN ('RENT UNDER OFFER', 'FOR SALE & RENT UNDER OFFER')                                      THEN 'RENT UNDER OFFER'
                WHEN status IN ('RENTED','RENT UNDER OFFER & RENTED','FOR SALE & RENTED','RENT UNDER OFFER & RENTED & TO RENT'
                                ,'FOR SALE & RENTED & TO RENT','RENT UNDER OFFER & RENTED & SALE UNDER OFFER')          THEN 'RENTED'
                WHEN status IN ('SALE UNDER OFFER','FOR SALE & SALE UNDER OFFER','RENTED & SALE UNDER OFFER'
                                ,'RENT UNDER OFFER & SALE UNDER OFFER')                                                 THEN 'SALE UNDER OFFER'
                WHEN status IN ('SOLD','SALE UNDER OFFER & SOLD','FOR SALE & SOLD','RENTED & SOLD')                     THEN 'SOLD'
                WHEN status IN ('TO RENT', 'RENTED & TO RENT','SOLD & TO RENT','RENT UNDER OFFER & TO RENT','SALE UNDER OFFER & TO RENT'
                                ,'SALE UNDER OFFER & SOLD & TO RENT','FOR SALE & SALE UNDER OFFER & TO RENT' )          THEN 'TO RENT'
                ELSE null END AS status_2
        , *
from HM_alerts_x_HH
--------------------------------------------
INSERT INTO HM_alerts_x_HH_repro
SELECT
          cb_key_urn_household HH_key
        , experian_ref
        , add1
        , add2
        , add3
        , max(CASE WHEN rank1 = 1 THEN status_2 ELSE null       END)   AS Last_status
        , max(CASE WHEN rank1 = 1 THEN dt ELSE null             END)   AS Last_status_dt
        , max(CASE WHEN rank1 = 2 THEN status_2 ELSE null       END)   AS Status_1a
        , max(CASE WHEN rank1 = 2 THEN dt ELSE null             END)   AS status_dt_1a
        , max(CASE WHEN rank1 = 2 THEN status_2 ELSE null       END)   AS Status_2a
        , max(CASE WHEN rank1 = 3 THEN dt ELSE null             END)   AS status_dt_2a
        , max(CASE WHEN status_2 IN ('SOLD' , 'RENTED') THEN dt ELSE null END) AS LAST_completion_dt
        , CASE WHEN  Last_status_dt IN ('SOLD' , 'RENTED') THEN 'Completed' ELSE 'Running' END Completion_flag

FROM HM_alerts_x_HH2
GROUP BY
          cb_key_urn_household
        , experian_ref
        , add1
        , add2
        , add3
ORDER BY          cb_key_urn_household
        , experian_ref
commit
----------------------------------------
UPDATE HM_alerts_x_HH_repro
SET completion_flag = 'Completed'
WHERE Last_status in ('SOLD            ', 'RENTED          ')
commit
------------------------------------------------------------------------
CREATE VIEW HH_right_1 AS
SELECT DISTINCT
         cb_key_urn_household AS hh_key
        , a.add1
        , a.add2
        , count(a.property_id) prop_id
FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE as a
JOIN HM_alerts_x_HH_repro AS b ON b.hh_key = a.cb_key_urn_household AND a.add1 = b.add1
GROUP BY a.add1, a.add2, hh_key
COMMIT
------------------------------------------------------------------------
SELECT
         a.cb_key_urn_household AS hh_key
        , a.add1
        , a.add2
        , first_visible_date
        , reason_for_inclusion          AS status_1
        , filedate
        , RANK() OVER (PARTITION BY cb_key_urn_household ORDER BY filedate DESC) AS rank_1
        , CASE  WHEN reason_for_inclusion IN ('New Resale' )                                                                    THEN 'FOR SALE'
                WHEN reason_for_inclusion IN ('Status Updated to Let Agreed')                                                   THEN 'RENT UNDER OFFER'
                WHEN reason_for_inclusion IN ('Removed & Archived Rental','Removed Invisible Rental','Removed Rental')          THEN 'RENTED'
                WHEN reason_for_inclusion IN ('Status Update to SSTC/Under offe','Status Update to SSTC/Under offer')           THEN 'SALE UNDER OFFER'
                WHEN reason_for_inclusion IN ('Removed Resale','Removed & Archived Resale'
                                                ,'Removed Invisible Resale','Status Update to Sold')                            THEN 'SOLD'
                WHEN reason_for_inclusion IN ('New Rental')                                                                     THEN 'TO RENT'
                ELSE 'OTHER' END AS status_2
INTO HH_right_2
FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE as a
JOIN HM_alerts_x_HH_repro AS b ON b.hh_key = a.cb_key_urn_household AND a.add1 = b.add1
WHERE reason_for_inclusion not in ('Other','Updated Rental','Updated Resale')
COMMIT
--------------------------------------------
-------------------------------------
CREATE VIEW HH_right_3 AS
SELECT hh_key
        , add1
        , add2
        , first_visible_date
        , MIN(filedate) first_change
        , MAX(CASE WHEN rank_1 = 1 THEN status_2 END)                    AS Last_status
        , MAX(CASE WHEN rank_1 = 1 THEN filedate END)   AS Last_status_dt
        , MAX(CASE WHEN rank_1 = 2 THEN status_2 END)   AS Last_status_1
        , MAX(CASE WHEN rank_1 = 2 THEN filedate END)   AS Last_status_dt_1
        , MAX(CASE WHEN rank_1 = 3 THEN status_2 END)   AS Last_status_2
        , MAX(CASE WHEN rank_1 = 3 THEN filedate END)   AS Last_status_dt_2
        , max(CASE WHEN status_2 IN ('SOLD' , 'RENTED') THEN filedate ELSE null END) AS LAST_completion_dt
        , CASE WHEN  Last_status IN ('SOLD' , 'RENTED') THEN 'Completed' ELSE 'Running' END Completion_flag
FROM HH_right_2
GROUP BY hh_key
        , add1
        , add2
        , first_visible_date
COMMIT



-----------------------------------HM_alerts_x_HH_repro
-----------------------------------HM_ALERTS
-----------------------------------HM_prediction
-----------------------------------HH_right_3

CREATE VIEW HM_Consolidated AS
SELECT
          a.hh_key
        , CASE  WHEN a.Last_status in ('RENT UNDER OFFER', 'RENTED', 'TO RENT') THEN 'Renting'
                WHEN a.LAst_status in ('SALE UNDER OFFER', 'SOLD', 'FOR SALE') THEN 'Selling'
                ELSE 'check' END                                                                                                AS Type_of_tx_zoopla
        , CASE  WHEN b.Last_status in ('RENT UNDER OFFER', 'RENTED', 'TO RENT') THEN 'Renting'
                WHEN b.LAst_status in ('SALE UNDER OFFER', 'SOLD', 'FOR SALE') THEN 'Selling'
                ELSE 'check' END                                                                                                AS Type_of_tx_right
       , DATE (CASE WHEN  a.first_date > COALESCE(b.first_change, '1950-01-01') THEN coalesce(b.first_change, a.first_date)  ELSE coalesce(a.first_date, b.first_change) END)    AS First_add
        , DATE (a.first_date) First_dt_zoopla
        , DATE (b.first_change) First_dt_right
        , CASE WHEN  a.Last_status_dt > COALESCE(b.Last_status_dt, '1950-01-01')  THEN a.Last_status_dt else b.Last_status_dt END                       AS Last_dt
        , CASE WHEN  a.Last_status_dt > COALESCE(b.Last_status_dt, '1950-01-01')  THEN  a.Last_status else b.Last_status END                             AS Last_status
        , a.Last_status_dt                      AS Last_dt_zoopla
        , a.Last_status                         AS Last_st_zoopla
        , b.Last_status_dt                      AS Last_dt_right
        , b.Last_status                         AS Last_st_right
        , Status_1a                             AS Previous_st_zoopla
        , Last_status_1                         AS Previous_st_right
        , p.actual_completion_date
        , p.projected_completion_date
        , p.rented_projection
        , p.sold_projection
        , a.Completion_flag                     AS Zoopla_completion_flag
        , b.Completion_flag                     AS Right_completion_flag
        , CASE WHEN    (p.sold_projection is NULL
                AND     p.rented_projection IS NULL
                AND     p.actual_completion_date IS NULL
                AND     p.projected_completion_date IS NULL)
                THEN 0 ELSE 1 END      Prediction_flag
FROM HM_alerts_x_HH_repro AS a
JOIN Hm_prediction AS p ON p.HH_key     = a.HH_key
LEFT JOIN HH_RIGHT_3 AS b ON a.HH_key   = b.hh_key AND a.add1 = b.add1
commit
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------	
SELECT month(first_change) right_month, month(first_date) AS alerts, count(DISTInCT a.hh_key) hits
FROM HM_alerts_x_HH_repro AS a
JOIN  hH_RIGHT_3 AS b ON a.HH_key  = b.hh_key
GROUP bY right_month, alerts
-------------------------------------------------
SELECT a.Last_status last_right, b.Last_status AS Last_zoopla, count(DISTInCT a.hh_key) hits
FROM HM_alerts_x_HH_repro AS a
JOIN  hH_RIGHT_3 AS b ON a.HH_key  = b.hh_key
GROUP bY last_right, Last_zoopla

SELECT month(a.Last_status_dt) last_right, month(b.Last_status_dt) AS Last_zoopla, count(DISTInCT a.hh_key) hits
FROM HM_alerts_x_HH_repro AS a
JOIN  hH_RIGHT_3 AS b ON a.HH_key  = b.hh_key
GROUP bY last_right, Last_zoopla


--------------------------------------- OLIVE
--EXTRACTING ACCOUNT NUMBERS
-------------------------
ALTER TABLE pitteloudj.HM_accounts ADD Sky2 bigint default null
commit
-------------------------
SELECT
        sav.account_number
        , sav.cb_key_household
        , sav.acct_first_account_activation_dt act_dt
        , rank() OVER (PARTITION BY sav.cb_key_household ORDER BY sav.acct_first_account_activation_dt desc) rank_1
        INTO HM_temp1
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW AS sav
JOIN HM_accounts as a ON CAST(sav.cb_key_household AS bigint) = a.HH_KEY;
commit;
-------------------------
DELETE FROM HM_temp1 WHERE rank_1 > 1;
commit;
-------------------------
UPDATE HM_accounts
SET Sky2 = t.account_number
from HM_accounts as a
JOIN HM_temp1 as t ON t.cb_key_household = a.hh_key
COMMIT;
----------------------------------------------------
----------------------------------------------------
--------------------------CREATING WORKING VIEW

CREATE VIEW HM_Attributes AS
SELECT DISTINCT
          a.ID_1
        , a.Sky_acc
        , a.HH_key
        , a.account_number      AS HH_urn
        , d.acct_first_account_activation_dt
        , CASE WHEN (sav.h_lifestage like 'Missing' OR sav.h_lifestage is null) THEN
                        (case ex.h_lifestage
                                when '00' then  'Very young family'
                                when '01' then  'Very young single'
                                when '02' then  'Very young homesharers'
                                when '03' then  'Young family'
                                when '04' then  'Young single'
                                when '05' then  'Young homesharers'
                                when '06' then  'Mature family'
                                when '07' then  'Mature singles'
                                when '08' then  'Mature homesharers'
                                when '09' then  'Older family'
                                when '10' then  'Older single'
                                when '11' then  'Older homesharers'
                                when '12' then  'Elderly family'
                                when '13' then  'Elderly single'
                                when '14' then  'Elderly homesharers'
                                when 'U' then  'Unclassified'
                                else 'Missing' end)
                         ELSE       sav.h_lifestage END      AS h_lifestage                       --Lifestage
        , CASE ex.filler_char15 WHEN '0' THEN 'Very Low'
                                WHEN '1' THEN 'Low'
                                WHEN '2' THEN 'Mid Low'
                                WHEN '3' THEN 'Mid'
                                WHEN '4' THEN 'Mid High'
                                WHEN '5' THEN 'High'
                                WHEN '6' THEN 'Very High'
                                WHEN 'U' THEN 'Unclassified'
                                ELSE null END AS h_affluence                  --Affluence
        , case ex.h_mosaic_uk_group
                when 'A' then 'Alpha Territory'
                when 'B' then 'Professional Rewards'
                when 'C' then 'Rural Solitude'
                when 'D' then 'Small Town Diversity'
                when 'E' then 'Active Retirement'
                when 'F' then 'Suburban Mindsets'
                when 'G' then 'Careers and Kids'
                when 'H' then 'New Homemakers'
                when 'I' then 'Ex-Council Community'
                when 'J' then 'Claimant Cultures'
                when 'K' then 'Upper Floor Living'
                when 'L' then 'Elderly Needs'
                when 'M' then 'Industrial Heritage'
                when 'N' then 'Terraced Melting Pot'
                when 'O' then 'Liberal Opinions'
                when 'U' then 'Unclassified'
                else null end Mosaic_Group                                           --Mosaic Group
        , CASE WHEN sav.property_type = 'Unclassified'  THEN
                        (CASE  h_residence_type_v2
                                when '0' then 'Detached'
                                when '1' then 'Semi-detached'
                                when '2' then 'Bungalow'
                                when '3' then 'Terraced'
                                when '4' then 'Flat'
                                when 'U' then 'Unclassified'
                                else null end)
                          ELSE sav.property_type END  Residence_Type                           --Property type
        , CASE ex.h_property_council_taxation
                when '0' then 'England - Up to £40k, Wales - Up to £30, Scotland - Up to £27,0'
                when '1' then 'England - £40k to £52k, Wales - £30k to £39, Scotland - £27 to £35'
                when '2' then 'England - £52k to £68k, Wales - £39k to £51, Scotland - £35 to £45'
                when '3' then 'England - £68k to £88k, Wales - £51k to £66, Scotland - £45 to £58'
                when '4' then 'England - £88k to £120k, Wales - £66k to £90, Scotland - £58 to £80'
                when '5' then 'England - £120k to £160k, Wales - £90k to £120,, Scotland - £80 to £106'
                when '6' then 'England - £160k to £320k, Wales - £120k to £240, Scotland - £106 to £212'
                when '7' then 'England - Over £320k, Wales - Over £240k, Scotland - Over £212k'
                when 'U' then 'Unclassified'
                else null END               AS 'Council Tax Band'
        , case ex.h_residence_type_v2
                when '0'        then    'Detached'
                when '1'        then 'Semi-detached'
                when '2'        then 'Bungalow'
                when '3'        then 'Terraced'
                when '4'        then 'Flat'
                when 'U'        then 'Unclassified'
                else null END               AS 'Residence Type V2'
        , sav.tenure                                            --Tenure
        , sav.region
        , sav.social_class
        , sav.current_package                           --Packages
FROM HM_accounts AS a
LEFT JOIN sk_prod.EXPERIAN_CONSUMERVIEW AS ex ON a.HH_key = ex.cb_key_household
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW AS sav ON a.Sky_acc = CAST(sav.account_number AS bigint)
JOIN tenure_c AS d ON CAST(d.account_number AS bigint) = a.Sky2;
commit;
