SELECT 
	HH_key
	, last_dt EXP_last_dt
	, last_dt CAL_last_dt 
	, CAL_ZO_flag
	, EXP_ZO_flag
	, EXP_PR_flag
	, last_type_of_tx
	, last_st
INTO HM_Comparisson
FROM HM_HH_key_ALL
COMMIT
------------------------------------------------
CREATE HG INDEX idx01 on HM_Comparisson(HH_key)
COMMIT
------------------------------------------------
SELECT
          a.HH_key
        , max(d.dt)        AS max_ZO
        , CAST(max(CASE  WHEN    DATEDIFF(dd,
							COALESCE( RentedDate, RentUnderOfferDate, ToRentDate,'1900-01-01'),
                            COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate, '1900-01-01') ) <=0 THEN
                                COALESCE(RentedDate, RentUnderOfferDate, ToRentDate)
                                ELSE COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate) END)
                                AS DATE) AS max_CAL_ZO
INTO #TEMP1
FROM  HM_Comparisson as a 
LEFT JOIN  sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH             AS d ON a.HH_key = d.cb_key_household
LEFT JOIN  sk_uat.CC_HOME_MOVERS                                AS e ON a.HH_key = e.cb_key_household
GROUP BY a.HH_key
commit
------------------------------------------------
CREATE HG INDEX idx01 on #TEMP1(HH_key)
COMMIT
------------------------------------------------
UPDATE HM_Comparisson
SET     EXP_last_dt = max_ZO,
		CAL_last_dt = max_CAL_ZO
FROM  HM_Comparisson AS a
LEFT JOIN #TEMP1 as b ON a.HH_key = b.HH_key
COMMIT
------------------------------------------------
SELECT 	  'EXP' source
		, YEAR(EXP_last_dt) year1
		, MONTH(EXP_last_dt) month1
		, count(DISTINCT HH_key) HH_count
FROM HM_Comparisson
WHERE EXP_last_dt BETWEEN '2013-06-01' AND '2013-11-30'
GROUP BY year1, month1
UNION 
SELECT 	  'CAL' source
		, YEAR(CAL_last_dt) year1
		, MONTH(CAL_last_dt) month1
		, count(DISTINCT HH_key) HH_count
FROM HM_Comparisson
WHERE CAL_last_dt BETWEEN '2013-06-01' AND '2013-11-30'
GROUP BY year1, month1
------------------------------------------------
SELECT
        count(HH_KEY)           Total_Rows
        , sum(CAL_ZO_flag) Callcredit
        , sum(EXP_ZO_flag) Experian
        , SUM(CASE WHEN EXP_ZO_flag =1 AND CAL_ZO_flag = 1 THEN 1 ELSE 0 END) Intersection
        , SUM(CASE WHEN EXP_ZO_flag =0 AND CAL_ZO_flag = 1 THEN 1 ELSE 0 END) Unique_CAL
        , SUM(CASE WHEN EXP_ZO_flag =1 AND CAL_ZO_flag = 0 THEN 1 ELSE 0 END) Unique_EXP
FROM HM_Comparisson
WHERE EXP_last_dt BETWEEN '2013-06-01' AND '2013-11-30'
                OR CAL_last_dt  BETWEEN '2013-06-01' AND '2013-11-30'
------------------------------------------------
SELECT DISTINCT top 100
          HH_key
        , EXP_last_dt
        , CAL_last_dt
        , RentedDate
		, RentUnderOfferDate
		, ToRentDate
		, SoldDate
		, SaleUnderOfferDate
		, ForSaleDate
		, max(CASE WHEN rank1 = 1 THEN dt ELSE null END) dt1
		, max(CASE WHEN rank1 = 2 THEN dt ELSE null END) dt2
		, max(CASE WHEN rank1 = 3 THEN dt ELSE null END) dt3
		, max(CASE WHEN rank1 = 4 THEN dt ELSE null END) dt4
		, max(CASE WHEN rank1 = 5 THEN dt ELSE null END) dt5
		, max(CASE WHEN rank1 = 1 THEN status_2 ELSE null END) st1
		, max(CASE WHEN rank1 = 2 THEN status_2 ELSE null END) st2
		, max(CASE WHEN rank1 = 3 THEN status_2 ELSE null END) st3
		, max(CASE WHEN rank1 = 4 THEN status_2 ELSE null END) st4
		, max(CASE WHEN rank1 = 5 THEN status_2 ELSE null END) st5
--INTO HM_sample_accuracy
FROM HM_Comparisson as a 
JOIN  sk_uat.CC_HOME_MOVERS                                                AS e ON a.HH_key = e.cb_key_household
JOIN HM_alerts_x_HH2 														AS v ON a.HH_key = v.cb_key_household AND rank1 <=5
WHERE EXP_ZO_flag =1 AND CAL_ZO_flag =1
AND DATEDIFF (month, EXP_last_dt, CAL_last_dt) <=1
AND month(CAL_last_dt) = 9
GROUP BY 
			HH_key
        , EXP_last_dt
        , CAL_last_dt
        , RentedDate
		, RentUnderOfferDate
		, ToRentDate
		, SoldDate
		, SaleUnderOfferDate
		, ForSaleDate
------------------------------------------------
SET DATEFORMAT DMY
SELECT DISTINCT TOP 1000
          a.HH_key
        , b.projected_completion_date           AS EXP_completion_dt
		, b.actual_completion_date
        , CASE WHEN last_type_of_tx = 1 THEN 'Renting'
               WHEN last_type_of_tx = 2 THEN 'Selling'
               ELSE null END type_of_tx
        , CASE WHEN last_type_of_tx = 1  THEN CAST(COALESCE(RentedDate_Derived, RentUnderOfferDate_Derived, ToRentDate_Derived) AS DATE)
               WHEN last_type_of_tx = 2  THEN CAST(COALESCE(SoldDate_Derived, SaleUnderOfferDate_Derived) AS DATE)
               ELSE null END Derived_date
        , TRIM(LAST_st) last_st
FROM  HM_Comparisson                            AS a
JOIN sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA as b ON a.hH_key = b.cb_key_household
JOIN sk_uat.CC_HOME_MOVERS          AS e ON a.HH_key = e.cb_key_household
JOIN HM_alerts_x_HH2                            AS v ON a.HH_key = v.cb_key_household AND rank1 <=5
WHERE projected_completion_date is not null
AND derived_date is not null
AND last_st in ('RENTED', 'RENT UNDER OFFER', 'SELL UNDER OFFER','SOLD');
OUTPUT TO 'C:\Users\pitteloj\Documents\Home Moving\Slides sample 1.csv' Format ASCII Delimited by ',' quote'';

------------------------------------------------ OLIVE PROD
CREATE TABLE HM_Sample_slides
        ( ID int IDENTITY
        , hh_key BIGINT
        , EXP_completion_dt DATE DEFAULT NULL
        , actual_completion_date DATE DEFAULT NULL
        , type_of_tx VARCHAR(30) DEFAULT NULL       
        , Derived_date DATE DEFAULT NULL
        , last_st VARCHAR(30) DEFAULT NULL
       )
COMMIT
TRUNCATE TABLE HM_Sample_slides;
SET DATEFORMAT DMY;

LOAD TABLE HM_Sample_slides
        (hh_key',',
        EXP_completion_dt',',
        actual_completion_date',',
        type_of_tx',',
        Derived_date',',
        last_st'\n')
From '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/Slides sample 1.csv'
Quotes off
Escapes off
Notify 1000;
COMMIT;
------------------------------------------------------------------------------------------------
UPDATE HM_Sample_slides
SET Sky_call_dt  = Sky__new_call_dt
    , Sky_act_dt  = Sky__new_act_dt
FROM  HM_Sample_slides as a 
JOIN HM_CAL_accounts as b ON a.hh_key = b.hh_key 
COMMIT
------------------------------------------------------------------------------------------------
SELECT ID
        ,hh_key
        ,EXP_completion_dt
        ,actual_completion_date
        ,type_of_tx
        ,Derived_date
        ,sky_act_dt
        ,Sky_call_dt,last_st
from HM_Sample_slides
WHERE Sky_act_dt is not null
------------------------------------------------------------------------------------------------
SELECT
	  cb_key_household
	, CAST ( SoldDate AS DATE) AS sold_dt
FROM sk_uat.CC_HOME_MOVERS
WHERE CAST(SoldDate as DATE) between '01/10/2013' AND '31/10/2013'
OUTPUT TO 'C:\Users\pitteloj\Documents\Home Moving\Sold_October.csv' Format ASCII Delimited by ',' quote'';

CREATE TABLE HM_October_sold
        ( ID int IDENTITY
        , hh_key BIGINT
        , Sold_dt DATE DEFAULT NULL
        , prev_sky_acct	BIGINT DEFAULT NULL
		, new_skyacct  BIGINT DEFAULT NULL
		, Moved	bit Default 0 
		, Churned bit Default 0 
		, Still_same_add bit Default 0 
		, Reason VARCHAR(30) DEFAULT NULL       
		)
COMMIT

LOAD TABLE HM_October_sold
        (hh_key',',
        sold_dt'\n')
From '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/Sold_October.csv'
Quotes off
Escapes off
Notify 1000;
COMMIT;

UPDATE HM_October_sold
SET       prev_sky_acct = sky_churn_account
        , new_Skyacct   =  sky_new_account
FROM HM_October_sold as a
JOIN HM_ALL_Attrib as b on a.hh_key = b.hh_key
COMMIT

----------------------------------------------------------
SELECT
        account_number
       , hh_key
       , effective_from_dt as churn_date
       , case when status_code = 'PO'
             then 'CUSCAN'
             else 'SYSCAN'
         end as churn_type
       , RANK() OVER (PARTITION BY  csh.account_number
                     ORDER BY  csh.effective_from_dt,csh.cb_row_id) AS churn_rank--Rank to get the first event
INTO #all_churn_records
FROM sk_prod.cust_subs_hist as csh
JOIN HM_ALL_Attrib as b ON csh.account_number = b.sky_churn_account
WHERE subscription_sub_type ='DTV Primary Viewing'     --DTV stack
   and status_code in ('PO','SC')                       --CUSCAN and SYSCAN status codes
   and prev_status_code in ('AC','AB','PC')             --Previously ACTIVE
   and status_code_changed = 'Y'
   and effective_from_dt between DATADD(month,last_dt,-2) AND DATADD(month,last_dt,1)
   and effective_from_dt != effective_to_dt
DELETE FROM HM_all_churn_records WHERE churn_rank >1
---------------------------------------------- UPDATING Churned Accounts
UPDATE HM_ALL_Attrib
SET Sky_churn_account = account_number 
FROM HM_ALL_Attrib as a
JOIN HM_all_churn_records as b ON a.hh_key = b.hh_key
---------------------------------------------- Extracting addresses by HH From addresses table
SELECT
account_number
        , hh_key
        , AD_EFFECTIVE_FROM_DT
        , ad_effective_to_dt
INTO  HM_addresses_reference
FROM sk_prod.CUST_ALL_ADDRESS as ad
JOIN HM_ALL_Attrib as b ON ad.cb_key_household = hh_key  
WHERE ad_effective_to_dt >= '2013-01-01'
commit
---------------------------------------------- UPDATING Old Accounts between -70 and 35 days before last_st
UPDATE HM_ALL_Attrib
SET Sky_churn_account = account_number
FROM HM_ALL_Attrib as a
JOIN HM_addresses_reference as b ON b.hh_key = a.HH_key
WHERE Sky_churn_account is null
AND ad_effective_to_dt Between DATEADD(day, -70, last_dt) AND DATEADD(day, 35, last_dt)
---------------------------------------------- UPDATING New Accounts between -40 and 180 days after last_st
UPDATE HM_ALL_Attrib
SET Sky_new_account = account_number
FROM HM_ALL_Attrib as a
JOIN HM_addresses_reference as b ON b.hh_key = a.HH_key
WHERE Sky_churn_account is null
AND ad_effective_from_dt Between DATEADD(day, -40, last_dt) AND DATEADD(day, 180, last_dt)
----------------------------------------------
UPDATE HM_October_sold
SET       prev_sky_acct =  sky_churn_account
        , new_Skyacct   =  sky_new_account
        , Churned = CASE WHEN sky_churn_account is not null and SKY_cancellation_dt is not null THEN 1 ELSE Null END
FROM HM_October_sold as a
JOIN HM_ALL_Attrib as b on a.hh_key = b.hh_key
COMMIT
-----------------------------------------------
SELECT  DATE(DATEADD(mm,DATEDIFF(mm,'1980-01-01',max_CAL_ZO),'1980-01-01'))       AS max_CAL_dt
        , DATE(DATEADD(mm,DATEDIFF(mm,'1980-01-01',max_EXP_ZO),'1980-01-01'))      AS max_EXP_dt
        , COUNT(HH_KEY) TOTAL_HH
        
FROM (
        

SELECT
                         hH_key
                        , CAST(max(CASE  WHEN    DATEDIFF(dd,
                                                                COALESCE( RentedDate, RentUnderOfferDate, ToRentDate,'1900-01-01'),
                                    COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate, '1900-01-01') ) <=0 THEN
                                        COALESCE(RentedDate, RentUnderOfferDate, ToRentDate)
                                        ELSE COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate) END)
                                        AS DATE) AS max_CAL_ZO
                        , max(d.dt)        AS max_EXP_ZO
                        , DATEDIFF (dd,max_EXP_ZO,max_CAL_ZO) Days_diff
                        , CASE WHEN Days_diff < 60 THEN 1 ELSE 0 END matching
                                                , COALESCE(c.cb_address_postcode_area, d.cb_address_postcode_area) AS cb_address_postcode_area

        INTO HM_ALL_comparisson
                FROM  HM_HH_key_ALL AS a
        LEFT JOIN sk_uat.CC_HOME_MOVERS                      AS c ON a.HH_key = c.cb_key_household
        LEFT JOIN  sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH             AS d ON a.HH_key = d.cb_key_household
        GROUP BY hh_key, c.cb_address_postcode_area
        HAVING          (max_CAL_ZO between '2013-03-01' AND '2013-12-31' OR max_CAL_ZO is null)
                AND     (max_EXP_ZO between '2013-03-01' AND '2013-12-31' OR max_EXP_ZO is null)
--------------------------------------------------------------
UPDATE HM_October_sold
SET moved =1
FROM HM_October_sold    AS e
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON sav.account_number = CAST(e.prev_sky_acct AS VARCHAR(12))
WHERE
        sav.cb_key_household <> hh_key
		AND 
--------------------------------------------------------------
UPDATE HM_October_sold
SET Still_same_add =1
FROM HM_October_sold    AS e
JOIN sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav ON sav.account_number = CAST(e.prev_sky_acct AS VARCHAR(12))
WHERE
         cust_active_dtv = 1
        AND  sav.cb_key_household =hh_key
--------------------------------------------------------------