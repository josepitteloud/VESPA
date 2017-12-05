-------------- MODEL COMPARISSION
---------Calcredit


SELECT 
          cb_key_household              AS HH_key
        , COALESCE( RentedDate, RentUnderOfferDate, ToRentDate)         AS      rent_dt
		, COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate)                 AS      sell_dt
        , CASE  WHEN    DATEDIFF(dd,COALESCE(rent_dt,'1900-01-01'),COALESCE(sell_dt,'1900-01-01') ) <=0
                        THEN rent_dt
                        ELSE sell_dt END                         AS max_dt
        , CASE WHEN  DATEDIFF(dd, COALESCE(rent_dt,'1900-01-01'),COALESCE(sell_dt,'1900-01-01')) <=0 THEN 'rent' ELSE 'sell' END AS last_st1
        , COALESCE( RentedDate_derived, RentUnderOfferDate_derived, ToRentDate_derived)         AS      rent_dt_der
		, COALESCE(SoldDate_derived,SaleUnderOfferDate_derived,ForSaleDate_derived)                 AS      sell_dt_der
        , CASE  WHEN   last_st1 = 'rent'
                        THEN rent_dt_der
                        ELSE sell_dt_der END                    AS Max_Moving_date
		, RANK() OVER (PARTITION BY HH_key ORDER BY max_dt DESC) as rank1
        , CASE  WHEN  last_st1 = 'rent' AND (RENT_dt = RentedDate) THEN 'RENTED'
                WHEN  last_st1 = 'rent' AND (RENT_dt = RentUnderOfferDate) THEN 'RENT UNDER OFFER'
                WHEN  last_st1 = 'rent' AND (RENT_dt = ToRentDate) THEN 'TO RENT'
                WHEN  last_st1 = 'sell' AND (SELL_dt = SoldDate) THEN 'SOLD'
                WHEN  last_st1 = 'sell' AND (SELL_dt = SaleUnderOfferDate) THEN 'SALE UNDER OFFER'
                WHEN  last_st1 = 'sell' AND (SELL_dt = ForSaleDate) THEN 'FOR SALE'
                ELSE 'OTHER' END                                                                                        AS status_2
INTO HM_Cal_dates_1				
FROM sk_uat.CC_HOME_MOVERS

DELETE FROM HM_Cal_dates_1				 WHERE rank1 > 1

CREATE HG INDEX idx1 ON HM_Cal_dates_1	(HH_key)
COMMIT 
---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
CREATE TABLE HM_CAL_accounts
        ( ID int IDENTITY
        , hh_key BIGINT
        , last_dt date DEFAULT NULL
        , move_date date DEFAULT NULL
        , SKY_old_acct bigint DEFAULT NULL
        , SKY_new_acct bigint DEFAULT NULL
        , Sky__new_call_dt date DEFAULT NULL
        , Sky__new_act_dt date DEFAULT NULL
		, new_lead_time2 int default null	
		, new_lead_time DATE DEFAULT NULL
        , last_dt_dum   VARCHAR(30) DEFAULT NULL
        , move_date_dum VARCHAR(30) DEFAULT NULL
		, Calcredit_flag BIT DEFAULT 0
		, Experian_flag BIT DEFAULT 0
        )
commit;


---------------------------------------------------------------------------------------
LOAD TABLE HM_CAL_accounts
	(hh_key',',
    last_dt_dum',',
    move_date_dum‘\n’)
From '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/Cal_accounts.csv'
Quotes off
Escapes off
Notify 1000;
COMMIT
---------------------------------------------------------------------------------------
CREATE HG INDEX idd1 ON HM_CAL_accounts (HH_key)
CREATE HG INDEX idd2 ON HM_CAL_accounts (SKY_old_acct)
CREATE HG INDEX idd3 ON HM_CAL_accounts (SKY_new_acct)
commit
---------------------------------------------------------------------------------------
SET DATEFORMAT DMY
UPDATE HM_CAL_accounts
SET     last_dt = DATE (last_dt_dum),
        move_date = DATE(move_date_dum)
COMMIT;
---------------------------------------------------------------------------------------
------------ Updating New Sky accounts 
---------------------------------------------------------------------------------------
SELECT account_number
	, acct_first_account_activation_dt 
	, cb_key_household
	, rank () OVER (PARTITION BY account_number ORDER BY acct_first_account_activation_dt ASC) as rank1
INTO #temp1 
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
JOIN HM_CAL_accounts as b ON b.HH_key = sav.cb_key_household
WHERE acct_first_account_activation_dt >= last_dt

DELETE FROM #temp1  WHERE rank1 > 1 
COMMIT
---------------------------------------------------------------------------------------
CREATE HG INDEX idv1 ON #temp1 (cb_key_household) 
---------------------------------------------------------------------------------------
UPDATE HM_CAL_accounts 
SET SKY_new_acct = account_number,
	Sky__new_act_dt = acct_first_account_activation_dt 
FROM HM_CAL_accounts  	AS a 
JOIN HM_CAL_accounts 	AS b ON b.HH_key = sav.cb_key_household
---------------------------------------------------------------------------------------
------------ Updating Old Sky accounts 
---------------------------------------------------------------------------------------
SELECT account_number
	, acct_first_account_activation_dt 
	, cb_key_household
	, rank () OVER (PARTITION BY account_number ORDER BY acct_first_account_activation_dt DESC) as rank1
INTO #temp1 
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
JOIN HM_CAL_accounts as b ON b.HH_key = sav.cb_key_household
WHERE acct_first_account_activation_dt <= last_dt

DELETE FROM #temp1  WHERE rank1 > 1 
COMMIT
---------------------------------------------------------------------------------------
CREATE HG INDEX idv1 ON #temp1 (cb_key_household) 
---------------------------------------------------------------------------------------
UPDATE HM_CAL_accounts 
SET SKY_old_acct = account_number
FROM HM_CAL_accounts  	AS a 
JOIN #temp1 	AS b ON a.HH_key = b.cb_key_household
---------------------------------------------------------------------------------------
------------ EXPERIAN MODEL **************************
---------------------------------------------------------------------------------------
-- select top 10 * FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA
SET DATEFORMAT YMD
SELECT 
          a.cb_key_household hh_key
        , a.rented_projection
        , a.sold_projection
        , a.actual_completion_date
        , a.projected_completion_date
        , b.last_source
        , b.last_dt
        , b.last_type_of_tx
        , CASE WHEN  a.rented_projection IS NULL
				AND a.actual_completion_date IS NULL
                AND a.projected_completion_date IS NULL
                AND a.sold_projection  IS NULL
            THEN 1
            ELSE 0 END                                              AS empty_flag
        , CASE WHEN     DATEDIFF(dd, COALESCE(a.rented_projection, GETDATE()) , GETDATE()) < 0
					OR DATEDIFF(dd, COALESCE(a.sold_projection  , GETDATE()) , GETDATE()) <0
                    OR DATEDIFF(dd, COALESCE(a.projected_completion_date, GETDATE()) ,GETDATE ()) < 0
				THEN 1
                ELSE 0 END                                      AS valid_dt_flag
		, DATEDIFF(week, a.actual_completion_date, a.projected_completion_date)                 AS week_accy_model              --Model error in WEEKS
        , CASE WHEN b.last_source = 'EXP_ZO'
				THEN DATEDIFF(week, CASE last_type_of_tx  	WHEN 1 THEN COALESCE(a.rented_projection, GETDATE())
															WHEN 2 THEN COALESCE(a.sold_projection  , GETDATE())
					ELSE null END, b.last_dt)
                    ELSE null END                                                           AS weeks_accy_table
INTO HM_EXP_PRED_accounts
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA AS a
LEFT JOIN  HM_HH_key_ALL as b ON a.cb_key_household = b.hH_key
---------------------------------------
SELECt
          COUNT(*) total_rows
        , COUNT(DISTINCT hh_keY)                AS unique_HH
        , COUNT(rented_projection)              AS rented_proj_count
        , COUNT(sold_projection)                AS sold_proj_count
        , COUNT(actual_completion_date)         AS actual_count
        , COUNT(projected_completion_date)      AS proj_completion_count
        , last_source
        , last_st
        , CASE last_type_of_tx WHEN 1 THEN 'Renting'
                              WHEN 2 THEN 'Selling'
                              ELSE 'OTHER'
                              END               AS type_of_tx
        , SUM(empty_flag)               AS empty_count
        , SUM(valid_dt_flag)            AS valid_dt_count
from HM_EXP_PRED_accounts
----------------------------------------------------------------
---------Exporting HH keys
----------------------------------------------------------------
SELECT HH_key
	, last_dt last_dt_dum   
	, COALESCE (actual_completion_date, projected_completion_date) move_date_dum
FROM HM_EXP_PRED_accounts 
OUTPUT TO 'C:\Users\pitteloj\Documents\Home Moving\Experian_pred_keys' Format ASCII Delimited by ',' quote''
----------------------------------------------------------------
CREATE TABLE HM_CAL_accounts_temp
        ( ID int IDENTITY
        , hh_key BIGINT
        , last_dt VARCHAR(30) DEFAULT NULL
        , move_date VARCHAR(30) DEFAULT NULL)
		COMMIT

LOAD TABLE HM_CAL_accounts_temp
	(hh_key',',
    last_dt',',
    move_date‘\n’)
From '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/Experian_pred_keys.csv'
Quotes off
Escapes off
Notify 1000;
COMMIT

UPDATE HM_CAL_accounts
SET experian_flag = 1
WHERE EXISTS (SELECT hh_key FROM HM_CAL_accounts_temp as a WHERE a.hh_key = HM_CAL_accounts.hh_key)

DELETE FROM HM_CAL_accounts_temp WHERE EXISTS (SELECT hh_key FROM HM_CAL_accounts AS a WHERE a.hh_key = HM_CAL_accounts_temp.hh_key)
---------------------------------------
SELECT
          min(b.created_dt) dt
        , a.SKY_new_acct
INTO #temp1
FROM HM_CAL_accounts as a
JOIN sk_prod.CUST_SUBSCRIPTIONS as b ON CAST(a.SKY_new_acct AS VARCHAR) = b.account_number AND a.SKY_new_acct is not null
WHERE b.created_dt > a.last_dt
GROUP BY  a.SKY_new_acct

CREATE HG INDEX idx1 ON #temp1 (SKY_new_acct)

UPDATE HM_CAL_accounts
SET Sky__new_call_dt = dt
FROM HM_CAL_accounts as a
JOIN #temp1 as t ON t.SKY_new_acct = a.SKY_new_acct
commit
-------------------------------------------
SELECT top 10 account_number
        , last_dt
        , created_dt
        ,ent_cat_prod_start_dt
        ,first_activation_dt
        ,first_enablement_dt
        ,ph_subs_link_sk_start_dt
        ,prev_ent_cat_prod_start_dt
        ,prev_status_start_dt
        ,status_start_dt

FROM HM_CAL_accounts as a
JOIN sk_prod.CUST_SUBSCRIPTIONS as b ON CAST(a.SKY_new_acct AS VARCHAR) = b.account_number AND a.SKY_new_acct is not null
-------------------------------------






