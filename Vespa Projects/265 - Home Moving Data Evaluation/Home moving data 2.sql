CREATE TABLE HM_HH_key_ALL
(       ID                      int identity	--
        , HH_key        bigint not null			--
        , first_dt      datetime default null	--
        , first_st varchar(50) default null		
        , first_source varchar(50) default null	--
        , last_dt       datetime default null	--
        , last_st       varchar(50) default null 
        , last_source varchar(50) default null	--
        , EXP_RM_flag bit default 0                     -- Experian Rightmove flag
        , EXP_ZO_flag   bit default 0                   -- Experian zoopla flag
        , EXP_PR_flag   bit default 0                   -- Experian Predicitve model flag
        , CAL_ZO_flag   bit default 0                   -- Callcredit zoopla flag
        , completion_flag bit default 0         		-- if last status is sold / rented
        , last_type_of_tx int default 0                 -- 1 renting, 2 selling, else 0
        , primary key (HH_key)
        , EXP_RM_count int default 0		--
        , EXP_ZO_count int default 0		--
        , EXP_PR_count int default 0		--
        , CAL_ZO_count int default 0		--
        )
COMMIT

INSERT INTO HM_HH_key_ALL (HH_key)
SELECT DISTINCT cb_key_household
FROM sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA
UNION
SELECT DISTINCT cb_key_household
FROM sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE
UNION
SELECT DISTINCT cb_key_household
FROM sk_uat_data.EXPERIAN_MOVER_ALERTS
UNION
SELECT DISTINCT cb_key_household
FROM sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH
UNION
SELECT DISTINCT cb_key_household
top 1 * 
FROM sk_uat.CC_HOME_MOVERS
COMMIT
------------------------------------------------------------
------------------------------------------------------------
SELECT
          a.HH_key
        , max(c.filedate)  AS max_RM
        , max(d.dt)        AS max_ZO
        , CAST(max(CASE  WHEN    DATEDIFF(dd,
							COALESCE( RentedDate, RentUnderOfferDate, ToRentDate,'1900-01-01'),
                            COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate, '1900-01-01') ) <=0 THEN
                                COALESCE(RentedDate, RentUnderOfferDate, ToRentDate)
                                ELSE COALESCE(SoldDate,SaleUnderOfferDate,ForSaleDate) END)
                                AS DATE) AS max_CAL_ZO
        , min(c.filedate) AS min_RM
        , min(d.dt)       AS min_ZO
        , CAST( min(CASE WHEN       DATEDIFF(dd,   COALESCE(ToRentDate,RentUnderOfferDate,RentedDate,'2100-12-31') ,
                                COALESCE(ForSaleDate,SaleUnderOfferDate,SoldDate,'2100-12-31')  ) >=0     THEN
                                COALESCE(ToRentDate,RentUnderOfferDate,RentedDate)
                                ELSE COALESCE(ForSaleDate, SaleUnderOfferDate,SoldDate)END)
                                AS DATE) AS min_CAL_ZO
        , count(DISTINCT URN)                                                   AS CC_count
        , count(DISTINCT property_id)                                   AS RM_count
        , count(DISTINCT experian_ref)                                  AS EXP_ZO_count
                , count(DISTINCT b.cb_key_household)                    AS EXP_PR_count
        , EXP_RM_flag = max(CASE WHEN c.cb_key_household is null THEN 0 ELSE 1 END)
        , EXP_ZO_flag = max(CASE WHEN d.cb_key_household is null THEN 0 ELSE 1 END)
        , EXP_PR_flag = MAX(CASE WHEN b.cb_key_household is null THEN 0 ELSE 1 END)
        , CAL_ZO_flag = MAX(CASE WHEN e.cb_key_household is null THEN 0 ELSE 1 END)
INTO #TEMP1
FROM  HM_HH_key_ALL AS a
LEFT JOIN  sk_uat_data.EXPERIAN_MOVER_PREDICTION_DATA   AS b ON a.HH_key = b.cb_key_household
LEFT JOIN  sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE                 AS c ON a.HH_key = c.cb_key_household
LEFT JOIN  sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH             AS d ON a.HH_key = d.cb_key_household
LEFT JOIN  sk_uat.CC_HOME_MOVERS                                                AS e ON a.HH_key = e.cb_key_household
GROUP BY a.HH_key
commit
CREATE HG INDEX idx01 on #TEMP1(HH_key)

UPDATE HM_HH_key_ALL
SET     last_dt = CASE  WHEN DATEDIFF(dd,max_RM,COALESCE(max_ZO,max_RM)) <= 0 
								AND DATEDIFF(dd,max_RM,COALESCE(max_CAL_ZO,max_RM))<= 0 THEN max_RM
                        WHEN DATEDIFF(dd,max_CAL_ZO,COALESCE(max_RM,max_CAL_ZO)) <= 0
								AND DATEDIFF(dd,max_CAL_ZO,COALESCE(max_ZO,max_CAL_ZO))<= 0 THEN max_CAL_ZO
                        WHEN DATEDIFF(dd,max_ZO,COALESCE(max_RM,max_ZO)) <= 0 
								AND DATEDIFF(dd,max_ZO,COALESCE(max_CAL_ZO,max_ZO))<= 0 THEN max_ZO
                        ELSE null END
        ,first_dt = CASE  WHEN DATEDIFF(dd,min_RM,COALESCE(min_ZO,min_RM)) >= 0 
								AND DATEDIFF(dd,min_RM,COALESCE(min_CAL_ZO,min_RM))>= 0 THEN min_RM
                        WHEN DATEDIFF(dd,min_CAL_ZO,COALESCE(min_RM,min_CAL_ZO)) >= 0
								AND DATEDIFF(dd,min_CAL_ZO,COALESCE(min_ZO,min_CAL_ZO))>= 0 THEN min_CAL_ZO
                        WHEN DATEDIFF(dd,min_ZO,COALESCE(min_RM,min_ZO)) >= 0 
								AND DATEDIFF(dd,min_ZO,COALESCE(min_CAL_ZO,min_ZO))>= 0 THEN min_ZO
                        ELSE null END
					
        ,last_source  = CASE  WHEN DATEDIFF(dd,max_RM,COALESCE(max_ZO,max_RM)) <= 0 
								AND DATEDIFF(dd,max_RM,COALESCE(max_CAL_ZO,max_RM))<= 0 THEN 'EXP_RM'
                        WHEN DATEDIFF(dd,max_CAL_ZO,COALESCE(max_RM,max_CAL_ZO)) <= 0
								AND DATEDIFF(dd,max_CAL_ZO,COALESCE(max_ZO,max_CAL_ZO))<= 0 THEN 'CAL_ZO'
                        WHEN DATEDIFF(dd,max_ZO,COALESCE(max_RM,max_ZO)) <= 0 
								AND DATEDIFF(dd,max_ZO,COALESCE(max_CAL_ZO,max_ZO))<= 0 THEN 'EXP_ZO'
                        ELSE null END
		,first_source = CASE  WHEN DATEDIFF(dd,min_RM,COALESCE(min_ZO,min_RM)) >= 0 
								AND DATEDIFF(dd,min_RM,COALESCE(min_CAL_ZO,min_RM))>= 0 THEN 'EXP_RM'
                        WHEN DATEDIFF(dd,min_CAL_ZO,COALESCE(min_RM,min_CAL_ZO)) >= 0
								AND DATEDIFF(dd,min_CAL_ZO,COALESCE(min_ZO,min_CAL_ZO))>= 0 THEN 'CAL_ZO'
                        WHEN DATEDIFF(dd,min_ZO,COALESCE(min_RM,min_ZO)) >= 0 
								AND DATEDIFF(dd,min_ZO,COALESCE(min_CAL_ZO,min_ZO))>= 0 THEN 'EXP_ZO'
                        ELSE null END
    ,a.EXP_RM_flag = CASE WHEN b.EXP_RM_flag = 1 THEN 1 ELSE 0 END
    ,a.EXP_ZO_flag = CASE WHEN b.EXP_ZO_flag = 1 THEN 1 ELSE 0 END
    ,a.EXP_PR_flag = CASE WHEN b.EXP_PR_flag = 1 THEN 1 ELSE 0 END
    ,a.CAL_ZO_flag = CASE WHEN b.CAL_ZO_flag = 1 THEN 1 ELSE 0 END
    ,a.EXP_RM_count = b.RM_count
    ,a.EXP_ZO_count = b.EXP_ZO_count
    ,a.EXP_PR_count = b.EXP_PR_count
    ,a.CAL_ZO_count = b.CC_count
FROM  HM_HH_key_ALL AS a
LEFT JOIN #TEMP1 as b ON a.HH_key = b.HH_key

commit

------------------------------------------------------------
--UPDATING RM flags
------------------------------------------------------------
SELECT
         HH_key
        , rank() OVER(PARTITION BY HH_key ORDER BY c.filedate DESC)  rank_max_DT
        , rank() OVER(PARTITION BY HH_key ORDER BY c.filedate ASC)  rank_min_DT
		, CASE  WHEN reason_for_inclusion IN ('New Resale' )                                                                    THEN 'FOR SALE'
                WHEN reason_for_inclusion IN ('Status Updated to Let Agreed')                                                   THEN 'RENT UNDER OFFER'
                WHEN reason_for_inclusion IN ('Removed & Archived Rental','Removed Invisible Rental','Removed Rental')          THEN 'RENTED'
                WHEN reason_for_inclusion IN ('Status Update to SSTC/Under offe','Status Update to SSTC/Under offer')           THEN 'SALE UNDER OFFER'
                WHEN reason_for_inclusion IN ('Removed Resale','Removed & Archived Resale'
                                                ,'Removed Invisible Resale','Status Update to Sold')                            THEN 'SOLD'
                WHEN reason_for_inclusion IN ('New Rental')                                                                     THEN 'TO RENT'
                ELSE 'OTHER' END AS status_2
        , CASE  WHEN status_2 IN ('RENT UNDER OFFER', 'RENTED','TO RENT') THEN 'RENTING'
                        WHEN status_2 IN ('FOR SALE','SALE UNDER OFFER','SOLD' ) THEN 'SELLING'
                                ELSE NULL       END AS Type_of_tx
        , CASE  WHEN status_2 IN ( 'RENTED','SOLD') THEN 'COMPLETED'
                        WHEN status_2 IN ('FOR SALE','TO RENT' ) THEN 'RUNNING'
						WHEN status_2 IN ('SALE UNDER OFFER','RENT UNDER OFFER' ) THEN 'UNDER OFFER'
                                ELSE NULL       END AS completion_FLAG
        , c.filedate
INTO #TEMP1
FROM  HM_HH_key_ALL AS a
INNER JOIN  sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE                 AS c ON a.HH_key = c.cb_key_household
WHERE last_source = 'EXP_RM'

UPDATE HM_HH_key_ALL
SET last_st     =       CASE WHEN last_source = 'EXP_RM'
                                AND rank_max_DT = 1
                                AND a.HH_key = b.HH_key
                        THEN status_2 ELSE last_st END        ,
    a.completion_flag =   CASE WHEN b.completion_FLAG =  'COMPLETED'
                                AND last_source = 'EXP_RM'
                                AND rank_max_DT = 1
                                AND a.HH_key = b.HH_key
                        THEN 1 ELSE     a.completion_flag  END  ,
    last_type_of_tx = CASE WHEN Type_of_tx =  'RENTING' AND last_source = 'EXP_RM'
                                AND rank_max_DT = 1 AND a.HH_key = b.HH_key THEN 1
                           WHEN Type_of_tx = 'SELLING' AND last_source = 'EXP_RM'
                                AND rank_max_DT = 1 AND a.HH_key = b.HH_key THEN 2
                           ELSE last_type_of_tx END
FROM HM_HH_key_ALL      AS a
JOIN #TEMP1 AS b        ON a.HH_key = b.HH_key AND rank_max_DT  = 1
DROP TABLE #TEMP1
commit
--------------------------------------------------------
--UPDATING EXP ZOOPLA flags
--------------------------------------------------------
SELECT
         cb_key_household
        , rank() OVER(PARTITION BY HH_key ORDER BY dt DESC)  rank_max_DT
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

INTO #TEMP1
FROM  sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH                 
DELETE FROM #TEMP1 WHERE rank> 1

SELECT count(cb_key_household) hits, status_2 fROM #TEMP1 GrOUP BY status_2
DROP TABLe #temp1

UPDATE HM_HH_key_ALL
SET last_st     =       CASE WHEN last_source = 'EXP_ZO'
                                AND rank_max_DT = 1
                                AND a.HH_key = b.HH_key
                        THEN status_2 ELSE last_st END        ,
    a.completion_flag =   CASE WHEN b.completion_FLAG =  'COMPLETED'
                                AND last_source = 'EXP_ZO'
                                AND rank_max_DT = 1
                                AND a.HH_key = b.HH_key
                        THEN 1 ELSE     a.completion_flag  END  ,
    last_type_of_tx = CASE WHEN Type_of_tx =  'RENTING' AND last_source = 'EXP_ZO'
                                AND rank_max_DT = 1 AND a.HH_key = b.HH_key THEN 1
                           WHEN Type_of_tx = 'SELLING' AND last_source = 'EXP_ZO'
                                AND rank_max_DT = 1 AND a.HH_key = b.HH_key THEN 2
                           ELSE last_type_of_tx END
FROM HM_HH_key_ALL      AS a
JOIN #TEMP1 AS b        ON a.HH_key = b.HH_key AND rank_max_DT  = 1
WHERE last_source = 'EXP_ZO'
DROP TABLE #TEMP1
commit
--------------------------------------------------------
--UPDATING Calcredit ZOOPLA flags
--------------------------------------------------------
SELECT
         HH_key
		, COALESCE(RentedDate_Derived, RentedDate, RentUnderOfferDate_Derived, RentUnderOfferDate, ToRentDate_Derived ,	ToRentDate,'1900-01-01') 	AS RENT_dt
        , COALESCE(SoldDate_Derived,SoldDate,SaleUnderOfferDate_Derived,SaleUnderOfferDate,ForSaleDate_Derived,ForSaleDate, '1900-01-01') 			AS SELL_dt
		, CASE WHEN  DATEDIFF(dd, RENT_dt, SELL_dt) <=0 THEN 'rent' ELSE 'sell' END AS last_st1
        , CASE  WHEN  last_st1 = 'rent' AND (RENT_dt = RentedDate) THEN 'RENTED'
				WHEN  last_st1 = 'rent' AND (RENT_dt = RentUnderOfferDate) THEN 'RENT UNDER OFFER'
				WHEN  last_st1 = 'rent' AND (RENT_dt = ToRentDate) THEN 'TO RENT'
				WHEN  last_st1 = 'sell' AND (SELL_dt = SoldDate) THEN 'SOLD'
				WHEN  last_st1 = 'sell' AND (SELL_dt = SaleUnderOfferDate) THEN 'SALE UNDER OFFER'
				WHEN  last_st1 = 'sell' AND (SELL_dt = ForSaleDate) THEN 'FOR SALE'
				ELSE 'OTHER' END 
			AS status_2
        , CASE  WHEN status_2 IN ('RENT UNDER OFFER', 'RENTED','TO RENT') THEN 'RENTING'
                        WHEN status_2 IN ('FOR SALE','SALE UNDER OFFER','SOLD' ) THEN 'SELLING'
                                ELSE NULL       END AS Type_of_tx
        , CASE  WHEN status_2 IN ( 'RENTED','SOLD') THEN 'COMPLETED'
                        WHEN status_2 IN ('FOR SALE','TO RENT' ) THEN 'RUNNING'
						WHEN status_2 IN ('SALE UNDER OFFER','RENT UNDER OFFER' ) THEN 'UNDER OFFER'
                                ELSE NULL       END AS completion_FLAG
INTO #TEMP1
FROM  HM_HH_key_ALL AS a
INNER JOIN  sk_uat.CC_HOME_MOVERS                      AS c ON a.HH_key = c.cb_key_household
WHERE last_source = 'CAL_ZO'
UPDATE HM_HH_key_ALL
SET last_st     =       CASE WHEN last_source = 'CAL_ZO'
                               AND a.HH_key = b.HH_key
                        THEN status_2 ELSE last_st END        ,
    a.completion_flag =   CASE WHEN b.completion_FLAG =  'COMPLETED'
                                AND last_source = 'CAL_ZO'
                                AND a.HH_key = b.HH_key
                        THEN 1 ELSE     a.completion_flag  END  ,
    last_type_of_tx = CASE WHEN Type_of_tx =  'RENTING' AND last_source = 'CAL_ZO'
                                 AND a.HH_key = b.HH_key THEN 1
                           WHEN Type_of_tx = 'SELLING' AND last_source = 'CAL_ZO'
                                 AND a.HH_key = b.HH_key THEN 2
                           ELSE last_type_of_tx END
FROM HM_HH_key_ALL      AS a
JOIN #TEMP1 AS b        ON a.HH_key = b.HH_key
WHERE last_source = 'CAL_ZO'
DROP TABLE #TEMP1
commit

--------------------------------------------------------
--UPDATING first st EXP RM
--------------------------------------------------------
SELECT
	 HH_key
	, rank() OVER(PARTITION BY HH_key ORDER BY c.filedate ASC)  rank_min_DT
	, CASE  WHEN reason_for_inclusion IN ('New Resale' )                                                                    THEN 'FOR SALE'
			WHEN reason_for_inclusion IN ('Status Updated to Let Agreed')                                                   THEN 'RENT UNDER OFFER'
			WHEN reason_for_inclusion IN ('Removed & Archived Rental','Removed Invisible Rental','Removed Rental')          THEN 'RENTED'
			WHEN reason_for_inclusion IN ('Status Update to SSTC/Under offe','Status Update to SSTC/Under offer')           THEN 'SALE UNDER OFFER'
			WHEN reason_for_inclusion IN ('Removed Resale','Removed & Archived Resale'
											,'Removed Invisible Resale','Status Update to Sold')                            THEN 'SOLD'
			WHEN reason_for_inclusion IN ('New Rental')                                                                     THEN 'TO RENT'
			ELSE 'OTHER' END AS status_2
	, c.filedate
INTO #TEMP1
FROM  HM_HH_key_ALL AS a
INNER JOIN  sk_uat_data.EXPERIAN_MOVER_RIGHTMOVE                 AS c ON a.HH_key = c.cb_key_household
WHERE first_source = 'EXP_RM'
UPDATE HM_HH_key_ALL
SET first_st     =       CASE WHEN first_source = 'EXP_RM'
                                AND rank_min_DT = 1
                                AND a.HH_key = b.HH_key
                        THEN status_2 ELSE first_st END   
FROM HM_HH_key_ALL      AS a
JOIN #TEMP1 AS b        ON a.HH_key = b.HH_key AND rank_min_DT  = 1
DROP TABLE #TEMP1
commit
--------------------------------------------------------
--UPDATING EXP ZOOPLA first st flags
--------------------------------------------------------
SELECT
         HH_key
        , rank() OVER(PARTITION BY HH_key ORDER BY c.dt ASC)  rank_min_DT
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
            , c.dt
INTO #TEMP1
FROM  HM_HH_key_ALL AS a
INNER JOIN  sk_uat_data.EXPERIAN_MOVER_ALERTS_PER_HH                 AS c ON a.HH_key = c.cb_key_household
WHERE first_source = 'EXP_ZO'

UPDATE HM_HH_key_ALL
SET first_st     =       CASE WHEN first_source = 'EXP_ZO'
                                AND rank_min_DT = 1
                                AND a.HH_key = b.HH_key
                        THEN status_2 ELSE first_st END        

FROM HM_HH_key_ALL      AS a
JOIN #TEMP1 AS b        ON a.HH_key = b.HH_key AND rank_min_DT  = 1
WHERE first_source = 'EXP_ZO'
DROP TABLE #TEMP1
commit
--------------------------------------------------------
--UPDATING Calcredit ZOOPLA flags
--------------------------------------------------------
SELECT
         HH_key
		, COALESCE(ToRentDate,ToRentDate_Derived ,RentUnderOfferDate,RentUnderOfferDate_Derived,RentedDate,RentedDate_Derived,'2014-10-01') 	AS RENT_dt
        , COALESCE(ForSaleDate,ForSaleDate_Derived,SaleUnderOfferDate,SaleUnderOfferDate_Derived,SoldDate,SoldDate_Derived, '2014-10-01') 			AS SELL_dt
		, CASE WHEN  DATEDIFF(dd, RENT_dt, SELL_dt) >=0 THEN 'rent' ELSE 'sell' END AS last_st1
        , CASE  WHEN  last_st1 = 'rent' AND (RENT_dt = RentedDate) THEN 'RENTED'
				WHEN  last_st1 = 'rent' AND (RENT_dt = RentUnderOfferDate) THEN 'RENT UNDER OFFER'
				WHEN  last_st1 = 'rent' AND (RENT_dt = ToRentDate) THEN 'TO RENT'
				WHEN  last_st1 = 'sell' AND (SELL_dt = SoldDate) THEN 'SOLD'
				WHEN  last_st1 = 'sell' AND (SELL_dt = SaleUnderOfferDate) THEN 'SALE UNDER OFFER'
				WHEN  last_st1 = 'sell' AND (SELL_dt = ForSaleDate) THEN 'FOR SALE'
				ELSE 'OTHER' END 
			AS status_2
INTO #TEMP1
FROM  HM_HH_key_ALL AS a
INNER JOIN  sk_uat.CC_HOME_MOVERS                      AS c ON a.HH_key = c.cb_key_household
WHERE first_source = 'CAL_ZO'
UPDATE HM_HH_key_ALL
SET first_st     =       CASE WHEN first_source = 'CAL_ZO'
                               AND a.HH_key = b.HH_key
                        THEN status_2 ELSE first_st END        
 
FROM HM_HH_key_ALL      AS a
JOIN #TEMP1 AS b        ON a.HH_key = b.HH_key
WHERE first_source = 'CAL_ZO'
DROP TABLE #TEMP1
commit
---------------------------------------------------------------------
-------------------- EXTRACTING Attributes from OLIVE
---------------------------------------------------------------------
CREATE TABLE HM_accounts_all_EXP_cal
(hH_key bigint default null
	, first_dt DATE DEFAULT NULL
	, last_dt DATE DEFAULT NULL)
COMMIT

LOAD TABLE HM_accounts_all_EXP_cal
(	hH_key',',
	first_dt',',
	last_dt'\n')
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/ALL_accounts_CAL_and_EXP.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000;

INSERT INTO HM_accounts_all_cal
SELECT
          CAST (ID AS INT)
        , CAST (hh_key aS bigint)
        , CAST (first_dt as DATE)
        , CAST (last_dt as DATE)
FROM  HM_accounts_all_EXP_cal

CREATE HG INDEX idx1 ON HM_accounts_all_cal(hh_key)

ALTER TABLE HM_accounts_all_cal
ADD ( Sky_Churn_account 	bigint default null
	, Sky_Churn_act_dt 		DATE default null
	, Sky_Churn_call_dt		DATE default null
	, Sky_Cancellation_dt	DATE default null
	, Sky_New_account 		bigint default null
	, SKY_new_request_dt	DATE default null
    , Sky_new_activation_dt DATE default null
    , Region				VARCHAR (50) Default null
	, Region_4				VARCHAR (50) Default null
	, lifestage				VARCHAR (30) Default null
	, lifestage_v2			VARCHAR (20) Default null
	, lifestage_v3			VARCHAR (20) Default null
	, affluence				VARCHAR (15) Default null
	, affluence_v2			VARCHAR (10) Default null
	, Mosaic				VARCHAR (50) Default null
	, property_type			VARCHAR (20) Default null
	, council_tax_band		VARCHAR (50) Default null
	, council_tax_band_v2	VARCHAR (10) Default null
	, tenure				VARCHAR (30) Default null
	, social_class			VARCHAR (5) Default null
	, current_package		VARCHAR (20) Default null
	, hh_composition 		VARCHAR (30) Default null)
COMMIT

SELECT cb_key_household
	, account_number
	, acct_first_account_activation_dt
	, INSTALL_CB_KEY_HOUSEHOLD
	, install_addr_effective_from_dt
	, 

	
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
JOIN HM_accounts_all_EXP_cal as b ON b.HH_key = sav.cb_key_
WHERE 
--------------------------------------------------------------------------

SET dateformat DMY;
LOAD TABLE HM_ALL_Attrib
(       ID',',
        hH_key',',
        first_dt',',
        last_dt'\n')
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/JosEP/All_keys2.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000;
--------------------------------------------------------------------------
SELECT account_number
        , acct_first_account_activation_dt
        , cb_key_household
        , rank () OVER (PARTITION BY account_number ORDER BY acct_first_account_activation_dt ASC) as rank1
INTO #temp1
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
JOIN HM_ALL_Attrib as b ON b.HH_key = SAV.cb_key_household
WHERE acct_first_account_activation_dt >= last_dt

DELETE FROM #temp1  WHERE rank1 > 1
COMMIT
CREATE HG INDEX idv1 ON #temp1 (cb_key_household)

UPDATE HM_ALL_Attrib
SET SKY_new_account = account_number,
        Sky_new_activation_dt = acct_first_account_activation_dt
FROM   HM_ALL_Attrib            AS a
JOIN   #temp1                   AS b ON a.HH_key = b.cb_key_household
COMMIT
DROP TABLE #temp1
--------------------------------------------------------------------------
SELECT account_number
        , acct_first_account_activation_dt
        , cb_key_household
        , rank () OVER (PARTITION BY account_number ORDER BY acct_first_account_activation_dt DESC) as rank1
INTO #temp1
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
JOIN HM_ALL_Attrib as b ON b.HH_key = SAV.cb_key_household
WHERE acct_first_account_activation_dt <= last_dt

DELETE FROM #temp1  WHERE rank1 > 1
COMMIT
CREATE HG INDEX idv1 ON #temp1 (cb_key_household)

UPDATE HM_ALL_Attrib
SET Sky_Churn_account 	= account_number,
    Sky_Churn_act_dt 	= acct_first_account_activation_dt
FROM   HM_ALL_Attrib            AS a
JOIN   #temp1                   AS b ON a.HH_key = b.cb_key_household
COMMIT
DROP TABLE #temp1
--------------------------------------------------------------------------
--------------------------------------------------------------------------
SELECT
	 
	account_number
	, Region
	, CASE WHEN Region in ('Liverpool/Birkenhead metropolitan area',
		'HTV Wales','Cardiff and South Wales valleys metropolitan area',
		'Yorkshire','Midlands','Manchester metropolitan area',
		'Birmingham metropolitan area','Leicester metropolitan area',
		'Leeds-Bradford metropolitan area','Nottingham-Derby metropolitan area',
		'North-West','Sheffield metropolitan area','East-of-England') then 'M'
		WHEN region in ('Border-England','Central Scotland',
		'Border-Scotland','Newcastle-Sunderland metropolitan area',
		'Edinburgh metropolitan area','Ulster','Belfast metropolitan area',
		'Northern Scotland','Glasgow metropolitan area','North-East') THEN 'N'
		WHEN region IN ('South-West','Meridian','HTV West','Channel Islands','Brighton/Worthing/Littlehampton metropolitan area',
		'Bristol metropolitan area','Portsmouth/Southampton metropolitan area') THEN 'S'
		WHEN region = 'London' THEN 'L'
		ELSE 'X' END region2
	, affluence_bands
	, children_in_hh
	, current_package
	, h_lifestage
	, CASE 	WHEN h_lifestage like 'Elderly%' THEN 'E'
			WHEN h_lifestage like 'Older%' THEN 'O'
			WHEN h_lifestage like 'Mature%' THEN 'M'
			WHEN h_lifestage like 'Young%' THEN 'Y'
			WHEN h_lifestage like 'Very%' THEN 'V'
			ELSE 'X' END 			AS lifestage_2
	, CASE 	WHEN h_lifestage like '%single%' THEN 'S'
			WHEN h_lifestage like '%homeshares%' THEN 'O'
			WHEN h_lifestage like '%family%' THEN 'F'
			ELSE 'X' END			AS lifestage_3
	, home_owner_status
	, income_bands AS affluence_2
	, household_composition
	, mosaic_segments
	, social_class
	, RANK() OVER (PARTITION BY account_number ORDER BY acct_first_account_activation_dt ASC) AS rank1
INTO #temp
FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW as SAV
JOIN HM_ALL_Attrib as b ON b.SKY_new_account = SAV.account_number

DELETE FROM #temp WHERE rank1 > 1 
CREATE HG INDEX index1 ON #temp(account_number)
COMMIT

UPDATE HM_ALL_Attrib
SET   a.region 			= t.region
	, a.Region_4 		= region2
	, a.lifestage		= h_lifestage
	, a.lifestage_v2	= lifestage_2			
	, a.lifestage_v3 	= lifestage_3		
	, a.affluence		= affluence_bands		
	, a.affluence_v2	= affluence_2
	, a.Mosaic			= mosaic_segments		
	, a.social_class	= t.social_class	
	, a.current_package	= t.current_package	
	, a.hh_composition 	= t.household_composition
	, a.home_owner_status =	t.home_owner_status 
	, a.children_in_hh 	= t.children_in_hh
FROM HM_ALL_Attrib as a
JOIN #temp as t ON a.SKY_new_account = t.account_number
commit

SELECT DISTINCT 
	, cb_key_household
	, CASE WHEN sav.property_type = 'Unclassified'  THEN
                        (CASE  h_residence_type_v2
                                when '0' then 'Detached'
                                when '1' then 'Semi-detached'
                                when '2' then 'Bungalow'
                                when '3' then 'Terraced'
                                when '4' then 'Flat'
                                when 'U' then 'Unclassified'
                                else null end)
                          ELSE sav.property_type END  Residence_Type                       	
	, ex.h_property_council_taxation 
	, ex.tenure
	, RANK() OVER (PARTITION BY cb_key_household ORDER BY cb_key_individual ASC) AS rank1
INTO #temp
FROM sk_prod.EXPERIAN_CONSUMERVIEW AS ex
JOIN HM_ALL_Attrib as b ON b.hh_key = ex.cb_key_household
COMMIT 

DELETE FROM #temp WHERE rank1 > 1 
CREATE HG INDEX index1 ON #temp(cb_key_household)
COMMIT

UPDATE HM_ALL_Attrib
SET   a.council_tax_band 	= ex.h_property_council_taxation
	, a.property_type		= ex.Residence_Type			
	, a.tenure				= ex.tenure
FROM HM_ALL_Attrib AS a 
JOIN #temp AS t ON a.hh_key = t.cb_key_household

--------------------------------------------------------------------------
----- 		QA -----------------------------------------------------------
--------------------------------------------------------------------------
-- Checking hh_keys duplicates
SELECT top 100 HH_key , count(*) hits from HM_HH_key_ALL
GROUP BY HH_key
HAVING hits >1

SELECT COALESCE(RentedDate_Derived, RentedDate, RentUnderOfferDate_Derived, RentUnderOfferDate, ToRentDate_Derived ,ToRentDate) i2
       , COALESCE(SoldDate_Derived,SoldDate,SaleUnderOfferDate_Derived,SaleUnderOfferDate,ForSaleDate_Derived,ForSaleDate) i3
       , DATEDIFF(dd, getdatE(), COALESCE(SoldDate_Derived,SoldDate,SaleUnderOfferDate_Derived,SaleUnderOfferDate,ForSaleDate_Derived,ForSaleDate)) dd2
FROM          sk_uat.CC_HOME_MOVERS  WHERE cb_key_household = 1961725312106496





