
SELECT DISTINCT top 10
FROM sleary.callcredit_admart_evaluation_20141117

userfield_1
userfield_2
userfield_3
userfield_4
userfield_5
userfield_6
userfield_7
userfield_8


--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ MOBILE Dataset		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Creating working view----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW Calcredit_mobile
AS
SELECT
	row_number () OVER (ORDER BY a.cb_key_household, a.cb_key_family, a.cb_key_individual) row_id
	 , set_code    		AS Model_code
	 , 'Mobile' 		AS Model_name
	, a.cb_key_family   
	, a.cb_key_household    
	, a.cb_key_individual   
	, a.cb_address_postcode 
	, a.cb_address_postcode_area 
	, a.cb_address_postcode_outcode 
	, userfield_1 		AS provider				--Mobile Phone Provider
	, userfield_3		AS head_of_hh			--Head of Household
	, CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
	, v1.scaling_segment_ID
	, w.weighting 
FROM sleary.callcredit_admart_evaluation_20141117 as a 
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 
LEFT JOIN ( SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
			FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
			JOIN vespa_analysts.SC2_intervals 			as l	ON ve.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
			WHERE panel_id_vespa in (11, 12)
			AND status_vespa = 'Enabled') AS v1 ON v1.cb_key_household = a.cb_key_household 
LEFT JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID
WHERE set_code  ='S_0606'




UPDATE Calcredit_mobile1
SET a.weighting = w.weighting
FROM Calcredit_mobile1 as a
JOIN ( SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
            FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
            JOIN vespa_analysts.SC2_intervals           as l    ON sav.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
            ) AS v1 ON v1.cb_key_household = a.cb_key_household
JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID


--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Distinct values		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT cb_address_postcode_area, count (*)   hits  	FROM Calcredit_mobile 	GROUP BY cb_address_postcode_area
SELECT DISTINCT provider, count (*)   hits                   	FROM Calcredit_mobile 	GROUP BY provider 
SELECT DISTINCT head_of_hh, count (*)   hits                    FROM Calcredit_mobile 	GROUP BY head_of_hh
SELECT DISTINCT cb_address_postcode_outcode, count (*)   hits   FROM Calcredit_mobile 	GROUP BY cb_address_postcode_outcode
------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------- Distinct values		----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
SELECT 'Household' Level_, provider, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) experian, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_mobile		AS a
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
GROUP BY provider

SELECT DISTINCT cb_address_postcode_area, count (*)   hits  	FROM Calcredit_mobile 	
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
	GROUP BY cb_address_postcode_area
SELECT DISTINCT provider, count (*)   hits                   	FROM Calcredit_mobile 	
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
	GROUP BY provider 
SELECT DISTINCT head_of_hh, count (*)   hits                    FROM Calcredit_mobile 	
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
	GROUP BY head_of_hh
SELECT DISTINCT cb_address_postcode_outcode, count (*)   hits   FROM Calcredit_mobile 	
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
	GROUP BY cb_address_postcode_outcode
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Duplicates keys		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_individual, count(*) hits FROM Calcredit_mobile GROUP BY cb_key_individual) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(*) hits FROM Calcredit_mobile GROUP BY cb_key_household) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(*) hits FROM Calcredit_mobile GROUP BY cb_key_family) as v
GROUP BY hits
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Duplicates keys	-Adsmartables ------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT      hits,   count(*) count_
FROM (SELECT a.cb_key_individual, count(*) hits FROM Calcredit_mobile as a
       WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0 AND cv.account_number IS NOT NULL)
        GROUP BY a.cb_key_individual) as v
GROUP BY hits
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT      hits,   count(*) count_
FROM (SELECT a.cb_key_household, count(*) hits FROM Calcredit_mobile as a
       WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0 AND cv.account_number IS NOT NULL)
        GROUP BY a.cb_key_household) as v
GROUP BY hits
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT      hits,   count(*) count_
FROM (SELECT a.cb_key_family, count(*) hits FROM Calcredit_mobile as a
       WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
        GROUP BY a.cb_key_family) as v
GROUP BY hits

------------------------------------------------------------------ HH Composition		----------------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(DISTINCT cb_key_individual) hits FROM Calcredit_mobile GROUP BY cb_key_family) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(DISTINCT cb_key_individual) hits FROM Calcredit_mobile GROUP BY cb_key_household) as v
GROUP BY hits
------------------------------------------------------------------ HH Composition	- Adsmartables ------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(DISTINCT cb_key_individual) hits FROM Calcredit_mobile 
		 WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
		GROUP BY cb_key_family) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(DISTINCT cb_key_individual) hits FROM Calcredit_mobile 
		 WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
		 GROUP BY cb_key_household) as v
GROUP BY hits
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Match rates			----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Experian  match		----------------------------------------------------
SELECT 'Household' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) exp_hh, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_mobile		AS a
JOIN experian_consumerview 	AS cv ON a.cb_key_household = cv.cb_key_household 
UNION
SELECT 'Family' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_family) experian, count(DISTINCT a.cb_key_family) unique
FROM Calcredit_mobile		AS a
JOIN experian_consumerview 	AS cv ON a.cb_key_family = cv.cb_key_family 
UNION 
SELECT 'Individual' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_individual) experian, count(DISTINCT a.cb_key_individual) unique
FROM Calcredit_mobile		AS a
JOIN experian_consumerview 	AS cv ON a.cb_key_individual = cv.cb_key_individual 
------------------------------------------------------------------ Sky Base 			----------------------------------------------------

SELECT 'Household' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) exp_hh, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_mobile		AS a
JOIN cust_single_account_view AS cv ON a.cb_key_household = cv.cb_key_household 
WHERE    cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND account_number IS NOT NULL
UNION
SELECT 'Family' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_family) experian, count(DISTINCT a.cb_key_family) unique
FROM Calcredit_mobile		AS a
JOIN cust_single_account_view 	AS cv ON a.cb_key_family = cv.cb_key_family 
WHERE    cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND account_number IS NOT NULL
UNION 
SELECT 'Individual' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_individual) experian, count(DISTINCT a.cb_key_individual) unique
FROM Calcredit_mobile		AS a
JOIN cust_single_account_view 	AS cv ON a.cb_key_individual = cv.cb_key_individual 
WHERE    cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND account_number IS NOT NULL

------------------------------------------------------------------ 	Adsmart			----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
SELECT base.account_number
      ,CASE  WHEN x_pvr_type ='PVR6'                                THEN 1
             WHEN x_pvr_type ='PVR5'                                THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Samsung' THEN 1
             WHEN x_pvr_type ='PVR4' AND x_manufacturer = 'Pace'    THEN 1
                                                                    ELSE 0       END AS Adsmartable
      ,SUM(Adsmartable) AS T_AdSm_box
INTO SetTop
FROM   CUST_SET_TOP_BOX  AS SetTop
inner join AdSmart as Base         on SetTop.account_number = Base.account_number
WHERE box_replaced_dt = '9999-09-09'
GROUP BY base.account_number
	,x_pvr_type
	,x_manufacturer
	,box_replaced_dt;
commit;
------------------------------------------------------------------
select distinct(account_number), sum(T_AdSm_box) AS T_ADMS
into kjdl
from SetTop
GROUP BY account_number;
commit;
------------------------------------------------------------------      create index on SetTop
CREATE   HG INDEX idx10 ON kjdl(account_number);
commit;
------------------------------------------------------------------
SELECT DISTINCT a.account_number, b.cb_key_household, b.cb_key_individual
INTO adsmartables_20141126
FROM kjdl                       AS  a
JOIN cust_single_account_view   AS  b ON a.account_number = b.account_number AND b.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'
WHERE  cust_active_dtv = 1
AND a.T_ADMS > 0
commit
CREATE HG INDEX qwd ON adsmartables_20141126 (account_number)
CREATE HG INDEX ewf ON adsmartables_20141126 (cb_key_household)
------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
SELECT 'Household' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) experian, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_mobile		AS a
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
UNION
SELECT 'Individual' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_individual) experian, count(DISTINCT a.cb_key_individual) unique_hh
FROM Calcredit_mobile		AS a
JOIN adsmartables_20141126 	AS cv ON a.cb_key_individual = cv.cb_key_individual 
	WHERE cv.cb_key_individual > 0             	AND cv.account_number IS NOT NULL


------------------------------------------------------------------ 	Panel			----------------------------------------------------

SELECT 'Household' Level_, count(DISTINCT a.row_id) calcredit, count(cv.cb_key_household) experian, count(DISTINCT a.cb_key_household) unique_key
FROM Calcredit_mobile						AS a
JOIN cust_single_account_view 				AS cv ON a.cb_key_household = cv.cb_key_household 
JOIN Vespa_Analysts.Vespa_Single_Box_View 	AS c  ON c.account_number 	= cv.account_number AND panel_ID_vespa in(11,12) And status_vespa = 'Enabled'
WHERE    cv.cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
UNION
SELECT 'Family' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_family) experian, count(DISTINCT a.cb_key_family) unique_key
FROM Calcredit_mobile		AS a
JOIN cust_single_account_view 	AS cv ON a.cb_key_family = cv.cb_key_family 
JOIN Vespa_Analysts.Vespa_Single_Box_View 	AS c  ON c.account_number 	= cv.account_number AND panel_ID_vespa in(11,12) And status_vespa = 'Enabled'
WHERE    cv.cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL
UNION 
SELECT 'Individual' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_individual) experian, count(DISTINCT a.cb_key_individual) unique_key
FROM Calcredit_mobile		AS a
JOIN cust_single_account_view 	AS cv ON a.cb_key_individual = cv.cb_key_individual 
JOIN Vespa_Analysts.Vespa_Single_Box_View 	AS c  ON c.account_number 	= cv.account_number AND panel_ID_vespa in(11,12) And status_vespa = 'Enabled'
WHERE   cv.cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL

------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------- 	Gathering totals by source----------------------------------------------

---------------------------------------------------------- 	Experian 					----------------------------------------------
SELECT count(*) row_, count(DISTINCT cb_key_household) HH, count(DISTINCT cb_key_family) family, count(DISTINCT cb_key_individual)
FROM experian_consumerview
WHERE cb_address_postcode_outcode IN ('B77','B31','B98','B23','B90','B44','B37','B14','B32','B20','B36','B11','B92','B33','B63','B26','B13','B29',
'B68','B21','B71','B69','B67','B97','B43','B8','B45','B24','B91','B28','B74','B70','B42','B30','B27','B78',
'B60','B75','B65','B61','B62','B17','B16','B76','B66','B79','B9','B10','B73','B38','B6','B34','B19','B18',
'B64','B25','B12','B93','B46','B15','B35','B5','B47','B72','B7','B49','B80','B1','B94','B95','B48','B50',
'B96','B3','B4','B2','B40')
---------------------------------------------------------- 	Sky Base					----------------------------------------------
SELECT count(*) row_, count(DISTINCT cb_key_household) HH, count(DISTINCT cb_key_family) family, count(DISTINCT cb_key_individual)
FROM cust_single_account_view
WHERE cb_address_postcode_outcode IN ('B77','B31','B98','B23','B90','B44','B37','B14','B32','B20','B36','B11','B92','B33','B63','B26','B13','B29',
'B68','B21','B71','B69','B67','B97','B43','B8','B45','B24','B91','B28','B74','B70','B42','B30','B27','B78',
'B60','B75','B65','B61','B62','B17','B16','B76','B66','B79','B9','B10','B73','B38','B6','B34','B19','B18',
'B64','B25','B12','B93','B46','B15','B35','B5','B47','B72','B7','B49','B80','B1','B94','B95','B48','B50',
'B96','B3','B4','B2','B40')
AND cv.cust_active_dtv = 1	AND cv.cb_key_household > 0 
---------------------------------------------------------- 	Sky Base					----------------------------------------------
SELECT count(*) row_, count(DISTINCT a.cb_key_household) HH, count(DISTINCT b.cb_key_family) family, count(DISTINCT a.cb_key_individual)
FROM adsmartables_20141126			AS a
JOIN cust_single_account_view	 	AS b ON a.account_number = b.account_number
WHERE b.cb_address_postcode_outcode IN ('B77','B31','B98','B23','B90','B44','B37','B14','B32','B20','B36','B11','B92','B33','B63','B26','B13','B29',
	'B68','B21','B71','B69','B67','B97','B43','B8','B45','B24','B91','B28','B74','B70','B42','B30','B27','B78',
	'B60','B75','B65','B61','B62','B17','B16','B76','B66','B79','B9','B10','B73','B38','B6','B34','B19','B18',
	'B64','B25','B12','B93','B46','B15','B35','B5','B47','B72','B7','B49','B80','B1','B94','B95','B48','B50',
	'B96','B3','B4','B2','B40')
AND b.cb_key_individual > 0             	AND b.account_number IS NOT NULL
---------------------------------------------------------- 	Panel 						----------------------------------------------
SELECT count(*) row_, count(DISTINCT a.cb_key_household) HH, count(DISTINCT a.cb_key_family) family, count(DISTINCT a.cb_key_individual)
FROM cust_single_account_view 				AS a
JOIN Vespa_Analysts.Vespa_Single_Box_View 	AS c  ON c.account_number 	= a.account_number AND panel_ID_vespa in(11,12) And status_vespa = 'Enabled'
WHERE    a.cust_active_dtv = 1	AND a.cb_key_household > 0             	AND a.account_number IS NOT NULL
AND  a.cb_address_postcode_outcode IN ('B77','B31','B98','B23','B90','B44','B37','B14','B32','B20','B36','B11','B92','B33','B63','B26','B13','B29',
	'B68','B21','B71','B69','B67','B97','B43','B8','B45','B24','B91','B28','B74','B70','B42','B30','B27','B78',
	'B60','B75','B65','B61','B62','B17','B16','B76','B66','B79','B9','B10','B73','B38','B6','B34','B19','B18',
	'B64','B25','B12','B93','B46','B15','B35','B5','B47','B72','B7','B49','B80','B1','B94','B95','B48','B50',
	'B96','B3','B4','B2','B40')

------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------- 	Carriers per household/family-------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(DISTINCT provider) hits FROM Calcredit_mobile GROUP BY cb_key_household) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(DISTINCT provider) hits FROM Calcredit_mobile GROUP BY cb_key_family) as v
GROUP BY hits
---------------------------------------------------------- 	Carriers per household/family Adsmartables ------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(DISTINCT provider) hits FROM Calcredit_mobile 
		 WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
		GROUP BY cb_key_household) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(DISTINCT provider) hits FROM Calcredit_mobile 
		 WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
		GROUP BY cb_key_family) as v
GROUP BY hits

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Car Insurance Dataset	------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Creating working view----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW Calcredit_motor
AS
SELECT
	row_number () OVER (ORDER BY a.cb_key_household, a.cb_key_family, a.cb_key_individual) row_id
	 , set_code    		AS Model_code
	 , 'Car Insurance' 		AS Model_name
	, a.cb_key_family   
	, a.cb_key_household    
	, a.cb_key_individual   
	, cb_address_postcode 
	, cb_address_postcode_area 
	, cb_address_postcode_outcode 
	, userfield_1 		AS Car_Reg_Year				--Mobile Phone Provider
	, userfield_2		AS Car_Puchase_Year			--Head of Household
	, userfield_3 		AS Make_Car					--Make of car
	, userfield_4		AS Model
	, userfield_5		AS Car_Van_flag
	, userfield_6		AS No_of_cars
	, userfield_8		AS Renew_month
	, USERDATEFIELD_1 	AS Renewal_date
	, CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
	, v1.scaling_segment_ID
    , w.weighting
FROM sleary.callcredit_admart_evaluation_20141117 as a 
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 
LEFT JOIN ( SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
			FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
			JOIN vespa_analysts.SC2_intervals 			as l	ON ve.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
			WHERE panel_id_vespa in (11, 12)
			AND status_vespa = 'Enabled') AS v1 ON v1.cb_key_household = a.cb_key_household 
LEFT JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID			
WHERE set_code  ='S_0604'


UPDATE Calcredit_mobile1
SET a.weighting = w.weighting
FROM Calcredit_mobile1 as a
JOIN ( SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
            FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
            JOIN vespa_analysts.SC2_intervals           as l    ON sav.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
            ) AS v1 ON v1.cb_key_household = a.cb_key_household
JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID



SELECT 
	  Car_Reg_Year
	, Make_Car
	, No_of_cars
	, adsmart_flag
	, count(*) rows
	, count(DISTINCT cb_key_household) HH_count
	, count(DISTINCT cb_key_individual) individual_count
	, count(DISTINCT cb_key_family) family_count
FROM Calcredit_motor
GROUP BY  Car_Reg_Year
	, Make_Car
	, No_of_cars
	, adsmart_flag

--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Duplicates keys		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_individual, adsmart_flag, count(*) hits FROM Calcredit_motor GROUP BY cb_key_individual,adsmart_flag) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, adsmart_flag, count(*) hits FROM Calcredit_motor GROUP BY cb_key_household, adsmart_flag) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, adsmart_flag, count(*) hits FROM Calcredit_motor GROUP BY cb_key_family, adsmart_flag) as v
GROUP BY hits
------------------------------------------------------------------ HH Composition		----------------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(DISTINCT cb_key_individual) hits FROM Calcredit_motor GROUP BY cb_key_family) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(DISTINCT cb_key_individual) hits FROM Calcredit_motor GROUP BY cb_key_household) as v
GROUP BY hits
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Duplicates keys		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT 'Calcredit' source, cb_address_postcode_area, count (*)   hits    FROM Calcredit_motor            GROUP BY cb_address_postcode_area UNION
SELECT DISTINCT 'SAV'       source, cb_address_postcode_area, count (*)   hits    FROM cust_single_account_view   GROUP BY cb_address_postcode_area UNION
SELECT DISTINCT 'EXPERIAN'  source, cb_address_postcode_area, count (*)   hits    FROM EXPERIAN_consumerview      GROUP BY cb_address_postcode_area
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Match rates			----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Experian  match		----------------------------------------------------
SELECT 'Household' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) exp_hh, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_motor		AS a
JOIN experian_consumerview 	AS cv ON a.cb_key_household = cv.cb_key_household 
UNION
SELECT 'Family' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_family) experian, count(DISTINCT a.cb_key_family) unique_
FROM Calcredit_motor		AS a
JOIN experian_consumerview 	AS cv ON a.cb_key_family = cv.cb_key_family 
UNION 
SELECT 'Individual' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_individual) experian, count(DISTINCT a.cb_key_individual) unique_
FROM Calcredit_motor		AS a
JOIN experian_consumerview 	AS cv ON a.cb_key_individual = cv.cb_key_individual 
------------------------------------------------------------------ Sky Base 			----------------------------------------------------

SELECT 'Household' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) exp_hh, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_motor		AS a
JOIN cust_single_account_view AS cv ON a.cb_key_household = cv.cb_key_household 
WHERE    cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND account_number IS NOT NULL
UNION
SELECT 'Family' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_family) experian, count(DISTINCT a.cb_key_family) unique_
FROM Calcredit_motor		AS a
JOIN cust_single_account_view 	AS cv ON a.cb_key_family = cv.cb_key_family 
WHERE    cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND account_number IS NOT NULL
UNION 
SELECT 'Individual' Level_, count(DISTINCT a.row_id) cal, count(cv.cb_key_individual) experian, count(DISTINCT a.cb_key_individual) unique_
FROM Calcredit_motor		AS a
JOIN cust_single_account_view 	AS cv ON a.cb_key_individual = cv.cb_key_individual 
WHERE    cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND account_number IS NOT NULL
------------------------------------------------------------------ 	Adsmart			----------------------------------------------------
SELECT 'Household' Level_, count(DISTINCT a.row_id) cal_hh, count(cv.cb_key_household) experian, count(DISTINCT a.cb_key_household) unique_hh
FROM Calcredit_mobile		AS a
JOIN adsmartables_20141126 	AS cv ON a.cb_key_household = cv.cb_key_household 
	WHERE cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL	
------------------------------------------------------------------ 	Panel			----------------------------------------------------
SELECT 'Household' Level_, count(DISTINCT a.row_id) calcredit, count(cv.cb_key_household) experian, count(DISTINCT a.cb_key_household) unique_key
FROM Calcredit_mobile						AS a
JOIN cust_single_account_view 				AS cv ON a.cb_key_household = cv.cb_key_household 
JOIN Vespa_Analysts.Vespa_Single_Box_View 	AS c  ON c.account_number 	= cv.account_number AND panel_ID_vespa in(11,12) And status_vespa = 'Enabled'
WHERE    cv.cust_active_dtv = 1	AND cv.cb_key_household > 0             	AND cv.account_number IS NOT NULL	
	
--------------------------------------------------------------------------------------------------------------------------------------------	
SELECT DISTINCT Car_Puchase_Year, adsmart_flag, count (*)   hits  	FROM Calcredit_motor 			GROUP BY Car_Puchase_Year,adsmart_flag
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------- Renewal Month --------------------------------------------------------------------------
SELECT Renew_month, count(*) hits, count(DISTINCT  cb_key_household) FROM Calcredit_motor GROUP BY Renew_month
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------- Home & insurance HH --------------------------------------------------------------------
SELECT 
		count (DISTINCT a.cb_key_household) unique_HH, 
	, 	COUNT (*) rows_
	, 	count (a.cb_key_household) a_HH
	, 	count (b.cb_key_household) b_HH
	FROM Calcredit_motor 	AS a 
	JOIN Calcredit_home		AS b ON a.cb_key_household = b.cb_key_household
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT
    ' Motor' source
	,    COUNT (DISTINCT a.cb_key_household) unique_HH
    ,   COUNT (*) rows_
FROM Calcredit_motor    AS a
JOIN (SELECT DISTINCT cb_key_household FROM Calcredit_home)      AS b ON a.cb_key_household = b.cb_key_household
UNION

SELECT
    'Home' source 
	,    COUNT (DISTINCT a.cb_key_household) unique_HH
    ,   COUNT (*) rows_
FROM Calcredit_home		AS a
JOIN (SELECT DISTINCT cb_key_household FROM Calcredit_motor    )      AS b ON a.cb_key_household = b.cb_key_household
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Home Insurance Dataset --------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

CREATE VIEW Calcredit_home 
AS
SELECT
	row_number () OVER (ORDER BY cb_key_household, cb_key_family, cb_key_individual) row_id
	 , set_code    		AS Model_code
	 , 'Home Insurance' 		AS Model_name
	, cb_key_family   
	, cb_key_household    
	, cb_key_individual   
	, cb_address_postcode 
	, cb_address_postcode_area 
	, cb_address_postcode_outcode 
	, userfield_1 		AS bedrooms				--Mobile Phone Provider
	, userfield_2		AS Type_of_house		--Head of Household
	, userfield_3 		AS Tenure				--Make of car
	, userfield_4		AS has_garden
	, userfield_5		AS south_facing
	, userfield_8		AS Renew_month
	, CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
FROM sleary.callcredit_admart_evaluation_20141117
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 
WHERE set_code  ='S_0603'

--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Distinct values		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT cb_address_postcode_area, count (*)   hits  	FROM Calcredit_mobile 	GROUP BY cb_address_postcode_area
SELECT DISTINCT provider, count (*)   hits                   	FROM Calcredit_mobile 	GROUP BY provider 
SELECT DISTINCT head_of_hh, count (*)   hits                    FROM Calcredit_mobile 	GROUP BY head_of_hh
SELECT DISTINCT cb_address_postcode_outcode, count (*)   hits   FROM Calcredit_mobile 	GROUP BY cb_address_postcode_outcode












--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Connected 				------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW Calcredit_connected 
AS
SELECT
	row_number () OVER (ORDER BY a.cb_key_household, a.cb_key_family, a.cb_key_individual) row_id
	 , set_code    		AS Model_code
	 , 'Calcredit Connected' 		AS Model_name
	, a.cb_key_family   
	, a.cb_key_household    
	, a.cb_key_individual   
	, a.cb_address_postcode 
	, cb_address_postcode_area 
	, cb_address_postcode_outcode 
	, userfield_1 		AS Connected_Group				--Connected Group
	, userfield_2		AS Connected_Type				--Connected Type
	, userfield_3 		AS Head_of_HH					--Head of HH
	, CASE 	WHEN userfield_1 =  '1'  THEN 	'Online Trendsetters'
			WHEN userfield_1 =  '2'  THEN 	'Family Fun'
			WHEN userfield_1 =  '3'  THEN 	'Spending Big'
			WHEN userfield_1 =  '4'  THEN 	'Follow The Leader'
			WHEN userfield_1 =  '5'  THEN 	'Lagging Behind'
			ELSE 'Unknown' END Connected_group_desc
	, CASE 	WHEN userfield_2 =  '1A'  THEN 	'Professional Gamesters'
			WHEN userfield_2 =  '1B'  THEN 	'Wireless Socialites'
			WHEN userfield_2 =  '1C'  THEN 	'Enthusiastic Bloggers'
			WHEN userfield_2 =  '1D'  THEN 	'Cyber Singles'
			WHEN userfield_2 =  '2A'  THEN 	'Gadget Families'
			WHEN userfield_2 =  '2B'  THEN 	'Expensive Entertainment'
			WHEN userfield_2 =  '2C'  THEN 	'Broadband Families'
			WHEN userfield_2 =  '3A'  THEN 	'Fully Loaded'
			WHEN userfield_2 =  '3B'  THEN 	'PDA Professionals'
			WHEN userfield_2 =  '3C'  THEN 	'Grey Skypers '
			WHEN userfield_2 =  '3D'  THEN 	'Sceptical Surfers'
			WHEN userfield_2 =  '3E'  THEN 	'Thrifty Researchers'
			WHEN userfield_2 =  '4A'  THEN 	'Savvy Surfers'
			WHEN userfield_2 =  '4B'  THEN 	'Social Media Addicts'
			WHEN userfield_2 =  '4C'  THEN 	'Grown-up Gamers'
			WHEN userfield_2 =  '4D'  THEN 	'Digital Dabblers'
			WHEN userfield_2 =  '4E'  THEN 	'Technology Ticklers'
			WHEN userfield_2 =  '5A'  THEN 	'Telly Addicts'
			WHEN userfield_2 =  '5B'  THEN 	'Late Learners'
			WHEN userfield_2 =  '5C'  THEN 	'Traditional Technology'
			WHEN userfield_2 =  '5D'  THEN 	'Retired Interests'
			WHEN userfield_2 =  '5E'  THEN 	'Limited Resources'
			WHEN userfield_2 =  '5F'  THEN 	'Keep it Simple Seniors'
			ELSE 'Unknown' END Connected_type_desc
	, CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
FROM sleary.callcredit_admart_evaluation_20141117 as a 
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 
WHERE set_code  ='S_0605'


--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Basic counts ----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------

SELECT
	adsmart_flag
	, count(*) 								AS total_rows
	, count(cb_key_household) 				AS households
	, count(DISTINCT cb_key_household) 		AS Unique_households
	, count(cb_key_family) 					AS Families
	, count(DISTINCT cb_key_family) 		AS Unique_Families
	, count(cb_key_individual) 				AS Individuals
	, count(DISTINCT cb_key_individual) 	AS Unique_individuals
	, count(cb_address_postcode_area) 			AS area
	, count(DISTINCT cb_address_postcode_area) 	AS Unique_area
	, count(cb_address_postcode_outcode) 			AS Districts
	, count(DISTINCT cb_address_postcode_outcode) 	AS Unique_Districts
	, count(Connected_Group) 				AS groups
	, count(DISTINCT Connected_Group) 		AS Unique_Groups
	, count(Connected_Type) 				AS types
	, count(DISTINCT Connected_Type) 		AS Unique_Types
	, count(Head_of_HH) 					AS Head_of_HH
	, count(DISTINCT Head_of_HH) 		AS Unique_Head_of_HH
FROM Calcredit_connected
GROUP BY adsmart_flag

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT  	'Postal Area'  Field
	, cb_address_postcode_area AS Label
	, adsmart_flag
	, counT(*) hits
FROM 	Calcredit_connected
GROUP BY 	  Label	, adsmart_flag
UNION
SELECT 	'Postal District'  Field 
	, cb_address_postcode_outcode AS District
	, adsmart_flag
	, counT(*) hits
FROM 	Calcredit_connected
GROUP BY 	  District	, adsmart_flag
UNION
SELECT 	'Connected Group'  Field 
	, Connected_Group AS District
	, adsmart_flag
	, counT(*) hits
FROM 	Calcredit_connected
GROUP BY 	  District	, adsmart_flag
UNION
SELECT 	'Connected Type'  Field 
	, Connected_Type AS District
	, adsmart_flag
	, counT(*) hits
FROM 	Calcredit_connected
GROUP BY 	  District	, adsmart_flag
UNION
SELECT 	'Head of HH'  Field 
	, Head_of_HH AS District
	, adsmart_flag
	, counT(*) hits
FROM 	Calcredit_connected
GROUP BY 	  District	, adsmart_flag
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Duplicates keys		----------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_individual, adsmart_flag, count(*) hits FROM Calcredit_connected GROUP BY cb_key_individual,adsmart_flag) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, adsmart_flag, count(*) hits 	FROM Calcredit_connected GROUP BY cb_key_household, adsmart_flag) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, adsmart_flag, count(*) hits 	FROM Calcredit_connected GROUP BY cb_key_family, adsmart_flag) as v
GROUP BY hits
------------------------------------------------------------------ HH Composition	-------------------------------------------------------
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_family, count(DISTINCT cb_key_individual) hits FROM Calcredit_mobile 
		 WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
		GROUP BY cb_key_family) as v
GROUP BY hits
SELECT 		hits, 	count(*) count_
FROM (SELECT cb_key_household, count(DISTINCT cb_key_individual) hits FROM Calcredit_mobile 
		 WHERE cb_key_household in ( SELECT cb_key_household FROM adsmartables_20141126  AS cv WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL)
		 GROUP BY cb_key_household) as v
GROUP BY hits

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------ Homemovers 				------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW Calcredit_homemovers
AS
SELECT
	  row_number () OVER (ORDER BY a.cb_key_household, a.cb_key_family, a.cb_key_individual) row_id
	, set_code
	, a.cb_key_family
	, a.cb_key_household
	, a.cb_key_individual
	, a.cb_address_postcode
	, a.cb_address_postcode_area
	, a.cb_address_postcode_outcode
	, a.userdatefield_1		AS status_change
	, a.userfield_1			AS Status 
	, CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
FROM sleary.whenfresh_admart_evaluation_20141118 AS a 
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 

COMMIT 

SELECT * INTO Calcredit_homemovers_table FROM Calcredit_homemovers
COMMIT
CREATE HG INDEX id1 ON Calcredit_homemovers_table(cb_key_household)
COMMIT
GRANT ALL ON Calcredit_homemovers_table TO PEM06
COMMIT


SELECT 
	set_code
	, count(*) 					AS Total_rows
	, count(cb_key_household) 	AS Total_HH
	, count(cb_key_family)		AS Total_family
	, count(cb_key_individual)	AS Total_individual
	, COUNT(Status)				AS Total_status
	, COUNT(status_change)		AS Total_status_change
	, COUNT(adsmart_flag)		AS Total_adsmart_flag
	, COUNT(cb_address_postcode_area)	AS Total_areas
------------------------------------------
	, count(DISTINCT cb_key_household)		AS	Dis_HH
	, count(DISTINCT cb_key_family)			AS	Dis_family
	, count(DISTINCT cb_key_individual)		AS 	Dis_individual
	, COUNT(DISTINCT Status)				AS 	Dis_status
	, COUNT(DISTINCT status_change)			AS	Dis_status_chg
	, COUNT(DISTINCT adsmart_flag)			AS	Dis_adsmart_flag
	, COUNT(DISTINCT cb_address_postcode_area)	AS	Dis_areas
FROM  Calcredit_homemovers 
GROUP BY set_code

SELECT 
	, count(*) 					AS Total_rows
	, count(cb_key_household) 	AS Total_HH
	, count(cb_key_family)		AS Total_family
	, count(cb_key_individual)	AS Total_individual
	, COUNT(Status)				AS Total_status
	, COUNT(status_change)		AS Total_status_change
	, COUNT(adsmart_flag)		AS Total_adsmart_flag
	, COUNT(cb_address_postcode_area)	AS Total_areas
------------------------------------------
	, count(DISTINCT cb_key_household)		AS	Dis_HH
	, count(DISTINCT cb_key_family)			AS	Dis_family
	, count(DISTINCT cb_key_individual)		AS 	Dis_individual
	, COUNT(DISTINCT Status)				AS 	Dis_status
	, COUNT(DISTINCT status_change)			AS	Dis_status_chg
	, COUNT(DISTINCT adsmart_flag)			AS	Dis_adsmart_flag
		, COUNT(DISTINCT cb_address_postcode_area)	AS	Dis_areas
FROM  Calcredit_homemovers 
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
SELECT 
	  Status	
	, set_code, adsmart_flag
	, count(*) 
FROM  Calcredit_homemovers 
GROUP BY Status	
	, set_code, adsmart_flag
------------------------------------------
SELECT 
	  DATE(status_change)			AS	Dis_status_chg
	, set_code, adsmart_flag
	, COUNT(*)						AS hits
FROM  Calcredit_homemovers 
GROUP BY 
	Dis_status_chg
	, set_code, adsmart_flag
------------------------------------------	
SELECT 
	  cb_address_postcode_area	AS	Dis_areas
	, set_code, adsmart_flag
	, COUNT(*)						AS hits
FROM  Calcredit_homemovers 
GROUP BY 
	Dis_areas
	, set_code, adsmart_flag
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
SELECT hits, count(hh)
FROM (SELECT cb_key_household AS hh, count(*) hits FROM Calcredit_homemovers WHERE adsmart_flag = 1 GROUP BY cb_key_household) as v 
GROUP BY hits
------------------------------------------------------------------------------------

SELECT 
	  cb_key_household 
	, count(*) hits 
	, CAST(NULL AS VARCHAR(20) )	AS Status_1
	, CAST(NULL AS VARCHAR(20) )	AS Status_2
	, CAST(NULL AS VARCHAR(20) )	AS Status_3
	, CAST(NULL AS VARCHAR(20) )	AS Status_4
INTO V317_Calcredit_homemovers_changes
FROM Calcredit_homemovers 
GROUP BY cb_key_household
HAVING hits >1

COMMIT 

SELECT
    a.cb_key_household
    , min(status) status
INTO #t1
FROM V317_Calcredit_homemovers_changes  AS a
JOIN Calcredit_homemovers               AS b ON a.cb_key_household = b.cb_key_household AND set_code = 'S_0609'
GROUP BY a.cb_key_household

UPDATE V317_Calcredit_homemovers_changes
SET Status_1 = Status
FROM V317_Calcredit_homemovers_changes  AS a
JOIN #t1               AS b ON a.cb_key_household = b.cb_key_household

SELECT
    a.cb_key_household
    , min(status) status
INTO #t2
FROM V317_Calcredit_homemovers_changes  AS a
JOIN Calcredit_homemovers               AS b ON a.cb_key_household = b.cb_key_household AND set_code = 'S_0610'
GROUP BY a.cb_key_household

UPDATE V317_Calcredit_homemovers_changes
SET Status_2 = Status
FROM V317_Calcredit_homemovers_changes  AS a
JOIN #t2               AS b ON a.cb_key_household = b.cb_key_household


SELECT
    a.cb_key_household
    , min(status) status
INTO #t3
FROM V317_Calcredit_homemovers_changes  AS a
JOIN Calcredit_homemovers               AS b ON a.cb_key_household = b.cb_key_household AND set_code = 'S_0611'
GROUP BY a.cb_key_household

UPDATE V317_Calcredit_homemovers_changes
SET Status_3 = Status
FROM V317_Calcredit_homemovers_changes  AS a
JOIN #t3               AS b ON a.cb_key_household = b.cb_key_household

SELECT
    a.cb_key_household
    , min(status) status
INTO #t4
FROM V317_Calcredit_homemovers_changes  AS a
JOIN Calcredit_homemovers               AS b ON a.cb_key_household = b.cb_key_household AND set_code = 'S_0612'
GROUP BY a.cb_key_household

UPDATE V317_Calcredit_homemovers_changes
SET Status_4 = Status
FROM V317_Calcredit_homemovers_changes  AS a
JOIN #t4               AS b ON a.cb_key_household = b.cb_key_household
------------------------------------------------------------------------------------

ALTER TABLE V317_Calcredit_homemovers_changes ADD status_consolidated VARCHAR(100)
commit


UPDATE V317_Calcredit_homemovers_changes
SET status_consolidated = 	CASE WHEN Status_1 IS NOT NULL THEN LEFT(Status_1,25)||'-'	ELSE '' END ||
							CASE WHEN Status_2 IS NOT NULL THEN LEFT(Status_2,25)||'-'	ELSE '' END ||
							CASE WHEN Status_3 IS NOT NULL THEN LEFT(Status_3,25)||'-'	ELSE '' END ||
							LEFT(Status_4,25)
COMMIT







CREATE VIEW Calcredit_homemovers
AS
SELECT
	  row_number () OVER (ORDER BY a.cb_key_household, a.cb_key_family, a.cb_key_individual) row_id
	, set_code
	, a.cb_key_family
	, a.cb_key_household
	, a.cb_key_individual
	, a.cb_address_postcode
	, a.cb_address_postcode_area
	, a.cb_address_postcode_outcode
	, a.userdatefield_1		AS status_change
	, a.userfield_1			AS Status 
	, CASE WHEN b.cb_key_household IS NULL THEN 0 ELSE 1 END AS adsmart_flag
FROM sleary.whenfresh_admart_evaluation_20141118 AS a 
LEFT JOIN (SELECT DISTINCT cb_key_household 
			FROM adsmartables_20141126  AS cv 
			WHERE cv.cb_key_household > 0  AND cv.account_number IS NOT NULL) as b ON a.cb_key_household = b.cb_key_household 

COMMIT 









---------------------------------------------------------------------------	VALIDATION scaled vs adsmartables
SELECT 'mobile' source
    , SUM(adsmart_flag) adsmartables
    , SUm(CASE WHEN weighting >0 THEN 1 ELSE 0 END) panel
    , SUM(weighting) scaled_weighted
FROM Calcredit_mobile1
          WHERE adsmart_flag =1

UNION

SELECT 'motor' source
    , SUM (adsmart_flag_)    AS adsmartables
    , SUM (CASE WHEN weighting_ > 0     THEN 1 ELSE NULL END) panel
    , SUM(weighting_) scaled_weighted
FROM (SELECT cb_key_household
            , min(adsmart_flag) AS adsmart_flag_
            , min(weighting) weighting_
        FROM Calcredit_motor1
		          WHERE adsmart_flag =1
        GROUP BY cb_key_household) as v
UNION

SELECT 'connected' source
    , SUM (adsmart_flag_)    AS adsmartables
    , SUM (CASE WHEN weighting_ > 0     THEN 1 ELSE NULL END) panel
    , SUM(weighting_) scaled_weighted
FROM(SELECT cb_key_household
            , min(adsmart_flag) AS adsmart_flag_
            , min(weighting) weighting_
        FROM    (SELECT a.cb_key_household, adsmart_flag, w.weighting
                FROM Calcredit_connected1 as a
                LEFT JOIN (  SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
                            FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
                            JOIN vespa_analysts.SC2_intervals           as l    ON sav.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
                            ) AS v1 ON v1.cb_key_household = a.cb_key_household
                LEFT JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID) as vv
				WHERE adsmart_flag =1
                GROUP BY cb_key_household) as dd

UNION

SELECT 'Baby' source
    , SUM (adsmart_flag_)    AS adsmartables
    , SUM (CASE WHEN weighting_ > 0     THEN 1 ELSE NULL END) panel
    , SUM(weighting_) scaled_weighted
FROM (SELECT cb_key_household
            , min(adsmart_flag) AS adsmart_flag_
            , min(weighting) weighting_
        FROM v317_Emmas_view
        GROUP BY cb_key_household) as v			


UNION

SELECT 'Whenfresh homemovers' source
    , SUM (adsmart_flag_)    AS adsmartables
    , SUM (CASE WHEN weighting_ > 0     THEN 1 ELSE NULL END) panel
    , SUM(weighting_) scaled_weighted
FROM(SELECT cb_key_household
            , min(adsmart_flag) AS adsmart_flag_
            , min(weighting) weighting_
        FROM    (SELECT a.cb_key_household, adsmart_flag, w.weighting
                FROM Calcredit_homemovers1 as a
                LEFT JOIN (  SELECT DISTINCT sav.cb_key_household, l.scaling_segment_ID
                            FROM CUST_SINGLE_ACCOUNT_VIEW   as sav
                            JOIN vespa_analysts.SC2_intervals           as l    ON sav.account_number = l.account_number AND  '2014-12-15'  between l.reporting_starts and l.reporting_ends
                            ) AS v1 ON v1.cb_key_household = a.cb_key_household
                LEFT JOIN  vespa_analysts.SC2_weightings as w ON w.scaling_day = '2014-12-15' AND w.scaling_segment_ID = v1.scaling_segment_ID) as vv
				WHERE adsmart_flag =1
                GROUP BY cb_key_household) as dd



		
				
				