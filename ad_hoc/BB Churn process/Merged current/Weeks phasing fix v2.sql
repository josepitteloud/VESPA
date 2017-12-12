DROP TABLE IF EXISTS weeks_fix  ;
DROP TABLE IF EXISTS weeks_fix_2;
DROP TABLE IF EXISTS weeks_fix_3;
DROP TABLE IF EXISTS weeks_fix_agg;
DROP TABLE IF EXISTS weeks_fix_results_1;
DROP TABLE IF EXISTS weeks_fix_results_2;
DROP TABLE IF EXISTS weeks_fix_results_3;
DROP TABLE IF EXISTS weeks_fix_results_4;
DROP TABLE IF EXISTS weeks_fix_draft;
GO
--- Selecting cancellations from previous years 
SELECT a.subs_week_and_year 
    , end_date 
    , CASE WHEN b.subs_month_end = 'Y' THEN 1 ELSE 0 END eom 
    , SUM(eom) OVER (PARTITION  BY churn_type  ORDER BY a.subs_week_and_year DESC) batch
    , churn_type
    , COUNT(*) hits 
INTO weeks_fix  
FROM BB_Churn_cancellations AS a 
JOIN sky_calendar AS x ON a.end_date = DATEADD (week, 1, x.calendar_Date)
LEFT JOIN sky_calendar AS b ON x.subs_week_and_year = b.subs_week_and_year  AND b.subs_month_end = 'Y'
WHERE now_v1 = 0 
GROUP BY a.subs_week_and_year , churn_type , eom, end_date 
ORDER by churn_type, a.subs_week_and_year 

COMMIT 
------ Adding week number within the batch (Quarter)

SELECT * 
        , row_number()  OVER (PARTITION BY churn_type, batch ORDER BY subs_week_and_year) wk_id
INTO weeks_fix_2
from weeks_fix

COMMIT 

------ Adding aggregates and size of the batch (Quarter)
SELECT *
    , SUM(hits) OVER (PARTITION BY churn_type, batch ) sum_batch
    , count(* ) OVER (PARTITION BY churn_type, batch ) size
    , prop = CAST(hits AS FLOAT) / CAST(sum_batch AS FLOAT) 
INTO weeks_fix_3
FROM weeks_fix_2
ORDER by churn_type, subs_week_and_year 

COMMIT 

------ Aggregating and keeping only full quarters 
SELECT churn_type
    , size
    , wk_id
    , AVG(prop) 
INTO weeks_fix_agg
FROM     weeks_fix_3
WHERE size IN (4,5)
GROUP BY churn_type
    , wk_id
    , size
ORDER BY churn_type, size, wk_id     

COMMIT 

--------- Aggregating the result table 
SELECT a.subs_week_and_year
    , end_date
    , SUM(HM) AS HM
    , 'HM' AS churn_type 
    , CASE WHEN b.subs_month_end = 'Y' THEN 1 ELSE 0 END eom 
    , SUM(eom) OVER (ORDER BY a.subs_week_and_year DESC) batch
INTO weeks_fix_results_1
FROM FORECAST_Looped_Sim_Output_Platform AS a
JOIN sky_calendar AS x ON a.end_date = DATEADD (week, 0, x.calendar_Date)
LEFT JOIN sky_calendar AS b ON x.subs_week_and_year = b.subs_week_and_year AND b.subs_month_end = 'Y'
GROUP BY a.subs_week_and_year
    , end_date
	, eom
UNION 
SELECT a.subs_week_and_year
    , end_date
    , SUM(CUscan)   AS CusCan
    , 'Cuscan'      AS churn_type 
    , CASE WHEN b.subs_month_end = 'Y' THEN 1 ELSE 0 END eom 
    , SUM(eom) OVER (ORDER BY a.subs_week_and_year DESC) batch
FROM FORECAST_Looped_Sim_Output_Platform AS a 
JOIN sky_calendar AS x ON a.end_date = DATEADD (week, 0, x.calendar_Date)
LEFT JOIN sky_calendar AS b ON x.subs_week_and_year = b.subs_week_and_year   AND b.subs_month_end = 'Y'
GROUP BY a.subs_week_and_year
    , end_date
	, eom

------ Adding week number	
SELECT * 
        , row_number()  OVER (PARTITION BY churn_type,batch ORDER BY churn_type,batch,subs_week_and_year) wk_id
INTO weeks_fix_results_2
from weeks_fix_results_1

COMMIT 

SELECT *
    , SUM(a.hm) OVER (PARTITION BY  churn_type,a.batch ) hits 
    , count(* ) OVER (PARTITION BY  churn_type,a.batch ) size
INTO weeks_fix_results_3
FROM weeks_fix_results_2    AS a 
ORDER by  subs_week_and_year 	

COMMIT 

----- Defining threshold for the number of accounts to be moved to the next week 
SELECT a.*	
    , MAX(b.expression )          prop
    , total = ROUND(hits * prop, 0) 
    , diff = HM-total
	, SUM ( diff) OVER (PARTITION BY a.batch,a.churn_type ORDER BY a.wk_id ) diff2
INTO     weeks_fix_results_4
FROM weeks_fix_results_3 AS a 
JOIN weeks_fix_agg          AS b ON a.wk_id = b.wk_id AND a.size = b.size and TRIM(a.churn_type) = TRIM(b.churn_type)
GROUP BY a.subs_week_and_year,a.end_date,a.HM,a.churn_type,a.eom,a.batch,a.wk_id,a.hits,a.size

COMMIT

-- SELECT * from weeks_fix_results_4
SELECT account_number
        , end_date
        , subs_week_and_year
        , CASE  WHEN syscan + Cuscan + HM + _3rd_party > 1 THEN 'Multichurn??'
            WHEN syscan = 1 THEN 'SysCan'
            WHEN CUscan = 1 THEN 'CusCan'
            WHEN HM = 1 THEN 'HM'
            WHEN _3rd_Party = 1 THEN '3rd Party'
            ELSE 'No Churn' END AS  Churn_type    	            
        , row_number () OVER (PARTITION BY Churn_type, subs_week_and_year ORDER BY 	rand_bb_offer_applied DESC  ) rnd_id 
        , CAST (0 AS BIT) AS moved
        , CAST(NULL AS DATE) AS new_end_date
        , CAST(NULL AS INT) AS new_week
INTO   weeks_fix_draft
FROM FORECAST_Looped_Sim_Output_Platform
WHERE syscan + Cuscan + HM + _3rd_party >= 1
ORDER BY Churn_type, subs_week_and_year, rnd_id

COMMIT 
CREATE LF INDEX ID1 ON weeks_fix_draft(churn_type)
CREATE HG INDEX ID2 ON weeks_fix_draft(rnd_id)
CREATE LF INDEX ID3 ON weeks_fix_draft(subs_week_and_year)
COMMIT 

----- Picking accounts that are moving and updating the new week and end_date 
UPDATE weeks_fix_draft
SET moved = 1
    , new_end_date = DATEADD(week, 1 , a.end_date) 
FROM    weeks_fix_draft AS a
JOIN    weeks_fix_results_4 AS b ON a.subs_week_and_year = b.subs_week_and_year AND LOWER(a.churn_type) = LOWER(b.churn_type) AND a.rnd_id <= b.diff2

UPDATE weeks_fix_draft
SET new_week = CAST(b.subs_week_and_year AS INT)
FROM weeks_fix_draft AS a 
JOIN sky_calendar AS b ON DATEADD(WEEK, 1,a.new_end_date ) = b.calendar_Date 

----- Updating accounts that haven't moved 
UPDATE weeks_fix_draft
SET new_week = subs_week_and_year 
    , new_end_date  = end_date 
WHERE moved = 0 
CREATE HG INDEX id9 ON weeks_fix_draft 	(account_number ) 

----- Creating Final view 
CREATE OR REPLACE VIEW FORECAST_Looped_Sim_Output_Platform_fixed
AS 
SELECT a.*, b.new_week, new_end_date
FROM FORECAST_Looped_Sim_Output_Platform 	AS a 
JOIN weeks_fix_draft 						AS b ON a.account_number = b.account_number AND a.subs_week_and_year = b.subs_week_and_year
COMMIT