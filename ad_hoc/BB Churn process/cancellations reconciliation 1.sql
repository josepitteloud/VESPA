
SELECT 'Actuals' Source, CAST(subS_week_and_year AS INT) week, churn_type , count(*) hits 
FROM BB_Churn_cancellations_2 AS a
JOIN sky_calendar AS b ON a.CN_DT = b.calendar_date
WHERE bb_cust_type = 'SABB'
GROUP BY churn_type, week 
UNION 

SELECT 'Forecast'
    , subs_week_and_year	
    , CASE  WHEN syscan + Cuscan + HM + _3rd_party > 1 THEN 'Multichurn??'
            WHEN syscan = 1 THEN 'SysCan'
            WHEN CUscan = 1 THEN 'CusCan'
            WHEN HM = 1 THEN 'HM'
            WHEN _3rd_Party = 1 THEN '3rd Party'
            ELSE 'No Churn' END AS  Churn_type    	
     , 4*count(*) hits 
FROM FORECAST_Looped_Sim_Output_Platform
GROUP BY subs_week_and_year	,churn_type	


-------------------------------------------------------------------------------------------------



DROP TABLE BB_churn_prev_segment


    SELECT account_number, max(end_date) mx_dt
    INTO #t1 
    FROM FORECAST_Looped_Sim_Output_Platform
    WHERE sabb_FORECAST_SEGMENT NOT IN ('PC','AB','BCRQ') 
    GROUP BY account_number
    COMMIT 
    CREATE HG INDEX ID1 ON #t1(account_number)
    CREATE DATE INDEX ID2 ON #t1(mx_dt)
    COMMIT 
    SELECT a.account_number, max (sabb_FORECAST_SEGMENT) segment
    INTO BB_churn_prev_segment
    FROM #t1 AS a 
    JOIN FORECAST_Looped_Sim_Output_Platform AS b ON a.account_number = b.account_number AND b.end_date = a.mx_dt 
    group by a.account_number
    COMMIT 
    CREATE HG INDEX ID1 ON BB_churn_prev_segment(account_number)
    CREATE LF INDEX ID2 ON BB_churn_prev_segment(segment)
    COMMIT 
    
CREATE OR REPLACE VIEW BB_churn_cube AS 
SELECT 
    sabb_FORECAST_SEGMENT 
    , end_date
    , subS_week_and_year
    , churn_type
    , CASE  WHEN  BB_syscan + BB_Cuscan + BB_HM + BB_3rd_party > 1 THEN 'Multichurn??'
            WHEN BB_syscan = 1 THEN 'SysCan'
            WHEN BB_CUscan = 1 THEN 'CusCan'
            WHEN BB_HM = 1 THEN 'HM'
            WHEN BB_3rd_Party = 1 THEN '3rd Party'
            ELSE 'No PL' END AS  BB_PL_type
    , CASE  WHEN syscan + Cuscan + HM + _3rd_party > 1 THEN 'Multichurn??'
            WHEN syscan = 1 THEN 'SysCan'
            WHEN CUscan = 1 THEN 'CusCan'
            WHEN HM = 1 THEN 'HM'
            WHEN _3rd_Party = 1 THEN '3rd Party'
            ELSE 'No Churn' END AS  BB_Churn_type    
    , SABB_churn                    
    , b.segment AS segment_prev_PL
    , COUNT(*) HITS 
    
FROM FORECAST_Looped_Sim_Output_Platform AS a 
LEFT JOIN BB_churn_prev_segment AS b ON a.account_number = b.account_number 
group by 
  sabb_FORECAST_SEGMENT 
    , end_date
    , subS_week_and_year
    , churn_type
    , BB_PL_type
    , BB_Churn_type
    , SABB_churn
    , segment_prev_PL
    COMMIT 
