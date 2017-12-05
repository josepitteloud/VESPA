---------------- Consolidated output - -----------------------
---------- Cancellations 
SELECT 'Actuals CN' Source
    , CAST(subS_week_and_year AS INT) week
	, CAST(sabb_segment AS VARCHAR) segment
    , CASE 	WHEN LOWER (churn_type) LIKE '%syscan%' THEN 'SysCan'
			WHEN LOWER (churn_type) LIKE '%cuscan%' THEN 'CusCan'
			WHEN LOWER (churn_type) LIKE '%hm%' THEN 'HM'
			WHEN LOWER (churn_type) LIKE '%3rd%' THEN '3rd Party'
			ELSE churn_type ENd churntype
    , count(*) hits 
FROM BB_Churn_cancellations_6 AS a
JOIN sky_calendar AS b ON a.CN_DT = b.calendar_date
WHERE bb_cust_type = 'SABB'
	AND plus_at_CN = 0 
	AND now_v1  = 0 
GROUP BY churn_type
        , week 
		, segment
UNION 
SELECT 'Forecast CN'
    ,  CAST(subs_week_and_year	AS INT) week 
	, CAST(SABB_forecast_segment AS VARCHAR) segment
    , CASE  WHEN syscan + Cuscan + HM + _3rd_party > 1 THEN 'Multichurn??'
            WHEN syscan = 1 THEN 'SysCan'
            WHEN CUscan = 1 THEN 'CusCan'
            WHEN HM = 1 THEN 'HM'
            WHEN _3rd_Party = 1 THEN '3rd Party'
            ELSE 'No Churn' END AS  Churn_type    	
     , 4 * count(*) hits 
FROM FORECAST_Looped_Sim_Output_Platform
WHERE syscan + Cuscan + HM + _3rd_party >= 1 ----------ONLY CN accounts
GROUP BY subs_week_and_year	,churn_type	, SABB_forecast_segment

UNION 

---------- Pipeline Entries
SELECT 'Forecast PL'
    ,  CAST(subs_week_and_year	AS INT) week
	, CAST(SABB_forecast_segment AS VARCHAR) segment
    , CASE  WHEN BB_syscan + BB_Cuscan + BB_HM + BB_3rd_party > 1 THEN 'MultiPL??'
            WHEN BB_syscan = 1 THEN 'SysCan'
            WHEN BB_CUscan = 1 THEN 'CusCan'
            WHEN BB_HM = 1 THEN 'HM'
            WHEN BB_3rd_party = 1 THEN '3rd Party'
            ELSE 'No PL' END AS  Churn_type    	
     , 4 * count(*) hits 
FROM FORECAST_Looped_Sim_Output_Platform
WHERE BB_syscan + BB_Cuscan + BB_HM + BB_3rd_party >= 1 ----------ONLY PL accounts
GROUP BY subs_week_and_year	,churn_type	, SABB_forecast_segment


UNION 

SELECT 
	  'Actual PL' SOURCE 
    ,  CAST(PL_week	AS INT) WEEK
	, CAST(segment AS VARCHAR) segment
	, CASE 	WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MultiPL??'
			WHEN BB_Enter_SysCan >= 1 	THEN 'SysCan'
            WHEN BB_Enter_CusCan >= 1 	THEN 'CusCan'
            WHEN BB_Enter_HM >= 1 		THEN 'HM'
            WHEN BB_Enter_3rd_Party >= 1 THEN '3rd Party'
            ELSE 'No PL' END AS  Churn_type    	
	 , count(*) hits  
FROM (SELECT * FROM CUST_CONSOLIDATED_2015 WHERE sky_plus = 0 AND now_tv = 0
		UNION 
		SELECT * FROM CUST_CONSOLIDATED_2016 WHERE sky_plus = 0 AND now_tv = 0
		UNION
		SELECT * FROM CUST_CONSOLIDATED_2017 WHERE sky_plus = 0 AND now_tv = 0
			) AS v 
GROUP BY     PL_week	
	, segment
	, Churn_type
UNION 

SELECT 'Forecast base' Source
	, CAST(subs_week_and_year	AS INT) week
	, CAST(SABB_forecast_segment AS VARCHAR) segment
    , 'Full SABB BASE'  AS  Churn_type    	
    , 4 * count(*) hits 
FROM FORECAST_Looped_Sim_Output_Platform
GROUP BY WEEK, SEGMENT  

UNION 
SELECT 
	  'Actual Base' SOURCE 
    ,  CAST(subs_week_and_year	AS INT) WEEK
	, CAST(segment AS VARCHAR) segment
	, 'Full SABB BASE'  AS  Churn_type    	  	
	 , count(*) hits  
FROM (SELECT * FROM CUST_CONSOLIDATED_2014 WHERE sky_plus = 0 AND now_tv = 0
			UNION 
		SELECT * FROM CUST_CONSOLIDATED_2015 WHERE sky_plus = 0 AND now_tv = 0
			UNION 
		SELECT * FROM CUST_CONSOLIDATED_2016 WHERE sky_plus = 0 AND now_tv = 0
		UNION
		SELECT * FROM CUST_CONSOLIDATED_2017 WHERE sky_plus = 0 AND now_tv = 0
			) AS v 
GROUP BY     subs_week_and_year	
	, segment
	, Churn_type
	
	
	
	
	
	
	
	
	
	
	
	
	
	