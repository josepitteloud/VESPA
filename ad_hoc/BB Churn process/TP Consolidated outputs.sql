---------------- Consolidated output - -----------------------
---------- Cancellations 
SELECT 'Actuals CN' Source
    , CAST(subS_week_and_year AS INT) week
	, CAST(segment AS VARCHAR) segment
    , CASE 	WHEN LOWER (churn_type) LIKE '%syscan%' THEN 'SysCan'
			WHEN LOWER (churn_type) LIKE '%cuscan%' THEN 'CusCan'
			WHEN LOWER (churn_type) LIKE '%hm%' THEN 'HM'
			WHEN LOWER (churn_type) LIKE '%3rd%' THEN '3rd Party'
			ELSE churn_type ENd churntype
    , count(*) hits 
FROM BB_Churn_cancellations_5 AS a
JOIN sky_calendar AS b ON a.CN_DT = b.calendar_date
WHERE bb_cust_type = 'Triple Play'
	AND prodplat_churn_type = 'Product'	
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
FROM TP_FORECAST_Looped_Sim_Output_Platform
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
FROM TP_FORECAST_Looped_Sim_Output_Platform
WHERE BB_syscan + BB_Cuscan + BB_HM + BB_3rd_party >= 1 ----------ONLY PL accounts
GROUP BY subs_week_and_year	,churn_type	, SABB_forecast_segment


UNION 

SELECT 
	  'Actual PL' SOURCE 
    , CAST(agg.subs_week_and_year	AS INT) WEEK
	, CAST(node AS VARCHAR) segment
	, CASE 	WHEN Enter_SysCan + Enter_CusCan + Enter_HM + Enter_3rd_Party > 1 THEN 'MultiPL??'
			WHEN Enter_SysCan >= 1 	THEN 'SysCan'
            WHEN Enter_CusCan >= 1 	THEN 'CusCan'
            WHEN Enter_HM >= 1 		THEN 'HM'
            WHEN Enter_3rd_Party >= 1 THEN '3rd Party'
            ELSE 'No PL' END AS  Churn_type    	
	 , count(*) hits  
FROM citeam.DTV_FCAST_WEEKLY_BASE AS agg
JOIN citeam.Broadband_Comms_Pipeline  AS a ON a.account_number = agg.account_number AND agg.end_date BETWEEN DATEADD(WEEK,-1, a.event_dt) AND a.event_dt
LEFT JOIN BB_TP_Product_Churn_segments_lookup AS c  ON agg.Talk_tenure 	= c.Talk_tenure
												AND agg.RTM 				= c.RTM 
												AND agg.my_sky_login_3m 	= c.my_sky_login_3m
												AND agg.Simple_Segment   	= c.Simple_Segment
												AND agg.DTV_TA_calls_1m 	= c.DTV_TA_calls_1m
												AND agg.BB_all_calls_1m 	= c.BB_all_calls_1m
WHERE agg.end_date BETWEEN '2016-01-01' AND GETDATE() AND dtV_active 	= 1 AND bb_active = 1 
GROUP BY     WEEK	
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
	, CAST(c.node AS VARCHAR) segment
	, 'Full SABB BASE'  AS  Churn_type    	  	
	 , count(*) hits  
FROM citeam.DTV_FCAST_WEEKLY_BASE AS agg
LEFT JOIN BB_TP_Product_Churn_segments_lookup AS c  ON  agg.Talk_tenure 		= c.Talk_tenure
													AND agg.RTM 				= c.RTM 
													AND agg.my_sky_login_3m 	= c.my_sky_login_3m
													AND agg.Simple_Segment   	= c.Simple_Segment
													AND agg.DTV_TA_calls_1m 	= c.DTV_TA_calls_1m
													AND agg.BB_all_calls_1m 	= c.BB_all_calls_1m
WHERE dtV_active = 1 AND bb_active = 1 													
GROUP BY     subs_week_and_year	
	, segment
	, Churn_type
