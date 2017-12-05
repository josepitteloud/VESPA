SELECT end_date, roi, count(*) hits
FROM citeam.CUST_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, roi
----------------------------------------------------------------------------
SELECT end_date, Simple_Segment
, count(*) hits
FROM citeam.CUST_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, Simple_Segment

SELECT end_date, Simple_Segment
, count(*) hits
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, Simple_Segment
----------------------------------------------------------------------------
SELECT end_date, Talk_tenure
, count(*) hits
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, Talk_tenure
----------------------------------------------------------------------------
SELECT end_date, my_sky_login_3m
, count(*) hits
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, my_sky_login_3m
----------------------------------------------------------------------------
SELECT end_date, BB_all_calls_1m
, count(*) hits
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, BB_all_calls_1m
----------------------------------------------------------------------------
SELECT end_date, RTM
, count(*) hits
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, RTM
----------------------------------------------------------------------------
SELECT end_date, DTV_TA_calls_1m
, count(*) hits
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE bb_active = 1 AND DTV_active = 1 
group by end_date, DTV_TA_calls_1m
----------------------------------------------------------------------------
SELECT 'All Churn' type_of_churn
    , null AS PL_entry
	, count( DISTINCT account_number) hits 
	, 'CSH plain' Source
	, subs_week_and_year 
FROM CUST_SUBS_HIST AS a
JOIN sky_calendar As b On a.effective_from_dt = b.calendar_date 
WHERE Status_Code IN ('CN', 'SC','PO')
    AND subscription_sub_type = 'Broadband DSL Line'
    AND effective_from_dt >= '2015-01-01' 
    group by  subs_week_and_year
-------------------------------------------------------------------	
DROP TABLE BB_churn_cancellations
SELECT 
      account_number
    , event_dt
    , AB_Effective_To_Dt
    , AB_Future_Sub_Effective_Dt
INTO #t1 
from Broadband_Comms_Pipeline
WHERE  AB_Next_Status_Code IN ('BCRQ') 
COMMIT     
CREATE HG   INDEX id1 ON #t1 (account_number) 
CREATE DATE INDEX id2 ON #t1 (event_dt) 
CREATE DATE INDEX id3 ON #t1 (AB_Effective_To_Dt) 
CREATE DATE INDEX id4 ON #t1 (AB_Future_Sub_Effective_Dt) 
COMMIT 
SELECT a.account_number
    
    , event_dt        AS PL_dt
    , effective_to_dt   AS cancellation_dt
INTO #t2     
FROM CUST_SUBS_HIST AS a
JOIN #t1 AS b ON a.account_number = b.account_number AND effective_from_dt >= AB_Effective_To_Dt AND effective_from_dt <= DATEADD(DAY, 30,AB_Future_Sub_Effective_Dt)
WHERE prev_status_Code IN ('BCRQ') 
    AND Status_Code IN ('CN', 'SC','PO')
    AND subscription_sub_type = 'Broadband DSL Line'



SELECT 
    account_number
    , event_dt PL_dt
    , COALESCE ( PC_Future_Sub_Effective_Dt,BCRQ_Future_Sub_Effective_Dt,AB_Future_Sub_Effective_Dt) cancellation_dt
    , CASE  WHEN enter_syscan = 1 THEN 'Syscan' 
            WHEN enter_cuscan = 1 THEN 'Cuscan' 
            WHEN enter_HM = 1 THEN 'HM' 
            WHEN Enter_3rd_Party = 1 THEN '3rd Party' 
            ELSE 'Weird' END  AS churn_type 
INTO BB_churn_cancellations 
from Broadband_Comms_Pipeline
WHERE  AB_Next_Status_Code IN ('CN', 'SC','PO') 
    OR BCRQ_Next_Status_Code IN ('CN', 'SC','PO') 
    OR PC_Next_Status_Code IN ('CN', 'SC','PO') 
UNION    
SELECT *,'Syscan' FROM #t2  

SELECT  churn_type, PL_dt , count(*) hits 
FROM BB_churn_cancellations
GROUP BY churn_type, PL_dt 
-------------------------------------------------
SELECT 
      event_dt PL_dt
	  , country
	  , bB_cust_type
    , CASE  WHEN enter_syscan = 1 THEN 'Syscan' 
            WHEN enter_cuscan = 1 THEN 'Cuscan' 
            WHEN enter_HM = 1 THEN 'HM' 
            WHEN Enter_3rd_Party = 1 THEN '3rd Party' 
            ELSE 'Weird' END  AS churn_type 
	, COUNT( DISTINCT account_number) hits
FROM Broadband_Comms_Pipeline
GROUP BY churn_type,    event_dt PL_dt
	  , bB_cust_type
		, country
--------------------------------------

