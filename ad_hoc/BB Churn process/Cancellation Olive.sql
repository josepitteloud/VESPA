SELECT DISTINCT
	  a.account_number
	, a.effective_from_dt
	, status_code
INTO #t1	
FROM CUST_SUBS_HIST AS a 
WHERE Status_Code IN ('CN', 'SC','PO')
    AND subscription_sub_type = 'Broadband DSL Line'
	AND effective_from_dt >= '2016-01-01'
COMMIT 
CREATE HG INDEX id1 ON #t1 (account_number)
CREATE DATE INDEX id2 ON #t1 (effective_from_dt)
COMMIT 

SELECT 	  DISTINCT 
	  a.account_number
	, a.effective_from_dt CN_DT 
	, a.status_code AS CSH_status_code
	, b.event_dt
	, b.country
	, CASE WHEN b.status_code = 'PC' THEN PC_Effective_To_Dt
           WHEN b.status_code = 'BCRQ' THEN  BCRQ_Effective_To_Dt
           WHEN b.status_code = 'AB' THEN AB_Effective_To_Dt ELSE NULL END  AS intended_CN_dt
	, CASE  WHEN b.enter_syscan = 1 THEN 'Syscan' 
            WHEN b.enter_cuscan = 1 THEN 'Cuscan' 
            WHEN b.enter_HM = 1 THEN 'HM' 
            WHEN b.Enter_3rd_Party = 1 THEN '3rd Party' 
            ELSE 'Weird' END  AS churn_type 
    , b.bb_cust_type
	, b.prodplat_churn_type 
	, rank() OVER (PARTITION BY a.account_number ORDER BY event_dt  DESC) AS rankk
	, DATEDIFF (day,event_dt, CN_DT)	AS PL_duration
	, CAST (NULL  AS VARCHAR(2)) 		AS SABB_segment
	, CAST (NULL  AS VARCHAR(2)) 		AS TP_segment
	, DATEADD ( day, 5-datepart(weekday, event_dt), event_dt)  				AS base_dt
INTO BB_Churn_cancellations_11
FROM #t1 AS a 
LEFT JOIN citeam.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number 
										AND a.effective_from_dt >= event_dt 

DELETE FROM BB_Churn_cancellations_11 WHERE rankk <> 1 
										
GO
CREATE HG INDEX ID1 ON BB_Churn_cancellations_11(account_number)
CREATE DTTM INDEX ID2 ON BB_Churn_cancellations_11(base_dt)
GO

UPDATE BB_Churn_cancellations_11
SET SABB_segment = CAST(CASE 	WHEN node IN (22, 46, 49, 70, 75, 71) THEN 1
													WHEN node IN ( 83, 53, 43, 82, 73, 57) THEN 2
													WHEN node IN ( 63, 47, 68, 42, 62, 12, 39, 11, 35) THEN 3
													WHEN node IN ( 21, 74, 72) THEN 4
													WHEN node IN ( 40, 36, 66, 60, 65) THEN 5
													WHEN node IN ( 77, 31, 84, 56, 76) THEN 6
													WHEN node IN ( 10, 41, 67) THEN 7
													WHEN node IN ( 61, 51, 64, 24, 50) THEN 8
													WHEN node IN ( 27, 55, 85, 81, 79, 80, 54) THEN 9
													WHEN node IN ( 9) THEN 10
													ELSE 0 END AS VARCHAR(2))
FROM BB_Churn_cancellations_11 As a 
JOIN pitteloudj.DTV_Fcast_Weekly_Base AS b ON a.account_number = b.account_number AND DATEADD(week,-2, a.base_dt) = b.end_date 
LEFT JOIN BB_SABB_Churn_segments_lookup AS c  ON b.BB_offer_rem_and_end = c.BB_offer_rem_and_end
												AND b.BB_tenure 			= c.BB_tenure 
												AND b.my_sky_login_3m 		= c.my_sky_login_3m
												AND b.talk_type 			= c.talk_type
												AND b.home_owner_status 	= c.home_owner_status
												AND b.BB_all_calls_1m 		= c.BB_all_calls_1m
												
												
												
												
GO
ALTER TABLE BB_Churn_cancellations_11
ADD (plus_at_PL bit DEFAULT 0 , plus_at_CN BIT DEFAULT 0, now_v1 BIT DEFAULT 0 , now_v2 BIT DEFAULT 0 , now_v3 BIT DEFAULT 0  )
GO
UPDATE BB_Churn_cancellations_11
SET plus_at_CN = 1
FROM BB_Churn_cancellations_11  as a 
JOIN CUST_SUBS_HIST AS b ON a.account_number = b.account_number AND  DATEADD (week, -1, a.CN_DT)  BETWEEN b.effective_from_dt AND b.effective_to_dt
                        AND b.Status_Code IN ('AB', 'AC','PC')
                        AND b.subscription_sub_type = 'DTV Sky+'
GO
UPDATE BB_Churn_cancellations_11
SET plus_at_PL = 1
FROM BB_Churn_cancellations_11  as a 
JOIN CUST_SUBS_HIST AS b ON a.account_number = b.account_number AND   event_dt  BETWEEN b.effective_from_dt AND b.effective_to_dt
                        AND b.Status_Code IN ('AB', 'AC','PC')
                        AND b.subscription_sub_type = 'DTV Sky+'
GO						
/*						
SELECT country, churn_type, subs_week_and_year cn_week, bb_cust_type,  plus_at_CN, plus_at_PL, prodplat_churn_type, count(*) hits 
FROM BB_Churn_cancellations_11 AS a 
JOIN sky_calendar As b On a.CN_DT = b.calendar_date 
WHERE CSH_status_code <> 'PO'
GROUP BY country,churn_type, cn_week, bb_cust_type, plus_at_CN, plus_at_PL,prodplat_churn_type
*/

ALTER TABLE BB_Churn_cancellations_11
ADD (subs_week_and_year INT DEFAULT NULL 
    , end_date DATE default null )

GO
UPDATE BB_Churn_cancellations_11
SET a.subs_week_and_year  = b.subs_week_and_year
FROM  BB_Churn_cancellations_11 as a 
JOIN (SELECT calendar_date AS dt , subs_week_and_year FROM  sky_calendar )As b On a.CN_DT = b.dt

GO
UPDATE BB_Churn_cancellations_11
SET now_v1 = 1 
FROM BB_Churn_cancellations_11 AS a
JOIN citeam.nowtv_accounts_ents AS csav ON a.account_number= csav.account_number
	WHERE     CN_DT -1 BETWEEN  period_start_date AND period_end_date  
	
GO
UPDATE BB_Churn_cancellations_11
SET now_v2 = 1 
FROM BB_Churn_cancellations_11 AS a
JOIN citeam.nowtv_accounts_ents AS csav ON a.account_number= csav.account_number
	WHERE     event_dt-1 BETWEEN  period_start_date AND period_end_date     

GO
UPDATE BB_Churn_cancellations_11
SET a.end_date  = b.dt
FROM  BB_Churn_cancellations_11 as a 
JOIN (SELECT MIN(calendar_date ) - 1 AS dt , subs_week_and_year FROM  sky_calendar GROUP BY subs_week_and_year ) As b On CAST( a.subs_week_and_year AS INT) = CAST ( b.subs_week_and_year AS INT) 

GO

GRANT ALL ON BB_Churn_cancellations_11 to spencerc2




