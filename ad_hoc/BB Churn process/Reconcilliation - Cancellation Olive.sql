SELECT account_number
    , Subs_Week_And_Year
    , event_dt CN_DT
    , country
    , status_code CN_status_code
INTO churn_bb_2017    
FROM Decisioning.Churn_BB

COMMIT 
CREATE HG INDEX ID1 ON churn_bb_2017(account_number)
CREATE HG INDEX ID2 ON churn_bb_2017(Subs_Week_And_Year)
CREATE DATE INDEX ID3 ON churn_bb_2017(CN_DT)
CREATE LF INDEX ID4 ON churn_bb_2017(CN_status_code)

COMMIT 
DROP TABLE PL_Entries_2017;
SELECT a.account_number 
    , b.Subs_Week_And_Year  AS PL_Subs_Week_And_Year
    , event_dt              AS PL_DT
    , prev_status_code
    , status_code           AS PL_status_code
    , churn_type     = CASE   WHEN enter_syscan = 1 THEN 'syscan'
                              WHEN enter_cuscan = 1 THEN 'cuscan' 
                              WHEN enter_hm = 1 THEN 'HM'
                              WHEN enter_3rd_party = 1 THEN '3rd_party' ELSE 'NA' END  
    , COALESCE(b.PC_Next_Status_Code, b.AB_next_status_code, b.BCRQ_Next_Status_Code) AS CN_next_status_code
    , COALESCE(b.PC_effective_to_dt, b.AB_effective_to_dt, b.BCRQ_effective_to_dt) AS CN_next_effective_to_dt
    , COALESCE(b.PC_future_sub_effective_dt, b.AB_future_sub_effective_dt, b.BCRQ_future_sub_effective_dt) AS CN_future_sub_effective_dt
    , PC_effective_to_dt
    , bb_cust_type AS PL_bb_cust_type 
    , RANK() OVER (PARTITION BY a.account_number ORDER BY PL_DT DESC) rankk
INTO PL_Entries_2017
FROM  churn_bb_2017 As a    
JOIN  CITeam.PL_Entries_BB AS b ON a.account_number = b.account_number AND PL_DT <= CN_DT
AND     CN_next_status_code         = CN_status_code 
    
COMMIT 
CREATE HG INDEX ID1 ON PL_Entries_2017(account_number)
CREATE HG INDEX ID2 ON PL_Entries_2017(PL_Subs_Week_And_Year)
CREATE DATE INDEX ID3 ON PL_Entries_2017(PL_DT)
CREATE LF INDEX ID4 ON PL_Entries_2017(PL_status_code)

COMMIT 

SELECT  a.account_number 
        , Subs_Week_And_Year
        , CN_DT
        , country
        , CN_status_code
        , PL_Subs_Week_And_Year
        , PL_DT
        , prev_status_code
        ,  PL_status_code
        ,  churn_type
        ,  CN_next_status_code
        ,  CN_next_effective_to_dt
        ,  CN_future_sub_effective_dt
        ,  PC_effective_to_dt
        ,  PL_bb_cust_type
INTO chunr_consolidated_2017        
FROM      churn_bb_2017 AS a 
LEFT JOIN  PL_Entries_2017 AS b ON a.account_number = b.account_number AND b.rankk = 1   
        sp_columns PL_Entries_2017
        
        
SELECT     Subs_Week_And_Year, PL_status_code, churn_type, PL_bb_cust_type,country, now_at_PL,now_at_CN, skyplus_at_PL, skyplus_at_CN, count(*) hits FROM chunr_consolidated_2017
WHERE   Subs_Week_And_Year >= 201701
GROUP BY   Subs_Week_And_Year, PL_status_code, churn_type,country,  PL_bb_cust_type,now_at_PL,now_at_CN, skyplus_at_PL, skyplus_at_CN


ALTER TABLE chunr_consolidated_2017
ADD (now_at_PL AS BIT DEFAULT 0 , now_at_CN BIT DEFAULT 0, skyplus_at_PL BIT DEFAULT 0 , skyplus_at_CN BIT DEFAULT 0 ) 


SET TEMPORARY OPTION Query_Temp_Space_Limit = 0; 
UPDATE chunr_consolidated_2017
SET now_at_PL = 1 
FROM chunr_consolidated_2017 AS a
JOIN citeam.nowtv_accounts_ents AS csav ON a.account_number= csav.account_number
	WHERE     PL_DT-1 BETWEEN  period_start_date AND period_end_date   
	AND         csav.subscriber_this_period = 1  

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0; 
UPDATE chunr_consolidated_2017
SET now_at_CN = 1 
FROM chunr_consolidated_2017 AS a
JOIN citeam.nowtv_accounts_ents AS csav ON a.account_number= csav.account_number
	WHERE     CN_DT-1 BETWEEN  period_start_date AND period_end_date   
	AND         csav.subscriber_this_period = 1  


SET TEMPORARY OPTION Query_Temp_Space_Limit = 0; 
UPDATE chunr_consolidated_2017
SET skyplus_at_PL = 1 
FROM chunr_consolidated_2017 AS a 
JOIN SKY_calendar AS x ON DATEADD(WEEK, -1, a.PL_DT) = x.calendar_date 
JOIN citeam.cust_weekly_base AS b ON a.account_number = b.account_number AND CAST(x.subs_week_and_year AS INT)= b.subs_week_and_year
AND b.skyplus_active = 1;

UPDATE chunr_consolidated_2017
SET skyplus_at_CN = 1 
FROM chunr_consolidated_2017 AS a 
JOIN SKY_calendar AS x ON DATEADD(WEEK, -1, a.CN_DT) = x.calendar_date 
JOIN citeam.cust_weekly_base AS b ON a.account_number = b.account_number AND CAST(x.subs_week_and_year AS INT) = b.subs_week_and_year
AND b.skyplus_active = 1