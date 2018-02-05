SELECT a.account_number 
      ,  COALESCE(a.PC_Future_Sub_Effective_Dt, a.AB_Future_Sub_Effective_Dt, a.BCRQ_Future_Sub_Effective_Dt) AS CN_DT
      ,  COALESCE(a.PC_Next_Status_Code, a.AB_next_status_code, a.BCRQ_Next_Status_Code) AS CSH_status_code
      ,  a.event_dt
      ,  b.country
      ,  CASE WHEN a.status_code = 'PC'   THEN PC_Effective_To_Dt
            WHEN a.status_code = 'BCRQ' THEN BCRQ_Effective_To_Dt
            WHEN a.status_code = 'AB'     THEN AB_Effective_To_Dt ELSE NULL END  AS intended_CN_dt          
      , CASE  WHEN a.enter_syscan = 1     THEN 'Syscan' 
            WHEN a.enter_cuscan = 1       THEN 'Cuscan' 
            WHEN a.enter_HM = 1           THEN 'HM' 
            WHEN a.Enter_3rd_Party = 1    THEN '3rd Party' 
            ELSE 'Syscan - BCRQ' END  AS churn_type 
      , a.bb_cust_type
      , a.prodplat_churn_type 
      , rank() OVER (PARTITION BY a.account_number ORDER BY event_dt  DESC) AS rankk
      , DATEDIFF (day,event_dt, CN_DT)    AS PL_duration
      , CAST (NULL  AS VARCHAR(2))        AS SABB_segment
      , CAST (NULL  AS VARCHAR(2))        AS TP_segment
      , DATEADD ( day, 5-datepart(weekday, event_dt), event_dt)                     AS base_dt
      , 0 AS plus_at_PL 
      , 0 AS plus_at_CN 
      , 0 AS now_v1     
      , 0 AS now_v2     
      , 0 AS now_v3     
      , b.Subs_Week_And_Year
      , b.end_date
INTO BB_Churn_cancellations_12
FROM CITeam.PL_Entries_BB           AS a
LEFT JOIN SKY_calendar                    AS x ON x.calendar_date = DATEADD(wk,-1, a.event_dt) 
LEFT JOIN CITeam.Cust_Weekly_Base   AS b ON a.account_number = b.account_number 
                                        AND CASt(x.Subs_Week_And_Year AS INT) = b.Subs_Week_And_Year -- Join to the week when the PL happened
                                        AND b.skyplus_active = 0  -- NO sky plus
                                        AND bb_active = 1 
                                        AND dTV_active = 0 
LEFT JOIN citeam.nowtv_accounts_ents AS csav ON a.account_number= csav.account_number 
                                        AND Event_Dt BETWEEN  period_start_date AND period_end_date  
                                        AND csav.subscriber_this_period = 1 
WHERE  
            CSH_status_code IN ('CN', 'SC','PO')
      AND csav.account_number IS NULL     
      
GO
CREATE HG   INDEX ID1 ON BB_Churn_cancellations_12(account_number)
CREATE DTTM INDEX ID2 ON BB_Churn_cancellations_12(base_dt)
GO




