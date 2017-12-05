DECLARE @mx_dt DATE DEFAULT NULL 
SET @mx_dt = (SELECT max(end_date) FROM pitteloudj.DTV_FCAST_WEEKLY_BASE) 

INSERT INTO DTV_FCAST_WEEKLY_BASE (Time_Since_Last_AB,SC_Gross_Terminations,Placeholder_1,
	Accessibility_DTV_PC,TA_DTV_Offer_Applied,Account_Number,WC_DTV_PC,Affluence,Previous_AB_Count,BB_Active,Subs_Week,BB_all_calls_1m,TA_Save_Count,BB_offer_rem_and_end,
	Time_To_Offer_End_BB,BB_Package,Web_Chat_TA_Not_Saved,BB_Status_Code,PO_Pipeline_Cancellations,BB_tenure,RTM,Country,Sports_Tenure,CusCan_Forecast_Segment,Subs_Year,
	Days_Since_Last_Payment_Dt,TA_Event_Count,Downgrade_View,Talk_tenure,DTV_AB,Time_Since_Last_TA_call,DTV_Active,Time_To_Offer_End_LR,DTV_PC,Web_Chat_TA_Cnt,DTV_Status_Code,
	WebChatOutBd_TA_Not_Saved,DTV_Status_Code_EoW,Placeholder_2,DTV_TA_calls_1m,Prem_Segment,DTV_Tenure,Previous_ABs,End_Date,Same_Day_Cancels,Future_Subs_Effective_Subs_Quarter,
	Simple_Segment,Future_Subs_Effective_Subs_Week,	Subs_Quarter,Future_Subs_Effective_Subs_Week_And_Year,Subs_Week_And_Year,Future_Subs_Effective_Subs_Year,
	SysCan_Forecast_Segment,Had_Offer_In_Last_Year,TA_DTV_PC,HD_segment,TA_Non_Save_Count,	home_owner_status,TA_Sky_Plus_Save,Min_Term_PC,talk_type,Movies_Tenure,Time_Since_Last_AB_Int,
	my_sky_login_3m,Time_To_Offer_End,New_Customer,Time_To_Offer_End_DTV,	Offer_Applied_BB,Unique_TA_Caller,Offer_Applied_DTV,WC_Sky_Plus_Save,Offer_Length_DTV,Web_Chat_TA_Customers,
	Other_PC,Web_Chat_TA_Saved,Package_Desc,WebChatOutBd_TA_Saved,_24MF_BB_Offer,PC_Action, sky_plus)
SELECT Time_Since_Last_AB,SC_Gross_Terminations,Placeholder_1,
	Accessibility_DTV_PC,TA_DTV_Offer_Applied,Account_Number,WC_DTV_PC,Affluence,Previous_AB_Count,BB_Active,Subs_Week,BB_all_calls_1m,TA_Save_Count,BB_offer_rem_and_end,
	Time_To_Offer_End_BB,BB_Package,Web_Chat_TA_Not_Saved,BB_Status_Code,PO_Pipeline_Cancellations,BB_tenure,RTM,Country,Sports_Tenure,CusCan_Forecast_Segment,Subs_Year,
	Days_Since_Last_Payment_Dt,TA_Event_Count,Downgrade_View,Talk_tenure,DTV_AB,Time_Since_Last_TA_call,DTV_Active,Time_To_Offer_End_LR,DTV_PC,Web_Chat_TA_Cnt,DTV_Status_Code,
	WebChatOutBd_TA_Not_Saved,DTV_Status_Code_EoW,Placeholder_2,DTV_TA_calls_1m,Prem_Segment,DTV_Tenure,Previous_ABs,End_Date,Same_Day_Cancels,Future_Subs_Effective_Subs_Quarter,
	Simple_Segment,Future_Subs_Effective_Subs_Week,	Subs_Quarter,Future_Subs_Effective_Subs_Week_And_Year,Subs_Week_And_Year,Future_Subs_Effective_Subs_Year,
	SysCan_Forecast_Segment,Had_Offer_In_Last_Year,TA_DTV_PC,HD_segment,TA_Non_Save_Count,	home_owner_status,TA_Sky_Plus_Save,Min_Term_PC,talk_type,Movies_Tenure,Time_Since_Last_AB_Int,
	my_sky_login_3m,Time_To_Offer_End,New_Customer,Time_To_Offer_End_DTV,	Offer_Applied_BB,Unique_TA_Caller,Offer_Applied_DTV,WC_Sky_Plus_Save,Offer_Length_DTV,Web_Chat_TA_Customers,
	Other_PC,Web_Chat_TA_Saved,Package_Desc,WebChatOutBd_TA_Saved,_24MF_BB_Offer,PC_Action, 0 
FROM citeam.DTV_FCAST_WEEKLY_BASE
WHERE end_date > @mx_dt
	AND dtv_active = 0
	AND bb_active = 1 
	AND end_date <= getdate () 
	
	
-----------------------------------------------------------
-----------		my_sky update
-----------------------------------------------------------
SET TEMPORARY OPTION Query_Temp_Space_Limit = 0

SELECT COUNT(DISTINCT visit_date) AS visit_days
      , mr.account_number
	  , end_date
INTO #days_visited_3m
FROM vespa_shared.mysky_daily_usage 	AS mr 
JOIN pitteloudj.DTV_Fcast_Weekly_Base 	AS base ON BASE.account_number = mr.account_number
WHERE visit_date BETWEEN DATEADD(mm,-3,end_date) AND end_date
AND end_date > @mx_dt
GROUP BY mr.account_number, end_date

COMMIT 
CREATE HG INDEX id1 ON #days_visited_3m (account_number) 
CREATE DATE INDEX ID2 ON #days_visited_3m(end_date)
COMMIT 

UPDATE pitteloudj.DTV_Fcast_Weekly_Base
SET my_sky_login_3m = CASE WHEN visit_days > 2 THEN 3 ELSE visit_days END 
FROM pitteloudj.DTV_Fcast_Weekly_Base AS base
INNER JOIN #days_visited_3m AS dv ON base.account_number = dv.account_number AND base.end_date = dv.end_date
WHERE  base.end_date > @mx_dt

MESSAGE 'my_sky_login_3m updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #days_visited_3m


-----------------------------------------------------------
-----------		talktype
-----------------------------------------------------------     
      
SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
SELECT CSH.account_number
      ,current_product_description
	  ,end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt ASC, CSH.cb_row_id ASC) AS rank_1  
INTO #talk_holding
FROM cust_subs_hist 						AS CSH
INNER JOIN pitteloudj.DTV_Fcast_Weekly_Base AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
AND   csh.status_code IN ('A','PC','FBP','RI','FBI','BCRQ')
AND   end_date BETWEEN effective_from_dt AND effective_to_dt
AND   effective_to_dt > effective_from_dt
AND    base.end_date > @mx_dt

COMMIT 
DELETE FROM #talk_holding WHERE rank_1 <> 1 
CREATE HG INDEX id1 ON #talk_holding (account_number) 
CREATE DATE INDEX id2 ON #talk_holding (end_date) 
COMMIT 

UPDATE pitteloudj.DTV_Fcast_Weekly_Base
SET talk_type = CASE WHEN current_product_description LIKE 'Sky Talk 24 / 7%' THEN 'Sky Talk 24 / 7'
                     WHEN current_product_description LIKE 'Sky Talk Anytime Extra%' THEN 'Sky Talk Anytime Extra'
                     WHEN current_product_description LIKE 'Anytime%' THEN 'Sky Talk Anytime'
                     WHEN current_product_description LIKE 'Off Peak%' THEN 'Off Peak'
                     WHEN current_product_description LIKE 'Sky Talk Freetime%' THEN 'Sky Talk Freetime'
                     WHEN current_product_description LIKE 'Sky Talk International Extra%' THEN 'Sky Talk International Extra'
                     WHEN current_product_description LIKE 'Sky Talk Unlimited%' THEN 'Sky Talk Unlimited'
                     WHEN current_product_description LIKE 'Sky Talk Anytime%' THEN 'Sky Talk Anytime'
                     WHEN current_product_description LIKE 'Sky Talk Evenings and Weekends%' THEN 'Sky Talk Evenings and Weekends'
                     WHEN current_product_description LIKE 'Missing at load' THEN 'NONE'
                     ELSE current_product_description END
FROM pitteloudj.DTV_Fcast_Weekly_Base 	AS base
INNER JOIN #talk_holding 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date
WHERE base.end_date > @mx_dt

MESSAGE 'talk_type updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #talk_holding
    
----------------------------
-----------------------------------------------------------
-----------		offer rem
-----------------------------------------------------------     


SET TEMPORARY OPTION Query_Temp_Space_Limit = 0
SELECT base.account_number
      ,MAX(offer_duration) AS offer_length
      ,MAX(DATEDIFF(DD, end_date, intended_offer_end_dt)) AS length_rem
	  , BB_current_offer_duration_rem = CASE WHEN length_rem > 2854 THEN 2854
                                          WHEN length_rem < 0    THEN 0
                                          ELSE length_rem 
										END 
		, end_date 
INTO #current_bb_offer_length      
FROM  pitteloudj.DTV_Fcast_Weekly_Base AS base
INNER JOIN citeam.offer_usage_all AS oua
ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line'
AND end_date >= offer_start_dt_actual
AND end_date <  offer_end_dt_actual
AND intended_total_offer_value_yearly IS NOT NULL
AND  end_date > @mx_dt
GROUP BY base.account_number, end_date

COMMIT 

SELECT base.account_number
      ,offer_end_dt_actual
      ,rank() over(PARTITION BY base.account_number, end_date ORDER BY offer_start_dt_actual DESC) AS latest_offer
	  , end_date
	  , BB_time_since_last_offer_end = DATEDIFF(DD, offer_end_dt_actual, end_date)
INTO #prev_bb_offer_dt      
FROM  pitteloudj.DTV_Fcast_Weekly_Base 			AS base
INNER JOIN citeam.offer_usage_all 				AS oua 			ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line'
		AND end_date >  offer_start_dt_actual
		AND end_date >= offer_end_dt_actual
		AND intended_total_offer_value_yearly IS NOT NULL
		AND  end_date > @mx_dt

COMMIT 
DELETE FROM #prev_bb_offer_dt      WHERE latest_offer <>1
CREATE HG INDEX id1 ON #prev_bb_offer_dt (account_number)
CREATE DATE INDEX id2 ON #prev_bb_offer_dt (offer_end_dt_actual)
COMMIT

UPDATE  pitteloudj.DTV_Fcast_Weekly_Base
SET BB_offer_rem_and_end_raw =  CASE WHEN BB_current_offer_duration_rem > 0 THEN BB_current_offer_duration_rem 
								WHEN (BB_current_offer_duration_rem = 0 OR BB_current_offer_duration_rem  IS NULL) AND BB_time_since_last_offer_end <> - 9999 THEN (0 - BB_time_since_last_offer_end) 
								ELSE - 9999 END
FROM  pitteloudj.DTV_Fcast_Weekly_Base		AS a	 
LEFT JOIN #current_bb_offer_length  	AS b ON a.account_number = b.account_number  AND a.end_date = b.end_date
LEFT JOIN #prev_bb_offer_dt      		AS c ON a.account_number = c.account_number  AND a.end_date = c.end_date
WHERE  a.end_date > @mx_dt

COMMIT 							

UPDATE  pitteloudj.DTV_Fcast_Weekly_Base 
SET BB_offer_rem_and_end = CASE WHEN BB_offer_rem_and_end_raw BETWEEN -9998 AND -1015 	THEN -3
								WHEN BB_offer_rem_and_end_raw BETWEEN -1015 AND -215 	THEN -2 
								WHEN BB_offer_rem_and_end_raw BETWEEN -215  AND -75  	THEN -1
								WHEN BB_offer_rem_and_end_raw BETWEEN -74  AND -0    	THEN 0
								WHEN BB_offer_rem_and_end_raw BETWEEN 1    AND 62    	THEN 1
								WHEN BB_offer_rem_and_end_raw BETWEEN 63   AND 162   	THEN 2
								WHEN BB_offer_rem_and_end_raw BETWEEN 163  AND 271		THEN 3
								WHEN BB_offer_rem_and_end_raw >271						THEN 4
								ELSE -9999 END 
WHERE  end_date > @mx_dt

COMMIT 
MESSAGE 'BB_offer_rem_and_end updated: '||@@rowcount type status to client
COMMIT 
UPDATE pitteloudj.DTV_Fcast_Weekly_Base
SET BB_offer_rem_and_end = -9999
WHERE BB_offer_rem_and_end IS NULL
COMMIT 
UPDATE 	pitteloudj.DTV_Fcast_Weekly_Base
SET a.BB_first_act_dt= b.BB_first_act_dt
	, a.BB_latest_act_dt = b.BB_latest_act_dt
FROM 	pitteloudj.DTV_Fcast_Weekly_Base AS a 
JOIN 	citeam.cust_Fcast_Weekly_Base	AS b ON a.account_number = b.account_number AND a.end_date = b.end_date
WHERE b.end_date > @mx_dt

SELECT DISTINCT a.account_number, 1 sky_plus, b.end_date
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   pitteloudj.DTV_Fcast_Weekly_Base AS b ON a.account_number = b.account_number AND     	b.end_date BETWEEN a.effective_from_dt AND a.effective_to_dt 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND b.end_date > @mx_dt

UPDATE pitteloudj.DTV_Fcast_Weekly_Base 
SET sky_plus = 1 
FROM pitteloudj.DTV_Fcast_Weekly_Base AS a 
JOIN #skyplus AS b ON a.account_number = b.account_number AND b.end_date = a.end_date



---------------------------
GO 

CREATE or replace  VIEW bb_forecast_vars_1 AS 
SELECT 'talk_type' var              , end_date, talk_type, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY talk_type, end_date
UNION 
SELECT 'my_sky_login_3m' var        , end_date, CAST(my_sky_login_3m AS VARCHAR) log, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY log, end_date
UNION 
SELECT 'BB_offer_rem_and_end' var   , end_date, CAST(BB_offer_rem_and_end AS VARCHAR) rem, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY rem, end_date
UNION 
SELECT 'home_owner_status' var      , end_date, home_owner_status, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY home_owner_status, end_date
UNION 
SELECT 'BB_all_calls_1m' var        , end_date, CAST(BB_all_calls_1m AS VARCHAR) BB, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY BB, end_date
UNION 
SELECT 'BB_tenure' var              , end_date, CAST(BB_tenure AS VARCHAR) BB_tenure, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY BB_tenure, end_date
UNION 
SELECT 'sky_plus' var               , end_date, CAST(sky_plus AS VARCHAR) skypl, count(*) hits from pitteloudj.DTV_FCAST_WEEKLY_BASE WHERE end_date >= DATEADD(MONTH, -6 , getdate()) GROUP BY skypl, end_date
