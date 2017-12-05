CREATE OR REPLACE PROCEDURE SABB_my_sky_login_prob 
AS BEGIN 

	MESSAGE CAST(now() as timestamp)||' | SABB_my_sky_login_prob - Initialization begin ' TO CLIENT
	
	DECLARE @mx_dt  DATE 
	SET @mx_dt = (SELECT max(end_date) from cust_fcast_weekly_base_2) 

	IF EXISTS ( SELECT tname FROM syscatalog WHERE creator = USER_NAME()  AND UPPER(tname) = UPPER('SABB_my_sky_login_prob_TABLE') AND tabletype = 'TABLE')                
	DROP TABLE SABB_my_sky_login_prob_TABLE

	SELECT account_number
		,CAST ( NULL AS VARCHAR(20) ) segment_sa
		,MAX(CASE WHEN end_date = @mx_dt THEN my_sky_login_3m_raw ELSE 0 END) curr_count													-- my_sky_login_3m_raw on the current week
		,MAX(CASE WHEN end_date = DATEADD(DAY, -7, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END) count_1w									-- -- my_sky_login_3m_raw on the Previous week
		, Calls_LW = CASE WHEN curr_count < count_1w THEN 0 ELSE curr_count - count_1w	END 												-- Logins made last week
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -12,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN my_sky_login_3m_raw ELSE 0 END) L_12 	-- Max Login in the past 12 month
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -9,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN my_sky_login_3m_raw ELSE 0 END) L_9	-- Max Login in the past 9 month
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -6,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN my_sky_login_3m_raw ELSE 0 END) L_6	-- Max Login in the past 6 month
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -3,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN my_sky_login_3m_raw ELSE 0 END) L_3	-- Max Login in the past 3 month
		, CASE  WHEN L_12 = 0 THEN 1
				WHEN L_9 = 0 THEN 2
				WHEN L_6 = 0 THEN 3
				WHEN L_3 = 0 THEN 4
				ELSE 5 END Prob_Group																										-- Picking the longest Group
	INTO #mysky_prob_1
	FROM 
		cust_fcast_weekly_base_2
	WHERE  bb_active = 1 AND dtv_active = 0								
		AND account_number IN (SELECT DISTINCT account_number fROM FORECAST_Base_Sample)
		AND BB_latest_act_dt IS NOT NULL
	GROUP  BY 	account_number

	COMMIT
	CREATE HG INDEX id1 ON #mysky_prob_1(account_number)
	COMMIT

	UPDATE #mysky_prob_1 
	SET a.segment_sa = b.segment_sa
	FROM #mysky_prob_1 AS a 
	JOIN DTV_fcast_weekly_base_2 AS b ON a.account_number = b.account_number AND b.end_Date = @mx_dt
	COMMIT 


	SELECT 1 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW) rank_id
	INTO #mysky_prob_2
	FROM #mysky_prob_1
	WHERE L_12 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION
	SELECT 2 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW) rank_id
	FROM #mysky_prob_1
	WHERE L_9 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION	
	SELECT 3 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW) rank_id
	FROM #mysky_prob_1
	WHERE L_6 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION	
	SELECT 4 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW) rank_id
	FROM #mysky_prob_1
	WHERE L_3 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION
	SELECT 5 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW) rank_id
	FROM #mysky_prob_1
	WHERE Prob_Group = 5 
	GROUP BY Calls_LW
		, segment_sa

	SELECT * 
				, SUM( prob) OVER (PARTITION BY  Prob_Group, segment_sa ORDER BY RANK_ID)  UPPER_LIMIT 
		INTO #mysky_prob_3
	FROM #mysky_prob_2
		
				
	SELECT 	  a.Prob_Group
			, a.Calls_LW
			, a.segment_sa
			, COALESCE(b.UPPER_LIMIT, 0) Lower_limit
			, a.UPPER_LIMIT
	INTO SABB_my_sky_login_prob_TABLE
	FROM #mysky_prob_3 AS a 
	LEFT JOIN #mysky_prob_3 AS b ON a.segment_sa = b.segment_sa
						  AND a.Prob_Group = b.Prob_Group
						  AND a.rank_id -1 = b.rank_id 
		
	
	MESSAGE CAST(now() as timestamp)||' | SABB_my_sky_login_prob_TABLE - COMPLETED: '||@@rowcount TO CLIENT
	
	COMMIT 
	CREATE LF INDEX id1 ON SABB_my_sky_login_prob_TABLE(Prob_Group)
	CREATE LF INDEX id2 ON SABB_my_sky_login_prob_TABLE(Calls_LW)
	CREATE HG INDEX id3 ON SABB_my_sky_login_prob_TABLE(Lower_limit)
	CREATE HG INDEX id4 ON SABB_my_sky_login_prob_TABLE(UPPER_LIMIT)
	GRANT SELECT ON SABB_my_sky_login_prob_TABLE TO citeam, vespa_group_low_security
	
	COMMIT 
	MESSAGE CAST(now() as timestamp)||' | SABB_my_sky_login_prob - COMPLETED ' TO CLIENT
END 

	   
--================================================================================================================================================	   
--================================================================================================================================================	   
--================================================================================================================================================
	   
	   CREATE OR REPLACE PROCEDURE SABB_BB_Calls_prob 
AS BEGIN 

	MESSAGE CAST(now() as timestamp)||' | SABB_BB_Calls_prob - Initialization begin ' TO CLIENT
	
	DECLARE @mx_dt  DATE 
	SET @mx_dt = (SELECT max(end_date) from cust_fcast_weekly_base_2) 

	IF EXISTS ( SELECT tname FROM syscatalog WHERE creator = USER_NAME()  AND UPPER(tname) = UPPER('SABB_BB_Calls_prob_TABLE') AND tabletype = 'TABLE')                
	DROP TABLE SABB_BB_Calls_prob_TABLE

	SELECT account_number
		,CAST ( NULL AS VARCHAR(20) ) segment_sa
		,MAX(CASE WHEN end_date = @mx_dt THEN BB_all_calls_1m_raw ELSE 0 END) curr_count													-- BB_all_calls_1m_raw on the current week
		,MAX(CASE WHEN end_date = DATEADD(DAY, -7, @mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END) count_1w									-- -- BB_all_calls_1m_raw on the Previous week
		, Calls_LW = CASE WHEN curr_count < count_1w THEN 0 ELSE curr_count - count_1w	END 												-- Logins made last week
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -12,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END) L_12 	-- Max Login in the past 12 month
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -9,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END) L_9	-- Max Login in the past 9 month
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -6,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END) L_6	-- Max Login in the past 6 month
		,MAX(CASE WHEN  end_date BETWEEN DATEADD(MONTH, -3,@mx_dt) AND DATEADD(WEEK, -1,@mx_dt) THEN BB_all_calls_1m_raw ELSE 0 END) L_3	-- Max Login in the past 3 month
		, CASE  WHEN L_12 = 0 THEN 1
				WHEN L_9 = 0 THEN 2
				WHEN L_6 = 0 THEN 3
				WHEN L_3 = 0 THEN 4
				ELSE 5 END Prob_Group																										-- Picking the longest Group
	INTO #BB_CALLS_prob_1
	FROM 
		cust_fcast_weekly_base_2
	WHERE  bb_active = 1 AND dtv_active = 0								
		AND account_number IN (SELECT DISTINCT account_number fROM FORECAST_Base_Sample)
		AND BB_latest_act_dt IS NOT NULL
	GROUP  BY 	account_number

	COMMIT
	CREATE HG INDEX id1 ON #BB_CALLS_prob_1(account_number)
	COMMIT

	UPDATE #BB_CALLS_prob_1 
	SET a.segment_sa = b.segment_sa
	FROM #BB_CALLS_prob_1 AS a 
	JOIN DTV_fcast_weekly_base_2 AS b ON a.account_number = b.account_number AND b.end_Date = @mx_dt
	COMMIT 


	SELECT 1 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC) rank_id
	INTO #BB_calls_prob_2
	FROM #BB_CALLS_prob_1
	WHERE L_12 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION
	SELECT 2 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC) rank_id
	FROM #BB_CALLS_prob_1
	WHERE L_9 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION	
	SELECT 3 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC ) rank_id
	FROM #BB_CALLS_prob_1
	WHERE L_6 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION	
	SELECT 4 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC) rank_id
	FROM #BB_CALLS_prob_1
	WHERE L_3 = 0 
	GROUP BY Calls_LW
		, segment_sa
	UNION
	SELECT 5 Prob_Group
		, Calls_LW
		, segment_sa
		, COUNT ( 1) hits
		, SUM( hits) OVER (PARTITION BY segment_sa)  t_segment
		, prob = CASE WHEN t_segment >0 THEN CAST (hits AS FLOAT) / CAST(t_segment AS FLOAT) ELSE 0 END 
		, RANK () OVER (PARTITION BY segment_sa ORDER BY Calls_LW DESC) rank_id
	FROM #BB_CALLS_prob_1
	WHERE Prob_Group = 5 
	GROUP BY Calls_LW
		, segment_sa

	SELECT * 
				, SUM( prob) OVER (PARTITION BY  Prob_Group, segment_sa ORDER BY RANK_ID )  UPPER_LIMIT 
	INTO #BB_CALLS_prob_3
	FROM #BB_calls_prob_2
		
				
	SELECT 	  a.Prob_Group
			, a.Calls_LW
			, a.segment_sa
			, COALESCE(b.UPPER_LIMIT, 0) Lower_limit
			, a.UPPER_LIMIT
	INTO SABB_BB_Calls_prob_TABLE
	FROM #BB_CALLS_prob_3 AS a 
	LEFT JOIN #BB_CALLS_prob_3 AS b ON a.segment_sa = b.segment_sa
						  AND a.Prob_Group = b.Prob_Group
						  AND a.rank_id -1 = b.rank_id 
		
	
	MESSAGE CAST(now() as timestamp)||' | SABB_BB_Calls_prob_TABLE - COMPLETED: '||@@rowcount TO CLIENT
	
	COMMIT 
	CREATE LF INDEX id1 ON SABB_BB_Calls_prob_TABLE(Prob_Group)
	CREATE LF INDEX id2 ON SABB_BB_Calls_prob_TABLE(Calls_LW)
	CREATE HG INDEX id3 ON SABB_BB_Calls_prob_TABLE(Lower_limit)
	CREATE HG INDEX id4 ON SABB_BB_Calls_prob_TABLE(UPPER_LIMIT)
	GRANT SELECT ON SABB_BB_Calls_prob_TABLE TO citeam, vespa_group_low_security
	
	COMMIT 
	MESSAGE CAST(now() as timestamp)||' | SABB_BB_Calls_prob - COMPLETED ' TO CLIENT
END 
