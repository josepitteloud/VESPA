CREATE OR REPLACE PROCEDURE TP_my_sky_login_prob AS

BEGIN
	message convert(TIMESTAMP, now()) || ' | TP_my_sky_login_prob - Initialization begin ' TO client

	DECLARE @mx_dt DATE

	SET @mx_dt = (SELECT max(end_date) FROM TP_FORECAST_Base_Sample )

	IF EXISTS (SELECT tname FROM syscatalog WHERE creator = USER_NAME() AND UPPER(tname) = UPPER('TP_my_sky_login_prob_TABLE') AND tabletype = 'TABLE' )
	DROP TABLE TP_my_sky_login_prob_TABLE
	
	--------------------------------------------------------------------------------------------------------------------------------------------		
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
		WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	@mx_dt BETWEEN effective_from_dt AND effective_to_dt 
	
	COMMIT 
	CREATE HG INDEX id1 ON #skyplus (account_number)
	-------------------------------------------------------------------------------------------------------------------------------------------
	SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        NOW_TV_SUBS_HIST AS csav
	JOIN 		FORECAST_Base_Sample AS a ON a.account_number= csav.account_number
	WHERE       @mx_dt BETWEEN effective_from_dt AND effective_to_dt 
	
	COMMIT 
	CREATE HG INDEX id1 ON #nowtv(account_number) 
	COMMIT
	--------------------------------------------------------------------------------------------------------------------------------------------	
	
	SELECT account_number
		, 'segment_TP' 	= convert(VARCHAR(20), NULL)
		, 'curr_count' 	= MAX(CASE WHEN end_date = @mx_dt THEN my_sky_login_3m_raw ELSE 0 END)
		, 'count_1w' 	= MAX(CASE WHEN end_date = DATEADD(day, - 7, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'Calls_LW' 	= CASE WHEN curr_count < count_1w THEN 0 ELSE curr_count - count_1w END
		, 'L_12' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 12, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'L_9' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 9, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'L_6' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 6, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'L_3' 		= MAX(CASE WHEN end_date BETWEEN DATEADD(month, - 3, @mx_dt) AND DATEADD(week, - 1, @mx_dt) THEN my_sky_login_3m_raw ELSE 0 END)
		, 'Prob_Group' 	= CASE WHEN L_12 = 0 THEN 1 WHEN L_9 = 0 THEN 2 WHEN L_6 = 0 THEN 3 WHEN L_3 = 0 THEN 4 ELSE 5 -- Picking the longest Group
			END
	INTO #mysky_prob_1
	FROM citeam.cust_fcast_weekly_base
	--LEFT JOIN #skyplus 	AS b ON a.account_number = b.account_number 
	--LEFT JOIN #nowtv	AS c ON a.account_number = c.account_number 
	WHERE bb_active = 1 AND dtv_active = 1 
		--	AND (b.account_number IS NULL OR c.account_number IS NULL )
			AND BB_latest_act_dt IS NOT NULL
	GROUP BY account_number

	COMMIT WORK
	CREATE hg INDEX id1 ON #mysky_prob_1 (account_number)
	COMMIT WORK

	UPDATE #mysky_prob_1 AS a
	SET a.segment_TP = d.segment
	FROM #mysky_prob_1 AS a
	INNER JOIN DTV_fcast_weekly_base AS b ON a.account_number = b.account_number AND b.end_Date = @mx_dt
	LEFT JOIN BB_TP_Product_Churn_segments_lookup AS d  ON b.DTV_TA_calls_1m 	= d.DTV_TA_calls_1m
													 AND b.RTM 					= d.RTM 
													 AND b.my_sky_login_3m 		= d.my_sky_login_3m
													 AND b.Talk_tenure 			= d.Talk_tenure
													 AND b.Simple_Segment 		= d.Simple_Segment
													 AND b.BB_all_calls_1m 		= d.BB_all_calls_1m 

	COMMIT WORK

	SELECT 'Prob_Group' = 1
		, Calls_LW
		, segment_TP
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_TP)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_TP ORDER BY Calls_LW ASC)
	INTO #mysky_prob_2
	FROM #mysky_prob_1
	WHERE L_12 = 0
	GROUP BY Calls_LW
		, segment_TP
	
	UNION
	
	SELECT 'Prob_Group' = 2
		, Calls_LW
		, segment_TP
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_TP)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_TP ORDER BY Calls_LW ASC)
	FROM #mysky_prob_1
	WHERE L_9 = 0
	GROUP BY Calls_LW
		, segment_TP
	
	UNION
	
	SELECT 'Prob_Group' = 3
		, Calls_LW
		, segment_TP
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_TP)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_TP ORDER BY Calls_LW ASC)
	FROM #mysky_prob_1
	WHERE L_6 = 0
	GROUP BY Calls_LW
		, segment_TP
	
	UNION
	
	SELECT 'Prob_Group' = 4
		, Calls_LW
		, segment_TP
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_TP)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_TP ORDER BY Calls_LW ASC)
	FROM #mysky_prob_1
	WHERE L_3 = 0
	GROUP BY Calls_LW
		, segment_TP
	
	UNION
	
	SELECT 'Prob_Group' = 5
		, Calls_LW
		, segment_TP
		, 'hits' 		= COUNT(1)
		, 't_segment' 	= SUM(hits) OVER (PARTITION BY segment_TP)
		, 'prob' 		= CASE WHEN t_segment > 0 THEN convert(REAL, hits) / convert(REAL, t_segment) ELSE 0 END
		, 'rank_id' 	= RANK() OVER (PARTITION BY segment_TP ORDER BY Calls_LW ASC )
	FROM #mysky_prob_1
	WHERE Prob_Group = 5
	GROUP BY Calls_LW
		, segment_TP

	SELECT *
		, 'UPPER_LIMIT' = SUM(prob) OVER ( PARTITION BY Prob_Group , segment_TP ORDER BY RANK_ID ASC ) 
	INTO #mysky_prob_3
	FROM #mysky_prob_2

	SELECT a.Prob_Group
		, a.Calls_LW
		, a.segment_TP
		, 'Lower_limit' = COALESCE(b.UPPER_LIMIT, 0)
		, a.UPPER_LIMIT
	INTO TP_my_sky_login_prob_TABLE
	FROM #mysky_prob_3 AS a
	LEFT JOIN #mysky_prob_3 AS b ON a.segment_TP = b.segment_TP AND a.Prob_Group = b.Prob_Group AND a.rank_id - 1 = b.rank_id 
	
	message convert(TIMESTAMP, now()) || ' | TP_my_sky_login_prob_TABLE - COMPLETED: ' || @@rowcount TO client

	COMMIT 
	CREATE lf INDEX id1 ON TP_my_sky_login_prob_TABLE (Prob_Group)
	CREATE lf INDEX id2 ON TP_my_sky_login_prob_TABLE (Calls_LW)
	CREATE hg INDEX id3 ON TP_my_sky_login_prob_TABLE (Lower_limit)
	CREATE hg INDEX id4 ON TP_my_sky_login_prob_TABLE (UPPER_LIMIT)
	
	GRANT SELECT ON TP_my_sky_login_prob_TABLE TO citeam , vespa_group_low_security

	COMMIT WORK message convert(TIMESTAMP, now()) || ' | TP_my_sky_login_prob - COMPLETED ' TO client
END
GO

