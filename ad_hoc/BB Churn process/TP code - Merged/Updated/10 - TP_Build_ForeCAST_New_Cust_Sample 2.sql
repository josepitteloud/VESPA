CREATE OR REPLACE PROCEDURE TP_Build_ForeCAST_New_Cust_Sample (IN LV INT)

BEGIN
	DECLARE Obs_Dt DATE;
	DECLARE @multiplier BIGINT;

	SET @multiplier = DATEPART(millisecond, now()) + 738;

	message cast(now() AS TIMESTAMP) || ' | TP_Build_ForeCAST_New_Cust_Sample - Build_ForeCAST_New_Cust_Sample -  Begin ' TO client;

	TRUNCATE TABLE TP_FORECAST_New_Cust_Sample;

	SET Obs_Dt = (SELECT max(calendar_date) FROM subs_calendar(LV / 100 - 1, LV / 100) WHERE Subs_Week_And_Year < LV );
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	INSERT INTO TP_FORECAST_New_Cust_Sample
	SELECT 
		  a.account_number
		, a.end_date
		, a.subs_year AS year
		, a.subs_week_of_year AS week
		, a.subs_week_and_year AS year_week
		
		, a.BB_status_code
		, cast(NULL AS VARCHAR(4)) AS BB_Status_Code_EoW
		, CASE WHEN a.BB_Active > 0 THEN 'BB' ELSE 'Non BB' END AS BB_Segment
		, CASE WHEN a.ROI > 0 THEN 'ROI' ELSE 'UK' END AS country
		, a.BB_package
		
		, CAST( NULL AS VARCHAR(20)) AS Churn_type
		, DTV_TA_calls_1m_raw
		, DTV_TA_calls_1m
		, COALESCE(b.RTM, 'UNKNOWN') RTM
		, talk_tenure_raw
		
		, b.Talk_tenure  
		, y.visit 		AS my_sky_login_3m_raw
		, y.mysky		AS my_sky_login_3m
		, BB_all_calls_1m_raw
		, BB_all_calls_1m
		
		, CASE 	WHEN trim(b.simple_segment) IN ('1 Secure') THEN '1 Secure' 
				WHEN trim(b.simple_segment) IN ('2 Start') THEN '2 Start' 
				WHEN trim(b.simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
				WHEN trim(b.simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
				WHEN trim(b.simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
				WHEN trim(b.simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
				ELSE 'Other/Unknown' END AS Simple_Segments
		, cast(0 AS TINYINT) AS node_TP
		, cast(NULL AS VARCHAR(20)) AS segment_TP
		, cast(NULL AS DATE) AS PL_Future_Sub_Effective_Dt
		, cast(NULL AS VARCHAR(100)) AS DTV_Activation_Type
		
		, Curr_Offer_start_Date_BB
		, curr_offer_end_date_Intended_BB
		, Prev_offer_end_date_BB
		, cast(NULL AS DATE) AS Future_offer_Start_dt
		, cast(NULL AS DATE) AS Future_end_Start_dt
		
		, BB_first_act_dt
		, BB_latest_act_dt
		, rand(number() * @multiplier) AS rand_sample
		, cast(NULL AS VARCHAR(10)) AS sample
		, CASE WHEN a.bb_active = 1 AND a.dtv_active = 1 THEN 1 ELSE 0 END AS TP_flag
		
		, CAST (0 AS BIT) AS Sky_plus  	
		, CAST (0 AS BIT) AS nowtv_flag 
		
	FROM citeam.CUST_Fcast_Weekly_Base AS a 
	JOIN citeam.DTV_Fcast_Weekly_Base AS b ON a.end_date = b.end_date AND a.account_number = b.account_number
	LEFT JOIN my_sky AS y ON a.account_number = y.account_number AND y.end_date = a.end_date																												
	WHERE a.end_date BETWEEN Obs_Dt - 5 * 7 AND Obs_Dt 
			AND a.bb_active = 1 
			AND a.dtv_active = 1 
			AND BB_latest_act_dt BETWEEN (a.end_date - 6) AND a.end_date
			AND BB_latest_act_dt IS NOT NULL;

	message cast(now() AS TIMESTAMP) || ' | TP_Build_ForeCAST_New_Cust_Sample -  Insert Into TP_FORECAST_New_Cust_Sample completed: ' || @@rowcount TO client;

-----------------------------------------************************------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------	
								
								
								
	COMMIT WORK;
	
	SELECT a.account_number
			, CASE 	WHEN enter_SysCan > 0 THEN 'SysCan' 
					WHEN Enter_CusCan > 0 THEN 'CusCan' 
					WHEN Enter_HM > 0 THEN 'HM' 
					WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
					ELSE NULL END AS Churn_type
			, a.subs_week_and_year
			, rank() OVER (PARTITION BY a.account_number, a.subs_week_and_year ORDER BY event_dt ASC) rankk
	INTO #tc
	FROM CITeam.Broadband_Comms_Pipeline AS a 
	JOIN TP_FORECAST_New_Cust_Sample AS b ON a.account_number = b.account_number AND a.subs_week_and_year = CAST(b.year_week AS INT) ;
	
	DELETE FROM #tc WHERE rankk > 1;
	
	UPDATE TP_FORECAST_New_Cust_Sample
	SET a.Churn_type = b.Churn_type
	FROM TP_FORECAST_New_Cust_Sample AS a 
	JOIN #tc AS b ON a.account_number = b.account_number AND CAST(a.year_week AS INT)  = b.subs_week_and_year;
	
	DROP TABLE #tc ;
  
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
	
	
	SELECT a.account_number
		, a.end_date
		, B.subs_year
		, B.subs_week_of_year
		, CASE WHEN b.Enter_SysCan > 0 THEN 'SysCan' WHEN b.Enter_CusCan > 0 THEN 'CusCan' WHEN b.Enter_HM > 0 THEN 'HM' WHEN b.Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END AS Churn_type
		, RANK() OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC) AS week_rnk
	INTO #t1
	FROM TP_FORECAST_New_Cust_Sample AS a
	INNER JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND a.year = b.subs_year AND a.week = b.subs_week_of_year
	WHERE a.Churn_type = 'MULTI';

	COMMIT WORK;

	DELETE FROM #t1 WHERE week_rnk > 1;
	CREATE hg INDEX IO1 ON #t1 (account_number);
	CREATE DATE INDEX IO2 ON #t1 (end_date);
	COMMIT WORK;

	UPDATE TP_FORECAST_New_Cust_Sample AS a
	SET a.Churn_type = b.Churn_type
	FROM TP_FORECAST_New_Cust_Sample AS a
	INNER JOIN #t1 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date;

	message cast(now() AS TIMESTAMP) || ' | TP_Build_ForeCAST_New_Cust_Sample -  Churn_type fixed: ' || @@rowcount TO client;

	DROP TABLE #t1;

	COMMIT WORK;

	UPDATE TP_FORECAST_New_Cust_Sample AS a
	SET a.node_TP = Coalesce(sub_segment,0)
		, a.segment_TP = Coalesce(c.segment,'No Segment')
	FROM TP_FORECAST_New_Cust_Sample AS a
	LEFT JOIN BB_TP_Product_Churn_segments_lookup AS c  ON a.Talk_tenure = c.Talk_tenure
												AND a.RTM 			= c.RTM 
												AND a.my_sky_login_3m 		= c.my_sky_login_3m
												AND a.Simple_Segments			= c.Simple_Segment
												AND a.DTV_TA_calls_1m 	= c.DTV_TA_calls_1m
												AND a.BB_all_calls_1m 		= c.BB_all_calls_1m;
		
	message cast(now() AS TIMESTAMP) || ' | TP_Build_ForeCAST_New_Cust_Sample -  DTV_fcast variables updated: ' || @@rowcount TO client;

	UPDATE TP_FORECAST_New_Cust_Sample AS sample
	SET sample.PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM TP_FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC';

	UPDATE TP_FORECAST_New_Cust_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
	FROM TP_FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'AB';

	UPDATE TP_FORECAST_New_Cust_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM TP_FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ';

	UPDATE TP_FORECAST_New_Cust_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE PL_Future_Sub_Effective_Dt IS NULL;
	
	
			
------------==================================++++++++++++++++++++++++++++++++==========================================---------------
------------==================================Sky+ and Now tv updates +++++++==========================================---------------
/*
	message convert(TIMESTAMP, now()) || ' | TP_Build_ForeCAST_New_Cust_Sample - Sky+ and NowTV updates: begin' TO client;
		SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        NOW_TV_SUBS_HIST AS csav
	JOIN 		TP_FORECAST_New_Cust_Sample AS a ON a.account_number= csav.account_number
	WHERE       end_date BETWEEN effective_from_dt AND effective_to_dt ;
	
	COMMIT ;
	CREATE HG INDEX id1 ON #nowtv(account_number) ;
	COMMIT;
	
	UPDATE TP_FORECAST_New_Cust_Sample
	SET nowtv_flag = 1 
	FROM TP_FORECAST_New_Cust_Sample 	AS a 
	JOIN #nowtv					AS b ON a.account_number = b.account_number ;
	COMMIT ;
	DROP TABLE #nowtv;
	DELETE FROM TP_FORECAST_New_Cust_Sample WHERE nowtv_flag = 1 ;
	
	----------------------------------------------------------------------------------------
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   TP_FORECAST_New_Cust_Sample AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	end_date BETWEEN effective_from_dt AND effective_to_dt ;
	COMMIT ;
	CREATE HG INDEX id1 ON #skyplus (account_number);
	COMMIT ;
	
	UPDATE TP_FORECAST_New_Cust_Sample
	SET Sky_plus = 1 
	FROM TP_FORECAST_New_Cust_Sample 	AS a 
	JOIN #skyplus					AS b ON a.account_number = b.account_number ;
	COMMIT ;
	DROP TABLE #skyplus;
	
	DELETE FROM TP_FORECAST_New_Cust_Sample WHERE Sky_plus = 1 ;
	*/
	-----------------------------------------
	
	COMMIT;
	message convert(TIMESTAMP, now()) || ' | TP_Build_ForeCAST_New_Cust_Sample - Sky+ and NowTV updates completed' TO client;
 
  
message cast(now() AS TIMESTAMP) || ' | TP_Build_ForeCAST_New_Cust_Sample -  COMPLETED' TO client
END
GO


