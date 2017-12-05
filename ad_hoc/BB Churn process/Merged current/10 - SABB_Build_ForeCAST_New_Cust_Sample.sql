
CREATE OR REPLACE PROCEDURE SABB_Build_ForeCAST_New_Cust_Sample (IN LV INT)

BEGIN
	DECLARE Obs_Dt DATE;
	DECLARE @multiplier BIGINT;

	SET @multiplier = DATEPART(millisecond, now()) + 738;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample - Build_ForeCAST_New_Cust_Sample -  Begin ' TO client;

	TRUNCATE TABLE FORECAST_New_Cust_Sample;

	SET Obs_Dt = (SELECT max(calendar_date) FROM subs_calendar(LV / 100 - 1, LV / 100) WHERE Subs_Week_And_Year < LV );
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	INSERT INTO FORECAST_New_Cust_Sample
	SELECT end_date
		, subs_year AS year
		, subs_week_of_year AS week
		, subs_week_and_year AS year_week
		, account_number
		, BB_status_code
		, CASE WHEN BB_Active > 0 THEN 'BB' ELSE 'Non BB' END AS BB_Segment
		, CASE WHEN ROI > 0 THEN 'ROI' ELSE 'UK' END AS country
		, BB_package
		, CASE 	WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI' 
				WHEN BB_Enter_SysCan > 0 THEN 'SysCan' 
				WHEN BB_Enter_CusCan > 0 THEN 'CusCan' 
				WHEN BB_Enter_HM > 0 THEN 'HM' 
				WHEN BB_Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
		, cast(NULL AS VARCHAR(4)) AS BB_Status_Code_EoW
		, CASE 	WHEN trim(simple_segment) IN ('1 Secure') THEN '1 Secure' 
				WHEN trim(simple_segment) IN ('2 Start') THEN '2 Start' 
				WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
				WHEN trim(simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
				WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
				WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
				ELSE 'Other/Unknown' END AS Simple_Segments
		, BB_offer_rem_and_end_raw
		, cast(NULL AS INT) AS BB_offer_rem_and_end
		, BB_tenure_raw
		, cast(NULL AS INT) AS BB_tenure
		, my_sky_login_3m_raw
		, cast(NULL AS INT) AS my_sky_login_3m
		, talk_type
		, home_owner_status
		, BB_all_calls_1m_raw
		, cast(NULL AS INT) AS BB_all_calls_1m
		, cast(0 AS TINYINT) AS node_SA
		, cast(NULL AS VARCHAR(20)) AS segment_SA
		, cast(NULL AS DATE) AS PL_Future_Sub_Effective_Dt
		, cast(NULL AS VARCHAR(100)) AS DTV_Activation_Type
		, Curr_Offer_start_Date_BB
		, curr_offer_end_date_Intended_BB
		, Prev_offer_end_date_BB
		, cast(NULL AS DATE) AS Future_offer_Start_dt
		, cast(NULL AS DATE) AS Future_end_Start_dt
		, BB_latest_act_dt
		, BB_first_act_dt
		, rand(number() * @multiplier) AS rand_sample
		, cast(NULL AS VARCHAR(10)) AS sample
		, CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END AS SABB_flag
		, CAST (0 AS BIT) AS Sky_plus  	
		, CAST (0 AS BIT) AS nowtv_flag 
	FROM citeam.CUST_Fcast_Weekly_Base
	WHERE end_date BETWEEN Obs_Dt - 5 * 7 AND Obs_Dt 
			AND bb_active = 1 
			AND dtv_active = 0 
			AND BB_latest_act_dt BETWEEN (end_date - 6) 
			AND end_date 
			AND BB_latest_act_dt IS NOT NULL;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  Insert Into FORECAST_New_Cust_Sample completed: ' || @@rowcount TO client;

-----------------------------------------************************------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------	

-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
	COMMIT WORK;

	SELECT a.account_number
		, a.end_date
		, B.subs_year
		, B.subs_week_of_year
		, CASE WHEN b.Enter_SysCan > 0 THEN 'SysCan' WHEN b.Enter_CusCan > 0 THEN 'CusCan' WHEN b.Enter_HM > 0 THEN 'HM' WHEN b.Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END AS Churn_type
		, RANK() OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC) AS week_rnk
	INTO #t1
	FROM FORECAST_New_Cust_Sample AS a
	INNER JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND a.year = b.subs_year AND a.week = b.subs_week_of_year
	WHERE a.Churn_type = 'MULTI';

	COMMIT WORK;

	DELETE FROM #t1 WHERE week_rnk > 1;
	CREATE hg INDEX IO1 ON #t1 (account_number);
	CREATE DTTM INDEX IO2 ON #t1 (end_date);
	COMMIT WORK;

	UPDATE FORECAST_New_Cust_Sample AS a
	SET a.Churn_type = b.Churn_type
	FROM FORECAST_New_Cust_Sample AS a
	INNER JOIN #t1 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date;

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  Churn_type fixed: ' || @@rowcount TO client;

	DROP TABLE #t1;

	COMMIT WORK;

	UPDATE FORECAST_New_Cust_Sample AS a
	SET a.BB_offer_rem_and_end = b.BB_offer_rem_and_end
		, a.BB_tenure = b.BB_tenure
		, a.my_sky_login_3m = b.my_sky_login_3m
		, a.BB_all_calls_1m = b.BB_all_calls_1m
		, a.talk_type  = b.talk_type 
	FROM FORECAST_New_Cust_Sample AS a
	INNER JOIN DTV_Fcast_Weekly_Base AS b ON a.account_number = b.account_number AND a.end_date = b.end_date;

	UPDATE FORECAST_New_Cust_Sample AS a
	SET a.node_SA = CAST(CASE 	WHEN node IN (22, 46, 49, 70, 75, 71) THEN 1
													WHEN node IN ( 83, 53, 43, 82, 73, 57) THEN 2
													WHEN node IN ( 63, 47, 68, 42, 62, 12, 39, 11, 35) THEN 3
													WHEN node IN ( 21, 74, 72) THEN 4
													WHEN node IN ( 40, 36, 66, 60, 65) THEN 5
													WHEN node IN ( 77, 31, 84, 56, 76) THEN 6
													WHEN node IN ( 10, 41, 67) THEN 7
													WHEN node IN ( 61, 51, 64, 24, 50) THEN 8
													WHEN node IN ( 27, 55, 85, 81, 79, 80, 54) THEN 9
													WHEN node IN ( 9) THEN 10
													ELSE 0 END AS VARCHAR(4))		
		, a.segment_SA = c.segment
	FROM FORECAST_New_Cust_Sample AS a
	LEFT JOIN BB_SABB_Churn_segments_lookup AS c  ON a.BB_offer_rem_and_end = c.BB_offer_rem_and_end
												AND a.BB_tenure 			= c.BB_tenure 
												AND a.my_sky_login_3m 		= c.my_sky_login_3m
												AND a.talk_type 			= c.talk_type
												AND a.home_owner_status 	= c.home_owner_status
												AND a.BB_all_calls_1m 		= c.BB_all_calls_1m;
		
	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  DTV_fcast variables updated: ' || @@rowcount TO client;

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET sample.PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC';

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'AB';

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM FORECAST_New_Cust_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
			AND MoR.AB_Future_Sub_Effective_Dt > sample.end_date 
			AND MoR.event_dt <= sample.end_date 
			AND (MoR.AB_effective_to_dt > sample.end_date OR MoR.AB_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ';

	UPDATE FORECAST_New_Cust_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE PL_Future_Sub_Effective_Dt IS NULL;
	
	
			
------------==================================++++++++++++++++++++++++++++++++==========================================---------------
------------==================================Sky+ and Now tv updates +++++++==========================================---------------

	message convert(TIMESTAMP, now()) || ' | SABB_Build_ForeCAST_New_Cust_Sample - Sky+ and NowTV updates: begin' TO client;
		SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        citeam.nowtv_accounts_ents AS csav
	JOIN 		FORECAST_New_Cust_Sample AS a ON a.account_number= csav.account_number
	WHERE       end_date BETWEEN  period_start_date AND period_end_date  ;
	
	COMMIT ;
	CREATE HG INDEX id1 ON #nowtv(account_number) ;
	COMMIT;
	
	UPDATE FORECAST_New_Cust_Sample
	SET nowtv_flag = 1 
	FROM FORECAST_New_Cust_Sample 	AS a 
	JOIN #nowtv					AS b ON a.account_number = b.account_number ;
	COMMIT ;
	DROP TABLE #nowtv;
	DELETE FROM FORECAST_New_Cust_Sample WHERE nowtv_flag = 1 ;
	
	----------------------------------------------------------------------------------------
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   FORECAST_New_Cust_Sample AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	end_date BETWEEN effective_from_dt AND effective_to_dt ;
	COMMIT ;
	CREATE HG INDEX id1 ON #skyplus (account_number);
	COMMIT ;
	
	UPDATE FORECAST_New_Cust_Sample
	SET Sky_plus = 1 
	FROM FORECAST_New_Cust_Sample 	AS a 
	JOIN #skyplus					AS b ON a.account_number = b.account_number ;
	COMMIT ;
	DROP TABLE #skyplus;
	
	DELETE FROM FORECAST_New_Cust_Sample WHERE Sky_plus = 1 ;
	-----------------------------------------
	
	COMMIT;
	message convert(TIMESTAMP, now()) || ' | SABB_Build_ForeCAST_New_Cust_Sample - Sky+ and NowTV updates completed' TO client;
	

	message cast(now() AS TIMESTAMP) || ' | SABB_Build_ForeCAST_New_Cust_Sample -  COMPLETED' TO client
END
GO


