CREATE OR REPLACE PROCEDURE TP_Forecast_Create_Opening_Base (
	@Forecast_Start_Wk INT
	, @sample_pct REAL
	)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - Begining - Initialising Environment' TO client

	IF NOT EXISTS (SELECT tname FROM syscatalog WHERE creator = user_name() AND tabletype = 'TABLE' AND upper(tname) = UPPER('TP_FORECAST_Base_Sample'))
	
	BEGIN
		CREATE TABLE TP_FORECAST_Base_Sample (
			  account_number VARCHAR(20) NULL DEFAULT NULL
			, end_date DATE NULL DEFAULT NULL
			, subs_year INT NULL DEFAULT NULL
			, subs_week_and_year INT NULL DEFAULT NULL
			, subs_week_of_year TINYINT NULL DEFAULT NULL
			
			, weekid BIGINT NULL DEFAULT NULL
			, BB_Status_Code VARCHAR(4) NULL DEFAULT NULL
			, BB_Segment VARCHAR(30) NULL DEFAULT NULL
			, country VARCHAR(3) NULL DEFAULT NULL
			, BB_package VARCHAR(50) NULL DEFAULT NULL
			
			, churn_type VARCHAR(10) NULL DEFAULT NULL
			, DTV_TA_calls_1m_raw INT NULL DEFAULT 0
			, DTV_TA_calls_1m INT NULL DEFAULT 0
			, RTM VARCHAR(30) NULL DEFAULT 'NONE'
			, Talk_tenure_raw INT NULL DEFAULT 0
			
			, Talk_tenure INT NULL DEFAULT 0
			, my_sky_login_3m_raw INT NULL DEFAULT 0
			, my_sky_login_3m INT NULL DEFAULT 0
			, BB_all_calls_1m_raw INT NULL DEFAULT 0
			, BB_all_calls_1m INT NULL DEFAULT 0
			
			, Simple_Segments VARCHAR(13) NULL DEFAULT 'UNKNOWN'
			, node_TP TINYINT NULL DEFAULT 0
			, segment_TP VARCHAR(20) NULL DEFAULT 'UNKNOWN'
			, PL_Future_Sub_Effective_Dt DATETIME NULL DEFAULT NULL
			, DTV_Activation_Type VARCHAR(100) NULL DEFAULT NULL
			
			, Curr_Offer_start_Date_BB DATETIME NULL DEFAULT NULL
			, Curr_offer_end_date_Intended_BB DATETIME NULL DEFAULT NULL
			, Prev_offer_end_date_BB DATETIME NULL DEFAULT NULL
			, Future_offer_Start_dt DATETIME NULL DEFAULT NULL
			, Future_end_Start_dt DATETIME NULL DEFAULT NULL
			
			, BB_latest_act_dt DATETIME NULL DEFAULT NULL
			, BB_first_act_dt DATETIME NULL DEFAULT NULL
			, rand_sample REAL NULL DEFAULT NULL
			, sample VARCHAR(10) NULL DEFAULT NULL
			, TP_flag 	BIT NOT NULL DEFAULT 0
			
			, Sky_plus  	BIT NOT NULL DEFAULT 0
			, nowtv_flag 	BIT NOT NULL DEFAULT 0
			) 
			
	message convert (TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - TP_FORECAST_Base_Sample' TO client
	END

	DECLARE @base_date DATE
	DECLARE @true_sample_rate REAL
	DECLARE @multiplier BIGINT

	SET @multiplier = DATEPART(millisecond, now()) + 738
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0

	SELECT * INTO #Sky_Calendar FROM Subs_Calendar(@Forecast_Start_Wk / 100 - 1, @Forecast_Start_Wk / 100)

	SET @base_date = ( SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = @Forecast_Start_Wk )
	SET @multiplier = DATEPART(millisecond, now()) + 1 message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - @base_date: ' || @base_date TO client

	-- drop table if exists #base_sample
	DELETE FROM TP_FORECAST_Base_Sample 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - Cleaning TP_FORECAST_Base_Sample '||@base_date TO client

	INSERT INTO TP_FORECAST_Base_Sample
	SELECT a.account_number
		, a.end_date
		, subs_year
		, 'subs_week_and_year' = convert(INT, subs_week_and_year)
		, subs_week_of_year
		
		, 'weekid' = (subs_year - 2010) * 52 + subs_week_of_year
		, BB_Status_Code
		, 'BB_Segment' = CASE WHEN BB_Active > 0 THEN 'BB' ELSE 'Non BB' END
		, 'country' = CASE WHEN ROI > 0 THEN 'ROI' ELSE 'UK' END
		, BB_package
		
		, CAST( NULL AS VARCHAR(20)) AS Churn_type
		, DTV_TA_calls_1m_raw
		, 'DTV_TA_calls_1m' = convert(INT, NULL)
		, CASE WHEN RTM IS NOT NULL THEN RTM ELSE 'UNKNOWN' END 
		, Talk_tenure_raw
		
		, 'Talk_tenure' = convert(INT, NULL)
		, y.visit 	AS  my_sky_login_3m_raw
		, y.mysky 	AS my_sky_login_3m
		, BB_all_calls_1m_raw
		, 'BB_all_calls_1m' = convert(INT, NULL)
		
		, 'Simple_Segments' = CASE 	WHEN trim(simple_segment) IN ('1 Secure') THEN '1 Secure' 
									WHEN trim(simple_segment) IN ('2 Start') THEN '2 Start' 
									WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
									WHEN trim(simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
									WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
									WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
									ELSE 'UNKNOWN' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
									END
		, 'node_TP' 		= convert(TINYINT, 0)
		, 'segment_TP' 		= convert(VARCHAR(20), NULL)
		, 'PL_Future_Sub_Effective_Dt' 	= convert(DATE, NULL)
		, 'DTV_Activation_Type' 		= convert(VARCHAR(100), NULL)
		
		, Curr_Offer_start_Date_BB
		, curr_offer_end_date_Intended_BB
		, Prev_offer_end_date_BB
		, 'Future_offer_Start_dt' = convert(DATE, NULL)
		, 'Future_end_Start_dt' = convert(DATE, NULL)
		
		, BB_latest_act_dt
		, BB_first_act_dt
		, 'rand_sample' = rand(number() * @multiplier)
		, 'sample' = convert(VARCHAR(10), NULL)
		, 'TP_flag' = CASE WHEN bb_active = 1 AND dtv_active = 0 THEN 1 ELSE 0 END
		
		, 0 -- Sky plus
		, 0 -- NowTV
	FROM citeam.CUST_Fcast_Weekly_Base AS a
	LEFT JOIN my_sky AS y ON a.account_number = y.account_number AND y.end_date = a.end_date
	WHERE a.end_date = @base_date 
		AND bb_active = 1 
		AND dtv_active = 1 --??? do we need a sabb flag?
		AND BB_latest_act_dt IS NOT NULL --??? do we have this, or a bb_act_date?
		--???? changes to the where clause here?
		
		
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - Insert Into TP_FORECAST_Base_Sample completed: ' || @@rowcount TO client

	COMMIT WORK
-----------------------------------------************************------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------	

	COMMIT WORK
	
	SELECT a.account_number
			, CASE 	WHEN enter_SysCan > 0 THEN 'SysCan' 
					WHEN Enter_CusCan > 0 THEN 'CusCan' 
					WHEN Enter_HM > 0 THEN 'HM' 
					WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
					ELSE NULL END AS Churn_type
			, a.subs_week_and_year
			, rank() OVER (PARTITION BY a.account_number, a.subs_week_and_year ORDER BY a.event_dt ASC) rankk
	INTO #tc
	FROM CITeam.Broadband_Comms_Pipeline AS a 
	JOIN TP_FORECAST_Base_Sample AS b ON a.account_number = b.account_number AND a.subs_week_and_year = b.subs_week_and_year
	
	DELETE FROM #tc WHERE rankk > 1
	
	UPDATE TP_FORECAST_Base_Sample
	SET a.Churn_type = b.Churn_type
	FROM TP_FORECAST_Base_Sample AS a 
	JOIN #tc AS b ON a.account_number = b.account_number AND a.subs_week_and_year = b.subs_week_and_year
	
	DROP TABLE #tc 



-----------------------------------------************************------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------	

		SELECT DISTINCT account_number, 1 'dummy'
		INTO 	#BCRQ
		FROM  	CUST_SUBS_HIST
		WHERE 	status_code = 'BCRQ' 
			AND	@base_date  BETWEEN effective_from_dt AND  effective_to_dt
			AND subscription_sub_type = 'Broadband DSL Line'
			AND status_code_changed = 'Y'
			AND effective_to_dt > effective_from_dt
		COMMIT 
		CREATE HG INDEX id1 ON #BCRQ(account_number) 
		COMMIT

	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - BCRQ 1: ' || @@rowcount TO client

		SELECT a.account_number
			, a.end_date
			, a.subs_year
			, 'subs_week_and_year' = convert(INT, a.subs_week_and_year)
			, a.subs_week_of_year
			, 'weekid' = (a.subs_year - 2010) * 52 + subs_week_of_year
			, a.BB_Status_Code
			, 'BB_Segment' = CASE WHEN a.BB_Active > 0 THEN 'BB' ELSE 'Non BB' END
			, 'country' = CASE WHEN a.ROI > 0 THEN 'ROI' ELSE 'UK' END
			, a.BB_package
			, 'Churn_type' = CASE WHEN a.BB_Enter_SysCan + a.BB_Enter_CusCan + a.BB_Enter_HM + a.BB_Enter_3rd_Party > 1 THEN 'MULTI' --- UPDATED next
				WHEN a.BB_Enter_SysCan > 0 THEN 'SysCan' WHEN a.BB_Enter_CusCan > 0 THEN 'CusCan' WHEN a.BB_Enter_HM > 0 THEN 'HM' WHEN a.BB_Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END
			, a.DTV_TA_calls_1m_raw
			, b.DTV_TA_calls_1m
			, CASE WHEN b.RTM IS NOT NULL THEN b.RTM ELSE 'UNKNOWN' END AS RTM
			, a.Talk_tenure_raw
			, b.Talk_tenure
			, y.visit 	AS my_sky_login_3m_raw
			, y.mysky	AS my_sky_login_3m
			, a.BB_all_calls_1m_raw
			, b.BB_all_calls_1m
			, 'Simple_Segments' = CASE 	WHEN trim(b.simple_segment) IN ('1 Secure') THEN '1 Secure' 
										WHEN trim(b.simple_segment) IN ('2 Start') THEN '2 Start' 
										WHEN trim(b.simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
										WHEN trim(b.simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
										WHEN trim(b.simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
										WHEN trim(b.simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
										ELSE 'UNKNOWN' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
										END
			, 'node_TP' 		= convert(TINYINT, 0)
			, 'segment_TP' 		= convert(VARCHAR(20), NULL)
			, 'PL_Future_Sub_Effective_Dt' 	= convert(DATE, NULL)
			, 'DTV_Activation_Type' 		= convert(VARCHAR(100), NULL)
			, a.Curr_Offer_start_Date_BB
			, a.curr_offer_end_date_Intended_BB
			, a.Prev_offer_end_date_BB
			, 'Future_offer_Start_dt' = convert(DATE, NULL)
			, 'Future_end_Start_dt' = convert(DATE, NULL)
			, a.BB_latest_act_dt
			, a.BB_first_act_dt
			, 'rand_sample' = rand(number() * @multiplier)
			, 'sample' = convert(VARCHAR(10), NULL)
			, 'TP_flag' = CASE WHEN a.bb_active = 1 AND a.dtv_active = 0 THEN 1 ELSE 0 END
			, RANK() OVER (PARTITION BY a.account_number ORDER BY a.end_date DESC ) AS rankk
		INTO #BCRQ_details
		FROM citeam.CUST_Fcast_Weekly_Base 	AS a
		JOIN #BCRQ									AS c ON a.account_number =c.account_number 
		JOIN citeam.DTV_Fcast_Weekly_Base AS b ON a.end_date = b.end_date AND a.account_number = b.account_number
		LEFT JOIN my_sky AS y ON a.account_number = y.account_number AND y.end_date = a.end_date
		WHERE a.end_date >= DATEADD ( MONTH, -3, @base_date) 
			AND a.BB_latest_act_dt IS NOT NULL 

			message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - BCRQ 2: ' || @@rowcount TO client
			
		COMMIT 
		CREATE HG index id1 ON #BCRQ_details(account_number)
		CREATE LF index id2 ON #BCRQ_details(rankk)
		COMMIT

		DELETE FROM #BCRQ_details WHERE rankk <> 1 
		COMMIT

		UPDATE #BCRQ_details SET end_date = @base_date
			, bb_status_code = 'BCRQ' 
		COMMIT 

		message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - BCRQ 3: ' || @@rowcount TO client
		
		SELECT end_date 
				, MAX(subs_year)								AS subs_year
				, MAX(convert(INT, subs_week_and_year))			AS subs_week_and_year
				, MAX(subs_week_of_year) 						AS subs_week_of_year
				, MAX(weekid) 									AS weekid
		INTO #t1x
		FROM TP_FORECAST_Base_Sample
		GROUP BY end_date 
		
		COMMIT
		

		UPDATE #BCRQ_details
		SET a.subs_year 			= b.subs_year
			, a.subs_week_and_year 	= b.subs_week_and_year 	
			, a.subs_week_of_year	= b.subs_week_of_year
			, a.weekid 				= b.weekid
		FROM #BCRQ_details 	AS a 
		JOIN #t1x 			AS b ON a.end_date = b.end_date

		COMMIT
		DELETE FROM #BCRQ_details WHERE account_number IN( SELECT distinct account_number FROM TP_FORECAST_Base_Sample) 
		
		INSERT INTO TP_FORECAST_Base_Sample
		SELECT 	account_number,	end_date,	subs_year,	subs_week_and_year,	subs_week_of_year
				,weekid	,BB_Status_Code	,BB_Segment	,country	,BB_package
				,churn_type,	DTV_TA_calls_1m_raw,	DTV_TA_calls_1m,	RTM,	Talk_tenure_raw
				,Talk_tenure,	my_sky_login_3m_raw,	my_sky_login_3m,	BB_all_calls_1m_raw, BB_all_calls_1m
				,Simple_Segments,	node_TP,	segment_TP,	PL_Future_Sub_Effective_Dt,	DTV_Activation_Type
				,Curr_Offer_start_Date_BB, Curr_offer_end_date_Intended_BB,	Prev_offer_end_date_BB,	Future_offer_Start_dt,	Future_end_Start_dt
				,BB_latest_act_dt, 	BB_first_act_dt,	rand_sample,	sample,	TP_flag
				,	0,0
		FROM #BCRQ_details --- todo: add deduping
		
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------
-----------------------------------------***********************-------------------------------------------------------

	SELECT a.account_number
		, a.end_date
		, a.subs_year
		, a.subs_week_of_year
		, 'Churn_type' = CASE WHEN b.Enter_SysCan > 0 THEN 'SysCan' WHEN b.Enter_CusCan > 0 THEN 'CusCan' WHEN b.Enter_HM > 0 THEN 'HM' WHEN b.Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END
		, 'week_rnk' = RANK() OVER (PARTITION BY a.account_number, a.end_date ORDER BY b.event_dt DESC, rand_sample ASC)
	INTO #t1
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN CITEAM.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND a.end_date >= Event_Dt
	WHERE (a.Churn_type = 'MULTI' OR (a.Churn_type IS NULL AND BB_Status_Code IN ('AB', 'PC', 'BCRQ')))

	COMMIT WORK

	DELETE	FROM #t1
	WHERE week_rnk > 1

	CREATE hg INDEX IO1 ON #t1 (account_number)

	CREATE DATE INDEX IO2 ON #t1 (end_date)

	COMMIT WORK

	UPDATE TP_FORECAST_Base_Sample AS a
	SET a.Churn_type = b.Churn_type
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN #t1 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date

	DROP TABLE #t1

	COMMIT WORK

	--		select * from TP_FORECAST_Base_Sample where bb_status_code='BCRQ'
	UPDATE TP_FORECAST_Base_Sample AS a
	SET a.Churn_type = CASE WHEN bb_status_code IN ('PC', 'BCRQ') THEN 'CusCan' ELSE 'SysCan' END
	FROM TP_FORECAST_Base_Sample AS a
	WHERE a.churn_type IS NULL AND bb_status_code IN ('PC', 'BCRQ', 'AB')

	COMMIT WORK 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - Missing churn_types for pipeline entries filled with defaults: ' || @@rowcount TO client

	UPDATE TP_FORECAST_Base_Sample AS a
	SET a.Talk_tenure = b.Talk_tenure
		, a.RTM 		= CASE WHEN b.RTM IS NOT NULL THEN b.RTM ELSE 'UNKNOWN' END 
--		, a.my_sky_login_3m = b.my_sky_login_3m
		, a.BB_all_calls_1m = b.BB_all_calls_1m
		, a.DTV_TA_calls_1m = b.DTV_TA_calls_1m
		, a.Simple_Segments = CASE WHEN b.Simple_Segment  = 'Other/Unknown' THEN 'UNKNOWN' ELSE  b.Simple_Segment END 
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN citeam.DTV_Fcast_Weekly_Base AS b ON a.account_number = b.account_number AND a.end_date = b.end_date
	
	UPDATE TP_FORECAST_Base_Sample AS a
	SET a.node_TP = sub_segment
		, a.segment_TP = c.segment
	FROM TP_FORECAST_Base_Sample AS a
	LEFT JOIN BB_TP_Product_Churn_segments_lookup AS c  ON a.Talk_tenure = c.Talk_tenure
												AND a.RTM 			= c.RTM 
												AND a.my_sky_login_3m 		= c.my_sky_login_3m
												AND a.Simple_Segments			= c.Simple_Segment
												AND a.DTV_TA_calls_1m 	= c.DTV_TA_calls_1m
												AND a.BB_all_calls_1m 		= c.BB_all_calls_1m
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - First Update TP_FORECAST_Base_Sample completed: ' || @@rowcount TO client

	---????update this?
	UPDATE TP_FORECAST_Base_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM TP_FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
												AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
												AND MoR.event_dt <= sample.end_date 
												AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC' AND sample.PL_Future_Sub_Effective_Dt IS NULL 
	
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - PC future sub effective dt set ' || @@rowcount TO client
	----------------========================****************============================----------------------
	----------------==================   PL_effective date FIX for PC   ================----------------------
	----------------========================****************============================----------------------
	
	UPDATE TP_FORECAST_Base_Sample AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, base.end_date + dur.Days_To_churn)
	FROM TP_FORECAST_Base_Sample AS base
	INNER JOIN TP_DTV_PC_Duration_Dist AS dur ON RAND(CAST(account_number AS INT) * rand_sample) BETWEEN dur.PC_Days_Lower_Prcntl AND dur.PC_Days_Upper_Prcntl
	WHERE  PL_Future_Sub_Effective_Dt  IS NULL
			AND base.BB_Status_Code = 'PC' 
		
	----------------========================****************============================----------------------
	----------------========================****************============================----------------------
	----------------========================****************============================----------------------
	
	UPDATE TP_FORECAST_Base_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE BB_Status_Code = 'PC' AND PL_Future_Sub_Effective_Dt IS NULL 
		---????update this?
		------------==================================++++++++++++++++++++++++++++++++==========================================---------------
		------------==================================++++++++++++++++++++++++++++++++==========================================---------------
		------------==================================++++++++++++++++++++++++++++++++==========================================---------------
		message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base - UPDATE AB_Future_Sub_Effective_Dt Begin' TO client

	SELECT a.account_number
		, b.event_dt
		, a.BB_status_code
		, a.end_date
		, 'rankk' = RANK() OVER (PARTITION BY a.account_number ORDER BY b.event_dt DESC)
	INTO #AB_BCRQ_2
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND b.event_dt <= a.end_date
	WHERE a.BB_status_code IN ('AB', 'BCRQ') 
		AND PL_Future_Sub_Effective_Dt IS NULL 
		AND b.enter_syscan = 1

	DELETE FROM #AB_BCRQ_2 WHERE rankk > 1 ---- LATEST PL

	SELECT a.account_number
		, a.event_dt
		, a.end_date
		, a.BB_status_code
		, 'next_cancel_status' = b.status_code
		, 'next_cancel_dt' = b.effective_from_dt
		, 'rankk' = RANK() OVER (PARTITION BY a.account_number ORDER BY b.effective_from_dt ASC, b.cb_row_id ASC)
	INTO #AB_BCRQ_3
	FROM #AB_BCRQ_2 AS a
	INNER JOIN cust_subs_hist AS b ON a.account_number = b.account_number AND a.event_dt <= b.effective_from_dt
	WHERE b.subscription_sub_type = 'Broadband DSL Line' 
			AND b.status_code_changed = 'Y' 
			AND b.effective_from_dt <> b.effective_to_dt 
			AND b.prev_status_code IN ('BCRQ') 
			AND b.status_code IN ('CN', 'SC', 'PO')
			AND DATEDIFF (WEEK,  event_dt, next_cancel_dt) <= 12

	DELETE FROM #AB_BCRQ_3 WHERE rankk > 1 

	CREATE hg INDEX id1 ON #AB_BCRQ_3 (account_number)

	UPDATE TP_FORECAST_Base_Sample AS a
	SET PL_Future_Sub_Effective_Dt = next_cancel_dt
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN #AB_BCRQ_3 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date

	DROP TABLE #AB_BCRQ_3 
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 1/2' TO client

	----------------------------------------------------------------
	------------- Accounts in the pipeline 
	----------------------------------------------------------------
	SELECT a.account_number
		, a.end_date
		, b.event_dt
		, 'randx' = RAND(convert(INT, RIGHT(a.account_number, 6)) * DATEPART(ms, GETDATE()))
		, 'rankk' = RANK() OVER (PARTITION BY a.account_number ORDER BY b.event_dt DESC)
	INTO #AB_2
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS b ON a.account_number = b.account_number AND b.event_dt <= a.end_date
	WHERE PL_Future_Sub_Effective_Dt IS NULL 
		AND a.BB_status_code IN ('AB') 
		AND b.enter_syscan = 1

	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base -  AB check: '||@@rowcount TO client
	
	DELETE FROM #AB_2 WHERE rankk > 1

	SELECT *
		, 'next_cancel_dt' = DATEADD(day, 65, event_dt) 
	INTO #AB_3
	FROM #AB_2

	
	DROP TABLE #AB_2
	
	UPDATE TP_FORECAST_Base_Sample AS a
	SET PL_Future_Sub_Effective_Dt = next_cancel_dt
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN #AB_3 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 
	
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base -  AB check2 : '||@@rowcount TO client
	message convert(TIMESTAMP, now()) || ' | SABB_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 2/2' TO client
	DROP TABLE #AB_3
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	------------==================================++++++++++++++++++++++++++++++++==========================================---------------
	----------------------------------------
	UPDATE TP_FORECAST_Base_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.BCRQ_Future_Sub_Effective_Dt
	FROM TP_FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
													AND MoR.BCRQ_Future_Sub_Effective_Dt > sample.end_date 
													AND MoR.event_dt <= sample.end_date 
													AND (MoR.BCRQ_effective_to_dt > sample.end_date OR MoR.BCRQ_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'BCRQ' AND sample.PL_Future_Sub_Effective_Dt IS NULL
		
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - BCRQ future sub effective dt set ' || @@rowcount TO client

	UPDATE TP_FORECAST_Base_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE BB_Status_Code = 'BCRQ' AND PL_Future_Sub_Effective_Dt IS NULL

	--sample to speed up processing
	UPDATE TP_FORECAST_Base_Sample
	SET sample = CASE WHEN rand_sample < @sample_pct THEN 'A' ELSE 'B' END
		
------------==================================++++++++++++++++++++++++++++++++==========================================---------------
------------==================================Sky+ and Now tv updates +++++++==========================================---------------

	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - Sky+ and NowTV updates: begin' TO client
	/*
	SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        NOW_TV_SUBS_HIST AS csav
	JOIN 		TP_FORECAST_Base_Sample AS a ON a.account_number= csav.account_number
	WHERE       end_date BETWEEN effective_from_dt AND effective_to_dt 
	
	COMMIT 
	CREATE HG INDEX id1 ON #nowtv(account_number) 
	COMMIT
	
	UPDATE TP_FORECAST_Base_Sample
	SET nowtv_flag = 1 
	FROM TP_FORECAST_Base_Sample 	AS a 
	JOIN #nowtv					AS b ON a.account_number = b.account_number 
	COMMIT 
	DELETE FROM TP_FORECAST_Base_Sample WHERE nowtv_flag = 1 
	DROP TABLE #nowtv
	----------------------------------------------------------------------------------------
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   TP_FORECAST_Base_Sample AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	end_date BETWEEN effective_from_dt AND effective_to_dt 
	COMMIT 
	CREATE HG INDEX id1 ON #skyplus (account_number)
	COMMIT 
	
	UPDATE TP_FORECAST_Base_Sample
	SET Sky_plus = 1 
	FROM TP_FORECAST_Base_Sample 	AS a 
	JOIN #skyplus					AS b ON a.account_number = b.account_number 
	
	DELETE FROM TP_FORECAST_Base_Sample WHERE Sky_plus = 1 
	DROP TABLE #skyplus
*/
	
	COMMIT
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - Sky+ and NowTV updates completed' TO client
	
-- Grant execute rights to the members of CITeam
END
GRANT EXECUTE ON SABB_Forecast_Create_Opening_Base TO CITeam , vespa_group_low_security
GO



