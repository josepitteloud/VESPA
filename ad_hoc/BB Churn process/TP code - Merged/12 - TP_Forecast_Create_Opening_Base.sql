CREATE OR REPLACE PROCEDURE TP_Forecast_Create_Opening_Base (
	@Forecast_Start_Wk INT
	, @sample_pct REAL
	)
AS
BEGIN
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Begining - Initialising Environment' TO client

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
			, TP_flag BIT NOT NULL DEFAULT 0
			) 
			
	message convert (TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - TP_FORECAST_Base_Sample' TO client
	END

	DECLARE @base_date DATE
	DECLARE @true_sample_rate REAL
	DECLARE @multiplier BIGINT

	SET @multiplier = DATEPART(millisecond, now()) + 738
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0

	SELECT * INTO #Sky_Calendar FROM Subs_Calendar(@Forecast_Start_Wk / 100 - 1, @Forecast_Start_Wk / 100)

	SET @base_date = ( SELECT max(calendar_date - 7) FROM #sky_calendar WHERE subs_week_and_year = @Forecast_Start_Wk )
	SET @multiplier = DATEPART(millisecond, now()) + 1 message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - @base_date: ' || @base_date TO client

	-- drop table if exists #base_sample
	DELETE FROM TP_FORECAST_Base_Sample 
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Cleaning TP_FORECAST_Base_Sample ' TO client

	INSERT INTO TP_FORECAST_Base_Sample
	SELECT account_number
		, end_date
		, subs_year
		, 'subs_week_and_year' = convert(INT, subs_week_and_year)
		, subs_week_of_year
		, 'weekid' = (subs_year - 2010) * 52 + subs_week_of_year
		, BB_Status_Code
		, 'BB_Segment' = CASE WHEN BB_Active > 0 THEN 'BB' ELSE 'Non BB' END
		, 'country' = CASE WHEN ROI > 0 THEN 'ROI' ELSE 'UK' END
		, BB_package
		, 'Churn_type' = CASE WHEN BB_Enter_SysCan + BB_Enter_CusCan + BB_Enter_HM + BB_Enter_3rd_Party > 1 THEN 'MULTI' --- UPDATED next
			WHEN BB_Enter_SysCan > 0 THEN 'SysCan' WHEN BB_Enter_CusCan > 0 THEN 'CusCan' WHEN BB_Enter_HM > 0 THEN 'HM' WHEN BB_Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END
		, DTV_TA_calls_1m_raw
		, 'DTV_TA_calls_1m' = convert(INT, NULL)
		, RTM
		, Talk_tenure_raw
		, 'Talk_tenure' = convert(INT, NULL)
		, my_sky_login_3m_raw
		, 'my_sky_login_3m' = convert(INT, NULL)
		, BB_all_calls_1m_raw
		, 'BB_all_calls_1m' = convert(INT, NULL)
		, 'Simple_Segments' = CASE 	WHEN trim(simple_segment) IN ('1 Secure') THEN '1 Secure' 
									WHEN trim(simple_segment) IN ('2 Start') THEN '2 Start' 
									WHEN trim(simple_segment) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
									WHEN trim(simple_segment) IN ('4 Support', '3 Support') THEN '4 Support' 
									WHEN trim(simple_segment) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
									WHEN trim(simple_segment) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
									ELSE 'Other/Unknown' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
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
		, 'TP_flag' = CASE WHEN bb_active = 1 AND dtv_active = 1 THEN 1 ELSE 0 END
	FROM jcartwright.cust_fcast_weekly_base_2
	WHERE end_date = @base_date AND bb_active = 1 AND dtv_active = 1 --??? do we need a TP flag?
		AND BB_latest_act_dt IS NOT NULL --??? do we have this, or a bb_act_date?
		--???? changes to the where clause here?
		
		
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Insert Into TP_FORECAST_Base_Sample completed: ' || @@rowcount TO client

	COMMIT WORK

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
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - Missing churn_types for pipeline entries filled with defaults: ' || @@rowcount TO client

	UPDATE TP_FORECAST_Base_Sample AS a
	SET a.DTV_TA_calls_1m = b.DTV_TA_calls_1m
		, a.Talk_tenure = b.Talk_tenure
		, a.my_sky_login_3m = b.my_sky_login_3m
		, a.BB_all_calls_1m = b.BB_all_calls_1m
		, a.node_TP = b.node_TP
		, a.segment_TP = b.segment_TP
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN jcartwright.DTV_FCAST_WEEKLY_BASE_2 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - First Update TP_FORECAST_Base_Sample completed: ' || @@rowcount TO client

	---????update this?
	UPDATE TP_FORECAST_Base_Sample AS sample
	SET PL_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
	FROM TP_FORECAST_Base_Sample AS sample
	INNER JOIN CITeam.Broadband_Comms_Pipeline AS MoR ON MoR.account_number = sample.account_number 
												AND MoR.PC_Future_Sub_Effective_Dt > sample.end_date 
												AND MoR.event_dt <= sample.end_date 
												AND (MoR.PC_effective_to_dt > sample.end_date OR MoR.PC_effective_to_dt IS NULL)
	WHERE sample.BB_Status_Code = 'PC' AND sample.PL_Future_Sub_Effective_Dt IS NULL 
	
	
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - PC future sub effective dt set ' || @@rowcount TO client
	----------------========================****************============================----------------------
	----------------==================   PL_effective date FIX for PC   ================----------------------
	----------------========================****************============================----------------------
	
	UPDATE TP_FORECAST_Base_Sample AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, base.end_date + dur.Days_To_churn)
	FROM TP_FORECAST_Base_Sample AS base
	INNER JOIN DTV_PC_Duration_Dist AS dur ON RAND(CAST(account_number AS INT) * rand_sample) BETWEEN dur.PC_Days_Lower_Prcntl AND dur.PC_Days_Upper_Prcntl
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
		message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base - UPDATE AB_Future_Sub_Effective_Dt Begin' TO client

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

	DELETE FROM #AB_BCRQ_3 WHERE rankk > 1 

	CREATE hg INDEX id1 ON #AB_BCRQ_3 (account_number)

	UPDATE TP_FORECAST_Base_Sample AS a
	SET PL_Future_Sub_Effective_Dt = next_cancel_dt
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN #AB_BCRQ_3 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date

	DROP TABLE #AB_BCRQ_3 message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 1/2' TO client

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

	DELETE FROM #AB_2 WHERE rankk > 1

	SELECT *
		, 'next_cancel_dt' = CASE 	WHEN randx <= .25 THEN DATEADD(day, 15, event_dt) 
									WHEN randx BETWEEN .25 AND .79 THEN DATEADD(day, 60, event_dt) 
									WHEN randx >= .79 THEN DATEADD(day, 65, event_dt) ELSE event_dt END
	INTO #AB_3
	FROM #AB_2

	
	DROP TABLE #AB_2
	
	UPDATE TP_FORECAST_Base_Sample AS a
	SET PL_Future_Sub_Effective_Dt = next_cancel_dt
	FROM TP_FORECAST_Base_Sample AS a
	INNER JOIN #AB_3 AS b ON a.account_number = b.account_number AND a.end_date = b.end_date message convert(TIMESTAMP, now()) || ' | TP_Forecast_Create_Opening_Base -  UPDATE AB_Future_Sub_Effective_Dt checkpoint 2/2' TO client

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
		
	message convert(TIMESTAMP, now()) || ' | Forecast_Create_Opening_Base - BCRQ future sub effective dt set ' || @@rowcount TO client

	UPDATE TP_FORECAST_Base_Sample AS sample
	SET BB_Status_Code = 'AC'
	WHERE BB_Status_Code = 'BCRQ' AND PL_Future_Sub_Effective_Dt IS NULL

	--sample to speed up processing
	UPDATE TP_FORECAST_Base_Sample
	SET sample = CASE WHEN rand_sample < @sample_pct THEN 'A' ELSE 'B' END
		
END

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON TP_Forecast_Create_Opening_Base TO CITeam , vespa_group_low_security
GO



