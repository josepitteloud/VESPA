CREATE OR REPLACE PROCEDURE AB_Status_Movement_Probabilities 
			(IN @ForeCAST_Start_Week INT) 
	result (
		Churn_type VARCHAR(10)
	, Status_Code VARCHAR(4)
	, Wks_To_Intended_Churn VARCHAR(20)
	, Status_Code_EoW VARCHAR(4)
	, Status_Code_EoW_Rnk INT
	, ReAC_Offer_Applied TINYINT
	, Cnt INT
	, Cum_Total_Cohort_ABs INT
	, Total_Cohort_ABs INT
	, Percentile_Lower_Bound REAL
	, Percentile_Upper_Bound REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - Initialization Begin' TO client;

	SELECT * INTO #Sky_Calendar FROM /*CITeam.*/Subs_Calendar(@ForeCAST_Start_Week / 100 - 1, @ForeCAST_Start_Week / 100);
	
	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	SET @Lw6dt = (SELECT min(calendar_date - 6 * 7) FROM #sky_calendar WHERE subs_week_and_year = @ForeCAST_Start_Week) ;
	SET @Hw6dt = (SELECT min(calendar_date - 1) 	FROM #sky_calendar WHERE subs_week_and_year = @ForeCAST_Start_Week) ;
	
	SELECT mor.account_number
		, mor.status_code
		, mor.event_dt
		, mor.AB_Future_Sub_Effective_Dt
		, cast(mor.AB_Future_Sub_Effective_Dt - datepart(weekday, mor.AB_Future_Sub_Effective_Dt + 2) + 7 AS DATE) AS AB_Future_Sub_Effective_Dt_End_Dt
		, mor.AB_Effective_To_Dt
		, mor.AB_Next_status_code AS Next_status_code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS AB_ReAC_Offer_Applied
		, CASE 	WHEN mor.Enter_SysCan > 0 THEN 'SysCan' 
				WHEN mor.Enter_CusCan > 0 THEN 'CusCan' 
				WHEN mor.Enter_HM > 0 THEN 'HM' 
				WHEN mor.Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #AB_Intended_Churn
	FROM citeam.Broadband_Comms_Pipeline AS mor
	LEFT JOIN citeam.Offers_Software AS oua ON oua.account_number = mor.account_number 
									AND oua.offer_leg_start_dt_Actual = MoR.AB_Effective_To_Dt 
									AND MoR.AB_Next_Status_Code = 'AC' 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
									AND oua.subs_type = 'Broadband DSL Line'
	JOIN (	SELECT DISTINCT account_number FROM citeam.CUST_Fcast_Weekly_Base	
			WHERE end_date BETWEEN @Lw6dt AND @Hw6dt
			AND DTV_active = 0 AND bb_active = 1 ) AS y ON y.account_number = MoR.account_number
	WHERE AB_Future_Sub_Effective_Dt BETWEEN @Lw6dt AND @Hw6dt
			AND AB_Future_Sub_Effective_Dt IS NOT NULL 
			AND AB_Next_status_code IS NOT NULL 
			AND AB_Effective_To_Dt <= AB_Future_Sub_Effective_Dt 
			AND (status_code = 'AB' OR (status_code = 'BCRQ' AND Churn_type = 'SysCan'));

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts Begin' TO client;

	--------------------------------------------------------------------------------------------------------------------------------------------		
	
				
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   #AB_Intended_Churn AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	a.status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	@Hw6dt BETWEEN effective_from_dt AND effective_to_dt ;
		
	
					
	DELETE FROM #AB_Intended_Churn
	WHERE account_number IN (SELECT account_number FROM #skyplus);
	DROP TABLE #skyplus;
	--------------------------------------------------------------------------------------------------------------------------------------------		
	--------------------------------------------------------------------------------------------------------------------------------------------		
	SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        citeam.nowtv_accounts_ents AS csav
	JOIN 		#AB_Intended_Churn AS a ON a.account_number= csav.account_number
	WHERE       @Hw6dt BETWEEN period_start_date AND period_end_date;
						
	DELETE FROM #AB_Intended_Churn
	WHERE account_number IN (SELECT account_number FROM #nowtv);
	DROP TABLE #nowtv;
	--------------------------------------------------------------------------------------------------------------------------------------------

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - Index created' TO client;

	SELECT a.account_number
		, a.event_dt
		, b.status_code AS next_cancel_status
		, b.effective_from_dt AS next_cancel_dt
		, RANK() OVER (PARTITION BY a.account_number ORDER BY b.effective_from_dt ASC, b.cb_row_id ASC) AS rankk
	INTO #AB_BCRQ
	FROM #AB_Intended_Churn AS a
	INNER JOIN cust_subs_hist AS b ON a.account_number = b.account_number AND a.AB_Effective_To_Dt <= b.effective_from_dt
	WHERE b.subscription_sub_type = 'Broadband DSL Line' AND b.status_code_changed = 'Y' AND b.status_code IN ('PO', 'SC', 'CN') 
		AND a.Next_status_code IN ('BCRQ') 
		AND b.effective_from_dt <> b.effective_to_dt 
		AND b.prev_status_code IN ('BCRQ');

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 1/2' TO client;

	DELETE FROM #AB_BCRQ WHERE rankk > 1;

	UPDATE #AB_Intended_Churn AS a
	SET AB_Future_Sub_Effective_Dt = DATEADD(day, 65, a.event_dt) -- next_cancel_dt
		, Next_status_code = next_cancel_status
	FROM #AB_Intended_Churn AS a
	INNER JOIN #AB_BCRQ AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt AND a.status_code = 'AB';

	DROP TABLE #AB_BCRQ;

	UPDATE #AB_Intended_Churn
	SET Next_status_code = 'CN'
	WHERE Next_status_code = 'BCRQ';

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 2/2' TO client;

	SELECT AB_s.*
		, CASE 	WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 5 THEN 'Churn in next 6 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 6 THEN 'Churn in next 7 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 7 THEN 'Churn in next 8 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 8 THEN 'Churn in next 9 wks' 
				WHEN (cast(AB_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 >= 9 THEN 'Churn in next 10+ wks' END AS Wks_To_Intended_Churn
		, sc.Calendar_date AS End_date
		, CASE 	WHEN sc.calendar_date + 7 BETWEEN event_dt AND AB_Effective_To_Dt THEN 'AB' 
				WHEN sc.calendar_date + 7 BETWEEN AB_Effective_To_Dt AND AB_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END AS Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 = AB_Effective_To_Dt - datepart(weekday, AB_Effective_To_Dt + 2) + 7 AND Status_Code_EoW = 'AC' THEN AB_s.AB_ReAC_Offer_Applied ELSE 0 END AS AB_ReAC_Offer_Applied_EoW
		, (CASE WHEN Status_Code_EoW = 'AC' AND AB_ReAC_Offer_Applied = 0 THEN 1 
				WHEN Status_Code_EoW = 'AC' AND AB_ReAC_Offer_Applied = 1 THEN 2 
				WHEN Status_Code_EoW = 'CN' THEN 3 
				WHEN Status_Code_EoW = 'BCRQ' THEN 4 
				WHEN Status_Code_EoW = 'PC' THEN 5 
				WHEN Status_Code_EoW = 'PO' THEN 6 
				WHEN Status_Code_EoW = 'SC' THEN 7 ELSE 0 END) AS Status_Code_EoW_Rnk
	INTO #AB_PL_Status
	FROM #AB_Intended_Churn AS AB_s
	INNER JOIN #sky_calendar AS sc ON sc.calendar_date BETWEEN AB_s.event_dt AND AB_s.AB_Effective_To_Dt - 1 AND sc.subs_last_day_of_week = 'Y';

	SELECT Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, AB_ReAC_Offer_Applied_EoW
		, count() AS AB_s
		, Sum(AB_s) OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type ORDER BY Status_Code_EoW_Rnk ASC) AS Cum_Total_Cohort_ABs
		, Sum(AB_s) OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type) AS Total_Cohort_ABs
		, cast(NULL AS REAL) AS AB_Percentile_Lower_Bound
		, cast(Cum_Total_Cohort_ABs AS REAL) / Total_Cohort_ABs AS AB_Percentile_Upper_Bound
		, Row_Number() OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type ORDER BY Status_Code_EoW_Rnk ASC) AS Row_ID
		, Churn_type
		, status_code
	INTO #AB_Percentiles
	FROM #AB_PL_Status
	GROUP BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, AB_ReAC_Offer_Applied_EoW
		, Churn_type
		, status_code
	ORDER BY status_code ASC
		, Churn_type ASC
		, Wks_To_Intended_Churn ASC
		, Status_Code_EoW_Rnk ASC
		, Status_Code_EoW ASC
		, AB_ReAC_Offer_Applied_EoW ASC;

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - AB_Percentiles populated: ' || @@rowcount TO client;

	UPDATE #AB_Percentiles AS pcp
	SET AB_Percentile_Lower_Bound = cast(Coalesce(pcp2.AB_Percentile_Upper_Bound, 0) AS REAL)
	FROM #AB_Percentiles AS pcp
	LEFT JOIN #AB_Percentiles AS pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Row_ID = pcp.Row_ID - 1 AND pcp.Churn_type = pcp2.Churn_type;

	DROP VARIABLE @Lw6dt;
	DROP VARIABLE @Hw6dt;
	
	SELECT Churn_type
		, status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, AB_ReAC_Offer_Applied_EoW
		, AB_s
		, Cum_Total_Cohort_ABs
		, Total_Cohort_ABs
		, AB_Percentile_Lower_Bound
		, AB_Percentile_Upper_Bound
	FROM #AB_Percentiles;

	message cast(now() AS TIMESTAMP) || ' | AB_Status_Movement_Probabilities - Completed' TO client
END
GO

