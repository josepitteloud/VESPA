CREATE OR REPLACE PROCEDURE TP_PC_Status_Movement_Probabilities 
			(IN ForeCAST_Start_Week INT) 
	result (
		churn_type VARCHAR(10)
	, Initial_status_code VARCHAR(10)
	, Wks_To_Intended_Churn VARCHAR(20)
	, Status_Code_EoW VARCHAR(4)
	, Status_Code_EoW_Rnk INT
	, ReAC_Offer_Applied TINYINT
	, Cnt INT
	, Cum_Total_Cohort INT
	, Total_Cohort INT
	, Percentile_Lower_Bound REAL
	, Percentile_Upper_Bound REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | TP_PC_Status_Movement_Probabilities - Initialising Environment' TO client;

	SELECT * INTO #Sky_Calendar FROM CITeam.Subs_Calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	DECLARE @Lw6dt DATE ;
	DECLARE @Hw6dt DATE ;
	SET @L6w = (SELECT min(calendar_date - 6 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) ;
	SET @H6w =  (SELECT min(calendar_date - 1) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) ;
	
	SELECT MoR.account_number
		, MoR.status_code
		, MoR.event_dt
		, MoR.PC_Future_Sub_Effective_Dt
		, cast(MoR.PC_Future_Sub_Effective_Dt - datepart(weekday, MoR.PC_Future_Sub_Effective_Dt + 2) + 7 AS DATE) AS PC_Future_Sub_Effective_Dt_End_Dt
		, MoR.PC_Effective_To_Dt
		, MoR.PC_Next_status_code AS Next_status_code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS PC_ReAC_Offer_Applied
		, CASE 	WHEN MoR.Enter_SysCan > 0 THEN 'SysCan' 
				WHEN MoR.Enter_CusCan > 0 THEN 'CusCan' 
				WHEN MoR.Enter_HM > 0 THEN 'HM' 
				WHEN MoR.Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #PC_Intended_Churn
	FROM CITEAM.Broadband_Comms_Pipeline AS mor
	LEFT JOIN offer_usage_all AS oua ON oua.account_number = mor.account_number 
								AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt 
								AND MoR.PC_Next_Status_Code = 'AC' 
								AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
								AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
								AND oua.subs_type = 'Broadband DSL Line'
	JOIN (	SELECT DISTINCT account_number FROM jcartwright.CUST_Fcast_Weekly_Base_2	
		WHERE end_date BETWEEN @L6w AND @H6w
		AND DTV_active = 1 AND bb_active = 1 ) AS y ON y.account_number = mor.account_number									
	WHERE MoR.PC_Future_Sub_Effective_Dt BETWEEN @L6w AND @H6w
		AND (MoR.status_code IN ('PC') OR (MoR.status_code IN ('BCRQ') 
		AND churn_type IN ('CusCan', '3rd Party', 'HM'))) 
		AND MoR.PC_Future_Sub_Effective_Dt IS NOT NULL 
		AND Next_status_code IS NOT NULL 
		AND MoR.PC_Effective_To_Dt <= MoR.PC_Future_Sub_Effective_Dt;

	SELECT PCs.*
		, CASE 	WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
				WHEN (cast(PC_Future_Sub_Effective_Dt AS INT) - cast(End_Date AS INT)) / 7 >= 5 THEN 'Churn in next 6+ wks' END AS Wks_To_Intended_Churn
		, sc.Calendar_date AS End_date
		, CASE 	WHEN sc.calendar_date + 7 BETWEEN event_dt AND PC_Effective_To_Dt THEN 'PC' 
				WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END AS Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt AND Status_Code_EoW = 'AC' THEN PCs.PC_ReAC_Offer_Applied 
				ELSE 0 END AS PC_ReAC_Offer_Applied_EoW
		, (CASE Status_Code_EoW WHEN 'AC' THEN 1 
								WHEN 'CN' THEN 2 
								WHEN 'BCRQ' THEN 3 
								WHEN 'PO' THEN 4 
								WHEN 'AB' THEN 5 
								WHEN 'SC' THEN 6 END) - PC_ReAC_Offer_Applied_EoW AS Status_Code_EoW_Rnk
	INTO #PC_PL_Status
	FROM #PC_Intended_Churn AS PCs
	INNER JOIN #sky_calendar AS sc ON sc.calendar_date BETWEEN PCs.event_dt AND PCs.PC_Effective_To_Dt - 1 
			AND sc.subs_last_day_of_week = 'Y';

	SELECT churn_type
		, status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, PC_ReAC_Offer_Applied_EoW
		, count() AS PCs
		, SUM(PCs) OVER (PARTITION BY Wks_To_Intended_Churn ORDER BY Status_Code_EoW_Rnk ASC) AS Cum_Total_Cohort_PCs
		, SUM(PCs) OVER (PARTITION BY Wks_To_Intended_Churn) AS Total_Cohort_PCs
		, cast(NULL AS REAL) AS PC_Percentile_Lower_Bound
		, cast(Cum_Total_Cohort_PCs AS REAL) / Total_Cohort_PCs AS PC_Percentile_Upper_Bound
	INTO #PC_Percentiles
	FROM #PC_PL_Status
	GROUP BY status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, PC_ReAC_Offer_Applied_EoW
		, churn_type
	ORDER BY Wks_To_Intended_Churn ASC
		, Status_Code_EoW_Rnk ASC
		, Status_Code_EoW ASC
		, PC_ReAC_Offer_Applied_EoW ASC
		, churn_type ASC;

	message cast(now() AS TIMESTAMP) || ' | PC_Status_Movement_Probabilities - PC_Percentiles Populated: ' || @@rowcount TO client;

	UPDATE #PC_Percentiles AS pcp
	SET PC_Percentile_Lower_Bound = cast(Coalesce(pcp2.PC_Percentile_Upper_Bound, 0) AS REAL)
	FROM #PC_Percentiles AS pcp
	LEFT JOIN #PC_Percentiles AS pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Status_Code_EoW_Rnk = pcp.Status_Code_EoW_Rnk - 1;

	message cast(now() AS TIMESTAMP) || ' | TP_PC_Status_Movement_Probabilities - Initialising Completed' TO client;

	SELECT * FROM #PC_Percentiles
END
GO

