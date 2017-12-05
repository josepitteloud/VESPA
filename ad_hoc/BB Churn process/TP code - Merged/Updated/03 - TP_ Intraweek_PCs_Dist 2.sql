CREATE OR REPLACE PROCEDURE TP_Intraweek_PCs_Dist 
		(IN ForeCAST_Start_Week INT) 
	result (
	Churn_type VARCHAR(10)
	, Status_code VARCHAR(4)
	, Next_Status_Code VARCHAR(4)
	, PC_ReAC_Offer_Applied TINYINT
	, PCs INT
	, IntaWk_PC_Lower_Pctl REAL
	, IntaWk_PC_Upper_Pctl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | Intraweek_PCs_Dist - Initialization begin ' TO client;

	
	SELECT * INTO #Sky_Calendar FROM subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);
	
	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	
	
	SET @Lw6dt = (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week );
	SET @Hw6dt = (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) ;
	
	
	SELECT 
		 mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, mor.event_dt + 2) AS PC_Event_End_Dt
		, mor.PC_Effective_To_Dt
		, mor.PC_Effective_To_Dt - datepart(weekday, mor.PC_Effective_To_Dt + 2) AS PC_Effective_To_End_Dt
		, mor.account_number
		, MoR.PC_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS PC_ReAC_Offer_Applied
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #Acc_PC_Events_Same_Week
	FROM citeam.Broadband_Comms_Pipeline AS MoR
	LEFT JOIN citeam.offer_usage_all AS oua ON oua.account_number = mor.account_number AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt AND MoR.PC_Next_Status_Code = 'AC' 
									AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' AND oua.subs_type = 'Broadband DSL Line'
	JOIN (	SELECT DISTINCT account_number FROM citeam.CUST_Fcast_Weekly_Base	
			WHERE end_date BETWEEN @Lw6dt AND @Hw6dt
			AND DTV_active = 1 AND bb_active = 1 ) AS y ON y.account_number = MoR.account_number
	WHERE   mor.event_dt BETWEEN @Lw6dt AND @Hw6dt -- Last 6 Wk PC conversions
		AND mor.status_code = 'PC'
		AND prodplat_churn_type = 'Product'
		AND bb_cust_type = 'Triple Play';

	--------------------------------------------------------------------------------------------------------------------------------------------		
	
	/*			
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   #Acc_PC_Events_Same_Week AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	@Hw6dt BETWEEN effective_from_dt AND effective_to_dt ;
						
	DELETE FROM #Acc_PC_Events_Same_Week
	WHERE account_number IN (SELECT account_number FROM #skyplus);
	DROP TABLE #skyplus;
	
	--------------------------------------------------------------------------------------------------------------------------------------------
	-------------------------------------------------------------------------------------------------------------------------------------------		
	SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        NOW_TV_SUBS_HIST AS csav
	JOIN 		#Acc_PC_Events_Same_Week AS a ON a.account_number= csav.account_number
	WHERE       @Hw6dt BETWEEN effective_from_dt AND effective_to_dt ;
						
	DELETE FROM #Acc_PC_Events_Same_Week
	WHERE account_number IN (SELECT account_number FROM #nowtv);
	DROP TABLE #nowtv;
	*/
	--------------------------------------------------------------------------------------------------------------------------------------------	
		
	
	SELECT Coalesce(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'PC') AS Next_Status_Code
		, cast(CASE Next_Status_Code 	WHEN 'AC' THEN 1 
										WHEN 'CN' THEN 2 
										WHEN 'BCRQ' THEN 3 
										WHEN 'AB' THEN 4 
										WHEN 'SC' THEN 5 
										WHEN 'PO' THEN 5 
										ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.PC_ReAC_Offer_Applied ELSE 0 END AS INT) AS PC_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk ASC , PC_ReAC_Offer_Applied ASC ) AS Row_ID
		, churn_type
		, COUNT() AS PCs
	INTO #PC_Events_Same_Week
	FROM #Acc_PC_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, PC_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC ) AS Cum_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type) AS Total_PCs
		, cast(Cum_PCs AS REAL) / Total_PCs AS IntaWk_PC_Upper_Pctl
	INTO #PC_Events
	FROM #PC_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type;

	DROP VARIABLE @Lw6dt;
	DROP VARIABLE @Hw6dt;

	SELECT pc1.churn_type
		, 'PC' AS Status_code
		, pc1.Next_Status_Code
		, pc1.PC_ReAC_Offer_Applied
		, pc1.PCs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) AS IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #PC_Events AS pc1
	LEFT JOIN #PC_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.churn_type = pc2.churn_type;

	message cast(now() AS TIMESTAMP) || ' | Intraweek_PCs_Dist - COMPLETED' TO client
END
GO

