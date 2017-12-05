CREATE OR REPLACE PROCEDURE PC_Duration_Dist 
			(IN ForeCAST_Start_Week INT) 
	result (
		churn_type VARCHAR(10)
	, Days_To_churn INT
	, PCs INT
	, Total_PCs INT
	, PC_Days_Lower_Prcntl REAL
	, PC_Days_Upper_Prcntl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | PC_Duration_Dist - BEGIN ' TO client;

	SELECT * INTO #Sky_Calendar FROM /*Citeam.*/subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	SET @Lw6dt = (SELECT max(calendar_date - 6 * 7 + 1) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) ;
	SET @Hw6dt = (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) ;
	
	
	SELECT mor.account_number 
		, CASE WHEN status_code IN ('PC') THEN 1 WHEN status_code IN ('BCRQ') AND churn_type IN ('CusCan' , '3rd Party' , 'HM' ) THEN 2 ELSE 0 END AS source
		, event_dt - datepart(weekday, event_dt + 2) AS PC_Event_End_Dt
		, CASE 	WHEN source = 1 THEN PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) 
				WHEN source = 2 THEN PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) 
				ELSE NULL END AS PC_Effective_To_End_Dt
		, CASE 	WHEN source = 1 THEN PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) 
				WHEN source = 2 THEN PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) 
				ELSE NULL END AS PC_Future_Sub_End_Dt
		, PC_Future_Sub_Effective_Dt - PC_Event_End_Dt AS Days_To_churn
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #PC_Events_Days_To_Intended_Churn
	FROM citeam.Broadband_Comms_Pipeline AS mor
	JOIN (	SELECT DISTINCT account_number FROM citeam.CUST_Fcast_Weekly_Base	
			WHERE end_date BETWEEN @Lw6dt AND @Hw6dt
			AND DTV_active = 0 AND bb_active = 1 ) AS y ON y.account_number = MoR.account_number	
	WHERE event_dt BETWEEN @Lw6dt AND @Hw6dt
			AND (status_code IN ('PC') OR (status_code IN ('BCRQ') 
			AND churn_type IN ('CusCan', '3rd Party', 'HM'))) 
			AND Days_To_churn > 0;

	--------------------------------------------------------------------------------------------------------------------------------------------		
	
				
	SELECT DISTINCT a.account_number, 1 sky_plus
	INTO #skyplus
	FROM   CUST_SUBS_HIST 		AS a
	JOIN   #PC_Events_Days_To_Intended_Churn AS b ON a.account_number = b.account_number 
	WHERE  subscription_sub_type = 'DTV Sky+'
		AND        	status_code='AC'
		AND        	first_activation_dt<=today()               
		AND        	a.account_number is not null
		AND        	a.account_number <> '?'
		AND     	@Hw6dt BETWEEN effective_from_dt AND effective_to_dt ;
		
	
					
	DELETE FROM #PC_Events_Days_To_Intended_Churn
	WHERE account_number IN (SELECT account_number FROM #skyplus);
	DROP TABLE #skyplus;
	--------------------------------------------------------------------------------------------------------------------------------------------		
	-------------------------------------------------------------------------------------------------------------------------------------------		
	SELECT DISTINCT a.account_number, 1 nowtv
	INTO 		#nowtv
	FROM        citeam.nowtv_accounts_ents  AS csav
	JOIN 		#PC_Events_Days_To_Intended_Churn AS a ON a.account_number= csav.account_number
	WHERE       @Hw6dt BETWEEN  period_start_date AND period_end_date ;
						
	DELETE FROM #PC_Events_Days_To_Intended_Churn
	WHERE account_number IN (SELECT account_number FROM #nowtv);
	DROP TABLE #nowtv;
	--------------------------------------------------------------------------------------------------------------------------------------------
	
	SELECT churn_type
		, Days_To_churn
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Days_To_churn ASC) AS Row_ID
		, count() AS PCs
		, SUM(PCs) OVER (PARTITION BY churn_type) AS Total_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Days_To_churn ASC) AS Cum_PCs
		, cast(PCs AS REAL) / Total_PCs AS Pct_PCs
		, cast(NULL AS REAL) AS PC_Days_Lower_Prcntl
		, cast(Cum_PCs AS REAL) / Total_PCs AS PC_Days_Upper_Prcntl
	INTO #PC_Days_Prcntl
	FROM #PC_Events_Days_To_Intended_Churn
	GROUP BY Days_To_churn
		, churn_type
	ORDER BY churn_type ASC
		, Days_To_churn ASC;

	UPDATE #PC_Days_Prcntl AS pc1
	SET pc1.PC_Days_Lower_Prcntl = Coalesce(pc2.PC_Days_Upper_Prcntl, 0)
	FROM #PC_Days_Prcntl AS pc1
	LEFT JOIN #PC_Days_Prcntl AS pc2 ON pc2.Row_ID = pc1.Row_ID - 1;

	DROP VARIABLE @Lw6dt;
	DROP VARIABLE @Hw6dt;

	SELECT churn_type
		, Days_To_churn
		, PCs
		, Total_PCs
		, PC_Days_Lower_Prcntl
		, PC_Days_Upper_Prcntl
	FROM #PC_Days_Prcntl;
	
	message cast(now() AS TIMESTAMP) || ' | PC_Duration_Dist - BEGIN ' TO client
END
GO


