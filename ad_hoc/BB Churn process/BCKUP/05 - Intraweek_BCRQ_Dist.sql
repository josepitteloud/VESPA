CREATE OR REPLACE PROCEDURE Intraweek_BCRQ_Dist 
		(IN ForeCAST_Start_Week INT) 
	result (
			Churn_type VARCHAR(10)
		, Status_Code VARCHAR(4)
		, Next_Status_Code VARCHAR(4)
		, BCRQ_ReAC_Offer_Applied TINYINT
		, BCRQ INT
		, IntaWk_BCRQ_Lower_Pctl REAL
		, IntaWk_BCRQ_Upper_Pctl REAL
		)

BEGIN
	SELECT * INTO #Sky_Calendar FROM /*Citeam.*/subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	SET @Lw6dt = (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week );
	SET @Hw6dt = (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) ;
	
	SELECT 
		mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, event_dt + 2) AS BCRQ_Event_End_Dt
		, mor.BCRQ_Effective_To_Dt
		, mor.BCRQ_Effective_To_Dt - datepart(weekday, mor.BCRQ_Effective_To_Dt + 2) AS BCRQ_Effective_To_End_Dt
		, mor.account_number
		, MoR.BCRQ_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS BCRQ_ReAC_Offer_Applied
		, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
				WHEN Enter_CusCan > 0 THEN 'CusCan' 
				WHEN Enter_HM > 0 THEN 'HM' 
				WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
				ELSE NULL END AS Churn_type
	INTO #Acc_BCRQ_Events_Same_Week
	FROM CITeam.PL_Entries_BB AS MoR
	LEFT JOIN Decisioning.Offers_Software AS oua ON oua.account_number = mor.account_number 
									AND oua.offer_leg_start_dt_Actual = MoR.PC_Effective_To_Dt 
									AND MoR.PC_Next_Status_Code = 'AC' 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%'
									AND oua.offer_leg_start_dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND oua.subscription_sub_type = 'Broadband DSL Line'	
	LEFT JOIN citeam.nowtv_accounts_ents AS c ON c.account_number = MoR.account_number AND MoR.event_dt BETWEEN period_start_date AND period_end_date									
	JOIN (	SELECT DISTINCT account_number 
			FROM citeam.Cust_Weekly_Base
			WHERE end_date BETWEEN @Lw6dt AND @Hw6dt
				AND DTV_active = 0 
				AND bb_active = 1 
				AND skyplus_active = 0) AS y ON y.account_number = MoR.account_number									
	WHERE mor.event_dt BETWEEN @Lw6dt AND @Hw6dt
		AND mor.status_code = 'BCRQ'
		AND c.account_number IS NULL	
		;

	--------------------------------------------------------------------------------------------------------------------------------------------		
	--------------------------------------------------------------------------------------------------------------------------------------------	
				
	SELECT Coalesce(CASE WHEN BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'BCRQ') AS Next_Status_Code
		, cast(CASE Next_Status_Code 	WHEN 'AC' THEN 1 
										WHEN 'CN' THEN 2 
										WHEN 'BCRQ' THEN 3 
										WHEN 'AB' THEN 4 
										WHEN 'SC' THEN 5 
										WHEN 'PO' THEN 5 
										ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt THEN MoR.BCRQ_ReAC_Offer_Applied ELSE 0 END AS INT) AS BCRQ_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk ASC , BCRQ_ReAC_Offer_Applied ASC ) AS Row_ID
		, churn_type
		, COUNT() AS BCRQs
	INTO #BCRQ_Events_Same_Week
	FROM #Acc_BCRQ_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, BCRQs
		, churn_type
		, SUM(BCRQs) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC ) AS Cum_BCRQs
		, SUM(BCRQs) OVER (PARTITION BY churn_type) AS Total_BCRQs
		, cast(Cum_BCRQs AS REAL) / Total_BCRQs AS IntaWk_BCRQ_Upper_Pctl
	INTO #BCRQ_Events
	FROM #BCRQ_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, BCRQs
		, churn_type;

	DROP VARIABLE @Lw6dt;
	DROP VARIABLE @Hw6dt;
	
	SELECT pc1.churn_type
		, 'BCRQ' AS Status_code
		, pc1.Next_Status_Code
		, pc1.BCRQ_ReAC_Offer_Applied
		, pc1.BCRQs
		, Coalesce(pc2.IntaWk_BCRQ_Upper_Pctl, 0) AS IntaWk_BCRQ_Lower_Pctl
		, pc1.IntaWk_BCRQ_Upper_Pctl
	FROM #BCRQ_Events AS pc1
	LEFT JOIN #BCRQ_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.churn_type = pc2.churn_type;
END
GO

