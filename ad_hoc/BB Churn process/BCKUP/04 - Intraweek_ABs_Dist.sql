CREATE OR REPLACE PROCEDURE Intraweek_ABs_Dist (IN ForeCAST_Start_Week INT) result (
	Churn_type VARCHAR(10)
	, Status_Code VARCHAR(4)
	, Next_Status_Code VARCHAR(4)
	, AB_ReAC_Offer_Applied TINYINT
	, ABs INT
	, IntaWk_AB_Lower_Pctl REAL
	, IntaWk_AB_Upper_Pctl REAL
	)

BEGIN
	message cast(now() AS TIMESTAMP) || ' | Intraweek_ABs_Dist - Initialization begin ' TO client;

	SELECT * INTO #Sky_Calendar FROM /*Citeam.*/subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);
	
	CREATE OR REPLACE VARIABLE @Lw6dt DATE ;
	CREATE OR REPLACE VARIABLE @Hw6dt DATE ;
	SET @Lw6dt = (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week );
	SET @Hw6dt = (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) ;
	
	
	SELECT 
		mor.subs_week_and_year
		, mor.event_dt
		, mor.event_dt - datepart(weekday, event_dt + 2) AS AB_Event_End_Dt
		, mor.AB_Effective_To_Dt
		, mor.AB_Effective_To_Dt - datepart(weekday, mor.AB_Effective_To_Dt + 2) AS AB_Effective_To_End_Dt
		, mor.account_number
		, MoR.AB_Next_Status_Code AS Next_Status_Code
		, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END AS AB_ReAC_Offer_Applied
		, CASE WHEN Enter_SysCan > 0 THEN 'SysCan' WHEN Enter_CusCan > 0 THEN 'CusCan' WHEN Enter_HM > 0 THEN 'HM' WHEN Enter_3rd_Party > 0 THEN '3rd Party' ELSE NULL END AS Churn_type
	INTO #Acc_AB_Events_Same_Week
	FROM CITeam.PL_Entries_BB AS MoR
	LEFT JOIN Decisioning.Offers_Software AS oua ON oua.account_number = mor.account_number AND oua.offer_leg_start_dt_Actual = MoR.AB_Effective_To_Dt
									AND MoR.AB_Next_Status_Code = 'AC' 
									AND oua.offer_leg_start_dt_Actual = oua.Whole_offer_Start_Dt_Actual 
									AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
									AND oua.subscription_sub_type = 'Broadband DSL Line'		
	LEFT JOIN citeam.nowtv_accounts_ents AS c ON c.account_number = MoR.account_number AND MoR.event_dt BETWEEN period_start_date AND period_end_date
	JOIN (	SELECT DISTINCT account_number 
			FROM citeam.Cust_Weekly_Base
			WHERE end_date BETWEEN @Lw6dt AND @Hw6dt
				AND DTV_active = 0 
				AND bb_active = 1 
				AND skyplus_active = 0) AS y ON y.account_number = MoR.account_number
	WHERE mor.event_dt BETWEEN @Lw6dt AND @Hw6dt 
			AND Mor.status_code = 'AB'
			AND c.account_number IS NULL		-- Exclude Now TV
			;

	--------------------------------------------------------------------------------------------------------------------------------------------		
	--------------------------------------------------------------------------------------------------------------------------------------------	
				
			
	SELECT Churn_type
		, Coalesce(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'AB') AS Next_Status_Code
		, cast(CASE WHEN Next_Status_Code = 'AC' AND AB_ReAC_Offer_Applied = 0 THEN 1 
					WHEN Next_Status_Code = 'AC' AND AB_ReAC_Offer_Applied = 1 THEN 2 
					WHEN Next_Status_Code = 'CN' THEN 3 WHEN Next_Status_Code = 'BCRQ' THEN 4 
					WHEN Next_Status_Code = 'PC' THEN 5 WHEN Next_Status_Code = 'PO' THEN 6 
					WHEN Next_Status_Code = 'SC' THEN 7 
					ELSE 0 END AS INT) AS Next_Status_Code_Rnk
		, cast(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.AB_ReAC_Offer_Applied ELSE 0 END AS INT) AS AB_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY Churn_type ORDER BY Next_Status_Code_Rnk ASC) AS Row_ID
		, count(*) AS ABs
	INTO #AB_Events_Same_Week
	FROM #Acc_AB_Events_Same_Week AS MoR
	GROUP BY Next_Status_Code
		, AB_ReAC_Offer_Applied
		, Churn_type;

	DROP TABLE #Acc_AB_Events_Same_Week;

	SELECT Row_ID
		, Churn_type
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, sum(ABs) OVER (
			PARTITION BY Churn_type ORDER BY Row_ID ASC
			) AS Cum_ABs
		, sum(ABs) OVER (PARTITION BY Churn_type) AS Total_ABs
		, cast(Cum_ABs AS REAL) / Total_ABs AS IntaWk_PC_Upper_Pctl
	INTO #AB_Events
	FROM #AB_Events_Same_Week AS pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, Churn_type;

	DROP TABLE #AB_Events_Same_Week;
	DROP VARIABLE @Lw6dt;
	DROP VARIABLE @Hw6dt;

	SELECT pc1.Churn_type
		, 'AB' AS Status_code
		, pc1.Next_Status_Code
		, pc1.AB_ReAC_Offer_Applied
		, pc1.ABs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) AS IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #AB_Events AS pc1
	LEFT JOIN #AB_Events AS pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.Churn_type = pc2.Churn_type;

	message cast(now() AS TIMESTAMP) || ' | Intraweek_ABs_Dist - Completed' TO client

	
	
END
GO

