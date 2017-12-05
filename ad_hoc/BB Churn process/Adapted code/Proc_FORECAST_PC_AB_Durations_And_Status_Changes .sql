
------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of ABs will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------





CREATE OR REPLACE PROCEDURE Intraweek_ABs_Dist 
			(IN ForeCAST_Start_Week INT) 
			RESULT (Churn_type  VARCHAR(10)
					, Status_Code VARCHAR(4)
					, Next_Status_Code VARCHAR(4)
					, AB_ReAC_Offer_Applied TINYINT
					, ABs INT
					, IntaWk_AB_Lower_Pctl FLOAT
					, IntaWk_AB_Upper_Pctl FLOAT)

BEGIN
		MESSAGE CAST(now() as timestamp)||' | Intraweek_ABs_Dist - Initialization begin ' TO CLIENT;
	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);


	SELECT mor.subs_week_and_year
			, mor.event_dt
			, mor.event_dt - datepart(weekday, event_dt + 2) 								AS AB_Event_End_Dt
			, mor.AB_Effective_To_Dt
			, mor.AB_Effective_To_Dt - datepart(weekday, mor.AB_Effective_To_Dt + 2) 		AS AB_Effective_To_End_Dt
			, mor.account_number
			, MoR.AB_Next_Status_Code												AS Next_Status_Code
			, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END 					AS AB_ReAC_Offer_Applied  
			, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
					WHEN Enter_CusCan > 0 THEN 'CusCan'
					WHEN Enter_HM	  > 0 THEN 'HM'
					WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
		INTO #Acc_AB_Events_Same_Week
		FROM citeam.Broadband_Comms_Pipeline AS MoR								----------------- REPLACE BY CITEAM
		LEFT JOIN offer_usage_all oua ON oua.account_number = mor.account_number 
										AND oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt 
										AND MoR.AB_Next_Status_Code = 'AC' 
										AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
										AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
										AND oua.subs_type = 'Broadband DSL Line'
		WHERE mor.event_dt BETWEEN (SELECT max(calendar_date - 6 - 5 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) -- Last 6 Wk PC conversions
				AND (SELECT max(calendar_date) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
				AND Mor.status_code = 'AB';
--				AND AB_Pending_Terminations > 0   -- TBC
				
			
				
	SELECT 	Churn_type 
		, Coalesce(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'AB') Next_Status_Code
		, CAST(CASE WHEN Next_Status_Code 	= 'AC' AND 	AB_ReAC_Offer_Applied = 0 THEN 1 
                    WHEN Next_Status_Code 	= 'AC' AND 	AB_ReAC_Offer_Applied = 1 THEN 2	
                    WHEN Next_Status_Code 	= 'CN' 	 THEN 3
                    WHEN Next_Status_Code 	= 'BCRQ' THEN 4 
                    WHEN Next_Status_Code 	= 'PC' 	 THEN 5 
                    WHEN Next_Status_Code 	= 'PO' 	 THEN 6 
                    WHEN Next_Status_Code 	= 'SC' 	 THEN 7 
                    ELSE 0 END AS INT ) Next_Status_Code_Rnk
		, CAST(CASE WHEN AB_Effective_To_End_Dt = AB_Event_End_Dt THEN MoR.AB_ReAC_Offer_Applied ELSE 0 END AS INT) AB_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY Churn_type ORDER BY Next_Status_Code_Rnk) Row_ID
		, count(*) AS ABs 
	INTO #AB_Events_Same_Week
	FROM #Acc_AB_Events_Same_Week MoR
	GROUP BY Next_Status_Code
		, AB_ReAC_Offer_Applied
		,Churn_type;
	
	DROP TABLE #Acc_AB_Events_Same_Week;
	
	SELECT Row_ID
		, Churn_type
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, sum(ABs) OVER (PARTITION BY Churn_type ORDER BY Row_ID) Cum_ABs
		, sum(ABs) OVER (PARTITION BY Churn_type ) Total_ABs
		, CAST(Cum_ABs AS FLOAT) / Total_ABs AS IntaWk_PC_Upper_Pctl
	INTO #AB_Events
	FROM #AB_Events_Same_Week pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, AB_ReAC_Offer_Applied
		, ABs
		, Churn_type;

	DROP TABLE #AB_Events_Same_Week;
	
	SELECT 	pc1.Churn_type
		, 'AB'	AS Status_code
		, pc1.Next_Status_Code
		, pc1.AB_ReAC_Offer_Applied
		, pc1.ABs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #AB_Events pc1
	LEFT JOIN #AB_Events pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.Churn_type = pc2.Churn_type;
	
	MESSAGE CAST(now() as timestamp)||' | Intraweek_ABs_Dist - Completed' TO CLIENT;
END;

GRANT EXECUTE ON Intraweek_ABs_Dist  TO CITeam, vespa_group_low_security;


GO
------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from AB to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
--DECLARE @ForeCAST_Start_Week INT
--SET @ForeCAST_Start_Week = 201601







CREATE OR REPLACE PROCEDURE AB_Status_Movement_Probabilities (IN @ForeCAST_Start_Week INT) 
		RESULT 
			(Churn_type  VARCHAR(10)
			, Status_Code VARCHAR(4)
			, Wks_To_Intended_Churn VARCHAR(20)
			, Status_Code_EoW VARCHAR(4)
			, Status_Code_EoW_Rnk INT
			, AB_ReAC_Offer_Applied TINYINT
			, AB_s INT
			, Cum_Total_Cohort_ABs INT
			, Total_Cohort_ABs INT
			, AB_Percentile_Lower_Bound FLOAT
			, AB_Percentile_Upper_Bound FLOAT
			)

BEGIN
	MESSAGE cast(now() as timestamp)||' | AB_Status_Movement_Probabilities - Initialization Begin' TO CLIENT;
	
	SELECT * INTO #Sky_Calendar FROM CITeam.Subs_Calendar(@ForeCAST_Start_Week / 100 - 1, @ForeCAST_Start_Week / 100);

	SELECT    mor.account_number
			, mor.status_code
			, mor.event_dt
			, mor.AB_Future_Sub_Effective_Dt
			, CAST (mor.AB_Future_Sub_Effective_Dt - datepart(weekday, mor.AB_Future_Sub_Effective_Dt + 2) + 7 AS DATE) 	AS AB_Future_Sub_Effective_Dt_End_Dt
			, mor.AB_Effective_To_Dt
			, mor.AB_Next_status_code  																						AS Next_status_code
			, CASE WHEN oua.offer_id  IS NOT NULL THEN 1 ELSE 0 END 														AS AB_ReAC_Offer_Applied
			, CASE 	WHEN mor.Enter_SysCan > 0 THEN 'SysCan' 
					WHEN mor.Enter_CusCan > 0 THEN 'CusCan'
					WHEN mor.Enter_HM	  > 0 THEN 'HM'
					WHEN mor.Enter_3rd_Party > 0 THEN '3rd Party' 
					ELSE NULL END 																							AS Churn_type 
		INTO #AB_Intended_Churn
		FROM citeam.Broadband_Comms_Pipeline			AS mor	
		LEFT JOIN offer_usage_all oua ON oua.account_number = mor.account_number 
										AND oua.offer_Start_Dt_Actual = MoR.AB_Effective_To_Dt 
										AND MoR.AB_Next_Status_Code = 'AC' 
										AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
										AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
										AND oua.subs_type = 'Broadband DSL Line'
		WHERE AB_Future_Sub_Effective_Dt BETWEEN (SELECT min(calendar_date - 6 * 7) FROM #sky_calendar WHERE subs_week_and_year = @ForeCAST_Start_Week) -- Last 6 Wk PC conversions
											 AND (SELECT min(calendar_date - 1) FROM #sky_calendar WHERE subs_week_and_year = @ForeCAST_Start_Week) 
				AND AB_Future_Sub_Effective_Dt IS NOT NULL 
				AND AB_Next_status_code IS NOT NULL 
				AND AB_Effective_To_Dt <= AB_Future_Sub_Effective_Dt
				AND (status_code = 'AB' OR (status_code = 'BCRQ' AND Churn_type = 'SysCan'));
	
---------------------------------------------------------------------------------------------------------------
----------		UPDATE to flag BCRQ to CN accounts
---------------------------------------------------------------------------------------------------------------
	    MESSAGE cast(now() as timestamp)||' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts Begin' TO CLIENT;
		
		SELECT a.account_number 
			, a.event_dt
			, b.status_code 			AS next_cancel_status
			, b.effective_from_dt		AS next_cancel_dt 	
			,RANK() OVER (PARTITION BY a.account_number ORDER BY b.effective_from_dt , b.cb_row_id) rankk
		INTO #AB_BCRQ
		FROM #AB_Intended_Churn 		AS a 
		JOIN cust_subs_hist 			AS b  	ON a.account_number = b.account_number AND a.AB_Effective_To_Dt <= b.effective_from_dt
		WHERE   b.subscription_sub_type = 'Broadband DSL Line'
			AND b.status_code_changed = 'Y'
			AND b.status_code in ('PO','SC','CN')              	
			AND a.Next_status_code in ('BCRQ')  
			AND b.effective_from_dt != b.effective_to_dt
			AND b.prev_status_code IN ('BCRQ') ;
		MESSAGE cast(now() as timestamp)||' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 1/2' TO CLIENT;
		
		DELETE FROM #AB_BCRQ WHERE  rankk > 1 ;
		
		UPDATE #AB_Intended_Churn
		SET AB_Future_Sub_Effective_Dt = next_cancel_dt
			, Next_status_code = next_cancel_status
		FROM #AB_Intended_Churn 	AS a 
		JOIN  #AB_BCRQ 				AS b ON a.account_number = b.account_number AND a.event_dt = b.event_dt AND a.status_code = 'AB'; 
		
		DROP TABLE #AB_BCRQ ;
		
		UPDATE #AB_Intended_Churn
		SET Next_status_code = 'CN' 
		WHERE Next_status_code = 'BCRQ';
	MESSAGE cast(now() as timestamp)||' | AB_Status_Movement_Probabilities - UPDATE to flag BCRQ to CN accounts checkpoint 2/2' TO CLIENT;
					
	
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------				
				
				
	SELECT AB_s.*
		, CASE 	WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 5 THEN 'Churn in next 6 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 6 THEN 'Churn in next 7 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 7 THEN 'Churn in next 8 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 8 THEN 'Churn in next 9 wks' 
				WHEN (CAST(AB_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 >= 9 THEN 'Churn in next 10+ wks' 
				END 																														AS Wks_To_Intended_Churn
		, sc.Calendar_date End_date
		, CASE 	WHEN sc.calendar_date + 7 BETWEEN event_dt AND AB_Effective_To_Dt THEN 'AB' 
				WHEN sc.calendar_date + 7 BETWEEN AB_Effective_To_Dt AND AB_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END 		AS Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 = AB_Effective_To_Dt - datepart(weekday, AB_Effective_To_Dt + 2) + 7 
					AND Status_Code_EoW = 'AC' THEN AB_s.AB_ReAC_Offer_Applied 
					ELSE 0 END 																												AS AB_ReAC_Offer_Applied_EoW
		, (CASE 	WHEN Status_Code_EoW = 'AC'       	AND 	AB_ReAC_Offer_Applied = 0 THEN 1 
                    WHEN Status_Code_EoW = 'AC'   	AND 	AB_ReAC_Offer_Applied = 1 THEN 2	
                    WHEN Status_Code_EoW = 'CN' 	THEN 3
                    WHEN Status_Code_EoW = 'BCRQ'  THEN 4 
                    WHEN Status_Code_EoW = 'PC' 	THEN 5 
                    WHEN Status_Code_EoW = 'PO' 	THEN 6 
                    WHEN Status_Code_EoW = 'SC' 	THEN 7
					ELSE 0 END 							) 																AS Status_Code_EoW_Rnk
	INTO #AB_PL_Status
	FROM #AB_Intended_Churn 		AS AB_s
	JOIN #sky_calendar sc ON sc.calendar_date BETWEEN AB_s.event_dt AND AB_s.AB_Effective_To_Dt - 1 
							AND sc.subs_last_day_of_week = 'Y';
	
		
	SELECT Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, AB_ReAC_Offer_Applied_EoW
		, count(*) 																							AS AB_s
		, Sum(AB_s) OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type ORDER BY Status_Code_EoW_Rnk) 		AS Cum_Total_Cohort_ABs
		, Sum(AB_s) OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type) 									AS Total_Cohort_ABs
		, CAST(NULL AS FLOAT) 																				AS AB_Percentile_Lower_Bound
		, CAST(Cum_Total_Cohort_ABs AS FLOAT) / Total_Cohort_ABs 											AS AB_Percentile_Upper_Bound
		, Row_Number() OVER (PARTITION BY Wks_To_Intended_Churn, Churn_type ORDER BY Status_Code_EoW_Rnk) 	AS Row_ID
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
	ORDER BY status_code
		, Churn_type
		, Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, AB_ReAC_Offer_Applied_EoW;

	MESSAGE cast(now() as timestamp)||' | AB_Status_Movement_Probabilities - AB_Percentiles populated: '||@@rowcount TO CLIENT;
		
	UPDATE #AB_Percentiles pcp
	SET AB_Percentile_Lower_Bound = CAST(Coalesce(pcp2.AB_Percentile_Upper_Bound, 0) AS FLOAT)
	FROM #AB_Percentiles pcp
	LEFT JOIN #AB_Percentiles pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Row_ID = pcp.Row_ID - 1 AND pcp.Churn_type = pcp2.Churn_type;
	
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
	
	MESSAGE cast(now() as timestamp)||' | AB_Status_Movement_Probabilities - Completed' TO CLIENT;
	
END;

GRANT EXECUTE ON AB_Status_Movement_Probabilities TO CITeam, vespa_group_low_security;

--SELECT * FROM CITeam.AB_Status_Movement_Probabilities(201601)

------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of PCs will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------
--CREATE variable ForeCAST_Start_Week INT
--SET ForeCAST_Start_Week = 201601

GO


CREATE OR REPLACE PROCEDURE Intraweek_PCs_Dist (IN ForeCAST_Start_Week INT) RESULT 
		( Churn_type  VARCHAR(10)
		, Status_code VARCHAR(4)
		, Next_Status_Code VARCHAR(4)
		, PC_ReAC_Offer_Applied TINYINT
		, PCs INT
		, IntaWk_PC_Lower_Pctl FLOAT
		, IntaWk_PC_Upper_Pctl FLOAT 
		)

BEGIN
	MESSAGE CAST(now() as timestamp)||' | Intraweek_PCs_Dist - Initialization begin ' TO CLIENT;
	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT mor.subs_week_and_year
			, mor.event_dt
			, mor.event_dt - datepart(weekday, mor.event_dt + 2) 							AS PC_Event_End_Dt
			, mor.PC_Effective_To_Dt
			, mor.PC_Effective_To_Dt - datepart(weekday, mor.PC_Effective_To_Dt + 2) 		AS PC_Effective_To_End_Dt
			, mor.account_number
			-- ,csh.status_code Next_Status_Code1
			, MoR.PC_Next_Status_Code Next_Status_Code
			, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END PC_ReAC_Offer_Applied
			, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
					WHEN Enter_CusCan > 0 THEN 'CusCan'
					WHEN Enter_HM	  > 0 THEN 'HM'
					WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
		INTO #Acc_PC_Events_Same_Week
		FROM citeam.Broadband_Comms_Pipeline AS MoR
		LEFT JOIN offer_usage_all oua ON oua.account_number = mor.account_number 
										AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt 
										AND MoR.PC_Next_Status_Code = 'AC' 
										AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
										AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
										AND oua.subs_type = 'Broadband DSL Line'
		WHERE mor.event_dt BETWEEN  (SELECT max(calendar_date - 6 - 5 * 7) 	FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) -- Last 6 Wk PC conversions
								AND (SELECT max(calendar_date) 				FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
								AND mor.status_code = 'PC';
							--AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0);
							
							

	SELECT Coalesce(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'PC') 		AS 	Next_Status_Code
		, CAST(CASE Next_Status_Code 	WHEN 'AC' 	THEN 1 
										WHEN 'CN' 	THEN 2 
										WHEN 'BCRQ' THEN 3
										WHEN 'AB' 	THEN 4
										WHEN 'SC' 	THEN 5
										WHEN 'PO' 	THEN 5
										ELSE 0 END AS INT) 																	AS 	Next_Status_Code_Rnk
		, CAST(CASE WHEN PC_Effective_To_End_Dt = PC_Event_End_Dt THEN MoR.PC_ReAC_Offer_Applied ELSE 0 END AS INT) 		AS 	PC_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk, PC_ReAC_Offer_Applied) 					AS 	Row_ID
		, churn_type
		, COUNT(*) 																											AS  PCs 
	INTO #PC_Events_Same_Week
	FROM #Acc_PC_Events_Same_Week MoR
	GROUP BY Next_Status_Code
		, PC_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		, churn_type
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Row_ID) 					AS Cum_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type ) 									AS Total_PCs 
		, CAST(Cum_PCs AS FLOAT) / Total_PCs 										AS IntaWk_PC_Upper_Pctl
	INTO #PC_Events
	FROM #PC_Events_Same_Week pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, PC_ReAC_Offer_Applied
		, PCs
		,churn_type;

	SELECT 
		  pc1.churn_type 
		, 'PC' 	AS Status_code
		, pc1.Next_Status_Code
		, pc1.PC_ReAC_Offer_Applied
		, pc1.PCs
		, Coalesce(pc2.IntaWk_PC_Upper_Pctl, 0) 			AS IntaWk_PC_Lower_Pctl
		, pc1.IntaWk_PC_Upper_Pctl
	FROM #PC_Events pc1
	LEFT JOIN #PC_Events pc2 ON pc2.row_id = pc1.row_id - 1 AND pc1.churn_type = pc2.churn_type;
	
		MESSAGE CAST(now() as timestamp)||' | Intraweek_PCs_Dist - COMPLETED' TO CLIENT;
END;

GRANT EXECUTE ON Intraweek_PCs_Dist TO CITeam, vespa_group_low_security;



GO
------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc duration between PC and intended churn date ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
--CREATE variable ForeCAST_Start_Week INT
--SET ForeCAST_Start_Week = 201601

CREATE OR REPLACE PROCEDURE PC_Duration_Dist (IN ForeCAST_Start_Week INT) RESULT (
		  churn_type VARCHAR (10)
		, Days_To_churn INT
		, PCs INT
		, Total_PCs INT
		, PC_Days_Lower_Prcntl FLOAT
		, PC_Days_Upper_Prcntl FLOAT
		)


BEGIN

	MESSAGE CAST(now() as timestamp)||' | PC_Duration_Dist - BEGIN ' TO CLIENT;

	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT 	
		  CASE 	WHEN status_code IN ('PC') THEN 1
				WHEN status_code IN ('BCRQ') AND churn_type IN ('CusCan','3rd Party','HM') THEN 2
				ELSE 0 END 																									AS source
		, event_dt - datepart(weekday, event_dt + 2) 																		AS PC_Event_End_Dt
		, CASE 	WHEN source = 1 THEN PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) 							
				WHEN source = 2 THEN PC_Effective_To_Dt - datepart(weekday, PC_Effective_To_Dt + 2) -------	##################REPLACE by BCRQ fields when available 		################	
				ELSE NULL END 																								AS PC_Effective_To_End_Dt
		, CASE 	WHEN source = 1 THEN PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2) 		
				WHEN source = 2 THEN PC_Future_Sub_Effective_Dt - datepart(weekday, PC_Future_Sub_Effective_Dt + 2)	-------##########	REPLACE by BCRQ fields when available 	########
				ELSE NULL END 																								AS PC_Future_Sub_End_Dt
		, PC_Future_Sub_Effective_Dt - PC_Event_End_Dt 																		AS Days_To_churn
		, CASE 	WHEN Enter_SysCan > 0 			THEN 'SysCan' 
					WHEN Enter_CusCan > 0 		THEN 'CusCan'
					WHEN Enter_HM	  > 0 		THEN 'HM'
					WHEN Enter_3rd_Party > 0 	THEN '3rd Party' 
					ELSE NULL END 							AS Churn_type 
	INTO #PC_Events_Days_To_Intended_Churn
	FROM citeam.Broadband_Comms_Pipeline 
	WHERE event_dt BETWEEN  (SELECT max(calendar_date - 6 * 7 + 1) 	FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) -- Last 6 Wk PC conversions
						AND (SELECT max(calendar_date)				FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) 
					AND (status_code IN ('PC') OR (status_code IN ('BCRQ') AND churn_type IN ('CusCan','3rd Party','HM')));
				--AND (Same_Day_Cancels > 0 OR PC_Pending_Cancellations > 0 OR Same_Day_PC_Reactivations > 0) AND PC_Event_End_Dt != PC_Effective_To_End_Dt AND PC_Event_End_Dt != PC_Future_Sub_End_Dt AND PC_Future_Sub_Effective_Dt > event_dt
				
	SELECT
		  churn_type 
		, Days_To_churn
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Days_To_churn) 		AS Row_ID
		, count(*) 																	AS PCs
		, SUM(PCs) OVER (PARTITION BY churn_type) 									AS Total_PCs
		, SUM(PCs) OVER (PARTITION BY churn_type ORDER BY Days_To_churn) 			AS Cum_PCs
		, CAST(PCs AS FLOAT) / Total_PCs 											AS Pct_PCs
		, CAST(NULL AS FLOAT) 														AS PC_Days_Lower_Prcntl
		, CAST(Cum_PCs AS FLOAT) / Total_PCs 										AS PC_Days_Upper_Prcntl
	INTO #PC_Days_Prcntl
	FROM #PC_Events_Days_To_Intended_Churn
	GROUP BY Days_To_churn, churn_type
	ORDER BY churn_type, Days_To_churn;

	UPDATE #PC_Days_Prcntl pc1
	SET pc1.PC_Days_Lower_Prcntl = Coalesce(pc2.PC_Days_Upper_Prcntl, 0)
	FROM #PC_Days_Prcntl pc1
	LEFT JOIN #PC_Days_Prcntl pc2 ON pc2.Row_ID = pc1.Row_ID - 1;

	SELECT
		  churn_type
		, Days_To_churn
		, PCs
		, Total_PCs
		, PC_Days_Lower_Prcntl
		, PC_Days_Upper_Prcntl
	FROM #PC_Days_Prcntl;
	MESSAGE CAST(now() as timestamp)||' | PC_Duration_Dist - BEGIN ' TO CLIENT;
END;

GRANT EXECUTE ON PC_Duration_Dist  TO CITeam, vespa_group_low_security;
GO
------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc rates for customers moving from PC to another status ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE PC_Status_Movement_Probabilities (IN ForeCAST_Start_Week INT) RESULT (
		  churn_type VARCHAR(10) 
		, Initial_status_code VARCHAR(10) 
		, Wks_To_Intended_Churn VARCHAR(20)
		, Status_Code_EoW VARCHAR(4)
		, Status_Code_EoW_Rnk INT
		, PC_ReAC_Offer_Applied TINYINT
		, PCs INT
		, Cum_Total_Cohort_PCs INT
		, Total_Cohort_PCs INT
		, PC_Percentile_Lower_Bound FLOAT
		, PC_Percentile_Upper_Bound FLOAT
		)

BEGIN
	MESSAGE cast(now() as timestamp)||' | PC_Status_Movement_Probabilities - Initialising Environment' TO CLIENT;
	SELECT * INTO #Sky_Calendar FROM CITeam.Subs_Calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT MoR.account_number
			, MoR.status_code
			, MoR.event_dt
			, MoR.PC_Future_Sub_Effective_Dt
			, CAST(MoR.PC_Future_Sub_Effective_Dt - datepart(weekday, MoR.PC_Future_Sub_Effective_Dt + 2) + 7 AS DATE) 			AS PC_Future_Sub_Effective_Dt_End_Dt
			, MoR.PC_Effective_To_Dt
			, MoR.PC_Next_status_code 																							AS Next_status_code
			, CASE WHEN oua.offer_id IS NOT NULL THEN 1 ELSE 0 END 																AS PC_ReAC_Offer_Applied
			, CASE 	WHEN MoR.Enter_SysCan > 0 		THEN 'SysCan' 
					WHEN MoR.Enter_CusCan > 0 		THEN 'CusCan'
					WHEN MoR.Enter_HM	  > 0 		THEN 'HM'
					WHEN MoR.Enter_3rd_Party > 0 	THEN '3rd Party' 
				ELSE NULL END 																									AS Churn_type 

		INTO #PC_Intended_Churn
		FROM CITEAM.Broadband_Comms_Pipeline AS mor
		LEFT JOIN offer_usage_all oua ON oua.account_number = mor.account_number 
										AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt 
										AND MoR.PC_Next_Status_Code = 'AC' 
										AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
										AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
										AND oua.subs_type = 'Broadband DSL Line'
		WHERE MoR.PC_Future_Sub_Effective_Dt BETWEEN (SELECT min(calendar_date - 6 * 7) FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) -- Last 6 Wk PC conversions
											AND  (SELECT min(calendar_date - 1) 	FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
						AND (MoR.status_code IN ('PC') OR (MoR.status_code IN ('BCRQ') AND churn_type IN ('CusCan','3rd Party','HM')))
						AND MoR.PC_Future_Sub_Effective_Dt IS NOT NULL 
						AND Next_status_code IS NOT NULL 
						AND MoR.PC_Effective_To_Dt <= MoR.PC_Future_Sub_Effective_Dt;

	SELECT PCs.*
		, CASE 	WHEN (CAST(PC_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
				WHEN (CAST(PC_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
				WHEN (CAST(PC_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
				WHEN (CAST(PC_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
				WHEN (CAST(PC_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
				WHEN (CAST(PC_Future_Sub_Effective_Dt AS INT) - CAST(End_Date AS INT)) / 7 >= 5 THEN 'Churn in next 6+ wks'
			END 																														AS Wks_To_Intended_Churn
		, sc.Calendar_date End_date
		, CASE 	WHEN sc.calendar_date + 7 BETWEEN event_dt AND PC_Effective_To_Dt THEN 'PC' 
				WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt THEN Next_Status_Code END 	AS Status_Code_EoW
		, CASE WHEN sc.calendar_date + 7 BETWEEN PC_Effective_To_Dt AND PC_Future_Sub_Effective_Dt_End_Dt 
												AND Status_Code_EoW = 'AC' THEN PCs.PC_ReAC_Offer_Applied ELSE 0 END 					AS PC_ReAC_Offer_Applied_EoW
		, (CASE Status_Code_EoW 	WHEN 'AC' 	THEN 1 
									WHEN 'CN' 	THEN 2 
									WHEN 'BCRQ' THEN 3 
									WHEN 'PO' 	THEN 4 
									WHEN 'AB' 	THEN 5 
									WHEN 'SC' 	THEN 6 END) - PC_ReAC_Offer_Applied_EoW 													AS Status_Code_EoW_Rnk
		
	INTO #PC_PL_Status
	FROM #PC_Intended_Churn PCs
	INNER JOIN #sky_calendar sc ON sc.calendar_date BETWEEN PCs.event_dt AND PCs.PC_Effective_To_Dt - 1 AND sc.subs_last_day_of_week = 'Y';

	
	SELECT churn_type
		, status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW
		, Status_Code_EoW_Rnk
		, PC_ReAC_Offer_Applied_EoW
		, count(*) 																					AS PCs
		, SUM(PCs) OVER (PARTITION BY Wks_To_Intended_Churn ORDER BY Status_Code_EoW_Rnk) 			AS Cum_Total_Cohort_PCs
		, SUM(PCs) OVER (PARTITION BY Wks_To_Intended_Churn) 										AS Total_Cohort_PCs
		, CAST(NULL AS FLOAT) 																		AS PC_Percentile_Lower_Bound
		, CAST(Cum_Total_Cohort_PCs AS FLOAT) / Total_Cohort_PCs 									AS PC_Percentile_Upper_Bound
		
	INTO #PC_Percentiles
	FROM #PC_PL_Status
	GROUP BY 
		status_code
		, Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, PC_ReAC_Offer_Applied_EoW
		, churn_type
		
	ORDER BY Wks_To_Intended_Churn
		, Status_Code_EoW_Rnk
		, Status_Code_EoW
		, PC_ReAC_Offer_Applied_EoW
		, churn_type
		;
	
	MESSAGE cast(now() as timestamp)||' | PC_Status_Movement_Probabilities - PC_Percentiles Populated: '||@@rowcount TO CLIENT;
	
	UPDATE #PC_Percentiles pcp
	SET PC_Percentile_Lower_Bound = CAST(Coalesce(pcp2.PC_Percentile_Upper_Bound, 0) AS FLOAT)
	FROM #PC_Percentiles pcp
	LEFT JOIN #PC_Percentiles pcp2 ON pcp2.Wks_To_Intended_Churn = pcp.Wks_To_Intended_Churn AND pcp2.Status_Code_EoW_Rnk = pcp.Status_Code_EoW_Rnk - 1;

	MESSAGE cast(now() as timestamp)||' | PC_Status_Movement_Probabilities - Initialising Completed' TO CLIENT;
	SELECT * FROM #PC_Percentiles 
END;

GRANT EXECUTE ON PC_Status_Movement_Probabilities TO CITeam, vespa_group_low_security;

GO

--------------------------------------------------------------- BCRQ -----------------------------------------------------------------------------------





------------------------------------------------------------------------------------------------------------------------------
-- Procedure to calc what percentage of BCRQ will churn or reactivate before the end of the week ------------------------------
------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE Intraweek_BCRQ_Dist (IN ForeCAST_Start_Week INT) RESULT 
		(Churn_type  VARCHAR(10)
		, Status_Code VARCHAR(4)
		, Next_Status_Code VARCHAR(4)
		, BCRQ_ReAC_Offer_Applied TINYINT
		, BCRQ INT
		, IntaWk_BCRQ_Lower_Pctl FLOAT
		, IntaWk_BCRQ_Upper_Pctl FLOAT 
		)

BEGIN
	SELECT * INTO #Sky_Calendar FROM Citeam.subs_calendar(ForeCAST_Start_Week / 100 - 1, ForeCAST_Start_Week / 100);

	SELECT mor.subs_week_and_year
			, mor.event_dt
			, mor.event_dt - datepart(weekday, event_dt + 2) 							AS BCRQ_Event_End_Dt
			, mor.BCRQ_Effective_To_Dt
			, mor.BCRQ_Effective_To_Dt - datepart(weekday, mor.BCRQ_Effective_To_Dt + 2) 	AS BCRQ_Effective_To_End_Dt
			, mor.account_number
			, MoR.BCRQ_Next_Status_Code Next_Status_Code
			, CASE WHEN oua.offer_id  IS NOT NULL THEN 1 ELSE 0 END 				AS BCRQ_ReAC_Offer_Applied
			, CASE 	WHEN Enter_SysCan > 0 THEN 'SysCan' 
					WHEN Enter_CusCan > 0 THEN 'CusCan'
					WHEN Enter_HM	  > 0 THEN 'HM'
					WHEN Enter_3rd_Party > 0 THEN '3rd Party' 
					ELSE NULL END 												AS Churn_type 
		INTO #Acc_BCRQ_Events_Same_Week
		FROM citeam.Broadband_Comms_Pipeline AS MoR
		LEFT JOIN offer_usage_all oua ON oua.account_number = mor.account_number 
										AND oua.offer_Start_Dt_Actual = MoR.PC_Effective_To_Dt 
										AND MoR.PC_Next_Status_Code = 'AC' 
										AND oua.offer_Start_Dt_Actual = oua.Whole_offer_Start_Dt_Actual 
										AND lower(oua.offer_dim_description) NOT LIKE '%price protection%' 
										AND oua.subs_type = 'Broadband DSL Line'
		WHERE mor.event_dt BETWEEN  (SELECT max(calendar_date - 6 - 5 * 7) 	FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week ) -- Last 6 Wk BCRQ conversions
								AND (SELECT max(calendar_date) 				FROM #sky_calendar WHERE subs_week_and_year = ForeCAST_Start_Week) 
								AND mor.status_code = 'BCRQ';
							--AND (Same_Day_Cancels > 0 OR BCRQ_Pending_Cancellations > 0 OR Same_Day_BCRQ_Reactivations > 0);
							
							

	SELECT Coalesce(CASE WHEN BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt THEN MoR.Next_Status_Code ELSE NULL END, 'BCRQ') 		AS 	Next_Status_Code
		, CAST(CASE Next_Status_Code 	WHEN 'AC' 	THEN 1 
										WHEN 'CN' 	THEN 2 
										WHEN 'BCRQ' THEN 3
										WHEN 'AB' 	THEN 4
										WHEN 'SC' 	THEN 5
										WHEN 'PO' 	THEN 5
										ELSE 0 END AS INT) 																	AS 	Next_Status_Code_Rnk
		, CAST(CASE WHEN BCRQ_Effective_To_End_Dt = BCRQ_Event_End_Dt THEN MoR.BCRQ_ReAC_Offer_Applied ELSE 0 END AS INT) 		AS 	BCRQ_ReAC_Offer_Applied
		, Row_number() OVER (PARTITION BY churn_type ORDER BY Next_Status_Code_Rnk, BCRQ_ReAC_Offer_Applied) 					AS 	Row_ID
		, churn_type
		, COUNT(*) 																											AS  BCRQs 
	INTO #BCRQ_Events_Same_Week
	FROM #Acc_BCRQ_Events_Same_Week MoR
	GROUP BY Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, churn_type;

	SELECT Row_ID
		, Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, BCRQs
		, churn_type
		, SUM(BCRQs) OVER (PARTITION BY churn_type ORDER BY Row_ID) 					AS Cum_BCRQs
		, SUM(BCRQs) OVER (PARTITION BY churn_type ) 									AS Total_BCRQs 
		, CAST(Cum_BCRQs AS FLOAT) / Total_BCRQs 										AS IntaWk_BCRQ_Upper_Pctl
	INTO #BCRQ_Events
	FROM #BCRQ_Events_Same_Week pc1
	GROUP BY Row_ID
		, Next_Status_Code
		, BCRQ_ReAC_Offer_Applied
		, BCRQs
		,churn_type;

	SELECT 
		  pc1.churn_type 
		, 'BCRQ' AS Status_code
		, pc1.Next_Status_Code
		, pc1.BCRQ_ReAC_Offer_Applied
		, pc1.BCRQs
		, Coalesce(pc2.IntaWk_BCRQ_Upper_Pctl, 0) 			AS IntaWk_BCRQ_Lower_Pctl
		, pc1.IntaWk_BCRQ_Upper_Pctl
	FROM #BCRQ_Events pc1
	LEFT JOIN #BCRQ_Events pc2 ON pc2.row_id = pc1.row_id - 1 AND   pc1.churn_type =   pc2.churn_type 
END;

GRANT EXECUTE ON Intraweek_BCRQ_Dist TO CITeam, vespa_group_low_security
