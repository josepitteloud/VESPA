CREATE OR REPLACE PROCEDURE Forecast_SABB_Loop_Table_2_Actions (IN Counter INT, IN Rate_Multiplier REAL) sql security invoker

BEGIN
	DECLARE multiplier BIGINT;
	DECLARE multiplier_2 BIGINT;

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Initializing' TO client;

	SET multiplier = DATEPART(millisecond, now()) + 1;
	SET multiplier_2 = DATEPART(millisecond, now()) + 2;

	DROP TABLE IF EXISTS TP_intraweek_movements;
	DROP TABLE IF EXISTS weekly_movements;
		--------------------------------------------------------------------------------------------------------------
		-- Predicted rates -------------------------------------------------------------------------------------------
		--------------------------------------------------------------------------------------------------------------
		--- rates ----
		UPDATE Forecast_Loop_Table_2 AS a
		SET pred_bb_enter_SysCan_rate = Coalesce(b.pred_SysCan_rate, 0)
			, pred_bb_enter_CusCan_rate = Coalesce(b.pred_CusCan_rate, 0)
			, pred_bb_enter_HM_rate = Coalesce(b.pred_HM_rate, 0)
			, pred_bb_enter_3rd_party_rate = Coalesce(b.pred_3rd_party_rate, 0)
			, pred_BB_Offer_Applied_rate = Coalesce(b.pred_BB_Offer_Applied_rate, 0)
		FROM Forecast_Loop_Table_2 AS a
		LEFT JOIN SABB_predicted_values AS b ON (a.subs_week_of_year = b.subs_week OR (a.subs_week_of_year = 53 AND b.subs_week = 52)) AND a.sabb_forecast_segment = b.sabb_forecast_segment;

	------ SysCan trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_SysCan_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'SysCan Entry';

	------ SysCan cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_SysCan_rate = CASE WHEN pred_bb_enter_SysCan_rate + pred_bb_enter_SysCan_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_SysCan_rate + pred_bb_enter_SysCan_YoY_Trend END;

	------ CusCan trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_CusCan_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'CusCan Entry';

	------ CusCan cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_CusCan_rate = CASE WHEN pred_bb_enter_CusCan_rate + pred_bb_enter_CusCan_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_CusCan_rate + pred_bb_enter_CusCan_YoY_Trend END;

	------ HM trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_HM_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'HM Entry';

	------ HM cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_HM_rate = CASE WHEN pred_bb_enter_HM_rate + pred_bb_enter_HM_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_HM_rate + pred_bb_enter_HM_YoY_Trend END;

	------ 3rd party trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_3rd_party_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = '3rd Party Entry';

	------ 3rd party cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_3rd_party_rate = CASE WHEN pred_bb_enter_3rd_party_rate + pred_bb_enter_3rd_party_YoY_Trend <= 0 THEN 0 ELSE pred_bb_enter_3rd_party_rate + pred_bb_enter_3rd_party_YoY_Trend END;

	------ BB offer applied trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_BB_Offer_Applied_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (cast(counter - 1 AS REAL) / 53), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'BB Offer Applied';

	------ BB offer applied cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_BB_Offer_Applied_rate = CASE WHEN pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend <= 0 THEN 0 ELSE pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend END;

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 1 ' TO client;

	UPDATE Forecast_Loop_Table_2
	SET rand_action_Pipeline = CASE WHEN BB_status_code IN ('AB', 'BCRQ', 'PC') THEN 1 
			ELSE NULL 
			END;

	UPDATE Forecast_Loop_Table_2
	SET rand_action_Pipeline = rand(number() * multiplier + 4)
	WHERE rand_action_Pipeline IS NULL;

	DROP TABLE

	IF EXISTS #Pipeline_Rank;
		SELECT account_number
			, rand_action_Pipeline
			, count() OVER (PARTITION BY SABB_Forecast_segment) AS Total_Cust_In_SABB_Segment
			, cast(rank() OVER (PARTITION BY SABB_Forecast_segment ORDER BY rand_action_Pipeline ASC) AS REAL) AS SABB_Group_rank
			, CASE WHEN rand_action_Pipeline <= cum_bb_enter_SysCan_rate THEN 1 ELSE 0 END AS BB_SysCan
			, CASE WHEN rand_action_Pipeline > cum_bb_enter_SysCan_rate AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate) THEN 1 ELSE 0 END AS BB_CusCan
			, CASE WHEN rand_action_Pipeline > (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate) THEN 1 ELSE 0 END AS BB_HM
			, CASE WHEN rand_action_Pipeline > (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate + cum_bb_enter_3rd_party_rate) THEN 1 ELSE 0 END AS BB_3rd_Party
		INTO #Pipeline_Rank
		FROM Forecast_Loop_Table_2;

	COMMIT WORK;

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 2 ' TO client;

	CREATE hg INDEX idx_1 ON #Pipeline_Rank (account_number);

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_SysCan = 1
		, churn_type = 'SysCan'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_SysCan = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_CusCan = 1
		, churn_type = 'CusCan'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_CusCan = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_HM = 1
		, churn_type = 'HM'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_HM = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_3rd_Party = 1
		, churn_type = '3rd Party'
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank AS b ON b.account_number = a.account_number AND b.BB_3rd_Party = 1;

	DROP TABLE #Pipeline_Rank;
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 3 ' TO client;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_Offer_Applied = 1
	WHERE bb_status_code = 'AC' AND bb_syscan = 0 AND BB_CusCan = 0 AND BB_HM = 0 AND BB_3rd_party = 0 AND rand_BB_Offer_Applied <= pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend AND end_date BETWEEN curr_offer_start_date_bb AND curr_offer_end_date_intended_bb;

	---??? I believe this should hold the offer BB_Offer_Applied_rate for statuses that stay at AC - therefore need to alter the definition in the pipeline rate proc to capture the BB_Offer_Applied only for these
	---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
	--- but this will give the intraweek percentiles in a format we can use:
	---??? code starts here:
	SELECT churn_type
		, CASE WHEN status_code IN ('AB', 'BCRQ', 'PC') THEN 'PL' ELSE status_code END AS pseudo_status
		, CASE WHEN next_status_code IN ('AB', 'PC', 'BCRQ') THEN 'PL' ELSE next_status_code END AS next_pseudo_status
		, AB_ReAC_offer_applied
		, sum(ABs) AS Cnt
		, Row_number() OVER (PARTITION BY Churn_type ORDER BY pseudo_status ASC) AS Row_ID
	INTO #im
	FROM (SELECT *
		FROM Intrawk_AB_Pct
		WHERE next_status_code NOT IN ('AP')
				UNION
		SELECT *
		FROM Intrawk_PC_Pct
		WHERE next_status_code NOT IN ('AP')
				UNION
		SELECT *
		FROM Intrawk_BCRQ_Pct
		WHERE next_status_code NOT IN ('AP')
		) AS x
	GROUP BY churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied
	ORDER BY churn_type ASC
		, pseudo_status ASC
		, next_pseudo_status ASC
		, AB_ReAC_offer_applied ASC;

	SELECT Row_ID
		, churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied
		, cnt
		, SUM(cnt) OVER (PARTITION BY churn_type ORDER BY Row_ID ASC) AS acum_abs
		, SUM(cnt) OVER (PARTITION BY churn_type) AS acum_abs1
		, cast(acum_abs AS REAL) / acum_abs1 AS prob
	INTO #t1
	FROM #im;
	
	DROP TABLE #im;

	SELECT t1.churn_type
		, t1.pseudo_status
		, t1.next_pseudo_status
		, t1.AB_ReAC_offer_applied AS ReAC_Offer_Applied
		, t1.cnt
		, t1.acum_abs
		, t1.acum_abs1
		, COALESCE(t2.prob, 0) AS Lower
		, t1.prob AS UPPER
	INTO TP_intraweek_movements
	FROM #t1 AS t1
	LEFT JOIN #t1 AS t2 ON t1.row_id = t2.row_id + 1 AND t1.Churn_type = t2.Churn_type;

	DROP TABLE #t1; 
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 4 ' TO client;

		UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM TP_intraweek_movements
		WHERE churn_type = 'SysCan'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_SysCan > 0;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM TP_intraweek_movements
		WHERE churn_type = 'CusCan'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_CusCan > 0;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM TP_intraweek_movements
		WHERE churn_type = 'HM'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_HM > 0;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = COALESCE(AB.ReAC_Offer_Applied, 0)
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN (
		SELECT *
		FROM TP_intraweek_movements
		WHERE churn_type = '3rd Party'
		) AS AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_3rd_party > 0;

	--?? final update is that if eow status code is set to PL then set the eow status to a real pipeline status (doesn't really matter which one ) - signifying that they are still in the pipeline
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = CASE WHEN BB_SysCan = 1 THEN 'AB' ELSE 'PC' END
	FROM Forecast_Loop_Table_2 AS base
	WHERE base.BB_status_code_EoW = 'PL';

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 5 ' TO client;

	---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
	--- but this will give the churn week status movement percentiles in a format we can use:
	---??? code starts here:
	SELECT churn_type
		, Wks_to_intended_churn
		, CASE WHEN Status_Code_EoW IN ('AB', 'PC', 'BCRQ') THEN 'PL' ELSE Status_Code_EoW END AS next_pseudo_status_EoW
		, ReAC_offer_applied
		, sum(Cnt) AS Cnt
		, Row_number() OVER (PARTITION BY Churn_type, Wks_to_intended_churn ORDER BY next_pseudo_status_EoW ASC , ReAC_offer_applied ASC) AS Row_ID
	INTO #wm
	FROM (
		SELECT * FROM PC_PL_Status_Change_Dist
		UNION
		SELECT * FROM AB_PL_Status_Change_Dist
		) AS x
	--??? extend this bad status filter?
	WHERE (status_code_eow NOT IN ('PA') 
			OR (Wks_to_intended_churn = 'Churn in next 1 wks' AND status_code_eow NOT IN ('AB', 'PC', 'BCRQ')))
	GROUP BY churn_type
		, next_pseudo_status_EoW
		, ReAC_offer_applied
		, Wks_to_intended_churn
	ORDER BY churn_type ASC
		, Wks_to_intended_churn ASC
		, next_pseudo_status_EoW ASC
		, ReAC_offer_applied ASC;

	SELECT Row_ID
		, churn_type
		, Wks_to_intended_churn
		, next_pseudo_status_EoW
		, ReAC_offer_applied
		, cnt
		, SUM(cnt) OVER (PARTITION BY churn_type, Wks_to_intended_churn ORDER BY Row_ID ASC) AS acum_abs
		, SUM(cnt) OVER (PARTITION BY churn_type, Wks_to_intended_churn) AS acum_abs1
		, cast(acum_abs AS REAL) / acum_abs1 AS prob
	INTO #t2
	FROM #wm;

	DROP TABLE #wm;
	
	SELECT t1.churn_type
		, t1.Wks_to_intended_churn
		, t1.next_pseudo_status_EoW
		, t1.ReAC_offer_applied
		, t1.cnt
		, t1.acum_abs
		, t1.acum_abs1
		, COALESCE(t2.prob, 0) AS Lower_
		, t1.prob AS UPPER_
	INTO weekly_movements
	FROM #t2 AS t1
	LEFT JOIN #t2 AS t2 ON t1.row_id = t2.row_id + 1 AND t1.Churn_type = t2.Churn_type AND t1.Wks_to_intended_churn = t2.Wks_to_intended_churn;
	
	DROP TABLE #t2;
	---??? code ends here
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 6 ' TO client;

	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = PC.next_pseudo_status_EoW
		, BB_Offer_Applied = ReAC_offer_applied
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN weekly_movements AS PC ON base.rand_BB_pipeline_Status_Change BETWEEN PC.lower_ AND PC.upper_ 
														AND trim(base.churn_type) = pc.churn_type 
														AND CASE 	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 0 THEN 'Churn in next 1 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 1 THEN 'Churn in next 2 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 2 THEN 'Churn in next 3 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 3 THEN 'Churn in next 4 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 4 THEN 'Churn in next 5 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 >= 5 THEN 'Churn in next 6+ wks' END = PC.Wks_To_Intended_Churn
	WHERE BB_Status_Code IN ('PC', 'BCRQ', 'AB') 
		AND bb_syscan = 0 AND BB_CusCan = 0 AND BB_HM = 0 AND BB_3rd_party = 0 AND trim(base.churn_type) IN ('CusCan', 'HM', '3rd Party');

	-- ??? does this make sense to change like this?  -- is this correct?
	---??? need a join to make the cuscan, syscan, 3rd party, HM all join to the right sections.  If we do ths we can get rid of the section below
	---??? check all the names in the code above!
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = AB.next_pseudo_status_EoW
		, BB_Offer_Applied = AB.ReAC_offer_applied
	FROM Forecast_Loop_Table_2 AS base
	INNER JOIN weekly_movements AS AB ---??? obviously need this table
		ON base.rand_BB_pipeline_Status_Change BETWEEN AB.lower_ AND AB.upper_ AND trim(base.churn_type) = ab.churn_type 
														AND CASE 	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 0 THEN 'Churn in next 1 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 1 THEN 'Churn in next 2 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 2 THEN 'Churn in next 3 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 3 THEN 'Churn in next 4 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 4 THEN 'Churn in next 4 wks'
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 5 THEN 'Churn in next 6 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 6 THEN 'Churn in next 7 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 7 THEN 'Churn in next 8 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 = 8 THEN 'Churn in next 9 wks' 
																	WHEN datediff(day, base.End_Date, base.PL_Future_Sub_Effective_Dt) / 7 >= 9 THEN 'Churn in next 10+ wks' END = AB.Wks_To_Intended_Churn
	WHERE BB_Status_Code IN ('PC', 'BCRQ', 'AB') 
			AND trim(base.churn_type) IN ('SysCan') 
			AND bb_syscan = 0 
			AND BB_CusCan = 0 
			AND BB_HM = 0 
			AND BB_3rd_party = 0;

	--?? final update is that if eow status code is set to PL then set the eow status to the current status (i.e. nothing has changed)
	UPDATE Forecast_Loop_Table_2 AS base
	SET BB_Status_Code_EoW = CASE WHEN BB_SysCan = 1 THEN 'AB' WHEN BB_CusCan = 1 OR BB_HM = 1 OR BB_3rd_Party = 1 THEN 'PC' ELSE BB_Status_Code END
	FROM Forecast_Loop_Table_2 AS base
	WHERE base.BB_status_code_EoW = 'PL';

	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 7 ' TO client;

	---??? check all the names in the code above!
	UPDATE Forecast_Loop_Table_2 AS base
	SET CusCan = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = 'CusCan';

	--	AND BB_CusCan > 0;
	UPDATE Forecast_Loop_Table_2 AS base
	SET SysCan = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = 'SysCan';

	--	AND BB_SysCan > 0;
	UPDATE Forecast_Loop_Table_2 AS base
	SET HM = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = 'HM';

	--AND BB_HM > 0;
	UPDATE Forecast_Loop_Table_2 AS base
	SET _3rd_Party = 1
	WHERE BB_Status_Code_EoW = 'CN' AND trim(churn_type) = '3rd Party'
		--	AND BB_3rd_party > 0;
		/*xx*/
END
GO


