CREATE OR REPLACE PROCEDURE Forecast_SABB_Loop_Table_2_Actions (
		IN Counter INT
		, IN Rate_Multiplier FLOAT
		) 

BEGIN
	DECLARE multiplier BIGINT;
	DECLARE multiplier_2 BIGINT;

	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Loop_Table_2_Actions - Initialising Environment' TO CLIENT;

	SET multiplier = DATEPART(millisecond, now()) + 1;
	SET multiplier_2 = DATEPART(millisecond, now()) + 2;
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
	SET pred_bb_enter_SysCan_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (Cast(counter - 1 AS FLOAT) / 52 + 1), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
									AND d.Metric = 'SysCan Entry';

	------ SysCan cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_SysCan_rate = pred_bb_enter_SysCan_rate + pred_bb_enter_SysCan_YoY_Trend;

	------ CusCan trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_CusCan_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (Cast(counter - 1 AS FLOAT) / 52 + 1), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
									AND d.Metric = 'CusCan Entry';
	------ CusCan cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_CusCan_rate = pred_bb_enter_CusCan_rate + pred_bb_enter_CusCan_YoY_Trend;

	------ HM trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_HM_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (Cast(counter - 1 AS FLOAT) / 52 + 1), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
									AND d.Metric = 'HM Entry';

	------ HM cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_HM_rate = pred_bb_enter_HM_rate + pred_bb_enter_HM_YoY_Trend;

	------ 3rd party trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_bb_enter_3rd_party_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (Cast(counter - 1 AS FLOAT) / 52 + 1), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
									AND d.Metric = '3rd Party Entry';

	------ 3rd party cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_bb_enter_3rd_party_rate = pred_bb_enter_3rd_party_rate + pred_bb_enter_3rd_party_YoY_Trend;

	------ BB offer applied trend ------
	UPDATE Forecast_Loop_Table_2 AS a
	SET pred_BB_Offer_Applied_YoY_Trend = Coalesce(d.Grad_Coeff * 4 * (Cast(counter - 1 AS FLOAT) / 52 + 1), 0)
	FROM Forecast_Loop_Table_2 AS a
	LEFT JOIN Fcast_Regr_Coeffs AS d ON a.sabb_forecast_segment = d.fcast_segment
		--         and d.LV = Forecast_Start_Wk
		AND d.Metric = 'BB Offer Applied';

	------ BB offer applied cum ----
	UPDATE Forecast_Loop_Table_2 AS a
	SET cum_BB_Offer_Applied_rate = pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend;

	----???? it is possible that the trend value is not appropriate in these circumstances and that we should actually be using 
	--- a non trend value like this for the movements into pipeline:
	--- set cum_bb_enter_SysCan_rate = pred_bb_enter_SysCan_rate
	--- we will review at an appropriate time!
	--- don't think we need this section:
	--------------------------------------------------------------------------------------------------------------
	-- TA/WC Volumes, Saves & Offers Applied  --------------------------------------------------------------------
	--------------------------------------------------------------------------------------------------------------
	--??? have deleted this - look at previous code to restore
	--------------------------------------------------------------------------------------------------------------
	-- Pending Cancels -------------------------------------------------------------------------------------------
	--------------------------------------------------------------------------------------------------------------
	--- pred DTV_PC ----
	---??? I don't think we need the conversion rate because we have used the calling rate functionality to drive the conversion to PC so far.  Therefore we use the rates thath come out of there (probably the cum rates)
	---- ??? therefore commenting this out for now
	-- we already have
	-- cum_bb_enter_SysCan_rate	 
	-- cum_bb_enter_CusCan_rate	 
	-- cum_bb_enter_HM_rate	 
	-- cum_bb_enter_3rd_party_rate	 
	-- we will use these going forward ....
	---??? still need to model SkyPlus Saves (wa sin original model next to TA, but is TA now out of scope?)
	---?? BB_Offer_Applied needs work
	UPDATE Forecast_Loop_Table_2
	SET rand_action_Pipeline = CASE WHEN BB_status_code NOT IN ('AB', 'BCRQ', 'PC') THEN 1 ----??? change this CASE to be whatever the scope of the deniminator was when creating the rates
			ELSE NULL END;---??? i'm not sure this correct, but let's get this running through first

	UPDATE Forecast_Loop_Table_2
	SET rand_action_Pipeline = rand(number(*) * multiplier + 4)
	WHERE rand_action_Pipeline IS NULL;

	DROP TABLE IF EXISTS #Pipeline_Rank;
	
	SELECT account_number
		, rand_action_Pipeline
		--,sum(TA_Call_Cust+WC_Call_Cust) over(partition by Syscan_Forecast_segment) SysCan_Seg_CusCan_Actions
		, count(*) OVER (PARTITION BY sabb_forecast_segment) Total_Cust_In_SABB_Segment
		, cast(rank() OVER (PARTITION BY sabb_forecast_segment ORDER BY rand_action_Pipeline) AS FLOAT) AS SABB_Group_rank
		, cast(rank() OVER (PARTITION BY sabb_forecast_segment ORDER BY rand_action_Pipeline) AS FLOAT) / cast(SABB_segment_count AS FLOAT) AS pct_pipeline_count
		, CASE WHEN rand_action_Pipeline <= cum_bb_enter_SysCan_rate * (Total_Cust_In_SABB_Segment / Total_Cust_In_SABB_Segment) THEN 1 ELSE 0 END AS BB_SysCan
		, CASE WHEN rand_action_Pipeline > cum_bb_enter_SysCan_rate * (Total_Cust_In_SABB_Segment / Total_Cust_In_SABB_Segment) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate) THEN 1 ELSE 0 END AS BB_CusCan
		, CASE WHEN rand_action_Pipeline > (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate) * (Total_Cust_In_SABB_Segment / Total_Cust_In_SABB_Segment) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate) THEN 1 ELSE 0 END AS BB_HM
		, CASE WHEN rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate) * (Total_Cust_In_SABB_Segment / Total_Cust_In_SABB_Segment) AND rand_action_Pipeline <= (cum_bb_enter_SysCan_rate + cum_bb_enter_CusCan_rate + cum_bb_enter_HM_rate + cum_bb_enter_3rd_party_rate) THEN 1 ELSE 0 END AS BB_3rd_Party
	INTO #Pipeline_Rank
	FROM Forecast_Loop_Table_2;

	COMMIT;
	CREATE hg INDEX idx_1 ON #Pipeline_Rank (account_number);

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_SysCan = 1
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank b ON b.account_number = a.account_number AND b.BB_SysCan = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_CusCan = 1
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank b ON b.account_number = a.account_number AND b.BB_CusCan = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_HM = 1
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank b ON b.account_number = a.account_number AND b.BB_HM = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_3rd_Party = 1
	FROM Forecast_Loop_Table_2 AS a
	INNER JOIN #Pipeline_Rank b ON b.account_number = a.account_number AND b.BB_3rd_Party = 1;

	UPDATE Forecast_Loop_Table_2 AS a
	SET BB_Offer_Applied = 1
	WHERE churn_type IN ('CusCan') 
		AND bb_status_code IN ('PC', 'BCRQ') 
		AND rand_BB_Offer_Applied <= pred_BB_Offer_Applied_rate + pred_BB_Offer_Applied_YoY_Trend
		
		
		---??? double check what this where clause does  - I think as it stands it creates too many offers. Need something in stead of TA_Call_Cust=0
		--???? REALLY REALLY do need to know what to do here
		;
	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 1/2' TO CLIENT;

	---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
	--- but this will give the intraweek percentiles in a format we can use:
	---??? code starts here:
	SELECT churn_type
		, CASE WHEN status_code IN ('AB', 'BCRQ', 'PC') THEN 'PL' ELSE status_code END AS pseudo_status
		, CASE WHEN next_status_code IN ('AB', 'PC', 'BCRQ') THEN 'PL' ELSE next_status_code END AS next_pseudo_status
		, AB_ReAC_offer_applied
		, sum(ABs) AS Cnt
		, Row_number() OVER (PARTITION BY Churn_type ORDER BY pseudo_status) Row_ID
	INTO #im
	FROM (SELECT * FROM Intrawk_AB_Pct
			UNION
		SELECT * FROM Intrawk_PC_Pct
		UNION 
		SELECT * FROM Intrawk_BCRQ_Pct) x
	GROUP BY churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied
	ORDER BY churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied;

	SELECT Row_ID
		, churn_type
		, pseudo_status
		, next_pseudo_status
		, AB_ReAC_offer_applied
		, cnt
		, SUM(cnt) OVER (PARTITION BY churn_type ORDER BY Row_ID) acum_abs
		, SUM(cnt) OVER (PARTITION BY churn_type) acum_abs1
		, CAST(acum_abs AS FLOAT) / acum_abs1 prob
	INTO #t1
	FROM #im;


	SELECT t1.churn_type
		, t1.pseudo_status
		, t1.next_pseudo_status
		, t1.AB_ReAC_offer_applied AS ReAC_Offer_Applied
		, t1.cnt
		, t1.acum_abs
		, t1.acum_abs1
		, COALESCE(t2.prob, 0) AS Lower
		, t1.prob AS UPPER
	INTO #intraweek_movements
	FROM #t1 t1
	LEFT JOIN #t1 t2 ON t1.row_id = t2.row_id + 1 AND t1.Churn_type = t2.Churn_type;

	----??? code ends here 
	UPDATE Forecast_Loop_Table_2
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = AB.ReAC_Offer_Applied
	FROM Forecast_Loop_Table_2 base
	INNER JOIN (
		SELECT *
		FROM #intraweek_movements
		WHERE churn_type = 'SysCan'
		) AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_SysCan > 0;

	UPDATE Forecast_Loop_Table_2
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = AB.ReAC_Offer_Applied
	FROM Forecast_Loop_Table_2 base
	INNER JOIN (
		SELECT *
		FROM #intraweek_movements
		WHERE churn_type = 'CusCan'
		) AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_CusCan > 0;
	UPDATE Forecast_Loop_Table_2
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = AB.ReAC_Offer_Applied
	FROM Forecast_Loop_Table_2 base
	INNER JOIN (
		SELECT *
		FROM #intraweek_movements
		WHERE churn_type = 'HM'
		) AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_HM > 0;

	UPDATE Forecast_Loop_Table_2
	SET BB_Status_Code_EoW = AB.Next_pseudo_status
		, BB_Offer_Applied = AB.ReAC_Offer_Applied
	FROM Forecast_Loop_Table_2 base
	INNER JOIN (
		SELECT *
		FROM #intraweek_movements
		WHERE churn_type = '3rd Party'
		) AB ON base.rand_Intrawk_BB_SysCan BETWEEN AB.lower AND AB.upper
	WHERE BB_3rd_party > 0;

	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Loop_Table_2_Actions - Checkpoint 2/2' TO CLIENT;
	
	---???? the following code will need to be tidied up and productionised so it works for every date and uses data in the right schema
	--- but this will give the churn week status movement percentiles in a format we can use:
	---??? code starts here:
	SELECT churn_type
		, Wks_to_intended_churn
		, CASE WHEN Status_Code_EoW IN ('AB', 'PC', 'BCRQ') THEN 'PL' ELSE Status_Code_EoW END AS next_pseudo_status_EoW
		, PC_ReAC_offer_applied AS ReAC_offer_applied
		, sum(PCs) AS Cnt
		, Row_number() OVER (PARTITION BY Churn_type, Wks_to_intended_churn ORDER BY next_pseudo_status_EoW, ReAC_offer_applied) Row_ID
	INTO #wm
	FROM (SELECT *
			FROM PC_PL_Status_Change_Dist
			WHERE status_code_eow NOT IN ('PA')) x --??? extend this "bad status" filter?
	GROUP BY churn_type
		, next_pseudo_status_EoW
		, ReAC_offer_applied
		, Wks_to_intended_churn
	ORDER BY churn_type
		, Wks_to_intended_churn
		, next_pseudo_status_EoW
		, ReAC_offer_applied;

	SELECT Row_ID
		, churn_type
		, Wks_to_intended_churn
		, next_pseudo_status_EoW
		, ReAC_offer_applied
		, cnt
		, SUM(cnt) OVER (PARTITION BY churn_type, Wks_to_intended_churn ORDER BY Row_ID) acum_abs
		, SUM(cnt) OVER (PARTITION BY churn_type, Wks_to_intended_churn) acum_abs1
		, CAST(acum_abs AS FLOAT) / acum_abs1 prob
	INTO #t2
	FROM #wm;

	SELECT t1.churn_type
		, t1.Wks_to_intended_churn
		, t1.next_pseudo_status_EoW
		, t1.ReAC_offer_applied
		, t1.cnt
		, t1.acum_abs
		, t1.acum_abs1
		, COALESCE(t2.prob, 0) AS Lower
		, t1.prob AS UPPER
	INTO #weekly_movements
	FROM #t2 t1
	LEFT JOIN #t2 t2 ON t1.row_id = t2.row_id + 1 AND t1.Churn_type = t2.Churn_type AND t1.Wks_to_intended_churn = t2.Wks_to_intended_churn;
	
	
	---??? code ends here
	UPDATE Forecast_Loop_Table_2 base
	SET BB_Status_Code_EoW = PC.next_pseudo_status_EoW
		, BB_Offer_Applied = ReAC_offer_applied
	FROM Forecast_Loop_Table_2 base
	INNER JOIN #weekly_movements PC ---??? obviously need this table
		ON base.rand_BB_PIPELINE_Status_Change BETWEEN PC.lower AND PC.upper AND CASE 
		WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
		WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
		WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
		WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
		WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 4 THEN 'Churn in next 5 wks' 
		WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 >= 5 THEN 'Churn in next 6+ wks'
					--           ??? what are we going to call PL_Future_Sub_Effective_Dt?
			END = PC.Wks_To_Intended_Churn
	WHERE BB_Status_Code IN ('PC', 'BCRQ', 'AB') ---??? not very happy at referring to PC, AB and BCRQ here!
		AND BB_CusCan = 0 AND BB_HM = 0 AND BB_3rd_party = 0;-- ??? does this make sense to change like this?  -- is this correct?
		---??? need a join to make the cuscan, syscan, 3rd party, HM all join to the right sections.  If we do ths we can get rid of the section below
		---??? check all the names in the code above!

	UPDATE Forecast_Loop_Table_2 base
	SET BB_Status_Code_EoW = AB.Status_Code_EoW
		, BB_Offer_Applied = AB.AB_ReAC_Offer_Applied
	FROM Forecast_Loop_Table_2 base
	INNER JOIN AB_PL_Status_Change_Dist AB ---??? obviously need this table
		ON base.rand_BB_PIPELINE_Status_Change BETWEEN AB.AB_Percentile_Lower_Bound AND AB.AB_Percentile_Upper_Bound AND CASE WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 0 THEN 'Churn in next 1 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 1 THEN 'Churn in next 2 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 2 THEN 'Churn in next 3 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 3 THEN 'Churn in next 4 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 4 THEN 'Churn in next 4 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 5 THEN 'Churn in next 6 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 6 THEN 'Churn in next 7 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 7 THEN 'Churn in next 8 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 = 8 THEN 'Churn in next 9 wks' 
																														WHEN (cast(base.PL_Future_Sub_Effective_Dt AS INT) - cast(base.End_Date AS INT)) / 7 >= 9 THEN 'Churn in next 10+ wks'
																																		END = AB.Wks_To_Intended_Churn
	WHERE BB_Status_Code = 'AB' AND BB_SysCan = 0;

	---??? check all the names in the code above!
	UPDATE Forecast_Loop_Table_2 base
	SET BB_CusCan = 1
	WHERE BB_Status_Code_EoW = 'CN' AND BB_Enter_CusCan > 0;

	UPDATE Forecast_Loop_Table_2 base
	SET BB_SysCan = 1
	WHERE BB_Status_Code_EoW = 'CN' AND BB_Enter_SysCan > 0;

	UPDATE Forecast_Loop_Table_2 base
	SET BB_HM = 1
	WHERE BB_Status_Code_EoW = 'CN' AND BB_Enter_HM > 0;

	UPDATE Forecast_Loop_Table_2 base
	SET BB_3rd_Party = 1
	WHERE BB_Status_Code_EoW = 'CN' AND BB_Enter_3rd_party > 0;
	
	MESSAGE CAST(now() as timestamp)||' | Forecast_SABB_Loop_Table_2_Actions - Completed' TO CLIENT;
END;

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON Forecast_SABB_Loop_Table_2_Actions TO CITeam;
