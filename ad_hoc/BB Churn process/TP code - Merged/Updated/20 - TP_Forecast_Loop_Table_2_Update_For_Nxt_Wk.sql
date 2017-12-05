CREATE OR REPLACE PROCEDURE TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk AS

BEGIN
	--------------------------------------------------------------------------
	-- Update table for start of next loop -----------------------------------
	--------------------------------------------------------------------------
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Initializing' TO client

	-- set the expected churn date for non-syscan pipeline entries based on previous experience 	
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, base.end_date + dur.Days_To_churn)
	FROM TP_ForeCAST_Loop_Table_2 AS base
	INNER JOIN TP_DTV_PC_Duration_Dist AS dur ON rand_BB_NotSysCan_Duration BETWEEN dur.PC_Days_Lower_Prcntl AND dur.PC_Days_Upper_Prcntl
	WHERE (BB_3rd_Party > 0 OR BB_CusCan > 0 OR BB_HM > 0) AND base.BB_Status_Code_EoW = 'PC' 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for non-syscan PL entries ' || @@rowcount TO client

	-- set the expected churn date to be 50 days on for SysCan 	  
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, base.end_date + 65)
	FROM TP_ForeCAST_Loop_Table_2 AS base
	WHERE BB_SysCan > 0 AND base.BB_Status_Code_EoW IN ('AB', 'BCRQ') 
	
	-- set the expected churn date to be 65 days on for SysCan 	  
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = convert(DATE, CASE WHEN BB_Status_Code_EoW = 'AB' THEN base.end_date + 65 ELSE base.end_date + 14 END )
	FROM TP_ForeCAST_Loop_Table_2 AS base
	WHERE BB_SysCan > 0 AND base.BB_Status_Code_EoW IN ('AB', 'BCRQ') 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for syscan PL entries ' || @@rowcount TO client
	
	--- Setting next curr_offer_start_date_BB
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET curr_offer_start_date_BB = end_date + 3
		, Curr_Offer_end_Date_Intended_BB = dateadd(month, Total_Offer_Duration_Mth, end_date + 3)
	FROM TP_ForeCAST_Loop_Table_2 AS base
	INNER JOIN TP_Offer_Applied_Dur_Dist AS offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound AND Offer_Segment = 'Other' ---??? check where we get this table from
	WHERE BB_Offer_Applied = 1 AND NOT (BB_Status_Code IN ('AB', 'PC', 'BCRQ') AND BB_Status_Code_EoW = 'AC') 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Active: Updating curr_offer_start_date_BB DONE ' || @@rowcount TO client

	--- Setting next curr_offer_start_date_BB
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET curr_offer_start_date_BB = end_date + 3
		, Curr_Offer_end_Date_Intended_BB = dateadd(month, Total_Offer_Duration_Mth, end_date + 3)
	FROM -- Default 10m offer
		TP_ForeCAST_Loop_Table_2 AS base
	INNER JOIN TP_Offer_Applied_Dur_Dist AS offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound AND Offer_Segment = 'Reactivations' ---??? check where we get this table from
	WHERE BB_Offer_Applied = 1 AND BB_Status_Code IN ('AB', 'PC', 'BCRQ') 
				AND BB_Status_Code_EoW = 'AC' 
				
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Reactivations: Updating curr_offer_start_date_BB DONE ' || @@rowcount TO client 
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating curr_offer_start_date_BB DONE' TO client

	--- Setting next BB_status code 
	UPDATE TP_ForeCAST_Loop_Table_2
	SET BB_Status_Code = Coalesce(BB_Status_Code_EoW, BB_Status_Code) 
	
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating status code ' || @@rowcount TO client

	--- Clearing not pipeline accounts Future effective dt
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET PL_Future_Sub_Effective_Dt = NULL ---?? note changed name here
	WHERE BB_Status_Code NOT IN ('PC', 'AB', 'BCRQ') 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Clear the churn date for non PL ' || @@rowcount TO client

	--- Clearing the pipeline entry status codes
	UPDATE TP_ForeCAST_Loop_Table_2 AS base
	SET BB_SysCan = 0
		, BB_CusCan = 0
		, BB_HM = 0
		, BB_3rd_Party = 0

	--	BB_offer_applied = 0   -- set BB offer applied back to zero after triggering
	--- Updating organic growth variables
	UPDATE TP_ForeCAST_Loop_Table_2
	SET end_date = end_date + 7
		, Talk_tenure_raw = Talk_tenure_raw + 7
		, DTV_Activation_Type = NULL
		, weekid = weekid + 1

	--- Setting offer end date when expiration date happen in the previous week
	UPDATE TP_ForeCAST_Loop_Table_2
	SET Prev_offer_end_date_BB = Curr_Offer_end_Date_Intended_BB
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 1/3' TO client

	--- Clearing Offer end date when curr offer ended on the previous week
	UPDATE TP_ForeCAST_Loop_Table_2
	SET Curr_Offer_end_Date_Intended_BB = NULL
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date

	UPDATE TP_ForeCAST_Loop_Table_2
	SET BB_offer_applied = 0
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date

	--- 
	UPDATE TP_ForeCAST_Loop_Table_2
	SET Prev_offer_end_date_BB = NULL
	WHERE Prev_offer_end_date_BB < (end_date) - 53 * 7 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 2/3' TO client

	DECLARE @dt DATE 
	SET @dt = COALESCE ((SELECT MIN (end_date) FROM TP_FORECAST_Looped_Sim_Output_Platform) , (SELECT min(end_date) FROM TP_ForeCAST_Loop_Table_2) )
			
		
		message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating my_sky_login_3m_raw ' TO client

	SELECT base.account_number
		, base.end_Date
		, 'visit_days' = SUM(visit)
	INTO #days_visited_3m_2
	FROM (SELECT account_number
			, visit_date
			, 'visit' = 1
		FROM vespa_shared.mysky_daily_usage
		WHERE visit_date <= @dt
		UNION
		SELECT account_number
			, end_date
			, my_sky_login_LW
		FROM TP_FORECAST_Looped_Sim_Output_Platform
		) AS v
	INNER JOIN TP_ForeCAST_Loop_Table_2 AS base ON BASE.account_number = v.account_number
	WHERE visit_date BETWEEN DATEADD(wk, 1, DATEADD(mm, - 3, end_date)) AND end_date
	GROUP BY base.account_number
		, base.end_date 
		
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - days_visited_3m_2:  ' || @@rowcount TO client

	----------------------------------------------------------------------	
	COMMIT WORK
	CREATE hg INDEX ID1 ON #days_visited_3m_2 (account_number)
	CREATE DATE INDEX ID2 ON #days_visited_3m_2 (end_date)
	CREATE lf INDEX ID3 ON #days_visited_3m_2 (visit_days)
	COMMIT WORK

	--- Updating CALLS					
	SELECT base.account_number
		, 'call_count' = SUM(calls)
		, end_date
	INTO #BBCalls_Temp_1m_2
	FROM (SELECT account_number
			, call_date
			, 'calls' = COUNT(1)
		FROM cust_inbound_calls AS a
		INNER JOIN (SELECT 'min_dt' = DATEADD(month, - 13, MIN(end_date)) FROM TP_ForeCAST_Loop_Table_2) AS b ON a.call_date >= b.min_dt 
		WHERE 	call_date <= @dt
				AND contact_activity = 'Inbound' 
				AND service_call_type IN (
				'SCT_CUSSER_BBusage', 'SCT_SALOLY_EOODirect_TP', 'SCT_SALRET_BB_Campaign2', 'SCT_SALRET_BB_Churn', 'SCT_SALRET_BB_MAC', 'SCT_SALRET_BB_Online', 'SCT_SALRET_BB_PIPELINE', 'SCT_SALRET_BB_TA', 'SCT_SALRET_BB_TA_Xfer'
				, 'SCT_SALRET_BB_TVWinback', 'SCT_SALRET_BB_Value', 'SCT_SALRET_BB_Value_SA', 'SCT_SALRET_BB_Value_SA_Xfer', 'SCT_SALRET_BB_Value_Xfer', 'SCT_SALRET_BB_Value2', 'SCT_SALRET_ELP_BB', 'SCT_SALTRN_BB_TA_Xfer', 'SCT_SALRET_BB_Campaign1'
				, 'SCT_SALRET_BB_HighChurn', 'SCT_SALRET_BB_Value_D&G', 'SCT_SALRET_BB_HighChurn_Xfer', 'SCT_CUSSER_BBusage', 'SCT_SALOLY_EOODirect_TP', 'SCT_SALRET_BB_Campaign2', 'SCT_SALRET_BB_Churn', 'SCT_SALRET_BB_MAC', 'SCT_SALRET_BB_Online'
				, 'SCT_SALRET_BB_PIPELINE', 'SCT_SALRET_BB_TA', 'SCT_SALRET_BB_TA_Xfer', 'SCT_SALRET_BB_TVWinback', 'SCT_SALRET_BB_Value', 'SCT_SALRET_BB_Value_SA', 'SCT_SALRET_BB_Value_SA_Xfer', 'SCT_SALRET_BB_Value_Xfer', 'SCT_SALRET_BB_Value2'
				, 'SCT_SALRET_ELP_BB', 'SCT_SALTRN_BB_TA_Xfer', 'SCT_SALRET_BB_Campaign1', 'SCT_SALRET_BB_HighChurn', 'SCT_SALRET_BB_Value_D&G', 'SCT_SALRET_BB_HighChurn_Xfer', 'SCT_HLPALL_NowTV_Cancel_Xfer', 'SCT_SALRET_ELP_Xfer', 'SCT_SALTRN_BB_TA_Xfer'
				, 'SCT_SALRET_BB_Value_SA_Xfer', 'SCT_SALVAL_BB_Syscan', 'SCT_SALRET_BB_Campaign3', 'SCT_HLPTV__PriceTalk_AVS', 'SCT_HLPTV__PriceTalk_TO', 'SCT_OTHCTT_DN1', 'SCT_SALRET_PriceTalk', 'Support Broadband and Talk', 'SCT_WELBBT_Fibre', 'SCT_WELBBT_Fibre_Engineer'
				, 'SCT_WELBBT_Fibre_NL', 'SCT_WELBBT_Fibre_Staff', 'SCT_WELBBT_Fibre_Staff_Xfer', 'SCT_WELBBT_Fibre_Xfer', 'SCT_WELBBT_IncompleteJob', 'SCT_WELBBT_LinePlant_Xfer', 'SCT_WELBBT_MoveHome_Xfer', 'SCT_WELBBT_Nuisance_Xfer', 'SCT_WELBBT_Order', 'SCT_WELBBT_OrderRecovery_Direct'
				, 'SCT_WELBBT_OrderRecovery_Xfer', 'SCT_WELBBT_Order_Engineer', 'SCT_WELBBT_Order_NL', 'SCT_WELBBT_Order_Xfer', 'SCT_WELBBT_Slamming_Direct', 'SCT_WELBBT_Staff_Order', 'SCT_WELBBT_Staff_Order_Xfer', 'SCT_WELBBT_Support_Xfer', 'SCT_WELBBT_TalkTechnical'
				, 'SCT_WELBBT_Technical', 'SCT_WELBBT_Tech_TO', 'SCT_SUPBBT_Case_Broadband', 'SCT_SUPBBT_Case_Broadband_NL', 'SCT_SUPBBT_Case_Talk', 'SCT_SUPBBT_Case_Talk_NL'
				, 'Broadband (One Service)'
				, 'Broadband Escalation (One Service)'
				, 'Complaints Broadband'
				, 'Complaints Broadband (ROI)'
				, 'Complaints Broadband and Talk (MYSKY)'
				, 'Escalation Broadband'
				, 'Escalation Broadband (ROI)'
				, 'EST Broadband and Talk'
				, 'Fibre Broadband'
				, 'General Pool for 16 Olympus Retention'
				, 'General Pool for 17 Pro Broadband'
				, 'Help and Troubleshooting (Broadband)'
				, 'Help and Troubleshooting Broadband / Talk (ROI)'
				, 'Moving Home Talk / Broadband (ROI)'
				, 'Pro Broadband'
				, 'Product Information Broadband / Talk (ROI)'
				, 'Product Missold Broadband and Talk'
				, 'SCT_CUSDBT_BBTech'
				, 'SCT_CUSDBT_Spin_BBTech'
				, 'SCT_CUSSER_BBusage'
				, 'SCT_DIALLER_CAM_DIGEXP_BBT'
				, 'SCT_DIALLER_CAM_ONEEXP_BBT'
				, 'SCT_DIALLER_CAM_ONEEXP_BBTPlus'
				, 'SCT_DIALLER_CAM_OSSEXP_BBT_Help'
				, 'SCT_DIALLER_CAM_OSSEXP_BBT_Welcome'
				, 'SCT_DIALLER_CAM_OSSEXP_HM_BBT'
				, 'SCT_DIGEXP_BBT_Fibre_Xfer'
				, 'SCT_DIGEXP_BBT_Xfer'
				, 'SCT_ESCCOM_Escalation_BBT_Xfer'
				, 'SCT_ESCCOM_LeaderSupport_BBT_Xfer'
				, 'SCT_HLPBBT_Alarm'
				, 'SCT_HLPBBT_BB_Engineer'
				, 'SCT_HLPBBT_BB_Engineer_NL'
				, 'SCT_HLPBBT_BB_Online'
				, 'SCT_HLPBBT_BB_Online_NL'
				, 'SCT_HLPBBT_BB_Router'
				, 'SCT_HLPBBT_BB_Router_NL'
				, 'SCT_HLPBBT_BB_Technical'
				, 'SCT_HLPBBT_BB_Technical_HSS'
				, 'SCT_HLPBBT_BB_Technical_NL'
				, 'SCT_HLPBBT_BB_Technical_TO'
				, 'SCT_HLPBBT_BB_Tech_HSS_TO'
				, 'SCT_HLPBBT_BB_Tech_Xfer'
				, 'SCT_HLPBBT_ClosedOutage'
				, 'SCT_HLPBBT_Fibre_D&G'
				, 'SCT_HLPBBT_Fibre_Xfer'
				, 'SCT_HLPBBT_Fix_Xfer'
				, 'SCT_HLPBBT_Main_TO'
				, 'SCT_HLPBBT_PDS_Xfer'
				, 'SCT_HLPBBT_Pro_Case'
				, 'SCT_HLPBBT_Pro_Tech_BB'
				, 'SCT_HLPBBT_Pro_Tech_Comb'
				, 'SCT_HLPBBT_Pro_Tech_Talk'
				, 'SCT_HLPBBT_Pro_Tech_Xfer'
				, 'SCT_HLPBBT_Pro_Upg_BB'
				, 'SCT_HLPBBT_Pro_Upg_BB_TO'
				, 'SCT_HLPBBT_Pro_Upg_Talk'
				, 'SCT_HLPBBT_Pro_WebHost'
				, 'SCT_HLPBBT_ST_Tech_Xfer'
				, 'SCT_HLPBBT_TalkTechnical'
				, 'SCT_HLPBBT_Talk_Engineer'
				, 'SCT_HLPBBT_Talk_Tarriff'
				, 'SCT_HLPBBT_Talk_Tarriff_NL'
				, 'SCT_HLPBBT_Talk_Technical'
				, 'SCT_HLPBBT_Talk_Technical_HSS'
				, 'SCT_HLPBBT_Talk_Technical_NL'
				, 'SCT_HLPBBT_Talk_Tech_HSS_TO'
				, 'SCT_HLPBBT_Talk_Tech_TO'
				, 'SCT_HLPBBT_Technical'
				, 'SCT_HLPBBT_Tech_Connect'
				, 'SCT_HLPBBT_Tech_Connect_NL'
				, 'SCT_HLPBBT_Tech_Fibre'
				, 'SCT_HLPBBT_Tech_Fibre_NL'
				, 'SCT_HLPBBT_Tech_NL_FB'
				, 'SCT_HLPBBT_Tech_TO'
				, 'SCT_ONEEXP_BBT'
				, 'SCT_ONEEXP_BBTPlus_Xfer'
				, 'SCT_ONEEXP_BBT_Xfer'
				, 'SCT_OSSEXP_BBT'
				, 'SCT_OSSEXP_BBT_APP'
				, 'SCT_OSSEXP_BBT_Help'
				, 'SCT_OSSEXP_BBT_Help_Xfer'
				, 'SCT_OSSEXP_BBT_Welcome_Xfer'
				, 'SCT_OSSEXP_HM_BBT_Xfer'
				, 'SCT_REPEXR_BBST'
				, 'SCT_REPEXR_BBST_Order'
				, 'SCT_REPEXR_BBST_Order_TO'
				, 'SCT_REPEXR_BBST_TO'
				, 'SCT_REPHLP_BBST'
				, 'SCT_REPHLP_BBST_Direct'
				, 'SCT_REPHLP_BBST_TO'
				, 'SCT_REPHLP_BBST_Xfer'
				, 'SCT_REPHLP_Fibre'
				, 'SCT_REPWEL_BBST'
				, 'SCT_REPWEL_BBST_TO'
				, 'SCT_REPWEL_Fibre'
				, 'SCT_SALATT_Olympus_Direct'
				, 'SCT_SALATT_Olympus_Redirect'
				, 'SCT_SALATT_Olympus_Xfer   '
				, 'SCT_SALEXC_BB'
				, 'SCT_SALEXC_BBFF'
				, 'SCT_SALEXC_BBMAC'
				, 'SCT_SALEXC_BBMAC_Xfer'
				, 'SCT_SALEXC_BBNLP'
				, 'SCT_SALEXC_BBNoLR'
				, 'SCT_SALEXC_BBPreActive'
				, 'SCT_SALEXC_BB_Xfer'
				, 'SCT_SALEXC_Fibre'
				, 'SCT_SALEXC_Olympus'
				, 'SCT_SALEXC_ROI_BBT_Upgrades'
				, 'SCT_SALEXC_ROI_SwitcherBB'
				, 'SCT_SALOLY_EOODDR_CAN_TP'
				, 'SCT_SALOLY_EOODDR_DGBT_TP'
				, 'SCT_SALOLY_EOODirect_TP   '
				, 'SCT_SALOLY_Olympus_Xfer'
				, 'SCT_SALPAT_ROI_BB'
				, 'SCT_SALPAT_ROI_BB_Xfer'
				, 'SCT_SALPAT_ROI_Fibre_Direct '
				, 'SCT_SALRET_BB_Campaign1'
				, 'SCT_SALRET_BB_Campaign2'
				, 'SCT_SALRET_BB_Campaign3'
				, 'SCT_SALRET_BB_Churn'
				, 'SCT_SALRET_BB_HighChurn'
				, 'SCT_SALRET_BB_HighChurn_Xfer'
				, 'SCT_SALRET_BB_MAC'
				, 'SCT_SALRET_BB_Online'
				, 'SCT_SALRET_BB_PIPELINE'
				, 'SCT_SALRET_BB_TA'
				, 'SCT_SALRET_BB_TA_Xfer'
				, 'SCT_SALRET_BB_TVWinback'
				, 'SCT_SALRET_BB_Value'
				, 'SCT_SALRET_BB_Value2'
				, 'SCT_SALRET_BB_ValueBill'
				, 'SCT_SALRET_BB_ValueBill_TO'
				, 'SCT_SALRET_BB_Value_D&G'
				, 'SCT_SALRET_BB_Value_SA'
				, 'SCT_SALRET_BB_Value_SA_Xfer'
				, 'SCT_SALRET_BB_Value_Xfer'
				, 'SCT_SALRET_ELP_BB'
				, 'SCT_SALRTM_BBINFO'
				, 'SCT_SALRTM_SHMS_Olympus'
				, 'SCT_SALTRN_BB_HighChurn'
				, 'SCT_SALTRN_BB_HighChurn_Xfer'
				, 'SCT_SALTRN_BB_TA_Xfer'
				, 'SCT_SALVAL_BB_Syscan'
				)
		GROUP BY account_number
			, call_date
		
		UNION
		
		SELECT account_number
			, end_date
			, calls_LW
		FROM TP_FORECAST_Looped_Sim_Output_Platform
		WHERE calls_LW > 0
		) AS TEMP
	INNER JOIN TP_ForeCAST_Loop_Table_2 AS base ON base.account_number = TEMP.account_number
	WHERE call_date BETWEEN DATEADD(week, 1, DATEADD(mm, - 1, end_date)) AND end_date
	GROUP BY base.account_number
		, end_date 
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - BBCalls_Temp_1m_2:  ' || @@rowcount TO client

	COMMIT WORK

	CREATE hg INDEX ID1 ON #BBCalls_Temp_1m_2 (account_number)
	CREATE DATE INDEX ID2 ON #BBCalls_Temp_1m_2 (end_date)
	CREATE lf INDEX ID3 ON #BBCalls_Temp_1m_2 (call_count)
	COMMIT WORK
	
	------------------------------------------------------------------------------	------------------------------------------------------------------------------
	SELECT account_number
		, SUM(Num_past1m_TA) AS DTV_count
		, end_date
	INTO  #TV_Temp_1m_2
	FROM   (SELECT base.account_number
				,  base.end_date
				,  SUM(ta_c.total_calls) AS Num_past1m_TA
			INTO #ta_previous
			FROM TP_ForeCAST_Loop_Table_2 	AS base
			JOIN citeam.view_cust_calls_hist 				AS ta_c		ON base.account_number = ta_c.account_number 
															AND ta_c.event_dt BETWEEN DATEADD(WEEK,-1,base.end_date) AND base.end_date 
															AND ta_c.DTV = 1 
															AND ta_c.typeofevent IN ('TA') 
															AND ta_c.event_dt <= @dt
			GROUP BY base.account_number, base.end_date
			UNION 
			SELECT account_number
					, end_date
					, DTV_LW
				FROM TP_FORECAST_Looped_Sim_Output_Platform
				WHERE DTV_LW > 0
				) AS TEMP
	WHERE end_date BETWEEN DATEADD(week, 1, DATEADD(mm, - 1, end_date)) AND end_date
	GROUP BY account_number, end_date 

	COMMIT WORK

	CREATE hg INDEX ID1 ON #TV_Temp_1m_2 (account_number)
	CREATE DATE INDEX ID2 ON #TV_Temp_1m_2 (end_date)
	CREATE lf INDEX ID3 ON #TV_Temp_1m_2 (DTV_count)
	COMMIT WORK
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - TV_Temp_1m_2:  ' || @@rowcount TO client
	
	------------------------------------------------------------------------------	------------------------------------------------------------------------------

	

	SELECT b.account_number
		, b.segment_TP
		, 'L_12' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 12, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.mysky ELSE 0 END)		-- Max Login in the past 12 month
		, 'L_9' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 9, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.mysky ELSE 0 END) -- Max Login in the past 9 month
		, 'L_6' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 6, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.mysky ELSE 0 END) -- Max Login in the past 6 month
		, 'L_3' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 3, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.mysky ELSE 0 END)
		
		, 'C_12' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 12, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) -- Max Login in the past 12 month
		, 'C_9' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 9, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) -- Max Login in the past 9 month
		, 'C_6' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 6, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) -- Max Login in the past 6 month
		, 'C_3' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 3, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END)
		, 'D_12' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 12, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.DTV_TA_calls_1m_raw ELSE 0 END) -- Max Login in the past 12 month
		, 'D_9' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 9, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.DTV_TA_calls_1m_raw ELSE 0 END) -- Max Login in the past 9 month
		, 'D_6' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 6, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.DTV_TA_calls_1m_raw ELSE 0 END) -- Max Login in the past 6 month
		, 'D_3' = MAX(CASE WHEN a.end_date BETWEEN DATEADD(month, - 3, b.end_date) AND DATEADD(week, - 1, b.end_date) THEN a.DTV_TA_calls_1m_raw ELSE 0 END)
		, 'Login_group' = CASE WHEN L_12 = 0 THEN 1 WHEN L_9 = 0 THEN 2 WHEN L_6 = 0 THEN 3 WHEN L_3 = 0 THEN 4 ELSE 5 END
		, 'Call_group' = CASE WHEN C_12 = 0 THEN 1 WHEN C_9 = 0 THEN 2 WHEN C_6 = 0 THEN 3 WHEN C_3 = 0 THEN 4 ELSE 5 END
		, 'DTV_Call_group' = CASE WHEN D_12 = 0 THEN 1 WHEN D_9 = 0 THEN 2 WHEN D_6 = 0 THEN 3 WHEN D_3 = 0 THEN 4 ELSE 5 END
		, 'Rand_Login' = convert(REAL, NULL)
		, 'Rand_call' = convert(REAL, NULL)
		, 'Rand_DTV' = convert(REAL, NULL)
		--, RANK() OVER (PARTITION BY b.account_number ORDER BY a.end_Date DESC) AS rankk
	INTO #t_prob
	FROM TP_ForeCAST_Loop_Table_2 AS b
	JOIN t_int AS a ON a.account_number = b.account_number 
	GROUP BY b.account_number
		, b.segment_TP
	

	
	COMMIT WORK

	UPDATE #t_prob
	SET Rand_LOGIN = RAND((convert(REAL, account_number)) * DATEPART(ms, GETDATE()))
		, Rand_call = RAND((convert(REAL, account_number) * 10) * DATEPART(ms, GETDATE()))

	CREATE hg INDEX ID1 ON #t_prob (account_number)
	CREATE lf INDEX ID2 ON #t_prob (segment_TP)
	CREATE lf INDEX ID3 ON #t_prob (Login_group)
	CREATE lf INDEX ID4 ON #t_prob (Call_group) 
	
	
	message convert (TIMESTAMP , now() ) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - t_prob:  ' || @@rowcount TO client

	UPDATE TP_ForeCAST_Loop_Table_2 AS a
	SET a.my_sky_login_3m_raw = COALESCE(c.Calls_LW, 0) + COALESCE(d.visit_days, 0)
		, a.my_sky_login_LW = COALESCE(c.Calls_LW, 0)
	FROM TP_ForeCAST_Loop_Table_2 AS a
	INNER JOIN #t_prob AS b ON a.account_number = b.account_number
	LEFT JOIN #days_visited_3m_2 AS d ON a.account_number = d.account_number
	LEFT JOIN TP_my_sky_login_prob_TABLE AS c ON b.Login_group = c.Prob_Group AND b.segment_TP = c.segment_TP AND Rand_login BETWEEN Lower_limit AND UPPER_LIMIT 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing TP_ForeCAST_Loop_Table 1/4:  ' || @@rowcount TO client

	UPDATE TP_ForeCAST_Loop_Table_2 AS a
	SET a.BB_all_calls_1m_raw = COALESCE(c.Calls_LW, 0) + COALESCE(d.call_count, 0)
		, a.Calls_LW = COALESCE(c.Calls_LW, 0)
	FROM TP_ForeCAST_Loop_Table_2 AS a
	INNER JOIN #t_prob AS b ON a.account_number = b.account_number
	LEFT JOIN #BBCalls_Temp_1m_2 AS d ON a.account_number = d.account_number
	LEFT JOIN TP_BB_Calls_prob_TABLE AS c ON b.Call_group = c.Prob_Group AND b.segment_TP = c.segment_TP AND Rand_call BETWEEN Lower_limit AND UPPER_LIMIT 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing TP_ForeCAST_Loop_Table_2/4:  ' || @@rowcount TO client 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 3/4' TO client

	UPDATE TP_ForeCAST_Loop_Table_2 AS a
	SET a.DTV_TA_calls_1m_raw = COALESCE(c.Calls_LW, 0) + COALESCE(d.DTV_count, 0)
		, a.DTV_LW = COALESCE(c.Calls_LW, 0)
	FROM TP_ForeCAST_Loop_Table_2 AS a
	INNER JOIN #t_prob AS b ON a.account_number = b.account_number
	LEFT JOIN #TV_Temp_1m_2 AS d ON a.account_number = d.account_number 
	LEFT JOIN TP_DTV_Calls_prob_TABLE AS c ON b.DTV_Call_group = c.Prob_Group AND b.segment_TP = c.segment_TP AND Rand_call BETWEEN Lower_limit AND UPPER_LIMIT 
	
	message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing TP_ForeCAST_Loop_Table_2/2:  ' || @@rowcount TO client message convert(TIMESTAMP, now()) || ' | TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 4/4' TO client

	--- Refreshing binned variables
	UPDATE TP_ForeCAST_Loop_Table_2
	SET my_sky_login_3m = CASE WHEN my_sky_login_3m_raw > 2 THEN 3 ELSE my_sky_login_3m_raw END
		, BB_all_calls_1m = CASE WHEN BB_all_calls_1m_raw = 0 THEN 0 ELSE 1 END
		, DTV_TA_calls_1m = CASE WHEN DTV_TA_calls_1m_raw > 0 THEN 1 ELSE 0 END 
		, talk_tenure = CASE 	WHEN Talk_tenure_raw <= 65 	THEN 1 
							WHEN Talk_tenure_raw <= 203 	THEN 2
							WHEN Talk_tenure_raw <= 351 	THEN 3
							WHEN Talk_tenure_raw <= 512 	THEN 4
							WHEN Talk_tenure_raw <= 699 	THEN 5
							WHEN Talk_tenure_raw <= 932 	THEN 6
							WHEN Talk_tenure_raw <= 1234	THEN 7
							WHEN Talk_tenure_raw <= 1645	THEN 8
							WHEN Talk_tenure_raw <= 2216	THEN 9
							WHEN Talk_tenure_raw > 2216	THEN 10
							ELSE 0 END 

	--- Refreshing nodes and segments
	UPDATE TP_ForeCAST_Loop_Table_2 AS a
	SET TP_forecast_segment = convert(VARCHAR(4), sub_segment)
		, segment_TP = segment
	FROM TP_ForeCAST_Loop_Table_2 AS a
	INNER JOIN BB_TP_Product_Churn_segments_lookup AS b ON a.DTV_TA_calls_1m = b.DTV_TA_calls_1m 
								AND COALESCE(a.RTM , 'UNKNOWN') = b.RTM
								AND a.my_sky_login_3m = b.my_sky_login_3m 
								AND a.Talk_tenure = b.Talk_tenure 
								AND CASE 	WHEN trim(a.simple_segments) IN ('1 Secure') THEN '1 Secure' 
											WHEN trim(a.simple_segments) IN ('2 Start') THEN '2 Start' 
											WHEN trim(a.simple_segments) IN ('3 Stimulate', '2 Stimulate') THEN '3 Stimulate' 
											WHEN trim(a.simple_segments) IN ('4 Support', '3 Support') THEN '4 Support' 
											WHEN trim(a.simple_segments) IN ('5 Stabilise', '4 Stabilise') THEN '5 Stabilise' 
											WHEN trim(a.simple_segments) IN ('6 Suspense', '5 Suspense') THEN '6 Suspense' 
											ELSE 'UNKNOWN' -- ??? check the simple segment coding here that cleans this up, but generally looks ok
											END		 = b.Simple_Segment 
								AND a.BB_all_calls_1m = b.BB_all_calls_1m

	DROP TABLE #days_visited_3m_2
	DROP TABLE #BBCalls_Temp_1m_2
	DROP TABLE #t_prob
END
GO
-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON TP_Forecast_Loop_Table_2_Update_For_Nxt_Wk TO CITeam
GO

