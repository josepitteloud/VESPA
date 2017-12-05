CREATE OR REPLACE PROCEDURE SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk 
AS 
BEGIN
	--------------------------------------------------------------------------
	-- Update table for start of next loop -----------------------------------
	--------------------------------------------------------------------------
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Initializing' TO CLIENT 

	
-- set the expected churn date for non-syscan pipeline entries based on previous experience 	
	Update Forecast_Loop_Table_2 base
Set PL_Future_Sub_Effective_Dt  = Cast(base.end_date + dur.Days_To_churn as date)
from Forecast_Loop_Table_2 base
     inner join
     DTV_PC_Duration_Dist dur
     on rand_BB_NotSysCan_Duration between dur.PC_Days_Lower_Prcntl and dur.PC_Days_Upper_Prcntl
	where (BB_3rd_Party > 0
      or
      BB_CusCan > 0
      or
      BB_HM > 0
      )
      and base.BB_Status_Code_EoW = 'PC' 

	  	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for non-syscan PL entries '||@@rowcount TO CLIENT 

-- set the expected churn date to be 50 days on for SysCan 	  
	Update Forecast_Loop_Table_2 base
	Set PL_Future_Sub_Effective_Dt  = Cast(base.end_date + 50 as date)
	from Forecast_Loop_Table_2 base
	where BB_SysCan > 0
      and base.BB_Status_Code_EoW in ('AB','BCRQ') 

	  	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating churn date for syscan PL entries '||@@rowcount TO CLIENT 	  
	--- Setting next curr_offer_start_date_BB
	UPDATE Forecast_Loop_Table_2 base
	SET   curr_offer_start_date_BB = end_date + 3
		, Curr_Offer_end_Date_Intended_BB = dateadd(month, Total_Offer_Duration_Mth, end_date + 3) -- Default 10m offer
	FROM Forecast_Loop_Table_2 base
	INNER JOIN Offer_Applied_Dur_Dist offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound
	AND Offer_Segment = 'Other' 			---??? check where we get this table from
	WHERE 		BB_Offer_Applied = 1 
			AND not (BB_Status_Code IN ('AB', 'PC', 'BCRQ') 
			AND BB_Status_Code_EoW = 'AC') 
			
MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Active: Updating curr_offer_start_date_BB DONE '||@@rowcount TO CLIENT 
			--- Setting next curr_offer_start_date_BB
	UPDATE Forecast_Loop_Table_2 base
	SET   curr_offer_start_date_BB = end_date + 3
		, Curr_Offer_end_Date_Intended_BB = dateadd(month, Total_Offer_Duration_Mth, end_date + 3) -- Default 10m offer
	FROM Forecast_Loop_Table_2 base
	INNER JOIN Offer_Applied_Dur_Dist offer ON base.rand_New_Off_Dur BETWEEN offer.Dur_Pctl_Lower_Bound AND offer.Dur_Pctl_Upper_Bound 
	AND Offer_Segment = 'Reactivations' 			---??? check where we get this table from
	WHERE 		BB_Offer_Applied = 1 
			AND BB_Status_Code IN ('AB', 'PC', 'BCRQ') 
			AND BB_Status_Code_EoW = 'AC' 
MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Reactivations: Updating curr_offer_start_date_BB DONE '||@@rowcount TO CLIENT 
	
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating curr_offer_start_date_BB DONE' TO CLIENT 

		--- Setting next BB_status code 
	UPDATE Forecast_Loop_Table_2
			SET BB_Status_Code = Coalesce(BB_Status_Code_EoW, BB_Status_Code) 

MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating status code '||@@rowcount TO CLIENT 

		--- Clearing not pipeline accounts Future effective dt
	UPDATE Forecast_Loop_Table_2 base
	SET PL_Future_Sub_Effective_Dt = NULL ---?? note changed name here
	WHERE BB_Status_Code NOT IN ('PC', 'AB', 'BCRQ') 
	
MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Clear the churn date for non PL '||@@rowcount TO CLIENT 
	
			--- Clearing the pipeline entry status codes
	UPDATE Forecast_Loop_Table_2 base
	SET 
	BB_SysCan = 0,
	BB_CusCan = 0,
	BB_HM = 0 ,
	BB_3rd_Party = 0 
	--	BB_offer_applied = 0   -- set BB offer applied back to zero after triggering
	
		--- Updating organic growth variables
	UPDATE Forecast_Loop_Table_2
	SET   end_date 				= end_date + 7
		, BB_tenure_raw 		= BB_tenure_raw +7
		, DTV_Activation_Type 	= NULL
		, weekid 				= weekid + 1 
		
		--- Setting offer end date when expiration date happen in the previous week
	UPDATE Forecast_Loop_Table_2
	SET Prev_offer_end_date_BB = Curr_Offer_end_Date_Intended_BB
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date 
	
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 1/3' TO CLIENT 

		--- Clearing Offer end date when curr offer ended on the previous week
	UPDATE Forecast_Loop_Table_2
	SET Curr_Offer_end_Date_Intended_BB = NULL
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date 
	
	UPDATE Forecast_Loop_Table_2
	SET BB_offer_applied = 0
	WHERE Curr_Offer_end_Date_Intended_BB <= end_date 
		--- 
	UPDATE Forecast_Loop_Table_2
	SET Prev_offer_end_date_BB = NULL
	WHERE Prev_offer_end_date_BB < (end_date) - 53 * 7 
		
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 2/3' TO CLIENT 

		--- Updating Offer Remaining days
	UPDATE Forecast_Loop_Table_2
	SET BB_offer_rem_and_end_raw = CASE WHEN BB_Offer_Applied = 1 THEN DATEDIFF(DAY, end_date, Curr_Offer_end_Date_Intended_BB) 
										ELSE BB_offer_rem_and_end_raw - 7
										END 
	
	-----======== PLACEHOLDERS FOR CALLS AND MY SKY LOGIN
		--- Updating my_sky_login_3m_raw 
		MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updating my_sky_login_3m_raw ' TO CLIENT 
	SELECT base.account_number 
		, base.end_Date
		, SUM(visit) AS visit_days
	INTO #days_visited_3m_2
	FROM 
		(SELECT account_number , visit_date, 1 visit
		FROM vespa_shared.mysky_daily_usage
		UNION 
		SELECT account_number, end_date, my_sky_login_LW
		FROM FORECAST_Looped_Sim_Output_Platform
		)  			AS v
	JOIN Forecast_Loop_Table_2 	AS base ON BASE.account_number = v.account_number
	WHERE visit_date BETWEEN DATEADD(wk, 1, DATEADD(mm,-3,end_date)) AND end_date
	GROUP BY base.account_number
	, base.end_date  
	
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - days_visited_3m_2:  '||@@rowcount TO CLIENT 
	----------------------------------------------------------------------	
		
	COMMIT  
	CREATE HG INDEX ID1 ON #days_visited_3m_2(account_number) 
	CREATE DTTM INDEX ID2 ON #days_visited_3m_2(end_date) 
	CREATE LF INDEX ID3 ON #days_visited_3m_2(visit_days) 
	COMMIT 
	
		--- Updating CALLS					
	SELECT base.account_number
		  , SUM(calls) AS call_count 
		  , end_date
	INTO #BBCalls_Temp_1m_2
	FROM 
		(	SELECT account_number
				,  call_date 
				,  COUNT(1) AS calls
			FROM cust_inbound_calls					AS a
			JOIN (SELECT DATEADD (MONTH, -3 , MIN(end_date)) min_dt FROM Forecast_Loop_Table_2) 	AS b ON a.call_date >=  b.min_dt 
			AND contact_activity = 'Inbound'
			AND   service_call_type IN ('SCT_CUSSER_BBusage','SCT_SALOLY_EOODirect_SABB','SCT_SALRET_BB_Campaign2','SCT_SALRET_BB_Churn'
									 ,'SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer'
									 ,'SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer'
									 ,'SCT_SALRET_BB_Value2','SCT_SALRET_ELP_BB','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_HighChurn'
									 ,'SCT_SALRET_BB_Value_D&G','SCT_SALRET_BB_HighChurn_Xfer','SCT_CUSSER_BBusage','SCT_SALOLY_EOODirect_SABB','SCT_SALRET_BB_Campaign2'
									 ,'SCT_SALRET_BB_Churn','SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer'
									 ,'SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer'
									 ,'SCT_SALRET_BB_Value2','SCT_SALRET_ELP_BB','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_HighChurn','SCT_SALRET_BB_Value_D&G'
									 ,'SCT_SALRET_BB_HighChurn_Xfer','SCT_HLPALL_NowTV_Cancel_Xfer','SCT_SALRET_ELP_Xfer','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Value_SA_Xfer'
									 ,'SCT_SALVAL_BB_Syscan','SCT_SALRET_BB_Campaign3'/* Extras */,'SCT_HLPTV__PriceTalk_AVS','SCT_HLPTV__PriceTalk_TO','SCT_OTHCTT_DN1'
									,'SCT_SALRET_PriceTalk','Support Broadband and Talk','SCT_WELBBT_Fibre','SCT_WELBBT_Fibre_Engineer','SCT_WELBBT_Fibre_NL','SCT_WELBBT_Fibre_Staff'
									,'SCT_WELBBT_Fibre_Staff_Xfer','SCT_WELBBT_Fibre_Xfer','SCT_WELBBT_IncompleteJob','SCT_WELBBT_LinePlant_Xfer','SCT_WELBBT_MoveHome_Xfer'
									,'SCT_WELBBT_Nuisance_Xfer','SCT_WELBBT_Order','SCT_WELBBT_OrderRecovery_Direct','SCT_WELBBT_OrderRecovery_Xfer','SCT_WELBBT_Order_Engineer'
									,'SCT_WELBBT_Order_NL','SCT_WELBBT_Order_Xfer','SCT_WELBBT_Slamming_Direct','SCT_WELBBT_Staff_Order','SCT_WELBBT_Staff_Order_Xfer'
									,'SCT_WELBBT_Support_Xfer','SCT_WELBBT_TalkTechnical','SCT_WELBBT_Technical','SCT_WELBBT_Tech_TO','SCT_SUPBBT_Case_Broadband'
									,'SCT_SUPBBT_Case_Broadband_NL','SCT_SUPBBT_Case_Talk','SCT_SUPBBT_Case_Talk_NL','Broadband (One Service)','Broadband Escalation (One Service)'
									,'Complaints Broadband','Complaints Broadband (ROI)','Complaints Broadband and Talk (MYSKY)','Escalation Broadband','Escalation Broadband (ROI)'
									,'EST Broadband and Talk','Fibre Broadband','General Pool for 16 Olympus Retention','General Pool for 17 Pro Broadband','Help and Troubleshooting (Broadband)'
									,'Help and Troubleshooting Broadband / Talk (ROI)','Moving Home Talk / Broadband (ROI)','Pro Broadband','Product Information Broadband / Talk (ROI)'
									,'Product Missold Broadband and Talk','SCT_CUSDBT_BBTech','SCT_CUSDBT_Spin_BBTech','SCT_CUSSER_BBusage','SCT_DIALLER_CAM_DIGEXP_BBT'
									,'SCT_DIALLER_CAM_ONEEXP_BBT','SCT_DIALLER_CAM_ONEEXP_BBTPlus','SCT_DIALLER_CAM_OSSEXP_BBT_Help','SCT_DIALLER_CAM_OSSEXP_BBT_Welcome','SCT_DIALLER_CAM_OSSEXP_HM_BBT'
									,'SCT_DIGEXP_BBT_Fibre_Xfer','SCT_DIGEXP_BBT_Xfer','SCT_ESCCOM_Escalation_BBT_Xfer','SCT_ESCCOM_LeaderSupport_BBT_Xfer'
									,'SCT_HLPBBT_Alarm','SCT_HLPBBT_BB_Engineer','SCT_HLPBBT_BB_Engineer_NL','SCT_HLPBBT_BB_Online','SCT_HLPBBT_BB_Online_NL'
									,'SCT_HLPBBT_BB_Router','SCT_HLPBBT_BB_Router_NL','SCT_HLPBBT_BB_Technical','SCT_HLPBBT_BB_Technical_HSS','SCT_HLPBBT_BB_Technical_NL'
									,'SCT_HLPBBT_BB_Technical_TO','SCT_HLPBBT_BB_Tech_HSS_TO','SCT_HLPBBT_BB_Tech_Xfer','SCT_HLPBBT_ClosedOutage','SCT_HLPBBT_Fibre_D&G'
									,'SCT_HLPBBT_Fibre_Xfer','SCT_HLPBBT_Fix_Xfer','SCT_HLPBBT_Main_TO','SCT_HLPBBT_PDS_Xfer','SCT_HLPBBT_Pro_Case','SCT_HLPBBT_Pro_Tech_BB'
									,'SCT_HLPBBT_Pro_Tech_Comb','SCT_HLPBBT_Pro_Tech_Talk','SCT_HLPBBT_Pro_Tech_Xfer','SCT_HLPBBT_Pro_Upg_BB','SCT_HLPBBT_Pro_Upg_BB_TO'
									,'SCT_HLPBBT_Pro_Upg_Talk','SCT_HLPBBT_Pro_WebHost','SCT_HLPBBT_ST_Tech_Xfer','SCT_HLPBBT_TalkTechnical','SCT_HLPBBT_Talk_Engineer'
									,'SCT_HLPBBT_Talk_Tarriff','SCT_HLPBBT_Talk_Tarriff_NL','SCT_HLPBBT_Talk_Technical','SCT_HLPBBT_Talk_Technical_HSS','SCT_HLPBBT_Talk_Technical_NL'
									,'SCT_HLPBBT_Talk_Tech_HSS_TO','SCT_HLPBBT_Talk_Tech_TO','SCT_HLPBBT_Technical','SCT_HLPBBT_Tech_Connect','SCT_HLPBBT_Tech_Connect_NL'
									,'SCT_HLPBBT_Tech_Fibre','SCT_HLPBBT_Tech_Fibre_NL','SCT_HLPBBT_Tech_NL_FB','SCT_HLPBBT_Tech_TO','SCT_ONEEXP_BBT','SCT_ONEEXP_BBTPlus_Xfer'
									,'SCT_ONEEXP_BBT_Xfer','SCT_OSSEXP_BBT','SCT_OSSEXP_BBT_APP','SCT_OSSEXP_BBT_Help','SCT_OSSEXP_BBT_Help_Xfer','SCT_OSSEXP_BBT_Welcome_Xfer'
									,'SCT_OSSEXP_HM_BBT_Xfer','SCT_REPEXR_BBST','SCT_REPEXR_BBST_Order','SCT_REPEXR_BBST_Order_TO','SCT_REPEXR_BBST_TO','SCT_REPHLP_BBST'
									,'SCT_REPHLP_BBST_Direct','SCT_REPHLP_BBST_TO','SCT_REPHLP_BBST_Xfer','SCT_REPHLP_Fibre','SCT_REPWEL_BBST','SCT_REPWEL_BBST_TO','SCT_REPWEL_Fibre'
									,'SCT_SALATT_Olympus_Direct','SCT_SALATT_Olympus_Redirect','SCT_SALATT_Olympus_Xfer   ','SCT_SALEXC_BB','SCT_SALEXC_BBFF'
									,'SCT_SALEXC_BBMAC','SCT_SALEXC_BBMAC_Xfer','SCT_SALEXC_BBNLP','SCT_SALEXC_BBNoLR','SCT_SALEXC_BBPreActive','SCT_SALEXC_BB_Xfer'
									,'SCT_SALEXC_Fibre','SCT_SALEXC_Olympus','SCT_SALEXC_ROI_BBT_Upgrades','SCT_SALEXC_ROI_SwitcherBB','SCT_SALOLY_EOODDR_CAN_SABB'
									,'SCT_SALOLY_EOODDR_DGBT_SABB','SCT_SALOLY_EOODirect_SABB   ','SCT_SALOLY_Olympus_Xfer','SCT_SALPAT_ROI_BB','SCT_SALPAT_ROI_BB_Xfer'
									,'SCT_SALPAT_ROI_Fibre_Direct ','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_Campaign2','SCT_SALRET_BB_Campaign3'
									,'SCT_SALRET_BB_Churn','SCT_SALRET_BB_HighChurn','SCT_SALRET_BB_HighChurn_Xfer','SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online'
									,'SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer','SCT_SALRET_BB_TVWinback'
									,'SCT_SALRET_BB_Value','SCT_SALRET_BB_Value2','SCT_SALRET_BB_ValueBill','SCT_SALRET_BB_ValueBill_TO'
									,'SCT_SALRET_BB_Value_D&G','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer'
									,'SCT_SALRET_ELP_BB','SCT_SALRTM_BBINFO','SCT_SALRTM_SHMS_Olympus','SCT_SALTRN_BB_HighChurn','SCT_SALTRN_BB_HighChurn_Xfer'
									,'SCT_SALTRN_BB_TA_Xfer','SCT_SALVAL_BB_Syscan')
							GROUP BY account_number ,  call_date 
					UNION 
				SELECT account_number
						, end_date
						, calls_LW
				FROM FORECAST_Looped_Sim_Output_Platform
				WHERE calls_LW > 0) AS temp
	JOIN Forecast_Loop_Table_2 	AS base 	ON    base.account_number = temp.account_number
	WHERE call_date BETWEEN DATEADD(WEEK, 1 , DATEADD(mm,-1,end_date)) AND end_date
	GROUP BY base.account_number, end_date		 
	
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - BBCalls_Temp_1m_2:  '||@@rowcount TO CLIENT 
	
	COMMIT  
	CREATE HG INDEX ID1 ON #BBCalls_Temp_1m_2(account_number) 
	CREATE DTTM INDEX ID2 ON #BBCalls_Temp_1m_2(end_date) 
	CREATE LF INDEX ID3 ON #BBCalls_Temp_1m_2(call_count) 
	COMMIT 
		
		
	
	SELECT 
		  b.account_number
		, b.segment_sa
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -12,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END) L_12 	-- Max Login in the past 12 month
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -9,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END) L_9	-- Max Login in the past 9 month
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -6,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END) L_6	-- Max Login in the past 6 month
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -3,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.my_sky_login_3m_raw ELSE 0 END) L_3	
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -12,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) C_12 	-- Max Login in the past 12 month
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -9,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) C_9	-- Max Login in the past 9 month
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -6,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) C_6	-- Max Login in the past 6 month
		,MAX(CASE WHEN  a.end_date BETWEEN DATEADD(MONTH, -3,b.end_date) AND DATEADD(WEEK, -1,b.end_date) THEN a.BB_all_calls_1m_raw ELSE 0 END) C_3	
		, CASE  WHEN L_12 = 0 THEN 1
				WHEN L_9 = 0 THEN 2	
				WHEN L_6 = 0 THEN 3
				WHEN L_3 = 0 THEN 4
				ELSE 5 END Login_group
		, CASE  WHEN C_12 = 0 THEN 1
				WHEN C_9 = 0 THEN 2
				WHEN C_6 = 0 THEN 3
				WHEN C_3 = 0 THEN 4
				ELSE 5 END Call_group
		, CAST (NULL AS FLOAT) Rand_Login
		, CAST (NULL AS FLOAT) Rand_call		
	INTO #t_prob
	FROM Forecast_Loop_Table_2 AS b
	JOIN cust_fcast_weekly_base_2 AS a ON a.account_number = b.account_number
	GROUP BY b.account_number
		, b.segment_sa 
	COMMIT 
	UPDATE #t_prob
	SET  Rand_LOGIN = RAND((CAST (account_number AS FLOAT))  * DATEPART (ms, GETDATE()))
		, Rand_call = RAND((CAST (account_number AS FLOAT)*10) * DATEPART (ms, GETDATE())) 
	
	CREATE HG INDEX ID1 ON #t_prob (account_number) 
	CREATE LF INDEX ID2 ON #t_prob (segment_sa) 
	CREATE LF INDEX ID3 ON #t_prob (Login_group) 
	CREATE LF INDEX ID4 ON #t_prob (Call_group) 
	
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - t_prob:  '||@@rowcount TO CLIENT 
		
	UPDATE Forecast_Loop_Table_2
	SET a.my_sky_login_3m_raw = COALESCE (c.Calls_LW, 0 ) + COALESCE (d.visit_days, 0 )
		,a.my_sky_login_LW = COALESCE (c.Calls_LW, 0 )
	FROM Forecast_Loop_Table_2	AS a 
	JOIN #t_prob AS b	ON a.account_number = b.account_number
	LEFT JOIN #days_visited_3m_2 AS d ON a.account_number = d.account_number
	LEFT JOIN SABB_my_sky_login_prob_TABLE AS c ON b.Login_group = c.Prob_Group
											AND b.segment_sa = c.segment_sa
											AND Rand_login BETWEEN Lower_limit AND UPPER_LIMIT  
											
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing Forecast_Loop_Table_2/1:  '||@@rowcount TO CLIENT 									
	
	UPDATE Forecast_Loop_Table_2
	SET a.BB_all_calls_1m_raw = COALESCE (c.Calls_LW, 0 ) + COALESCE (d.call_count, 0 )
		,a.Calls_LW = COALESCE(c.Calls_LW, 0 )
	FROM Forecast_Loop_Table_2	AS a 
	JOIN #t_prob AS b	ON a.account_number = b.account_number
	LEFT JOIN #BBCalls_Temp_1m_2 AS d ON a.account_number = d.account_number
	LEFT JOIN SABB_BB_Calls_prob_TABLE AS c ON b.Login_group = c.Prob_Group
											AND b.segment_sa = c.segment_sa
											AND Rand_call BETWEEN Lower_limit AND UPPER_LIMIT  
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Updateing Forecast_Loop_Table_2/2:  '||@@rowcount TO CLIENT 																	
											
	MESSAGE CAST(now() as timestamp)||' | SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk - Checkpoint 3/3' TO CLIENT 

		--- Refreshing binned variables
	UPDATE Forecast_Loop_Table_2
	SET   my_sky_login_3m 		= CASE 	WHEN my_sky_login_3m_raw > 2 THEN 3 ELSE my_sky_login_3m_raw END 
		, BB_all_calls_1m 		= CASE 	WHEN BB_all_calls_1m_raw = 0 THEN 0 ELSE 1 END 
		, BB_offer_rem_and_end 	= CASE 	WHEN BB_offer_rem_and_end_raw BETWEEN -9998 AND -1015 	THEN -3
										WHEN BB_offer_rem_and_end_raw BETWEEN -1015 AND -215 	THEN -2 
										WHEN BB_offer_rem_and_end_raw BETWEEN -215  AND -75  	THEN -1
										WHEN BB_offer_rem_and_end_raw BETWEEN -74  AND -0    	THEN 0
										WHEN BB_offer_rem_and_end_raw BETWEEN 1    AND 62    	THEN 1
										WHEN BB_offer_rem_and_end_raw BETWEEN 63   AND 162   	THEN 2
										WHEN BB_offer_rem_and_end_raw BETWEEN 163  AND 271		THEN 3
										WHEN BB_offer_rem_and_end_raw >271						THEN 4
										ELSE -9999 END
		, BB_tenure 	= CASE 	WHEN BB_tenure_raw <= 118 				THEN 1
								WHEN BB_tenure_raw BETWEEN 119 AND 231  THEN 2
								WHEN BB_tenure_raw BETWEEN 231 AND 329  THEN 3
								WHEN BB_tenure_raw BETWEEN 329 AND 391  THEN 4
								WHEN BB_tenure_raw BETWEEN 392 AND 499  THEN 5
								WHEN BB_tenure_raw BETWEEN 499 AND 641  THEN 6
								WHEN BB_tenure_raw BETWEEN 641 AND 1593 THEN 7
								WHEN BB_tenure_raw > 1593 				THEN 8	
								ELSE -1 END  

		--- Refreshing nodes and segments
	UPDATE Forecast_Loop_Table_2
	SET SABB_forecast_segment = CAST(node AS VARCHAR(4))
		, segment_sa = segment
	FROM Forecast_Loop_Table_2 AS a
	JOIN BB_SABB_Churn_segments_lookup AS b  ON a.BB_offer_rem_and_end 	= b.BB_offer_rem_and_end
											AND a.BB_tenure 			= b.BB_tenure 
											AND a.my_sky_login_3m 		= b.my_sky_login_3m
											AND a.talk_type 			= b.talk_type
											AND a.home_owner_status 	= b.home_owner_status
											AND a.BB_all_calls_1m 		= b.BB_all_calls_1m 
																						
										
	DROP TABLE #days_visited_3m_2								
	DROP TABLE #BBCalls_Temp_1m_2								
	DROP TABLE #t_prob
END 			

-- Grant execute rights to the members of CITeam
GRANT EXECUTE ON SABB_Forecast_Loop_Table_2_Update_For_Nxt_Wk TO CITeam 
	