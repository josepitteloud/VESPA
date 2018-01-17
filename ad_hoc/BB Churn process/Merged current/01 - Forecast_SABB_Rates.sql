CREATE OR REPLACE PROCEDURE Forecast_SABB_Rates (IN Forecast_Start_Wk INT) 

BEGIN
	DECLARE var_End_date DATE;
	DECLARE _1st_Wk1 INT;
	DECLARE _Lst_Wk INT;
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - Initialization begin ' TO client;
	SELECT * INTO #sky_calendar FROM subs_calendar(Forecast_Start_Wk / 100 - 3, Forecast_Start_Wk / 100);
				   
																		  

	SET var_End_date = (SELECT max(calendar_date - 7)
						FROM #sky_calendar
						WHERE subs_week_and_year = Forecast_Start_Wk);
	SET _Lst_Wk = (SELECT max(subs_week_and_year) 
					FROM #sky_calendar
					WHERE calendar_date = var_End_date);
			
	SET _1st_Wk1 = CASE WHEN remainder(Forecast_Start_Wk, 100) < 52 THEN (Forecast_Start_Wk / 100 - 3) * 100 + remainder(Forecast_Start_Wk, 100) ELSE (Forecast_Start_Wk / 100 - 2) * 100 + 1 END;
	
	SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

	DROP TABLE IF EXISTS #SABB_weekly_agg;
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - checkpoint 1 ' TO client;	
	--------------------------------------------------------------------------------------------------------------------------------------------		

	--========================================================================================
	--------------------- BB_all_calls_1m
	--========================================================================================
	DROP TABLE IF EXISTS BBCalls_Temp_1m;
	
	SELECT base.account_number
		  , COUNT(1) AS call_count
		  , CAST(end_date AS DATE)  AS end_date 
	INTO BBCalls_Temp_1m
	FROM cust_inbound_calls					AS temp
	JOIN citeam.Cust_Weekly_Base 	AS base 	ON    base.account_number = temp.account_number
	WHERE call_date BETWEEN DATEADD(mm,-1,end_date) AND DATEADD(dd,-1,end_date)
			AND contact_activity = 'Inbound'
			AND base.bb_active = 1 And base.dtv_active =0 
			AND   service_call_type IN ('SCT_CUSSER_BBusage','SCT_SALOLY_EOODirect_SABB','SCT_SALRET_BB_Campaign2','SCT_SALRET_BB_Churn'
									 ,'SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer'
									 ,'SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer'
									 ,'SCT_SALRET_BB_Value2','SCT_SALRET_ELP_BB','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_HighChurn'
									 ,'SCT_SALRET_BB_Value_D&G','SCT_SALRET_BB_HighChurn_Xfer','SCT_CUSSER_BBusage','SCT_SALOLY_EOODirect_SABB','SCT_SALRET_BB_Campaign2'
									 ,'SCT_SALRET_BB_Churn','SCT_SALRET_BB_MAC','SCT_SALRET_BB_Online','SCT_SALRET_BB_PIPELINE','SCT_SALRET_BB_TA','SCT_SALRET_BB_TA_Xfer'
									 ,'SCT_SALRET_BB_TVWinback','SCT_SALRET_BB_Value','SCT_SALRET_BB_Value_SA','SCT_SALRET_BB_Value_SA_Xfer','SCT_SALRET_BB_Value_Xfer'
									 ,'SCT_SALRET_BB_Value2','SCT_SALRET_ELP_BB','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Campaign1','SCT_SALRET_BB_HighChurn','SCT_SALRET_BB_Value_D&G'
									 ,'SCT_SALRET_BB_HighChurn_Xfer','SCT_HLPALL_NowTV_Cancel_Xfer','SCT_SALRET_ELP_Xfer','SCT_SALTRN_BB_TA_Xfer','SCT_SALRET_BB_Value_SA_Xfer'
									 ,'SCT_SALVAL_BB_Syscan','SCT_SALRET_BB_Campaign3','SCT_HLPTV__PriceTalk_AVS','SCT_HLPTV__PriceTalk_TO','SCT_OTHCTT_DN1'
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
	GROUP BY base.account_number, end_date;
	
	CREATE HG INDEX id1 ON BBCalls_Temp_1m (account_number) ;
	CREATE DATE INDEX ID2 ON BBCalls_Temp_1m(end_date);
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - checkpoint 1.1 ' TO client;	

	--------------------------------------------------------------------------------------------------------------------------------------------	
	--------------------------------------------------------------------------------------------------------------------------------------------	
	-----------------------------------------------------------
	-----------		my_sky update
	-----------------------------------------------------------
	DROP TABLE IF EXISTS days_visited_3m;
	
	SELECT COUNT(DISTINCT visit_date) AS visit_days
		  , BASE.account_number AS account_number
		  , CAST(end_date AS DATE)  AS end_date 
	INTO days_visited_3m
	FROM vespa_shared.mysky_daily_usage 	AS mr 
	JOIN citeam.Cust_Weekly_Base  	AS base ON BASE.account_number = mr.account_number
	WHERE visit_date BETWEEN DATEADD(mm,-3,end_date) AND end_date
		AND base.bb_active = 1 And base.dtv_active = 0
		AND end_date > DATEADD(month, -40, var_End_date) 
	GROUP BY base.account_number, end_date;
	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - checkpoint 1.2 ' TO client;	
	
	CREATE HG INDEX id1 ON days_visited_3m (account_number) ;
	CREATE DATE INDEX ID2 ON days_visited_3m(end_date);
	

	--------------------------------------------------------------------------------------------------------------------------------------------	
	--------------------------------------------------------------------------------------------------------------------------------------------	
	message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - checkpoint 2 ' TO client;	
	
		SELECT 
				z.subs_year
			,   z.subs_week_of_year as Subs_week
			,   z.subs_week_and_year 
			, CAST(CASE 	WHEN d.node IN (22, 46, 49, 70, 75, 71) THEN 1
							WHEN d.node IN ( 83, 53, 43, 82, 73, 57) THEN 2
							WHEN d.node IN ( 63, 47, 68, 42, 62, 12, 39, 11, 35) THEN 3
							WHEN d.node IN ( 21, 74, 72) THEN 4
							WHEN d.node IN ( 40, 36, 66, 60, 65) THEN 5
							WHEN d.node IN ( 77, 31, 84, 56, 76) THEN 6
							WHEN d.node IN ( 10, 41, 67) THEN 7
							WHEN d.node IN ( 61, 51, 64, 24, 50) THEN 8
							WHEN d.node IN ( 27, 55, 85, 81, 79, 80, 54) THEN 9
							WHEN d.node IN ( 9) THEN 10
							ELSE 0 END AS VARCHAR(4))	 AS SABB_forecast_segment  
			, Count() AS n
			, CASE WHEN DATEDIFF (DAY, z.Curr_Offer_Start_Dt_BB, z.end_date) BETWEEN 0 AND 7  THEN 1 ELSE 0 END  		AS BB_Offer_applied
																											
			, cast(SUM(PL.enter_SysCan) AS REAL) 		AS bb_enter_SysCan
			, cast(SUM(PL.enter_cuscan) AS REAL) 		AS bb_enter_CusCan
			, cast(SUM(PL.enter_HM) AS REAL) 			AS bb_enter_HM
			, cast(SUM(PL.enter_3rd_party) AS REAL) 		AS bb_enter_3rd_party
			, dense_rank() OVER (ORDER BY z.subs_week_and_year DESC) AS week_id
			, CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, ((week_id-1) / 13) + 1 AS quarter_id
			, Max(Subs_Week) OVER (PARTITION BY z.Subs_Year) AS Max_Subs_Week
			, cast(0 as smallint) as Max_week_id
			--- lines added here for last payment_date
			,CASE WHEN Cast(z.end_date - CASE WHEN day(z.end_date) < z.payment_due_day_of_month THEN Cast('' || year(dateadd(month, - 1, z.end_date)) || '-' || month(dateadd(month, - 1, z.end_date)) || '-' || z.payment_due_day_of_month AS DATE) 
											WHEN day(z.end_date) >= z.payment_due_day_of_month THEN Cast('' || year(z.end_date) || '-' || month(z.end_date) || '-' || z.payment_due_day_of_month AS DATE) END AS INT) 			BETWEEN 7 AND 14 THEN '7to14' 
											ELSE 'other' END AS Days_Since_Last_Payment_Dt_Bin

			, CAST (CASE 	WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) <= 118 				THEN 1
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) BETWEEN 119 AND 231  	THEN 2
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) BETWEEN 231 AND 329  	THEN 3
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) BETWEEN 329 AND 391  	THEN 4
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) BETWEEN 392 AND 499  	THEN 5
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) BETWEEN 499 AND 641  	THEN 6
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) BETWEEN 641 AND 1593 	THEN 7
							WHEN DATEDIFF(dd, BB_Last_Activation_Dt , z.end_date) > 1593 				THEN 8	
							ELSE -1 END AS INT ) 			AS BB_tenure_x 
			,  CASE WHEN Talk_Product_Holding LIKE 'Sky Talk 24 / 7%' THEN 'Sky Talk 24 / 7'
								 WHEN Talk_Product_Holding LIKE 'Sky Talk Anytime Extra%' THEN 'Sky Talk Anytime Extra'
								 WHEN Talk_Product_Holding LIKE 'Sky Pay As You Talk%' THEN 'Sky Pay As You Talk'
								 WHEN Talk_Product_Holding LIKE 'Anytime%' THEN 'Sky Talk Anytime'
								 WHEN Talk_Product_Holding LIKE 'Off Peak%' THEN 'Off Peak'
								 WHEN Talk_Product_Holding LIKE 'Sky Talk Freetime%' THEN 'Sky Talk Freetime'
								 WHEN Talk_Product_Holding LIKE 'Sky Talk International Extra%' THEN 'Sky Talk International Extra'
								 WHEN Talk_Product_Holding LIKE 'Sky Talk Unlimited%' THEN 'Sky Talk Unlimited'
								 WHEN Talk_Product_Holding LIKE 'Sky Talk Anytime%' THEN 'Sky Talk Anytime'
								 WHEN Talk_Product_Holding LIKE 'Sky Talk Evenings and Weekends%' THEN 'Sky Talk Evenings and Weekends'
								 WHEN Talk_Product_Holding LIKE '%Weekend%' THEN 'Sky Talk Evenings and Weekends'
								 WHEN Talk_Product_Holding LIKE '%Freetime%' THEN 'Sky Talk Freetime'
								 WHEN Talk_Product_Holding LIKE 'Missing at load' THEN 'NONE'
								 ELSE 'NONE' END  												AS talk_type_x
							
			, CAST(CASE WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN -9998 AND -1015 	THEN -3
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN -1015 AND -215 	THEN -2 
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN -215  AND -75  	THEN -1
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN -74  AND -0    	THEN 0
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN 1    AND 62    	THEN 1
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN 63   AND 162   	THEN 2
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END BETWEEN 163  AND 271		THEN 3
							WHEN CASE WHEN Curr_Offer_Actual_End_Dt_BB	IS NOT NULL THEN DATEDIFF(dd, Curr_Offer_Actual_End_Dt_BB, z.end_date) 
								ELSE DATEDIFF(dd, Prev_Offer_Actual_End_Dt_BB, z.end_date) END >271						THEN 4
							ELSE -9999 END   AS INT)											AS BB_offer_rem_and_end_x
				,   CAST(CASE 	WHEN temp.call_count IS NULL THEN 0 
										WHEN temp.call_count > 0 THEN 1 ELSE 0 END AS INT) 		AS  BB_all_calls_1m_x
			,  	CAST(CASE WHEN visit_days > 2 THEN 3 ELSE visit_days END  AS INT) 				AS 	my_sky_login_3m_x
		INTO #SABB_weekly_agg2
		FROM citeam.Cust_Weekly_Base AS z 
		LEFT JOIN CITeam.PL_Entries_BB  			AS PL 	ON z.account_number = PL.account_number 		AND z.subs_week_and_year = PL.subs_week_and_year
		LEFT JOIN BBCalls_Temp_1m 					AS temp ON z.account_number = temp.account_number 		AND z.end_date = temp.end_date
		LEFT JOIN days_visited_3m 					AS dv 	ON z.account_number = dv.account_number 		AND z.end_date = dv.end_date
		LEFT JOIN BB_SABB_Churn_segments_lookup AS d  ON BB_offer_rem_and_end_x		 	= d.BB_offer_rem_and_end
													 AND BB_tenure_x 					= d.BB_tenure 
													 AND talk_type_x 					= d.talk_type
													 AND z.home_owner_status 			= d.home_owner_status
													 AND my_sky_login_3m_x 		= d.my_sky_login_3m
													 AND BB_all_calls_1m_x		= d.BB_all_calls_1m 
		LEFT JOIN citeam.nowtv_accounts_ents AS c ON z.account_number = c.account_number AND z.End_date BETWEEN period_start_date AND period_end_date
		WHERE z.subs_week_and_year BETWEEN _1st_Wk1 AND _Lst_Wk
			AND z.bb_active = 1 And z.dtv_active =0 and subs_week<>53 
			AND c.account_number IS NULL
			AND z.skyplus_active = 0 
		GROUP BY z.subs_year
			, subs_week
			, z.subs_week_and_year
			, SABB_forecast_segment
			, BB_Offer_applied
			, BB_tenure_x  
			, talk_type_x
			, BB_offer_rem_and_end_x
			, BB_all_calls_1m_x
			, my_sky_login_3m_x
			, Days_Since_Last_Payment_Dt_Bin
			;
   
		message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - checkpoint 3 ' TO client;

	DROP TABLE BBCalls_Temp_1m;
	DROP TABLE days_visited_3m;
	
	SELECT  subs_year
			, Subs_week
			, subs_week_and_year
			,  SABB_forecast_segment
			, sum(n) AS n
			, sum(BB_Offer_Applied) AS BB_Offer_Applied				-- TODO: Affected by the bb_offerapplied
			, sum(bb_enter_SysCan) AS bb_enter_SysCan
			, sum(bb_enter_CusCan) AS bb_enter_CusCan
			, sum(bb_enter_HM) AS bb_enter_HM
			, sum(bb_enter_3rd_party) AS bb_enter_3rd_party
			, week_id
			, week_position
			, last_quarter
			, quarter_id
			, Max_Subs_Week
			, Max_week_id
		INTO #SABB_weekly_agg
		FROM #SABB_weekly_agg2
		GROUP BY subs_year
			, subs_week
			, subs_week_and_year
			, SABB_forecast_segment
			, week_id
			, week_position
			, last_quarter
			, quarter_id
			, Max_Subs_Week
			, Max_week_id;
												 
		UPDATE #SABB_weekly_agg2
		SET SABB_forecast_segment = cast(cast(SABB_forecast_segment AS INT) + 10 AS CHAR)
		WHERE sabb_forecast_segment <> '0' AND days_since_last_payment_dt_bin = '7to14';

		UPDATE #SABB_weekly_agg
		SET week_position = CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END
			, last_quarter = CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END
			, quarter_id = ((week_id - 1) / 13) + 1;

		UPDATE #SABB_weekly_agg
		SET subs_week = subs_week - 1
		WHERE Max_Subs_Week = 53;

		UPDATE #SABB_weekly_agg
		SET Subs_Week_And_Year = Subs_Year * 100 + subs_week;

		DELETE
		FROM #SABB_weekly_agg
		WHERE subs_week = 0;

	DROP TABLE IF EXISTS #SABB_forecast_summary_1;
	
		SELECT subs_week
			, sabb_forecast_segment
			, sum(CASE WHEN Week_Position = 'Prev' THEN n ELSE 0 END) AS prev_n
			, sum(CASE WHEN Week_Position = 'Prev' THEN agg.BB_Offer_Applied ELSE 0 END) AS prev_BB_Offer_Applied
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_SysCan ELSE 0 END) AS prev_bb_enter_SysCan
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_CusCan ELSE 0 END) AS prev_bb_enter_CusCan
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_HM ELSE 0 END) AS prev_bb_enter_HM
			, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_3rd_party ELSE 0 END) AS prev_bb_enter_3rd_party
			, sum(CASE WHEN Week_Position = 'Curr' THEN n ELSE 0 END) AS curr_n
			, sum(CASE WHEN Week_Position = 'Curr' THEN agg.BB_Offer_Applied ELSE 0 END) AS curr_BB_Offer_Applied
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_SysCan ELSE 0 END) AS curr_bb_enter_SysCan
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_CusCan ELSE 0 END) AS curr_bb_enter_CusCan
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_HM ELSE 0 END) AS curr_bb_enter_HM
			, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_3rd_party ELSE 0 END) AS curr_bb_enter_3rd_party
			, sum(0) AS LQ_n
			, sum(0) AS LQ_BB_Offer
		INTO #SABB_forecast_summary_1
		FROM #SABB_weekly_agg AS agg
		GROUP BY subs_week
			, sabb_forecast_segment;

	DROP TABLE IF EXISTS #cuscan_forecast_summary_LQ;
	
		SELECT SABB_forecast_segment
			, sum(n) AS LQ_n
			, sum(BB_offer_applied) AS LQ_BB_Offer
		INTO #cuscan_forecast_summary_LQ
		FROM #SABB_weekly_agg
		WHERE last_quarter = 'Y'
		GROUP BY SABB_forecast_segment;

	message cast(now() AS TIMESTAMP) || ' | 7' TO client;

	UPDATE #SABB_forecast_summary_1 AS a
	SET a.LQ_n = b.LQ_n
		, a.LQ_BB_Offer = b.LQ_BB_Offer
	FROM #SABB_forecast_summary_1 AS a
	LEFT JOIN #cuscan_forecast_summary_LQ AS b ON a.SABB_forecast_segment = b.SABB_forecast_segment;

	DROP TABLE

	IF EXISTS #SABB_forecast_summary_2;
		SELECT *
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_SysCan AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_SysCan_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_CusCan AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_CusCan_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_HM AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_HM_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_3rd_party AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_3rd_party_rate
			, CASE WHEN curr_n >= 100 THEN (cast(curr_BB_Offer_Applied AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_BB_Offer_Applied_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_SysCan AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_SysCan_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_CusCan AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_CusCan_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_HM AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_HM_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_3rd_party AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_3rd_party_rate
			, CASE WHEN prev_n >= 100 THEN (cast(prev_BB_Offer_Applied AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_BB_Offer_Applied_rate
			, CASE WHEN LQ_n >= 100 THEN cast(LQ_BB_Offer AS REAL) / cast(LQ_n AS REAL) ELSE 0 END AS LQ_DTV_Offer_rate
			, CASE WHEN (curr_n + prev_n) >= 100 THEN cast(curr_n AS REAL) / (cast(curr_n AS REAL) + cast(prev_n AS REAL)) ELSE 0 END AS curr_share
			, 1 - curr_share AS prev_share
		INTO #SABB_forecast_summary_2
		FROM #SABB_forecast_summary_1;

	DROP TABLE IF EXISTS #SABB_forecast_summary_3;
	
	SELECT *
			, curr_bb_enter_SysCan_rate AS pred_bb_enter_SysCan_rate
			, curr_bb_enter_CusCan_rate AS pred_bb_enter_CusCan_rate
			, curr_bb_enter_HM_rate AS pred_bb_enter_HM_rate
			, curr_bb_enter_3rd_party_rate AS pred_bb_enter_3rd_party_rate
			, curr_BB_Offer_Applied_rate AS pred_BB_Offer_Applied_rate
		INTO #SABB_forecast_summary_3
		FROM #SABB_forecast_summary_2;

		message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - DaysToPaymentDate adjustments - START' TO client;		
		
	update #SABB_weekly_agg2
	set week_position = case 
	  when week_id between  1 and  52 then 'Curr'
	  when week_id between 53 and 104 then 'Prev'
	  else null
	 end
	,last_quarter = case when week_id between 1 and 13 then 'Y' else 'N' end 
	,quarter_id = ((week_id-1)/13)+1 ;


	UPDATE #SABB_weekly_agg2 
	SET subs_week = subs_week - 1
	WHERE Max_Subs_Week = 53;

	UPDATE #SABB_weekly_agg2
	SET Subs_Week_And_Year = Subs_Year * 100 + subs_week;

	DELETE
	FROM #SABB_weekly_agg2
	WHERE subs_week = 0;

	DROP TABLE IF EXISTS #SABB_forecast_summary_12;
			SELECT subs_week
				, sabb_forecast_segment
				, sum(CASE WHEN Week_Position = 'Prev' THEN n ELSE 0 END) AS prev_n
				, sum(CASE WHEN Week_Position = 'Prev' THEN agg.BB_Offer_Applied ELSE 0 END) AS prev_BB_Offer_Applied
				, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_SysCan ELSE 0 END) AS prev_bb_enter_SysCan
				, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_CusCan ELSE 0 END) AS prev_bb_enter_CusCan
				, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_HM ELSE 0 END) AS prev_bb_enter_HM
				, sum(CASE WHEN Week_Position = 'Prev' THEN bb_enter_3rd_party ELSE 0 END) AS prev_bb_enter_3rd_party
				, sum(CASE WHEN Week_Position = 'Curr' THEN n ELSE 0 END) AS curr_n
				, sum(CASE WHEN Week_Position = 'Curr' THEN agg.BB_Offer_Applied ELSE 0 END) AS curr_BB_Offer_Applied
				, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_SysCan ELSE 0 END) AS curr_bb_enter_SysCan
				, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_CusCan ELSE 0 END) AS curr_bb_enter_CusCan
				, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_HM ELSE 0 END) AS curr_bb_enter_HM
				, sum(CASE WHEN Week_Position = 'Curr' THEN bb_enter_3rd_party ELSE 0 END) AS curr_bb_enter_3rd_party
				, sum(0) AS LQ_n
				, sum(0) AS LQ_BB_Offer
			INTO #SABB_forecast_summary_12
			FROM #SABB_weekly_agg2 AS agg
			GROUP BY subs_week
				, sabb_forecast_segment;

		DROP TABLE

		IF EXISTS #cuscan_forecast_summary_LQ2;
			SELECT SABB_forecast_segment
				, sum(n) AS LQ_n
				, sum(BB_offer_applied) AS LQ_BB_Offer
			INTO #cuscan_forecast_summary_LQ2
			FROM #SABB_weekly_agg2
			WHERE last_quarter = 'Y'
			GROUP BY SABB_forecast_segment;

		message cast(now() AS TIMESTAMP) || ' | 7' TO client;

		UPDATE #SABB_forecast_summary_12 AS a
		SET a.LQ_n = b.LQ_n
			, a.LQ_BB_Offer = b.LQ_BB_Offer
		FROM #SABB_forecast_summary_12 AS a
		LEFT JOIN #cuscan_forecast_summary_LQ2 AS b ON a.SABB_forecast_segment = b.SABB_forecast_segment;

		DROP TABLE

		IF EXISTS #SABB_forecast_summary_22;
			SELECT *
				, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_SysCan AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_SysCan_rate
				, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_CusCan AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_CusCan_rate
				, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_HM AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_HM_rate
				, CASE WHEN curr_n >= 100 THEN (cast(curr_bb_enter_3rd_party AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_bb_enter_3rd_party_rate
				, CASE WHEN curr_n >= 100 THEN (cast(curr_BB_Offer_Applied AS REAL) / cast(curr_n AS REAL)) ELSE 0 END AS curr_BB_Offer_Applied_rate
				, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_SysCan AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_SysCan_rate
				, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_CusCan AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_CusCan_rate
				, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_HM AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_HM_rate
				, CASE WHEN prev_n >= 100 THEN (cast(prev_bb_enter_3rd_party AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_bb_enter_3rd_party_rate
				, CASE WHEN prev_n >= 100 THEN (cast(prev_BB_Offer_Applied AS REAL) / cast(prev_n AS REAL)) ELSE 0 END AS prev_BB_Offer_Applied_rate
				, CASE WHEN LQ_n >= 100 THEN cast(LQ_BB_Offer AS REAL) / cast(LQ_n AS REAL) ELSE 0 END AS LQ_DTV_Offer_rate
				, CASE WHEN (curr_n + prev_n) >= 100 THEN cast(curr_n AS REAL) / (cast(curr_n AS REAL) + cast(prev_n AS REAL)) ELSE 0 END AS curr_share
				, 1 - curr_share AS prev_share
			INTO #SABB_forecast_summary_22
			FROM #SABB_forecast_summary_12;

		DROP TABLE IF EXISTS #SABB_forecast_summary_32;
		
		SELECT *
				, curr_bb_enter_SysCan_rate AS pred_bb_enter_SysCan_rate
				, curr_bb_enter_CusCan_rate AS pred_bb_enter_CusCan_rate
				, curr_bb_enter_HM_rate AS pred_bb_enter_HM_rate
				, curr_bb_enter_3rd_party_rate AS pred_bb_enter_3rd_party_rate
				, curr_BB_Offer_Applied_rate AS pred_BB_Offer_Applied_rate
			INTO #SABB_forecast_summary_32
			FROM #SABB_forecast_summary_22;

		message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - DaysToPaymentDate adjustments - END' TO client;		
		message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - Integrate both runs' TO client;	
			
		--- we now have #SABB_forecast_summary_32 and #SABB_forecast_summary_3
		--- need to combine into 1 file taking:
		--- make a copy of 3, adding 10 to segments - then create union to make a table for all segments with "full rates"
		--- then replace syscan variables with those from 32
	DROP TABLE

	IF EXISTS #copy;
		SELECT subs_week
			, cast(cast(SABB_forecast_segment AS INT) + 10 AS VARCHAR(4)) AS SABB_forecast_segment
			, pred_bb_enter_SysCan_rate
			, pred_bb_enter_CusCan_rate
			, pred_bb_enter_HM_rate
			, pred_bb_enter_3rd_party_rate
			, pred_BB_Offer_Applied_rate
			, prev_bb_enter_SysCan_rate
			, prev_bb_enter_CusCan_rate
			, prev_bb_enter_HM_rate
			, prev_bb_enter_3rd_party_rate
			, prev_BB_Offer_Applied_rate
		INTO #copy
		FROM #SABB_forecast_summary_3;

	DROP TABLE

	IF EXISTS #final;
		SELECT *
		INTO #final
		FROM (
			SELECT subs_week
				, SABB_forecast_segment
				, pred_bb_enter_SysCan_rate
				, pred_bb_enter_CusCan_rate
				, pred_bb_enter_HM_rate
				, pred_bb_enter_3rd_party_rate
				, pred_BB_Offer_Applied_rate
				, prev_bb_enter_SysCan_rate
				, prev_bb_enter_CusCan_rate
				, prev_bb_enter_HM_rate
				, prev_bb_enter_3rd_party_rate
				, prev_BB_Offer_Applied_rate
			FROM #SABB_forecast_summary_3
			
			UNION ALL
			
			SELECT * FROM #copy
			) AS tmp;

	DROP TABLE IF EXISTS skyplus;
		message cast(now() AS TIMESTAMP) || ' | Forecast_SABB_Rates - END' TO client;

	SELECT a.subs_week
		, b.SABB_forecast_segment
		, a.pred_bb_enter_SysCan_rate AS pred_SysCan_rate
		, b.pred_bb_enter_CusCan_rate AS pred_CusCan_rate
		, b.pred_bb_enter_HM_rate AS pred_HM_rate
		, b.pred_bb_enter_3rd_party_rate AS pred_3rd_party_rate
		, b.pred_BB_Offer_Applied_rate
		, a.prev_bb_enter_SysCan_rate AS prev_SysCan_rate
		, b.prev_bb_enter_CusCan_rate AS prev_CusCan_rate
		, b.prev_bb_enter_HM_rate AS prev_HM_rate
		, b.prev_bb_enter_3rd_party_rate AS prev_3rd_party_rate
		, b.prev_BB_Offer_Applied_rate
	INTO SABB_predicted_values
	FROM #SABB_forecast_summary_32 AS a
	INNER JOIN #final b ON a.subs_week = b.subs_week AND a.SABB_forecast_segment = b.SABB_forecast_segment

		END
		GO
