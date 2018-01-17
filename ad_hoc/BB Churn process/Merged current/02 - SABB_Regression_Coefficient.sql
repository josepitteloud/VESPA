CREATE OR REPLACE PROCEDURE SABB_Regression_Coefficient (
	IN LV INT
	, IN Regression_Yrs TINYINT
	) 
BEGIN
	DECLARE Dynamic_SQL VARCHAR(1000);
	DECLARE Y3W52 INT;
	DECLARE Y1W01 INT;
	DECLARE var_End_date DATE;

	SELECT *
	INTO #Sky_Calendar
	FROM /*CITeam.*/ Subs_Calendar(LV / 100 - Regression_Yrs - 1, LV / 100);

	DROP TABLE IF EXISTS #Regr_Wks;
	
		SELECT cast(sc.subs_week_and_year AS INT) AS Subs_week_and_year
			, Row_Number() OVER (
				ORDER BY Subs_week_and_year DESC
				) AS Wk_Rnk
		INTO #Regr_Wks
		FROM #sky_calendar AS sc
		WHERE cast(sc.subs_Week_and_year AS INT) < LV AND Subs_Week_of_year <> 53
		GROUP BY Subs_week_and_year;

	DELETE
	FROM #Regr_Wks
	WHERE Wk_Rnk > Regression_Yrs * 52 + 13;

	SET Y1W01 		= (SELECT min(Subs_week_and_year) FROM #Regr_Wks );
	SET Y3W52 		= (SELECT max(Subs_week_and_year) FROM #Regr_Wks );
	SET var_End_date = (SELECT max(calendar_date) FROM #Sky_Calendar WHERE Subs_week_and_year = Y3W52 );

	
	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - checkpoint 1 ' TO client;
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
	
	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - checkpoint 1.1 ' TO client;	

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
	
	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - checkpoint 1.2 ' TO client;	
	
	CREATE HG INDEX id1 ON days_visited_3m (account_number) ;
	CREATE DATE INDEX ID2 ON days_visited_3m(end_date);
	
		
	
	--------------------------------------------------------------------------------------------------------------------------------------------		
	-------------------------------------------------------------------------------------------------------------------------------------------		
	

	DROP TABLE IF EXISTS #SABB_weekly_agg;
	DROP TABLE IF EXISTS #t_acct;
	
		SELECT 	z.subs_year
			,   z.subs_week_of_year as Subs_week
			,   z.subs_week_and_year 
			,   dense_rank() OVER ( ORDER BY z.subs_week_and_year DESC ) AS week_id
			, 	CASE WHEN week_id BETWEEN 1 AND 52 THEN 'Curr' WHEN week_id BETWEEN 53 AND 104 THEN 'Prev' ELSE NULL END AS week_position
			, 	CASE WHEN week_id BETWEEN 1 AND 13 THEN 'Y' ELSE 'N' END AS last_quarter
			, 	((week_id - 1) / 13) + 1 AS quarter_id
			, 	Max(Subs_Week) OVER (PARTITION BY z.Subs_Year) AS Max_Subs_Week
			, 	z.end_date
			
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
			, 	cast(SUM(PL.enter_SysCan) AS REAL) 		AS bb_enter_SysCan
			, 	cast(SUM(PL.enter_cuscan) AS REAL) 		AS bb_enter_CusCan
			, 	cast(SUM(PL.enter_HM) AS REAL) 			AS bb_enter_HM
			, 	cast(SUM(PL.enter_3rd_party) AS REAL) 	AS bb_enter_3rd_party
				
			--- lines added here for last payment_date
			, CASE 	WHEN Cast(z.end_date - CASE WHEN day(z.end_date) < z.payment_due_day_of_month THEN Cast('' || year(dateadd(month, - 1, z.end_date)) || '-' || month(dateadd(month, - 1, z.end_date)) || '-' || z.payment_due_day_of_month AS DATE) 
					WHEN day(z.end_date) >= z.payment_due_day_of_month THEN Cast('' || year(z.end_date) || '-' || month(z.end_date) || '-' || z.payment_due_day_of_month AS DATE) END AS INT) BETWEEN 7 AND 14 THEN '7to14' 
					ELSE 'other' END AS Days_Since_Last_Payment_Dt_Bin
			--- end of lines added
			
			, 	CASE WHEN DATEDIFF (DAY, z.Curr_Offer_Start_Dt_BB, z.end_date) BETWEEN 0 AND 7  THEN 1 ELSE 0 END  		AS BB_Offer_applied
			, 	Count() AS n
			
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
			
		INTO #t_acct
		FROM citeam.Cust_Weekly_Base AS z
		LEFT JOIN CITeam.PL_Entries_BB  			AS PL 	ON z.account_number = PL.account_number 		AND z.subs_week_and_year = PL.subs_week_and_year
		LEFT JOIN citeam.nowtv_accounts_ents AS c ON c.account_number = z.account_number AND z.End_date BETWEEN period_start_date AND period_end_date
		LEFT JOIN BBCalls_Temp_1m 					AS temp ON z.account_number = temp.account_number 		AND z.end_date = temp.end_date
		LEFT JOIN days_visited_3m 					AS dv 	ON z.account_number = dv.account_number 		AND z.end_date = dv.end_date
		LEFT JOIN BB_SABB_Churn_segments_lookup AS d  ON BB_offer_rem_and_end_x		 	= d.BB_offer_rem_and_end
													 AND BB_tenure_x 					= d.BB_tenure 
													 AND talk_type_x 					= d.talk_type
													 AND z.home_owner_status 			= d.home_owner_status
													 AND my_sky_login_3m_x 		= d.my_sky_login_3m
													 AND BB_all_calls_1m_x		= d.BB_all_calls_1m 
													 
		WHERE z.subs_week_and_year BETWEEN Y1W01 AND Y3W52 AND subs_week <> 53 
				AND c.account_number IS NULL
				AND z.skyplus_active = 0 
				AND z.bb_active = 1 
				AND z.dtv_active = 0
		GROUP BY 
				z.subs_year
			, 	subs_week
			,	z.subs_week_and_year
			, 	z.end_date
			, 	z.account_number
			, 	Days_Since_Last_Payment_Dt_Bin
			, 	SABB_forecast_segment
			, 	BB_Offer_applied
			,	BB_tenure_x
			,	my_sky_login_3m_x
			,	BB_offer_rem_and_end_x
			,	talk_type_x
			, 	z.home_owner_status
			, 	BB_all_calls_1m_x;

	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - #t_acct' TO client;

	SELECT a.subs_year
		, a.subs_week
		, a.subs_week_and_year
		, a.week_id
		, a.week_position
		, a.last_quarter
		, a.quarter_id
		, a.Max_Subs_Week
		, a.Days_Since_Last_Payment_Dt_Bin
		, a.SABB_forecast_segment
		, cast(sum(BB_Offer_Applied) AS REAL) AS BB_Offer_Applied -- TODO: Check this definition of offer applied
		, cast(sum(bb_enter_SysCan) AS REAL) AS bb_enter_SysCan
		, cast(sum(bb_enter_CusCan) AS REAL) AS bb_enter_CusCan
		, cast(sum(bb_enter_HM) AS REAL) AS bb_enter_HM
		, cast(sum(bb_enter_3rd_party) AS REAL) AS bb_enter_3rd_party
		, Count(*) AS n
	INTO #SABB_weekly_agg2
	FROM #t_acct AS a
	GROUP BY a.subs_year
		, a.subs_week
		, a.subs_week_and_year
		, a.week_id
		, a.week_position
		, a.last_quarter
		, a.quarter_id
		, a.Max_Subs_Week
		, a.SABB_forecast_segment
		, a.Days_Since_Last_Payment_Dt_Bin;

	SELECT subs_year
		, subs_week
		, subs_week_and_year
		, week_id
		, week_position
		, last_quarter
		, quarter_id
		, Max_Subs_Week
		, sum(BB_Offer_Applied) AS BB_Offer_Applied -- TODO: Check this definition of offer applied
		, sum(bb_enter_SysCan) AS bb_enter_SysCan
		, sum(bb_enter_CusCan) AS bb_enter_CusCan
		, sum(bb_enter_HM) AS bb_enter_HM
		, sum(bb_enter_3rd_party) AS bb_enter_3rd_party
		, SABB_forecast_segment
		, sum(n) AS n
	INTO #SABB_weekly_agg
	FROM #SABB_weekly_agg2
	GROUP BY subs_year
		, subs_week
		, subs_week_and_year
		, week_id
		, week_position
		, last_quarter
		, quarter_id
		, Max_Subs_Week
		, Days_Since_Last_Payment_Dt_Bin
		, SABB_forecast_segment;

	---- update SABB forecast segment to allow for payment dates
	UPDATE #SABB_weekly_agg2
	SET SABB_forecast_segment = cast(cast(SABB_forecast_segment AS INT) + 10 AS CHAR)
	WHERE sabb_forecast_segment <> '0' AND days_since_last_payment_dt_bin = '7to14';--- payment date bin is one of the values

	--- now create rest of #SABB_weekly_agg
	DROP TABLE IF EXISTS #copy; 
	
		SELECT subs_year
			, subs_week
			, subs_week_and_year
			, week_id
			, week_position
			, last_quarter
			, quarter_id
			, Max_Subs_Week
			, BB_Offer_Applied
			, bb_enter_SysCan
			, bb_enter_CusCan
			, bb_enter_HM
			, bb_enter_3rd_party
			, cast(cast(SABB_forecast_segment AS INT) + 10 AS VARCHAR(4)) AS SABB_forecast_segment
			, n
		INTO #copy
		FROM #SABB_weekly_agg;

	DROP TABLE IF EXISTS #swa;
	
		SELECT *
		INTO #swa
		FROM (
			SELECT subs_year
				, subs_week
				, subs_week_and_year
				, week_id
				, week_position
				, last_quarter
				, quarter_id
				, Max_Subs_Week
				, BB_Offer_Applied
				, bb_enter_SysCan
				, bb_enter_CusCan
				, bb_enter_HM
				, bb_enter_3rd_party
				, cast(cast(SABB_forecast_segment AS INT) + 10 AS VARCHAR(4)) AS SABB_forecast_segment
				, n
			FROM #SABB_weekly_agg
			
			UNION ALL
			
			SELECT subs_year
				, subs_week
				, subs_week_and_year
				, week_id
				, week_position
				, last_quarter
				, quarter_id
				, Max_Subs_Week
				, BB_Offer_Applied
				, bb_enter_SysCan
				, bb_enter_CusCan
				, bb_enter_HM
				, bb_enter_3rd_party
				, SABB_forecast_segment
				, n
			FROM #SABB_weekly_agg
			) AS tmp;

	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - #swa' TO client;
	
	DROP TABLE IF EXISTS #SABB_weekly_agg;
		SELECT *
		INTO #SABB_weekly_agg
		FROM #swa;

	DROP TABLE IF EXISTS #Regr_inputs;
	
		SELECT quarter_id
			, agg.SABB_forecast_segment
			, row_number() OVER (
				PARTITION BY agg.SABB_forecast_segment ORDER BY quarter_id DESC
				) AS x
			, sum(cast(BB_enter_CusCan AS REAL)) / sum(n) AS BB_enter_CusCan
			, sum(cast(BB_enter_SysCan AS REAL)) / sum(n) AS BB_enter_SysCan
			, sum(cast(BB_enter_HM AS REAL)) / sum(n) AS BB_enter_HM
			, sum(cast(BB_enter_3rd_party AS REAL)) / sum(n) AS BB_enter_3rd_party
			, sum(cast(BB_Offer_Applied AS REAL)) / sum(n) AS BB_Offer_Applied
			, x * x AS xx
			, x * BB_enter_CusCan AS x_BB_enter_CusCan
			, x * BB_enter_SysCan AS x_BB_enter_SysCan
			, x * BB_enter_HM AS x_BB_enter_HM
			, x * BB_enter_3rd_party AS x_BB_enter_3rd_party
			, x * BB_Offer_Applied AS x_BB_Offer_Applied
			, Sum(n) AS cell_n
			, cast(NULL AS REAL) AS BB_enter_CusCan_regression
			, cast(NULL AS REAL) AS BB_enter_SysCan_regression
			, cast(NULL AS REAL) AS BB_enter_HM_regression
			, cast(NULL AS REAL) AS BB_enter_3rd_party_regression
			, cast(NULL AS REAL) AS BB_Offer_Applied_regression
		INTO #Regr_inputs
		FROM #SABB_weekly_agg AS agg
		GROUP BY quarter_id
			, agg.SABB_forecast_segment;

	DROP TABLE

	IF EXISTS #Regr_coeff;
		SELECT SABB_forecast_segment
			, sum(cell_n) AS n
			, sum(cast(cell_n AS BIGINT) * x) AS sum_x
			, sum(cast(cell_n AS BIGINT) * xx) AS sum_xx
			, sum(cell_n * BB_enter_CusCan) AS sum_BB_enter_CusCan
			, sum(cell_n * BB_enter_SysCan) AS sum_BB_enter_SysCan
			, sum(cell_n * BB_enter_HM) AS sum_BB_enter_HM
			, sum(cell_n * BB_enter_3rd_party) AS sum_BB_enter_3rd_party
			, sum(cell_n * BB_Offer_Applied) AS sum_BB_Offer_Applied
			, sum(cell_n * x_BB_enter_CusCan) AS sum_x_BB_enter_CusCan
			, sum(cell_n * x_BB_enter_SysCan) AS sum_x_BB_enter_SysCan
			, sum(cell_n * x_BB_enter_HM) AS sum_x_BB_enter_HM
			, sum(cell_n * x_BB_enter_3rd_party) AS sum_x_BB_enter_3rd_party
			, sum(cell_n * x_BB_Offer_Applied) AS sum_x_BB_Offer_Applied
			, cast(NULL AS REAL) AS b0_BB_enter_CusCan
			, cast(NULL AS REAL) AS b0_BB_enter_SysCan
			, cast(NULL AS REAL) AS b0_BB_enter_HM
			, cast(NULL AS REAL) AS b0_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b0_BB_Offer_Applied
			, cast(NULL AS REAL) AS b1_BB_enter_CusCan
			, cast(NULL AS REAL) AS b1_BB_enter_SysCan
			, cast(NULL AS REAL) AS b1_BB_enter_HM
			, cast(NULL AS REAL) AS b1_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b1_BB_Offer_Applied
		INTO #Regr_coeff
		FROM #Regr_inputs
		GROUP BY SABB_forecast_segment
		HAVING n > 1000;

	UPDATE #Regr_coeff SET b1_BB_enter_CusCan 	= (sum_x_BB_enter_CusCan - (sum_BB_enter_CusCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
						 , b1_BB_enter_SysCan 	= (sum_x_BB_enter_SysCan - (sum_BB_enter_SysCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
						 , b1_BB_enter_HM 		= (sum_x_BB_enter_HM - (sum_BB_enter_HM * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
						 , b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party - (sum_BB_enter_3rd_party * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
						 , b1_BB_Offer_Applied 	= (sum_x_BB_Offer_Applied - (sum_BB_Offer_Applied * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);
	
	UPDATE #Regr_coeff SET 	 b0_BB_enter_CusCan 	= sum_BB_enter_CusCan / n - b1_BB_enter_CusCan * sum_x / n
							,b0_BB_enter_SysCan 	= sum_BB_enter_SysCan / n - b1_BB_enter_SysCan * sum_x / n
							,b0_BB_enter_HM 		= sum_BB_enter_HM / n - b1_BB_enter_HM * sum_x / n
							,b0_BB_enter_3rd_party 	= sum_BB_enter_3rd_party / n - b1_BB_enter_3rd_party * sum_x / n
							,b0_BB_Offer_Applied 	= sum_BB_Offer_Applied / n - b1_BB_Offer_Applied * sum_x / n;

	-- pass 2
	DROP TABLE IF EXISTS #Regr_inputs2;
		
		SELECT quarter_id
			, agg.SABB_forecast_segment
			, row_number() OVER (
				PARTITION BY agg.SABB_forecast_segment ORDER BY quarter_id DESC
				) AS x
			, sum(cast(BB_enter_CusCan AS REAL)) / sum(n) AS BB_enter_CusCan
			, sum(cast(BB_enter_SysCan AS REAL)) / sum(n) AS BB_enter_SysCan
			, sum(cast(BB_enter_HM AS REAL)) / sum(n) AS BB_enter_HM
			, sum(cast(BB_enter_3rd_party AS REAL)) / sum(n) AS BB_enter_3rd_party
			, sum(cast(BB_Offer_Applied AS REAL)) / sum(n) AS BB_Offer_Applied
			, x * x AS xx
			, x * BB_enter_CusCan AS x_BB_enter_CusCan
			, x * BB_enter_SysCan AS x_BB_enter_SysCan
			, x * BB_enter_HM AS x_BB_enter_HM
			, x * BB_enter_3rd_party AS x_BB_enter_3rd_party
			, x * BB_Offer_Applied AS x_BB_Offer_Applied
			, Sum(n) AS cell_n
			, cast(NULL AS REAL) AS BB_enter_CusCan_regression
			, cast(NULL AS REAL) AS BB_enter_SysCan_regression
			, cast(NULL AS REAL) AS BB_enter_HM_regression
			, cast(NULL AS REAL) AS BB_enter_3rd_party_regression
			, cast(NULL AS REAL) AS BB_Offer_Applied_regression
		INTO #Regr_inputs2
		FROM #SABB_weekly_agg2 AS agg
		GROUP BY quarter_id
			, agg.SABB_forecast_segment;

	message cast(now() AS TIMESTAMP) || ' | SABB_Regression_Coefficient - #Regr_inputs2' TO client;
	
	DROP TABLE IF EXISTS #Regr_coeff2; 
	
		SELECT SABB_forecast_segment
			, sum(cell_n) AS n
			, sum(cast(cell_n AS BIGINT) * x) AS sum_x
			, sum(cast(cell_n AS BIGINT) * xx) AS sum_xx
			, sum(cell_n * BB_enter_CusCan) AS sum_BB_enter_CusCan
			, sum(cell_n * BB_enter_SysCan) AS sum_BB_enter_SysCan
			, sum(cell_n * BB_enter_HM) AS sum_BB_enter_HM
			, sum(cell_n * BB_enter_3rd_party) AS sum_BB_enter_3rd_party
			, sum(cell_n * BB_Offer_Applied) AS sum_BB_Offer_Applied
			, sum(cell_n * x_BB_enter_CusCan) AS sum_x_BB_enter_CusCan
			, sum(cell_n * x_BB_enter_SysCan) AS sum_x_BB_enter_SysCan
			, sum(cell_n * x_BB_enter_HM) AS sum_x_BB_enter_HM
			, sum(cell_n * x_BB_enter_3rd_party) AS sum_x_BB_enter_3rd_party
			, sum(cell_n * x_BB_Offer_Applied) AS sum_x_BB_Offer_Applied
			, cast(NULL AS REAL) AS b0_BB_enter_CusCan
			, cast(NULL AS REAL) AS b0_BB_enter_SysCan
			, cast(NULL AS REAL) AS b0_BB_enter_HM
			, cast(NULL AS REAL) AS b0_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b0_BB_Offer_Applied
			, cast(NULL AS REAL) AS b1_BB_enter_CusCan
			, cast(NULL AS REAL) AS b1_BB_enter_SysCan
			, cast(NULL AS REAL) AS b1_BB_enter_HM
			, cast(NULL AS REAL) AS b1_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b1_BB_Offer_Applied
		INTO #Regr_coeff2
		FROM #Regr_inputs2
		GROUP BY SABB_forecast_segment
		HAVING n > 1000;

	UPDATE #Regr_coeff2 
	SET  b1_BB_enter_CusCan = (sum_x_BB_enter_CusCan - (sum_BB_enter_CusCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_SysCan = (sum_x_BB_enter_SysCan - (sum_BB_enter_SysCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_HM 	= (sum_x_BB_enter_HM - (sum_BB_enter_HM * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party - (sum_BB_enter_3rd_party * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_Offer_Applied = (sum_x_BB_Offer_Applied - (sum_BB_Offer_Applied * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff2
	SET  b0_BB_enter_CusCan = sum_BB_enter_CusCan / n - b1_BB_enter_CusCan * sum_x / n
		,b0_BB_enter_SysCan = sum_BB_enter_SysCan / n - b1_BB_enter_SysCan * sum_x / n
		,b0_BB_enter_HM 	= sum_BB_enter_HM / n - b1_BB_enter_HM * sum_x / n
		,b0_BB_enter_3rd_party = sum_BB_enter_3rd_party / n - b1_BB_enter_3rd_party * sum_x / n
		,b0_BB_Offer_Applied = sum_BB_Offer_Applied / n - b1_BB_Offer_Applied * sum_x / n;

	DROP TABLE #t_acct;

	-----------------------------------------------------------------------------------------------------------
	----------  Pipeline entry events -----------------------------------------------------------------
	-----------------------------------------------------------------------------------------------------------
	-- pass 1
	DROP TABLE

	IF EXISTS #Regr_inputs;
		SELECT quarter_id
			, agg.SABB_forecast_segment
			, row_number() OVER (
				PARTITION BY agg.SABB_forecast_segment ORDER BY quarter_id DESC
				) AS x
			, sum(cast(BB_enter_CusCan AS REAL)) / sum(n) AS BB_enter_CusCan
			, sum(cast(BB_enter_SysCan AS REAL)) / sum(n) AS BB_enter_SysCan
			, sum(cast(BB_enter_HM AS REAL)) / sum(n) AS BB_enter_HM
			, sum(cast(BB_enter_3rd_party AS REAL)) / sum(n) AS BB_enter_3rd_party
			, sum(cast(BB_Offer_Applied AS REAL)) / sum(n) AS BB_Offer_Applied
			, x * x AS xx
			, x * BB_enter_CusCan AS x_BB_enter_CusCan
			, x * BB_enter_SysCan AS x_BB_enter_SysCan
			, x * BB_enter_HM AS x_BB_enter_HM
			, x * BB_enter_3rd_party AS x_BB_enter_3rd_party
			, x * BB_Offer_Applied AS x_BB_Offer_Applied
			, Sum(n) AS cell_n
			, cast(NULL AS REAL) AS BB_enter_CusCan_regression
			, cast(NULL AS REAL) AS BB_enter_SysCan_regression
			, cast(NULL AS REAL) AS BB_enter_HM_regression
			, cast(NULL AS REAL) AS BB_enter_3rd_party_regression
			, cast(NULL AS REAL) AS BB_Offer_Applied_regression
		INTO #Regr_inputs
		FROM #SABB_weekly_agg AS agg
		GROUP BY quarter_id
			, agg.SABB_forecast_segment;

	DROP TABLE

	IF EXISTS #Regr_coeff;
		SELECT SABB_forecast_segment
			, sum(cell_n) AS n
			, sum(cast(cell_n AS BIGINT) * x) AS sum_x
			, sum(cast(cell_n AS BIGINT) * xx) AS sum_xx
			, sum(cell_n * BB_enter_CusCan) AS sum_BB_enter_CusCan
			, sum(cell_n * BB_enter_SysCan) AS sum_BB_enter_SysCan
			, sum(cell_n * BB_enter_HM) AS sum_BB_enter_HM
			, sum(cell_n * BB_enter_3rd_party) AS sum_BB_enter_3rd_party
			, sum(cell_n * BB_Offer_Applied) AS sum_BB_Offer_Applied
			, sum(cell_n * x_BB_enter_CusCan) AS sum_x_BB_enter_CusCan
			, sum(cell_n * x_BB_enter_SysCan) AS sum_x_BB_enter_SysCan
			, sum(cell_n * x_BB_enter_HM) AS sum_x_BB_enter_HM
			, sum(cell_n * x_BB_enter_3rd_party) AS sum_x_BB_enter_3rd_party
			, sum(cell_n * x_BB_Offer_Applied) AS sum_x_BB_Offer_Applied
			, cast(NULL AS REAL) AS b0_BB_enter_CusCan
			, cast(NULL AS REAL) AS b0_BB_enter_SysCan
			, cast(NULL AS REAL) AS b0_BB_enter_HM
			, cast(NULL AS REAL) AS b0_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b0_BB_Offer_Applied
			, cast(NULL AS REAL) AS b1_BB_enter_CusCan
			, cast(NULL AS REAL) AS b1_BB_enter_SysCan
			, cast(NULL AS REAL) AS b1_BB_enter_HM
			, cast(NULL AS REAL) AS b1_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b1_BB_Offer_Applied
		INTO #Regr_coeff
		FROM #Regr_inputs
		GROUP BY SABB_forecast_segment
		HAVING n > 1000;

	UPDATE #Regr_coeff
	SET  b1_BB_enter_CusCan = (sum_x_BB_enter_CusCan - (sum_BB_enter_CusCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_SysCan = (sum_x_BB_enter_SysCan - (sum_BB_enter_SysCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_HM 	= (sum_x_BB_enter_HM - (sum_BB_enter_HM * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party - (sum_BB_enter_3rd_party * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_Offer_Applied = (sum_x_BB_Offer_Applied - (sum_BB_Offer_Applied * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff
	SET  b0_BB_enter_CusCan = sum_BB_enter_CusCan / n - b1_BB_enter_CusCan * sum_x / n
		,b0_BB_enter_SysCan = sum_BB_enter_SysCan / n - b1_BB_enter_SysCan * sum_x / n
		,b0_BB_enter_HM 	= sum_BB_enter_HM / n - b1_BB_enter_HM * sum_x / n
		,b0_BB_enter_3rd_party = sum_BB_enter_3rd_party / n - b1_BB_enter_3rd_party * sum_x / n
		,b0_BB_Offer_Applied = sum_BB_Offer_Applied / n - b1_BB_Offer_Applied * sum_x / n;

	-- pass 2
	DROP TABLE IF EXISTS #Regr_inputs2;
		SELECT quarter_id
			, agg.SABB_forecast_segment
			, row_number() OVER (
				PARTITION BY agg.SABB_forecast_segment ORDER BY quarter_id DESC
				) AS x
			, sum(cast(BB_enter_CusCan AS REAL)) / sum(n) AS BB_enter_CusCan
			, sum(cast(BB_enter_SysCan AS REAL)) / sum(n) AS BB_enter_SysCan
			, sum(cast(BB_enter_HM AS REAL)) / sum(n) AS BB_enter_HM
			, sum(cast(BB_enter_3rd_party AS REAL)) / sum(n) AS BB_enter_3rd_party
			, sum(cast(BB_Offer_Applied AS REAL)) / sum(n) AS BB_Offer_Applied
			, x * x AS xx
			, x * BB_enter_CusCan AS x_BB_enter_CusCan
			, x * BB_enter_SysCan AS x_BB_enter_SysCan
			, x * BB_enter_HM AS x_BB_enter_HM
			, x * BB_enter_3rd_party AS x_BB_enter_3rd_party
			, x * BB_Offer_Applied AS x_BB_Offer_Applied
			, Sum(n) AS cell_n
			, cast(NULL AS REAL) AS BB_enter_CusCan_regression
			, cast(NULL AS REAL) AS BB_enter_SysCan_regression
			, cast(NULL AS REAL) AS BB_enter_HM_regression
			, cast(NULL AS REAL) AS BB_enter_3rd_party_regression
			, cast(NULL AS REAL) AS BB_Offer_Applied_regression
		INTO #Regr_inputs2
		FROM #SABB_weekly_agg2 AS agg
		GROUP BY quarter_id
			, agg.SABB_forecast_segment;

	DROP TABLE IF EXISTS #Regr_coeff2;
	
		SELECT SABB_forecast_segment
			, sum(cell_n) AS n
			, sum(cast(cell_n AS BIGINT) * x) AS sum_x
			, sum(cast(cell_n AS BIGINT) * xx) AS sum_xx
			, sum(cell_n * BB_enter_CusCan) AS sum_BB_enter_CusCan
			, sum(cell_n * BB_enter_SysCan) AS sum_BB_enter_SysCan
			, sum(cell_n * BB_enter_HM) AS sum_BB_enter_HM
			, sum(cell_n * BB_enter_3rd_party) AS sum_BB_enter_3rd_party
			, sum(cell_n * BB_Offer_Applied) AS sum_BB_Offer_Applied
			, sum(cell_n * x_BB_enter_CusCan) AS sum_x_BB_enter_CusCan
			, sum(cell_n * x_BB_enter_SysCan) AS sum_x_BB_enter_SysCan
			, sum(cell_n * x_BB_enter_HM) AS sum_x_BB_enter_HM
			, sum(cell_n * x_BB_enter_3rd_party) AS sum_x_BB_enter_3rd_party
			, sum(cell_n * x_BB_Offer_Applied) AS sum_x_BB_Offer_Applied
			, cast(NULL AS REAL) AS b0_BB_enter_CusCan
			, cast(NULL AS REAL) AS b0_BB_enter_SysCan
			, cast(NULL AS REAL) AS b0_BB_enter_HM
			, cast(NULL AS REAL) AS b0_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b0_BB_Offer_Applied
			, cast(NULL AS REAL) AS b1_BB_enter_CusCan
			, cast(NULL AS REAL) AS b1_BB_enter_SysCan
			, cast(NULL AS REAL) AS b1_BB_enter_HM
			, cast(NULL AS REAL) AS b1_BB_enter_3rd_party
			, cast(NULL AS REAL) AS b1_BB_Offer_Applied
		INTO #Regr_coeff2
		FROM #Regr_inputs2
		GROUP BY SABB_forecast_segment
		HAVING n > 1000;

	UPDATE #Regr_coeff2
	SET  b1_BB_enter_CusCan = (sum_x_BB_enter_CusCan - (sum_BB_enter_CusCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_SysCan = (sum_x_BB_enter_SysCan - (sum_BB_enter_SysCan * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_HM 	= (sum_x_BB_enter_HM - (sum_BB_enter_HM * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_enter_3rd_party = (sum_x_BB_enter_3rd_party - (sum_BB_enter_3rd_party * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n)
		,b1_BB_Offer_Applied = (sum_x_BB_Offer_Applied - (sum_BB_Offer_Applied * sum_x) / n) / (sum_xx - (sum_x * sum_x) / n);

	UPDATE #Regr_coeff2
	SET  b0_BB_enter_CusCan = sum_BB_enter_CusCan / n - b1_BB_enter_CusCan * sum_x / n
		,b0_BB_enter_SysCan = sum_BB_enter_SysCan / n - b1_BB_enter_SysCan * sum_x / n
		,b0_BB_enter_HM 	= sum_BB_enter_HM / n - b1_BB_enter_HM * sum_x / n
		,b0_BB_enter_3rd_party = sum_BB_enter_3rd_party / n - b1_BB_enter_3rd_party * sum_x / n
		,b0_BB_Offer_Applied = sum_BB_Offer_Applied / n - b1_BB_Offer_Applied * sum_x / n;

	---------------------------------------------------------------------------------------------------
	-- Set proc outputs -------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------------------
	SELECT *
	INTO FCAST_Regr_Coeffs
	FROM (
		SELECT LV
			, 'CusCan Entry' AS Metric
			, SABB_forecast_segment AS fcast_segment
			, b1_BB_enter_CusCan AS grad_coeff
			, b0_BB_enter_CusCan AS intercept_coeff
		FROM #Regr_coeff
		
		UNION ALL
		
		SELECT LV
			, 'SysCan Entry' AS Metric
			, SABB_forecast_segment AS fcast_segment
			, b1_BB_enter_SysCan AS grad_coeff
			, b0_BB_enter_SysCan AS intercept_coeff
		FROM #Regr_coeff2
		
		UNION ALL
		
		SELECT LV
			, 'HM Entry' AS Metric
			, SABB_forecast_segment AS fcast_segment
			, b1_BB_enter_HM AS grad_coeff
			, b0_BB_enter_HM AS intercept_coeff
		FROM #Regr_coeff
		
		UNION ALL
		
		SELECT LV
			, '3rd Party Entry' AS Metric
			, SABB_forecast_segment AS fcast_segment
			, b1_BB_enter_3rd_party AS grad_coeff
			, b0_BB_enter_3rd_party AS intercept_coeff
		FROM #Regr_coeff
		
		UNION ALL
		
		SELECT LV
			, 'BB Offer Applied' AS Metric
			, SABB_forecast_segment AS fcast_segment
			, b1_BB_Offer_Applied AS grad_coeff
			, b0_BB_Offer_Applied AS intercept_coeff
		FROM #Regr_coeff
		) a;

	DROP TABLE IF EXISTS skyplus;
	
	END 
	GO
