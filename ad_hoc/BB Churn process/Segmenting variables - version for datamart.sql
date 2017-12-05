
/*--========================================================================================
--------------------- BB-churn forecast required variables population
	--	This code adds the required variables to produce the BB churn propensity segments in the actual CUST_FCAST_WEEKLY_BASE and DTV_FCAST_WEEKLY_BASE
	-- 	The variables included in the CUST_FCAST_WEEKLY_BASE are the raw version, which then are binned for the proper usage in the DTV_FCAST_WEEKLY_BASE
	-- 	Some of then are used in the raw version in the segmentation which mean that the same version will be availabe in both tables 

	Variable included 
 	-CQM_Segment
	-DTV_TA_calls_1m
	-RTM
	-talk_tenure
	-my_sky_login_3m
	-BB_all_calls_1m
	-home_owner_status
	-talk_type
	-BB_offer_rem_and_end
	-BB_tenure
	

======================================================================================== */



--========================================================================================
--------------------- DTV_TA_calls_1m
--========================================================================================
SELECT base.account_number
		,base.end_date
		,SUM(ta_c.total_calls) AS Num_past1m_TA
INTO #ta_previous
FROM CUST_FCAST_WEEKLY_BASE 	AS base
JOIN view_cust_calls_hist 				AS ta_c		ON base.account_number = ta_c.account_number 
												AND ta_c.event_dt BETWEEN DATEADD(mm,-1,base.end_date) AND DATEADD(dd,-1,base.end_date) 
												AND ta_c.DTV = 1 
												AND ta_c.typeofevent IN ('TA') 
GROUP BY base.account_number, base.end_date

COMMIT
CREATE HG INDEX ID1 ON #ta_previous(account_number)
CREATE DATE INDEX ID2 ON #ta_previous(end_date)
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET DTV_TA_calls_1m_raw = Num_past1m_TA 
FROM CUST_FCAST_WEEKLY_BASE AS base
JOIN #ta_previous AS t1 ON base.account_number = t1.account_number AND base.end_date = t1.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET DTV_TA_calls_1m = CASE WHEN Num_past1m_TA > 0 THEN 1 ELSE 0 END 
FROM DTV_FCAST_WEEKLY_BASE AS base
JOIN #ta_previous AS t1 ON base.account_number = t1.account_number AND base.end_date = t1.end_date

MESSAGE 'DTV_TA_calls_1m updated: '||@@rowcount type status to client
COMMIT 
DROP TABLE #ta_previous
GO
--========================================================================================
--------------------- RTM
--========================================================================================

 
SELECT BASE.account_number
		, end_date
      ,RANK() over(PARTITION BY bbo.account_number,end_date ORDER BY order_dt DESC) AS rank_1
      ,rtm_level_1
INTO #RTM
FROM CUST_FCAST_WEEKLY_BASE 	AS base
INNER JOIN citeam.DM_BROADBAND_ORDERS 	AS bbo	ON bbo.account_number = base.account_number 
WHERE CHANGE_TYPE NOT IN ('A','C','M','R') 
AND bbo.order_dt <= end_date

COMMIT
DELETE FROM #RTM  WHERE rank_1 <> 1
CREATE HG INDEX ID1 ON #RTM(account_number)
CREATE DATE INDEX ID2 ON #RTM(end_date)
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET RTM    = rtm_level_1
FROM CUST_FCAST_WEEKLY_BASE AS base
JOIN #RTM ON #RTM.account_number = base.account_number AND base.end_date = #RTM.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET RTM    = rtm_level_1
FROM DTV_FCAST_WEEKLY_BASE AS base
JOIN #RTM ON #RTM.account_number = base.account_number AND base.end_date = #RTM.end_date


MESSAGE 'talk_tenure updated: '||@@rowcount type status to client
COMMIT 
DROP TABLE #RTM
GO
--========================================================================================
--------------------- talk_tenure
--========================================================================================

SELECT CSH.account_number
		,end_date
		,MAX(CSH.effective_from_dt) AS DT_last_TALK
		,DATEDIFF(day, DT_last_TALK, end_date)  AS tenure
INTO #talk_tenure
FROM cust_subs_hist 			AS CSH
JOIN CUST_FCAST_WEEKLY_BASE 			AS BASE   ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
	AND   csh.status_code = 'A'
	AND   prev_status_code NOT IN ('PC','FBP','BCRQ')
	AND   CSH.status_code_changed = 'Y'
	AND   end_date >= effective_from_dt
GROUP BY  CSH.account_number, end_date

COMMIT 
CREATE HG INDEX ID1 ON #talk_tenure(account_number)
CREATE DATE INDEX ID2 ON #talk_tenure(end_date)
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET talk_tenure_raw = tenure 
FROM CUST_FCAST_WEEKLY_BASE 		AS base
JOIN #talk_tenure 			AS hold ON hold.account_number = base.account_number and BASE.end_date = HOLD.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET talk_tenure = CASE 	WHEN tenure <= 65 	THEN 1 
						WHEN tenure <= 203 	THEN 2
						WHEN tenure <= 351 	THEN 3
						WHEN tenure <= 512 	THEN 4
						WHEN tenure <= 699 	THEN 5
						WHEN tenure <= 932 	THEN 6
						WHEN tenure <= 1234	THEN 7
						WHEN tenure <= 1645	THEN 8
						WHEN tenure <= 2216	THEN 9
						WHEN tenure > 2216	THEN 10
						ELSE 0 END 

FROM DTV_FCAST_WEEKLY_BASE 		AS base
JOIN #talk_tenure 			AS hold ON hold.account_number = base.account_number and BASE.end_date = HOLD.end_date

MESSAGE 'talk_tenure updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #talk_tenure 
GO

--========================================================================================
--------------------- my_sky_login_3m
--========================================================================================
SELECT COUNT(DISTINCT visit_date) AS visit_days
      , mr.account_number
	  , end_date
INTO #days_visited_3m
FROM vespa_shared.mysky_daily_usage 	AS mr 
JOIN CUST_FCAST_WEEKLY_BASE 	AS base ON BASE.account_number = mr.account_number
WHERE visit_date BETWEEN DATEADD(mm,-3,end_date) AND end_date
GROUP BY mr.account_number, end_date

COMMIT 
CREATE HG INDEX id1 ON #days_visited_3m (account_number) 
CREATE DATE INDEX ID2 ON #days_visited_3m(end_date)
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET my_sky_login_3m_raw = visit_days 
FROM CUST_FCAST_WEEKLY_BASE AS base
INNER JOIN #days_visited_3m AS dv ON base.account_number = dv.account_number AND base.end_date = dv.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET my_sky_login_3m = CASE WHEN visit_days > 2 THEN 3 ELSE visit_days END 
FROM DTV_FCAST_WEEKLY_BASE AS base
INNER JOIN #days_visited_3m AS dv ON base.account_number = dv.account_number AND base.end_date = dv.end_date

MESSAGE 'my_sky_login_3m updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #days_visited_3m
GO 

--========================================================================================
--------------------- BB_all_calls_1m
--========================================================================================

SELECT base.account_number
      , COUNT(1) AS call_count
	  , end_date
INTO #BBCalls_Temp_1m
FROM cust_inbound_calls					AS temp
JOIN CUST_FCAST_WEEKLY_BASE 	AS base 	ON    base.account_number = temp.account_number
WHERE call_date BETWEEN DATEADD(mm,-1,end_date) AND DATEADD(dd,-1,end_date)
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
GROUP BY base.account_number, end_date

COMMIT 
CREATE HG INDEX id1 ON #BBCalls_Temp_1m (account_number) 
CREATE DATE INDEX ID2 ON #BBCalls_Temp_1m(end_date)
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET BB_all_calls_1m_raw = call_count 
FROM CUST_FCAST_WEEKLY_BASE 	AS base
JOIN #BBCalls_Temp_1m 					AS temp ON base.account_number = temp.account_number AND base.end_date = temp.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET BB_all_calls_1m = CASE WHEN call_count = 0 THEN 0 ELSE 1 END  
FROM DTV_FCAST_WEEKLY_BASE 	AS base
JOIN #BBCalls_Temp_1m 					AS temp ON base.account_number = temp.account_number AND base.end_date = temp.end_date

MESSAGE 'BB_all_calls_1m updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #BBCalls_Temp_1m
GO


--========================================================================================
--------------------- home_owner_status
--========================================================================================
SELECT cv.cb_key_household
		,CASE WHEN h_tenure_v2 = '0' THEN 'Owner'
            WHEN h_tenure_v2 = '1' THEN 'Private Rent'
            WHEN h_tenure_v2 = '2' THEN 'Council Rent'
            ELSE 'UNKNOWN'
 		  END AS home_owner       
		  , end_date
		,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id DESC) AS rank_1
INTO #experian 
FROM EXPERIAN_CONSUMERVIEW 		AS CV
JOIN CUST_FCAST_WEEKLY_BASE 			AS base	ON cv.cb_key_household = base.hh_key

COMMIT 
DELETE FROM #experian WHERE rank_1 <> 1 
CREATE HG INDEX id1 ON #experian (cb_key_household) 
CREATE DATE INDEX ID2 ON #experian(end_date)
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET home_owner_status = home_owner              
FROM CUST_FCAST_WEEKLY_BASE AS base
INNER JOIN #experian ON #experian.cb_key_household = base.hh_key 

UPDATE DTV_FCAST_WEEKLY_BASE
SET home_owner_status = home_owner              
FROM DTV_FCAST_WEEKLY_BASE AS base
INNER JOIN #experian ON #experian.cb_key_household = base.hh_key 


MESSAGE 'home_owner_status updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #experian
GO

--========================================================================================
--------------------- talk_type
--========================================================================================

SELECT CSH.account_number
      ,current_product_description
	  ,end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt ASC, CSH.cb_row_id ASC) AS rank_1  
INTO #talk_holding
FROM cust_subs_hist 						AS CSH
INNER JOIN CUST_FCAST_WEEKLY_BASE AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
AND   csh.status_code IN ('A','PC','FBP','RI','FBI','BCRQ')
AND   end_date BETWEEN effective_from_dt AND effective_to_dt
AND   effective_to_dt > effective_from_dt

COMMIT 
DELETE FROM #talk_holding WHERE rank_1 <> 1 
CREATE HG INDEX id1 ON #talk_holding (account_number) 
CREATE DATE INDEX id2 ON #talk_holding (end_date) 
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET talk_type = SET talk_type = CASE WHEN current_product_description LIKE 'Sky Talk 24 / 7%' THEN 'Sky Talk 24 / 7'
                     WHEN current_product_description LIKE 'Sky Talk Anytime Extra%' THEN 'Sky Talk Anytime Extra'
                     WHEN current_product_description LIKE 'Anytime%' THEN 'Sky Talk Anytime'
                     WHEN current_product_description LIKE 'Off Peak%' THEN 'Off Peak'
                     WHEN current_product_description LIKE 'Sky Talk Freetime%' THEN 'Sky Talk Freetime'
                     WHEN current_product_description LIKE 'Sky Talk International Extra%' THEN 'Sky Talk International Extra'
                     WHEN current_product_description LIKE 'Sky Talk Unlimited%' THEN 'Sky Talk Unlimited'
                     WHEN current_product_description LIKE 'Sky Talk Anytime%' THEN 'Sky Talk Anytime'
                     WHEN current_product_description LIKE 'Syk Talk Evenings and Weekends%' THEN 'Sky Talk Evenings and Weekends'
                     WHEN current_product_description LIKE 'Missing at load' THEN 'NONE'
                     ELSE current_product_description END
FROM CUST_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #talk_holding 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET talk_type = CASE WHEN current_product_description LIKE 'Sky Talk 24 / 7%' THEN 'Sky Talk 24 / 7'
                     WHEN current_product_description LIKE 'Sky Talk Anytime Extra%' THEN 'Sky Talk Anytime Extra'
                     WHEN current_product_description LIKE 'Anytime%' THEN 'Sky Talk Anytime'
                     WHEN current_product_description LIKE 'Off Peak%' THEN 'Off Peak'
                     WHEN current_product_description LIKE 'Sky Talk Freetime%' THEN 'Sky Talk Freetime'
                     WHEN current_product_description LIKE 'Sky Talk International Extra%' THEN 'Sky Talk International Extra'
                     WHEN current_product_description LIKE 'Sky Talk Unlimited%' THEN 'Sky Talk Unlimited'
                     WHEN current_product_description LIKE 'Sky Talk Anytime%' THEN 'Sky Talk Anytime'
                     WHEN current_product_description LIKE 'Sky Talk Evenings and Weekends%' THEN 'Sky Talk Evenings and Weekends'
                     WHEN current_product_description LIKE 'Missing at load' THEN 'NONE'
                     ELSE current_product_description END
FROM DTV_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #talk_holding 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date


MESSAGE 'talk_type updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #talk_holding
GO


--========================================================================================
--------------------- BB_offer_rem_and_end
--========================================================================================

SELECT base.account_number
      ,MAX(offer_duration) AS offer_length
      ,MAX(DATEDIFF(DD, end_date, intended_offer_end_dt)) AS length_rem
	  , BB_current_offer_duration_rem = CASE WHEN length_rem > 2854 THEN 2854
                                          WHEN length_rem < 0    THEN 0
                                          ELSE length_rem 
										END 
		, end_date 
INTO #current_bb_offer_length      
FROM CUST_FCAST_WEEKLY_BASE AS base
INNER JOIN offer_usage_all AS oua
ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line'
AND end_date >= offer_start_dt_actual
AND end_date <  offer_end_dt_actual
AND intended_total_offer_value_yearly IS NOT NULL
GROUP BY base.account_number, end_date;


SELECT base.account_number
      ,offer_end_dt_actual
      ,rank() over(PARTITION BY base.account_number, end_date ORDER BY offer_start_dt_actual DESC) AS latest_offer
	  , end_date
	  , BB_time_since_last_offer_end = DATEDIFF(DD, offer_end_dt_actual, end_date)
INTO #prev_bb_offer_dt      
FROM CUST_FCAST_WEEKLY_BASE 			AS base
INNER JOIN offer_usage_all 				AS oua 			ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line'
		AND end_date >  offer_start_dt_actual
		AND end_date >= offer_end_dt_actual
		AND intended_total_offer_value_yearly IS NOT NULL

COMMIT 
DELETE FROM #prev_bb_offer_dt      WHERE latest_offer <>1
CREATE HG INDEX id1 ON #prev_bb_offer_dt (account_number)
CREATE DATE INDEX id1 ON #prev_bb_offer_dt (offer_end_dt_actual)
COMMIT

UPDATE CUST_FCAST_WEEKLY_BASE
SET BB_offer_rem_and_end_raw =  CASE WHEN BB_current_offer_duration_rem > 0 THEN BB_current_offer_duration_rem 
								WHEN (BB_current_offer_duration_rem = 0 OR BB_current_offer_duration_rem  IS NULL) AND BB_time_since_last_offer_end <> - 9999 THEN (0 - BB_time_since_last_offer_end) 
								ELSE - 9999 END
FROM CUST_FCAST_WEEKLY_BASE		AS a	 
LEFT JOIN #current_bb_offer_length  	AS b ON a.account_number = b.account_number  AND a.end_date = b.end_date
LEFT JOIN #prev_bb_offer_dt      		AS c ON a.account_number = c.account_number  AND a.end_date = c.end_date
							
UPDATE DTV_FCAST_WEEKLY_BASE 
SET BB_offer_rem_and_end = CASE WHEN BB_offer_rem_and_end_raw BETWEEN -9998 AND -1015 	THEN -3
								WHEN BB_offer_rem_and_end_raw BETWEEN -1015 AND -215 	THEN -2 
								WHEN BB_offer_rem_and_end_raw BETWEEN -215  AND -75  	THEN -1
								WHEN BB_offer_rem_and_end_raw BETWEEN -74  AND -0    	THEN 0
								WHEN BB_offer_rem_and_end_raw BETWEEN 1    AND 62    	THEN 1
								WHEN BB_offer_rem_and_end_raw BETWEEN 63   AND 162   	THEN 2
								WHEN BB_offer_rem_and_end_raw BETWEEN 163  AND 271		THEN 3
								WHEN BB_offer_rem_and_end_raw >271						THEN 4
								ELSE -9999 END 
FROM DTV_FCAST_WEEKLY_BASE AS a
JOIN CUST_FCAST_WEEKLY_BASE AS b ON a.account_number = b.account_number  AND a.end_date = b.end_date

COMMIT 
MESSAGE 'BB_offer_rem_and_end updated: '||@@rowcount type status to client

GO								

--========================================================================================
--------------------- BB_tenure
--========================================================================================


SELECT 	csh.Account_number
		, end_date
		, MAX(effective_from_dt) max_bb_act
		, MIN(effective_from_dt) min_bb_act
		, BB_tenure_days = DATEDIFF(dd,max_bb_act,end_date)
INTO    #Act
FROM    cust_subs_hist 						AS csh
JOIN	CUST_FCAST_WEEKLY_BASE	AS acc ON  csh.account_number = acc.account_number 
													AND end_date >= effective_from_dt 
WHERE  	status_code = 'AC'
        AND Prev_status_code NOT IN ('AB','AC','PC')
        AND subscription_sub_type ='Broadband DSL Line'
        AND status_code_changed = 'Y'
		AND effective_to_dt > effective_from_dt
GROUP BY 	csh.Account_number, end_date

COMMIT 
CREATE HG INDEX id1 ON #Act (account_number) 
CREATE DATE INDEX id2 ON #Act (end_date) 
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET BB_tenure_raw = BB_tenure_days 
	 , BB_latest_act_dt = max_bb_act
	 , BB_first_act_dt  = min_bb_act
FROM CUST_FCAST_WEEKLY_BASE AS a 
JOIN #Act 			AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 

UPDATE DTV_FCAST_WEEKLY_BASE
SET BB_tenure = CASE 	WHEN BB_tenure_days <= 118 				 THEN 1
						WHEN BB_tenure_days BETWEEN 119 AND 231  THEN 2
						WHEN BB_tenure_days BETWEEN 231 AND 329  THEN 3
						WHEN BB_tenure_days BETWEEN 329 AND 391  THEN 4
						WHEN BB_tenure_days BETWEEN 392 AND 499  THEN 5
						WHEN BB_tenure_days BETWEEN 499 AND 641  THEN 6
						WHEN BB_tenure_days BETWEEN 641 AND 1593 THEN 7
						WHEN BB_tenure_days > 1593 				 THEN 8	
						ELSE -1 END 
		, BB_latest_act_dt = max_bb_act
		, BB_first_act_dt  = min_bb_act						
FROM DTV_FCAST_WEEKLY_BASE AS a 
JOIN #Act 			AS b ON a.account_number = b.account_number AND a.end_date = b.end_date 

COMMIT 
MESSAGE 'BB_tenure updated: '||@@rowcount type status to client

DROP TABLE #Act

GO


--========================================================================================
--------------------- BB_Package
--========================================================================================

SELECT CSH.account_number
      ,current_product_description
	  ,end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt DESC, CSH.cb_row_id ASC) AS rank1  
INTO #BB_PACK
FROM cust_subs_hist 						AS CSH
INNER JOIN CUST_FCAST_WEEKLY_BASE AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'Broadband DSL Line'
AND   csh.status_code IN ('AB','AC','PC','BCRQ')
AND   end_date >= effective_from_dt 
AND   effective_to_dt > effective_from_dt

COMMIT 
DELETE FROM #BB_PACK WHERE rank1 <> 1 
CREATE HG INDEX id1 	ON #BB_PACK (account_number) 
CREATE DATE INDEX id2 	ON #BB_PACK (end_date) 
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET BB_Package  =  COALESCE(current_product_description 'UNKNOWN') 
FROM CUST_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #BB_PACK 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET BB_Package  =  COALESCE(current_product_description 'UNKNOWN') 
FROM DTV_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #BB_PACK 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date

--========================================================================================
--------------------- BB_status_code
--========================================================================================


SELECT CSH.account_number
	  , status_code
	  ,end_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt DESC, CSH.cb_row_id ASC) AS rank1  
INTO #BB_status
FROM cust_subs_hist 						AS CSH
INNER JOIN CUST_FCAST_WEEKLY_BASE AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'Broadband DSL Line'
AND   end_date BETWEEN effective_from_dt AND effective_to_dt
AND   effective_to_dt > effective_from_dt

COMMIT 
DELETE FROM #BB_status WHERE rank1 <> 1 
CREATE HG INDEX id1 	ON #BB_status (account_number) 
CREATE DATE INDEX id2 	ON #BB_status (end_date) 
COMMIT 

UPDATE CUST_FCAST_WEEKLY_BASE
SET BB_status_code  =  status_code
FROM CUST_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #BB_status 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date

UPDATE DTV_FCAST_WEEKLY_BASE
SET BB_status_code  =  status_code
FROM DTV_FCAST_WEEKLY_BASE 	AS base
INNER JOIN #BB_status 				AS hold	ON hold.account_number = base.account_number AND base.end_date = hold.end_date
