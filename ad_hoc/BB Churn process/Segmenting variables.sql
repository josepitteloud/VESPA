
CREATE TABLE BB_forecast_segmenting_variables (
						account_number                   VARCHAR(12)                             
						,hh_key                          BIGINT         DEFAULT -999             
						,observation_date                DATE                                    
						--,broadband_package               VARCHAR(30)    DEFAULT 'NONE'           
						--,Simple_Segment                  VARCHAR(30)    DEFAULT 'UNKNOWN'        
						,DTV_TA_calls_1m                 INTEGER        DEFAULT 0                
						,RTM                             VARCHAR(20)    DEFAULT 'UNKNOWN'        
						,Talk_tenure                     INTEGER        DEFAULT 0                
						,my_sky_login_3m                 INTEGER        DEFAULT 0                
						,BB_all_calls_1m                 INTEGER        DEFAULT 0                
						,BB_offer_rem_and_end            FLOAT          DEFAULT -9999            
						,home_owner_status               VARCHAR(20)    DEFAULT 'UNKNOWN'        
						,BB_tenure						 INTEGER        DEFAULT 0    
						,talk_type                       VARCHAR(30)    DEFAULT 'NONE'           
						,CQM_Segment                     TINYINT        DEFAULT 0          
						,simple_segment					 VARCHAR(30)    DEFAULT NULL           
						)
						
COMMIT 						
GO
						
CREATE HG INDEX act_no 		ON BB_forecast_segmenting_variables_JAN(account_number)
CREATE HG INDEX hh_key 		ON BB_forecast_segmenting_variables_JAN(hh_key)
CREATE DATE INDEX ob_date 	ON BB_forecast_segmenting_variables_JAN(observation_date)
                                    
GRANT ALL ON BB_forecast_segmenting_variables_JAN TO vespa_group_low_security
GO
--========================================================================================
--------------------- Populating the table 
--========================================================================================
DECLARE @dt DATE
SET @dt  =  '2017-02-02'
		/*(SELECT max(end_date) 
			FROM cust_fcast_weekly_base
			WHERE  end_date <= getdate() )*/

INSERT INTO BB_forecast_segmenting_variables_JAN (account_number, hh_key, observation_date, simple_segment, BB_offer_rem_and_end) 
SELECT DISTINCT 
	  account_number
	, cb_key_household
	, end_dt
	, simple_segment
	, CASE WHEN Days_Curr_Offer_End_BB IS NOT NULL THEN Days_Curr_Offer_End_BB 
								WHEN Days_prev_offer_end_BB IS NOT NULL THEN Days_prev_offer_end_BB
								ELSE -9999 END 
FROM cust_fcast_weekly_base
WHERE end_date = @dt

MESSAGE 'TABLE populated: '||@@rowcount type status to client
COMMIT 
GO


--========================================================================================
--------------------- CQM_Segment
--========================================================================================

UPDATE BB_forecast_segmenting_variables_JAN 
SET CQM_Segment = COALESCE (CQM_Score,0) 
	, hh_key = cb_key_household
FROM BB_forecast_segmenting_variables_JAN 	AS a 
JOIN attach_view_all					AS b ON a.account_number = b.account_number 
											AND CAST(	YEAR(a.observation_date)||(RIGHT('00'||MONTH(a.observation_date),2)) AS INT )= monthyear

COMMIT 
MESSAGE 'CQM_Segment updated: '||@@rowcount type status to client

GO




--========================================================================================
--------------------- DTV_TA_calls_1m
--========================================================================================



SELECT base.account_number
		,base.Observation_date
		,SUM(ta_c.total_calls) AS Num_past1m_TA
INTO #ta_previous
FROM BB_forecast_segmenting_variables_JAN 	AS base
JOIN view_cust_calls_hist 				AS ta_c		ON base.account_number = ta_c.account_number 
												AND ta_c.event_dt BETWEEN DATEADD(mm,-1,base.Observation_date) AND DATEADD(dd,-1,base.Observation_date) 
												AND ta_c.DTV = 1 
												AND ta_c.typeofevent IN ('TA') 
GROUP BY base.account_number, base.Observation_date

COMMIT

UPDATE BB_forecast_segmenting_variables_JAN
SET DTV_TA_calls_1m = CASE WHEN Num_past1m_TA > 0 THEN 1 ELSE 0 END 
FROM BB_forecast_segmenting_variables_JAN AS base
JOIN #ta_previous AS t1 ON base.account_number = t1.account_number AND base.Observation_date = t1.Observation_date

MESSAGE 'DTV_TA_calls_1m updated: '||@@rowcount type status to client
COMMIT 
DROP TABLE #ta_previous
GO
--========================================================================================
--------------------- RTM
--========================================================================================

 
SELECT BASE.account_number
		, Observation_date
      ,RANK() over(PARTITION BY bbo.account_number,Observation_date ORDER BY order_dt DESC) AS rank
      ,rtm_level_1
INTO #RTM
FROM BB_forecast_segmenting_variables_JAN 	AS base
INNER JOIN citeam.DM_BROADBAND_ORDERS 	AS bbo	ON bbo.account_number = base.account_number 
WHERE CHANGE_TYPE NOT IN ('A','C','M','R') 
AND bbo.order_dt <= Observation_date

COMMIT

UPDATE BB_forecast_segmenting_variables_JAN
SET RTM    = rtm_level_1
FROM BB_forecast_segmenting_variables_JAN AS base
JOIN #RTM ON #RTM.account_number = base.account_number AND base.Observation_date = #RTM.Observation_date
WHERE rank = 1

MESSAGE 'talk_tenure updated: '||@@rowcount type status to client
COMMIT 
DROP TABLE #RTM
GO
--========================================================================================
--------------------- talk_tenure
--========================================================================================

SELECT CSH.account_number
		,Observation_date
		,MAX(CSH.effective_from_dt) AS DT_last_TALK
		,DATEDIFF(day, DT_last_TALK, Observation_date)  AS tenure
INTO #talk_tenure
FROM cust_subs_hist 			AS CSH
JOIN BB_forecast_segmenting_variables_JAN 			AS BASE   ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
	AND   csh.status_code = 'A'
	AND   prev_status_code NOT IN ('PC','FBP','BCRQ')
	AND   CSH.status_code_changed = 'Y'
	AND   observation_date >= effective_from_dt
GROUP BY  CSH.account_number, Observation_date

COMMIT 

UPDATE BB_forecast_segmenting_variables_JAN
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

FROM BB_forecast_segmenting_variables_JAN 		AS base
JOIN #talk_tenure 			AS hold ON hold.account_number = base.account_number and BASE.Observation_date = HOLD.Observation_date

MESSAGE 'talk_tenure updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #talk_tenure 
GO

--========================================================================================
--------------------- my_sky_login_3m
--========================================================================================
SELECT COUNT(DISTINCT visit_date) AS visit_days
      , mr.account_number
	  , Observation_date
INTO #days_visited_3m
FROM vespa_shared.mysky_daily_usage 	AS mr 
JOIN BB_forecast_segmenting_variables_JAN 	AS base ON BASE.account_number = mr.account_number
WHERE visit_date BETWEEN DATEADD(mm,-3,Observation_date) AND Observation_date
GROUP BY mr.account_number, Observation_date
COMMIT 
CREATE HG INDEX id1 ON #days_visited_3m (account_number) 

UPDATE BB_forecast_segmenting_variables_JAN
SET my_sky_login_3m = CASE WHEN visit_days > 2 THEN 3 ELSE visit_days END 
FROM BB_forecast_segmenting_variables_JAN AS base
INNER JOIN #days_visited_3m AS dv ON base.account_number = dv.account_number AND base.Observation_date = dv.Observation_date

MESSAGE 'my_sky_login_3m updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #days_visited_3m
GO 

--========================================================================================
--------------------- BB_all_calls_1m
--========================================================================================

SELECT base.account_number
      , COUNT(1) AS call_count
	  , Observation_date
INTO #BBCalls_Temp_1m
FROM cust_inbound_calls					AS temp
JOIN BB_forecast_segmenting_variables_JAN 	AS base 	ON    base.account_number = temp.account_number
WHERE call_date BETWEEN DATEADD(mm,-1,Observation_date) AND DATEADD(dd,-1,Observation_date)
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
GROUP BY base.account_number, Observation_date

COMMIT 
CREATE HG INDEX id1 ON #BBCalls_Temp_1m (account_number) 
COMMIT 

UPDATE BB_forecast_segmenting_variables_JAN
SET BB_all_calls_1m = CASE WHEN call_count = 0 THEN 0 ELSE 1 END 
FROM BB_forecast_segmenting_variables_JAN 	AS base
JOIN #BBCalls_Temp_1m 					AS temp ON base.account_number = temp.account_number AND base.Observation_date = temp.Observation_date

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
		,rank() over(PARTITION BY cv.cb_key_household ORDER BY cb_row_id DESC) AS rank
INTO #experian 
FROM EXPERIAN_CONSUMERVIEW 		AS CV
JOIN BB_forecast_segmenting_variables_JAN 			AS base	ON cv.cb_key_household = base.hh_key

COMMIT 
CREATE HG INDEX id1 ON #experian (cb_key_household) 
COMMIT 

UPDATE BB_forecast_segmenting_variables_JAN
SET home_owner_status = home_owner              
FROM BB_forecast_segmenting_variables_JAN AS base
INNER JOIN #experian ON #experian.cb_key_household = base.hh_key 
WHERE rank = 1

MESSAGE 'home_owner_status updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #experian
GO

--========================================================================================
--------------------- talk_type
--========================================================================================

SELECT CSH.account_number
      ,current_product_description
	  ,observation_date
      ,rank() over(PARTITION BY base.account_number ORDER BY effective_from_dt ASC, CSH.cb_row_id ASC) AS rank  
INTO #talk_holding
FROM cust_subs_hist 						AS CSH
INNER JOIN BB_forecast_segmenting_variables_JAN AS BASE  ON BASE.account_number = CSH.account_number
WHERE csh.subscription_sub_type = 'SKY TALK SELECT'
AND   csh.status_code IN ('A','PC','FBP','RI','FBI','BCRQ')
AND   observation_date BETWEEN effective_from_dt AND effective_to_dt
AND   effective_to_dt > effective_from_dt

COMMIT 
CREATE HG INDEX id1 ON #talk_holding (account_number) 
COMMIT 

UPDATE BB_forecast_segmenting_variables_JAN
SET talk_type = current_product_description
FROM BB_forecast_segmenting_variables_JAN 	AS base
INNER JOIN #talk_holding 				AS hold	ON hold.account_number = base.account_number AND base.Observation_date = hold.Observation_date
WHERE rank = 1

MESSAGE 'talk_type updated: '||@@rowcount type status to client

COMMIT 
DROP TABLE #talk_holding
GO


--========================================================================================
--------------------- BB_offer_rem_and_end
--========================================================================================
ALTER TABLE BB_forecast_segmenting_variables_JAN
ADD BB_current_offer_duration_rem INT
--Current BB Offer Length
SELECT base.account_number
      ,MAX(offer_duration) AS offer_length
      ,MAX(DATEDIFF(DD,Observation_date,intended_offer_end_dt)) AS length_rem
INTO #current_bb_offer_length      
FROM BB_forecast_segmenting_variables_JAN AS base
INNER JOIN offer_usage_all AS oua
ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line'
AND Observation_date >= offer_start_dt_actual
AND Observation_date <  offer_end_dt_actual
AND intended_total_offer_value_yearly IS NOT NULL
GROUP BY base.account_number;

UPDATE BB_forecast_segmenting_variables_JAN
SET BB_current_offer_duration_rem = CASE WHEN length_rem > 2854 THEN 2854
                                          WHEN length_rem < 0    THEN 0
                                          ELSE length_rem 
                                    END 
FROM BB_forecast_segmenting_variables_JAN AS base
INNER JOIN #current_bb_offer_length AS offer
ON offer.account_number = base.account_number;


--Last BB Offer End Date
SELECT base.account_number
	, offer_end_dt_actual
	, rank() OVER (PARTITION BY base.account_number ORDER BY offer_start_dt_actual DESC) AS latest_offer
INTO #prev_bb_offer_dt
FROM BB_forecast_segmenting_variables_JAN AS base
INNER JOIN offer_usage_all AS oua ON oua.account_number = base.account_number
WHERE subs_type = 'Broadband DSL Line' 
		AND Observation_date > offer_start_dt_actual 
		AND Observation_date >= offer_end_dt_actual 
		AND intended_total_offer_value_yearly IS NOT NULL;

UPDATE BB_forecast_segmenting_variables_JAN
SET BB_time_since_last_offer_end = DATEDIFF(DD, offer_end_dt_actual, Observation_date)
FROM BB_forecast_segmenting_variables_JAN AS base
INNER JOIN #prev_bb_offer_dt AS offer ON offer.account_number = base.account_number
WHERE latest_offer = 1;

--Combined BB Offer Start and End
UPDATE BB_forecast_segmenting_variables_JAN
SET BB_offer_rem_and_end = CASE WHEN BB_current_offer_duration_rem > 0 THEN BB_current_offer_duration_rem 
								WHEN BB_current_offer_duration_rem = 0 AND BB_time_since_last_offer_end <> - 9999 THEN (0 - BB_time_since_last_offer_end) 
								ELSE - 9999 END;
								
								
--========================================================================================
UPDATE BB_forecast_segmenting_variables_JAN 
SET BB_offer_rem_and_end = CASE WHEN BB_offer_rem_and_end BETWEEN -9998 AND -1015 	THEN -3
								WHEN BB_offer_rem_and_end BETWEEN -1015 AND -215 	THEN -2 
								WHEN BB_offer_rem_and_end BETWEEN -215  AND -75  	THEN -1
								WHEN BB_offer_rem_and_end BETWEEN -74  AND -0    	THEN 0
								WHEN BB_offer_rem_and_end BETWEEN 1    AND 62    	THEN 1
								WHEN BB_offer_rem_and_end BETWEEN 63   AND 162   	THEN 2
								WHEN BB_offer_rem_and_end BETWEEN 163  AND 271		THEN 3
								WHEN BB_offer_rem_and_end >271						THEN 4
								ELSE -9999 END 

COMMIT 
MESSAGE 'BB_offer_rem_and_end updated: '||@@rowcount type status to client

GO								



--========================================================================================
--------------------- BB_tenure
--========================================================================================

ac
SELECT 	csh.Account_number
		, observation_date
		, MAX(effective_from_dt) max_bb_act
		, BB_tenure_days = DATEDIFF(dd,max_bb_act,Observation_date)
INTO    #Act
FROM    cust_subs_hist 						AS csh
JOIN	BB_forecast_segmenting_variables_JAN	AS acc ON  csh.account_number = acc.account_number 
													AND observation_date BETWEEN effective_from_dt AND effective_to_dt
WHERE  	status_code = 'AC'
        AND Prev_status_code NOT IN ('AB','AC','PC')
        AND subscription_sub_type ='Broadband DSL Line'
        AND status_code_changed = 'Y'
		AND effective_to_dt > effective_from_dt
GROUP BY 	csh.Account_number, observation_date

COMMIT 
CREATE HG INDEX id1 ON #Act (account_number) 
COMMIT 

UPDATE BB_forecast_segmenting_variables_JAN
SET BB_tenure = CASE 	WHEN BB_tenure_days <= 118 				 THEN 1
						WHEN BB_tenure_days BETWEEN 119 AND 231  THEN 2
						WHEN BB_tenure_days BETWEEN 231 AND 329  THEN 3
						WHEN BB_tenure_days BETWEEN 329 AND 391  THEN 4
						WHEN BB_tenure_days BETWEEN 392 AND 499  THEN 5
						WHEN BB_tenure_days BETWEEN 499 AND 641  THEN 6
						WHEN BB_tenure_days BETWEEN 641 AND 1593 THEN 7
						WHEN BB_tenure_days > 1593 				 THEN 8	
						ELSE -1 END 
FROM BB_forecast_segmenting_variables_JAN AS a 
JOIN #Act 			AS b ON a.account_number = b.account_number AND a.observation_date = b.observation_date 

COMMIT 
MESSAGE 'BB_tenure updated: '||@@rowcount type status to client

DROP TABLE #Act

GO












UPDATE BB_forecast_segmenting_variables_JAN
SET node_sa			 = CASE WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 1                                                                                                       	THEN 9
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  IN (2,-1,0, 3)   				                                                                            THEN 10
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  IN (4,5) 				                                                                                	THEN 11	-- = 499	
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 6		                                                                                         			THEN 12	--<= 641
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')            	THEN 60	/* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m = 0 AND home_owner_status IN ('Owner')                                            	THEN 61 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m > 0 AND home_owner_status IN ('Council Rent','Owner')                             	THEN 62 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 7 AND my_sky_login_3m > 0 AND home_owner_status IN ('Private Rent','UNKNOWN')                           	THEN 63 /* <= 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 8 AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends') 	THEN 64 /* > 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 8 AND my_sky_login_3m = 0 AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')     	THEN 65 /* > 1593 */ 
							WHEN BB_offer_rem_and_end IN (-9999,-3) AND BB_tenure  = 8 AND my_sky_login_3m > 0                                                                               	THEN 35 /* > 1593 */ 
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 0 AND talk_type 	  IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')         			THEN 36 
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')   THEN 66
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Owner')                                   THEN 67
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m > 0 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Council Rent','Owner')                          THEN 68
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m = 1 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Private Rent','UNKNOWN')                        THEN 82
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m > 1 AND talk_type NOT IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra') AND home_owner_status IN ('Private Rent','UNKNOWN')                        THEN 83
							WHEN BB_offer_rem_and_end = -2  AND my_sky_login_3m > 0 AND talk_type IN ('Sky Talk Anytime','Sky Talk Freetime','Sky Talk Anytime Extra')                        	THEN 39
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Talk International Extra','Sky Talk Anytime Extra')                            	THEN 40
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m = 0 AND talk_type IN ('Sky Talk International Extra','Sky Talk Anytime Extra')                                	THEN 41
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m > 0 AND home_owner_status IN ('Council Rent','Owner','UNKNOWN')                                               	THEN 42
							WHEN BB_offer_rem_and_end = -1  AND my_sky_login_3m > 0 AND home_owner_status IN ('Private Rent')                                                                 	THEN 43
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')     AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN')   THEN 70
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')     AND home_owner_status IN ('Owner')                                   THEN 71
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Council Rent','Owner')                    THEN 72
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m = 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited') AND home_owner_status IN ('Private Rent','UNKNOWN')                  THEN 73
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m > 0 AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')                       				THEN 46
							WHEN BB_offer_rem_and_end = 0   AND my_sky_login_3m > 0 AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Freetime','Sky Talk Unlimited')                   				THEN 47
							WHEN BB_offer_rem_and_end = 1   AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                                                          				THEN 21
							WHEN BB_offer_rem_and_end = 1   AND talk_type IN ('Sky Pay As You Talk')                                                                                               				THEN 22
							WHEN BB_offer_rem_and_end = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m = 0 AND home_owner_status IN ('Council Rent','Owner')          				THEN 74
							WHEN BB_offer_rem_and_end = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m = 0 AND home_owner_status IN ('Private Rent','UNKNOWN')        				THEN 75
							WHEN BB_offer_rem_and_end = 1	AND talk_type IN ('Sky Talk Evenings and Weekends') AND my_sky_login_3m > 0                                                            				THEN 49
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Council Rent')                                                                                              				THEN 24
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                       				THEN 50
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                           				THEN 51
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m = 0 AND BB_tenure IN (0,-1,1,2)                                				THEN 76
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m = 0 AND BB_tenure IN (3,4,5,6,7,8)                             				THEN 77
							WHEN BB_offer_rem_and_end = 2	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m > 0                                                            				THEN 53
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Council Rent')                                                                                              				THEN 27
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                       				THEN 54
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','Sky Talk Evenings and Weekends')                           				THEN 55
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m = 0				                                                            THEN 56
							WHEN BB_offer_rem_and_end = 3	AND home_owner_status IN ('Private Rent','UNKNOWN') AND my_sky_login_3m > 0                                                            				THEN 57
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure IN (0,-1,1,2,3,4) AND my_sky_login_3m = 0 	THEN 84
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure IN (0,-1,1,2,3,4)	AND my_sky_login_3m > 0 THEN 85
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Council Rent','Private Rent','UNKNOWN') AND BB_tenure IN (5,6,7,8)                               THEN 79
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Owner') AND talk_type IN ('Sky Pay As You Talk','NONE','Sky Talk Anytime Extra')                 THEN 80
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m = 0 AND home_owner_status IN ('Owner') AND talk_type NOT IN ('Sky Pay As You Talk','NONE','Sky Talk Anytime Extra')             THEN 81
							WHEN BB_offer_rem_and_end = 4	AND BB_all_calls_1m > 0                                                                                                                             THEN 31  
							ELSE 0
                         END
		

UPDATE BB_forecast_segmenting_variables_JAN
SET segment_sa = CASE 	WHEN node_sa IN ( 22,  46,  49,  70,  75,  71,  83,  53,  43,  82,  73,  57,  63,  47,  68,  42,  62,  12,  39,  11,  35) THEN 'High Risk'
					WHEN node_sa IN ( 21,  74,  72,  40,  36,  66,  60,  65,  77,  31,  84,  56,  76,  10,  41,  67) THEN 'Medium Risk'
					WHEN node_sa IN ( 61,  51,  64,  24,  50,  27,  55,  85,  81,  79,  80,  54,  9) THEN 'Low Risk'
					ELSE 'No segment'
				END
GO 